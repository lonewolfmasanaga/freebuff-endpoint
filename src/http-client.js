// HTTP(S) client with universal proxy support: SOCKS4/5(h), HTTP(S) CONNECT, or direct.
// The proxy applies ONLY to upstream codebuff.com traffic — the local listener is untouched.
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { Agent, ProxyAgent, request as undiciRequest, interceptors, buildConnector } from 'undici';
import { SocksClient } from 'socks';

const CLI_VERSION = '0.0.156';

export function currentCliVersion() {
  return CLI_VERSION;
}

export function generateUserAgent() {
  return `ai-sdk/openai-compatible/1.0.25/codebuff`;
}

const B36 = '0123456789abcdefghijklmnopqrstuvwxyz';
export function generateClientSessionId() {
  const out = new Array(13);
  for (let i = 0; i < 13; i++) out[i] = B36[Math.floor(Math.random() * 36)];
  return out.join('');
}

function detectFingerprint() {
  if (fingerprintCache !== undefined) return fingerprintCache;
  fingerprintCache = null; // memoized: resolved once per process, not at import time
  try {
    const credsDir = process.env.MANICODE_CREDS_DIR || path.join(os.homedir(), '.config', 'manicode');
    const p = path.join(credsDir, 'credentials.json');
    const cred = JSON.parse(fs.readFileSync(p, 'utf8'));
    if (cred?.default?.fingerprintId && cred?.default?.fingerprintHash) {
      fingerprintCache = `${cred.default.fingerprintId}:${cred.default.fingerprintHash}`;
    }
  } catch { /* ignore */ }
  return fingerprintCache;
}

let dispatcher = null;
let fingerprintCache; // undefined until first resolve
export let proxyDescription = 'direct';

/** Forwarding the real CLI fingerprint binds all traffic to one install identity — opt-in. */
function fingerprintHeaderOrNull() {
  if (!/^(1|true|yes)$/i.test(process.env.FREEBUFF_SEND_FINGERPRINT || '')) return null;
  return detectFingerprint();
}

/** Active dispatcher for auxiliary outbound sync traffic (model registry). */
export function getDispatcher() {
  if (!dispatcher) configureProxy('');
  return dispatcher;
}

/** Swap in a new dispatcher; retire the old one after a grace period for in-flight requests. */
function setDispatcher(next, description) {
  const old = dispatcher;
  dispatcher = next;
  proxyDescription = description;
  if (old) {
    const t = setTimeout(() => { old.close?.().catch?.(() => {}); }, 30_000);
    t.unref?.();
  }
}

/** Custom undici connector: dial the SOCKS proxy, then tunnel to the target. */
function makeSocksConnector(proxyUrl) {
  const u = new URL(proxyUrl);
  const proxy = {
    host: u.hostname,
    port: Number(u.port) || 1080,
    type: u.protocol.startsWith('socks4') ? 4 : 5,
    userId: u.username ? decodeURIComponent(u.username) : undefined,
    password: u.password ? decodeURIComponent(u.password) : undefined,
  };
  const establishTls = buildConnector({ timeout: 20_000 });

  return function connect(opts, callback) {
    // Prefer bare hostname — opts.host can embed the port and break SOCKS CONNECT.
    const host = opts.hostname || String(opts.host || '').split(':')[0];
    const port = Number(opts.port) || (opts.protocol === 'https:' ? 443 : 80);
    // Connector contract: invoke the callback exactly once, always.
    let done = false;
    const once = (err, sock) => {
      if (done) return;
      done = true;
      callback(err, sock);
    };
    SocksClient.createConnection({
      proxy,
      command: 'connect',
      destination: { host, port },
      timeout: 20_000,
    })
      .then(({ socket }) => {
        socket.setNoDelay(true);
        if (opts.protocol !== 'https:') {
          once(null, socket);
          return;
        }
        // TLS through the tunnel; servername drives SNI.
        try {
          establishTls({ ...opts, httpSocket: socket, servername: opts.servername || host }, (err, tlsSock) => {
            if (err) {
              try { socket.destroy(); } catch { /* already dead */ }
            }
            once(err, tlsSock);
          });
        } catch (err) {
          try { socket.destroy(); } catch { /* already dead */ }
          once(err, null);
        }
      })
      .catch((err) => once(err, null));
  };
}

/** Build (or rebuild) the dispatcher for the configured PROXY_URL ('' = direct). */
export function configureProxy(proxyUrl) {
  proxyUrl = (proxyUrl || '').trim();
  if (typeof globalThis.__freebuffBaseURL !== 'string' || !globalThis.__freebuffBaseURL) {
    globalThis.__freebuffBaseURL = 'https://www.codebuff.com'; // documented default
  }
  const redirectInterceptor = interceptors.redirect({ maxRedirections: 5 }); // upstream issues 307s; Go's default client follows them
  if (!dispatcher) {
    // Guarantee a working egress even when PROXY_URL is invalid — undici
    // throws on a null dispatcher and upstreamRequest depends on one.
    setDispatcher(new Agent({ connect: { timeout: 15_000 } }).compose(redirectInterceptor), 'direct (no proxy)');
  }
  if (!proxyUrl) {
    setDispatcher(new Agent({ connect: { timeout: 15_000 } }).compose(redirectInterceptor), 'direct (no proxy)');
    return { ok: true, mode: proxyDescription };
  }
  let u;
  try {
    u = new URL(proxyUrl);
  } catch {
    return { ok: false, error: `invalid PROXY_URL: ${proxyUrl}` };
  }
  if (u.protocol === 'http:' || u.protocol === 'https:') {
    // Native CONNECT-tunneling proxy (also works for https-over-http-proxy).
    setDispatcher(new ProxyAgent({ uri: proxyUrl, connectTimeout: 15_000 }).compose(redirectInterceptor), `HTTP proxy ${u.host}`);
    return { ok: true, mode: proxyDescription };
  }
  if (u.protocol.startsWith('socks')) {
    setDispatcher(new Agent({ connect: makeSocksConnector(proxyUrl) }).compose(redirectInterceptor), `SOCKS proxy ${u.host}`);
    return { ok: true, mode: proxyDescription };
  }
  return { ok: false, error: `unsupported proxy protocol: ${u.protocol} (use socks5:// or http://)` };
}

/**
 * Upstream request. Returns { status, headers, stream } where stream is a
 * standard web ReadableStream of the raw response body (SSE-safe).
 */
export async function upstreamRequest({ method = 'POST', pathname, authToken, body, extraHeaders = {} }) {
  const base = globalThis.__freebuffBaseURL.replace(/\/+$/, '');
  const fingerprint = fingerprintHeaderOrNull();
  const headers = {
    authorization: `Bearer ${authToken}`,
    accept: 'application/json, text/event-stream',
    'user-agent': generateUserAgent(),
    ...(fingerprint ? { 'x-codebuff-fingerprint': fingerprint } : {}),
    ...extraHeaders,
  };
  if (body !== undefined) headers['content-type'] = 'application/json';

  const res = await undiciRequest(base + pathname, {
    method,
    headers,
    body: body !== undefined ? JSON.stringify(body) : undefined,
    // Never pass null: undici throws "Cannot read properties of null" and the
    // global dispatcher (no proxy) is a safer last resort than a crash.
    dispatcher: dispatcher || undefined,
  });

  const h = {};
  for (const [k, v] of Object.entries(res.headers)) {
    // Keep multiple set-cookie headers intact as an array; join everything else.
    h[k] = Array.isArray(v) ? (k.toLowerCase() === 'set-cookie' ? v : v.join(', ')) : String(v);
  }
  return { status: res.statusCode, headers: h, stream: toWebStream(res.body) };
}

/** Normalize undici's response body into a standard web ReadableStream. */
function toWebStream(body) {
  if (!body) return null;
  if (typeof body.getReader === 'function') return body;
  const iterator = body[Symbol.asyncIterator]();
  return new ReadableStream({
    async pull(controller) {
      try {
        const { done, value } = await iterator.next();
        if (done) controller.close();
        else controller.enqueue(value);
      } catch (e) {
        controller.error(e);
      }
    },
    cancel(reason) {
      try { iterator.return?.(reason)?.catch?.(() => {}); } catch { /* ignore */ }
    },
  });
}

export async function readJson(stream, limit = 1_000_000) {
  const reader = stream.getReader();
  const chunks = [];
  let total = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      chunks.push(Buffer.from(value));
      total += value.length;
      if (total > limit) throw new Error('response too large');
    }
  } finally {
    // Release the socket even when we abandon the body mid-read.
    try { await reader.cancel(); } catch { /* stream already closed/errored */ }
    try { reader.releaseLock(); } catch { /* lock already released */ }
  }
  return JSON.parse(Buffer.concat(chunks).toString('utf8'));
}
