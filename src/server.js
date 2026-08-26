// Freebuff Endpoint — OpenAI + Anthropic compatible gateway for Freebuff's free models.
import http from 'node:http';
import crypto from 'node:crypto';
import { config, resolveTokens, saveConfig, detectCliToken } from './config.js';
import { configureProxy, proxyDescription, currentCliVersion } from './http-client.js';
import { ModelRegistry } from './registry.js';
import { RunManager } from './runs.js';
import { runCompletion, openaiError } from './openai.js';
import { handleMessages, anthropicError, estimateTokens } from './anthropic.js';
import { sendDashboard } from './dashboard.js';

// ---- tiny logger -----------------------------------------------------------
const log = {
  info: (...a) => console.log(new Date().toISOString(), 'INFO ', ...a),
  warn: (...a) => console.warn(new Date().toISOString(), 'WARN ', ...a),
  error: (...a) => console.error(new Date().toISOString(), 'ERROR', ...a),
};

globalThis.__freebuffBaseURL = config.UPSTREAM_BASE_URL;

// ---- boot ------------------------------------------------------------------
const proxyResult = configureProxy(config.PROXY_URL);
if (!proxyResult.ok) log.warn(`proxy config problem: ${proxyResult.error} (falling back to ${proxyDescription})`);

const tokens = resolveTokens();
if (!tokens.length) {
  log.error(
    'No auth tokens. Either log in with `freebuff` CLI (auto-detected), set AUTH_TOKENS in config.json, or export AUTH_TOKENS=...',
  );
  process.exit(1);
}
log.info(`${tokens.length} auth token(s) loaded`);

const registry = new ModelRegistry(log);
registry.start(config.REGISTRY_REFRESH_MIN);

const runs = new RunManager(log, config);
runs.setTokens(tokens);

let shuttingDown = false;

// Opaque id <-> token maps for the dashboard (never ship raw tokens back).
const tokenIdByValue = new Map();
const tokenValueById = new Map();

// Optional prewarm: session + a default-agent run so first request is fast.
(async () => {
  try {
    const prewarmModel = (config.PREWARM_MODEL || '').trim() || registry.models()[0];
    const agent = prewarmModel ? registry.agentForModel(prewarmModel) : null;
    if (agent) {
      const l = await runs.acquire(agent);
      runs.release(l);
    }
    log.info('prewarmed free session');
  } catch (e) {
    log.warn(`prewarm skipped: ${e.message}`);
  }
})();

// ---- helpers ----------------------------------------------------------------
const BODY_LIMIT = 5_000_000; // 5MB is generous for chat payloads

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    let settled = false;
    const done = (fn, arg) => {
      if (!settled) {
        settled = true;
        fn(arg);
      }
    };
    req.on('data', (c) => {
      size += c.length;
      if (size > BODY_LIMIT) {
        done(reject, new Error('body too large (limit 5MB)'));
        req.destroy(); // socket dies AFTER we've rejected; handler still responds if possible
        return;
      }
      chunks.push(c);
    });
    req.on('end', () => done(resolve, Buffer.concat(chunks).toString('utf8')));
    req.on('error', (e) => done(reject, e));
  });
}

/** Parse a JSON request body; responds and returns null on any problem. */
async function parseJsonObject(req, res, errFormatter) {
  let raw;
  try {
    raw = await readBody(req);
  } catch (e) {
    if (!res.headersSent) sendJson(res, e.message.startsWith('body too large') ? 413 : 400, errFormatter(e.message));
    return null;
  }
  try {
    const body = JSON.parse(raw);
    if (typeof body !== 'object' || body === null || Array.isArray(body)) throw new Error('JSON body must be an object');
    return body;
  } catch (e) {
    sendJson(res, 400, errFormatter(`bad JSON body: ${e.message}`));
    return null;
  }
}

function sendJson(res, status, body, extraHeaders = {}) {
  if (res.headersSent || res.destroyed) {
    res.destroy();
    return;
  }
  const payload = JSON.stringify(body);
  res.writeHead(status, {
    'content-type': 'application/json',
    'content-length': Buffer.byteLength(payload),
    ...extraHeaders,
  });
  res.end(payload);
}

function authorized(req) {
  if (!config.API_KEYS.length) return true;
  const h = req.headers.authorization || '';
  const key = h.startsWith('Bearer ') ? h.slice(7) : req.headers['x-api-key'] || '';
  return config.API_KEYS.includes(key);
}

/** Pump a web ReadableStream to the response as SSE; never throws. */
async function pumpSse(res, stream, extraHeaders = {}) {
  res.writeHead(200, {
    'content-type': 'text/event-stream; charset=utf-8',
    'cache-control': 'no-cache',
    connection: 'keep-alive',
    ...extraHeaders,
  });
  const reader = stream.getReader();
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      if (res.destroyed) break;
      res.write(value);
    }
  } catch {
    /* upstream cancelled / client gone */
  } finally {
    reader.releaseLock();
    stream.cancel?.().catch?.(() => {});
    if (!res.destroyed) res.end();
  }
}

/** Abort when the client disconnects; also enforces a max request lifetime. */
function requestSignal(req, timeoutMs = config.REQUEST_TIMEOUT_MS) {
  const controller = new AbortController();
  req.on('close', () => controller.abort());
  const cap = Math.min(Number(timeoutMs) || config.REQUEST_TIMEOUT_MS, config.REQUEST_TIMEOUT_MS + 30_000);
  const t = setTimeout(() => controller.abort(), cap);
  t.unref?.();
  controller.signal.addEventListener('abort', () => clearTimeout(t), { once: true });
  return controller.signal;
}

// ---- server -----------------------------------------------------------------
const server = http.createServer((req, res) => {
  // Handler is async but never allowed to reject: every path is guarded.
  handleRequest(req, res).catch((e) => {
    log.error(`request ${req.method} ${req.url} crashed: ${e.stack || e.message}`);
    if (!res.headersSent && !res.destroyed) {
      sendJson(res, 500, openaiError('internal error', 'internal'));
    } else {
      res.destroy();
    }
  });
});

res_on_error(server);

async function handleRequest(req, res) {
  let p = '/';
  try {
    p = new URL(req.url, 'http://localhost').pathname; // fixed base: only pathname is used
  } catch {
    /* keep '/' — malformed URL falls through to 404 */
  }

  if (req.method === 'OPTIONS') {
    res.writeHead(204, {
      'access-control-allow-origin': '*',
      'access-control-allow-methods': 'GET, POST, DELETE, OPTIONS',
      'access-control-allow-headers': 'authorization, content-type, x-api-key, anthropic-version',
    });
    return void res.end();
  }

  // Minimal unauthenticated liveness probe (no operational details).
  if (p === '/healthz') {
    return sendJson(res, 200, { ok: true });
  }

  // Everything below requires auth when API_KEYS is set. /health and /status are
  // operator endpoints and sit behind the same gate.
  if (!authorized(req)) {
    return sendJson(res, 401, openaiError('invalid API key', 'invalid_api_key'));
  }

  if (p === '/health') {
    return sendJson(res, 200, {
      service: 'freebuff-endpoint',
      status: shuttingDown ? 'shutting_down' : 'ok',
      cli_version_tracked: currentCliVersion(),
      proxy: proxyDescription,
      tokens: runs.snapshot().length,
      models: registry.models().length,
    });
  }

  if (p === '/status') {
    return sendJson(res, 200, {
      listen: config.LISTEN_ADDR,
      upstream: config.UPSTREAM_BASE_URL,
      proxy: proxyDescription,
      registry: registry.status(),
      tokens: runs.snapshot(),
      sessions: runs.sessions.snapshot(),
    });
  }

  // ---------- OpenAI surface ----------
  if (p === '/v1/models' && req.method === 'GET') {
    return sendJson(res, 200, {
      object: 'list',
      data: registry.models().map((id) => ({ id, object: 'model', created: 0, owned_by: 'freebuff' })),
    });
  }

  if (p === '/v1/chat/completions' && req.method === 'POST') {
    if (shuttingDown) return sendJson(res, 503, openaiError('server shutting down', 'unavailable'));
    const body = await parseJsonObject(req, res, (m) => openaiError(m, 'invalid_request'));
    if (!body) return;

    const model = typeof body.model === 'string' ? body.model : registry.models()[0];
    const wantStream = !!body.stream;
    const signal = requestSignal(req);

    // Pool-aware reroute: earned-pool models transparently fall back to an
    // unlimited model instead of surfacing a 429 ('' disables).
    let fallback = null;
    const fb = (config.POOL_FALLBACK_MODEL || '').trim();
    if (fb && fb !== model && registry.has(fb)) fallback = { preferredModel: fb, used: false };

    const result = await runCompletion({ registry, runs, log, model, payload: body, wantStream, signal, fallback });
    if (result.kind === 'sse') {
      // Surface fallback reroute info in the SSE response headers.
      const extra = { ...(result.headers || {}) };
      return pumpSse(res, result.stream, extra);
    }
    const headers = { ...(result.headers || {}) };
    if (result.retryAfterMs) headers['retry-after'] = String(Math.ceil(result.retryAfterMs / 1000));
    return sendJson(res, result.status, result.body, headers);
  }

  // ---------- Anthropic surface ----------
  if (p === '/v1/messages' && req.method === 'POST') {
    if (shuttingDown) return sendJson(res, 503, anthropicError('server shutting down', 503));
    const body = await parseJsonObject(req, res, (m) => anthropicError(m, 400));
    if (!body) return;

    const wantStream = !!body.stream;
    const signal = requestSignal(req);
    const result = await handleMessages({ registry, runs, log, body, wantStream, signal });
    if (result.stream) return pumpSse(res, result.stream);
    return sendJson(res, result.status, result.body);
  }

  if (p === '/v1/messages/count_tokens' && req.method === 'POST') {
    const body = await parseJsonObject(req, res, (m) => anthropicError(m, 400));
    if (!body) return;
    return sendJson(res, 200, { input_tokens: estimateTokens(body) });
  }

  // ---------- Dashboard ----------
  if (p === '/' && req.method === 'GET') {
    return sendDashboard(res);
  }

  // ---------- Admin API (dashboard) ----------
  if (p === '/admin/tokens' && req.method === 'GET') {
    const cli = detectCliToken();
    const seen = new Set();
    const entries = [];
    for (const [t, source] of [
      ...config.AUTH_TOKENS.map((t) => [t, 'config.json']),
      ...(cli && !config.AUTH_TOKENS_OVERRIDE_CLI ? [[cli, 'CLI auto-detect']] : []),
    ]) {
      if (seen.has(t)) continue;
      seen.add(t);
      // Opaque id -> full token map lets the GUI delete without ever
      // shipping raw tokens back over the wire.
      let id = tokenIdByValue.get(t);
      if (!id) {
        id = crypto.randomBytes(8).toString('hex');
        tokenIdByValue.set(t, id);
        tokenValueById.set(id, t);
      }
      entries.push({ id, masked: maskToken(t), source });
    }
    return sendJson(res, 200, { tokens: entries });
  }

  if (p === '/admin/tokens' && req.method === 'POST') {
    const body = await parseJsonObject(req, res, (m) => openaiError(m, 'invalid_request'));
    if (!body) return;
    const token = typeof body.token === 'string' ? body.token.trim() : '';
    if (token.length < 10) return sendJson(res, 400, openaiError('token looks invalid (too short)', 'invalid_request'));
    const configuredList = [...new Set([...(config.AUTH_TOKENS || []), token])];
    // Live set also includes the CLI-detected token unless overridden.
    const liveList = [...new Set([...resolveTokens(), token])];
    saveConfig({ AUTH_TOKENS: configuredList });
    await runs.setTokens(liveList);
    log.info(`token added via dashboard (${maskToken(token)}), ${liveList.length} active`);
    return sendJson(res, 200, { ok: true, count: liveList.length });
  }

  if (p === '/admin/tokens' && req.method === 'DELETE') {
    const body = await parseJsonObject(req, res, (m) => openaiError(m, 'invalid_request'));
    if (!body) return;
    const id = typeof body.id === 'string' ? body.id : '';
    const token = tokenValueById.get(id);
    if (!token) return sendJson(res, 404, openaiError('unknown token id (stale list?) — refresh', 'not_found'));
    const cli = detectCliToken();
    if (cli && token === cli && !config.AUTH_TOKENS.includes(cli)) {
      // The CLI-detected token is disabled via an override flag, otherwise it
      // would resurrect on every restart.
      saveConfig({ AUTH_TOKENS_OVERRIDE_CLI: true });
      config.AUTH_TOKENS_OVERRIDE_CLI = true;
      log.info('CLI auto-detect token disabled via dashboard');
    } else {
      saveConfig({ AUTH_TOKENS: (config.AUTH_TOKENS || []).filter((t) => t !== token) });
    }
    const liveList = resolveTokens().filter((t) => t !== token);
    await runs.setTokens(liveList);
    tokenIdByValue.delete(token);
    tokenValueById.delete(id);
    log.info(`token removed via dashboard (${maskToken(token)}), ${liveList.length} active`);
    return sendJson(res, 200, { ok: true, count: liveList.length });
  }

  if (p === '/admin/test' && req.method === 'POST') {
    if (shuttingDown) return sendJson(res, 503, openaiError('server shutting down', 'unavailable'));
    const body = await parseJsonObject(req, res, (m) => openaiError(m, 'invalid_request'));
    if (!body) return;
    const model = typeof body.model === 'string' ? body.model : registry.models()[0];
    const fb = (config.POOL_FALLBACK_MODEL || '').trim();
    const fallback = fb && fb !== model && registry.has(fb) ? { preferredModel: fb, used: false } : null;
    const result = await runCompletion({
      registry,
      runs,
      log,
      model,
      payload: { model, messages: [{ role: 'user', content: String(body.prompt || 'Say READY.') }], max_tokens: 300 },
      wantStream: false,
      signal: requestSignal(req),
      fallback,
    });
    if (result.kind !== 'json' || result.status !== 200) {
      const msg = result.body?.error?.message || 'upstream error';
      return sendJson(res, result.status >= 400 ? result.status : 502, openaiError(msg, 'upstream_error'));
    }
    return sendJson(res, 200, {
      ok: true,
      model: result.body.model,
      reply: result.body.choices?.[0]?.message?.content ?? '',
      served_by: result.body.freebuff_served_by ?? null,
    });
  }

  return sendJson(res, 404, openaiError(`no route: ${req.method} ${p}`, 'not_found'));
}

function maskToken(t) {
  return t ? `${t.slice(0, 6)}…${t.slice(-4)}` : '(none)';
}


function res_on_error(server) {
  server.on('clientError', (err, socket) => {
    try {
      if (socket.writable) socket.end('HTTP/1.1 400 Bad Request\r\n\r\n');
      else socket.destroy();
    } catch {
      socket.destroy();
    }
  });
}

server.listen(...parseListen(config.LISTEN_ADDR), () => {
  log.info(`freebuff-endpoint listening on ${config.LISTEN_ADDR}`);
  log.info(`upstream: ${config.UPSTREAM_BASE_URL} | egress: ${proxyDescription}`);
  log.info(`models: ${registry.models().length} (fallback until first sync)`);
});

for (const sig of ['SIGINT', 'SIGTERM']) {
  process.on(sig, async () => {
    if (shuttingDown) return process.exit(0);
    shuttingDown = true;
    log.info(`${sig} received, draining…`);
    setTimeout(() => process.exit(0), 8_000).unref?.(); // hard cap on graceful window
    let failed = false;
    try {
      await runs.shutdown();
      registry.stop();
      // Stop accepting new connections; close idle ones immediately.
      server.close();
      server.closeIdleConnections?.();
      server.closeAllConnections?.();
    } catch (e) {
      failed = true;
      log.warn(`shutdown cleanup issue: ${e.message}`);
    }
    process.exit(failed ? 1 : 0);
  });
}

process.on('unhandledRejection', (reason) => {
  log.error(`unhandledRejection: ${reason?.stack || reason}`);
});

function parseListen(addr) {
  // Accept 'host:port', ':port', or a bare port; fail fast with a clear error.
  if (typeof addr === 'number' && Number.isInteger(addr) && addr > 0 && addr < 65536) return [addr];
  const i = addr.lastIndexOf(':');
  const port = i === -1 ? Number(addr) : Number(addr.slice(i + 1));
  const host = addr.slice(0, i) || undefined;
  if (!Number.isInteger(port) || port <= 0 || port >= 65536) {
    throw new Error(`invalid LISTEN_ADDR "${addr}" — expected host:port or bare port (e.g. 127.0.0.1:8090)`);
  }
  return [port, host];
}
