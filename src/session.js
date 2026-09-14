// Free-session lifecycle per auth token: admission, waiting room, expiry, invalidation.
// Mirrors upstream semantics: POST /api/v1/freebuff/session (create/refresh),
// GET with x-freebuff-instance-id (poll while queued), DELETE (end).
import { upstreamRequest, readJson } from './http-client.js';
import { WaitingRoomError, SessionRateLimitedError, RegionBlockedError } from './errors.js';

// Admission verdicts that mean the account/region itself is not allowed to use
// this tier — terminal and not quota-related, so they surface as the clear
// 'region_or_account_blocked' error instead of a generic upstream failure.
const REGION_BLOCKED_STATUSES = new Set([
  'country_blocked',
  'country_restricted',
  'region_blocked',
  'region_restricted',
  'banned',
  'suspended',
  'full_access_required',
  'limited_only',
  'not_available_in_region',
]);

export class SessionManager {
  constructor(logger, { debounceMs = 0, maxQueueWaitMs = 120_000 } = {}) {
    this.log = logger;
    this.debounceMs = debounceMs;
    this.maxQueueWaitMs = maxQueueWaitMs;
    // token -> { status, instanceId, expiresAt(ms), model, position, queueDepth, retryAt(ms), lastError }
    this.sessions = new Map();
    this.warnedExpiryParse = false;
  }

  // Expiry safety margin must cover post-ensure latency (throttle slot wait can
  // be up to one full debounce interval, plus queueing behind other requests).
  get _expiryMarginMs() {
    return Math.max(5_000, this.debounceMs * 2);
  }

  invalidate(token, reason) {
    const s = this.sessions.get(token);
    if (s) {
      s.status = 'none';
      s.instanceId = null;
      s.lastError = reason || s.lastError;
    }
  }

  snapshot() {
    return [...this.sessions.entries()].map(([tokenKey, s]) => ({
      token: maskToken(tokenKey),
      status: s.status,
      instance: s.instanceId ? s.instanceId.slice(0, 8) : null,
      model: s.model || null,
      position: s.position ?? null,
      queue_depth: s.queueDepth ?? null,
      expires_in_s: s.expiresAt ? Math.max(0, Math.round((s.expiresAt - Date.now()) / 1000)) : null,
      last_error: s.lastError || null,
    }));
  }

  /**
   * Ensure an active session for this token. Returns instanceId ('' if disabled).
   * `model` selects which model the session is admitted for (x-freebuff-model
   * header, exactly like the CLI) — premium models require a full-access tier.
   * Throws WaitingRoomError when queued.
   */
  async ensure(token, model = null) {
    // A session admitted for model X cannot serve model Y; re-admit when the
    // requested model differs from what the session is actually bound to.
    // `cur.model` is server truth when upstream reports it (see below); when
    // unknown we defer to admission-time binding + the model_locked recovery
    // path rather than hammering the admission endpoint speculatively.
    let cur = this.sessions.get(token);
    if (model && cur && cur.model && cur.model !== model && (cur.status === 'active' || cur.status === 'queued')) {
      await this.end(token);
      cur = undefined;
    }
    // Waiting-room patience: keep polling the queue until admitted or the
    // deadline passes, instead of bouncing 503s at the client. A position-1/1
    // queue clears in seconds; the cap (WAITING_ROOM_MAX_WAIT_MS) is well
    // under the request timeout so a genuinely stuck queue still fails fast.
    const deadline = Date.now() + this.maxQueueWaitMs;
    for (let hop = 0; hop < 64; hop++) {
      let s = this.sessions.get(token);
      const now = Date.now();
      if (s && s.status === 'active' && s.instanceId && now < s.expiresAt - this._expiryMarginMs) {
        return s.instanceId;
      }
      if (s && s.status === 'queued' && now < (s.retryAt || 0)) {
        // A poll for this token is already scheduled by a concurrent request —
        // wait for its slot instead of throwing instantly (which turns every
        // client retry into a hard 503 without ever waiting in the queue).
        const wait = Math.min((s.retryAt || 0) - now, Math.max(1, deadline - now));
        if (wait > 0) {
          await sleep(wait + 50);
          continue;
        }
      }

      try {
        const state =
          s && s.status === 'queued' && s.instanceId
            ? await this.poll(token, s.instanceId)
            : await this.createOrRefresh(token, model);
        // Prefer the server-reported bound model over our request when present.
        const boundModel = state.model || state.currentModel || null;
        // Prefer the server-reported bound model over our request when present;
        // otherwise keep the active binding, falling back to what we asked for.
        const prevModel = cur && cur.status === 'active' ? cur.model : null;
        s = { ...s, ...normalizeState(state), model: boundModel || prevModel || model || null, lastError: null };
        if (state.expiresAt != null && s.expiresAt === 0 && !this.warnedExpiryParse) {
          this.warnedExpiryParse = true;
          this.log.warn(`unparseable session expiresAt (${JSON.stringify(state.expiresAt)}) — treating as expired`);
        }
        this.sessions.set(token, s);

        if (s.status === 'active') {
          this.log.info(`session active (${maskToken(token)}) instance=${s.instanceId?.slice(0, 8)}${s.model ? ` model=${s.model}` : ''}`);
          return s.instanceId;
        }
        if (s.status === 'queued') {
          const delay = clampDelay(state.estimatedWaitMs);
          const wakeAt = Date.now() + delay;
          if (wakeAt > deadline) {
            // Queue won't clear within our patience budget — surface it with a
            // meaningful retry-after so the client backs off instead of
            // hammering (and the runCompletion fallback can re-route).
            throw new WaitingRoomError(s.position, s.queueDepth, Math.max(5_000, deadline - Date.now()));
          }
          s.retryAt = wakeAt;
          this.log.info(
            `waiting room: position ${s.position}/${s.queueDepth}, poll in ${Math.round(delay / 1000)}s`,
          );
          // Drain the full wait (timers can wake marginally early) so the
          // next hop's retryAt guard never fires on our own poll cycle.
          while (Date.now() < wakeAt) await sleep(wakeAt - Date.now());
          continue;
        }
        if (s.status === 'ended' || s.status === 'superseded' || s.status === 'none') {
          continue; // recreate next hop
        }
        if (s.status === 'disabled') {
          return ''; // endpoint not enforcing sessions for this account
        }
      } catch (e) {
        if (e instanceof WaitingRoomError) throw e;
        if (e instanceof SessionRateLimitedError) throw e; // terminal verdict — propagate as-is
        if (e instanceof RegionBlockedError) throw e; // terminal verdict — propagate as-is
        const err = new Error(`session refresh failed: ${e.message}`);
        err.upstream = true;
        throw err;
      }
    }
    const final = this.sessions.get(token);
    if (final && final.status === 'queued') {
      throw new WaitingRoomError(final.position ?? 1, final.queueDepth ?? 1, Math.max(5_000, (final.retryAt || 0) - Date.now()));
    }
    throw new WaitingRoomError(final?.position ?? 1, final?.queueDepth ?? 1, 10_000);
  }

  async createOrRefresh(token, model = null) {
    const extraHeaders = {};
    if (model) extraHeaders['x-freebuff-model'] = model; // CLI sends the desired model at admission
    const res = await upstreamRequest({ pathname: '/api/v1/freebuff/session', authToken: token, body: {}, extraHeaders });
    if (res.status === 404) return { status: 'disabled' };
    if (res.status === 429 || res.status === 409 || res.status === 403) {
      // Structured admission verdicts: rate_limited / spend_limited / ip_capped /
      // model_locked / country_blocked / banned. Parse and rethrow typed.
      const body = await safeBody(res.stream);
      let parsed = null;
      try {
        parsed = JSON.parse(body);
      } catch { /* not json */ }
      if (parsed && parsed.status && parsed.status !== 'active') {
        if (['rate_limited', 'spend_limited', 'ip_capped'].includes(parsed.status)) {
          throw new SessionRateLimitedError(parsed);
        }
        if (REGION_BLOCKED_STATUSES.has(parsed.status)) {
          throw new RegionBlockedError(parsed.status);
        }
        if (parsed.status === 'model_locked') {
          // The upstream still has a session bound to another model (currentModel).
          // Local invalidation alone isn't enough — the ensure() loop would just
          // re-admit and hit model_locked again forever. End the server session so
          // the next admission can bind the requested model. This is the admission
          // counterpart of the completion path's model_locked recovery.
          await this.end(token).catch(() => {});
          return { status: 'none' }; // caller's ensure() loop recreates fresh
        }
        throw Object.assign(new Error(`admission refused: ${parsed.status}${parsed.message ? ` — ${parsed.message}` : ''}`), { status: res.status });
      }
      const e = new Error(`HTTP ${res.status}: ${body.slice(0, 300)}`);
      e.status = res.status;
      throw e;
    }
    if (res.status >= 400) {
      const body = await safeBody(res.stream);
      const e = new Error(`HTTP ${res.status}: ${body.slice(0, 300)}`);
      e.status = res.status;
      throw e;
    }
    return readJson(res.stream);
  }

  async poll(token, instanceId) {
    const res = await upstreamRequest({
      method: 'GET',
      pathname: '/api/v1/freebuff/session',
      authToken: token,
      extraHeaders: { 'x-freebuff-instance-id': instanceId },
    });
    if (res.status === 404) return { status: 'disabled' };
    if (res.status >= 400) {
      const body = await safeBody(res.stream);
      const e = new Error(`HTTP ${res.status}: ${body.slice(0, 300)}`);
      e.status = res.status;
      throw e;
    }
    return readJson(res.stream);
  }

  /**
   * End the upstream session for this token. The DELETE must name the exact
   * instance via x-freebuff-instance-id: without it upstream silently
   * ignores the request while a session is live or queued, so the
   * session-hour keeps billing and no refund is ever issued.
   * Returns { http, body } describing upstream's verdict, or null on failure.
   */
  async end(token) {
    const s = this.sessions.get(token);
    const extraHeaders = s && s.instanceId ? { 'x-freebuff-instance-id': s.instanceId } : {};
    let result = null;
    try {
      const res = await upstreamRequest({ method: 'DELETE', pathname: '/api/v1/freebuff/session', authToken: token, extraHeaders });
      result = {
        http: res.status,
        body: res.status < 400
          ? await readJson(res.stream).catch(() => null)
          : (await safeBody(res.stream)).slice(0, 120) || null,
      };
    } catch { /* best effort — local state is dropped regardless */ }
    this.sessions.delete(token);
    return result;
  }
}

/** Admission refused for quota/pool reasons — terminal for the request, not retryable. */
// (SessionRateLimitedError lives in ./errors.js)

function normalizeState(state) {
  const out = { raw: undefined };
  out.status = String(state.status || '').trim().toLowerCase();
  out.instanceId = state.instanceId || null;
  out.position = state.position ?? null;
  out.queueDepth = state.queueDepth ?? null;
  out.expiresAt = parseExpiry(state.expiresAt);
  return out;
}

/** Accept ISO strings, numeric epoch-ms, and numeric strings; 0 when unusable. */
function parseExpiry(v) {
  if (v == null || v === '') return 0;
  if (typeof v === 'number' && Number.isFinite(v)) return v > 1e12 ? v : v * 1000; // s vs ms heuristic
  const n = Number(v);
  if (v !== '' && Number.isFinite(n)) return n > 1e12 ? n : n * 1000;
  const t = Date.parse(v);
  if (Number.isFinite(t)) return t;
  return 0; // caller logs once via warnedExpiryParse flag on the manager
}

function clampDelay(estimatedWaitMs) {
  const d = Number(estimatedWaitMs) > 0 ? Number(estimatedWaitMs) : 5_000;
  return Math.min(Math.max(d, 2_000), 15_000);
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function maskToken(t) {
  return t ? `${t.slice(0, 6)}…${t.slice(-4)}` : '(none)';
}

async function safeBody(stream) {
  try {
    const reader = stream.getReader();
    const { value } = await reader.read();
    reader.cancel().catch(() => {});
    return value ? Buffer.from(value).toString('utf8') : '';
  } catch {
    return '';
  }
}
