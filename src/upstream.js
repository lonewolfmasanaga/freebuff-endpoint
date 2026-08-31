// Upstream client: agent-run lifecycle + chat completions with metadata injection,
// global debounce, and error classification for retry decisions.
import { upstreamRequest, readJson, generateClientSessionId } from './http-client.js';
import { wrapPayloadForUpstream } from './impersonate.js';

const SESSION_INVALID_CODES = new Set([
  'freebuff_update_required',
  'waiting_room_required',
  'waiting_room_queued',
  'session_superseded',
  'session_expired',
  'no_free_session',
]);

// Request-shape rejections: our fault, not the token's. Never cool down tokens for these.
const CONFORMANCE_CODES = new Set([
  'free_mode_cli_required',
  'free_mode_invalid_agent_model',
  'free_mode_invalid_agent_hierarchy',
]);

// Structured verdicts that arrive over HTTP 403 but are NOT auth failures:
// they describe account/region/quota state. They must not trip the 30-min
// auth cooldown — surface them as their own bucket instead.
const BLOCKED_CODES = new Set([
  'country_blocked',
  'country_restricted',
  'region_blocked',
  'region_restricted',
  'ip_capped',
  'banned',
  'suspended',
  'full_access_required',
  'limited_only',
  'not_available_in_region',
]);

// Model-binding conflicts: the SESSION was admitted for another model. The
// remedy is DELETE session + re-admit with x-freebuff-model (handled by the
// completion core's 'run' bucket special-case), so these stay in 'run'.
const MODEL_BINDING_CODES = new Set(['model_locked', 'session_model_mismatch']);

export class Upstream {
  constructor(logger, debounceMs) {
    this.log = logger;
    this.debounceMs = debounceMs;
    this.nextSlot = 0; // monotonic slot clock: every concurrent caller gets its own slot
    // Stable per (token, run) client identity — mirrors the CLI, which keeps one
    // client_id for the life of an agent-run rather than rotating per message.
    this.clientIds = new Map(); // `${authToken}:${runId}` -> clientId
  }

  async throttle() {
    const at = Math.max(this.nextSlot, Date.now());
    this.nextSlot = at + this.debounceMs;
    const wait = at - Date.now();
    if (wait > 0) await sleep(wait);
  }

  _clientIdFor(authToken, runId) {
    const key = `${authToken}:${runId}`;
    let id = this.clientIds.get(key);
    if (!id) {
      id = generateClientSessionId();
      this.clientIds.set(key, id);
      if (this.clientIds.size > 256) {
        // Bound memory: drop oldest entries (Map preserves insertion order).
        for (const k of this.clientIds.keys()) {
          if (this.clientIds.size <= 128) break;
          this.clientIds.delete(k);
        }
      }
    }
    return id;
  }

  async startRun(authToken, agentId) {
    await this.throttle();
    const res = await upstreamRequest({
      pathname: '/api/v1/agent-runs',
      authToken,
      body: { action: 'START', agentId },
    });
    const body = res.status >= 400 ? await readJson(res.stream).catch(() => ({})) : await readJson(res.stream);
    if (res.status >= 400 || !body.runId) {
      throw httpError(`start run failed: HTTP ${res.status} ${JSON.stringify(body).slice(0, 200)}`, res.status, body?.error);
    }
    return body.runId;
  }

  async finishRun(authToken, runId, totalSteps) {
    try {
      this.clientIds.delete(`${authToken}:${runId}`); // identity dies with the run
      await upstreamRequest({
        pathname: '/api/v1/agent-runs',
        authToken,
        body: { action: 'FINISH', runId, status: 'completed', totalSteps, directCredits: 0, totalCredits: 0 },
      }).then((r) => r.stream.cancel().catch(() => {}));
    } catch { /* best effort */ }
  }

  /**
   * Send a chat completion. Returns { status, headers, stream }.
   * `metadata` gets run_id / cost_mode / client_id / freebuff_instance_id merged in.
   */
  async chatCompletions({ authToken, payload, runId, instanceId }) {
    await this.throttle();
    const body = wrapPayloadForUpstream(payload); // CLI-conformant system marker + signature tool
    const metadata = { ...(body.codebuff_metadata || {}) };
    metadata.run_id = runId;
    metadata.cost_mode = 'free';
    metadata.client_id = this._clientIdFor(authToken, runId); // stable within a run
    if (instanceId) metadata.freebuff_instance_id = instanceId;
    body.codebuff_metadata = metadata;
    // Upstream always streams internally, but it validates that `stream_options`
    // (e.g. Hermes' { include_usage: true }) only ever accompanies stream=true —
    // sending it without stream trips a 400. In streaming mode we forward the
    // raw SSE, so preserve stream=true (usage flows through). In blocking mode
    // we reassemble upstream ourselves, so both belong to no one: strip them.
    if (body.stream) {
      body.stream = true;
    } else {
      delete body.stream;
      delete body.stream_options;
    }

    return upstreamRequest({ pathname: '/api/v1/chat/completions', authToken, body });
  }

  classify(statusCode, errorBodyText) {
    let code = '';
    let message = '';
    try {
      const parsed = JSON.parse(errorBodyText);
      code = parsed?.error || parsed?.status || '';
      message = parsed?.message || '';
    } catch { /* not json */ }
    if (CONFORMANCE_CODES.has(code) || /freebuff CLI/i.test(message)) return 'conformance';
    if (BLOCKED_CODES.has(code)) return 'blocked'; // quota/region verdict — no auth cooldown
    if (statusCode === 401) return 'auth';
    if (statusCode === 403 && !code) return 'auth'; // bare 403 with no structured verdict
    if (statusCode === 403 && MODEL_BINDING_CODES.has(code)) return 'run';
    if (SESSION_INVALID_CODES.has(code)) return 'session';
    if (MODEL_BINDING_CODES.has(code)) return 'run';
    if (statusCode === 429) return 'rate';
    return 'other';
  }

  statusLine() {
    return `debounce=${this.debounceMs}ms`;
  }
}

function httpError(message, status, code) {
  const e = new Error(message);
  e.status = status;
  e.code = code || '';
  return e;
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
