// Single-token run manager: one session per token, one open agent-run per
// agent, restarted when stale. No pooling, no rotation, no cooldowns.
// Keeps the lease shape openai.js expects (lease.pool.token) so the
// protocol layer stays untouched.
import { Upstream } from './upstream.js';
import { SessionManager } from './session.js';

function short(t) {
  return t ? `${t.slice(0, 6)}…` : '(none)';
}

export class RunManager {
  constructor(logger, config) {
    this.log = logger;
    this.cfg = config;
    this.upstream = new Upstream(logger, config.DEBOUNCE_MS);
    this.sessions = new SessionManager(logger, { debounceMs: config.DEBOUNCE_MS, maxQueueWaitMs: config.WAITING_ROOM_MAX_WAIT_MS });
    this.token = null;
    this.runs = new Map(); // agentId -> { id, startedAt, inflight, requests }
    this.shuttingDown = false;
    this.lastError = null;
  }

  setTokens(tokens) {
    if (this.shuttingDown) throw new Error('cannot setTokens while shutting down');
    const next = tokens[0] || null;
    if (next && next !== this.token) {
      // Switching tokens: drop cached runs (they belong to the old token).
      this.runs.clear();
    }
    this.token = next;
  }

  async acquire(agentId, model = null) {
    if (this.shuttingDown) throw new Error('gateway is shutting down');
    if (!this.token) throw new Error('no auth token configured');

    const instanceId = await this.sessions.ensure(this.token, model); // may throw WaitingRoomError

    let run = this.runs.get(agentId);
    const ageMin = run ? (Date.now() - run.startedAt) / 60_000 : Infinity;
    if (!run || ageMin >= this.cfg.ROTATION_INTERVAL_MIN) {
      run = await this._restartRun(agentId);
    }
    run.inflight++;
    run.requests++;
    // `pool` shim keeps openai.js's lease.pool.token access working.
    return { pool: { token: this.token }, run, instanceId, agentId };
  }

  async _restartRun(agentId) {
    const cur = this.runs.get(agentId);
    if (cur) {
      await this.upstream.finishRun(this.token, cur.id, cur.requests).catch(() => {});
    }
    const runId = await this.upstream.startRun(this.token, agentId);
    const run = { id: runId, startedAt: Date.now(), inflight: 0, requests: 0 };
    this.runs.set(agentId, run);
    this.log.info(`run started (${short(this.token)}) agent=${agentId} id=${runId.slice(0, 8)}…`);
    return run;
  }

  release(lease) {
    if (!lease) return;
    lease.run.inflight = Math.max(0, lease.run.inflight - 1);
  }

  invalidateRun(lease) {
    if (!lease) return;
    if (this.runs.get(lease.agentId)?.id === lease.run.id) {
      this.runs.delete(lease.agentId);
    }
  }

  /** Drop the cached run matching this id (single-token: token is unused). */
  invalidateRunById(_token, runId) {
    for (const [agentId, run] of this.runs) {
      if (run.id === runId) {
        this.runs.delete(agentId);
        this.log.info(`run invalidated agent=${agentId}`);
        return;
      }
    }
  }

  /**
   * Auth-failure path. Single-token mode has no pool to rotate to: invalidate
   * the session (forces re-admit on retry) and drop cached runs. No timed
   * cooldown — the next acquire simply re-admits.
   */
  markAuthCooldownByToken(_token) {
    this.runs.clear();
    this.lastError = `${new Date().toISOString()} auth rejected — session invalidated`;
    this.sessions.invalidate(_token, 'auth rejected');
    this.log.warn(`auth rejected (${_token ? 'token' : 'none'}) — session invalidated, runs dropped`);
  }

  snapshot() {
    if (!this.token) return [];
    return [{
      token: this.token.slice(0, 6) + '…',
      cooldown_s: null,
      last_error: this.lastError,
      runs: [...this.runs.entries()].map(([agentId, r]) => ({
        agent: agentId,
        run_id: r.id.slice(0, 8),
        age_min: Math.round((Date.now() - r.startedAt) / 60_000),
        inflight: r.inflight,
        requests: r.requests,
      })),
    }];
  }

  async shutdown() {
    this.shuttingDown = true;
    // Quiesce: let in-flight requests drain (bounded) before FINISHing runs.
    const deadline = Date.now() + 10_000;
    for (;;) {
      const inflight = [...this.runs.values()].reduce((m, r) => m + r.inflight, 0);
      if (inflight === 0 || Date.now() >= deadline) break;
      await new Promise((r) => setTimeout(r, 100));
    }
    for (const [, run] of [...this.runs]) {
      await this.upstream.finishRun(this.token, run.id, run.requests).catch(() => {});
    }
    this.runs.clear();
    if (this.token) {
      await this.sessions.end(this.token).catch(() => {});
    }
  }
}
