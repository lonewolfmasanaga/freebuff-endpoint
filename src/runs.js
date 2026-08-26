// Run manager: pools of auth tokens, each holding long-lived runs per agent.
// Round-robin across tokens; leases bracket each request; cooldowns on auth errors.
import { Upstream } from './upstream.js';
import { SessionManager } from './session.js';
import { WaitingRoomError } from './errors.js';

export class RunManager {
  constructor(logger, config) {
    this.log = logger;
    this.cfg = config;
    this.upstream = new Upstream(logger, config.DEBOUNCE_MS);
    this.sessions = new SessionManager(logger, { debounceMs: config.DEBOUNCE_MS });
    this.pools = [];
    this.rr = 0;
    this.shuttingDown = false;
    // Serializes rotation per (token, agentId) — closes the TOCTOU window where
    // two concurrent acquires both see a stale run and both START a new one.
    this.creating = new Map(); // `${token}:${agentId}` -> Promise<run>
  }

  setTokens(tokens) {
    const next = new Set(tokens);
    const removed = this.pools.filter((p) => !next.has(p.token));
    this.pools = tokens.map((token) => {
      if (this.shuttingDown) throw new Error('cannot setTokens while shutting down');
      const existing = this.pools.find((p) => p.token === token);
      if (existing) return existing;
      return {
        token,
        runs: new Map(), // agentId -> { id, startedAt, inflight, requests }
        cooldownUntil: 0,
        lastError: null,
      };
    });
    // Removed tokens: drain their upstream runs + sessions in the background so
    // credits aren't leaked and the session map doesn't grow unbounded.
    if (removed.length) {
      this._drainRemoved(removed).catch(() => {});
    }
  }

  async _drainRemoved(pools) {
    for (const pool of pools) {
      for (const [, run] of [...pool.runs]) {
        await this.upstream.finishRun(pool.token, run.id, run.requests).catch(() => {});
      }
      await this.sessions.end(pool.token).catch(() => {});
      this.log.info(`drained ${pools.length} removed token pool(s)`);
    }
  }

  /**
   * Acquire a lease on a healthy token with a live run for agentId.
   * Throws WaitingRoomError if every pool is queued.
   */
  async acquire(agentId, model = null) {
    if (this.shuttingDown) throw new Error('gateway is shutting down');
    let lastErr = null;
    const waitingErrors = [];
    for (let i = 0; i < this.pools.length; i++) {
      const pool = this.pools[(this.rr + i) % this.pools.length];
      try {
        const lease = await this.acquirePool(pool, agentId, model);
        this.rr = (this.rr + i + 1) % Math.max(this.pools.length, 1);
        return lease;
      } catch (e) {
        if (e instanceof WaitingRoomError) waitingErrors.push(e);
        else lastErr = e;
      }
    }
    if (waitingErrors.length === this.pools.length && waitingErrors.length > 0) throw waitingErrors[0];
    throw lastErr || new Error('no auth tokens configured');
  }

  async acquirePool(pool, agentId, model) {
    if (this.shuttingDown) throw new Error('gateway is shutting down');
    if (Date.now() < pool.cooldownUntil) {
      throw new Error(`token ${short(pool.token)} cooling down for another ${Math.ceil((pool.cooldownUntil - Date.now()) / 1000)}s`);
    }
    // If a sibling request is mid-rotation for this (token, agent), wait for it
    // to settle; we will then reuse its fresh run instead of double-STARTing.
    const createKey = `${pool.token}:${agentId}`;
    const prior = this.creating.get(createKey);
    if (prior) await prior.catch(() => {});

    const instanceId = await this.sessions.ensure(pool.token, model); // may throw WaitingRoomError
    let run = pool.runs.get(agentId);
    const ageMin = run ? (Date.now() - run.startedAt) / 60_000 : Infinity;
    if (!run || ageMin >= this.cfg.ROTATION_INTERVAL_MIN) {
      // Check-and-set is synchronous (no await between) so exactly one caller
      // becomes the creator; everyone else awaits the shared promise.
      let creation = this.creating.get(createKey);
      if (!creation) {
        creation = this._rotateRun(pool, agentId).finally(() => {
          if (this.creating.get(createKey) === creation) this.creating.delete(createKey);
        });
        this.creating.set(createKey, creation);
      }
      try {
        run = await creation;
        pool.lastError = null;
      } catch (e) {
        pool.lastError = `${new Date().toISOString()} ${e.message}`;
        throw e;
      }
    }
    run.inflight++;
    run.requests++;
    return { pool, run, instanceId, agentId };
  }

  async _rotateRun(pool, agentId) {
    // Re-read after the creation lock: a sibling may have rotated already.
    const cur = pool.runs.get(agentId);
    if (cur && (Date.now() - cur.startedAt) / 60_000 < this.cfg.ROTATION_INTERVAL_MIN) return cur;
    if (cur) await this.upstream.finishRun(pool.token, cur.id, cur.requests).catch(() => {}); // FINISH exactly once per superseded id
    const runId = await this.upstream.startRun(pool.token, agentId);
    const run = { id: runId, startedAt: Date.now(), inflight: 0, requests: 0 };
    pool.runs.set(agentId, run);
    this.log.info(`run started (${short(pool.token)}) agent=${agentId} id=${runId.slice(0, 8)}…`);
    return run;
  }

  release(lease) {
    if (!lease) return;
    lease.run.inflight = Math.max(0, lease.run.inflight - 1);
  }

  invalidateRun(lease) {
    if (!lease) return;
    // Leases carry agentId since the TOCTOU fix; remove by id in case the map moved on.
    if (lease.agentId && lease.pool.runs.get(lease.agentId)?.id === lease.run.id) {
      lease.pool.runs.delete(lease.agentId);
      return;
    }
    for (const [agentId, run] of lease.pool.runs) {
      if (run.id === lease.run.id) lease.pool.runs.delete(agentId);
    }
  }

  markAuthCooldown(lease, ms = 30 * 60_000) {
    if (!lease) return;
    lease.pool.cooldownUntil = Date.now() + ms;
    // Drop cached runs: they can never be FINISHed with a rejected token.
    lease.pool.runs.clear();
    lease.pool.lastError = `${new Date().toISOString()} auth rejected — cooling down`;
    this.sessions.invalidate(lease.pool.token, 'auth rejected');
    this.log.warn(`token ${short(lease.pool.token)} cooled down ${Math.round(ms / 60_000)}m after auth failure`);
  }

  invalidateRunById(token, runId, _reason) {
    const pool = this.pools.find((p) => p.token === token);
    if (!pool) return;
    for (const [agentId, run] of pool.runs) {
      if (run.id === runId) {
        pool.runs.delete(agentId);
        this.log.info(`run invalidated (${short(token)}) agent=${agentId}`);
      }
    }
  }

  markAuthCooldownByToken(token, ms = 30 * 60_000) {
    const pool = this.pools.find((p) => p.token === token);
    if (pool) this.markAuthCooldown({ pool }, ms);
  }

  snapshot() {
    return this.pools.map((p) => ({
      token: short(p.token),
      cooldown_s: p.cooldownUntil > Date.now() ? Math.round((p.cooldownUntil - Date.now()) / 1000) : null,
      last_error: p.lastError,
      runs: [...p.runs.entries()].map(([agentId, r]) => ({
        agent: agentId,
        run_id: r.id.slice(0, 8),
        age_min: Math.round((Date.now() - r.startedAt) / 60_000),
        inflight: r.inflight,
        requests: r.requests,
      })),
    }));
  }

  async shutdown() {
    this.shuttingDown = true;
    // Quiesce: let in-flight requests drain (bounded) before FINISHing runs,
    // so we never kill a run whose request is still executing.
    const deadline = Date.now() + 10_000;
    for (;;) {
      const inflight = this.pools.reduce(
        (n, p) => n + [...p.runs.values()].reduce((m, r) => m + r.inflight, 0),
        0,
      );
      if (inflight === 0 || Date.now() >= deadline) break;
      await new Promise((r) => setTimeout(r, 100));
    }
    for (const pool of [...this.pools]) {
      for (const [, run] of [...pool.runs]) {
        await this.upstream.finishRun(pool.token, run.id, run.requests).catch(() => {});
      }
      pool.runs.clear();
      await this.sessions.end(pool.token).catch(() => {});
    }
  }
}

function short(t) {
  return t ? `${t.slice(0, 6)}…` : '(none)';
}
