// Regression tests for the runs/upstream/session fix set (pure logic, no network).
import assert from 'node:assert';
import { Upstream } from '../src/upstream.js';
import { RunManager } from '../src/runs.js';
import { SessionManager } from '../src/session.js';

const log = { info() {}, warn() {}, error() {}, debug() {} };
let passed = 0;
const failures = [];
async function t(name, fn) {
  try {
    await fn();
    console.log('PASS', name);
    passed++;
  } catch (e) {
    console.log('FAIL', name, '::', e.message);
    failures.push(name);
  }
}

// ---- 1. throttle: monotonic slots, no thundering herd -----------------------
await t('throttle spaces 3 concurrent calls by ~debounce', async () => {
  const u = new Upstream(log, 60);
  const stamps = [];
  await Promise.all(
    [1, 2, 3].map(async () => {
      await u.throttle();
      stamps.push(Date.now());
    }),
  );
  stamps.sort((a, b) => a - b);
  const gaps = [stamps[1] - stamps[0], stamps[2] - stamps[1]];
  assert.ok(gaps.every((g) => g > 35 && g < 140), `gaps should be ~60ms, got ${gaps}`);
});

await t('throttle is instant when idle', async () => {
  const u = new Upstream(log, 50);
  const s = Date.now();
  await u.throttle();
  assert.ok(Date.now() - s < 30, 'first call should not wait');
});

// ---- 2. client_id stable per token+run --------------------------------------
await t('client_id stable within run, changes across runs', () => {
  const u = new Upstream(log, 10);
  const a = u._clientIdFor('tok', 'run1');
  assert.equal(u._clientIdFor('tok', 'run1'), a, 'same run -> same id');
  assert.notEqual(u._clientIdFor('tok', 'run2'), a, 'new run -> new id');
  u.finishRun('tok', 'run1'); // cleanup path also drops identity
  assert.notEqual(u._clientIdFor('tok', 'run1'), a, 'after FINISH -> fresh id');
});

// ---- 3. classify buckets ------------------------------------------------------
await t('classify: binding stays run; blocked 403s never auth', () => {
  const u = new Upstream(log, 10);
  assert.equal(u.classify(409, JSON.stringify({ error: 'session_model_mismatch' })), 'run');
  assert.equal(u.classify(409, JSON.stringify({ error: 'model_locked' })), 'run');
  assert.equal(u.classify(403, JSON.stringify({ error: 'country_blocked' })), 'blocked');
  assert.equal(u.classify(403, JSON.stringify({ status: 'ip_capped' })), 'blocked');
  assert.equal(u.classify(403, '{}'), 'auth', 'bare 403 without verdict stays auth');
  assert.equal(u.classify(401, ''), 'auth');
  assert.equal(u.classify(400, JSON.stringify({ error: 'free_mode_cli_required' })), 'conformance');
  assert.equal(u.classify(409, JSON.stringify({ error: 'session_expired' })), 'session');
  assert.equal(u.classify(429, JSON.stringify({ error: 'rate_limited' })), 'rate');
  // Transient router 404s arrive nested in the OpenAI error shape (or as plain
  // text) — both must classify as no_endpoints, never the terminal bucket.
  assert.equal(
    u.classify(404, JSON.stringify({ error: { message: 'No endpoints found for z-ai/glm-5.3-flash.', code: 404, type: null, param: null } })),
    'no_endpoints',
  );
  assert.equal(u.classify(404, 'No endpoints found for z-ai/glm-5.3-flash.'), 'no_endpoints');
  // A 404 without the no-endpoints message stays 'other'.
  assert.equal(u.classify(404, JSON.stringify({ error: { message: 'Nothing here' } })), 'other');
});

// ---- 3b. parseUpstreamError: flat + nested + plain-text bodies ---------------
await t('parseUpstreamError reads flat, nested and plain-text error bodies', async () => {
  const { parseUpstreamError } = await import('../src/upstream.js');
  assert.deepEqual(
    parseUpstreamError(JSON.stringify({ error: 'session_superseded', message: 'taken over' })),
    { code: 'session_superseded', message: 'taken over' },
  );
  assert.deepEqual(
    parseUpstreamError(JSON.stringify({ error: { message: 'No endpoints found for x.', code: 404 } })),
    { code: '', message: 'No endpoints found for x.' },
  );
  assert.deepEqual(parseUpstreamError('plain text boom'), { code: '', message: 'plain text boom' });
  assert.deepEqual(parseUpstreamError(''), { code: '', message: '' });
});

// ---- 4. TOCTOU: two concurrent acquires -> ONE startRun ----------------------
await t('concurrent acquires serialize rotation (one startRun)', async () => {
  let startCalls = 0;
  const finishCalls = [];
  const rm = new RunManager(log, { DEBOUNCE_MS: 1, ROTATION_INTERVAL_MIN: 0 });
  rm.upstream = {
    startRun: async () => {
      startCalls++;
      await new Promise((r) => setTimeout(r, 25));
      return `run-${startCalls}`;
    },
    finishRun: async (t, id) => finishCalls.push(id),
  };
  rm.sessions = {
    ensure: async () => {
      await new Promise((r) => setTimeout(r, 5));
      return 'inst-1';
    },
    invalidate() {},
    end: async () => {},
  };
  rm.setTokens(['TOKEN-AAAAAA']);
  const [l1, l2] = await Promise.all([rm.acquire('agent-x'), rm.acquire('agent-x')]);
  assert.equal(startCalls, 1, `expected exactly 1 startRun, got ${startCalls}`);
  assert.equal(l1.run.id, l2.run.id, 'both leases share one run object');
  assert.deepEqual(finishCalls, [], 'no premature FINISH');
});

await t('rotation finishes old run once and starts new', async () => {
  const rm = new RunManager(log, { DEBOUNCE_MS: 1, ROTATION_INTERVAL_MIN: 0 });
  let n = 0;
  const finished = [];
  rm.upstream = { startRun: async () => `r${++n}`, finishRun: async (t, id) => finished.push(id) };
  rm.sessions = { ensure: async () => 'i', invalidate() {}, end: async () => {} };
  rm.setTokens(['TOKEN-BBBBBB']);
  rm.runs.set('ag', { id: 'old', startedAt: Date.now() - 999 * 60000, inflight: 0, requests: 2 });
  const lease = await rm.acquire('ag');
  assert.equal(lease.run.id, 'r1');
  assert.deepEqual(finished, ['old'], 'old run FINISHed exactly once');
  assert.equal(rm.runs.get('ag').id, 'r1');
});

// ---- 5. shutdown quiesce gate -------------------------------------------------
await t('shutdown drains in-flight before FINISH, rejects new acquires', async () => {
  const rm = new RunManager(log, { DEBOUNCE_MS: 1, ROTATION_INTERVAL_MIN: 60 });
  const fin = [];
  rm.upstream = { startRun: async () => 'rr', finishRun: async (t, id) => fin.push(id) };
  rm.sessions = { ensure: async () => 'i', invalidate() {}, end: async () => {} };
  rm.setTokens(['TOKEN-CCCCCC']);
  const lease = await rm.acquire('ag'); // run 'rr' now holds inflight=1
  const shut = rm.shutdown();
  await new Promise((r) => setTimeout(r, 50));
  assert.deepEqual(fin, [], 'no FINISH while a request is in flight');
  rm.release(lease); // simulate request completing
  await shut;
  assert.deepEqual(fin, ['rr'], 'FINISH only after in-flight drained');
  await assert.rejects(() => rm.acquire('ag'), /shutting down/);
});

// ---- 6. expiry parsing accepts epoch numbers + numeric strings ----------------
await t('expiry accepted as epoch-ms number and ISO string', async () => {
  const sm = new SessionManager(log);
  const future = Date.now() + 3600000;
  sm.sessions.set('TOK', { status: 'ended' });
  sm.createOrRefresh = async () => ({ status: 'active', instanceId: 'inst-9', expiresAt: future });
  assert.equal(await sm.ensure('TOK', 'mimo/mimo-v2.5'), 'inst-9');
  assert.equal(sm.sessions.get('TOK').expiresAt, future);

  sm.sessions.set('TOK2', { status: 'ended' });
  sm.createOrRefresh = async () => ({ status: 'active', instanceId: 'inst-10', expiresAt: new Date(future).toISOString() });
  await sm.ensure('TOK2', null);
  assert.equal(sm.sessions.get('TOK2').expiresAt, future);
});

// ---- 7. waiting room polls with real sleep until active -----------------------
await t('queued state polls after delay until active', async () => {
  const sm = new SessionManager(log);
  let polls = 0;
  // Real protocol: admission responds queued WITH an instanceId; subsequent
  // GETs poll that instance until active.
  sm.createOrRefresh = async () => ({
    status: 'queued',
    instanceId: 'inst-q',
    position: 3,
    queueDepth: 3,
    estimatedWaitMs: 2100,
  });
  sm.poll = async (token, instanceId) => {
    assert.equal(instanceId, 'inst-q');
    polls++;
    return polls >= 2
      ? { status: 'active', instanceId: 'inst-q', expiresAt: Date.now() + 600000 }
      : { status: 'queued', instanceId: 'inst-q', position: 2, queueDepth: 3, estimatedWaitMs: 2100 };
  };
  const t0 = Date.now();
  const id = await sm.ensure('TOKQ', null);
  assert.equal(id, 'inst-q');
  assert.ok(polls >= 2, `should have polled at least twice, polled ${polls}`);
  assert.ok(Date.now() - t0 >= 2000, 'should have actually waited between hops');
});

console.log(`\n${passed} passed, ${failures.length} failed${failures.length ? ': ' + failures.join(', ') : ''}`);
process.exit(failures.length ? 1 : 0);
