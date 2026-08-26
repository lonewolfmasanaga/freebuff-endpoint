// Regression tests for the D fix set: mergeChunks variants, sseEvents CRLF/tail,
// error clamping. Pure logic, no network.
import assert from 'node:assert';
import { mergeChunks, consumeUpstreamSse } from '../src/openai.js';
import { sseEvents } from '../src/openai.js';
import { SessionRateLimitedError } from '../src/errors.js';

let passed = 0;
const failures = [];
async function t(name, fn) {
  try { await fn(); console.log('PASS', name); passed++; }
  catch (e) { console.log('FAIL', name, '::', e.message); failures.push(name); }
}

const chunk = (delta, extra = {}) => ({ choices: [{ index: 0, delta, finish_reason: null, ...extra }] });

// ---- mergeChunks: index-less fragments -> ONE call ---------------------------
await t('index-less argument fragments append to one call', () => {
  const merged = mergeChunks([
    chunk({ tool_calls: [{ function: { name: 'calc', arguments: '' } }] }),
    chunk({ tool_calls: [{ function: { arguments: '{"a":' } }] }),
    chunk({ tool_calls: [{ function: { arguments: '1}' } }] }),
  ]);
  const tcs = merged.choices[0].message.tool_calls;
  assert.equal(tcs.length, 1, `expected 1 call, got ${tcs.length}: ${JSON.stringify(tcs)}`);
  assert.equal(tcs[0].function.name, 'calc');
  assert.equal(tcs[0].function.arguments, '{"a":1}');
});

// ---- mergeChunks: full-name resend not duplicated -----------------------------
await t('full-name repeats are not duplicated', () => {
  const merged = mergeChunks([
    chunk({ tool_calls: [{ index: 0, function: { name: 'calculator', arguments: '' } }] }),
    chunk({ tool_calls: [{ index: 0, function: { name: 'calculator', arguments: '{' } }] }),
    chunk({ tool_calls: [{ index: 0, function: { name: 'calculator', arguments: '}' } }] }),
  ]);
  const tcs = merged.choices[0].message.tool_calls;
  assert.equal(tcs.length, 1);
  assert.equal(tcs[0].function.name, 'calculator');
  assert.equal(tcs[0].function.arguments, '{}');
});

// ---- mergeChunks: explicit indices create separate calls ----------------------
await t('indexed fragments stay in their own calls', () => {
  const merged = mergeChunks([
    chunk({ tool_calls: [{ index: 0, function: { name: 'a_fn', arguments: '{"x"' } }] }),
    chunk({ tool_calls: [{ index: 1, function: { name: 'b_fn', arguments: '{"y"' } }] }),
    chunk({ tool_calls: [{ index: 0, function: { arguments: ':1}' } }] }),
    chunk({ tool_calls: [{ index: 1, function: { arguments: ':2}' } }] }),
  ]);
  const tcs = merged.choices[0].message.tool_calls;
  assert.equal(tcs.length, 2);
  assert.equal(tcs[0].function.name, 'a_fn');
  assert.equal(tcs[0].function.arguments, '{"x":1}');
  assert.equal(tcs[1].function.name, 'b_fn');
  assert.equal(tcs[1].function.arguments, '{"y":2}');
});

// ---- mergeChunks: distinct ids start new calls even without index -------------
await t('id-tagged fragments split into separate calls without index', () => {
  const merged = mergeChunks([
    chunk({ tool_calls: [{ id: 'call_AAA', function: { name: 'one', arguments: '{}' } }] }),
    chunk({ tool_calls: [{ id: 'call_BBB', function: { name: 'two', arguments: '{}' } }] }),
  ]);
  const tcs = merged.choices[0].message.tool_calls;
  assert.equal(tcs.length, 2, `got ${JSON.stringify(tcs)}`);
  assert.equal(tcs[0].function.name, 'one');
  assert.equal(tcs[1].function.name, 'two');
});

// ---- mergeChunks: content + reasoning accumulate ------------------------------
await t('text and reasoning accumulate across chunks', () => {
  const merged = mergeChunks([
    chunk({ role: 'assistant', reasoning_content: 'think ' }),
    chunk({ reasoning_content: 'hard', content: 'ans' }),
    chunk({ content: 'wer!' }, { finish_reason: 'stop' }),
  ]);
  const m = merged.choices[0].message;
  assert.equal(m.content, 'answer!');
  assert.equal(m.reasoning_content, 'think hard');
  assert.equal(merged.choices[0].finish_reason, 'stop');
});

// ---- sseEvents: CRLF + unterminated tail --------------------------------------
async function drain(gen) {
  const out = [];
  for await (const ev of gen) out.push(ev);
  return out;
}
await t('sseEvents handles CRLF and flushes tail event', async () => {
  const body = new TextEncoder().encode(
    'data: {"a":1}\r\n\r\ndata: {"a":2}\r\n\r\ndata: {"a":3}\r\n\r\n',
  );
  const stream = new ReadableStream({
    start(c) {
      // Split awkwardly mid-boundary to simulate real fragmentation.
      c.enqueue(body.slice(0, 15));
      c.enqueue(body.slice(15));
      c.close();
    },
  });
  const evs = await drain(sseEvents(stream));
  assert.equal(evs.length, 3, `expected 3 events, got ${evs.length}`);
  assert.equal(JSON.parse(evs[2].data).a, 3);

  // Tail without trailing blank line must still flush.
  const tail = new TextEncoder().encode('data: {"b":9}');
  const stream2 = new ReadableStream({ start(c) { c.enqueue(tail); c.close(); } });
  const evs2 = await drain(sseEvents(stream2));
  assert.equal(evs2.length, 1);
  assert.equal(JSON.parse(evs2[0].data).b, 9);
});

// ---- errors: NaN clamp ---------------------------------------------------------
await t('SessionRateLimitedError clamps unparseable resetAt', () => {
  const e = new SessionRateLimitedError({ poolLabel: 'GLM', limit: 0, resetAt: 'not-a-date' });
  assert.ok(Number.isFinite(e.retryAfterMs), 'retryAfterMs must be finite');
  assert.equal(e.retryAfterMs, 3600_000);
  const ok = new SessionRateLimitedError({ poolLabel: 'GLM', limit: 0, resetAt: new Date(Date.now() + 60_000).toISOString() });
  assert.ok(ok.retryAfterMs > 0 && ok.retryAfterMs <= 60_000);
});

console.log(`\n${passed} passed, ${failures.length} failed${failures.length ? ': ' + failures.join(', ') : ''}`);
process.exit(failures.length ? 1 : 0);
