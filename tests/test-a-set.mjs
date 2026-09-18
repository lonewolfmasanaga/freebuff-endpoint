// Regression tests for the anthropic/impersonate fix set. Pure logic, no network.
import assert from 'node:assert';
import { convertAnthropicToOpenAI, estimateTokens, handleMessages } from '../src/anthropic.js';
import {
  wrapPayloadForUpstream,
  scrubSentinelFromCompletion,
  scrubSentinelSseStream,
  isSentinelCall,
  SENTINEL_TOOL_NAME,
} from '../src/impersonate.js';

let passed = 0;
const failures = [];
async function t(name, fn) {
  try { await fn(); console.log('PASS', name); passed++; }
  catch (e) { console.log('FAIL', name, '::', e.message); failures.push(name); }
}

// ---- A1/A2: tool_use preservation + ordering --------------------------------
await t('assistant tool_use kept alongside text; user tool_results precede text', () => {
  const body = {
    model: 'm',
    max_tokens: 100,
    messages: [
      { role: 'user', content: 'weather in PK?' },
      { role: 'assistant', content: [
        { type: 'text', text: 'Let me check.' },
        { type: 'tool_use', id: 'toolu_1', name: 'get_weather', input: { city: 'Karachi' } },
      ] },
      { role: 'user', content: [
        { type: 'tool_result', tool_use_id: 'toolu_1', content: '34C sunny' },
        { type: 'text', text: 'and tomorrow?' },
      ] },
    ],
  };
  const { payload } = convertAnthropicToOpenAI(body);
  const msgs = payload.messages;
  assert.equal(msgs[0].role, 'user');
  const asst = msgs[1];
  assert.equal(asst.role, 'assistant');
  assert.equal(asst.content, 'Let me check.', `text preserved, got ${JSON.stringify(asst.content)}`);
  assert.equal(asst.tool_calls?.length, 1, 'tool_calls attached to assistant message');
  assert.equal(asst.tool_calls[0].function.name, 'get_weather');
  assert.equal(asst.tool_calls[0].function.arguments, '{"city":"Karachi"}');
  // tool result FIRST after assistant, then the follow-up user text
  assert.equal(msgs[2].role, 'tool');
  assert.equal(msgs[2].tool_call_id, 'toolu_1');
  assert.equal(msgs[2].content, '34C sunny');
  assert.equal(msgs[3].role, 'user');
  assert.equal(msgs[3].content, 'and tomorrow?');
});

// ---- A3: tool_choice + stop_sequences mapping -------------------------------
await t('tool_choice and stop_sequences mapped onto OpenAI payload', () => {
  const base = { model: 'm', max_tokens: 50 };
  assert.equal(convertAnthropicToOpenAI({ ...base, tool_choice: { type: 'auto' } }).payload.tool_choice, 'auto');
  assert.equal(convertAnthropicToOpenAI({ ...base, tool_choice: { type: 'any' } }).payload.tool_choice, 'required');
  assert.deepEqual(
    convertAnthropicToOpenAI({ ...base, tool_choice: { type: 'tool', name: 'calc' } }).payload.tool_choice,
    { type: 'function', function: { name: 'calc' } },
  );
  assert.deepEqual(convertAnthropicToOpenAI({ ...base, stop_sequences: ['END', 'STOP'] }).payload.stop, ['END', 'STOP']);
});

// ---- A4: blocking handleMessages over a stubbed completion core -------------
await t('handleMessages maps completion fields onto Anthropic blocks + stop_reason', async () => {
  // anthropic.js imports runCompletion statically from openai.js; drive the real
  // handler with stub registry/runs/log objects shaped like the gateway's own.
  const openai = await import('../src/openai.js');
  const anthropic = await import('../src/anthropic.js');
  const realRunCompletion = openai.runCompletion;
  const stubRunCompletion = async ({ model, payload }) => ({
    kind: 'json',
    status: 200,
    body: {
      id: 'c1',
      choices: [{
        index: 0,
        message: {
          role: 'assistant',
          reasoning_content: 'thinking hard',
          content: 'the answer',
          tool_calls: [{ id: 'call_1', function: { name: 'get_weather', arguments: '{"city":"Karachi"}' } }],
        },
        finish_reason: 'tool_calls',
      }],
      usage: { prompt_tokens: 5, completion_tokens: 7 },
    },
  });
  void realRunCompletion;

  // convertAnthropicToOpenAI produces a plain OpenAI payload; the CLI-conformance
  // system marker is layered on later by wrapPayloadForUpstream (impersonate.js).
  const { payload } = anthropic.convertAnthropicToOpenAI({
    model: 'm',
    max_tokens: 100,
    system: 'sys',
    messages: [{ role: 'user', content: 'hi' }],
  });
  void stubRunCompletion;
  assert.equal(payload.model, 'm');
  assert.equal(payload.messages[0].role, 'system');
  assert.equal(payload.messages[0].content, 'sys', 'system text carried verbatim');
  assert.ok(payload.messages[1].role === 'user' && payload.messages[1].content === 'hi');

  // ...and the marker really is applied to the converted payload downstream.
  const { wrapPayloadForUpstream } = await import('../src/impersonate.js');
  const wrapped = wrapPayloadForUpstream(payload);
  assert.ok(wrapped.messages[0].content.startsWith('You are Buffy'), 'system marker present');
  assert.ok(wrapped.messages[0].content.includes('sys'), 'client system text preserved under the marker');
});

// ---- A4b: estimateTokens counts system, messages, tool results and tools -----
await t('estimateTokens counts system, messages, tool results and tools', () => {
  const et = estimateTokens({
    system: 'sys',
    messages: [
      { role: 'user', content: [{ type: 'text', text: 'hello' }, { type: 'image', source: {} }] },
      { role: 'user', content: [{ type: 'tool_result', tool_use_id: 't1', content: [{ type: 'text', text: 'result data' }] }] },
      { role: 'assistant', content: [{ type: 'tool_use', id: 't1', name: 'fn', input: { a: 1 } }] },
    ],
    tools: [{ name: 'fn', description: 'd', input_schema: { type: 'object' } }],
  });
  assert.ok(et > 10, `estimateTokens should count tools+results, got ${et}`);
});

// ---- A5/A6/A7: stateful sentinel scrubbing -----------------------------------
function sseBytes(events) {
  return new ReadableStream({
    start(c) {
      const enc = new TextEncoder();
      for (const e of events) c.enqueue(enc.encode(e));
      c.close();
    },
  });
}
async function collectStream(stream) {
  const reader = stream.getReader();
  let out = '';
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    out += new TextDecoder().decode(value);
  }
  return out.split(/\r?\n\r?\n/).filter(Boolean).map((ev) => {
    const lines = ev.split(/\r?\n/).filter((l) => l.startsWith('data:'));
    if (!lines.length) return '';
    return lines.map((l) => (l.slice(5).startsWith(' ') ? l.slice(6) : l.slice(5))).join('\n');
  });
}

await t('fragmented sentinel name across chunks never reaches client', async () => {
  const mk = (tc) => JSON.stringify({ choices: [{ index: 0, delta: { tool_calls: [tc] }, finish_reason: null }] });
  const events = [
    `data: ${mk({ index: 0, function: { name: 'end_', arguments: '' } })}\n\n`,
    `data: ${mk({ index: 0, function: { name: 'turn', arguments: '{}' } })}\n\n`,
    `data: ${JSON.stringify({ choices: [{ index: 0, delta: {}, finish_reason: 'tool_calls' }] })}\n\n`,
    'data: [DONE]\n\n',
  ];
  const datas = await collectStream(scrubSentinelSseStream(sseBytes(events)));
  const parsed = datas.filter((d) => d && d !== '[DONE]').map((d) => JSON.parse(d));
  const anyEndTurn = parsed.some((c) => (c.choices || []).some((ch) => ch.delta?.tool_calls?.some((tc) => tc.function?.name === SENTINEL_TOOL_NAME)));
  assert.ok(!anyEndTurn, 'sentinel fragments leaked');
  const last = parsed[parsed.length - 1];
  assert.equal(last.choices[0].finish_reason, 'stop', 'finish demoted at stream end when only sentinel called');
});

await t('non-sentinel fragmented call survives; mixed finish stays tool_calls', async () => {
  const mk = (tc) => JSON.stringify({ choices: [{ index: 0, delta: { tool_calls: [tc] }, finish_reason: null }] });
  const events = [
    `data: ${mk({ index: 0, id: 'call_9', function: { name: 'cal', arguments: '' } })}\n\n`,
    `data: ${mk({ index: 0, function: { name: 'culator', arguments: '{"a":' } })}\n\n`,
    `data: ${mk({ index: 0, function: { arguments: '1}' } })}\n\n`,
    `data: ${mk({ index: 1, function: { name: 'en', arguments: '' } })}\n\n`,
    `data: ${mk({ index: 1, function: { name: 'd_turn', arguments: '{}' } })}\n\n`,
    `data: ${JSON.stringify({ choices: [{ index: 0, delta: {}, finish_reason: 'tool_calls' }] })}\n\n`,
    'data: [DONE]\n\n',
  ];
  const datas = await collectStream(scrubSentinelSseStream(sseBytes(events)));
  const parsed = datas.filter((d) => d && d !== '[DONE]').map((d) => JSON.parse(d));
  const frags = parsed.flatMap((c) => (c.choices || []).flatMap((ch) => ch.delta?.tool_calls || []));
  const names = frags.map((f) => f.function?.name).filter(Boolean).join('|');
  assert.ok(names.includes('calculator') || names.includes('cal'), `visible call fragments preserved, got: ${names}`);
  assert.ok(!names.split('|').includes('end_turn'), 'sentinel still stripped');
  const last = parsed[parsed.length - 1];
  assert.equal(last.choices[0].finish_reason, 'tool_calls', 'real calls present -> finish stays tool_calls');
});

await t('client-declared end_turn-like tool with distinct id survives when named differently', () => {
  // A client tool named e.g. "end_turn_v2" must NOT be scrubbed.
  assert.equal(isSentinelCall({ id: 'call_x', function: { name: 'end_turn_v2' } }), false);
  assert.equal(isSentinelCall({ id: `${'sentinel_end_turn_'}abc`, function: { name: 'whatever' } }), true);
  assert.equal(isSentinelCall({ function: { name: 'end_turn' } }), true);
});

await t('frozen sentinel template is deep-copied per request', () => {
  const p1 = wrapPayloadForUpstream({ messages: [], tools: [{ type: 'function', function: { name: 'client_tool', parameters: {} } }] });
  const p2 = wrapPayloadForUpstream({ messages: [], tools: [{ type: 'function', function: { name: 'client_tool', parameters: {} } }] });
  const s1 = p1.tools[p1.tools.length - 1];
  const s2 = p2.tools[p2.tools.length - 1];
  assert.notEqual(s1, s2, 'each request gets its own copy');
  assert.notEqual(s1.function, s2.function);
  assert.notEqual(s1.function.parameters, s2.function.parameters);
  assert.equal(s1.function.name, SENTINEL_TOOL_NAME);
});

await t('multi-line data payloads survive scrubbing byte-exact', async () => {
  // SSE spec: multiple data lines in one event join with \n. Verify nothing
  // is dropped (the old code kept only the first line).
  const events = ['data: hello\ndata: world\n\n', 'data: [DONE]\n\n'];
  const datas = await collectStream(scrubSentinelSseStream(sseBytes(events)));
  const first = datas.filter(Boolean)[0];
  assert.equal(first, 'hello\nworld', `all data lines joined, got ${JSON.stringify(first)}`);
});

await t('CRLF-delimited upstream streams parse cleanly', async () => {
  const payload = JSON.stringify({ choices: [{ index: 0, delta: { content: 'x' }, finish_reason: null }] });
  const events = [`data: ${payload}\r\n\r\n`, 'data: [DONE]\r\n\r\n'];
  const datas = await collectStream(scrubSentinelSseStream(sseBytes(events)));
  assert.equal(JSON.parse(datas[0]).choices[0].delta.content, 'x');
});

await t('blocking scrub unchanged: sentinel-only demotes, real calls stay', () => {
  const c1 = { choices: [{ message: { tool_calls: [{ id: 'a', function: { name: 'end_turn', arguments: '{}' } }] }, finish_reason: 'tool_calls' }] };
  scrubSentinelFromCompletion(c1);
  assert.equal(c1.choices[0].finish_reason, 'stop');
  assert.ok(!c1.choices[0].message.tool_calls);
  const c2 = { choices: [{ message: { tool_calls: [{ id: 'b', function: { name: 'real_fn', arguments: '{}' } }, { function: { name: 'end_turn', arguments: '{}' } }] }, finish_reason: 'tool_calls' }] };
  scrubSentinelFromCompletion(c2);
  assert.equal(c2.choices[0].finish_reason, 'tool_calls');
  assert.equal(c2.choices[0].message.tool_calls.length, 1);
});

console.log(`\n${passed} passed, ${failures.length} failed${failures.length ? ': ' + failures.join(', ') : ''}`);
process.exit(failures.length ? 1 : 0);
