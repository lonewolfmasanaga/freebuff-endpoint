// Anthropic-compatible surface: /v1/messages (+ count_tokens).
// Converts Anthropic Messages <-> OpenAI chat completions, including SSE event streams.
import { runCompletion, openaiError } from './openai.js';

export function convertAnthropicToOpenAI(anthropicBody) {
  const messages = [];
  let systemText = '';
  if (typeof anthropicBody.system === 'string') {
    systemText = anthropicBody.system;
  } else if (Array.isArray(anthropicBody.system)) {
    systemText = anthropicBody.system.map((b) => (typeof b === 'string' ? b : b.text || '')).join('\n');
  }
  if (systemText) messages.push({ role: 'system', content: systemText });

  for (const m of anthropicBody.messages || []) {
    if (m.role !== 'user' && m.role !== 'assistant') continue;
    if (typeof m.content === 'string') {
      messages.push({ role: m.role, content: m.content });
      continue;
    }
    if (!Array.isArray(m.content)) continue;

    const text = [];
    const toolCalls = []; // assistant tool_use blocks -> OpenAI tool_calls
    const toolResults = []; // user tool_result blocks -> role:'tool' messages

    for (const block of m.content) {
      if (block.type === 'text') text.push(block.text);
      else if (block.type === 'image') text.push('[image omitted]');
      else if (block.type === 'tool_use' && m.role === 'assistant') {
        toolCalls.push({
          id: block.id || `call_${Math.random().toString(36).slice(2, 10)}`,
          type: 'function',
          function: { name: block.name || 'unknown', arguments: safeStringify(block.input ?? {}) },
        });
      } else if (block.type === 'tool_result') {
        toolResults.push({
          role: 'tool',
          tool_call_id: block.tool_use_id,
          content: renderToolResultContent(block),
        });
      }
    }

    if (m.role === 'assistant') {
      // Assistant turn: text + tool_calls travel together on ONE message, or
      // multi-turn tool conversations lose the call/result pairing entirely.
      const msg = { role: 'assistant', content: text.join('\n') || null };
      if (toolCalls.length) msg.tool_calls = toolCalls;
      messages.push(msg);
    } else {
      // User turn: OpenAI requires role:'tool' messages to come IMMEDIATELY
      // after the assistant tool_calls message — emit them first, then any
      // stray user text as a separate follow-up message.
      for (const t of toolResults) messages.push(t);
      if (text.length) messages.push({ role: 'user', content: text.join('\n') });
    }
  }

  const tools = (anthropicBody.tools || []).map((t) => ({
    type: 'function',
    function: {
      name: t.name,
      description: t.description || '',
      parameters: t.input_schema || { type: 'object', properties: {} },
    },
  }));

  const payload = {
    model: anthropicBody.model,
    messages,
  };
  if (tools.length) payload.tools = tools;
  if (anthropicBody.max_tokens) payload.max_tokens = anthropicBody.max_tokens;
  if (anthropicBody.temperature !== undefined) payload.temperature = anthropicBody.temperature;
  if (Array.isArray(anthropicBody.stop_sequences) && anthropicBody.stop_sequences.length) {
    payload.stop = anthropicBody.stop_sequences;
  }
  const tc = anthropicBody.tool_choice;
  if (tc?.type === 'auto') payload.tool_choice = 'auto';
  else if (tc?.type === 'any') payload.tool_choice = 'required';
  else if (tc?.type === 'tool' && tc.name) payload.tool_choice = { type: 'function', function: { name: tc.name } };

  return { payload };
}

function renderToolResultContent(block) {
  const parts = [];
  if (block.is_error) parts.push('[error]');
  if (typeof block.content === 'string') parts.push(block.content);
  else if (Array.isArray(block.content)) {
    for (const c of block.content) {
      if (c?.type === 'image') parts.push('[image]');
      else if (typeof c?.text === 'string') parts.push(c.text);
      else if (c != null) parts.push(typeof c === 'object' ? JSON.stringify(c) : String(c));
    }
  } else if (block.content != null) parts.push(String(block.content));
  return parts.filter((p) => p !== '').join('\n');
}

function safeStringify(v) {
  try {
    return typeof v === 'string' ? v : JSON.stringify(v ?? {});
  } catch {
    return '{}';
  }
}

export async function handleMessages({ registry, runs, log, body, wantStream, signal }) {
  const { payload } = convertAnthropicToOpenAI(body);
  const result = await runCompletion({
    registry,
    runs,
    log,
    model: body.model,
    payload,
    wantStream: false, // always consume fully; re-serialize in Anthropic shape
    signal,
  });

  if (result.kind !== 'json' || result.status !== 200) {
    const msg = result.body?.error?.message || 'upstream error';
    return { status: result.status >= 400 ? result.status : 502, body: anthropicError(msg, result.status) };
  }

  const completion = result.body;
  const choice = completion.choices?.[0] || { message: {}, finish_reason: 'stop' };
  const blocks = [];
  const tc = choice.message.tool_calls || [];

  if (choice.message.reasoning_content) {
    // Empty signatures fail strict client validation — omit unless real.
    const b = { type: 'thinking', thinking: String(choice.message.reasoning_content) };
    const sig = completion.choices?.[0]?.message?.reasoning_signature || choice.message.signature;
    if (sig) b.signature = String(sig);
    blocks.push(b);
  }
  if (choice.message.content) {
    blocks.push({ type: 'text', text: String(choice.message.content) });
  }
  for (const call of tc) {
    let input = {};
    try {
      input = JSON.parse(call.function?.arguments || '{}');
    } catch { /* leave {} */ }
    blocks.push({
      type: 'tool_use',
      id: call.id || `toolu_${Math.random().toString(36).slice(2, 10)}`,
      name: call.function?.name || 'unknown',
      input,
    });
  }
  if (!blocks.length) blocks.push({ type: 'text', text: '' });

  const stopReason =
    choice.finish_reason === 'tool_calls'
      ? 'tool_use'
      : choice.finish_reason === 'length'
        ? 'max_tokens'
        : 'end_turn';

  // Best-effort stop_sequence reporting: detect whether generation ended on one
  // of the requested sequences by inspecting the tail of the produced text.
  let stopSequence = null;
  const seqs = Array.isArray(body.stop_sequences) ? body.stop_sequences : [];
  if (choice.finish_reason !== 'length' && seqs.length) {
    const produced = blocks.find((b) => b.type === 'text')?.text || '';
    for (const s of seqs) {
      if (typeof s === 'string' && s && produced.endsWith(s)) {
        stopSequence = s;
        break;
      }
    }
  }

  const resp = {
    id: completion.id || `msg_${Date.now()}`,
    type: 'message',
    role: 'assistant',
    model: body.model,
    content: blocks,
    stop_reason: stopReason,
    stop_sequence: stopSequence,
    usage: {
      input_tokens: completion.usage?.prompt_tokens ?? 0,
      output_tokens: completion.usage?.completion_tokens ?? 0,
    },
  };

  if (!wantStream) return { status: 200, body: resp };

  // Re-serialize as an Anthropic SSE stream. Block indices are assigned
  // sequentially with a running counter over the SAME block list as the
  // blocking response (thinking included, no phantom empty text block), so
  // clients never see non-contiguous indices.
  const events = [];
  events.push(['message_start', { type: 'message_start', message: { ...resp, content: [] } }]);
  let idx = 0;
  for (const b of blocks) {
    if (b.type === 'text') {
      events.push(['content_block_start', { type: 'content_block_start', index: idx, content_block: { type: 'text', text: '' } }]);
      // Progressive emission: split into a few deltas rather than one giant blob.
      for (const piece of splitForStream(b.text)) {
        if (piece) events.push(['content_block_delta', { type: 'content_block_delta', index: idx, delta: { type: 'text_delta', text: piece } }]);
      }
      events.push(['content_block_stop', { type: 'content_block_stop', index: idx }]);
    } else if (b.type === 'thinking') {
      events.push(['content_block_start', { type: 'content_block_start', index: idx, content_block: { type: 'thinking', thinking: '' } }]);
      for (const piece of splitForStream(b.thinking)) {
        if (piece) events.push(['content_block_delta', { type: 'content_block_delta', index: idx, delta: { type: 'thinking_delta', thinking: piece } }]);
      }
      if (b.signature) {
        events.push(['content_block_delta', { type: 'content_block_delta', index: idx, delta: { type: 'signature_delta', signature: b.signature } }]);
      }
      events.push(['content_block_stop', { type: 'content_block_stop', index: idx }]);
    } else if (b.type === 'tool_use') {
      events.push(['content_block_start', { type: 'content_block_start', index: idx, content_block: { type: 'tool_use', id: b.id, name: b.name } }]);
      events.push(['content_block_delta', { type: 'content_block_delta', index: idx, delta: { type: 'input_json_delta', partial_json: safeStringify(b.input) } }]);
      events.push(['content_block_stop', { type: 'content_block_stop', index: idx }]);
    }
    idx++;
  }
  events.push([
    'message_delta',
    { type: 'message_delta', delta: { stop_reason: stopReason, stop_sequence: stopSequence }, usage: { output_tokens: resp.usage.output_tokens } },
  ]);
  events.push(['message_stop', { type: 'message_stop' }]);

  const encoder = new TextEncoder();
  const stream = new ReadableStream({
    start(controller) {
      for (const [name, data] of events) {
        controller.enqueue(encoder.encode(`event: ${name}\ndata: ${JSON.stringify(data)}\n\n`));
      }
      controller.close();
    },
  });
  return { status: 200, stream };
}

/** Split long text into a few progressive stream pieces (~240 chars each). */
function splitForStream(text) {
  const s = String(text || '');
  if (s.length <= 480) return s ? [s] : [];
  const size = Math.ceil(s.length / 4);
  const parts = [];
  for (let i = 0; i < s.length; i += size) parts.push(s.slice(i, i + size));
  return parts;
}

export function anthropicError(message, statusCode) {
  const type =
    statusCode === 401 ? 'authentication_error' : statusCode === 429 ? 'rate_limit_error' : statusCode >= 500 ? 'api_error' : 'invalid_request_error';
  return { type: 'error', error: { type, message } };
}

export function estimateTokens(body) {
  // Rough heuristic; upstream has no cheap public tokenizer endpoint.
  const texts = [];
  if (typeof body.system === 'string') texts.push(body.system);
  else if (Array.isArray(body.system)) texts.push(body.system.map((b) => b?.text || '').join(''));
  for (const m of body.messages || []) {
    if (typeof m.content === 'string') texts.push(m.content);
    else if (Array.isArray(m.content)) {
      for (const b of m.content) {
        texts.push(b?.text || '');
        if (b?.type === 'tool_result') {
          texts.push(renderToolResultContent(b));
        } else if (b?.type === 'tool_use') {
          texts.push(`${b.name || ''}${safeStringify(b.input ?? {})}`);
        }
      }
    }
  }
  try {
    if (Array.isArray(body.tools) && body.tools.length) texts.push(JSON.stringify(body.tools));
  } catch { /* non-serializable tools — skip */ }
  const chars = texts.join('').length;
  return Math.ceil(chars / 4);
}
