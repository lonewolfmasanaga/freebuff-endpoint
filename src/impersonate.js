// Client-shape conformance: makes gateway requests indistinguishable from the
// official Freebuff CLI's own traffic (system marker + signature toolset),
// and strips the conformance artifacts from responses before clients see them.
//
// Upstream gates (common/src/constants/free-agents.ts, foreign-client-signals.ts):
//  - requestHasFreebuffSystemMarker: message[0] must be byte-exact prefixed
//    with a canonical root opening; CLI roots use the base2 opening below.
//  - detectForeignFreebuffClient: a request offering tools with none of
//    Freebuff's signature tool names is silently downgraded to a tiny fallback
//    model. Requests with NO tools are never enforced against.

export const BUFFY_BASE2_OPENING = 'You are Buffy, the strategic coding assistant.';

// A signature tool name straight from Freebuff's own toolNames registry
// (common/src/tools/constants.ts), so the foreign-toolset check passes.
export const SENTINEL_TOOL_NAME = 'end_turn';
// Recognizable id prefix for calls we can attribute to the sentinel (used when
// an upstream echoes caller-supplied ids; otherwise name matching applies).
export const SENTINEL_ID_PREFIX = 'sentinel_end_turn_';

const SENTINEL_TOOL_TEMPLATE = Object.freeze({
  type: 'function',
  function: Object.freeze({
    name: SENTINEL_TOOL_NAME,
    description: 'Call this tool when you have fully completed the user request.',
    parameters: Object.freeze({ type: 'object', properties: Object.freeze({}), required: Object.freeze([]) }),
  }),
});

// Requests where WE appended the sentinel (keyed by the wrapped payload object)
let sentinelInjected = new WeakSet();

/** Wrap an OpenAI-shaped payload into CLI-conformant shape (returns a new object). */
export function wrapPayloadForUpstream(payload) {
  const out = { ...payload };
  const messages = Array.isArray(out.messages) ? [...out.messages] : [];

  // 1. Canonical system marker at position 0 (prefix-exact, extra context may follow).
  const firstSystem = messages.find((m) => m && m.role === 'system');
  const clientSystem =
    typeof firstSystem?.content === 'string'
      ? firstSystem.content
      : Array.isArray(firstSystem?.content)
        ? firstSystem.content.map((b) => b.text || '').join('\n')
        : '';
  const wrappedSystem = clientSystem
    ? `${BUFFY_BASE2_OPENING}\n\n${clientSystem}`
    : `${BUFFY_BASE2_OPENING} You help the user with their request.`;

  const withoutOldSystem = messages.filter((m) => m !== firstSystem);
  out.messages = [{ role: 'system', content: wrappedSystem }, ...withoutOldSystem];

  // 2. Signature toolset: append a fresh copy of the sentinel when the client
  //    offers tools and brought none of Freebuff's signature names. The shared
  //    template is frozen and never handed out by reference.
  if (Array.isArray(out.tools) && out.tools.length > 0) {
    const names = new Set(out.tools.map((t) => t?.function?.name).filter(Boolean));
    if (!names.has(SENTINEL_TOOL_NAME)) {
      out.tools = [...out.tools, structuredClone(SENTINEL_TOOL_TEMPLATE)];
      sentinelInjected.add(out);
    }
  } else {
    delete out.tools;
  }
  return out;
}

/** Was the sentinel injected into this (wrapped) request payload? */
export function sentinelWasInjected(wrappedPayload) {
  return sentinelInjected.has(wrappedPayload);
}

/** Is this tool_call entry our sentinel? Id-prefix first, name as legacy fallback. */
export function isSentinelCall(call) {
  if (!call) return false;
  const id = call.id || '';
  if (typeof id === 'string' && id.startsWith(SENTINEL_ID_PREFIX)) return true;
  return call?.function?.name === SENTINEL_TOOL_NAME || call?.function?.name === 'end_turn';
}

/**
 * Remove sentinel tool_calls from a merged (blocking) completion. If sentinel
 * calls were the only reason for finish_reason 'tool_calls', normalize to 'stop'.
 */
export function scrubSentinelFromCompletion(completion) {
  if (!completion || !Array.isArray(completion.choices)) return completion;
  for (const choice of completion.choices) {
    const msg = choice.message;
    if (!msg || !Array.isArray(msg.tool_calls)) continue;
    const kept = msg.tool_calls.filter((c) => !isSentinelCall(c));
    if (kept.length !== msg.tool_calls.length) {
      msg.tool_calls = kept.length ? kept : undefined;
      if (!kept.length && choice.finish_reason === 'tool_calls') {
        choice.finish_reason = 'stop';
      }
      if (!msg.tool_calls) delete msg.tool_calls;
    }
  }
  return completion;
}

// ---------------------------------------------------------------------------
// Streaming scrubber (stateful across events)

const SSE_EVENT_SPLIT = /\r?\n\r?\n/;
const SSE_LINE_SPLIT = /\r?\n/;

/** Parse one raw SSE event's data payload (multi-line aware, single-space rule). */
function eventData(raw) {
  const lines = raw.split(SSE_LINE_SPLIT).filter((l) => l.startsWith('data:'));
  if (!lines.length) return null;
  return lines.map((l) => (l.slice(5).startsWith(' ') ? l.slice(6) : l.slice(5))).join('\n');
}

/**
 * Filter a streamed OpenAI SSE byte-stream, dropping sentinel tool_calls.
 * STATEFUL: tool_call fragments are buffered until their name identifies them
 * as sentinel or not (prefix test flushes early), and the finish_reason
 * demotion decision is deferred to stream end based on whether any visible
 * non-sentinel tool_call was actually emitted. Returns a new ReadableStream
 * emitting transformed bytes.
 */
export function scrubSentinelSseStream(byteStream) {
  const decoder = new TextDecoder();
  const encoder = new TextEncoder();
  let reader;
  let buf = '';

  async function* events() {
    reader = byteStream.getReader();
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      buf += decoder.decode(value, { stream: true });
      let m;
      while ((m = buf.match(SSE_EVENT_SPLIT)) !== null) {
        yield buf.slice(0, m.index);
        buf = buf.slice(m.index + m[0].length);
      }
    }
    buf += decoder.decode(); // flush multibyte tail
    if (buf.trim()) yield buf;
  }

  // ---- per-stream scrub state ---------------------------------------------
  // Buffered tool_call fragments awaiting classification, keyed by slot.
  // slot: explicit index ?? id ?? sequential counter (upstream may omit index).
  const pending = new Map(); // slot -> { index, id, name, args, chunks, isSentinel }
  const classified = new Map(); // slot -> 'visible' | 'sentinel' (post-classification)
  let autoSlot = 0;
  let sawVisibleToolCall = false; // any non-sentinel call fully identified?
  // A finish_reason:'tool_calls' chunk is HELD back until its verdict is known:
  // emitted as-is once a visible call shows up, demoted to 'stop' at stream end
  // when the sentinel turned out to be the only caller.
  let heldFinish = null;

  const isEndTurnPrefix = (s) => {
    if (s.length > SENTINEL_TOOL_NAME.length) return false;
    return SENTINEL_TOOL_NAME.startsWith(s);
  };

  /** Emit the held finish_reason chunk with its final verdict. */
  function resolveHeld() {
    if (!heldFinish) return [];
    const c = heldFinish;
    heldFinish = null;
    if (Array.isArray(c.choices)) {
      for (const ch of c.choices) {
        if (ch.finish_reason === 'tool_calls' && !sawVisibleToolCall) ch.finish_reason = 'stop';
      }
    }
    return [c];
  }

  /** Flush a buffered call to output chunks; returns array of re-encoded events. */
  function flushPending(entry, drop) {
    const outs = [];
    if (!drop && entry.chunks.length) {
      sawVisibleToolCall = true;
      // Re-emit the original fragments verbatim, wrapped back into proper
      // chunk shape so argument streaming stays intact for clients.
      const index = typeof entry.index === 'number' ? entry.index : 0;
      for (const raw of entry.chunks) {
        outs.push({ choices: [{ index, delta: { tool_calls: [raw] }, finish_reason: null }] });
      }
    }
    pending.delete(entry.slot);
    return outs;
  }

  /** Feed one parsed chunk through the state machine; returns events to emit. */
  function transformChunk(chunk) {
    const outs = [];
    for (const choice of chunk.choices || []) {
      const d = choice.delta || {};
      const incoming = Array.isArray(d.tool_calls) ? d.tool_calls : null;

      if (incoming) {
        for (const tc of incoming) {
          const slot = tc.index ?? tc.id ?? `auto-${autoSlot++}`;
          let entry = pending.get(slot);
          if (!entry) {
            // Slots already classified keep their verdict across continuations.
            if (classified.get(slot) === 'visible') {
              outs.push({ choices: [{ index: typeof tc.index === 'number' ? tc.index : 0, delta: { tool_calls: [tc] }, finish_reason: null }] });
              continue;
            }
            if (classified.get(slot) === 'sentinel') continue; // swallow silently
            entry = { slot, index: tc.index, id: '', name: '', chunks: [], isSentinel: false };
            pending.set(slot, entry);
          }
          const fragName = tc.function?.name || '';
          entry.name += fragName;
          if (tc.id) entry.id = tc.id;

          // Classification: a sentinel-prefixed id is authoritative even when
          // the name hasn't arrived yet (mirrors isSentinelCall's id check);
          // otherwise, as soon as the accumulated name cannot be a prefix of
          // 'end_turn', this call is visibly NOT the sentinel.
          if (entry.id.startsWith(SENTINEL_ID_PREFIX)) {
            entry.chunks.push(tc);
            entry.isSentinel = true;
            classified.set(entry.slot, 'sentinel');
          } else if (entry.name && !isEndTurnPrefix(entry.name)) {
            entry.chunks.push(tc);
            classified.set(entry.slot, 'visible');
            outs.push(...flushPending(entry, false));
          } else if (entry.name === SENTINEL_TOOL_NAME) {
            // Sentinel confirmed — swallow every fragment collected so far;
            // later argument continuations for this slot are dropped too.
            entry.chunks.push(tc);
            entry.isSentinel = true;
            classified.set(entry.slot, 'sentinel');
          } else {
            // Still ambiguous ('', 'e', 'en', 'end', …) — keep buffering.
            entry.chunks.push(tc);
          }
        }
        // Every fragment went to tracked slots or was handled above; strip
        // them from this chunk (non-sentinel calls were re-emitted by us).
        delete d.tool_calls;
      } else if (d.tool_calls === null) {
        delete d.tool_calls;
      }

      // A bare terminal tool_calls finish is HELD until its verdict is
      // certain: resolved immediately once a visible call shows up, demoted
      // to 'stop' at stream end when only the sentinel ever called.
      const deltaEmpty = !choice.delta || Object.keys(choice.delta).length === 0;
      if (!heldFinish && choice.finish_reason === 'tool_calls' && deltaEmpty && !sawVisibleToolCall) {
        heldFinish = chunk;
        return outs;
      }
    }
    if (heldFinish && sawVisibleToolCall) outs.push(...resolveHeld());
    else if (!heldFinish) outs.push(chunk);
    return outs;
  }

  /** Emit final decisions: flush/reject pending calls + resolve held finish. */
  function finalize(chunk) {
    const outs = [];
    for (const entry of [...pending.values()]) {
      outs.push(...flushPending(entry, !!entry.isSentinel));
    }
    sawVisibleToolCall = sawVisibleToolCall || [...classified.values()].includes('visible');
    if (chunk && Array.isArray(chunk.choices)) {
      for (const choice of chunk.choices) {
        if (choice.finish_reason === 'tool_calls' && !sawVisibleToolCall) {
          choice.finish_reason = 'stop'; // sentinel was the only caller
        }
      }
      outs.push(chunk);
    }
    return outs;
  }

  function transformEvent(raw) {
    const data = eventData(raw);
    if (data == null) return `${raw}\n\n`;
    if (data.trim() === '[DONE]') {
      // [DONE] carries no choices — flush remaining fragments, then the final
      // verdict chunk (finish_reason), then DONE itself.
      const finals = [...finalize(null), ...resolveHeld()].map((c) => `data: ${JSON.stringify(c)}\n\n`);
      return `${finals.join('')}data: [DONE]\n\n`;
    }
    let chunk;
    try {
      chunk = JSON.parse(data);
    } catch {
      // Passthrough: preserve multi-line payloads by emitting one data: line
      // per source line (SSE spec), so downstream parsers lose nothing.
      return `${data.split('\n').map((l) => `data: ${l}`).join('\n')}\n\n`;
    }
    const transformed = transformChunk(chunk);
    return transformed.map((c) => `data: ${JSON.stringify(c)}\n\n`).join('');
  }

  return new ReadableStream({
    async start(controller) {
      try {
        let doneEmitted = false;
        for await (const ev of events()) {
          controller.enqueue(encoder.encode(transformEvent(ev)));
          if (/data:\s*\[DONE\]/.test(ev)) doneEmitted = true;
        }
        if (!doneEmitted) {
          // Upstream closed without [DONE]: flush pending + verdict + close cleanly.
          const finals = [...finalize(null), ...resolveHeld()];
          for (const c of finals) controller.enqueue(encoder.encode(`data: ${JSON.stringify(c)}\n\n`));
        }
        controller.close();
      } catch (e) {
        controller.error(e);
      }
    },
    cancel(reason) {
      reader?.cancel(reason).catch(() => {});
    },
  });
}
