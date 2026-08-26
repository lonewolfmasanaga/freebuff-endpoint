// OpenAI-compatible surface: /v1/models + /v1/chat/completions (stream & blocking).
// Also hosts the shared completion core used by the Anthropic surface.
import { readJson } from './http-client.js';
import { scrubSentinelFromCompletion, scrubSentinelSseStream } from './impersonate.js';
import { SessionRateLimitedError } from './errors.js';
import { config } from './config.js';

/** Map provider-specific completion fields onto standard OpenAI ones. */
function normalizeCompletion(c) {
  for (const choice of c?.choices || []) {
    const msg = choice.message;
    if (!msg) continue;
    if (msg.reasoning && !msg.reasoning_content) msg.reasoning_content = msg.reasoning;
    delete msg.reasoning;
    delete msg.refusal;
  }
  return c;
}

/** Emit a well-formed OpenAI chat.completion.chunk SSE stream from a full completion. */
export function synthesizeSseFromCompletion(completion) {
  const encoder = new TextEncoder();
  const base = {
    id: completion.id || 'chatcmpl-synth',
    object: 'chat.completion.chunk',
    created: completion.created || Math.floor(Date.now() / 1000),
    model: completion.model || 'unknown',
  };
  const events = [];
  events.push({ choices: [{ index: 0, delta: { role: 'assistant', content: '' }, finish_reason: null }] });

  const choices = completion.choices || [];
  const main = choices[0] || {};
  const msg = main.message || {};
  if (msg.reasoning_content) {
    events.push({ choices: [{ index: 0, delta: { reasoning_content: msg.reasoning_content }, finish_reason: null }] });
  }
  if (typeof msg.content === 'string' && msg.content.length) {
    // Split into word-ish chunks so streaming clients render progressively.
    const parts = msg.content.match(/\S+\s*/g) || [msg.content];
    for (const part of parts) {
      events.push({ choices: [{ index: 0, delta: { content: part }, finish_reason: null }] });
    }
  }
  if (Array.isArray(msg.tool_calls)) {
    for (const [i, tc] of msg.tool_calls.entries()) {
      events.push({
        choices: [
          { index: 0, delta: { tool_calls: [{ index: i, id: tc.id, type: 'function', function: { name: tc.function?.name, arguments: tc.function?.arguments } }] }, finish_reason: null },
        ],
      });
    }
  }
  events.push({ choices: [{ index: 0, delta: {}, finish_reason: main.finish_reason || 'stop' }], usage: completion.usage });

  let out = '';
  for (const ev of events) {
    out += `data: ${JSON.stringify({ ...base, ...ev })}\n\n`;
  }
  out += 'data: [DONE]\n\n';
  return new ReadableStream({
    start(controller) {
      controller.enqueue(encoder.encode(out));
      controller.close();
    },
  });
}

/**
 * Execute a chat completion end-to-end with retries across tokens/runs/sessions.
 * Returns { kind:'sse', stream } | { kind:'json', status, body, retryAfterMs? }.
 *
 * `fallback` (optional): { preferredModel } — when the requested model's pool
 * is exhausted (SessionRateLimitedError), re-route to preferredModel if it
 * differs. The served model is reported in the response body (`freebuff_served_by`)
 * and the `x-freebuff-served-by` header so clients stay informed.
 */
export async function runCompletion({ registry, runs, log, model, payload, wantStream, signal, fallback }) {
  const agentId = registry.agentForModel(model);
  if (!agentId) {
    return {
      kind: 'json',
      status: 400,
      body: openaiError(`model "${model}" not available`, 'model_not_found'),
    };
  }

  // Fallback eligibility is config-driven (POOL_FALLBACK_ELIGIBLE) so it can't
  // drift from deployments that add or retire unlimited-pool models.
  const FALLBACK_ELIGIBLE = new Set(Array.isArray(config.POOL_FALLBACK_ELIGIBLE) ? config.POOL_FALLBACK_ELIGIBLE : []);
  let servedByNote = null;

  let lastError = null;
  for (let attempt = 0; attempt < 3; attempt++) {
    if (signal?.aborted) throw new Error('client aborted');
    let lease = null;
    try {
      lease = await runs.acquire(agentId, model);
      const usedToken = lease.pool.token;
      const usedRunId = lease.run.id;
      const res = await runs.upstream.chatCompletions({
        authToken: lease.pool.token,
        payload,
        runId: lease.run.id,
        instanceId: lease.instanceId,
      });
      runs.release(lease); // run stays alive for reuse; lease was only for bookkeeping
      lease = null;

      const isSse = /text\/event-stream/i.test(res.headers['content-type'] || '');
      if (res.status >= 200 && res.status < 300) {
        if (isSse && wantStream) {
          watchAbort(signal, () => res.stream.cancel().catch(() => {}));
          return { kind: 'sse', stream: scrubSentinelSseStream(res.stream) };
        }
        let completion;
        if (isSse) {
          completion = await consumeUpstreamSse(res.stream);
        } else {
          completion = normalizeCompletion(await readJson(res.stream));
        }
        completion = scrubSentinelFromCompletion(
          completion || {
            id: 'chatcmpl-empty',
            object: 'chat.completion',
            created: Math.floor(Date.now() / 1000),
            model,
            choices: [{ index: 0, message: { role: 'assistant', content: '' }, finish_reason: 'stop' }],
          },
        );
        if (wantStream) {
          return { kind: 'sse', stream: synthesizeSseFromCompletion(completion) };
        }
        return { kind: 'json', status: 200, body: completion };
      }

      // Upstream rejected — classify and decide.
      const errText = await readAll(res.stream);
      const cls = runs.upstream.classify(res.status, errText);
      log.warn(`upstream ${res.status} (${cls}) model=${model}: ${errText.slice(0, 200)}`);
      lastError = new Error(`${res.status}: ${errText.slice(0, 300)}`);

      if (cls === 'session') {
        runs.sessions.invalidate(usedToken, errText.slice(0, 120));
        continue;
      }
      if (cls === 'conformance') {
        // Request shape rejected — retrying identically won't help; surface it.
        return {
          kind: 'json',
          status: 403,
          body: openaiError(`upstream rejected request shape (${errText.slice(0, 200)})`, 'free_mode_conformance'),
        };
      }
      if (cls === 'run') {
        let code = '';
        try {
          code = JSON.parse(errText)?.error || '';
        } catch { /* not json */ }
        if (code === 'model_locked' || code === 'session_model_mismatch') {
          // Session bound to another model: end it upstream and re-admit fresh
          // for the requested model (the CLI's own "switch models" path).
          await runs.sessions.end(usedToken).catch(() => {});
          runs.invalidateRunById(usedToken, usedRunId);
          log.info(`released model-locked session (${code}), re-admitting for ${model}`);
        } else {
          runs.invalidateRunById(usedToken, usedRunId);
        }
        continue;
      }
      if (cls === 'auth') {
        runs.markAuthCooldownByToken(usedToken);
        continue; // try another token
      }
      if (cls === 'blocked') {
        // Structured account/region/quota verdict (country_blocked, banned,
        // ip_capped…): terminal for this model, no retry, NO auth cooldown.
        let code = '';
        try {
          code = JSON.parse(errText)?.error || '';
        } catch { /* not json */ }
        return {
          kind: 'json',
          status: 403,
          body: openaiError(
            `model "${model}" unavailable for this account/region${code ? ` (${code})` : ''}`,
            'region_or_account_blocked',
          ),
        };
      }
      if (cls === 'rate') {
        return {
          kind: 'json',
          status: 429,
          body: openaiError('rate limited by upstream; retry shortly', 'rate_limit_exceeded'),
          retryAfterMs: 15_000,
        };
      }
      return {
        kind: 'json',
        status: res.status >= 400 ? res.status : 502,
        body: openaiError(errText.slice(0, 500) || 'upstream error', 'upstream_error'),
      };
    } catch (e) {
      if (e?.name === 'AbortError' || signal?.aborted) throw e; // client gone: never burn a retry
      if (e.name === 'WaitingRoomError') {
        return {
          kind: 'json',
          status: 503,
          body: openaiError(e.message, 'waiting_room_queued'),
          retryAfterMs: e.retryAfterMs,
        };
      }
      if (e instanceof SessionRateLimitedError) {
        // Terminal for this model+account. If a pool-aware fallback is wired
        // and not yet used, re-route to an unlimited model instead of failing.
        const alt = fallback?.preferredModel;
        if (
          fallback &&
          !fallback.used &&
          alt &&
          alt !== model &&
          FALLBACK_ELIGIBLE.has(alt) &&
          registry.agentForModel(alt)
        ) {
          fallback.used = true;
          const note = { requested: model, served_by: alt, reason: 'pool_exhausted', quota: e.info };
          log.warn(`pool exhausted for ${model}; falling back to ${alt}`);
          const r = await runCompletion({
            registry,
            runs,
            log,
            model: alt,
            payload: { ...payload, model: alt },
            wantStream,
            signal,
            fallback,
          });
          if (r.kind === 'json' && r.body && !r.body.error) {
            r.body.freebuff_served_by = note;
            r.headers = { ...(r.headers || {}), 'x-freebuff-served-by': alt };
          } else if (r.kind === 'sse') {
            // Streams can't carry a JSON note; expose the reroute on the result
            // so the HTTP layer can set response headers before pumping.
            r.headers = { 'x-freebuff-served-by': alt };
            r.servedBy = note;
          }
          return r;
        }
        return {
          kind: 'json',
          status: 429,
          body: openaiError(`${model}: ${e.message}`, 'insufficient_quota'),
          retryAfterMs: e.retryAfterMs,
          quota: e.info,
        };
      }
      lastError = e;
      log.error(`completion attempt failed: ${e.message}`);
    } finally {
      if (lease) {
        runs.release(lease); // never leak a lease on throw/early-exit paths
        lease = null;
      }
    }
  }

  return {
    kind: 'json',
    status: 502,
    body: openaiError(`all attempts failed${lastError ? `: ${lastError.message}` : ''}`, 'upstream_error'),
  };
}

/** Consume the upstream OpenAI SSE stream, merge all deltas into one chat.completion JSON. */
export async function consumeUpstreamSse(stream) {
  const events = [];
  for await (const ev of sseEvents(stream)) {
    if (ev.data === '[DONE]') break;
    try {
      events.push(JSON.parse(ev.data));
    } catch { /* skip malformed */ }
  }
  return mergeChunks(events);
}

export async function* sseEvents(stream) {
  const reader = stream.getReader();
  let buf = '';
  const dec = new TextDecoder();
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      buf += dec.decode(value, { stream: true }).replace(/\r\n/g, '\n'); // normalize CRLF
      let idx;
      while ((idx = buf.indexOf('\n\n')) !== -1) {
        const raw = buf.slice(0, idx);
        buf = buf.slice(idx + 2);
        const ev = parseSseEvent(raw);
        if (ev) yield ev;
      }
    }
    // Flush any final event not terminated by a blank line.
    buf += dec.decode();
    if (buf.trim()) {
      const ev = parseSseEvent(buf.replace(/\r\n/g, '\n'));
      if (ev) yield ev;
    }
  } finally {
    reader.cancel().catch(() => {});
  }
}

/** Parse one raw SSE event block (LF-normalized). Returns null when dataless. */
function parseSseEvent(raw) {
  const ev = { event: 'message', data: '' };
  for (const line of raw.split('\n')) {
    if (line.startsWith('data:')) ev.data += line.slice(5).replace(/^ /, '') + '\n';
    else if (line.startsWith('event:')) ev.event = line.slice(6).trim();
  }
  ev.data = ev.data.replace(/\n$/, '');
  return ev.data ? ev : null;
}

/** Merge streamed chat.completion.chunk list into a single chat.completion. */
export function mergeChunks(chunks) {
  let id = 'chatcmpl-merged';
  let model = '';
  let created = Math.floor(Date.now() / 1000);
  const choiceMap = new Map();
  let usage;

  for (const c of chunks) {
    if (!c || typeof c !== 'object') continue;
    if (c.id) id = c.id;
    if (c.model) model = c.model;
    if (c.created) created = c.created;
    if (c.usage) usage = c.usage;
    for (const ch of c.choices || []) {
      const i = ch.index ?? 0;
      const slot = choiceMap.get(i) || { role: 'assistant', content: '', tool_calls: [], finish_reason: null };
      const d = ch.delta || {};
      if (d.role) slot.role = d.role;
      if (typeof d.content === 'string') slot.content += d.content;
      if (d.reasoning_content) slot.reasoning_content = (slot.reasoning_content || '') + d.reasoning_content;
      for (const tc of d.tool_calls || []) {
        mergeToolCall(slot, tc);
      }
      if (ch.finish_reason) slot.finish_reason = ch.finish_reason;
      choiceMap.set(i, slot);
    }
  }

  const choices = [...choiceMap.entries()]
    .sort((a, b) => a[0] - b[0])
    .map(([i, slot]) => ({
      index: i,
      message: Object.fromEntries(
        Object.entries({
          role: slot.role || 'assistant',
          content: slot.content,
          reasoning_content: slot.reasoning_content,
          tool_calls: slot.tool_calls.length ? slot.tool_calls : undefined,
        }).filter(([, v]) => v !== undefined),
      ),
      finish_reason: slot.finish_reason || 'stop',
    }));

  const out = { id, object: 'chat.completion', created, model: model || 'unknown', choices };
  if (usage) out.usage = usage;
  return out;
}

/**
 * Merge one streamed tool_call fragment into the accumulator. Upstreams vary:
 * some omit `index` entirely, some resend the full name on every argument
 * delta. Explicit index wins; otherwise fragments attach to the most recent
 * call unless their id clearly identifies another one. Name fragments are only
 * appended when they actually extend the accumulated name.
 */
function mergeToolCall(slot, tc) {
  const calls = slot.tool_calls;
  let t = null;

  if (tc.index != null && Number.isInteger(tc.index)) {
    // Explicit indexing is authoritative — grow the array to fit.
    while (calls.length <= tc.index) {
      calls.push({ id: '', type: 'function', function: { name: '', arguments: '' } });
    }
    t = calls[tc.index];
    if (!t.function) t.function = { name: '', arguments: '' };
  } else {
    // No index: match by id against existing calls, else continue the most
    // recent call, else start a new one. (Two simultaneous id-less calls are
    // indistinguishable on the wire; attaching to the latest is the only
    // sane reading.)
    const byId = tc.id ? calls.find((c) => c.id === tc.id) : null;
    const last = calls[calls.length - 1];
    if (byId) t = byId;
    else if (last && (!tc.id || !last.id)) t = last;
    else {
      t = { id: '', type: 'function', function: { name: '', arguments: '' } };
      calls.push(t);
    }
  }

  if (tc.id) t.id = tc.id;
  const fragName = tc.function?.name || '';
  if (fragName && !t.function.name.endsWith(fragName)) {
    // Skip repeats of the already-accumulated name ('calculator' + 'calculator').
    t.function.name += fragName;
  }
  if (tc.function?.arguments) t.function.arguments += tc.function.arguments;
}

function watchAbort(signal, onCancel) {
  if (!signal) return;
  if (signal.aborted) return onCancel();
  signal.addEventListener('abort', onCancel, { once: true });
}

async function readAll(stream, limit = 2_000_000) {
  const reader = stream.getReader();
  const parts = [];
  let total = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      parts.push(Buffer.from(value));
      total += value.length;
      if (total > limit) break;
    }
  } finally {
    // Abandoning the body must still release the socket.
    try { await reader.cancel(); } catch { /* already closed/errored */ }
    try { reader.releaseLock(); } catch { /* lock already released */ }
  }
  return Buffer.concat(parts).toString('utf8');
}

export function openaiError(message, code) {
  return { error: { message, type: 'freebuff_endpoint_error', code: code || null } };
}
