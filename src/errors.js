// Shared typed errors for the session/admission layer, plus the human-facing
// error vocabulary both protocol surfaces (OpenAI + Anthropic) render from.
// Keeping the hint table here means a given machine code reads the same to a
// user no matter which client they came through.

export class WaitingRoomError extends Error {
  constructor(position, queueDepth, retryAfterMs) {
    super(`waiting room queued (position ${position}/${queueDepth})`);
    this.name = 'WaitingRoomError';
    this.position = position;
    this.queueDepth = queueDepth;
    this.retryAfterMs = retryAfterMs;
  }
}

/** Account/region verdicts (country_blocked, banned, …) — terminal, not retryable. */
export class RegionBlockedError extends Error {
  constructor(status) {
    super(`account or region blocked (${status})`);
    this.name = 'RegionBlockedError';
    this.status = status;
  }
}

/** Admission refused for quota/pool reasons — terminal for the request, not retryable. */
export class SessionRateLimitedError extends Error {
  constructor(info) {
    const label = info.poolLabel || info.pool || info.model || 'session';
    super(
      `upstream ${label} pool exhausted for this account (limit ${info.limit ?? 0}); resets ${info.resetAt || 'later'}`,
    );
    this.name = 'SessionRateLimitedError';
    this.info = info;
    const raw =
      Number(info.retryAfterMs) > 0
        ? Number(info.retryAfterMs)
        : info.resetAt
          ? Math.max(0, Date.parse(info.resetAt) - Date.now())
          : 60_000;
    // Unparseable resetAt yields NaN — never leak `retry-after: NaN` headers.
    this.retryAfterMs = Number.isFinite(raw) ? raw : 3600_000;
  }
}

/**
 * Actionable, plain-English guidance keyed by the machine error code.
 * Add a row whenever a new code is surfaced so users know both WHAT happened
 * and WHAT to do next instead of guessing from a bare status.
 */
const HINTS = {
  invalid_api_key:
    'Send a valid key via Authorization: Bearer <key>, or clear API_KEYS to open the gateway.',
  model_not_found:
    'Run GET /v1/models for this gateway\'s list and pick a valid model id.',
  waiting_room_queued:
    'Freebuff\'s free tier is busy — retry in a few seconds, or switch to an unlimited model (mimo/mimo-v2.5, deepseek/deepseek-v4-flash) to skip the queue.',
  insufficient_quota:
    'Your pool for this model is exhausted for now — switch to an unlimited model (mimo/mimo-v2.5, deepseek/deepseek-v4-flash), or come back after the pool resets.',
  rate_limit_exceeded:
    'You\'re being rate-limited upstream — wait a moment, then retry, and reduce how many requests you send at once.',
  region_or_account_blocked:
    'This model is unusable from your account or region right now — the gateway can\'t change that.',
  free_mode_conformance:
    'The upstream rejected the request shape — this is a gateway or upstream-side issue, not something your tool sent wrong.',
  upstream_error:
    'The gateway failed while talking to the Freebuff upstream — check your token and model, then retry.',
  unavailable:
    'The gateway is shutting down — retry in a moment.',
  internal:
    'An unexpected gateway error occurred — check the gateway\'s terminal logs.',
  not_found:
    'That endpoint doesn\'t exist — see the README for valid routes.',
  'invalid_request':
    'Check the request you sent — a field or the body was rejected.',
  invalid_request_error:
    'Check the request you sent — a field was malformed or invalid.',
};

/** Normalize code + cause into { code, hint, message }. Unknown codes carry no hint. */
export function describeError(code, cause = '') {
  const key = code || '';
  const hint = Object.hasOwn(HINTS, key) ? HINTS[key] : '';
  return {
    code: key || null,
    hint,
    message: cause || 'an error occurred while handling your request',
  };
}