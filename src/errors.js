// Shared typed errors for the session/admission layer.

export class WaitingRoomError extends Error {
  constructor(position, queueDepth, retryAfterMs) {
    super(`waiting room queued (position ${position}/${queueDepth})`);
    this.name = 'WaitingRoomError';
    this.position = position;
    this.queueDepth = queueDepth;
    this.retryAfterMs = retryAfterMs;
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
