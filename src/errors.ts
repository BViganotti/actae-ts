/**
 * Typed error hierarchy mirroring the Python SDK's error classes
 * (errors.py) and the Go SDK's errors.go.
 *
 * HTTP error mapping:
 *   - 401 → AuthError
 *   - 402 → LockError (control-plane subscription lapse)
 *   - 404 → NotFoundError (except "execution_not_found" → ExecutionNotFoundError)
 *   - 409 → typed conflicts by `status` code
 *   - 429 → RateLimitError
 *   - 5xx → ServerError
 *
 * Transport failures (refused, timeout, premature close) → ConnectionError,
 * and WebSocket handshake/auth failures → ConnectionError / AuthError.
 */

export class ActaeError extends Error {
  constructor(message: string) {
    super(message);
    this.name = new.target.name;
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

/** Authentication failure: missing/invalid API key, or JWT expiry during
 * WebSocket auth. */
export class AuthError extends ActaeError {}

/** Control-plane subscription lock (HTTP 402). The Actae instance has lapsed
 * and refuses service until the subscription is renewed. */
export class LockError extends ActaeError {}

/** The WebSocket connection failed or dropped, or an HTTP transport error
 * occurred (server unreachable, handshake failure, timeout). */
export class ConnectionError extends ActaeError {}

/** HTTP 429 (too many requests). Carries the server's retry-after seconds. */
export class RateLimitError extends ActaeError {
  constructor(
    public readonly retryAfterSeconds: number,
    message = '',
  ) {
    const msg = `Rate limited — retry after ${retryAfterSeconds}s${
      message ? `: ${message}` : ''
    }`;
    super(msg);
  }
}

/** Generic non-success HTTP status. */
export class APIError extends ActaeError {
  constructor(
    public readonly statusCode: number,
    message = '',
  ) {
    const msg = `HTTP ${statusCode}${message ? `: ${message}` : ''}`;
    super(msg);
  }
}

/** HTTP 404. */
export class NotFoundError extends APIError {
  constructor(message = '') {
    super(404, message);
  }
}

/** HTTP 5xx. */
export class ServerError extends APIError {
  constructor(message = '') {
    super(500, message);
  }
}

/** HTTP 409 "snapshot_boundary_required": a fork's at_cursor has no saved
 * state boundary at or before it. */
export class SnapshotBoundaryError extends ActaeError {}

/** HTTP 409 "version_conflict": an optimistic-concurrency guard rejected a
 * write. */
export class VersionConflictError extends ActaeError {}

/** Raised by AgentSession in strict ("exact") boundary mode when a fork
 * cannot restore a checkpoint at the requested step and the session refused
 * to silently fall forward to later (contaminating) state. */
export class NoRestorableCheckpointError extends ActaeError {}

/** HTTP 409 "idempotency_conflict": a fork operation_id was reused with a
 * different request. */
export class IdempotencyConflictError extends ActaeError {}

/** HTTP 409 "channel_conflict": a fork's new_channel_id already exists under
 * a different source or boundary. */
export class ChannelConflictError extends ActaeError {}
/** A frozen member or side-effect policy blocked an unsafe counterfactual. */
export class CounterfactualBlockedError extends ActaeError {}

/**
 * A forked channel's `toolPolicies` refused to execute a tool.
 *
 * The server responds `409 fork_tool_blocked` and persists a `tool.blocked`
 * boundary event. This happens when the effective policy is `block`, or
 * `replay` with no exact source match (a miss/mismatch). The tool was not
 * executed. Resolve the inherited result, change the policy to `auto`/`live`,
 * or continue the fork without that call.
 */
export class ForkToolBlockedError extends ActaeError {
  /** The persisted boundary identifier, when the server supplied one. */
  boundary?: string;
}

/** HTTP 409 "consumer_not_found": consumer-group errors (missing group,
 * expired lease). */
export class ConsumerError extends ActaeError {}

/** HTTP 409 "idempotency_key_mismatch": an execution claim collided with a
 * different request. */
export class IdempotencyKeyMismatchError extends ActaeError {}

/** HTTP 409 "execution_not_owned": an execution operation supplied a stale
 * claim token. */
export class ExecutionNotOwnedError extends ActaeError {}

/** HTTP 404 "execution_not_found". */
export class ExecutionNotFoundError extends APIError {
  constructor(message = '') {
    super(404, message || 'Execution not found');
  }
}

/** AgentSession lifecycle violation. */
export class SessionError extends ActaeError {}

/** Raised when stepping on a completed session, or resuming a completed
 * non-fork channel. */
export class SessionCompletedError extends ActaeError {}

/** Raised by CancelWakeup when the wake-up already fired/failed/cancelled
 * (HTTP 409) — a benign no-op outcome. */
export class WakeupAlreadyFiredError extends ActaeError {}
