/**
 * Shared option interfaces, channel-id validation and TLS resolution shared
 * by the HTTP client (client.ts) and the WebSocket layer (client_ws.ts).
 */

// ---------------------------------------------------------------------------
// Channel id validation
// ---------------------------------------------------------------------------

const CHANNEL_ID_RE = /^[A-Za-z0-9._:-]+$/;

/** Enforces the shared channel-id grammar: 1..=256 bytes of
 * [A-Za-z0-9._:-], no slashes or whitespace. Fails fast on the client so a
 * channel that can never be addressed over HTTP/WS is never created.
 * Human-readable names belong in metadata, not the id. */
export function validateChannelId(id: string): void {
  if (id === '') {
    throw new TypeError('channel id is required');
  }
  if (id.length > 256) {
    throw new TypeError('channel id exceeds maximum length of 256');
  }
  if (!CHANNEL_ID_RE.test(id)) {
    throw new TypeError(
      `invalid channel id "${id}" — use [A-Za-z0-9._:-] only (no slashes or whitespace)`,
    );
  }
}

// ---------------------------------------------------------------------------
// TLS
// ---------------------------------------------------------------------------

/** PEM material or file paths for mTLS / custom-CA TLS configuration. */
export interface TLSOptions {
  /** PEM CA certificate(s). */
  ca?: string | Buffer;
  /** PEM client certificate (mTLS). */
  cert?: string | Buffer;
  /** PEM client private key (mTLS). */
  key?: string | Buffer;
  /** When true, reject connections to servers with self-signed certs not in
   * `ca` (default false for dev self-signed compatibility). */
  rejectUnauthorized?: boolean;
}

export interface ResolvedTLS {
  ca?: Buffer;
  cert?: Buffer;
  key?: Buffer;
  rejectUnauthorized?: boolean;
}

export function resolveTLS(tls?: TLSOptions): ResolvedTLS | undefined {
  if (!tls) return undefined;
  const out: ResolvedTLS = {};
  if (tls.rejectUnauthorized !== undefined) {
    out.rejectUnauthorized = tls.rejectUnauthorized;
  }
  if (tls.ca !== undefined) {
    out.ca = Buffer.isBuffer(tls.ca) ? tls.ca : Buffer.from(tls.ca);
  }
  if (tls.cert !== undefined) {
    out.cert = Buffer.isBuffer(tls.cert) ? tls.cert : Buffer.from(tls.cert);
  }
  if (tls.key !== undefined) {
    out.key = Buffer.isBuffer(tls.key) ? tls.key : Buffer.from(tls.key);
  }
  return out;
}

// ---------------------------------------------------------------------------
// Client options
// ---------------------------------------------------------------------------

/** Configures an ActaeClient. `endpoint` (HTTP base URL) or `wsEndpoint`
 * must be provided. */
export interface ClientOptions {
  /** API key for authentication. Required. */
  apiKey: string;
  /** HTTP API base URL (e.g. "http://localhost:8002"). */
  endpoint?: string;
  /** WebSocket URL. Auto-derived from `endpoint` when omitted
   * (https:// → wss://, http:// → ws://, `/ws` appended). */
  wsEndpoint?: string;
  /** TLS configuration (custom CAs, mTLS client certs). */
  tls?: TLSOptions;
  /** Timeout for HTTP requests and WebSocket operations, in ms
   * (default 30000). */
  timeout?: number;
  /** WebSocket auto-reconnect on drop. undefined means true (reconnect
   * enabled); pass a value to override. It is optional so that setting
   * `timeout` can never accidentally change the default. */
  autoReconnect?: boolean;
  /** Number of consecutive failed reconnect attempts before giving up
   * (default 10). */
  maxReconnectFailures?: number;
  /** JSON ping cadence in ms (server drops idle connections > 300s). Zero
   * uses the 30s default; negative disables client pings. */
  keepAliveInterval?: number;
  /** Deliver your own Publish'd events to locally registered OnMessage
   * callbacks. The server does not echo a broadcast back to the publishing
   * connection, so without this a single client that subscribes AND
   * publishes never sees its own events. Default false (wire parity with the
   * Python SDK). */
  echoSelf?: boolean;
}

// ---------------------------------------------------------------------------
// HTTP request option structs (Python/Go parity)
// ---------------------------------------------------------------------------

export interface RecordOptions {
  actor?: string;
  agentId?: string;
  userId?: string;
  metadata?: JsonObjectLike;
  /** Makes the record idempotent: retrying with the same channel_id +
   * operation_id returns the original persisted event instead of inserting a
   * duplicate. Generate a fresh UUID per logical record attempt. */
  operationId?: string;
  /** Records a durable step → cursor mapping in the server-owned step
   * index, making step→cursor resolution and fork_at_step exact
   * server-side. */
  stepNumber?: number;
  dependencies?: Array<{ eventId: string; dependencyType?: string }>;
}

export interface ReplayOptions {
  cursor?: number;
  limit?: number;
  eventType?: string;
}

export interface QueryOptions {
  channelIds?: string[];
  eventType?: string;
  actor?: string;
  cursorStart?: number;
  cursorEnd?: number;
  from?: string;
  to?: string;
  limit?: number;
  offset?: number;
}

export interface TransitionOptions {
  actor?: string;
  agentId?: string;
  userId?: string;
  metadata?: JsonObjectLike;
  operationId?: string;
  expectedVersion?: number;
  expectedCursor?: number;
  dependencies?: Array<{ eventId: string; dependencyType?: string }>;
}

export interface SaveStateOptions {
  expectedVersion?: number;
  expectedCursor?: number;
}

export interface ListStatesOptions {
  limit?: number;
  offset?: number;
}

export interface ForkOptions {
  displayName?: string;
  reason?: string;
  experimentMetadata?: JsonObjectLike;
  operationId?: string;
  manifest?: JsonObjectLike;
  /**
   * Side-effect policy for the child channel
   * (`{"tool": "replay"|"block"|"live"|"auto", "*": default}`). Omitted means
   * the server default `auto`: an exact inherited tool result is replayed;
   * new or changed work runs live.
   */
  toolPolicies?: JsonObjectLike;
  expectedVersion?: number;
  expectedCursor?: number;
}

export interface ExperimentOptions {
  description?: string;
  baselineChannelId?: string;
}

export interface ExperimentMemberOptions {
  role?: string;
  declaredDelta?: JsonObjectLike;
}

export interface UpdateMetadataOptions {
  displayName?: string;
  reason?: string;
  experimentMetadata?: JsonObjectLike;
}

export interface SignupOptions {
  email: string;
  password: string;
  name?: string;
}

export interface ListGroupsOptions {
  channelId?: string;
}

export interface ListWakeupsOptions {
  channelId?: string;
  status?: string;
}

export interface ClaimExecutionOptions {
  dedupFields?: string[];
  leaseSeconds?: number;
  emitReplayEvent?: boolean;
  actor?: string;
}

export interface FailExecutionOptions {
  claimToken?: string;
  errorType?: string;
  stack?: string;
}

/** Structural alias so these options can be built without importing the full
 * JsonObject type (keeps options.ts dependency-light). */
export type JsonObjectLike = Record<string, unknown>;
