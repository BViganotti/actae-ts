/**
 * ActaeClient — the full HTTP API client, mirroring the Python SDK's
 * `ActaeClient` and the Go SDK's `Client`.
 *
 * Transport: a small `node:http`/`node:https` wrapper (no HTTP dependency).
 * Response bodies are parsed with the number-aware `parseJson` (out-of-range
 * int64s stay exact as bigint, sonic-rs markers unwrapped). Non-2xx statuses
 * map to the typed error hierarchy in `errors.ts`.
 *
 * Optional fields follow the TS idiom (`undefined` = unset) where the Go SDK
 * uses pointers.
 */

import * as http from 'node:http';
import * as https from 'node:https';

import {
  APIError,
  AuthError,
  ChannelConflictError,
  CounterfactualBlockedError,
  ForkToolBlockedError,
  ConsumerError,
  ConnectionError,
  ExecutionNotFoundError,
  IdempotencyConflictError,
  IdempotencyKeyMismatchError,
  LockError,
  NotFoundError,
  RateLimitError,
  ServerError,
  SnapshotBoundaryError,
  VersionConflictError,
  ExecutionNotOwnedError,
  WakeupAlreadyFiredError,
} from './errors.js';
import {
  asList,
  asMap,
  asString,
  parseJson,
  stringify,
  type JsonObject,
  type JsonValue,
  unwrapSonic,
} from './json.js';
import {
  type ResolvedTLS,
  resolveTLS,
  validateChannelId,
  type ClaimExecutionOptions,
  type ClientOptions,
  type ExperimentMemberOptions,
  type ExperimentOptions,
  type FailExecutionOptions,
  type ForkOptions,
  type ListGroupsOptions,
  type ListStatesOptions,
  type ListWakeupsOptions,
  type QueryOptions,
  type RecordOptions,
  type ReplayOptions,
  type SaveStateOptions,
  type SignupOptions,
  type TransitionOptions,
  type UpdateMetadataOptions,
} from './options.js';
import {
  authResultFromResponse,
  forkInfoFromDict,
  channelMetadataFromDict,
  claimedWorkFromResponse,
  decisionTrailFrom,
  eventFromQuery,
  eventFromRecord,
  eventFromReplay,
  executionClaimFromResponse,
  executionInfoFromResponse,
  forkReceiptFromDict,
  executionGroupFrom,
  executionGroupMemberFrom,
  groupMessageFrom,
  memberLeaseFrom,
  groupInfoFromResponse,
  groupOffsetFromResponse,
  healthStatusFromResponse,
  metricsSnapshotFromResponse,
  readinessResultFromResponse,
  stateDiffFrom,
  stateSnapshotFrom,
  type AuthResult,
  type ForkInfo,
  type ChannelMetadata,
  type ClaimedWork,
  type DecisionTrail,
  type Event,
  type ExecutionClaim,
  type ExecutionInfo,
  type ExecutionGroup,
  type ExecutionGroupMember,
  type GroupMessage,
  type MemberLease,
  type ForkReceipt,
  type GroupInfo,
  type GroupOffset,
  type HealthStatus,
  type MetricsSnapshot,
  type ReadinessResult,
  type StateDiff,
  type StateSnapshot,
  type StateVersionInfo,
  type StepResolution,
  type TransitionResult,
  type UserInfo,
  type Wakeup,
  userInfoFromDict,
  wakeupFromResponse,
} from './types.js';
import { newUUID } from './uuid.js';
import { deterministicOperationKey } from './deterministic.js';
import { GroupSession } from './groups.js';
import {
  WebSocketConnection,
  type DisconnectedHandler,
  type ErrorHandler,
  type MessageHandler,
  type ReconnectHandler,
  type SubscribedHandler,
  type PublishOptions as WSPublishOptions,
  type EventStream,
} from './client_ws.js';

/** Durable human-in-the-loop approval events (see docs/HUMAN_APPROVAL.md). */
export const APPROVAL_REQUESTED = 'approval.requested';
export const APPROVAL_DECIDED = 'approval.decided';
export const APPROVAL_EXPIRED = 'approval.expired';

/** Reads a string field from an opaque event payload (returns '' if absent). */
function payloadField(payload: JsonValue, key: string): string {
  if (typeof payload !== 'object' || payload === null || Array.isArray(payload)) return '';
  const value = (payload as JsonObject)[key];
  return typeof value === 'string' ? value : '';
}

export type {
  ClaimExecutionOptions,
  ClientOptions,
  ExperimentMemberOptions,
  ExperimentOptions,
  FailExecutionOptions,
  ForkOptions,
  ListGroupsOptions,
  ListStatesOptions,
  ListWakeupsOptions,
  QueryOptions,
  RecordOptions,
  ReplayOptions,
  SaveStateOptions,
  SignupOptions,
  TLSOptions,
  TransitionOptions,
  UpdateMetadataOptions,
} from './options.js';
export { validateChannelId } from './options.js';

// ---------------------------------------------------------------------------
// HTTP transport
// ---------------------------------------------------------------------------

interface HttpResponse {
  status: number;
  contentType: string;
  body: string;
}

function performRequest(
  url: URL,
  method: string,
  headers: Record<string, string>,
  body: string | undefined,
  tls: ResolvedTLS | undefined,
  timeoutMs: number,
  signal?: AbortSignal,
): Promise<HttpResponse> {
  return new Promise<HttpResponse>((resolve, reject) => {
    const isHttps = url.protocol === 'https:';
    const reqOpts: https.RequestOptions = {
      method,
      hostname: url.hostname,
      port: url.port ? Number(url.port) : undefined,
      path: url.pathname + url.search,
      headers,
      rejectUnauthorized:
        tls?.rejectUnauthorized !== undefined ? tls.rejectUnauthorized : true,
    };
    if (isHttps && tls) {
      if (tls.ca !== undefined) reqOpts.ca = tls.ca;
      if (tls.cert !== undefined) reqOpts.cert = tls.cert;
      if (tls.key !== undefined) reqOpts.key = tls.key;
    }

    const req = (isHttps ? https : http).request(reqOpts, (res) => {
      const chunks: Buffer[] = [];
      res.on('data', (chunk: Buffer) => chunks.push(chunk));
      res.on('end', () => {
        resolve({
          status: res.statusCode ?? 0,
          contentType: res.headers['content-type'] ?? '',
          body: Buffer.concat(chunks).toString('utf8'),
        });
      });
    });

    req.setTimeout(timeoutMs, () => {
      req.destroy(
        new ConnectionError(`HTTP request timed out after ${timeoutMs}ms`),
      );
    });

    if (signal) {
      const onAbort = () => req.destroy(abortError(signal));
      if (signal.aborted) {
        onAbort();
      } else {
        signal.addEventListener('abort', onAbort, { once: true });
      }
    }

    req.on('error', (err: Error) => {
      if (err instanceof ConnectionError) {
        reject(err);
      } else if (err.name === 'AbortError' || signal?.aborted) {
        reject(abortError(signal!));
      } else {
        reject(new ConnectionError(`HTTP request failed: ${err.message}`));
      }
    });

    if (body !== undefined) {
      req.write(body);
    }
    req.end();
  });
}

function abortError(signal: AbortSignal): Error {
  const e = new Error(signal.reason?.toString?.() ?? 'request aborted');
  e.name = 'AbortError';
  return e;
}

function escapePath(rawPath: string): string {
  // Percent-encode each path segment, preserving existing %-escapes.
  return rawPath
    .split('/')
    .map((seg) => {
      if (seg === '' || seg.includes('%')) return seg;
      return encodeURIComponent(seg);
    })
    .join('/');
}

function asInt(m: JsonObject, key: string, def: number): number {
  const raw = m[key];
  if (typeof raw === 'number') return Math.trunc(raw);
  if (typeof raw === 'bigint') return Number(raw);
  if (typeof raw === 'string') {
    const n = Number(raw);
    if (Number.isSafeInteger(n)) return n;
  }
  return def;
}

function optStr(m: JsonObject, key: string): string | undefined {
  const v = asString(m[key]);
  return v === '' ? undefined : v;
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

/** The Actae client: full HTTP API plus WebSocket realtime (see client_ws).
 * Safe for concurrent use. */
export class ActaeClient {
  private readonly apiKey: string;
  private readonly endpoint: string;
  private readonly timeout: number;
  private readonly tls?: ResolvedTLS;
  private readonly maxReconnectFailures: number;
  private readonly keepAlive: number;

  /** WebSocket auto-reconnect on drop (mutable via `disconnect()`). */
  autoReconnect: boolean;
  /** Deliver own publishes to local OnMessage callbacks. */
  echoSelf: boolean;
  /** The derived WebSocket URL (http→ws, https→wss, /ws appended). */
  readonly wsEndpoint: string;

  constructor(opts: ClientOptions) {
    if (!opts.apiKey) {
      throw new TypeError('api_key is required');
    }
    if (!opts.endpoint && !opts.wsEndpoint) {
      throw new TypeError('at least one of endpoint or ws_endpoint is required');
    }
    this.apiKey = opts.apiKey;
    this.timeout = opts.timeout !== undefined && opts.timeout > 0 ? opts.timeout : 30000;
    this.autoReconnect = opts.autoReconnect ?? true;
    this.echoSelf = opts.echoSelf ?? false;
    this.maxReconnectFailures =
      opts.maxReconnectFailures !== undefined && opts.maxReconnectFailures > 0
        ? opts.maxReconnectFailures
        : 10;
    const ka = opts.keepAliveInterval ?? 0;
    this.keepAlive = ka < 0 ? 0 : ka === 0 ? 30000 : ka;
    this.tls = resolveTLS(opts.tls);

    this.endpoint = (opts.endpoint ?? '').replace(/\/+$/, '');
    let ws = opts.wsEndpoint;
    if (!ws && this.endpoint !== '') {
      const derived = this.endpoint
        .replace(/^https:\/\//, 'wss://')
        .replace(/^http:\/\//, 'ws://');
      ws = derived.endsWith('/ws') ? derived : `${derived}/ws`;
    }
    this.wsEndpoint = (ws ?? '').replace(/\/+$/, '');
  }

  /** The configured HTTP base URL. */
  get httpEndpoint(): string {
    return this.endpoint;
  }

  // ---------------------------------------------------------------------
  // Request engine
  // ---------------------------------------------------------------------

  private async do<T extends JsonValue | string = JsonValue | string>(
    method: string,
    path: string,
    query?: Record<string, string>,
    body?: unknown,
    headers?: Record<string, string>,
    signal?: AbortSignal,
  ): Promise<T> {
    let url: URL;
    try {
      url = new URL(this.endpoint + escapePath(path));
    } catch {
      throw new ConnectionError(`Invalid endpoint URL: ${this.endpoint}`);
    }
    if (query) {
      for (const [k, v] of Object.entries(query)) {
        url.searchParams.set(k, v);
      }
    }

    const reqHeaders: Record<string, string> = {
      'x-api-key': this.apiKey,
      ...(headers ?? {}),
    };
    let payload: string | undefined;
    if (body !== undefined) {
      reqHeaders['content-type'] = 'application/json';
      payload = stringify(body);
    }

    const resp = await performRequest(
      url,
      method,
      reqHeaders,
      payload,
      this.tls,
      this.timeout,
      signal,
    );

    const isText =
      resp.contentType.includes('text/') ||
      resp.contentType.includes('application/text');
    if (isText) {
      if (resp.status >= 400) {
        throw this.mapHTTPError(resp.status, resp.body);
      }
      return resp.body as T;
    }

    let parsed: JsonValue;
    if (resp.body.trim() === '') {
      parsed = {} as JsonObject;
    } else {
      try {
        parsed = parseJson(resp.body);
      } catch {
        if (resp.status >= 400) {
          parsed = { message: resp.body.trim() } as JsonObject;
        } else {
          return resp.body as T;
        }
      }
    }

    if (resp.status >= 400) {
      throw this.mapHTTPError(resp.status, parsed);
    }
    return parsed as T;
  }

  private mapHTTPError(status: number, parsed: JsonValue): Error {
    const m = asMap(parsed);
    const statusCode = m ? asString(m['status']) : '';
    let errorMsg = m ? asString(m['error']) : '';
    if (errorMsg === '') errorMsg = m ? asString(m['message']) : '';

    switch (status) {
      case 402:
        return new LockError(errorMsg || 'Subscription lock enforced');
      case 429:
        return new RateLimitError(
          m ? asInt(m, 'retry_after_seconds', 60) : 60,
          errorMsg,
        );
      case 401:
        return new AuthError(errorMsg || 'Authentication failed');
      case 404:
        if (statusCode === 'execution_not_found') {
          return new ExecutionNotFoundError(errorMsg);
        }
        return new NotFoundError(errorMsg);
      case 409:
        switch (statusCode) {
          case 'snapshot_boundary_required':
            return new SnapshotBoundaryError(errorMsg);
          case 'version_conflict':
            return new VersionConflictError(errorMsg);
          case 'consumer_not_found':
            return new ConsumerError(errorMsg);
          case 'idempotency_key_mismatch':
            return new IdempotencyKeyMismatchError(errorMsg);
          case 'execution_not_owned':
            return new ExecutionNotOwnedError(errorMsg);
          case 'idempotency_conflict':
            return new IdempotencyConflictError(errorMsg);
          case 'channel_conflict':
            return new ChannelConflictError(errorMsg);
          case 'counterfactual_blocked':
            return new CounterfactualBlockedError(errorMsg);
          case 'fork_tool_blocked':
            return new ForkToolBlockedError(errorMsg);
          default:
            return new APIError(status, errorMsg);
        }
      default:
        if (status >= 500) {
          return new ServerError(errorMsg);
        }
        return new APIError(status, errorMsg);
    }
  }

  // ---------------------------------------------------------------------
  // Events — HTTP API
  // ---------------------------------------------------------------------

  /** Records a new event on a channel.
   * POST /api/v1/events/record. Returns the persisted Event with its
   * server-assigned cursor and ID. When `operationId` is set and an event
   * with the same channel_id + operation_id already exists, the original
   * event is returned (idempotent replay, safe for retries). */
  async record(
    channelId: string,
    eventType: string,
    payload: JsonValue,
    opts: RecordOptions = {},
  ): Promise<Event> {
    validateChannelId(channelId);
    const meta: JsonObject = { actor: opts.actor ?? '' };
    if (opts.agentId !== undefined) meta['agent_id'] = opts.agentId;
    if (opts.userId !== undefined) meta['user_id'] = opts.userId;
    if (opts.metadata !== undefined) meta['metadata'] = opts.metadata as JsonObject;
    const body: JsonObject = {
      channel_id: channelId,
      event_type: eventType,
      payload: payload as JsonValue,
      metadata: meta,
    };
    if (opts.operationId !== undefined) body['operation_id'] = opts.operationId;
    if (opts.stepNumber !== undefined) body['step_number'] = opts.stepNumber;
    if (opts.dependencies !== undefined) body['dependencies'] = opts.dependencies.map(value => ({ event_id: value.eventId, dependency_type: value.dependencyType ?? 'causal' }));

    const result = await this.do<JsonObject>('POST', '/api/v1/events/record', undefined, body);
    const ev = eventFromRecord(asMap(result['event']) ?? {});
    ev.metadata = opts.metadata as JsonObject | undefined;
    ev.agentId = opts.agentId;
    ev.userId = opts.userId;
    return ev;
  }

  /** Replays events from a channel, starting at an optional cursor.
   * GET /api/v1/events/replay/{channel_id}. Limit defaults to 100 (server
   * max 1000 — page with `cursor` for more). */
  async replay(channelId: string, opts: ReplayOptions = {}): Promise<Event[]> {
    validateChannelId(channelId);
    const limit = opts.limit !== undefined && opts.limit > 0 ? opts.limit : 100;
    const query: Record<string, string> = { limit: String(limit) };
    if (opts.cursor !== undefined) query['cursor'] = String(opts.cursor);
    if (opts.eventType !== undefined) query['event_type'] = opts.eventType;

    const result = await this.do<JsonObject>(
      'GET',
      `/api/v1/events/replay/${channelId}`,
      query,
    );
    const events: Event[] = [];
    for (const raw of asList(result['events'])) {
      const ev = asMap(raw);
      if (ev) events.push(eventFromReplay(ev));
    }
    return events;
  }

  async causalGraph(eventId: string, maxNodes = 1000): Promise<JsonObject> {
    return this.do<JsonObject>('GET', `/api/v1/events/${eventId}/causal-graph`, { max_nodes: String(maxNodes) });
  }

  /** Searches events across channels with flexible filters.
   * POST /api/v1/events/query. Limit defaults to 100 (server max 1000). */
  async query(opts: QueryOptions = {}): Promise<Event[]> {
    const limit = opts.limit !== undefined && opts.limit > 0 ? opts.limit : 100;
    const body: JsonObject = { limit };
    if (opts.channelIds !== undefined) body['channel_ids'] = opts.channelIds;
    if (opts.eventType !== undefined) body['event_type'] = opts.eventType;
    if (opts.actor !== undefined) body['actor'] = opts.actor;
    if (opts.cursorStart !== undefined) body['cursor_start'] = opts.cursorStart;
    if (opts.cursorEnd !== undefined) body['cursor_end'] = opts.cursorEnd;
    if (opts.from !== undefined) body['from'] = opts.from;
    if (opts.to !== undefined) body['to'] = opts.to;
    if (opts.offset !== undefined) body['offset'] = opts.offset;

    const result = await this.do<JsonObject>('POST', '/api/v1/events/query', undefined, body);
    const events: Event[] = [];
    for (const raw of asList(result['events'])) {
      const ev = asMap(raw);
      if (ev) events.push(eventFromQuery(ev));
    }
    return events;
  }

  /** Returns the latest cursor value for a channel, or undefined when the
   * channel has no events. GET /api/v1/events/cursor/{channel_id}. */
  async getCursor(channelId: string): Promise<number | undefined> {
    validateChannelId(channelId);
    const result = await this.do<JsonObject>('GET', `/api/v1/events/cursor/${channelId}`);
    const raw = result['latest_cursor'];
    if (raw === undefined || raw === null) return undefined;
    return asInt(result, 'latest_cursor', 0);
  }

  /** Alias for getCursor. */
  async latestCursor(channelId: string): Promise<number | undefined> {
    return this.getCursor(channelId);
  }

  /** Atomically persists an event AND its resulting state snapshot in one
   * server-side transaction. POST /api/v1/events/transition. Returns the
   * persisted Event and the new state version. Raises VersionConflictError
   * on guard mismatch. */
  async transition(
    channelId: string,
    eventType: string,
    payload: JsonValue,
    state: JsonObject,
    opts: TransitionOptions = {},
  ): Promise<TransitionResult> {
    validateChannelId(channelId);
    const meta: JsonObject = { actor: opts.actor ?? '' };
    if (opts.agentId !== undefined) meta['agent_id'] = opts.agentId;
    if (opts.userId !== undefined) meta['user_id'] = opts.userId;
    if (opts.metadata !== undefined) meta['metadata'] = opts.metadata as JsonObject;
    const body: JsonObject = {
      channel_id: channelId,
      type: eventType,
      payload: payload as JsonValue,
      state: state as JsonObject,
      metadata: meta,
    };
    if (opts.operationId !== undefined) body['operation_id'] = opts.operationId;
    if (opts.expectedVersion !== undefined) body['expected_version'] = opts.expectedVersion;
    if (opts.expectedCursor !== undefined) body['expected_cursor'] = opts.expectedCursor;
    if (opts.dependencies !== undefined) body['dependencies'] = opts.dependencies.map(value => ({ event_id: value.eventId, dependency_type: value.dependencyType ?? 'causal' }));

    const result = await this.do<JsonObject>('POST', '/api/v1/events/transition', undefined, body);
    return {
      event: eventFromRecord(asMap(result['event']) ?? {}),
      stateVersion: asInt(result, 'state_version', 0),
    };
  }

  // ---------------------------------------------------------------------
  // State — versioned snapshots
  // ---------------------------------------------------------------------

  /** Saves a cursor-aligned context snapshot for agent resume. Each call
   * creates a new immutable version. POST /api/v1/state/{channel_id}.
   * Returns the assigned version number. */
  async saveState(
    channelId: string,
    cursor: number,
    state: JsonObject,
    opts: SaveStateOptions = {},
  ): Promise<number> {
    validateChannelId(channelId);
    const body: JsonObject = {
      channel_id: channelId,
      cursor,
      state: state as JsonObject,
    };
    if (opts.expectedVersion !== undefined) body['expected_version'] = opts.expectedVersion;
    if (opts.expectedCursor !== undefined) body['expected_cursor'] = opts.expectedCursor;

    const result = await this.do<JsonObject>('POST', `/api/v1/state/${channelId}`, undefined, body);
    return asInt(result, 'version', 0);
  }

  /** Returns the latest saved state snapshot for a channel, or undefined
   * when no state has been saved. GET /api/v1/state/{channel_id}. */
  async latestState(channelId: string): Promise<StateSnapshot | undefined> {
    validateChannelId(channelId);
    const result = await this.do<JsonObject>('GET', `/api/v1/state/${channelId}`);
    return stateSnapshotFrom(result);
  }

  /** Lists state version history for a channel (metadata only, no blobs).
   * GET /api/v1/state/{channel_id}/versions. Entries are ordered
   * newest-first. */
  async listStates(
    channelId: string,
    opts: ListStatesOptions = {},
  ): Promise<StateVersionInfo[]> {
    validateChannelId(channelId);
    const limit = opts.limit !== undefined && opts.limit > 0 ? opts.limit : 100;
    const query: Record<string, string> = {
      limit: String(limit),
      offset: String(opts.offset ?? 0),
    };
    const result = await this.do<JsonObject>(
      'GET',
      `/api/v1/state/${channelId}/versions`,
      query,
    );
    const out: StateVersionInfo[] = [];
    for (const raw of asList(result['versions'])) {
      const v = asMap(raw);
      if (v) {
        out.push({
          version: asInt(v, 'version', 0),
          cursor: asInt(v, 'cursor', 0),
          timestamp: asString(v['timestamp']),
        });
      }
    }
    return out;
  }

  /** Returns a specific state snapshot by version, or undefined when the
   * version does not exist. GET /api/v1/state/{channel_id}/version/{version}. */
  async getState(
    channelId: string,
    version: number,
  ): Promise<StateSnapshot | undefined> {
    validateChannelId(channelId);
    const result = await this.do<JsonObject>(
      'GET',
      `/api/v1/state/${channelId}/version/${version}`,
    );
    return stateSnapshotFrom(result);
  }

  /** Deletes a specific state snapshot version.
   * DELETE /api/v1/state/{channel_id}/version/{version}. */
  async deleteState(channelId: string, version: number): Promise<void> {
    validateChannelId(channelId);
    await this.do('DELETE', `/api/v1/state/${channelId}/version/${version}`);
  }

  // ---------------------------------------------------------------------
  // Channels — forks, metadata, tree, diff, trail
  // ---------------------------------------------------------------------

  /** Lists all active channel IDs known to Actae. GET /api/v1/channels. */
  async listChannels(): Promise<string[]> {
    const result = await this.do<JsonObject>('GET', '/api/v1/channels');
    const out: string[] = [];
    for (const raw of asList(result['channels'])) {
      if (typeof raw === 'string') out.push(raw);
    }
    return out;
  }

  /** Forks a channel at a cursor into a new experiment fork.
   * POST /api/v1/channels/fork.
   *
   * Boundary semantics: at_cursor=0 forks from the channel's latest saved
   * state; at_cursor>0 forks from the latest snapshot whose cursor <=
   * at_cursor (SnapshotBoundaryError when none exists). Passing the same
   * operation_id replays the fork idempotently.
   *
   * Returns the immutable ForkReceipt (requested/resolved boundary, source
   * state version + SHA-256, restorability, reproducibility grade). */
  async fork(
    sourceChannelId: string,
    newChannelId: string,
    atCursor: number,
    opts: ForkOptions = {},
  ): Promise<ForkReceipt> {
    validateChannelId(sourceChannelId);
    validateChannelId(newChannelId);
    const operationId = opts.operationId ?? newUUID();
    const payload: JsonObject = {
      source_channel_id: sourceChannelId,
      new_channel_id: newChannelId,
      at_cursor: atCursor,
      operation_id: operationId,
    };
    if (opts.displayName !== undefined && opts.displayName !== '') {
      payload['display_name'] = opts.displayName;
    }
    if (opts.reason !== undefined && opts.reason !== '') {
      payload['reason'] = opts.reason;
    }
    if (opts.experimentMetadata !== undefined) {
      payload['experiment_metadata'] = opts.experimentMetadata as JsonObject;
    }
    if (opts.manifest !== undefined) payload['manifest'] = opts.manifest as JsonObject;
    if (opts.toolPolicies !== undefined) payload['tool_policies'] = opts.toolPolicies as JsonObject;
    if (opts.expectedVersion !== undefined) payload['expected_version'] = opts.expectedVersion;
    if (opts.expectedCursor !== undefined) payload['expected_cursor'] = opts.expectedCursor;

    const data = await this.do<JsonObject>('POST', '/api/v1/channels/fork', undefined, payload);
    return forkReceiptFromDict(data);
  }

  /** Fetches the immutable fork receipt for a channel.
   * GET /api/v1/channels/{channel_id}/receipt. */
  async getForkReceipt(channelId: string): Promise<ForkReceipt> {
    validateChannelId(channelId);
    const data = await this.do<JsonObject>('GET', `/api/v1/channels/${channelId}/receipt`);
    return forkReceiptFromDict(data);
  }

  /** Highest step number recorded on a channel (0 = none).
   * GET /api/v1/channels/{channel_id}/steps.
   *
   * O(1) crash-recovery probe used by AgentSession.resume. Returns undefined
   * when the endpoint is unavailable (older server), so callers can fall back
   * to replay-based recovery. */
  async latestStepNumber(channelId: string): Promise<number | undefined> {
    validateChannelId(channelId);
    try {
      const data = await this.do<JsonObject>(
        'GET',
        `/api/v1/channels/${channelId}/steps`,
      );
      return asInt(data, 'last_step_number', 0);
    } catch (err) {
      if (err instanceof NotFoundError) return undefined;
      throw err;
    }
  }

  /** Resolves a step number to the channel that OWNS it and its cursor via
   * the server-owned step index. GET /api/v1/channels/{channel_id}/steps/{n}.
   *
   * Walks the fork lineage server-side: a fork inherits its first
   * forked_at_step steps from its parent, so step N of a fork resolves
   * against the ancestor that recorded it. Returns undefined when the step is
   * not indexed (channels created before the index — callers fall back to
   * replay-based resolution). */
  async resolveStep(
    channelId: string,
    stepNumber: number,
  ): Promise<StepResolution | undefined> {
    validateChannelId(channelId);
    try {
      const data = await this.do<JsonObject>(
        'GET',
        `/api/v1/channels/${channelId}/steps/${stepNumber}`,
      );
      return {
        channelId: asString(data['channel_id']),
        stepNumber: asInt(data, 'step_number', stepNumber),
        cursor: asInt(data, 'cursor', 0),
        eventId: optStr(data, 'event_id'),
      };
    } catch (err) {
      if (err instanceof NotFoundError) return undefined;
      throw err;
    }
  }

  /** Records a fork outcome + optional score.
   * PATCH /api/v1/channels/{channel_id}/outcome. */
  async setOutcome(
    channelId: string,
    outcome: string,
    score?: number,
  ): Promise<JsonObject> {
    validateChannelId(channelId);
    const payload: JsonObject = { outcome };
    if (score !== undefined) payload['score'] = score;
    return this.do<JsonObject>(
      'PATCH',
      `/api/v1/channels/${channelId}/outcome`,
      undefined,
      payload,
    );
  }

  /** Promotes a winning fork into its parent (append-only merge).
   * POST /api/v1/channels/{channel_id}/promote. */
  async promoteChannel(channelId: string): Promise<JsonObject> {
    validateChannelId(channelId);
    return this.do<JsonObject>('POST', `/api/v1/channels/${channelId}/promote`);
  }

  /** Soft-deletes a channel (optionally the whole subtree) and purges its
   * rows. DELETE /api/v1/channels/{channel_id}. */
  async deleteChannel(channelId: string, recursive = false): Promise<JsonObject> {
    validateChannelId(channelId);
    const query = recursive ? { recursive: 'true' } : undefined;
    return this.do<JsonObject>(
      'DELETE',
      `/api/v1/channels/${channelId}`,
      query,
    );
  }

  /** Fetches the combined "what changed?" comparison between two forks
   * (state diff + manifest diff + tool diff + output + metrics).
   * GET /api/v1/channels/compare?left=&right=. */
  async compareChannels(left: string, right: string): Promise<JsonObject> {
    validateChannelId(left);
    validateChannelId(right);
    return this.do<JsonObject>('GET', '/api/v1/channels/compare', { left, right });
  }

  /** Returns metadata for a channel, or undefined on HTTP 404.
   * GET /api/v1/channels/{channel_id}/metadata. */
  async getChannelMetadata(
    channelId: string,
  ): Promise<ChannelMetadata | undefined> {
    validateChannelId(channelId);
    try {
      const data = await this.do<JsonObject>('GET', `/api/v1/channels/${channelId}/metadata`);
      return channelMetadataFromDict(data);
    } catch (err) {
      if (err instanceof NotFoundError) return undefined;
      throw err;
    }
  }

  /** Lists direct children (forks) of a channel.
   * GET /api/v1/channels/{channel_id}/forks. */
  async listForks(channelId: string): Promise<ChannelMetadata[]> {
    validateChannelId(channelId);
    const data = await this.do<JsonObject>('GET', `/api/v1/channels/${channelId}/forks`);
    const out: ChannelMetadata[] = [];
    for (const raw of asList(data['forks'])) {
      const b = asMap(raw);
      if (b) out.push(channelMetadataFromDict(b));
    }
    return out;
  }

  /** Returns the full execution tree rooted at a channel.
   * GET /api/v1/channels/{root_channel_id}/fork-tree. */
  async getForkTree(rootChannelId: string): Promise<ForkInfo> {
    validateChannelId(rootChannelId);
    const data = await this.do<JsonObject>('GET', `/api/v1/channels/${rootChannelId}/fork-tree`);
    return forkInfoFromDict(data);
  }

  /** Structurally diffs two channels' latest saved states — the
   * counterfactual-debugging primitive: fork two fixes from a failing run,
   * then diff their states to see which one fixed it.
   * GET /api/v1/channels/diff?left={left}&right={right}. */
  async diffStates(left: string, right: string): Promise<StateDiff> {
    validateChannelId(left);
    validateChannelId(right);
    const data = await this.do<JsonObject>('GET', '/api/v1/channels/diff', { left, right });
    return stateDiffFrom(data);
  }

  /** Returns the full lineage-as-audit view for a channel: the chain back to
   * the origin_run_id root, the boundary state it forked from, and the
   * idempotent tool-execution ledger rows on this channel.
   * GET /api/v1/channels/{channel_id}/trail. */
  async decisionTrail(channelId: string): Promise<DecisionTrail> {
    validateChannelId(channelId);
    const data = await this.do<JsonObject>('GET', `/api/v1/channels/${channelId}/trail`);
    return decisionTrailFrom(data);
  }

  /** Updates channel metadata (partial — omitted fields retain their existing
   * values). PUT /api/v1/channels/{channel_id}/metadata. */
  async updateMetadata(
    channelId: string,
    opts: UpdateMetadataOptions,
  ): Promise<ChannelMetadata> {
    validateChannelId(channelId);
    const payload: JsonObject = { channel_id: channelId };
    if (opts.displayName !== undefined) payload['display_name'] = opts.displayName;
    if (opts.reason !== undefined) payload['reason'] = opts.reason;
    if (opts.experimentMetadata !== undefined) {
      payload['experiment_metadata'] = opts.experimentMetadata as JsonObject;
    }
    const data = await this.do<JsonObject>(
      'PUT',
      `/api/v1/channels/${channelId}/metadata`,
      undefined,
      payload,
    );
    return channelMetadataFromDict(data);
  }

  // ---------------------------------------------------------------------
  // Experiments
  // ---------------------------------------------------------------------

  /** Creates an experiment group (baseline + variants).
   * POST /api/v1/experiments. */
  async createExperiment(
    name: string,
    opts: ExperimentOptions = {},
  ): Promise<JsonObject> {
    const payload: JsonObject = { name };
    if (opts.description !== undefined && opts.description !== '') {
      payload['description'] = opts.description;
    }
    if (opts.baselineChannelId !== undefined && opts.baselineChannelId !== '') {
      payload['baseline_channel_id'] = opts.baselineChannelId;
    }
    return this.do<JsonObject>('POST', '/api/v1/experiments', undefined, payload);
  }

  /** Lists experiment groups. GET /api/v1/experiments. */
  async listExperiments(): Promise<JsonValue[]> {
    const data = await this.do<JsonValue>('GET', '/api/v1/experiments');
    if (Array.isArray(data)) return data;
    return asList((data as JsonObject)['experiments']);
  }

  /** Adds a fork to an experiment group.
   * POST /api/v1/experiments/{group_id}/members. */
  async addExperimentMember(
    groupId: string,
    channelId: string,
    opts: ExperimentMemberOptions = {},
  ): Promise<JsonObject> {
    validateChannelId(channelId);
    const payload: JsonObject = {
      channel_id: channelId,
      role: opts.role ?? 'variant',
    };
    if (opts.declaredDelta !== undefined) {
      payload['declared_delta'] = opts.declaredDelta as JsonObject;
    }
    return this.do<JsonObject>(
      'POST',
      `/api/v1/experiments/${groupId}/members`,
      undefined,
      payload,
    );
  }

  /** Ranks an experiment's members by result score.
   * GET /api/v1/experiments/{group_id}/rank. */
  async rankExperiment(groupId: string): Promise<JsonObject> {
    return this.do<JsonObject>('GET', `/api/v1/experiments/${groupId}/rank`);
  }

  // ---------------------------------------------------------------------
  // Health & readiness
  // ---------------------------------------------------------------------

  /** Checks the health of the Actae server. GET /healthz. */
  async healthCheck(): Promise<HealthStatus> {
    const result = await this.do<JsonObject>('GET', '/healthz');
    return healthStatusFromResponse(result);
  }

  /** Checks whether the Actae server is ready to accept traffic.
   * GET /readyz. */
  async readinessCheck(): Promise<ReadinessResult> {
    const result = await this.do<JsonObject>('GET', '/readyz');
    return readinessResultFromResponse(result);
  }

  /** Returns Prometheus-format metrics. GET /metrics. */
  async getMetricsText(): Promise<string> {
    return this.do<string>('GET', '/metrics');
  }

  /** Returns structured JSON metrics. GET /metrics.json. */
  async getMetricsJson(): Promise<MetricsSnapshot> {
    const result = await this.do<JsonObject>('GET', '/metrics.json');
    return metricsSnapshotFromResponse(result);
  }

  // ---------------------------------------------------------------------
  // Auth API
  // ---------------------------------------------------------------------

  /** Creates a new user account. POST /api/v1/auth/signup. */
  async signup(opts: SignupOptions): Promise<AuthResult> {
    const body: JsonObject = { email: opts.email, password: opts.password };
    if (opts.name !== undefined && opts.name !== '') body['name'] = opts.name;
    const result = await this.do<JsonObject>('POST', '/api/v1/auth/signup', undefined, body);
    return authResultFromResponse(result);
  }

  /** Authenticates with email and password. POST /api/v1/auth/login. */
  async login(email: string, password: string): Promise<AuthResult> {
    const body: JsonObject = { email, password };
    const result = await this.do<JsonObject>('POST', '/api/v1/auth/login', undefined, body);
    return authResultFromResponse(result);
  }

  /** Invalidates a JWT token server-side. POST /api/v1/auth/logout. */
  async logout(jwtToken: string): Promise<void> {
    await this.do('POST', '/api/v1/auth/logout', undefined, undefined, {
      authorization: `Bearer ${jwtToken}`,
    });
  }

  /** Returns the current user's profile for a JWT token. GET /api/v1/auth/me. */
  async getMe(jwtToken: string): Promise<UserInfo> {
    const result = await this.do<JsonObject>(
      'GET',
      '/api/v1/auth/me',
      undefined,
      undefined,
      { authorization: `Bearer ${jwtToken}` },
    );
    return userInfoFromDict(asMap(result['user']) ?? {});
  }

  // ---------------------------------------------------------------------
  // Consumer groups
  // ---------------------------------------------------------------------

  /** Creates a durable consumer group bound to a channel. POST /api/v1/groups. */
  async createGroup(
    groupId: string,
    channelId: string,
    metadata?: JsonObject,
  ): Promise<GroupInfo> {
    validateChannelId(channelId);
    const body: JsonObject = { group_id: groupId, channel_id: channelId };
    if (metadata !== undefined) body['metadata'] = metadata as JsonObject;
    const data = await this.do<JsonObject>('POST', '/api/v1/groups', undefined, body);
    return groupInfoFromResponse(data);
  }

  /** Lists consumer groups, optionally filtered by channel. GET /api/v1/groups. */
  async listGroups(opts: ListGroupsOptions = {}): Promise<GroupInfo[]> {
    const query = opts.channelId ? { channel_id: opts.channelId } : undefined;
    const data = await this.do<JsonObject>('GET', '/api/v1/groups', query);
    const out: GroupInfo[] = [];
    for (const raw of asList(data['groups'])) {
      const g = asMap(raw);
      if (g) out.push(groupInfoFromResponse(g));
    }
    return out;
  }

  /** Deletes a consumer group (cascades consumers and offsets).
   * DELETE /api/v1/groups/{group_id}. */
  async deleteGroup(groupId: string): Promise<void> {
    await this.do('DELETE', `/api/v1/groups/${groupId}`);
  }

  /** Registers (or renews) a consumer's lease and durable offset row.
   * POST /api/v1/groups/{group_id}/join. leaseSeconds defaults to 60. */
  async joinGroup(
    groupId: string,
    consumerId: string,
    leaseSeconds?: number,
  ): Promise<GroupOffset> {
    const lease = leaseSeconds !== undefined && leaseSeconds > 0 ? leaseSeconds : 60;
    const data = await this.do<JsonObject>(
      'POST',
      `/api/v1/groups/${groupId}/join`,
      undefined,
      { consumer_id: consumerId, lease_seconds: lease },
    );
    return groupOffsetFromResponse(data);
  }

  /** Claims a batch of events for a consumer (at-least-once semantics).
   * POST /api/v1/groups/{group_id}/work. limit defaults to 100. */
  async claimWork(
    groupId: string,
    consumerId: string,
    limit?: number,
  ): Promise<ClaimedWork> {
    const lim = limit !== undefined && limit > 0 ? limit : 100;
    const data = await this.do<JsonObject>(
      'POST',
      `/api/v1/groups/${groupId}/work`,
      undefined,
      { consumer_id: consumerId, limit: lim },
    );
    return claimedWorkFromResponse(data);
  }

  /** Acknowledges processed work up to and including cursor.
   * POST /api/v1/groups/{group_id}/ack. cursor is a CHANNEL cursor
   * (Event.channelCursor), not the global cursor, and commits a watermark:
   * pass the highest contiguously processed channel cursor. */
  async ackWork(groupId: string, consumerId: string, cursor: number): Promise<void> {
    await this.do(
      'POST',
      `/api/v1/groups/${groupId}/ack`,
      undefined,
      { consumer_id: consumerId, cursor },
    );
  }

  /** Extends a consumer's lease. POST /api/v1/groups/{group_id}/heartbeat. */
  async heartbeat(groupId: string, consumerId: string, leaseSeconds?: number): Promise<void> {
    const lease = leaseSeconds !== undefined && leaseSeconds > 0 ? leaseSeconds : 60;
    await this.do(
      'POST',
      `/api/v1/groups/${groupId}/heartbeat`,
      undefined,
      { consumer_id: consumerId, lease_seconds: lease },
    );
  }

  /** Returns per-consumer durable offsets for a group.
   * GET /api/v1/groups/{group_id}/offsets. */
  async groupOffsets(groupId: string): Promise<GroupOffset[]> {
    const data = await this.do<JsonObject>('GET', `/api/v1/groups/${groupId}/offsets`);
    const out: GroupOffset[] = [];
    for (const raw of asList(data['offsets'])) {
      const o = asMap(raw);
      if (o) out.push(groupOffsetFromResponse(o));
    }
    return out;
  }

  /** Creates a durable execution group for independently running agents. */
  async createExecutionGroup(groupId: string, metadata: JsonObject = {}): Promise<ExecutionGroup> {
    validateChannelId(groupId);
    const data = await this.do<JsonObject>('POST', '/api/v1/execution-groups', undefined, { group_id: groupId, metadata });
    return executionGroupFrom(asMap(data['group']) ?? {});
  }
  executionGroup(groupId: string): import('./groups.js').GroupSession {
    validateChannelId(groupId);
    return new GroupSession(this, groupId);
  }
  group(groupId: string): import('./groups.js').GroupSession { return this.executionGroup(groupId); }
  async executionGroupMembers(groupId: string): Promise<ExecutionGroupMember[]> {
    const data = await this.do<JsonObject>('GET', `/api/v1/execution-groups/${groupId}/members`);
    return asList(data['members']).flatMap(value => { const member = asMap(value); return member ? [executionGroupMemberFrom(member)] : []; });
  }
  async listExecutionGroups(): Promise<ExecutionGroup[]> {
    const data = await this.do<JsonObject>('GET', '/api/v1/execution-groups');
    return asList(data['groups']).flatMap(v => { const m = asMap(v); return m ? [executionGroupFrom(m)] : []; });
  }
  async addExecutionGroupMember(groupId: string, memberId: string, channelId: string, opts: { role?: string; metadata?: JsonObject } = {}): Promise<ExecutionGroupMember> {
    validateChannelId(memberId); validateChannelId(channelId);
    const body: JsonObject = { member_id: memberId, channel_id: channelId, metadata: opts.metadata ?? {} };
    if (opts.role !== undefined) body['role'] = opts.role;
    const data = await this.do<JsonObject>('POST', `/api/v1/execution-groups/${groupId}/members`, undefined, body);
    return executionGroupMemberFrom(asMap(data['member']) ?? {});
  }
  async claimMember(groupId: string, memberId: string, ownerId: string, leaseSeconds = 60): Promise<MemberLease> {
    const data = await this.do<JsonObject>('POST', `/api/v1/execution-groups/${groupId}/members/${memberId}/claim`, undefined, { owner_id: ownerId, lease_seconds: leaseSeconds });
    return memberLeaseFrom(asMap(data['lease']) ?? {});
  }
  async heartbeatMember(groupId: string, memberId: string, ownerId: string, generation: number, leaseSeconds = 60): Promise<MemberLease> {
    const data = await this.do<JsonObject>('POST', `/api/v1/execution-groups/${groupId}/members/${memberId}/heartbeat`, undefined, { owner_id: ownerId, generation, lease_seconds: leaseSeconds });
    return memberLeaseFrom(asMap(data['lease']) ?? {});
  }
  async releaseMember(groupId: string, memberId: string, ownerId: string, generation: number): Promise<void> {
    await this.do<JsonObject>('POST', `/api/v1/execution-groups/${groupId}/members/${memberId}/release`, undefined, { owner_id: ownerId, generation });
  }
  async sendGroupMessage(groupId: string, fromMemberId: string, toMemberId: string, messageType: string, payload: JsonValue, opts: { causalContext?: JsonObject; operationId?: string } = {}): Promise<GroupMessage> {
    const body: JsonObject = { from_member_id: fromMemberId, to_member_id: toMemberId, type: messageType, payload };
    if (opts.causalContext !== undefined) body['causal_context'] = opts.causalContext;
    if (opts.operationId !== undefined) body['operation_id'] = opts.operationId;
    const data = await this.do<JsonObject>('POST', `/api/v1/execution-groups/${groupId}/messages`, undefined, body);
    return groupMessageFrom(asMap(data['message']) ?? {});
  }
  async groupMessages(groupId: string, memberId: string, opts: { after?: string; limit?: number } = {}): Promise<GroupMessage[]> {
    const query: Record<string, string> = { limit: String(opts.limit ?? 100) }; if (opts.after !== undefined) query['after'] = opts.after;
    const data = await this.do<JsonObject>('GET', `/api/v1/execution-groups/${groupId}/members/${memberId}/messages`, query);
    return asList(data['messages']).flatMap(v => { const m = asMap(v); return m ? [groupMessageFrom(m)] : []; });
  }
  async acknowledgeGroupMessage(messageId: string, ownerId: string, generation: number): Promise<GroupMessage> {
    const data = await this.do<JsonObject>('POST', `/api/v1/execution-group-messages/${messageId}/ack`, undefined, { owner_id: ownerId, generation });
    return groupMessageFrom(asMap(data['message']) ?? {});
  }
  async forkExecutionGroup(forkGroupId: string, sourceGroupId: string, interventionMemberId: string, interventionCursor: number, memberPolicies: JsonObject, toolPolicies: JsonObject): Promise<JsonObject> {
    const data = await this.do<JsonObject>('POST', '/api/v1/execution-group-forks', undefined, { fork_group_id: forkGroupId, source_group_id: sourceGroupId, intervention_member_id: interventionMemberId, intervention_cursor: interventionCursor, member_policies: memberPolicies, tool_policies: toolPolicies });
    return asMap(data['fork']) ?? {};
  }
  async getExecutionGroupFork(forkGroupId: string): Promise<JsonObject> {
    const data = await this.do<JsonObject>('GET', `/api/v1/execution-group-forks/${forkGroupId}`);
    return asMap(data['fork']) ?? {};
  }
  async promoteExecutionGroupForkMember(forkGroupId: string, memberId: string): Promise<JsonObject> {
    const data = await this.do<JsonObject>('POST', `/api/v1/execution-group-forks/${forkGroupId}/members/${memberId}/promote`, undefined, {});
    return asMap(data['fork']) ?? {};
  }

  // ---------------------------------------------------------------------
  // Persisted wake-ups
  // ---------------------------------------------------------------------

  /** Schedules a wake-up for a channel. At or after runAt (RFC 3339) the
   * server fires a scheduler.wakeup event on the channel.
   * POST /api/v1/scheduler/wakeups. */
  async scheduleWakeup(
    channelId: string,
    runAt: string,
    payload?: JsonObject,
  ): Promise<Wakeup> {
    const body: JsonObject = { channel_id: channelId, run_at: runAt };
    if (payload !== undefined) body['payload'] = payload as JsonObject;
    const data = await this.do<JsonObject>('POST', '/api/v1/scheduler/wakeups', undefined, body);
    return wakeupFromResponse(data);
  }

  /** Lists wake-ups, optionally filtered by channel and status.
   * GET /api/v1/scheduler/wakeups. */
  async listWakeups(opts: ListWakeupsOptions = {}): Promise<Wakeup[]> {
    const query: Record<string, string> = {};
    if (opts.channelId !== undefined) query['channel_id'] = opts.channelId;
    if (opts.status !== undefined) query['status'] = opts.status;
    const data = await this.do<JsonObject>(
      'GET',
      '/api/v1/scheduler/wakeups',
      Object.keys(query).length ? query : undefined,
    );
    const out: Wakeup[] = [];
    for (const raw of asList(data['wakeups'])) {
      const w = asMap(raw);
      if (w) out.push(wakeupFromResponse(w));
    }
    return out;
  }

  /** Fetches a single wake-up, or undefined on HTTP 404.
   * GET /api/v1/scheduler/wakeups/{id}. */
  async getWakeup(wakeupId: string): Promise<Wakeup | undefined> {
    try {
      const data = await this.do<JsonObject>('GET', `/api/v1/scheduler/wakeups/${wakeupId}`);
      return wakeupFromResponse(data);
    } catch (err) {
      if (err instanceof NotFoundError) return undefined;
      throw err;
    }
  }

  /** Cancels a pending wake-up. Returns true when cancelled; when the
   * wake-up already fired/failed/cancelled (HTTP 409) throws
   * WakeupAlreadyFiredError — treat as a benign no-op.
   * DELETE /api/v1/scheduler/wakeups/{id}. */
  async cancelWakeup(wakeupId: string): Promise<boolean> {
    try {
      await this.do('DELETE', `/api/v1/scheduler/wakeups/${wakeupId}`);
      return true;
    } catch (err) {
      if (err instanceof APIError && err.statusCode === 409) {
        throw new WakeupAlreadyFiredError(
          'wakeup already fired, failed, or cancelled',
        );
      }
      throw err;
    }
  }

  // ---------------------------------------------------------------------
  // Durable human approval (human-in-the-loop)
  // ---------------------------------------------------------------------

  /** Records an `approval.requested` event and returns its request id.
   * POST /api/v1/events/record (+ a durable wake-up when `timeoutSeconds` is
   * set). The agent process need not stay alive; a human resolves it with
   * `decideApproval`, and the agent observes it with `waitForApproval`. */
  async requestApproval(
    channelId: string,
    opts: { summary: string; details?: JsonObject; timeoutSeconds?: number; requester?: string; requestId?: string },
  ): Promise<string> {
    if (!opts.summary) throw new TypeError('summary is required');
    const requestId = opts.requestId ?? newUUID();
    const payload: JsonObject = { request_id: requestId, summary: opts.summary };
    if (opts.details !== undefined) payload['details'] = opts.details;
    if (opts.timeoutSeconds !== undefined) payload['timeout_seconds'] = opts.timeoutSeconds;
    await this.record(channelId, APPROVAL_REQUESTED, payload, {
      actor: opts.requester ?? 'agent',
      operationId: requestId,
    });
    if (opts.timeoutSeconds !== undefined) {
      const deadline = new Date(Date.now() + opts.timeoutSeconds * 1000).toISOString();
      await this.scheduleWakeup(channelId, deadline, {
        approval_request_id: requestId,
        kind: 'approval_timeout',
      });
    }
    return requestId;
  }

  /** Records the human decision (`approved` | `rejected`) as an
   * `approval.decided` event. Actae never resumes anything itself. */
  async decideApproval(
    channelId: string,
    requestId: string,
    opts: { decision: string; actor: string; reason?: string; operationId?: string },
  ): Promise<Event> {
    if (opts.decision !== 'approved' && opts.decision !== 'rejected') {
      throw new TypeError("decision must be 'approved' or 'rejected'");
    }
    // No volatile field (e.g. a timestamp) belongs in the payload: the
    // deterministic operation id must make an identical decision replay, and
    // the event's own timestamp records when it was committed.
    const payload: JsonObject = {
      request_id: requestId,
      decision: opts.decision,
    };
    if (opts.reason !== undefined) payload['reason'] = opts.reason;
    // A deterministic operation id keyed by (requestId, decision) makes a
    // retried/duplicated decision idempotent (same decision -> the original
    // event). A different decision derives a different id and is recorded as
    // a new event (the application applies last-wins).
    const operationId =
      opts.operationId ?? deterministicOperationKey('approval', opts.decision, requestId);
    return this.record(channelId, APPROVAL_DECIDED, payload, {
      actor: opts.actor,
      operationId,
    });
  }

  /** Polls the channel until the decision for `requestId` appears. On timeout
   * records `approval.expired` and returns undefined. */
  async waitForApproval(
    channelId: string,
    requestId: string,
    opts: { timeoutSeconds?: number; pollInterval?: number; startCursor?: number } = {},
  ): Promise<Event | undefined> {
    const pollInterval = (opts.pollInterval ?? 1) * 1000;
    const deadline =
      opts.timeoutSeconds !== undefined ? Date.now() + opts.timeoutSeconds * 1000 : undefined;
    let cursor = opts.startCursor ?? 0;
    for (;;) {
      const events = await this.replay(channelId, { cursor, limit: 1000 });
      for (const ev of events) {
        if (ev.cursor > cursor) cursor = ev.cursor;
        if (ev.eventType === APPROVAL_DECIDED && payloadField(ev.payload, 'request_id') === requestId) {
          return ev;
        }
      }
      if (deadline !== undefined && Date.now() >= deadline) {
        await this.record(channelId, APPROVAL_EXPIRED, { request_id: requestId }, { actor: 'system' });
        return undefined;
      }
      await new Promise((resolve) => setTimeout(resolve, pollInterval));
    }
  }

  // ---------------------------------------------------------------------
  // Idempotent tool executions
  // ---------------------------------------------------------------------

  /** Claims an idempotent tool execution (create-or-replay).
   * POST /api/v1/executions/claim. The idempotency unit is
   * (channel_id, key_name); a completed execution with a matching request
   * hash replays its persisted result. A mismatch raises
   * IdempotencyKeyMismatchError. */
  async claimExecution(
    channelId: string,
    keyName: string,
    toolName: string,
    params?: JsonValue,
    opts: ClaimExecutionOptions = {},
  ): Promise<ExecutionClaim> {
    const body: JsonObject = {
      channel_id: channelId,
      key_name: keyName,
      tool_name: toolName,
    };
    if (params !== undefined) body['params'] = params as JsonValue;
    if (opts.dedupFields !== undefined) body['dedup_fields'] = opts.dedupFields;
    if (opts.leaseSeconds !== undefined) body['lease_seconds'] = opts.leaseSeconds;
    if (opts.emitReplayEvent) body['emit_replay_event'] = true;
    if (opts.actor !== undefined) body['actor'] = opts.actor;

    const data = await this.do<JsonObject>('POST', '/api/v1/executions/claim', undefined, body);
    return executionClaimFromResponse(data);
  }

  /** Completes a claimed execution with its persisted result.
   * POST /api/v1/executions/{id}/complete. A stale claim token raises
   * ExecutionNotOwnedError; duplicate completes are idempotent. */
  async completeExecution(
    executionId: string,
    claimToken?: string,
    result?: JsonValue,
  ): Promise<ExecutionInfo> {
    const body: JsonObject = {};
    if (claimToken !== undefined) body['claim_token'] = claimToken;
    if (result !== undefined) body['result'] = result as JsonValue;
    const data = await this.do<JsonObject>(
      'POST',
      `/api/v1/executions/${executionId}/complete`,
      undefined,
      body,
    );
    return executionInfoFromResponse(asMap(data['execution']) ?? {});
  }

  /** Marks a claimed execution failed with a structured error.
   * POST /api/v1/executions/{id}/fail. */
  async failExecution(
    executionId: string,
    message: string,
    opts: FailExecutionOptions = {},
  ): Promise<ExecutionInfo> {
    const body: JsonObject = { message };
    if (opts.claimToken !== undefined) body['claim_token'] = opts.claimToken;
    if (opts.errorType !== undefined) body['error_type'] = opts.errorType;
    if (opts.stack !== undefined) body['stack'] = opts.stack;
    const data = await this.do<JsonObject>(
      'POST',
      `/api/v1/executions/${executionId}/fail`,
      undefined,
      body,
    );
    return executionInfoFromResponse(asMap(data['execution']) ?? {});
  }

  /** Extends a running execution's lease.
   * POST /api/v1/executions/{id}/heartbeat. */
  async heartbeatExecution(
    executionId: string,
    claimToken?: string,
    leaseSeconds?: number,
  ): Promise<ExecutionInfo> {
    const body: JsonObject = {};
    if (claimToken !== undefined) body['claim_token'] = claimToken;
    if (leaseSeconds !== undefined) body['lease_seconds'] = leaseSeconds;
    const data = await this.do<JsonObject>(
      'POST',
      `/api/v1/executions/${executionId}/heartbeat`,
      undefined,
      body,
    );
    return executionInfoFromResponse(asMap(data['execution']) ?? {});
  }

  /** Cancels a running execution without storing a result.
   * POST /api/v1/executions/{id}/cancel. */
  async cancelExecution(executionId: string, claimToken?: string): Promise<ExecutionInfo> {
    const body: JsonObject = {};
    if (claimToken !== undefined) body['claim_token'] = claimToken;
    const data = await this.do<JsonObject>(
      'POST',
      `/api/v1/executions/${executionId}/cancel`,
      undefined,
      body,
    );
    return executionInfoFromResponse(asMap(data['execution']) ?? {});
  }

  /** Fetches a single execution by id. Raises ExecutionNotFoundError when it
   * does not exist. GET /api/v1/executions/{id}. */
  async getExecution(executionId: string): Promise<ExecutionInfo> {
    const data = await this.do<JsonObject>('GET', `/api/v1/executions/${executionId}`);
    return executionInfoFromResponse(asMap(data['execution']) ?? {});
  }

  /** Lists executions for a channel, newest first.
   * GET /api/v1/executions?channel_id=...&limit=... Limit must be 1–100. */
  async listExecutions(channelId: string, limit?: number): Promise<ExecutionInfo[]> {
    const lim = limit !== undefined && limit > 0 ? limit : 50;
    const data = await this.do<JsonObject>('GET', '/api/v1/executions', {
      channel_id: channelId,
      limit: String(lim),
    });
    const out: ExecutionInfo[] = [];
    for (const raw of asList(data['executions'])) {
      const e = asMap(raw);
      if (e) out.push(executionInfoFromResponse(e));
    }
    return out;
  }

  /** Deletes an execution record. DELETE /api/v1/executions/{id}. Raises
   * ExecutionNotFoundError when it did not exist. */
  async deleteExecution(executionId: string): Promise<void> {
    await this.do('DELETE', `/api/v1/executions/${executionId}`);
  }

  // ---------------------------------------------------------------------
  // WebSocket — connection lifecycle (see client_ws for the wire protocol)
  // ---------------------------------------------------------------------

  private _ws?: WebSocketConnection;

  private getWs(): WebSocketConnection {
    if (!this._ws) {
      this._ws = new WebSocketConnection({
        apiKey: this.apiKey,
        wsEndpoint: this.wsEndpoint,
        timeout: this.timeout,
        maxReconnectFailures: this.maxReconnectFailures,
        keepAlive: this.keepAlive,
        tls: this.tls,
        autoReconnect: () => this.autoReconnect,
        echoSelf: () => this.echoSelf,
      });
    }
    return this._ws;
  }

  /** Opens the WebSocket connection and authenticates with the API key.
   * Safe to call multiple times — subsequent calls are no-ops while
   * connected. Throws ConnectionError on handshake failure and AuthError on
   * rejection/timeout. */
  async connect(signal?: AbortSignal): Promise<void> {
    return this.getWs().connect(signal);
  }

  /** Closes the WebSocket connection and stops auto-reconnect. Safe to call
   * multiple times. After disconnect, auto-reconnect stays disabled. */
  disconnect(): void {
    this.autoReconnect = false;
    this.getWs().disconnect();
  }

  /** Reports whether the WebSocket is connected and authenticated. */
  isConnected(): boolean {
    return this.getWs().isConnected();
  }

  /** Reports whether WebSocket authentication completed. */
  isAuthenticated(): boolean {
    return this.getWs().isAuthenticated();
  }

  /** Returns the server-assigned connection ID, or "" before authentication. */
  connectionId(): string {
    return this.getWs().connectionId();
  }

  /** Returns a copy of the topic → cursor map for active subscriptions. */
  subscribedTopics(): Record<string, number> {
    return this.getWs().subscribedTopics();
  }

  // ---------------------------------------------------------------------
  // WebSocket — callbacks
  // ---------------------------------------------------------------------

  /** Registers a broadcast callback. Callbacks accumulate. */
  onMessage(cb: MessageHandler): void {
    this.getWs().onMessage(cb);
  }

  /** Registers the WebSocket error callback (single; replaces previous). */
  onError(cb: ErrorHandler): void {
    this.getWs().onError(cb);
  }

  /** Registers the subscription-confirmation callback (single). */
  onSubscribed(cb: SubscribedHandler): void {
    this.getWs().onSubscribed(cb);
  }

  /** Registers the disconnection callback (single). Fires before
   * auto-reconnect. */
  onDisconnected(cb: DisconnectedHandler): void {
    this.getWs().onDisconnected(cb);
  }

  /** Registers the reconnection callback (single). Fires after all topics
   * have been resubscribed. */
  onReconnect(cb: ReconnectHandler): void {
    this.getWs().onReconnect(cb);
  }

  // ---------------------------------------------------------------------
  // WebSocket — subscribe / publish / stream
  // ---------------------------------------------------------------------

  /** Subscribes to a topic for real-time events. When cursor is set, events
   * from that cursor onward are replayed before live events. When wait is
   * true (default), blocks until the server confirms. */
  async subscribe(topic: string, cursor?: number, wait = true, signal?: AbortSignal): Promise<void> {
    return this.getWs().subscribe(topic, cursor, wait, signal);
  }

  /** Subscribes and blocks until the server confirms — the common case. */
  async subscribeAndWait(topic: string, cursor?: number): Promise<void> {
    return this.getWs().subscribe(topic, cursor, true);
  }

  /** Unsubscribes from a topic. */
  async unsubscribe(topic: string): Promise<void> {
    return this.getWs().unsubscribe(topic);
  }

  /** Publishes an event to a topic over WebSocket and returns the persisted
   * Event from the server Ack (no HTTP round-trip). With echoSelf set, the
   * returned event is also delivered to local OnMessage callbacks. */
  async publish(topic: string, payload: JsonValue, opts?: WSPublishOptions, signal?: AbortSignal): Promise<Event> {
    return this.getWs().publish(topic, payload, opts, signal);
  }

  /** Streams live events for a topic (Python SDK `stream()` parity). Ends
   * when the WebSocket disconnects. */
  stream(topic: string, cursor?: number, signal?: AbortSignal): EventStream {
    return this.getWs().stream(topic, cursor, signal);
  }

  // ---------------------------------------------------------------------
  // Namespaced facades (Python SDK parity: client.events.record(...) ===
  // client.record(...))
  // ---------------------------------------------------------------------

  private _facades?: Facades;

  /** Read-only namespaced method subsets mirroring the Python SDK. */
  get events(): EventsFacade {
    return this.getFacades().events;
  }

  get state(): StateFacade {
    return this.getFacades().state;
  }

  get channels(): ChannelsFacade {
    return this.getFacades().channels;
  }

  get executions(): ExecutionsFacade {
    return this.getFacades().executions;
  }

  get groups(): GroupsFacade {
    return this.getFacades().groups;
  }

  get wakeups(): WakeupsFacade {
    return this.getFacades().wakeups;
  }

  get health(): HealthFacade {
    return this.getFacades().health;
  }

  get auth(): AuthFacade {
    return this.getFacades().auth;
  }

  get ws(): WsFacade {
    return this.getFacades().ws;
  }

  private getFacades(): Facades {
    if (this._facades) return this._facades;
    const c = this;
    const facades: Facades = {
      events: {
        record: (channelId, eventType, payload, opts) =>
          c.record(channelId, eventType, payload, opts),
        replay: (channelId, opts) => c.replay(channelId, opts),
        query: (opts) => c.query(opts),
        transition: (channelId, eventType, payload, state, opts) =>
          c.transition(channelId, eventType, payload, state, opts),
        getCursor: (id) => c.getCursor(id),
        latestCursor: (id) => c.latestCursor(id),
      },
      state: {
        saveState: (channelId, cursor, state, opts) =>
          c.saveState(channelId, cursor, state, opts),
        latestState: (id) => c.latestState(id),
        listStates: (id, opts) => c.listStates(id, opts),
        getState: (id, version) => c.getState(id, version),
        deleteState: (id, version) => c.deleteState(id, version),
      },
      channels: {
        listChannels: () => c.listChannels(),
        fork: (source, child, atCursor, opts) => c.fork(source, child, atCursor, opts),
        getChannelMetadata: (id) => c.getChannelMetadata(id),
        listForks: (id) => c.listForks(id),
        getForkTree: (id) => c.getForkTree(id),
        updateMetadata: (id, opts) => c.updateMetadata(id, opts),
        diffStates: (left, right) => c.diffStates(left, right),
        decisionTrail: (id) => c.decisionTrail(id),
      },
      executions: {
        claim: (channelId, keyName, toolName, params, opts) =>
          c.claimExecution(channelId, keyName, toolName, params, opts),
        complete: (id, claimToken, result) => c.completeExecution(id, claimToken, result),
        fail: (id, message, opts) => c.failExecution(id, message, opts),
        heartbeat: (id, claimToken, leaseSeconds) =>
          c.heartbeatExecution(id, claimToken, leaseSeconds),
        cancel: (id, claimToken) => c.cancelExecution(id, claimToken),
        get: (id) => c.getExecution(id),
        list: (channelId, limit) => c.listExecutions(channelId, limit),
        delete: (id) => c.deleteExecution(id),
      },
      groups: {
        create: (groupId, channelId, metadata) => c.createGroup(groupId, channelId, metadata),
        list: (opts) => c.listGroups(opts),
        delete: (groupId) => c.deleteGroup(groupId),
        join: (groupId, consumerId, leaseSeconds) => c.joinGroup(groupId, consumerId, leaseSeconds),
        claimWork: (groupId, consumerId, limit) => c.claimWork(groupId, consumerId, limit),
        ackWork: (groupId, consumerId, cursor) => c.ackWork(groupId, consumerId, cursor),
        heartbeat: (groupId, consumerId, leaseSeconds) =>
          c.heartbeat(groupId, consumerId, leaseSeconds),
        offsets: (groupId) => c.groupOffsets(groupId),
      },
      wakeups: {
        schedule: (channelId, runAt, payload) => c.scheduleWakeup(channelId, runAt, payload),
        list: (opts) => c.listWakeups(opts),
        get: (id) => c.getWakeup(id),
        cancel: (id) => c.cancelWakeup(id),
      },
      health: {
        check: () => c.healthCheck(),
        readiness: () => c.readinessCheck(),
        metricsText: () => c.getMetricsText(),
        metricsJson: () => c.getMetricsJson(),
      },
      auth: {
        signup: (opts) => c.signup(opts),
        login: (email, password) => c.login(email, password),
        logout: (jwt) => c.logout(jwt),
        me: (jwt) => c.getMe(jwt),
      },
      ws: {
        connect: (signal) => c.connect(signal),
        disconnect: () => c.disconnect(),
        subscribe: (topic, cursor, wait, signal) => c.subscribe(topic, cursor, wait, signal),
        unsubscribe: (topic) => c.unsubscribe(topic),
        publish: (topic, payload, opts, signal) => c.publish(topic, payload, opts, signal),
        stream: (topic, cursor, signal) => c.stream(topic, cursor, signal),
        onMessage: (cb) => c.onMessage(cb),
        onError: (cb) => c.onError(cb),
        onSubscribed: (cb) => c.onSubscribed(cb),
        onDisconnected: (cb) => c.onDisconnected(cb),
        onReconnect: (cb) => c.onReconnect(cb),
      },
    };
    this._facades = facades;
    return facades;
  }
}

export interface EventsFacade {
  record: (
    channelId: string,
    eventType: string,
    payload: JsonValue,
    opts?: RecordOptions,
  ) => Promise<Event>;
  replay: (channelId: string, opts?: ReplayOptions) => Promise<Event[]>;
  query: (opts?: QueryOptions) => Promise<Event[]>;
  transition: (
    channelId: string,
    eventType: string,
    payload: JsonValue,
    state: JsonObject,
    opts?: TransitionOptions,
  ) => Promise<TransitionResult>;
  getCursor: (channelId: string) => Promise<number | undefined>;
  latestCursor: (channelId: string) => Promise<number | undefined>;
}

export interface StateFacade {
  saveState: (
    channelId: string,
    cursor: number,
    state: JsonObject,
    opts?: SaveStateOptions,
  ) => Promise<number>;
  latestState: (channelId: string) => Promise<StateSnapshot | undefined>;
  listStates: (channelId: string, opts?: ListStatesOptions) => Promise<StateVersionInfo[]>;
  getState: (channelId: string, version: number) => Promise<StateSnapshot | undefined>;
  deleteState: (channelId: string, version: number) => Promise<void>;
}

export interface ChannelsFacade {
  listChannels: () => Promise<string[]>;
  fork: (source: string, child: string, atCursor: number, opts?: ForkOptions) => Promise<ForkReceipt>;
  getChannelMetadata: (channelId: string) => Promise<ChannelMetadata | undefined>;
  listForks: (channelId: string) => Promise<ChannelMetadata[]>;
  getForkTree: (channelId: string) => Promise<ForkInfo>;
  updateMetadata: (channelId: string, opts: UpdateMetadataOptions) => Promise<ChannelMetadata>;
  diffStates: (left: string, right: string) => Promise<StateDiff>;
  decisionTrail: (channelId: string) => Promise<DecisionTrail>;
}

export interface ExecutionsFacade {
  claim: (
    channelId: string,
    keyName: string,
    toolName: string,
    params?: JsonValue,
    opts?: ClaimExecutionOptions,
  ) => Promise<ExecutionClaim>;
  complete: (id: string, claimToken?: string, result?: JsonValue) => Promise<ExecutionInfo>;
  fail: (id: string, message: string, opts?: FailExecutionOptions) => Promise<ExecutionInfo>;
  heartbeat: (id: string, claimToken?: string, leaseSeconds?: number) => Promise<ExecutionInfo>;
  cancel: (id: string, claimToken?: string) => Promise<ExecutionInfo>;
  get: (id: string) => Promise<ExecutionInfo>;
  list: (channelId: string, limit?: number) => Promise<ExecutionInfo[]>;
  delete: (id: string) => Promise<void>;
}

export interface GroupsFacade {
  create: (groupId: string, channelId: string, metadata?: JsonObject) => Promise<GroupInfo>;
  list: (opts?: ListGroupsOptions) => Promise<GroupInfo[]>;
  delete: (groupId: string) => Promise<void>;
  join: (groupId: string, consumerId: string, leaseSeconds?: number) => Promise<GroupOffset>;
  claimWork: (groupId: string, consumerId: string, limit?: number) => Promise<ClaimedWork>;
  ackWork: (groupId: string, consumerId: string, cursor: number) => Promise<void>;
  heartbeat: (groupId: string, consumerId: string, leaseSeconds?: number) => Promise<void>;
  offsets: (groupId: string) => Promise<GroupOffset[]>;
}

export interface WakeupsFacade {
  schedule: (channelId: string, runAt: string, payload?: JsonObject) => Promise<Wakeup>;
  list: (opts?: ListWakeupsOptions) => Promise<Wakeup[]>;
  get: (id: string) => Promise<Wakeup | undefined>;
  cancel: (id: string) => Promise<boolean>;
}

export interface HealthFacade {
  check: () => Promise<HealthStatus>;
  readiness: () => Promise<ReadinessResult>;
  metricsText: () => Promise<string>;
  metricsJson: () => Promise<MetricsSnapshot>;
}

export interface AuthFacade {
  signup: (opts: SignupOptions) => Promise<AuthResult>;
  login: (email: string, password: string) => Promise<AuthResult>;
  logout: (jwtToken: string) => Promise<void>;
  me: (jwtToken: string) => Promise<UserInfo>;
}

export interface WsFacade {
  connect: (signal?: AbortSignal) => Promise<void>;
  disconnect: () => void;
  subscribe: (
    topic: string,
    cursor?: number,
    wait?: boolean,
    signal?: AbortSignal,
  ) => Promise<void>;
  unsubscribe: (topic: string) => Promise<void>;
  publish: (
    topic: string,
    payload: JsonValue,
    opts?: WSPublishOptions,
    signal?: AbortSignal,
  ) => Promise<Event>;
  stream: (topic: string, cursor?: number, signal?: AbortSignal) => EventStream;
  onMessage: (cb: MessageHandler) => void;
  onError: (cb: ErrorHandler) => void;
  onSubscribed: (cb: SubscribedHandler) => void;
  onDisconnected: (cb: DisconnectedHandler) => void;
  onReconnect: (cb: ReconnectHandler) => void;
}

interface Facades {
  events: EventsFacade;
  state: StateFacade;
  channels: ChannelsFacade;
  executions: ExecutionsFacade;
  groups: GroupsFacade;
  wakeups: WakeupsFacade;
  health: HealthFacade;
  auth: AuthFacade;
  ws: WsFacade;
}

// ---------------------------------------------------------------------------
// Convenience constructors & helpers
// ---------------------------------------------------------------------------

/** Builds a client from the standard Actae environment variables. When an env
 * var is set it overrides the corresponding ClientOptions field:
 *
 *   ACTAE_URL      — HTTP base URL (e.g. "http://localhost:8002")
 *   ACTAE_WS_URL   — WebSocket URL (optional; derived from ACTAE_URL)
 *   ACTAE_API_KEY  — API key (required)
 *
 * Throws when ACTAE_API_KEY is missing and no opts.apiKey was given. */
export function newClientFromEnv(opts: ClientOptions = {} as ClientOptions): ActaeClient {
  const env = (key: string): string | undefined => process.env[key];
  const merged: ClientOptions = { ...opts };
  if (env('ACTAE_API_KEY')) merged.apiKey = env('ACTAE_API_KEY')!;
  if (env('ACTAE_URL')) merged.endpoint = env('ACTAE_URL')!;
  if (env('ACTAE_WS_URL')) merged.wsEndpoint = env('ACTAE_WS_URL')!;
  if (!merged.apiKey) {
    throw new TypeError(
      'api_key is required (set ACTAE_API_KEY or pass ClientOptions.apiKey)',
    );
  }
  return new ActaeClient(merged);
}

/** Helper for providing explicit values where a field is optional. Provided
 * for Go-SDK parity (`actae.Ptr`); the TS idiom is to pass values directly. */
export function ptr<T>(v: T): T {
  return v;
}
