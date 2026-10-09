/**
 * Data types returned by the Actae API. Every type has a `fromResponse`
 * factory (mirroring the Python SDK's `from_response` classmethods and the
 * Go SDK's `*FromResponse` funcs). Optional fields use `undefined` (TS
 * idiom) where the Python SDK uses `None` / the Go SDK uses pointers.
 */

import {
  asBool,
  asInt64,
  asFloat,
  asList,
  asMap,
  asString,
  asStringList,
  type JsonObject,
  type JsonValue,
  unwrapSonic,
} from './json.js';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function optString(m: JsonObject, key: string): string | undefined {
  const v = asString(m[key]);
  return v === '' ? undefined : v;
}

function optInt(m: JsonObject, key: string): number | undefined {
  const raw = m[key];
  if (raw === undefined || raw === null) return undefined;
  return asInt64(raw);
}

function optFloat(m: JsonObject, key: string): number | undefined {
  const raw = m[key];
  if (raw === undefined || raw === null) return undefined;
  return asFloat(raw);
}

/** Value of key as a JsonObject with sonic-rs markers unwrapped. */
function sonicMap(m: JsonObject, key: string): JsonObject | undefined {
  const raw = asMap(m[key]);
  if (!raw) return undefined;
  return asMap(unwrapSonic(raw));
}

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

/** A single event persisted in an Actae channel. */
export interface Event {
  /** Server-assigned event UUID. */
  id: string;
  /** The channel this event belongs to (a channel = a WebSocket topic). */
  channelId: string;
  /** User-defined type label (e.g. "agent.step"). */
  eventType: string;
  /** Event data. JSON numbers arrive as number (integral-safe) or bigint. */
  payload: JsonValue;
  /** Who/what created the event. */
  actor: string;
  /** Gapless per-channel event index (always >= 1, never skips). */
  cursor: number;
  /** Compatibility alias for cursor (server emits both names). */
  channelCursor?: number;
  /** Server-assigned RFC 3339 timestamp. */
  timestamp: string;
  /** Agent identifier (replay/query/broadcast paths only). */
  agentId?: string;
  /** User identifier (replay/query/broadcast paths only). */
  userId?: string;
  /** Metadata dict (replay/query/broadcast paths only). */
  metadata?: JsonObject;
  /** ID of the parent event this event causally depends on. */
  dependsOn?: string;
  /** Per-subscriber delivery tracking ID (WebSocket broadcast only). */
  deliveryId?: string;
}

export function eventFromRecord(m: JsonObject): Event {
  return {
    id: asString(m['id']),
    channelId: asString(m['channel_id']),
    eventType: asString(m['type']),
    payload: unwrapSonic(m['payload']),
    actor: asString(m['actor']),
    cursor: asInt64(m['cursor']),
    channelCursor: optInt(m, 'channel_cursor'),
    timestamp: asString(m['timestamp']),
    dependsOn: optString(m, 'depends_on'),
  };
}

export function eventFromReplay(m: JsonObject): Event {
  const e = eventFromRecord(m);
  e.agentId = optString(m, 'agent_id');
  e.metadata = asMap(m['metadata']);
  return e;
}

export function eventFromQuery(m: JsonObject): Event {
  const e = eventFromRecord(m);
  e.agentId = optString(m, 'agent_id');
  return e;
}

export function eventFromBroadcast(m: JsonObject): Event {
  let typ = asString(m['event_type']);
  if (typ === '') typ = asString(m['type']);
  if (typ === '') typ = 'broadcast';
  return {
    id: asString(m['id']),
    channelId: asString(m['channel_id']),
    eventType: typ,
    payload: unwrapSonic(m['payload']),
    actor: asString(m['actor']),
    cursor: asInt64(m['cursor']),
    channelCursor: optInt(m, 'channel_cursor'),
    timestamp: asString(m['timestamp']),
    metadata: asMap(m['metadata']),
    dependsOn: optString(m, 'depends_on'),
    deliveryId: optString(m, 'delivery_id'),
  };
}

/** Durable distributed-agent coordination group (not a consumer group). */
export interface ExecutionGroup { groupId: string; status: string; metadata: JsonObject; createdAt: string; updatedAt: string; }
export interface ExecutionGroupMember { groupId: string; memberId: string; channelId: string; role?: string; metadata: JsonObject; ownerId?: string; generation: number; leaseUntil?: string; }
export interface MemberLease { groupId: string; memberId: string; ownerId: string; generation: number; leaseUntil: string; }
export interface GroupMessage { messageId: string; groupId: string; fromMemberId: string; toMemberId: string; messageType: string; payload: JsonValue; causalContext?: JsonObject; sourceEventId: string; deliveryEventId: string; status: string; acknowledgedAt?: string; }
export function executionGroupFrom(m: JsonObject): ExecutionGroup { return { groupId: asString(m['group_id']), status: asString(m['status']) || 'active', metadata: asMap(m['metadata']) ?? {}, createdAt: asString(m['created_at']), updatedAt: asString(m['updated_at']) }; }
export function executionGroupMemberFrom(m: JsonObject): ExecutionGroupMember { return { groupId: asString(m['group_id']), memberId: asString(m['member_id']), channelId: asString(m['channel_id']), role: optString(m,'role'), metadata: asMap(m['metadata']) ?? {}, ownerId: optString(m,'owner_id'), generation: asInt64(m['generation']), leaseUntil: optString(m,'lease_until') }; }
export function memberLeaseFrom(m: JsonObject): MemberLease { return { groupId:asString(m['group_id']),memberId:asString(m['member_id']),ownerId:asString(m['owner_id']),generation:asInt64(m['generation']),leaseUntil:asString(m['lease_until']) }; }
export function groupMessageFrom(m: JsonObject): GroupMessage { return { messageId:asString(m['message_id']),groupId:asString(m['group_id']),fromMemberId:asString(m['from_member_id']),toMemberId:asString(m['to_member_id']),messageType:asString(m['type']),payload:unwrapSonic(m['payload']),causalContext:asMap(m['causal_context']),sourceEventId:asString(m['source_event_id']),deliveryEventId:asString(m['delivery_event_id']),status:asString(m['status']),acknowledgedAt:optString(m,'acknowledged_at') }; }

// ---------------------------------------------------------------------------
// Health / readiness / metrics
// ---------------------------------------------------------------------------

export interface HealthComponent {
  status: string;
  details: JsonObject;
}

/** Result of GET /healthz. */
export interface HealthStatus {
  status: string;
  timestamp: string;
  instanceId: string;
  components: Record<string, HealthComponent>;
  checkDurationMs: number;
}

export function healthStatusFromResponse(m: JsonObject): HealthStatus {
  const components: Record<string, HealthComponent> = {};
  const raw = asMap(m['components']);
  if (raw) {
    for (const [name, v] of Object.entries(raw)) {
      const comp = asMap(v);
      if (!comp) continue;
      const details: JsonObject = {};
      for (const [k, val] of Object.entries(comp)) {
        if (k !== 'status') details[k] = val;
      }
      components[name] = { status: asString(comp['status']), details };
    }
  }
  return {
    status: asString(m['status']),
    timestamp: asString(m['timestamp']),
    instanceId: asString(m['instance_id']),
    components,
    checkDurationMs: asInt64(m['check_duration_ms']),
  };
}

/** Result of GET /readyz. */
export interface ReadinessResult {
  status: string;
  timestamp: string;
  instanceId: string;
  checkDurationMs: number;
  databaseReady: boolean;
  capacityAvailable: boolean;
  uptimeSeconds: number;
  connectionUtilization: number;
  activeConnections: number;
  maxConnections: number;
}

export function readinessResultFromResponse(m: JsonObject): ReadinessResult {
  const checks = asMap(m['readiness_checks']) ?? {};
  return {
    status: asString(m['status']),
    timestamp: asString(m['timestamp']),
    instanceId: asString(m['instance_id']),
    checkDurationMs: asInt64(m['check_duration_ms']),
    databaseReady: asBool(checks['database_ready']),
    capacityAvailable: asBool(checks['capacity_available']),
    uptimeSeconds: asInt64(checks['uptime_seconds']),
    connectionUtilization: asFloat(checks['connection_utilization']),
    activeConnections: asInt64(checks['active_connections']),
    maxConnections: asInt64(checks['max_connections']),
  };
}

/** Result of GET /metrics.json. */
export interface MetricsSnapshot {
  status: string;
  timestamp: string;
  uptimeSeconds: number;
  websocketConnections: number;
  topicCount: number;
  messagesSent: number;
  messagesReceived: number;
  totalMessages: number;
  backPressure: JsonObject;
}

export function metricsSnapshotFromResponse(m: JsonObject): MetricsSnapshot {
  const ws = asMap(m['websocket']) ?? {};
  const bp = asMap(ws['back_pressure']) ?? {};
  return {
    status: asString(m['status']),
    timestamp: asString(m['timestamp']),
    uptimeSeconds: asInt64(m['uptime_seconds']),
    websocketConnections: asInt64(ws['connections']),
    topicCount: asInt64(ws['topics']),
    messagesSent: asInt64(ws['messages_sent']),
    messagesReceived: asInt64(ws['messages_received']),
    totalMessages: asInt64(ws['total_messages']),
    backPressure: bp,
  };
}

// ---------------------------------------------------------------------------
// Auth
// ---------------------------------------------------------------------------

export interface UserInfo {
  id: string;
  email: string;
  emailVerified: boolean;
  name?: string;
  image?: string;
  createdAt?: string;
}

export function userInfoFromDict(m: JsonObject): UserInfo {
  return {
    id: asString(m['id']),
    email: asString(m['email']),
    emailVerified: asBool(m['email_verified']),
    name: optString(m, 'name'),
    image: optString(m, 'image'),
    createdAt: optString(m, 'created_at'),
  };
}

export interface AuthResult {
  user: UserInfo;
  token: string;
}

export function authResultFromResponse(m: JsonObject): AuthResult {
  return {
    user: userInfoFromDict(asMap(m['user']) ?? {}),
    token: asString(m['token']),
  };
}

// ---------------------------------------------------------------------------
// Channels / forks
// ---------------------------------------------------------------------------

/** Metadata for a channel or fork in the Actae execution tree. */
export interface ChannelMetadata {
  channelId: string;
  parentChannelId?: string;
  originRunId: string;
  /** RESOLVED fork boundary: the snapshot cursor the child inherited. */
  forkedAtCursor: number;
  forkedAt: string;
  displayName?: string;
  reason?: string;
  experimentMetadata?: JsonObject;
  /** Boundary the caller asked for (0 = latest). */
  requestedAtCursor: number;
  /** Snapshot cursor the child actually inherited. */
  resolvedStateCursor: number;
  /** Alias of resolvedStateCursor. */
  resolvedCursor: number;
  /** Parent boundary event the fork.started depends on. */
  resolvedEventId?: string;
  /** Source snapshot version copied (0 = none). */
  sourceStateVersion: number;
  /** SHA-256 fingerprint of the copied state. */
  sourceStateSha256?: string;
  /** True when a state snapshot was copied. */
  restorable: boolean;
  /** Immutable fork manifest supplied at fork time. */
  manifest?: JsonObject;
  /** Grade: state_exact | context_exact | execution_replayable | best_effort. */
  reproducibility?: string;
  /** Side-effect policy map for a forked channel (undefined = auto). */
  toolPolicies?: JsonObject;
  /** Outcome: promoted | rejected | inconclusive | crashed. */
  outcome?: string;
  /** Result score recorded for ranking (higher is better). */
  resultScore?: number;
  /** Soft-deleted channel marker. */
  deletedAt?: string;
}

export function channelMetadataFromDict(m: JsonObject): ChannelMetadata {
  const resolvedStateCursor = asInt64(m['resolved_state_cursor']);
  return {
    channelId: asString(m['channel_id']),
    parentChannelId: optString(m, 'parent_channel_id'),
    originRunId: asString(m['origin_run_id']),
    forkedAtCursor: asInt64(m['forked_at_cursor']),
    forkedAt: asString(m['forked_at']),
    displayName: optString(m, 'display_name'),
    reason: optString(m, 'reason'),
    experimentMetadata: sonicMap(m, 'experiment_metadata'),
    requestedAtCursor: asInt64(m['requested_at_cursor']),
    resolvedStateCursor,
    resolvedCursor: asInt64(m['resolved_cursor']) || resolvedStateCursor,
    resolvedEventId: optString(m, 'resolved_event_id'),
    sourceStateVersion: asInt64(m['source_state_version']),
    sourceStateSha256: optString(m, 'source_state_sha256'),
    restorable: asBool(m['restorable']),
    manifest: sonicMap(m, 'manifest'),
    reproducibility: optString(m, 'reproducibility'),
    toolPolicies: sonicMap(m, 'tool_policies'),
    outcome: optString(m, 'outcome'),
    resultScore: optFloat(m, 'result_score'),
    deletedAt: optString(m, 'deleted_at'),
  };
}

/** The immutable fork receipt returned at creation and queryable via
 * GetForkReceipt — the provenance record for the dashboard and audit. */
export interface ForkReceipt {
  forkId: string;
  sourceChannelId?: string;
  childChannelId: string;
  requestedCursor: number;
  resolvedCursor: number;
  resolvedEventId?: string;
  sourceStateVersion: number;
  sourceStateSha256?: string;
  restorable: boolean;
  replayed: boolean;
  manifest?: JsonObject;
  reproducibility?: string;
  /** Child channel's side-effect policy map (undefined = server default auto). */
  toolPolicies?: JsonObject;
}

export function forkReceiptFromDict(m: JsonObject): ForkReceipt {
  let forkId = asString(m['fork_id']);
  if (forkId === '') forkId = asString(m['channel_id']);
  let childId = asString(m['child_channel_id']);
  if (childId === '') childId = asString(m['channel_id']);
  const requested =
    asInt64(m['requested_cursor']) || asInt64(m['requested_at_cursor']);
  const resolved =
    asInt64(m['resolved_cursor']) || asInt64(m['resolved_state_cursor']);
  return {
    forkId,
    sourceChannelId: optString(m, 'source_channel_id'),
    childChannelId: childId,
    requestedCursor: requested,
    resolvedCursor: resolved,
    resolvedEventId: optString(m, 'resolved_event_id'),
    sourceStateVersion: asInt64(m['source_state_version']),
    sourceStateSha256: optString(m, 'source_state_sha256'),
    restorable: asBool(m['restorable']),
    replayed: asBool(m['replayed']),
    manifest: sonicMap(m, 'manifest'),
    reproducibility: optString(m, 'reproducibility'),
    toolPolicies: sonicMap(m, 'tool_policies'),
  };
}

/** Recursive execution-tree node returned by GetForkTree. */
export interface ForkInfo {
  channelId: string;
  displayName?: string;
  reason?: string;
  forkedAtCursor: number;
  eventCount: number;
  latestCursor?: number;
  children: ForkInfo[];
}

export function forkInfoFromDict(m: JsonObject): ForkInfo {
  const children: ForkInfo[] = [];
  for (const raw of asList(m['children'])) {
    const child = asMap(raw);
    if (child) children.push(forkInfoFromDict(child));
  }
  return {
    channelId: asString(m['channel_id']),
    displayName: optString(m, 'display_name'),
    reason: optString(m, 'reason'),
    forkedAtCursor: asInt64(m['forked_at_cursor']),
    eventCount: asInt64(m['event_count']),
    latestCursor: optInt(m, 'latest_cursor'),
    children,
  };
}

/** Result of an atomic event+state transition. */
export interface TransitionResult {
  event: Event;
  stateVersion: number;
}

/** Result of resolving a step number against the server-owned step index. */
export interface StepResolution {
  channelId: string;
  stepNumber: number;
  cursor: number;
  eventId?: string;
}

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

/** Cursor-aligned versioned state snapshot returned by LatestState / GetState. */
export interface StateSnapshot {
  /** Channel cursor the snapshot is aligned to. */
  cursor: number;
  /** Immutable version number. The latest-state response does not carry it
   * (0); versioned lookups do. */
  version: number;
  /** Snapshot time (RFC 3339); empty on LatestState. */
  timestamp: string;
  /** The state dict. */
  state: JsonObject;
}

/** Version-history metadata for a channel (no state blob), from ListStates. */
export interface StateVersionInfo {
  version: number;
  cursor: number;
  timestamp: string;
}

export function stateSnapshotFrom(m: JsonObject): StateSnapshot | undefined {
  if (m['cursor'] === undefined || m['cursor'] === null) return undefined;
  const state = asMap(unwrapSonic(m['state'])) ?? {};
  return {
    cursor: asInt64(m['cursor']),
    version: asInt64(m['version']),
    timestamp: asString(m['timestamp']),
    state,
  };
}

// ---------------------------------------------------------------------------
// Diff / decision trail
// ---------------------------------------------------------------------------

/** Point-in-time state snapshot referenced by a diff or decision trail. */
export interface StatePoint {
  channelId: string;
  cursor: number;
  state: JsonObject;
}

/** One structural difference between two fork states at a JSON path. */
export interface StateDiffEntry {
  path: string[];
  kind: string;
  left?: JsonValue;
  right?: JsonValue;
}

/** Structural diff of two channels' latest saved states. */
export interface StateDiff {
  leftChannelId: string;
  rightChannelId: string;
  left: StatePoint;
  right: StatePoint;
  common?: StatePoint;
  leftDivergedAtCursor?: number;
  rightDivergedAtCursor?: number;
  entries: StateDiffEntry[];
  truncated: boolean;
  entryCountTotal: number;
  maxEntries: number;
}

export interface LineageHop {
  channelId: string;
  parentChannelId?: string;
  forkedAtCursor: number;
  displayName?: string;
  reason?: string;
}

export interface ExecutionLedgerEntry {
  id: string;
  channelId: string;
  keyName: string;
  toolName: string;
  status: string;
  params?: JsonValue;
  result?: JsonValue;
  error?: JsonValue;
  startedCursor?: number;
  completedCursor?: number;
}

/** Full lineage-as-audit view for a channel. */
export interface DecisionTrail {
  channelId: string;
  originRunId: string;
  ancestry: LineageHop[];
  boundary?: StatePoint;
  executions: ExecutionLedgerEntry[];
}

export function statePointFrom(m?: JsonObject): StatePoint | undefined {
  if (!m) return undefined;
  return {
    channelId: asString(m['channel_id']),
    cursor: asInt64(m['cursor']),
    state: asMap(unwrapSonic(m['state'])) ?? {},
  };
}

export function stateDiffEntryFrom(m: JsonObject): StateDiffEntry {
  return {
    path: asStringList(m['path']) ?? [],
    kind: asString(m['kind']),
    left: m['left'] as JsonValue | undefined,
    right: m['right'] as JsonValue | undefined,
  };
}

export function stateDiffFrom(m: JsonObject): StateDiff {
  const left = statePointFrom(asMap(m['left'])) ?? {
    channelId: '',
    cursor: 0,
    state: {},
  };
  const right = statePointFrom(asMap(m['right'])) ?? {
    channelId: '',
    cursor: 0,
    state: {},
  };
  const entries: StateDiffEntry[] = [];
  for (const raw of asList(m['entries'])) {
    const e = asMap(raw);
    if (e) entries.push(stateDiffEntryFrom(e));
  }
  return {
    leftChannelId: asString(m['left_channel_id']),
    rightChannelId: asString(m['right_channel_id']),
    left,
    right,
    common: statePointFrom(asMap(m['common'])),
    leftDivergedAtCursor: optInt(m, 'left_diverged_at_cursor'),
    rightDivergedAtCursor: optInt(m, 'right_diverged_at_cursor'),
    entries,
    truncated: asBool(m['truncated']),
    entryCountTotal: asInt64(m['entry_count_total']),
    maxEntries: asInt64(m['max_entries']) || 500,
  };
}

export function lineageHopFrom(m: JsonObject): LineageHop {
  return {
    channelId: asString(m['channel_id']),
    parentChannelId: optString(m, 'parent_channel_id'),
    forkedAtCursor: asInt64(m['forked_at_cursor']),
    displayName: optString(m, 'display_name'),
    reason: optString(m, 'reason'),
  };
}

export function executionLedgerEntryFrom(m: JsonObject): ExecutionLedgerEntry {
  return {
    id: asString(m['id']),
    channelId: asString(m['channel_id']),
    keyName: asString(m['key_name']),
    toolName: asString(m['tool_name']),
    status: asString(m['status']),
    params: m['params'] as JsonValue | undefined,
    result: m['result'] as JsonValue | undefined,
    error: m['error'] as JsonValue | undefined,
    startedCursor: optInt(m, 'started_cursor'),
    completedCursor: optInt(m, 'completed_cursor'),
  };
}

export function decisionTrailFrom(m: JsonObject): DecisionTrail {
  const ancestry: LineageHop[] = [];
  for (const raw of asList(m['ancestry'])) {
    const h = asMap(raw);
    if (h) ancestry.push(lineageHopFrom(h));
  }
  const executions: ExecutionLedgerEntry[] = [];
  for (const raw of asList(m['executions'])) {
    const e = asMap(raw);
    if (e) executions.push(executionLedgerEntryFrom(e));
  }
  return {
    channelId: asString(m['channel_id']),
    originRunId: asString(m['origin_run_id']),
    ancestry,
    boundary: statePointFrom(asMap(m['boundary'])),
    executions,
  };
}

// ---------------------------------------------------------------------------
// Consumer groups
// ---------------------------------------------------------------------------

export interface GroupInfo {
  groupId: string;
  channelId: string;
  createdAt: string;
  metadata?: JsonObject;
}

export function groupInfoFromResponse(m: JsonObject): GroupInfo {
  return {
    groupId: asString(m['group_id']),
    channelId: asString(m['channel_id']),
    createdAt: asString(m['created_at']),
    metadata: sonicMap(m, 'metadata'),
  };
}

export interface GroupOffset {
  groupId: string;
  consumerId: string;
  lastCursor: number;
  claimedCursor: number;
  updatedAt: string;
}

export function groupOffsetFromResponse(m: JsonObject): GroupOffset {
  return {
    groupId: asString(m['group_id']),
    consumerId: asString(m['consumer_id']),
    lastCursor: asInt64(m['last_cursor']),
    claimedCursor: asInt64(m['claimed_cursor']),
    updatedAt: asString(m['updated_at']),
  };
}

/** A work batch claimed by a consumer. */
export interface ClaimedWork {
  groupId: string;
  consumerId: string;
  events: Event[];
  leaseUntil: string;
}

export function claimedWorkFromResponse(m: JsonObject): ClaimedWork {
  const events: Event[] = [];
  for (const raw of asList(m['events'])) {
    const ev = asMap(raw);
    if (ev) events.push(eventFromReplay(ev));
  }
  return {
    groupId: asString(m['group_id']),
    consumerId: asString(m['consumer_id']),
    events,
    leaseUntil: asString(m['lease_until']),
  };
}

// ---------------------------------------------------------------------------
// Wake-ups
// ---------------------------------------------------------------------------

/** A persisted scheduled wake-up. */
export interface Wakeup {
  id: string;
  channelId: string;
  runAt: string;
  status: string;
  payload?: JsonObject;
  createdAt: string;
  executedAt?: string;
  attempts: number;
  error?: string;
}

export function wakeupFromResponse(m: JsonObject): Wakeup {
  return {
    id: asString(m['id']),
    channelId: asString(m['channel_id']),
    runAt: asString(m['run_at']),
    status: asString(m['status']),
    payload: sonicMap(m, 'payload'),
    createdAt: asString(m['created_at']),
    executedAt: optString(m, 'executed_at'),
    attempts: asInt64(m['attempts']),
    error: optString(m, 'error'),
  };
}

// ---------------------------------------------------------------------------
// Idempotent tool executions
// ---------------------------------------------------------------------------

/** A single idempotent tool execution ledger record. Status is one of
 * running | completed | failed | interrupted | cancelled | expired. */
export interface ExecutionInfo {
  id: string;
  channelId: string;
  keyName: string;
  toolName: string;
  status: string;
  attempts: number;
  leaseUntil?: string;
  startedCursor?: number;
  completedCursor?: number;
  startedEventId?: string;
  completedEventId?: string;
  replayEmitted: boolean;
  params?: JsonObject;
  result?: JsonValue;
  error?: JsonValue;
  createdAt: string;
  updatedAt: string;
  completedAt?: string;
}

export function executionInfoFromResponse(m: JsonObject): ExecutionInfo {
  return {
    id: asString(m['id']),
    channelId: asString(m['channel_id']),
    keyName: asString(m['key_name']),
    toolName: asString(m['tool_name']),
    status: asString(m['status']),
    attempts: asInt64(m['attempts']),
    leaseUntil: optString(m, 'lease_until'),
    startedCursor: optInt(m, 'started_cursor'),
    completedCursor: optInt(m, 'completed_cursor'),
    startedEventId: optString(m, 'started_event_id'),
    completedEventId: optString(m, 'completed_event_id'),
    replayEmitted: asBool(m['replay_emitted']),
    params: sonicMap(m, 'params'),
    result: unwrapSonic(m['result']),
    error: unwrapSonic(m['error']),
    createdAt: asString(m['created_at']),
    updatedAt: asString(m['updated_at']),
    completedAt: optString(m, 'completed_at'),
  };
}

/** Outcome of claiming an idempotent tool execution. Status is one of
 * claimed | replayed | reclaimed | in_progress. */
export interface ExecutionClaim {
  status: string;
  execution: ExecutionInfo;
  result?: JsonValue;
  claimToken?: string;
}

export function executionClaimFromResponse(m: JsonObject): ExecutionClaim {
  return {
    status: asString(m['status']),
    execution: executionInfoFromResponse(asMap(m['execution']) ?? {}),
    result: unwrapSonic(m['result']),
    claimToken: optString(m, 'claim_token'),
  };
}

