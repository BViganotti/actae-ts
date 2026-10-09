/**
 * @actae/sdk — TypeScript SDK for Actae, a real-time event store for agent
 * workflows. Feature-parity port of the Python SDK (actae-client) and the Go
 * SDK. Node 20+ ESM-only.
 *
 * ```ts
 * import { ActaeClient } from '@actae/sdk';
 *
 * const client = new ActaeClient({ apiKey: 'sk-...', endpoint: 'http://localhost:8002' });
 * const event = await client.record('my-channel', 'agent.step', { input: 'hello' });
 * ```
 */

export {
  ActaeClient,
  APPROVAL_REQUESTED,
  APPROVAL_DECIDED,
  APPROVAL_EXPIRED,
  newClientFromEnv,
  ptr,
  validateChannelId,
} from './client.js';
export type {
  AuthFacade,
  ChannelsFacade,
  EventsFacade,
  ExecutionsFacade,
  GroupsFacade,
  HealthFacade,
  StateFacade,
  WakeupsFacade,
  WsFacade,
} from './client.js';
export type {
  ClientOptions,
  ClaimExecutionOptions,
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
export type {
  MessageHandler,
  ErrorHandler,
  SubscribedHandler,
  DisconnectedHandler,
  ReconnectHandler,
  EventStream,
  PublishOptions,
} from './client_ws.js';

export {
  ActaeError,
  AuthError,
  LockError,
  ConnectionError,
  RateLimitError,
  APIError,
  NotFoundError,
  ServerError,
  SnapshotBoundaryError,
  VersionConflictError,
  NoRestorableCheckpointError,
  IdempotencyConflictError,
  ChannelConflictError,
  CounterfactualBlockedError,
  ForkToolBlockedError,
  ConsumerError,
  IdempotencyKeyMismatchError,
  ExecutionNotOwnedError,
  ExecutionNotFoundError,
  SessionError,
  SessionCompletedError,
  WakeupAlreadyFiredError,
} from './errors.js';

export {
  AgentSession,
  SESSION_STARTED,
  SESSION_COMPLETED,
  SessionStatus,
  type AgentSessionOptions,
  type ForkSessionOptions,
  type ResumeOptions,
  type StepOptions,
  type StateFn,
  type SessionTransport,
} from './session.js';
export { StateManager } from './state_manager.js';
export { GroupSession, GroupMemberSession } from './groups.js';

export {
  SONIC_NUMBER_KEY,
  parseJson,
  unwrapSonic,
  asInt64,
  asBigInt,
  asFloat,
  asString,
  asMap,
  asList,
  asBool,
  asStringList,
  type JsonObject,
  type JsonValue,
  type JsonPrimitive,
} from './json.js';
export { newUUID } from './uuid.js';
export {
  deterministicOperationKey,
  canonicalStepContent,
} from './deterministic.js';
export {
  Actae,
  ActaeScope,
  ClaudeProvider,
  CodexProvider,
  CopilotProvider,
  FrameworkProviders,
  LangChainProvider,
  LangGraphProvider,
  OpenAIProvider,
  OrchestratorProvider,
  RUN_CARRIER_SCHEMA,
  channelForRun,
  parseCarrier,
} from './runtime.js';
export type {
  ActaeCarrier,
  EffectOptions,
  LifecycleErrorMode,
  ObserveOptions,
  RunOptions,
  ToolOptions,
  WorkflowOptions,
} from './runtime.js';
export { SDK_VERSION } from './version.js';
export { FleetClient, FleetSessionTransport, requireComplete, type FleetClientOptions, type FleetRequestOptions, type InstanceError, type Page, type PartialResult } from './fleet.js';
export { fleetStream, type BrowserFleetStreamOptions } from './browser.js';
export { MANIFEST_OPERATIONS, MANIFEST_BY_ID, manifestOperation, type ManifestOperation } from './manifest.js';

export {
  eventFromRecord,
  eventFromReplay,
  eventFromQuery,
  eventFromBroadcast,
  channelMetadataFromDict,
  forkReceiptFromDict,
  forkInfoFromDict,
  healthStatusFromResponse,
  readinessResultFromResponse,
  metricsSnapshotFromResponse,
  userInfoFromDict,
  authResultFromResponse,
  stateSnapshotFrom,
  statePointFrom,
  stateDiffFrom,
  stateDiffEntryFrom,
  lineageHopFrom,
  executionLedgerEntryFrom,
  decisionTrailFrom,
  groupInfoFromResponse,
  groupOffsetFromResponse,
  claimedWorkFromResponse,
  wakeupFromResponse,
  executionInfoFromResponse,
  executionClaimFromResponse,
  executionGroupFrom,
  executionGroupMemberFrom,
  memberLeaseFrom,
  groupMessageFrom,
} from './types.js';
export type {
  Event,
  HealthComponent,
  HealthStatus,
  ReadinessResult,
  MetricsSnapshot,
  UserInfo,
  AuthResult,
  ChannelMetadata,
  ForkReceipt,
  ForkInfo,
  TransitionResult,
  StepResolution,
  StateSnapshot,
  StateVersionInfo,
  StatePoint,
  StateDiffEntry,
  StateDiff,
  LineageHop,
  ExecutionLedgerEntry,
  DecisionTrail,
  GroupInfo,
  GroupOffset,
  ClaimedWork,
  Wakeup,
  ExecutionInfo,
  ExecutionClaim,
  ExecutionGroup,
  ExecutionGroupMember,
  MemberLease,
  GroupMessage,
} from './types.js';
