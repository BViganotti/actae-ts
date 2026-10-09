/** Stable framework-independent contract shared by every Actae adapter. */

import type { ActaeClient } from '../client.js';
import type { JsonObject, JsonValue } from '../json.js';

export const CHECKPOINT_SCHEMA = 'actae.framework-checkpoint/v1' as const;
export const CHECKPOINT_METADATA_KEY = '_actae_adapter' as const;

export const ActaeFeature = {
  Events: 'events',
  Replay: 'replay',
  Checkpoints: 'checkpoints',
  Resume: 'resume',
  Forks: 'forks',
  ToolExecutions: 'tool_executions',
  Experiments: 'experiments',
  ExecutionGroups: 'execution_groups',
  Wakeups: 'wakeups',
  CausalLineage: 'causal_lineage',
} as const;
export type ActaeFeature = (typeof ActaeFeature)[keyof typeof ActaeFeature];
export const FULL_FEATURE_SET: ReadonlySet<ActaeFeature> = new Set(Object.values(ActaeFeature));

export type AdapterSupportLevel = 'certified' | 'preview' | 'observability';
export type ResumeFidelity =
  | 'checkpoint_exact'
  | 'session_native'
  | 'reconstructed'
  | 'context_seeded'
  | 'observe_only';

export class AdapterContractError extends Error {
  override readonly name = 'AdapterContractError';
}

export class ToolExecutionInProgressError extends Error {
  override readonly name = 'ToolExecutionInProgressError';

  constructor(
    readonly executionId: string,
    readonly keyName: string,
  ) {
    super(`tool execution ${JSON.stringify(keyName)} is already in progress (execution_id=${executionId})`);
  }
}

export interface AdapterCapabilitiesInput {
  framework: string;
  adapterVersion: string;
  supportLevel: AdapterSupportLevel;
  resumeFidelity: ResumeFidelity;
  features: Iterable<ActaeFeature>;
  nativeCheckpoint: boolean;
  notes?: string;
}

/** Machine-readable, testable support claim for one framework adapter. */
export class AdapterCapabilities {
  readonly framework: string;
  readonly adapterVersion: string;
  readonly supportLevel: AdapterSupportLevel;
  readonly resumeFidelity: ResumeFidelity;
  readonly features: ReadonlySet<ActaeFeature>;
  readonly nativeCheckpoint: boolean;
  readonly notes: string;

  constructor(input: AdapterCapabilitiesInput) {
    this.framework = required(input.framework, 'framework');
    this.adapterVersion = required(input.adapterVersion, 'adapterVersion');
    this.supportLevel = input.supportLevel;
    this.resumeFidelity = input.resumeFidelity;
    this.features = new Set(input.features);
    this.nativeCheckpoint = input.nativeCheckpoint;
    this.notes = input.notes ?? '';
    if (this.features.has(ActaeFeature.Resume) && !this.features.has(ActaeFeature.Checkpoints)) {
      throw new AdapterContractError('resume support requires checkpoint support');
    }
    if (this.features.has(ActaeFeature.Forks) && !this.features.has(ActaeFeature.Checkpoints)) {
      throw new AdapterContractError('fork support requires checkpoint support');
    }
    if (this.nativeCheckpoint && !this.features.has(ActaeFeature.Checkpoints)) {
      throw new AdapterContractError('nativeCheckpoint requires checkpoint support');
    }
  }

  supports(feature: ActaeFeature): boolean {
    return this.features.has(feature);
  }

  get missingFeatures(): ReadonlySet<ActaeFeature> {
    return new Set([...FULL_FEATURE_SET].filter((feature) => !this.features.has(feature)));
  }

  get fullParity(): boolean {
    return this.missingFeatures.size === 0;
  }

  toJSON(): JsonObject {
    return {
      framework: this.framework,
      adapter_version: this.adapterVersion,
      support_level: this.supportLevel,
      resume_fidelity: this.resumeFidelity,
      features: [...this.features].sort(),
      native_checkpoint: this.nativeCheckpoint,
      full_parity: this.fullParity,
      notes: this.notes,
    };
  }
}

export interface FrameworkEventInput {
  kind: string;
  framework: string;
  runId: string;
  payload?: JsonObject;
  parentRunId?: string;
  agentId?: string;
  occurredAt?: string;
}

export function frameworkEvent(input: FrameworkEventInput): JsonObject {
  const value: JsonObject = {
    schema: 'actae.framework-event/v1',
    kind: required(input.kind, 'kind'),
    framework: required(input.framework, 'framework'),
    run_id: required(input.runId, 'runId'),
    parent_run_id: input.parentRunId ?? null,
    agent_id: input.agentId ?? null,
    occurred_at: input.occurredAt ?? new Date().toISOString(),
    data: input.payload ?? {},
  };
  assertJson(value, 'framework event');
  return value;
}

export interface CheckpointEnvelopeInput {
  framework: string;
  adapterVersion: string;
  channelId: string;
  portableState?: JsonObject;
  nativeCheckpoint?: JsonValue;
  applicationState?: JsonObject;
  pendingWork?: JsonObject;
  manifest?: JsonObject;
  eventCursor?: number;
  eventId?: string;
  frameworkVersion?: string;
  createdAt?: string;
  schema?: string;
}

/** Portable Actae state plus an optional framework-native opaque checkpoint. */
export class CheckpointEnvelope {
  readonly framework: string;
  readonly adapterVersion: string;
  readonly channelId: string;
  readonly portableState: JsonObject;
  readonly nativeCheckpoint: JsonValue;
  readonly applicationState: JsonObject;
  readonly pendingWork: JsonObject;
  readonly manifest: JsonObject;
  readonly eventCursor?: number;
  readonly eventId?: string;
  readonly frameworkVersion?: string;
  readonly createdAt: string;
  readonly schema: typeof CHECKPOINT_SCHEMA;

  constructor(input: CheckpointEnvelopeInput) {
    if ((input.schema ?? CHECKPOINT_SCHEMA) !== CHECKPOINT_SCHEMA) {
      throw new AdapterContractError(`unsupported checkpoint schema: ${input.schema ?? ''}`);
    }
    this.schema = CHECKPOINT_SCHEMA;
    this.framework = required(input.framework, 'framework');
    this.adapterVersion = required(input.adapterVersion, 'adapterVersion');
    this.channelId = required(input.channelId, 'channelId');
    this.portableState = input.portableState ?? {};
    this.nativeCheckpoint = input.nativeCheckpoint ?? null;
    this.applicationState = input.applicationState ?? {};
    this.pendingWork = input.pendingWork ?? {};
    this.manifest = input.manifest ?? {};
    if (input.eventCursor !== undefined) {
      if (!Number.isSafeInteger(input.eventCursor) || input.eventCursor < 0) {
        throw new AdapterContractError('eventCursor must be a non-negative safe integer');
      }
      this.eventCursor = input.eventCursor;
    }
    if (input.eventId !== undefined) this.eventId = input.eventId;
    if (input.frameworkVersion !== undefined) this.frameworkVersion = input.frameworkVersion;
    this.createdAt = input.createdAt ?? new Date().toISOString();
    assertJson(this.toJSON(), 'checkpoint envelope');
  }

  toJSON(): JsonObject {
    return {
      schema: this.schema,
      framework: this.framework,
      framework_version: this.frameworkVersion ?? null,
      adapter_version: this.adapterVersion,
      channel_id: this.channelId,
      event_cursor: this.eventCursor ?? null,
      event_id: this.eventId ?? null,
      created_at: this.createdAt,
      portable_state: this.portableState,
      native_checkpoint: this.nativeCheckpoint,
      application_state: this.applicationState,
      pending_work: this.pendingWork,
      manifest: this.manifest,
    };
  }

  static fromJSON(value: JsonObject): CheckpointEnvelope {
    return new CheckpointEnvelope({
      schema: asRequiredString(value['schema'], 'schema'),
      framework: asRequiredString(value['framework'], 'framework'),
      frameworkVersion: asOptionalString(value['framework_version'], 'framework_version'),
      adapterVersion: asRequiredString(value['adapter_version'], 'adapter_version'),
      channelId: asRequiredString(value['channel_id'], 'channel_id'),
      eventCursor: asOptionalNumber(value['event_cursor'], 'event_cursor'),
      eventId: asOptionalString(value['event_id'], 'event_id'),
      createdAt: asRequiredString(value['created_at'], 'created_at'),
      portableState: asObject(value['portable_state'], 'portable_state'),
      nativeCheckpoint: value['native_checkpoint'] ?? null,
      applicationState: asObject(value['application_state'], 'application_state'),
      pendingWork: asObject(value['pending_work'], 'pending_work'),
      manifest: asObject(value['manifest'], 'manifest'),
    });
  }
}

export function embedCheckpointMetadata(
  state: JsonObject,
  envelope: CheckpointEnvelope,
): JsonObject {
  // Native SDK state may legitimately contain object properties whose value
  // is undefined; ActaeClient's wire serializer omits those just like
  // JSON.stringify. Validate the envelope itself, but preserve native state.
  return { ...state, [CHECKPOINT_METADATA_KEY]: envelope.toJSON() };
}

export function extractCheckpointEnvelope(state: JsonObject): CheckpointEnvelope | undefined {
  const raw = state[CHECKPOINT_METADATA_KEY];
  if (raw === undefined || raw === null) return undefined;
  return CheckpointEnvelope.fromJSON(asObject(raw, CHECKPOINT_METADATA_KEY));
}

export function stripCheckpointMetadata(state: JsonObject): JsonObject {
  const result = { ...state };
  delete result[CHECKPOINT_METADATA_KEY];
  return result;
}

export interface AdapterCheckpointStateOptions {
  framework: string;
  channelId: string;
  portableState?: JsonObject;
  nativeCheckpoint?: JsonValue;
  pendingWork?: JsonObject;
  manifest?: JsonObject;
  eventCursor?: number;
  eventId?: string;
  frameworkVersion?: string;
  adapterVersion?: string;
}

/** Attach the shared checkpoint metadata without changing native state keys. */
export function adapterCheckpointState(
  state: JsonObject,
  options: AdapterCheckpointStateOptions,
): JsonObject {
  return embedCheckpointMetadata(state, new CheckpointEnvelope({
    framework: options.framework,
    frameworkVersion: options.frameworkVersion,
    adapterVersion: options.adapterVersion ?? '2',
    channelId: options.channelId,
    portableState: options.portableState,
    nativeCheckpoint: options.nativeCheckpoint,
    pendingWork: options.pendingWork,
    manifest: options.manifest,
    eventCursor: options.eventCursor,
    eventId: options.eventId,
  }));
}

export interface ToolExecutionOptions {
  dedupFields?: string[];
  leaseSeconds?: number;
  heartbeatIntervalMs?: number;
  emitReplayEvent?: boolean;
  signal?: AbortSignal;
}

export type ToolCallable<T extends JsonValue> = (signal: AbortSignal) => T | Promise<T>;

/** Framework-neutral idempotent effect wrapper with automatic lease renewal. */
export class ActaeToolExecutor {
  constructor(
    readonly actae: ActaeClient,
    readonly channelId: string,
    readonly actor = 'framework-tool',
  ) {
    required(channelId, 'channelId');
  }

  async execute<T extends JsonValue>(
    keyName: string,
    toolName: string,
    params: JsonValue,
    invoke: ToolCallable<T>,
    options: ToolExecutionOptions = {},
  ): Promise<T> {
    required(keyName, 'keyName');
    required(toolName, 'toolName');
    const leaseSeconds = options.leaseSeconds ?? 60;
    if (!Number.isInteger(leaseSeconds) || leaseSeconds < 1) {
      throw new TypeError('leaseSeconds must be a positive integer');
    }
    const interval = options.heartbeatIntervalMs ?? Math.max(250, Math.min(leaseSeconds * 1000 / 3, 30_000));
    if (!(interval > 0)) throw new TypeError('heartbeatIntervalMs must be positive');
    const claim = await this.actae.claimExecution(this.channelId, keyName, toolName, params, {
      dedupFields: options.dedupFields,
      leaseSeconds,
      emitReplayEvent: options.emitReplayEvent ?? true,
      actor: this.actor,
    });
    if (claim.status === 'replayed') return claim.result as T;
    if (claim.status === 'in_progress') {
      throw new ToolExecutionInProgressError(claim.execution.id, keyName);
    }
    if (claim.status !== 'claimed' && claim.status !== 'reclaimed') {
      throw new AdapterContractError(`unknown execution claim status: ${claim.status}`);
    }
    if (!claim.claimToken) {
      throw new AdapterContractError('owned execution claim is missing claimToken');
    }

    const controller = new AbortController();
    let rejectExternalAbort: (reason: unknown) => void = () => undefined;
    const externalAbort = new Promise<never>((_resolve, reject) => {
      rejectExternalAbort = reject;
    });
    // Absorb rejections at creation time: the signal may already be aborted
    // when the executor attaches its race handlers (or abort may fire before
    // the race starts), so a member rejecting early must never surface as an
    // unhandled rejection. Matches the Python adapter's asyncio.wait semantics.
    externalAbort.catch(() => undefined);
    const abort = () => {
      controller.abort(options.signal?.reason);
      rejectExternalAbort(abortError(options.signal!));
    };
    options.signal?.addEventListener('abort', abort, { once: true });
    if (options.signal?.aborted) abort();
    let stopped = false;
    let heartbeatError: unknown;
    let rejectHeartbeatFailure: (reason: unknown) => void = () => undefined;
    // A lease-loss must resolve the caller promptly even if a third-party
    // tool ignores AbortSignal. The underlying call cannot be forcibly
    // terminated in JavaScript, but it is no longer allowed to complete the
    // Actae execution after ownership has been lost.
    const heartbeatFailure = new Promise<never>((_resolve, reject) => {
      rejectHeartbeatFailure = reject;
    });
    heartbeatFailure.catch(() => undefined);
    let wakeHeartbeat: (() => void) | undefined;
    const heartbeat = (async () => {
      while (!stopped) {
        await new Promise<void>((resolve) => {
          const timer = setTimeout(resolve, interval);
          wakeHeartbeat = () => {
            clearTimeout(timer);
            resolve();
          };
        });
        wakeHeartbeat = undefined;
        if (stopped) return;
        try {
          await this.actae.heartbeatExecution(claim.execution.id, claim.claimToken, leaseSeconds);
        } catch (error) {
          heartbeatError = error;
          controller.abort(error);
          rejectHeartbeatFailure(error);
          return;
        }
      }
    })();

    try {
      if (controller.signal.aborted) throw abortError(controller.signal);
      // Attach a no-op catch to the invocation so a losing invocation
      // rejection is absorbed rather than surfacing as an unhandled rejection
      // while Promise.race settles on another member (externalAbort and
      // heartbeatFailure are already absorbed at creation time).
      const invocation = Promise.resolve(invoke(controller.signal));
      invocation.catch(() => undefined);
      const result = await Promise.race([
        invocation,
        heartbeatFailure,
        externalAbort,
      ]);
      if (options.signal?.aborted) throw abortError(options.signal);
      assertJson(result, 'tool result');
      if (heartbeatError !== undefined) throw heartbeatError;
      await this.actae.completeExecution(claim.execution.id, claim.claimToken, result);
      return result;
    } catch (error) {
      if (options.signal?.aborted) {
        await this.bestEffortCancel(claim.execution.id, claim.claimToken);
      } else {
        await this.bestEffortFail(claim.execution.id, claim.claimToken, error);
      }
      throw error;
    } finally {
      stopped = true;
      wakeHeartbeat?.();
      options.signal?.removeEventListener('abort', abort);
      await heartbeat;
    }
  }

  private async bestEffortCancel(id: string, token: string): Promise<void> {
    try { await this.actae.cancelExecution(id, token); } catch { /* preserve cancellation */ }
  }

  private async bestEffortFail(id: string, token: string, error: unknown): Promise<void> {
    const err = error instanceof Error ? error : new Error(String(error));
    try {
      await this.actae.failExecution(id, err.message || err.name, {
        claimToken: token,
        errorType: err.name,
        stack: err.stack,
      });
    } catch { /* preserve the tool's original error */ }
  }
}

export interface ActaeRunContextInput {
  actae: ActaeClient;
  channelId: string;
  framework: string;
  runId: string;
  parentRunId?: string;
  actor?: string;
}

/** Uniform Actae surface passed to any supported JS agent framework. */
export class ActaeRunContext {
  readonly actae: ActaeClient;
  readonly channelId: string;
  readonly framework: string;
  readonly runId: string;
  readonly parentRunId?: string;
  readonly actor?: string;

  constructor(input: ActaeRunContextInput) {
    this.actae = input.actae;
    this.channelId = required(input.channelId, 'channelId');
    this.framework = required(input.framework, 'framework');
    this.runId = required(input.runId, 'runId');
    if (input.parentRunId !== undefined) this.parentRunId = input.parentRunId;
    if (input.actor !== undefined) this.actor = input.actor;
  }

  get tools(): ActaeToolExecutor {
    return new ActaeToolExecutor(this.actae, this.channelId, this.actor ?? `${this.framework}-tool`);
  }

  async record(
    kind: string,
    payload: JsonObject,
    options: { operationId?: string } = {},
  ): Promise<unknown> {
    return this.actae.record(
      this.channelId,
      `framework.${kind}`,
      frameworkEvent({
        kind,
        framework: this.framework,
        runId: this.runId,
        parentRunId: this.parentRunId,
        payload,
      }),
      { actor: this.actor ?? this.framework, operationId: options.operationId },
    );
  }

  async saveCheckpoint(envelope: CheckpointEnvelope): Promise<number> {
    if (envelope.channelId !== this.channelId || envelope.framework !== this.framework) {
      throw new AdapterContractError('checkpoint does not belong to this run context');
    }
    const cursor = envelope.eventCursor ?? await this.actae.latestCursor(this.channelId) ?? 0;
    return this.actae.saveState(this.channelId, cursor, envelope.toJSON());
  }

  async loadCheckpoint(): Promise<CheckpointEnvelope | undefined> {
    const snapshot = await this.actae.latestState(this.channelId);
    if (!snapshot) return undefined;
    if (snapshot.state['schema'] === CHECKPOINT_SCHEMA) {
      return CheckpointEnvelope.fromJSON(snapshot.state);
    }
    return extractCheckpointEnvelope(snapshot.state);
  }

  async replay(options: { cursor?: number; limit?: number } = {}): Promise<unknown[]> {
    return this.actae.replay(this.channelId, options);
  }

  async fork(
    newChannelId: string,
    options: {
      atCursor?: number;
      reason?: string;
      manifest?: JsonObject;
      operationId?: string;
    } = {},
  ): Promise<ActaeRunContext> {
    await this.actae.fork(this.channelId, newChannelId, options.atCursor ?? 0, {
      displayName: newChannelId,
      reason: options.reason,
      manifest: options.manifest,
      operationId: options.operationId,
    });
    return new ActaeRunContext({
      actae: this.actae,
      channelId: newChannelId,
      framework: this.framework,
      runId: newChannelId,
      parentRunId: this.runId,
      actor: this.actor,
    });
  }

  async createExperiment(name: string, description?: string): Promise<JsonObject> {
    return this.actae.createExperiment(name, {
      description,
      baselineChannelId: this.channelId,
    });
  }

  async addToExperiment(
    groupId: string,
    options: { role?: string; declaredDelta?: JsonObject } = {},
  ): Promise<JsonObject> {
    return this.actae.addExperimentMember(groupId, this.channelId, options);
  }

  async createExecutionGroup(groupId: string, metadata: JsonObject = {}): Promise<unknown> {
    return this.actae.createExecutionGroup(groupId, metadata);
  }

  async scheduleWakeup(runAt: string, payload?: JsonObject): Promise<unknown> {
    return this.actae.scheduleWakeup(this.channelId, runAt, payload);
  }
}

function required(value: string, label: string): string {
  if (value.trim() === '') throw new AdapterContractError(`${label} must not be empty`);
  return value;
}

function asObject(value: JsonValue | undefined, label: string): JsonObject {
  if (value === undefined || value === null) return {};
  if (Array.isArray(value) || typeof value !== 'object') {
    throw new AdapterContractError(`${label} must be an object`);
  }
  return value as JsonObject;
}

function asRequiredString(value: JsonValue | undefined, label: string): string {
  if (typeof value !== 'string' || value === '') {
    throw new AdapterContractError(`${label} must be a non-empty string`);
  }
  return value;
}

function asOptionalString(value: JsonValue | undefined, label: string): string | undefined {
  if (value === undefined || value === null) return undefined;
  if (typeof value !== 'string') throw new AdapterContractError(`${label} must be a string`);
  return value;
}

function asOptionalNumber(value: JsonValue | undefined, label: string): number | undefined {
  if (value === undefined || value === null) return undefined;
  if (typeof value !== 'number') throw new AdapterContractError(`${label} must be a number`);
  return value;
}

function assertJson(value: unknown, label: string, seen = new Set<object>()): void {
  if (value === null || typeof value === 'string' || typeof value === 'boolean' || typeof value === 'bigint') return;
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) throw new AdapterContractError(`${label} must not contain NaN or Infinity`);
    return;
  }
  if (typeof value !== 'object') throw new AdapterContractError(`${label} must be JSON-serializable`);
  if (seen.has(value)) throw new AdapterContractError(`${label} must not contain cycles`);
  seen.add(value);
  try {
    if (Array.isArray(value)) {
      for (const item of value) assertJson(item, label, seen);
    } else {
      for (const item of Object.values(value as Record<string, unknown>)) assertJson(item, label, seen);
    }
  } finally {
    seen.delete(value);
  }
}

function abortError(signal: AbortSignal): Error {
  if (signal.reason instanceof Error) return signal.reason;
  return new Error(signal.reason === undefined ? 'operation aborted' : String(signal.reason));
}
