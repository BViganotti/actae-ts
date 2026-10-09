/** Low-friction, orchestrator-safe Actae integration surface. */

import { AsyncLocalStorage } from 'node:async_hooks';
import { createHash, randomUUID } from 'node:crypto';

import { ActaeClient, newClientFromEnv, validateChannelId } from './client.js';
import { deterministicOperationKey } from './deterministic.js';
import { stringify, type JsonObject, type JsonValue } from './json.js';
import type { ClientOptions } from './options.js';
import {
  ActaeRunContext,
  CheckpointEnvelope,
  type ToolExecutionOptions,
} from './adapters/contract.js';
import {
  ActaeCheckpointSaver,
  type ActaeCheckpointSaverOptions,
} from './adapters/langgraph.js';
import { ActaeClaudeSessionStore } from './adapters/claude.js';
import {
  ActaeContextSaver,
  ChainResumer,
  type ActaeContextSaverOptions,
} from './adapters/langchain.js';
import {
  installActaeTracing,
  type ActaeTracingProcessorOptions,
} from './adapters/openai.js';
import {
  CopilotManager,
  type CopilotManagerOptions,
} from './adapters/copilot.js';
import {
  CodexOTLPReceiver,
  type CodexOTLPReceiverOptions,
} from './adapters/codex.js';
import {
  CLAUDE_CAPABILITIES,
  CODEX_CAPABILITIES,
  COPILOT_CAPABILITIES,
  LANGCHAIN_CAPABILITIES,
  LANGGRAPH_CAPABILITIES,
  OPENAI_AGENTS_CAPABILITIES,
} from './adapters/profiles.js';


const CHANNEL_RE = /^[A-Za-z0-9._:\-]{1,256}$/;
const INVALID_CHANNEL_RE = /[^A-Za-z0-9._:\-]+/g;
export const RUN_CARRIER_SCHEMA = 'actae.run-carrier/v1' as const;

/** Maps arbitrary native IDs to deterministic, collision-safe channels. */
export function channelForRun(runId: string, namespace = 'run'): string {
  required(runId, 'runId');
  required(namespace, 'namespace');
  const candidate = `${namespace}:${runId}`;
  if (CHANNEL_RE.test(candidate) && Buffer.byteLength(candidate, 'utf8') <= 256) return candidate;
  const safeNamespace = namespace.replace(INVALID_CHANNEL_RE, '-').replace(/^[-._:]+|[-._:]+$/g, '') || 'run';
  const slug = (runId.replace(INVALID_CHANNEL_RE, '-').replace(/^[-._:]+|[-._:]+$/g, '').slice(0, 80) || 'run');
  const digest = createHash('sha256').update(runId, 'utf8').digest('hex').slice(0, 16);
  const result = `${safeNamespace.slice(0, 80)}:${slug}:${digest}`;
  validateChannelId(result);
  return result;
}

export type LifecycleErrorMode = 'raise' | 'warn';

export interface RunOptions {
  id: string;
  framework?: string;
  channelId?: string;
  parentRunId?: string;
  actor?: string;
  workflowId?: string;
  nativeRunId?: string;
  attempt?: number;
  /** Stable native execution identity. Defaults to attempt:N or a UUID. */
  executionId?: string;
  metadata?: JsonObject;
}

export interface WorkflowOptions extends Omit<RunOptions, 'id' | 'framework' | 'workflowId'> {
  id: string;
  orchestrator?: string;
}

/** Credential-free identity safe to serialize into task/activity payloads. */
export interface ActaeCarrier {
  schema: typeof RUN_CARRIER_SCHEMA;
  channel_id: string;
  run_id: string;
  framework: string;
  workflow_id: string | null;
}

export function parseCarrier(value: unknown): ActaeCarrier {
  if (!isPlainObject(value) || value['schema'] !== RUN_CARRIER_SCHEMA) {
    throw new TypeError('unsupported or invalid Actae run carrier');
  }
  const allowed = new Set(['schema', 'channel_id', 'run_id', 'framework', 'workflow_id']);
  const unexpected = Object.keys(value).filter((key) => !allowed.has(key));
  if (unexpected.length > 0) {
    throw new TypeError(`Actae run carrier contains unsupported fields: ${unexpected.sort().join(', ')}`);
  }
  if (typeof value['channel_id'] !== 'string') {
    throw new TypeError('carrier.channel_id must be a string');
  }
  if (typeof value['run_id'] !== 'string') {
    throw new TypeError('carrier.run_id must be a string');
  }
  if (typeof value['framework'] !== 'string') {
    throw new TypeError('carrier.framework must be a string');
  }
  const channelId = value['channel_id'];
  const runId = value['run_id'];
  const framework = value['framework'];
  validateChannelId(channelId);
  required(runId, 'carrier.run_id');
  required(framework, 'carrier.framework');
  const workflowId = value['workflow_id'];
  if (workflowId !== null && workflowId !== undefined && typeof workflowId !== 'string') {
    throw new TypeError('carrier.workflow_id must be a string or null');
  }
  if (workflowId === '') throw new TypeError('carrier.workflow_id must not be empty');
  return {
    schema: RUN_CARRIER_SCHEMA,
    channel_id: channelId,
    run_id: runId,
    framework,
    workflow_id: workflowId ?? null,
  };
}

export class ActaeScope {
  constructor(
    readonly owner: Actae,
    readonly context: ActaeRunContext,
    readonly workflowId?: string,
    readonly attempt?: number,
    readonly nativeRunId?: string,
    readonly invocationId = '',
    readonly metadata: JsonObject = {},
  ) {}

  get channelId(): string { return this.context.channelId; }
  get runId(): string { return this.context.runId; }
  get parentRunId(): string | undefined { return this.context.parentRunId; }
  get framework(): string { return this.context.framework; }
  get tools() { return this.context.tools; }

  carrier(): ActaeCarrier {
    return {
      schema: RUN_CARRIER_SCHEMA,
      channel_id: this.channelId,
      run_id: this.runId,
      framework: this.framework,
      workflow_id: this.workflowId ?? null,
    };
  }

  record(kind: string, payload: JsonObject, operationId?: string): Promise<unknown> {
    return this.context.record(kind, payload, { operationId });
  }

  effect<T extends JsonValue>(
    key: string,
    toolName: string,
    params: JsonValue,
    invoke: (signal: AbortSignal) => T | Promise<T>,
    options: ToolExecutionOptions = {},
  ): Promise<T> {
    return this.tools.execute(key, toolName, params, invoke, options);
  }

  child<T>(id: string, fn: (scope: ActaeScope) => T | Promise<T>, options: Omit<RunOptions, 'id' | 'parentRunId'> = {}): Promise<T> {
    return this.owner.run({
      ...options,
      id,
      framework: options.framework ?? this.framework,
      parentRunId: this.runId,
    }, fn);
  }
}

export interface EffectOptions<TArgs extends unknown[]> extends ToolExecutionOptions {
  key: string | ((...args: TArgs) => string);
  name?: string;
  params?: (...args: TArgs) => JsonValue;
  /** Append Actae's cancellation signal to the wrapped function arguments. */
  passSignal?: boolean;
}

export interface ToolOptions<TArgs extends unknown[]> {
  name?: string;
  params?: (...args: TArgs) => JsonValue;
  captureResult?: boolean;
}

export interface ObserveOptions<TArgs extends unknown[]> extends Omit<RunOptions, 'id' | 'attempt'> {
  runId: string | ((...args: TArgs) => string);
  attempt?: number | ((...args: TArgs) => number | undefined);
}

export class LangGraphProvider {
  constructor(private readonly owner: Actae) {}
  readonly capabilities = LANGGRAPH_CAPABILITIES;

  checkpointer(options: ActaeCheckpointSaverOptions = {}): ActaeCheckpointSaver {
    return new ActaeCheckpointSaver(this.owner.client, { channel: 'langgraph', ...options });
  }

  config(threadId: string, checkpointNs = '', configurable: Record<string, unknown> = {}): Record<string, unknown> {
    required(threadId, 'threadId');
    return { configurable: { thread_id: threadId, checkpoint_ns: checkpointNs, ...configurable } };
  }
}

export class ClaudeProvider {
  constructor(private readonly owner: Actae) {}
  readonly capabilities = CLAUDE_CAPABILITIES;
  sessionStore(): ActaeClaudeSessionStore { return new ActaeClaudeSessionStore(this.owner.client); }
}

export class LangChainProvider {
  constructor(private readonly owner: Actae) {}
  readonly capabilities = LANGCHAIN_CAPABILITIES;
  callback(options: ActaeContextSaverOptions = {}): ActaeContextSaver {
    return new ActaeContextSaver(this.owner.client, options);
  }
  callbacks(existing: unknown[] = [], options: ActaeContextSaverOptions = {}): unknown[] {
    return [...existing, this.callback(options)];
  }
  resumer(options: ActaeContextSaverOptions = {}): ChainResumer {
    return new ChainResumer(this.owner.client, options);
  }
}

export class OpenAIProvider {
  constructor(private readonly owner: Actae) {}
  readonly capabilities = OPENAI_AGENTS_CAPABILITIES;

  tracing(options: ActaeTracingProcessorOptions = {}) {
    return installActaeTracing(this.owner.client, options);
  }

  async run(agent: unknown, input: unknown, options: { runId: string; channelId?: string; runner?: Record<string, unknown> }): Promise<unknown> {
    const sdk = await loadOpenAIAgents();
    return this.owner.run({
      id: options.runId,
      framework: 'openai-agents',
      channelId: options.channelId,
    }, async (scope) => {
      const result = await sdk.run(agent, input, options.runner);
      await this.saveState(scope, result.state.toJSON());
      return result;
    });
  }

  async resume(agent: unknown, options: { runId: string; channelId?: string; runner?: Record<string, unknown> }): Promise<unknown> {
    const sdk = await loadOpenAIAgents();
    const channelId = options.channelId ?? channelForRun(options.runId, 'openai-agents');
    return this.owner.run({ id: options.runId, framework: 'openai-agents', channelId }, async (scope) => {
      const envelope = await scope.context.loadCheckpoint();
      if (!envelope || !isPlainObject(envelope.nativeCheckpoint)) {
        throw new Error(`no restorable OpenAI Agents RunState for ${JSON.stringify(options.runId)}`);
      }
      const state = await sdk.RunState.fromString(agent, JSON.stringify(envelope.nativeCheckpoint));
      const result = await sdk.run(agent, state, options.runner);
      await this.saveState(scope, result.state.toJSON());
      return result;
    });
  }

  async fork(sourceRunId: string, newRunId: string, options: { atCursor?: number; reason?: string } = {}): Promise<string> {
    const source = channelForRun(sourceRunId, 'openai-agents');
    const target = channelForRun(newRunId, 'openai-agents');
    await this.owner.client.fork(source, target, options.atCursor ?? 0, {
      displayName: newRunId,
      reason: options.reason,
      manifest: { framework: 'openai-agents', native: 'RunState' },
    });
    return target;
  }

  private async saveState(scope: ActaeScope, nativeState: JsonObject): Promise<void> {
    const cursor = await this.owner.client.latestCursor(scope.channelId) ?? 0;
    await scope.context.saveCheckpoint(new CheckpointEnvelope({
      framework: 'openai-agents',
      adapterVersion: '3',
      channelId: scope.channelId,
      eventCursor: cursor,
      portableState: { current_turn: nativeState['currentTurn'] ?? nativeState['current_turn'] ?? null },
      nativeCheckpoint: nativeState,
      manifest: { native: 'RunState' },
    }));
  }
}

export class CodexProvider {
  constructor(private readonly owner: Actae) {}
  readonly capabilities = CODEX_CAPABILITIES;
  receiver(options: CodexOTLPReceiverOptions = {}): CodexOTLPReceiver {
    return new CodexOTLPReceiver(this.owner.client, options);
  }
}

export class CopilotProvider {
  constructor(private readonly owner: Actae) {}
  readonly capabilities = COPILOT_CAPABILITIES;
  manager(options: CopilotManagerOptions = {}): CopilotManager {
    return new CopilotManager(this.owner.client, options);
  }
}

export class OrchestratorProvider {
  readonly deterministic: boolean;

  constructor(private readonly owner: Actae, readonly name: string, deterministic?: boolean) {
    required(name, 'orchestrator name');
    this.deterministic = deterministic ?? new Set(['temporal', 'durable-functions', 'azure-durable-functions']).has(name.toLowerCase());
  }

  carrier(workflowId: string): ActaeCarrier {
    required(workflowId, 'workflowId');
    return {
      schema: RUN_CARRIER_SCHEMA,
      channel_id: channelForRun(workflowId, this.name),
      run_id: workflowId,
      framework: this.name,
      workflow_id: workflowId,
    };
  }

  workflow<T>(options: Omit<WorkflowOptions, 'orchestrator'>, fn: (scope: ActaeScope) => T | Promise<T>): Promise<T> {
    if (this.deterministic) {
      throw new Error(
        `${this.name} workflow code must not perform Actae network I/O during deterministic replay; ` +
        'use orchestrator.carrier(workflowId) in the workflow and orchestrator.activity(...) in the activity/worker boundary',
      );
    }
    return this.owner.workflow({ ...options, orchestrator: this.name }, fn);
  }

  activity<T>(
    options: Omit<RunOptions, 'framework' | 'parentRunId' | 'workflowId'> & { parent: ActaeCarrier | unknown },
    fn: (scope: ActaeScope) => T | Promise<T>,
  ): Promise<T> {
    const { parent: rawParent, ...runOptions } = options;
    const parent = parseCarrier(rawParent);
    return this.owner.run({
      ...runOptions,
      framework: `${this.name}.activity`,
      parentRunId: parent.run_id,
      workflowId: parent.workflow_id ?? parent.run_id,
      nativeRunId: options.nativeRunId ?? options.id,
    }, fn);
  }

  effectKey(logicalTaskId: string, effect: string, businessId: string): string {
    return deterministicOperationKey('actae.orchestrator.effect', effect, logicalTaskId, businessId);
  }
}

export class FrameworkProviders {
  readonly langgraph: LangGraphProvider;
  readonly claude: ClaudeProvider;
  readonly langchain: LangChainProvider;
  readonly openai: OpenAIProvider;
  readonly copilot: CopilotProvider;
  readonly codex: CodexProvider;

  constructor(private readonly owner: Actae) {
    this.langgraph = new LangGraphProvider(owner);
    this.claude = new ClaudeProvider(owner);
    this.langchain = new LangChainProvider(owner);
    this.openai = new OpenAIProvider(owner);
    this.copilot = new CopilotProvider(owner);
    this.codex = new CodexProvider(owner);
  }

  claudeSessionStore(): ActaeClaudeSessionStore {
    return this.claude.sessionStore();
  }

  langchainCallback(options: ActaeContextSaverOptions = {}): ActaeContextSaver {
    return this.langchain.callback(options);
  }

  langchainResumer(options: ActaeContextSaverOptions = {}): ChainResumer {
    return this.langchain.resumer(options);
  }

  openaiTracing(options: ActaeTracingProcessorOptions = {}) {
    return this.openai.tracing(options);
  }

  /** Compatibility helper for callers that prefer a direct manager factory. */
  copilotManager(options: CopilotManagerOptions = {}): CopilotManager {
    return this.copilot.manager(options);
  }

  codexReceiver(options: CodexOTLPReceiverOptions = {}): CodexOTLPReceiver {
    return this.codex.receiver(options);
  }
}

/** One entry point for Actae scopes, effects, frameworks, and orchestrators. */
export class Actae {
  readonly frameworks: FrameworkProviders;
  readonly langgraph: LangGraphProvider;
  readonly claude: ClaudeProvider;
  readonly langchain: LangChainProvider;
  readonly openai: OpenAIProvider;
  readonly copilot: CopilotProvider;
  readonly codex: CodexProvider;
  private readonly storage = new AsyncLocalStorage<ActaeScope>();

  constructor(
    readonly client: ActaeClient,
    readonly lifecycleErrors: LifecycleErrorMode = 'raise',
  ) {
    if (!client || typeof client.record !== 'function') {
      throw new TypeError('client must be an ActaeClient-compatible object');
    }
    if (lifecycleErrors !== 'raise' && lifecycleErrors !== 'warn') {
      throw new TypeError("lifecycleErrors must be 'raise' or 'warn'");
    }
    this.frameworks = new FrameworkProviders(this);
    this.langgraph = this.frameworks.langgraph;
    this.claude = this.frameworks.claude;
    this.langchain = this.frameworks.langchain;
    this.openai = this.frameworks.openai;
    this.copilot = this.frameworks.copilot;
    this.codex = this.frameworks.codex;
  }

  static fromEnv(options: Partial<ClientOptions> = {}, lifecycleErrors: LifecycleErrorMode = 'raise'): Actae {
    return new Actae(newClientFromEnv(options as ClientOptions), lifecycleErrors);
  }

  current(requiredScope = true): ActaeScope | undefined {
    const scope = this.storage.getStore();
    if (!scope && requiredScope) {
      throw new Error('no active Actae run; use actae.run(...)');
    }
    return scope;
  }

  close(): void {
    this.client.disconnect();
  }

  orchestrator(name: string, options: { deterministic?: boolean } = {}): OrchestratorProvider {
    return new OrchestratorProvider(this, name, options.deterministic);
  }

  observe<TArgs extends unknown[], TResult>(
    options: ObserveOptions<TArgs>,
    fn: (...args: TArgs) => TResult | Promise<TResult>,
  ): (...args: TArgs) => Promise<TResult> {
    return async (...args: TArgs) => {
      const id = typeof options.runId === 'function' ? options.runId(...args) : options.runId;
      const attempt = typeof options.attempt === 'function' ? options.attempt(...args) : options.attempt;
      const { runId: _runId, ...runOptions } = options;
      return this.run({ ...runOptions, id, attempt }, async () => fn(...args));
    };
  }

  use<T>(scope: ActaeScope, fn: () => T): T {
    if (scope.owner !== this) throw new TypeError('scope belongs to another Actae instance');
    return this.storage.run(scope, fn);
  }

  async run<T>(idOrOptions: string | RunOptions, fn: (scope: ActaeScope) => T | Promise<T>): Promise<T> {
    const options: RunOptions = typeof idOrOptions === 'string' ? { id: idOrOptions } : idOrOptions;
    required(options.id, 'id');
    const framework = options.framework ?? 'custom';
    required(framework, 'framework');
    for (const [label, value] of [
      ['parentRunId', options.parentRunId],
      ['workflowId', options.workflowId],
      ['nativeRunId', options.nativeRunId],
      ['executionId', options.executionId],
    ] as const) {
      if (value !== undefined) required(value, label);
    }
    if (options.attempt !== undefined && (!Number.isInteger(options.attempt) || options.attempt < 0)) {
      throw new TypeError('attempt must be a non-negative integer');
    }
    assertJson(options.metadata ?? {}, 'run metadata');
    const ambient = this.current(false);
    const context = new ActaeRunContext({
      actae: this.client,
      channelId: options.channelId ?? channelForRun(options.id, framework),
      framework,
      runId: options.id,
      parentRunId: options.parentRunId ?? ambient?.runId,
      actor: options.actor,
    });
    const scope = new ActaeScope(
      this,
      context,
      options.workflowId,
      options.attempt,
      options.nativeRunId,
      options.executionId ?? (options.attempt !== undefined ? `attempt:${options.attempt}` : randomUUID()),
      options.metadata ?? {},
    );
    return this.storage.run(scope, async () => {
      await this.lifecycle(scope, 'run.started', lifecyclePayload(scope));
      let result: T;
      try {
        result = await fn(scope);
      } catch (error) {
        const err = asError(error);
        const cancelled = isCancellation(error);
        try {
          await this.lifecycle(scope, cancelled ? 'run.cancelled' : 'run.failed', {
            ...lifecyclePayload(scope),
            [cancelled ? 'cancellation' : 'error']: {
              type: err.name,
              message: err.message.slice(0, 4000),
            },
          });
        } catch (recordError) {
          console.warn('Actae failed to record run failure; preserving application error', recordError);
        }
        throw error;
      }
      await this.lifecycle(scope, 'run.completed', {
        ...lifecyclePayload(scope),
      });
      return result;
    });
  }

  workflow<T>(idOrOptions: string | WorkflowOptions, fn: (scope: ActaeScope) => T | Promise<T>): Promise<T> {
    const options: WorkflowOptions = typeof idOrOptions === 'string' ? { id: idOrOptions } : idOrOptions;
    const orchestrator = options.orchestrator ?? 'orchestrator';
    if (new Set(['temporal', 'durable-functions', 'azure-durable-functions']).has(orchestrator.toLowerCase())) {
      throw new Error(
        `${orchestrator} workflow code is replayed deterministically; use ` +
        `actae.orchestrator(${JSON.stringify(orchestrator)}).carrier(workflowId) in the workflow ` +
        'and .activity(...) at the worker boundary',
      );
    }
    return this.run({
      ...options,
      framework: orchestrator,
      workflowId: options.id,
      nativeRunId: options.nativeRunId ?? options.id,
    }, fn);
  }

  effect<TArgs extends unknown[], TResult extends JsonValue>(
    options: EffectOptions<TArgs>,
    fn: (...args: TArgs) => TResult | Promise<TResult>,
  ): (...args: TArgs) => Promise<TResult> {
    if (!options || options.key === undefined) throw new TypeError('effect key is required');
    const toolName = options.name ?? (fn.name || 'tool');
    return async (...args: TArgs): Promise<TResult> => {
      const scope = this.current();
      const params = options.params ? options.params(...args) : defaultParams(args);
      assertJson(params, 'effect parameters');
      const key = resolveKey(options.key, args, params);
      const { name: _name, params: _params, passSignal, key: _key, ...executionOptions } = options;
      return scope!.tools.execute(
        key,
        toolName,
        params,
        async (signal) => {
          const callArgs = passSignal ? [...args, signal] as unknown as TArgs : args;
          return fn(...callArgs);
        },
        executionOptions,
      );
    };
  }

  tool<TArgs extends unknown[], TResult extends JsonValue>(
    fn: (...args: TArgs) => TResult | Promise<TResult>,
    options: ToolOptions<TArgs> = {},
  ): (...args: TArgs) => Promise<TResult> {
    const toolName = options.name ?? (fn.name || 'tool');
    return async (...args: TArgs): Promise<TResult> => {
      const scope = this.current()!;
      const callId = randomUUID();
      const params = options.params ? options.params(...args) : defaultParams(args);
      assertJson(params, 'tool parameters');
      await scope.record('tool.started', { tool: toolName, tool_call_id: callId, params });
      try {
        const result = await fn(...args);
        assertJson(result, 'tool result');
        await scope.record('tool.completed', {
          tool: toolName,
          tool_call_id: callId,
          ...(options.captureResult === false ? {} : { result }),
        });
        return result;
      } catch (error) {
        const err = asError(error);
        const cancelled = isCancellation(error);
        try {
          await scope.record(cancelled ? 'tool.cancelled' : 'tool.failed', {
            tool: toolName,
            tool_call_id: callId,
            [cancelled ? 'cancellation' : 'error']: {
              type: err.name,
              message: err.message.slice(0, 4000),
            },
          });
        } catch (recordError) {
          console.warn('Actae failed to record tool failure; preserving tool error', recordError);
        }
        throw error;
      }
    };
  }

  private async lifecycle(scope: ActaeScope, kind: string, payload: JsonObject): Promise<void> {
    const operationId = deterministicOperationKey(
      'actae.runtime', kind, scope.channelId, scope.runId, scope.invocationId,
    );
    try {
      await scope.record(kind, payload, operationId);
    } catch (error) {
      if (this.lifecycleErrors === 'raise') throw error;
      console.warn(`Actae lifecycle write failed (${kind})`, error);
    }
  }
}

function lifecyclePayload(scope: ActaeScope): JsonObject {
  return {
    workflow_id: scope.workflowId ?? null,
    native_run_id: scope.nativeRunId ?? null,
    attempt: scope.attempt ?? null,
    invocation_id: scope.invocationId,
    metadata: scope.metadata,
  };
}

function defaultParams(args: readonly unknown[]): JsonValue {
  if (args.length === 1 && isPlainObject(args[0])) return args[0] as JsonObject;
  return { args: args as JsonValue[] };
}

function resolveKey<TArgs extends unknown[]>(
  source: string | ((...args: TArgs) => string),
  args: TArgs,
  params: JsonValue,
): string {
  let key: string;
  if (typeof source === 'function') {
    key = source(...args);
  } else {
    key = source.replace(/\{([A-Za-z0-9_.]+)\}/g, (_match, path: string) => {
      let value: unknown = params;
      for (const segment of path.split('.')) {
        if (!value || typeof value !== 'object' || Array.isArray(value) || !(segment in value)) {
          throw new TypeError(`effect key template field is missing: ${path}`);
        }
        value = (value as Record<string, unknown>)[segment];
      }
      if (value === undefined || value === null || typeof value === 'object') {
        throw new TypeError(`effect key template field must be scalar: ${path}`);
      }
      return String(value);
    });
  }
  required(key, 'effect key');
  if (Buffer.byteLength(key, 'utf8') > 512) throw new TypeError('effect key must not exceed 512 UTF-8 bytes');
  return key;
}

function assertJson(value: unknown, label: string): void {
  try {
    assertJsonValue(value, label, new Set<object>());
    stringify(value);
  } catch (error) {
    throw new TypeError(`${label} must be JSON-serializable: ${asError(error).message}`);
  }
}

function assertJsonValue(value: unknown, label: string, seen: Set<object>): void {
  if (value === null || typeof value === 'string' || typeof value === 'boolean' || typeof value === 'bigint') return;
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) throw new TypeError(`${label} contains NaN or Infinity`);
    return;
  }
  if (typeof value !== 'object') throw new TypeError(`${label} contains ${typeof value}`);
  if (seen.has(value)) throw new TypeError(`${label} contains a cycle`);
  seen.add(value);
  try {
    if (Array.isArray(value)) {
      for (const item of value) assertJsonValue(item, label, seen);
    } else {
      for (const item of Object.values(value as Record<string, unknown>)) {
        assertJsonValue(item, label, seen);
      }
    }
  } finally {
    seen.delete(value);
  }
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function required(value: string, label: string): void {
  if (typeof value !== 'string' || value.trim() === '') throw new TypeError(`${label} must be a non-empty string`);
}

function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

function isCancellation(error: unknown): boolean {
  if (!(error instanceof Error)) return false;
  return error.name === 'AbortError' || error.name === 'CancelledError' || error.name === 'CanceledError';
}

interface OpenAIAgentsRuntime {
  run: (
    agent: unknown,
    input: unknown,
    options?: Record<string, unknown>,
  ) => Promise<{ state: { toJSON: () => JsonObject } }>;
  RunState: {
    fromString: (
      agent: unknown,
      state: string,
    ) => Promise<unknown>;
  };
}

async function loadOpenAIAgents(): Promise<OpenAIAgentsRuntime> {
  try {
    const sdk = await import('@openai/agents');
    if (typeof sdk.run !== 'function' || typeof sdk.RunState?.fromString !== 'function') {
      throw new TypeError('@openai/agents does not expose run + RunState.fromString');
    }
    return sdk as unknown as OpenAIAgentsRuntime;
  } catch (error) {
    throw new TypeError(
      `OpenAI durable runs require @openai/agents with RunState support: ${asError(error).message}`,
    );
  }
}
