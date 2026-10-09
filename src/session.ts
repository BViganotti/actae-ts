/**
 * AgentSession — high-level agent instrumentation with step recording, fork
 * at any step, crash recovery and cursor-aligned state snapshots. Ports the
 * Python SDK's `session.py` (via the Go SDK's `session.go`).
 *
 * Lifecycle: created → started → stepping → completed | crashed.
 */

import type { ActaeClient } from './client.js';
import {
  APIError,
  ConnectionError,
  NoRestorableCheckpointError,
  SessionCompletedError,
  SessionError,
  SnapshotBoundaryError,
} from './errors.js';
import { asInt64, type JsonObject, type JsonValue } from './json.js';
import type { ReplayOptions } from './options.js';
import type {
  Event,
  ForkReceipt,
  StepResolution,
  StateSnapshot,
} from './types.js';
import { canonicalStepContent, deterministicOperationKey } from './deterministic.js';
import { SDK_VERSION } from './version.js';

/** Minimal direct/fleet contract consumed by AgentSession. */
export type SessionTransport = Pick<ActaeClient,
  'record' | 'replay' | 'saveState' | 'latestState' | 'fork' |
  'getChannelMetadata' | 'updateMetadata' | 'resolveStep' | 'latestStepNumber' |
  'setOutcome' | 'promoteChannel' | 'createExperiment' | 'addExperimentMember'>;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

export const SESSION_STARTED = 'session.started';
export const SESSION_COMPLETED = 'session.completed';

export const SessionStatus = {
  Created: 'created',
  Started: 'started',
  Stepping: 'stepping',
  Completed: 'completed',
  Crashed: 'crashed',
} as const;

export type SessionStatusValue = (typeof SessionStatus)[keyof typeof SessionStatus];

const maxStoredCursors = 1000;
const maxLineageDepth = 10;
const maxSessionRetries = 3;
const retryBaseDelay = 500;
const retryMaxDelay = 5000;

/** Reconstruct the per-step cursor list from the durable event log.
 *
 * Session metadata is written on lifecycle transitions, so a hard process
 * death can leave it lagging; every `step()` writes `step_number` into its
 * payload in the same server transaction as the event. Returns undefined when
 * no step events exist (caller falls back to metadata). */
async function recoverStepProgress(
  actae: SessionTransport,
  channelId: string,
): Promise<number[] | undefined> {
  const byStep = new Map<number, number>();
  let cursor: number | undefined;
  let seen = 0;
  while (seen < 50000) {
    let batch: Event[];
    try {
      batch = await actae.replay(channelId, { cursor, limit: 1000 });
    } catch {
      return undefined; // best-effort fallback, never fatal
    }
    if (!batch || batch.length === 0) break;
    for (const ev of batch) {
      const payload = ev.payload;
      if (payload && typeof payload === 'object' && !Array.isArray(payload)) {
        const raw = (payload as JsonObject)['step_number'];
        let stepNumber: number | undefined;
        if (typeof raw === 'number') stepNumber = raw;
        else if (typeof raw === 'bigint') stepNumber = Number(raw);
        if (
          stepNumber !== undefined &&
          Number.isFinite(stepNumber) &&
          stepNumber >= 1 &&
          !byStep.has(stepNumber)
        ) {
          byStep.set(stepNumber, ev.cursor);
        }
      }
    }
    seen += batch.length;
    const last = batch[batch.length - 1]!.cursor;
    if (cursor !== undefined && last <= cursor) break;
    cursor = last;
  }
  if (byStep.size === 0) return undefined;
  const max = Math.max(...byStep.keys());
  const cursors: number[] = [];
  for (let i = 1; i <= max; i++) cursors.push(byStep.get(i) ?? 0);
  return cursors;
}

// ---------------------------------------------------------------------------
// Options & types
// ---------------------------------------------------------------------------

/** Returns the current agent state dict. Called every SnapshotInterval
 * steps; returning undefined skips the snapshot. */
export type StateFn = () => JsonObject | undefined;

export interface AgentSessionOptions {
  /** Optional human-readable name for the dashboard. Falls back to `name`
   * when empty. */
  displayName?: string;
  /** Returns the current agent state; snapshots are saved every
   * SnapshotInterval steps. */
  stateFn?: StateFn;
  /** Saves a state snapshot every N steps (default 1). */
  snapshotInterval?: number;
  /** Arbitrary parameters dict stored in metadata. */
  params?: JsonObject;
}

export interface StepOptions {
  /** Input data for the step. */
  input?: JsonValue;
  /** Output data from the step. */
  output?: JsonValue;
  /** Delta context generated at this step (stored in the event payload so
   * the dashboard can reconstruct cumulative state by merging deltas). */
  context?: JsonValue;
  /** Optional key-value metadata attached to the event. */
  metadata?: JsonObject;
}

export interface ForkSessionOptions {
  displayName?: string;
  params?: JsonObject;
  stateFn?: StateFn;
  reason?: string;
  /** "exact" (default), "approximate" or "lineage_only". See the fork()
   * docstring for the strict/approximate/lineage_only policy. */
  boundaryMode?: 'exact' | 'approximate' | 'lineage_only';
  /** Immutable fork manifest echoed in the receipt. */
  manifest?: JsonObject;
  /** Opaque descriptor of the intended change (e.g. {model: 'new-model'})
   * recorded on the fork metadata. The application reads session.intervention
   * and applies it; Actae never interprets it. */
  intervention?: JsonObject;
  /** Child's side-effect policy map ({tool: replay|block|live|auto, '*': def});
   * undefined = auto. */
  toolPolicies?: JsonObject;
}

export interface ResumeOptions {
  /** When set, forks the source channel at this step into a new channel
   * (name required). When unset, resumes the channel in place (crash
   * recovery). */
  forkAtStep?: number;
  /** Required when forkAtStep is set (the new fork channel). */
  name?: string;
  /** Params merged over the stored session params. */
  params?: JsonObject;
  /** State callback for the resumed session. */
  stateFn?: StateFn;
  /** SnapshotInterval for the resumed session (default 1). */
  snapshotInterval?: number;
  /** "exact" (default), "approximate" or "lineage_only". */
  boundaryMode?: 'exact' | 'approximate' | 'lineage_only';
  /** Immutable fork manifest echoed in the receipt. */
  manifest?: JsonObject;
  /** Opaque intervention descriptor recorded on the fork metadata
   * (application-applied; Actae never interprets it). */
  intervention?: JsonObject;
  /** Child's side-effect policy map (undefined = auto). */
  toolPolicies?: JsonObject;
}

// ---------------------------------------------------------------------------
// AgentSession
// ---------------------------------------------------------------------------

/** Wraps an Actae channel and records agent steps as events. Tracks a step →
 * cursor mapping so you can fork at a user-facing step number rather than a
 * raw server cursor, and saves state snapshots every snapshotInterval steps
 * via an optional stateFn.
 *
 * Not safe for concurrent step()/fork()/resume() calls. */
export class AgentSession {
  private readonly actae: SessionTransport;
  private readonly name: string;
  private readonly displayName: string;
  private stateFn?: StateFn;
  private readonly snapshotInterval: number;
  private readonly params: JsonObject;

  private channelIdValue = '';
  private stepCount = 0;
  private cursors: number[] = [];
  private startedAt = '';
  private status: SessionStatusValue = SessionStatus.Created;
  private resumed = false;

  private inheritedStateValue?: JsonObject;

  private boundaryRestorableValue?: boolean;
  private requestedBoundary = 0;
  private resolvedBoundary = 0;
  private sourceStateVersion = 0;
  private sourceStateSha256Value = '';
  private reproducibilityValue = '';
  private forkReceiptValue?: ForkReceipt;

  /** Opaque intervention descriptor recorded on the fork (application-applied;
   * Actae stores it, never interprets it). */
  private interventionValue: JsonObject = {};
  /** Side-effect policy applied to this fork's channel (undefined = auto). */
  private toolPoliciesValue?: JsonObject;

  private stepping = false;

  constructor(actae: SessionTransport, name: string, opts: AgentSessionOptions = {}) {
    if (!name) {
      throw new TypeError('name is required');
    }
    this.actae = actae;
    this.name = name;
    this.displayName = opts.displayName ?? '';
    this.stateFn = opts.stateFn;
    this.snapshotInterval =
      opts.snapshotInterval !== undefined && opts.snapshotInterval >= 1
        ? opts.snapshotInterval
        : 1;
    this.params = opts.params ?? {};
  }

  /** The Actae channel backing this session ("" before start()). */
  get channelId(): string {
    return this.channelIdValue;
  }

  /** The session name (used as the channel identifier). */
  get sessionName(): string {
    return this.name;
  }

  /** Number of steps recorded so far. */
  get stepCountValue(): number {
    return this.stepCount;
  }

  /** Current lifecycle status. */
  get sessionStatus(): SessionStatusValue {
    return this.status;
  }

  /** Cursor for each recorded step (index == step − 1). */
  get cursorsList(): number[] {
    return [...this.cursors];
  }

  /** Session parameters dict (copy). */
  get paramsValue(): JsonObject {
    return { ...this.params };
  }

  /** Whether this fork session inherited a restorable state checkpoint.
   * undefined for non-fork sessions; false for lineage-only forks. */
  get boundaryRestorable(): boolean | undefined {
    return this.boundaryRestorableValue;
  }

  /** The fork boundary the caller asked for (0 = latest). */
  get requestedBoundaryCursor(): number {
    return this.requestedBoundary;
  }

  /** The snapshot cursor the fork actually inherited. Exceeds
   * requestedBoundaryCursor when an approximate fallback inherited later
   * state. */
  get resolvedBoundaryCursor(): number {
    return this.resolvedBoundary;
  }

  /** Source snapshot version copied at fork time. */
  get sourceStateVersionValue(): number {
    return this.sourceStateVersion;
  }

  /** SHA-256 fingerprint of the copied state. */
  get sourceStateSha256(): string {
    return this.sourceStateSha256Value;
  }

  /** Server-computed reproducibility grade for this fork. */
  get reproducibility(): string {
    return this.reproducibilityValue;
  }

  /** Immutable fork receipt returned at creation, or undefined for non-fork
   * sessions. */
  get forkReceipt(): ForkReceipt | undefined {
    return this.forkReceiptValue;
  }

  /** The opaque intervention descriptor recorded on this fork (copy). The
   * application reads it and applies the change; Actae never interprets it. */
  get intervention(): JsonObject {
    return { ...this.interventionValue };
  }

  /** The side-effect tool policy applied to this fork's channel, or undefined
   * for the server default (`auto`). */
  get toolPolicies(): JsonObject | undefined {
    return this.toolPoliciesValue ? { ...this.toolPoliciesValue } : undefined;
  }

  /** The state snapshot this fork inherited from its parent at the fork
   * boundary (steps 1..N's data), or undefined for a fresh session. */
  get inheritedState(): JsonObject | undefined {
    return this.inheritedStateValue ? { ...this.inheritedStateValue } : undefined;
  }

  // -------------------------------------------------------------------------
  // Lifecycle
  // -------------------------------------------------------------------------

  /** Begins recording on the session channel: records a session.started
   * event and persists session metadata. */
  async start(): Promise<void> {
    if (this.resumed) {
      this.status = SessionStatus.Started;
      await this.updateMetadataStatus(SessionStatus.Started);
      return;
    }
    const ev = await this.actae.record(
      this.name,
      SESSION_STARTED,
      {
        session_name: this.name,
        params: this.params,
      } as JsonObject,
      {
        actor: 'agent_session',
        metadata: {
          session_name: this.name,
          snapshot_interval: this.snapshotInterval,
          has_state_fn: this.stateFn !== undefined,
        } as JsonObject,
      },
    );
    this.channelIdValue = ev.channelId;
    this.startedAt = ev.timestamp;
    this.status = SessionStatus.Started;
    await this.updateMetadataStatus(SessionStatus.Started);
  }

  /** Marks the session completed: records a session.completed event and
   * persists the final metadata. Safe to call multiple times; a completed or
   * crashed session is a no-op. */
  async complete(): Promise<void> {
    if (this.status === SessionStatus.Completed || this.status === SessionStatus.Crashed) {
      return;
    }
    const stepCount = this.stepCount;
    const channelId = this.channelIdValue;

    if (channelId !== '') {
      try {
        await this.actae.record(
          channelId,
          SESSION_COMPLETED,
          { session_name: this.name, total_steps: stepCount } as JsonObject,
          { actor: 'agent_session', metadata: { total_steps: stepCount } as JsonObject },
        );
      } catch (err) {
        this.errorCb?.(`failed to record session.completed for '${this.name}': ${(err as Error).message}`);
      }
    }

    this.status = SessionStatus.Completed;
    await this.updateMetadataStatus(SessionStatus.Completed);
  }

  /** Marks the session crashed with the given reason (persisted in
   * metadata). Recording failures are logged, not fatal. */
  async crash(reason: string): Promise<void> {
    this.status = SessionStatus.Crashed;
    this.errorCb?.(`session '${this.name}' crashed: ${reason}`);
    try {
      await this.updateMetadataStatus(SessionStatus.Crashed, { crash_reason: reason });
    } catch (err) {
      this.errorCb?.(`failed to update metadata after crash for '${this.name}': ${(err as Error).message}`);
    }
  }

  private errorCb?: (msg: string) => void;

  /** Registers a callback for logged non-fatal errors (session lifecycle
   * recording failures). */
  onError(cb: (msg: string) => void): void {
    this.errorCb = cb;
  }

  // -------------------------------------------------------------------------
  // Steps
  // -------------------------------------------------------------------------

  /** Records a single execution step as an event, with retry on transient
   * failures, and returns the persisted Event (inspect cursor for fork
   * resolution). */
  async step(stepType: string, opts: StepOptions = {}): Promise<Event> {
    this.requireStatus(SessionStatus.Started, SessionStatus.Stepping);
    const channelId = this.channelIdValue;
    if (channelId === '') {
      throw new SessionError(`Session '${this.name}' has no channel — call start() first`);
    }
    const stepNum = this.stepCount + 1;
    this.status = SessionStatus.Stepping;

    const eventMeta: JsonObject = { step_number: stepNum };
    if (opts.metadata) {
      for (const [k, v] of Object.entries(opts.metadata)) eventMeta[k] = v;
    }

    const payload: JsonObject = {
      step_number: stepNum,
      input: (opts.input ?? null) as JsonValue,
      output: (opts.output ?? null) as JsonValue,
    };
    if (opts.context !== undefined) payload['context'] = opts.context as JsonValue;

    // A DETERMINISTIC operation id per step makes the record idempotent:
    // if the first attempt persisted but its response was lost, the retry
    // (or a crash-recovery re-drive of the same step) returns the original
    // event instead of recording a duplicate. The id is a UUIDv5 over the
    // step's (scope, type, channel, number, canonical content) —
    // byte-identical to the Go SDK's DeterministicOperationKey and the
    // Python SDK's _step_operation_id — so two identical step events
    // (same channel, same step_number, same payload/metadata) derive the
    // SAME id and the server replays; a step whose content differs derives
    // a distinct id and records anew.
    const operationId = deterministicOperationKey(
      'agent-session',
      stepType,
      channelId,
      String(stepNum),
      canonicalStepContent(payload, eventMeta),
    );
    const stepNumber = stepNum;
    const record = () =>
      this.actae.record(channelId, stepType, payload as JsonValue, {
        actor: 'agent_session',
        metadata: eventMeta,
        operationId,
        stepNumber,
      });

    const ev = await this.retryTransientEvent('step', record);

    this.stepCount = stepNum;
    this.cursors.push(ev.cursor);

    if (this.stateFn && stepNum % this.snapshotInterval === 0) {
      let state: JsonObject | undefined;
      try {
        state = this.stateFn();
      } catch (err) {
        this.errorCb?.(`state_fn panicked on step ${stepNum} — skipping snapshot: ${(err as Error).message}`);
      }
      if (state !== undefined) {
        try {
          await this.actae.saveState(channelId, ev.cursor, state);
        } catch (err) {
          this.errorCb?.(`save_state failed on step ${stepNum}: ${(err as Error).message}`);
        }
      }
    }

    if (stepNum % this.snapshotInterval === 0 && this.status !== SessionStatus.Stepping) {
      await this.updateMetadataStatus(SessionStatus.Stepping);
    }
    return ev;
  }

  // -------------------------------------------------------------------------
  // Fork / resume
  // -------------------------------------------------------------------------

  /** Forks this session at a given step into a new experiment fork.
   * Returns an *unstarted* AgentSession — call start() on it to begin
   * recording events on the fork channel. */
  async fork(atStep: number, name: string, opts: ForkSessionOptions = {}): Promise<AgentSession> {
    this.requireStatus(
      SessionStatus.Started,
      SessionStatus.Stepping,
      SessionStatus.Completed,
      SessionStatus.Crashed,
    );
    if (this.channelIdValue === '') {
      throw new SessionError('Session has no channel — was it started?');
    }
    if (!name) {
      throw new TypeError('fork name is required');
    }

    let sourceChannelID = this.channelIdValue;
    let cursor = await this.cursorForStep(atStep);
    if (cursor === 0) {
      const resolved = await this.resolveForkSource(this.channelIdValue, atStep);
      sourceChannelID = resolved.channelId;
      cursor = resolved.cursor;
    }

    const mergedParams: JsonObject = { ...this.params, ...(opts.params ?? {}) };

    const experimentMetadata: JsonObject = {
      session_name: name,
      params: mergedParams,
      forked_from: sourceChannelID,
      forked_from_channel: this.channelIdValue,
      forked_at_step: atStep,
      forked_at_cursor: cursor,
      status: SessionStatus.Created,
    };
    if (opts.intervention !== undefined) {
      experimentMetadata['intervention'] = opts.intervention;
    }

    const displayName = opts.displayName ?? name;
    const reason = opts.reason ?? `Forked from ${this.name} at step ${atStep}`;

    const boundaryMode = opts.boundaryMode ?? 'exact';
    const receipt = await this.performFork(
      sourceChannelID,
      name,
      cursor,
      atStep,
      displayName,
      reason,
      experimentMetadata,
      boundaryMode,
      opts.manifest,
      opts.toolPolicies,
    );
    this.applyForkReceipt(receipt);

    const stateFn = opts.stateFn ?? this.stateFn;
    const session = new AgentSession(this.actae, name, {
      displayName: opts.displayName,
      stateFn,
      snapshotInterval: this.snapshotInterval,
      params: mergedParams,
    });
    session.channelIdValue = name;
    session.applyForkReceipt(receipt);
    session.interventionValue = { ...(opts.intervention ?? {}) };
    await session.primeForkSession(this.channelIdValue, atStep);
    return session;
  }

  /** Resumes a previously-run (or crashed) session.
   *
   * Crash recovery (no forkAtStep): resumes from the last recorded step;
   * raises SessionCompletedError when the channel is already completed.
   *
   * Fork-from-existing (forkAtStep set): forks the source channel at the
   * given step into a fresh channel with name. */
  static async resume(actae: SessionTransport, channelId: string, opts: ResumeOptions = {}): Promise<AgentSession> {
    const meta = await actae.getChannelMetadata(channelId);
    if (!meta) {
      throw new SessionError(`Channel '${channelId}' not found (no metadata)`);
    }

    const expMeta: JsonObject = meta.experimentMetadata ?? {};
    const status = (expMeta['status'] as string) ?? 'unknown';

    if (opts.forkAtStep !== undefined) {
      if (!opts.name) {
        throw new SessionError("'name' is required when forkAtStep is set");
      }
      const session = new AgentSession(actae, opts.name, {
        displayName: meta.displayName,
        stateFn: opts.stateFn,
        snapshotInterval: opts.snapshotInterval,
        params: opts.params,
      });
      session.channelIdValue = opts.name;
      session.interventionValue = { ...(opts.intervention ?? {}) };
      await session.forkFromChannel(
        channelId,
        opts.forkAtStep,
        opts.boundaryMode ?? 'exact',
        opts.manifest,
        opts.intervention,
        opts.toolPolicies,
      );
      return session;
    }

    if (status === SessionStatus.Completed) {
      throw new SessionCompletedError(
        `Session '${channelId}' is already completed. Pass forkAtStep to fork from it.`,
      );
    } else if (status === SessionStatus.Crashed) {
      // resuming crashed session
    }

    const sessionParams: JsonObject = {};
    const stored = expMeta['params'];
    if (stored !== undefined && typeof stored === 'object' && !Array.isArray(stored)) {
      Object.assign(sessionParams, stored);
    }
    Object.assign(sessionParams, opts.params ?? {});

    let cursors: number[] = [];
    const raw = expMeta['cursors'];
    if (Array.isArray(raw)) {
      for (const c of raw) {
        if (typeof c === 'number') cursors.push(Math.trunc(c));
        else if (typeof c === 'string') {
          const n = Number(c);
          if (Number.isSafeInteger(n)) cursors.push(n);
        }
      }
    }
    let stepCount = asInt64(expMeta['step_count']);
    // The durable event log is authoritative: a hard process death skips the
    // metadata write, so reconstruct real progress rather than resuming a
    // hard-killed run back at step 1.
    // Recover true progress from the server-owned step index (O(1)); fall back
    // to a replay scan when unsupported. Only override metadata when the
    // recovered value is AHEAD of it (retention can purge old events; a resume
    // must never go backwards).
    let lastStep: number | undefined;
    try {
      lastStep = await actae.latestStepNumber(channelId);
    } catch {
      lastStep = undefined;
    }
    if (lastStep !== undefined) {
      if (lastStep > stepCount) {
        stepCount = lastStep;
        while (cursors.length < stepCount) cursors.push(0);
      }
    } else {
      const recovered = await recoverStepProgress(actae, channelId);
      if (recovered && recovered.length > stepCount) {
        cursors = recovered;
        stepCount = recovered.length;
      }
    }

    const displayName = meta.displayName && meta.displayName !== '' ? meta.displayName : channelId;

    const session = new AgentSession(actae, displayName, {
      displayName,
      stateFn: opts.stateFn,
      snapshotInterval: opts.snapshotInterval,
      params: sessionParams,
    });
    session.channelIdValue = channelId;
    session.stepCount = stepCount;
    session.cursors = cursors;
    session.startedAt = (expMeta['started_at'] as string) ?? '';
    session.status = SessionStatus.Started;
    session.resumed = true;
    // Preserve a fork's intervention descriptor across crash recovery.
    const storedIntervention = expMeta['intervention'];
    if (storedIntervention !== undefined && typeof storedIntervention === 'object' && !Array.isArray(storedIntervention)) {
      session.interventionValue = { ...(storedIntervention as JsonObject) };
    }
    const snap = await actae.latestState(channelId);
    if (snap) session.inheritedStateValue = snap.state;
    return session;
  }

  /** Resume the exact child named by a fork receipt; never guess "latest". */
  static async resumeFromFork(actae: SessionTransport, receipt: ForkReceipt, opts: ResumeOptions = {}): Promise<AgentSession> {
    if (!receipt.childChannelId) throw new SessionError('fork receipt has no childChannelId');
    return AgentSession.resume(actae, receipt.childChannelId, opts);
  }

  /** Convenience wrapper over the static resume() for this session's
   * channel. */
  async resume(opts: ResumeOptions = {}): Promise<AgentSession> {
    if (this.channelIdValue === '') {
      throw new SessionError('Session has no channel — was it started?');
    }
    return AgentSession.resume(this.actae, this.channelIdValue, opts);
  }

  // -------------------------------------------------------------------------
  // Experiment helpers
  // -------------------------------------------------------------------------

  /** Records this fork's outcome (promoted | rejected | inconclusive |
   * crashed) and an optional numeric result score. */
  async setOutcome(outcome: string, score?: number): Promise<void> {
    const channelId = this.ensureChannel();
    await this.actae.setOutcome(channelId, outcome, score);
  }

  /** Promotes this winning fork into its parent (append-only merge). */
  async promote(): Promise<JsonObject> {
    const channelId = this.ensureChannel();
    return this.actae.promoteChannel(channelId);
  }

  /** Creates an experiment group. */
  async createExperiment(name: string, opts?: { description?: string; baselineChannelId?: string }): Promise<JsonObject> {
    return this.actae.createExperiment(name, opts);
  }

  /** Adds this channel to an experiment group as a variant/baseline. */
  async addToExperiment(
    groupId: string,
    opts: { role?: 'variant' | 'baseline'; declaredDelta?: JsonObject } = {},
  ): Promise<JsonObject> {
    return this.actae.addExperimentMember(groupId, this.ensureChannel(), {
      role: opts.role,
      declaredDelta: opts.declaredDelta,
    });
  }

  // -------------------------------------------------------------------------
  // Internal helpers
  // -------------------------------------------------------------------------

  private ensureChannel(): string {
    if (this.channelIdValue === '') {
      throw new SessionError(`Session '${this.name}' has no channel — call start() first`);
    }
    return this.channelIdValue;
  }

  private requireStatus(...allowed: SessionStatusValue[]): void {
    if (allowed.includes(this.status)) return;
    throw new SessionError(`Session '${this.name}' is ${this.status}, cannot perform operation.`);
  }

  private async updateMetadataStatus(status: SessionStatusValue, extra?: JsonObject): Promise<void> {
    const channelId = this.ensureChannel();
    await this.retryTransient('update_metadata', async () => {
      let existing: JsonObject = {};
      const meta = await this.actae.getChannelMetadata(channelId);
      if (meta?.experimentMetadata) existing = meta.experimentMetadata;

      const stored = this.cursors.slice(-maxStoredCursors);
      const metaMap: JsonObject = {
        session_name: this.name,
        params: this.params,
        status,
        started_at: this.startedAt,
        step_count: this.stepCount,
        cursors: stored,
        cursors_stored: stored.length,
      };
      if (this.cursors.length > maxStoredCursors) {
        metaMap['cursors_warning'] = `Only last ${maxStoredCursors} cursors stored; total steps: ${this.stepCount}`;
      }

      const merged: JsonObject = { ...existing, ...metaMap, ...(extra ?? {}) };
      const displayName = this.displayName !== '' ? this.displayName : this.name;
      await this.actae.updateMetadata(channelId, {
        displayName,
        experimentMetadata: merged,
      });
    });
  }

  private cursorForStep(stepNumber: number): number {
    if (stepNumber < 1) {
      throw new SessionError(`step_number must be >= 1, got ${stepNumber}`);
    }
    if (this.cursors.length === 0) {
      throw new SessionError(
        `Session '${this.name}' has no recorded steps yet — cannot resolve step ${stepNumber}`,
      );
    }
    if (stepNumber > this.cursors.length) {
      throw new SessionError(
        `Step ${stepNumber} exceeds the ${this.cursors.length} steps recorded on session '${this.name}'`,
      );
    }
    return this.cursors[stepNumber - 1] as number;
  }

  /** Resolves the channel that OWNS a step and its cursor, walking the fork
   * lineage and falling back to replay-based position resolution. */
  private async resolveForkSource(
    channelId: string,
    atStep: number,
  ): Promise<{ channelId: string; cursor: number }> {
    if (atStep < 1) {
      throw new SessionError(`fork_at_step must be >= 1, got ${atStep}`);
    }

    // Server-owned step index first: exact, durable and lineage-aware.
    const resolved = await this.actae.resolveStep(channelId, atStep);
    if (resolved) {
      return { channelId: resolved.channelId, cursor: resolved.cursor };
    }

    // Walk up to the owning channel.
    let owner = channelId;
    for (let i = 0; i < maxLineageDepth; i++) {
      const meta = await this.actae.getChannelMetadata(owner);
      if (!meta) break;
      const forkedAtStep = asInt64(meta.experimentMetadata?.['forked_at_step']);
      if (forkedAtStep === 0 || atStep > forkedAtStep) break;
      const parent = meta.parentChannelId;
      if (!parent || parent === owner) break;
      owner = parent;
    }

    // Fast path: cursors list stored in the owner's metadata.
    let cursor: number | undefined;
    const ownerMeta = await this.actae.getChannelMetadata(owner);
    if (ownerMeta?.experimentMetadata) {
      const raw = ownerMeta.experimentMetadata['cursors'];
      if (Array.isArray(raw) && atStep <= raw.length) {
        const c = raw[atStep - 1];
        if (typeof c === 'number') cursor = Math.trunc(c);
        else if (typeof c === 'string') {
          const n = Number(c);
          if (Number.isSafeInteger(n)) cursor = n;
        }
      }
    }

    // Fallback: replay the owner's events and resolve by position.
    if (cursor === undefined) {
      const sourceEvents: Event[] = [];
      let pageCursor: number | undefined;
      for (;;) {
        let batch: Event[];
        try {
          batch = await this.actae.replay(owner, {
            cursor: pageCursor,
            limit: 1000,
          } as ReplayOptions);
        } catch {
          break;
        }
        sourceEvents.push(...batch);
        if (batch.length === 0 || atStep <= sourceEvents.length) break;
        const last = batch[batch.length - 1] as Event;
        if (pageCursor !== undefined && last.cursor <= pageCursor) break;
        pageCursor = last.cursor;
      }
      if (sourceEvents.length === 0) {
        throw new SessionError(`Channel '${owner}' has no events — cannot resolve step ${atStep}`);
      }
      const filtered = sourceEvents.filter(
        (ev) => ev.eventType !== 'fork.started' && ev.eventType !== SESSION_STARTED && ev.eventType !== SESSION_COMPLETED,
      );
      if (filtered.length === 0) {
        throw new SessionError(`Channel '${owner}' has no user events — cannot resolve step ${atStep}`);
      }
      if (atStep > filtered.length) {
        throw new SessionError(
          `Step ${atStep} exceeds the ${filtered.length} steps recorded on channel '${owner}'`,
        );
      }
      // Validate cursor monotonicity before trusting position-based mapping.
      for (let i = 1; i < filtered.length; i++) {
        const prev = filtered[i - 1] as Event;
        const cur = filtered[i] as Event;
        if (cur.cursor <= prev.cursor) {
          throw new SessionError(
            `Channel '${owner}' has non-monotonic cursors (event ${i}: cursor ${cur.cursor} <= cursor ${prev.cursor}). Position-based step resolution is unreliable for this channel. Fork at a raw cursor instead.`,
          );
        }
      }
      cursor = (filtered[atStep - 1] as Event).cursor;
    }

    return { channelId: owner, cursor };
  }

  private async forkFromChannel(
    sourceChannelID: string,
    atStep: number,
    boundaryMode: string,
    manifest?: JsonObject,
    intervention?: JsonObject,
    toolPolicies?: JsonObject,
  ): Promise<void> {
    if (atStep < 1) {
      throw new SessionError(`fork_at_step must be >= 1, got ${atStep}`);
    }
    const resolved = await this.resolveForkSource(sourceChannelID, atStep);
    const owner = resolved.channelId;
    const cursor = resolved.cursor;

    const reason = `Forked from ${owner} at step ${atStep} (cursor ${cursor})`;
    const forkMetadata: JsonObject = {
      session_name: this.name,
      params: this.params,
      forked_from: owner,
      forked_at_step: atStep,
      forked_at_cursor: cursor,
      status: SessionStatus.Created,
    };
    if (intervention !== undefined) {
      forkMetadata['intervention'] = intervention;
    }
    const receipt = await this.performFork(owner, this.name, cursor, atStep, this.name, reason, forkMetadata, boundaryMode, manifest, toolPolicies);
    this.applyForkReceipt(receipt);
    this.interventionValue = { ...(intervention ?? {}) };
    await this.primeForkSession(sourceChannelID, atStep);
  }

  /** Primes a fork session so it continues at atStep + 1 with the parent's
   * inherited state. Sets stepCount, restores the parent's cursors (up to
   * the fork step), loads the inherited state snapshot, and makes stateFn
   * return it. */
  private async primeForkSession(sourceChannelID: string, atStep: number): Promise<void> {
    this.stepCount = atStep;
    const sourceMeta = await this.actae.getChannelMetadata(sourceChannelID);
    if (sourceMeta?.experimentMetadata) {
      const raw = sourceMeta.experimentMetadata['cursors'];
      if (Array.isArray(raw)) {
        const cursors: number[] = [];
        for (let i = 0; i < Math.min(atStep, raw.length); i++) {
          const c = raw[i];
          if (typeof c === 'number') cursors.push(Math.trunc(c));
          else if (typeof c === 'string') {
            const n = Number(c);
            if (Number.isSafeInteger(n)) cursors.push(n);
          }
        }
        this.cursors = cursors;
      }
    }

    let inherited: JsonObject | undefined;
    const snap = await this.actae.latestState(this.channelIdValue);
    if (snap) inherited = snap.state;

    // Never silently prime a fork with contaminated state: when the fork
    // resolved BEYOND the requested boundary, the inherited snapshot
    // contains data from after the fork point. Refuse to expose it as steps
    // 1..N's data.
    if (this.requestedBoundary > 0 && this.resolvedBoundary > this.requestedBoundary) {
      this.errorCb?.(
        `fork '${this.name}' inherited cursor ${this.resolvedBoundary} but requested ${this.requestedBoundary} — NOT priming with the contaminated inherited state. inherited_state is undefined; set boundaryMode='approximate' explicitly if you intend to continue from the latest state.`,
      );
      inherited = undefined;
    }

    this.inheritedStateValue = inherited;
    // The caller's stateFn is intentionally NOT wrapped: it must reflect the
    // fork's evolving live state, and `inheritedState` is the seed the caller
    // folds into that live state once. (Python parity — wrapping it to always
    // return the inherited prefix would make the fork channel persist only
    // the inherited steps, never the fork's own output.)
  }

  private async performFork(
    sourceChannel: string,
    childName: string,
    cursor: number,
    atStep: number,
    displayName: string,
    reason: string,
    experimentMetadata: JsonObject,
    boundaryMode: string,
    manifest?: JsonObject,
    toolPolicies?: JsonObject,
  ): Promise<ForkReceipt> {
    if (boundaryMode !== 'exact' && boundaryMode !== 'approximate' && boundaryMode !== 'lineage_only') {
      throw new SessionError(
        `invalid boundary_mode "${boundaryMode}" (expected exact|approximate|lineage_only)`,
      );
    }

    const doFork = (c: number) =>
      this.actae.fork(sourceChannel, childName, c, {
        displayName,
        reason,
        experimentMetadata,
        manifest: buildManifest(manifest),
        toolPolicies,
      });

    let receipt: ForkReceipt;
    try {
      receipt = await doFork(cursor);
    } catch (err) {
      if (!(err instanceof SnapshotBoundaryError)) throw err;
      if (boundaryMode === 'exact') {
        throw new NoRestorableCheckpointError(
          `No restorable checkpoint at step ${atStep} (cursor ${cursor}) on channel "${sourceChannel}": no saved snapshot at or before the boundary. Save state on every step (StateFn + snapshotInterval=1) or fork with boundaryMode='approximate' / 'lineage_only'.`,
        );
      }
      this.errorCb?.(
        `no saved state at step ${atStep} cursor ${cursor} on "${sourceChannel}" — boundary_mode=${boundaryMode} forking from the latest state (inherited state is NOT an exact checkpoint)`,
      );
      receipt = await doFork(0);
      receipt.requestedCursor = cursor;
    }

    if (receipt.requestedCursor > 0 && receipt.resolvedCursor > receipt.requestedCursor) {
      this.errorCb?.(
        `fork "${childName}" resolved to cursor ${receipt.resolvedCursor} which is BEYOND the requested ${receipt.requestedCursor} — inherited state is not an exact checkpoint (temporal contamination risk). Inspect resolvedBoundaryCursor.`,
      );
    }
    return receipt;
  }

  private applyForkReceipt(r: ForkReceipt): void {
    this.boundaryRestorableValue = r.restorable;
    this.requestedBoundary = r.requestedCursor;
    this.resolvedBoundary = r.resolvedCursor;
    this.sourceStateVersion = r.sourceStateVersion;
    this.sourceStateSha256Value = r.sourceStateSha256 ?? '';
    this.reproducibilityValue = r.reproducibility ?? '';
    this.toolPoliciesValue = r.toolPolicies;
    this.forkReceiptValue = r;
  }

  // -------------------------------------------------------------------------
  // Retry helpers
  // -------------------------------------------------------------------------

  private isRetryable(err: unknown): boolean {
    if (err instanceof ConnectionError) return true;
    if (err instanceof APIError) {
      return err.statusCode >= 500 || err.statusCode === 429;
    }
    return false;
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  private async retryTransient(op: string, fn: () => Promise<void>): Promise<void> {
    let lastErr: unknown;
    for (let attempt = 1; attempt <= maxSessionRetries + 1; attempt++) {
      try {
        await fn();
        return;
      } catch (err) {
        lastErr = err;
        if (!this.isRetryable(err) || attempt > maxSessionRetries) throw err;
        const delay = Math.min(retryBaseDelay * 2 ** (attempt - 1), retryMaxDelay);
        this.errorCb?.(`${op} attempt ${attempt}/${maxSessionRetries + 1} failed — retrying in ${delay}ms: ${(err as Error).message}`);
        await this.sleep(delay);
      }
    }
    throw lastErr;
  }

  private async retryTransientEvent(op: string, fn: () => Promise<Event>): Promise<Event> {
    let lastErr: unknown;
    for (let attempt = 1; attempt <= maxSessionRetries + 1; attempt++) {
      try {
        return await fn();
      } catch (err) {
        lastErr = err;
        if (!this.isRetryable(err) || attempt > maxSessionRetries) throw err;
        const delay = Math.min(retryBaseDelay * 2 ** (attempt - 1), retryMaxDelay);
        this.errorCb?.(`${op} attempt ${attempt}/${maxSessionRetries + 1} failed — retrying in ${delay}ms: ${(err as Error).message}`);
        await this.sleep(delay);
      }
    }
    throw lastErr;
  }
}

// ---------------------------------------------------------------------------
// Manifest building
// ---------------------------------------------------------------------------

/** Merges caller-supplied manifest fields over an auto-detected environment
 * fingerprint (runtime version + SDK version). The server grades
 * context_exact only when model + environment + dependencies + seed are all
 * present, so callers must supply those to claim context-exact
 * reproducibility. */
export function buildManifest(caller?: JsonObject): JsonObject {
  const out: JsonObject = {
    environment: {
      node: process.versions.node,
      platform: `${process.platform}/${process.arch}`,
      actae: SDK_VERSION,
    } as JsonObject,
  };
  if (caller) {
    for (const [k, v] of Object.entries(caller)) out[k] = v;
  }
  return out;
}
