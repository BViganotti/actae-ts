/**
 * GitHub Copilot SDK integration for Actae (Go `actae/copilot` parity).
 *
 * Every Copilot session event — user messages, assistant turns and answers,
 * tool executions, errors, sub-agents, permission requests — is recorded as a
 * first-class Actae event on a per-session channel, giving you:
 *
 *   - a complete, replayable timeline of every Copilot session,
 *   - session hooks (pre/post tool use, user prompts, session start/end,
 *     errors, MCP calls) recorded as events,
 *   - fork an entire session at a specific event into a new experiment
 *     fork, and
 *   - cursor-aligned state snapshots for crash recovery and resumption.
 *
 * ```ts
 * import { CopilotManager } from '@actae/sdk/adapters/copilot';
 * import { CopilotClient } from '@github/copilot-sdk';
 *
 * const mgr = new CopilotManager(actae);
 * const handle = await mgr.startSession(client, { configurable: {} });
 * await handle.session.send({ type: 'user.message', text: 'Refactor the parser' });
 * ...
 * await mgr.stopAll();
 * ```
 *
 * The adapter is structural: it only requires a Copilot session with `on`,
 * `getEvents` and `sessionId`, so it works with the official `@github/copilot-sdk`
 * (optional peer) and is testable with a fake.
 */

import type { ActaeClient } from '../client.js';
import { stringify, type JsonObject, type JsonValue } from '../json.js';
import type { StateVersionInfo } from '../types.js';
import { adapterCheckpointState, stripCheckpointMetadata } from './contract.js';

export class CopilotError extends Error {}

// ---------------------------------------------------------------------------
// Types (structural — mirror @github/copilot-sdk SessionEvent / Session)
// ---------------------------------------------------------------------------

export interface CopilotSessionEvent {
  id: string;
  type: string;
  data?: unknown;
  parentId?: string | null;
  agentId?: string | null;
  ephemeral?: boolean | null;
}

export interface CopilotSessionLike {
  readonly sessionId: string;
  on(handler: (event: CopilotSessionEvent) => void): void | (() => void);
  getEvents?(): Promise<CopilotSessionEvent[]>;
}

export interface CopilotClientLike {
  createSession(config: Record<string, unknown>): Promise<CopilotSessionLike>;
  resumeSession(sessionId: string, config: Record<string, unknown>): Promise<CopilotSessionLike>;
  getSessionMetadata?(sessionId: string): Promise<Record<string, unknown> | undefined>;
}

export interface CopilotRecorderOptions {
  /** Actae channel prefix for unattached sessions ("copilot:<session-id>"). */
  channelPrefix?: string;
  /** Overrides the Actae channel for the recorded session. */
  channelId?: string;
  /** Actor stamped on every recorded event (default "copilot"). */
  actor?: string;
  /** Bounds the async record queue (default 1024). */
  queueSize?: number;
  /** Records transient/ephemeral events (default: skipped). */
  includeEphemeral?: boolean;
  /** Restricts recording to the given copilot event types. */
  eventTypes?: string[];
}

interface RecordJob {
  eventType: string;
  payload: JsonValue;
  metadata: JsonObject;
}

const DEFAULT_ACTOR = 'copilot';
const DEFAULT_QUEUE_SIZE = 1024;
const MAX_SEEN_EVENTS = 5000;

/** Asynchronously records Copilot session events into a single Actae channel.
 * Recording is non-blocking (queue + worker); transient Actae failures are
 * logged and dropped — the recorder never stalls the agent. */
export class CopilotRecorder {
  readonly actae: ActaeClient;
  private readonly opts: Required<Pick<CopilotRecorderOptions, 'channelPrefix' | 'actor' | 'queueSize' | 'includeEphemeral'>> & CopilotRecorderOptions;

  private session?: CopilotSessionLike;
  private channel = '';
  private attached = false;
  private sessionEnded = false;

  private readonly jobs: RecordJob[] = [];
  private running = false;
  private readonly seen = new Set<string>();
  private readonly seenOrder: string[] = [];
  private readonly eventCursors = new Map<string, number>();
  private unsub?: () => void;

  constructor(actae: ActaeClient, options: CopilotRecorderOptions = {}) {
    this.actae = actae;
    this.opts = {
      channelPrefix: options.channelPrefix ?? 'copilot',
      actor: options.actor ?? DEFAULT_ACTOR,
      queueSize: options.queueSize ?? DEFAULT_QUEUE_SIZE,
      includeEphemeral: options.includeEphemeral ?? false,
      ...options,
    };
  }

  /** The Actae channel backing this recorder ("" until attach). */
  channelId(): string {
    return this.channel;
  }

  /** The attached Copilot session ID ("" before attach). */
  sessionId(): string {
    return this.session?.sessionId ?? '';
  }

  /** Binds the recorder to a Copilot session, starts the worker, registers
   * the event handler and records the session.started lifecycle event. */
  async attach(session: CopilotSessionLike): Promise<void> {
    if (this.attached) return;
    this.session = session;
    this.channel = this.opts.channelId ?? `${this.opts.channelPrefix}:${session.sessionId}`;
    this.attached = true;

    this.startWorker();
    const unsub = session.on((event) => this.handleEvent(event));
    if (typeof unsub === 'function') this.unsub = unsub;

    try {
      const ev = await this.actae.record(this.channel, 'copilot.session.started', {
        session_id: session.sessionId,
      } as JsonValue, {
        actor: this.opts.actor,
        metadata: { session_id: session.sessionId, source: 'recorder.attach' } as JsonObject,
      });
      this.eventCursors.set(`session.started:${session.sessionId}`, ev.cursor);
    } catch {
      // best-effort
    }
  }

  /** Stops the worker after draining the queue. Safe to call multiple times. */
  async stop(): Promise<void> {
    this.unsub?.();
    this.unsub = undefined;
    await this.drain();
    this.running = false;
  }

  /** Returns the Actae cursor recorded for a Copilot event ID. */
  cursorForEvent(eventId: string): number | undefined {
    return this.eventCursors.get(eventId);
  }

  /** Persists an arbitrary event type on the recorder's channel. */
  async record(eventType: string, payload: JsonValue, metadata: JsonObject = {}): Promise<number> {
    const channel = this.channel;
    if (channel === '') throw new CopilotError('recorder not attached to a session');
    if (metadata['session_id'] === undefined) metadata['session_id'] = this.sessionId();
    const ev = await this.actae.record(channel, eventType, payload, {
      actor: this.opts.actor,
      metadata,
    });
    return ev.cursor;
  }

  /** Queues a session-hook invocation for recording. Fires immediately. */
  recordHook(name: string, sessionId: string, input: unknown): void {
    this.push({
      eventType: `copilot.hook.${name}`,
      payload: { hook: name, input: safeJson(input) } as JsonValue,
      metadata: { session_id: sessionId } as JsonObject,
    });
  }

  /** Records the copilot.session.ended lifecycle event at most once. */
  async recordSessionEnded(reason: string): Promise<void> {
    if (this.sessionEnded) return;
    this.sessionEnded = true;
    const channel = this.channel;
    if (channel === '') return;
    try {
      await this.actae.record(channel, 'copilot.session.ended', {
        session_id: this.sessionId(),
        reason,
      } as JsonValue, {
        actor: this.opts.actor,
        metadata: { session_id: this.sessionId(), reason } as JsonObject,
      });
    } catch {
      // best-effort
    }
  }

  /** Replays the session's full event history and records events not already
   * seen (live-attach recovery after a restart). Dedup is by Copilot event
   * ID. Returns the number of newly recorded events. */
  async backfill(): Promise<number> {
    if (!this.session?.getEvents) {
      throw new CopilotError('recorder not attached to a session with getEvents');
    }
    const events = await this.session.getEvents();
    let recorded = 0;
    for (const event of events) {
      if (this.seen.has(event.id)) continue;
      this.handleEvent(event);
      recorded++;
    }
    return recorded;
  }

  private handleEvent(event: CopilotSessionEvent): void {
    if (event.data === undefined || event.data === null) return;
    if (event.ephemeral && !this.opts.includeEphemeral) return;
    if (this.opts.eventTypes && !this.opts.eventTypes.includes(event.type)) return;
    if (this.seen.has(event.id)) return;

    this.seen.add(event.id);
    this.seenOrder.push(event.id);
    if (this.seenOrder.length > MAX_SEEN_EVENTS) {
      const old = this.seenOrder.shift()!;
      this.seen.delete(old);
    }

    const metadata: JsonObject = { session_id: this.sessionId(), event_id: event.id };
    if (event.parentId) metadata['parent_event_id'] = event.parentId;
    if (event.agentId) metadata['agent_id'] = event.agentId;
    if (event.ephemeral !== undefined) metadata['ephemeral'] = event.ephemeral;

    this.push({
      eventType: `copilot.${event.type}`,
      payload: { event_type: event.type, data: safeJson(event.data) } as JsonValue,
      metadata,
    });
  }

  private push(job: RecordJob): void {
    if (this.jobs.length >= this.opts.queueSize) {
      console.warn(`copilot: record queue full, dropping ${job.eventType}`);
      return;
    }
    this.jobs.push(job);
    if (!this.running) this.startWorker();
  }

  private startWorker(): void {
    if (this.running) return;
    this.running = true;
    void (async () => {
      while (this.jobs.length > 0) {
        const job = this.jobs.shift()!;
        await this.persist(job);
      }
      this.running = false;
    })();
  }

  private async drain(): Promise<void> {
    while (this.jobs.length > 0) {
      const job = this.jobs.shift()!;
      await this.persist(job);
    }
  }

  private async persist(job: RecordJob): Promise<void> {
    let channel = this.channel;
    if (channel === '') {
      const sid = job.metadata['session_id'];
      if (typeof sid === 'string' && sid !== '') channel = `${this.opts.channelPrefix}:${sid}`;
    }
    if (channel === '') {
      console.warn(`copilot: dropping ${job.eventType} — recorder has no channel`);
      return;
    }
    try {
      const ev = await this.actae.record(channel, job.eventType, job.payload, {
        actor: this.opts.actor,
        metadata: job.metadata,
      });
      const eventId = job.metadata['event_id'];
      if (typeof eventId === 'string' && eventId !== '') {
        this.eventCursors.set(eventId, ev.cursor);
      }
    } catch (err) {
      console.warn(`copilot: record ${job.eventType} failed: ${(err as Error).message}`);
    }
  }
}

function safeJson(value: unknown): JsonValue {
  if (value === undefined) return null;
  if (
    value === null ||
    typeof value === 'boolean' ||
    typeof value === 'number' ||
    typeof value === 'bigint' ||
    typeof value === 'string'
  ) {
    return value as JsonValue;
  }
  try {
    return JSON.parse(stringify(value)) as JsonValue;
  } catch {
    return String(value);
  }
}

// ---------------------------------------------------------------------------
// Manager
// ---------------------------------------------------------------------------

export interface CopilotManagerOptions {
  /** Actae channel naming prefix for sessions (default "copilot"). */
  channelPrefix?: string;
  /** Default recorder options for tracked sessions. */
  recorder?: CopilotRecorderOptions;
}

export interface SessionHandle {
  session: CopilotSessionLike;
  recorder: CopilotRecorder;
  channel: string;
}

/** Owns one Recorder per tracked Copilot session and maps Copilot session IDs
 * to Actae channels. Safe for concurrent use. */
export class CopilotManager {
  readonly actae: ActaeClient;
  private readonly prefix: string;
  private readonly recOpts: CopilotRecorderOptions;
  private readonly recorders = new Map<string, CopilotRecorder>();

  constructor(actae: ActaeClient, options: CopilotManagerOptions = {}) {
    this.actae = actae;
    this.prefix = options.channelPrefix ?? 'copilot';
    this.recOpts = options.recorder ?? {};
  }

  /** The Actae channel for a Copilot session. */
  channelFor(sessionId: string): string {
    return `${this.prefix}:${sessionId}`;
  }

  /** Creates a Copilot session with hooks wired for recording, tracks it,
   * and returns the handle. */
  async startSession(client: CopilotClientLike, config: Record<string, unknown>): Promise<SessionHandle> {
    const recorder = this.newRecorder(this.recOpts);
    const session = await client.createSession(config);
    await recorder.attach(session);
    this.track(session.sessionId, recorder);
    await this.recordSessionInfo(client, recorder);
    return { session, recorder, channel: recorder.channelId() };
  }

  /** Resumes an existing Copilot session with hooks wired for recording. */
  async resumeSession(client: CopilotClientLike, sessionId: string, config: Record<string, unknown>): Promise<SessionHandle> {
    const recorder = this.newRecorder(this.recOpts);
    const session = await client.resumeSession(sessionId, config);
    await recorder.attach(session);
    this.track(session.sessionId, recorder);
    await this.recordSessionInfo(client, recorder);
    return { session, recorder, channel: recorder.channelId() };
  }

  /** The recorder tracking a session, or undefined. */
  recorderFor(sessionId: string): CopilotRecorder | undefined {
    return this.recorders.get(sessionId);
  }

  /** Records the copilot.session.ended lifecycle event for a tracked session
   * (at most once; idempotent with the session_end hook). */
  async endSession(sessionId: string, reason: string): Promise<void> {
    const rec = this.recorderFor(sessionId);
    if (!rec) throw new CopilotError(`session ${sessionId} is not tracked`);
    await rec.recordSessionEnded(reason);
  }

  /** Stops and forgets a session's recorder. */
  async untrack(sessionId: string): Promise<void> {
    const rec = this.recorders.get(sessionId);
    this.recorders.delete(sessionId);
    if (rec) await rec.stop();
  }

  /** Stops every tracked recorder. Safe to call multiple times. */
  async stopAll(): Promise<void> {
    const recs = [...this.recorders.values()];
    this.recorders.clear();
    await Promise.all(recs.map((r) => r.stop()));
  }

  /** Forks a tracked session's channel at the cursor of a specific Copilot
   * event (identified by its event ID). */
  async fork(
    sessionId: string,
    atEventId: string,
    options: { newChannelId: string; displayName?: string; reason?: string },
  ): Promise<string> {
    const rec = this.recorderFor(sessionId);
    if (!rec) throw new CopilotError(`session ${sessionId} is not tracked`);
    const sourceChannel = rec.channelId() || this.channelFor(sessionId);

    const cursor = rec.cursorForEvent(atEventId);
    if (cursor === undefined) {
      throw new CopilotError(`event ${atEventId} not resolved to a cursor on ${sourceChannel}`);
    }

    const newChannel = options.newChannelId;
    const displayName = options.displayName ?? newChannel;
    const reason = options.reason ?? `Forked from ${sourceChannel} at event ${atEventId}`;

    await this.actae.fork(sourceChannel, newChannel, cursor, {
      displayName,
      reason,
      experimentMetadata: {
        session_name: newChannel,
        forked_from: sourceChannel,
        forked_at_event: atEventId,
        forked_at_cursor: cursor,
        status: 'created',
      } as JsonObject,
    });
    return newChannel;
  }

  /** Saves a cursor-aligned state snapshot for a session (StateManager
   * semantics). Returns the assigned version. */
  async snapshot(sessionId: string, state: JsonObject): Promise<number> {
    const channel = this.recorderFor(sessionId)?.channelId() || this.channelFor(sessionId);
    const cursor = (await this.actae.latestCursor(channel)) ?? 0;
    return this.actae.saveState(channel, cursor, adapterCheckpointState(state, {
      framework: 'github-copilot-sdk',
      channelId: channel,
      portableState: state,
      nativeCheckpoint: { session_id: sessionId },
      eventCursor: cursor,
    }));
  }

  /** Returns the latest saved state for a session, or undefined. */
  async loadSnapshot(sessionId: string): Promise<JsonObject | undefined> {
    const channel = this.recorderFor(sessionId)?.channelId() || this.channelFor(sessionId);
    const snap = await this.actae.latestState(channel);
    return snap ? stripCheckpointMetadata(snap.state) : undefined;
  }

  /** Lists state version history for a session (metadata only, newest-first). */
  async listSnapshots(sessionId: string): Promise<StateVersionInfo[]> {
    const channel = this.recorderFor(sessionId)?.channelId() || this.channelFor(sessionId);
    return this.actae.listStates(channel);
  }

  // -------------------------------------------------------------------------

  private newRecorder(opts: CopilotRecorderOptions): CopilotRecorder {
    return new CopilotRecorder(this.actae, { ...opts, channelPrefix: this.prefix });
  }

  private track(sessionId: string, rec: CopilotRecorder): void {
    this.recorders.set(sessionId, rec);
  }

  private async recordSessionInfo(client: CopilotClientLike, rec: CopilotRecorder): Promise<void> {
    if (!client.getSessionMetadata) return;
    const sessionId = rec.sessionId();
    try {
      const meta = await client.getSessionMetadata(sessionId);
      if (!meta) return;
      const payload: JsonObject = { session_id: sessionId };
      for (const key of ['start_time', 'modified_time', 'summary', 'context'] as const) {
        if (meta[key] !== undefined) payload[key] = meta[key] as JsonValue;
      }
      if (meta['is_remote'] !== undefined) payload['is_remote'] = meta['is_remote'] as boolean;
      await rec.record('copilot.session.info', payload as JsonValue, { session_id: sessionId });
    } catch (err) {
      console.warn(`copilot: fetch session metadata for ${sessionId} failed: ${(err as Error).message}`);
    }
  }
}
