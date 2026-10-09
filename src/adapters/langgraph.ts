/**
 * LangGraph.js Checkpointer adapter for Actae.
 *
 * Implements the LangGraph.js checkpoint-saver protocol (structural — no
 * runtime dependency on `@langchain/langgraph`) using Actae as the backend.
 * Every checkpoint is stored as a cursor-aligned, immutable state snapshot,
 * enabling deterministic resume with full context.
 *
 * ```ts
 * import { ActaeCheckpointSaver } from '@actae/sdk/adapters/langgraph';
 * const saver = new ActaeCheckpointSaver(actae);
 * const graph = builder.compile({ checkpointer: saver });
 * await graph.ainvoke(input, { configurable: { thread_id: '1' } });
 *
 * // fork & resume:
 * const forkCfg = await saver.forkThread(
 *   { configurable: { thread_id: '1' } },
 *   { newThreadId: '1-fix', reason: 'refine node 5' },
 * );
 * await graph.ainvoke(null, forkCfg);
 * ```
 *
 * Design mirrors the Python adapter: LangGraph state is treated as opaque,
 * checkpoints/metadata/pending-writes are serialized as base64 envelopes, and
 * the channel is a deterministic prefix + bounded SHA-256 identity segment
 * derived from `thread_id` and `checkpoint_ns`.
 */

import { createHash } from 'node:crypto';

import type { ActaeClient } from '../client.js';
import { adapterCheckpointState } from './contract.js';
import { asInt64, asMap, asString, stringify, type JsonObject, type JsonValue } from '../json.js';
import type { ChannelMetadata } from '../types.js';

export class ActaeLangGraphError extends Error {}

const ENVELOPE_VERSION = 1;
const IDENTITY_DIGEST_BYTES = 16;
const PAGE_SIZE = 100;
const MAX_DEFERRED_WRITES = 256;
const EVENT_ACTOR = 'langgraph-checkpointer';
const EVENT_CHECKPOINT = 'langgraph.checkpoint';
const EVENT_PENDING_WRITES = 'langgraph.pending_writes';
const EMPTY_TYPE = '__empty__';

// ---------------------------------------------------------------------------
// Serde
// ---------------------------------------------------------------------------

/** Minimal typed serializer compatible with LangGraph.js's
 * `SerializerProtocol` ({ dumpsTyped, loadsTyped }). Falls back to JSON with
 * a type tag. Pass LangGraph's own `JsonPlusSerializer` for full fidelity. */
export interface ActaeSerializer {
  dumpsTyped(value: unknown): [string, Uint8Array];
  loadsTyped(data: [string, Uint8Array]): unknown;
}

export class JsonFallbackSerializer implements ActaeSerializer {
  dumpsTyped(value: unknown): [string, Uint8Array] {
    const tag = value === null ? 'null' : Array.isArray(value) ? 'list' : typeof value;
    const json = stringify(value);
    return [tag, new TextEncoder().encode(json)];
  }

  loadsTyped(data: [string, Uint8Array]): unknown {
    const [, bytes] = data;
    const json = new TextDecoder().decode(bytes);
    return JSON.parse(json);
  }
}

// ---------------------------------------------------------------------------
// Channel resolution
// ---------------------------------------------------------------------------

function digest(...parts: string[]): string {
  return createHash('sha256').update(parts.join('\x00')).digest('hex').slice(0, IDENTITY_DIGEST_BYTES);
}

export function channelForConfig(
  config: Record<string, unknown>,
  options: { channel?: string; channelResolver?: (config: Record<string, unknown>) => string },
): string {
  const threadId = requireThreadId(config);
  if (options.channelResolver) {
    const resolved = options.channelResolver(config);
    if (typeof resolved !== 'string' || resolved === '') {
      throw new ActaeLangGraphError('channelResolver must return a non-empty string');
    }
    return resolved;
  }
  const ns = checkpointNs(config);
  return `${options.channel ?? 'langgraph'}:${digest(threadId, ns)}`;
}

function requireThreadId(config: Record<string, unknown>): string {
  if (!config || typeof config !== 'object') {
    throw new ActaeLangGraphError(`config must be an object, got ${typeof config}`);
  }
  const configurable = (config['configurable'] ?? {}) as Record<string, unknown>;
  const threadId = configurable['thread_id'];
  if (!threadId || typeof threadId !== 'string' || threadId === '') {
    throw new ActaeLangGraphError(
      "config['configurable']['thread_id'] is required and must be a non-empty string",
    );
  }
  return threadId;
}

function checkpointNs(config: Record<string, unknown>): string {
  const configurable = (config['configurable'] ?? {}) as Record<string, unknown>;
  const ns = configurable['checkpoint_ns'];
  return typeof ns === 'string' ? ns : '';
}

function checkpointIdOf(config?: Record<string, unknown>): string | undefined {
  if (!config) return undefined;
  const configurable = (config['configurable'] ?? {}) as Record<string, unknown>;
  const id = configurable['checkpoint_id'];
  return typeof id === 'string' && id !== '' ? id : undefined;
}

// ---------------------------------------------------------------------------
// Envelope helpers
// ---------------------------------------------------------------------------

function pack(serde: ActaeSerializer, value: unknown): { t: string; b: string } {
  if (value instanceof EmptyChannelErrorSentinel) {
    return { t: EMPTY_TYPE, b: '' };
  }
  const [tag, blob] = serde.dumpsTyped(value);
  return { t: tag, b: Buffer.from(blob).toString('base64') };
}

function unpack(serde: ActaeSerializer, entry: { t?: string; b?: string } | unknown): unknown {
  if (!entry || typeof entry !== 'object') {
    throw new ActaeLangGraphError(`corrupt envelope entry: ${String(entry)}`);
  }
  const e = entry as { t?: string; b?: string };
  if (e.t === EMPTY_TYPE) return new EmptyChannelErrorSentinel();
  const blob = Buffer.from(e.b ?? '', 'base64');
  return serde.loadsTyped([e.t ?? 'json', new Uint8Array(blob)]);
}

/** Marker for LangGraph's EmptyChannelError (no value in a channel). */
class EmptyChannelErrorSentinel {
  readonly __empty = true;
}

// ---------------------------------------------------------------------------
// Checkpoint saver
// ---------------------------------------------------------------------------

export interface ActaeCheckpointSaverOptions {
  /** Channel prefix for checkpoint storage (default "langgraph"). */
  channel?: string;
  /** Optional config → channel resolver replacing prefix+digest. */
  channelResolver?: (config: Record<string, unknown>) => string;
  /** Optional LangGraph SerializerProtocol for lossless packing. */
  serde?: ActaeSerializer;
}

export interface CheckpointTuple {
  config: Record<string, unknown>;
  checkpoint: Record<string, unknown>;
  metadata: Record<string, unknown>;
  parentConfig?: Record<string, unknown>;
  pendingWrites?: Array<[string, string, unknown]>;
}

/** LangGraph.js checkpoint-saver backed by Actae state snapshots.
 *
 * Implements the saver protocol structurally (getNextVersion, put,
 * putWrites, getTuple, list, deleteThread) plus forkThread — no
 * `@langchain/langgraph` import is required, so the adapter works even when
 * the framework is absent (it is only used by LangGraph at runtime). */
export class ActaeCheckpointSaver {
  readonly actae: ActaeClient;
  readonly channel: string;
  private readonly channelResolver?: (config: Record<string, unknown>) => string;
  private readonly serde: ActaeSerializer;
  private readonly threadNamespaces = new Map<string, Set<string>>();
  private readonly deferredWrites = new Map<string, Map<string, JsonObject>>();
  private inflight = Promise.resolve();

  constructor(actae: ActaeClient, options: ActaeCheckpointSaverOptions = {}) {
    const channel = options.channel ?? 'langgraph';
    if (!channel || typeof channel !== 'string') {
      throw new TypeError('channel must be a non-empty string');
    }
    for (const m of ['record', 'saveState', 'latestState', 'listStates', 'getState'] as const) {
      if (typeof (actae as unknown as Record<string, unknown>)[m] !== 'function') {
        throw new TypeError(`actae must implement ${m}`);
      }
    }
    this.actae = actae;
    this.channel = channel;
    this.channelResolver = options.channelResolver;
    this.serde = options.serde ?? new JsonFallbackSerializer();
  }

  get configSpecs(): Array<{ id: string; scope: string; default: string }> {
    return [
      { id: 'thread_id', scope: 'checkpoint', default: '' },
      { id: 'checkpoint_ns', scope: 'checkpoint', default: '' },
      { id: 'checkpoint_id', scope: 'checkpoint', default: '' },
    ];
  }

  /** Monotonic string channel versions (matches InMemorySaver). */
  getNextVersion(current: unknown, _channelVersions?: Record<string, unknown>): string {
    let currentV = 0;
    if (typeof current === 'number') currentV = current;
    else if (typeof current === 'string') {
      currentV = Number(current.split('.')[0]);
      if (Number.isNaN(currentV)) currentV = 0;
    }
    const nextV = currentV + 1;
    const nextH = Math.random().toString(16).slice(2, 18);
    return `${String(nextV).padStart(32, '0')}.${nextH.padEnd(16, '0')}`;
  }

  channelForConfig(config: Record<string, unknown>): string {
    return channelForConfig(config, {
      channel: this.channel,
      channelResolver: this.channelResolver,
    });
  }

  /** Serializes the current envelope as JSON (used by tests/audit). */
  private recordNamespace(threadId: string, ns: string): void {
    const set = this.threadNamespaces.get(threadId) ?? new Set<string>();
    set.add(ns);
    this.threadNamespaces.set(threadId, set);
  }

  // -------------------------------------------------------------------------
  // Persistence
  // -------------------------------------------------------------------------

  private async saveEnvelope(channel: string, envelope: JsonObject, eventType: string): Promise<void> {
    const event = await this.actae.record(channel, eventType, {
      checkpoint_id: envelope['checkpoint_id'] ?? null,
    } as JsonObject, { actor: EVENT_ACTOR });
    envelope['event_cursor'] = event.cursor;
    envelope['event_id'] = event.id;
    await this.actae.saveState(channel, event.cursor, adapterCheckpointState(envelope, {
      framework: 'langgraph',
      channelId: channel,
      portableState: {
        thread_id: envelope['thread_id'] ?? null,
        checkpoint_ns: envelope['checkpoint_ns'] ?? null,
      },
      nativeCheckpoint: { checkpoint_id: envelope['checkpoint_id'] ?? null },
      pendingWork: { count: Array.isArray(envelope['pending_writes']) ? envelope['pending_writes'].length : 0 },
      eventCursor: event.cursor,
      eventId: event.id,
    }));
  }

  private async latestEnvelope(channel: string): Promise<JsonObject | undefined> {
    const snapshot = await this.actae.latestState(channel);
    if (!snapshot) return undefined;
    this.validateEnvelope(snapshot.state);
    return snapshot.state;
  }

  private async findEnvelope(channel: string, checkpointId: string): Promise<JsonObject | undefined> {
    let offset = 0;
    for (;;) {
      const versions = await this.actae.listStates(channel, { limit: PAGE_SIZE, offset });
      if (versions.length === 0) return undefined;
      for (const entry of versions) {
        const snapshot = await this.actae.getState(channel, entry.version);
        if (!snapshot) continue;
        this.validateEnvelope(snapshot.state);
        if (snapshot.state['checkpoint_id'] === checkpointId) return snapshot.state;
      }
      offset += versions.length;
      if (versions.length < PAGE_SIZE) return undefined;
    }
  }

  private validateEnvelope(envelope: unknown): void {
    const e = asMap(envelope);
    if (!e) throw new ActaeLangGraphError(`corrupt checkpoint snapshot on channel: ${String(envelope)}`);
    if (e['v'] !== ENVELOPE_VERSION) {
      throw new ActaeLangGraphError(
        `checkpoint snapshot format version mismatch: found ${e['v']}, expected ${ENVELOPE_VERSION}`,
      );
    }
  }

  private newEnvelope(opts: {
    threadId: string;
    checkpointNs: string;
    checkpointId?: string;
    parentCheckpointId?: string;
    checkpoint?: unknown;
    metadata?: unknown;
    pendingWrites?: JsonObject[];
  }): JsonObject {
    const envelope: JsonObject = {
      v: ENVELOPE_VERSION,
      thread_id: opts.threadId,
      checkpoint_ns: opts.checkpointNs,
      checkpoint_id: opts.checkpointId ?? null,
      parent_checkpoint_id: opts.parentCheckpointId ?? null,
      pending_writes: opts.pendingWrites ?? [],
      event_cursor: null,
      event_id: null,
    };
    if (opts.checkpoint !== undefined) envelope['checkpoint'] = pack(this.serde, opts.checkpoint);
    if (opts.metadata !== undefined) envelope['metadata'] = pack(this.serde, opts.metadata);
    return envelope;
  }

  private tupleFromEnvelope(config: Record<string, unknown>, envelope: JsonObject): CheckpointTuple | undefined {
    if (envelope['checkpoint'] === undefined) return undefined;
    // The requested config is authoritative. A server-side fork inherits the
    // source snapshot bytes, including its old thread_id; using that inherited
    // value here would route continuation checkpoints back to the parent.
    const effectiveThreadId = requireThreadId(config);
    const effectiveNamespace = checkpointNs(config);
    const checkpoint = unpack(this.serde, envelope['checkpoint']);
    const metadata = envelope['metadata'] !== undefined
      ? unpack(this.serde, envelope['metadata'])
      : {};
    let pendingWrites: Array<[string, string, unknown]> | undefined;
    const rawWrites = envelope['pending_writes'];
    if (Array.isArray(rawWrites) && rawWrites.length > 0) {
      pendingWrites = (rawWrites as JsonObject[])
        .sort((a, b) => {
          const ta = asString(a['task_id']);
          const tb = asString(b['task_id']);
          return ta < tb ? -1 : ta > tb ? 1 : asInt64(a['idx']) - asInt64(b['idx']);
        })
        .map((w) => [asString(w['task_id']), asString(w['channel']), unpack(this.serde, w)]);
    }
    const parentId = envelope['parent_checkpoint_id'];
    let parentConfig: Record<string, unknown> | undefined;
    if (typeof parentId === 'string' && parentId !== '') {
      parentConfig = {
        configurable: {
          thread_id: effectiveThreadId,
          checkpoint_ns: effectiveNamespace,
          checkpoint_id: parentId,
        },
      };
    }
    // Fill the resolved checkpoint_id/ns into the returned config (MemorySaver
    // parity): LangGraph relies on `tuple.config.configurable.checkpoint_id`
    // to carry the checkpoint identity forward (e.g. getState.config).
    const tupleConfig: Record<string, unknown> = {
      ...config,
      configurable: {
        ...((config['configurable'] as Record<string, unknown> | undefined) ?? {}),
        thread_id: effectiveThreadId,
        checkpoint_ns: effectiveNamespace,
        checkpoint_id: asString(envelope['checkpoint_id']),
      },
    };
    return {
      config: tupleConfig,
      checkpoint: checkpoint as Record<string, unknown>,
      metadata: metadata as Record<string, unknown>,
      parentConfig,
      pendingWrites,
    };
  }

  /** Returns the checkpoint for a config (LangGraph base `get` parity). */
  async get(config: Record<string, unknown>): Promise<Record<string, unknown> | undefined> {
    const value = await this.getTuple(config);
    return value ? value.checkpoint : undefined;
  }

  /** Prevents JSON.stringify from traversing the saver when it appears in a
   * runnable `configurable` (base `toJSON` parity). */
  toJSON(): string {
    return '[ActaeCheckpointSaver]';
  }

  /** Walks the parent chain accumulating per-channel pending writes + seed —
   * used by LangGraph to reconstruct `DeltaChannel` state (base
   * `getDeltaChannelHistory` parity). */
  async getDeltaChannelHistory(options: {
    config: Record<string, unknown>;
    channels: string[];
  }): Promise<Record<string, { writes: Array<[string, string, unknown]>; seed?: unknown }>> {
    const { config, channels } = options;
    if (channels.length === 0) return {};
    const collectedByCh: Record<string, Array<[string, string, unknown]>> = {};
    const seedByCh: Record<string, unknown> = {};
    const remaining = new Set(channels);
    for (const ch of channels) collectedByCh[ch] = [];
    let cursorConfig = (await this.getTuple(config))?.parentConfig;
    while (cursorConfig != null && remaining.size > 0) {
      const tup = await this.getTuple(cursorConfig);
      if (tup === undefined) break;
      if (tup.pendingWrites && tup.pendingWrites.length > 0) {
        const perChannel: Record<string, Array<[string, string, unknown]>> = {};
        for (const write of tup.pendingWrites) {
          const ch = write[1];
          if (remaining.has(ch)) (perChannel[ch] ??= []).push(write);
        }
        for (const ch of Object.keys(perChannel)) {
          const block = perChannel[ch]!;
          block.sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0));
          for (let i = block.length - 1; i >= 0; i -= 1) collectedByCh[ch]!.push(block[i]!);
        }
      }
      for (const ch of Array.from(remaining)) {
        if (Object.prototype.hasOwnProperty.call(tup.checkpoint.channel_values, ch)) {
          seedByCh[ch] = (tup.checkpoint.channel_values as Record<string, unknown>)[ch];
          remaining.delete(ch);
        }
      }
      cursorConfig = tup.parentConfig;
    }
    const result: Record<string, { writes: Array<[string, string, unknown]>; seed?: unknown }> = {};
    for (const ch of channels) {
      const entry: { writes: Array<[string, string, unknown]>; seed?: unknown } = {
        writes: collectedByCh[ch]!.slice().reverse(),
      };
      if (Object.prototype.hasOwnProperty.call(seedByCh, ch)) entry.seed = seedByCh[ch];
      result[ch] = entry;
    }
    return result;
  }

  // -------------------------------------------------------------------------
  // Writes
  // -------------------------------------------------------------------------

  /** Stores a checkpoint as a cursor-aligned Actae snapshot. Returns the
   * config carrying the new checkpoint_id. */
  async put(
    config: Record<string, unknown>,
    checkpoint: Record<string, unknown>,
    metadata: Record<string, unknown>,
    _newVersion?: unknown,
  ): Promise<Record<string, unknown>> {
    const threadId = requireThreadId(config);
    const ns = checkpointNs(config);
    this.recordNamespace(threadId, ns);
    const channel = this.channelForConfig(config);
    const checkpointId = asString(checkpoint['id']);
    const parentCheckpointId = checkpointIdOf(config);

    const envelope = this.newEnvelope({
      threadId,
      checkpointNs: ns,
      checkpointId,
      parentCheckpointId,
      checkpoint,
      metadata,
    });

    await this.serialize(async () => {
      await this.saveEnvelope(channel, envelope, EVENT_CHECKPOINT);
      await this.flushDeferredWrites(channel, threadId, ns, checkpointId, envelope);
    });

    return {
      configurable: {
        thread_id: threadId,
        checkpoint_ns: ns,
        checkpoint_id: checkpointId,
      },
    };
  }

  /** Merges a task's pending writes into the target checkpoint (idempotent
   * by (task_id, idx)); persists a new snapshot version of the same
   * checkpoint. */
  async putWrites(
    config: Record<string, unknown>,
    writes: Array<[string, unknown]>,
    taskId: string,
    _taskPath?: string,
  ): Promise<void> {
    const threadId = requireThreadId(config);
    const ns = checkpointNs(config);
    this.recordNamespace(threadId, ns);
    const channel = this.channelForConfig(config);
    const targetId = checkpointIdOf(config);
    if (!targetId) {
      throw new ActaeLangGraphError(
        "putWrites requires config['configurable']['checkpoint_id']",
      );
    }

    await this.serialize(async () => {
      let envelope = await this.latestEnvelope(channel);
      if (!envelope || envelope['checkpoint_id'] !== targetId) {
        envelope = await this.findEnvelope(channel, targetId);
      }
      if (!envelope) {
        this.deferWrites(threadId, ns, targetId, writes, taskId);
        return;
      }

      const merged = new Map<string, JsonObject>();
      const raw = envelope['pending_writes'];
      if (Array.isArray(raw)) {
        for (const w of raw as JsonObject[]) {
          merged.set(`${asString(w['task_id'])}:${asInt64(w['idx'])}`, w);
        }
      }
      writes.forEach(([channelName, value], idx) => {
        merged.set(`${taskId}:${idx}`, {
          task_id: taskId,
          idx,
          channel: channelName,
          ...pack(this.serde, value),
        });
      });
      const updated: JsonObject = {
        ...envelope,
        pending_writes: [...merged.values()].sort((a, b) => {
          const ta = asString(a['task_id']);
          const tb = asString(b['task_id']);
          return ta < tb ? -1 : ta > tb ? 1 : asInt64(a['idx']) - asInt64(b['idx']);
        }),
      };
      await this.saveEnvelope(channel, updated, EVENT_PENDING_WRITES);
    });
  }

  private async flushDeferredWrites(
    channel: string,
    threadId: string,
    ns: string,
    checkpointId: string,
    envelope: JsonObject,
  ): Promise<void> {
    const key = `${threadId}\x00${ns}\x00${checkpointId}`;
    const merged = this.deferredWrites.get(key);
    if (!merged) return;
    this.deferredWrites.delete(key);
    const updated: JsonObject = {
      ...envelope,
      pending_writes: [...merged.values()].sort((a, b) => {
        const ta = asString(a['task_id']);
        const tb = asString(b['task_id']);
        return ta < tb ? -1 : ta > tb ? 1 : asInt64(a['idx']) - asInt64(b['idx']);
      }),
    };
    await this.saveEnvelope(channel, updated, EVENT_PENDING_WRITES);
  }

  private deferWrites(
    threadId: string,
    ns: string,
    targetId: string,
    writes: Array<[string, unknown]>,
    taskId: string,
  ): void {
    const key = `${threadId}\x00${ns}\x00${targetId}`;
    const deferred = this.deferredWrites.get(key) ?? new Map<string, JsonObject>();
    writes.forEach(([channelName, value], idx) => {
      deferred.set(`${taskId}:${idx}`, {
        task_id: taskId,
        idx,
        channel: channelName,
        ...pack(this.serde, value),
      });
    });
    this.deferredWrites.set(key, deferred);
    if (this.deferredWrites.size > MAX_DEFERRED_WRITES) {
      const staleKey = this.deferredWrites.keys().next().value as string;
      this.deferredWrites.delete(staleKey);
    }
  }

  // -------------------------------------------------------------------------
  // Reads
  // -------------------------------------------------------------------------

  /** Returns the latest checkpoint for the thread, or the requested
   * checkpoint_id. */
  async getTuple(config: Record<string, unknown>): Promise<CheckpointTuple | undefined> {
    requireThreadId(config);
    const channel = this.channelForConfig(config);
    const checkpointId = checkpointIdOf(config);
    const envelope = checkpointId
      ? await this.findEnvelope(channel, checkpointId)
      : await this.latestEnvelope(channel);
    if (!envelope) return undefined;
    return this.tupleFromEnvelope(config, envelope);
  }

  /** Yields checkpoints newest-first, honoring `before` (exclusive),
   * `filter` (all metadata keys must match) and `limit`. */
  async *list(
    config: Record<string, unknown>,
    options: {
      filter?: Record<string, unknown>;
      before?: Record<string, unknown>;
      limit?: number;
    } = {},
  ): AsyncGenerator<CheckpointTuple> {
    const threadId = requireThreadId(config);
    const ns = checkpointNs(config);
    const channel = this.channelForConfig(config);
    const wantId = checkpointIdOf(config);
    const beforeId = checkpointIdOf(options.before);

    const seen = new Map<string, JsonObject>();
    let offset = 0;
    for (;;) {
      const versions = await this.actae.listStates(channel, { limit: PAGE_SIZE, offset });
      if (versions.length === 0) break;
      for (const entry of versions) {
        const snapshot = await this.actae.getState(channel, entry.version);
        if (!snapshot) continue;
        this.validateEnvelope(snapshot.state);
        const cid = snapshot.state['checkpoint_id'];
        if (typeof cid !== 'string' || seen.has(cid)) continue;
        if (wantId !== undefined && cid !== wantId) continue;
        seen.set(cid, snapshot.state);
      }
      offset += versions.length;
      if (versions.length < PAGE_SIZE) break;
    }

    const ordered = [...seen.values()].sort((a, b) =>
      asString(a['checkpoint_id']) < asString(b['checkpoint_id']) ? 1 : -1,
    );
    let count = 0;
    for (const envelope of ordered) {
      const cid = asString(envelope['checkpoint_id']);
      if (beforeId !== undefined && cid >= beforeId) continue;
      const metadata = envelope['metadata'] !== undefined
        ? (unpack(this.serde, envelope['metadata']) as Record<string, unknown>)
        : {};
      if (options.filter) {
        let matches = true;
        for (const [k, v] of Object.entries(options.filter)) {
          if (metadata[k] !== v) {
            matches = false;
            break;
          }
        }
        if (!matches) continue;
      }
      const tuple = this.tupleFromEnvelope(
        { configurable: { thread_id: threadId, checkpoint_ns: ns, checkpoint_id: cid } },
        envelope,
      );
      if (tuple) {
        yield tuple;
        count++;
        if (options.limit !== undefined && count >= options.limit) break;
      }
    }
  }

  // -------------------------------------------------------------------------
  // Fork & delete
  // -------------------------------------------------------------------------

  /** Forks a thread's checkpoint into a NEW thread's channel (the
   * "fork a LangGraph run at step N, refine step N+1" workflow). Returns the
   * fork's config ready for `ainvoke(null, forkCfg)`. */
  async forkThread(
    config: Record<string, unknown>,
    options: { newThreadId: string; reason?: string },
  ): Promise<Record<string, unknown>> {
    const threadId = requireThreadId(config);
    const ns = checkpointNs(config);
    const srcChannel = this.channelForConfig(config);
    const requestedId = checkpointIdOf(config);
    const envelope = requestedId
      ? await this.findEnvelope(srcChannel, requestedId)
      : await this.latestEnvelope(srcChannel);
    if (!envelope || envelope['checkpoint'] === undefined) {
      throw new ActaeLangGraphError(`thread '${threadId}' has no checkpoint to fork`);
    }
    const checkpointId = asString(envelope['checkpoint_id']);
    const eventCursor = asInt64(envelope['event_cursor']);
    if (eventCursor === 0) {
      throw new ActaeLangGraphError(
        `thread '${threadId}' checkpoint ${checkpointId} has no event cursor — cannot fork`,
      );
    }

    const forkConfig = {
      configurable: {
        thread_id: options.newThreadId,
        checkpoint_ns: ns,
        checkpoint_id: checkpointId,
      },
    };
    const forkChannel = this.channelForConfig(forkConfig);
    await this.actae.fork(srcChannel, forkChannel, eventCursor, {
      displayName: options.newThreadId,
      reason: options.reason ?? `Forked thread ${threadId} at checkpoint ${checkpointId.slice(0, 8)}`,
    });
    this.recordNamespace(options.newThreadId, ns);

    return forkConfig;
  }

  /** Deletes all checkpoints and writes for a thread. */
  async deleteThread(threadId: string): Promise<void> {
    const tid = String(threadId);
    for (const key of [...this.deferredWrites.keys()]) {
      if (key.startsWith(`${tid}\x00`)) this.deferredWrites.delete(key);
    }
    const namespaces = this.threadNamespaces.get(tid) ?? new Set(['']);
    for (const ns of namespaces) {
      const channel = this.channelForConfig({
        configurable: { thread_id: tid, checkpoint_ns: ns },
      });
      let offset = 0;
      for (;;) {
        const versions = await this.actae.listStates(channel, { limit: PAGE_SIZE, offset });
        if (versions.length === 0) break;
        for (const entry of versions) {
          await this.actae.deleteState(channel, entry.version);
        }
        offset += versions.length;
        if (versions.length < PAGE_SIZE) break;
      }
    }
    this.threadNamespaces.delete(tid);
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  /** Serializes overlapping checkpoint writes to the same channel. */
  private async serialize(fn: () => Promise<void>): Promise<void> {
    const run = this.inflight.then(fn, fn);
    this.inflight = run.then(
      () => undefined,
      () => undefined,
    );
    await run;
  }
}

/** Loads the source channel metadata for a config (audit/debug helper). */
export async function channelMetadata(
  saver: ActaeCheckpointSaver,
  config: Record<string, unknown>,
): Promise<ChannelMetadata | undefined> {
  return saver.actae.getChannelMetadata(saver.channelForConfig(config));
}
