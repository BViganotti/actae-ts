/**
 * Claude Agent SDK Session Store adapter for Actae.
 *
 * Mirrors the Python adapter (`actae_client.adapters.claude`): implements the
 * `claude-agent-sdk` `SessionStore` protocol backed by Actae, giving the
 * Claude Agent SDK durable, cursor-aligned conversation state that survives
 * process restarts, is forkable at any transcript point, and streams every
 * transcript append as a real-time event.
 *
 * ```ts
 * import { ActaeClaudeSessionStore } from '@actae/sdk/adapters/claude';
 * const store = new ActaeClaudeSessionStore(actae);
 * const options = { sessionStore: store }; // ClaudeAgentOptions
 * // ...query(..., options) persists the full transcript in Actae
 * ```
 *
 * The protocol is structural (no runtime dependency on `claude-agent-sdk`);
 * both snake_case (Python SDK shape) and camelCase aliases are provided.
 */

import { createHash } from 'node:crypto';

import type { ActaeClient } from '../client.js';
import type { JsonObject } from '../json.js';
import { adapterCheckpointState } from './contract.js';

const EVENT_APPEND = 'claude.session.append';
const EVENT_DELETE = 'claude.session.delete';
const ACTOR = 'claude-session-store';

// ---------------------------------------------------------------------------
// Types (structural — mirror claude_agent_sdk types)
// ---------------------------------------------------------------------------

export interface SessionKey {
  project_key: string;
  session_id: string;
  subpath?: string;
}

export type SessionStoreEntry = JsonObject;

export interface SessionStoreListEntry {
  session_id: string;
  mtime: number;
}

export interface SessionListSubkeysKey {
  project_key: string;
  session_id: string;
}

export interface SessionSummaryEntry {
  session_id: string;
  mtime: number;
  data: JsonObject;
}

// ---------------------------------------------------------------------------
// Channel resolution
// ---------------------------------------------------------------------------

function digest(...parts: string[]): string {
  return createHash('sha256').update(parts.join('\x00')).digest('hex').slice(0, 16);
}

/** Deterministically resolves the Actae channel for a Claude session key. */
export function channelForSession(projectKey: string, sessionId: string, subpath?: string): string {
  if (subpath === undefined) return `claude:${digest(projectKey, sessionId)}`;
  return `claude:${digest(projectKey, sessionId, subpath)}`;
}

function channelForIndex(projectKey: string): string {
  return `claude:idx:${digest(projectKey)}`;
}

// ---------------------------------------------------------------------------
// ActaeClaudeSessionStore
// ---------------------------------------------------------------------------

export interface Snapshot {
  entries: SessionStoreEntry[];
  mtime: number;
  subkeys: string[];
}

/** Durable Claude Agent SDK `SessionStore` backed by Actae.
 *
 * Every transcript append is persisted as a cursor-aligned, versioned state
 * snapshot on a deterministic per-session channel and streamed as a
 * `claude.session.append` event — so a running Claude conversation is visible
 * in real time, replayable, and forkable from any transcript point. */
export class ActaeClaudeSessionStore {
  readonly actae: ActaeClient;
  private lastMtime = 0;

  constructor(actae: ActaeClient) {
    this.actae = actae;
  }

  /** Appends transcript entries and persists a new snapshot version.
   *
   * Entries carrying a stable `uuid` are treated as an idempotency key
   * (upsert / ignore-duplicate) so retries and `importSessionToStore()`
   * replays never create duplicate rows — mirroring the SessionStore
   * contract. Entries without a `uuid` are always appended. */
  async append(key: SessionKey, entries: SessionStoreEntry[]): Promise<void> {
    if (entries.length === 0) return;
    const ch = channelForSession(key.project_key, key.session_id, key.subpath);
    const snapshot = (await this.loadSnapshot(ch)) ?? emptySnapshot();
    const mtime = this.nextMtime();
    snapshot.entries = mergeEntries(snapshot.entries, entries);
    snapshot.mtime = mtime;

    await this.actae.transition(
      ch,
      EVENT_APPEND,
      {
        session_id: key.session_id,
        subpath: key.subpath ?? null,
        count: entries.length,
        last_type: entries[entries.length - 1]?.['type'] ?? null,
      } as JsonObject,
      this.checkpointState(ch, snapshot),
      { actor: ACTOR },
    );
    if (key.subpath !== undefined) {
      await this.registerSubkey(key.project_key, key.session_id, key.subpath);
    } else {
      await this.updateProjectIndex(key.project_key, key.session_id, mtime, entries);
    }
  }

  /** Returns all transcript entries for a key, or null if absent (or emptied
   * by delete — per the SessionStore contract, "never written" and "emptied"
   * may both return null). */
  async load(key: SessionKey): Promise<SessionStoreEntry[] | null> {
    const ch = channelForSession(key.project_key, key.session_id, key.subpath);
    const snapshot = await this.loadSnapshot(ch);
    if (!snapshot) return null;
    // A delete persists an empty snapshot with mtime 0 — treat as absent.
    if (snapshot.entries.length === 0 && snapshot.mtime === 0) return null;
    return [...snapshot.entries];
  }

  /** Forks a Claude session's transcript into a new session. */
  async forkSession(
    projectKey: string,
    sessionId: string,
    newSessionId: string,
    options: { subpath?: string; reason?: string } = {},
  ): Promise<SessionKey> {
    const srcCh = channelForSession(projectKey, sessionId, options.subpath);
    const snapshot = await this.loadSnapshot(srcCh);
    if (!snapshot) {
      throw new TypeError(`Claude session '${sessionId}' has no transcript to fork`);
    }
    const dstCh = channelForSession(projectKey, newSessionId, options.subpath);
    const cursor = (await this.actae.latestCursor(srcCh)) ?? 0;
    await this.actae.fork(srcCh, dstCh, cursor, {
      displayName: newSessionId,
      reason: options.reason ?? `Forked Claude session ${sessionId} → ${newSessionId}`,
    });

    const mtime = this.nextMtime();
    const entries = [...snapshot.entries];
    if (options.subpath !== undefined) {
      const mainCh = channelForSession(projectKey, newSessionId);
      const mainSnapshot = (await this.loadSnapshot(mainCh)) ?? emptySnapshot();
      if (!mainSnapshot.subkeys.includes(options.subpath)) {
        mainSnapshot.subkeys.push(options.subpath);
        const mainCursor = (await this.actae.latestCursor(mainCh)) ?? 0;
        await this.actae.saveState(mainCh, mainCursor, this.checkpointState(mainCh, mainSnapshot, mainCursor));
      }
    } else {
      await this.updateProjectIndex(projectKey, newSessionId, mtime, entries);
    }
    return { project_key: projectKey, session_id: newSessionId, subpath: options.subpath };
  }

  /** Deletes a transcript. Deleting a main transcript cascades to its
   * subkey transcripts. */
  async delete(key: SessionKey): Promise<void> {
    const mainCh = channelForSession(key.project_key, key.session_id);
    const mainSnapshot = await this.loadSnapshot(mainCh);
    const subkeys = mainSnapshot ? [...mainSnapshot.subkeys] : [];

    const ch = channelForSession(key.project_key, key.session_id, key.subpath);
    if (key.subpath === undefined) {
      for (const sub of subkeys) {
        const subCh = channelForSession(key.project_key, key.session_id, sub);
        const subCursor = (await this.actae.latestCursor(subCh)) ?? 0;
        await this.actae.transition(
          subCh,
          EVENT_DELETE,
          { session_id: key.session_id, subpath: sub } as JsonObject,
          this.checkpointState(subCh, emptySnapshot(), subCursor),
          { actor: ACTOR },
        );
      }
      await this.removeFromProjectIndex(key.project_key, key.session_id);
    } else if (mainSnapshot && mainSnapshot.subkeys.includes(key.subpath)) {
      mainSnapshot.subkeys = mainSnapshot.subkeys.filter((s) => s !== key.subpath);
      const mainCursor = (await this.actae.latestCursor(mainCh)) ?? 0;
      await this.actae.saveState(mainCh, mainCursor, this.checkpointState(mainCh, mainSnapshot, mainCursor));
    }

    const cursor = (await this.actae.latestCursor(ch)) ?? 0;
    await this.actae.transition(
      ch,
      EVENT_DELETE,
      { session_id: key.session_id, subpath: key.subpath ?? null } as JsonObject,
      this.checkpointState(ch, emptySnapshot(), cursor),
      { actor: ACTOR },
    );
  }

  /** Lists main transcripts for a project with their storage mtimes. */
  async list_sessions(projectKey: string): Promise<SessionStoreListEntry[]> {
    const index = await this.loadIndex(projectKey);
    return Object.entries(index.sessions).map(([sessionId, meta]) => ({
      session_id: sessionId,
      mtime: (meta as { mtime: number }).mtime,
    }));
  }

  /** camelCase form (the Claude Agent SDK `SessionStore` contract). */
  async listSessions(projectKey: string): Promise<Array<{ sessionId: string; mtime: number }>> {
    const index = await this.loadIndex(projectKey);
    return Object.entries(index.sessions).map(([sessionId, meta]) => ({
      sessionId,
      mtime: (meta as { mtime: number }).mtime,
    }));
  }

  /** Lists subkey paths (subagent transcripts) for a session. */
  async list_subkeys(key: SessionListSubkeysKey): Promise<string[]> {
    const ch = channelForSession(key.project_key, key.session_id);
    const snapshot = await this.loadSnapshot(ch);
    return snapshot ? [...snapshot.subkeys] : [];
  }

  /** camelCase form (the Claude Agent SDK `SessionStore` contract). */
  async listSubkeys(key: { projectKey: string; sessionId: string }): Promise<string[]> {
    return this.list_subkeys({ project_key: key.projectKey, session_id: key.sessionId });
  }

  /** Returns SDK-owned summary sidecars for a project, verbatim. */
  async list_session_summaries(projectKey: string): Promise<SessionSummaryEntry[]> {
    const index = await this.loadIndex(projectKey);
    return Object.entries(index.summaries).map(([sessionId, meta]) => ({
      session_id: sessionId,
      mtime: (meta as { mtime: number }).mtime,
      data: (meta as { data: JsonObject }).data,
    }));
  }

  /** camelCase form (the Claude Agent SDK `SessionStore` contract). */
  async listSessionSummaries(projectKey: string): Promise<Array<{ sessionId: string; mtime: number; data: JsonObject }>> {
    const index = await this.loadIndex(projectKey);
    return Object.entries(index.summaries).map(([sessionId, meta]) => ({
      sessionId,
      mtime: (meta as { mtime: number }).mtime,
      data: (meta as { data: JsonObject }).data,
    }));
  }

  // -------------------------------------------------------------------------
  // Internals
  // -------------------------------------------------------------------------

  private nextMtime(): number {
    const nowMs = Date.now();
    if (nowMs <= this.lastMtime) return this.lastMtime + 1;
    this.lastMtime = nowMs;
    return nowMs;
  }

  private async loadSnapshot(channel: string): Promise<Snapshot | undefined> {
    const snapshot = await this.actae.latestState(channel);
    return snapshot ? (snapshot.state as unknown as Snapshot) : undefined;
  }

  private checkpointState(
    channel: string,
    state: Snapshot | { sessions: Record<string, unknown>; summaries: Record<string, unknown> },
    cursor?: number,
  ): JsonObject {
    const raw = state as unknown as JsonObject;
    const isTranscript = 'entries' in state;
    const portable: JsonObject = isTranscript
      ? {
          entry_count: (state as Snapshot).entries.length,
          mtime: (state as Snapshot).mtime,
          subkeys: [...(state as Snapshot).subkeys],
        }
      : {
          session_count: Object.keys((state as { sessions: Record<string, unknown> }).sessions).length,
          summary_count: Object.keys((state as { summaries: Record<string, unknown> }).summaries).length,
        };
    return adapterCheckpointState(raw, {
      framework: 'claude-agent-sdk',
      channelId: channel,
      portableState: portable,
      nativeCheckpoint: { storage: 'session_store' },
      eventCursor: cursor,
    });
  }

  private async loadIndex(projectKey: string): Promise<{ sessions: Record<string, { mtime: number }>; summaries: Record<string, { mtime: number; data: JsonObject }> }> {
    const snapshot = await this.actae.latestState(channelForIndex(projectKey));
    if (!snapshot) return { sessions: {}, summaries: {} };
    const state = snapshot.state as unknown as {
      sessions?: Record<string, { mtime: number }>;
      summaries?: Record<string, { mtime: number; data: JsonObject }>;
    };
    return {
      sessions: { ...(state.sessions ?? {}) },
      summaries: { ...(state.summaries ?? {}) },
    };
  }

  private async saveIndex(projectKey: string, index: { sessions: Record<string, unknown>; summaries: Record<string, unknown> }): Promise<void> {
    const ch = channelForIndex(projectKey);
    const cursor = (await this.actae.latestCursor(ch)) ?? 0;
    await this.actae.saveState(ch, cursor, this.checkpointState(ch, index, cursor));
  }

  private async updateProjectIndex(projectKey: string, sessionId: string, mtime: number, entries: SessionStoreEntry[]): Promise<void> {
    const index = await this.loadIndex(projectKey);
    index.sessions[sessionId] = { mtime };
    const prev = index.summaries[sessionId];
    index.summaries[sessionId] = {
      ...(await foldSessionSummary(prev?.data, { project_key: projectKey, session_id: sessionId, subpath: null }, entries, { mtime })),
      mtime,
    };
    await this.saveIndex(projectKey, index as unknown as { sessions: Record<string, unknown>; summaries: Record<string, unknown> });
  }

  private async registerSubkey(projectKey: string, sessionId: string, subpath: string): Promise<void> {
    const mainCh = channelForSession(projectKey, sessionId);
    const snapshot = (await this.loadSnapshot(mainCh)) ?? emptySnapshot();
    if (!snapshot.subkeys.includes(subpath)) {
      snapshot.subkeys.push(subpath);
      const cursor = (await this.actae.latestCursor(mainCh)) ?? 0;
      await this.actae.saveState(mainCh, cursor, this.checkpointState(mainCh, snapshot, cursor));
    }
  }

  private async removeFromProjectIndex(projectKey: string, sessionId: string): Promise<void> {
    const index = await this.loadIndex(projectKey);
    delete index.sessions[sessionId];
    delete index.summaries[sessionId];
    await this.saveIndex(projectKey, index as unknown as { sessions: Record<string, unknown>; summaries: Record<string, unknown> });
  }
}

function emptySnapshot(): Snapshot {
  return { entries: [], mtime: 0, subkeys: [] };
}

/** Merges an incoming batch into the stored transcript. Entries carrying a
 * `uuid` are deduplicated (a later entry with the same `uuid` replaces the
 * stored one — upsert); entries without a `uuid` are always appended. This is
 * the SessionStore idempotency contract: retries and import replays must not
 * create duplicate rows. */
export function mergeEntries(stored: SessionStoreEntry[], incoming: SessionStoreEntry[]): SessionStoreEntry[] {
  const byUuid = new Map<string, number>();
  stored.forEach((entry, idx) => {
    const uuid = entry['uuid'];
    if (typeof uuid === 'string') byUuid.set(uuid, idx);
  });
  const out = [...stored];
  for (const entry of incoming) {
    const uuid = entry['uuid'];
    if (typeof uuid === 'string' && byUuid.has(uuid)) {
      out[byUuid.get(uuid)!] = entry; // upsert in place
    } else {
      if (typeof uuid === 'string') byUuid.set(uuid, out.length);
      out.push(entry);
    }
  }
  return out;
}

// ---------------------------------------------------------------------------
// Summary folding (claude-agent-sdk parity)
// ---------------------------------------------------------------------------

let foldSessionSummaryImpl: ((prev: JsonObject | undefined, key: { project_key: string; session_id: string; subpath: string | null }, entries: SessionStoreEntry[], opts: { mtime?: number }) => { data: JsonObject; mtime: number }) | undefined;

const CLAUDE_SDK_MODULES = ['@anthropic-ai/claude-agent-sdk', 'claude-agent-sdk'];

/** Loads `foldSessionSummary` from the installed Claude Agent SDK, falling
 * back to a minimal fold when the SDK is absent (the SDK package is an
 * optional peer, not a runtime dependency of the core). */
async function foldSessionSummary(
  prev: JsonObject | undefined,
  key: { project_key: string; session_id: string; subpath: string | null },
  entries: SessionStoreEntry[],
  opts: { mtime?: number },
): Promise<{ data: JsonObject; mtime: number }> {
  if (foldSessionSummaryImpl === undefined) {
    for (const pkg of CLAUDE_SDK_MODULES) {
      try {
        const mod = (await import(pkg)) as { foldSessionSummary?: unknown };
        if (typeof mod.foldSessionSummary === 'function') {
          foldSessionSummaryImpl = mod.foldSessionSummary as unknown as typeof foldSessionSummaryImpl;
          break;
        }
      } catch {
        // try the next package name
      }
    }
    if (foldSessionSummaryImpl === undefined) {
      foldSessionSummaryImpl = minimalFold;
    }
  }
  return foldSessionSummaryImpl(prev, key, entries, opts);
}

/** Minimal SDK-compatible fold: keeps the accumulated raw data and tracks the
 * latest mtime. Full folding (token/summary extraction) is applied when the
 * Claude Agent SDK package is installed. */
function minimalFold(
  prev: JsonObject | undefined,
  key: { project_key: string; session_id: string; subpath: string | null },
  _entries: SessionStoreEntry[],
  opts: { mtime?: number },
): { data: JsonObject; mtime: number } {
  return {
    data: prev ? { ...prev } : { session_id: key.session_id },
    mtime: opts.mtime ?? 0,
  };
}
