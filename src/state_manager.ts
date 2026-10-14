/**
 * StateManager — framework-agnostic versioned cursor-aligned state snapshots
 * for any agent framework (Python `adapters/base.py` / Go `state_manager.go`
 * parity). Each save creates a new immutable version.
 */

import type { ActaeClient } from './client.js';
import { SnapshotBoundaryError } from './errors.js';
import type { JsonObject } from './json.js';
import type { StateSnapshot, StateVersionInfo } from './types.js';

export class StateManager {
  private readonly actae: ActaeClient;
  private readonly channel: string;

  constructor(actae: ActaeClient, channel: string) {
    this.actae = actae;
    this.channel = channel;
  }

  /** Returns the managed channel ID. */
  get channelId(): string {
    return this.channel;
  }

  /** Persists a state snapshot aligned to the channel's latest cursor.
   * Returns the assigned version number. */
  async save(state: JsonObject): Promise<number> {
    const latest = await this.actae.latestCursor(this.channel);
    return this.actae.saveState(this.channel, latest ?? 0, state);
  }

  /** Returns the latest state snapshot's state dict, or undefined when no
   * state has been saved yet. */
  async load(): Promise<JsonObject | undefined> {
    const snapshot = await this.actae.latestState(this.channel);
    return snapshot?.state;
  }

  /** Returns the full latest StateSnapshot, or undefined when no state has
   * been saved yet. */
  async loadSnapshot(): Promise<StateSnapshot | undefined> {
    return this.actae.latestState(this.channel);
  }

  /** Loads the latest state, falling back to `defaultState` when none
   * exists. */
  async resume(defaultState?: JsonObject): Promise<JsonObject> {
    const state = await this.load();
    if (state !== undefined) return state;
    if (defaultState !== undefined) return defaultState;
    return {};
  }

  /** Forks this state into a new channel, returning a StateManager bound to
   * it — the framework-agnostic "fork at the current point, refine from
   * here" primitive. load() on the returned manager returns the inherited
   * state.
   *
   * The server forks the latest state at-or-before the channel's latest
   * cursor. If no snapshot boundary exists at that cursor
   * (SnapshotBoundaryError), the fork is retried once at at_cursor=0 (latest
   * state) so event-only channels still fork. */
  async fork(newChannel: string, options: { reason?: string } = {}): Promise<StateManager> {
    const latest = await this.actae.latestCursor(this.channel);
    const reason = options.reason ?? `Forked ${this.channel} → ${newChannel}`;
    const doFork = (atCursor: number): Promise<unknown> =>
      this.actae.fork(this.channel, newChannel, atCursor, {
        displayName: newChannel,
        reason,
      });
    try {
      await doFork(latest ?? 0);
    } catch (err) {
      if (!(err instanceof SnapshotBoundaryError)) throw err;
      await doFork(0);
    }
    return new StateManager(this.actae, newChannel);
  }

  /** Lists all state version history for this channel (metadata only, no
   * blobs), newest-first. */
  async listVersions(): Promise<StateVersionInfo[]> {
    return this.actae.listStates(this.channel);
  }

  /** Loads a specific state snapshot version, or undefined when the version
   * does not exist. */
  async getVersion(version: number): Promise<StateSnapshot | undefined> {
    return this.actae.getState(this.channel, version);
  }

  /** Deletes a specific state snapshot version. */
  async deleteVersion(version: number): Promise<void> {
    return this.actae.deleteState(this.channel, version);
  }
}
