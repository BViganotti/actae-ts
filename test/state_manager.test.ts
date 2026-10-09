import { describe, expect, it } from 'vitest';
import { StateManager } from '../src/state_manager.js';
import { SnapshotBoundaryError } from '../src/errors.js';
import { FakeActaeClient } from './helpers.js';

describe('StateManager', () => {
  it('save aligns to the latest cursor and returns a version', async () => {
    const actae = new FakeActaeClient();
    await actae.record('ch', 't', {});
    const mgr = new StateManager(actae as never, 'ch');
    const version = await mgr.save({ a: 1 });
    expect(version).toBeGreaterThan(0);
    const stored = actae.channels.get('ch')!.states[0]!;
    expect(stored.cursor).toBe(1);
  });

  it('load returns the latest state', async () => {
    const actae = new FakeActaeClient();
    const mgr = new StateManager(actae as never, 'ch');
    await mgr.save({ a: 1 });
    await mgr.save({ a: 2 });
    expect(await mgr.load()).toEqual({ a: 2 });
  });

  it('load returns undefined when no state saved', async () => {
    const actae = new FakeActaeClient();
    const mgr = new StateManager(actae as never, 'ch');
    expect(await mgr.load()).toBeUndefined();
  });

  it('resume falls back to defaultState', async () => {
    const actae = new FakeActaeClient();
    const mgr = new StateManager(actae as never, 'ch');
    expect(await mgr.resume({ init: true })).toEqual({ init: true });
    expect(await mgr.resume()).toEqual({});
  });

  it('listVersions / getVersion / deleteVersion', async () => {
    const actae = new FakeActaeClient();
    const mgr = new StateManager(actae as never, 'ch');
    const v1 = await mgr.save({ a: 1 });
    const v2 = await mgr.save({ a: 2 });
    const versions = await mgr.listVersions();
    expect(versions.map((v) => v.version)).toEqual([v2, v1]);
    expect((await mgr.getVersion(v1))?.state).toEqual({ a: 1 });
    await mgr.deleteVersion(v1);
    expect(await mgr.getVersion(v1)).toBeUndefined();
    expect((await mgr.listVersions()).length).toBe(1);
  });
});

describe('StateManager channel + snapshot details', () => {
  it('exposes the managed channel id', async () => {
    const actae = new FakeActaeClient();
    const mgr = new StateManager(actae as never, 'exp-channel');
    expect(mgr.channelId).toBe('exp-channel');
  });
  it('loadSnapshot returns the full snapshot', async () => {
    const actae = new FakeActaeClient();
    const mgr = new StateManager(actae as never, 'ch');
    await mgr.save({ a: 1 });
    const snap = await mgr.loadSnapshot();
    expect(snap?.state).toEqual({ a: 1 });
    expect(snap?.cursor).toBeGreaterThanOrEqual(0);
  });
});

describe('StateManager.fork', () => {
  it('forks at the latest cursor and inherits the state', async () => {
    const actae = new FakeActaeClient();
    await actae.record('src', 't', {});
    const mgr = new StateManager(actae as never, 'src');
    await mgr.save({ step: 1, accum: 'base' });

    const fork = await mgr.fork('child', { reason: 'refine' });
    expect(fork.channelId).toBe('child');
    expect(actae.forks).toEqual([
      { source: 'src', child: 'child', atCursor: 1 },
    ]);
    // load on the fork returns the inherited state
    expect(await fork.load()).toEqual({ step: 1, accum: 'base' });
  });

  it('fork evolves independently; source is unaffected', async () => {
    const actae = new FakeActaeClient();
    const mgr = new StateManager(actae as never, 'src');
    await mgr.save({ step: 1, accum: 'base' });
    const fork = await mgr.fork('child');
    await fork.save({ step: 2, accum: 'refined' });

    expect(await fork.load()).toEqual({ step: 2, accum: 'refined' });
    expect(await mgr.load()).toEqual({ step: 1, accum: 'base' });
  });

  it('retries at at_cursor=0 on SnapshotBoundaryError', async () => {
    const actae = new FakeActaeClient();
    await actae.record('src', 't', {});
    await actae.saveState('src', 1, { accum: 'base' });
    actae.forkErrors['child'] = new SnapshotBoundaryError('no saved state boundary');
    const mgr = new StateManager(actae as never, 'src');

    const fork = await mgr.fork('child');
    // the first attempt (at_cursor=1) failed and is not recorded; the retry
    // at at_cursor=0 (latest state) succeeded
    expect(actae.forks.map((f) => f.atCursor)).toEqual([0]);
    expect(await fork.load()).toEqual({ accum: 'base' });
  });

  it('propagates non-boundary fork errors', async () => {
    const actae = new FakeActaeClient();
    await actae.record('src', 't', {});
    actae.forkErrors['child'] = new Error('nope');
    const mgr = new StateManager(actae as never, 'src');
    await expect(mgr.fork('child')).rejects.toThrow('nope');
  });
});
