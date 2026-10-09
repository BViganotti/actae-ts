import { describe, expect, it } from 'vitest';
import {
  AgentSession,
  buildManifest,
  SESSION_COMPLETED,
  SESSION_STARTED,
} from '../src/session.js';
import {
  APIError,
  ConnectionError,
  NoRestorableCheckpointError,
  SessionCompletedError,
  SessionError,
  SnapshotBoundaryError,
} from '../src/errors.js';
import { FakeActaeClient } from './helpers.js';

function newSession(opts: Record<string, unknown> = {}) {
  const actae = new FakeActaeClient();
  const session = new AgentSession(actae as never, 'run-1', {
    stateFn: (opts.stateFn as (() => Record<string, unknown>) | undefined) ?? (() => ({ step: 1 })),
    snapshotInterval: (opts.snapshotInterval as number) ?? 1,
    ...opts,
  } as never);
  return { actae, session };
}
async function started(opts: Record<string, unknown> = {}) {
  const h = newSession(opts);
  await h.session.start();
  return h;
}

describe('AgentSession constructor', () => {
  it('requires a name', () => {
    const actae = new FakeActaeClient();
    expect(() => new AgentSession(actae as never, '')).toThrow('name is required');
  });
  it('snapshotInterval < 1 defaults to 1', () => {
    const actae = new FakeActaeClient();
    const s = new AgentSession(actae as never, 'x', { snapshotInterval: 0 } as never);
    expect((s as unknown as { snapshotInterval: number }).snapshotInterval).toBe(1);
  });
});

describe('AgentSession lifecycle details', () => {
  it('start on a resumed session just marks started + updates metadata', async () => {
    const { actae, session } = await started();
    (session as unknown as { resumed: boolean }).resumed = true;
    await session.start();
    expect(session.sessionStatus).toBe('started');
    const rec = actae.recordedEvents.filter((r) => r.eventType === SESSION_STARTED);
    expect(rec).toHaveLength(1); // no duplicate session.started
  });
  it('complete is a no-op on a completed or crashed session', async () => {
    const { actae, session } = await started();
    await session.complete();
    const n1 = actae.recordedEvents.filter((r) => r.eventType === SESSION_COMPLETED).length;
    await session.complete();
    const n2 = actae.recordedEvents.filter((r) => r.eventType === SESSION_COMPLETED).length;
    expect(n2).toBe(n1);
    await session.crash('boom');
    const before = actae.recordedEvents.length;
    await session.complete();
    expect(actae.recordedEvents.length).toBe(before);
  });
  it('complete surfaces record failures through onError', async () => {
    const { actae, session } = await started();
    const errors: string[] = [];
    session.onError((m) => errors.push(m));
    const original = actae.record.bind(actae);
    actae.record = async () => {
      throw new ConnectionError('nope');
    };
    await session.complete();
    expect(session.sessionStatus).toBe('completed');
    expect(errors.some((m) => m.includes('session.completed'))).toBe(true);
    actae.record = original;
  });
  it('crash surfaces metadata-update failures through onError', async () => {
    const { actae, session } = await started();
    const errors: string[] = [];
    session.onError((m) => errors.push(m));
    const original = actae.updateMetadata.bind(actae);
    actae.updateMetadata = async () => {
      throw new ConnectionError('db down');
    };
    await session.crash('boom');
    expect(errors.some((m) => m.includes('update metadata'))).toBe(true);
    actae.updateMetadata = original;
  });
  it('step without a channel raises SessionError', async () => {
    const { session } = newSession();
    await expect(session.step('a')).rejects.toBeInstanceOf(SessionError);
  });
  it('state_fn panic is logged, snapshot skipped, step still succeeds', async () => {
    const { actae, session } = await started({
      stateFn: () => {
        throw new Error('state boom');
      },
    });
    const errors: string[] = [];
    session.onError((m) => errors.push(m));
    const ev = await session.step('a');
    expect(ev.cursor).toBeGreaterThan(0);
    expect(errors.some((m) => m.includes('state_fn panicked'))).toBe(true);
    expect(actae.channels.get('run-1')!.states.length).toBe(0);
  });
  it('save_state failure is logged, not fatal', async () => {
    const { actae, session } = await started();
    const errors: string[] = [];
    session.onError((m) => errors.push(m));
    const original = actae.saveState.bind(actae);
    actae.saveState = async () => {
      throw new ConnectionError('save fail');
    };
    const ev = await session.step('a');
    expect(ev.cursor).toBeGreaterThan(0);
    expect(errors.some((m) => m.includes('save_state failed'))).toBe(true);
    actae.saveState = original;
  });
  it('step transitions the session to stepping', async () => {
    const { actae, session } = await started();
    await session.step('a');
    expect(session.sessionStatus).toBe('stepping');
    expect(actae.channels.get('run-1')!.metadata['status']).toBe('started');
  });
  it('updateMetadataStatus emits cursors_warning beyond 1000 cursors', async () => {
    const { actae, session } = await started();
    const fake = session as unknown as { cursors: number[]; stepCount: number };
    fake.cursors = Array.from({ length: 1005 }, (_, i) => i + 1);
    fake.stepCount = 1005;
    await session.complete();
    const meta = actae.channels.get('run-1')!.metadata;
    expect(meta['cursors_warning']).toContain('Only last 1000');
  });
});

describe('AgentSession fork details', () => {
  it('fork requires a started channel', async () => {
    const { session } = await started();
    (session as unknown as { channelIdValue: string }).channelIdValue = '';
    await expect(session.fork(1, 'x')).rejects.toThrow(/no channel/);
  });
  it('fork uses the server step index when available (inherited step)', async () => {
    const { actae, session } = await started();
    await session.step('a');
    (session as unknown as { cursors: number[] }).cursors = [0];
    actae.resolveStepResult = { channelId: 'ancestor', cursor: 7, stepNumber: 1 };
    const fork = await session.fork(1, 'fix');
    expect(fork.channelId).toBe('fix');
    const f = actae.forks.at(-1);
    expect(f?.source).toBe('ancestor');
    expect(f?.atCursor).toBe(7);
  });
  it('fork walks the lineage to the owning channel via forked_at_step', async () => {
    const { actae, session } = await started();
    await session.step('a');
    (session as unknown as { cursors: number[] }).cursors = [0];
    const ch = actae.channels.get('run-1')!;
    ch.parent = 'grandparent';
    ch.metadata['forked_at_step'] = 3;
    // Owner metadata has no cursors → falls back to replay.
    actae.replayEvents.set('grandparent', [
      { id: 'e1', channelId: 'grandparent', eventType: 'step', cursor: 2, timestamp: 't' },
      { id: 'e2', channelId: 'grandparent', eventType: 'step', cursor: 4, timestamp: 't' },
    ]);
    const fork = await session.fork(1, 'fix');
    const f = actae.forks.at(-1);
    expect(f?.source).toBe('grandparent');
    expect(f?.atCursor).toBe(2);
  });
  it('resolveForkSource uses the owner cursors list fast path', async () => {
    const { actae, session } = await started();
    await session.step('a');
    (session as unknown as { cursors: number[]; stepCount: number }).cursors = [0, 0];
    (session as unknown as { stepCount: number }).stepCount = 2;
    const ch = actae.channels.get('run-1')!;
    ch.metadata['cursors'] = ['10', '20', '30']; // string cursors
    const fork = await session.fork(2, 'fix');
    const f = actae.forks.at(-1);
    expect(f?.atCursor).toBe(20);
  });
  it('replay fallback raises when the owner has no events', async () => {
    const { actae, session } = await started();
    await session.step('a');
    (session as unknown as { cursors: number[] }).cursors = [0];
    actae.replayEvents.set('run-1', []);
    await expect(session.fork(1, 'fix')).rejects.toThrow(/no events/);
  });
  it('replay fallback raises when only lifecycle markers exist', async () => {
    const { actae, session } = await started();
    await session.step('a');
    (session as unknown as { cursors: number[] }).cursors = [0];
    actae.replayEvents.set('run-1', [
      { id: 'b', channelId: 'run-1', eventType: 'fork.started', cursor: 1, timestamp: 't' },
      { id: 's', channelId: 'run-1', eventType: SESSION_STARTED, cursor: 2, timestamp: 't' },
    ]);
    await expect(session.fork(1, 'fix')).rejects.toThrow(/no user events/);
  });
  it('replay fallback validates cursor monotonicity', async () => {
    const { actae, session } = await started();
    await session.step('a');
    (session as unknown as { cursors: number[] }).cursors = [0];
    actae.replayEvents.set('run-1', [
      { id: 'e1', channelId: 'run-1', eventType: 'step', cursor: 5, timestamp: 't' },
      { id: 'e2', channelId: 'run-1', eventType: 'step', cursor: 3, timestamp: 't' },
    ]);
    await expect(session.fork(1, 'fix')).rejects.toThrow(/non-monotonic cursors/);
  });
  it('replay fallback raises when step exceeds recorded steps', async () => {
    const { actae, session } = await started();
    await session.step('a');
    (session as unknown as { cursors: number[] }).cursors = [0];
    actae.replayEvents.set('run-1', [
      { id: 'e1', channelId: 'run-1', eventType: 'step', cursor: 1, timestamp: 't' },
    ]);
    await expect(session.fork(2, 'fix')).rejects.toThrow(/exceeds the 1 steps/);
  });
  it('lineage_only mode proceeds with no state copy', async () => {
    const { actae, session } = await started();
    await session.step('a');
    actae.forkErrors['fix'] = new SnapshotBoundaryError('no snapshot');
    const fork = await session.fork(1, 'fix', { boundaryMode: 'lineage_only' });
    expect(fork.forkReceipt?.childChannelId).toBe('fix');
  });
  it('contamination (resolved > requested) emits a warning and refuses to prime', async () => {
    const { actae, session } = await started();
    await session.step('a');
    actae.forkErrors['fix'] = new SnapshotBoundaryError('no snapshot');
    const fork = await session.fork(1, 'fix', { boundaryMode: 'approximate' });
    const errors: string[] = [];
    fork.onError((m) => errors.push(m));
    (fork as unknown as { requestedBoundary: number }).requestedBoundary = 1;
    (fork as unknown as { resolvedBoundary: number }).resolvedBoundary = 9;
    await (fork as unknown as {
      primeForkSession: (s: string, n: number) => Promise<void>;
    }).primeForkSession('run-1', 1);
    expect(errors.some((m) => m.includes('NOT priming'))).toBe(true);
    expect(fork.inheritedState).toBeUndefined();
    expect(actae.channels.has('run-1')).toBe(true);
  });
  it('primeForkSession wraps stateFn with the inherited snapshot', async () => {
    const { actae, session } = await started();
    await session.step('a');
    const fork = await session.fork(1, 'fix');
    const fn = (fork as unknown as { stateFn: () => Record<string, unknown> | undefined }).stateFn;
    expect(fn?.()).toEqual({ step: 1 });
    void actae;
  });
});

describe('AgentSession resume details', () => {
  it('resume forkAtStep requires a name', async () => {
    const { actae, session } = await started();
    await session.step('a');
    await expect(AgentSession.resume(actae as never, 'run-1', { forkAtStep: 1 })).rejects.toThrow(/name/);
  });
  it('resume parses string cursors from metadata', async () => {
    const { actae, session } = await started();
    await session.step('a');
    await session.step('b');
    const ch = actae.channels.get('run-1')!;
    ch.metadata['cursors'] = ['1', '2'];
    ch.metadata['step_count'] = 2;
    const resumed = await AgentSession.resume(actae as never, 'run-1', {});
    expect(resumed.cursorsList).toEqual([1, 2]);
    expect(resumed.stepCountValue).toBe(2);
  });
  it('resume with forkAtStep + exact boundary propagates NoRestorableCheckpointError', async () => {
    const { actae, session } = await started();
    await session.step('a');
    actae.forkErrors['forked'] = new SnapshotBoundaryError('none');
    await expect(
      AgentSession.resume(actae as never, 'run-1', { forkAtStep: 1, name: 'forked' }),
    ).rejects.toBeInstanceOf(NoRestorableCheckpointError);
  });
  it('resume restores inherited state from the last snapshot', async () => {
    const { actae, session } = await started();
    await session.step('a');
    await session.crash('boom');
    const resumed = await AgentSession.resume(actae as never, 'run-1', {});
    expect(resumed.inheritedState).toEqual({ step: 1 });
  });
  it('session-level resume wrapper', async () => {
    const { actae, session } = await started();
    await session.step('a');
    const resumed = await session.resume({});
    expect(resumed.sessionStatus).toBe('started');
    void actae;
  });
});

describe('AgentSession retry logic', () => {
  it('retries transient failures and succeeds on the second attempt', async () => {
    const { actae, session } = await started();
    const errors: string[] = [];
    session.onError((m) => errors.push(m));
    let calls = 0;
    const original = actae.updateMetadata.bind(actae);
    actae.updateMetadata = async (...args: [string, Record<string, unknown>]) => {
      calls++;
      if (calls === 1) throw new ConnectionError('flaky');
      return original(...args);
    };
    await session.complete();
    expect(calls).toBe(2);
    expect(errors.some((m) => m.includes('retrying'))).toBe(true);
  });
  it('non-retryable errors are rethrown immediately', async () => {
    const { actae, session } = await started();
    const original = actae.updateMetadata.bind(actae);
    actae.updateMetadata = async () => {
      throw new SessionError('not transient');
    };
    await expect(session.complete()).rejects.toBeInstanceOf(SessionError);
    actae.updateMetadata = original;
  });
  it('isRetryable treats 5xx and 429 as retryable, 4xx as not', async () => {
    const { session } = await started();
    const isRetryable = (session as unknown as { isRetryable: (e: unknown) => boolean }).isRetryable;
    expect(isRetryable(new ConnectionError('x'))).toBe(true);
    expect(isRetryable(new APIError(500, 'x'))).toBe(true);
    expect(isRetryable(new APIError(429, 'x'))).toBe(true);
    expect(isRetryable(new APIError(400, 'x'))).toBe(false);
    expect(isRetryable(new Error('plain'))).toBe(false);
  });
});

describe('AgentSession experiment helpers', () => {
  it('createExperiment + addToExperiment call the client', async () => {
    const { actae, session } = await started();
    const group = await session.createExperiment('exp-1', { description: 'd' });
    expect(group['name']).toBe('exp-1');
    const member = await session.addToExperiment(group['id'] as string, {
      role: 'variant',
      declaredDelta: { model: 'm' },
    });
    expect(member['channel_id']).toBe('run-1');
    void actae;
  });
  it('experiment helpers require a started channel', async () => {
    const { session } = newSession();
    await expect(session.setOutcome('promoted')).rejects.toThrow(/no channel/);
    await expect(session.addToExperiment('g')).rejects.toThrow(/no channel/);
  });
});

describe('buildManifest', () => {
  it('merges caller fields over the environment fingerprint', () => {
    const m = buildManifest({ model: 'gpt-5', seed: 42 });
    expect(m['model']).toBe('gpt-5');
    expect(m['seed']).toBe(42);
    expect((m['environment'] as Record<string, unknown>)['node']).toBeTypeOf('string');
  });
  it('works with no caller manifest', () => {
    const m = buildManifest();
    expect((m['environment'] as Record<string, unknown>)['actae']).toBeTypeOf('string');
  });
});
