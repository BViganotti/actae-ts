import { describe, expect, it } from 'vitest';
import { AgentSession, SESSION_STARTED, SESSION_COMPLETED } from '../src/session.js';
import { SnapshotBoundaryError, ConnectionError, SessionError, SessionCompletedError, NoRestorableCheckpointError } from '../src/errors.js';
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

describe('AgentSession lifecycle', () => {
  it('start records session.started and sets channel id', async () => {
    const { actae, session } = newSession();
    expect(session.sessionStatus).toBe('created');
    await session.start();
    expect(session.channelId).toBe('run-1');
    expect(session.sessionStatus).toBe('started');
    const rec = actae.recordedEvents.find((r) => r.eventType === SESSION_STARTED);
    expect(rec).toBeTruthy();
    expect(rec!.opts['actor']).toBe('agent_session');
  });

  it('step requires started status', async () => {
    const { session } = newSession();
    await expect(session.step('agent.step', {})).rejects.toBeInstanceOf(SessionError);
  });

  it('step records event with step_number and operation_id, appends cursor', async () => {
    const { actae, session } = await started();
    const ev = await session.step('agent.step', { input: { q: 'hi' }, output: { a: 1 } });
    expect(ev.eventType).toBe('agent.step');
    expect(session.stepCountValue).toBe(1);
    expect(session.cursorsList).toEqual([ev.cursor]);
    const rec = actae.recordedEvents.find((r) => r.eventType === 'agent.step');
    expect(rec!.opts['stepNumber']).toBe(1);
    expect(rec!.opts['operationId']).toBeTypeOf('string');
    expect(rec!.opts['metadata']).toMatchObject({ step_number: 1 });
  });

  it('saves a state snapshot on each step (snapshotInterval=1)', async () => {
    const { actae, session } = await started();
    await session.step('agent.step');
    const states = actae.channels.get('run-1')!.states;
    expect(states.length).toBe(1);
    expect(states[0]!.state).toEqual({ step: 1 });
  });

  it('saves every N steps with snapshotInterval>1', async () => {
    const { actae, session } = await started({ snapshotInterval: 2 });
    await session.step('a');
    expect(actae.channels.get('run-1')!.states.length).toBe(0);
    await session.step('b');
    expect(actae.channels.get('run-1')!.states.length).toBe(1);
  });

  it('retries transient failures with a fresh operation_id', async () => {
    const { actae, session } = await started();
    let calls = 0;
    const original = actae.record.bind(actae);
    actae.record = async (...args: [string, string, unknown, Record<string, unknown>]) => {
      calls++;
      if (calls === 1) throw new ConnectionError('flaky');
      return original(...args);
    };
    const ev = await session.step('agent.step');
    expect(ev.cursor).toBeGreaterThan(0);
    expect(calls).toBe(2);
  });

  it('complete records session.completed', async () => {
    const { actae, session } = await started();
    await session.step('a');
    await session.complete();
    expect(session.sessionStatus).toBe('completed');
    expect(actae.recordedEvents.some((r) => r.eventType === SESSION_COMPLETED)).toBe(true);
  });

  it('crash updates metadata with reason', async () => {
    const { actae, session } = await started();
    await session.crash('out of tokens');
    expect(session.sessionStatus).toBe('crashed');
    const meta = actae.channels.get('run-1')!.metadata;
    expect(meta['status']).toBe('crashed');
    expect(meta['crash_reason']).toBe('out of tokens');
  });
});

describe('AgentSession fork receipts', () => {
  it('resumes the exact child named by a receipt', async () => {
    const actae = new FakeActaeClient();
    await new AgentSession(actae as never, 'child', {}).start();
    const resumed = await AgentSession.resumeFromFork(actae as never, {
      forkId: 'child', childChannelId: 'child', requestedCursor: 1, resolvedCursor: 1,
      sourceStateVersion: 1, restorable: true, replayed: false,
    });
    expect(resumed.channelId).toBe('child');
  });
});

describe('AgentSession fork', () => {
  it('forks at a step with boundary provenance', async () => {
    const { actae, session } = await started();
    await session.step('a');
    await session.step('b');
    const fork = await session.fork(2, 'run-1-fix', { reason: 'refine' });
    expect(fork.channelId).toBe('run-1-fix');
    expect(fork.stepCountValue).toBe(2);
    expect(fork.inheritedState).toEqual({ step: 1 });
    expect(fork.forkReceipt?.childChannelId).toBe('run-1-fix');
    expect(fork.forkReceipt?.restorable).toBe(true);
    const f = actae.forks.find((x) => x.child === 'run-1-fix');
    expect(f?.source).toBe('run-1');
  });

  it('fork primes cursors from the parent metadata (steps 1..N)', async () => {
    const { actae, session } = await started();
    await session.step('a');
    await session.step('b');
    await session.step('c');
    // Simulate a fully-synced channel (metadata cursors caught up to the run).
    actae.channels.get('run-1')!.metadata['cursors'] = session.cursorsList;
    const fork = await session.fork(2, 'run-1-fix');
    expect(fork.cursorsList).toEqual([session.cursorsList[0], session.cursorsList[1]]);
  });

  it('exact mode raises NoRestorableCheckpointError when no boundary', async () => {
    const { actae, session } = await started();
    await session.step('a');
    actae.forkErrors['run-1-fix'] = new SnapshotBoundaryError('no snapshot');
    await expect(session.fork(1, 'run-1-fix')).rejects.toBeInstanceOf(NoRestorableCheckpointError);
  });

  it('approximate mode falls back to latest state', async () => {
    const { actae, session } = await started();
    await session.step('a');
    actae.forkErrors['run-1-fix'] = new SnapshotBoundaryError('no snapshot');
    const fork = await session.fork(1, 'run-1-fix', { boundaryMode: 'approximate' });
    expect(fork.forkReceipt?.requestedCursor).toBeGreaterThan(0);
  });

  it('approximate mode refuses contaminated state when resolved beyond boundary', async () => {
    // Parent state exists at a LATER cursor (step 3) than the requested fork
    // boundary (step 1). The approximate fallback resolves to the latest
    // snapshot, which contains data from AFTER the fork point — the SDK must
    // NOT expose it as steps 1..1's data.
    const { actae, session } = await started();
    await session.step('a');
    await session.step('b');
    await session.step('c'); // latest snapshot is now at cursor 3
    actae.forkErrors['run-1-fix'] = new SnapshotBoundaryError('no snapshot');
    const fork = await session.fork(1, 'run-1-fix', { boundaryMode: 'approximate' });
    expect(fork.resolvedBoundaryCursor).toBeGreaterThan(fork.requestedBoundaryCursor);
    expect(fork.inheritedState).toBeUndefined();
    expect(fork.boundaryRestorable).toBe(true);
  });

  it('approximate mode inherits normally when boundary is exact', async () => {
    // Fork at step 2 with a snapshot at/before cursor 2: no drift, the
    // inherited state is steps 1..2's data.
    const { actae, session } = await started();
    await session.step('a');
    await session.step('b');
    actae.channels.get('run-1')!.metadata['cursors'] = session.cursorsList;
    const fork = await session.fork(2, 'run-1-fix', { boundaryMode: 'approximate' });
    expect(fork.resolvedBoundaryCursor).toBe(fork.requestedBoundaryCursor);
    expect(fork.inheritedState).toBeDefined();
  });

  it('lineage_only mode forks without a state copy', async () => {
    const { actae, session } = await started();
    await session.step('a');
    // No state snapshots on the parent: strict would raise, lineage_only
    // creates the fork with no state copy.
    actae.channels.get('run-1')!.states = [];
    actae.forkErrors['run-1-lo'] = new SnapshotBoundaryError('no snapshot');
    const fork = await session.fork(1, 'run-1-lo', { boundaryMode: 'lineage_only' });
    expect(fork.boundaryRestorable).toBe(false);
    expect(fork.resolvedBoundaryCursor).toBe(0);
    expect(fork.inheritedState).toBeUndefined();
  });

  it('does NOT wrap stateFn — the fork persists its own evolving live state', async () => {
    const live: Record<string, unknown> = { prefix: true };
    const { actae, session } = newSession({ stateFn: () => live });
    await session.start();
    for (let i = 1; i <= 5; i++) {
      live['step'] = i;
      await session.step('step');
    }
    const fork = await session.fork(5, 'run-1-fix', { stateFn: () => live });
    expect(fork.inheritedState).toBeDefined();
    await fork.start();
    live['prefix'] = false;
    live['refined'] = true;
    await fork.step('step6');
    const states = actae.channels.get('run-1-fix')!.states;
    const last = states[states.length - 1]!.state;
    // The caller's stateFn is intentionally NOT wrapped: the fork must
    // persist its OWN evolving live state, not a frozen inherited copy.
    expect(last['refined']).toBe(true);
    expect(last['prefix']).toBe(false);
  });

  it('validates boundary_mode', async () => {
    const { session } = await started();
    await session.step('a');
    await expect(session.fork(1, 'x', { boundaryMode: 'bogus' as never })).rejects.toThrow(/invalid boundary_mode/);
  });

  it('fork requires a name', async () => {
    const { session } = await started();
    await session.step('a');
    await expect(session.fork(1, '')).rejects.toThrow(/fork name is required/);
  });
});

describe('AgentSession resume', () => {
  it('crash recovery resumes from stored metadata', async () => {
    const { actae, session } = await started();
    await session.step('a');
    await session.step('b');
    await session.crash('boom');
    const resumed = await AgentSession.resume(actae as never, 'run-1', {});
    expect(resumed.sessionStatus).toBe('started');
    expect(resumed.stepCountValue).toBe(2);
    expect(resumed.cursorsList).toEqual(session.cursorsList);
  });

  it('resume completed raises SessionCompletedError', async () => {
    const { actae, session } = await started();
    await session.step('a');
    await session.complete();
    await expect(AgentSession.resume(actae as never, 'run-1', {})).rejects.toBeInstanceOf(SessionCompletedError);
  });

  it('resume with forkAtStep forks into a new channel', async () => {
    const { actae, session } = await started();
    await session.step('a');
    await session.step('b');
    const resumed = await AgentSession.resume(actae as never, 'run-1', { forkAtStep: 1, name: 'forked' });
    expect(resumed.channelId).toBe('forked');
    expect(resumed.stepCountValue).toBe(1);
  });

  it('resume missing channel raises SessionError', async () => {
    const { actae } = newSession();
    await expect(AgentSession.resume(actae as never, 'nope', {})).rejects.toBeInstanceOf(SessionError);
  });
});

describe('AgentSession experiment helpers', () => {
  it('setOutcome and promote call the client', async () => {
    const { actae, session } = await started();
    await session.setOutcome('promoted', 0.9);
    await session.promote();
    // no throw = passed; client doubles record nothing here
    expect(actae.channels.has('run-1')).toBe(true);
  });
});

describe('AgentSession edge cases', () => {
  it('cursorForStep validates bounds', async () => {
    const { session } = await started();
    await session.step('a');
    await expect(session.fork(0, 'x')).rejects.toThrow(/step_number must be >= 1/);
    await expect(session.fork(5, 'x')).rejects.toThrow(/exceeds the 1 steps/);
  });

  it('resume merges stored + provided params', async () => {
    const { actae, session } = await started();
    await session.step('a');
    const resumed = await AgentSession.resume(actae as never, 'run-1', { params: { extra: 1 } });
    expect(resumed.paramsValue).toMatchObject({ extra: 1 });
  });

  it('step before start raises SessionError', async () => {
    const { session } = newSession();
    await expect(session.step('agent.step')).rejects.toBeInstanceOf(SessionError);
  });

  it('resume on a fork uses the session resume wrapper', async () => {
    const { actae, session } = await started();
    await session.step('a');
    const resumed = await session.resume({});
    expect(resumed.sessionStatus).toBe('started');
    expect(actae.channels.has('run-1')).toBe(true);
  });

  it('resume wrapper requires a started channel', async () => {
    const { session } = newSession();
    await expect(session.resume({})).rejects.toThrow(/no channel/);
  });
});

async function started(opts: Record<string, unknown> = {}) {
  const h = newSession(opts);
  await h.session.start();
  return h;
}

describe('AgentSession hard-kill recovery', () => {
  it('reconstructs step progress from the event log when metadata lags', async () => {
    const { actae, session } = await started();
    await session.step('s')
    await session.step('s')
    await session.step('s')
    // Simulate a hard process death: metadata never learned about steps 2..3.
    const meta = actae.channels.get('run-1')!.metadata
    meta['step_count'] = 1
    meta['cursors'] = [2]

    const resumed = await AgentSession.resume(actae as never, 'run-1', {})
    expect(resumed.stepCountValue).toBe(3)
    expect(resumed.cursorsList).toHaveLength(3)
  })
})
