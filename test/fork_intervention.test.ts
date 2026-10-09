import { describe, expect, it } from 'vitest';
import { AgentSession } from '../src/session.js';
import { ForkToolBlockedError } from '../src/errors.js';
import { ForkToolBlockedError as IndexForkToolBlockedError } from '../src/index.js';
import { forkReceiptFromDict } from '../src/types.js';
import { FakeActaeClient } from './helpers.js';

async function started(opts: Record<string, unknown> = {}) {
  const actae = new FakeActaeClient();
  const session = new AgentSession(actae as never, 'run-1', {
    stateFn: () => ({ step: 'live' }),
    snapshotInterval: 1,
    ...opts,
  } as never);
  await session.start();
  for (let i = 0; i < 3; i++) await session.step('step', { input: i });
  return { actae, session };
}

describe('AgentSession fork intervention + tool policies', () => {
  it('records the intervention on the fork metadata and exposes it', async () => {
    const { session } = await started();
    const intervention = { model: 'candidate-model', temperature: 0.2 };
    const fork = await session.fork(2, 'fork-2', {
      intervention,
      toolPolicies: { 'email.send': 'block', '*': 'auto' },
    });
    expect(fork.intervention).toEqual(intervention);
    expect(fork.toolPolicies).toEqual({ 'email.send': 'block', '*': 'auto' });
    expect(fork.forkReceipt?.toolPolicies).toEqual({ 'email.send': 'block', '*': 'auto' });
  });

  it('sends toolPolicies on the fork request', async () => {
    const { actae, session } = await started();
    await session.fork(2, 'fork-2', {
      intervention: { model: 'x' },
      toolPolicies: { 'email.send': 'replay' },
    });
    expect(actae.forkOptions['fork-2']?.['toolPolicies']).toEqual({ 'email.send': 'replay' });
    const meta = actae.channels.get('fork-2')!.metadata;
    expect(meta['intervention']).toEqual({ model: 'x' });
  });

  it('resume(forkAtStep) forwards intervention and toolPolicies', async () => {
    const { actae, session } = await started();
    const resumed = await AgentSession.resume(actae as never, session.channelId, {
      forkAtStep: 1,
      name: 'fork-resume',
      intervention: { model: 'resumed' },
      toolPolicies: { 'email.send': 'replay' },
    });
    expect(resumed.intervention).toEqual({ model: 'resumed' });
    expect(resumed.toolPolicies).toEqual({ 'email.send': 'replay' });
    const meta = actae.channels.get('fork-resume')!.metadata;
    expect(meta['intervention']).toEqual({ model: 'resumed' });
  });

  it('ForkToolBlockedError is exported and distinct', () => {
    expect(ForkToolBlockedError).toBe(IndexForkToolBlockedError);
    const err = new ForkToolBlockedError('blocked');
    expect(err).toBeInstanceOf(Error);
    expect(err.name).toBe('ForkToolBlockedError');
  });

  it('forkReceiptFromDict parses tool_policies', () => {
    const r = forkReceiptFromDict({
      child_channel_id: 'c',
      tool_policies: { 'email.send': 'block' },
    });
    expect(r.toolPolicies).toEqual({ 'email.send': 'block' });
  });
});
