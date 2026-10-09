/**
 * Live side-effect-aware fork + recovery + approval suite (opt-in).
 *
 * Proves, against a real Actae server, the TypeScript SDK's:
 *   - fork `toolPolicies`: `auto` replays an inherited result, `block` refuses
 *     a new irreversible call with a persisted `tool.blocked` boundary;
 *   - `intervention` descriptor round-trip;
 *   - `latestStepNumber` + `resume` progress recovery;
 *   - `requestApproval` / `decideApproval` idempotency.
 *
 *     ACTAE_LIVE=1 npm run test:live -- test/fork_intervention_live.test.ts
 */

import { describe, expect, it } from 'vitest';
import { randomUUID } from 'node:crypto';
import { ActaeClient, AgentSession, ForkToolBlockedError } from '../src/index.js';

const LIVE = process.env.ACTAE_LIVE === '1';
const url = process.env.ACTAE_URL ?? 'http://localhost:8002';
const apiKey = process.env.ACTAE_API_KEY ?? 'sk-dev-0000000000000000000000';

function client(): ActaeClient {
  return new ActaeClient({ apiKey, endpoint: url, timeout: 15000 });
}

function chan(prefix: string): string {
  return `ts-intervention-${prefix}-${randomUUID()}`;
}

async function baseline(actae: ActaeClient, name: string): Promise<AgentSession> {
  const session = new AgentSession(actae, name, {
    stateFn: () => ({ step: session.stepCountValue }),
    snapshotInterval: 1,
  });
  await session.start();
  await session.step('research', { output: { sources: 3 } });
  const claim = await actae.claimExecution(name, 'email-1', 'email.send', { to: 'a@b.c' });
  expect(claim.status).toBe('claimed');
  await actae.completeExecution(claim.execution.id, claim.claimToken, { messageId: 'm1' });
  await session.step('send_email', { output: { messageId: 'm1' } });
  return session;
}

describe.runIf(LIVE)('live fork intervention & recovery', () => {
  it('auto replays an inherited effect; block refuses a new one; intervention round-trips', async () => {
    const actae = client();
    const base = chan('base');
    const session = await baseline(actae, base);

    const auto = await session.fork(2, chan('auto'));
    const replayed = await actae.claimExecution(auto.channelId, 'email-1', 'email.send', { to: 'a@b.c' });
    expect(replayed.status).toBe('replayed');
    expect(replayed.result).toEqual({ messageId: 'm1' });

    const blocked = await session.fork(2, chan('block'), { toolPolicies: { 'email.send': 'block' } });
    let refused = false;
    try {
      await actae.claimExecution(blocked.channelId, 'email-2', 'email.send', { to: 'x@y.z' });
    } catch (err) {
      refused = err instanceof ForkToolBlockedError;
    }
    expect(refused).toBe(true);

    const intervention = { model: 'candidate-model' };
    const child = await session.fork(1, chan('intervention'), { intervention });
    expect(child.intervention).toEqual(intervention);
    const meta = await actae.getChannelMetadata(child.channelId);
    expect((meta?.experimentMetadata as Record<string, unknown>)?.['intervention']).toEqual(intervention);
  });

  it('latestStepNumber reflects progress and resume recovers it', async () => {
    const actae = client();
    const name = chan('recover');
    await baseline(actae, name);
    const last = await actae.latestStepNumber(name);
    expect(last).toBe(2);
    const resumed = await AgentSession.resume(actae, name, {});
    expect(resumed.stepCountValue).toBe(2);
  });

  it('decideApproval is idempotent per decision', async () => {
    const actae = client();
    const name = chan('approval');
    const ack = await actae.record(name, 'seed', {}, { actor: 'test' });
    expect(ack.cursor).toBeGreaterThan(0);

    const requestId = await actae.requestApproval(name, { summary: 'Approve invoice' });
    const first = await actae.decideApproval(name, requestId, { decision: 'approved', actor: 'alice' });
    const retry = await actae.decideApproval(name, requestId, { decision: 'approved', actor: 'alice' });
    expect(retry.id).toBe(first.id);
    const decided = await actae.replay(name, { eventType: 'approval.decided' });
    expect(decided).toHaveLength(1);
  });
});
