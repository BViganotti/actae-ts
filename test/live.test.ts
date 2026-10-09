import { describe, expect, it } from 'vitest';
import { ActaeClient, AgentSession } from '../src/index.js';

const LIVE = process.env.ACTAE_LIVE === '1';
const url = process.env.ACTAE_URL ?? 'http://localhost:8002';
const apiKey = process.env.ACTAE_API_KEY ?? 'sk-dev-0000000000000000000000';

function client(): ActaeClient {
  return new ActaeClient({ apiKey, endpoint: url, timeout: 10000 });
}

describe.runIf(LIVE)('live Actae integration', () => {
  it('record → replay → state → fork round-trips', async () => {
    const c = client();
    const channel = `ts-live-${Date.now()}`;
    const ev = await c.record(channel, 'agent.step', { n: 1 }, { actor: 'agent' });
    expect(ev.cursor).toBeGreaterThan(0);

    const replayed = await c.replay(channel, { limit: 10 });
    expect(replayed.length).toBe(1);

    const cursor = (await c.getCursor(channel))!;
    await c.saveState(channel, cursor, { messages: ['a'] });
    const latest = await c.latestState(channel);
    expect(latest?.state).toEqual({ messages: ['a'] });

    const receipt = await c.fork(channel, `${channel}-fix`, cursor, { reason: 'live test' });
    expect(receipt.childChannelId).toBe(`${channel}-fix`);
    expect(receipt.restorable).toBe(true);
  });

  it('WebSocket subscribe + publish delivers events', async () => {
    // The server does not echo broadcasts to the publishing connection, so
    // delivery is verified with a separate subscriber (wire parity).
    const publisher = client();
    const subscriber = client();
    const channel = `ts-live-ws-${Date.now()}`;
    await publisher.connect();
    await subscriber.connect();
    const received: string[] = [];
    subscriber.onMessage((topic, e) => received.push(`${topic}:${e.eventType}`));
    await subscriber.subscribe(channel);
    const pub = await publisher.publish(channel, { hi: true });
    expect(pub.cursor).toBeGreaterThan(0);
    await new Promise((r) => setTimeout(r, 500));
    expect(received.length).toBeGreaterThanOrEqual(1);
    publisher.disconnect();
    subscriber.disconnect();
  });

  it('AgentSession step → fork at step N continues at N+1 with inherited state', async () => {
    const c = client();
    const session = new AgentSession(c, `ts-live-sess-${Date.now()}`, {
      stateFn: () => ({ count: session.stepCountValue }),
      snapshotInterval: 1,
    });
    await session.start();
    for (let i = 1; i <= 4; i++) {
      await session.step('agent.step', { input: { n: i }, output: { ok: true } });
    }
    const fork = await session.fork(3, `${session.channelId}-fix`, { reason: 'live fork' });
    await fork.start();
    const refined = await fork.step('agent.step', { input: { refined: true }, output: { ok: true } });
    expect(refined.cursor).toBeGreaterThan(0);
    expect(fork.stepCountValue).toBe(4);
    expect(fork.inheritedState).toBeDefined();
    await session.complete();
  });
});
