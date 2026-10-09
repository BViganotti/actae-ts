/**
 * Actae TypeScript SDK — live smoke test against a running Actae.
 *
 * End-to-end: health → record/replay → state → fork → WebSocket
 * subscribe/publish → AgentSession step/fork.
 *
 * Usage:
 *   cd actae && cargo run -- --dev
 *   cd sdks/typescript && npm run build && npm run smoke
 *   ACTAE_URL=http://localhost:8002 ACTAE_API_KEY=sk-dev-... npm run smoke
 */

import { ActaeClient, newClientFromEnv, AgentSession } from '../dist/index.js';

function assert(cond, msg) {
  if (!cond) {
    throw new Error(`SMOKE FAIL: ${msg}`);
  }
  console.log(`  ok — ${msg}`);
}

async function main() {
  const client = newClientFromEnv({
    apiKey: process.env.ACTAE_API_KEY ?? 'sk-dev-0000000000000000000000',
    endpoint: process.env.ACTAE_URL ?? 'http://localhost:8002',
  });

  console.log('health');
  const health = await client.healthCheck();
  assert(health.status === 'healthy' || health.status === 'ok', `health=${health.status}`);

  const channel = `smoke-${Date.now()}`;

  console.log('events');
  const ev = await client.record(channel, 'agent.step', { input: 'hello' }, { actor: 'agent' });
  assert(ev.cursor > 0, `recorded cursor=${ev.cursor}`);
  const replayed = await client.replay(channel, { limit: 10 });
  assert(replayed.length === 1, `replayed ${replayed.length}`);

  console.log('state + fork');
  const cursor = (await client.getCursor(channel)) ?? 0;
  await client.saveState(channel, cursor, { messages: ['hello'] });
  const latest = await client.latestState(channel);
  assert(latest?.state.messages.length === 1, 'state saved');
  const receipt = await client.fork(channel, `${channel}-fix`, cursor, { reason: 'smoke' });
  assert(receipt.childChannelId === `${channel}-fix`, 'forked');

  console.log('websocket');
  // The server does not echo broadcasts to the publishing connection, so
  // delivery is verified with a separate subscriber (wire parity). For a
  // single client, use `echoSelf: true` (see sdks/go/README.md).
  const subscriber = newClientFromEnv({
    apiKey: process.env.ACTAE_API_KEY ?? 'sk-dev-0000000000000000000000',
    endpoint: process.env.ACTAE_URL ?? 'http://localhost:8002',
  });
  await client.connect();
  assert(client.isConnected(), 'connected');
  const received = [];
  subscriber.onMessage((topic, e) => received.push([topic, e]));
  await subscriber.connect();
  await subscriber.subscribe(channel);
  await new Promise((r) => setTimeout(r, 100));
  const pub = await client.publish(channel, { note: 'over ws' });
  assert(pub.cursor > 0, `published cursor=${pub.cursor}`);
  await new Promise((r) => setTimeout(r, 300));
  assert(received.length >= 1, `ws delivered ${received.length} event(s)`);
  client.disconnect();
  subscriber.disconnect();

  console.log('AgentSession step/fork');
  const session = new AgentSession(client, `${channel}-session`, {
    stateFn: () => ({ messages: ['s1'] }),
    snapshotInterval: 1,
  });
  await session.start();
  await session.step('agent.step', { input: { n: 1 }, output: { ok: true } });
  await session.step('agent.step', { input: { n: 2 }, output: { ok: true } });
  const fork = await session.fork(1, `${session.channelId}-fix`, { reason: 'smoke fork' });
  await fork.start();
  await fork.step('agent.step', { input: { refined: true }, output: { ok: true } });
  assert(fork.stepCountValue === 2, `fork continues at step ${fork.stepCountValue}`);
  assert(fork.inheritedState !== undefined, 'fork inherited state');
  await session.complete();

  console.log('SMOKE PASS');
  process.exit(0);
}

main().catch((err) => {
  console.error('SMOKE FAIL:', err);
  process.exit(1);
});
