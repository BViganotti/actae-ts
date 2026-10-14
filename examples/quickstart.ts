/**
 * Actae TypeScript SDK — quickstart example.
 *
 * Record an event, replay it back, then fork the channel.
 *
 * Run against dev mode:
 *   cd actae && cargo run -- --dev
 *   cd sdks/typescript && node examples/quickstart.ts
 */

import { ActaeClient, newClientFromEnv } from '../dist/index.js';

async function main() {
  const client = newClientFromEnv({
    apiKey: 'sk-dev-0000000000000000000000',
    endpoint: 'http://localhost:8002',
  });

  const channel = `demo-${Date.now()}`;

  // Record an event.
  const event = await client.record(channel, 'agent.step', {
    input: 'hello',
    model: 'test-model',
  }, { actor: 'agent' });
  console.log(`recorded ${event.eventType} cursor=${event.cursor} id=${event.id}`);

  // Replay it back.
  const events = await client.replay(channel, { limit: 10 });
  console.log(`replayed ${events.length} event(s)`);

  // Save state and fork the channel at the current cursor.
  const cursor = (await client.getCursor(channel)) ?? 0;
  await client.saveState(channel, cursor, { messages: ['hello'] });
  const receipt = await client.fork(channel, `${channel}-fix`, cursor, {
    displayName: `${channel}-fix`,
    reason: 'refine the next step',
  });
  console.log(`forked ${channel} → ${receipt.childChannelId} (restorable=${receipt.restorable})`);

  // WebSocket realtime.
  await client.connect();
  client.onMessage((topic, ev) => console.log(`ws[${topic}] ${ev.eventType}`));
  await client.subscribe(channel);
  const pub = await client.publish(channel, { note: 'over ws' });
  console.log(`published over ws cursor=${pub.cursor}`);

  client.disconnect();
  process.exit(0);
}

main().catch((err) => {
  console.error('quickstart failed:', err);
  process.exit(1);
});
