/**
 * Actae TypeScript SDK — AgentSession example: step → fork → resume.
 *
 * Runs a 5-step agent, forks at step 3 into a "fix" fork, and shows the
 * inherited state (steps 1..3's data) so step 4 can run against it without
 * re-running steps 1-3.
 *
 * Run against dev mode:
 *   cd actae && cargo run -- --dev
 *   cd sdks/typescript && node examples/session.ts
 */

import { ActaeClient, AgentSession } from '../dist/index.js';

async function main() {
  const client = new ActaeClient({
    apiKey: 'sk-dev-0000000000000000000000',
    endpoint: 'http://localhost:8002',
  });

  const state = { messages: [] as string[], step: 0 };
  const session = new AgentSession(client, `session-${Date.now()}`, {
    displayName: 'session demo',
    stateFn: () => ({ ...state, messages: [...state.messages] }),
    snapshotInterval: 1,
  });

  await session.start();
  for (let i = 1; i <= 5; i++) {
    const out = await session.step('agent.step', { input: { n: i }, output: { ok: true } });
    state.messages.push(`step ${i}`);
    state.step = i;
    console.log(`step ${i} → cursor ${out.cursor}`);
  }

  // Fork at step 3: the fork continues at step 4 with steps 1..3's state.
  const fork = await session.fork(3, `${session.channelId}-fix`, { reason: 'refine step 4' });
  await fork.start();
  const inheritedMessages = fork.inheritedState?.messages;
  const messageCount = Array.isArray(inheritedMessages) ? inheritedMessages.length : 0;
  console.log(`forked: stepCount=${fork.stepCountValue} inheritedState.messages=${messageCount}`);

  const refined = await fork.step('agent.step', { input: { refined: true }, output: { ok: true } });
  console.log(`fork step 4 → cursor ${refined.cursor} (step count ${fork.stepCountValue})`);

  await session.complete();
  client.disconnect();
  process.exit(0);
}

main().catch((err) => {
  console.error('session demo failed:', err);
  process.exit(1);
});
