import { describe, expect, it } from 'vitest';
import { CallbackManager } from '@langchain/core/callbacks/manager';
import { ActaeContextSaver, ChainResumer } from '../src/adapters/langchain.js';
import { FakeActaeClient } from './helpers.js';

/**
 * Real-framework integration against the actual `@langchain/core` callback
 * manager: a real `CallbackManager` drives `ActaeContextSaver` through its
 * handler methods exactly as a LangChain run would.
 */

async function driveSaver(saver: ActaeContextSaver) {
  const manager = await CallbackManager.configure([saver]);
  const llmRun = (await manager.handleLLMStart({ name: 'test-model' }, ['hello'], 'run-llm-1'))[0];
  await llmRun.handleLLMEnd({ generations: [[{ text: 'assistant reply' }]] });

  const toolRun = await manager.handleToolStart({ name: 'search' }, ['query'], 'run-tool-1');
  await toolRun.handleToolEnd('search result');

  const chainRun = await manager.handleChainStart({ name: 'my-chain' }, ['input'], 'run-chain-1');
  await chainRun.handleChainEnd({ output: 'final' });

  await saver.flush();
}

describe('ActaeContextSaver × real @langchain/core callbacks', () => {
  it('captures LLM/tool/chain callbacks into a context snapshot', async () => {
    const actae = new FakeActaeClient();
    const saver = new ActaeContextSaver(actae as never, { channel: 'lc', saveEveryN: 1 });
    await driveSaver(saver);

    const ctx = await saver.loadContext();
    expect(ctx?.['messages']).toHaveLength(1);
    expect((ctx!['messages'] as Array<{ role: string; content: string }>)[0]).toMatchObject({
      role: 'assistant',
      content: 'assistant reply',
    });
    expect(ctx!['tool_outputs']).toHaveLength(1);
    expect(ctx!['chain_steps']).toHaveLength(1);

    const stored = actae.channels.get('lc')!.states;
    expect(stored.length).toBeGreaterThan(0);
  });

  it('ChainResumer.resume seeds a real invoke from the saved context', async () => {
    const actae = new FakeActaeClient();
    const saver = new ActaeContextSaver(actae as never, { channel: 'lc', saveEveryN: 1 });
    await driveSaver(saver);

    const resumer = new ChainResumer(actae as never, { channel: 'lc' });
    let invoked: unknown;
    const chain = {
      async invoke(input: unknown) {
        invoked = input;
        return 'result';
      },
    };
    await resumer.resume(chain);
    expect(invoked).toBe('assistant reply');
  });
});
