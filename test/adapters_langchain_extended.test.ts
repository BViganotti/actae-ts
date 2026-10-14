import { describe, expect, it, vi } from 'vitest';
import {
  ActaeContextSaver,
  ChainResumer,
} from '../src/adapters/langchain.js';
import { FakeActaeClient } from './helpers.js';

describe('ActaeContextSaver edge forks', () => {
  it('handleLLMEnd appends message-based generations', async () => {
    const actae = new FakeActaeClient();
    const saver = new ActaeContextSaver(actae as never, { channel: 'lc', saveEveryN: 1 });
    await saver.handleLLMEnd({
      generations: [[{ message: { content: 'via-message' } }]],
    });
    await saver.flush();
    const snap = await actae.latestState('lc');
    const msgs = (snap!.state as { context: { messages: Array<{ content: string }> } }).context.messages;
    expect(msgs[0]!.content).toBe('{"content":"via-message"}');
  });

  it('handleLLMEnd falls back to stringified output on access failure', async () => {
    const actae = new FakeActaeClient();
    const saver = new ActaeContextSaver(actae as never, { channel: 'lc', saveEveryN: 1 });
    const throwing = new Proxy({}, { get() { throw new Error('boom'); } });
    await saver.handleLLMEnd(throwing);
    await saver.flush();
    const snap = await actae.latestState('lc');
    const msgs = (snap!.state as { context: { messages: Array<{ content: string }> } }).context.messages;
    expect(msgs[0]!.content.length).toBeGreaterThan(0);
  });

  it('serialize stringifies un-serializable members', async () => {
    const actae = new FakeActaeClient();
    const saver = new ActaeContextSaver(actae as never, { channel: 'lc', saveEveryN: 1 });
    const circular: Record<string, unknown> = {};
    circular['self'] = circular;
    await saver.handleToolEnd(circular);
    await saver.flush();
    const snap = await actae.latestState('lc');
    const outs = (snap!.state as { context: { tool_outputs: Array<{ output: unknown }> } }).context.tool_outputs;
    expect(outs[0]!.output).toBeDefined();
  });

  it('stringify tolerates bigint and malformed input', async () => {
    const actae = new FakeActaeClient();
    const saver = new ActaeContextSaver(actae as never, { channel: 'lc', saveEveryN: 1 });
    await saver.handleLLMEnd({ generations: [[{ text: '' }]] }); // empty text → stringify(output)
    await saver.flush();
    const snap = await actae.latestState('lc');
    const msgs = (snap!.state as { context: { messages: Array<{ content: string }> } }).context.messages;
    expect(msgs[0]!.content).toBeTypeOf('string');
  });

  it('loadContext returns undefined when state has no context key', async () => {
    const actae = new FakeActaeClient();
    const saver = new ActaeContextSaver(actae as never, { channel: 'lc' });
    await actae.saveState('lc', 1, { not_context: true });
    expect(await saver.loadContext()).toBeUndefined();
  });

  it('saveNow logs a warning when the save fails', async () => {
    const actae = new FakeActaeClient();
    const saver = new ActaeContextSaver(actae as never, { channel: 'lc', saveEveryN: 1 });
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    const original = actae.saveState.bind(actae);
    actae.saveState = async () => {
      throw new Error('db down');
    };
    await saver.handleLLMEnd({ generations: [[{ text: 'hi' }]] });
    await saver.flush();
    expect(warn).toHaveBeenCalled();
    warn.mockRestore();
    actae.saveState = original;
  });
});

describe('ChainResumer forks', () => {
  it('resume invokes with defaultInput when no context exists', async () => {
    const actae = new FakeActaeClient();
    const resumer = new ChainResumer(actae as never, { channel: 'lc' });
    const chain = { invoke: vi.fn(async (input: unknown) => input) };
    await resumer.resume(chain, { seed: 1 });
    expect(chain.invoke).toHaveBeenCalledWith({ seed: 1 }, expect.anything());
  });

  it('resume seeds from the last assistant message when context exists', async () => {
    const actae = new FakeActaeClient();
    const saver = new ActaeContextSaver(actae as never, { channel: 'lc', saveEveryN: 1 });
    await saver.handleLLMEnd({ generations: [[{ text: 'continue here' }]] });
    await saver.flush();
    const resumer = new ChainResumer(actae as never, { channel: 'lc' });
    const chain = { invoke: vi.fn(async (input: unknown) => input) };
    await resumer.resume(chain, { seed: 1 });
    expect(chain.invoke).toHaveBeenCalledWith('continue here', expect.anything());
  });

  it('fork falls back to at_cursor=0 when the boundary fork fails', async () => {
    const actae = new FakeActaeClient();
    await actae.record('lc', 'step', {}, {}); // gives latestCursor > 0
    actae.forkErrors['lc-fork'] = new Error('no boundary');
    const resumer = new ChainResumer(actae as never, { channel: 'lc' });
    const forked = await resumer.fork('lc-fork');
    expect(forked.channel).toBe('lc-fork');
    const f = actae.forks.at(-1);
    expect(f?.atCursor).toBe(0);
  });
});
