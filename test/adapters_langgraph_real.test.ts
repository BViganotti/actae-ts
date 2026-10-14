import { describe, expect, it } from 'vitest';
import { StateGraph } from '@langchain/langgraph';
import { ActaeCheckpointSaver } from '../src/adapters/langgraph.js';
import { FakeActaeClient } from './helpers.js';

/**
 * Real-framework integration: run an actual LangGraph.js StateGraph through
 * the ActaeCheckpointSaver. Uses the in-memory FakeActaeClient (the adapter
 * only ever talks to the Actae client surface), so no server is required.
 */

function buildGraph(saver: ActaeCheckpointSaver) {
  const g = new StateGraph({ channels: { value: { reducer: (a: number, b: number) => b } } } as never);
  g.addNode('increment', async (s: { value?: number }) => ({ value: (s.value ?? 0) + 1 }));
  g.addEdge('__start__', 'increment');
  g.addEdge('increment', '__end__');
  return g.compile({ checkpointer: saver } as never);
}

// LangGraph's root-thread config carries checkpoint_ns as undefined → ''.
const cfg = (threadId: string, extra: Record<string, unknown> = {}) => ({
  configurable: { thread_id: threadId, ...extra },
});

describe('ActaeCheckpointSaver × real LangGraph.js', () => {
  it('compiles and runs a graph, persisting checkpoints in Actae', async () => {
    const actae = new FakeActaeClient();
    const saver = new ActaeCheckpointSaver(actae as never, { channel: 'lg' });
    const graph = buildGraph(saver);

    const result = await graph.invoke({}, cfg('t1'));
    expect(result).toEqual({ value: 1 });

    const tuple = await saver.getTuple(cfg('t1'));
    expect(tuple).toBeDefined();
    expect(tuple!.checkpoint).toMatchObject({
      channel_values: { value: 1 },
    });
    expect(typeof tuple!.checkpoint.id).toBe('string');
  });

  it('accumulates state across invocations and getState resumes', async () => {
    const actae = new FakeActaeClient();
    const saver = new ActaeCheckpointSaver(actae as never, { channel: 'lg' });
    const graph = buildGraph(saver);

    await graph.invoke({}, cfg('t1'));
    await graph.invoke({}, cfg('t1'));
    await graph.invoke({}, cfg('t1'));

    // channel value is the reducer accumulator from the checkpoint
    const state = await graph.getState(cfg('t1'));
    expect(state.values).toEqual({ value: 3 });

    // LangGraph.js writes ~3 checkpoints per invoke (one per super-step),
    // so 3 invokes yield 9 distinct checkpoints — same as MemorySaver.
    const all: unknown[] = [];
    for await (const t of saver.list(cfg('t1'))) all.push(t);
    expect(all.length).toBe(9);
    // newest first: consecutive ids are monotonically non-increasing
    const ids = all.map((t) => (t as { checkpoint: { id: string } }).checkpoint.id);
    const sorted = [...ids].sort((a, b) => (a < b ? 1 : -1));
    expect(ids).toEqual(sorted);
  });

  it('supports LangGraph interrupts/resume semantics via getTuple', async () => {
    const actae = new FakeActaeClient();
    const saver = new ActaeCheckpointSaver(actae as never, { channel: 'lg' });
    const graph = buildGraph(saver);

    await graph.invoke({}, cfg('t1'));
    const state = await graph.getState(cfg('t1'));
    const checkpointId = state.config?.configurable?.checkpoint_id as string;
    expect(checkpointId).toBeTypeOf('string');

    // getTuple with an explicit checkpoint_id resolves that exact checkpoint
    const tuple = await saver.getTuple(cfg('t1', { checkpoint_id: checkpointId }));
    expect(tuple?.checkpoint.id).toBe(checkpointId);
  });

  it('forkThread forks the run into a new thread that resumes independently', async () => {
    const actae = new FakeActaeClient();
    const saver = new ActaeCheckpointSaver(actae as never, { channel: 'lg' });
    const graph = buildGraph(saver);

    await graph.invoke({}, cfg('run-1'));
    await graph.invoke({}, cfg('run-1'));
    const forkCfg = await saver.forkThread(cfg('run-1'), { newThreadId: 'run-1-fix', reason: 'refine' });

    // The fork inherits the checkpoint value (2) on a fresh thread.
    const forkState = await graph.getState(forkCfg);
    expect(forkState.values).toEqual({ value: 2 });
    // Re-invoking a completed graph from its latest checkpoint returns the
    // checkpoint state without re-running nodes (MemorySaver parity).
    const result = await graph.invoke(null, forkCfg);
    expect(result).toEqual({ value: 2 });

    // Original thread unaffected.
    const original = await graph.getState(cfg('run-1'));
    expect(original.values).toEqual({ value: 2 });
  });

  it('restart durability: a fresh saver resumes the thread from Actae', async () => {
    const actae = new FakeActaeClient();

    // First process: run the graph twice.
    const saver1 = new ActaeCheckpointSaver(actae as never, { channel: 'lg' });
    const graph1 = buildGraph(saver1);
    await graph1.invoke({}, cfg('t1'));
    await graph1.invoke({}, cfg('t1'));

    // "Restart": a brand-new saver (no in-memory state) over the same Actae
    // data, like a new process after a crash.
    const saver2 = new ActaeCheckpointSaver(actae as never, { channel: 'lg' });
    const graph2 = buildGraph(saver2);
    const state = await graph2.getState(cfg('t1'));
    expect(state.values).toEqual({ value: 2 });

    // The restarted process continues from the persisted checkpoint.
    const result = await graph2.invoke({}, cfg('t1'));
    expect(result).toEqual({ value: 3 });
  });

  it('deleteThread clears all checkpoint versions for a thread', async () => {
    const actae = new FakeActaeClient();
    const saver = new ActaeCheckpointSaver(actae as never, { channel: 'lg' });
    const graph = buildGraph(saver);
    await graph.invoke({}, cfg('t1'));
    await graph.invoke({}, cfg('t1'));
    await saver.deleteThread('t1');
    expect(await saver.getTuple(cfg('t1'))).toBeUndefined();
  });
});
