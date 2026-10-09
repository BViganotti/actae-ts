import { describe, expect, it } from 'vitest';
import { StateGraph, type StateSnapshot } from '@langchain/langgraph';
import { CallbackManager } from '@langchain/core/callbacks/manager';
import { ActaeClient } from '../src/index.js';
import { ActaeCheckpointSaver } from '../src/adapters/langgraph.js';
import { ActaeClaudeSessionStore } from '../src/adapters/claude.js';
import { ActaeContextSaver, ChainResumer } from '../src/adapters/langchain.js';
import { CopilotManager, type CopilotClientLike, type CopilotSessionEvent, type CopilotSessionLike } from '../src/adapters/copilot.js';
import type { JsonObject } from '../src/json.js';

/**
 * LIVE fork tests for the framework adapters, driven by the REAL Actae
 * server (dev instance on localhost:8002). Each adapter's fork primitive is
 * exercised end-to-end: record state through the framework, fork it, and
 * verify the fork's inherited state and continuity.
 *
 * Run: ACTAE_LIVE=1 npx vitest run --config vitest.live.config.ts test/adapters_fork_live.test.ts
 */

const LIVE = process.env.ACTAE_LIVE === '1';
const url = process.env.ACTAE_URL ?? 'http://localhost:8002';
const apiKey = process.env.ACTAE_API_KEY ?? 'sk-dev-0000000000000000000000';

function client(): ActaeClient {
  return new ActaeClient({ apiKey, endpoint: url, timeout: 15000 });
}

// ---------------------------------------------------------------------------
// LangGraph: ActaeCheckpointSaver.forkThread(config, { newThreadId, reason })
// ---------------------------------------------------------------------------

interface Msg {
  role: string;
  content: string;
}

/** A linear 4-node messages graph: n1→n2→n3→n4. Node 3 is the "refined" node
 * the fork re-runs. Messages are plain JSON objects because the adapter's
 * default JsonFallbackSerializer JSON-round-trips checkpoints (LangGraph's
 * own JsonPlusSerializer is not shipped in the TS packages, so AIMessage
 * structured payloads would lose `.content`). */
function buildMessagesGraph(saver: ActaeCheckpointSaver, n3Suffix: string) {
  const last = (s: { messages?: Msg[] }): string => {
    const msgs = s.messages ?? [];
    return msgs.length ? msgs[msgs.length - 1]!.content : '';
  };
  const g = new StateGraph({
    channels: {
      messages: {
        reducer: (a: Msg[], b: Msg[]) => [...a, ...(Array.isArray(b) ? b : [b])],
        default: () => [] as Msg[],
      },
    },
  } as never);
  g.addNode('n1', async (s: { messages?: Msg[] }) => ({ messages: [{ role: 'assistant', content: `a1:${last(s)}` }] }));
  g.addNode('n2', async (s: { messages?: Msg[] }) => ({ messages: [{ role: 'assistant', content: `a2:${last(s)}` }] }));
  g.addNode('n3', async (s: { messages?: Msg[] }) => ({ messages: [{ role: 'assistant', content: `${n3Suffix}:${last(s)}` }] }));
  g.addNode('n4', async (s: { messages?: Msg[] }) => ({ messages: [{ role: 'assistant', content: `a4:${last(s)}` }] }));
  g.addEdge('__start__', 'n1');
  g.addEdge('n1', 'n2');
  g.addEdge('n2', 'n3');
  g.addEdge('n3', 'n4');
  g.addEdge('n4', '__end__');
  return g.compile({ checkpointer: saver } as never);
}

// LangGraph's root-thread config carries checkpoint_ns as undefined → ''.
const cfg = (threadId: string, extra: Record<string, unknown> = {}) => ({
  configurable: { thread_id: threadId, ...extra },
});

describe.runIf(LIVE)('ActaeCheckpointSaver.forkThread (live)', () => {
  it('forks a run at an intermediate checkpoint and re-runs only the tail', async () => {
    const c = client();
    const suffix = Date.now().toString(36);
    const thread = `lg-t-${suffix}`;
    const saver = new ActaeCheckpointSaver(c, { channel: `lg-fork-${suffix}` });

    // Full run (node 3 = "BASE"): [u0, a1:u0, a2:a1:u0, BASE:a2:a1:u0, a4:BASE:a2:a1:u0].
    const full = buildMessagesGraph(saver, 'BASE');
    await full.invoke({ messages: [{ role: 'user', content: 'u0' }] }, cfg(thread));

    // State history is newest-first; find the checkpoint right after node 2
    // (3 messages accumulated — the moment before node 3 runs).
    const history: StateSnapshot[] = [];
    for await (const h of full.getStateHistory(cfg(thread))) history.push(h);
    const afterN2 = history.find((h) => (h.values?.messages as Msg[] | undefined)?.length === 3);
    expect(afterN2).toBeDefined();
    const checkpointId = afterN2!.config?.configurable?.checkpoint_id as string | undefined;
    expect(checkpointId).toBeTypeOf('string');

    // Fork at that exact checkpoint (the "fork at step N, refine step N+1" move).
    const forkCfg = await saver.forkThread(cfg(thread, { checkpoint_id: checkpointId }), {
      newThreadId: `${thread}-fix`,
      reason: 'refine node 3',
    });

    // The fork inherits the parent's checkpoint state at the fork point.
    const forkTuple = await saver.getTuple(forkCfg);
    expect(forkTuple).toBeDefined();
    const inherited = (forkTuple!.checkpoint.channel_values as { messages: Msg[] }).messages;
    expect(inherited.map((m) => m.content)).toEqual(['u0', 'a1:u0', 'a2:a1:u0']);

    // A fresh graph on the same saver sees the preserved state before re-running.
    const forkGraph = buildMessagesGraph(saver, 'FORK');
    const preState = await forkGraph.getState(forkCfg);
    expect((preState.values as { messages: Msg[] }).messages).toHaveLength(3);

    // Re-run from the fork: node 3 re-runs with the FORK prompt and node 4
    // follows, while the inherited n1/n2 prefix stays byte-identical.
    const parentChannel = saver.channelForConfig(cfg(thread));
    const parentEventCountBeforeContinuation = (await c.replay(parentChannel, { limit: 200 })).length;
    const res = await forkGraph.invoke(null, forkCfg);
    expect(res.messages.map((m: Msg) => m.content)).toEqual([
      'u0',
      'a1:u0',
      'a2:a1:u0',
      'FORK:a2:a1:u0',
      'a4:FORK:a2:a1:u0',
    ]);

    // The fork is a real channel with its own lineage event, and it exists in
    // the channel registry.
    const forkChannel = saver.channelForConfig(forkCfg);
    expect((await c.listChannels()).map((ch) => ch.toLowerCase())).toContain(forkChannel.toLowerCase());
    const forkEvents = await c.replay(forkChannel, { limit: 100 });
    expect(forkEvents.length).toBeGreaterThan(0);
    expect(forkEvents.every((e) => e.channelId === forkChannel)).toBe(true);
    expect(forkEvents.some((e) => e.eventType === 'fork.started')).toBe(true);

    // The requested fork config remains authoritative: continuation writes
    // never mutate the source thread and the fork advances independently.
    expect((await c.replay(parentChannel, { limit: 200 })).length).toBe(parentEventCountBeforeContinuation);
    // Query the fork thread's LATEST checkpoint (the fork config carries the
    // boundary checkpoint_id, so getTuple(forkCfg) resolves the boundary —
    // the latest fork state is reached via the thread id alone).
    const forkTupleAfter = await saver.getTuple({ configurable: { thread_id: `${thread}-fix` } });
    expect(
      ((forkTupleAfter!.checkpoint.channel_values as { messages: Msg[] }).messages).map((m) => m.content),
    ).toEqual(['u0', 'a1:u0', 'a2:a1:u0', 'FORK:a2:a1:u0', 'a4:FORK:a2:a1:u0']);
  });
});

// ---------------------------------------------------------------------------
// Claude: ActaeClaudeSessionStore.forkSession(projectKey, sessionId, newSessionId, { subpath?, reason? })
// ---------------------------------------------------------------------------

describe.runIf(LIVE)('ActaeClaudeSessionStore.forkSession (live)', () => {
  it('forks a session transcript into a new session, discoverable via listSessions', async () => {
    const c = client();
    const store = new ActaeClaudeSessionStore(c);
    const suffix = Date.now().toString(36);
    const projectKey = `proj-${suffix}`;
    const entry = (uuid: string, type: string, text: string, ts: string): Record<string, unknown> => ({
      type,
      text,
      uuid,
      timestamp: ts,
      parent_tool_use_id: null,
    });
    const key = (sessionId: string) => ({ project_key: projectKey, session_id: sessionId });

    await store.append(key('s1'), [
      entry('u1', 'user', 'hello', '2026-01-01T00:00:00Z'),
      entry('u2', 'assistant', 'hi', '2026-01-01T00:00:01Z'),
      entry('u3', 'assistant', 'bye', '2026-01-01T00:00:02Z'),
    ] as never);

    const newKey = await store.forkSession(projectKey, 's1', 's1-fix', { reason: 'live fork' });
    expect(newKey).toEqual({ project_key: projectKey, session_id: 's1-fix' });

    // All three entries carried over, in order.
    const loaded = await store.load(newKey);
    expect(loaded).toHaveLength(3);
    expect(loaded!.map((e) => e['text'])).toEqual(['hello', 'hi', 'bye']);

    // The fork is discoverable through the project index.
    const sessions = await store.listSessions(projectKey);
    expect(sessions.map((s) => s.sessionId)).toContain('s1-fix');
  });
});

// ---------------------------------------------------------------------------
// LangChain: ChainResumer.fork(newChannel, { reason? })
// ---------------------------------------------------------------------------

describe.runIf(LIVE)('ChainResumer.fork (live)', () => {
  it('forks the saved context into a new channel', async () => {
    const c = client();
    const suffix = Date.now().toString(36);
    const channel = `lc-fork-${suffix}`;
    const saver = new ActaeContextSaver(c, { channel, saveEveryN: 1 });

    // Drive the saver through a real @langchain/core CallbackManager.
    const manager = await CallbackManager.configure([saver]);
    const llmRun = (await manager.handleLLMStart({ name: 'test-model' }, ['hello'], `run-llm-${suffix}`))[0];
    await llmRun.handleLLMEnd({ generations: [[{ text: 'assistant reply' }]] });
    const toolRun = await manager.handleToolStart({ name: 'search' }, ['query'], `run-tool-${suffix}`);
    await toolRun.handleToolEnd('search result');
    const chainRun = await manager.handleChainStart({ name: 'my-chain' }, ['input'], `run-chain-${suffix}`);
    await chainRun.handleChainEnd({ output: 'final' });
    await saver.flush();

    const resumer = new ChainResumer(c, { channel });
    const fork = await resumer.fork(`${channel}-fix`, { reason: 'live fork' });

    // ChainResumer exposes `resume`/`fork` (no loadContext) — read the fork's
    // inherited context through an ActaeContextSaver bound to the fork channel.
    const forkSaver = new ActaeContextSaver(c, { channel: fork.channel });
    const ctx = await forkSaver.loadContext();
    expect(ctx?.['messages']).toHaveLength(1);
    expect((ctx!['messages'] as Array<{ role: string; content: string }>)[0]).toMatchObject({
      role: 'assistant',
      content: 'assistant reply',
    });
    expect(ctx!['tool_outputs']).toHaveLength(1);
    expect(ctx!['chain_steps']).toHaveLength(1);
  });
});

// ---------------------------------------------------------------------------
// Copilot: CopilotManager.fork(sessionId, atEventId, { newChannelId, displayName?, reason? })
// ---------------------------------------------------------------------------

describe.runIf(LIVE)('CopilotManager.fork (live)', () => {
  it('forks a recorded session at an event into a new replayable channel', async () => {
    const c = client();
    const suffix = Date.now().toString(36);
    const sid = `copilot-s-${suffix}`;
    const prefix = `copilot-fork-${suffix}`;
    const mgr = new CopilotManager(c, { channelPrefix: prefix });

    // Minimal session-like object: the recorder is structural and only needs
    // `on`, `sessionId` (getEvents is optional).
    const handlers: Array<(e: CopilotSessionEvent) => void> = [];
    const clientLike: CopilotClientLike = {
      async createSession() {
        return {
          sessionId: sid,
          on(handler) {
            handlers.push(handler);
            return undefined;
          },
        } as CopilotSessionLike;
      },
      async resumeSession(sessionId) {
        return { sessionId, on: () => undefined };
      },
    };

    const handle = await mgr.startSession(clientLike, {});
    expect(handle.channel).toBe(`${prefix}:${sid}`);

    // Drive the recorder with Copilot event shapes (records asynchronously).
    for (const ev of [
      { id: 'e1', type: 'user.message', data: { text: 'hello' } },
      { id: 'e2', type: 'assistant.message', data: { text: 'hi' } },
      { id: 'e3', type: 'tool_execution.complete', data: { ok: true } },
    ]) {
      handlers[0]!(ev);
    }
    await handle.recorder.stop(); // drain the async queue so cursors resolve

    // Save a snapshot at the latest cursor so the fork inherits state.
    const snapshotState = { messages: ['hello', 'hi'], ok: true } as JsonObject;
    await mgr.snapshot(sid, snapshotState);

    const newChannel = `${prefix}:${sid}-fix`;
    const forked = await mgr.fork(sid, 'e3', { newChannelId: newChannel });
    expect(forked).toBe(newChannel);

    // The fork channel exists and is replayable.
    const channels = await c.listChannels();
    expect(channels).toContain(newChannel);
    const replay = await c.replay(newChannel);
    expect(Array.isArray(replay)).toBe(true);

    // The fork inherits the saved snapshot (the checkpoint metadata added
    // by snapshot() is stripped on read, exactly like the Go manager).
    const latest = await mgr.loadSnapshot(sid);
    expect(latest).toEqual(snapshotState);

    // A follow-up event on the fork channel is replayable (the fork lives).
    const fwd = await c.record(newChannel, 'copilot.user.message', { text: 'refined' }, { actor: 'copilot' });
    const replay2 = await c.replay(newChannel);
    expect(replay2.map((e) => e.cursor)).toContain(fwd.cursor);

    await mgr.untrack(sid);
  });
});
