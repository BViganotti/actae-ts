import { afterEach, describe, expect, it, vi } from 'vitest';
import { ActaeCheckpointSaver, ActaeLangGraphError, channelForConfig } from '../src/adapters/langgraph.js';
import { ActaeContextSaver, ChainResumer } from '../src/adapters/langchain.js';
import { ActaeClaudeSessionStore, channelForSession } from '../src/adapters/claude.js';
import { ActaeTracingProcessor } from '../src/adapters/openai.js';
import { FakeActaeClient } from './helpers.js';

const threadCfg = (threadId: string, extra: Record<string, unknown> = {}) => ({
  configurable: { thread_id: threadId, checkpoint_ns: 'default', ...extra },
});

describe('ActaeCheckpointSaver (LangGraph)', () => {
  function make() {
    const actae = new FakeActaeClient();
    const saver = new ActaeCheckpointSaver(actae as never, { channel: 'lg' });
    return { actae, saver };
  }

  it('channelForConfig derives prefix+digest and validates thread_id', () => {
    expect(channelForConfig(threadCfg('t1'), { channel: 'lg' })).toBe(
      channelForConfig(threadCfg('t1'), { channel: 'lg' }),
    );
    expect(channelForConfig(threadCfg('t1'), { channel: 'lg' })).not.toBe(
      channelForConfig(threadCfg('t2'), { channel: 'lg' }),
    );
    expect(() => channelForConfig({}, { channel: 'lg' })).toThrow(ActaeLangGraphError);
  });

  it('put persists an envelope snapshot and returns a config with checkpoint_id', async () => {
    const { actae, saver } = make();
    const cfg = await saver.put(
      threadCfg('t1'),
      { id: 'cp1', ts: 't', channel_values: { messages: [] }, channel_versions: {}, versions_seen: {} },
      { source: 'test' },
    );
    expect((cfg.configurable as any).checkpoint_id).toBe('cp1');
    const ch = actae.channels.get('lg:' + channelForConfig(threadCfg('t1'), { channel: 'lg' }).split(':')[1]);
    expect(ch).toBeDefined();
    expect(ch!.states.length).toBe(1);
    expect(ch!.events.some((e) => e.eventType === 'langgraph.checkpoint')).toBe(true);
  });

  it('getTuple returns the latest checkpoint', async () => {
    const { actae, saver } = make();
    const ch = await saver.put(threadCfg('t1'), {
      id: 'cp1',
      ts: 't',
      channel_values: { counter: 5 },
      channel_versions: {},
      versions_seen: {},
    }, { source: 'test' });
    const tuple = await saver.getTuple(threadCfg('t1'));
    expect(tuple).toBeDefined();
    expect((tuple!.checkpoint as any).channel_values.counter).toBe(5);
    expect((tuple!.metadata as any).source).toBe('test');
    // getTuple with explicit checkpoint_id
    const t2 = await saver.getTuple(threadCfg('t1', { checkpoint_id: ch.configurable.checkpoint_id }));
    expect(t2).toBeDefined();
  });

  it('putWrites merges pending writes idempotently and persists a new version', async () => {
    const { actae, saver } = make();
    const cfg = await saver.put(threadCfg('t1'), {
      id: 'cp1',
      ts: 't',
      channel_values: {},
      channel_versions: {},
      versions_seen: {},
    }, {});
    await saver.putWrites(threadCfg('t1', { checkpoint_id: 'cp1' }), [['channel', { v: 1 }]], 'task-1');
    const tuple = await saver.getTuple(threadCfg('t1'));
    expect(tuple!.pendingWrites).toEqual([['task-1', 'channel', { v: 1 }]]);
  });

  it('buffers writes for not-yet-saved checkpoints and flushes on put', async () => {
    const { actae, saver } = make();
    // putWrites before put: target checkpoint doesn't exist yet → deferred
    await saver.putWrites(threadCfg('t1', { checkpoint_id: 'cpX' }), [['ch', { v: 1 }]], 'task-1');
    expect(actae.channels.size).toBe(0); // nothing persisted yet
    await saver.put(threadCfg('t1'), {
      id: 'cpX',
      ts: 't',
      channel_values: {},
      channel_versions: {},
      versions_seen: {},
    }, {});
    const tuple = await saver.getTuple(threadCfg('t1'));
    expect(tuple!.pendingWrites).toEqual([['task-1', 'ch', { v: 1 }]]);
  });

  it('list yields checkpoints newest-first with limit/before', async () => {
    const { saver } = make();
    await saver.put(threadCfg('t1'), { id: 'cp1', ts: 't1', channel_values: {}, channel_versions: {}, versions_seen: {} }, {});
    await saver.put(threadCfg('t1'), { id: 'cp2', ts: 't2', channel_values: {}, channel_versions: {}, versions_seen: {} }, {});
    const all: unknown[] = [];
    for await (const t of saver.list(threadCfg('t1'))) all.push(t);
    expect(all.length).toBe(2);
    // newest first
    const ids = all.map((t) => (t as any).checkpoint.id);
    expect(ids).toEqual(['cp2', 'cp1']);
  });

  it('getNextVersion produces monotonic version strings', () => {
    const { saver } = make();
    const v1 = saver.getNextVersion(0);
    const v2 = saver.getNextVersion(v1);
    expect(Number(v1.split('.')[0])).toBe(1);
    expect(Number(v2.split('.')[0])).toBe(2);
  });

  it('deleteThread removes all checkpoint versions', async () => {
    const { actae, saver } = make();
    await saver.put(threadCfg('t1'), { id: 'cp1', ts: 't', channel_values: {}, channel_versions: {}, versions_seen: {} }, {});
    const prefix = channelForConfig(threadCfg('t1'), { channel: 'lg' }).split(':')[1];
    const ch = actae.channels.get('lg:' + prefix)!;
    expect(ch.states.length).toBe(1);
    await saver.deleteThread('t1');
    expect(ch.states.length).toBe(0);
  });

  it('forkThread forks the latest checkpoint into a new thread channel', async () => {
    const { actae, saver } = make();
    await saver.put(threadCfg('run-1'), {
      id: 'cp1',
      ts: 't',
      channel_values: { messages: [{ role: 'user', content: 'hi' }] },
      channel_versions: {},
      versions_seen: {},
    }, {});
    const forkCfg = await saver.forkThread(threadCfg('run-1'), { newThreadId: 'run-1-fix', reason: 'refine' });
    expect((forkCfg.configurable as any).thread_id).toBe('run-1-fix');
    expect((forkCfg.configurable as any).checkpoint_id).toBe('cp1');
    const forkChannel = channelForConfig(threadCfg('run-1-fix'), { channel: 'lg' });
    const ch = actae.channels.get(forkChannel)!;
    expect(ch.parent).toBe(channelForConfig(threadCfg('run-1'), { channel: 'lg' }));
    expect(ch.states.length).toBe(1);
    expect(ch.states[0]!.state['checkpoint_id']).toBe('cp1');
  });
});

describe('ActaeContextSaver / ChainResumer (LangChain)', () => {
  it('saves context snapshots every N callbacks and loads them back', async () => {
    const actae = new FakeActaeClient();
    const saver = new ActaeContextSaver(actae as never, { channel: 'lc', saveEveryN: 1 });
    await saver.handleLLMEnd({ generations: [[{ text: 'hello' }]] });
    await saver.handleToolEnd({ result: 42 });
    await saver.handleLLMEnd({ generations: [[{ text: 'world' }]] });
    await saver.flush();
    const ctx = await saver.loadContext();
    expect(ctx?.['messages']).toHaveLength(2);
    expect((ctx!['tool_outputs'] as unknown[])).toHaveLength(1);
    const saved = actae.channels.get('lc')!;
    expect(saved.states.length).toBeGreaterThan(0);
  });

  it('ChainResumer.resume seeds from the last assistant message', async () => {
    const actae = new FakeActaeClient();
    const saver = new ActaeContextSaver(actae as never, { channel: 'lc', saveEveryN: 1 });
    await saver.handleLLMEnd({ generations: [[{ text: 'last answer' }]] });
    await saver.flush();
    const resumer = new ChainResumer(actae as never, { channel: 'lc' });
    let invoked: unknown;
    const chain = {
      invoke: async (input: unknown) => {
        invoked = input;
        return 'out';
      },
    };
    await resumer.resume(chain);
    expect(invoked).toBe('last answer');
  });
});

describe('ActaeClaudeSessionStore (Claude Agent SDK)', () => {
  it('channelForSession is deterministic and distinguishes subpaths', () => {
    const a = channelForSession('proj', 's1');
    const b = channelForSession('proj', 's1');
    const c = channelForSession('proj', 's1', 'subagents/x');
    expect(a).toBe(b);
    expect(a).not.toBe(c);
    expect(c.startsWith('claude:')).toBe(true);
  });

  it('append persists entries and load returns them', async () => {
    const actae = new FakeActaeClient();
    const store = new ActaeClaudeSessionStore(actae as never);
    const key = { project_key: 'proj', session_id: 's1' };
    await store.append(key, [{ type: 'user', text: 'hi' } as never]);
    await store.append(key, [{ type: 'assistant', text: 'hello' } as never]);
    const entries = await store.load(key);
    expect(entries).toHaveLength(2);
    expect(entries![0]).toMatchObject({ type: 'user' });
  });

  it('load returns null for absent session', async () => {
    const actae = new FakeActaeClient();
    const store = new ActaeClaudeSessionStore(actae as never);
    expect(await store.load({ project_key: 'p', session_id: 'missing' })).toBeNull();
  });

  it('forkSession copies the transcript and registers it', async () => {
    const actae = new FakeActaeClient();
    const store = new ActaeClaudeSessionStore(actae as never);
    await store.append({ project_key: 'p', session_id: 's1' }, [{ type: 'user', text: 'hi' } as never]);
    const key = await store.forkSession('p', 's1', 's1-fix', { reason: 'refine' });
    expect(key.session_id).toBe('s1-fix');
    const entries = await store.load(key);
    expect(entries).toHaveLength(1);
    const sessions = await store.list_sessions('p');
    expect(sessions.map((s) => s.session_id)).toContain('s1-fix');
  });

  it('delete main transcript cascades to subkeys', async () => {
    const actae = new FakeActaeClient();
    const store = new ActaeClaudeSessionStore(actae as never);
    await store.append({ project_key: 'p', session_id: 's1' }, [{ type: 'user', text: 'hi' } as never]);
    await store.append({ project_key: 'p', session_id: 's1', subpath: 'subagents/x' }, [{ type: 'assistant', text: 'ok' } as never]);
    await store.delete({ project_key: 'p', session_id: 's1' });
    expect(await store.load({ project_key: 'p', session_id: 's1' })).toBeNull();
    expect(await store.load({ project_key: 'p', session_id: 's1', subpath: 'subagents/x' })).toBeNull();
  });

  it('list_subkeys tracks subagent transcripts', async () => {
    const actae = new FakeActaeClient();
    const store = new ActaeClaudeSessionStore(actae as never);
    await store.append({ project_key: 'p', session_id: 's1' }, [{ type: 'user', text: 'hi' } as never]);
    await store.append({ project_key: 'p', session_id: 's1', subpath: 'subagents/x' }, [{} as never]);
    const subkeys = await store.list_subkeys({ project_key: 'p', session_id: 's1' });
    expect(subkeys).toContain('subagents/x');
  });
});

describe('ActaeTracingProcessor (OpenAI Agents)', () => {
  it('records trace start/span end/trace end events and a run snapshot', async () => {
    const actae = new FakeActaeClient();
    const p = new ActaeTracingProcessor(actae as never, { channel: 'oa' });
    p.on_trace_start({ trace_id: 'tr1', name: 'agent-run' });
    p.on_span_start({ trace_id: 'tr1', span_id: 'sp1', parent_id: null, started_at: '2026-01-01T00:00:00.000Z' });
    p.on_span_end({
      trace_id: 'tr1',
      span_id: 'sp1',
      parent_id: null,
      started_at: '2026-01-01T00:00:00.000Z',
      ended_at: '2026-01-01T00:00:00.500Z',
      span_data: { name: 'agent', model: 'gpt-4', input: 'hi', output: 'bye' },
    });
    p.on_trace_end({ trace_id: 'tr1', name: 'agent-run' });
    await p.shutdown();
    const events = actae.recordedEvents.filter((e) => e.channelId === 'oa');
    expect(events.some((e) => e.eventType === 'openai.trace.start')).toBe(true);
    expect(events.some((e) => e.eventType === 'openai.span.end')).toBe(true);
    expect(events.some((e) => e.eventType === 'openai.trace.end')).toBe(true);
    const ch = actae.channels.get('oa')!;
    const snap = ch.states[ch.states.length - 1]!;
    expect(snap.state['trace_id']).toBe('tr1');
    expect((snap.state['spans'] as unknown[]).length).toBe(1);
    expect((snap.state['spans'] as any)[0].model).toBe('gpt-4');
  });

  it('includeInputsOutputs=false strips inputs/outputs', async () => {
    const actae = new FakeActaeClient();
    const p = new ActaeTracingProcessor(actae as never, { includeInputsOutputs: false });
    p.on_trace_start({ trace_id: 'tr1', name: 'agent-run' });
    p.on_span_end({
      trace_id: 'tr1',
      span_id: 's1',
      started_at: '2026-01-01T00:00:00.000Z',
      ended_at: '2026-01-01T00:00:00.500Z',
      span_data: { name: 'agent', input: 'secret', output: 'more' },
    });
    p.on_trace_end({ trace_id: 'tr1', name: 'agent-run' });
    await p.shutdown();
    const snap = actae.channels.get('openai_agents')!.states[0]!;
    expect((snap.state['spans'] as any)[0].input).toBeUndefined();
    expect((snap.state['spans'] as any)[0].name).toBe('agent');
  });

  it('installActaeTracing throws a helpful error when @openai/agents is absent', async () => {
    const actae = new FakeActaeClient();
    // Simulate the package being absent: the adapter's dynamic import of
    // '@openai/agents' must reject so the try/catch raises the helpful error.
    // vi.doMock + vi.resetModules force the dynamic import through the mock.
    vi.doMock('@openai/agents', () => {
      throw new Error("Cannot find package '@openai/agents'");
    });
    vi.resetModules();
    try {
      const { installActaeTracing: freshInstall } = await import('../src/adapters/openai.js');
      await expect(freshInstall(actae as never)).rejects.toThrow(/@openai\/agents/);
    } finally {
      vi.doUnmock('@openai/agents');
      vi.resetModules();
    }
  });
});
