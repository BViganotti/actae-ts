import { describe, expect, it } from 'vitest';
import { CopilotManager, CopilotRecorder, CopilotError } from '../src/adapters/copilot.js';
import type { CopilotSessionEvent, CopilotSessionLike, CopilotClientLike } from '../src/adapters/copilot.js';
import { FakeActaeClient, waitFor } from './helpers.js';

function fakeSession(sessionId: string, opts: { history?: CopilotSessionEvent[] } = {}) {
  const handlers: Array<(e: CopilotSessionEvent) => void> = [];
  const session: CopilotSessionLike = {
    sessionId,
    on(handler) {
      handlers.push(handler);
      return () => {
        const idx = handlers.indexOf(handler);
        if (idx >= 0) handlers.splice(idx, 1);
      };
    },
    async getEvents() {
      return opts.history ?? [];
    },
  };
  return {
    session,
    emit: (e: CopilotSessionEvent) => handlers.forEach((h) => h(e)),
  };
}

function fakeClient(opts: { history?: CopilotSessionEvent[] } = {}) {
  const sessions = new Map<string, ReturnType<typeof fakeSession>>();
  const client: CopilotClientLike & { sessions: typeof sessions; createCount: number } = {
    sessions,
    createCount: 0,
    async createSession(config) {
      this.createCount++;
      const sid = (config as { session_id?: string }).session_id ?? `sess-${this.createCount}`;
      const s = fakeSession(sid, opts);
      sessions.set(sid, s);
      return s.session;
    },
    async resumeSession(sessionId) {
      const s = fakeSession(sessionId, opts);
      sessions.set(sessionId, s);
      return s.session;
    },
    async getSessionMetadata(sessionId) {
      return { session_id: sessionId, start_time: 't', summary: 'hi' };
    },
  };
  return { client, sessions };
}

describe('CopilotRecorder', () => {
  it('records session.started on attach and events via the session handler', async () => {
    const actae = new FakeActaeClient();
    const rec = new CopilotRecorder(actae as never, { channelPrefix: 'copilot' });
    const { session, emit } = fakeSession('s1');
    await rec.attach(session);
    expect(rec.channelId()).toBe('copilot:s1');
    expect(rec.sessionId()).toBe('s1');

    emit({ id: 'e1', type: 'user.message', data: { text: 'hi' } });
    emit({ id: 'e2', type: 'tool_execution.complete', data: { ok: true } });
    emit({ id: 'e1', type: 'user.message', data: { text: 'dup' } }); // dedup
    await rec.stop();

    const events = actae.recordedEvents.filter((e) => e.channelId === 'copilot:s1');
    const types = events.map((e) => e.eventType);
    expect(types).toContain('copilot.session.started');
    expect(types).toContain('copilot.user.message');
    expect(types).toContain('copilot.tool_execution.complete');
    // dedup: only one user.message recorded
    expect(types.filter((t) => t === 'copilot.user.message')).toHaveLength(1);
    // event_id tracked in metadata
    const msg = events.find((e) => e.eventType === 'copilot.user.message')!;
    expect(msg.opts.metadata).toMatchObject({ event_id: 'e1', session_id: 's1' });
    // event → cursor map populated
    expect(rec.cursorForEvent('e1')).toBeTypeOf('number');
  });

  it('skips ephemeral events unless includeEphemeral', async () => {
    const actae = new FakeActaeClient();
    const rec = new CopilotRecorder(actae as never);
    const { session, emit } = fakeSession('s1');
    await rec.attach(session);
    emit({ id: 'e1', type: 'some.event', data: { x: 1 }, ephemeral: true });
    await rec.stop();
    expect(actae.recordedEvents.filter((e) => e.eventType === 'copilot.some.event')).toHaveLength(0);

    const rec2 = new CopilotRecorder(actae as never, { includeEphemeral: true });
    const s2 = fakeSession('s2');
    await rec2.attach(s2.session);
    s2.emit({ id: 'e1', type: 'some.event', data: { x: 1 }, ephemeral: true });
    await rec2.stop();
    expect(actae.recordedEvents.filter((e) => e.eventType === 'copilot.some.event')).toHaveLength(1);
  });

  it('honors eventTypes filters', async () => {
    const actae = new FakeActaeClient();
    const rec = new CopilotRecorder(actae as never, { eventTypes: ['user.message'] });
    const { session, emit } = fakeSession('s1');
    await rec.attach(session);
    emit({ id: 'e1', type: 'user.message', data: {} });
    emit({ id: 'e2', type: 'tool_call', data: {} });
    await rec.stop();
    const types = actae.recordedEvents.filter((e) => e.channelId === 'copilot:s1').map((e) => e.eventType);
    expect(types).toContain('copilot.user.message');
    expect(types).not.toContain('copilot.tool_call');
  });

  it('backfill records history not already seen (live-attach recovery)', async () => {
    const actae = new FakeActaeClient();
    const rec = new CopilotRecorder(actae as never);
    const history: CopilotSessionEvent[] = [
      { id: 'h1', type: 'user.message', data: { text: 'old' } },
      { id: 'h2', type: 'assistant.message', data: { text: 'old2' } },
    ];
    const { session, emit } = fakeSession('s1', { history });
    await rec.attach(session);
    // one event arrived live before backfill
    emit({ id: 'h1', type: 'user.message', data: { text: 'old' } });
    const count = await rec.backfill();
    await rec.stop();
    expect(count).toBe(1); // only h2 is new
    const types = actae.recordedEvents.filter((e) => e.channelId === 'copilot:s1').map((e) => e.eventType);
    expect(types).toContain('copilot.assistant.message');
  });

  it('record and recordSessionEnded persist lifecycle events once', async () => {
    const actae = new FakeActaeClient();
    const rec = new CopilotRecorder(actae as never);
    await rec.attach(fakeSession('s1').session);
    await rec.record('copilot.annotation', { note: 'x' }, {});
    await rec.recordSessionEnded('done');
    await rec.recordSessionEnded('again'); // no-op
    await rec.stop();
    const types = actae.recordedEvents.filter((e) => e.channelId === 'copilot:s1').map((e) => e.eventType);
    expect(types.filter((t) => t === 'copilot.session.ended')).toHaveLength(1);
  });

  it('record before attach throws', async () => {
    const actae = new FakeActaeClient();
    const rec = new CopilotRecorder(actae as never);
    await expect(rec.record('x', {})).rejects.toBeInstanceOf(CopilotError);
  });
});

describe('CopilotManager', () => {
  it('startSession tracks a recorder and records session.info', async () => {
    const actae = new FakeActaeClient();
    const mgr = new CopilotManager(actae as never);
    const { client } = fakeClient();
    const handle = await mgr.startSession(client, { session_id: 's1', configurable: {} });
    expect(handle.channel).toBe('copilot:s1');
    expect(mgr.recorderFor('s1')).toBe(handle.recorder);
    const types = actae.recordedEvents.filter((e) => e.channelId === 'copilot:s1').map((e) => e.eventType);
    expect(types).toContain('copilot.session.started');
    expect(types).toContain('copilot.session.info');
  });

  it('resumeSession attaches to an existing session', async () => {
    const actae = new FakeActaeClient();
    const mgr = new CopilotManager(actae as never);
    const { client } = fakeClient();
    const handle = await mgr.resumeSession(client, 'existing', {});
    expect(handle.recorder.sessionId()).toBe('existing');
    expect(mgr.recorderFor('existing')).toBeDefined();
  });

  it('endSession / untrack / stopAll', async () => {
    const actae = new FakeActaeClient();
    const mgr = new CopilotManager(actae as never);
    const { client } = fakeClient();
    await mgr.startSession(client, { session_id: 's1' });
    await mgr.startSession(client, { session_id: 's2' });
    await mgr.endSession('s1', 'bye');
    await mgr.untrack('s1');
    expect(mgr.recorderFor('s1')).toBeUndefined();
    await mgr.stopAll();
    expect(mgr.recorderFor('s2')).toBeUndefined();
    const ended = actae.recordedEvents.filter((e) => e.eventType === 'copilot.session.ended');
    expect(ended).toHaveLength(1);
  });

  it('endSession on untracked session throws', async () => {
    const actae = new FakeActaeClient();
    const mgr = new CopilotManager(actae as never);
    await expect(mgr.endSession('nope', 'x')).rejects.toBeInstanceOf(CopilotError);
  });

  it('fork forks the channel at an event cursor', async () => {
    const actae = new FakeActaeClient();
    const mgr = new CopilotManager(actae as never);
    const { client } = fakeClient();
    const handle = await mgr.startSession(client, { session_id: 's1' });
    // Emit a session event so the worker records it and populates the
    // event → cursor map (Go parity: direct record() doesn't map cursors).
    client.sessions.get('s1')!.emit({ id: 'ev-1', type: 'user.message', data: { text: 'x' } });
    await waitFor(() => handle.recorder.cursorForEvent('ev-1') !== undefined, 3000);
    const cursor = handle.recorder.cursorForEvent('ev-1')!;

    const forkChannel = await mgr.fork('s1', 'ev-1', { newChannelId: 'fork-1' });
    expect(forkChannel).toBe('fork-1');
    const f = actae.forks.find((x) => x.child === 'fork-1');
    expect(f?.source).toBe('copilot:s1');
    expect(f?.atCursor).toBe(cursor);
  });

  it('fork with unknown event throws', async () => {
    const actae = new FakeActaeClient();
    const mgr = new CopilotManager(actae as never);
    const { client } = fakeClient();
    await mgr.startSession(client, { session_id: 's1' });
    await expect(mgr.fork('s1', 'missing', { newChannelId: 'f' })).rejects.toThrow(/not resolved/);
  });

  it('snapshot / loadSnapshot / listSnapshots via StateManager semantics', async () => {
    const actae = new FakeActaeClient();
    const mgr = new CopilotManager(actae as never);
    const { client } = fakeClient();
    await mgr.startSession(client, { session_id: 's1' });
    const version = await mgr.snapshot('s1', { messages: ['a'] });
    expect(version).toBeGreaterThan(0);
    expect(await mgr.loadSnapshot('s1')).toEqual({ messages: ['a'] });
    const versions = await mgr.listSnapshots('s1');
    expect(versions.map((v) => v.version)).toContain(version);
  });
});
