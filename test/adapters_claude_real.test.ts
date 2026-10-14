import { describe, expect, it } from 'vitest';
import { InMemorySessionStore, type SessionStore } from '@anthropic-ai/claude-agent-sdk';
import { ActaeClaudeSessionStore } from '../src/adapters/claude.js';
import { FakeActaeClient } from './helpers.js';

/**
 * Real-framework integration against the actual `@anthropic-ai/claude-agent-sdk`.
 * Verifies the store (1) satisfies the `SessionStore` type (compile-time),
 * (2) matches `InMemorySessionStore` behavior for the same operations, and
 * (3) honors the uuid idempotency contract + summary folding.
 */

function entry(uuid: string, type: string, text: string, ts: string): Record<string, unknown> {
  return { type, text, uuid, timestamp: ts, parent_tool_use_id: null };
}

const key = (sessionId: string, subpath?: string) => ({ project_key: 'proj', session_id: sessionId, ...(subpath ? { subpath } : {}) });

describe('ActaeClaudeSessionStore × real claude-agent-sdk', () => {
  it('satisfies the real SessionStore type', () => {
    // Compile-time conformance: assignable to the SDK's SessionStore.
    const store: SessionStore = new ActaeClaudeSessionStore(new FakeActaeClient() as never);
    expect(store).toBeDefined();
  });

  it('matches InMemorySessionStore behavior for the core protocol', async () => {
    const mem = new InMemorySessionStore();
    const actae = new FakeActaeClient();
    const store = new ActaeClaudeSessionStore(actae as never);

    const entries = [
      entry('u1', 'user', 'hello', '2026-01-01T00:00:00Z'),
      entry('u2', 'assistant', 'hi', '2026-01-01T00:00:01Z'),
    ];
    await mem.append(key('s1'), entries as never);
    await store.append(key('s1'), entries as never);

    expect(await store.load(key('s1'))).toEqual(await mem.load(key('s1')));

    // append more, both accumulate
    await mem.append(key('s1'), [entry('u3', 'assistant', 'bye', '2026-01-01T00:00:02Z')] as never);
    await store.append(key('s1'), [entry('u3', 'assistant', 'bye', '2026-01-01T00:00:02Z')] as never);
    expect(await store.load(key('s1'))).toEqual(await mem.load(key('s1')));
    expect(await store.load(key('s1'))).toHaveLength(3);
  });

  it('honors the uuid idempotency contract (no duplicate rows on retry)', async () => {
    const actae = new FakeActaeClient();
    const store = new ActaeClaudeSessionStore(actae as never);
    const e = entry('dup-uuid', 'user', 'hi', '2026-01-01T00:00:00Z');
    await store.append(key('s1'), [e] as never);
    // Retry / import-replay of the same uuid must NOT duplicate.
    await store.append(key('s1'), [e] as never);
    await store.append(key('s1'), [{ ...e, type: 'user', text: 'updated' }] as never);
    expect(await store.load(key('s1'))).toHaveLength(1);
    // latest write wins (upsert)
    const loaded = await store.load(key('s1'));
    expect(loaded?.[0]).toMatchObject({ text: 'updated' });
  });

  it('listSessions / listSessionSummaries match the real shapes', async () => {
    const actae = new FakeActaeClient();
    const store = new ActaeClaudeSessionStore(actae as never);
    await store.append(key('s1'), [entry('u1', 'user', 'hi', '2026-01-01T00:00:00Z')] as never);

    const sessions = await store.listSessions('proj');
    expect(sessions[0]).toMatchObject({ sessionId: 's1' });
    expect(typeof sessions[0]!.mtime).toBe('number');

    const summaries = await store.listSessionSummaries('proj');
    expect(summaries[0]!.sessionId).toBe('s1');
    expect(summaries[0]!.data).toBeDefined(); // folded via the real foldSessionSummary
    expect(typeof summaries[0]!.mtime).toBe('number');
  });

  it('listSubkeys follows the camelCase key contract', async () => {
    const actae = new FakeActaeClient();
    const store = new ActaeClaudeSessionStore(actae as never);
    await store.append(key('s1'), [entry('u1', 'user', 'hi', '2026-01-01T00:00:00Z')] as never);
    await store.append(key('s1', 'subagents/x'), [entry('u2', 'assistant', 'ok', '2026-01-01T00:00:01Z')] as never);
    const subkeys = await store.listSubkeys({ projectKey: 'proj', sessionId: 's1' });
    expect(subkeys).toContain('subagents/x');
  });

  it('delete removes the transcript and cascades subkeys', async () => {
    const actae = new FakeActaeClient();
    const store = new ActaeClaudeSessionStore(actae as never);
    await store.append(key('s1'), [entry('u1', 'user', 'hi', '2026-01-01T00:00:00Z')] as never);
    await store.append(key('s1', 'subagents/x'), [entry('u2', 'assistant', 'ok', '2026-01-01T00:00:01Z')] as never);
    await store.delete(key('s1'));
    expect(await store.load(key('s1'))).toBeNull();
    expect(await store.load(key('s1', 'subagents/x'))).toBeNull();
  });
});
