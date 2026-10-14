import { describe, it, expect } from 'vitest';
import {
  eventFromRecord,
  eventFromReplay,
  eventFromBroadcast,
  channelMetadataFromDict,
  forkReceiptFromDict,
  forkInfoFromDict,
  healthStatusFromResponse,
  readinessResultFromResponse,
  metricsSnapshotFromResponse,
  authResultFromResponse,
  stateSnapshotFrom,
  stateDiffFrom,
  decisionTrailFrom,
  groupInfoFromResponse,
  claimedWorkFromResponse,
  wakeupFromResponse,
  executionInfoFromResponse,
  executionClaimFromResponse,
} from '../src/types.js';
import { SONIC_NUMBER_KEY } from '../src/json.js';

describe('eventFromRecord', () => {
  it('parses a record response', () => {
    const ev = eventFromRecord({
      id: 'uuid',
      channel_id: 'ch',
      type: 'agent.step',
      payload: { input: 'hi' },
      actor: 'agent',
      cursor: 3,
      channel_cursor: 3,
      timestamp: '2026-01-01T00:00:00Z',
      depends_on: 'parent-id',
    });
    expect(ev).toMatchObject({
      id: 'uuid',
      channelId: 'ch',
      eventType: 'agent.step',
      payload: { input: 'hi' },
      actor: 'agent',
      cursor: 3,
      channelCursor: 3,
      timestamp: '2026-01-01T00:00:00Z',
      dependsOn: 'parent-id',
    });
    expect(ev.metadata).toBeUndefined();
    expect(ev.agentId).toBeUndefined();
  });
});

describe('eventFromReplay / eventFromQuery', () => {
  it('fills agent_id and metadata from replay', () => {
    const ev = eventFromReplay({
      id: 'uuid',
      channel_id: 'ch',
      type: 't',
      payload: null,
      cursor: 1,
      timestamp: 't',
      agent_id: 'agent-1',
      metadata: { session: 's1' },
    });
    expect(ev.agentId).toBe('agent-1');
    expect(ev.metadata).toEqual({ session: 's1' });
  });
});

describe('eventFromBroadcast', () => {
  it('parses a flat broadcast frame', () => {
    const ev = eventFromBroadcast({
      id: 'e1',
      channel_id: 'ch',
      type: 'broadcast',
      payload: { ok: true },
      cursor: 5,
      timestamp: 't',
      delivery_id: 'd1',
    });
    expect(ev.id).toBe('e1');
    expect(ev.cursor).toBe(5);
    expect(ev.payload).toEqual({ ok: true });
    expect(ev.deliveryId).toBe('d1');
  });

  it('falls back event_type to type then broadcast', () => {
    expect(eventFromBroadcast({ type: 'custom', payload: null }).eventType).toBe('custom');
    expect(eventFromBroadcast({ payload: null }).eventType).toBe('broadcast');
  });
});

describe('channelMetadataFromDict', () => {
  it('parses fork provenance fields', () => {
    const meta = channelMetadataFromDict({
      channel_id: 'child',
      parent_channel_id: 'parent',
      origin_run_id: 'root',
      forked_at_cursor: 10,
      forked_at: 't',
      requested_at_cursor: 10,
      resolved_state_cursor: 10,
      resolved_event_id: 'ev',
      source_state_version: 4,
      source_state_sha256: 'abc123',
      restorable: true,
      manifest: { model: 'gpt-4' },
      reproducibility: 'state_exact',
      outcome: 'promoted',
      result_score: 0.9,
    });
    expect(meta.parentChannelId).toBe('parent');
    expect(meta.originRunId).toBe('root');
    expect(meta.forkedAtCursor).toBe(10);
    expect(meta.requestedAtCursor).toBe(10);
    expect(meta.resolvedStateCursor).toBe(10);
    expect(meta.resolvedCursor).toBe(10);
    expect(meta.sourceStateVersion).toBe(4);
    expect(meta.sourceStateSha256).toBe('abc123');
    expect(meta.restorable).toBe(true);
    expect(meta.reproducibility).toBe('state_exact');
    expect(meta.outcome).toBe('promoted');
    expect(meta.resultScore).toBe(0.9);
  });

  it('resolvedCursor falls back to resolved_state_cursor', () => {
    const meta = channelMetadataFromDict({
      channel_id: 'c',
      resolved_state_cursor: 7,
    });
    expect(meta.resolvedCursor).toBe(7);
  });
});

describe('forkReceiptFromDict', () => {
  it('parses and falls back channel_id aliases', () => {
    const r = forkReceiptFromDict({
      channel_id: 'child',
      source_channel_id: 'parent',
      requested_at_cursor: 3,
      resolved_state_cursor: 3,
      source_state_version: 1,
      restorable: true,
      replayed: false,
    });
    expect(r.forkId).toBe('child');
    expect(r.childChannelId).toBe('child');
    expect(r.sourceChannelId).toBe('parent');
    expect(r.requestedCursor).toBe(3);
    expect(r.resolvedCursor).toBe(3);
    expect(r.restorable).toBe(true);
  });
});

describe('forkInfoFromDict', () => {
  it('recurses children', () => {
    const tree = forkInfoFromDict({
      channel_id: 'root',
      event_count: 5,
      children: [
        { channel_id: 'a', event_count: 2, children: [] },
        { channel_id: 'b', event_count: 1, children: [{ channel_id: 'b1', children: [] }] },
      ],
    });
    expect(tree.channelId).toBe('root');
    expect(tree.children).toHaveLength(2);
    expect(tree.children[1]?.children).toHaveLength(1);
  });
});

describe('health/readiness/metrics', () => {
  it('healthStatusFromResponse splits component details', () => {
    const h = healthStatusFromResponse({
      status: 'healthy',
      timestamp: 't',
      instance_id: 'i1',
      check_duration_ms: 5,
      components: {
        database: { status: 'ok', latency_ms: 2 },
      },
    });
    expect(h.status).toBe('healthy');
    expect(h.checkDurationMs).toBe(5);
    expect(h.components['database']).toEqual({ status: 'ok', details: { latency_ms: 2 } });
  });

  it('readinessResultFromResponse reads checks', () => {
    const r = readinessResultFromResponse({
      status: 'ready',
      readiness_checks: {
        database_ready: true,
        capacity_available: true,
        uptime_seconds: 42,
        connection_utilization: 0.5,
      },
    });
    expect(r.databaseReady).toBe(true);
    expect(r.uptimeSeconds).toBe(42);
    expect(r.connectionUtilization).toBe(0.5);
  });

  it('metricsSnapshotFromResponse reads websocket subtree', () => {
    const m = metricsSnapshotFromResponse({
      status: 'ok',
      websocket: {
        connections: 3,
        topics: 2,
        messages_sent: 10,
        total_messages: 20,
        back_pressure: { ch: 5 },
      },
    });
    expect(m.websocketConnections).toBe(3);
    expect(m.topicCount).toBe(2);
    expect(m.totalMessages).toBe(20);
    expect(m.backPressure).toEqual({ ch: 5 });
  });
});

describe('auth', () => {
  it('authResultFromResponse', () => {
    const a = authResultFromResponse({
      user: { id: 'u1', email: 'a@b.c', email_verified: true, name: 'Ann' },
      token: 'jwt',
    });
    expect(a.token).toBe('jwt');
    expect(a.user.email).toBe('a@b.c');
    expect(a.user.name).toBe('Ann');
  });
});

describe('state', () => {
  it('stateSnapshotFrom returns undefined without cursor', () => {
    expect(stateSnapshotFrom({ state: {} })).toBeUndefined();
  });

  it('stateSnapshotFrom parses with cursor', () => {
    const s = stateSnapshotFrom({
      cursor: 9,
      version: 2,
      timestamp: 't',
      state: { a: 1, nested: { [SONIC_NUMBER_KEY]: '5' } },
    });
    expect(s?.cursor).toBe(9);
    expect(s?.version).toBe(2);
    expect(s?.state).toEqual({ a: 1, nested: 5 });
  });
});

describe('diff / trail', () => {
  it('stateDiffFrom parses entries and LCA', () => {
    const d = stateDiffFrom({
      left_channel_id: 'L',
      right_channel_id: 'R',
      left: { channel_id: 'L', cursor: 5, state: { a: 1 } },
      right: { channel_id: 'R', cursor: 6, state: { a: 2 } },
      common: { channel_id: 'P', cursor: 4, state: { a: 1 } },
      left_diverged_at_cursor: 4,
      right_diverged_at_cursor: 4,
      truncated: false,
      entries: [
        { path: ['a'], kind: 'changed', left: 1, right: 2 },
        { path: ['b'], kind: 'added', right: 3 },
      ],
    });
    expect(d.leftChannelId).toBe('L');
    expect(d.common?.state).toEqual({ a: 1 });
    expect(d.entries).toHaveLength(2);
    expect(d.entries[0]?.path).toEqual(['a']);
    expect(d.entries[0]?.kind).toBe('changed');
    expect(d.maxEntries).toBe(500);
  });

  it('decisionTrailFrom parses ancestry and executions', () => {
    const t = decisionTrailFrom({
      channel_id: 'c',
      origin_run_id: 'root',
      ancestry: [{ channel_id: 'root', forked_at_cursor: 0 }],
      boundary: { channel_id: 'p', cursor: 3, state: { x: 1 } },
      executions: [
        { id: 'e1', channel_id: 'c', key_name: 'k', tool_name: 'tool', status: 'completed' },
      ],
    });
    expect(t.originRunId).toBe('root');
    expect(t.ancestry).toHaveLength(1);
    expect(t.boundary?.cursor).toBe(3);
    expect(t.executions[0]?.toolName).toBe('tool');
  });
});

describe('groups / wakeups / executions', () => {
  it('groupInfoFromResponse', () => {
    const g = groupInfoFromResponse({ group_id: 'g', channel_id: 'c', created_at: 't', metadata: { x: 1 } });
    expect(g.groupId).toBe('g');
    expect(g.metadata).toEqual({ x: 1 });
  });

  it('claimedWorkFromResponse parses events', () => {
    const w = claimedWorkFromResponse({
      group_id: 'g',
      consumer_id: 'u1',
      lease_until: 't',
      events: [{ id: 'e1', channel_id: 'c', type: 't', cursor: 1, timestamp: 't' }],
    });
    expect(w.events).toHaveLength(1);
    expect(w.events[0]?.cursor).toBe(1);
  });

  it('wakeupFromResponse', () => {
    const w = wakeupFromResponse({
      id: 'w1',
      channel_id: 'c',
      run_at: 't',
      status: 'pending',
      attempts: 2,
    });
    expect(w.status).toBe('pending');
    expect(w.attempts).toBe(2);
  });

  it('executionInfoFromResponse', () => {
    const e = executionInfoFromResponse({
      id: 'x1',
      channel_id: 'c',
      key_name: 'k',
      tool_name: 'tool',
      status: 'completed',
      attempts: 1,
      replay_emitted: true,
      params: { a: 1 },
      result: 42,
    });
    expect(e.status).toBe('completed');
    expect(e.replayEmitted).toBe(true);
    expect(e.result).toBe(42);
  });

  it('executionClaimFromResponse', () => {
    const c = executionClaimFromResponse({
      status: 'claimed',
      claim_token: 'tok',
      execution: { id: 'x1', status: 'running' },
    });
    expect(c.status).toBe('claimed');
    expect(c.claimToken).toBe('tok');
    expect(c.execution.id).toBe('x1');
  });
});
