import { afterEach, describe, expect, it } from 'vitest';
import * as http from 'node:http';
import type { AddressInfo } from 'node:net';
import {
  ActaeClient,
  newClientFromEnv,
  ptr,
} from '../src/client.js';
import {
  APIError,
  AuthError,
  ChannelConflictError,
  ConsumerError,
  ConnectionError,
  ExecutionNotFoundError,
  ExecutionNotOwnedError,
  IdempotencyConflictError,
  IdempotencyKeyMismatchError,
  LockError,
  NotFoundError,
  RateLimitError,
  ServerError,
  SnapshotBoundaryError,
  VersionConflictError,
} from '../src/errors.js';
import { createTestServer, type Router } from './helpers.js';

let servers: Array<{ close: () => Promise<void> }> = [];
async function serve(router: Router) {
  const s = await createTestServer(router);
  servers.push(s);
  return s;
}
afterEach(async () => {
  await Promise.all(servers.map((s) => s.close()));
  servers = [];
});

const client = (baseUrl: string, opts: Record<string, unknown> = {}) =>
  new ActaeClient({ apiKey: 'k', endpoint: baseUrl, ...opts } as never);

describe('constructor edge forks', () => {
  it('accepts wsEndpoint-only clients and strips trailing slashes', () => {
    const c = new ActaeClient({ apiKey: 'k', wsEndpoint: 'ws://host:1234/ws/' });
    expect(c.wsEndpoint).toBe('ws://host:1234/ws');
    expect(c.httpEndpoint).toBe('');
  });
  it('maxReconnectFailures <= 0 falls back to default', () => {
    const c = new ActaeClient({ apiKey: 'k', endpoint: 'http://x', maxReconnectFailures: 0 });
    expect((c as unknown as { maxReconnectFailures: number }).maxReconnectFailures).toBe(10);
  });
  it('keepAliveInterval normalization (<0 → 0, 0 → 30000, >0 → value)', () => {
    const neg = new ActaeClient({ apiKey: 'k', endpoint: 'http://x', keepAliveInterval: -5 });
    const zero = new ActaeClient({ apiKey: 'k', endpoint: 'http://x', keepAliveInterval: 0 });
    const pos = new ActaeClient({ apiKey: 'k', endpoint: 'http://x', keepAliveInterval: 9000 });
    expect((neg as unknown as { keepAlive: number }).keepAlive).toBe(0);
    expect((zero as unknown as { keepAlive: number }).keepAlive).toBe(30000);
    expect((pos as unknown as { keepAlive: number }).keepAlive).toBe(9000);
  });
  it('endpoint trailing slashes are stripped', () => {
    const c = new ActaeClient({ apiKey: 'k', endpoint: 'http://x:8002///' });
    expect(c.httpEndpoint).toBe('http://x:8002');
  });
});

describe('request engine edge cases', () => {
  it('invalid endpoint URL raises ConnectionError', async () => {
    const c = new ActaeClient({ apiKey: 'k', endpoint: 'not a url' });
    await expect(c.listChannels()).rejects.toBeInstanceOf(ConnectionError);
  });
  it('returns raw body string for text/plain 2xx responses', async () => {
    const s = await serve(() => ({ headers: { 'content-type': 'text/plain' }, body: 'raw' }));
    const c = client(s.baseUrl);
    expect(await c.getMetricsText()).toBe('raw');
  });
  it('returns raw body string when JSON parse fails on 2xx', async () => {
    const s = await serve(() => ({ headers: { 'content-type': 'application/json' }, body: 'not-json' }));
    const c = client(s.baseUrl);
    expect(await c.getMetricsText()).toBe('not-json');
  });
  it('empty 2xx body parses as {}', async () => {
    const s = await serve(() => ({ headers: { 'content-type': 'application/json' }, body: '' }));
    const c = client(s.baseUrl);
    await expect(c.listChannels()).resolves.toEqual([]);
  });
  it('request timeout raises ConnectionError', async () => {
    // A raw server that never responds, so only the client timeout settles it.
    const raw = http.createServer((_req, _res) => {
      /* never respond */
    });
    await new Promise<void>((resolve) => raw.listen(0, '127.0.0.1', resolve));
    const addr = raw.address() as AddressInfo;
    servers.push({ close: () => new Promise((r) => raw.close(() => r())) });
    const c = client(`http://127.0.0.1:${addr.port}`, { timeout: 50 });
    await expect(c.listChannels()).rejects.toThrow(/timed out/);
  });
});

describe('error mapping — every status fork', () => {
  async function expectErr(status: number, body: unknown, type: unknown, props?: Record<string, unknown>) {
    const s = await serve(() => ({ status, body }));
    const c = client(s.baseUrl);
    const p = c.listChannels();
    await expect(p).rejects.toBeInstanceOf(type as never);
    if (props) await expect(p).rejects.toMatchObject(props);
    await p.catch(() => undefined);
  }
  it('402 → LockError', async () => expectErr(402, { error: 'lock' }, LockError));
  it('403 → APIError', async () => expectErr(403, { error: 'forbidden' }, APIError));
  it('429 → RateLimitError with retry_after', async () => {
    const s = await serve(() => ({ status: 429, body: { retry_after_seconds: 5, error: 'slow' } }));
    const c = client(s.baseUrl);
    await expect(c.listChannels()).rejects.toMatchObject({ retryAfterSeconds: 5 });
  });
  it('404 execution_not_found → ExecutionNotFoundError', async () =>
    expectErr(404, { status: 'execution_not_found', error: 'x' }, ExecutionNotFoundError));
  it('404 other → NotFoundError', async () => expectErr(404, { error: 'x' }, NotFoundError));
  it('409 snapshot_boundary_required', async () =>
    expectErr(409, { status: 'snapshot_boundary_required' }, SnapshotBoundaryError));
  it('409 version_conflict', async () => expectErr(409, { status: 'version_conflict' }, VersionConflictError));
  it('409 consumer_not_found', async () => expectErr(409, { status: 'consumer_not_found' }, ConsumerError));
  it('409 idempotency_key_mismatch', async () =>
    expectErr(409, { status: 'idempotency_key_mismatch' }, IdempotencyKeyMismatchError));
  it('409 execution_not_owned', async () =>
    expectErr(409, { status: 'execution_not_owned' }, ExecutionNotOwnedError));
  it('409 idempotency_conflict', async () =>
    expectErr(409, { status: 'idempotency_conflict' }, IdempotencyConflictError));
  it('409 channel_conflict', async () => expectErr(409, { status: 'channel_conflict' }, ChannelConflictError));
  it('409 unknown status → APIError', async () => expectErr(409, { status: 'weird' }, APIError));
  it('500 → ServerError', async () => expectErr(500, { error: 'boom' }, ServerError));
  it('non-object 500 body maps to ServerError', async () => expectErr(500, 'boom', ServerError));
  it('429 without body object → default retry 60', async () => {
    const s = await serve(() => ({ status: 429, body: 'plain' }));
    const c = client(s.baseUrl);
    await expect(c.listChannels()).rejects.toMatchObject({ retryAfterSeconds: 60 });
  });
});

describe('cursor / state / channels / fork details', () => {
  it('getCursor parses numeric-string latest_cursor', async () => {
    const s = await serve(() => ({ body: { latest_cursor: '42' } }));
    const c = client(s.baseUrl);
    expect(await c.getCursor('ch')).toBe(42);
  });
  it('latestCursor is an alias', async () => {
    const s = await serve(() => ({ body: { latest_cursor: 9 } }));
    const c = client(s.baseUrl);
    expect(await c.latestCursor('ch')).toBe(9);
  });
  it('fork sends expected_version/expected_cursor/experiment_metadata and a fixed operation_id', async () => {
    const s = await serve(() => ({ body: { fork_id: 'f', child_channel_id: 'c' } }));
    const c = client(s.baseUrl);
    await c.fork('parent', 'child', 0, {
      operationId: 'op-fixed',
      expectedVersion: 3,
      expectedCursor: 5,
      experimentMetadata: { temp: 0.3 },
    });
    expect(s.requests[0]!.json).toMatchObject({
      operation_id: 'op-fixed',
      expected_version: 3,
      expected_cursor: 5,
      experiment_metadata: { temp: 0.3 },
    });
  });
  it('getForkReceipt fetches and parses the receipt', async () => {
    const s = await serve(() => ({
      body: {
        fork_id: 'f', source_channel_id: 'p', child_channel_id: 'c',
        requested_cursor: 7, resolved_cursor: 5, restorable: true,
        source_state_version: 2, source_state_sha256: 'abc', reproducibility: 'state_exact',
      },
    }));
    const c = client(s.baseUrl);
    const r = await c.getForkReceipt('c');
    expect(r.requestedCursor).toBe(7);
    expect(r.resolvedCursor).toBe(5);
    expect(r.reproducibility).toBe('state_exact');
    expect(s.requests[0]!.url).toBe('/api/v1/channels/c/receipt');
  });
  it('resolveStep parses a step resolution', async () => {
    const s = await serve(() => ({ body: { channel_id: 'owner', step_number: 3, cursor: 12, event_id: 'e1' } }));
    const c = client(s.baseUrl);
    const r = await c.resolveStep('ch', 3);
    expect(r).toMatchObject({ channelId: 'owner', stepNumber: 3, cursor: 12, eventId: 'e1' });
  });
  it('resolveStep rethrows non-404 errors', async () => {
    const s = await serve(() => ({ status: 500, body: { error: 'boom' } }));
    const c = client(s.baseUrl);
    await expect(c.resolveStep('ch', 3)).rejects.toBeInstanceOf(ServerError);
  });
  it('setOutcome includes score when given', async () => {
    const s = await serve(() => ({ body: { channel_id: 'c', outcome: 'promoted', result_score: 0.9 } }));
    const c = client(s.baseUrl);
    await c.setOutcome('c', 'promoted', 0.9);
    expect(s.requests[0]!.json).toEqual({ outcome: 'promoted', score: 0.9 });
  });
  it('setOutcome omits score when absent', async () => {
    const s = await serve(() => ({ body: {} }));
    const c = client(s.baseUrl);
    await c.setOutcome('c', 'crashed');
    expect(s.requests[0]!.json).toEqual({ outcome: 'crashed' });
  });
  it('promoteChannel POSTs to promote', async () => {
    const s = await serve(() => ({ body: { status: 'promoted' } }));
    const c = client(s.baseUrl);
    await c.promoteChannel('c');
    expect(s.requests[0]!.method).toBe('POST');
    expect(s.requests[0]!.url).toBe('/api/v1/channels/c/promote');
  });
  it('deleteChannel default vs recursive', async () => {
    const s = await serve(() => ({ body: {} }));
    const c = client(s.baseUrl);
    await c.deleteChannel('c');
    await c.deleteChannel('c', true);
    expect(s.requests[0]!.url).toBe('/api/v1/channels/c');
    expect(s.requests[1]!.url).toBe('/api/v1/channels/c?recursive=true');
  });
  it('compareChannels builds query', async () => {
    const s = await serve(() => ({ body: { left_channel_id: 'a', right_channel_id: 'b' } }));
    const c = client(s.baseUrl);
    await c.compareChannels('a', 'b');
    expect(s.requests[0]!.url).toBe('/api/v1/channels/compare?left=a&right=b');
  });
  it('listForks parses metadata', async () => {
    const s = await serve(() => ({ body: { forks: [{ channel_id: 'b1', forked_at_cursor: 4 }] } }));
    const c = client(s.baseUrl);
    const forks = await c.listForks('p');
    expect(forks[0]!.channelId).toBe('b1');
  });
  it('getForkTree parses recursive ForkInfo', async () => {
    const s = await serve(() => ({
      body: { channel_id: 'root', event_count: 3, latest_cursor: 9, children: [{ channel_id: 'kid', children: [] }] },
    }));
    const c = client(s.baseUrl);
    const tree = await c.getForkTree('root');
    expect(tree.channelId).toBe('root');
    expect(tree.children?.[0]?.channelId).toBe('kid');
  });
  it('diffStates parses per-side divergence + truncation', async () => {
    const s = await serve(() => ({
      body: {
        left_channel_id: 'L', right_channel_id: 'R',
        left: { channel_id: 'L', cursor: 1, state: {} },
        right: { channel_id: 'R', cursor: 1, state: {} },
        left_diverged_at_cursor: 1, right_diverged_at_cursor: 2,
        truncated: true, entry_count_total: 3, max_entries: 500,
        entries: [{ path: ['a'], kind: 'added' }],
      },
    }));
    const c = client(s.baseUrl);
    const d = await c.diffStates('L', 'R');
    expect(d.leftDivergedAtCursor).toBe(1);
    expect(d.rightDivergedAtCursor).toBe(2);
    expect(d.truncated).toBe(true);
    expect(d.entryCountTotal).toBe(3);
    expect(d.entries[0]!.kind).toBe('added');
  });
  it('decisionTrail parses ancestry + boundary + executions', async () => {
    const s = await serve(() => ({
      body: {
        channel_id: 'c', origin_run_id: 'root',
        ancestry: [{ channel_id: 'c', forked_at_cursor: 0 }],
        boundary: { channel_id: 'p', cursor: 1, state: {} },
        executions: [{ id: 'x1', channel_id: 'c', key_name: 'k', tool_name: 't', status: 'completed' }],
      },
    }));
    const c = client(s.baseUrl);
    const t = await c.decisionTrail('c');
    expect(t.originRunId).toBe('root');
    expect(t.ancestry[0]!.channelId).toBe('c');
    expect(t.boundary?.channelId).toBe('p');
    expect(t.executions[0]!.keyName).toBe('k');
  });
});

describe('experiments / health / auth details', () => {
  it('createExperiment includes description and baseline', async () => {
    const s = await serve(() => ({ body: { group_id: 'g1' } }));
    const c = client(s.baseUrl);
    await c.createExperiment('exp', { description: 'd', baselineChannelId: 'base' });
    expect(s.requests[0]!.json).toEqual({ name: 'exp', description: 'd', baseline_channel_id: 'base' });
  });
  it('listExperiments handles both array and {experiments:[...]}', async () => {
    const s1 = await serve(() => ({ body: [{ group_id: 'a' }] }));
    const c1 = client(s1.baseUrl);
    expect((await c1.listExperiments()) as unknown[]).toHaveLength(1);
    const s2 = await serve(() => ({ body: { experiments: [{ group_id: 'b' }] } }));
    const c2 = client(s2.baseUrl);
    expect((await c2.listExperiments()) as unknown[]).toHaveLength(1);
  });
  it('addExperimentMember defaults role to variant and sends delta', async () => {
    const s = await serve(() => ({ body: {} }));
    const c = client(s.baseUrl);
    await c.addExperimentMember('g1', 'ch', { declaredDelta: { model: 'm' } });
    expect(s.requests[0]!.json).toEqual({ channel_id: 'ch', role: 'variant', declared_delta: { model: 'm' } });
  });
  it('rankExperiment GETs the rank endpoint', async () => {
    const s = await serve(() => ({ body: { count: 2 } }));
    const c = client(s.baseUrl);
    await c.rankExperiment('g1');
    expect(s.requests[0]!.url).toBe('/api/v1/experiments/g1/rank');
  });
  it('readinessCheck + getMetricsJson parse responses', async () => {
    const s = await serve(() => ({ body: { status: 'ready' } }));
    const c = client(s.baseUrl);
    const r = await c.readinessCheck();
    expect(r.status).toBe('ready');
    const s2 = await serve(() => ({ body: { status: 'ok', uptime_seconds: 5, websocket: { connections: 1, topics: 2, total_messages: 10 } } }));
    const c2 = client(s2.baseUrl);
    const m = await c2.getMetricsJson();
    expect(m.uptimeSeconds).toBe(5);
    expect(m.websocketConnections).toBe(1);
    expect(m.totalMessages).toBe(10);
  });
  it('signup sends optional name', async () => {
    const s = await serve(() => ({ body: { user: { id: 'u', email: 'a@b.c' }, token: 't' } }));
    const c = client(s.baseUrl);
    const a = await c.signup({ email: 'a@b.c', password: 'pw', name: 'n' });
    expect(a.token).toBe('t');
    expect(s.requests[0]!.json).toEqual({ email: 'a@b.c', password: 'pw', name: 'n' });
  });
  it('logout sends Bearer header', async () => {
    const s = await serve(() => ({ body: {} }));
    const c = client(s.baseUrl);
    await c.logout('jwt');
    expect(s.requests[0]!.headers['authorization']).toBe('Bearer jwt');
  });
});

describe('groups / wakeups / executions details', () => {
  it('listGroups filters by channel when given', async () => {
    const s = await serve(() => ({ body: { groups: [{ group_id: 'g', channel_id: 'c', created_at: 't' }] } }));
    const c = client(s.baseUrl);
    const groups = await c.listGroups({ channelId: 'c' });
    expect(groups[0]!.groupId).toBe('g');
    expect(s.requests[0]!.url).toContain('channel_id=c');
  });
  it('deleteGroup and groupOffsets hit their endpoints', async () => {
    const s = await serve(() => ({ body: {} }));
    const c = client(s.baseUrl);
    await c.deleteGroup('g');
    await c.groupOffsets('g');
    expect(s.requests[0]!.method).toBe('DELETE');
    expect(s.requests[1]!.url).toBe('/api/v1/groups/g/offsets');
  });
  it('joinGroup defaults lease to 60 when absent/invalid', async () => {
    const s = await serve(() => ({ body: { group_id: 'g', consumer_id: 'u', last_cursor: 0, claimed_cursor: 0, updated_at: 't' } }));
    const c = client(s.baseUrl);
    await c.joinGroup('g', 'u');
    await c.joinGroup('g', 'u', -1);
    expect(s.requests[0]!.json).toEqual({ consumer_id: 'u', lease_seconds: 60 });
    expect(s.requests[1]!.json).toEqual({ consumer_id: 'u', lease_seconds: 60 });
  });
  it('heartbeat sends lease_seconds', async () => {
    const s = await serve(() => ({ body: {} }));
    const c = client(s.baseUrl);
    await c.heartbeat('g', 'u', 30);
    expect(s.requests[0]!.json).toEqual({ consumer_id: 'u', lease_seconds: 30 });
  });
  it('getWakeup returns undefined on 404', async () => {
    const s = await serve(() => ({ status: 404, body: { error: 'x' } }));
    const c = client(s.baseUrl);
    expect(await c.getWakeup('w1')).toBeUndefined();
  });
  it('getWakeup parses a wakeup', async () => {
    const s = await serve(() => ({ body: { id: 'w1', channel_id: 'c', run_at: 'r', status: 'pending' } }));
    const c = client(s.baseUrl);
    const w = await c.getWakeup('w1');
    expect(w?.id).toBe('w1');
  });
  it('cancelWakeup returns boolean from deleted flag', async () => {
    const s = await serve(() => ({ body: { deleted: true } }));
    const c = client(s.baseUrl);
    expect(await c.cancelWakeup('w1')).toBe(true);
  });
  it('claimExecution parses replayed + in_progress results', async () => {
    const s = await serve(() => ({
      body: { status: 'replayed', result: { ok: true }, execution: { id: 'x1', channel_id: 'c', key_name: 'k', tool_name: 't', status: 'completed', attempts: 1 } },
    }));
    const c = client(s.baseUrl);
    const r = await c.claimExecution('c', 'k', 't', { a: 1 });
    expect(r.status).toBe('replayed');
    const s2 = await serve(() => ({
      body: { status: 'in_progress', lease_until: 'later', execution: { id: 'x2', channel_id: 'c', key_name: 'k', tool_name: 't', status: 'running', attempts: 1 } },
    }));
    const c2 = client(s2.baseUrl);
    const r2 = await c2.claimExecution('c', 'k', 't', { a: 1 });
    expect(r2.status).toBe('in_progress');
  });
  it('failExecution / heartbeatExecution / cancelExecution / getExecution / listExecutions / deleteExecution', async () => {
    const s = await serve(() => ({ body: { status: 'failed' } }));
    const c = client(s.baseUrl);
    await c.failExecution('x1', 'boom', { claimToken: 'tok', errorType: 'TypeError' });
    expect(s.requests[0]!.json).toEqual({ claim_token: 'tok', error_type: 'TypeError', message: 'boom' });

    const s2 = await serve(() => ({ body: { status: 'running' } }));
    const c2 = client(s2.baseUrl);
    await c2.heartbeatExecution('x1', 'tok', 30);
    expect(s2.requests[0]!.json).toEqual({ claim_token: 'tok', lease_seconds: 30 });

    const s3 = await serve(() => ({ body: { status: 'cancelled' } }));
    const c3 = client(s3.baseUrl);
    await c3.cancelExecution('x1', 'tok');
    expect(s3.requests[0]!.json).toEqual({ claim_token: 'tok' });

    const s4 = await serve(() => ({ body: { execution: { id: 'x1', channel_id: 'c', key_name: 'k', tool_name: 't', status: 'running', attempts: 1 } } }));
    const c4 = client(s4.baseUrl);
    const ex = await c4.getExecution('x1');
    expect(ex.id).toBe('x1');

    const s5 = await serve(() => ({ body: { executions: [{ id: 'x1', channel_id: 'c', key_name: 'k', tool_name: 't', status: 'completed', attempts: 1 }] } }));
    const c5 = client(s5.baseUrl);
    const list = await c5.listExecutions('c', 50);
    expect(list[0]!.id).toBe('x1');
    expect(s5.requests[0]!.url).toContain('limit=50');

    const s6 = await serve(() => ({ body: {} }));
    const c6 = client(s6.baseUrl);
    await c6.deleteExecution('x1');
    expect(s6.requests[0]!.method).toBe('DELETE');
  });
});

describe('facades — remaining surfaces', () => {
  it('state facade delegates', async () => {
    const s = await serve(() => ({ body: { cursor: 5, state: {} } }));
    const c = client(s.baseUrl);
    await c.state.latestState('ch');
    expect(s.requests[0]!.url).toBe('/api/v1/state/ch');
  });
  it('channels facade delegates fork + trail + diff + tree + forks', async () => {
    const s = await serve(() => ({ body: { fork_id: 'f' } }));
    const c = client(s.baseUrl);
    await c.channels.fork('p', 'c', 0);
    await c.channels.decisionTrail('c');
    await c.channels.diffStates('a', 'b');
    await c.channels.getForkTree('r');
    await c.channels.listForks('p');
    expect(s.requests[0]!.url).toBe('/api/v1/channels/fork');
  });
  it('executions facade delegates', async () => {
    const s = await serve(() => ({ body: { status: 'claimed', execution: { id: 'x', channel_id: 'c', key_name: 'k', tool_name: 't', status: 'running', attempts: 1 }, claim_token: 't' } }));
    const c = client(s.baseUrl);
    await c.executions.claim('c', 'k', 't', {});
    await c.executions.list('c');
    await c.executions.get('x');
    await c.executions.delete('x');
    expect(s.requests[0]!.url).toBe('/api/v1/executions/claim');
  });
  it('groups facade delegates', async () => {
    const s = await serve(() => ({ body: {} }));
    const c = client(s.baseUrl);
    await c.groups.create('g', 'c');
    await c.groups.list();
    await c.groups.delete('g');
    await c.groups.join('g', 'u');
    await c.groups.offsets('g');
    expect(s.requests[0]!.method).toBe('POST');
  });
  it('wakeups facade delegates', async () => {
    const s = await serve(() => ({ body: {} }));
    const c = client(s.baseUrl);
    await c.wakeups.schedule('c', '2026-01-01T00:00:00Z');
    await c.wakeups.list();
    await c.wakeups.cancel('w');
    expect(s.requests[0]!.url).toBe('/api/v1/scheduler/wakeups');
  });
  it('health facade delegates readiness + metrics', async () => {
    const s = await serve(() => ({ body: { status: 'ok' } }));
    const c = client(s.baseUrl);
    await c.health.readiness();
    await c.health.metricsJson();
    expect(s.requests[0]!.url).toBe('/readyz');
  });
  it('auth facade delegates signup/logout', async () => {
    const s = await serve(() => ({ body: { user: { id: 'u', email: 'a@b.c' }, token: 't' } }));
    const c = client(s.baseUrl);
    await c.auth.signup({ email: 'a@b.c', password: 'pw' });
    await c.auth.login('a@b.c', 'pw');
    expect(s.requests[0]!.url).toBe('/api/v1/auth/signup');
  });
  it('ws facade exposes lifecycle functions', () => {
    const c = new ActaeClient({ apiKey: 'k', endpoint: 'http://x' });
    expect(typeof c.ws.connect).toBe('function');
    expect(typeof c.ws.disconnect).toBe('function');
    expect(typeof c.ws.subscribe).toBe('function');
    expect(typeof c.ws.publish).toBe('function');
  });
});

describe('ptr helper', () => {
  it('returns its argument', () => {
    expect(ptr(42)).toBe(42);
    expect(ptr('x')).toBe('x');
    const o = { a: 1 };
    expect(ptr(o)).toBe(o);
  });
});

describe('newClientFromEnv edge', () => {
  const oldEnv = { ...process.env };
  afterEach(() => {
    process.env = { ...oldEnv };
  });
  it('ACTAE_WS_URL overrides derivation', () => {
    process.env['ACTAE_API_KEY'] = 'k';
    process.env['ACTAE_URL'] = 'http://host';
    process.env['ACTAE_WS_URL'] = 'ws://other:9000/ws';
    const c = newClientFromEnv();
    expect(c.wsEndpoint).toBe('ws://other:9000/ws');
    delete process.env['ACTAE_URL'];
    delete process.env['ACTAE_WS_URL'];
    expect(() => newClientFromEnv()).toThrow(/endpoint/);
  });
});
