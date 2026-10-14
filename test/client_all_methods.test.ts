import { afterEach, describe, expect, it } from 'vitest';
import { ActaeClient } from '../src/client.js';
import { createTestServer, type Router } from './helpers.js';

/**
 * End-to-end coverage of every HTTP method + facade on the client surface
 * (the remaining uncovered paths from the coverage report), each against a
 * minimal fake server response.
 */

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

const ok = (body: unknown, status = 200) => ({ status, body });

describe('channels (remaining methods)', () => {
  it('getForkReceipt', async () => {
    const s = await serve(() => ok({ fork_id: 'f', child_channel_id: 'c' }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    const r = await c.getForkReceipt('c');
    expect(r.childChannelId).toBe('c');
    expect(s.requests[0]!.url).toBe('/api/v1/channels/c/receipt');
  });

  it('resolveStep success', async () => {
    const s = await serve(() => ok({ channel_id: 'owner', step_number: 2, cursor: 9, event_id: 'e' }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    const r = await c.resolveStep('ch', 2);
    expect(r).toMatchObject({ channelId: 'owner', cursor: 9, eventId: 'e' });
  });

  it('setOutcome', async () => {
    const s = await serve(() => ok({ outcome: 'promoted' }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    await c.setOutcome('ch', 'promoted', 0.9);
    expect(s.requests[0]!.method).toBe('PATCH');
    expect(s.requests[0]!.json).toEqual({ outcome: 'promoted', score: 0.9 });
  });

  it('promoteChannel', async () => {
    const s = await serve(() => ok({ merged: true }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    await c.promoteChannel('ch');
    expect(s.requests[0]!.url).toBe('/api/v1/channels/ch/promote');
  });

  it('deleteChannel non-recursive vs recursive', async () => {
    const s = await serve(() => ok({ deleted: true }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    await c.deleteChannel('ch');
    expect(s.requests[0]!.url).toBe('/api/v1/channels/ch');
    await c.deleteChannel('ch', true);
    expect(s.requests[1]!.url).toBe('/api/v1/channels/ch?recursive=true');
  });

  it('compareChannels', async () => {
    const s = await serve(() => ok({ left_channel_id: 'L' }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    await c.compareChannels('L', 'R');
    expect(s.requests[0]!.url).toBe('/api/v1/channels/compare?left=L&right=R');
  });

  it('listForks', async () => {
    const s = await serve(() => ok({ forks: [{ channel_id: 'b1' }] }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    const forks = await c.listForks('ch');
    expect(forks[0]!.channelId).toBe('b1');
  });

  it('getForkTree', async () => {
    const s = await serve(() => ok({ channel_id: 'root', event_count: 3, children: [] }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    const tree = await c.getForkTree('root');
    expect(tree.channelId).toBe('root');
    expect(tree.eventCount).toBe(3);
  });
});

describe('experiments', () => {
  it('createExperiment', async () => {
    const s = await serve(() => ok({ id: 'exp-1' }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    const r = await c.createExperiment('A/B', { description: 'd', baselineChannelId: 'base' });
    expect(r).toEqual({ id: 'exp-1' });
    expect(s.requests[0]!.json).toEqual({ name: 'A/B', description: 'd', baseline_channel_id: 'base' });
  });

  it('listExperiments (array and object forms)', async () => {
    const s = await serve(() => ok([{ id: 'a' }]));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    expect(await c.listExperiments()).toEqual([{ id: 'a' }]);
    const s2 = await serve(() => ok({ experiments: [{ id: 'b' }] }));
    const c2 = new ActaeClient({ apiKey: 'k', endpoint: s2.baseUrl });
    expect(await c2.listExperiments()).toEqual([{ id: 'b' }]);
  });

  it('addExperimentMember defaults role to variant', async () => {
    const s = await serve(() => ok({ ok: true }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    await c.addExperimentMember('g1', 'ch');
    expect(s.requests[0]!.json).toEqual({ channel_id: 'ch', role: 'variant' });
  });

  it('rankExperiment', async () => {
    const s = await serve(() => ok({ ranking: [] }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    await c.rankExperiment('g1');
    expect(s.requests[0]!.url).toBe('/api/v1/experiments/g1/rank');
  });
});

describe('wakeups (remaining)', () => {
  it('getWakeup returns undefined on 404', async () => {
    const s = await serve(() => ({ status: 404, body: { error: 'x' } }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    expect(await c.getWakeup('w1')).toBeUndefined();
  });

  it('cancelWakeup throws WakeupAlreadyFiredError on 409', async () => {
    const s = await serve(() => ({ status: 409, body: { error: 'already' } }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    await expect(c.cancelWakeup('w1')).rejects.toThrow(/already fired/);
  });
});

describe('executions (remaining)', () => {
  it('failExecution with structured error', async () => {
    const s = await serve(() => ok({ execution: { id: 'x1', status: 'failed' } }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    const e = await c.failExecution('x1', 'boom', { errorType: 'Timeout', stack: 'at f' });
    expect(e.status).toBe('failed');
    expect(s.requests[0]!.json).toEqual({ message: 'boom', error_type: 'Timeout', stack: 'at f' });
  });

  it('heartbeatExecution extends lease', async () => {
    const s = await serve(() => ok({ execution: { id: 'x1', status: 'running' } }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    await c.heartbeatExecution('x1', 'tok', 30);
    expect(s.requests[0]!.json).toEqual({ claim_token: 'tok', lease_seconds: 30 });
  });

  it('cancelExecution', async () => {
    const s = await serve(() => ok({ execution: { id: 'x1', status: 'cancelled' } }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    await c.cancelExecution('x1');
    expect(s.requests[0]!.url).toBe('/api/v1/executions/x1/cancel');
  });

  it('listExecutions defaults limit to 50', async () => {
    const s = await serve(() => ok({ executions: [] }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    await c.listExecutions('ch');
    expect(s.requests[0]!.url).toContain('limit=50');
    expect(s.requests[0]!.url).toContain('channel_id=ch');
  });

  it('deleteExecution', async () => {
    const s = await serve(() => ok({}));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    await c.deleteExecution('x1');
    expect(s.requests[0]!.method).toBe('DELETE');
  });
});

describe('health / auth (remaining)', () => {
  it('readinessCheck', async () => {
    const s = await serve(() => ok({ status: 'ready', readiness_checks: { database_ready: true } }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    const r = await c.readinessCheck();
    expect(r.databaseReady).toBe(true);
  });

  it('getMetricsJson', async () => {
    const s = await serve(() => ok({ status: 'ok', websocket: { connections: 2 } }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    const m = await c.getMetricsJson();
    expect(m.websocketConnections).toBe(2);
  });

  it('signup + logout', async () => {
    const s = await serve(() => ok({ user: { id: 'u1', email: 'a@b.c', email_verified: true }, token: 'jwt' }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    const a = await c.signup({ email: 'a@b.c', password: 'pw', name: 'Ann' });
    expect(a.token).toBe('jwt');
    expect(s.requests[0]!.json).toMatchObject({ email: 'a@b.c', name: 'Ann' });
    await c.logout('jwt');
    expect(s.requests[1]!.headers['authorization']).toBe('Bearer jwt');
  });
});

describe('groups (remaining)', () => {
  it('deleteGroup / joinGroup / heartbeat / groupOffsets', async () => {
    const s = await serve(() => ok({ group_id: 'g', consumer_id: 'u1', last_cursor: 1, claimed_cursor: 0, updated_at: 't' }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    await c.deleteGroup('g');
    expect(s.requests[0]!.method).toBe('DELETE');
    const off = await c.joinGroup('g', 'u1', 120);
    expect(off.groupId).toBe('g');
    await c.heartbeat('g', 'u1', 120);
    expect(s.requests[2]!.json).toEqual({ consumer_id: 'u1', lease_seconds: 120 });
    await c.groupOffsets('g');
    expect(s.requests[3]!.url).toBe('/api/v1/groups/g/offsets');
  });

  it('listGroups filters by channel', async () => {
    const s = await serve(() => ok({ groups: [] }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    await c.listGroups({ channelId: 'ch' });
    expect(s.requests[0]!.url).toContain('channel_id=ch');
  });
});

describe('facades + aliases', () => {
  it('all nine facades are wired', async () => {
    const s = await serve(() => ok({ event: { id: 'e1', channel_id: 'c', type: 't', cursor: 1 } }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    expect(typeof c.events.record).toBe('function');
    expect(typeof c.events.replay).toBe('function');
    expect(typeof c.state.saveState).toBe('function');
    expect(typeof c.channels.fork).toBe('function');
    expect(typeof c.executions.claim).toBe('function');
    expect(typeof c.groups.create).toBe('function');
    expect(typeof c.wakeups.schedule).toBe('function');
    expect(typeof c.health.check).toBe('function');
    expect(typeof c.auth.login).toBe('function');
    expect(typeof c.ws.connect).toBe('function');
    expect(typeof c.ws.stream).toBe('function');
    const ev = await c.events.record('c', 't', {});
    expect(ev.cursor).toBe(1);
  });

  it('latestCursor is an alias of getCursor', async () => {
    const s = await serve(() => ok({ latest_cursor: 42 }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    expect(await c.latestCursor('ch')).toBe(42);
    expect(await c.getCursor('ch')).toBe(42);
    expect(s.requests).toHaveLength(2);
  });

  it('httpEndpoint exposes the base URL', () => {
    const c = new ActaeClient({ apiKey: 'k', endpoint: 'http://h:1' });
    expect(c.httpEndpoint).toBe('http://h:1');
  });
});
