import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { ActaeClient, newClientFromEnv } from '../src/client.js';
import { AuthError, ConnectionError, NotFoundError, RateLimitError } from '../src/errors.js';
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

describe('ActaeClient constructor', () => {
  it('requires api_key', () => {
    expect(() => new ActaeClient({ apiKey: '' })).toThrow('api_key is required');
  });
  it('requires endpoint or ws_endpoint', () => {
    expect(() => new ActaeClient({ apiKey: 'k' })).toThrow(/endpoint/);
  });
  it('derives wsEndpoint from endpoint', () => {
    const c = new ActaeClient({ apiKey: 'k', endpoint: 'http://localhost:8002' });
    expect(c.wsEndpoint).toBe('ws://localhost:8002/ws');
    const c2 = new ActaeClient({ apiKey: 'k', endpoint: 'https://host:8443' });
    expect(c2.wsEndpoint).toBe('wss://host:8443/ws');
    const c3 = new ActaeClient({ apiKey: 'k', endpoint: 'https://host/ws' });
    expect(c3.wsEndpoint).toBe('wss://host/ws');
  });
  it('defaults timeout and reconnect', () => {
    const c = new ActaeClient({ apiKey: 'k', endpoint: 'http://x' });
    expect(c.autoReconnect).toBe(true);
    expect(c.echoSelf).toBe(false);
  });
});

describe('validateChannelId', () => {
  const c = () => new ActaeClient({ apiKey: 'k', endpoint: 'http://x' });
  it('rejects empty and invalid ids', async () => {
    await expect(c().record('', 't', {})).rejects.toThrow(/channel id is required/);
    await expect(c().record('bad id', 't', {})).rejects.toThrow(/invalid channel id/);
    await expect(c().record('bad/id', 't', {})).rejects.toThrow(/invalid channel id/);
    await expect(c().record('a'.repeat(300), 't', {})).rejects.toThrow(/maximum length/);
  });
  it('accepts valid ids', async () => {
    await expect(c().record('ok.ch_1:fine-x', 't', {})).rejects.not.toThrow(/invalid channel id/);
  });
});

describe('events', () => {
  it('record sends the right body and fills metadata from opts', async () => {
    const s = await serve(() => ({
      body: {
        event: { id: 'e1', channel_id: 'ch', type: 'agent.step', payload: { x: 1 }, actor: '', cursor: 7, timestamp: 't' },
      },
    }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    const ev = await c.record('ch', 'agent.step', { x: 1 }, {
      actor: 'agent',
      agentId: 'a1',
      userId: 'u1',
      metadata: { m: 1 },
      operationId: 'op-1',
      stepNumber: 3,
    });
    expect(ev.id).toBe('e1');
    expect(ev.cursor).toBe(7);
    expect(ev.agentId).toBe('a1');
    expect(ev.userId).toBe('u1');
    expect(ev.metadata).toEqual({ m: 1 });
    const req = s.requests[0]!;
    expect(req.method).toBe('POST');
    expect(req.url).toBe('/api/v1/events/record');
    expect(req.headers['x-api-key']).toBe('k');
    expect(req.json).toMatchObject({
      channel_id: 'ch',
      event_type: 'agent.step',
      payload: { x: 1 },
      metadata: { actor: 'agent', agent_id: 'a1', user_id: 'u1', metadata: { m: 1 } },
      operation_id: 'op-1',
      step_number: 3,
    });
  });

  it('replay sends cursor/limit/event_type query params', async () => {
    const s = await serve(() => ({ body: { events: [] } }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    await c.replay('ch', { cursor: 5, limit: 50, eventType: 'agent.step' });
    const req = s.requests[0]!;
    expect(req.method).toBe('GET');
    expect(req.url).toBe('/api/v1/events/replay/ch?limit=50&cursor=5&event_type=agent.step');
  });

  it('replay defaults limit to 100', async () => {
    const s = await serve(() => ({ body: { events: [] } }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    await c.replay('ch');
    expect(s.requests[0]!.url).toContain('limit=100');
  });

  it('query builds filter body', async () => {
    const s = await serve(() => ({ body: { events: [] } }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    await c.query({ channelIds: ['a', 'b'], eventType: 't', actor: 'x', cursorStart: 1, cursorEnd: 9, from: 'f', to: 't', limit: 5, offset: 2 });
    expect(s.requests[0]!.json).toMatchObject({
      channel_ids: ['a', 'b'],
      event_type: 't',
      actor: 'x',
      cursor_start: 1,
      cursor_end: 9,
      from: 'f',
      to: 't',
      limit: 5,
      offset: 2,
    });
  });

  it('getCursor returns undefined for empty channel', async () => {
    const s = await serve(() => ({ body: { latest_cursor: null } }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    expect(await c.getCursor('ch')).toBeUndefined();
  });

  it('transition sends event+state and returns version', async () => {
    const s = await serve(() => ({ body: { event: { id: 'e1', channel_id: 'ch', type: 't', cursor: 1 }, state_version: 4 } }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    const r = await c.transition('ch', 't', { x: 1 }, { y: 2 }, { expectedVersion: 3 });
    expect(r.stateVersion).toBe(4);
    expect(s.requests[0]!.json).toMatchObject({
      channel_id: 'ch',
      type: 't',
      state: { y: 2 },
      expected_version: 3,
    });
  });
});

describe('state', () => {
  it('saveState returns version', async () => {
    const s = await serve(() => ({ body: { version: 9 } }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    expect(await c.saveState('ch', 5, { a: 1 }, { expectedCursor: 5 })).toBe(9);
    expect(s.requests[0]!.json).toMatchObject({ channel_id: 'ch', cursor: 5, state: { a: 1 }, expected_cursor: 5 });
  });

  it('latestState returns snapshot', async () => {
    const s = await serve(() => ({ body: { cursor: 5, state: { a: 1 } } }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    const snap = await c.latestState('ch');
    expect(snap?.cursor).toBe(5);
    expect(snap?.state).toEqual({ a: 1 });
  });

  it('listStates parses versions', async () => {
    const s = await serve(() => ({ body: { versions: [{ version: 2, cursor: 5, timestamp: 't' }] } }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    const v = await c.listStates('ch');
    expect(v).toEqual([{ version: 2, cursor: 5, timestamp: 't' }]);
  });

  it('getState hits versioned path', async () => {
    const s = await serve(() => ({ body: { cursor: 5, version: 2, state: {} } }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    await c.getState('ch', 2);
    expect(s.requests[0]!.url).toBe('/api/v1/state/ch/version/2');
  });

  it('deleteState uses DELETE', async () => {
    const s = await serve(() => ({ body: {} }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    await c.deleteState('ch', 2);
    expect(s.requests[0]!.method).toBe('DELETE');
  });
});

describe('channels & forks', () => {
  it('listChannels', async () => {
    const s = await serve(() => ({ body: { channels: ['a', 'b'] } }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    expect(await c.listChannels()).toEqual(['a', 'b']);
  });

  it('fork sends boundary payload and generates operation_id', async () => {
    const s = await serve(() => ({
      body: { fork_id: 'f', child_channel_id: 'child', requested_cursor: 5, resolved_cursor: 5, restorable: true },
    }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    const r = await c.fork('parent', 'child', 5, { displayName: 'c', reason: 'why', manifest: { model: 'm' } });
    expect(r.forkId).toBe('f');
    expect(r.restorable).toBe(true);
    const body = s.requests[0]!.json as Record<string, unknown>;
    expect(body['source_channel_id']).toBe('parent');
    expect(body['new_channel_id']).toBe('child');
    expect(body['at_cursor']).toBe(5);
    expect(body['operation_id']).toBeTypeOf('string');
    expect(body['manifest']).toEqual({ model: 'm' });
  });

  it('resolveStep returns undefined on 404', async () => {
    const s = await serve(() => ({ status: 404, body: { error: 'not found' } }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    expect(await c.resolveStep('ch', 3)).toBeUndefined();
  });

  it('getChannelMetadata returns undefined on 404', async () => {
    const s = await serve(() => ({ status: 404, body: { error: 'nope' } }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    expect(await c.getChannelMetadata('ch')).toBeUndefined();
  });

  it('diffStates builds left/right query', async () => {
    const s = await serve(() => ({
      body: { left_channel_id: 'L', right_channel_id: 'R', left: { state: {} }, right: { state: {} }, entries: [], truncated: false },
    }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    await c.diffStates('L', 'R');
    expect(s.requests[0]!.url).toBe('/api/v1/channels/diff?left=L&right=R');
  });

  it('updateMetadata partial update', async () => {
    const s = await serve(() => ({ body: { channel_id: 'ch' } }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    await c.updateMetadata('ch', { displayName: 'new' });
    expect(s.requests[0]!.method).toBe('PUT');
    expect(s.requests[0]!.json).toEqual({ channel_id: 'ch', display_name: 'new' });
  });
});

describe('health / auth / groups / wakeups / executions', () => {
  it('healthCheck parses components', async () => {
    const s = await serve(() => ({ body: { status: 'healthy', components: { db: { status: 'ok' } } } }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    const h = await c.healthCheck();
    expect(h.status).toBe('healthy');
    expect(h.components['db']?.status).toBe('ok');
  });

  it('getMetricsText returns raw text', async () => {
    const s = await serve(() => ({ headers: { 'content-type': 'text/plain' }, body: 'http_requests 5' }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    expect(await c.getMetricsText()).toBe('http_requests 5');
  });

  it('login sends bearer-able body and parses auth result', async () => {
    const s = await serve(() => ({ body: { user: { id: 'u1', email: 'a@b.c', email_verified: true }, token: 'jwt' } }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    const a = await c.login('a@b.c', 'pw');
    expect(a.token).toBe('jwt');
    expect(a.user.id).toBe('u1');
  });

  it('getMe sends Bearer header', async () => {
    const s = await serve(() => ({ body: { user: { id: 'u1', email: 'a@b.c' } } }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    await c.getMe('jwt-123');
    expect(s.requests[0]!.headers['authorization']).toBe('Bearer jwt-123');
    // api key still attached
    expect(s.requests[0]!.headers['x-api-key']).toBe('k');
  });

  it('createGroup / claimWork / ackWork', async () => {
    const s = await serve(() => ({ body: { group_id: 'g', channel_id: 'c' } }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    await c.createGroup('g', 'c', { team: 'x' });
    await c.claimWork('g', 'u1', 10);
    await c.ackWork('g', 'u1', 5);
    expect(s.requests[0]!.json).toMatchObject({ group_id: 'g', channel_id: 'c', metadata: { team: 'x' } });
    expect(s.requests[1]!.url).toBe('/api/v1/groups/g/work');
    expect(s.requests[2]!.json).toMatchObject({ consumer_id: 'u1', cursor: 5 });
  });

  it('scheduleWakeup / listWakeups / cancelWakeup', async () => {
    const s = await serve(() => ({ body: { id: 'w1', channel_id: 'c', run_at: 'r', status: 'pending' } }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    await c.scheduleWakeup('c', '2026-01-01T00:00:00Z');
    await c.listWakeups({ status: 'pending' });
    await c.cancelWakeup('w1');
    expect(s.requests[0]!.url).toBe('/api/v1/scheduler/wakeups');
    expect(s.requests[1]!.url).toContain('status=pending');
    expect(s.requests[2]!.method).toBe('DELETE');
  });

  it('claimExecution / completeExecution with claim token', async () => {
    const s = await serve(() => ({ body: { status: 'claimed', claim_token: 'tok', execution: { id: 'x1', status: 'running' } } }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    const claim = await c.claimExecution('c', 'k1', 'tool', { a: 1 }, { dedupFields: ['a'] });
    expect(claim.claimToken).toBe('tok');
    await c.completeExecution('x1', 'tok', { ok: true });
    expect(s.requests[1]!.json).toEqual({ claim_token: 'tok', result: { ok: true } });
  });
});

describe('error mapping', () => {
  it('401 → AuthError', async () => {
    const s = await serve(() => ({ status: 401, body: { error: 'bad key' } }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    await expect(c.listChannels()).rejects.toBeInstanceOf(AuthError);
  });

  it('429 → RateLimitError with retry-after', async () => {
    const s = await serve(() => ({ status: 429, body: { retry_after_seconds: 90 } }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    await expect(c.listChannels()).rejects.toMatchObject({ retryAfterSeconds: 90 });
  });

  it('404 → NotFoundError', async () => {
    const s = await serve(() => ({ status: 404, body: { error: 'x' } }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    await expect(c.listChannels()).rejects.toBeInstanceOf(NotFoundError);
  });

  it('connection refused → ConnectionError', async () => {
    const c = new ActaeClient({ apiKey: 'k', endpoint: 'http://127.0.0.1:1', timeout: 1000 });
    await expect(c.listChannels()).rejects.toBeInstanceOf(ConnectionError);
  });

  it('non-json error body maps by status', async () => {
    const s = await serve(() => ({ status: 500, headers: { 'content-type': 'text/plain' }, body: 'boom' }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    await expect(c.listChannels()).rejects.toMatchObject({ statusCode: 500 });
  });
});

describe('facades', () => {
  it('client.events.record === client.record', async () => {
    const s = await serve(() => ({ body: { event: { id: 'e1', channel_id: 'ch', type: 't', cursor: 1 } } }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    const a = await c.events.record('ch', 't', {});
    const b = await c.record('ch', 't', {});
    expect(a.id).toBe('e1');
    expect(b.id).toBe('e1');
    expect(s.requests).toHaveLength(2);
  });

  it('client.health.check exists', async () => {
    const s = await serve(() => ({ body: { status: 'ok' } }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    const h = await c.health.check();
    expect(h.status).toBe('ok');
  });
});

describe('newClientFromEnv', () => {
  const oldEnv = { ...process.env };
  beforeEach(() => {
    process.env['ACTAE_API_KEY'] = 'env-key';
    process.env['ACTAE_URL'] = 'http://env-host:1234';
  });
  afterEach(() => {
    process.env = { ...oldEnv };
  });
  it('reads env vars and requires api key', async () => {
    const c = newClientFromEnv();
    expect(c.httpEndpoint).toBe('http://env-host:1234');
    expect(c.wsEndpoint).toBe('ws://env-host:1234/ws');
    delete process.env['ACTAE_API_KEY'];
    expect(() => newClientFromEnv()).toThrow(/ACTAE_API_KEY/);
  });
});
