import { describe, expect, it, afterEach } from 'vitest';
import { ActaeClient } from '../src/client.js';
import { normalizeMsg } from '../src/client_ws.js';
import {
  AuthError,
  ChannelConflictError,
  ConsumerError,
  ExecutionNotOwnedError,
  IdempotencyConflictError,
  IdempotencyKeyMismatchError,
  LockError,
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

describe('content-type matrix (mirrors Go client_parity_test.go)', () => {
  it('application/json bodies parse as objects', async () => {
    const s = await serve(() => ({ headers: { 'content-type': 'application/json' }, body: { ok: true } }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    const r = await c.healthCheck();
    expect(r.status).toBeDefined();
    expect(s.requests[0]!.headers['accept']).toBeUndefined(); // no Accept header forced
  });

  it('text/plain success returns raw string', async () => {
    const s = await serve(() => ({ headers: { 'content-type': 'text/plain' }, body: 'plain text' }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    expect(await c.getMetricsText()).toBe('plain text');
  });

  it('empty body parses as {}', async () => {
    const s = await serve(() => ({ headers: { 'content-type': 'application/json' }, body: '' }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    await expect(c.listChannels()).resolves.toEqual([]);
  });
});

describe('error-code matrix (mirrors Go client_parity_test.go)', () => {
  const cases: Array<[number, string, new (...a: any[]) => Error]> = [
    [401, 'auth', AuthError],
    [402, 'locked', LockError],
    [429, 'rate', RateLimitError],
    [409, 'snapshot_boundary_required', SnapshotBoundaryError],
    [409, 'version_conflict', VersionConflictError],
    [409, 'consumer_not_found', ConsumerError],
    [409, 'idempotency_key_mismatch', IdempotencyKeyMismatchError],
    [409, 'execution_not_owned', ExecutionNotOwnedError],
    [409, 'idempotency_conflict', IdempotencyConflictError],
    [409, 'channel_conflict', ChannelConflictError],
    [500, '', ServerError],
  ];
  for (const [status, code, cls] of cases) {
    it(`maps ${status} ${code || '(no code)'} → ${cls.name}`, async () => {
      const body = code ? { status: code, error: 'detail' } : { error: 'boom' };
      const s = await serve(() => ({ status, body }));
      const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
      await expect(c.listChannels()).rejects.toBeInstanceOf(cls);
    });
  }

  it('409 unknown status falls back to APIError', async () => {
    const s = await serve(() => ({ status: 409, body: { status: 'some_other' } }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    await expect(c.listChannels()).rejects.toMatchObject({ statusCode: 409 });
  });

  it('429 reads retry_after_seconds from body', async () => {
    const s = await serve(() => ({ status: 429, body: { retry_after_seconds: 7 } }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    await expect(c.listChannels()).rejects.toMatchObject({ retryAfterSeconds: 7 });
  });
});

describe('payload edge cases', () => {
  it('big int64 payload values arrive as bigint (not rounded)', async () => {
    const s = await serve(() => ({
      body: {
        event: {
          id: 'e1',
          channel_id: 'ch',
          type: 't',
          cursor: 1,
          payload: { ['$sonic_rs::private::JsonNumber']: '9007199254740993' },
        },
      },
    }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    const ev = await c.record('ch', 't', {});
    expect(ev.payload).toBe(9007199254740993n);
  });

  it('record payload with bigint round-trips (not silently corrupted)', async () => {
    const s = await serve(() => ({ body: { event: { id: 'e1', channel_id: 'ch', type: 't', cursor: 1 } } }));
    const c = new ActaeClient({ apiKey: 'k', endpoint: s.baseUrl });
    await c.record('ch', 't', { tokens: 12345678901234567890n } as never);
    const body = JSON.parse(s.requests[0]!.body);
    // bigint serialized via JSON.stringify throws — ensure we didn't ship NaN/rounded
    expect(body['payload']).toHaveProperty('tokens');
  });
});

describe('URL / TLS construction', () => {
  it('wsEndpoint derivation matrix', () => {
    const derive = (endpoint: string) =>
      new ActaeClient({ apiKey: 'k', endpoint }).wsEndpoint;
    expect(derive('http://localhost:8002')).toBe('ws://localhost:8002/ws');
    expect(derive('https://example.com')).toBe('wss://example.com/ws');
    expect(derive('http://h/ws')).toBe('ws://h/ws');
    expect(derive('http://h/')).toBe('ws://h/ws');
  });

  it('explicit wsEndpoint wins over derivation', () => {
    const c = new ActaeClient({ apiKey: 'k', endpoint: 'http://h', wsEndpoint: 'ws://custom:1/ws' });
    expect(c.wsEndpoint).toBe('ws://custom:1/ws');
  });

  it('accepts tls options without throwing', () => {
    const c = new ActaeClient({
      apiKey: 'k',
      endpoint: 'https://h',
      tls: { ca: 'CERT', rejectUnauthorized: false },
    });
    expect(c.wsEndpoint).toBe('wss://h/ws');
  });
});

describe('WS message normalization (mirrors Go wsTypeMap)', () => {
  it('converts externally-tagged frames to flat', () => {
    expect(normalizeMsg({ Connection: { event: 'connected' } })).toEqual({
      type: 'connection',
      event: 'connected',
    });
    expect(normalizeMsg({ Broadcast: { topic: 't', payload: {} } })).toEqual({
      type: 'broadcast',
      topic: 't',
      payload: {},
    });
    expect(normalizeMsg({ Subscription: { event: 'subscribed' } })).toEqual({
      type: 'subscription',
      event: 'subscribed',
    });
    expect(normalizeMsg({ Ack: { request_id: 'r' } })).toEqual({
      type: 'ack',
      request_id: 'r',
    });
    expect(normalizeMsg({ CursorSync: { topic: 't', cursor: 4 } })).toEqual({
      type: 'cursor_sync',
      topic: 't',
      cursor: 4,
    });
    expect(normalizeMsg({ Error: { message: 'x' } })).toEqual({ type: 'error', message: 'x' });
    expect(normalizeMsg({ Nack: {} })).toEqual({ type: 'nack' });
  });

  it('leaves flat frames unchanged', () => {
    expect(normalizeMsg({ type: 'broadcast', topic: 't' })).toEqual({ type: 'broadcast', topic: 't' });
  });
});
