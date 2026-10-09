import { describe, expect, it } from 'vitest';
import { FleetClient, requireComplete } from '../src/fleet.js';
import type { SessionTransport } from '../src/session.js';

function fakeFetch(seen: Array<{ url: string; init?: RequestInit }>): typeof fetch {
  return (async (url: string | URL | Request, init?: RequestInit) => {
    seen.push({ url: String(url), init });
    if (String(url).endsWith('/api/v1/fleet/token')) {
      return new Response(JSON.stringify({ access_token: 'fleet-token' }), { status: 200 });
    }
    return new Response(JSON.stringify({ ok: true }), { status: 200 });
  }) as typeof fetch;
}

describe('FleetClient canonical transport', () => {
  it('uses canonical query, replay and state routes', async () => {
    const seen: Array<{ url: string; init?: RequestInit }> = [];
    const c = new FleetClient({ endpoint: 'https://fleet.example/', organizationToken: 'org', organizationId: 'org-1', fetch: fakeFetch(seen) });
    await c.query('inst-1', { limit: 10, channel_ids: ['run'] });
    await c.replay('inst-1', 'run', { cursor: 3 });
    await c.saveState('inst-1', 'run', 7, { x: 1 });
    await c.latestState('inst-1', 'run');
    expect(seen.map((x) => [x.init?.method, new URL(x.url).pathname])).toEqual([
      ['POST', '/api/v1/fleet/token'],
      ['POST', '/v1/organizations/org-1/events/query'],
      ['POST', '/v1/organizations/org-1/instances/inst-1/ops/actae.api.v1.events.replay'],
      ['PUT', '/v1/organizations/org-1/instances/inst-1/state/run'],
      ['POST', '/v1/organizations/org-1/instances/inst-1/ops/actae.api.v1.state.load'],
    ]);
    expect(seen[1].init?.body).toBe(JSON.stringify({ instances: ['inst-1'], query: { limit: 10, channel_ids: ['run'] } }));
    expect(seen[3].init?.body).toBe(JSON.stringify({ channel_id: 'run', cursor: 7, state: { x: 1 } }));
  });

  it('sends idempotency and confirmation headers', async () => {
    const seen: Array<{ url: string; init?: RequestInit }> = [];
    const c = new FleetClient({ endpoint: 'https://fleet.example', organizationToken: 'org', organizationId: 'org-1', fetch: fakeFetch(seen) });
    await c.call('DELETE', '/api/v1/channels/run', undefined, { instanceId: 'inst-1', idempotencyKey: 'op-1', confirmTarget: 'run' });
    const h = new Headers(seen.at(-1)?.init?.headers);
    expect(h.get('Idempotency-Key')).toBe('op-1');
    expect(h.get('X-Actae-Confirm-Target')).toBe('run');
    expect(h.get('X-Actae-Instance-Id')).toBe('inst-1');
  });

  it('refuses unconfirmed destructive commands before exchange', async () => {
    const c = new FleetClient({ endpoint: 'https://fleet.example', organizationToken: 'org', organizationId: 'org-1', fetch: fakeFetch([]) });
    await expect(c.call('DELETE', '/api/v1/channels/run')).rejects.toThrow('confirmTarget');
  });

  it('returns partial fleet results without hiding failures', async () => {
    const seen: Array<{ url: string; init?: RequestInit }> = [];
    const fetch = fakeFetch(seen);
    const c = new FleetClient({ endpoint: 'https://fleet.example', organizationToken: 'org', organizationId: 'org-1', fetch: (async (url, init) => {
      if (String(url).endsWith('/v1/organizations/org-1/events/query')) return new Response(JSON.stringify({ events: [{ id: 'ok' }], errors: [{ instance_id: 'down', code: 'unavailable', message: 'offline', retryable: true }] }));
      return fetch(url, init);
    }) as typeof globalThis.fetch });
    const result = await c.queryMany(['ok', 'down']);
    expect(result.errors[0].instanceId).toBe('down');
    expect(() => requireComplete(result)).toThrow('partial fleet result');
  });

  it('provides the shared session transport shape', () => {
    const c = new FleetClient({ endpoint: 'https://fleet.example', organizationToken: 'org', organizationId: 'org-1', fetch: fakeFetch([]) });
    const transport: SessionTransport = c.forInstance('inst-1');
    expect(transport).toBeDefined();
  });

  it('converts session fork options to the canonical wire shape', async () => {
    const seen: Array<{ url: string; init?: RequestInit }> = [];
    const c = new FleetClient({ endpoint: 'https://fleet.example', organizationToken: 'org', fetch: fakeFetch(seen) });
    await c.forInstance('inst-1').fork('parent', 'child', 4, { displayName: 'Child', operationId: 'op-1' });
    expect(seen.at(-1)?.init?.body).toBe(JSON.stringify({ source_channel_id: 'parent', new_channel_id: 'child', at_cursor: 4, display_name: 'Child', operation_id: 'op-1' }));
  });

  it('deduplicates fleet stream event keys', async () => {
    const lines = ['data: {"instance_id":"i-1","event_id":"e-1"}\n', 'data: {"instance_id":"i-1","event_id":"e-1"}\n', 'data: {"instance_id":"i-1","event_id":"e-2"}\n'];
    const stream = new ReadableStream<Uint8Array>({ start(controller) { const enc = new TextEncoder(); for (const line of lines) controller.enqueue(enc.encode(line)); controller.close(); } });
    const c = new FleetClient({ endpoint: 'https://fleet.example', organizationToken: 'org', fetch: (async (url) => {
      if (String(url).endsWith('/api/v1/fleet/token')) return new Response(JSON.stringify({ access_token: 'fleet' }));
      return new Response(stream, { status: 200 });
    }) as typeof globalThis.fetch });
    const values: unknown[] = []; for await (const value of c.stream({ instanceId: 'i-1' })) values.push(value);
    expect(values).toHaveLength(2);
  });

  it('requires a one-use ticket or provider for reconnectable streams', async () => {
    const c = new FleetClient({ endpoint: 'https://fleet.example', organizationToken: 'org', fetch: fakeFetch([]) });
    const iterator = c.streamWithReconnect({});
    await expect(iterator.next()).rejects.toThrow('ticket');
  });
});
