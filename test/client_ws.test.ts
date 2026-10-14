import { afterEach, describe, expect, it } from 'vitest';
import { WebSocketServer, type WebSocket } from 'ws';
import type { AddressInfo } from 'node:net';
import { ActaeClient } from '../src/client.js';
import { AuthError, ConnectionError } from '../src/errors.js';
import { delay, waitFor } from './helpers.js';

interface WsServer {
  wss: WebSocketServer;
  url: string;
  sockets: WebSocket[];
  close: () => Promise<void>;
  dropAll: () => void;
}

async function createWsServer(onMessage: (ws: WebSocket, msg: unknown) => void): Promise<WsServer> {
  const wss = new WebSocketServer({ port: 0 });
  const sockets: WebSocket[] = [];
  wss.on('connection', (ws) => {
    sockets.push(ws);
    ws.on('message', (data) => {
      try {
        onMessage(ws, JSON.parse(data.toString()));
      } catch {
        /* ignore malformed test frames */
      }
    });
    ws.on('close', () => {
      const idx = sockets.indexOf(ws);
      if (idx >= 0) sockets.splice(idx, 1);
    });
  });
  await new Promise<void>((resolve) => wss.once('listening', resolve));
  const port = (wss.address() as AddressInfo).port;
  return {
    wss,
    url: `ws://127.0.0.1:${port}/ws`,
    sockets,
    dropAll: () => {
      for (const s of [...sockets]) s.close();
    },
    close: () =>
      new Promise((resolve) => {
        for (const s of [...sockets]) s.terminate();
        wss.close(() => resolve());
      }),
  };
}

/** Standard Actae test server behavior: authenticate, confirm subscriptions,
 * ack broadcasts. */
function standardServer(opts: { connectionId?: string } = {}) {
  let nextCursor = 1;
  return createWsServer((ws, msg: any) => {
    switch (msg.type) {
      case 'auth':
        ws.send(JSON.stringify({
          type: 'connection',
          event: 'authenticated',
          payload: { connection_id: opts.connectionId ?? 'conn-1' },
        }));
        break;
      case 'subscribe':
        ws.send(JSON.stringify({
          type: 'subscription',
          event: 'subscribed',
          topic: msg.topic,
          cursor: msg.cursor ?? 0,
        }));
        break;
      case 'unsubscribe':
        ws.send(JSON.stringify({
          type: 'subscription',
          event: 'unsubscribed',
          topic: msg.topic,
        }));
        break;
      case 'broadcast':
        ws.send(JSON.stringify({
          type: 'ack',
          request_id: msg.request_id,
          operation_id: msg.operation_id,
          id: 'ev-' + nextCursor,
          channel_id: msg.topic,
          event_type: 'broadcast',
          payload: msg.payload,
          cursor: nextCursor,
          channel_cursor: nextCursor,
          timestamp: new Date().toISOString(),
        }));
        nextCursor++;
        break;
    }
  });
}

let servers: WsServer[] = [];
async function serve(fn: (ws: WebSocket, msg: unknown) => void) {
  const s = await createWsServer(fn);
  servers.push(s);
  return s;
}
afterEach(async () => {
  await Promise.all(servers.map((s) => s.close()));
  servers = [];
});

describe('WebSocket connect/auth', () => {
  it('connects, authenticates, and exposes connection id', async () => {
    const srv = await serve((ws, msg: any) => {
      if (msg.type === 'auth') {
        ws.send(JSON.stringify({ type: 'connection', event: 'authenticated', payload: { connection_id: 'conn-9' } }));
      }
    });
    const c = new ActaeClient({ apiKey: 'k', endpoint: 'http://x', wsEndpoint: srv.url });
    await c.connect();
    expect(c.isConnected()).toBe(true);
    expect(c.isAuthenticated()).toBe(true);
    expect(c.connectionId()).toBe('conn-9');
    c.disconnect();
  });

  it('is a no-op when already connected', async () => {
    const srv = await serve((ws, msg: any) => {
      if (msg.type === 'auth') ws.send(JSON.stringify({ type: 'connection', event: 'authenticated' }));
    });
    const c = new ActaeClient({ apiKey: 'k', endpoint: 'http://x', wsEndpoint: srv.url });
    await c.connect();
    await c.connect(); // no second connection
    expect(srv.sockets).toHaveLength(1);
    c.disconnect();
  });

  it('throws AuthError when auth times out', async () => {
    const srv = await serve(() => {});
    const c = new ActaeClient({ apiKey: 'k', endpoint: 'http://x', wsEndpoint: srv.url, timeout: 200 });
    await expect(c.connect()).rejects.toBeInstanceOf(AuthError);
  });

  it('throws ConnectionError for unreachable host', async () => {
    const c = new ActaeClient({ apiKey: 'k', endpoint: 'http://x', wsEndpoint: 'ws://127.0.0.1:1/ws', timeout: 500 });
    await expect(c.connect()).rejects.toBeInstanceOf(ConnectionError);
  });
});

describe('subscribe / unsubscribe', () => {
  it('subscribe(wait=true) resolves on confirmation and fires onSubscribed', async () => {
    const srv = await standardServer();
    const c = new ActaeClient({ apiKey: 'k', endpoint: 'http://x', wsEndpoint: srv.url });
    await c.connect();
    let cbTopic: string | undefined;
    let cbCursor: number | undefined;
    c.onSubscribed((t, cur) => {
      cbTopic = t;
      cbCursor = cur;
    });
    await c.subscribe('ch', 3, true);
    expect(c.subscribedTopics()['ch']).toBe(3);
    expect(cbTopic).toBe('ch');
    expect(cbCursor).toBe(3);
    c.disconnect();
  });

  it('subscribe(wait=false) returns immediately', async () => {
    const srv = await standardServer();
    const c = new ActaeClient({ apiKey: 'k', endpoint: 'http://x', wsEndpoint: srv.url });
    await c.connect();
    await c.subscribe('ch', undefined, false);
    expect(c.isConnected()).toBe(true);
    c.disconnect();
  });

  it('unsubscribe removes topic from subscribed map', async () => {
    const srv = await standardServer();
    const c = new ActaeClient({ apiKey: 'k', endpoint: 'http://x', wsEndpoint: srv.url });
    await c.connect();
    await c.subscribe('ch');
    await c.unsubscribe('ch');
    await waitFor(() => c.subscribedTopics()['ch'] === undefined);
    c.disconnect();
  });

  it('subscribe throws ConnectionError when not connected', async () => {
    const srv = await standardServer();
    const c = new ActaeClient({ apiKey: 'k', endpoint: 'http://x', wsEndpoint: srv.url });
    await expect(c.subscribe('ch')).rejects.toBeInstanceOf(ConnectionError);
  });
});

describe('broadcast delivery', () => {
  it('delivers broadcasts to accumulated onMessage callbacks', async () => {
    const srv = await serve((ws, msg: any) => {
      if (msg.type === 'auth') ws.send(JSON.stringify({ type: 'connection', event: 'authenticated' }));
      else if (msg.type === 'subscribe') {
        ws.send(JSON.stringify({ type: 'subscription', event: 'subscribed', topic: msg.topic }));
        // deliver a broadcast frame: event nested inside payload (wire format)
        ws.send(JSON.stringify({
          type: 'broadcast',
          topic: msg.topic,
          payload: {
            event: {
              id: 'b1',
              channel_id: msg.topic,
              type: 'broadcast',
              payload: { value: 42 },
              cursor: 5,
              timestamp: 't',
            },
          },
        }));
      }
    });
    const c = new ActaeClient({ apiKey: 'k', endpoint: 'http://x', wsEndpoint: srv.url });
    await c.connect();
    const received: Array<[string, any]> = [];
    c.onMessage((topic, event) => received.push([topic, event]));
    c.onMessage((topic, event) => received.push([topic, event]));
    await c.subscribe('ch');
    await waitFor(() => received.length === 2);
    expect(received[0]).toEqual(['ch', expect.objectContaining({ id: 'b1', cursor: 5, payload: { value: 42 } })]);
    c.disconnect();
  });
});

describe('publish', () => {
  it('returns the persisted event from the ack', async () => {
    const srv = await standardServer();
    const c = new ActaeClient({ apiKey: 'k', endpoint: 'http://x', wsEndpoint: srv.url });
    await c.connect();
    const ev = await c.publish('ch', { hello: 'world' }, { operationId: 'op-1' });
    expect(ev.id).toBe('ev-1');
    expect(ev.cursor).toBe(1);
    expect(ev.channelId).toBe('ch');
    expect(ev.payload).toEqual({ hello: 'world' });
    c.disconnect();
  });

  it('delivers own publish locally when echoSelf is set', async () => {
    const srv = await standardServer();
    const c = new ActaeClient({ apiKey: 'k', endpoint: 'http://x', wsEndpoint: srv.url, echoSelf: true });
    await c.connect();
    const received: any[] = [];
    c.onMessage((topic, event) => received.push([topic, event]));
    const ev = await c.publish('ch', { x: 1 });
    await waitFor(() => received.length === 1);
    expect(received[0]![0]).toBe('ch');
    expect(received[0]![1].id).toBe(ev.id);
    c.disconnect();
  });

  it('throws ConnectionError when not connected', async () => {
    const srv = await standardServer();
    const c = new ActaeClient({ apiKey: 'k', endpoint: 'http://x', wsEndpoint: srv.url });
    await expect(c.publish('ch', {})).rejects.toBeInstanceOf(ConnectionError);
  });
});

describe('stream', () => {
  it('yields live events and ends on disconnect', async () => {
    const srv = await serve((ws, msg: any) => {
      if (msg.type === 'auth') ws.send(JSON.stringify({ type: 'connection', event: 'authenticated' }));
      else if (msg.type === 'subscribe') {
        ws.send(JSON.stringify({ type: 'subscription', event: 'subscribed', topic: msg.topic }));
        ws.send(JSON.stringify({
          type: 'broadcast',
          topic: msg.topic,
          payload: {
            event: { id: 's1', channel_id: msg.topic, type: 'broadcast', payload: { n: 1 }, cursor: 1, timestamp: 't' },
          },
        }));
      }
    });
    const c = new ActaeClient({ apiKey: 'k', endpoint: 'http://x', wsEndpoint: srv.url });
    await c.connect();
    const events: any[] = [];
    const stream = c.stream('ch');
    const iterator = stream[Symbol.asyncIterator]();
    (async () => {
      for (;;) {
        const { value, done } = await iterator.next();
        if (done) break;
        events.push(value);
      }
    })();
    await waitFor(() => events.length >= 1);
    expect(events[0]).toMatchObject({ id: 's1', payload: { n: 1 } });
    srv.dropAll();
    // stream should close when disconnected
    await waitFor(() => events.length === 1 && c.isConnected() === false, 3000);
    c.disconnect();
  });
});

describe('reconnect', () => {
  it('reconnects and resubscribes after a drop', async () => {
    let authCount = 0;
    const srv = await serve((ws, msg: any) => {
      if (msg.type === 'auth') {
        authCount++;
        ws.send(JSON.stringify({ type: 'connection', event: 'authenticated', payload: { connection_id: `conn-${authCount}` } }));
      } else if (msg.type === 'subscribe') {
        ws.send(JSON.stringify({ type: 'subscription', event: 'subscribed', topic: msg.topic, cursor: 3 }));
      }
    });
    const c = new ActaeClient({
      apiKey: 'k',
      endpoint: 'http://x',
      wsEndpoint: srv.url,
      keepAliveInterval: -1,
    });
    await c.connect();
    await c.subscribe('ch', 3, true);
    expect(c.subscribedTopics()['ch']).toBe(3);
    let reconnected = false;
    c.onReconnect(() => {
      reconnected = true;
    });
    srv.dropAll();
    await waitFor(() => authCount >= 2, 8000);
    await waitFor(() => c.isConnected() && reconnected, 8000);
    expect(c.subscribedTopics()['ch']).toBe(3);
    c.disconnect();
  });

  it('stops reconnecting after disconnect()', async () => {
    let authCount = 0;
    const srv = await serve((ws, msg: any) => {
      if (msg.type === 'auth') {
        authCount++;
        ws.send(JSON.stringify({ type: 'connection', event: 'authenticated' }));
      }
    });
    const c = new ActaeClient({ apiKey: 'k', endpoint: 'http://x', wsEndpoint: srv.url, keepAliveInterval: -1 });
    await c.connect();
    c.disconnect();
    srv.dropAll();
    await delay(300);
    expect(authCount).toBe(1);
  });
});
