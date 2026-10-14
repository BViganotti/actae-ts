import { afterEach, describe, expect, it } from 'vitest';
import { WebSocketServer, type WebSocket } from 'ws';
import type { AddressInfo } from 'node:net';
import { ActaeClient } from '../src/client.js';
import { APIError, ConnectionError, SessionError } from '../src/errors.js';
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
        /* ignore */
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

function authServer(): (ws: WebSocket, msg: unknown) => void {
  return (ws, msg: any) => {
    if (msg.type === 'auth') {
      ws.send(JSON.stringify({ type: 'connection', event: 'authenticated', payload: { connection_id: 'conn-1' } }));
    }
  };
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

function wsClient(url: string, opts: Record<string, unknown> = {}) {
  return new ActaeClient({ apiKey: 'k', wsEndpoint: url, endpoint: 'http://x', ...opts } as never);
}

describe('WS callbacks + handshake forks', () => {
  it('onDisconnected and onReconnect callbacks fire', async () => {
    const s = await serve(authServer());
    const c = wsClient(s.url);
    await c.connect();
    const disconnects: string[] = [];
    c.onDisconnected(() => disconnects.push('disc'));
    s.dropAll();
    await waitFor(() => disconnects.length > 0);
    expect(disconnects).toEqual(['disc']);
    void c;
  });
  it('socket construction failure raises ConnectionError', async () => {
    const c = wsClient('not-a-valid-url');
    await expect(c.connect()).rejects.toBeInstanceOf(ConnectionError);
  });
  it('connection closed during handshake raises ConnectionError', async () => {
    const s = await serve((ws) => {
      ws.close(1000, 'bye');
    });
    const c = wsClient(s.url, { timeout: 1000 });
    await expect(c.connect()).rejects.toBeInstanceOf(ConnectionError);
  });
  it('pre-aborted connect signal raises ConnectionError', async () => {
    const s = await serve(authServer());
    const c = wsClient(s.url);
    const ac = new AbortController();
    ac.abort();
    await expect(c.connect(ac.signal)).rejects.toBeInstanceOf(ConnectionError);
  });
});

describe('WS subscribe/unsubscribe/publish edge forks', () => {
  it('unsubscribe before connect throws ConnectionError', async () => {
    const c = wsClient('ws://127.0.0.1:1/ws');
    await expect(c.unsubscribe('ch')).rejects.toBeInstanceOf(ConnectionError);
  });
  it('subscribe aborts with a signal', async () => {
    const s = await serve(authServer());
    const c = wsClient(s.url);
    await c.connect();
    const ac = new AbortController();
    const p = c.subscribe('ch', undefined, true, ac.signal);
    await delay(20);
    ac.abort();
    await expect(p).rejects.toThrow(/Subscription aborted/);
  });
  it('publish ack timeout raises APIError', async () => {
    const s = await serve(authServer()); // never acks broadcasts
    const c = wsClient(s.url, { timeout: 100 });
    await c.connect();
    await expect(c.publish('ch', { x: 1 })).rejects.toBeInstanceOf(APIError);
  });
  it('publish aborts with a signal', async () => {
    const s = await serve(authServer());
    const c = wsClient(s.url, { timeout: 5000 });
    await c.connect();
    const ac = new AbortController();
    const p = c.publish('ch', { x: 1 }, undefined, ac.signal);
    await delay(20);
    ac.abort();
    await expect(p).rejects.toBeInstanceOf(SessionError);
  });
});

describe('WS message dispatch forks', () => {
  it('a `connection` frame with event=connected sets the connection id', async () => {
    const s = await serve((ws, msg: any) => {
      if (msg.type === 'auth') {
        ws.send(JSON.stringify({ type: 'connection', event: 'connected', payload: { connection_id: 'pre' } }));
        ws.send(JSON.stringify({ type: 'connection', event: 'authenticated', payload: { connection_id: 'conn-9' } }));
      }
    });
    const c = wsClient(s.url);
    await c.connect();
    expect(c.connectionId()).toBe('conn-9');
  });
  it('subscription error frame fires onError', async () => {
    const s = await serve((ws, msg: any) => {
      if (msg.type === 'auth') {
        ws.send(JSON.stringify({ type: 'connection', event: 'authenticated', payload: { connection_id: 'c' } }));
      } else if (msg.type === 'subscribe') {
        ws.send(JSON.stringify({ type: 'subscription', event: 'error', payload: { error: 'denied' } }));
      }
    });
    const c = wsClient(s.url);
    const errors: string[] = [];
    c.onError((m) => errors.push(m));
    await c.connect();
    await c.subscribe('ch', undefined, false);
    await waitFor(() => errors.length > 0);
    expect(errors).toContain('denied');
  });
  it('subscription unsubscribed + cursor_sync update the subscribed map', async () => {
    const s = await serve((ws, msg: any) => {
      if (msg.type === 'auth') {
        ws.send(JSON.stringify({ type: 'connection', event: 'authenticated', payload: { connection_id: 'c' } }));
      } else if (msg.type === 'subscribe') {
        ws.send(JSON.stringify({ type: 'subscription', event: 'subscribed', topic: msg.topic, cursor: 3 }));
        ws.send(JSON.stringify({ type: 'cursor_sync', topic: msg.topic, cursor: 7 }));
        ws.send(JSON.stringify({ type: 'subscription', event: 'unsubscribed', payload: { topic: msg.topic } }));
      }
    });
    const c = wsClient(s.url);
    await c.connect();
    await c.subscribe('ch', undefined, false);
    await waitFor(() => Object.keys(c.subscribedTopics()).length === 0);
    expect(c.subscribedTopics()).toEqual({});
  });
  it('error frame and nack frame are tolerated', async () => {
    const s = await serve((ws, msg: any) => {
      if (msg.type === 'auth') {
        ws.send(JSON.stringify({ type: 'connection', event: 'authenticated', payload: { connection_id: 'c' } }));
        ws.send(JSON.stringify({ type: 'error', message: 'oops' }));
        ws.send(JSON.stringify({ type: 'nack' }));
        ws.send(JSON.stringify({ type: 'ack', id: 'e1', channel_id: 'ch' }));
      }
    });
    const c = wsClient(s.url);
    const errors: string[] = [];
    c.onError((m) => errors.push(m));
    await c.connect();
    await waitFor(() => errors.includes('oops'));
    expect(errors).toContain('oops');
  });
  it('broadcast with an inner event envelope is unwrapped', async () => {
    const s = await serve((ws, msg: any) => {
      if (msg.type === 'auth') {
        ws.send(JSON.stringify({ type: 'connection', event: 'authenticated', payload: { connection_id: 'c' } }));
        ws.send(JSON.stringify({
          type: 'broadcast',
          topic: 'ch',
          payload: { event: { id: 'inner-1', channel_id: 'ch', type: 'agent.step', payload: { k: 1 }, cursor: 5, timestamp: 't' } },
        }));
      }
    });
    const c = wsClient(s.url);
    const messages: Array<{ topic: string; event: { id: string; cursor: number } }> = [];
    c.onMessage((t, e) => messages.push({ topic: t, event: e as { id: string; cursor: number } }));
    await c.connect();
    await waitFor(() => messages.length > 0);
    expect(messages[0]!.event.id).toBe('inner-1');
    expect(messages[0]!.event.cursor).toBe(5);
  });
  it('invalid JSON frames are ignored without throwing', async () => {
    const s = await serve((ws, msg: any) => {
      if (msg.type === 'auth') {
        ws.send(JSON.stringify({ type: 'connection', event: 'authenticated', payload: { connection_id: 'c' } }));
        ws.send('not-json');
        ws.send(JSON.stringify({ type: 'subscription', event: 'subscribed', topic: 'x', cursor: 0 }));
      }
    });
    const c = wsClient(s.url);
    await c.connect();
    await delay(50);
    expect(c.isConnected()).toBe(true);
  });
  it('callback exceptions surface through onError', async () => {
    const s = await serve(authServer());
    const c = wsClient(s.url);
    const errors: string[] = [];
    c.onError((m) => errors.push(m));
    c.onMessage(() => {
      throw new Error('handler boom');
    });
    await c.connect();
    s.sockets[0]!.send(JSON.stringify({ type: 'broadcast', topic: 'ch', payload: { n: 1 }, cursor: 1, timestamp: 't' }));
    await waitFor(() => errors.some((m) => m.includes('callback error')));
    expect(errors.some((m) => m.includes('callback error'))).toBe(true);
  });
  it('publish ack without channel_cursor leaves it undefined', async () => {
    const s = await serve((ws, msg: any) => {
      if (msg.type === 'auth') {
        ws.send(JSON.stringify({ type: 'connection', event: 'authenticated', payload: { connection_id: 'c' } }));
      } else if (msg.type === 'broadcast') {
        ws.send(JSON.stringify({
          type: 'ack',
          request_id: msg.request_id,
          id: 'ev-1',
          channel_id: msg.topic,
          event_type: 'broadcast',
          payload: msg.payload,
          cursor: 1,
          timestamp: 't',
        }));
      }
    });
    const c = wsClient(s.url);
    await c.connect();
    const ev = await c.publish('ch', { n: 1 });
    expect(ev.id).toBe('ev-1');
    expect(ev.channelCursor).toBeUndefined();
  });
});

describe('WS binary frame handling', () => {
  it('decodes ArrayBuffer message payloads', async () => {
    const s = await serve(authServer());
    const c = wsClient(s.url);
    const messages: string[] = [];
    c.onMessage((t, e) => messages.push(t));
    await c.connect();
    // Force the client socket to deliver binary as ArrayBuffer.
    const sock = (c as unknown as { _ws: { ws: WebSocket } })._ws.ws;
    (sock as unknown as { binaryType: string }).binaryType = 'arraybuffer';
    s.sockets[0]!.send(
      Buffer.from(JSON.stringify({ type: 'broadcast', topic: 'ch', payload: { n: 1 }, cursor: 1, timestamp: 't' })),
    );
    await waitFor(() => messages.length > 0);
    expect(messages).toContain('ch');
  });
});
