import { afterEach, describe, expect, it } from 'vitest';
import { WebSocketServer, type WebSocket } from 'ws';
import type { AddressInfo } from 'node:net';
import { ActaeClient } from '../src/client.js';
import { delay, waitFor } from './helpers.js';

interface WsServer {
  wss: WebSocketServer;
  url: string;
  sockets: WebSocket[];
  close: () => Promise<void>;
}

let servers: WsServer[] = [];
async function serve(fn: (ws: WebSocket, msg: unknown) => void): Promise<WsServer> {
  const wss = new WebSocketServer({ port: 0 });
  const sockets: WebSocket[] = [];
  wss.on('connection', (ws) => {
    sockets.push(ws);
    ws.on('message', (data) => {
      try {
        fn(ws, JSON.parse(data.toString()));
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
    close: () =>
      new Promise((resolve) => {
        for (const s of [...sockets]) s.terminate();
        wss.close(() => resolve());
      }),
  };
}
afterEach(async () => {
  await Promise.all(servers.map((s) => s.close()));
  servers = [];
});

describe('WebSocket keepalive + error frames', () => {
  it('sends JSON ping frames on the keepalive interval', async () => {
    const pings: unknown[] = [];
    const srv = await serve((ws, msg: any) => {
      if (msg.type === 'auth') {
        ws.send(JSON.stringify({ type: 'connection', event: 'authenticated' }));
      } else if (msg.type === 'connection' && msg.event === 'ping') {
        pings.push(msg);
      }
    });
    const c = new ActaeClient({ apiKey: 'k', endpoint: 'http://x', wsEndpoint: srv.url, keepAliveInterval: 50 });
    await c.connect();
    await waitFor(() => pings.length >= 1, 3000);
    expect(pings.length).toBeGreaterThanOrEqual(1);
    c.disconnect();
  });

  it('disabling keepalive with a negative interval sends no pings', async () => {
    let pings = 0;
    const srv = await serve((ws, msg: any) => {
      if (msg.type === 'auth') ws.send(JSON.stringify({ type: 'connection', event: 'authenticated' }));
      else if (msg.type === 'connection' && msg.event === 'ping') pings++;
    });
    const c = new ActaeClient({ apiKey: 'k', endpoint: 'http://x', wsEndpoint: srv.url, keepAliveInterval: -1 });
    await c.connect();
    await delay(150);
    expect(pings).toBe(0);
    c.disconnect();
  });

  it('error frames fire the onError callback', async () => {
    const srv = await serve((ws, msg: any) => {
      if (msg.type === 'auth') {
        ws.send(JSON.stringify({ type: 'connection', event: 'authenticated' }));
        ws.send(JSON.stringify({ type: 'error', message: 'boom' }));
      }
    });
    const c = new ActaeClient({ apiKey: 'k', endpoint: 'http://x', wsEndpoint: srv.url });
    const errors: string[] = [];
    c.onError((m) => errors.push(m));
    await c.connect();
    await waitFor(() => errors.length >= 1);
    expect(errors[0]).toBe('boom');
    c.disconnect();
  });

  it('externally-tagged error frame normalizes to a flat error', async () => {
    const srv = await serve((ws, msg: any) => {
      if (msg.type === 'auth') {
        ws.send(JSON.stringify({ type: 'connection', event: 'authenticated' }));
        ws.send(JSON.stringify({ Error: { message: 'tagged boom' } }));
      }
    });
    const c = new ActaeClient({ apiKey: 'k', endpoint: 'http://x', wsEndpoint: srv.url });
    const errors: string[] = [];
    c.onError((m) => errors.push(m));
    await c.connect();
    await waitFor(() => errors.length >= 1);
    expect(errors[0]).toBe('tagged boom');
    c.disconnect();
  });

  it('client WS delegations report state', async () => {
    const srv = await serve((ws, msg: any) => {
      if (msg.type === 'auth') {
        ws.send(JSON.stringify({ type: 'connection', event: 'authenticated', payload: { connection_id: 'conn-42' } }));
      } else if (msg.type === 'subscribe') {
        ws.send(JSON.stringify({ type: 'subscription', event: 'subscribed', topic: msg.topic, cursor: 7 }));
      }
    });
    const c = new ActaeClient({ apiKey: 'k', endpoint: 'http://x', wsEndpoint: srv.url });
    expect(c.isConnected()).toBe(false);
    expect(c.isAuthenticated()).toBe(false);
    expect(c.connectionId()).toBe('');
    await c.connect();
    expect(c.isConnected()).toBe(true);
    expect(c.isAuthenticated()).toBe(true);
    expect(c.connectionId()).toBe('conn-42');
    await c.subscribeAndWait('ch');
    expect(c.subscribedTopics()['ch']).toBe(7);
    c.disconnect();
  });
});
