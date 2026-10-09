import { afterEach, describe, expect, it } from 'vitest';
import { WebSocketServer, type WebSocket } from 'ws';
import type { AddressInfo } from 'node:net';
import { ActaeClient } from '../src/client.js';
import { waitFor } from './helpers.js';

/**
 * Concurrency stress: parallel publishes with correlated acks, concurrent
 * subscribes, and high-throughput message delivery — the Go `-race`-style
 * verification for the WS layer (ack correlation, waiter maps, reader
 * dispatch under interleaving).
 */

interface WsServer {
  url: string;
  sockets: WebSocket[];
  close: () => Promise<void>;
}

let servers: WsServer[] = [];
afterEach(async () => {
  await Promise.all(servers.map((s) => s.close()));
  servers = [];
});

/** Server that authenticates, confirms subscriptions, acks publishes with a
 * slight random delay so acks interleave. */
async function serve(): Promise<WsServer> {
  const wss = new WebSocketServer({ port: 0 });
  const sockets: WebSocket[] = [];
  let cursor = 0;
  wss.on('connection', (ws) => {
    sockets.push(ws);
    ws.on('message', (data) => {
      let msg: any;
      try {
        msg = JSON.parse(data.toString());
      } catch {
        return;
      }
      if (msg.type === 'auth') {
        ws.send(JSON.stringify({ type: 'connection', event: 'authenticated', payload: { connection_id: 'c' } }));
      } else if (msg.type === 'subscribe') {
        ws.send(JSON.stringify({ type: 'subscription', event: 'subscribed', topic: msg.topic, cursor: 0 }));
      } else if (msg.type === 'unsubscribe') {
        ws.send(JSON.stringify({ type: 'subscription', event: 'unsubscribed', topic: msg.topic }));
      } else if (msg.type === 'broadcast') {
        // Deliberately out-of-order ack timing so acks interleave across
        // concurrent publishes.
        const myCursor = ++cursor;
        const delay = Math.random() * 20;
        setTimeout(() => {
          ws.send(JSON.stringify({
            type: 'ack',
            request_id: msg.request_id,
            id: `ev-${myCursor}`,
            channel_id: msg.topic,
            event_type: 'broadcast',
            payload: msg.payload,
            cursor: myCursor,
            channel_cursor: myCursor,
            timestamp: new Date().toISOString(),
          }));
        }, delay);
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
    url: `ws://127.0.0.1:${port}/ws`,
    sockets,
    close: () =>
      new Promise((resolve) => {
        for (const s of [...sockets]) s.terminate();
        wss.close(() => resolve());
      }),
  };
}

describe('WebSocket concurrency', () => {
  it('correlates N concurrent publishes to the correct ack (no cross-waiter mixup)', async () => {
    const srv = await serve();
    const c = new ActaeClient({ apiKey: 'k', endpoint: 'http://x', wsEndpoint: srv.url });
    await c.connect();

    const N = 50;
    const results = await Promise.all(
      Array.from({ length: N }, (_, i) => c.publish(`ch-${i % 3}`, { idx: i })),
    );
    // Every publish must resolve to ITS OWN event (cursor = its own).
    for (let i = 0; i < N; i++) {
      expect(results[i]!.id).toBe(`ev-${i + 1}`);
      expect(results[i]!.payload).toEqual({ idx: i });
    }
    c.disconnect();
  });

  it('concurrent subscribe/unsubscribe across topics stays consistent', async () => {
    const srv = await serve();
    const c = new ActaeClient({ apiKey: 'k', endpoint: 'http://x', wsEndpoint: srv.url });
    await c.connect();

    const topics = Array.from({ length: 10 }, (_, i) => `t${i}`);
    await Promise.all(topics.map((t) => c.subscribe(t)));
    await waitFor(() => Object.keys(c.subscribedTopics()).length === 10);
    expect(Object.keys(c.subscribedTopics()).sort()).toEqual([...topics].sort());

    await Promise.all(topics.map((t) => c.unsubscribe(t)));
    await waitFor(() => Object.keys(c.subscribedTopics()).length === 0);
    expect(c.subscribedTopics()).toEqual({});
    c.disconnect();
  });

  it('sustains 200 concurrent publishes without dropped acks', async () => {
    const srv = await serve();
    const c = new ActaeClient({ apiKey: 'k', endpoint: 'http://x', wsEndpoint: srv.url });
    await c.connect();

    const N = 200;
    const results = await Promise.allSettled(
      Array.from({ length: N }, (_, i) => c.publish('load', { i })),
    );
    const fulfilled = results.filter((r) => r.status === 'fulfilled');
    expect(fulfilled.length).toBe(N); // none rejected, none timed out
    c.disconnect();
  }, 20000);

  it('concurrent onMessage callbacks + publishes never cross-deliver', async () => {
    const srv = await serve();
    const c = new ActaeClient({ apiKey: 'k', endpoint: 'http://x', wsEndpoint: srv.url, echoSelf: true });
    await c.connect();
    const received: Array<{ idx: number; payload: number }> = [];
    c.onMessage((topic, ev) => {
      received.push({ idx: ev.cursor, payload: (ev.payload as { i: number }).i });
    });
    await Promise.all(
      Array.from({ length: 30 }, (_, i) => c.publish('echo', { i })),
    );
    await waitFor(() => received.length >= 30, 5000);
    // Each echoed event must pair cursor ↔ payload consistently.
    for (const r of received) {
      expect(r.payload).toBe(r.idx - 1);
    }
    c.disconnect();
  });
});
