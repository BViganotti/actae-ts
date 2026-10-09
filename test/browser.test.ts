import { describe, expect, it } from 'vitest';
import { WebSocket, WebSocketServer } from 'ws';
import { fleetStream } from '../src/browser.js';

function withBrowserWebSocket(run: () => Promise<void>): Promise<void> {
  const prev = (globalThis as any).WebSocket;
  (globalThis as any).WebSocket = WebSocket;
  return run().finally(() => {
    if (prev === undefined) delete (globalThis as any).WebSocket;
    else (globalThis as any).WebSocket = prev;
  });
}

function startServer(): Promise<{ url: string; clients: () => any[]; close: () => Promise<void> }> {
  return new Promise((resolve) => {
    const wss = new WebSocketServer({ port: 0 }, () => {
      const addr = wss.address() as any;
      resolve({
        url: `ws://127.0.0.1:${addr.port}`,
        clients: () => [...(wss.clients as any)],
        close: () => new Promise((r) => wss.close(() => r(undefined))),
      });
    });
  });
}

// Wait for the server to actually accept the client instead of sleeping a
// fixed 30ms: under full-suite load the handshake can take longer than a
// fixed sleep, and sending on an undefined client silently no-ops (the
// generator then never yields and the test hangs until its timeout).
async function waitForClient(server: { clients: () => any[] }): Promise<any> {
  const deadline = Date.now() + 5000;
  for (;;) {
    const client = server.clients()[0];
    if (client) return client;
    if (Date.now() > deadline) throw new Error('fleet stream client never connected');
    await new Promise((r) => setTimeout(r, 5));
  }
}

describe('browser fleet stream (PR-019)', () => {
  it('delivers frames and ends on server close (one-shot)', async () => {
    await withBrowserWebSocket(async () => {
      const server = await startServer();
      const got: any[] = [];
      const gen = fleetStream({ url: server.url + '/v1/organizations/o/events/stream', ticket: 'tkt-1' });
      const waiter = (async () => {
        for await (const item of gen) got.push(item);
      })();
      const client = await waitForClient(server);
      client.send(JSON.stringify({ type: 'broadcast', topic: 'ch', payload: { event: { id: 'e1', cursor: 5 } } }));
      await new Promise((r) => setTimeout(r, 30));
      client.close();
      await waiter;
      await server.close();
      expect(got).toHaveLength(1);
      expect(got[0].payload.event.id).toBe('e1');
    });
  });

  it('surfaces a lag error on queue overflow instead of buffering unbounded', async () => {
    await withBrowserWebSocket(async () => {
      const server = await startServer();
      const gen = fleetStream({ url: server.url + '/stream', ticket: 'tkt-2', maxQueueFrames: 2 });
      // Establish the connection by requesting the first frame (pending).
      const first = gen.next();
      const client = await waitForClient(server);
      // While the consumer is NOT draining, flood the socket: the bounded
      // queue (cap 2) overflows and the stream surfaces a typed lag error.
      client?.send(JSON.stringify({ n: 1 }));
      client?.send(JSON.stringify({ n: 2 }));
      client?.send(JSON.stringify({ n: 3 }));
      client?.send(JSON.stringify({ n: 4 }));
      let gotLag = false;
      try {
        await first;
        for (let i = 0; i < 8; i++) {
          const r = await gen.next();
          if (r.done) break;
        }
      } catch (e) {
        if (String(e).includes('lag')) gotLag = true;
        else throw e;
      }
      expect(gotLag).toBe(true);
      for (const c of server.clients()) c.close();
      await new Promise((r) => setTimeout(r, 20));
      await server.close();
    });
  });
});