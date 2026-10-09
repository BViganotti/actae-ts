/**
 * WebSocket layer for Actae — subscribe/unsubscribe/publish with ack-based
 * publish confirmation, cursor tracking, auto-reconnect and keepalive.
 * Mirrors the Python SDK's `client_ws.py` and the Go SDK's `client_ws.go`.
 *
 * The public surface is exposed through the ActaeClient (which owns a
 * WebSocketConnection); this class implements the wire protocol and the
 * reconnect/callback machinery.
 */

import WebSocket from 'ws';

import {
  APIError,
  AuthError,
  ConnectionError,
  SessionError,
} from './errors.js';
import {
  asInt64,
  asMap,
  asString,
  parseJson,
  stringify,
  unwrapSonic,
  type JsonObject,
  type JsonValue,
} from './json.js';
import { validateChannelId, type ResolvedTLS } from './options.js';
import { eventFromBroadcast, type Event } from './types.js';
import { newUUID } from './uuid.js';

// ---------------------------------------------------------------------------
// Handler types (Python callback parity)
// ---------------------------------------------------------------------------

/** Receives (topic, event) for every broadcast on a subscribed topic. */
export type MessageHandler = (topic: string, event: Event) => void;

/** Receives a server-side error message string (subscription errors, error
 * frames). */
export type ErrorHandler = (message: string) => void;

/** Receives (topic, cursor) when a subscription is confirmed. cursor is the
 * topic cursor at subscription time (undefined when unknown). */
export type SubscribedHandler = (topic: string, cursor?: number) => void;

/** Fires when the WebSocket connection drops (before auto-reconnect). */
export type DisconnectedHandler = () => void;

/** Fires after auto-reconnect completes and all topics are resubscribed. */
export type ReconnectHandler = () => void;

export interface PublishOptions {
  /** Makes the publish idempotent: retrying with the same topic +
   * operation_id replays the original persisted event instead of
   * duplicating. */
  operationId?: string;
}

export interface WSEngineOptions {
  apiKey: string;
  wsEndpoint: string;
  timeout: number;
  maxReconnectFailures: number;
  keepAlive: number;
  tls?: ResolvedTLS;
  /** Mutable on the owning client — read live so disconnect() disables
   * reconnect. */
  autoReconnect: () => boolean;
  /** Mutable on the owning client. */
  echoSelf: () => boolean;
}

const WS_TYPE_MAP: Record<string, string> = {
  Connection: 'connection',
  Broadcast: 'broadcast',
  Subscription: 'subscription',
  Ack: 'ack',
  CursorSync: 'cursor_sync',
  Error: 'error',
  Nack: 'nack',
};

/** Bounds how long Subscribe(wait=true) waits for the server's confirmation
 * (mirrors the Python SDK's 10s). */
const SUBSCRIBE_ACK_TIMEOUT = 10_000;

/** Converts externally-tagged Actae messages ({"Broadcast": {...}}) into the
 * flat SDK format ({"type": "broadcast", ...}). */
export function normalizeMsg(data: JsonObject): JsonObject {
  for (const [key, typeName] of Object.entries(WS_TYPE_MAP)) {
    const inner = asMap(data[key]);
    if (inner) {
      return { type: typeName, ...inner };
    }
  }
  return data;
}

/** A single-stream subscription handle returned by `stream()`. */
export interface EventStream {
  [Symbol.asyncIterator](): AsyncIterator<Event>;
  /** Detaches the stream; the underlying topic subscription stays. */
  close(): void;
}

// ---------------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------------

export class WebSocketConnection {
  private readonly opts: WSEngineOptions;
  private ws?: WebSocket;
  private gen = 0;
  private connected = false;
  private authenticated = false;
  private connectionIdValue = '';
  private readonly subscribed = new Map<string, number>();
  private readonly subWaiters = new Map<string, () => void>();
  private authWaiters: Array<() => void> = [];
  private readonly pendingAcks = new Map<string, (msg: JsonObject) => void>();
  private readonly ackQueue: JsonObject[] = [];
  private readonly ackWaiters: Array<{ resolve: (m: JsonObject) => void }> = [];
  private reconnectFailures = 0;
  private keepAliveTimer?: NodeJS.Timeout;
  private writeChain: Promise<void> = Promise.resolve();
  private closed = false;

  // callbacks
  private readonly messageCbs: MessageHandler[] = [];
  private errorCb?: ErrorHandler;
  private subscribedCb?: SubscribedHandler;
  private disconnectedCb?: DisconnectedHandler;
  private reconnectCb?: ReconnectHandler;
  private readonly disconnectedInternal: Array<() => void> = [];

  constructor(opts: WSEngineOptions) {
    this.opts = opts;
  }

  // -------------------------------------------------------------------------
  // Callback registration
  // -------------------------------------------------------------------------

  /** Registers a broadcast callback. Callbacks accumulate; every registered
   * callback receives each broadcast (and EchoSelf events). */
  onMessage(cb: MessageHandler): void {
    this.messageCbs.push(cb);
  }

  /** Registers a broadcast callback and returns an unregister function. */
  addMessageHandler(cb: MessageHandler): () => void {
    const idx = this.messageCbs.length;
    this.messageCbs.push(cb);
    return () => {
      if (idx >= this.messageCbs.length) return;
      this.messageCbs.splice(idx, 1);
    };
  }

  /** Registers an internal disconnect callback (used by stream() to end its
   * channel) and returns an unregister function. */
  addDisconnectedHandler(cb: () => void): () => void {
    const idx = this.disconnectedInternal.length;
    this.disconnectedInternal.push(cb);
    return () => {
      if (idx >= this.disconnectedInternal.length) return;
      this.disconnectedInternal.splice(idx, 1);
    };
  }

  /** Registers the error callback (single; replaces previous). */
  onError(cb: ErrorHandler): void {
    this.errorCb = cb;
  }

  /** Registers the subscription-confirmation callback (single). */
  onSubscribed(cb: SubscribedHandler): void {
    this.subscribedCb = cb;
  }

  /** Registers the disconnection callback (single). */
  onDisconnected(cb: DisconnectedHandler): void {
    this.disconnectedCb = cb;
  }

  /** Registers the reconnection callback (single). */
  onReconnect(cb: ReconnectHandler): void {
    this.reconnectCb = cb;
  }

  // -------------------------------------------------------------------------
  // Connection lifecycle
  // -------------------------------------------------------------------------

  /** Opens the WebSocket connection and authenticates. Safe to call multiple
   * times — subsequent calls are no-ops while connected. Returns
   * ConnectionError when the handshake fails and AuthError when the API key
   * is rejected or authentication times out. */
  async connect(signal?: AbortSignal): Promise<void> {
    if (this.connected) return;
    this.closed = false;
    await this.doConnect(signal);
  }

  private doConnect(signal?: AbortSignal): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      const tls = this.opts.tls;
      let socket: WebSocket;
      try {
        socket = new WebSocket(this.opts.wsEndpoint, {
          handshakeTimeout: this.opts.timeout,
          rejectUnauthorized:
            tls?.rejectUnauthorized !== undefined ? tls.rejectUnauthorized : true,
          ca: tls?.ca,
          cert: tls?.cert,
          key: tls?.key,
        });
      } catch (err) {
        reject(new ConnectionError(`WebSocket connection failed: ${(err as Error).message}`));
        return;
      }

      let settled = false;
      const fail = (err: Error) => {
        if (settled) return;
        settled = true;
        try {
          socket.terminate();
        } catch {
          /* ignore */
        }
        reject(err);
      };
      const success = () => {
        if (settled) return;
        settled = true;
        resolve();
      };

      const cleanupHandlers = () => {
        socket.off('open', onOpen);
        socket.off('error', onSocketError);
        socket.off('close', onClose);
      };

      const onOpen = () => {
        this.installSocket(socket);
        this.gen++;
        const gen = this.gen;
        this.attachReader(socket, gen);
        const authSent = this.sendRaw(socket, { type: 'auth', api_key: this.opts.apiKey });
        if (!authSent) {
          fail(new ConnectionError('WebSocket connection failed: auth send failed'));
          return;
        }
        let timer: NodeJS.Timeout | undefined = setTimeout(() => {
          timer = undefined;
          cleanupHandlers();
          fail(new AuthError('Authentication timed out'));
        }, this.opts.timeout);
        const onAuth = () => {
          if (timer !== undefined) clearTimeout(timer);
          cleanupHandlers();
          success();
        };
        this.authWaiters.push(onAuth);
        if (this.authenticated) {
          this.authWaiters = this.authWaiters.filter((w) => w !== onAuth);
          onAuth();
        }
      };

      const onClose = () => {
        if (!settled) {
          cleanupHandlers();
          fail(new ConnectionError('WebSocket connection failed: closed during handshake'));
        }
      };

      const onSocketError = (err: Error) => {
        if (!settled) {
          cleanupHandlers();
          fail(new ConnectionError(`WebSocket connection failed: ${err.message}`));
        }
      };

      socket.on('open', onOpen);
      socket.on('error', onSocketError);
      socket.on('close', onClose);

      if (signal) {
        const onAbort = () => fail(new ConnectionError('WebSocket connection aborted'));
        if (signal.aborted) {
          onAbort();
        } else {
          signal.addEventListener('abort', onAbort, { once: true });
        }
      }
    });
  }

  /** Installs a newly-opened socket as the current connection, closing any
   * previous one. */
  private installSocket(socket: WebSocket): void {
    const old = this.ws;
    this.ws = socket;
    this.connected = false;
    this.authenticated = false;
    this.connectionIdValue = '';
    this.ackQueue.length = 0;
    this.pendingAcks.clear();
    if (old && old !== socket) {
      try {
        old.close();
      } catch {
        /* ignore */
      }
    }
  }

  /** Attaches message/close/error listeners to a socket, keyed by generation. */
  private attachReader(socket: WebSocket, gen: number): void {
    socket.on('message', (data: WebSocket.RawData) => {
      if (gen !== this.gen || this.ws !== socket) return;
      this.handleMessage(data);
    });
    socket.on('close', () => {
      if (gen === this.gen && this.ws === socket) {
        this.handleSocketClose(gen);
      }
    });
    socket.on('error', (err: Error) => {
      this.errorCb?.(err.message);
    });
  }

  /** Disconnects and stops auto-reconnect. Safe to call multiple times. */
  disconnect(): void {
    this.closed = true;
    this.teardown();
  }

  private teardown(): void {
    const socket = this.ws;
    this.ws = undefined;
    this.connected = false;
    this.authenticated = false;
    this.pendingAcks.clear();
    this.subWaiters.clear();
    if (this.keepAliveTimer) {
      clearInterval(this.keepAliveTimer);
      this.keepAliveTimer = undefined;
    }
    if (socket) {
      try {
        socket.close();
      } catch {
        /* ignore */
      }
    }
  }

  isConnected(): boolean {
    return this.connected;
  }

  isAuthenticated(): boolean {
    return this.authenticated;
  }

  connectionId(): string {
    return this.connectionIdValue;
  }

  /** Returns a copy of the topic → cursor map for active subscriptions. */
  subscribedTopics(): Record<string, number> {
    const out: Record<string, number> = {};
    for (const [t, c] of this.subscribed) out[t] = c;
    return out;
  }

  // -------------------------------------------------------------------------
  // Subscribe / publish
  // -------------------------------------------------------------------------

  /** Subscribes to a topic for real-time events. When cursor is set, events
   * from that cursor onward are replayed before live events. When wait is
   * true, blocks until the server confirms (up to 10s). */
  async subscribe(topic: string, cursor?: number, wait = true, signal?: AbortSignal): Promise<void> {
    validateChannelId(topic);
    if (!this.connected) {
      throw new ConnectionError('Not connected');
    }
    const msg: JsonObject = { type: 'subscribe', topic };
    if (cursor !== undefined) msg['cursor'] = cursor;

    if (!wait) {
      if (!this.send(msg)) throw new ConnectionError('Not connected');
      return;
    }

    await new Promise<void>((resolve, reject) => {
      const onConfirm = () => {
        clearTimeout(timer);
        if (this.subWaiters.get(topic) === onConfirm) this.subWaiters.delete(topic);
        resolve();
      };
      this.subWaiters.set(topic, onConfirm);
      const timer = setTimeout(() => {
        if (this.subWaiters.get(topic) === onConfirm) this.subWaiters.delete(topic);
        reject(
          new ConnectionError(`Subscription confirmation for ${topic} timed out`),
        );
      }, SUBSCRIBE_ACK_TIMEOUT);
      const onAbort = () => {
        if (this.subWaiters.get(topic) === onConfirm) this.subWaiters.delete(topic);
        clearTimeout(timer);
        reject(new ConnectionError('Subscription aborted'));
      };
      if (signal) {
        if (signal.aborted) {
          onAbort();
          return;
        }
        signal.addEventListener('abort', onAbort, { once: true });
      }
      if (!this.send(msg)) {
        if (this.subWaiters.get(topic) === onConfirm) this.subWaiters.delete(topic);
        clearTimeout(timer);
        reject(new ConnectionError('Not connected'));
      }
    });
  }

  /** Unsubscribes from a topic. */
  async unsubscribe(topic: string): Promise<void> {
    if (!this.connected) {
      throw new ConnectionError('Not connected');
    }
    this.send({ type: 'unsubscribe', topic });
  }

  /** Publishes an event to a topic over WebSocket. The server persists the
   * event and sends back an Ack carrying the full event; returns that
   * persisted Event without an extra HTTP round-trip.
   *
   * The server does not echo a broadcast back to the publishing connection;
   * with echoSelf set, the returned event is also delivered to local
   * OnMessage callbacks. */
  async publish(
    topic: string,
    payload: JsonValue,
    opts?: PublishOptions,
    signal?: AbortSignal,
  ): Promise<Event> {
    validateChannelId(topic);
    if (!this.connected) {
      throw new ConnectionError('Not connected');
    }

    const requestId = newUUID();
    const msg: JsonObject = {
      type: 'broadcast',
      topic,
      payload: payload as JsonValue,
      request_id: requestId,
    };
    if (opts?.operationId !== undefined) msg['operation_id'] = opts.operationId;

    const ack = await new Promise<JsonObject>((resolve, reject) => {
      this.pendingAcks.set(requestId, resolve);
      if (!this.send(msg)) {
        this.pendingAcks.delete(requestId);
        reject(new ConnectionError('Not connected'));
        return;
      }
      const timer = setTimeout(() => {
        this.pendingAcks.delete(requestId);
        reject(new APIError(500, 'No Ack received from server after publish'));
      }, this.opts.timeout);
      const onAbort = () => {
        clearTimeout(timer);
        this.pendingAcks.delete(requestId);
        reject(new SessionError('publish aborted'));
      };
      if (signal) {
        if (signal.aborted) {
          onAbort();
        } else {
          signal.addEventListener('abort', onAbort, { once: true });
        }
      }
    });

    const event = eventFromAck(ack, topic, payload);
    if (this.opts.echoSelf()) {
      this.fireMessage(topic, event);
    }
    return event;
  }

  /** Streams live events for a topic (Python SDK `stream()` parity). When
   * cursor is set, events with cursor strictly greater than it are replayed
   * before live events. The stream ends when the WebSocket disconnects. */
  stream(topic: string, cursor?: number, signal?: AbortSignal): EventStream {
    const unsubMsg = this.addMessageHandler((t, event) => {
      if (t !== topic) return;
      queue.enqueue(event);
    });
    let unsubDisc: (() => void) | undefined;
    let closed = false;
    let startError: unknown;
    let startResolve!: () => void;
    const started = new Promise<void>((resolve) => {
      startResolve = resolve;
    });

    const queue = createEventQueue();

    const closeStream = () => {
      if (closed) return;
      closed = true;
      unsubMsg();
      unsubDisc?.();
      queue.close();
    };
    unsubDisc = this.addDisconnectedHandler(closeStream);

    const init = (async () => {
      try {
        await this.subscribe(topic, cursor, true, signal);
        startResolve();
      } catch (err) {
        startError = err;
        closeStream();
        startResolve();
      }
    })();

    void init;
    return {
      [Symbol.asyncIterator]() {
        return (async function* () {
          await started;
          if (startError !== undefined) throw startError;
          while (!closed) {
            const item = await queue.next();
            if (item.done) break;
            yield item.value as Event;
          }
        })();
      },
      close() {
        closeStream();
      },
    };
  }

  // -------------------------------------------------------------------------
  // Reader and dispatch
  // -------------------------------------------------------------------------

  private handleMessage(data: WebSocket.RawData): void {
    let text: string;
    if (typeof data === 'string') {
      text = data;
    } else if (data instanceof ArrayBuffer) {
      text = Buffer.from(data).toString('utf8');
    } else if (Array.isArray(data)) {
      text = Buffer.concat(
        data.map((d) => (Buffer.isBuffer(d) ? d : Buffer.from(d))),
      ).toString('utf8');
    } else if (Buffer.isBuffer(data)) {
      text = data.toString('utf8');
    } else {
      return;
    }
    let msg: JsonObject;
    try {
      const parsed = parseJson(text);
      const m = asMap(parsed);
      if (!m) return;
      msg = m;
    } catch {
      return;
    }
    this.dispatch(normalizeMsg(msg));
  }

  private handleSocketClose(gen: number): void {
    const wasConnected = this.connected;
    this.connected = false;
    this.authenticated = false;
    this.ws = undefined;
    this.pendingAcks.clear();
    if (this.keepAliveTimer) {
      clearInterval(this.keepAliveTimer);
      this.keepAliveTimer = undefined;
    }

    if (wasConnected) {
      const onDisc = this.disconnectedCb;
      const internals = [...this.disconnectedInternal];
      onDisc?.();
      for (const cb of internals) this.safeCall(cb);
      if (this.opts.autoReconnect() && !this.closed) {
        void this.reconnectLoop();
      }
    }
  }

  private dispatch(data: JsonObject): void {
    const type = asString(data['type']);
    switch (type) {
      case 'connection': {
        const event = asString(data['event']);
        const payload = asMap(data['payload']) ?? {};
        if (event === 'authenticated') {
          this.authenticated = true;
          const id = asString(payload['connection_id']);
          if (id !== '') this.connectionIdValue = id;
          this.connected = true;
          const waiters = this.authWaiters;
          this.authWaiters = [];
          for (const w of waiters) this.safeCall(w);
          this.startKeepAlive();
        } else if (event === 'connected') {
          this.connectionIdValue = asString(payload['connection_id']);
        }
        break;
      }
      case 'broadcast': {
        const topic = asString(data['topic']);
        const event = this.eventFromBroadcastMsg(data);
        this.fireMessage(topic, event);
        break;
      }
      case 'subscription': {
        const event = asString(data['event']);
        const payload = asMap(data['payload']) ?? {};
        if (event === 'subscribed') {
          let topic = asString(data['topic']);
          if (topic === '') topic = asString(payload['topic']);
          const cursorRaw = data['cursor'] ?? payload['cursor'];
          const cursor =
            cursorRaw !== undefined && cursorRaw !== null ? asInt64(cursorRaw) : undefined;
          if (cursor !== undefined) this.subscribed.set(topic, cursor);
          else this.subscribed.set(topic, 0);
          const waiter = this.subWaiters.get(topic);
          if (waiter) this.safeCall(waiter);
          this.subscribedCb?.(topic, cursor);
        } else if (event === 'unsubscribed') {
          let topic = asString(payload['topic']);
          if (topic === '') topic = asString(data['topic']);
          this.subscribed.delete(topic);
        } else if (event === 'error') {
          let msg = asString(payload['error']);
          if (msg === '') msg = 'Subscription failed';
          this.fireError(msg);
        }
        break;
      }
      case 'cursor_sync': {
        const topic = asString(data['topic']);
        this.subscribed.set(topic, asInt64(data['cursor']));
        break;
      }
      case 'ack': {
        const rid = asString(data['request_id']);
        if (rid !== '') {
          const waiter = this.pendingAcks.get(rid);
          this.pendingAcks.delete(rid);
          if (waiter) this.safeCall(() => waiter(data));
          return;
        }
        const pending = this.ackWaiters.shift();
        if (pending) {
          this.safeCall(() => pending.resolve(data));
        } else {
          this.ackQueue.push(data);
        }
        break;
      }
      case 'error': {
        let msg = asString(data['message']);
        if (msg === '') msg = 'Unknown WebSocket error';
        this.fireError(msg);
        break;
      }
      case 'nack':
        break;
    }
  }

  private eventFromBroadcastMsg(data: JsonObject): Event {
    const inner = asMap(data['payload']);
    if (inner) {
      const innerEvent = asMap(inner['event']);
      if (innerEvent) return eventFromBroadcast(innerEvent);
    }
    return eventFromBroadcast({
      id: '',
      channel_id: asString(data['topic']),
      payload: data['payload'] as JsonValue,
      type: 'broadcast',
      actor: asString(data['actor']),
      cursor: asInt64(data['cursor']),
      timestamp: asString(data['timestamp']),
    });
  }

  private fireMessage(topic: string, event: Event): void {
    const cbs = [...this.messageCbs];
    for (const cb of cbs) {
      this.safeCall(() => cb(topic, event));
    }
  }

  private fireError(msg: string): void {
    this.errorCb?.(msg);
  }

  private safeCall(fn: () => void): void {
    try {
      fn();
    } catch (err) {
      this.errorCb?.(`callback error: ${(err as Error).message}`);
    }
  }

  // -------------------------------------------------------------------------
  // Reconnection and keepalive
  // -------------------------------------------------------------------------

  private async reconnectLoop(): Promise<void> {
    let delay = 500;
    const maxDelay = 30_000;
    for (;;) {
      await sleep(delay);
      if (this.connected || !this.opts.autoReconnect() || this.closed) return;

      try {
        await this.doConnect();
      } catch {
        this.reconnectFailures++;
        if (this.reconnectFailures >= this.opts.maxReconnectFailures) {
          this.closed = true;
          return;
        }
        delay = Math.min(delay * 2, maxDelay);
        continue;
      }

      const topics = new Map(this.subscribed);
      for (const [topic, cur] of topics) {
        try {
          await this.subscribe(topic, cur > 0 ? cur : undefined, true);
        } catch {
          /* log-and-continue */
        }
      }
      this.reconnectFailures = 0;
      this.reconnectCb?.();
      return;
    }
  }

  private startKeepAlive(): void {
    const interval = this.opts.keepAlive;
    if (interval <= 0) return;
    this.keepAliveTimer = setInterval(() => {
      this.send({ type: 'connection', event: 'ping', payload: {} });
    }, interval);
  }

  // -------------------------------------------------------------------------
  // Send helpers
  // -------------------------------------------------------------------------

  private send(msg: JsonObject): boolean {
    const socket = this.ws;
    if (!socket || socket.readyState !== WebSocket.OPEN) return false;
    this.writeChain = this.writeChain.then(() => {
      if (socket.readyState === WebSocket.OPEN) {
        socket.send(stringify(msg));
      }
    });
    return true;
  }

  private sendRaw(socket: WebSocket, msg: JsonObject): boolean {
    if (socket.readyState !== WebSocket.OPEN) return false;
    socket.send(stringify(msg));
    return true;
  }
}

function eventFromAck(m: JsonObject, topic: string, payload: JsonValue): Event {
  const channelId = m['channel_id'] !== undefined ? asString(m['channel_id']) : topic;
  const eventType = m['event_type'] !== undefined ? asString(m['event_type']) : 'broadcast';
  const ackPayload = m['payload'] !== undefined ? unwrapSonic(m['payload']) : payload;
  return {
    id: asString(m['id']),
    channelId,
    eventType,
    payload: ackPayload,
    actor: asString(m['actor']),
    cursor: asInt64(m['cursor']),
    channelCursor:
      m['channel_cursor'] !== undefined && m['channel_cursor'] !== null
        ? asInt64(m['channel_cursor'])
        : undefined,
    timestamp: asString(m['timestamp']),
  };
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ---------------------------------------------------------------------------
// Event queue for stream()
// ---------------------------------------------------------------------------

interface QueueItem {
  value?: Event;
  done: boolean;
}

function createEventQueue() {
  const buffer: Event[] = [];
  let closed = false;
  const waiters: Array<(item: QueueItem) => void> = [];

  return {
    enqueue(event: Event): void {
      if (closed) return;
      const waiter = waiters.shift();
      if (waiter) {
        waiter({ value: event, done: false });
      } else {
        buffer.push(event);
      }
    },
    close(): void {
      if (closed) return;
      closed = true;
      for (const w of waiters.splice(0)) w({ done: true });
    },
    async next(): Promise<QueueItem> {
      if (buffer.length > 0) {
        return { value: buffer.shift(), done: false };
      }
      if (closed) return { done: true };
      return new Promise<QueueItem>((resolve) => {
        waiters.push(resolve);
        if (closed) {
          const idx = waiters.indexOf(resolve);
          if (idx >= 0) waiters.splice(idx, 1);
          resolve({ done: true });
        }
      });
    },
  };
}
