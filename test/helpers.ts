import * as http from 'node:http';
import * as os from 'node:os';
import * as path from 'node:path';
import * as fs from 'node:fs';
import type { AddressInfo } from 'node:net';

export interface RequestRecord {
  method: string;
  url: string;
  headers: http.IncomingHttpHeaders;
  body: string;
  json: unknown;
}

export type Router = (
  req: http.IncomingMessage,
  body: unknown,
  record: RequestRecord,
) => { status?: number; headers?: Record<string, string>; body?: unknown };

/** A configurable HTTP test server that records requests and routes
 * responses. Returns { server, baseUrl, requests, close }. */
export function createTestServer(router: Router) {
  const requests: RequestRecord[] = [];
  const server = http.createServer((req, res) => {
    const chunks: Buffer[] = [];
    req.on('data', (c: Buffer) => chunks.push(c));
    req.on('end', () => {
      const body = Buffer.concat(chunks).toString('utf8');
      let json: unknown;
      try {
        json = body === '' ? null : JSON.parse(body);
      } catch {
        json = null;
      }
      const record: RequestRecord = { method: req.method ?? '', url: req.url ?? '', headers: req.headers, body, json };
      requests.push(record);
      const result = router(req, json, record);
      const status = result.status ?? 200;
      const headers = result.headers ?? { 'content-type': 'application/json' };
      let payload = result.body;
      let raw = '';
      if (typeof payload === 'string') {
        raw = payload;
      } else if (payload !== undefined) {
        raw = JSON.stringify(payload);
      }
      res.writeHead(status, headers);
      res.end(raw);
    });
  });

  return new Promise<{
    server: http.Server;
    baseUrl: string;
    requests: RequestRecord[];
    close: () => Promise<void>;
  }>((resolve) => {
    server.listen(0, '127.0.0.1', () => {
      const port = (server.address() as AddressInfo).port;
      resolve({
        server,
        baseUrl: `http://127.0.0.1:${port}`,
        requests,
        close: () => new Promise((r) => server.close(() => r())),
      });
    });
  });
}

/** Creates a temporary directory (for TLS cert path tests). */
export function tempDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'actae-test-'));
}

export function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export async function waitFor(fn: () => boolean, timeoutMs = 5000): Promise<void> {
  const start = Date.now();
  while (!fn()) {
    if (Date.now() - start > timeoutMs) {
      throw new Error(`waitFor timed out after ${timeoutMs}ms`);
    }
    await delay(10);
  }
}

// ---------------------------------------------------------------------------
// Fake ActaeClient for session / state / adapter tests
// ---------------------------------------------------------------------------

export interface FakeChannel {
  events: Array<Record<string, unknown>>;
  states: Array<{ version: number; cursor: number; state: Record<string, unknown> }>;
  metadata: Record<string, unknown>;
  parent?: string;
}

let versionCounter = 1;
let cursorCounter = 1;

/** An in-memory ActaeClient double implementing the surface used by
 * AgentSession, StateManager and the adapters. */
export class FakeActaeClient {
  channels = new Map<string, FakeChannel>();
  forks: Array<{ source: string; child: string; atCursor: number }> = [];
  /** Fork options keyed by child channel (for intervention/tool-policy tests). */
  forkOptions: Record<string, Record<string, unknown>> = {};
  recordedEvents: Array<{ channelId: string; eventType: string; payload: unknown; opts: Record<string, unknown> }> = [];
  forkErrors: Record<string, Error | undefined> = {};
  replayEvents = new Map<string, Array<Record<string, unknown>>>();
  resolveStepResult?: { channelId: string; cursor: number; stepNumber: number } | undefined;

  constructor() {
    cursorCounter = 1;
    versionCounter = 1;
  }

  private channel(id: string): FakeChannel {
    let ch = this.channels.get(id);
    if (!ch) {
      ch = { events: [], states: [], metadata: {} };
      this.channels.set(id, ch);
    }
    return ch;
  }

  async record(
    channelId: string,
    eventType: string,
    payload: unknown,
    opts: Record<string, unknown> = {},
  ): Promise<{ id: string; channelId: string; eventType: string; payload: unknown; cursor: number; timestamp: string; actor: string; metadata?: Record<string, unknown>; agentId?: string; userId?: string }> {
    const ch = this.channel(channelId);
    const cursor = cursorCounter++;
    const ev = {
      id: `ev-${cursor}`,
      channelId,
      eventType,
      payload,
      cursor,
      timestamp: new Date().toISOString(),
      actor: (opts.actor as string) ?? '',
      metadata: opts.metadata as Record<string, unknown> | undefined,
      agentId: opts.agentId as string | undefined,
      userId: opts.userId as string | undefined,
    };
    ch.events.push({ ...ev });
    this.recordedEvents.push({ channelId, eventType, payload, opts });
    return ev;
  }

  async saveState(channelId: string, cursor: number, state: Record<string, unknown>): Promise<number> {
    const ch = this.channel(channelId);
    const version = versionCounter++;
    ch.states.push({ version, cursor, state: JSON.parse(JSON.stringify(state)) });
    return version;
  }

  async transition(
    channelId: string,
    eventType: string,
    payload: unknown,
    state: Record<string, unknown>,
    opts: Record<string, unknown> = {},
  ): Promise<{ event: Record<string, unknown>; stateVersion: number }> {
    const ev = await this.record(channelId, eventType, payload, opts);
    await this.saveState(channelId, ev.cursor, state);
    return { event: ev as unknown as Record<string, unknown>, stateVersion: 1 };
  }

  async latestState(channelId: string): Promise<{ cursor: number; version: number; timestamp: string; state: Record<string, unknown> } | undefined> {
    const ch = this.channels.get(channelId);
    if (!ch || ch.states.length === 0) return undefined;
    const s = ch.states[ch.states.length - 1]!;
    return { cursor: s.cursor, version: s.version, timestamp: 't', state: JSON.parse(JSON.stringify(s.state)) };
  }

  async listStates(channelId: string, _opts?: Record<string, unknown>): Promise<Array<{ version: number; cursor: number; timestamp: string }>> {
    const ch = this.channels.get(channelId);
    if (!ch) return [];
    return [...ch.states].reverse().map((s) => ({ version: s.version, cursor: s.cursor, timestamp: 't' }));
  }

  async getState(channelId: string, version: number): Promise<{ cursor: number; version: number; timestamp: string; state: Record<string, unknown> } | undefined> {
    const ch = this.channels.get(channelId);
    const s = ch?.states.find((x) => x.version === version);
    if (!s) return undefined;
    return { cursor: s.cursor, version: s.version, timestamp: 't', state: JSON.parse(JSON.stringify(s.state)) };
  }

  async deleteState(channelId: string, version: number): Promise<void> {
    const ch = this.channels.get(channelId);
    if (!ch) return;
    ch.states = ch.states.filter((s) => s.version !== version);
  }

  async latestCursor(channelId: string): Promise<number | undefined> {
    const ch = this.channels.get(channelId);
    const evs = ch?.events ?? [];
    return evs.length > 0 ? (evs[evs.length - 1]!.cursor as number) : undefined;
  }

  async getChannelMetadata(channelId: string): Promise<Record<string, unknown> | undefined> {
    const ch = this.channels.get(channelId);
    if (!ch) return undefined;
    return {
      channelId,
      parentChannelId: ch.parent,
      originRunId: '',
      forkedAtCursor: 0,
      forkedAt: '',
      requestedAtCursor: 0,
      resolvedStateCursor: 0,
      resolvedCursor: 0,
      sourceStateVersion: 0,
      restorable: false,
      experimentMetadata: { ...(ch.metadata as Record<string, unknown>) },
      displayName: (ch.metadata['session_name'] as string) ?? channelId,
    };
  }

  async updateMetadata(channelId: string, opts: Record<string, unknown>): Promise<Record<string, unknown>> {
    const ch = this.channel(channelId);
    const merged = { ...ch.metadata, ...((opts.experimentMetadata as Record<string, unknown>) ?? {}) };
    ch.metadata = merged;
    ch.metadata['display_name'] = opts.displayName ?? channelId;
    return ch.metadata as Record<string, unknown>;
  }

  async fork(source: string, child: string, atCursor: number, opts: Record<string, unknown> = {}): Promise<Record<string, unknown>> {
    const err = this.forkErrors[child];
    // The real server rejects at_cursor>0 without a boundary, but at_cursor=0
    // (latest) always works.
    if (err && atCursor > 0) throw err;
    const src = this.channels.get(source);
    this.forks.push({ source, child, atCursor });
    this.forkOptions[child] = opts;
    const ch = this.channel(child);
    ch.parent = source;
    ch.metadata = {
      ...((opts.experimentMetadata as Record<string, unknown>) ?? {}),
      forked_from: source,
      forked_at_cursor: atCursor,
    };
    // Copy the latest state snapshot into the child (server behavior).
    let resolvedCursor = atCursor;
    if (src && src.states.length > 0) {
      // Mirror the real server: the fallback (at_cursor=0) and normal forks
      // resolve to the nearest saved snapshot at-or-before the requested
      // cursor; a "latest" request resolves to the newest snapshot's cursor.
      let pick = src.states[0]!;
      for (const st of src.states) {
        if (atCursor <= 0 || st.cursor <= atCursor) pick = st;
        else break;
      }
      resolvedCursor = pick.cursor;
      ch.states.push({ version: versionCounter++, cursor: pick.cursor, state: JSON.parse(JSON.stringify(pick.state)) });
    }
    return {
      forkId: child,
      sourceChannelId: source,
      childChannelId: child,
      requestedCursor: atCursor,
      resolvedCursor,
      sourceStateVersion: src && src.states.length > 0 ? src.states[src.states.length - 1]!.version : 0,
      sourceStateSha256: 'sha',
      restorable: src ? src.states.length > 0 : false,
      replayed: false,
      manifest: (opts.manifest as Record<string, unknown>) ?? {},
      reproducibility: 'state_exact',
      toolPolicies: (opts.toolPolicies as Record<string, unknown>) ?? undefined,
    };
  }

  async resolveStep(channelId: string, stepNumber: number): Promise<Record<string, unknown> | undefined> {
    return this.resolveStepResult;
  }

  async latestStepNumber(channelId: string): Promise<number> {
    const ch = this.channels.get(channelId);
    let last = 0;
    for (const event of ch?.events ?? []) {
      const payload = (event as { payload?: unknown }).payload;
      if (payload && typeof payload === 'object' && !Array.isArray(payload)) {
        const raw = (payload as Record<string, unknown>)['step_number'];
        if (typeof raw === 'number' && raw > last) last = raw;
        else if (typeof raw === 'bigint' && Number(raw) > last) last = Number(raw);
      }
    }
    return last;
  }

  async replay(channelId: string, _opts?: Record<string, unknown>): Promise<Array<Record<string, unknown>>> {
    const custom = this.replayEvents.get(channelId);
    if (custom) return custom.map((e) => ({ ...e }));
    const ch = this.channels.get(channelId);
    return (ch?.events ?? []).map((e) => ({ ...e }));
  }

  async setOutcome(_channelId: string, _outcome: string, _score?: number): Promise<Record<string, unknown>> {
    return { ok: true };
  }

  async promoteChannel(_channelId: string): Promise<Record<string, unknown>> {
    return { promoted: true };
  }

  async createExperiment(name: string): Promise<Record<string, unknown>> {
    return { id: `exp-${name}`, name };
  }

  async addExperimentMember(_groupId: string, channelId: string): Promise<Record<string, unknown>> {
    return { channel_id: channelId };
  }
}
