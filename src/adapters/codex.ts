/**
 * Codex CLI observability adapter for Actae.
 *
 * Ports the Python adapter (`actae_client.adapters.codex`). Codex (the
 * open-source OpenAI coding agent) can export OpenTelemetry log events
 * describing each run to an OTLP endpoint. This adapter hosts that endpoint
 * and forwards the `codex.*` events into Actae channels keyed by
 * `conversation.id`, persisting a per-conversation state snapshot so the
 * dashboard, structural state-diff and decision-trail work on Codex sessions
 * exactly like on any other channel.
 *
 * ```ts
 * import { CodexOTLPReceiver } from '@actae/sdk/adapters/codex';
 * const receiver = new CodexOTLPReceiver(actae, { port: 4319 });
 * await receiver.start();
 * // ... run `codex` with the [otel] config in ~/.codex/config.toml ...
 * await receiver.stop();
 * ```
 *
 * Codex's rollout JSONL remains the source of truth for local resume; this is
 * a read-only observability mirror and never modifies Codex state.
 */

import * as http from 'node:http';
import type { AddressInfo } from 'node:net';

import type { ActaeClient } from '../client.js';
import type { JsonObject, JsonValue } from '../json.js';
import { adapterCheckpointState } from './contract.js';

const ACTOR = 'codex-cli';

/** Deterministic Actae channel for a Codex conversation id. */
export function channelForConversation(conversationId: string): string {
  return `codex:${conversationId}`;
}

/** Converts an OTLP key/value array into a plain dict. */
export function kvToDict(attrs: unknown): JsonObject {
  const out: JsonObject = {};
  if (!Array.isArray(attrs)) return out;
  for (const kv of attrs) {
    if (!kv || typeof kv !== 'object' || Array.isArray(kv)) continue;
    const entry = kv as Record<string, unknown>;
    const key = entry['key'];
    if (typeof key !== 'string') continue;
    const value = entry['value'];
    if (value && typeof value === 'object' && !Array.isArray(value)) {
      const oneof = value as Record<string, unknown>;
      for (const vk of ['stringValue', 'intValue', 'doubleValue', 'boolValue'] as const) {
        if (vk in oneof) {
          out[key] = oneof[vk] as JsonValue;
          break;
        }
      }
      if (!(key in out)) out[key] = value as JsonValue;
    } else {
      out[key] = value as JsonValue;
    }
  }
  return out;
}

export interface CodexOTLPReceiverOptions {
  /** Bind host (default 127.0.0.1). */
  host?: string;
  /** Bind port (default 4319). 0 = ephemeral (tests). */
  port?: number;
}

interface CodexEvent {
  name: string;
  ts?: string;
  [key: string]: unknown;
}

/** An OTLP/HTTP log receiver that mirrors Codex CLI events into Actae.
 *
 * Listens for Codex's OTLP log export (`codex.*` events on `POST /v1/logs`)
 * and records them to per-conversation Actae channels. State snapshots
 * accumulate token usage and tool calls so Actae's diff / trail / dashboard
 * work on Codex sessions. */
export class CodexOTLPReceiver {
  readonly actae: ActaeClient;
  readonly host: string;
  readonly port: number;

  private server?: http.Server;
  private portBound?: number;

  constructor(actae: ActaeClient, options: CodexOTLPReceiverOptions = {}) {
    this.actae = actae;
    this.host = options.host ?? '127.0.0.1';
    this.port = options.port ?? 4319;
  }

  /** The actually-bound port (matches `port` after start unless 0/ephemeral). */
  get boundPort(): number | undefined {
    return this.portBound;
  }

  /** The full OTLP logs URL the Codex exporter should target. */
  get logsUrl(): string {
    return `http://${this.host}:${this.portBound ?? this.port}/v1/logs`;
  }

  /** Starts the OTLP receiver (binds host:port and serves /v1/logs). */
  start(): Promise<void> {
    return new Promise((resolve, reject) => {
      const server = http.createServer((req, res) => {
        if (req.method === 'POST' && (req.url === '/v1/logs' || req.url?.startsWith('/v1/logs'))) {
          void this.handleLogs(req, res);
          return;
        }
        if (req.method === 'GET' && (req.url === '/health' || req.url === '/healthz')) {
          res.writeHead(200, { 'content-type': 'application/json' });
          res.end(JSON.stringify({ status: 'ok' }));
          return;
        }
        res.writeHead(404, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ error: 'not found' }));
      });
      server.on('error', reject);
      server.listen(this.port, this.host, () => {
        this.server = server;
        this.portBound = (server.address() as AddressInfo).port;
        resolve();
      });
    });
  }

  /** Stops the receiver and releases the port. */
  stop(): Promise<void> {
    return new Promise((resolve) => {
      const server = this.server;
      this.server = undefined;
      this.portBound = undefined;
      if (!server) {
        resolve();
        return;
      }
      server.close(() => resolve());
    });
  }

  private handleLogs(req: http.IncomingMessage, res: http.ServerResponse): void {
    const chunks: Buffer[] = [];
    req.on('data', (c: Buffer) => chunks.push(c));
    req.on('end', () => {
      const raw = Buffer.concat(chunks).toString('utf8');
      let body: unknown;
      try {
        body = JSON.parse(raw);
      } catch {
        res.writeHead(400, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ error: 'invalid JSON' }));
        return;
      }
      void (async () => {
        try {
          const count = await this.processLogs(body);
          res.writeHead(200, { 'content-type': 'application/json' });
          res.end(JSON.stringify({ processed: count }));
        } catch (err) {
          res.writeHead(500, { 'content-type': 'application/json' });
          res.end(JSON.stringify({ error: (err as Error).message }));
        }
      })();
    });
  }

  /** Parses an OTLP/HTTP JSON log export and mirrors codex.* events. */
  async processLogs(body: unknown): Promise<number> {
    let count = 0;
    if (!body || typeof body !== 'object' || Array.isArray(body)) return 0;
    const root = body as JsonObject;
    const resourceLogs = Array.isArray(root['resourceLogs']) ? (root['resourceLogs'] as unknown[]) : [];
    for (const rl of resourceLogs) {
      const scopeLogs = asObjArray(rl, 'scopeLogs');
      for (const sl of scopeLogs) {
        const logRecords = asObjArray(sl, 'logRecords');
        for (const lr of logRecords) {
          const event = this.extractEvent(lr);
          if (!event) continue;
          await this.mirrorEvent(event);
          count++;
        }
      }
    }
    return count;
  }

  /** Extracts a codex.* event from one OTLP log record, or undefined. */
  extractEvent(logRecord: JsonObject): CodexEvent | undefined {
    const attrs = kvToDict(logRecord['attributes']);
    const body = logRecord['body'];
    let eventName = '';
    if (body && typeof body === 'object' && !Array.isArray(body)) {
      eventName = (body as JsonObject)['stringValue'] as string;
    } else if (typeof body === 'string') {
      eventName = body;
    }
    if (typeof eventName !== 'string' || !eventName.startsWith('codex.')) return undefined;
    const event: CodexEvent = {
      name: eventName,
      ...attrs,
    };
    if (logRecord['timeUnixNano'] !== undefined) {
      event['ts'] = String(logRecord['timeUnixNano']);
    }
    return event;
  }

  /** Records one codex.* event to its conversation channel. */
  async mirrorEvent(event: CodexEvent): Promise<void> {
    const conversationId = String(event['conversation.id'] ?? event['conversation_id'] ?? 'unknown');
    const channel = channelForConversation(conversationId);
    const name = event['name'];
    const payload: JsonObject = {};
    for (const [k, v] of Object.entries(event)) {
      if (k !== 'name' && k !== 'ts') payload[k] = v as JsonValue;
    }

    try {
      await this.actae.record(channel, name, payload as JsonValue, { actor: ACTOR });
    } catch {
      return; // best-effort mirror — never break the receiver
    }

    const snapshot = await this.currentSnapshot(channel);
    const tokens = (snapshot['tokens'] as JsonObject | undefined) ?? {};
    const tools = Array.isArray(snapshot['tools']) ? (snapshot['tools'] as JsonObject[]) : [];
    snapshot['tokens'] = tokens;
    snapshot['tools'] = tools;

    if (name === 'codex.sse_event') {
      for (const tokKey of [
        'input_token_count',
        'output_token_count',
        'cached_token_count',
        'reasoning_token_count',
        'tool_token_count',
      ]) {
        if (event[tokKey] !== undefined) {
          const prev = Number(tokens[tokKey] ?? 0);
          tokens[tokKey] = prev + Number(event[tokKey] ?? 0);
        }
      }
    } else if (name === 'codex.tool_result') {
      tools.push({
        tool_name: (event['tool_name'] ?? null) as JsonValue,
        call_id: (event['call_id'] ?? null) as JsonValue,
        duration_ms: (event['duration_ms'] ?? null) as JsonValue,
        success: (event['success'] ?? null) as JsonValue,
      });
    } else if (name === 'codex.conversation_starts') {
      snapshot['conversation_id'] = conversationId;
    }

    try {
      const cursor = (await this.actae.latestCursor(channel)) ?? 0;
      await this.actae.saveState(channel, cursor, adapterCheckpointState(snapshot, {
        framework: 'codex',
        channelId: channel,
        portableState: {
          conversation_id: snapshot['conversation_id'] ?? null,
          tokens: snapshot['tokens'] ?? {},
          tools: snapshot['tools'] ?? [],
        },
        nativeCheckpoint: { resume_authority: 'codex-rollout' },
        eventCursor: cursor,
      }));
    } catch {
      // best-effort
    }
  }

  private async currentSnapshot(channel: string): Promise<JsonObject> {
    try {
      const snap = await this.actae.latestState(channel);
      if (snap && snap.state && typeof snap.state === 'object') {
        return { ...(snap.state as JsonObject) };
      }
    } catch {
      // fall through
    }
    return {};
  }
}

function asObjArray(obj: unknown, key: string): JsonObject[] {
  if (!obj || typeof obj !== 'object' || Array.isArray(obj)) return [];
  const raw = (obj as JsonObject)[key];
  if (!Array.isArray(raw)) return [];
  const out: JsonObject[] = [];
  for (const item of raw) {
    if (item && typeof item === 'object' && !Array.isArray(item)) {
      out.push(item as JsonObject);
    }
  }
  return out;
}
