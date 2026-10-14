/**
 * OpenAI Agents SDK tracing adapter for Actae.
 *
 * Ports the Python adapter (`actae_client.adapters.openai_agents`) against
 * the **real JS SDK** (`@openai/agents`): implements the `TracingProcessor`
 * interface. Every agent run (trace) and every operation within it (spans:
 * agent runs, LLM generations, function/tool calls) is streamed as a
 * real-time event, and each completed run is persisted as a cursor-aligned,
 * versioned state snapshot.
 *
 * ```ts
 * import { installActaeTracing } from '@actae/sdk/adapters/openai';
 * installActaeTracing(actae); // call once, before Runner.run
 * const result = await Runner.run(agent, 'Hello');
 * ```
 *
 * The primary method names follow the JS `TracingProcessor` contract
 * (camelCase, async); snake_case aliases mirror the Python SDK surface.
 * Persistence is fire-and-forget from the SDK's callbacks; `forceFlush()` /
 * `shutdown()` await the pending writes.
 */

import type { ActaeClient } from '../client.js';
import type { JsonObject, JsonValue } from '../json.js';
import { adapterCheckpointState } from './contract.js';

const EVENT_TRACE_START = 'openai.trace.start';
const EVENT_SPAN_END = 'openai.span.end';
const EVENT_TRACE_END = 'openai.trace.end';
const ACTOR = 'openai-agents';

function isoNow(): string {
  return new Date().toISOString();
}

/** Extracts a compact, JSON-safe summary from a tracing span. Handles both
 * the real `@openai/agents` `Span` shape (camelCase accessors) and raw
 * objects (snake_case fallback) so the adapter keeps working across SDK
 * versions. */
export function spanSummary(span: unknown, includeInputsOutputs: boolean): JsonObject {
  const anySpan = span as Record<string, unknown>;
  const data = (anySpan['spanData'] ?? anySpan['span_data']) as Record<string, unknown> | undefined;
  const summary: JsonObject = {
    type: data ? String((data['type'] ?? data['name'] ?? 'unknown')) : 'unknown',
    span_id: (anySpan['spanId'] ?? anySpan['span_id'] ?? null) as JsonValue,
    parent_id: (anySpan['parentId'] ?? anySpan['parent_id'] ?? null) as JsonValue,
    started_at: (anySpan['startedAt'] ?? anySpan['started_at'] ?? null) as JsonValue,
    ended_at: (anySpan['endedAt'] ?? anySpan['ended_at'] ?? null) as JsonValue,
  };
  const started = anySpan['startedAt'] ?? anySpan['started_at'];
  const ended = anySpan['endedAt'] ?? anySpan['ended_at'];
  if (typeof started === 'string' && typeof ended === 'string') {
    const s = Date.parse(started);
    const e = Date.parse(ended);
    if (Number.isFinite(s) && Number.isFinite(e)) {
      summary['duration_ms'] = e - s;
    }
  }

  const error = anySpan['error'] ?? anySpan['error'];
  if (error && typeof error === 'object') {
    const err = error as { message?: unknown; data?: unknown };
    summary['error'] = {
      message: (err.message ?? String(error)) as JsonValue,
      data: (err.data ?? null) as JsonValue,
    };
  }

  if (data) {
    for (const attr of [
      'name',
      'model',
      'model_config',
      'agent_name',
      'to_agent',
      'from_agent',
      'output_type',
      'turn',
      'triggered',
      'handoffs',
      'tools',
      'usage',
      'metadata',
      'input',
      'output',
    ]) {
      const v = data[attr];
      if (v !== undefined) {
        if (!includeInputsOutputs && (attr === 'input' || attr === 'output')) continue;
        summary[attr] = v as JsonValue;
      }
    }
  }
  return summary;
}

// ---------------------------------------------------------------------------
// ActaeTracingProcessor
// ---------------------------------------------------------------------------

export interface ActaeTracingProcessorOptions {
  /** Channel for tracing events and run snapshots (default "openai_agents"). */
  channel?: string;
  /** Persist span input/output payloads. Set false to keep only
   * names/timings/errors (e.g. to avoid storing sensitive content). */
  includeInputsOutputs?: boolean;
}

interface TraceEntry {
  name: unknown;
  groupId: unknown;
  metadata: unknown;
  startedAt: string;
  spans: JsonObject[];
}

/** `@openai/agents` `TracingProcessor` that mirrors traces into Actae.
 *
 * Every span end is recorded as an `openai.span.end` event; every trace is
 * recorded as `openai.trace.start`/`openai.trace.end` events and persisted as
 * a cursor-aligned state snapshot containing the full span tree. */
export class ActaeTracingProcessor {
  readonly actae: ActaeClient;
  readonly channel: string;
  readonly includeInputsOutputs: boolean;

  private readonly traces = new Map<string, TraceEntry>();
  private pending: Array<Promise<void>> = [];

  constructor(actae: ActaeClient, options: ActaeTracingProcessorOptions = {}) {
    this.actae = actae;
    this.channel = options.channel ?? 'openai_agents';
    this.includeInputsOutputs = options.includeInputsOutputs ?? true;
  }

  // -------------------------------------------------------------------------
  // TracingProcessor protocol (JS camelCase, async)
  // -------------------------------------------------------------------------

  async onTraceStart(trace: unknown): Promise<void> {
    try {
      const t = trace as { traceId?: unknown; trace_id?: unknown; name?: unknown; groupId?: unknown; group_id?: unknown; metadata?: unknown };
      const traceId = String(t.traceId ?? t.trace_id ?? '');
      const groupId = t.groupId ?? t.group_id ?? null;
      this.traces.set(traceId, {
        name: t.name ?? null,
        groupId,
        metadata: t.metadata ?? null,
        startedAt: isoNow(),
        spans: [],
      });
      this.schedule(this.recordTraceStart(traceId, t.name ?? null));
    } catch (err) {
      this.logErr('onTraceStart', err);
    }
  }

  async onSpanStart(span: unknown): Promise<void> {
    try {
      const traceId = String((span as { traceId?: unknown; trace_id?: unknown }).traceId ?? (span as { trace_id?: unknown }).trace_id ?? '');
      if (!this.traces.has(traceId)) {
        this.traces.set(traceId, {
          name: null,
          groupId: null,
          metadata: null,
          startedAt: isoNow(),
          spans: [],
        });
      }
    } catch (err) {
      this.logErr('onSpanStart', err);
    }
  }

  async onSpanEnd(span: unknown): Promise<void> {
    try {
      const traceId = String((span as { traceId?: unknown; trace_id?: unknown }).traceId ?? (span as { trace_id?: unknown }).trace_id ?? '');
      const summary = spanSummary(span, this.includeInputsOutputs);
      const entry = this.traces.get(traceId) ?? {
        name: null,
        groupId: null,
        metadata: null,
        startedAt: isoNow(),
        spans: [],
      };
      entry.spans.push(summary);
      this.traces.set(traceId, entry);
      this.schedule(this.recordSpanEnd(traceId, summary));
    } catch (err) {
      this.logErr('onSpanEnd', err);
    }
  }

  async onTraceEnd(trace: unknown): Promise<void> {
    try {
      const t = trace as { traceId?: unknown; trace_id?: unknown; name?: unknown; groupId?: unknown; group_id?: unknown; metadata?: unknown };
      const traceId = String(t.traceId ?? t.trace_id ?? '');
      const groupId = t.groupId ?? t.group_id ?? null;
      const entry = this.traces.get(traceId) ?? {
        name: t.name ?? null,
        groupId: t.groupId ?? null,
        metadata: t.metadata ?? null,
        startedAt: isoNow(),
        spans: [],
      };
      const snapshot: JsonObject = {
        trace_id: traceId,
        name: entry.name as JsonValue,
        group_id: entry.groupId as JsonValue,
        metadata: entry.metadata as JsonValue,
        started_at: entry.startedAt,
        ended_at: isoNow(),
        spans: entry.spans,
      };
      this.schedule(this.recordTraceEnd(traceId, snapshot));
    } catch (err) {
      this.logErr('onTraceEnd', err);
    }
  }

  /** Awaits all pending Actae writes. */
  async forceFlush(): Promise<void> {
    while (this.pending.length > 0) {
      const snapshot = [...this.pending];
      await Promise.allSettled(snapshot);
    }
  }

  /** Awaits all pending Actae writes. */
  async shutdown(_timeout?: number): Promise<void> {
    await this.forceFlush();
  }

  // -------------------------------------------------------------------------
  // Python SDK surface aliases (snake_case, sync callbacks)
  // -------------------------------------------------------------------------

  on_trace_start(trace: unknown): void {
    void this.onTraceStart(trace);
  }

  on_span_start(span: unknown): void {
    void this.onSpanStart(span);
  }

  on_span_end(span: unknown): void {
    void this.onSpanEnd(span);
  }

  on_trace_end(trace: unknown): void {
    void this.onTraceEnd(trace);
  }

  force_flush(): Promise<void> {
    return this.forceFlush();
  }

  /** Forks a specific run's snapshot into a new channel.
   *
   * The OpenAI Agents SDK is stateless (traces only, no native resume), so
   * Actae's value is observability + cross-run comparison: this copies the
   * snapshot of one completed run (`traceId`) into `newChannel` so you can
   * compare two runs structurally or keep a fork of a run for reference.
   * The Agents SDK does not resume — it re-runs — so this is a
   * fork-for-comparison, not a fork-for-resume.
   *
   * @param traceId The run's trace id to fork.
   * @param newChannel Channel ID for the fork.
   * @throws TypeError if no snapshot for `traceId` is found. */
  async forkTrace(
    traceId: string,
    newChannel: string,
    options: { reason?: string } = {},
  ): Promise<void> {
    // Locate the version whose snapshot carries this trace_id (newest first).
    let targetCursor: number | undefined;
    let offset = 0;
    for (;;) {
      const versions = await this.actae.listStates(this.channel, { limit: 100, offset });
      if (versions.length === 0) break;
      for (const sv of versions) {
        const snap = await this.actae.getState(this.channel, sv.version);
        const state = snap?.state as JsonObject | undefined;
        if (state?.['trace_id'] === traceId) {
          targetCursor = sv.cursor;
          break;
        }
      }
      if (targetCursor !== undefined) break;
      offset += versions.length;
      if (versions.length < 100) break;
    }
    if (targetCursor === undefined) {
      throw new TypeError(`no Actae snapshot found for trace_id '${traceId}'`);
    }
    await this.actae.fork(this.channel, newChannel, targetCursor, {
      displayName: newChannel,
      reason: options.reason ?? `Forked run ${traceId} → ${newChannel}`,
    });
  }

  // -------------------------------------------------------------------------
  // Internals
  // -------------------------------------------------------------------------

  private schedule(p: Promise<void>): void {
    const tracked = p.catch((err: Error) => {
      this.logErr('background write', err);
    });
    this.pending.push(tracked);
    tracked.finally(() => {
      const idx = this.pending.indexOf(tracked);
      if (idx >= 0) this.pending.splice(idx, 1);
    });
  }

  private async recordTraceStart(traceId: string, name: unknown): Promise<void> {
    await this.actae.record(this.channel, EVENT_TRACE_START, {
      trace_id: traceId,
      name: (name ?? null) as JsonValue,
    } as JsonObject, { actor: ACTOR });
  }

  private async recordSpanEnd(traceId: string, summary: JsonObject): Promise<void> {
    await this.actae.record(this.channel, EVENT_SPAN_END, {
      trace_id: traceId,
      ...summary,
    } as JsonObject, { actor: ACTOR });
  }

  private async recordTraceEnd(traceId: string, snapshot: JsonObject): Promise<void> {
    await this.actae.record(this.channel, EVENT_TRACE_END, {
      trace_id: traceId,
    } as JsonObject, { actor: ACTOR });
    const cursor = (await this.actae.latestCursor(this.channel)) ?? 0;
    await this.actae.saveState(this.channel, cursor, adapterCheckpointState(snapshot, {
      framework: 'openai-agents',
      channelId: this.channel,
      portableState: {
        trace_id: snapshot['trace_id'] ?? null,
        workflow_name: snapshot['workflow_name'] ?? null,
        span_count: Array.isArray(snapshot['spans']) ? snapshot['spans'].length : 0,
      },
      nativeCheckpoint: { resume: 'unavailable' },
      eventCursor: cursor,
    }));
  }

  private logErr(op: string, err: unknown): void {
    console.warn(`actae: ActaeTracingProcessor.${op} failed: ${(err as Error).message}`);
  }
}

/** Registers an ActaeTracingProcessor as a global OpenAI Agents tracing
 * processor. Requires `@openai/agents` to be installed. */
export async function installActaeTracing(
  actae: ActaeClient,
  options: ActaeTracingProcessorOptions = {},
): Promise<ActaeTracingProcessor> {
  let agents: { addTraceProcessor?: (p: unknown) => void; setTraceProcessors?: (ps: unknown[]) => void };
  try {
    agents = (await import('@openai/agents')) as {
      addTraceProcessor?: (p: unknown) => void;
      setTraceProcessors?: (ps: unknown[]) => void;
    };
  } catch {
    throw new TypeError(
      'installActaeTracing requires @openai/agents. Install it with: npm install @openai/agents',
    );
  }
  const processor = new ActaeTracingProcessor(actae, options);
  const add = agents.addTraceProcessor;
  if (typeof add === 'function') {
    add(processor);
    return processor;
  }
  const set = agents.setTraceProcessors;
  if (typeof set === 'function') {
    set([processor]);
    return processor;
  }
  throw new TypeError('@openai/agents is installed but exposes no addTraceProcessor');
}
