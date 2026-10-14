/**
 * Export Actae execution events to OpenTelemetry as GenAI spans (optional).
 *
 * Actae is the execution layer *under* observability: this bridge mirrors a
 * channel's durable events into any OTel-shaped tracer so a Langfuse / Phoenix
 * / OTel backend can chart latency, tokens and errors next to your traces.
 *
 * The core SDK has **no OpenTelemetry dependency**. Pass any object with
 * `startAsCurrentSpan(name, { attributes })` (the real `@opentelemetry/api`
 * tracer qualifies); this module never imports it.
 */

import type { JsonObject, JsonValue } from '../json.js';
import type { Event } from '../types.js';

export const GEN_AI_REQUEST_MODEL = 'gen_ai.request.model';
export const GEN_AI_RESPONSE_MODEL = 'gen_ai.response.model';
export const GEN_AI_USAGE_INPUT_TOKENS = 'gen_ai.usage.input_tokens';
export const GEN_AI_USAGE_OUTPUT_TOKENS = 'gen_ai.usage.output_tokens';
export const GEN_AI_TOOL_NAME = 'gen_ai.tool.name';
export const GEN_AI_OPERATION_NAME = 'gen_ai.operation.name';

export const ACTAE_CHANNEL_ID = 'actae.channel_id';
export const ACTAE_CURSOR = 'actae.cursor';
export const ACTAE_EVENT_TYPE = 'actae.event_type';
export const ACTAE_ACTOR = 'actae.actor';
export const ACTAE_TRACE_ID = 'actae.trace_id';
export const ACTAE_SPAN_ID = 'actae.span_id';
export const ACTAE_LATENCY_MS = 'actae.latency_ms';

/** A span-shaped projection of one Actae event. */
export interface ActaeSpan {
  name: string;
  attributes: Record<string, string | number | boolean>;
}

/** The minimal tracer shape this bridge drives. */
export interface OtelLikeTracer {
  startAsCurrentSpan(
    name: string,
    options?: { attributes?: Record<string, string | number | boolean> },
  ): { end(): void } | undefined;
}

/** Event metadata that correlates an Actae event with an OTel trace. */
export function traceContext(traceId: string, spanId?: string): JsonObject {
  const ctx: JsonObject = { trace_id: traceId };
  if (spanId) ctx['span_id'] = spanId;
  return ctx;
}

function primitive(value: unknown): string | number | boolean | undefined {
  if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
    return value;
  }
  return undefined;
}

function first(payload: JsonObject, ...keys: string[]): string | number | boolean | undefined {
  for (const key of keys) {
    const value = primitive(payload[key]);
    if (value !== undefined) return value;
  }
  return undefined;
}

function asObject(value: JsonValue | undefined): JsonObject | undefined {
  if (value === undefined || value === null || typeof value !== 'object' || Array.isArray(value)) {
    return undefined;
  }
  return value as JsonObject;
}

/** Project an Actae event onto a span name + OTel attributes (pure). */
export function eventToSpan(event: Event): ActaeSpan {
  const attributes: Record<string, string | number | boolean> = {
    [ACTAE_CHANNEL_ID]: event.channelId,
    [ACTAE_CURSOR]: event.cursor,
    [ACTAE_EVENT_TYPE]: event.eventType,
    [ACTAE_ACTOR]: event.actor,
  };

  const metadata = event.metadata;
  if (metadata) {
    const traceId = primitive(metadata['trace_id']);
    if (traceId !== undefined) attributes[ACTAE_TRACE_ID] = traceId;
    const spanId = primitive(metadata['span_id']);
    if (spanId !== undefined) attributes[ACTAE_SPAN_ID] = spanId;
  }

  const payload = asObject(event.payload);
  if (payload) {
    const model = first(payload, 'model', 'request_model');
    if (model !== undefined) attributes[GEN_AI_REQUEST_MODEL] = model;
    const responseModel = first(payload, 'response_model');
    if (responseModel !== undefined) attributes[GEN_AI_RESPONSE_MODEL] = responseModel;
    const tool = first(payload, 'tool', 'tool_name');
    if (tool !== undefined) attributes[GEN_AI_TOOL_NAME] = tool;
    const operation = first(payload, 'operation', 'operation_name');
    if (operation !== undefined) attributes[GEN_AI_OPERATION_NAME] = operation;
    const inputTokens = first(payload, 'input_tokens', 'prompt_tokens', 'tokens');
    if (inputTokens !== undefined) attributes[GEN_AI_USAGE_INPUT_TOKENS] = inputTokens;
    const outputTokens = first(payload, 'output_tokens', 'completion_tokens');
    if (outputTokens !== undefined) attributes[GEN_AI_USAGE_OUTPUT_TOKENS] = outputTokens;
    const latency = first(payload, 'latency_ms', 'duration_ms');
    if (latency !== undefined) attributes[ACTAE_LATENCY_MS] = latency;
  }

  return { name: event.eventType || 'actae.event', attributes };
}

/** Mirror Actae events into an OTel-shaped tracer. */
export class ActaeOtelBridge {
  constructor(private readonly tracer: OtelLikeTracer) {}

  exportEvent(event: Event): ActaeSpan {
    const span = eventToSpan(event);
    const handle = this.tracer.startAsCurrentSpan(span.name, { attributes: span.attributes });
    handle?.end();
    return span;
  }

  exportEvents(events: Event[]): ActaeSpan[] {
    return events.map((event) => this.exportEvent(event));
  }

  /** Replay a channel once and export every event as a span. */
  async exportChannel(
    actae: { replay(channelId: string, opts?: { cursor?: number; limit?: number; eventType?: string }): Promise<Event[]> },
    channelId: string,
    opts: { cursor?: number; limit?: number; eventType?: string } = {},
  ): Promise<ActaeSpan[]> {
    const events = await actae.replay(channelId, {
      cursor: opts.cursor ?? 0,
      limit: opts.limit ?? 1000,
      eventType: opts.eventType,
    });
    return this.exportEvents(events);
  }
}
