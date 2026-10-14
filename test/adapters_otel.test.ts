import { describe, expect, it } from 'vitest';
import {
  ACTAE_TRACE_ID,
  GEN_AI_REQUEST_MODEL,
  GEN_AI_TOOL_NAME,
  GEN_AI_USAGE_INPUT_TOKENS,
  ActaeOtelBridge,
  eventToSpan,
  traceContext,
} from '../src/adapters/otel.js';
import type { Event } from '../src/types.js';

function event(overrides: Partial<Event> = {}): Event {
  return {
    id: 'e1',
    channelId: 'ch-1',
    eventType: 'agent.step',
    payload: {},
    actor: 'agent',
    cursor: 1,
    timestamp: 't',
    ...overrides,
  };
}

class FakeTracer {
  spans: Array<{ name: string; attributes: Record<string, string | number | boolean> }> = [];
  startAsCurrentSpan(
    name: string,
    options?: { attributes?: Record<string, string | number | boolean> },
  ) {
    this.spans.push({ name, attributes: { ...(options?.attributes ?? {}) } });
    return { end: () => undefined };
  }
}

describe('OpenTelemetry bridge', () => {
  it('traceContext builds correlation metadata', () => {
    expect(traceContext('abc', 'def')).toEqual({ trace_id: 'abc', span_id: 'def' });
    expect(traceContext('abc')).toEqual({ trace_id: 'abc' });
  });

  it('lifts GenAI conventions and trace context', () => {
    const span = eventToSpan(
      event({
        eventType: 'llm.call',
        cursor: 7,
        payload: { model: 'gpt-5', input_tokens: 1200, tool: 'web_search' },
        metadata: { trace_id: 'abc' },
      }),
    );
    expect(span.name).toBe('llm.call');
    expect(span.attributes['actae.cursor']).toBe(7);
    expect(span.attributes[GEN_AI_REQUEST_MODEL]).toBe('gpt-5');
    expect(span.attributes[GEN_AI_USAGE_INPUT_TOKENS]).toBe(1200);
    expect(span.attributes[GEN_AI_TOOL_NAME]).toBe('web_search');
    expect(span.attributes[ACTAE_TRACE_ID]).toBe('abc');
  });

  it('ignores non-primitive payload fields', () => {
    const span = eventToSpan(event({ payload: { model: { nested: 1 } } }));
    expect(GEN_AI_REQUEST_MODEL in span.attributes).toBe(false);
  });

  it('exports spans through a tracer', () => {
    const tracer = new FakeTracer();
    const bridge = new ActaeOtelBridge(tracer);
    bridge.exportEvent(event({ eventType: 'tool.started', payload: { tool: 'charge' } }));
    expect(tracer.spans).toHaveLength(1);
    expect(tracer.spans[0]!.name).toBe('tool.started');
    expect(tracer.spans[0]!.attributes[GEN_AI_TOOL_NAME]).toBe('charge');
  });

  it('exportChannel replays then exports', async () => {
    const tracer = new FakeTracer();
    const bridge = new ActaeOtelBridge(tracer);
    const calls: unknown[] = [];
    const actae = {
      async replay(channelId: string, opts?: unknown) {
        calls.push([channelId, opts]);
        return [event({ eventType: 'a' }), event({ eventType: 'b', cursor: 2 })];
      },
    };
    const spans = await bridge.exportChannel(actae, 'ch-1', { cursor: 3, limit: 50 });
    expect(spans.map((s) => s.name)).toEqual(['a', 'b']);
    expect(calls).toEqual([['ch-1', { cursor: 3, limit: 50, eventType: undefined }]]);
    expect(tracer.spans).toHaveLength(2);
  });
});
