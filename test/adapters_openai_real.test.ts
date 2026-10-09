import { describe, expect, it } from 'vitest';
import { addTraceProcessor, withTrace, Span, setTracingDisabled } from '@openai/agents';
import type { TracingProcessor } from '@openai/agents';
import { ActaeTracingProcessor, installActaeTracing, spanSummary } from '../src/adapters/openai.js';
import { FakeActaeClient } from './helpers.js';

/**
 * Real-framework integration against the actual `@openai/agents` SDK.
 * `withTrace` drives the full processor lifecycle (onTraceStart/onTraceEnd)
 * without needing an LLM; real `Span` objects exercise the extraction.
 */

describe('ActaeTracingProcessor × real @openai/agents', () => {
  it('implements the TracingProcessor interface (structural check)', () => {
    const actae = new FakeActaeClient();
    const p = new ActaeTracingProcessor(actae as never);
    const required = ['onTraceStart', 'onTraceEnd', 'onSpanStart', 'onSpanEnd', 'shutdown', 'forceFlush'];
    for (const m of required) {
      expect(typeof (p as unknown as Record<string, unknown>)[m]).toBe('function');
    }
  });

  it('withTrace flows a real trace lifecycle through the processor', async () => {
    // The SDK disables tracing when NODE_ENV=test (vitest) — re-enable it.
    setTracingDisabled(false);
    const actae = new FakeActaeClient();
    const processor = new ActaeTracingProcessor(actae as never, { channel: 'oa' });
    addTraceProcessor(processor as unknown as TracingProcessor);

    await withTrace('real-run', async () => {
      // inside a trace, no explicit work needed — lifecycle is driven by the SDK
    });
    await processor.shutdown();

    const events = actae.recordedEvents.filter((e) => e.channelId === 'oa');
    expect(events.some((e) => e.eventType === 'openai.trace.start')).toBe(true);
    expect(events.some((e) => e.eventType === 'openai.trace.end')).toBe(true);
    const snap = actae.channels.get('oa')?.states.at(-1)?.state;
    expect(snap?.['name']).toBe('real-run');
  });

  it('extracts summaries from real Span objects', async () => {
    const noop: TracingProcessor = {
      async onTraceStart() {},
      async onTraceEnd() {},
      async onSpanStart() {},
      async onSpanEnd() {},
      async shutdown() {},
      async forceFlush() {},
    };
    const span = new Span({
      traceId: 'trace-1',
      spanId: 'span-1',
      parentId: 'parent-1',
      data: {
        type: 'generation',
        name: 'gpt-4',
        model: 'gpt-4o',
        input: [{ role: 'user', content: 'hi' }],
        output: [{ type: 'text', text: 'hello' }],
        usage: { input_tokens: 10, output_tokens: 5 },
      },
      startedAt: '2026-01-01T00:00:00.000Z',
      endedAt: '2026-01-01T00:00:00.500Z',
    }, noop);

    const summary = spanSummary(span, true);
    expect(summary['type']).toBe('generation');
    expect(summary['name']).toBe('gpt-4');
    expect(summary['model']).toBe('gpt-4o');
    expect(summary['span_id']).toBe('span-1');
    expect(summary['parent_id']).toBe('parent-1');
    expect(summary['duration_ms']).toBe(500);
    expect(summary['input']).toBeDefined();
    expect(summary['usage']).toMatchObject({ input_tokens: 10 });

    const stripped = spanSummary(span, false);
    expect(stripped['input']).toBeUndefined();
    expect(stripped['output']).toBeUndefined();
    expect(stripped['name']).toBe('gpt-4');
  });

  it('captures span errors from real Span.error', async () => {
    const noop: TracingProcessor = {
      async onTraceStart() {},
      async onTraceEnd() {},
      async onSpanStart() {},
      async onSpanEnd() {},
      async shutdown() {},
      async forceFlush() {},
    };
    const span = new Span({
      traceId: 'trace-1',
      data: { type: 'function', name: 'tool' },
    }, noop);
    span.setError({ message: 'kaboom', data: { code: 42 } });

    const summary = spanSummary(span, true);
    expect(summary['error']).toEqual({ message: 'kaboom', data: { code: 42 } });
  });

  it('installActaeTracing registers against the real SDK', async () => {
    setTracingDisabled(false);
    const actae = new FakeActaeClient();
    const processor = await installActaeTracing(actae as never, { channel: 'oa' });
    expect(processor).toBeInstanceOf(ActaeTracingProcessor);
    await withTrace('installed', async () => {});
    await processor.shutdown();
    expect(actae.recordedEvents.some((e) => e.eventType === 'openai.trace.start')).toBe(true);
  });

  it('forkTrace forks the snapshot carrying the trace_id (newest-first)', async () => {
    const actae = new FakeActaeClient();
    // newest-first: cursor 3 (no trace), cursor 2 (trace t2), cursor 1 (trace t1)
    await actae.saveState('oa', 1, { trace_id: 't1', name: 'run-1' });
    await actae.saveState('oa', 2, { trace_id: 't2', name: 'run-2' });
    await actae.saveState('oa', 3, { other: true });
    const processor = new ActaeTracingProcessor(actae as never, { channel: 'oa' });

    await processor.forkTrace('t1', 'fork-t1', { reason: 'compare' });

    // t1 is the OLDEST snapshot — the scan must page through the newer
    // snapshots and pick cursor 1.
    expect(actae.forks).toEqual([
      { source: 'oa', child: 'fork-t1', atCursor: 1 },
    ]);
  });

  it('forkTrace throws TypeError when no snapshot matches', async () => {
    const actae = new FakeActaeClient();
    await actae.saveState('oa', 1, { trace_id: 't1' });
    await actae.saveState('oa', 2, { other: true });
    const processor = new ActaeTracingProcessor(actae as never, { channel: 'oa' });

    await expect(processor.forkTrace('missing', 'fork-x')).rejects.toThrow(TypeError);
    expect(actae.forks).toEqual([]);
  });
});
