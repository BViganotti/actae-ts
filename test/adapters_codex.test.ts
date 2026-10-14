import { afterEach, describe, expect, it } from 'vitest';
import { CodexOTLPReceiver, channelForConversation, kvToDict } from '../src/adapters/codex.js';
import { FakeActaeClient } from './helpers.js';

let receivers: CodexOTLPReceiver[] = [];
afterEach(async () => {
  await Promise.all(receivers.map((r) => r.stop()));
  receivers = [];
});

async function startReceiver(actae: FakeActaeClient) {
  const r = new CodexOTLPReceiver(actae as never, { host: '127.0.0.1', port: 0 });
  await r.start();
  receivers.push(r);
  return r;
}

function postLogs(url: string, body: unknown): Promise<{ status: number; json: unknown }> {
  return fetch(url, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  }).then(async (res) => ({ status: res.status, json: await res.json() }));
}

function otlpRecord(name: string, attrs: Record<string, string | number>, extra: Record<string, unknown> = {}) {
  return {
    timeUnixNano: '1700000000000000000',
    body: { stringValue: name },
    attributes: Object.entries(attrs).map(([key, value]) => ({
      key,
      value: typeof value === 'number' ? { intValue: value } : { stringValue: value },
    })),
    ...extra,
  };
}

describe('CodexOTLPReceiver', () => {
  it('channelForConversation is deterministic', () => {
    expect(channelForConversation('abc')).toBe('codex:abc');
    expect(channelForConversation('a b')).toBe('codex:a b');
  });

  it('kvToDict converts OTLP oneofs', () => {
    const d = kvToDict([
      { key: 's', value: { stringValue: 'x' } },
      { key: 'i', value: { intValue: 5 } },
      { key: 'b', value: { boolValue: true } },
      { key: 'd', value: { doubleValue: 1.5 } },
      { key: 'o', value: { other: 1 } },
      { key: 'raw', value: 'plain' },
    ]);
    expect(d).toEqual({ s: 'x', i: 5, b: true, d: 1.5, o: { other: 1 }, raw: 'plain' });
  });

  it('receives OTLP logs over HTTP and mirrors codex.* events', async () => {
    const actae = new FakeActaeClient();
    const r = await startReceiver(actae);
    const res = await postLogs(r.logsUrl, {
      resourceLogs: [
        {
          scopeLogs: [
            {
              logRecords: [
                otlpRecord('codex.conversation_starts', { 'conversation.id': 'conv-1' }),
                otlpRecord('codex.sse_event', {
                  'conversation.id': 'conv-1',
                  input_token_count: 100,
                  output_token_count: 50,
                }),
                otlpRecord('codex.tool_result', {
                  'conversation.id': 'conv-1',
                  tool_name: 'shell',
                  call_id: 'c1',
                  duration_ms: 120,
                  success: true,
                }),
                otlpRecord('some.other.event', { 'conversation.id': 'conv-1' }),
              ],
            },
          ],
        },
      ],
    });
    expect(res.status).toBe(200);
    expect((res.json as { processed: number }).processed).toBe(3);

    const recorded = actae.recordedEvents.filter((e) => e.channelId === 'codex:conv-1');
    expect(recorded.map((e) => e.eventType)).toContain('codex.conversation_starts');
    expect(recorded.map((e) => e.eventType)).toContain('codex.sse_event');
    expect(recorded.map((e) => e.eventType)).toContain('codex.tool_result');
    expect(recorded.map((e) => e.eventType)).not.toContain('some.other.event');

    // state snapshot accumulates tokens + tools
    const snap = actae.channels.get('codex:conv-1')!.states.at(-1)!.state;
    expect(snap['tokens']).toEqual({ input_token_count: 100, output_token_count: 50 });
    expect(snap['tools']).toEqual([
      { tool_name: 'shell', call_id: 'c1', duration_ms: 120, success: true },
    ]);
    expect(snap['conversation_id']).toBe('conv-1');
  });

  it('token accumulation across multiple sse events', async () => {
    const actae = new FakeActaeClient();
    const r = await startReceiver(actae);
    await postLogs(r.logsUrl, {
      resourceLogs: [{ scopeLogs: [{ logRecords: [
        otlpRecord('codex.sse_event', { 'conversation.id': 'c', output_token_count: 10 }),
        otlpRecord('codex.sse_event', { 'conversation.id': 'c', output_token_count: 15 }),
      ] }] }],
    });
    const snap = actae.channels.get('codex:c')!.states.at(-1)!.state;
    expect(snap['tokens']).toEqual({ output_token_count: 25 });
  });

  it('rejects malformed JSON with 400', async () => {
    const actae = new FakeActaeClient();
    const r = await startReceiver(actae);
    const res = await fetch(r.logsUrl, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: '{not json',
    });
    expect(res.status).toBe(400);
  });

  it('exposes a health endpoint', async () => {
    const actae = new FakeActaeClient();
    const r = await startReceiver(actae);
    const base = r.logsUrl.replace('/v1/logs', '');
    const res = await fetch(`${base}/health`);
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ status: 'ok' });
  });

  it('extractEvent returns undefined for non-codex events', () => {
    const r = new CodexOTLPReceiver(new FakeActaeClient() as never, { port: 0 });
    expect(r.extractEvent({ body: { stringValue: 'other' }, attributes: [] })).toBeUndefined();
    expect(r.extractEvent({ body: 'plain-text' })).toBeUndefined();
    const ev = r.extractEvent({ body: { stringValue: 'codex.api_request' }, attributes: [] });
    expect(ev?.name).toBe('codex.api_request');
  });

  it('stop releases the port', async () => {
    const actae = new FakeActaeClient();
    const r = await startReceiver(actae);
    const url = r.logsUrl;
    await r.stop();
    // re-starting on the same receiver works
    await r.start();
    expect(r.logsUrl).not.toBe(url);
  });
});
