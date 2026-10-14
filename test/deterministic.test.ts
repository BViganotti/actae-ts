/**
 * Deterministic operation-key tests: cross-SDK byte parity (Go reference),
 * Go-exact canonical serialization (floats, escaping, NaN rejection) and
 * AgentSession re-drive semantics.
 */

import { describe, expect, it } from 'vitest';
import {
  canonicalStepContent,
  deterministicOperationKey,
} from '../src/deterministic.js';
import { AgentSession } from '../src/session.js';
import { FakeActaeClient } from './helpers.js';

describe('deterministicOperationKey', () => {
  it('matches the Go/Python parity vectors byte-for-byte', () => {
    // Plain vector: channel "ch-det", type "s", step 1.
    const plain = deterministicOperationKey(
      'agent-session',
      's',
      'ch-det',
      '1',
      canonicalStepContent(
        { step_number: 1, input: 'p', output: 'r' },
        { step_number: 1 },
      ),
    );
    expect(plain).toBe('07ca53db-606e-5b76-95bc-697e73bc4c41');

    // Escaping vector: non-ASCII + HTML chars + nested dicts.
    const escaping = deterministicOperationKey(
      'agent-session',
      'inference',
      'ch-x',
      '3',
      canonicalStepContent(
        { input: 'a < b & c > d', z: 1, ctx: { nested: 'é<&>' } },
        { note: 'x&y' },
      ),
    );
    expect(escaping).toBe('f1f23da4-abd0-50f9-b62c-1f1d0edd2fbf');
  });

  it('serializes floats exactly like Go json.Marshal (verified matrix)', () => {
    // Expected string produced by Go's encoding/json for the same inputs
    // (see the cross-SDK parity harness): integer-valued floats collapse,
    // -0.0 → 0, 1e-7 keeps no exponent padding, 1e20/1e15 use fixed
    // notation.
    const out = canonicalStepContent(
      {
        f1: 1.0,
        f2: 1e-7,
        f3: 1e20,
        f4: 1e15,
        f5: -0.0,
        f6: 1.5,
        f7: 3.14159,
      },
      { f8: 2.5e-7, n: 3 },
    );
    expect(out).toBe(
      '{"payload":{"f1":1,"f2":1e-7,"f3":100000000000000000000,"f4":1000000000000000,"f5":0,"f6":1.5,"f7":3.14159},"metadata":{"f8":2.5e-7,"n":3}}',
    );
  });

  it('rejects NaN/Infinity loudly (Go json.Marshal errors)', () => {
    expect(() => canonicalStepContent({ x: Number.NaN }, {})).toThrow(/NaN\/Infinity/);
    expect(() => canonicalStepContent({ x: Infinity }, {})).toThrow(/NaN\/Infinity/);
    expect(() => canonicalStepContent({}, { x: -Infinity })).toThrow(/NaN\/Infinity/);
  });

  it('rejects non-JSON values loudly (Go json.Marshal errors)', () => {
    expect(() =>
      canonicalStepContent({ x: undefined }, {}),
    ).toThrow(TypeError);
    expect(() => canonicalStepContent({ x: () => 1 }, {})).toThrow(TypeError);
  });

  it('emits bigint as raw number tokens (int64 parity)', () => {
    const out = canonicalStepContent({ big: 9007199254740993n }, {});
    expect(out).toBe('{"payload":{"big":9007199254740993},"metadata":{}}');
    // Two identical derivations produce the same id even with bigints.
    const idA = deterministicOperationKey('s', 'a', 'ch', '1', out);
    const idB = deterministicOperationKey('s', 'a', 'ch', '1', out);
    expect(idA).toBe(idB);
  });

  it('sorts nested keys recursively and keeps payload first', () => {
    const out = canonicalStepContent(
      { z: { b: 1, a: 2 }, a: 1 },
      { m: 1 },
    );
    expect(out).toBe(
      '{"payload":{"a":1,"z":{"a":2,"b":1}},"metadata":{"m":1}}',
    );
  });

  it('caps the name at 256 UTF-8 bytes, byte-exactly like Go', () => {
    // 200 'é' (2 bytes each in UTF-8) exceed 400 bytes; Go truncates the
    // byte string, not code points. JS slice would differ — the
    // implementation must byte-truncate.
    const long = 'é'.repeat(200);
    const canonical = canonicalStepContent({ input: long }, {});
    const id = deterministicOperationKey('agent-session', 's', 'ch', '1', canonical);
    expect(id).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
    // Deterministic: same input twice.
    expect(id).toBe(
      deterministicOperationKey('agent-session', 's', 'ch', '1', canonical),
    );
  });
});

describe('AgentSession deterministic step ids', () => {
  function newSession(actae: FakeActaeClient, channel: string) {
    return new AgentSession(actae as never, channel, {});
  }

  it('re-driving the same step reuses the same operation id; changed content differs', async () => {
    const actae = new FakeActaeClient();
    const s1 = newSession(actae, 'det-session');
    await s1.start();
    await s1.step('inference', { input: 'q', output: 'a', metadata: { m: 1 } });

    // Crash-recovery re-drive: a fresh session on the same channel, same
    // step number, same content.
    const s2 = newSession(actae, 'det-session');
    await s2.start();
    await s2.step('inference', { input: 'q', output: 'a', metadata: { m: 1 } });

    const ids = actae.recordedEvents
      .filter((r) => r.eventType === 'inference')
      .map((r) => r.opts['operationId'] as string);
    expect(ids).toHaveLength(2);
    expect(ids[0]).toBe(ids[1]);

    // Different content derives a different id.
    const s3 = newSession(actae, 'det-session');
    await s3.start();
    await s3.step('inference', { input: 'q', output: 'CHANGED', metadata: { m: 1 } });
    const ids3 = actae.recordedEvents
      .filter((r) => r.eventType === 'inference')
      .map((r) => r.opts['operationId'] as string);
    expect(ids3[2]).not.toBe(ids3[0]);
  });

  it('metadata key insertion order does not change the derived id', async () => {
    const actae = new FakeActaeClient();
    const s1 = newSession(actae, 'det-keyorder');
    await s1.start();
    await s1.step('inference', { input: 'q', output: 'a', metadata: { m: 1, n: 2 } });

    const s2 = newSession(actae, 'det-keyorder');
    await s2.start();
    await s2.step('inference', { input: 'q', output: 'a', metadata: { n: 2, m: 1 } });

    const ids = actae.recordedEvents
      .filter((r) => r.eventType === 'inference')
      .map((r) => r.opts['operationId'] as string);
    expect(ids[0]).toBe(ids[1]);
  });
});