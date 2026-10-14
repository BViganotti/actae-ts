import { describe, it, expect } from 'vitest';
import {
  asBigInt,
  asBool,
  asFloat,
  asInt64,
  asList,
  asMap,
  asString,
  asStringList,
  parseJson,
  parseNumericString,
  stringify,
  unwrapSonic,
  SONIC_NUMBER_KEY,
} from '../src/json.js';

describe('parseJson', () => {
  it('parses simple objects', () => {
    expect(parseJson('{"a":1,"b":"x","c":true,"d":null,"e":[1,2]}')).toEqual({
      a: 1,
      b: 'x',
      c: true,
      d: null,
      e: [1, 2],
    });
  });

  it('keeps out-of-safe-range integers as bigint (never rounds)', () => {
    const v = parseJson('{"cursor": 9007199254740993}') as { cursor: unknown };
    expect(v.cursor).toBe(9007199254740993n);
  });

  it('keeps negative out-of-range integers as bigint', () => {
    const v = parseJson('[-9007199254740993]') as [unknown];
    expect(v[0]).toBe(-9007199254740993n);
  });

  it('parses floats and exponents', () => {
    expect(parseJson('{"x": 1.5, "y": 2e3, "z": -0.25}')).toEqual({ x: 1.5, y: 2000, z: -0.25 });
  });

  it('unwraps sonic-rs markers in the same pass', () => {
    const v = parseJson(`{"payload": {"${SONIC_NUMBER_KEY}": "12345678901234567890"}}`) as {
      payload: unknown;
    };
    expect(v.payload).toBe(12345678901234567890n);
  });

  it('handles unicode escapes', () => {
    expect(parseJson('"\\u0041\\u00e9"')).toBe('Aé');
  });

  it('rejects malformed JSON', () => {
    expect(() => parseJson('{"a":')).toThrow(SyntaxError);
    expect(() => parseJson('nope')).toThrow(SyntaxError);
    expect(() => parseJson('{"a":1,}')).toThrow(SyntaxError);
  });

  it('handles nested arrays and whitespace', () => {
    expect(parseJson('  [ 1 , 2 , [ 3 ] ]  ')).toEqual([1, 2, [3]]);
  });
});

describe('unwrapSonic', () => {
  it('replaces markers with numbers', () => {
    expect(unwrapSonic({ [SONIC_NUMBER_KEY]: '42' })).toBe(42);
  });

  it('replaces markers with bigint for out-of-range', () => {
    expect(unwrapSonic({ [SONIC_NUMBER_KEY]: '123456789012345678901234567890' })).toBe(
      123456789012345678901234567890n,
    );
  });

  it('leaves failed conversions untouched', () => {
    const bad = { [SONIC_NUMBER_KEY]: 'not-a-number' };
    expect(unwrapSonic(bad)).toEqual(bad);
  });

  it('recurses into objects and arrays', () => {
    const input = {
      a: { [SONIC_NUMBER_KEY]: '1' },
      b: [{ [SONIC_NUMBER_KEY]: '2.5' }],
    };
    expect(unwrapSonic(input)).toEqual({ a: 1, b: [2.5] });
  });

  it('passes through primitives', () => {
    expect(unwrapSonic('x')).toBe('x');
    expect(unwrapSonic(1)).toBe(1);
    expect(unwrapSonic(null)).toBe(null);
    expect(unwrapSonic(true)).toBe(true);
  });
});

describe('parseNumericString', () => {
  it('parses integral values to safe numbers', () => {
    expect(parseNumericString('123')).toEqual({ ok: true, value: 123 });
  });
  it('parses out-of-range integral values to bigint', () => {
    expect(parseNumericString('9007199254740993')).toEqual({
      ok: true,
      value: 9007199254740993n,
    });
  });
  it('parses floats', () => {
    expect(parseNumericString('1.5')).toEqual({ ok: true, value: 1.5 });
  });
  it('rejects garbage', () => {
    expect(parseNumericString('abc').ok).toBe(false);
    expect(parseNumericString('').ok).toBe(false);
  });
});

describe('accessors', () => {
  it('asInt64 normalizes number/bigint/string', () => {
    expect(asInt64(42)).toBe(42);
    expect(asInt64(42.9)).toBe(42);
    expect(asInt64(42n)).toBe(42);
    expect(asInt64('42')).toBe(42);
    expect(asInt64('42.5', 7)).toBe(7);
    expect(asInt64('not-a-number', -1)).toBe(-1);
    expect(asInt64(null, 7)).toBe(7);
    expect(asInt64(undefined, 7)).toBe(7);
    // out-of-safe-range bigint → default (cannot be represented as number)
    expect(asInt64(9007199254740993n, -2)).toBe(-2);
  });

  it('asBigInt converts numbers and numeric strings', () => {
    expect(asBigInt(42)).toBe(42n);
    expect(asBigInt('9007199254740993')).toBe(9007199254740993n);
    expect(asBigInt(42n)).toBe(42n);
    expect(asBigInt('x')).toBeNull();
    expect(asBigInt(null)).toBeNull();
  });

  it('asFloat handles numbers/bigint/strings', () => {
    expect(asFloat(1.5)).toBe(1.5);
    expect(asFloat(2n)).toBe(2);
    expect(asFloat('1.25')).toBe(1.25);
    expect(asFloat('x', -1)).toBe(-1);
  });

  it('asString/asBool', () => {
    expect(asString('x')).toBe('x');
    expect(asString(5)).toBe('');
    expect(asBool(true)).toBe(true);
    expect(asBool(1)).toBe(false);
    expect(asBool('false')).toBe(false);
  });

  it('asMap/asList', () => {
    expect(asMap({ a: 1 })).toEqual({ a: 1 });
    expect(asMap([1])).toBeUndefined();
    expect(asMap('x')).toBeUndefined();
    expect(asList([1, 2])).toEqual([1, 2]);
    expect(asList('x')).toEqual([]);
  });

  it('asStringList filters to strings', () => {
    expect(asStringList(['a', 1, 'b'])).toEqual(['a', 'b']);
    expect(asStringList('x')).toBeUndefined();
  });
});

describe('stringify (BigInt-aware serializer)', () => {
  it('emits bigint as raw JSON number tokens', () => {
    expect(stringify({ n: 9007199254740993n })).toBe('{"n":9007199254740993}');
    expect(stringify([1n, 2n])).toBe('[1,2]');
    expect(stringify(-12345678901234567890n)).toBe('-12345678901234567890');
  });

  it('round-trips parseJson output', () => {
    const v = parseJson('{"cursor":9007199254740993,"name":"x"}');
    expect(stringify(v)).toBe('{"cursor":9007199254740993,"name":"x"}');
  });

  it('serializes plain objects like JSON.stringify', () => {
    expect(stringify({ a: 1, b: 'x', c: true, d: null, e: [1, 2] })).toBe(
      JSON.stringify({ a: 1, b: 'x', c: true, d: null, e: [1, 2] }),
    );
  });

  it('handles Date and RegExp', () => {
    expect(stringify(new Date('2026-01-01T00:00:00Z'))).toBe('"2026-01-01T00:00:00.000Z"');
    expect(stringify(/ab+c/)).toBe('"ab+c"');
  });

  it('omits undefined/function/symbol in objects, null in arrays', () => {
    expect(stringify({ a: undefined, b: 1, c: () => 1 })).toBe('{"b":1}');
    expect(stringify([undefined, 1])).toBe('[null,1]');
  });

  it('throws on cyclic references', () => {
    const obj: Record<string, unknown> = { a: 1 };
    obj['self'] = obj;
    expect(() => stringify(obj)).toThrow(/cyclic/);
  });

  it('serializes NaN/Infinity as null', () => {
    expect(stringify({ a: NaN, b: Infinity })).toBe('{"a":null,"b":null}');
  });

  it('escapes control characters and quotes', () => {
    expect(stringify({ s: 'a"b\nc\t\\d\u0001' })).toBe('"a\\"b\\nc\\t\\\\d\\u0001"'.replace(/^"|"$/g, '') && '{"s":"a\\"b\\nc\\t\\\\d\\u0001"}');
  });

  it('uses toJSON when present', () => {
    const withToJSON = { toJSON: () => ({ folded: true }) };
    expect(stringify(withToJSON)).toBe('{"folded":true}');
  });

  it('escapes unicode and preserves existing escapes', () => {
    expect(stringify({ é: 'café' })).toBe(JSON.stringify({ é: 'café' }));
  });
});
