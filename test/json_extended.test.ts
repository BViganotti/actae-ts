import { describe, expect, it } from 'vitest';
import {
  asBigInt,
  asFloat,
  asInt64,
  parseJson,
  parseNumericString,
  SONIC_NUMBER_KEY,
  stringify,
  unwrapSonic,
} from '../src/json.js';

describe('parseJson error forks', () => {
  it('rejects trailing characters', () => {
    expect(() => parseJson('{} extra')).toThrow(/trailing/);
  });
  it('rejects an object missing a colon', () => {
    expect(() => parseJson('{"a" 1}')).toThrow(/'/);
  });
  it('rejects an object missing a comma or brace', () => {
    expect(() => parseJson('{"a":1 "b":2}')).toThrow(/,|\}/);
  });
  it('rejects an array missing a comma or bracket', () => {
    expect(() => parseJson('[1 2]')).toThrow(/,|\]/);
  });
  it('rejects a bare number missing digits', () => {
    expect(() => parseJson('1.')).toThrow(/fraction/);
    expect(() => parseJson('1e')).toThrow(/exponent/);
  });
  it('rejects an exponent without digits after the sign', () => {
    expect(() => parseJson('1e+')).toThrow(/exponent/);
  });
  it('parses huge-exponent floats to Infinity', () => {
    expect(parseJson('1e999')).toBe(Infinity);
  });
  it('rejects unexpected tokens', () => {
    expect(() => parseJson('x')).toThrow(/Unexpected token/);
    expect(() => parseJson('{')).toThrow(/Expected string key/);
    expect(() => parseJson('[1,'.slice(0, 3) + '}')).toThrow();
  });
  it('rejects invalid literals', () => {
    expect(() => parseJson('tru')).toThrow(/expected 'true'/);
  });
});

describe('parseString escape forks', () => {
  it('decodes every short escape', () => {
    expect(parseJson('"a\\\\b"')).toBe('a\\b');
    expect(parseJson('"a\\/b"')).toBe('a/b');
    expect(parseJson('"\\b"')).toBe('\b');
    expect(parseJson('"\\f"')).toBe('\f');
    expect(parseJson('"\\n"')).toBe('\n');
    expect(parseJson('"\\r"')).toBe('\r');
    expect(parseJson('"\\t"')).toBe('\t');
  });
  it('decodes unicode escapes', () => {
    expect(parseJson('"\\u0041"')).toBe('A');
    expect(parseJson('"\\u00e9"')).toBe('é');
  });
  it('rejects an invalid unicode escape', () => {
    expect(() => parseJson('"\\u12"')).toThrow(/unicode/);
  });
  it('rejects an invalid escape', () => {
    expect(() => parseJson('"\\q"')).toThrow(/Invalid escape/);
  });
  it('rejects an unterminated string', () => {
    expect(() => parseJson('"abc')).toThrow(/Unterminated/);
  });
});

describe('parseNumericString + accessor edge forks', () => {
  it('rejects empty / sign-only strings', () => {
    expect(parseNumericString('')).toEqual({ ok: false });
    expect(parseNumericString('-')).toEqual({ ok: false });
    expect(parseNumericString('+')).toEqual({ ok: false });
  });
  it('asInt64 returns default for non-numeric and unsafe bigints', () => {
    expect(asInt64('   ')).toBe(0);
    expect(asInt64('not-a-number')).toBe(0);
    expect(asInt64(9007199254740993n)).toBe(0); // out of safe range
    expect(asInt64('9007199254740993')).toBe(0); // big-int string out of safe range
    expect(asInt64(undefined)).toBe(0);
  });
  it('asInt64 truncates floats and handles infinity', () => {
    expect(asInt64(3.9)).toBe(3);
    expect(asInt64(Infinity)).toBe(0);
  });
  it('asBigInt returns null for non-integers', () => {
    expect(asBigInt(1.5)).toBeNull();
    expect(asBigInt('')).toBeNull();
    expect(asBigInt('abc')).toBeNull();
    expect(asBigInt(null)).toBeNull();
  });
  it('asFloat converts bigint and rejects NaN strings', () => {
    expect(asFloat(10n)).toBe(10);
    expect(asFloat('abc')).toBe(0);
    expect(asFloat(NaN)).toBe(0);
    expect(asFloat(null)).toBe(0);
  });
});

describe('stringify edge forks', () => {
  it('serializes Date and RegExp', () => {
    expect(stringify(new Date('2026-01-01T00:00:00.000Z'))).toBe('"2026-01-01T00:00:00.000Z"');
    expect(stringify(/ab+c/)).toBe('"ab+c"');
  });
  it('uses toJSON when available', () => {
    expect(stringify({ toJSON: () => ({ ok: 1 }) })).toBe('{"ok":1}');
  });
  it('throws on cyclic references', () => {
    const a: Record<string, unknown> = {};
    a['self'] = a;
    expect(() => stringify(a)).toThrow(/cyclic/);
  });
  it('handles undefined/function/symbol in arrays and objects', () => {
    expect(stringify([undefined, function () {}, Symbol('x')])).toBe('[null,null,null]');
    expect(stringify({ a: undefined, b: 1 })).toBe('{"b":1}');
    expect(stringify({ a: () => {}, b: 2 })).toBe('{"b":2}');
  });
  it('serializes non-finite numbers as null', () => {
    expect(stringify(NaN)).toBe('null');
    expect(stringify(Infinity)).toBe('null');
  });
  it('serializes nested structures', () => {
    expect(stringify({ a: [1n, 'x', true, null] })).toBe('{"a":[1,"x",true,null]}');
  });
});

describe('unwrapSonic edge forks', () => {
  it('leaves markers with non-string values untouched', () => {
    const v = { [SONIC_NUMBER_KEY]: 0 };
    expect(unwrapSonic(v)).toEqual(v);
  });
  it('recurses into nested structures', () => {
    const out = unwrapSonic({
      a: [{ [SONIC_NUMBER_KEY]: '123' }],
      b: { nested: { [SONIC_NUMBER_KEY]: '9007199254740993' } },
    });
    expect((out as { a: unknown[] }).a[0]).toBe(123);
    expect(((out as { b: { nested: unknown } }).b as { nested: unknown }).nested).toBe(9007199254740993n);
  });
});
