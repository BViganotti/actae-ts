/**
 * JSON value model and wire-normalization helpers.
 *
 * Actae's server (via sonic-rs) wraps integers that cannot be represented
 * exactly as JSON numbers in a marker object:
 *   {"$sonic_rs::private::JsonNumber": "12345678901234567890"}
 *
 * Plain `JSON.parse` would silently round such values (they exceed
 * `Number.MAX_SAFE_INTEGER`), so this module ships:
 *
 *   - `parseJson` — a number-aware JSON parser that keeps out-of-safe-range
 *     integers as `bigint` (never silently precision-rounded).
 *   - `unwrapSonic` — recursively replaces the marker objects with native
 *     `number` / `bigint` / numeric `string` values.
 *   - `asInt64` / `asBigInt` / `asString` / `asMap` / `asList` — accessor
 *     helpers mirroring the Go SDK's `AsInt64`/`AsString`/`AsMap`/`AsList`
 *     ergonomics for payload values whose stored type varies (int64 vs
 *     float64 depending on how the server serialized them).
 */

export type JsonPrimitive = null | boolean | number | string | bigint;
export type JsonValue = JsonPrimitive | JsonValue[] | JsonObject;
export interface JsonObject {
  [key: string]: JsonValue;
}

export const SONIC_NUMBER_KEY = '$sonic_rs::private::JsonNumber';

const MAX_SAFE = Number.MAX_SAFE_INTEGER; // 2^53 - 1
const MIN_SAFE = Number.MIN_SAFE_INTEGER; // -(2^53 - 1)
const INT_RE = /^-?\d+$/;

// ---------------------------------------------------------------------------
// Number parsing
// ---------------------------------------------------------------------------

type NumResult = { ok: true; value: number | bigint } | { ok: false };

/** Parses a numeric string to number/bigint. Integral values within the safe
 * integer range become `number`; larger integral values become `bigint` (so
 * precision is never silently lost). Non-integral values become `number`. */
export function parseNumericString(raw: string): NumResult {
  if (raw === '' || raw === '-' || raw === '+') return { ok: false };
  if (INT_RE.test(raw)) {
    const n = Number(raw);
    if (Number.isSafeInteger(n)) return { ok: true, value: n };
    try {
      return { ok: true, value: BigInt(raw) };
    } catch {
      return { ok: false };
    }
  }
  const f = Number(raw);
  if (Number.isNaN(f)) return { ok: false };
  return { ok: true, value: f };
}

// ---------------------------------------------------------------------------
// Sonic-rs marker unwrapping
// ---------------------------------------------------------------------------

/** Recursively replaces sonic-rs number markers
 * ({"$sonic_rs::private::JsonNumber": "…"}) with native number/bigint values,
 * mirroring the Python SDK's `_unwrap_sonic`. Values that fail conversion are
 * left untouched. */
export function unwrapSonic(v: unknown): JsonValue {
  if (Array.isArray(v)) {
    return v.map(unwrapSonic) as JsonValue[];
  }
  if (v !== null && typeof v === 'object') {
    const obj = v as Record<string, unknown>;
    const keys = Object.keys(obj);
    if (keys.length === 1 && keys[0] === SONIC_NUMBER_KEY) {
      const raw = obj[SONIC_NUMBER_KEY];
      if (typeof raw === 'string' && raw !== '') {
        const num = parseNumericString(raw);
        if (num.ok) return num.value;
      }
      return obj as unknown as JsonValue;
    }
    const out: JsonObject = {};
    for (const k of keys) {
      out[k] = unwrapSonic(obj[k]);
    }
    return out;
  }
  return v as JsonValue;
}

// ---------------------------------------------------------------------------
// Number-aware JSON parser
// ---------------------------------------------------------------------------

class JsonParseError extends SyntaxError {}

/** Parses a JSON document, keeping integers outside the safe integer range as
 * `bigint`. Sonic-rs markers are unwrapped in the same pass. Throws
 * `SyntaxError` on malformed input. */
export function parseJson(raw: string): JsonValue {
  const p = new Parser(raw);
  const value = p.parseValue();
  p.skipWs();
  if (!p.atEnd()) {
    throw p.error('Unexpected trailing characters');
  }
  return unwrapSonic(value);
}

class Parser {
  private pos = 0;

  constructor(private readonly src: string) {}

  error(msg: string): JsonParseError {
    return new JsonParseError(`${msg} at position ${this.pos}`);
  }

  atEnd(): boolean {
    return this.pos >= this.src.length;
  }

  peek(): string {
    return this.src[this.pos] ?? '';
  }

  skipWs(): void {
    while (this.pos < this.src.length) {
      const c = this.src[this.pos];
      if (c === ' ' || c === '\t' || c === '\n' || c === '\r') {
        this.pos++;
      } else {
        break;
      }
    }
  }

  parseValue(): JsonValue {
    this.skipWs();
    const c = this.peek();
    switch (c) {
      case '{':
        return this.parseObject();
      case '[':
        return this.parseArray();
      case '"':
        return this.parseString();
      case 't':
        this.expectLiteral('true');
        return true;
      case 'f':
        this.expectLiteral('false');
        return false;
      case 'n':
        this.expectLiteral('null');
        return null;
      default:
        if (c === '-' || (c >= '0' && c <= '9')) {
          return this.parseNumber();
        }
        throw this.error(`Unexpected token '${c}'`);
    }
  }

  private expectLiteral(lit: string): void {
    if (this.src.slice(this.pos, this.pos + lit.length) !== lit) {
      throw this.error(`Unexpected token, expected '${lit}'`);
    }
    this.pos += lit.length;
  }

  private parseObject(): JsonObject {
    this.pos++; // consume '{'
    const out: JsonObject = {};
    this.skipWs();
    if (this.peek() === '}') {
      this.pos++;
      return out;
    }
    for (;;) {
      this.skipWs();
      if (this.peek() !== '"') {
        throw this.error('Expected string key');
      }
      const key = this.parseString();
      this.skipWs();
      if (this.peek() !== ':') {
        throw this.error("Expected ':'");
      }
      this.pos++;
      out[key] = this.parseValue();
      this.skipWs();
      const c = this.peek();
      if (c === ',') {
        this.pos++;
        continue;
      }
      if (c === '}') {
        this.pos++;
        return out;
      }
      throw this.error("Expected ',' or '}'");
    }
  }

  private parseArray(): JsonValue[] {
    this.pos++; // consume '['
    const out: JsonValue[] = [];
    this.skipWs();
    if (this.peek() === ']') {
      this.pos++;
      return out;
    }
    for (;;) {
      out.push(this.parseValue());
      this.skipWs();
      const c = this.peek();
      if (c === ',') {
        this.pos++;
        continue;
      }
      if (c === ']') {
        this.pos++;
        return out;
      }
      throw this.error("Expected ',' or ']'");
    }
  }

  private parseString(): string {
    if (this.peek() !== '"') {
      throw this.error('Expected string');
    }
    this.pos++; // consume opening quote
    let out = '';
    for (;;) {
      if (this.pos >= this.src.length) {
        throw this.error('Unterminated string');
      }
      const c = this.src[this.pos];
      if (c === '"') {
        this.pos++;
        return out;
      }
      if (c === '\\') {
        this.pos++;
        const esc = this.src[this.pos];
        switch (esc) {
          case '"':
            out += '"';
            break;
          case '\\':
            out += '\\';
            break;
          case '/':
            out += '/';
            break;
          case 'b':
            out += '\b';
            break;
          case 'f':
            out += '\f';
            break;
          case 'n':
            out += '\n';
            break;
          case 'r':
            out += '\r';
            break;
          case 't':
            out += '\t';
            break;
          case 'u': {
            const hex = this.src.slice(this.pos + 1, this.pos + 5);
            if (!/^[0-9a-fA-F]{4}$/.test(hex)) {
              throw this.error('Invalid unicode escape');
            }
            out += String.fromCharCode(parseInt(hex, 16));
            this.pos += 4;
            break;
          }
          default:
            throw this.error(`Invalid escape '\\${esc}'`);
        }
        this.pos++;
        continue;
      }
      out += c;
      this.pos++;
    }
  }

  private parseNumber(): number | bigint {
    const start = this.pos;
    if (this.peek() === '-') {
      this.pos++;
    }
    // integer part
    const intStart = this.pos;
    while (this.pos < this.src.length && /[0-9]/.test(this.src[this.pos] ?? '')) {
      this.pos++;
    }
    if (this.pos === intStart) {
      throw this.error('Invalid number');
    }
    let isFloat = false;
    // fraction
    if (this.peek() === '.') {
      isFloat = true;
      this.pos++;
      const fracStart = this.pos;
      while (this.pos < this.src.length && /[0-9]/.test(this.src[this.pos] ?? '')) {
        this.pos++;
      }
      if (this.pos === fracStart) {
        throw this.error('Invalid number (missing fraction digits)');
      }
    }
    // exponent
    const c = this.peek();
    if (c === 'e' || c === 'E') {
      isFloat = true;
      this.pos++;
      const sign = this.peek();
      if (sign === '+' || sign === '-') {
        this.pos++;
      }
      const expStart = this.pos;
      while (this.pos < this.src.length && /[0-9]/.test(this.src[this.pos] ?? '')) {
        this.pos++;
      }
      if (this.pos === expStart) {
        throw this.error('Invalid number (missing exponent digits)');
      }
    }
    const token = this.src.slice(start, this.pos);
    if (!isFloat) {
      const n = Number(token);
      if (Number.isSafeInteger(n)) return n;
      try {
        return BigInt(token);
      } catch {
        return n;
      }
    }
    const f = Number(token);
    if (Number.isNaN(f)) {
      throw this.error(`Invalid number '${token}'`);
    }
    return f;
  }
}

// ---------------------------------------------------------------------------
// Typed accessors (Go SDK As* parity)
// ---------------------------------------------------------------------------

/** Returns `v` as a number when it is an integer (number with an integral
 * value, bigint within the safe range, or numeric string); otherwise `def`. */
export function asInt64(v: unknown, def = 0): number {
  switch (typeof v) {
    case 'number':
      return Number.isFinite(v) ? Math.trunc(v) : def;
    case 'bigint': {
      const n = Number(v);
      return Number.isSafeInteger(n) ? n : def;
    }
    case 'string': {
      const trimmed = v.trim();
      if (trimmed === '') return def;
      const n = Number(trimmed);
      if (Number.isSafeInteger(n)) return n;
      try {
        const b = BigInt(trimmed);
        const nb = Number(b);
        return Number.isSafeInteger(nb) ? nb : def;
      } catch {
        return def;
      }
    }
    default:
      return def;
  }
}

/** Returns `v` as a bigint when it is an integer (number/bigint/numeric
 * string); otherwise null. */
export function asBigInt(v: unknown): bigint | null {
  switch (typeof v) {
    case 'bigint':
      return v;
    case 'number':
      return Number.isSafeInteger(v) ? BigInt(v) : null;
    case 'string': {
      const trimmed = v.trim();
      if (trimmed === '') return null;
      try {
        return BigInt(trimmed);
      } catch {
        return null;
      }
    }
    default:
      return null;
  }
}

/** Returns `v` as a float when it is numeric; otherwise `def`. */
export function asFloat(v: unknown, def = 0): number {
  switch (typeof v) {
    case 'number':
      return Number.isFinite(v) ? v : def;
    case 'bigint':
      return Number(v);
    case 'string': {
      const n = Number(v.trim());
      return Number.isNaN(n) ? def : n;
    }
    default:
      return def;
  }
}

/** Returns `v` as a string, or `def` when it is not one. */
export function asString(v: unknown, def = ''): string {
  return typeof v === 'string' ? v : def;
}

/** Returns `v` as a JsonObject, or undefined when it is not one. */
export function asMap(v: unknown): JsonObject | undefined {
  if (v !== null && typeof v === 'object' && !Array.isArray(v)) {
    return v as JsonObject;
  }
  return undefined;
}

/** Returns `v` as a JsonObject[] (empty when not an array). */
export function asList(v: unknown): JsonValue[] {
  return Array.isArray(v) ? (v as JsonValue[]) : [];
}

/** Returns `v` as a boolean, or `def` when it is not one. */
export function asBool(v: unknown, def = false): boolean {
  return typeof v === 'boolean' ? v : def;
}

/** Returns `v` as a string array when it is an array of strings (diff entry
 * paths), otherwise undefined. */
export function asStringList(v: unknown): string[] | undefined {
  if (!Array.isArray(v)) return undefined;
  const out: string[] = [];
  for (const item of v) {
    if (typeof item === 'string') out.push(item);
  }
  return out;
}

// ---------------------------------------------------------------------------
// BigInt-aware JSON serialization
// ---------------------------------------------------------------------------

/**
 * Serializes a value to JSON, emitting `bigint` values as raw JSON number
 * tokens (e.g. `12345678901234567890`) instead of throwing like
 * `JSON.stringify` does. This mirrors how the Python SDK (arbitrary-precision
 * int) and the Go SDK (int64) serialize large integers on the wire, so values
 * received from `parseJson` can be round-tripped back without precision loss.
 *
 * Semantics:
 *   - bigint → bare number token (no quotes, no rounding)
 *   - Date → ISO 8601 string
 *   - undefined / function / symbol → null in arrays, omitted in objects
 *   - cyclic references → TypeError
 */
export function stringify(value: unknown): string {
  const seen = new Set<object>();
  const serialize = (v: unknown, inArray: boolean): string => {
    if (v === null) return 'null';
    switch (typeof v) {
      case 'boolean':
        return v ? 'true' : 'false';
      case 'number':
        if (Number.isFinite(v)) return String(v);
        return 'null';
      case 'bigint':
        return v.toString();
      case 'string':
        return quote(v);
      case 'undefined':
      case 'function':
      case 'symbol':
        return inArray ? 'null' : '';
      case 'object': {
        if (v instanceof Date) return quote(v.toISOString());
        if (v instanceof RegExp) return quote(v.source);
        if (typeof (v as { toJSON?: unknown }).toJSON === 'function') {
          return serialize((v as { toJSON: () => unknown }).toJSON(), inArray);
        }
        if (seen.has(v)) throw new TypeError('cyclic reference in JSON serialization');
        seen.add(v);
        try {
          if (Array.isArray(v)) {
            const items = v.map((item) => serialize(item, true) || 'null');
            return `[${items.join(',')}]`;
          }
          const entries: string[] = [];
          for (const [k, val] of Object.entries(v as Record<string, unknown>)) {
            const s = serialize(val, false);
            if (s !== '') entries.push(`${quote(k)}:${s}`);
          }
          return `{${entries.join(',')}}`;
        } finally {
          seen.delete(v);
        }
      }
      default:
        return 'null';
    }
  };
  return serialize(value, false);
}

const ESCAPE_RE = /["\\\u0000-\u001f]/g;
const ESCAPE_MAP: Record<string, string> = {
  '"': '\\"',
  '\\': '\\\\',
  '\b': '\\b',
  '\f': '\\f',
  '\n': '\\n',
  '\r': '\\r',
  '\t': '\\t',
};

function quote(s: string): string {
  return `"${s.replace(ESCAPE_RE, (c) => {
    if (ESCAPE_MAP[c] !== undefined) return ESCAPE_MAP[c];
    const code = c.charCodeAt(0);
    return `\\u${code.toString(16).padStart(4, '0')}`;
  })}"`;
}
