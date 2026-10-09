/**
 * Deterministic operation keys — the byte-identical cross-SDK scheme of the
 * Go SDK's `deterministic.go` (the reference) and the Python SDK's
 * `_step_operation_id`.
 *
 * A UUIDv5 over NUL-separated (scope, action, identity...) parts, capped at
 * 256 bytes of UTF-8. Retrying the same logical operation — from a library
 * retry loop or after a process restart — produces the exact same id, which
 * Actae's server-side idempotency (record operation_id / transition replay)
 * turns into "return the original result instead of duplicating".
 *
 * The canonical content serialization (`canonicalStepContent`) mirrors Go's
 * `json.Marshal` of `{"payload": ..., "metadata": ...}` byte-for-byte:
 *
 *   - payload FIRST (Go struct field order, not alphabetical),
 *   - all object keys recursively sorted (Go sorts map keys),
 *   - numbers use JS `String()` — empirically identical to Go's float
 *     encoder on every tested value, including `-0` → `0`, `1e-7` (no
 *     exponent padding) and `1e20` (fixed notation),
 *   - `bigint` → bare number token (Go int64 parity),
 *   - raw UTF-8 for non-ASCII, with `<`, `>`, `&`, U+2028/U+2029 escaped
 *     exactly like Go (`\u003c` etc.),
 *   - NaN/Infinity REJECTED (Go's `json.Marshal` errors) — the caller gets
 *     a loud failure instead of a silently divergent id.
 *
 * Known edge: object key ordering uses UTF-16 code-unit order; identical to
 * Go's UTF-8 byte order for all code points below U+10000. Keys in the
 * private-use range U+E000..U+1F5FF paired with astral-plane keys sort
 * differently — practically unreachable in step payloads.
 */

import { createHash } from 'node:crypto';
import type { JsonObject, JsonValue } from './json.js';

/** Fixed UUIDv5 namespace shared with the Go SDK's `actaeNamespace`
 * (3f74e5f1-9b2c-4a7e-8f1d-000000000001). */
const ACTAE_NAMESPACE = new Uint8Array([
  0x3f, 0x74, 0xe5, 0xf1, 0x9b, 0x2c, 0x4a, 0x7e,
  0x8f, 0x1d, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
]);

const nameByteCap = 256;

/** Returns a stable UUIDv5 operation id for the (scope, action,
 * identity...) triple. Identity parts are NUL-separated (overlapping part
 * boundaries cannot collide) and the input is capped at 256 UTF-8 bytes —
 * the byte-identical derivation of the Go SDK's
 * `DeterministicOperationKey`. */
export function deterministicOperationKey(
  scope: string,
  action: string,
  ...identity: string[]
): string {
  let name = scope + '\x00' + action;
  for (const part of identity) {
    name += '\x00' + part;
  }
  const bytes = Buffer.from(name, 'utf8');
  const capped = bytes.length > nameByteCap ? bytes.subarray(0, nameByteCap) : bytes;

  const h = createHash('sha1');
  h.update(ACTAE_NAMESPACE);
  h.update(capped);
  const digest = h.digest();

  digest[6] = (digest[6]! & 0x0f) | 0x50; // version 5
  digest[8] = (digest[8]! & 0x3f) | 0x80; // variant 10

  const hex = digest.toString('hex');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20, 32)}`;
}

/** Byte-identical serialization to the Go SDK's `canonicalStepContent`:
 * `{"payload":{...},"metadata":{...}}` with recursively sorted keys. */
export function canonicalStepContent(payload: JsonObject, metadata: JsonObject): string {
  return `{"payload":${marshal(payload)},"metadata":${marshal(metadata)}}`;
}

function marshal(v: JsonValue): string {
  switch (typeof v) {
    case 'boolean':
      return v ? 'true' : 'false';
    case 'number':
      if (!Number.isFinite(v)) {
        throw new Error(
          'NaN/Infinity in step payload/metadata — Go json.Marshal rejects it; ' +
            'the record would fail server-side',
        );
      }
      return String(v);
    case 'bigint':
      return v.toString();
    case 'string':
      return quote(v);
    case 'object':
      if (v === null) return 'null';
      if (Array.isArray(v)) {
        return `[${v.map((item) => marshal(item)).join(',')}]`;
      }
      const keys = Object.keys(v).sort();
      const entries: string[] = [];
      for (const k of keys) {
        entries.push(`${quote(k)}:${marshal((v as JsonObject)[k] as JsonValue)}`);
      }
      return `{${entries.join(',')}}`;
    default:
      throw new TypeError(
        `Unsupported value of type '${typeof v}' in step payload/metadata — Go json.Marshal rejects it`,
      );
  }
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

/** Quotes a string like Go's `json.Marshal` (named escapes for control
 * characters, `\u00XX` for the rest, raw UTF-8) PLUS Go's escaping of
 * `<`, `>`, `&`, U+2028 and U+2029. */
function quote(s: string): string {
  const escaped = `"${s.replace(ESCAPE_RE, (c) => {
    if (ESCAPE_MAP[c] !== undefined) return ESCAPE_MAP[c];
    const code = c.charCodeAt(0);
    return `\\u${code.toString(16).padStart(4, '0')}`;
  })}"`;
  return escaped
    .replace(/</g, '\\u003c')
    .replace(/>/g, '\\u003e')
    .replace(/&/g, '\\u0026')
    .replace(/\u2028/g, '\\u2028')
    .replace(/\u2029/g, '\\u2029');
}