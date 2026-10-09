import { randomUUID } from 'node:crypto';

/** Returns a random UUID v4 string, mirroring Python's `str(uuid.uuid4())`
 * and the Go SDK's `newUUID`. Used for operation ids (record/fork/publish
 * idempotency keys). */
export function newUUID(): string {
  return randomUUID();
}
