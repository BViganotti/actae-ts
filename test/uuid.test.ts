import { describe, it, expect } from 'vitest';
import { newUUID } from '../src/uuid.js';

describe('newUUID', () => {
  it('returns RFC4122 v4 UUIDs', () => {
    for (let i = 0; i < 100; i++) {
      const id = newUUID();
      expect(id).toMatch(
        /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
      );
    }
  });

  it('produces unique ids', () => {
    const seen = new Set<string>();
    for (let i = 0; i < 1000; i++) seen.add(newUUID());
    expect(seen.size).toBe(1000);
  });
});
