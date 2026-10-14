import { afterEach, describe, expect, it } from 'vitest';
import { randomUUID } from 'node:crypto';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { newClientFromEnv } from '../src/client.js';

// Live, opt-in: proves the client authenticates from the credentials file alone
// (env credentials cleared). Enabled by ACTAE_LIVE=1 plus ACTAE_URL/ACTAE_API_KEY
// (the same gate as `npm run test:live`). Run:
//   ACTAE_LIVE=1 ACTAE_URL=http://localhost:8002 ACTAE_API_KEY=sk-dev-... \
//     npx vitest run test/credentials_live.test.ts
const liveUrl = process.env['ACTAE_URL'];
const liveKey = process.env['ACTAE_API_KEY'];
const live = process.env['ACTAE_LIVE'] === '1' && !!liveUrl && !!liveKey;

const savedEnv = {
  cfg: process.env['ACTAE_CONFIG_DIR'],
  url: process.env['ACTAE_URL'],
  key: process.env['ACTAE_API_KEY'],
  ws: process.env['ACTAE_WS_URL'],
};
afterEach(() => {
  for (const [name, value] of [
    ['ACTAE_CONFIG_DIR', savedEnv.cfg],
    ['ACTAE_URL', savedEnv.url],
    ['ACTAE_API_KEY', savedEnv.key],
    ['ACTAE_WS_URL', savedEnv.ws],
  ] as const) {
    if (value === undefined) delete process.env[name];
    else process.env[name] = value;
  }
});

describe('credentials file (live)', () => {
  it.skipIf(!live)('authenticates from the file alone', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'actae-ts-live-'));
    fs.writeFileSync(
      path.join(dir, 'client.json'),
      JSON.stringify({ endpoint: liveUrl, api_key: liveKey }),
      { mode: 0o600 },
    );
    process.env['ACTAE_CONFIG_DIR'] = dir;
    delete process.env['ACTAE_API_KEY'];
    delete process.env['ACTAE_URL'];
    delete process.env['ACTAE_WS_URL'];

    const client = newClientFromEnv();
    const channel = `sdk-creds-${randomUUID().replace(/-/g, '')}`;
    const event = await client.record(channel, 'sdk.credentials', { ok: true }, { actor: 'ts-live' });
    expect(event.eventType).toBe('sdk.credentials');
    expect(event.channelId).toBe(channel);
    expect(event.cursor).toBeGreaterThan(0);
  });
});
