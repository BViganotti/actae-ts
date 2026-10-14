import { afterEach, describe, expect, it } from 'vitest';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { loadClientCredentials } from '../src/credentials.js';
import { ActaeClient, newClientFromEnv } from '../src/client.js';

const savedCfg = process.env['ACTAE_CONFIG_DIR'];

function makeDir(body?: string): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'actae-ts-cred-'));
  if (body !== undefined) {
    fs.writeFileSync(path.join(dir, 'client.json'), body, { mode: 0o600 });
  }
  return dir;
}

afterEach(() => {
  if (savedCfg === undefined) delete process.env['ACTAE_CONFIG_DIR'];
  else process.env['ACTAE_CONFIG_DIR'] = savedCfg;
  delete process.env['ACTAE_API_KEY'];
  delete process.env['ACTAE_URL'];
  delete process.env['ACTAE_WS_URL'];
});

describe('loadClientCredentials', () => {
  it('returns undefined when missing', () => {
    process.env['ACTAE_CONFIG_DIR'] = makeDir();
    expect(loadClientCredentials()).toBeUndefined();
  });

  it('parses a valid file and strips a trailing slash', () => {
    process.env['ACTAE_CONFIG_DIR'] = makeDir(
      JSON.stringify({ schema: 'actae.client/v1', endpoint: 'http://127.0.0.1:8002/', api_key: 'sk-abc' }),
    );
    expect(loadClientCredentials()).toEqual({
      endpoint: 'http://127.0.0.1:8002',
      apiKey: 'sk-abc',
    });
  });

  it.each([
    '{not json',
    '[]',
    '"str"',
    '{"endpoint":"ftp://x","api_key":"sk-1"}',
    '{"endpoint":"http://","api_key":"sk-1"}',
    '{"endpoint":"http://127.0.0.1:8002","api_key":""}',
    '{"endpoint":"http://127.0.0.1:8002","api_key":"sk a b"}',
    '{"endpoint":"http://127.0.0.1:8002"}',
  ])('treats invalid payload as absent: %s', (body) => {
    process.env['ACTAE_CONFIG_DIR'] = makeDir(body);
    expect(loadClientCredentials()).toBeUndefined();
  });

  it('falls back to a single per-instance file', () => {
    const dir = makeDir();
    fs.writeFileSync(
      path.join(dir, 'client-8003.json'),
      JSON.stringify({ endpoint: 'http://127.0.0.1:8003', api_key: 'sk-secondary' }),
      { mode: 0o600 },
    );
    process.env['ACTAE_CONFIG_DIR'] = dir;
    expect(loadClientCredentials()).toEqual({
      endpoint: 'http://127.0.0.1:8003',
      apiKey: 'sk-secondary',
    });
  });

  it('ignores multiple per-instance files', () => {
    const dir = makeDir();
    for (const port of [8003, 8004]) {
      fs.writeFileSync(
        path.join(dir, `client-${port}.json`),
        JSON.stringify({ endpoint: `http://127.0.0.1:${port}`, api_key: 'sk-x' }),
        { mode: 0o600 },
      );
    }
    process.env['ACTAE_CONFIG_DIR'] = dir;
    expect(loadClientCredentials()).toBeUndefined();
  });

  it('prefers client.json over a secondary', () => {
    const dir = makeDir(
      JSON.stringify({ endpoint: 'http://127.0.0.1:8002', api_key: 'sk-default' }),
    );
    fs.writeFileSync(
      path.join(dir, 'client-8003.json'),
      JSON.stringify({ endpoint: 'http://127.0.0.1:8003', api_key: 'sk-secondary' }),
      { mode: 0o600 },
    );
    process.env['ACTAE_CONFIG_DIR'] = dir;
    expect(loadClientCredentials()?.apiKey).toBe('sk-default');
  });

  it('refuses to follow a symlink', () => {
    const dir = makeDir();
    const real = path.join(dir, 'real.json');
    fs.writeFileSync(real, JSON.stringify({ endpoint: 'http://evil', api_key: 'sk-evil' }));
    try {
      fs.symlinkSync(real, path.join(dir, 'client.json'));
    } catch {
      return; // symlinks unsupported on this platform
    }
    process.env['ACTAE_CONFIG_DIR'] = dir;
    expect(loadClientCredentials()).toBeUndefined();
  });
});

describe('client uses the credentials file', () => {
  it('bare constructor resolves from the file', () => {
    process.env['ACTAE_CONFIG_DIR'] = makeDir(
      JSON.stringify({ endpoint: 'http://127.0.0.1:8002', api_key: 'sk-from-file' }),
    );
    const c = new ActaeClient({});
    expect(c.httpEndpoint).toBe('http://127.0.0.1:8002');
    expect(c.wsEndpoint).toBe('ws://127.0.0.1:8002/ws');
  });

  it('env wins over the file', () => {
    process.env['ACTAE_CONFIG_DIR'] = makeDir(
      JSON.stringify({ endpoint: 'http://127.0.0.1:8002', api_key: 'sk-from-file' }),
    );
    process.env['ACTAE_API_KEY'] = 'sk-from-env';
    process.env['ACTAE_URL'] = 'http://env-host:9000';
    const c = newClientFromEnv();
    expect(c.httpEndpoint).toBe('http://env-host:9000');
  });

  it('explicit options win over the file', () => {
    process.env['ACTAE_CONFIG_DIR'] = makeDir(
      JSON.stringify({ endpoint: 'http://127.0.0.1:8002', api_key: 'sk-from-file' }),
    );
    const c = new ActaeClient({ apiKey: 'sk-explicit', endpoint: 'http://host:9999' });
    expect(c.httpEndpoint).toBe('http://host:9999');
  });

  it('missing everything throws an actionable error', () => {
    process.env['ACTAE_CONFIG_DIR'] = makeDir();
    expect(() => new ActaeClient({})).toThrow(/actae login/);
  });
});
