/**
 * Local client credentials (`$ACTAE_CONFIG_DIR/client.json`, default
 * `~/.actae/client.json`).
 *
 * Written by the `actae` server on first boot (and by `actae login`), this file
 * lets a self-hosted SDK authenticate with zero configuration. The Rust writer
 * is authoritative (see `docs/CLIENT_CREDENTIALS.md`); this reader mirrors its
 * validation: a missing, unreadable, symlinked, malformed, or invalid file is
 * treated as absent so callers can raise an actionable error.
 */

import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

export interface LocalClientCredentials {
  endpoint: string;
  apiKey: string;
}

/** The per-user Actae config directory (`ACTAE_CONFIG_DIR`, default `~/.actae`). */
export function configDir(): string {
  const dir = process.env['ACTAE_CONFIG_DIR'];
  if (dir && dir.length > 0) return dir;
  try {
    return path.join(os.homedir(), '.actae');
  } catch {
    return '';
  }
}

function validEndpoint(endpoint: string): boolean {
  if (!endpoint.startsWith('http://') && !endpoint.startsWith('https://')) return false;
  if (endpoint.length <= 'https://'.length) return false;
  for (const ch of endpoint) {
    const code = ch.codePointAt(0)!;
    if (code <= 0x20 || code === 0x7f) return false;
  }
  return true;
}

function validApiKey(key: string): boolean {
  if (key.length === 0) return false;
  for (const ch of key) {
    const code = ch.codePointAt(0)!;
    if (code <= 0x20 || code === 0x7f) return false;
  }
  return true;
}

/** Parse one credentials file; `undefined` when absent/unreadable/symlink/invalid. */
export function loadCredentialsFile(file: string): LocalClientCredentials | undefined {
  // Refuse to follow a symlink (parity with the Rust writer's O_NOFOLLOW).
  let stats: fs.Stats;
  try {
    stats = fs.lstatSync(file);
  } catch {
    return undefined;
  }
  if (stats.isSymbolicLink()) return undefined;

  let raw: string;
  try {
    raw = fs.readFileSync(file, 'utf8');
  } catch {
    return undefined;
  }
  let data: unknown;
  try {
    data = JSON.parse(raw);
  } catch {
    return undefined;
  }
  if (typeof data !== 'object' || data === null || Array.isArray(data)) return undefined;
  const record = data as Record<string, unknown>;
  const endpoint =
    typeof record['endpoint'] === 'string' ? record['endpoint'].trim().replace(/\/+$/, '') : '';
  const apiKey = typeof record['api_key'] === 'string' ? record['api_key'] : '';
  if (!validEndpoint(endpoint) || !validApiKey(apiKey)) return undefined;
  return { endpoint, apiKey };
}

/**
 * Read the local client credentials, or `undefined` when absent/invalid.
 *
 * Reads `client.json`; when it is absent but exactly one per-instance
 * `client-<port>.json` exists (a machine running more than one instance), that
 * file is used. Multiple secondaries are ambiguous and ignored.
 */
export function loadClientCredentials(): LocalClientCredentials | undefined {
  const dir = configDir();
  if (dir.length === 0) return undefined;

  const direct = loadCredentialsFile(path.join(dir, 'client.json'));
  if (direct !== undefined) return direct;

  let secondaries: string[];
  try {
    secondaries = fs
      .readdirSync(dir)
      .filter((name) => name.startsWith('client-') && name.endsWith('.json'))
      .sort();
  } catch {
    return undefined;
  }
  if (secondaries.length !== 1) return undefined;
  return loadCredentialsFile(path.join(dir, secondaries[0]!));
}
