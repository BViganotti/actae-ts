import { afterAll, describe, expect, it } from 'vitest';
import * as http from 'node:http';
import * as https from 'node:https';
import { execSync } from 'node:child_process';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import type { AddressInfo } from 'node:net';
import type { TLSSocket } from 'node:tls';
import { ActaeClient } from '../src/client.js';
import { ConnectionError } from '../src/errors.js';

/**
 * Real TLS/mTLS handshake tests: the SDK's `node:https` transport with a
 * self-signed CA and an mTLS client certificate. Certs are generated with
 * openssl at test time in a temp dir (gitignored, never committed).
 */

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'actae-tls-'));
const caPath = path.join(tmp, 'ca.crt');
const caKey = path.join(tmp, 'ca.key');
const serverCrt = path.join(tmp, 'server.crt');
const serverKey = path.join(tmp, 'server.key');
const clientCrt = path.join(tmp, 'client.crt');
const clientKey = path.join(tmp, 'client.key');

let opensslOk = false;
try {
  execSync('openssl version', { stdio: 'ignore' });
  opensslOk = true;
} catch {
  opensslOk = false;
}

function run(cmd: string) {
  execSync(cmd, { stdio: 'ignore' });
}

function genCerts() {
  run(`openssl req -x509 -newkey rsa:2048 -nodes -keyout ${caKey} -out ${caPath} -days 1 -subj "/CN=Test CA"`);
  run(`openssl req -newkey rsa:2048 -nodes -keyout ${serverKey} -out ${tmp}/server.csr -subj "/CN=localhost"`);
  run(`printf "subjectAltName=DNS:localhost,IP:127.0.0.1" > ${tmp}/ext.cnf`);
  run(`openssl x509 -req -in ${tmp}/server.csr -CA ${caPath} -CAkey ${caKey} -CAcreateserial -out ${serverCrt} -days 1 -extfile ${tmp}/ext.cnf`);
  run(`openssl req -newkey rsa:2048 -nodes -keyout ${clientKey} -out ${tmp}/client.csr -subj "/CN=actae-client"`);
  run(`openssl x509 -req -in ${tmp}/client.csr -CA ${caPath} -CAkey ${caKey} -CAcreateserial -out ${clientCrt} -days 1`);
}

if (opensslOk) genCerts();

afterAll(() => {
  if (opensslOk) {
    fs.rmSync(tmp, { recursive: true, force: true });
  }
});

const describeTls = opensslOk ? describe : describe.skip;

function plainServer(): Promise<{ server: https.Server; baseUrl: string }> {
  return new Promise((resolve) => {
    const server = https.createServer(
      { key: fs.readFileSync(serverKey), cert: fs.readFileSync(serverCrt) },
      (_req, res) => {
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ channels: ['tls-ok'] }));
      },
    );
    server.listen(0, '127.0.0.1', () => {
      const port = (server.address() as AddressInfo).port;
      resolve({ server, baseUrl: `https://127.0.0.1:${port}` });
    });
  });
}

describeTls('TLS transport (real handshake)', () => {
  it('rejects an untrusted server cert by default (rejectUnauthorized=true)', async () => {
    const { server, baseUrl } = await plainServer();
    try {
      const c = new ActaeClient({ apiKey: 'k', endpoint: baseUrl, timeout: 5000 });
      await expect(c.listChannels()).rejects.toBeInstanceOf(ConnectionError);
    } finally {
      await new Promise<void>((r) => server.close(() => r()));
    }
  });

  it('connects when the CA is trusted via tls.ca (Buffer and string forms)', async () => {
    const { server, baseUrl } = await plainServer();
    try {
      const viaBuffer = new ActaeClient({
        apiKey: 'k', endpoint: baseUrl, timeout: 5000,
        tls: { ca: fs.readFileSync(caPath) },
      });
      expect(await viaBuffer.listChannels()).toEqual(['tls-ok']);
      const viaString = new ActaeClient({
        apiKey: 'k', endpoint: baseUrl, timeout: 5000,
        tls: { ca: fs.readFileSync(caPath, 'utf8') },
      });
      expect(await viaString.listChannels()).toEqual(['tls-ok']);
    } finally {
      await new Promise<void>((r) => server.close(() => r()));
    }
  });

  it('fails when the CA does not match the server', async () => {
    const { server, baseUrl } = await plainServer();
    const other = fs.mkdtempSync(path.join(os.tmpdir(), 'actae-tls-other-'));
    const otherCa = path.join(other, 'ca.crt');
    try {
      run(`openssl req -x509 -newkey rsa:2048 -nodes -keyout ${other}/ca.key -out ${otherCa} -days 1 -subj "/CN=Other CA"`);
      const c = new ActaeClient({
        apiKey: 'k', endpoint: baseUrl, timeout: 5000,
        tls: { ca: fs.readFileSync(otherCa) },
      });
      await expect(c.listChannels()).rejects.toBeInstanceOf(ConnectionError);
    } finally {
      await new Promise<void>((r) => server.close(() => r()));
      fs.rmSync(other, { recursive: true, force: true });
    }
  });
});

describeTls('mTLS (client certificate presented to the server)', () => {
  it('presents the client cert; server observes the CN', async () => {
    let verified = false;
    const server = https.createServer(
      {
        key: fs.readFileSync(serverKey),
        cert: fs.readFileSync(serverCrt),
        ca: fs.readFileSync(caPath),
        requestCert: true,
        rejectUnauthorized: false,
      },
      (_req, res) => {
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ channels: ['mtls-ok'], client_verified: verified }));
      },
    );
    server.on('secureConnection', (socket: TLSSocket) => {
      verified = Boolean(socket.getPeerCertificate()?.subject?.CN === 'actae-client');
    });
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    const port = (server.address() as AddressInfo).port;
    const baseUrl = `https://127.0.0.1:${port}`;
    try {
      const c = new ActaeClient({
        apiKey: 'k',
        endpoint: baseUrl,
        timeout: 5000,
        tls: {
          ca: fs.readFileSync(caPath),
          cert: fs.readFileSync(clientCrt),
          key: fs.readFileSync(clientKey),
        },
      });
      const res = await c.listChannels();
      expect(res).toEqual(['mtls-ok']);
      expect(verified).toBe(true);
    } finally {
      await new Promise<void>((r) => server.close(() => r()));
    }
  });
});
