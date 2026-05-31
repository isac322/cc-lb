import { type ChildProcess, spawn } from 'node:child_process';
import fs from 'node:fs';
import net from 'node:net';
import path from 'node:path';
import { test as base, expect as baseExpect } from '@playwright/test';
import { Seeder } from './helpers/seed';

export const ADMIN_TOKEN = 'test-admin-token';
export const MASTER_KEY =
  '0000000000000000000000000000000000000000000000000000000000000000';
export const TEST_API_KEY_ENV = 'TEST_API_KEY';
export const TEST_API_KEY_VALUE = 'sk-ant-test-api-key-value';

/**
 * Worker-scoped fixtures. Each Playwright worker (one Node process) starts:
 *   - one `fake-anthropic` upstream stub,
 *   - one `cc-lb-server` (debug binary, redb storage in a temp data dir),
 *   - seeds the default `dummy` upstream via the admin REST API.
 *
 * Tests within the worker run in parallel browser contexts but share the
 * same backend. Cross-test isolation is achieved by:
 *   (a) creating fresh contexts (no cookie/localStorage bleed-over);
 *   (b) the test author using unique IDs / cleanup helpers when mutating
 *       shared backend state — see `e2e/helpers/seed.ts`.
 *
 * Per-worker port layout (workerIndex = N):
 *   - fake-anthropic:  8081 + N*10
 *   - proxy:           8080 + N*10
 *   - admin (also serves the dashboard SPA): 8082 + N*10
 *   - metrics:         9091 + N*10
 *
 * Each worker's data lives at `<workspaceRoot>/data-test-worker-<N>/`.
 */
export type WorkerBackend = {
  proxyUrl: string;
  adminUrl: string;
  fakeUrl: string;
  metricsUrl: string;
  dataDir: string;
  workerIndex: number;
  adminToken: string;
};

type Fixtures = Record<string, never>;
type WorkerFixtures = {
  backend: WorkerBackend;
  /** Convenience: per-page localStorage seeding + navigation. */
  adminPage: undefined;
  /** Pre-bound seeder against this worker's admin port. */
  seeder: Seeder;
};

export const test = base.extend<Fixtures, WorkerFixtures>({
  backend: [
    async ({}, use, workerInfo) => {
      const backend = await startWorkerBackend(workerInfo.workerIndex);
      try {
        await use(backend);
      } finally {
        await stopWorkerBackend(backend);
      }
    },
    { scope: 'worker', timeout: 60_000 },
  ],

  seeder: [
    async ({ backend }, use) => {
      await use(new Seeder(backend.adminUrl, backend.adminToken));
    },
    { scope: 'worker' },
  ],

  // Per-test convenience: configure a page so the admin token is preloaded.
  // Note: this is a TEST-scoped fixture but takes the worker `backend`
  // automatically through Playwright's fixture graph.
  adminPage: [
    async ({ page, backend }, use) => {
      await page.addInitScript(
        ([token]) => {
          try {
            globalThis.localStorage.setItem('cc-lb-admin-token', token as string);
          } catch {
            // localStorage may throw in incognito / restricted modes.
          }
        },
        [backend.adminToken],
      );
      await use(undefined);
    },
    { scope: 'test', auto: false },
  ],
});

export const expect = baseExpect;

// ---------- internals ----------

const WORKSPACE_ROOT = path.resolve(__dirname, '../../../..');

const childRegistry = new Map<number, { server: ChildProcess; fake: ChildProcess }>();

async function startWorkerBackend(workerIndex: number): Promise<WorkerBackend> {
  const proxyPort = 8080 + workerIndex * 10;
  const fakePort = 8081 + workerIndex * 10;
  const adminPort = 8082 + workerIndex * 10;
  const metricsPort = 9091 + workerIndex * 10;

  for (const p of [proxyPort, fakePort, adminPort, metricsPort]) {
    await ensurePortFree(p);
  }

  const dataDir = path.join(WORKSPACE_ROOT, `data-test-worker-${workerIndex}`);
  if (fs.existsSync(dataDir)) {
    fs.rmSync(dataDir, { recursive: true, force: true });
  }
  fs.mkdirSync(dataDir, { recursive: true });

  const configPath = path.join(dataDir, 'cc-lb-config.toml');
  fs.writeFileSync(
    configPath,
    `
[runtime]
data_dir = "${dataDir}"

[storage]
kind = "redb"
path = "${dataDir}/storage.redb"

[listener]
proxy_addr = "127.0.0.1:${proxyPort}"
admin_addr = "127.0.0.1:${adminPort}"
metrics_addr = "127.0.0.1:${metricsPort}"

[admin]
token_env = "CC_LB_ADMIN_TOKEN"

[api_keys.price_catalog]
cache_path = "${dataDir}/litellm.json"

[oauth.anthropic]
client_id = "test-client"
client_secret = "test-secret"
auth_url = "http://127.0.0.1:${fakePort}/oauth/authorize"
token_url = "http://127.0.0.1:${fakePort}/oauth/token"
redirect_uri = "http://127.0.0.1:${adminPort}/admin/oauth/callback"
scopes = ["messages", "files"]
`.trim() + '\n',
  );

  const env = {
    ...process.env,
    RUST_LOG: process.env.RUST_LOG ?? 'warn',
    CC_LB_MASTER_KEY: MASTER_KEY,
    CC_LB_ADMIN_TOKEN: ADMIN_TOKEN,
    CC_LB_BOOTSTRAP_ADMIN_TOKEN: ADMIN_TOKEN,
    [TEST_API_KEY_ENV]: TEST_API_KEY_VALUE,
  };

  const serverBin = path.join(WORKSPACE_ROOT, 'target/debug/cc-lb-server');
  const fakeBin = path.join(WORKSPACE_ROOT, 'target/debug/fake-anthropic');

  // 1) fake-anthropic first so cc-lb-server's OAuth endpoints can reach it.
  const fake = spawn(fakeBin, ['--port', String(fakePort)], {
    cwd: WORKSPACE_ROOT,
    stdio: 'ignore',
    env,
  });
  fake.on('error', (err) => console.error(`[worker ${workerIndex}] fake-anthropic error: ${err}`));

  // 2) cc-lb-server.
  const server = spawn(serverBin, ['serve', '--config', configPath], {
    cwd: WORKSPACE_ROOT,
    stdio: process.env.CC_LB_E2E_VERBOSE === '1' ? 'inherit' : 'ignore',
    env,
  });
  server.on('error', (err) => console.error(`[worker ${workerIndex}] cc-lb-server error: ${err}`));

  childRegistry.set(workerIndex, { server, fake });

  const adminUrl = `http://127.0.0.1:${adminPort}`;
  const proxyUrl = `http://127.0.0.1:${proxyPort}`;
  const fakeUrl = `http://127.0.0.1:${fakePort}`;
  const metricsUrl = `http://127.0.0.1:${metricsPort}`;

  await waitForAdminReady(adminUrl, ADMIN_TOKEN);
  await seedDummyUpstream(adminUrl, ADMIN_TOKEN, fakeUrl);

  return {
    proxyUrl,
    adminUrl,
    fakeUrl,
    metricsUrl,
    dataDir,
    workerIndex,
    adminToken: ADMIN_TOKEN,
  };
}

async function stopWorkerBackend(backend: WorkerBackend) {
  const procs = childRegistry.get(backend.workerIndex);
  if (procs) {
    childRegistry.delete(backend.workerIndex);
    for (const proc of [procs.server, procs.fake]) {
      if (proc.pid && !proc.killed) {
        try {
          proc.kill('SIGTERM');
        } catch (err) {
          console.warn(`[worker ${backend.workerIndex}] kill failed: ${err}`);
        }
      }
    }
    // Give them ~300ms to exit, then SIGKILL.
    await new Promise((r) => setTimeout(r, 300));
    for (const proc of [procs.server, procs.fake]) {
      if (proc.pid && !proc.killed) {
        try {
          proc.kill('SIGKILL');
        } catch {
          // ignore
        }
      }
    }
  }
  if (process.env.CC_LB_E2E_KEEP_DATA !== '1') {
    try {
      fs.rmSync(backend.dataDir, { recursive: true, force: true });
    } catch (err) {
      console.warn(`[worker ${backend.workerIndex}] data-dir cleanup failed: ${err}`);
    }
  }
}

async function waitForAdminReady(adminUrl: string, token: string): Promise<void> {
  const deadline = Date.now() + 45_000;
  let lastErr: unknown;
  while (Date.now() < deadline) {
    try {
      const res = await fetch(`${adminUrl}/admin/v1/status`, {
        headers: { Authorization: `Bearer ${token}` },
      });
      if (res.ok) return;
      lastErr = `status ${res.status}`;
    } catch (err) {
      lastErr = err;
    }
    await new Promise((r) => setTimeout(r, 250));
  }
  throw new Error(`admin API at ${adminUrl} did not become ready: ${String(lastErr)}`);
}

async function seedDummyUpstream(
  adminUrl: string,
  token: string,
  fakeUrl: string,
): Promise<void> {
  const res = await fetch(`${adminUrl}/admin/v1/upstreams`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      name: 'dummy',
      kind: 'custom',
      base_url: fakeUrl,
      api_key_env: TEST_API_KEY_ENV,
    }),
  });
  if (!res.ok) {
    const detail = await res.text().catch(() => '');
    throw new Error(`failed to seed dummy upstream: ${res.status} ${detail}`);
  }
}

async function ensurePortFree(port: number): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const probe = net.createServer();
    probe.unref();
    probe.once('error', (err: NodeJS.ErrnoException) => {
      if (err.code === 'EADDRINUSE') {
        reject(new Error(`port ${port} already in use; another test run may be active`));
      } else {
        // Any other error — treat as free (best-effort).
        resolve();
      }
    });
    probe.listen(port, '127.0.0.1', () => {
      probe.close(() => resolve());
    });
  });
}
