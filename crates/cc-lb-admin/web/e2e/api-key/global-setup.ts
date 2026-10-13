// Starts the real stack for the API-key suite and hands its addresses to the
// workers through `CCLB_E2E_STATE` (a JSON file). Readiness is polled over
// HTTP against a deadline; there are no fixed sleeps.

import { type ChildProcess, spawn, spawnSync } from 'node:child_process';
import {
  existsSync,
  mkdtempSync,
  openSync,
  readdirSync,
  readFileSync,
  statSync,
  writeFileSync,
} from 'node:fs';
import { createServer } from 'node:net';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { stopProcesses } from './global-teardown';
import type { E2eState } from './harness';

const here = path.dirname(fileURLToPath(import.meta.url));
const webDir = path.resolve(here, '../..');
const repoRoot = path.resolve(webDir, '../../..');

const ADMIN_TOKEN = 'e2e-admin-token-api-key-suite';
const MASTER_KEY = '0'.repeat(64);
/** PRE-01: strings only the deleted preview code carried. */
const PREVIEW_MARKERS =
  /preview-scenario-key|apiKeyScenarios|registerPreviewTransport|wrkspc_01Preview/;

function freePort(): Promise<number> {
  const { promise, resolve, reject } = Promise.withResolvers<number>();
  const server = createServer();
  server.unref();
  server.on('error', reject);
  server.listen(0, '127.0.0.1', () => {
    const address = server.address();
    server.close(() => {
      if (address && typeof address === 'object') resolve(address.port);
      else reject(new Error('no port'));
    });
  });
  return promise;
}

function binary(envName: string, name: string): string {
  const fromEnv = process.env[envName];
  const candidate = fromEnv ?? path.join(repoRoot, 'target', 'debug', name);
  if (!existsSync(candidate)) {
    throw new Error(
      `${candidate} is missing: run \`cargo build -p cc-lb-server -p fake-anthropic\` (or set ${envName})`,
    );
  }
  return candidate;
}

function filesUnder(dir: string): string[] {
  return readdirSync(dir).flatMap((entry) => {
    const full = path.join(dir, entry);
    return statSync(full).isDirectory() ? filesUnder(full) : [full];
  });
}

function buildAndCheckBundle(): void {
  const build = spawnSync('bunx', ['--bun', 'vite', 'build'], {
    cwd: webDir,
    stdio: 'inherit',
  });
  if (build.status !== 0) throw new Error('vite build failed');
  const offenders = filesUnder(path.join(webDir, 'dist')).filter((file) =>
    PREVIEW_MARKERS.test(readFileSync(file, 'latin1')),
  );
  if (offenders.length > 0) {
    throw new Error(`PRE-01: preview code in dist/: ${offenders.join(', ')}`);
  }
}

function start(
  name: string,
  command: string,
  args: string[],
  options: { cwd?: string; env?: NodeJS.ProcessEnv; logDir: string },
): ChildProcess {
  const log = openSync(path.join(options.logDir, `${name}.log`), 'a');
  const child = spawn(command, args, {
    cwd: options.cwd,
    env: options.env,
    stdio: ['ignore', log, log],
    detached: true,
  });
  child.unref();
  if (child.pid === undefined) throw new Error(`${name} did not start`);
  return child;
}

async function waitForHttp(url: string, name: string, logDir: string) {
  const deadline = Date.now() + 120_000;
  let lastError = '';
  while (Date.now() < deadline) {
    try {
      // Bound each probe by the time left, so a listener that accepts the
      // connection but never answers cannot hold setup past the deadline.
      const res = await fetch(url, {
        signal: AbortSignal.timeout(
          Math.max(1, Math.min(5_000, deadline - Date.now())),
        ),
      });
      // Only the status matters; release the connection before returning
      // or probing again.
      await res.body?.cancel();
      if (res.status < 500) return;
      lastError = `HTTP ${res.status}`;
    } catch (error) {
      lastError = String(error);
    }
    // Yield to the event loop between probes; readiness is the only gate.
    const { promise, resolve } = Promise.withResolvers<void>();
    setImmediate(resolve);
    await promise;
  }
  const log = path.join(logDir, `${name}.log`);
  throw new Error(
    `${name} not ready at ${url}: ${lastError}\n${existsSync(log) ? readFileSync(log, 'utf8') : ''}`,
  );
}

function ccLbConfig(p: {
  proxy: number;
  admin: number;
  metrics: number;
  fakeUrl: string;
  dir: string;
}): string {
  return `[listener]
proxy_addr = "127.0.0.1:${p.proxy}"
admin_addr = "127.0.0.1:${p.admin}"
metrics_addr = "127.0.0.1:${p.metrics}"

[body]
messages_cap_bytes = 33554432
files_cap_bytes = 104857600

[timeouts]
upstream_total_secs = 30
drain_secs = 1

[storage]
kind = "sqlite"
path = "${path.join(p.dir, 'cc-lb.sqlite')}"

[aead]
key_env = "CC_LB_MASTER_KEY"

[oauth.anthropic]
client_id = "fake-client"
auth_url = "${p.fakeUrl}/oauth/authorize"
token_url = "${p.fakeUrl}/oauth/token"
redirect_uri = "http://127.0.0.1:${p.admin}/oauth/callback"
scopes = ["messages", "files"]

[runtime]
data_dir = "${p.dir}"

[observability]
tracing_level = "info"
log_redaction = true
user_prompt_redaction = false

[[admin.auth.providers]]
kind = "static_token"
id = "e2e"
token_env = "CC_LB_ADMIN_TOKEN"
`;
}

export default async function globalSetup(): Promise<void> {
  buildAndCheckBundle();

  const dir = mkdtempSync(path.join(tmpdir(), 'cclb-e2e-api-key-'));
  const [fakePort, proxyPort, adminPort, metricsPort, webPort, deadPort] =
    await Promise.all(Array.from({ length: 6 }, () => freePort()));
  const fakeUrl = `http://127.0.0.1:${fakePort}`;
  const adminUrl = `http://127.0.0.1:${adminPort}`;
  const webUrl = `http://127.0.0.1:${webPort}`;
  const configPath = path.join(dir, 'cc-lb.toml');
  writeFileSync(
    configPath,
    ccLbConfig({
      proxy: proxyPort,
      admin: adminPort,
      metrics: metricsPort,
      fakeUrl,
      dir,
    }),
  );

  // Validate every binary before acquiring anything to clean up.
  const fakeBin = binary('FAKE_ANTHROPIC_BIN', 'fake-anthropic');
  const ccLbBin = binary('CC_LB_BIN', 'cc-lb');

  const state: E2eState = {
    adminUrl,
    webUrl,
    fakeUrl,
    deadUrl: `http://127.0.0.1:${deadPort}`,
    adminToken: ADMIN_TOKEN,
    pids: [],
    dir,
  };
  const statePath = path.join(dir, 'state.json');
  // Each started process is registered for teardown before the next step.
  const register = (child: ChildProcess) => {
    state.pids.push(child.pid ?? 0);
    writeFileSync(statePath, JSON.stringify(state));
  };
  writeFileSync(statePath, JSON.stringify(state));
  process.env.CCLB_E2E_STATE = statePath;

  try {
    register(
      start('fake-anthropic', fakeBin, ['--port', String(fakePort)], {
        env: process.env,
        logDir: dir,
      }),
    );
    register(
      start('cc-lb', ccLbBin, ['serve', '--config', configPath], {
        env: {
          ...process.env,
          CC_LB_ADMIN_TOKEN: ADMIN_TOKEN,
          CC_LB_MASTER_KEY: MASTER_KEY,
          RUST_LOG: 'info,hyper=warn,hyper_util=warn',
        },
        logDir: dir,
      }),
    );
    register(
      start(
        'vite-preview',
        'bunx',
        [
          '--bun',
          'vite',
          'preview',
          '--host',
          '127.0.0.1',
          '--port',
          String(webPort),
          '--strictPort',
        ],
        {
          cwd: webDir,
          env: { ...process.env, CC_LB_ADMIN_URL: adminUrl },
          logDir: dir,
        },
      ),
    );

    await waitForHttp(`${fakeUrl}/__requests`, 'fake-anthropic', dir);
    await waitForHttp(`${adminUrl}/admin/health`, 'cc-lb', dir);
    await waitForHttp(webUrl, 'vite-preview', dir);
  } catch (error) {
    // A failed setup may never reach teardown: stop what it started.
    await stopProcesses(state.pids);
    throw error;
  }
}
