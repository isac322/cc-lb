import { existsSync, readFileSync, statSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { defineConfig } from '@playwright/test';

const qaDir = path.dirname(fileURLToPath(import.meta.url));
const webDir = path.resolve(qaDir, '..');
const repoRoot = path.resolve(qaDir, '../../../..');

function requiredEnv(name: string): string {
  const value = process.env[name];
  if (value === undefined || !value.trim()) {
    throw new Error(`${name} is required and must not be empty`);
  }
  return value.trim();
}

function optionalEnv(name: string): string | undefined {
  const value = process.env[name];
  if (value === undefined) return undefined;
  if (!value.trim()) throw new Error(`${name} must not be empty when set`);
  return value.trim();
}

function requiredChoice<const T extends readonly string[]>(
  name: string,
  choices: T,
): T[number] {
  const value = requiredEnv(name);
  if (!(choices as readonly string[]).includes(value)) {
    throw new Error(`${name} must be one of ${choices.join(', ')}`);
  }
  return value as T[number];
}

function loopbackOrigin(raw: string, name: string): string {
  const url = new URL(raw);
  if (!['127.0.0.1', 'localhost', '::1'].includes(url.hostname)) {
    throw new Error(`${name} must use a loopback host`);
  }
  if (
    url.username ||
    url.password ||
    url.pathname !== '/' ||
    url.search ||
    url.hash
  ) {
    throw new Error(`${name} must be a credential-free origin`);
  }
  return url.origin;
}

function requireBeneath(child: string, parent: string, name: string): void {
  const relative = path.relative(parent, child);
  if (!relative || relative.startsWith('..') || path.isAbsolute(relative)) {
    throw new Error(`${name} must be beneath ${parent}`);
  }
}

const scratchDir = path.resolve(
  optionalEnv('KEEPALIVE_SCRATCH_DIR') ??
    path.join(repoRoot, 'target', 'keepalive-qa'),
);
const phase = requiredChoice('KEEPALIVE_PHASE', [
  'baseline',
  'candidate',
] as const);
const engine = requiredChoice('KEEPALIVE_ENGINE', [
  'sqlite',
  'postgres',
] as const);
const backendUrl = loopbackOrigin(
  requiredEnv('KEEPALIVE_BACKEND_URL'),
  'KEEPALIVE_BACKEND_URL',
);
const configuredAdminUrl = optionalEnv('CC_LB_ADMIN_URL');
if (
  configuredAdminUrl &&
  loopbackOrigin(configuredAdminUrl, 'CC_LB_ADMIN_URL') !== backendUrl
) {
  throw new Error('CC_LB_ADMIN_URL must equal KEEPALIVE_BACKEND_URL');
}

const webUrl = loopbackOrigin(
  optionalEnv('KEEPALIVE_WEB_URL') ?? 'http://127.0.0.1:5174',
  'KEEPALIVE_WEB_URL',
);
const manifestPath = path.resolve(requiredEnv('KEEPALIVE_MANIFEST'));
if (!existsSync(manifestPath) || !statSync(manifestPath).isFile()) {
  throw new Error(
    `KEEPALIVE_MANIFEST must name an existing file: ${manifestPath}`,
  );
}
if (!readFileSync(manifestPath, 'utf8').trim()) {
  throw new Error('KEEPALIVE_MANIFEST must not be empty');
}

const evidenceDir = path.resolve(requiredEnv('KEEPALIVE_EVIDENCE_DIR'));
requireBeneath(evidenceDir, scratchDir, 'KEEPALIVE_EVIDENCE_DIR');
const evidencePhase = `${engine}-${phase}`;
if (path.basename(evidenceDir) !== evidencePhase) {
  throw new Error(
    `KEEPALIVE_EVIDENCE_DIR must end in ${evidencePhase} for this phase`,
  );
}

const externalWeb = optionalEnv('KEEPALIVE_EXTERNAL_WEB');
if (externalWeb !== undefined && externalWeb !== '1') {
  throw new Error('KEEPALIVE_EXTERNAL_WEB must be 1 when set');
}

export default defineConfig({
  testDir: qaDir,
  testMatch: 'keepalive-performance-regression.spec.ts',
  outputDir: path.join(evidenceDir, 'playwright-output'),
  fullyParallel: false,
  forbidOnly: true,
  retries: 0,
  workers: 1,
  reporter: [['line']],
  timeout: 15 * 60_000,
  expect: { timeout: 30_000 },
  use: {
    baseURL: webUrl,
    trace: 'off',
    screenshot: 'off',
    video: 'off',
  },
  webServer:
    externalWeb === '1'
      ? undefined
      : {
          command: 'bun run dev -- --host 127.0.0.1 --port 5174',
          cwd: webDir,
          env: {
            ...process.env,
            CC_LB_ADMIN_URL: backendUrl,
          },
          url: webUrl,
          reuseExistingServer: false,
          timeout: 120_000,
        },
});
