import {
  expect,
  type BrowserContext,
  type Page,
  type Response,
  test,
} from '@playwright/test';
import { spawnSync } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import {
  appendFileSync,
  mkdirSync,
  openSync,
  readFileSync,
  statSync,
  writeFileSync,
  closeSync,
} from 'node:fs';
import path from 'node:path';
import { performance } from 'node:perf_hooks';
import { fileURLToPath } from 'node:url';
import * as z from 'zod';

const PHASE = requiredChoice('KEEPALIVE_PHASE', [
  'baseline',
  'candidate',
] as const);
const ENGINE = requiredChoice('KEEPALIVE_ENGINE', [
  'sqlite',
  'postgres',
] as const);
const BACKEND_URL = loopbackOrigin(requiredEnv('KEEPALIVE_BACKEND_URL'));
const WEB_URL = loopbackOrigin(
  process.env.KEEPALIVE_WEB_URL ?? 'http://127.0.0.1:5174',
);
const QA_DIR = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(QA_DIR, '../../../..');
const SCRATCH_DIR =
  process.env.KEEPALIVE_SCRATCH_DIR ??
  path.join(REPO_ROOT, 'target', 'keepalive-qa');
const MANIFEST_PATH =
  process.env.KEEPALIVE_MANIFEST ??
  path.join(SCRATCH_DIR, 'fixture-manifest.json');
const EVIDENCE_DIR =
  process.env.KEEPALIVE_EVIDENCE_DIR ?? path.join(SCRATCH_DIR, 'evidence');
const RUNNER =
  process.env.KEEPALIVE_RUNNER ?? path.join(QA_DIR, 'keepalive-qa-runner.py');
const PYTHON = process.env.KEEPALIVE_PYTHON ?? 'python3';
const TOKEN_FILE = process.env.KEEPALIVE_ADMIN_TOKEN_FILE;
const UI_VISIBILITY_MS = Number(
  process.env.KEEPALIVE_UI_VISIBILITY_MS ?? 30_000,
);
const PERF_HIDDEN_MS = Number(process.env.KEEPALIVE_PERF_HIDDEN_MS ?? 600_000);
const POLL_MS = 5_000;
const POLL_TIMEOUT_MS = 3 * POLL_MS;
if (!Number.isFinite(UI_VISIBILITY_MS) || UI_VISIBILITY_MS < 30_000) {
  throw new Error('KEEPALIVE_UI_VISIBILITY_MS must be at least 30000');
}
if (!Number.isFinite(PERF_HIDDEN_MS) || PERF_HIDDEN_MS < 600_000) {
  throw new Error('KEEPALIVE_PERF_HIDDEN_MS must be at least 600000');
}
const CURSOR_HORIZONS = ['24h', '7d', 'all'] as const;
const CURSOR_ORIGINAL_FIELDS = [
  'entry_id',
  'filter',
  'horizon_start_ms',
  'last_message_at_ms',
  'principal_id',
] as const;
const HORIZON_DURATION_MS = {
  '24h': 24 * 60 * 60 * 1_000,
  '7d': 7 * 24 * 60 * 60 * 1_000,
} as const;
type CursorHorizon = (typeof CURSOR_HORIZONS)[number];

const CursorPayloadSchema = z.object({
  horizon: z.enum(CURSOR_HORIZONS).optional(),
  principal_id: z.string(),
  horizon_start_ms: z.number().nullable(),
  filter: z.enum([
    'all',
    'renewed',
    'scheduled',
    'capped',
    'expired',
    'not_tracked',
    'error',
  ]),
  last_message_at_ms: z.number(),
  entry_id: z.string(),
});

const FixtureManifestSchema = z.object({
  fixture_version: z.literal(3),
  principals: z.object({
    empty: z.string(),
    low: z.string(),
    high_ui: z.string(),
    disabled: z.string(),
    d100k: z.string(),
    st_scale: z.string(),
  }),
  principal_names: z.record(z.string(), z.string()),
  low_ids: z.object({
    active: z.string(),
    scheduled: z.string(),
    terminal: z.string(),
    expired: z.string(),
    decision: z.string(),
    collision: z.string(),
    transition_decision: z.string(),
    transition_session: z.string(),
    cleanup: z.string(),
  }),
  datasets: z.record(
    z.string(),
    z.object({
      engine: z.enum(['sqlite', 'postgres']),
      phase: z.enum(['baseline', 'candidate']),
      server_url: z.string().nullable(),
      anchor_ms: z.number(),
      clock: z.object({
        mode: z.literal('live_utc_requests'),
        fixture_anchor_ms: z.number(),
        request_log_anchor: z.literal('started_at_utc'),
      }),
      lifecycle: z.object({
        expires_at_s: z.number(),
        active_enqueue_state: z.literal('enqueued'),
        run_at: z.literal('future'),
        updated_at_s: z.number(),
      }),
      runtime_profile: z.string(),
      fixture_sha256: z.string(),
      fixture_counts: z.record(z.string(), z.number()),
      expected_list_counts: z.object({
        high_ui: z.object({
          '24h': z.number(),
          '7d': z.number(),
          all: z.number(),
        }),
        d100k: z.object({ '24h': z.number() }),
      }),
    }),
  ),
});
type FixtureManifest = z.infer<typeof FixtureManifestSchema>;

const KeepaliveSummaryBodySchema = z.object({
  renewing_now: z.number(),
  sessions_last_5m: z.number(),
  renewals_fired: z.number(),
  cost_saved: z.number(),
});
const KeepaliveListBodySchema = z
  .object({
    summary: KeepaliveSummaryBodySchema,
    rows: z.array(z.object({ id: z.string() }).passthrough()),
    next_cursor: z.string().nullable(),
  })
  .passthrough();
const PrincipalListBodySchema = z
  .object({
    principals: z.array(
      z
        .object({
          id: z.string(),
          revision: z.number(),
          cache_keepalive: z.unknown(),
        })
        .passthrough(),
    ),
  })
  .passthrough();
const DetailTransitionBodySchema = z
  .object({ generation: z.number(), attempts: z.number().nullable() })
  .passthrough();

interface JsonObject {
  [key: string]: unknown;
}

const manifest = FixtureManifestSchema.parse(
  JSON.parse(readFileSync(MANIFEST_PATH, 'utf8')),
);
const dataset = manifest.datasets[`${ENGINE}:${PHASE}`];
if (!dataset) throw new Error(`manifest has no ${ENGINE}:${PHASE} dataset`);
if (dataset.server_url && loopbackOrigin(dataset.server_url) !== BACKEND_URL) {
  throw new Error('KEEPALIVE_BACKEND_URL does not match the prepared manifest');
}
if (dataset.clock.fixture_anchor_ms !== dataset.anchor_ms) {
  throw new Error(
    'manifest fixture anchor differs from clock.fixture_anchor_ms',
  );
}
const fixtureAnchorSeconds = Math.floor(dataset.anchor_ms / 1000);
if (dataset.lifecycle.updated_at_s !== fixtureAnchorSeconds) {
  throw new Error(
    'manifest lifecycle.updated_at_s must equal the fixture anchor',
  );
}
if (
  dataset.lifecycle.expires_at_s !==
  fixtureAnchorSeconds + 7 * 24 * 60 * 60
) {
  throw new Error(
    'manifest lifecycle.expires_at_s must be exactly seven days after the fixture anchor',
  );
}
mkdirSync(EVIDENCE_DIR, { recursive: true });
const RAW_JSONL = path.join(EVIDENCE_DIR, `browser-${ENGINE}-${PHASE}.jsonl`);
const MUTATION_JSONL = path.join(
  EVIDENCE_DIR,
  `mutations-${ENGINE}-${PHASE}.jsonl`,
);

function requiredEnv(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`${name} is required`);
  return value;
}

function requiredChoice<const T extends readonly string[]>(
  name: string,
  values: T,
): T[number] {
  const value = requiredEnv(name);
  const choices: readonly string[] = values;
  if (!choices.includes(value))
    throw new Error(`${name} must be one of ${values.join(', ')}`);
  return value as T[number];
}

function loopbackOrigin(raw: string): string {
  const parsed = new URL(raw);
  if (!['127.0.0.1', 'localhost', '::1'].includes(parsed.hostname)) {
    throw new Error(`non-loopback origin rejected: ${parsed.hostname}`);
  }
  if (
    parsed.username ||
    parsed.password ||
    parsed.pathname !== '/' ||
    parsed.search ||
    parsed.hash
  ) {
    throw new Error('server URL must be a credential-free origin');
  }
  return parsed.origin;
}

function adminToken(): string | null {
  if (!TOKEN_FILE) return null;
  const mode = statSync(TOKEN_FILE).mode & 0o777;
  if ((mode & 0o077) !== 0)
    throw new Error('KEEPALIVE_ADMIN_TOKEN_FILE must be mode 0600');
  return readFileSync(TOKEN_FILE, 'utf8').trim() || null;
}
function sanitizeTrace(tracePath: string): void {
  if (!TOKEN_FILE) return;
  const completed = spawnSync(
    PYTHON,
    [
      RUNNER,
      'evidence',
      '--manifest',
      MANIFEST_PATH,
      '--engine',
      ENGINE,
      '--phase',
      PHASE,
      '--sanitize-trace',
      tracePath,
      '--token-file',
      TOKEN_FILE,
    ],
    { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] },
  );
  if (completed.status !== 0) {
    throw new Error(`trace redaction failed: ${completed.stderr.trim()}`);
  }
}

function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`;
  if (value !== null && typeof value === 'object') {
    return `{${Object.entries(value)
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([key, item]) => `${JSON.stringify(key)}:${canonical(item)}`)
      .join(',')}}`;
  }
  const encoded = JSON.stringify(value);
  return encoded ?? 'null';
}

function sha256(value: string | Buffer): string {
  return createHash('sha256').update(value).digest('hex');
}

function sha256Json(value: unknown): string {
  return sha256(canonical(value));
}

function sha256Nullable(value: string | null | undefined): string | null {
  return value == null ? null : sha256(value);
}

function expectedHorizon(raw: string): CursorHorizon {
  const value = new URL(raw, BACKEND_URL).searchParams.get('horizon') ?? '24h';
  if (!CURSOR_HORIZONS.includes(value as CursorHorizon)) {
    throw new Error(`unsupported cursor horizon ${value}`);
  }
  return value as CursorHorizon;
}

function decodeCursor(cursor: string): z.infer<typeof CursorPayloadSchema> {
  let parsed: unknown;
  try {
    parsed = JSON.parse(Buffer.from(cursor, 'base64url').toString('utf8'));
  } catch (error) {
    throw new Error(`cursor is not valid base64url JSON: ${String(error)}`);
  }
  const payload = CursorPayloadSchema.parse(parsed);
  const expectedFields =
    PHASE === 'candidate'
      ? [...CURSOR_ORIGINAL_FIELDS, 'horizon'].sort()
      : [...CURSOR_ORIGINAL_FIELDS].sort();
  const actualFields = Object.keys(parsed as JsonObject).sort();
  if (canonical(actualFields) !== canonical(expectedFields)) {
    throw new Error(
      `${PHASE} cursor fields ${canonical(actualFields)} do not match ${canonical(expectedFields)}`,
    );
  }
  return payload;
}

function cursorOriginalFields(
  cursor: string,
  horizon: CursorHorizon,
): Omit<z.infer<typeof CursorPayloadSchema>, 'horizon'> {
  const payload = decodeCursor(cursor);
  if (PHASE === 'candidate' && payload.horizon !== horizon) {
    throw new Error(
      `candidate cursor horizon ${String(payload.horizon)} does not match ${horizon}`,
    );
  }
  if (PHASE === 'baseline' && payload.horizon !== undefined) {
    throw new Error('baseline cursor unexpectedly contains a horizon tag');
  }
  if (horizon === 'all' && payload.horizon_start_ms !== null) {
    throw new Error('all cursor must have a null horizon_start_ms');
  }
  if (horizon !== 'all' && payload.horizon_start_ms === null) {
    throw new Error(`${horizon} cursor must have a numeric horizon_start_ms`);
  }
  const { horizon: _horizon, ...original } = payload;
  return original;
}

function cursorEvidence(
  cursor: string | null,
  horizon: CursorHorizon,
): {
  originalFieldsSha: string | null;
  horizon: CursorHorizon | null;
} {
  if (cursor === null) return { originalFieldsSha: null, horizon: null };
  const original = cursorOriginalFields(cursor, horizon);
  return {
    originalFieldsSha: sha256Json(original),
    horizon: PHASE === 'candidate' ? horizon : null,
  };
}

async function waitPastCursorRequestSecond(
  page: Page,
  cursor: string,
  horizon: '24h' | '7d',
): Promise<void> {
  const original = cursorOriginalFields(cursor, horizon);
  if (original.horizon_start_ms === null) {
    throw new Error(`${horizon} cursor has no request-time anchor`);
  }
  const requestNowMs = original.horizon_start_ms + HORIZON_DURATION_MS[horizon];
  await page.waitForTimeout(Math.max(0, requestNowMs + 1_050 - Date.now()));
}

function responseError(body: unknown): unknown {
  return body !== null && typeof body === 'object' && 'error' in body
    ? body.error
    : undefined;
}

function iso(epochMs = Date.now()): string {
  return new Date(epochMs).toISOString();
}

function appendImmutableJsonl(file: string, record: JsonObject): void {
  const descriptor = openSync(file, 'a', 0o600);
  try {
    appendFileSync(descriptor, `${canonical(record)}\n`, { encoding: 'utf8' });
  } finally {
    closeSync(descriptor);
  }
}

function evidenceUrl(raw: string): {
  display: string;
  cursorSha: string | null;
} {
  const url = new URL(raw);
  const cursor = url.searchParams.get('cursor');
  if (cursor !== null)
    url.searchParams.set('cursor', `<sha256:${sha256(cursor)}>`);
  return { display: url.toString(), cursorSha: sha256Nullable(cursor) };
}

function jsonBody(bytes: Buffer): unknown {
  try {
    return JSON.parse(bytes.toString('utf8'));
  } catch {
    return { _non_json_body_sha256: sha256(bytes), _body_bytes: bytes.length };
  }
}

function keepaliveKind(url: URL): 'summary' | 'list' | 'detail' | null {
  const prefix = '/admin/v1/principals/';
  if (
    !url.pathname.startsWith(prefix) ||
    !url.pathname.includes('/cache-keepalive')
  )
    return null;
  const suffix = url.pathname.split('/cache-keepalive')[1];
  if (suffix && suffix !== '/') return 'detail';
  return url.searchParams.get('limit') === '0' ? 'summary' : 'list';
}

class BrowserNetworkRecorder {
  private pending = new Set<Promise<void>>();
  private failures: unknown[] = [];
  readonly observations: JsonObject[] = [];

  constructor(
    private readonly page: Page,
    private readonly caseId: string,
  ) {
    page.on('response', (response) => {
      const task = this.capture(response)
        .catch((error: unknown) => {
          this.failures.push(error);
        })
        .finally(() => this.pending.delete(task));
      this.pending.add(task);
    });
  }

  private async capture(response: Response): Promise<void> {
    const url = new URL(response.url());
    const kind = keepaliveKind(url);
    if (!kind) return;
    const request = response.request();
    const timing = request.timing();
    const bytes = Buffer.from(await response.body());
    const body = jsonBody(bytes);
    let nextCursor: string | null = null;
    if (body !== null && typeof body === 'object' && 'next_cursor' in body) {
      const candidate = body.next_cursor;
      if (candidate === null || typeof candidate === 'string')
        nextCursor = candidate;
    }
    const cursorMetadata =
      kind === 'list'
        ? cursorEvidence(nextCursor, expectedHorizon(response.url()))
        : { originalFieldsSha: null, horizon: null };
    const safeUrl = evidenceUrl(response.url());
    const responseStart =
      timing.responseStart >= 0 ? timing.responseStart : null;
    const requestStart = timing.startTime > 0 ? timing.startTime : Date.now();
    const record: JsonObject = {
      case_id: this.caseId,
      run_id: randomUUID(),
      phase: PHASE,
      engine: ENGINE,
      sample: null,
      cache_state: 'browser',
      runtime_profile: dataset.runtime_profile,
      started_at_utc: iso(requestStart),
      ended_at_utc: iso(),
      request: {
        method: request.method(),
        kind,
        exact_url_or_redacted_sha256: safeUrl.display,
        cursor_in_sha256: safeUrl.cursorSha,
      },
      response: {
        http_status: response.status(),
        canonical_body_sha256: sha256Json(body),
        body_bytes: bytes.length,
        cursor_out_sha256: sha256Nullable(nextCursor),
        cursor_out_original_fields_sha256: cursorMetadata.originalFieldsSha,
        cursor_out_horizon: cursorMetadata.horizon,
        ttfb_ms:
          responseStart === null
            ? null
            : Math.round(responseStart * 1_000) / 1_000,
        wall_ms: Math.max(
          0,
          Math.round((Date.now() - requestStart) * 1_000) / 1_000,
        ),
      },
      ui: null,
      backend: {
        sql_calls: null,
        sql_time_ms: null,
        pool_wait_ms: null,
        plan_artifact: null,
      },
    };
    this.observations.push(record);
    appendImmutableJsonl(RAW_JSONL, record);
  }

  async flush(): Promise<void> {
    await Promise.all([...this.pending]);
    if (this.failures.length > 0) {
      const failures = this.failures.splice(0);
      throw new AggregateError(failures, 'browser network evidence capture failed');
    }
  }
}

const recorders = new WeakMap<Page, BrowserNetworkRecorder>();
const tracePaths = new WeakMap<BrowserContext, string>();

test.beforeEach(async ({ page, context }, testInfo) => {
  await page.goto(WEB_URL);
  const required = page.getByRole('heading', { name: 'Admin token required' });
  const tokenFormVisible = await required
    .waitFor({ state: 'visible', timeout: 10_000 })
    .then(() => true)
    .catch(() => false);
  if (tokenFormVisible) {
    const token = adminToken();
    if (!token)
      throw new Error(
        'admin token is required; set KEEPALIVE_ADMIN_TOKEN_FILE',
      );
    await page.getByLabel('Bearer token').fill(token);
    await page.getByRole('button', { name: 'Sign in' }).click();
    await expect(required).toBeHidden();
  }
  const caseId = caseIdFromTitle(testInfo.title);
  const runDir = path.join(
    EVIDENCE_DIR,
    caseId,
    `${ENGINE}-${PHASE}-${randomUUID()}`,
  );
  mkdirSync(runDir, { recursive: true });
  const tracePath = path.join(runDir, 'trace.zip');
  await context.tracing.start({
    screenshots: true,
    snapshots: true,
    sources: true,
  });
  tracePaths.set(context, tracePath);
  recorders.set(page, new BrowserNetworkRecorder(page, caseId));
});

test.afterEach(async ({ page, context }, testInfo) => {
  await recorders.get(page)?.flush();
  const caseId = caseIdFromTitle(testInfo.title);
  const screenshotDir = path.join(EVIDENCE_DIR, caseId);
  mkdirSync(screenshotDir, { recursive: true });
  let screenshotPath: string | null = path.join(
    screenshotDir,
    `${ENGINE}-${PHASE}-test-end-${randomUUID()}.png`,
  );
  let screenshotError: string | null = null;
  try {
    await page.screenshot({ path: screenshotPath, fullPage: false });
  } catch (error) {
    screenshotPath = null;
    screenshotError = error instanceof Error ? error.message : String(error);
  }
  const tracePath = tracePaths.get(context);
  if (tracePath) {
    await context.tracing.stop({ path: tracePath });
    sanitizeTrace(tracePath);
  }
  appendImmutableJsonl(RAW_JSONL, {
    case_id: caseId,
    run_id: randomUUID(),
    phase: PHASE,
    engine: ENGINE,
    sample: null,
    cache_state: 'browser_test_end',
    runtime_profile: dataset.runtime_profile,
    started_at_utc: iso(),
    ended_at_utc: iso(),
    request: {
      method: 'TEST_END_OBSERVATION',
      exact_url_or_redacted_sha256: page.url(),
      cursor_in_sha256: null,
    },
    response: {
      http_status: null,
      canonical_body_sha256: null,
      body_bytes: null,
      cursor_out_sha256: null,
      ttfb_ms: null,
      wall_ms: null,
    },
    ui: {
      observed_at_utc: iso(),
      visible_row_ids_sha256: sha256Json(
        await visibleIds(page).catch(() => []),
      ),
      screenshot_path: screenshotPath,
      screenshot_error: screenshotError,
      test_status: testInfo.status,
      expected_status: testInfo.expectedStatus,
      trace_path: tracePath ?? null,
    },
    backend: {
      sql_calls: null,
      sql_time_ms: null,
      pool_wait_ms: null,
      plan_artifact: null,
    },
  });
});

function caseIdFromTitle(title: string): string {
  const match = /(?:CUR|UI|PERF|FLOW)-\d{2}/.exec(title);
  if (!match) throw new Error(`test title lacks QA ID: ${title}`);
  return match[0];
}

async function openPrincipal(
  page: Page,
  key: keyof FixtureManifest['principals'],
): Promise<void> {
  await page.goto(
    `${WEB_URL}/principals?selectedId=${manifest.principals[key]}`,
  );
  await expect(
    page.getByRole('heading', { name: manifest.principal_names[key] }),
  ).toBeVisible();
  await expect(page.getByTestId('cache-keepalive-card')).toBeVisible();
}

function listResponseMatches(
  response: Response,
  principalId: string,
  horizon: string,
  filter: string,
): boolean {
  const url = new URL(response.url());
  if (response.request().method() !== 'GET') return false;
  if (url.pathname !== `/admin/v1/principals/${principalId}/cache-keepalive`)
    return false;
  if (url.searchParams.has('limit')) return false;
  if ((url.searchParams.get('horizon') ?? '24h') !== horizon) return false;
  if (filter === 'all')
    return !url.searchParams.has('status') && !url.searchParams.has('error');
  if (filter === 'error')
    return (
      url.searchParams.get('error') === 'true' &&
      !url.searchParams.has('status')
    );
  return (
    url.searchParams.get('status') === filter && !url.searchParams.has('error')
  );
}

function summaryResponseMatches(
  response: Response,
  principalId: string,
): boolean {
  const url = new URL(response.url());
  return (
    response.request().method() === 'GET' &&
    url.pathname === `/admin/v1/principals/${principalId}/cache-keepalive` &&
    url.searchParams.get('limit') === '0'
  );
}

async function openDrawer(
  page: Page,
  key: keyof FixtureManifest['principals'] = 'high_ui',
): Promise<Response> {
  await openPrincipal(page, key);
  const principal = manifest.principals[key];
  const response = page.waitForResponse((item) =>
    listResponseMatches(item, principal, '24h', 'all'),
  );
  await page
    .getByTestId('cache-keepalive-card')
    .getByRole('button', { name: 'Sessions' })
    .click();
  const received = await response;
  await expect(
    page.getByTestId('cache-keepalive-sessions-drawer'),
  ).toBeVisible();
  return received;
}

function drawer(page: Page) {
  return page.getByTestId('cache-keepalive-sessions-drawer');
}

function horizonButton(page: Page, label: '24h' | '7d' | 'All') {
  return drawer(page)
    .locator('header')
    .getByRole('button', { name: label, exact: true });
}

function filterButton(page: Page, label: string) {
  return drawer(page)
    .locator('button[aria-pressed]')
    .filter({ hasText: new RegExp(`^${label}$`) });
}

async function visibleIds(page: Page): Promise<string[]> {
  return drawer(page)
    .locator('li[data-key]')
    .evaluateAll((nodes) =>
      nodes.map((node) => node.getAttribute('data-key') ?? ''),
    );
}

async function directRequest(
  caseId: string,
  method: 'GET' | 'PATCH',
  apiPath: string,
  options: { body?: unknown; ifMatch?: number } = {},
): Promise<{ status: number; body: unknown; bytes: Buffer; headers: Headers }> {
  const token = adminToken();
  const headers = new Headers({ Accept: 'application/json' });
  if (token) headers.set('Authorization', `Bearer ${token}`);
  if (options.body !== undefined)
    headers.set('Content-Type', 'application/json');
  if (options.ifMatch !== undefined)
    headers.set('If-Match', `W/"${options.ifMatch}"`);
  const startedAt = Date.now();
  const started = performance.now();
  const response = await fetch(`${BACKEND_URL}${apiPath}`, {
    method,
    headers,
    body: options.body === undefined ? undefined : JSON.stringify(options.body),
  });
  const ttfb = performance.now() - started;
  const bytes = Buffer.from(await response.arrayBuffer());
  const wall = performance.now() - started;
  const body = jsonBody(bytes);
  let nextCursor: string | null = null;
  if (body !== null && typeof body === 'object' && 'next_cursor' in body) {
    const candidate = body.next_cursor;
    if (candidate === null || typeof candidate === 'string')
      nextCursor = candidate;
  }
  const cursorMetadata =
    nextCursor === null
      ? { originalFieldsSha: null, horizon: null }
      : cursorEvidence(nextCursor, expectedHorizon(apiPath));
  const safeUrl = evidenceUrl(`${BACKEND_URL}${apiPath}`);
  appendImmutableJsonl(RAW_JSONL, {
    case_id: caseId,
    run_id: randomUUID(),
    phase: PHASE,
    engine: ENGINE,
    sample: null,
    cache_state: 'independent_direct',
    runtime_profile: dataset.runtime_profile,
    started_at_utc: iso(startedAt),
    ended_at_utc: iso(),
    request: {
      method,
      exact_url_or_redacted_sha256: safeUrl.display,
      cursor_in_sha256: safeUrl.cursorSha,
    },
    response: {
      http_status: response.status,
      canonical_body_sha256: sha256Json(body),
      body_bytes: bytes.length,
      cursor_out_sha256: sha256Nullable(nextCursor),
      cursor_out_original_fields_sha256: cursorMetadata.originalFieldsSha,
      cursor_out_horizon: cursorMetadata.horizon,
      ttfb_ms: Math.round(ttfb * 1_000) / 1_000,
      wall_ms: Math.round(wall * 1_000) / 1_000,
    },
    ui: null,
    backend: {
      sql_calls: null,
      sql_time_ms: null,
      pool_wait_ms: null,
      plan_artifact: null,
    },
  });
  return { status: response.status, body, bytes, headers: response.headers };
}

async function uiObservation(
  page: Page,
  caseId: string,
  label: string,
  startedAt: number,
  firstPaintMs: number,
  stablePaintMs: number,
): Promise<void> {
  const runId = randomUUID();
  const directory = path.join(EVIDENCE_DIR, caseId);
  mkdirSync(directory, { recursive: true });
  const screenshotPath = path.join(
    directory,
    `${ENGINE}-${PHASE}-${label}-${runId}.png`,
  );
  await page.screenshot({ path: screenshotPath, fullPage: false });
  const ids = await visibleIds(page).catch(() => []);
  appendImmutableJsonl(RAW_JSONL, {
    case_id: caseId,
    run_id: runId,
    phase: PHASE,
    engine: ENGINE,
    sample: null,
    cache_state: 'browser_ui',
    runtime_profile: dataset.runtime_profile,
    started_at_utc: iso(startedAt),
    ended_at_utc: iso(),
    request: {
      method: 'DOM_OBSERVATION',
      exact_url_or_redacted_sha256: page.url(),
      cursor_in_sha256: null,
    },
    response: {
      http_status: null,
      canonical_body_sha256: null,
      body_bytes: null,
      cursor_out_sha256: null,
      ttfb_ms: null,
      wall_ms: null,
    },
    ui: {
      observed_at_utc: iso(),
      first_paint_ms: Math.round(firstPaintMs * 1_000) / 1_000,
      stable_paint_ms: Math.round(stablePaintMs * 1_000) / 1_000,
      visible_row_ids_sha256: sha256Json(ids),
      screenshot_path: screenshotPath,
      label,
    },
    backend: {
      sql_calls: null,
      sql_time_ms: null,
      pool_wait_ms: null,
      plan_artifact: null,
    },
  });
}

function applyMutation(
  caseId: string,
  mutation: 'bump-summary' | 'reorder' | 'late-turn' | 'reactivate' | 'cleanup',
): void {
  const completed = spawnSync(
    PYTHON,
    [
      RUNNER,
      'evidence',
      '--manifest',
      MANIFEST_PATH,
      '--engine',
      ENGINE,
      '--phase',
      PHASE,
      '--mutation',
      mutation,
      '--case-id',
      caseId,
      '--records',
      MUTATION_JSONL,
    ],
    { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] },
  );
  if (completed.status !== 0)
    throw new Error(`fixture mutation failed: ${completed.stderr.trim()}`);
}

async function selectHorizon(
  page: Page,
  value: '24h' | '7d' | 'all',
  filter = 'all',
): Promise<Response> {
  const label = value === 'all' ? 'All' : value;
  const response = page.waitForResponse((item) =>
    listResponseMatches(item, manifest.principals.high_ui, value, filter),
  );
  await horizonButton(page, label).click();
  return response;
}

async function selectFilter(
  page: Page,
  filter: string,
  horizon = '24h',
): Promise<Response> {
  const labels: Record<string, string> = {
    all: 'All',
    renewed: 'Renewed',
    scheduled: 'Scheduled',
    capped: 'Capped',
    expired: 'Expired',
    not_tracked: 'Not tracked',
    error: 'Error',
  };
  const response = page.waitForResponse((item) =>
    listResponseMatches(item, manifest.principals.high_ui, horizon, filter),
  );
  await filterButton(page, labels[filter]).click();
  return response;
}

async function clickRow(
  page: Page,
  id: string,
  principal = manifest.principals.low,
): Promise<Response> {
  const encoded = encodeURIComponent(id);
  const expectedPath = `/admin/v1/principals/${principal}/cache-keepalive/${encoded}`;
  const response = page.waitForResponse((item) => {
    const url = new URL(item.url());
    return item.request().method() === 'GET' && url.pathname === expectedPath;
  });
  await drawer(page)
    .locator(`li[data-key=${JSON.stringify(id)}]`)
    .getByRole('button')
    .click();
  const received = await response;
  expect(received.request().url()).toContain(encoded);
  return received;
}

async function pollResponse(
  page: Page,
  predicate: (response: Response) => boolean,
): Promise<Response> {
  return page.waitForResponse(predicate, { timeout: POLL_TIMEOUT_MS });
}

async function setDocumentVisibility(
  page: Page,
  state: 'visible' | 'hidden',
): Promise<void> {
  await page.evaluate((nextState) => {
    Object.defineProperty(document, 'visibilityState', {
      configurable: true,
      get: () => nextState,
    });
    Object.defineProperty(document, 'hidden', {
      configurable: true,
      get: () => nextState === 'hidden',
    });
    document.dispatchEvent(new Event('visibilitychange'));
  }, state);
}

async function networkCount(page: Page, durationMs: number): Promise<number> {
  let count = 0;
  const listener = (response: Response) => {
    if (keepaliveKind(new URL(response.url()))) count += 1;
  };
  page.on('response', listener);
  await page.waitForTimeout(durationMs);
  page.off('response', listener);
  return count;
}

async function fullDirectPagination(
  caseId: string,
  principalId: string,
  horizon: CursorHorizon,
): Promise<{ ids: string[]; pageCount: number }> {
  let cursor: string | null = null;
  let pageCount = 0;
  const seenCursors = new Set<string>();
  const ids: string[] = [];
  let frozenHorizonStart: number | null | undefined;
  while (true) {
    const params = new URLSearchParams({ horizon });
    if (cursor) params.set('cursor', cursor);
    const response = await directRequest(
      caseId,
      'GET',
      `/admin/v1/principals/${principalId}/cache-keepalive?${params}`,
    );
    expect(response.status).toBe(200);
    const body = KeepaliveListBodySchema.parse(response.body);
    ids.push(...body.rows.map((row) => row.id));
    pageCount += 1;
    if (body.next_cursor === null) break;
    const original = cursorOriginalFields(body.next_cursor, horizon);
    frozenHorizonStart ??= original.horizon_start_ms;
    expect(original.horizon_start_ms).toBe(frozenHorizonStart);
    if (seenCursors.has(body.next_cursor)) {
      throw new Error(
        'direct pagination cursor cycle detected before terminal null',
      );
    }
    expect(sha256(body.next_cursor)).not.toBe(sha256Nullable(cursor));
    seenCursors.add(body.next_cursor);
    cursor = body.next_cursor;
  }
  return { ids, pageCount };
}

test('CUR-01 keeps 24h and 7d UI page chains valid after the request clock advances', async ({
  page,
}) => {
  for (const horizon of ['24h', '7d'] as const) {
    let firstResponse = await openDrawer(page);
    if (horizon === '7d') firstResponse = await selectHorizon(page, '7d');
    expect(firstResponse.status()).toBe(200);
    const firstBody = KeepaliveListBodySchema.parse(await firstResponse.json());
    expect(firstBody.next_cursor).not.toBeNull();
    const firstCursor = firstBody.next_cursor!;
    const firstOriginal = cursorOriginalFields(firstCursor, horizon);
    await waitPastCursorRequestSecond(page, firstCursor, horizon);
    const responsePromise = page.waitForResponse((response) => {
      const url = new URL(response.url());
      return (
        listResponseMatches(
          response,
          manifest.principals.high_ui,
          horizon,
          'all',
        ) && url.searchParams.get('cursor') === firstCursor
      );
    });
    await drawer(page)
      .getByRole('button', { name: 'Loading older sessions...' })
      .click();
    const secondResponse = await responsePromise;
    const secondBody = await secondResponse.json();
    if (PHASE === 'baseline') {
      expect(secondResponse.status()).toBe(400);
      expect(responseError(secondBody)).toBe('invalid_input');
    } else {
      expect(secondResponse.status()).toBe(200);
      const secondPage = KeepaliveListBodySchema.parse(secondBody);
      expect(secondPage.next_cursor).not.toBeNull();
      const secondOriginal = cursorOriginalFields(
        secondPage.next_cursor!,
        horizon,
      );
      expect(secondOriginal.horizon_start_ms).toBe(
        firstOriginal.horizon_start_ms,
      );
    }
  }
});

test('CUR-02 keeps valid all pagination and rejects horizon, principal, and filter mismatches', async () => {
  const cursors = new Map<CursorHorizon, string>();
  for (const horizon of CURSOR_HORIZONS) {
    const response = await directRequest(
      'CUR-02',
      'GET',
      `/admin/v1/principals/${manifest.principals.high_ui}/cache-keepalive?horizon=${horizon}&limit=1`,
    );
    expect(response.status).toBe(200);
    const body = KeepaliveListBodySchema.parse(response.body);
    expect(body.next_cursor).not.toBeNull();
    cursorOriginalFields(body.next_cursor!, horizon);
    cursors.set(horizon, body.next_cursor!);
  }

  const allCursor = cursors.get('all')!;
  const valid = await directRequest(
    'CUR-02',
    'GET',
    `/admin/v1/principals/${manifest.principals.high_ui}/cache-keepalive?horizon=all&limit=1&cursor=${encodeURIComponent(allCursor)}`,
  );
  expect(valid.status).toBe(200);

  const boundedCursor = encodeURIComponent(cursors.get('24h')!);
  const mismatchPaths = [
    `/admin/v1/principals/${manifest.principals.empty}/cache-keepalive?horizon=24h&cursor=${boundedCursor}`,
    `/admin/v1/principals/${manifest.principals.high_ui}/cache-keepalive?horizon=7d&cursor=${boundedCursor}`,
    `/admin/v1/principals/${manifest.principals.high_ui}/cache-keepalive?horizon=24h&status=renewed&cursor=${boundedCursor}`,
  ];
  for (const apiPath of mismatchPaths) {
    const response = await directRequest('CUR-02', 'GET', apiPath);
    expect(response.status).toBe(400);
    expect(responseError(response.body)).toBe('invalid_input');
  }
});

test('CUR-03 keeps the first-page horizon anchor across repeated clock advances', async ({
  page,
}) => {
  const first = await directRequest(
    'CUR-03',
    'GET',
    `/admin/v1/principals/${manifest.principals.high_ui}/cache-keepalive?horizon=24h&limit=1`,
  );
  expect(first.status).toBe(200);
  let cursor = KeepaliveListBodySchema.parse(first.body).next_cursor;
  expect(cursor).not.toBeNull();
  const frozenStart = cursorOriginalFields(cursor!, '24h').horizon_start_ms;

  for (let pageNumber = 2; pageNumber <= 5; pageNumber += 1) {
    await waitPastCursorRequestSecond(page, cursor!, '24h');
    const response = await directRequest(
      'CUR-03',
      'GET',
      `/admin/v1/principals/${manifest.principals.high_ui}/cache-keepalive?horizon=24h&limit=1&cursor=${encodeURIComponent(cursor!)}`,
    );
    if (PHASE === 'baseline') {
      expect(response.status).toBe(400);
      expect(responseError(response.body)).toBe('invalid_input');
      return;
    }
    expect(response.status).toBe(200);
    cursor = KeepaliveListBodySchema.parse(response.body).next_cursor;
    expect(cursor).not.toBeNull();
    expect(cursorOriginalFields(cursor!, '24h').horizon_start_ms).toBe(
      frozenStart,
    );
  }
});

test('UI-01 renders four metrics and flashes only a changed value from the actual summary', async ({
  page,
}) => {
  const started = Date.now();
  const paintStarted = performance.now();
  await openPrincipal(page, 'high_ui');
  const firstPaint = performance.now() - paintStarted;
  const card = page.getByTestId('cache-keepalive-card');
  await expect(card.getByText('Renewing now', { exact: true })).toBeVisible();
  await expect(
    card.getByText('Sessions (last 5m)', { exact: true }),
  ).toBeVisible();
  await expect(card.getByText('Renewals fired', { exact: true })).toBeVisible();
  await expect(card.getByText('Cost saved', { exact: true })).toBeVisible();
  const metrics = card.getByTestId('cache-keepalive-metric-value');
  await expect(metrics).toHaveCount(4);
  await expect(metrics.first()).toHaveAttribute('aria-busy', 'false');
  await expect(card.locator('.flash-text-active')).toHaveCount(0);
  const before = await metrics.allTextContents();
  await page.evaluate(() => {
    const root = document.querySelector('[data-testid="cache-keepalive-card"]');
    if (!root) throw new Error('cache keepalive card is not mounted');
    const metricElements = Array.from(
      root.querySelectorAll<HTMLElement>(
        '[data-testid="cache-keepalive-metric-value"]',
      ),
    );
    const values = () =>
      metricElements.map((element) => element.textContent ?? '');
    let previousValues = values();
    let previousFlashes = metricElements.map((element) =>
      element.classList.contains('flash-text-active'),
    );
    const events: Array<{
      kind: 'value' | 'flash-on' | 'flash-off';
      observed_at_ms: number;
      observed_at_performance_ms: number;
      index: number;
      value: string;
      previous_value?: string;
    }> = [];
    const observer = new MutationObserver(() => {
      const nextValues = values();
      const nextFlashes = metricElements.map((element) =>
        element.classList.contains('flash-text-active'),
      );
      const observedAtMs = Date.now();
      const observedAtPerformanceMs = performance.now();
      for (let index = 0; index < metricElements.length; index += 1) {
        if (nextValues[index] !== previousValues[index]) {
          events.push({
            kind: 'value',
            observed_at_ms: observedAtMs,
            observed_at_performance_ms: observedAtPerformanceMs,
            index,
            value: nextValues[index],
            previous_value: previousValues[index],
          });
        }
        if (nextFlashes[index] !== previousFlashes[index]) {
          events.push({
            kind: nextFlashes[index] ? 'flash-on' : 'flash-off',
            observed_at_ms: observedAtMs,
            observed_at_performance_ms: observedAtPerformanceMs,
            index,
            value: nextValues[index],
          });
        }
      }
      previousValues = nextValues;
      previousFlashes = nextFlashes;
    });
    observer.observe(root, {
      attributes: true,
      attributeFilter: ['class'],
      characterData: true,
      childList: true,
      subtree: true,
    });
    Object.assign(window, {
      __keepaliveMetricProbe: {
        events,
        installed_at_ms: Date.now(),
        initial_values: previousValues,
      },
    });
  });
  await directRequest(
    'UI-01',
    'GET',
    `/admin/v1/principals/${manifest.principals.high_ui}/cache-keepalive?limit=0`,
  );
  applyMutation('UI-01', 'bump-summary');
  const changedResponse = await pollResponse(page, (response) =>
    summaryResponseMatches(response, manifest.principals.high_ui),
  );
  const changedList = KeepaliveListBodySchema.parse(
    await changedResponse.json(),
  );
  const changedSummary = changedList.summary;
  await expect
    .poll(() =>
      page.evaluate(() => {
        const instrumentedWindow = window as typeof window & {
          __keepaliveMetricProbe: {
            events: Array<{ kind: string }>;
          };
        };
        return instrumentedWindow.__keepaliveMetricProbe.events.filter(
          (event) => event.kind === 'value',
        ).length;
      }),
    )
    .toBe(1);
  await expect
    .poll(() =>
      page.evaluate(() => {
        const instrumentedWindow = window as typeof window & {
          __keepaliveMetricProbe: {
            events: Array<{ kind: string }>;
          };
        };
        return instrumentedWindow.__keepaliveMetricProbe.events.filter(
          (event) => event.kind === 'flash-on',
        ).length;
      }),
    )
    .toBe(1);
  const changedEvents = await page.evaluate(() => {
    const instrumentedWindow = window as typeof window & {
      __keepaliveMetricProbe: {
        events: Array<{
          kind: 'value' | 'flash-on' | 'flash-off';
          observed_at_ms: number;
          observed_at_performance_ms: number;
          index: number;
          value: string;
          previous_value?: string;
        }>;
      };
    };
    return instrumentedWindow.__keepaliveMetricProbe.events;
  });
  const valueEvent = changedEvents.find((event) => event.kind === 'value');
  const flashEvent = changedEvents.find((event) => event.kind === 'flash-on');
  expect(valueEvent).toBeDefined();
  expect(flashEvent).toBeDefined();
  expect(flashEvent!.index).toBe(valueEvent!.index);
  expect(flashEvent!.value).toBe(valueEvent!.value);
  expect(valueEvent!.previous_value).toBe(before[valueEvent!.index]);
  const after = await metrics.allTextContents();
  expect(after).toEqual([
    changedSummary.renewing_now.toLocaleString('en-US'),
    changedSummary.sessions_last_5m.toLocaleString('en-US'),
    changedSummary.renewals_fired.toLocaleString('en-US'),
    `$${changedSummary.cost_saved.toFixed(2)}`,
  ]);
  expect(
    after
      .map((value, index) => ({ before: before[index], index, value }))
      .filter(({ before: previousValue, value }) => previousValue !== value),
  ).toEqual([
    {
      before: valueEvent!.previous_value,
      index: valueEvent!.index,
      value: valueEvent!.value,
    },
  ]);
  await expect
    .poll(() =>
      page.evaluate(() => {
        const instrumentedWindow = window as typeof window & {
          __keepaliveMetricProbe: {
            events: Array<{ kind: string }>;
          };
        };
        return instrumentedWindow.__keepaliveMetricProbe.events.filter(
          (event) => event.kind === 'flash-off',
        ).length;
      }),
    )
    .toBe(1);
  const repeatMarker = await page.evaluate(() => {
    const instrumentedWindow = window as typeof window & {
      __keepaliveMetricProbe: { events: unknown[] };
    };
    return instrumentedWindow.__keepaliveMetricProbe.events.length;
  });
  const repeatedResponse = await pollResponse(page, (response) =>
    summaryResponseMatches(response, manifest.principals.high_ui),
  );
  const repeatedList = KeepaliveListBodySchema.parse(
    await repeatedResponse.json(),
  );
  const repeatedSummary = repeatedList.summary;
  expect(repeatedSummary).toEqual(changedSummary);
  await page.evaluate(
    () =>
      new Promise<void>((resolve) => {
        requestAnimationFrame(() => resolve());
      }),
  );
  const repeatedObservation = await page.evaluate((marker) => {
    const instrumentedWindow = window as typeof window & {
      __keepaliveMetricProbe: {
        events: Array<{
          kind: string;
          observed_at_ms: number;
          observed_at_performance_ms: number;
          index: number;
          value: string;
        }>;
      };
    };
    return {
      observed_at_ms: Date.now(),
      values: Array.from(
        document.querySelectorAll<HTMLElement>(
          '[data-testid="cache-keepalive-card"] [data-testid="cache-keepalive-metric-value"]',
        ),
      ).map((element) => element.textContent ?? ''),
      events: instrumentedWindow.__keepaliveMetricProbe.events.slice(marker),
    };
  }, repeatMarker);
  expect(repeatedObservation.values).toEqual(after);
  expect(
    repeatedObservation.events.filter(
      (event) => event.kind === 'value' || event.kind === 'flash-on',
    ),
  ).toEqual([]);
  appendImmutableJsonl(RAW_JSONL, {
    case_id: 'UI-01',
    run_id: randomUUID(),
    phase: PHASE,
    engine: ENGINE,
    record_type: 'metric_flash_observation',
    changed_summary: changedSummary,
    repeated_summary: repeatedSummary,
    observed_at: iso(repeatedObservation.observed_at_ms),
    changed_events: changedEvents,
    repeated_same_value: repeatedObservation,
  });
  await uiObservation(
    page,
    'UI-01',
    'summary-flash',
    started,
    firstPaint,
    performance.now() - paintStarted,
  );
});

test('UI-02 preserves the real stale-revision 409 toggle behavior', async ({
  page,
}) => {
  await openPrincipal(page, 'low');
  const principals = await directRequest(
    'UI-02',
    'GET',
    '/admin/v1/principals',
  );
  const parsedPrincipals = PrincipalListBodySchema.parse(principals.body);
  const current = parsedPrincipals.principals.find(
    (item) => item.id === manifest.principals.low,
  );
  expect(current).toBeTruthy();
  if (!current)
    throw new Error('low principal is missing from the prepared fixture');
  const revision = current.revision;
  const currentConfig = current.cache_keepalive;
  const bump = await directRequest(
    'UI-02',
    'PATCH',
    `/admin/v1/principals/${manifest.principals.low}`,
    {
      body: { cache_keepalive: currentConfig },
      ifMatch: revision,
    },
  );
  expect(bump.status).toBe(200);
  const patch = page.waitForResponse(
    (response) =>
      response.request().method() === 'PATCH' &&
      new URL(response.url()).pathname ===
        `/admin/v1/principals/${manifest.principals.low}`,
  );
  await page.getByTestId('cache-keepalive-switch').click();
  const conflict = await patch;
  expect(conflict.status()).toBe(409);
  expect(await conflict.json()).toMatchObject({
    error: ENGINE === 'sqlite' ? 'storage_conflict' : 'stale_revision',
  });
  await expect(
    page.locator('[data-sonner-toast][data-type="error"][data-front="true"]'),
  ).toBeVisible();
  await expect(page.getByTestId('cache-keepalive-switch')).toBeChecked();
});

test('UI-03 distinguishes the horizon All control and sends exact horizon URLs', async ({
  page,
}) => {
  const started = Date.now();
  const paintStarted = performance.now();
  await openDrawer(page);
  const firstPaint = performance.now() - paintStarted;
  for (const horizon of ['7d', 'all', '24h'] as const) {
    const response = await selectHorizon(page, horizon);
    expect(response.status()).toBe(200);
    await expect(
      horizonButton(page, horizon === 'all' ? 'All' : horizon),
    ).toBeVisible();
    const overviewLabel = horizon === 'all' ? 'All time:' : `Last ${horizon}:`;
    await expect(
      drawer(page).getByText(overviewLabel, { exact: true }),
    ).toBeVisible();
  }
  await uiObservation(
    page,
    'UI-03',
    'horizon-controls',
    started,
    firstPaint,
    performance.now() - paintStarted,
  );
});

test('UI-04 verifies all 21 actual horizon/filter requests and selected chips', async ({
  page,
}) => {
  const filters = [
    'all',
    'renewed',
    'scheduled',
    'capped',
    'expired',
    'not_tracked',
    'error',
  ] as const;
  const labels: Record<(typeof filters)[number], string> = {
    all: 'All',
    renewed: 'Renewed',
    scheduled: 'Scheduled',
    capped: 'Capped',
    expired: 'Expired',
    not_tracked: 'Not tracked',
    error: 'Error',
  };
  for (const horizon of ['24h', '7d', 'all'] as const) {
    for (const filter of filters) {
      await openDrawer(page);
      if (horizon !== '24h') await selectHorizon(page, horizon);
      if (filter !== 'all') await selectFilter(page, filter, horizon);
      const selected = filterButton(page, labels[filter]);
      await expect(selected).toHaveAttribute('aria-pressed', 'true');
      const direct = await directRequest(
        'UI-04',
        'GET',
        (() => {
          const params = new URLSearchParams({ horizon });
          if (filter === 'error') params.set('error', 'true');
          else if (filter !== 'all') params.set('status', filter);
          return `/admin/v1/principals/${manifest.principals.high_ui}/cache-keepalive?${params}`;
        })(),
      );
      const uiIds = await visibleIds(page);
      const apiIds = KeepaliveListBodySchema.parse(direct.body).rows.map(
        (row) => row.id,
      );
      expect(uiIds).toEqual(apiIds);
      await page.getByRole('button', { name: 'Close history' }).click();
    }
  }
});

test('UI-05 preserves row ID, state, error, P&L, and attempt semantics', async ({
  page,
}) => {
  await openDrawer(page);
  for (const label of [
    'Renewed',
    'Scheduled',
    'Capped',
    'Expired',
    'Not tracked',
    'Error',
  ]) {
    await expect(
      drawer(page).getByText(label, { exact: true }).first(),
    ).toBeVisible();
  }
  const errorRow = drawer(page).locator('li[data-key="ui-000005"]');
  await expect(errorRow).toContainText('renewal dispatch unavailable');
  await expect(errorRow).toContainText('Error');
  await expect(errorRow).toContainText('/12');
  await expect(errorRow).toContainText('$');
  await expect(errorRow.getByRole('button')).toHaveClass(/border-l-red-500/);
  const direct = await directRequest(
    'UI-05',
    'GET',
    `/admin/v1/principals/${manifest.principals.high_ui}/cache-keepalive?horizon=24h`,
  );
  expect(await visibleIds(page)).toEqual(
    KeepaliveListBodySchema.parse(direct.body).rows.map((row) => row.id),
  );
});

test('UI-06 records FLIP geometry-before-style behavior and PAUSE_ANIMATIONS', async ({
  page,
}) => {
  await openDrawer(page);
  const animatedOrderBefore = await visibleIds(page);
  const movedKeys = animatedOrderBefore.slice(0, 2);
  expect(movedKeys).toHaveLength(2);
  await page.evaluate((keys) => {
    const root = document.querySelector(
      '[data-testid="cache-keepalive-sessions-drawer"]',
    );
    if (!root)
      throw new Error('cache keepalive sessions drawer is not mounted');
    const targetKeys = new Set(keys);
    const original = Element.prototype.getBoundingClientRect;
    const geometryKeys = new Set<string>();
    let iteration = 0;
    let lastStyles = new Map<string, string | null>();
    const events: Array<{
      kind: 'read' | 'write';
      phase: 'geometry' | 'flip-write' | 'animation-release' | 'cleanup';
      iteration: number;
      observed_at_ms: number;
      observed_at_performance_ms: number;
      key: string;
      rect?: { top: number; bottom: number; height: number };
      style?: string | null;
    }> = [];
    Element.prototype.getBoundingClientRect = function getBoundingClientRect() {
      const rect = original.call(this);
      const key = this.getAttribute('data-key');
      if (key && targetKeys.has(key)) {
        if (iteration === 0 || geometryKeys.size === targetKeys.size) {
          iteration += 1;
          geometryKeys.clear();
        }
        geometryKeys.add(key);
        events.push({
          kind: 'read',
          phase: 'geometry',
          iteration,
          observed_at_ms: Date.now(),
          observed_at_performance_ms: performance.now(),
          key,
          rect: { top: rect.top, bottom: rect.bottom, height: rect.height },
        });
      }
      return rect;
    };
    const observer = new MutationObserver((mutations) => {
      if (iteration === 0) return;
      for (const mutation of mutations) {
        const element = mutation.target as HTMLElement;
        const key = element.getAttribute('data-key');
        if (!key || !targetKeys.has(key)) continue;
        const style = element.getAttribute('style');
        if (lastStyles.get(key) === style) continue;
        lastStyles.set(key, style);
        const transform = element.style.transform;
        const transition = element.style.transition;
        const hasLiftStyle =
          element.style.position !== '' ||
          element.style.zIndex !== '' ||
          element.style.backgroundColor !== '' ||
          element.style.boxShadow !== '';
        const phase = transform.startsWith('translateY(')
          ? 'flip-write'
          : transition !== '' && hasLiftStyle
            ? 'animation-release'
            : 'cleanup';
        events.push({
          kind: 'write',
          phase,
          iteration,
          observed_at_ms: Date.now(),
          observed_at_performance_ms: performance.now(),
          key,
          style,
        });
      }
    });
    observer.observe(root, {
      subtree: true,
      attributes: true,
      attributeFilter: ['style'],
      attributeOldValue: true,
    });
    Object.assign(window, {
      __keepaliveFlipProbe: {
        events,
        installed_at_ms: Date.now(),
        reset() {
          events.length = 0;
          iteration = 0;
          geometryKeys.clear();
          lastStyles = new Map(
            Array.from(root.querySelectorAll<HTMLElement>('[data-key]'))
              .filter((element) => {
                const key = element.getAttribute('data-key');
                return key !== null && targetKeys.has(key);
              })
              .map((element) => [
                element.getAttribute('data-key')!,
                element.getAttribute('style'),
              ]),
          );
        },
      },
    });
  }, movedKeys);
  applyMutation('UI-06', 'reorder');
  await pollResponse(page, (response) =>
    listResponseMatches(response, manifest.principals.high_ui, '24h', 'all'),
  );
  await expect
    .poll(() => visibleIds(page).then((ids) => ids.slice(0, 2)))
    .toEqual([...movedKeys].reverse());
  await expect
    .poll(() =>
      page.evaluate(() => {
        const instrumentedWindow = window as typeof window & {
          __keepaliveFlipProbe: {
            events: Array<{ phase: string }>;
          };
        };
        return instrumentedWindow.__keepaliveFlipProbe.events.filter(
          (event) => event.phase === 'flip-write',
        ).length;
      }),
    )
    .toBeGreaterThanOrEqual(2);
  const events = await page.evaluate(() => {
    const instrumentedWindow = window as typeof window & {
      __keepaliveFlipProbe: {
        events: Array<{
          kind: 'read' | 'write';
          phase: 'geometry' | 'flip-write' | 'animation-release' | 'cleanup';
          iteration: number;
          observed_at_ms: number;
          observed_at_performance_ms: number;
          key: string;
          rect?: { top: number; bottom: number; height: number };
          style?: string | null;
        }>;
      };
    };
    return instrumentedWindow.__keepaliveFlipProbe.events;
  });
  const firstFlipWrite = events.findIndex(
    (event) => event.phase === 'flip-write',
  );
  expect(firstFlipWrite).toBeGreaterThan(0);
  const geometryEvents = events.slice(0, firstFlipWrite);
  expect(geometryEvents.every((event) => event.phase === 'geometry')).toBe(
    true,
  );
  expect([...new Set(geometryEvents.map((event) => event.key))].sort()).toEqual(
    [...movedKeys].sort(),
  );
  const animatedIteration = geometryEvents[0]?.iteration;
  expect(animatedIteration).toBeGreaterThan(0);
  expect(
    geometryEvents.every((event) => event.iteration === animatedIteration),
  ).toBe(true);
  const flipWrites = events.filter((event) => event.phase === 'flip-write');
  expect(
    flipWrites.every((event) => event.iteration === animatedIteration),
  ).toBe(true);
  expect([...new Set(flipWrites.map((event) => event.key))].sort()).toEqual(
    [...movedKeys].sort(),
  );
  const rectTopByKey = Object.fromEntries(
    geometryEvents
      .filter((event) => event.rect !== undefined)
      .map((event) => [event.key, event.rect!.top]),
  );
  expect(rectTopByKey[movedKeys[0]]).not.toBe(rectTopByKey[movedKeys[1]]);
  const deltaByKey = Object.fromEntries(
    flipWrites.map((event) => {
      expect(event.style).toMatch(
        /transform:\s*translateY\((-?\d+(?:\.\d+)?)px\)/,
      );
      return [
        event.key,
        Number(event.style!.match(/translateY\((-?\d+(?:\.\d+)?)px\)/)![1]),
      ];
    }),
  );
  expect(deltaByKey[movedKeys[0]]).toBeLessThan(0);
  expect(deltaByKey[movedKeys[1]]).toBeGreaterThan(0);
  await expect
    .poll(() =>
      drawer(page)
        .locator('li[data-key]')
        .evaluateAll(
          (items, keys) =>
            items.filter((item) => {
              const htmlItem = item as HTMLElement;
              return (
                keys.includes(htmlItem.dataset.key ?? '') &&
                (htmlItem.style.transform !== '' ||
                  htmlItem
                    .getAnimations()
                    .some(
                      (animation) =>
                        animation.playState === 'running' || animation.pending,
                    ))
              );
            }).length,
          movedKeys,
        ),
    )
    .toBe(0);
  const animationEvents = await page.evaluate(() => {
    const instrumentedWindow = window as typeof window & {
      __keepaliveFlipProbe: {
        events: Array<{
          kind: string;
          phase: string;
          observed_at_ms: number;
          observed_at_performance_ms: number;
          key: string;
          style?: string | null;
        }>;
      };
    };
    return instrumentedWindow.__keepaliveFlipProbe.events;
  });
  expect(
    animationEvents.some((event) => event.phase === 'animation-release'),
  ).toBe(true);
  expect(animationEvents.some((event) => event.phase === 'cleanup')).toBe(true);
  const pausedOrderBefore = await visibleIds(page);
  await page.evaluate(() => {
    const instrumentedWindow = window as typeof window & {
      PAUSE_ANIMATIONS: boolean;
      __keepaliveFlipProbe: { reset(): void };
    };
    instrumentedWindow.PAUSE_ANIMATIONS = true;
    instrumentedWindow.__keepaliveFlipProbe.reset();
  });
  applyMutation('UI-06', 'reorder');
  await pollResponse(page, (response) =>
    listResponseMatches(response, manifest.principals.high_ui, '24h', 'all'),
  );
  await expect
    .poll(() => visibleIds(page).then((ids) => ids.slice(0, 2)))
    .toEqual([...pausedOrderBefore.slice(0, 2)].reverse());
  await page.evaluate(
    () =>
      new Promise<void>((resolve) => {
        requestAnimationFrame(() => resolve());
      }),
  );
  const pausedEvents = await page.evaluate(() => {
    const instrumentedWindow = window as typeof window & {
      __keepaliveFlipProbe: {
        events: Array<{
          kind: string;
          phase: string;
          observed_at_ms: number;
          observed_at_performance_ms: number;
          key: string;
        }>;
      };
    };
    return instrumentedWindow.__keepaliveFlipProbe.events;
  });
  expect(pausedEvents).toEqual([]);
  appendImmutableJsonl(RAW_JSONL, {
    case_id: 'UI-06',
    run_id: randomUUID(),
    phase: PHASE,
    engine: ENGINE,
    record_type: 'flip_observation',
    observed_at: iso(),
    moved_keys: movedKeys,
    animated_events: animationEvents,
    paused_events: pausedEvents,
  });
});

test('UI-07 reaches wire terminal pagination beyond 20 pages without a hard cap', async ({
  page,
}) => {
  await openDrawer(page);
  const initial = await selectHorizon(page, 'all');
  let body = KeepaliveListBodySchema.parse(await initial.json());
  const uiIds = [...body.rows.map((row) => row.id)];
  let pageCount = 1;
  const seenCursors = new Set<string>();
  while (body.next_cursor !== null) {
    const expectedCursor = body.next_cursor;
    const responsePromise = page.waitForResponse((response) => {
      const url = new URL(response.url());
      return (
        listResponseMatches(
          response,
          manifest.principals.high_ui,
          'all',
          'all',
        ) && url.searchParams.get('cursor') === expectedCursor
      );
    });
    if (seenCursors.has(body.next_cursor)) {
      throw new Error(
        'browser pagination cursor cycle detected before terminal null',
      );
    }
    seenCursors.add(body.next_cursor);
    await drawer(page)
      .getByRole('button', { name: 'Loading older sessions...' })
      .click();
    const response = await responsePromise;
    body = KeepaliveListBodySchema.parse(await response.json());
    expect(
      sha256Nullable(new URL(response.url()).searchParams.get('cursor')),
    ).toBe(sha256(expectedCursor));
    uiIds.push(...body.rows.map((row) => row.id));
    await expect
      .poll(() => visibleIds(page).then((ids) => ids.length))
      .toBe(uiIds.length);
    pageCount += 1;
  }
  expect(pageCount).toBeGreaterThan(20);
  expect(new Set(uiIds).size).toBe(uiIds.length);
  await expect(drawer(page).getByText('· No more sessions ·')).toBeVisible();
  expect(await visibleIds(page)).toEqual(uiIds);
  const direct = await fullDirectPagination(
    'UI-07',
    manifest.principals.high_ui,
    'all',
  );
  expect(direct.pageCount).toBe(pageCount);
  expect(direct.ids).toEqual(uiIds);
  expect(direct.ids).toHaveLength(dataset.expected_list_counts.high_ui.all);
});

test('UI-08 preserves desktop split and mobile detail navigation', async ({
  page,
}) => {
  const screenshotDir = path.join(EVIDENCE_DIR, 'UI-08');
  mkdirSync(screenshotDir, { recursive: true });
  await page.setViewportSize({ width: 1024, height: 900 });
  await openDrawer(page, 'low');
  await clickRow(page, manifest.low_ids.active);
  const listRegion = page
    .getByTestId('cache-keepalive-session-list-region')
    .locator('..');
  await expect(listRegion).toHaveCSS('width', '440px');
  await expect(page.getByTestId('session-detail-content')).toBeVisible();
  await page.screenshot({
    path: path.join(
      screenshotDir,
      `${ENGINE}-${PHASE}-desktop-${randomUUID()}.png`,
    ),
  });
  await page.setViewportSize({ width: 800, height: 900 });
  await expect(listRegion).toBeHidden();
  await expect(
    page.getByRole('button', { name: 'Back to sessions' }),
  ).toBeVisible();
  await page.screenshot({
    path: path.join(
      screenshotDir,
      `${ENGINE}-${PHASE}-mobile-${randomUUID()}.png`,
    ),
  });
  await page.getByRole('button', { name: 'Back to sessions' }).click();
  await expect(listRegion).toBeVisible();
});

test('UI-09 renders pending and final turn timelines from actual detail JSON', async ({
  page,
}) => {
  const detailTimelineSchema = z
    .object({
      turns: z.array(
        z
          .object({
            pending: z.boolean(),
            followed_up: z.boolean(),
          })
          .passthrough(),
      ),
    })
    .passthrough();
  await openDrawer(page, 'low');
  const active = await clickRow(page, manifest.low_ids.active);
  const activeBody = detailTimelineSchema.parse(await active.json());
  expect(activeBody.turns[0]?.pending).toBe(true);
  const currentTurnCard = page
    .getByText('Current turn · Live', { exact: true })
    .locator('..');
  await expect(currentTurnCard).toBeVisible();
  await expect(
    currentTurnCard.getByText('waiting for follow-up', { exact: true }),
  ).toBeVisible();
  await expect(
    currentTurnCard.getByText('waiting for follow-up', { exact: true }),
  ).toHaveCount(1);
  await page.getByRole('button', { name: 'Back to sessions' }).click();
  const terminal = await clickRow(page, manifest.low_ids.terminal);
  const terminalBody = detailTimelineSchema.parse(await terminal.json());
  const finalTurn = terminalBody.turns[0];
  expect(finalTurn).toBeDefined();
  expect(finalTurn!.pending).toBe(false);
  const expectedFinalStatus = finalTurn!.followed_up
    ? 'cache used by follow-up'
    : 'no follow-up (loss)';
  const finalTurnCard = page
    .getByText('Final turn', { exact: true })
    .locator('..');
  await expect(finalTurnCard).toBeVisible();
  await expect(
    finalTurnCard.getByText(expectedFinalStatus, { exact: true }),
  ).toBeVisible();
  await expect(
    finalTurnCard.getByText(expectedFinalStatus, { exact: true }),
  ).toHaveCount(1);
  expect(sha256Json(activeBody)).not.toBe(sha256Json(terminalBody));
});

test('UI-10 expands config snapshot and raw record for session and encoded bare decision IDs', async ({
  page,
}) => {
  await openDrawer(page, 'low');
  await clickRow(page, manifest.low_ids.active);
  await page
    .getByRole('button', { name: /Config in effect at schedule time/ })
    .click();
  await expect(page.getByText('snapshot')).toBeVisible();
  await page.getByRole('button', { name: /Raw session record/ }).click();
  await expect(page.locator('pre')).toContainText('session_key_hash');
  await page.getByRole('button', { name: 'Back to sessions' }).click();
  const decision = await clickRow(page, manifest.low_ids.decision);
  expect(decision.request().url()).toContain(
    encodeURIComponent(manifest.low_ids.decision),
  );
  await page.getByRole('button', { name: /Raw session record/ }).click();
  await expect(page.locator('pre')).toContainText('not_tracked');
});

test('UI-11 pauses hidden polls and preserves independent resume behavior', async ({
  page,
}) => {
  await openDrawer(page, 'low');
  await clickRow(page, manifest.low_ids.active);
  const foregroundBefore = await networkCount(page, 2 * POLL_MS + 1_000);
  expect(foregroundBefore).toBeGreaterThanOrEqual(3);
  await setDocumentVisibility(page, 'hidden');
  const inFlightAfterHide = await networkCount(page, 1_000);
  const hidden = await networkCount(
    page,
    Math.max(0, UI_VISIBILITY_MS - 1_000),
  );
  expect(hidden).toBe(0);
  const resumed: string[] = [];
  const listener = (response: Response) => {
    const kind = keepaliveKind(new URL(response.url()));
    if (kind) resumed.push(kind);
  };
  page.on('response', listener);
  await setDocumentVisibility(page, 'visible');
  await expect
    .poll(() => resumed.filter((kind) => kind === 'summary').length, {
      timeout: POLL_TIMEOUT_MS,
    })
    .toBeGreaterThan(0);
  await expect
    .poll(() => resumed.filter((kind) => kind === 'detail').length, {
      timeout: POLL_TIMEOUT_MS,
    })
    .toBeGreaterThan(0);
  page.off('response', listener);
  appendImmutableJsonl(RAW_JSONL, {
    case_id: 'UI-11',
    run_id: randomUUID(),
    phase: PHASE,
    engine: ENGINE,
    sample: null,
    cache_state: 'browser_visibility',
    started_at_utc: iso(Date.now() - UI_VISIBILITY_MS),
    ended_at_utc: iso(),
    runtime_profile: dataset.runtime_profile,
    request: {
      method: 'VISIBILITY_OBSERVATION',
      exact_url_or_redacted_sha256: page.url(),
      cursor_in_sha256: null,
    },
    response: {
      http_status: null,
      canonical_body_sha256: sha256Json(resumed),
      body_bytes: null,
      cursor_out_sha256: null,
      ttfb_ms: null,
      wall_ms: UI_VISIBILITY_MS,
    },
    ui: {
      observed_at_utc: iso(),
      in_flight_after_hide: inFlightAfterHide,
      hidden_request_count: hidden,
      resumed_query_kinds: resumed,
      visible_row_ids_sha256: sha256Json(await visibleIds(page)),
      screenshot_path: null,
    },
    backend: {
      sql_calls: null,
      sql_time_ms: null,
      pool_wait_ms: null,
      plan_artifact: null,
    },
  });
});

test('UI-12 isolates card, list, and detail state across two principals', async ({
  page,
}) => {
  await openDrawer(page, 'high_ui');
  const p1Ids = await visibleIds(page);
  await page.getByRole('button', { name: 'Close history' }).click();
  await openDrawer(page, 'low');
  const p2Ids = await visibleIds(page);
  expect(p2Ids.some((id) => p1Ids.includes(id))).toBe(false);
  await clickRow(page, manifest.low_ids.active);
  await expect(page.getByTestId('session-detail-content')).toContainText(
    manifest.low_ids.active,
  );
  for (const id of p1Ids.slice(0, 10))
    await expect(page.getByTestId('session-detail-content')).not.toContainText(
      id,
    );
  const p2IdsBeforePolling = await visibleIds(page);
  const p2PollRequests = await networkCount(page, 2 * POLL_MS + 1_000);
  expect(p2PollRequests).toBeGreaterThanOrEqual(3);
  expect(await visibleIds(page)).toEqual(p2IdsBeforePolling);
});

test('PERF-04 rapid filter switching keeps actual responses and final query-key state', async ({
  page,
  context,
}) => {
  const cdp = await context.newCDPSession(page);
  await cdp.send('Network.enable');
  await cdp.send('Network.emulateNetworkConditions', {
    offline: false,
    latency: 250,
    downloadThroughput: -1,
    uploadThroughput: -1,
    connectionType: 'wifi',
  });
  await openDrawer(page);
  const lifecycle: Array<{ url: string; status: number; at: string }> = [];
  const listener = (response: Response) => {
    if (keepaliveKind(new URL(response.url())) === 'list')
      lifecycle.push({
        url: evidenceUrl(response.url()).display,
        status: response.status(),
        at: iso(),
      });
  };
  page.on('response', listener);
  const finalResponse = page.waitForResponse((response) =>
    listResponseMatches(response, manifest.principals.high_ui, '24h', 'error'),
  );
  const elapsed = await page.evaluate(async () => {
    const labels = ['Renewed', 'Scheduled', 'Capped', 'Expired', 'Error'];
    const buttons = [
      ...document.querySelectorAll<HTMLButtonElement>('button[aria-pressed]'),
    ];
    const started = performance.now();
    for (const [index, label] of labels.entries()) {
      buttons.find((button) => button.textContent?.trim() === label)?.click();
      if (index + 1 < labels.length) {
        await new Promise<void>((resolve) => setTimeout(resolve, 0));
      }
    }
    return performance.now() - started;
  });
  expect(elapsed).toBeLessThan(100);
  await expect(filterButton(page, 'Error')).toHaveAttribute(
    'aria-pressed',
    'true',
  );
  await finalResponse;
  await expect
    .poll(() => new Set(lifecycle.map((entry) => entry.url)).size)
    .toBeGreaterThanOrEqual(5);
  const direct = await directRequest(
    'PERF-04',
    'GET',
    `/admin/v1/principals/${manifest.principals.high_ui}/cache-keepalive?horizon=24h&error=true`,
  );
  expect(await visibleIds(page)).toEqual(
    KeepaliveListBodySchema.parse(direct.body).rows.map((row) => row.id),
  );
  page.off('response', listener);
  await cdp.send('Network.emulateNetworkConditions', {
    offline: false,
    latency: 0,
    downloadThroughput: -1,
    uploadThroughput: -1,
    connectionType: 'wifi',
  });
  appendImmutableJsonl(RAW_JSONL, {
    case_id: 'PERF-04',
    run_id: randomUUID(),
    phase: PHASE,
    engine: ENGINE,
    sample: null,
    cache_state: 'browser_network_shaped',
    runtime_profile: dataset.runtime_profile,
    started_at_utc: lifecycle[0]?.at ?? iso(),
    ended_at_utc: iso(),
    request: {
      method: 'NETWORK_LIFECYCLE',
      exact_url_or_redacted_sha256: page.url(),
      cursor_in_sha256: null,
    },
    response: {
      http_status: null,
      canonical_body_sha256: sha256Json(lifecycle),
      body_bytes: null,
      cursor_out_sha256: null,
      ttfb_ms: null,
      wall_ms: elapsed,
    },
    ui: {
      observed_at_utc: iso(),
      visible_row_ids_sha256: sha256Json(await visibleIds(page)),
      screenshot_path: null,
      final_filter: 'error',
    },
    backend: {
      sql_calls: null,
      sql_time_ms: null,
      pool_wait_ms: null,
      plan_artifact: null,
    },
  });
});

test('PERF-05 records frontend render work separately from actual API timing', async ({
  page,
}) => {
  await page.addInitScript(() => {
    const longTasks: Array<{ start: number; duration: number }> = [];
    const observer = new PerformanceObserver((entries) => {
      for (const entry of entries.getEntries()) {
        longTasks.push({ start: entry.startTime, duration: entry.duration });
      }
    });
    try {
      observer.observe({ type: 'longtask', buffered: true });
    } catch {
      // Chromium builds without Long Tasks support leave this evidence empty.
    }
    Object.assign(window, { __keepaliveLongTasks: longTasks });
  });
  await openDrawer(page);
  await selectHorizon(page, 'all');
  const secondPage = page.waitForResponse((response) => {
    const url = new URL(response.url());
    return (
      listResponseMatches(
        response,
        manifest.principals.high_ui,
        'all',
        'all',
      ) && url.searchParams.has('cursor')
    );
  });
  await drawer(page)
    .getByRole('button', { name: 'Loading older sessions...' })
    .click();
  await secondPage;
  await expect.poll(() => visibleIds(page).then((ids) => ids.length)).toBe(100);
  const before = {
    ids: await visibleIds(page),
    nodes: await drawer(page).locator('*').count(),
  };
  const started = Date.now();
  const paintStarted = performance.now();
  applyMutation('PERF-05', 'reorder');
  const response = await pollResponse(page, (item) =>
    listResponseMatches(item, manifest.principals.high_ui, 'all', 'all'),
  );
  const after = {
    ids: await visibleIds(page),
    nodes: await drawer(page).locator('*').count(),
  };
  const longTasks = await page.evaluate(() => {
    const instrumentedWindow = window as typeof window & {
      __keepaliveLongTasks: unknown[];
    };
    return instrumentedWindow.__keepaliveLongTasks;
  });
  expect(response.status()).toBe(200);
  expect(after.ids.length).toBe(before.ids.length);
  appendImmutableJsonl(RAW_JSONL, {
    case_id: 'PERF-05',
    run_id: randomUUID(),
    phase: PHASE,
    engine: ENGINE,
    sample: null,
    cache_state: 'browser_render',
    runtime_profile: dataset.runtime_profile,
    started_at_utc: iso(started),
    ended_at_utc: iso(),
    request: {
      method: 'RENDER_OBSERVATION',
      exact_url_or_redacted_sha256: page.url(),
      cursor_in_sha256: null,
    },
    response: {
      http_status: response.status(),
      canonical_body_sha256: sha256Json(await response.json()),
      body_bytes: null,
      cursor_out_sha256: null,
      ttfb_ms: null,
      wall_ms: null,
    },
    ui: {
      observed_at_utc: iso(),
      first_paint_ms: null,
      stable_paint_ms: performance.now() - paintStarted,
      visible_row_ids_sha256: sha256Json(after.ids),
      screenshot_path: null,
      before_nodes: before.nodes,
      after_nodes: after.nodes,
      long_tasks: longTasks,
    },
    backend: {
      sql_calls: null,
      sql_time_ms: null,
      pool_wait_ms: null,
      plan_artifact: null,
    },
  });
});

test('PERF-06 records relative foreground-hidden-foreground network and heap samples', async ({
  page,
  context,
}) => {
  await openDrawer(page, 'low');
  await clickRow(page, manifest.low_ids.active);
  const cdp = await context.newCDPSession(page);
  await cdp.send('Performance.enable');
  const heapSamples: Array<{
    at: string;
    bytes: number | null;
    visibility: string;
  }> = [];
  const sampleHeap = async (visibility: string) => {
    const metrics = await cdp.send('Performance.getMetrics');
    const heap =
      metrics.metrics.find((metric) => metric.name === 'JSHeapUsedSize')
        ?.value ?? null;
    heapSamples.push({ at: iso(), bytes: heap, visibility });
  };
  const observationStarted = Date.now();
  const foregroundBefore = await networkCount(page, 30_000);
  await sampleHeap('visible-before');
  await setDocumentVisibility(page, 'hidden');
  let hiddenRequests = 0;
  const hiddenListener = (response: Response) => {
    if (keepaliveKind(new URL(response.url()))) hiddenRequests += 1;
  };
  page.on('response', hiddenListener);
  const hiddenStarted = Date.now();
  await page.waitForTimeout(1_000);
  const inFlightAfterHide = hiddenRequests;
  hiddenRequests = 0;
  while (Date.now() - hiddenStarted < PERF_HIDDEN_MS) {
    await page.waitForTimeout(
      Math.min(30_000, PERF_HIDDEN_MS - (Date.now() - hiddenStarted)),
    );
    await sampleHeap('hidden');
  }
  page.off('response', hiddenListener);
  expect(hiddenRequests).toBe(0);
  await setDocumentVisibility(page, 'visible');
  const foregroundAfter = await networkCount(page, 30_000);
  await sampleHeap('visible-after');
  expect(foregroundBefore).toBeGreaterThan(0);
  expect(foregroundAfter).toBeGreaterThan(0);
  appendImmutableJsonl(RAW_JSONL, {
    case_id: 'PERF-06',
    run_id: randomUUID(),
    phase: PHASE,
    engine: ENGINE,
    sample: null,
    cache_state: 'browser_visibility',
    visibility_probe: 'page_script',
    mechanism: 'document_property_override',
    runtime_profile: dataset.runtime_profile,
    started_at_utc: iso(observationStarted),
    ended_at_utc: iso(),
    request: {
      method: 'VISIBILITY_HEAP_OBSERVATION',
      exact_url_or_redacted_sha256: page.url(),
      cursor_in_sha256: null,
    },
    response: {
      http_status: null,
      canonical_body_sha256: sha256Json(heapSamples),
      body_bytes: null,
      cursor_out_sha256: null,
      ttfb_ms: null,
      wall_ms: Date.now() - observationStarted,
    },
    ui: {
      observed_at_utc: iso(),
      visible_row_ids_sha256: sha256Json(await visibleIds(page)),
      screenshot_path: null,
      foreground_before_requests: foregroundBefore,
      in_flight_after_hide: inFlightAfterHide,
      hidden_requests: hiddenRequests,
      foreground_after_requests: foregroundAfter,
      heap_samples: heapSamples,
    },
    backend: {
      sql_calls: null,
      sql_time_ms: null,
      pool_wait_ms: null,
      plan_artifact: null,
    },
  });
});

test('FLOW-01 terminal reactivation appears on the next actual poll', async ({
  page,
}) => {
  await openDrawer(page, 'low');
  await clickRow(page, manifest.low_ids.terminal);
  await expect(page.getByText('Capped', { exact: true }).last()).toBeVisible();
  const before = await directRequest(
    'FLOW-01',
    'GET',
    `/admin/v1/principals/${manifest.principals.low}/cache-keepalive/${manifest.low_ids.terminal}`,
  );
  applyMutation('FLOW-01', 'reactivate');
  await pollResponse(page, (response) =>
    new URL(response.url()).pathname.endsWith(
      `/cache-keepalive/${manifest.low_ids.terminal}`,
    ),
  );
  await expect(
    page.getByText('Scheduled', { exact: true }).last(),
  ).toBeVisible();
  const after = await directRequest(
    'FLOW-01',
    'GET',
    `/admin/v1/principals/${manifest.principals.low}/cache-keepalive/${manifest.low_ids.terminal}`,
  );
  const beforeDetail = DetailTransitionBodySchema.parse(before.body);
  const afterDetail = DetailTransitionBodySchema.parse(after.body);
  expect(afterDetail.generation).toBe(beforeDetail.generation + 1);
  expect(afterDetail.attempts).toBe(0);
});

test('FLOW-02 late turn removes the decision and converges to one session representation', async ({
  page,
}) => {
  await openDrawer(page, 'low');
  await expect(
    drawer(page).locator(
      `li[data-key=${JSON.stringify(manifest.low_ids.transition_decision)}]`,
    ),
  ).toBeVisible();
  const before = await directRequest(
    'FLOW-02',
    'GET',
    `/admin/v1/principals/${manifest.principals.low}/cache-keepalive/${manifest.low_ids.transition_decision}`,
  );
  expect(before.status).toBe(200);
  applyMutation('FLOW-02', 'late-turn');
  await pollResponse(page, (response) =>
    listResponseMatches(response, manifest.principals.low, '24h', 'all'),
  );
  await expect(
    drawer(page).locator(
      `li[data-key=${JSON.stringify(manifest.low_ids.transition_decision)}]`,
    ),
  ).toHaveCount(0);
  await expect(
    drawer(page).locator(
      `li[data-key=${JSON.stringify(manifest.low_ids.transition_session)}]`,
    ),
  ).toHaveCount(1);
  const hidden = await directRequest(
    'FLOW-02',
    'GET',
    `/admin/v1/principals/${manifest.principals.low}/cache-keepalive/${manifest.low_ids.transition_decision}`,
  );
  expect(hidden.status).toBe(404);
});

test('FLOW-03 isolated cleanup removes the retained session from summary, list, and detail', async ({
  page,
}) => {
  await openDrawer(page, 'low');
  await expect(
    drawer(page).locator(
      `li[data-key=${JSON.stringify(manifest.low_ids.cleanup)}]`,
    ),
  ).toBeVisible();
  applyMutation('FLOW-03', 'cleanup');
  await pollResponse(page, (response) =>
    listResponseMatches(response, manifest.principals.low, '24h', 'all'),
  );
  await expect(
    drawer(page).locator(
      `li[data-key=${JSON.stringify(manifest.low_ids.cleanup)}]`,
    ),
  ).toHaveCount(0);
  const detail = await directRequest(
    'FLOW-03',
    'GET',
    `/admin/v1/principals/${manifest.principals.low}/cache-keepalive/${manifest.low_ids.cleanup}`,
  );
  expect(detail.status).toBe(404);
});

test('FLOW-07 delayed P1 completion never paints into the P2 route', async ({
  page,
  context,
}) => {
  const cdp = await context.newCDPSession(page);
  await cdp.send('Network.enable');
  await cdp.send('Network.emulateNetworkConditions', {
    offline: false,
    latency: 500,
    downloadThroughput: -1,
    uploadThroughput: -1,
    connectionType: 'wifi',
  });
  await openPrincipal(page, 'high_ui');
  const p1Request = page.waitForRequest((request) => {
    const url = new URL(request.url());
    return (
      request.method() === 'GET' &&
      url.pathname ===
        `/admin/v1/principals/${manifest.principals.high_ui}/cache-keepalive` &&
      url.searchParams.get('limit') === '0'
    );
  });
  const p1Response = page.waitForResponse((response) =>
    summaryResponseMatches(response, manifest.principals.high_ui),
  );
  await p1Request;
  await page
    .getByRole('button', {
      name: new RegExp(manifest.principal_names.low),
    })
    .click();
  await p1Response;
  await expect(
    page.getByRole('heading', { name: manifest.principal_names.low }),
  ).toBeVisible();
  await expect(
    page.getByRole('heading', { name: manifest.principal_names.high_ui }),
  ).toHaveCount(0);
  await cdp.send('Network.emulateNetworkConditions', {
    offline: false,
    latency: 0,
    downloadThroughput: -1,
    uploadThroughput: -1,
    connectionType: 'wifi',
  });
});

test('FLOW-08 records the actual-stack manifest and browser/API evidence boundary', async ({
  page,
}) => {
  await openDrawer(page, 'high_ui');
  const direct = await directRequest(
    'FLOW-08',
    'GET',
    `/admin/v1/principals/${manifest.principals.high_ui}/cache-keepalive?horizon=24h`,
  );
  expect(direct.status).toBe(200);
  expect(await visibleIds(page)).toEqual(
    KeepaliveListBodySchema.parse(direct.body).rows.map((row) => row.id),
  );
  const record = {
    recorded_at_utc: iso(),
    phase: PHASE,
    engine: ENGINE,
    backend_origin: BACKEND_URL,
    web_origin: WEB_URL,
    fixture_sha256: dataset.fixture_sha256,
    fixture_counts: dataset.fixture_counts,
    clock: dataset.clock,
    expected_list_counts: dataset.expected_list_counts,
    raw_jsonl_sha256_at_record_time: sha256(readFileSync(RAW_JSONL)),
    note: 'Browser used the Vite application and actual cc-lb API; no page.route or synthetic API response is installed.',
  };
  const output = path.join(
    EVIDENCE_DIR,
    `actual-stack-${ENGINE}-${PHASE}-${randomUUID()}.json`,
  );
  writeFileSync(output, `${canonical(record)}\n`, { mode: 0o600, flag: 'wx' });
});
