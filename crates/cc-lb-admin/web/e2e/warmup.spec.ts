import { expect, type Page, type Request, test } from '@playwright/test';
import type { AuthSession, QuotaSnapshot } from '../src/lib/api';
import { COPY } from '../src/lib/copy/warmup';

type WarmupPlugin = { wasm_registry_id: string; config: Record<string, unknown> } | null;
type UpstreamFixture = {
  id: string;
  name: string;
  kind: 'anthropic_api_key' | 'anthropic_oauth';
  enabled: boolean;
  spec_revision: number;
  base_url: string;
  warmup_enabled: boolean;
  warmup_dialect_plugin: WarmupPlugin;
  status: {
    last_apply_error: string | null;
    last_apply_at_unix_secs: number | null;
    last_warmup_at_unix_secs: number | null;
  };
};
type PluginFixture = {
  id: string;
  sha256_hex: string;
  name: string;
  original_filename: string;
  label: string | null;
  size_bytes: number;
  refcount: number;
  revision: number;
  uploaded_at_unix_secs: number;
  metadata: null;
  supported_slots: string[];
  hook_metadata: Record<string, { wire_version: number; description: string; usage: string; mode: 'active' | 'noop' }>;
};

type WarmupAttemptFixture = {
  id: string;
  upstream_id: string;
  attempted_at_unix_secs: number;
  completed_at_unix_secs: number | null;
  scheduled_for_unix_secs: number;
  trigger: 'scheduled' | 'manual';
  status: 'success' | 'skipped' | 'transient_failure' | 'permanent_failure';
  reason: string;
  dispatch_kind: 'not_dispatched' | 'http' | 'dialect_plugin' | null;
  http_status: number | null;
  cycle_key: number | null;
  expected_cycle_key: number | null;
  idle_secs_since_prev_window: number | null;
  replica_id: string | null;
  lease_holder: string | null;
  upstream_spec_revision: number;
  dialect_plugin_snapshot: {
    wasm_registry_id: string;
    config: Record<string, unknown>;
  } | null;
  error_detail: string | null;
};

type WarmupSummaryFixture = {
  upstream_id: string;
  last_attempt: WarmupAttemptFixture | null;
  recent_attempts: WarmupAttemptFixture[];
  next_scheduled_at_unix_secs: number | null;
  recent_summary_7d: {
    success: number;
    skipped: number;
    transient_failure: number;
    permanent_failure: number;
  };
  dialect_plugin: WarmupAttemptFixture['dialect_plugin_snapshot'];
};

function warmupAttempt(upstream: UpstreamFixture): WarmupAttemptFixture {
  const attemptedAt = Math.floor(Date.now() / 1000) - 20 * 60;
  return {
    id: `${upstream.id}-attempt-1`,
    upstream_id: upstream.id,
    attempted_at_unix_secs: attemptedAt,
    completed_at_unix_secs: attemptedAt + 1,
    scheduled_for_unix_secs: attemptedAt,
    trigger: 'scheduled',
    status: 'success',
    reason: 'cycle_advanced',
    dispatch_kind: 'http',
    http_status: 200,
    cycle_key: attemptedAt,
    expected_cycle_key: attemptedAt,
    idle_secs_since_prev_window: 300,
    replica_id: 'e2e-replica',
    lease_holder: 'e2e-replica',
    upstream_spec_revision: upstream.spec_revision,
    dialect_plugin_snapshot: upstream.warmup_dialect_plugin
      ? { ...upstream.warmup_dialect_plugin }
      : null,
    error_detail: null,
  };
}

function warmupSummary(upstream: UpstreamFixture): WarmupSummaryFixture {
  const attempt = warmupAttempt(upstream);
  return {
    upstream_id: upstream.id,
    last_attempt: attempt,
    recent_attempts: [attempt],
    next_scheduled_at_unix_secs: Math.floor(Date.now() / 1000) + 3600,
    recent_summary_7d: {
      success: 1,
      skipped: 0,
      transient_failure: 0,
      permanent_failure: 0,
    },
    dialect_plugin: attempt.dialect_plugin_snapshot,
  };
}

const specRevision = 4;

function oauthHealthy(overrides: Partial<UpstreamFixture> = {}): UpstreamFixture {
  return {
    id: 'oauth-healthy',
    name: 'oauth-healthy',
    kind: 'anthropic_oauth',
    enabled: true,
    spec_revision: specRevision,
    base_url: 'https://api.anthropic.com',
    warmup_enabled: true,
    warmup_dialect_plugin: null,
    status: {
      last_apply_error: null,
      last_apply_at_unix_secs: null,
      last_warmup_at_unix_secs: Math.floor(Date.now() / 1000) - 20 * 60,
    },
    ...overrides,
  };
}

function oauthDisabled(): UpstreamFixture {
  return {
    ...oauthHealthy({ id: 'oauth-disabled', name: 'oauth-disabled' }),
    enabled: false,
    spec_revision: 2,
    warmup_enabled: false,
    status: {
      last_apply_error: null,
      last_apply_at_unix_secs: null,
      last_warmup_at_unix_secs: null,
    },
  };
}

function apiKeyUpstream(): UpstreamFixture {
  return {
    id: 'api-key-id',
    name: 'api-key-id',
    kind: 'anthropic_api_key',
    enabled: true,
    spec_revision: 7,
    base_url: 'https://api.anthropic.com',
    warmup_enabled: false,
    warmup_dialect_plugin: null,
    status: {
      last_apply_error: null,
      last_apply_at_unix_secs: null,
      last_warmup_at_unix_secs: null,
    },
  };
}

function shapePlugin(id = 'anthropic-shape-v2'): PluginFixture {
  return {
    id,
    sha256_hex: '0'.repeat(64),
    name: id,
    original_filename: `${id}.wasm`,
    label: id,
    size_bytes: 1024,
    refcount: 0,
    revision: 1,
    uploaded_at_unix_secs: 1718380800,
    metadata: null,
    supported_slots: ['shape'],
    hook_metadata: {
      shape: { wire_version: 1, description: 'Warmup shape', usage: 'Warmup fixture', mode: 'active' },
    },
  };
}

function routerPlugin(): PluginFixture {
  return { ...shapePlugin('router-only'), supported_slots: ['router'] };
}

async function installAppFixtures(
  page: Page,
  options: {
    upstreams?: UpstreamFixture[];
    plugins?: PluginFixture[];
    latestWindows?: QuotaSnapshot[];
    warmupAttempts?: WarmupAttemptFixture[];
    onPatch?: (request: Request, upstream: UpstreamFixture) => Promise<{ status: number; body: unknown }> | { status: number; body: unknown } | null;
    onDelete?: (request: Request, upstream: UpstreamFixture) => Promise<{ status: number; body: unknown }> | { status: number; body: unknown } | null;
    onFireNow?: (request: Request) => Promise<{ status: number; body: unknown }> | { status: number; body: unknown };
  } = {},
) {
  const upstreams = options.upstreams ?? [oauthHealthy(), oauthDisabled(), apiKeyUpstream()];
  const plugins = options.plugins ?? [shapePlugin('anthropic-shape-v2'), routerPlugin()];
  const warmupAttempts = options.warmupAttempts ?? [warmupAttempt(upstreams[0])];
  let warmupAttemptRequests = 0;

  await page.addInitScript(() => {
    window.localStorage.setItem('cc-lb-admin-token', 'mock-token');
  });

  await page.route('**/admin/**', async (route) => {
    const request = route.request();
    const url = new URL(request.url());
    const pathname = url.pathname;
    const method = request.method();
    const json = (status: number, body: unknown, headers: Record<string, string> = {}) =>
      route.fulfill({
        status,
        contentType: 'application/json',
        headers,
        body: JSON.stringify(body),
      });

    if (pathname === '/admin/v1/auth/session' && method === 'GET') {
      return json(200, {
        authority: 'e2e',
        subject: 'e2e-operator',
        kind: 'human',
        provider_id: 'e2e',
        email: null,
        display_name: 'E2E operator',
        expires_at_unix_secs: Math.floor(Date.now() / 1000) + 3600,
        auth_mode: 'static_token',
      } satisfies AuthSession);
    }
    if (pathname === '/admin/v1/upstreams' && method === 'GET') {
      return json(200, { upstreams });
    }
    if (pathname === '/admin/v1/plugins/registry' && method === 'GET') {
      const slot = url.searchParams.get('slot');
      const entries = slot ? plugins.filter((plugin) => plugin.supported_slots.includes(slot)) : plugins;
      return json(200, { entries });
    }
    const upstreamMatch = pathname.match(/^\/admin\/v1\/upstreams\/([^/]+)$/);
    if (upstreamMatch && method === 'GET') {
      const upstream = upstreams.find((candidate) => candidate.id === upstreamMatch[1]);
      return upstream ? json(200, upstream, { etag: `"${upstream.spec_revision}"` }) : json(404, { error: 'not_found' });
    }
    const warmupSummaryMatch = pathname.match(/^\/admin\/v1\/upstreams\/([^/]+)\/warmup$/);
    if (warmupSummaryMatch && method === 'GET') {
      const upstream = upstreams.find((candidate) => candidate.id === warmupSummaryMatch[1]);
      return upstream ? json(200, warmupSummary(upstream)) : json(404, { error: 'not_found' });
    }
    if (upstreamMatch && method === 'PATCH') {
      const upstream = upstreams.find((candidate) => candidate.id === upstreamMatch[1]);
      if (!upstream) return json(404, { error: 'not_found' });
      const override = await options.onPatch?.(request, upstream);
      if (override) return json(override.status, override.body);
      const body = request.postDataJSON() as Partial<Pick<UpstreamFixture, 'warmup_enabled' | 'warmup_dialect_plugin'>>;
      Object.assign(upstream, body, { spec_revision: upstream.spec_revision + 1 });
      return json(200, upstream, { etag: `"${upstream.spec_revision}"` });
    }
    const clearMatch = pathname.match(/^\/admin\/v1\/upstreams\/([^/]+)\/warmup-dialect-plugin$/);
    if (clearMatch && method === 'DELETE') {
      const upstream = upstreams.find((candidate) => candidate.id === clearMatch[1]);
      if (!upstream) return json(404, { error: 'not_found' });
      const override = await options.onDelete?.(request, upstream);
      if (override) return json(override.status, override.body);
      upstream.warmup_dialect_plugin = null;
      upstream.spec_revision += 1;
      return json(200, upstream, { etag: `"${upstream.spec_revision}"` });
    }
    if (pathname.match(/^\/admin\/v1\/upstreams\/([^/]+)\/warmup\/fire-now$/) && method === 'POST') {
      const response = await (options.onFireNow?.(request) ?? { status: 200, body: { fired: true, cycle_key: 1718380800 } });
      return json(response.status, response.body);
    }
    if (
      pathname.match(/^\/admin\/v1\/upstreams\/([^/]+)\/warmup\/attempts$/) &&
      method === 'GET'
    ) {
      warmupAttemptRequests += 1;
      return json(200, { attempts: warmupAttempts, next_cursor: null });
    }
    if (pathname.match(/^\/admin\/v1\/upstreams\/([^/]+)\/oauth\/status$/)) {
      return json(200, {
        upstream_id: pathname.split('/')[4],
        kind: 'anthropic_oauth',
        has_credentials: true,
        status: 'active',
        expires_at_unix_secs: Math.floor(Date.now() / 1000) + 3600,
        refresh_token_present: true,
        scopes: ['user:inference'],
      });
    }
    if (pathname.match(/^\/admin\/v1\/upstreams\/([^/]+)\/subscription-metadata/)) {
      return json(200, { upstream_id: pathname.split('/')[4], subscription_metadata: null, organization_metadata: null });
    }
    if (pathname === '/admin/v1/status') {
      return json(200, {
        version: 'mock',
        git_sha: 'mock',
        uptime_secs: 1,
        build: { rust_version: 'mock', profile: 'debug', target: 'mock' },
        generation: 1,
        upstreams: upstreams.map((upstream) => ({ id: upstream.id, name: upstream.name, status: upstream.enabled ? 'active' : 'disabled', last_apply_at_unix_secs: null, last_apply_error: null })),
      });
    }
    if (pathname === '/admin/v1/dashboard/usage') {
      return json(200, {
        range: url.searchParams.get('range') ?? '24h',
        step: url.searchParams.get('step') ?? 'hour',
        group_by: url.searchParams.get('group_by') ?? 'model',
        window_start_unix_secs: Math.floor(Date.now() / 1000) - 3600,
        window_end_unix_secs: Math.floor(Date.now() / 1000),
        observed: true,
        series: [{ key: 'claude-sonnet', buckets: [{ bucket_start_unix_secs: Math.floor(Date.now() / 1000) - 1800, request_count: 1, input_tokens: 100, output_tokens: 200, cache_creation_input_tokens: 0, cache_read_input_tokens: 0, error_count: 0, virtual_cost_micros: 1234, latency_ms_sum: 100, latency_count: 1 }] }],
      });
    }
    if (pathname === '/admin/v1/events/recent') {
      return json(200, { events: [], observed: true, count: 0, limit: 5 });
    }
    if (pathname === '/admin/v1/subscription-quotas/latest') {
      return json(200, {
        now_unix_secs: Math.floor(Date.now() / 1000),
        max_staleness_secs: 300,
        upstreams: options.latestWindows
          ? [{ upstream_id: 'oauth-healthy', windows: options.latestWindows }]
          : [],
      });
    }
    if (pathname === '/admin/v1/subscription-quotas/series') {
      return json(200, { since_unix_secs: 0, until_unix_secs: 0, bucket_secs: 60, source: 'merged', series: [] });
    }
    return json(200, {});
  });
  return {
    warmupAttemptRequests: () => warmupAttemptRequests,
  };
}

async function openUpstreams(page: Page, selectedId = 'oauth-healthy') {
  await page.goto(`/upstreams?selectedId=${selectedId}`);
  await expect(page.getByTestId('warmup-card').or(page.getByTestId('api-usage-card'))).toBeVisible();
}

async function confirmFireNow(page: Page) {
  await page.getByTestId('warmup-fire-now').click();
  const dialog = page.getByRole('alertdialog', { name: COPY.confirmFireTitle });
  await expect(dialog).toBeVisible();
  await dialog.getByRole('button', { name: COPY.confirmFireConfirmLabel, exact: true }).click();
}

async function noSeriousA11yViolations(page: Page) {
  await page.addScriptTag({
    url: 'https://cdnjs.cloudflare.com/ajax/libs/axe-core/4.10.2/axe.min.js',
  });
  return page.evaluate(async () => {
    const region = document.querySelector('[data-testid="warmup-card"]');
    const axe = (window as typeof window & { axe?: { run: (context: Element, options: unknown) => Promise<{ violations: Array<{ id: string; impact: string | null }> }> } }).axe;
    if (!region || !axe) return [];
    const results = await axe.run(region, { runOnly: { type: 'tag', values: ['wcag2a', 'wcag2aa'] } });
    return results.violations.filter((violation) => ['serious', 'critical'].includes(violation.impact ?? ''));
  });
}

test.describe('WarmupCard', () => {

  test('Scenario 1: Steady-state happy path (OAuth + enabled + populated)', async ({ page }, testInfo) => {
    const seeded = oauthHealthy({ warmup_dialect_plugin: { wasm_registry_id: 'anthropic-shape-v2', config: {} } });
    await installAppFixtures(page, { upstreams: [seeded, oauthDisabled(), apiKeyUpstream()] });
    await openUpstreams(page);

    await expect(page.getByTestId('warmup-card')).toBeVisible();
    await expect(page.getByTestId('warmup-switch')).toBeChecked();
    await expect(page.getByTestId('warmup-last')).toContainText('Success');
    await expect(page.getByTestId('warmup-last')).toContainText(/\d+[mhd] ago/);
    await expect(page.getByTestId('warmup-plugin-select')).toContainText('anthropic-shape-v2');
    await expect(page.getByTestId('warmup-fire-now')).toBeEnabled();
    await page.screenshot({ path: testInfo.outputPath('scenario-1-steady-state.png'), fullPage: true });
  });

  test('Scenario 1a: Weekly-only warmup card, help, and confirmation use neutral quota-window copy', async ({ page }) => {
    const now = Math.floor(Date.now() / 1000);
    const baseSnap = {
      status: null,
      resets_at_unix_secs: now + 7 * 24 * 3600,
      surpassed_threshold: null,
      representative_claim: null,
      disabled_reason: null,
      extra_usage_enabled: null,
      extra_usage_monthly_limit: null,
      extra_usage_used_credits: null,
      age_secs: 0,
      source: 'api' as const,
      observed_at_unix_millis: now * 1000,
    };
    await installAppFixtures(page, {
      upstreams: [oauthHealthy()],
      latestWindows: [
        { ...baseSnap, window: '5h', state: 'absent', utilization: null, resets_at_unix_secs: null },
        { ...baseSnap, window: '7d', state: 'fresh', utilization: 0.2 },
      ],
    });
    await openUpstreams(page);
    const quota = page.getByTestId('quota-snapshot-grid');
    await expect(quota.getByRole('meter', { name: '7d', exact: true })).toBeVisible();
    await expect(quota.getByRole('meter', { name: '5h', exact: true })).toHaveCount(0);

    const fiveHourWording = /\b(?:5[-\s]*(?:h|hours?)|five[- ]hours?)\b/i;
    const card = page.getByTestId('warmup-card');
    await expect(card.getByText('Prepares the next quota window during idle gaps.', { exact: true })).toBeVisible();
    await expect(card).not.toContainText(fiveHourWording);
    await card.getByLabel('Warm-up help', { exact: true }).hover();
    const help = page.getByText(/^Warm-up sends a tiny background Anthropic request during idle time/);
    await expect(help).toBeVisible();
    await expect(help).toContainText('the next quota window');
    await expect(help).not.toContainText(fiveHourWording);

    await card.getByRole('button', { name: COPY.fireNowButtonLabel, exact: true }).click();
    const dialog = page.getByRole('alertdialog', { name: COPY.confirmFireTitle });
    await expect(dialog).toBeVisible();
    await expect(dialog.getByText(COPY.confirmFireBody.replace('{upstreamName}', 'oauth-healthy'), { exact: true })).toBeVisible();
    await expect(dialog).toContainText('current quota window');
    await expect(dialog).not.toContainText(fiveHourWording);
    await dialog.getByRole('button', { name: COPY.confirmFireConfirmLabel, exact: true }).click();
    await expect(page.getByText(COPY.fireSuccess, { exact: true })).toBeVisible();
    expect(COPY.fireSuccess).not.toMatch(fiveHourWording);
  });

  test('Scenario 1b: History data loads only after opening the drawer', async ({
    page,
  }) => {
    const fixtures = await installAppFixtures(page);
    await openUpstreams(page);

    expect(fixtures.warmupAttemptRequests()).toBe(0);
    await page.getByTestId('warmup-history-button').click();
    await expect(page.getByTestId('warmup-history-drawer')).toBeVisible();
    await expect.poll(() => fixtures.warmupAttemptRequests()).toBe(1);
  });

  test('Scenario 2: Fire-now confirm + 200 success', async ({ page }, testInfo) => {
    await installAppFixtures(page, {
      onFireNow: () => ({ status: 200, body: { fired: true, cycle_key: 1718380800 } }),
    });
    await openUpstreams(page);
    await confirmFireNow(page);

    await expect(page.getByTestId('warmup-fire-now')).toBeDisabled();
    await page.screenshot({ path: testInfo.outputPath('scenario-2-fire-success.png'), fullPage: true });
    await expect(page.getByText(COPY.fireSuccess)).toBeVisible();
    await expect(page.getByTestId('warmup-fire-now')).toBeEnabled({ timeout: 2_000 });
  });

  test('Scenario 3: Fire-now 202 lease_held inline panel', async ({ page }, testInfo) => {
    await installAppFixtures(page, {
      onFireNow: () => ({ status: 202, body: { fired: false, reason: 'lease_held', held_by: 'background-loop' } }),
    });
    await openUpstreams(page);
    await confirmFireNow(page);

    const panel = page.getByTestId('warmup-lease-panel');
    await expect(panel).toBeVisible();
    await expect(panel).toHaveAttribute('aria-live', 'polite');
    await expect(panel).toContainText('background-loop');
    await expect(panel).toContainText('Try again in ~30 seconds');
    await expect(page.locator('[data-sonner-toast][data-type="error"]')).toHaveCount(0);
    await page.screenshot({ path: testInfo.outputPath('scenario-3-lease-held.png'), fullPage: true });
    await expect(panel).toHaveCount(0, { timeout: 12_000 });
  });

  for (const reason of ['auth_failed', 'forbidden', 'bad_request', 'not_found', 'dialect_plugin_failed'] as const) {
    test(`Scenario 4: Fire-now 502 ${reason}`, async ({ page }, testInfo) => {
      await installAppFixtures(page, {
        onFireNow: () => ({ status: 502, body: { fired: false, reason } }),
      });
      await openUpstreams(page);
      await confirmFireNow(page);

      const panel = page.getByTestId('warmup-error-panel');
      await expect(panel).toHaveAttribute('data-reason', reason);
      await expect(panel).toHaveText(COPY.fireErrorReasons[reason]);
      await page.screenshot({ path: testInfo.outputPath(`scenario-4-error-${reason}.png`), fullPage: true });
    });
  }

  test('Scenario 5: Fire-now 503 transient', async ({ page }, testInfo) => {
    await installAppFixtures(page, {
      onFireNow: () => ({ status: 503, body: { fired: false, reason: 'dialect_plugin_transient' } }),
    });
    await openUpstreams(page);
    await confirmFireNow(page);

    const panel = page.getByTestId('warmup-error-panel');
    await expect(panel).toHaveAttribute('data-reason', 'dialect_plugin_transient');
    await expect(panel).toHaveText(COPY.fireErrorReasons.dialect_plugin_transient);
    await page.screenshot({ path: testInfo.outputPath('scenario-5-transient.png'), fullPage: true });
  });

  test('Scenario 6: Dialect plugin save (PATCH happy path)', async ({ page }, testInfo) => {
    let patchBody: unknown;
    let ifMatch: string | null = null;
    await installAppFixtures(page, {
      upstreams: [oauthHealthy({ warmup_dialect_plugin: null }), oauthDisabled(), apiKeyUpstream()],
      onPatch: async (request, upstream) => {
        patchBody = request.postDataJSON();
        ifMatch = request.headers()['if-match'] ?? null;
        upstream.warmup_dialect_plugin = { wasm_registry_id: 'anthropic-shape-v2', config: {} };
        upstream.spec_revision += 1;
        return { status: 200, body: upstream };
      },
    });
    await openUpstreams(page);
    await page.getByTestId('warmup-plugin-select').click();
    await page.getByRole('option', { name: 'anthropic-shape-v2', exact: true }).click();

    await expect.poll(() => patchBody).toEqual({ warmup_dialect_plugin: { wasm_registry_id: 'anthropic-shape-v2', config: {} } });
    expect(ifMatch).toBe(`W/"${specRevision}"`);
    await testInfo.attach('scenario-6-patch-body.json', { body: JSON.stringify(patchBody, null, 2), contentType: 'application/json' });
    await expect(page.getByTestId('warmup-plugin-select')).toContainText('anthropic-shape-v2');
    await expect(page.getByText(COPY.dialectPluginSaveSuccess)).toBeVisible();
    await page.screenshot({ path: testInfo.outputPath('scenario-6-plugin-save.png'), fullPage: true });
  });

  test('Scenario 7: Dialect plugin clear (DELETE)', async ({ page }, testInfo) => {
    const seeded = oauthHealthy({ warmup_dialect_plugin: { wasm_registry_id: 'anthropic-shape-v2', config: {} } });
    let deleteUrl = '';
    let ifMatch: string | null = null;
    await installAppFixtures(page, {
      upstreams: [seeded, oauthDisabled(), apiKeyUpstream()],
      onDelete: async (request, upstream) => {
        deleteUrl = new URL(request.url()).pathname;
        ifMatch = request.headers()['if-match'] ?? null;
        upstream.warmup_dialect_plugin = null;
        upstream.spec_revision += 1;
        return { status: 200, body: upstream };
      },
    });
    await openUpstreams(page);
    await page.getByTestId('warmup-plugin-clear').click();
    const dialog = page.getByRole('alertdialog', { name: COPY.confirmClearPluginTitle });
    await expect(dialog).toBeVisible();
    await dialog.getByRole('button', { name: COPY.confirmClearPluginConfirmLabel, exact: true }).click();

    await expect.poll(() => deleteUrl).toBe('/admin/v1/upstreams/oauth-healthy/warmup-dialect-plugin');
    expect(ifMatch).toBe(`W/"${specRevision}"`);
    await expect(page.getByTestId('warmup-plugin-select')).toContainText(COPY.defaultPluginOption);
    await expect(page.getByTestId('warmup-plugin-clear')).toHaveCount(0);
    await expect(page.getByText(COPY.dialectPluginClearSuccess)).toBeVisible();
    await page.screenshot({ path: testInfo.outputPath('scenario-7-plugin-clear.png'), fullPage: true });
  });

  test('Scenario 8: Stale-revision 409 recovery', async ({ page }, testInfo) => {
    let attempts = 0;
    await installAppFixtures(page, {
      onPatch: async (_request, upstream) => {
        attempts += 1;
        if (attempts === 1) return { status: 409, body: { error: 'stale_revision', current_revision: specRevision + 1 } };
        upstream.warmup_dialect_plugin = { wasm_registry_id: 'anthropic-shape-v2', config: {} };
        upstream.spec_revision = specRevision + 2;
        return { status: 200, body: upstream };
      },
    });
    await openUpstreams(page);
    await page.getByTestId('warmup-plugin-select').click();
    await page.getByRole('option', { name: 'anthropic-shape-v2', exact: true }).click();

    await expect(page.getByTestId('warmup-stale-hint')).toHaveText(COPY.staleRevisionHint);
    await expect(page.getByTestId('warmup-plugin-select')).toContainText('anthropic-shape-v2');
    await page.getByTestId('warmup-plugin-select').click();
    await page.getByRole('option', { name: 'anthropic-shape-v2', exact: true }).click();
    await expect.poll(() => attempts).toBe(2);
    await expect(page.getByTestId('warmup-stale-hint')).toHaveCount(0);
    await page.screenshot({ path: testInfo.outputPath('scenario-8-stale-revision.png'), fullPage: true });
  });

  test('Scenario 9: Disabled upstream still exposes warmup controls when warmup is enabled', async ({ page }, testInfo) => {
    const warmupOnlyUpstream = oauthHealthy({
      id: 'oauth-paused-stale',
      name: 'oauth-paused-stale',
      enabled: false,
      warmup_enabled: true,
      status: {
        last_apply_error: null,
        last_apply_at_unix_secs: null,
        last_warmup_at_unix_secs: Math.floor(Date.now() / 1000) - 2 * 60 * 60,
      },
    });
    await installAppFixtures(page, {
      upstreams: [oauthHealthy(), warmupOnlyUpstream, apiKeyUpstream()],
    });
    await openUpstreams(page, 'oauth-paused-stale');

    await expect(page.getByTestId('warmup-card')).toBeVisible();
    await expect(page.getByTestId('warmup-switch')).toBeChecked();
    await expect(page.getByTestId('warmup-fire-now')).toBeVisible();
    await expect(page.getByTestId('warmup-enable-btn')).toHaveCount(0);
    await page.screenshot({ path: testInfo.outputPath('scenario-9-warmup-only-while-disabled.png'), fullPage: true });
  });

  test('Scenario 10: api_key upstream renders nothing', async ({ page }, testInfo) => {
    await installAppFixtures(page);
    await openUpstreams(page, 'api-key-id');

    await expect(page.locator('[data-testid="warmup-card"]')).toHaveCount(0);
    await expect(page.getByTestId('api-usage-card')).toBeVisible();
    await page.screenshot({ path: testInfo.outputPath('scenario-10-api-key-hidden.png'), fullPage: true });
  });

  test('Scenario 11: Unknown plugin fallback', async ({ page }, testInfo) => {
    await installAppFixtures(page, {
      upstreams: [oauthHealthy({ warmup_dialect_plugin: { wasm_registry_id: 'ghost-plugin', config: {} } }), oauthDisabled(), apiKeyUpstream()],
      plugins: [shapePlugin('anthropic-shape-v2')],
    });
    await openUpstreams(page);

    await page.getByTestId('warmup-plugin-select').click();
    await expect(page.getByRole('option', { name: COPY.unknownPluginTemplate.replace('{id}', 'ghost-plugin'), exact: true })).toHaveCount(1);
    await expect(page.getByTestId('warmup-plugin-clear')).toBeVisible();
    await page.screenshot({ path: testInfo.outputPath('scenario-11-unknown-plugin.png'), fullPage: true });
  });

  test('Scenario 12: Zero shape-plugins available', async ({ page }, testInfo) => {
    await installAppFixtures(page, { plugins: [routerPlugin()] });
    await openUpstreams(page);

    await expect(page.getByTestId('warmup-plugin-select')).toHaveCount(0);
    await expect(page.getByText(COPY.noShapePluginsAvailable)).toBeVisible();
    await expect(page.getByTestId('warmup-card').getByRole('link', { name: 'Plugins' })).toHaveAttribute('href', '/plugins');
    await page.screenshot({ path: testInfo.outputPath('scenario-12-no-shape-plugins.png'), fullPage: true });
  });

  test('Scenario 13: A11y keyboard tab order + focus return', async ({ page }, testInfo) => {
    await installAppFixtures(page, {
      upstreams: [oauthHealthy({ warmup_dialect_plugin: { wasm_registry_id: 'anthropic-shape-v2', config: {} } }), oauthDisabled(), apiKeyUpstream()],
    });
    await openUpstreams(page);

    await page.getByTestId('warmup-card').focus();
    const expectedOrder = ['warmup-history-button', 'warmup-fire-now', 'warmup-switch', 'warmup-plugin-select', 'warmup-plugin-clear'];
    const focusOrder: string[] = [];
    for (let index = 0; index < expectedOrder.length; index += 1) {
      await page.keyboard.press('Tab');
      focusOrder.push(await page.evaluate(() => (document.activeElement as HTMLElement | null)?.dataset.testid ?? ''));
    }
    expect(focusOrder).toEqual(expectedOrder);

    await page.getByTestId('warmup-fire-now').click();
    const dialog = page.getByRole('alertdialog', { name: COPY.confirmFireTitle });
    await expect(dialog.getByRole('button', { name: COPY.confirmFireConfirmLabel, exact: true })).toBeFocused();
    await page.keyboard.press('Escape');
    await expect(page.getByTestId('warmup-fire-now')).toBeFocused();
    const violations = await noSeriousA11yViolations(page);
    await testInfo.attach('scenario-13-axe-results.json', { body: JSON.stringify(violations, null, 2), contentType: 'application/json' });
    expect(violations).toEqual([]);
    await page.screenshot({ path: testInfo.outputPath('scenario-13-a11y-focus.png'), fullPage: true });
  });
});
