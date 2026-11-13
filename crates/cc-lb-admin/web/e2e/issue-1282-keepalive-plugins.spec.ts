import { expect, type Page, test } from '@playwright/test';

const PRINCIPAL_ID = 'openai-principal';
const principal = {
  id: PRINCIPAL_ID,
  name: 'openai-principal',
  kind: 'machine',
  enabled: true,
  revision: 7,
  allowed_models: ['gpt-5.6'],
  allowed_upstreams: [],
  default_limits: [],
  cache_keepalive: {
    enabled: true,
    refresh_lead_time_5m_secs: 45,
    refresh_lead_time_1h_secs: 400,
    max_refreshes_per_session: 12,
    max_total_duration_secs: 14_400,
    snapshot_max_bytes: 8_388_608,
    min_cache_tokens: 10_000,
    classifier: {
      extra_wait_for_user_tools: [],
      treat_end_turn_as_ambiguous: false,
      openai_responses_enabled: true,
    },
  },
};

const upstream = {
  id: 'openai-upstream',
  name: 'OpenAI API key',
  kind: 'openai_api_key',
  enabled: true,
  base_url: null,
  spec_revision: 1,
  warmup_enabled: false,
  warmup_dialect_plugin: null,
  status: {
    last_apply_error: null,
    last_apply_at_unix_secs: null,
    last_warmup_at_unix_secs: null,
  },
};

const summary = {
  renewing_now: 1,
  sessions_last_5m: 2,
  retained_successful_hits: 3,
  cost_saved: null,
  total_spent: 0,
  total_avoided: null,
  unlisted_sessions: 0,
  unlisted_renewals: 0,
  unlisted_spent: 0,
  unlisted_net: 0,
  evidence_status: 'none',
  unknown_cycles: 0,
  retained_from_ms: null,
};

const plugins = ['First plugin', 'Second plugin'].map((name, index) => ({
  id: `plugin-${index + 1}`,
  sha256_hex: String(index + 1).repeat(64),
  name,
  original_filename: `plugin-${index + 1}.wasm`,
  description: `${name} description`,
  usage: `${name} usage`,
  hook_metadata: {},
  label: null,
  size_bytes: 1024,
  refcount: 1,
  revision: 1,
  uploaded_at_unix_secs: 1_700_000_000,
  is_builtin: false,
  version: '1.0.0',
  metadata: null,
  supported_slots: ['router'],
}));

type FixtureState = {
  registry: 'error' | 'populated' | 'empty';
  referencesFail: boolean;
};

async function installFixtures(
  page: Page,
  theme: 'dark' | 'light',
  state: FixtureState = { registry: 'empty', referencesFail: false },
) {
  await page.addInitScript((nextTheme) => {
    window.localStorage.setItem('cc-lb-admin-token', 'mock-token');
    window.localStorage.setItem('cclb.locale', 'en-US');
    window.localStorage.setItem('cclb.theme', nextTheme);
  }, theme);
  await page.route('**/admin/**', async (route) => {
    const { pathname } = new URL(route.request().url());
    const json = (body: unknown, status = 200) =>
      route.fulfill({
        status,
        contentType: 'application/json',
        body: JSON.stringify(body),
      });

    if (pathname === '/admin/v1/auth/session') {
      return json({
        authority: 'e2e',
        subject: 'operator',
        kind: 'human',
        provider_id: 'static-token',
        email: null,
        display_name: 'E2E operator',
        expires_at_unix_secs: null,
        auth_mode: 'static_token',
      });
    }
    if (pathname === '/admin/health' || pathname === '/admin/v1/status') {
      return json({
        status: 'ok',
        version: 'e2e',
        git_sha: 'fixture',
        uptime_secs: 1,
        build: { rust_version: 'fixture', profile: 'dev', target: 'fixture' },
        generation: 1,
        upstreams: [],
      });
    }
    if (pathname === '/admin/v1/principals') {
      return json({ principals: [principal] });
    }
    if (pathname === '/admin/v1/upstreams') {
      return json({ upstreams: [upstream] });
    }
    if (pathname === '/admin/v1/plugins/registry') {
      return state.registry === 'error'
        ? json({ error: { code: 'unavailable', message: 'Service unavailable' } }, 503)
        : json({ entries: state.registry === 'populated' ? plugins : [] });
    }
    if (pathname === '/admin/v1/plugins/registry/plugin-1/references') {
      return state.referencesFail
        ? json({ error: { code: 'unavailable', message: 'Service unavailable' } }, 503)
        : json({
            registry: plugins[0],
            refcount: 1,
            reference_fingerprint: 'fixture-references',
            references: [{
              kind: 'plugin_chain',
              chain_entry_id: 'chain-1',
              principal_id: PRINCIPAL_ID,
              principal_name: principal.name,
              slot: 'router',
              revision: 7,
            }],
          });
    }
    if (/^\/admin\/v1\/principals\/[^/]+\/plugin-chain$/.test(pathname)) {
      return json({ entries: [] });
    }
    if (/^\/admin\/v1\/principals\/[^/]+\/router-terminal$/.test(pathname)) {
      return json({ strategy: 'first-pick', revision: 7 });
    }
    if (/^\/admin\/v1\/principals\/[^/]+\/keys$/.test(pathname)) {
      return json({ keys: [] });
    }
    if (pathname === '/admin/v1/dashboard/usage') {
      return json({
        range: '24h',
        step: 'hour',
        group_by: 'principal',
        window_start_unix_secs: 1_700_000_000,
        window_end_unix_secs: 1_700_086_400,
        series: [],
        observed: true,
      });
    }
    if (pathname === '/admin/v1/events/recent') {
      return json({ events: [], next_cursor: null, count: 0, limit: 50, observed: true });
    }
    if (/^\/admin\/v1\/principals\/[^/]+\/cache-keepalive(?:\/summary)?$/.test(pathname)) {
      return json({ summary, groups: null, rows: [], next_cursor: null });
    }
    return json({});
  });
}

async function expectNoHorizontalOverflow(page: Page) {
  expect(
    await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth),
  ).toBe(true);
}

for (const { viewport, theme } of [
  { viewport: { width: 1440, height: 900 }, theme: 'dark' },
  { viewport: { width: 390, height: 844 }, theme: 'light' },
] as const) {
  test(`Cache keepalive has dual-horizon copy without overflow at ${viewport.width}px in ${theme}`, async ({ page }) => {
    await page.setViewportSize(viewport);
    await installFixtures(page, theme);
    await page.goto(`/principals?selectedId=${PRINCIPAL_ID}`);
    await expect(page.locator('html')).toHaveAttribute('data-theme', theme);
    await page.getByTestId('cache-keepalive-card').getByRole('button', { name: 'Settings', exact: true }).click();

    const drawer = page.getByRole('dialog', { name: /Cache keepalive settings/i });
    await expect(drawer).toBeVisible();
    await expect(drawer.getByLabel('Renewal lead time · 5m / 30m')).toHaveValue('45');
    await expect(drawer.getByText(
      'Seconds. Renews 45s before an Anthropic 5m or OpenAI 30m cache expires.',
      { exact: true },
    )).toBeVisible();
    await expect(drawer.getByLabel('Renewal lead time · 1h / 12h')).toHaveValue('400');
    await expect(drawer.getByText(
      'Seconds. Renews 400s before an Anthropic 1h cache expires. OpenAI 12h retention is recorded, not renewed.',
      { exact: true },
    )).toBeVisible();
    await expect(page.getByText(/5m TTL|1h TTL/)).toHaveCount(0);
    await expectNoHorizontalOverflow(page);
  });
}

for (const viewport of [
  { width: 390, height: 844 },
  { width: 768, height: 1024 },
  { width: 1440, height: 900 },
]) {
  for (const theme of ['dark', 'light'] as const) {
    test(`Plugin errors recover and retain stale data at ${viewport.width}px in ${theme}`, async ({ page }) => {
      const state: FixtureState = { registry: 'error', referencesFail: true };
      await page.setViewportSize(viewport);
      await installFixtures(page, theme, state);
      await page.goto('/plugins');
      await expect(page.locator('html')).toHaveAttribute('data-theme', theme);

      // Production retries once before settling; auto-wait for the final error.
      const alert = page.getByRole('alert');
      await expect(alert).toHaveCount(1);
      await expect(alert).toContainText('Could not load plugin library');
      await expect(alert).toBeVisible();
      const count = page.getByTestId('plugin-count-slot');
      await expect(count).toHaveText('Unavailable');
      await expect(count).not.toContainText('0 available');
      await expect(page.getByText('No plugins uploaded.', { exact: true })).toHaveCount(0);
      const retry = alert.getByRole('button', { name: 'Retry', exact: true });
      await expect(retry).toBeVisible();
      await expect(page.getByRole('button', { name: 'Clean orphaned uploads', exact: true })).toBeEnabled();
      if (viewport.width === 390) {
        const bounds = await retry.boundingBox();
        expect(bounds).not.toBeNull();
        expect(bounds?.height).toBeGreaterThanOrEqual(40);
      }
      await expectNoHorizontalOverflow(page);

      state.registry = 'populated';
      await retry.click();
      await expect(count).toHaveText('2 available · 0 not used anywhere');
      const pluginRow = (name: string) => viewport.width < 768
        ? page.getByRole('button', { name: new RegExp(`^${name}`) })
        : page.getByRole('row').filter({ hasText: name });
      await expect(pluginRow('First plugin')).toBeVisible();
      await expect(pluginRow('Second plugin')).toBeVisible();
      await expect(alert).toHaveCount(0);
      await expectNoHorizontalOverflow(page);

      // Keep the singleton QueryClient alive. Advance only Date (not timers)
      // beyond staleTime, then reconnect to exercise its real refetch path.
      state.registry = 'error';
      const now = await page.evaluate(() => Date.now());
      await page.clock.setFixedTime(new Date(now + 10_001));
      await page.context().setOffline(true);
      await page.context().setOffline(false);
      const warning = page.getByRole('status').filter({ hasText: 'Could not refresh plugin library' });
      await expect(warning).toBeVisible();
      await expect(warning).toHaveCount(1);
      await expect(warning).toContainText('Showing the last loaded plugin list.');
      await expect(count).toHaveText('2 available · 0 not used anywhere');
      await expect(pluginRow('First plugin')).toBeVisible();
      await expect(pluginRow('Second plugin')).toBeVisible();
      await expect(alert).toHaveCount(0);
      await expect(page.getByText('No plugins uploaded.', { exact: true })).toHaveCount(0);
      await expectNoHorizontalOverflow(page);

      state.registry = 'populated';
      await warning.getByRole('button', { name: 'Retry', exact: true }).click();
      await expect(warning).toHaveCount(0);
      await pluginRow('First plugin').click();
      await expect(page.getByRole('heading', { name: 'First plugin', exact: true })).toBeVisible();
      const usedBy = page.getByTestId('plugin-used-by-slot');
      const usageAlert = usedBy.getByRole('alert');
      await expect(usageAlert).toHaveCount(1);
      await expect(usageAlert).toBeVisible();
      await expect(usageAlert).toContainText('Could not load plugin usage');
      await expect(usedBy.getByText('This plugin is uploaded but not used anywhere yet.', { exact: true })).toHaveCount(0);
      await expectNoHorizontalOverflow(page);

      state.referencesFail = false;
      await usageAlert.getByRole('button', { name: 'Retry', exact: true }).click();
      await expect(usedBy.getByRole('link', { name: principal.name, exact: true })).toBeVisible();
      await expect(usedBy.getByRole('link', { name: principal.name, exact: true })).toHaveAttribute('href', `/principals?selectedId=${PRINCIPAL_ID}`);
      await expect(usedBy.getByRole('alert')).toHaveCount(0);
      await expect(usedBy.getByRole('button', { name: 'Retry', exact: true })).toHaveCount(0);
      await expectNoHorizontalOverflow(page);
    });
  }
}

test('Healthy empty plugin registry remains a genuine empty library', async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await installFixtures(page, 'dark');
  await page.goto('/plugins');
  await expect(page.getByTestId('plugin-count-slot')).toHaveText('0 available · 0 not used anywhere');
  await expect(page.getByRole('cell', { name: 'No plugins uploaded.', exact: true })).toBeVisible();
  await expect(page.getByRole('alert')).toHaveCount(0);
  await expect(page.getByRole('button', { name: 'Retry', exact: true })).toHaveCount(0);
  await expectNoHorizontalOverflow(page);
});
