import { expect, test, type Page } from '@playwright/test';

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
    refresh_lead_time_5m_secs: 30,
    refresh_lead_time_1h_secs: 300,
    max_refreshes_per_session: 12,
    max_total_duration_secs: 14_400,
    snapshot_max_bytes: 8_388_608,
    min_cache_tokens: 10_000,
    classifier: {
      extra_wait_for_user_tools: [],
      treat_end_turn_as_ambiguous: false,
      openai_responses_enabled: false,
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

async function installFixtures(page: Page) {
  await page.addInitScript(() => {
    window.localStorage.setItem('cc-lb-admin-token', 'mock-token');
  });
  await page.route('**/admin/**', async (route) => {
    const request = route.request();
    const url = new URL(request.url());
    const { pathname } = url;
    const json = (body: unknown) =>
      route.fulfill({
        status: 200,
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
    if (pathname === '/admin/v1/status') {
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
      return json({ entries: [] });
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
      return json({
        events: [],
        next_cursor: null,
        count: 0,
        limit: 50,
        observed: true,
      });
    }
    if (/^\/admin\/v1\/principals\/[^/]+\/cache-keepalive$/.test(pathname)) {
      return json({ summary, groups: null, rows: [], next_cursor: null });
    }
    return json({});
  });
}

test('OpenAI keepalive controls remain reachable across responsive principal details', async ({
  page,
}) => {
  await installFixtures(page);

  for (const viewport of [
    { width: 390, height: 844 },
    { width: 768, height: 1024 },
    { width: 1440, height: 900 },
  ]) {
    await page.setViewportSize(viewport);
    await page.goto(`/principals?selectedId=${PRINCIPAL_ID}`);

    await expect(page.getByTestId('cache-keepalive-card')).toBeVisible();
    const description = page.getByText(
      'Keeps prompt caches warm during idle gaps.',
      { exact: true },
    );
    await expect(description).toBeVisible();
    expect(
      await description.evaluate((element) => {
        const range = document.createRange();
        range.selectNodeContents(element);
        return range.getClientRects().length;
      }),
    ).toBe(1);

    const helpText = page.getByText(
      'OpenAI Responses keepalive is for GPT-5.6+ models: 30m sessions may renew during idle gaps, while 12h retention is recorded without read re-anchoring.',
      { exact: true },
    );
    await expect(helpText).not.toBeVisible();
    const help = page.getByRole('button', { name: 'Cache keepalive help' });
    await page.getByRole('button', { name: 'Sessions', exact: true }).focus();
    await page.keyboard.press('Shift+Tab');
    await expect(helpText).toBeVisible();
    await expect(help).toBeFocused();
    await page.keyboard.press('Escape');
    await expect(helpText).not.toBeVisible();
    await expect(help).toBeFocused();
    await help.hover();
    await expect(helpText).toBeVisible();
    await page.mouse.move(0, 0);
    await page.keyboard.press('Escape');
    await expect(helpText).not.toBeVisible();
    await help.focus();
    await page.keyboard.press('Tab');
    await page.keyboard.press('Shift+Tab');
    await expect(help).toBeFocused();
    await expect(helpText).toBeVisible();
    await page.keyboard.press('Escape');
    await expect(helpText).not.toBeVisible();
    await expect(help).toBeFocused();

    await expect(page.getByRole('button', { name: 'Settings' })).toBeVisible();
    await expect(page.getByRole('button', { name: 'Sessions' })).toBeVisible();
    await expect(page.locator('html')).toHaveJSProperty(
      'scrollWidth',
      viewport.width,
    );

    await page.getByRole('button', { name: 'Settings' }).click();
    await expect(
      page.getByRole('dialog', { name: /Cache keepalive settings/i }),
    ).toBeVisible();
    await expect(
      page.getByRole('switch', {
        name: 'OpenAI Responses GPT-5.6+ keepalive',
      }),
    ).toBeVisible();
    await page.keyboard.press('Escape');

    await page.getByRole('button', { name: 'Sessions' }).click();
    await expect(page.getByTestId('cache-keepalive-scope')).toContainText(
      'Principal-wide history',
    );
    await page.keyboard.press('Escape');
  }
});

for (const key of ['Enter', 'Space']) {
  test(`fresh request metric keeps keyboard focus through ${key} and Escape`, async ({ page }) => {
    await installFixtures(page);
    await page.route('**/admin/v1/events/recent**', (route) => route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({
        events: [{
          event_id: 'keyboard-metric',
          request_id: 'keyboard-metric',
          principal_id: PRINCIPAL_ID,
          ts: Math.floor(Date.now() / 1000),
          ts_ms: Date.now(),
          status: 200,
          duration_ms: 100,
          model: 'gpt-5.6',
          source_kind: 'messages',
        }],
        next_cursor: null,
        count: 1,
        limit: 50,
        observed: true,
      }),
    }));
    await page.goto(`/principals?selectedId=${PRINCIPAL_ID}`);
    const metric = page.getByRole('button', { name: /^Latency .* show breakdown$/ });
    const breakdown = page.getByText('Latency by responsibility', { exact: true });
    await expect(metric).toBeVisible();
    await metric.focus();
    await page.keyboard.press('Shift+Tab');
    await page.keyboard.press('Tab');
    await expect(metric).toBeFocused();
    await expect(breakdown).not.toBeVisible();
    await page.keyboard.press(key);
    await expect(breakdown).toBeVisible();
    await expect(metric).toBeFocused();
    await page.keyboard.press('Escape');
    await expect(breakdown).not.toBeVisible();
    await expect(metric).toBeFocused();
    await expect(page.getByRole('dialog', { name: 'Request detail' })).not.toBeVisible();
  });
}
