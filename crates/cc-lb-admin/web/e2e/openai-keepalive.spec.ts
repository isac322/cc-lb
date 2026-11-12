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
    await expect(page.getByRole('dialog', { name: 'Cache keepalive sessions' })).toBeVisible();
    await expect(page.getByText(/Principal-wide|All providers/)).toHaveCount(0);
    await page.keyboard.press('Escape');
  }
});

for (const key of ['Enter', 'Space', 'hover']) {
  test(`focused request metric keeps focus through ${key} and Escape`, async ({ page }) => {
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
    if (key === 'hover') await metric.hover();
    else await page.keyboard.press(key);
    await expect(breakdown).toBeVisible();
    await expect(metric).toBeFocused();
    await page.keyboard.press('Escape');
    await expect(breakdown).not.toBeVisible();
    await expect(metric).toBeFocused();
    await expect(page.getByRole('dialog', { name: 'Request detail' })).not.toBeVisible();
  });
}

const sessionStates = [
  ['renewed', 'Warm'],
  ['scheduled', 'Warm'],
  ['in_request', 'In use'],
  ['capped', 'At limit'],
  ['expired', 'Expired'],
  ['cancelled', 'Returned'],
  ['failed', 'Failed'],
  ['not_tracked', 'Not tracked'],
] as const;

async function installCycleFixtures(page: Page) {
  await installFixtures(page);
  await page.addInitScript(() => localStorage.setItem('cclb.timezone', 'UTC'));
  const base = Date.parse('2026-11-10T23:58:00Z');
  const rows = sessionStates.map(([state], index) => ({
    id: `${PRINCIPAL_ID}/thread:${index < 2 ? `01a1c484-${index}-${index === 0 ? 'aaaaaa' : 'bbbbbb'}` : `session-${index}`}`,
    state,
    is_active: state === 'scheduled' || state === 'renewed',
    last_message_at_ms: base + (8 - index) * 60_000,
    ttl: '5m',
    attempts: 1,
    retained_renewals: 3,
    max_attempts: 12,
    reason: state === 'cancelled' ? 'A new request arrived' : state,
    cancel_reason: state === 'cancelled' ? 'new_request' : null,
    generation: 3,
    upstream: 'Fixture upstream',
    error: state === 'failed' ? 'Renewal unavailable' : null,
    net_pnl: 0.1,
    total_spent: 0.03,
    total_avoided: 0.13,
    evidence_status: 'observed',
    session_key_hash: `session-${index}`,
    group_key: 'Agent in-turn',
    group_label: 'Agent in-turn',
    group_kind: 'state',
  }));
  const gap = (cycleId: string, pauseMs: number) => ({
    cycle_id: cycleId,
    pause_at_ms: pauseMs,
    cached_tokens: 20_000,
    pulses: [{
      pulse_number: 1, time_ms: pauseMs + 270_000, offset_sec: 270,
      since_prev_renewal_sec: null, spent_usd: 0.01, hit: true,
    }],
    ending_kind: 'followup_hit',
    ending_at_ms: pauseMs + 290_000,
    total_duration_sec: 290,
    since_last_renewal_sec: 20,
    spent_usd: 0.01,
    avoided_usd: 0.06,
    net_pnl_usd: 0.05,
  });
  const turns = [
    { cycle_id: 'current-cycle', time_ms: base + 870_000, pnl: null, renewals: 0, followed_up: null, evidence_status: 'unknown' },
    { cycle_id: null, time_ms: base + 570_000, pnl: 0.03, renewals: 1, followed_up: true, evidence_status: 'observed' },
    { cycle_id: 'previous-cycle', time_ms: base + 270_000, pnl: 0.05, renewals: 1, followed_up: true, evidence_status: 'observed' },
  ].map((turn) => ({ ...turn, turn_number: 1, label: 'agent-in-turn', pending: false }));
  await page.route('**/admin/v1/principals/*/cache-keepalive**', (route) => {
    const url = new URL(route.request().url());
    const row = rows.find((entry) => entry.id === url.searchParams.get('session_id')) ?? rows[0];
    const body = url.pathname.endsWith('/detail') ? {
      ...row,
      follow_up_outcomes: { observed: 1, pending: row.state === 'scheduled' ? 1 : 0, no_follow_up: 0, unknown: row.state === 'scheduled' ? 0 : 1 },
      renewal_tokens: 20_000, total_renewals_current_cycle: row.state === 'scheduled' ? 0 : 1,
      is_last_pending: false, config_snapshot: null,
      raw_record: {
        session_key_hash: row.session_key_hash, principal_id: PRINCIPAL_ID,
        upstream_id: 'fixture', generation: 3, renewal_count: 1,
        ttl: '5m', status: row.state, enqueue_state: null,
      },
      gaps: [gap('previous-cycle', base), { ...gap('current-cycle', base + 600_000), pulses: row.state === 'scheduled' ? [] : gap('current-cycle', base + 600_000).pulses, ending_kind: row.state === 'scheduled' ? 'scheduled_waiting' : 'followup_unknown', net_pnl_usd: null }],
      turns: row.state === 'scheduled' ? turns.filter((turn) => turn.cycle_id !== 'current-cycle') : turns,
    } : {
      summary,
      groups: [{ key: 'Agent in-turn', label: 'Agent in-turn', kind: 'state', sessions: rows.length, net_pnl: 0.8, total_spent: 0.24, total_avoided: 1.04 }],
      rows: url.searchParams.get('limit') === '0' ? [] : rows,
      next_cursor: null,
    };
    return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(body) });
  });
  return { rows };
}

for (const width of [390, 1440]) {
  test(`keepalive states and all cycle renewals remain visible at ${width}px`, async ({ page }, testInfo) => {
    await page.setViewportSize({ width, height: 1000 });
    await page.clock.install({ time: new Date('2026-11-11T12:00:00Z') });
    await installCycleFixtures(page);
    await page.goto(`/principals?selectedId=${PRINCIPAL_ID}`);
    await page.getByRole('button', { name: 'Sessions', exact: true }).click();
    for (let index = 0; index < sessionStates.length; index += 1) {
      const label = sessionStates[index][1];
      await expect(page.locator('[data-session-id]').nth(index).getByRole('status', { name: `Session state: ${label}`, exact: true })).toBeVisible();
    }
    await expect(page.locator('[data-session-id]').nth(0)).toContainText('01a1c484…aaaaaa');
    await expect(page.locator('[data-session-id]').nth(1)).toContainText('01a1c484…bbbbbb');
    for (let index = 2; index < sessionStates.length; index += 1) {
      await expect(page.locator('[data-session-id]').nth(index)).toContainText(`session-${index}`);
      await expect(page.locator('[data-session-id]').nth(index)).not.toContainText('…');
    }
    await page.screenshot({ path: testInfo.outputPath(`states-${width}.png`), animations: 'disabled' });
    await page.locator('[data-session-id]').first().click();
    await expect(page.getByRole('region', { name: 'Cycle 1 timeline' })).toBeVisible();
    await expect(page.getByRole('region', { name: 'Cycle 2 timeline' })).toBeVisible();
    await expect(page.getByText('11-10 23:58:00', { exact: true })).toBeVisible();
    await expect(page.getByText('Next message 11-11 00:02:50', { exact: true })).toBeVisible();
    await expect(page.getByRole('table')).toHaveCount(0);
    await expect(page.getByLabel('Follow-up outcomes')).toHaveAttribute('title', '1 observed · 0 no follow-up · 1 unknown · 0 pending');
    const firstCycle = page.getByRole('region', { name: 'Cycle 1 timeline' });
    await firstCycle.getByRole('button', { name: /Renewal #1:/ }).click();
    await expect(page.getByRole('tooltip')).toContainText('Hit');
    await expect(page.getByRole('tooltip')).toContainText('11 Nov 2026, 00:02:30');
    await expect(page.getByRole('tooltip')).toContainText('Observed');
    await page.keyboard.press('Escape');
    await expect(page.getByRole('dialog', { name: 'Cache keepalive sessions' })).toBeVisible();
    const secondCycle = page.getByRole('region', { name: 'Cycle 2 timeline' });
    await secondCycle.getByRole('button', { name: /Renewal #1:/ }).click();
    await expect(page.getByRole('tooltip')).toContainText('Unknown');
    await expect(page.getByRole('tooltip')).toContainText('Net');
    await page.keyboard.press('Escape');
    await expect(page.getByRole('region', { name: 'Additional renewal evidence' })).toContainText('Net +$0.03');
    await expect(page.locator('html')).toHaveJSProperty('scrollWidth', width);
    await page.screenshot({ path: testInfo.outputPath(`cycles-${width}.png`), animations: 'disabled' });
  });
}

for (const width of [390, 1440]) {
  test(`lifecycle change clears stale live detail at ${width}px`, async ({ page }, testInfo) => {
    await page.setViewportSize({ width, height: 1000 });
    await page.clock.install({ time: new Date('2026-11-11T12:00:00Z') });
    const fixture = await installCycleFixtures(page);
    fixture.rows[0].state = 'scheduled';
    await page.goto(`/principals?selectedId=${PRINCIPAL_ID}`);
    await page.getByRole('button', { name: 'Sessions', exact: true }).click();
    await page.locator('[data-session-id]').first().click();
    await expect(page.getByText(/Next renewal at/)).toBeVisible();
    const resume = Promise.withResolvers<void>();
    await page.route('**/cache-keepalive/detail**', async (route) => {
      await resume.promise;
      await route.fallback();
    });
    fixture.rows[0].state = 'cancelled';
    fixture.rows[0].is_active = false;
    fixture.rows[0].cancel_reason = 'new_request';
    fixture.rows[0].reason = 'A new request arrived';
    await page.clock.fastForward(5_000);
    await expect(page.getByTestId('session-detail-loading')).toBeVisible();
    await expect(page.getByText(/Next renewal at/)).not.toBeVisible();
    await page.screenshot({ path: testInfo.outputPath(`lifecycle-loading-${width}.png`), animations: 'disabled' });
    resume.resolve();
    await expect(page.getByText('A new request arrived', { exact: true })).toBeVisible();
    await expect(page.getByText(/Next renewal at/)).not.toBeVisible();
    await page.screenshot({ path: testInfo.outputPath(`lifecycle-cancelled-${width}.png`), animations: 'disabled' });
  });
}
