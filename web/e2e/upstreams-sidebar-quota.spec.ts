import { expect, type Page, test } from '@playwright/test';
import type { AuthSession, QuotaSnapshot } from '../src/lib/api';

type UpstreamFixture = {
  id: string;
  name: string;
  kind: 'anthropic_api_key' | 'anthropic_oauth';
  enabled: boolean;
  spec_revision: number;
  base_url: string;
  warmup_enabled: boolean;
  warmup_dialect_plugin: null;
  status: {
    last_apply_error: string | null;
    last_apply_at_unix_secs: number | null;
    last_warmup_at_unix_secs: number | null;
  };
};

function oauthHealthy(): UpstreamFixture {
  return {
    id: 'oauth-healthy',
    name: 'oauth-healthy',
    kind: 'anthropic_oauth',
    enabled: true,
    spec_revision: 4,
    base_url: 'https://api.anthropic.com',
    warmup_enabled: true,
    warmup_dialect_plugin: null,
    status: {
      last_apply_error: null,
      last_apply_at_unix_secs: null,
      last_warmup_at_unix_secs: Math.floor(Date.now() / 1000) - 20 * 60,
    },
  };
}

async function installAppFixtures(
  page: Page,
  options: {
    latestWindows?: QuotaSnapshot[];
  } = {},
) {
  const upstreams = [oauthHealthy()];
  let latestWindows = options.latestWindows ?? [];

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
      return json(200, { entries: [] });
    }
    const upstreamMatch = pathname.match(/^\/admin\/v1\/upstreams\/([^/]+)$/);
    if (upstreamMatch && method === 'GET') {
      const upstream = upstreams.find((candidate) => candidate.id === upstreamMatch[1]);
      return upstream
        ? json(200, upstream, { etag: `"${upstream.spec_revision}"` })
        : json(404, { error: 'not_found' });
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
      return json(200, {
        upstream_id: pathname.split('/')[4],
        subscription_metadata: null,
        organization_metadata: null,
      });
    }
    if (pathname === '/admin/v1/status') {
      return json(200, {
        version: 'mock',
        git_sha: 'mock',
        uptime_secs: 1,
        build: { rust_version: 'mock', profile: 'debug', target: 'mock' },
        generation: 1,
        upstreams: upstreams.map((upstream) => ({
          id: upstream.id,
          name: upstream.name,
          status: upstream.enabled ? 'active' : 'disabled',
          last_apply_at_unix_secs: null,
          last_apply_error: null,
        })),
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
        series: [],
      });
    }
    if (pathname === '/admin/v1/events/recent') {
      return json(200, { events: [], observed: true, count: 0, limit: 5 });
    }
    if (pathname === '/admin/v1/subscription-quotas/latest') {
      return json(200, {
        now_unix_secs: Math.floor(Date.now() / 1000),
        max_staleness_secs: 300,
        upstreams: [{ upstream_id: 'oauth-healthy', windows: latestWindows }],
      });
    }
    if (pathname === '/admin/v1/subscription-quotas/series') {
      return json(200, {
        since_unix_secs: 0,
        until_unix_secs: 0,
        bucket_secs: 60,
        source: 'merged',
        series: [],
      });
    }
    return json(200, {});
  });

  return {
    setLatestWindows(nextWindows: QuotaSnapshot[]) {
      latestWindows = nextWindows;
    },
  };
}

async function openUpstreams(
  page: Page,
  selectedId?: string,
): Promise<void> {
  const query = selectedId
    ? `?selectedId=${encodeURIComponent(selectedId)}`
    : '';
  await page.goto(`/upstreams${query}`, { waitUntil: 'commit' });
  if (selectedId) {
    await expect(page.getByTestId('quota-snapshot-grid')).toBeVisible();
  } else {
    await expect(
      page.getByRole('list', { name: 'Upstreams', exact: true }),
    ).toBeVisible();
  }
}

async function reloadSelectedUpstream(page: Page): Promise<void> {
  await page.reload({ waitUntil: 'commit' });
  await expect(page.getByTestId('quota-snapshot-grid')).toBeVisible();
}


test.describe('Upstreams Sidebar Quota', () => {
  test('Scenario: Fable meter transitions and responsive checks', async ({
    page,
  }, testInfo) => {
    const now = Math.floor(Date.now() / 1000);

    const baseSnap = {
      status: null,
      resets_at_unix_secs: null,
      surpassed_threshold: null,
      representative_claim: null,
      disabled_reason: null,
      extra_usage_enabled: null,
      extra_usage_monthly_limit: null,
      extra_usage_used_credits: null,
      age_secs: null,
    };

    // 1. Fresh Fable (within 7 days).
    let latestWindows: QuotaSnapshot[] = [
      { ...baseSnap, window: '5h', utilization: 0.1, state: 'fresh', source: 'api', observed_at_unix_millis: now * 1000 },
      { ...baseSnap, window: '7d', utilization: 0.2, state: 'fresh', source: 'api', observed_at_unix_millis: now * 1000 },
      { ...baseSnap, window: '7d_fable', utilization: 0.3, state: 'fresh', source: 'api', observed_at_unix_millis: now * 1000 },
      { ...baseSnap, window: 'overage', utilization: null, state: 'fresh', source: 'api', observed_at_unix_millis: now * 1000, extra_usage_enabled: true, extra_usage_monthly_limit: 100, extra_usage_used_credits: 50 },
    ];

    const fixtures = await installAppFixtures(page, { latestWindows });
    await page.setViewportSize({ width: 1280, height: 800 });
    await openUpstreams(page, 'oauth-healthy');

    const quota = page.getByTestId('quota-snapshot-grid');
    await expect(quota).toBeVisible();
    await expect(quota.getByRole('listitem')).toHaveCount(4);
    await expect(quota.getByRole('meter')).toHaveCount(4);
    await expect(quota.getByRole('meter').nth(0)).toHaveAccessibleName('5h');
    await expect(quota.getByRole('meter').nth(1)).toHaveAccessibleName('7d');
    await expect(quota.getByRole('meter').nth(2)).toHaveAccessibleName('7d (Fable)');
    await expect(quota.getByRole('meter').nth(3)).toHaveAccessibleName('Extra usage');
    await expect(quota.getByRole('meter', { name: '5h', exact: true })).toHaveAttribute('aria-valuenow', '10');
    await expect(quota.getByRole('meter', { name: '7d', exact: true })).toHaveAttribute('aria-valuenow', '20');
    await expect(quota.getByRole('meter', { name: '7d (Fable)', exact: true })).toHaveAttribute('aria-valuenow', '30');
    await expect(quota.getByRole('meter', { name: 'Extra usage', exact: true })).toHaveAttribute('aria-valuenow', '50');

    // 2. Unobserved Fable: the optional model window is omitted.
    latestWindows = [
      { ...baseSnap, window: '5h', utilization: 0.1, state: 'fresh', source: 'api', observed_at_unix_millis: now * 1000 },
      { ...baseSnap, window: '7d', utilization: 0.2, state: 'fresh', source: 'api', observed_at_unix_millis: now * 1000 },
      { ...baseSnap, window: '7d_fable', utilization: 0.3, state: 'unobserved', source: 'api', observed_at_unix_millis: now * 1000 },
      { ...baseSnap, window: 'overage', utilization: null, state: 'fresh', source: 'api', observed_at_unix_millis: now * 1000, extra_usage_enabled: true, extra_usage_monthly_limit: 100, extra_usage_used_credits: 50 },
    ];
    fixtures.setLatestWindows(latestWindows);
    await reloadSelectedUpstream(page);
    await expect(quota.getByRole('listitem')).toHaveCount(3);
    await expect(quota.getByRole('meter', { name: '5h', exact: true })).toHaveCount(1);
    await expect(quota.getByRole('meter', { name: '7d', exact: true })).toHaveCount(1);
    await expect(quota.getByRole('meter', { name: '7d (Fable)', exact: true })).toHaveCount(0);
    await expect(quota.getByRole('meter', { name: 'Extra usage', exact: true })).toHaveCount(1);
    await page.screenshot({ path: testInfo.outputPath('fable-sidebar-missing-desktop.png'), fullPage: true });

    // 3. Absent Fable: proven absent windows are omitted, while shared 7d remains.
    latestWindows = [
      { ...baseSnap, window: '5h', utilization: 0.1, state: 'fresh', source: 'api', observed_at_unix_millis: now * 1000 },
      { ...baseSnap, window: '7d', utilization: 0.2, state: 'fresh', source: 'api', observed_at_unix_millis: now * 1000 },
      { ...baseSnap, window: '7d_fable', utilization: null, state: 'absent', source: 'api', observed_at_unix_millis: now * 1000 },
      { ...baseSnap, window: 'overage', utilization: null, state: 'fresh', source: 'api', observed_at_unix_millis: now * 1000, extra_usage_enabled: true, extra_usage_monthly_limit: 100, extra_usage_used_credits: 50 },
    ];
    fixtures.setLatestWindows(latestWindows);
    await reloadSelectedUpstream(page);
    await expect(quota.getByRole('listitem')).toHaveCount(3);
    await expect(quota.getByRole('meter', { name: '5h', exact: true })).toHaveCount(1);
    await expect(quota.getByRole('meter', { name: '7d', exact: true })).toHaveCount(1);
    await expect(quota.getByRole('meter', { name: '7d (Fable)', exact: true })).toHaveCount(0);
    await expect(quota.getByRole('meter', { name: 'Extra usage', exact: true })).toHaveCount(1);

    // 4. Stale Fable (> 7 days): quota cards retain the stale reading.
    latestWindows = [
      { ...baseSnap, window: '5h', utilization: 0.1, state: 'fresh', source: 'api', observed_at_unix_millis: now * 1000 },
      { ...baseSnap, window: '7d', utilization: 0.2, state: 'fresh', source: 'api', observed_at_unix_millis: now * 1000 },
      { ...baseSnap, window: '7d_fable', utilization: 0.3, state: 'stale', source: 'api', observed_at_unix_millis: (now - 604801) * 1000 },
      { ...baseSnap, window: 'overage', utilization: null, state: 'fresh', source: 'api', observed_at_unix_millis: now * 1000, extra_usage_enabled: true, extra_usage_monthly_limit: 100, extra_usage_used_credits: 50 },
    ];
    fixtures.setLatestWindows(latestWindows);
    await reloadSelectedUpstream(page);
    await expect(quota.getByRole('listitem')).toHaveCount(4);
    await expect(quota.getByRole('meter')).toHaveCount(4);
    await expect(quota.getByRole('meter', { name: '5h', exact: true })).toHaveCount(1);
    await expect(quota.getByRole('meter', { name: '7d', exact: true })).toHaveCount(1);
    await expect(quota.getByRole('meter', { name: '7d (Fable)', exact: true })).toHaveAttribute('aria-valuenow', '30');
    await expect(quota.getByRole('meter', { name: 'Extra usage', exact: true })).toHaveCount(1);
    await page.screenshot({ path: testInfo.outputPath('fable-sidebar-stale-desktop.png'), fullPage: true });

    // 5. Responsive checks with fresh Fable.
    latestWindows = [
      { ...baseSnap, window: '5h', utilization: 0.1, state: 'fresh', source: 'api', observed_at_unix_millis: now * 1000 },
      { ...baseSnap, window: '7d', utilization: 0.2, state: 'fresh', source: 'api', observed_at_unix_millis: now * 1000 },
      { ...baseSnap, window: '7d_fable', utilization: 0.3, state: 'fresh', source: 'api', observed_at_unix_millis: now * 1000 },
      { ...baseSnap, window: 'overage', utilization: null, state: 'fresh', source: 'api', observed_at_unix_millis: now * 1000, extra_usage_enabled: true, extra_usage_monthly_limit: 100, extra_usage_used_credits: 50 },
    ];
    fixtures.setLatestWindows(latestWindows);

    for (const vp of [
      { width: 1280, height: 800, name: 'desktop' },
      { width: 768, height: 1024, name: 'tablet' },
      { width: 375, height: 667, name: 'mobile' },
    ]) {
      await page.setViewportSize({ width: vp.width, height: vp.height });
      await openUpstreams(page, 'oauth-healthy');

      await expect(quota.getByRole('meter', { name: '7d (Fable)', exact: true })).toBeVisible();
      expect(
        await page.evaluate(
          () => document.documentElement.scrollWidth > document.documentElement.clientWidth,
        ),
      ).toBe(false);
      await expect(quota.getByText('7d (Fable)', { exact: true })).toBeVisible();
      await page.screenshot({ path: testInfo.outputPath(`fable-sidebar-${vp.name}.png`), fullPage: true });
    }
  });

  test('Scenario: Explicit weekly-only quota hides absent 5h on desktop and mobile', async ({
    page,
  }) => {
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
    const latestWindows: QuotaSnapshot[] = [
      { ...baseSnap, window: '5h', state: 'absent', utilization: null, resets_at_unix_secs: null },
      { ...baseSnap, window: '7d', state: 'fresh', utilization: 0.2 },
      { ...baseSnap, window: '7d_fable', state: 'fresh', utilization: 0.3 },
      { ...baseSnap, window: 'overage', state: 'fresh', utilization: null, extra_usage_enabled: true, extra_usage_monthly_limit: 100, extra_usage_used_credits: 50 },
    ];
    await installAppFixtures(page, { latestWindows });

    for (const viewport of [
      { width: 1280, height: 800 },
      { width: 375, height: 667 },
    ]) {
      await page.setViewportSize(viewport);
      await openUpstreams(page);
      const upstreamList = page.getByRole('list', { name: 'Upstreams', exact: true });
      await expect(upstreamList).toBeVisible();
      await expect(upstreamList.getByText('7d', { exact: true })).toBeVisible();
      await expect(upstreamList.getByText('Fable', { exact: true })).toBeVisible();
      await expect(upstreamList.getByText('20%', { exact: true })).toBeVisible();
      await expect(upstreamList.getByText(/\b5h\b/)).toHaveCount(0);
      await expect(upstreamList.getByText('No reading', { exact: true })).toHaveCount(0);
      await expect(upstreamList.getByText(/^0%(?: used)?$/)).toHaveCount(0);

      await Promise.all([
        page.waitForURL(/selectedId=oauth-healthy/),
        upstreamList.getByRole('button', { name: /oauth-healthy/ }).click(),
      ]);
      await reloadSelectedUpstream(page);
      await expect(page).toHaveURL(/selectedId=oauth-healthy/);

      const quota = page.getByTestId('quota-snapshot-grid');
      await expect(quota).toBeVisible();
      await expect(quota.getByRole('listitem')).toHaveCount(3);
      await expect(quota.getByRole('meter')).toHaveCount(3);
      await expect(quota.getByRole('meter', { name: '5h', exact: true })).toHaveCount(0);
      await expect(quota.getByText(/\b5h\b/)).toHaveCount(0);
      await expect(quota.getByText('No reading', { exact: true })).toHaveCount(0);
      await expect(quota.getByText(/^0%(?: used)?$/)).toHaveCount(0);
      await expect(quota.getByRole('meter', { name: '7d', exact: true })).toHaveAttribute('aria-valuenow', '20');
      await expect(quota.getByRole('meter', { name: '7d (Fable)', exact: true })).toHaveAttribute('aria-valuenow', '30');
      await expect(quota.getByRole('meter', { name: 'Extra usage', exact: true })).toHaveAttribute('aria-valuenow', '50');
      await expect(quota.getByText('7d', { exact: true })).toBeVisible();
      await expect(quota.getByText('7d (Fable)', { exact: true })).toBeVisible();
      await expect(quota.getByText('Extra usage', { exact: true })).toBeVisible();
      expect(
        await page.evaluate(
          () => document.documentElement.scrollWidth > document.documentElement.clientWidth,
        ),
      ).toBe(false);
    }
  });
});

for (const locale of ['ko-KR', 'en-US'] as const) {
  test.describe(`Upstream reset phrase in ${locale}`, () => {
    test.use({ locale });

    test('Scenario: reset countdown reads as one phrase and stays aligned across rows', async ({ page }) => {
      const now = Math.floor(Date.now() / 1000);
      const snap = {
        status: null,
        surpassed_threshold: null,
        representative_claim: null,
        disabled_reason: null,
        extra_usage_enabled: null,
        extra_usage_monthly_limit: null,
        extra_usage_used_credits: null,
        age_secs: 0,
        source: 'api' as const,
        state: 'fresh' as const,
        observed_at_unix_millis: now * 1000,
      };
      await installAppFixtures(page, {
        latestWindows: [
          { ...snap, window: '5h', utilization: 0.4, resets_at_unix_secs: now + 2 * 3600 + 600 },
          { ...snap, window: '7d', utilization: 0.2, resets_at_unix_secs: now + 5 * 24 * 3600 + 600 },
        ],
      });

      for (const width of [1280, 375]) {
        await page.setViewportSize({ width, height: 800 });
        await openUpstreams(page, 'oauth-healthy');
        const quota = page.getByTestId('quota-snapshot-grid');
        await expect(quota.getByRole('listitem')).toHaveCount(2);
        await expect(quota.locator('[data-slot="window-reset-value"]')).toHaveText(
          locale === 'ko-KR' ? ['2시간', '5일'] : ['2h', '5d'],
        );

        // Text boxes, not cell boxes: the gap a reader sees between the words.
        const rows = await quota.getByRole('listitem').evaluateAll((items) =>
          items.map((item) =>
            ['window-reset-prefix', 'window-reset-value', 'window-reset-suffix'].map((slot) => {
              const range = document.createRange();
              range.selectNodeContents(item.querySelector(`[data-slot="${slot}"]`) as Node);
              const box = range.getBoundingClientRect();
              return box.width > 0 ? { left: Math.round(box.left), right: Math.round(box.right) } : null;
            }),
          ),
        );
        for (const [prefix, value, suffix] of rows) {
          // Korean: "2시간 후 초기화"; English: "Resets in 2h". One word space apart.
          const [first, second] = locale === 'ko-KR' ? [value, suffix] : [prefix, value];
          expect(first).not.toBeNull();
          expect(second).not.toBeNull();
          expect(second!.left - first!.right).toBeLessThanOrEqual(8);
        }
        // The shared columns still align the fixed words across rows.
        const fixedWord = rows.map(([prefix, , suffix]) => (locale === 'ko-KR' ? suffix : prefix)!.left);
        expect(fixedWord).toEqual(fixedWord.map(() => fixedWord[0]));
      }
    });
  });
}
