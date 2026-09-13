import { expect, type Page, test } from '@playwright/test';
import type { QuotaSnapshot } from '../src/lib/api';

type UpstreamFixture = {
  id: string;
  name: string;
  kind: 'anthropic_api_key' | 'anthropic_oauth';
  enabled: boolean;
  spec_revision: number;
  base_url: string;
  api_key_env?: string | null;
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

    if (pathname === '/admin/v1/upstreams' && method === 'GET') {
      return json(200, { upstreams });
    }
    if (pathname === '/admin/v1/plugins/registry' && method === 'GET') {
      return json(200, { entries: [] });
    }
    const upstreamMatch = pathname.match(/^\/admin\/v1\/upstreams\/([^/]+)$/);
    if (upstreamMatch && method === 'GET') {
      const upstream = upstreams.find((candidate) => candidate.id === upstreamMatch[1]);
      return upstream ? json(200, upstream, { etag: `"${upstream.spec_revision}"` }) : json(404, { error: 'not_found' });
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
    if (pathname === '/admin/usage') {
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
    if (pathname === '/admin/events/recent') {
      return json(200, { events: [], observed: true, count: 0, limit: 5 });
    }
    if (pathname === '/admin/v1/subscription-quotas/latest') {
      return json(200, {
        now_unix_secs: Math.floor(Date.now() / 1000),
        max_staleness_secs: 300,
        upstreams: [
          {
            upstream_id: 'oauth-healthy',
            windows: options.latestWindows ?? []
          }
        ]
      });
    }
    if (pathname === '/admin/v1/subscription-quotas/series') {
      return json(200, { since_unix_secs: 0, until_unix_secs: 0, bucket_secs: 60, source: 'merged', series: [] });
    }
    if (pathname === '/admin/v1/subscription-quotas/analysis') {
      return json(200, { since_unix_secs: 0, until_unix_secs: 0, now_unix_secs: 0, max_staleness_secs: 300, upstreams: [] });
    }
    return json(200, {});
  });
}

test.describe('Upstreams Sidebar Quota', () => {
  test('Scenario: Fable meter transitions and responsive checks', async ({ page }, testInfo) => {
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

    // 1. Fresh Fable (within 7 days)
    let latestWindows: QuotaSnapshot[] = [
      { ...baseSnap, window: '5h', utilization: 0.1, state: 'fresh', source: 'api', observed_at_unix_millis: now * 1000 },
      { ...baseSnap, window: '7d', utilization: 0.2, state: 'fresh', source: 'api', observed_at_unix_millis: now * 1000 },
      { ...baseSnap, window: '7d_fable', utilization: 0.3, state: 'fresh', source: 'api', observed_at_unix_millis: now * 1000 },
      { ...baseSnap, window: 'overage', utilization: null, state: 'fresh', source: 'api', observed_at_unix_millis: now * 1000, extra_usage_monthly_limit: 100, extra_usage_used_credits: 50 },
    ];

    await installAppFixtures(page, { latestWindows });
    await page.setViewportSize({ width: 1280, height: 800 });
    await page.goto('/upstreams?selectedId=oauth-healthy');

    // Wait for sidebar to render
    await expect(page.getByTestId('warmup-card').or(page.getByTestId('api-usage-card'))).toBeVisible();
    const sidebar = page.locator('aside').nth(1);
    await expect(sidebar).toBeVisible();

    // Check order: 5h, 7d, Fable, Extra
    const meters = sidebar.locator('.flex.items-center.gap-2.w-full.text-\\[10px\\].font-mono');
    await expect(meters).toHaveCount(4);
    await expect(meters.nth(0)).toContainText('5h');
    await expect(meters.nth(1)).toContainText('7d');
    await expect(meters.nth(2)).toContainText('Fable');
    await expect(meters.nth(3)).toContainText('Extra');

    // Check Fable percentage
    await expect(meters.nth(2)).toContainText('30%');

    // 2. Change to unobserved Fable
    latestWindows = [
      { ...baseSnap, window: '5h', utilization: 0.1, state: 'fresh', source: 'api', observed_at_unix_millis: now * 1000 },
      { ...baseSnap, window: '7d', utilization: 0.2, state: 'fresh', source: 'api', observed_at_unix_millis: now * 1000 },
      { ...baseSnap, window: '7d_fable', utilization: 0.3, state: 'unobserved', source: 'api', observed_at_unix_millis: now * 1000 },
      { ...baseSnap, window: 'overage', utilization: null, state: 'fresh', source: 'api', observed_at_unix_millis: now * 1000, extra_usage_monthly_limit: 100, extra_usage_used_credits: 50 },
    ];
    await installAppFixtures(page, { latestWindows });
    await page.reload();
    await expect(sidebar).toBeVisible();
    await expect(meters).toHaveCount(3);
    await expect(meters.nth(0)).toContainText('5h');
    await expect(meters.nth(1)).toContainText('7d');
    await expect(meters.nth(2)).toContainText('Extra');
    await page.screenshot({ path: testInfo.outputPath('fable-sidebar-missing-desktop.png'), fullPage: true });
    // 3. Hide a shared window that the successful usage response omitted
    latestWindows = [
      { ...baseSnap, window: '5h', utilization: 0.1, state: 'fresh', source: 'api', observed_at_unix_millis: now * 1000 },
      { ...baseSnap, window: '7d', utilization: null, state: 'absent', source: 'api', observed_at_unix_millis: now * 1000 },
      { ...baseSnap, window: '7d_fable', utilization: 0.3, state: 'fresh', source: 'api', observed_at_unix_millis: now * 1000 },
      { ...baseSnap, window: 'overage', utilization: null, state: 'fresh', source: 'api', observed_at_unix_millis: now * 1000, extra_usage_monthly_limit: 100, extra_usage_used_credits: 50 },
    ];
    await installAppFixtures(page, { latestWindows });
    await page.reload();
    await expect(sidebar).toBeVisible();
    await expect(meters).toHaveCount(3);
    await expect(meters.nth(0)).toContainText('5h');
    await expect(meters.nth(1)).toContainText('Fable');
    await expect(meters.nth(2)).toContainText('Extra');


    // 4. Change to stale Fable (> 7 days)
    latestWindows = [
      { ...baseSnap, window: '5h', utilization: 0.1, state: 'fresh', source: 'api', observed_at_unix_millis: now * 1000 },
      { ...baseSnap, window: '7d', utilization: 0.2, state: 'fresh', source: 'api', observed_at_unix_millis: now * 1000 },
      { ...baseSnap, window: '7d_fable', utilization: 0.3, state: 'stale', source: 'api', observed_at_unix_millis: (now - 604801) * 1000 },
      { ...baseSnap, window: 'overage', utilization: null, state: 'fresh', source: 'api', observed_at_unix_millis: now * 1000, extra_usage_monthly_limit: 100, extra_usage_used_credits: 50 },
    ];
    await installAppFixtures(page, { latestWindows });
    await page.reload();
    await expect(sidebar).toBeVisible();
    await expect(meters).toHaveCount(3);
    await expect(meters.nth(0)).toContainText('5h');
    await expect(meters.nth(1)).toContainText('7d');
    await expect(meters.nth(2)).toContainText('Extra');
    await page.screenshot({ path: testInfo.outputPath('fable-sidebar-stale-desktop.png'), fullPage: true });

    // 5. Responsive checks with fresh Fable
    latestWindows = [
      { ...baseSnap, window: '5h', utilization: 0.1, state: 'fresh', source: 'api', observed_at_unix_millis: now * 1000 },
      { ...baseSnap, window: '7d', utilization: 0.2, state: 'fresh', source: 'api', observed_at_unix_millis: now * 1000 },
      { ...baseSnap, window: '7d_fable', utilization: 0.3, state: 'fresh', source: 'api', observed_at_unix_millis: now * 1000 },
      { ...baseSnap, window: 'overage', utilization: null, state: 'fresh', source: 'api', observed_at_unix_millis: now * 1000, extra_usage_monthly_limit: 100, extra_usage_used_credits: 50 },
    ];
    await installAppFixtures(page, { latestWindows });

    const viewports = [
      { width: 1280, height: 800, name: 'desktop' },
      { width: 768, height: 1024, name: 'tablet' },
      { width: 375, height: 667, name: 'mobile' },
    ];

    for (const vp of viewports) {
      await page.setViewportSize({ width: vp.width, height: vp.height });
      await page.goto('/upstreams');

      await expect(meters.nth(2)).toBeVisible();

      // Assert no horizontal overflow
      const overflow = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(overflow).toBe(false);

      // Assert Fable text is not clipped
      const fableLabel = meters.nth(2).locator('.w-8.shrink-0.text-text-faint.truncate');
      const isClipped = await fableLabel.evaluate((el) => {
        return el.scrollWidth > el.clientWidth;
      });
      expect(isClipped).toBe(false);

      await page.screenshot({ path: testInfo.outputPath(`fable-sidebar-${vp.name}.png`), fullPage: true });
    }
  });
});
