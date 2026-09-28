import { expect, type Page, test } from '@playwright/test';

const UPSTREAM_ID = 'oauth-cadence';
const INITIAL_TIME = new Date('2026-06-18T00:00:01.000Z');
const RANGE_SECS = 7 * 24 * 60 * 60;

type RequestBounds = {
  sinceUnixSecs: number;
  untilUnixSecs: number;
};

function requestBounds(url: URL): RequestBounds {
  return {
    sinceUnixSecs: Number(url.searchParams.get('since_unix_secs')),
    untilUnixSecs: Number(url.searchParams.get('until_unix_secs')),
  };
}

type AppFixtureOptions = {
  delaySeries?: boolean;
};

function createResponseGate(initiallyBlocked = false) {
  let blocked = initiallyBlocked;
  const waiters: Array<() => void> = [];

  return {
    block: () => {
      blocked = true;
    },
    release: () => {
      blocked = false;
      for (const resolve of waiters.splice(0)) resolve();
    },
    wait: () =>
      blocked
        ? new Promise<void>((resolve) => {
            waiters.push(resolve);
          })
        : Promise.resolve(),
  };
}

async function installAppFixtures(
  page: Page,
  options: AppFixtureOptions = {},
) {
  const seriesGate = createResponseGate(options.delaySeries);
  let seriesResponse: 'data' | 'empty' = 'data';
  const seriesRequests: RequestBounds[] = [];

  await page.clock.install({ time: INITIAL_TIME });
  await page.addInitScript(() => {
    window.localStorage.setItem('cc-lb-admin-token', 'mock-token');
  });

  const upstream = {
    id: UPSTREAM_ID,
    name: 'Cadence OAuth',
    kind: 'anthropic_oauth',
    enabled: true,
    spec_revision: 1,
    base_url: 'https://api.anthropic.com',
    api_key_env: null,
    warmup_enabled: false,
    warmup_dialect_plugin: null,
    status: {
      last_apply_error: null,
      last_apply_at_unix_secs: null,
      last_warmup_at_unix_secs: null,
    },
  };

  await page.route('**/admin/**', async (route) => {
    const request = route.request();
    const url = new URL(request.url());
    const { pathname } = url;
    const method = request.method();
    const json = (status: number, body: unknown) =>
      route.fulfill({
        status,
        contentType: 'application/json',
        body: JSON.stringify(body),
      });

    // AuthRequiredGate blocks every route until this resolves.
    if (pathname === '/admin/v1/auth/session' && method === 'GET') {
      return json(200, {
        authority: 'e2e',
        subject: 'e2e-operator',
        kind: 'human',
        provider_id: 'static',
        email: null,
        display_name: 'E2E Operator',
        expires_at_unix_secs: null,
        auth_mode: 'static_token',
      });
    }
    if (pathname === '/admin/v1/upstreams' && method === 'GET') {
      return json(200, { upstreams: [upstream] });
    }
    if (pathname === '/admin/v1/principals' && method === 'GET') {
      return json(200, { principals: [] });
    }
    if (pathname === '/admin/v1/plugins/registry' && method === 'GET') {
      return json(200, { entries: [] });
    }
    if (pathname === `/admin/v1/upstreams/${UPSTREAM_ID}` && method === 'GET') {
      return route.fulfill({
        status: 200,
        contentType: 'application/json',
        headers: { etag: '"1"' },
        body: JSON.stringify(upstream),
      });
    }
    if (pathname === `/admin/v1/upstreams/${UPSTREAM_ID}/oauth/status`) {
      return json(200, {
        upstream_id: UPSTREAM_ID,
        kind: 'anthropic_oauth',
        has_credentials: true,
        status: 'active',
        expires_at_unix_secs: Math.floor(INITIAL_TIME.getTime() / 1000) + 3600,
        refresh_token_present: true,
        scopes: ['user:inference'],
      });
    }
    if (
      pathname ===
      `/admin/v1/upstreams/${UPSTREAM_ID}/subscription-metadata`
    ) {
      return json(200, {
        upstream_id: UPSTREAM_ID,
        subscription_metadata: null,
        organization_metadata: null,
      });
    }
    if (
      pathname === `/admin/v1/upstreams/${UPSTREAM_ID}/warmup` &&
      method === 'GET'
    ) {
      return json(200, {
        upstream_id: UPSTREAM_ID,
        last_attempt: null,
        recent_attempts: [],
        next_scheduled_at_unix_secs: null,
        recent_summary_7d: {
          success: 0,
          skipped: 0,
          transient_failure: 0,
          permanent_failure: 0,
        },
        dialect_plugin: null,
      });
    }
    if (pathname === '/admin/v1/status') {
      return json(200, {
        version: 'mock',
        git_sha: 'mock',
        uptime_secs: 1,
        build: { rust_version: 'mock', profile: 'debug', target: 'mock' },
        generation: 1,
        upstreams: [
          {
            id: UPSTREAM_ID,
            name: upstream.name,
            status: 'active',
            last_apply_at_unix_secs: null,
            last_apply_error: null,
          },
        ],
      });
    }
    if (pathname === '/admin/usage') {
      const untilUnixSecs = Math.floor(INITIAL_TIME.getTime() / 1000);
      return json(200, {
        range: url.searchParams.get('range') ?? '24h',
        step: url.searchParams.get('step') ?? 'hour',
        group_by: url.searchParams.get('group_by') ?? 'model',
        window_start_unix_secs: untilUnixSecs - 3600,
        window_end_unix_secs: untilUnixSecs,
        observed: true,
        series: [],
      });
    }
    if (pathname === '/admin/events/recent') {
      return json(200, { events: [], observed: true, count: 0, limit: 5 });
    }
    if (pathname === '/admin/v1/subscription-quotas/latest') {
      const nowUnixSecs = Math.floor(INITIAL_TIME.getTime() / 1000);
      return json(200, {
        now_unix_secs: nowUnixSecs,
        max_staleness_secs: 300,
        upstreams: [
          {
            upstream_id: UPSTREAM_ID,
            upstream_name: upstream.name,
            windows: [
              {
                window: '5h',
                state: 'fresh',
                source: 'api',
                utilization: 0.25,
                status: null,
                resets_at_unix_secs: nowUnixSecs + 3600,
                surpassed_threshold: false,
                representative_claim: null,
                disabled_reason: null,
                extra_usage_enabled: null,
                extra_usage_monthly_limit: null,
                extra_usage_used_credits: null,
                observed_at_unix_millis: nowUnixSecs * 1000,
                age_secs: 0,
              },
            ],
          },
        ],
      });
    }
    if (pathname === '/admin/v1/subscription-quotas/series') {
      const bounds = requestBounds(url);
      const responseMode = seriesResponse;
      seriesRequests.push(bounds);
      await seriesGate.wait();
      return json(200, {
        since_unix_secs: bounds.sinceUnixSecs,
        until_unix_secs: bounds.untilUnixSecs,
        bucket_secs: Number(url.searchParams.get('bucket_secs')),
        source: 'merged',
        series:
          responseMode === 'empty'
            ? []
            : [
                {
                  upstream_id: UPSTREAM_ID,
                  upstream_name: upstream.name,
                  window: '5h',
                  buckets: [
                    {
                      bucket_start_unix_secs: bounds.sinceUnixSecs,
                      utilization_last: 0.2,
                    },
                    {
                      bucket_start_unix_secs: bounds.untilUnixSecs,
                      utilization_last: 0.25,
                    },
                  ],
                  markers: [],
                },
              ],
      });
    }
    return json(200, {});
  });

  return {
    blockSeriesResponses: seriesGate.block,
    releaseSeriesResponses: seriesGate.release,
    seriesRequests,
    showEmptySeries: () => {
      seriesResponse = 'empty';
    },
    showSeriesData: () => {
      seriesResponse = 'data';
    },
  };
}

function quotaHistorySection(page: Page) {
  return page.getByRole('region', { name: 'Quota history', exact: true });
}

function quotaHistoryRange(page: Page, label: string) {
  return page
    .getByTestId('quota-history-range-control')
    .getByRole('radio', { name: label, exact: true });
}

// These route-mocked tests are browser-layer evidence for request and DOM
// behavior; backend storage and server transitions are covered separately.
test.describe('Upstream quota history series browser behavior (mock API)', () => {
  const rangeCases = [
    { label: '1h', durationSecs: 60 * 60 },
    { label: '6h', durationSecs: 6 * 60 * 60 },
    { label: '24h', durationSecs: 24 * 60 * 60 },
    { label: '7d', durationSecs: 7 * 24 * 60 * 60 },
  ] as const;

  for (const { label, durationSecs } of rangeCases) {
    test(`requests exact ${label} series bounds in the mock API browser layer`, async ({
      page,
    }) => {
      const fixtures = await installAppFixtures(page);
      const pageErrors: string[] = [];
      page.on('pageerror', (error) => pageErrors.push(error.message));
      await page.setViewportSize({ width: 1280, height: 800 });
      await page.goto(`/upstreams?selectedId=${UPSTREAM_ID}`);

      const quotaSection = quotaHistorySection(page);
      const rangeControl = page.getByTestId('quota-history-range-control');
      const chart = quotaSection.locator('.recharts-responsive-container');
      await expect(rangeControl).toBeVisible();

      let seriesRequestsBefore: RequestBounds[] = [];
      if (label !== '7d') {
        await expect(chart).toBeVisible();
        seriesRequestsBefore = [...fixtures.seriesRequests];
        await quotaHistoryRange(page, label).click();
      }

      await expect
        .poll(() =>
          fixtures.seriesRequests.find(
            (bounds) =>
              !seriesRequestsBefore.includes(bounds) &&
              bounds.untilUnixSecs - bounds.sinceUnixSecs === durationSecs,
          ),
        )
        .toBeDefined();
      await expect(chart).toBeVisible();
      await expect(quotaSection.getByText('No data in range')).toHaveCount(0);
      expect(pageErrors).toEqual([]);
    });
  }

  test('shows loading, keeps previous data while refetching, then empty and recovery in the mock API browser layer', async ({
    page,
  }) => {
    const fixtures = await installAppFixtures(page, { delaySeries: true });
    const pageErrors: string[] = [];
    page.on('pageerror', (error) => pageErrors.push(error.message));
    await page.setViewportSize({ width: 1280, height: 800 });
    await page.goto(`/upstreams?selectedId=${UPSTREAM_ID}`);

    const quotaSection = quotaHistorySection(page);
    const rangeControl = page.getByTestId('quota-history-range-control');
    const chart = quotaSection.locator('.recharts-responsive-container');
    await expect.poll(() => fixtures.seriesRequests.length).toBeGreaterThan(0);
    await expect(rangeControl).toBeVisible();
    await expect(
      page
        .getByRole('region', { name: 'Quota', exact: true })
        .getByTestId('quota-snapshot-grid'),
    ).toBeVisible();
    await expect(chart).toHaveCount(0);
    await expect(quotaSection.getByText('No data in range')).toHaveCount(0);
    await expect(quotaSection.getByText(/error/i)).toHaveCount(0);

    fixtures.releaseSeriesResponses();
    await expect(chart).toBeVisible();

    const loadedSeriesBounds = fixtures.seriesRequests.at(-1);
    if (!loadedSeriesBounds) {
      throw new Error('initial loaded series request was not observed');
    }
    fixtures.showEmptySeries();
    fixtures.blockSeriesResponses();
    const seriesRequestsBeforeEmpty = [...fixtures.seriesRequests];
    await page.clock.runFor(30_000);
    await expect
      .poll(() =>
        fixtures.seriesRequests.find(
          (bounds) =>
            !seriesRequestsBeforeEmpty.includes(bounds) &&
            bounds.untilUnixSecs - bounds.sinceUnixSecs === RANGE_SECS &&
            bounds.untilUnixSecs > loadedSeriesBounds.untilUnixSecs,
        ),
      )
      .toBeDefined();
    await expect(chart).toBeVisible();
    await expect(quotaSection.getByText('No data in range')).toHaveCount(0);
    fixtures.releaseSeriesResponses();
    await expect(quotaSection.getByText('No data in range')).toBeVisible();
    await expect(chart).toHaveCount(0);

    fixtures.showSeriesData();
    const seriesCountBeforeRecovery = fixtures.seriesRequests.length;
    await quotaHistoryRange(page, '24h').click();
    await expect
      .poll(() => fixtures.seriesRequests.length)
      .toBeGreaterThan(seriesCountBeforeRecovery);
    await expect(chart).toBeVisible();
    await expect(quotaSection.getByText('No data in range')).toHaveCount(0);
    expect(pageErrors).toEqual([]);
  });

  test('refetches series with advancing bounds on the 30-second poll in the mock API browser layer', async ({
    page,
  }) => {
    const fixtures = await installAppFixtures(page);
    const pageErrors: string[] = [];
    page.on('pageerror', (error) => pageErrors.push(error.message));
    await page.setViewportSize({ width: 1280, height: 800 });
    await page.goto(`/upstreams?selectedId=${UPSTREAM_ID}`);

    await expect.poll(() => fixtures.seriesRequests.length).toBeGreaterThan(0);
    await expect(page.getByTestId('quota-history-range-control')).toBeVisible();
    const quotaSection = quotaHistorySection(page);
    await expect(quotaSection.locator('.animate-pulse')).toHaveCount(0);
    await expect(quotaSection.getByText(/error/i)).toHaveCount(0);
    await expect(quotaSection.locator('.recharts-responsive-container')).toBeVisible();
    await expect(quotaSection.getByText('No data in range')).toHaveCount(0);

    const initialBounds = fixtures.seriesRequests[0];
    if (!initialBounds) {
      throw new Error('initial series request was not observed');
    }
    expect(initialBounds.untilUnixSecs - initialBounds.sinceUnixSecs).toBe(
      RANGE_SECS,
    );

    await page.clock.runFor(60_000);

    await expect
      .poll(() =>
        fixtures.seriesRequests.some(
          (bounds) =>
            bounds.untilUnixSecs - bounds.sinceUnixSecs === RANGE_SECS &&
            bounds.untilUnixSecs >= initialBounds.untilUnixSecs + 60,
        ),
      )
      .toBe(true);
    await expect(quotaSection.locator('.recharts-responsive-container')).toBeVisible();
    await expect(quotaSection.getByText('No data in range')).toHaveCount(0);
    expect(pageErrors).toEqual([]);
  });
});
