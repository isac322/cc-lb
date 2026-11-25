import { expect, type Page, test } from '@playwright/test';
import type {
  DashboardSummaryResponse,
  DashboardUsageResponse,
  UsageBucket,
} from '../src/lib/api';

const POLL_INTERVAL_MS = 5_000;
const POLL_UPDATE_TIMEOUT_MS = 2 * POLL_INTERVAL_MS + 1_000;
const BUCKET_START = Date.UTC(2026, 7, 17, 12) / 1000;

function usageBucket(overrides: Partial<UsageBucket>): UsageBucket {
  return {
    bucket_start_unix_secs: BUCKET_START,
    request_count: 0,
    input_tokens: 0,
    output_tokens: 0,
    cache_creation_input_tokens: 0,
    cache_read_input_tokens: 0,
    error_count: 0,
    virtual_cost_micros: 0,
    latency_ms_sum: 0,
    latency_count: 0,
    pricing_completeness: 'priced',
    priced_request_count: overrides.request_count ?? 0,
    ...overrides,
  };
}

const summary: DashboardSummaryResponse = {
  range: '24h',
  step: 'hour',
  window_start_unix_secs: BUCKET_START,
  window_end_unix_secs: BUCKET_START + 3_600,
  totals: {
    request_count: 12,
    input_tokens: 100,
    output_tokens: 200,
    cache_creation_input_tokens: 300,
    cache_read_input_tokens: 400,
    error_count: 0,
    error_rate: 0,
    virtual_cost_micros: 1_500_000,
    avg_latency_ms: 100,
    pricing_completeness: 'priced',
    priced_request_count: 12,
  },
  sparkline: {
    buckets: [
      usageBucket({
        request_count: 12,
        input_tokens: 100,
        output_tokens: 200,
        cache_creation_input_tokens: 300,
        cache_read_input_tokens: 400,
        virtual_cost_micros: 1_500_000,
        latency_ms_sum: 1_200,
        latency_count: 12,
      }),
    ],
  },
  observed: true,
};

const firstPrincipalUsage: DashboardUsageResponse = {
  range: '24h',
  step: 'hour',
  group_by: 'principal',
  window_start_unix_secs: BUCKET_START,
  window_end_unix_secs: BUCKET_START + 3_600,
  observed: true,
  series: [
    {
      key: 'principal-alpha',
      buckets: [
        usageBucket({
          request_count: 12,
          input_tokens: 100,
          output_tokens: 200,
          cache_creation_input_tokens: 300,
          cache_read_input_tokens: 400,
          virtual_cost_micros: 1_500_000,
          cost_input_micros: 100_000,
          cost_output_micros: 200_000,
          cost_cache_creation_5m_micros: 300_000,
          cost_cache_creation_1h_micros: 400_000,
          cost_cache_read_micros: 500_000,
        }),
      ],
    },
  ],
};

const refreshedPrincipalUsage: DashboardUsageResponse = {
  ...firstPrincipalUsage,
  series: [
    {
      key: 'principal-alpha',
      buckets: [
        usageBucket({
          request_count: 18,
          input_tokens: 200,
          output_tokens: 300,
          cache_creation_input_tokens: 400,
          cache_read_input_tokens: 600,
          virtual_cost_micros: 2_000_000,
          cost_input_micros: 200_000,
          cost_output_micros: 300_000,
          cost_cache_creation_5m_micros: 400_000,
          cost_cache_creation_1h_micros: 500_000,
          cost_cache_read_micros: 600_000,
        }),
      ],
    },
  ],
};

const emptyPrincipalUsage: DashboardUsageResponse = {
  ...firstPrincipalUsage,
  series: [],
};

const FIRST_COST_DETAILS = [
  'Cache read',
  '$0.5000',
  '33%',
  'Cache create 5m',
  '$0.3000',
  '20%',
  'Cache create 1h',
  '$0.4000',
  '27%',
  'Input',
  '$0.1000',
  '7%',
  'Output',
  '$0.2000',
  '13%',
  'Total',
  '$1.5000',
];

const REFRESHED_COST_DETAILS = [
  'Cache read',
  '$0.6000',
  '30%',
  'Cache create 5m',
  '$0.4000',
  '20%',
  'Cache create 1h',
  '$0.5000',
  '25%',
  'Input',
  '$0.2000',
  '10%',
  'Output',
  '$0.3000',
  '15%',
  'Total',
  '$2.0000',
];

const COST_COLORS = [
  'var(--color-series-cache-read)',
  'var(--color-series-cache-create-5m)',
  'var(--color-series-cache-create-1h)',
  'var(--color-series-input)',
  'var(--color-series-output)',
];

type UsageFixtureResponse = {
  status: number;
  body: unknown;
};

type OverviewFixtureOptions = {
  holdFirstUsage?: boolean;
  usageResponse: (requestNumber: number) => UsageFixtureResponse;
};

type OverviewFixtures = {
  releaseFirstUsage: () => void;
  usageRequestTimes: () => readonly number[];
  usageUrls: () => readonly string[];
};

async function installOverviewFixtures(
  page: Page,
  options: OverviewFixtureOptions,
): Promise<OverviewFixtures> {
  const usageRequestTimes: number[] = [];
  const usageUrls: string[] = [];
  let releaseFirstUsage = () => {};
  const firstUsageGate = new Promise<void>((resolve) => {
    releaseFirstUsage = resolve;
  });

  await page.addInitScript(() => {
    window.localStorage.setItem('cc-lb-admin-token', 'mock-token');
  });

  await page.route('**/admin/**', async (route) => {
    const request = route.request();
    const url = new URL(request.url());
    const { pathname } = url;
    const json = (status: number, body: unknown) =>
      route.fulfill({
        status,
        contentType: 'application/json',
        body: JSON.stringify(body),
      });

    if (pathname === '/admin/health') {
      return json(200, {
        status: 'ok',
        version: 'test',
        git_sha: 'test',
        uptime_secs: 1,
      });
    }
    if (pathname === '/admin/v1/auth/session') {
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
    if (pathname === '/admin/v1/principals') {
      return json(200, {
        principals: [{ id: 'principal-alpha', name: 'Principal Alpha' }],
      });
    }
    if (pathname === '/admin/v1/upstreams') {
      return json(200, {
        upstreams: [
          {
            id: 'upstream-alpha',
            name: 'Anthropic Alpha',
            kind: 'anthropic_oauth',
            enabled: true,
            base_url: null,
            warmup_enabled: false,
            warmup_dialect_plugin: null,
            spec_revision: 1,
            status: {
              last_apply_error: null,
              last_apply_at_unix_secs: null,
              last_warmup_at_unix_secs: null,
            },
          },
        ],
      });
    }
    if (pathname === '/admin/v1/upstreams/limit-resets') {
      return json(200, { upstreams: [] });
    }
    if (pathname === '/admin/v1/plugins/registry') {
      return json(200, { entries: [] });
    }
    if (pathname === '/admin/v1/status') {
      return json(200, {
        version: 'test',
        git_sha: 'test',
        uptime_secs: 1,
        build: { rust_version: 'mock', profile: 'debug', target: 'mock' },
        generation: 1,
        upstreams: [
          {
            id: 'upstream-alpha',
            name: 'Anthropic Alpha',
            status: 'active',
            last_apply_at_unix_secs: null,
            last_apply_error: null,
          },
        ],
      });
    }
    if (pathname === '/admin/v1/subscription-quotas/latest') {
      return json(200, {
        max_staleness_secs: 300,
        upstreams: [],
      });
    }
    if (/^\/admin\/v1\/upstreams\/[^/]+\/oauth\/status$/.test(pathname)) {
      return json(200, {
        status: 'connected',
        has_refresh_token: true,
        expires_at_unix_secs: Math.floor(Date.now() / 1000) + 3600,
      });
    }
    if (pathname === '/admin/v1/dashboard/summary') {
      return json(200, summary);
    }
    if (pathname === '/admin/v1/dashboard/usage') {
      const groupBy = url.searchParams.get('group_by') ?? '';
      const providers = url.searchParams.get('providers');
      if (groupBy !== 'principal' || providers !== 'anthropic') {
        if (options.holdFirstUsage && groupBy === 'principal') {
          await firstUsageGate;
        }
        return json(200, {
          range: url.searchParams.get('range') ?? '7d',
          step: url.searchParams.get('step') ?? 'hour',
          group_by: groupBy || 'upstream',
          window_start_unix_secs: BUCKET_START,
          window_end_unix_secs: BUCKET_START + 3_600,
          observed: true,
          series: [],
        });
      }
      usageUrls.push(request.url());
      usageRequestTimes.push(Date.now());
      const requestNumber = usageUrls.length;
      if (options.holdFirstUsage && requestNumber === 1) {
        await firstUsageGate;
      }
      const response = options.usageResponse(requestNumber);
      return json(response.status, response.body);
    }
    if (pathname === '/admin/v1/events/recent') {
      return json(200, { events: [], observed: true, count: 0, limit: 200 });
    }
    if (pathname === '/admin/v1/subscription-quotas/aggregate') {
      return json(200, {
        now_unix_secs: BUCKET_START + 3_600,
        window_anchor_unix_secs: BUCKET_START,
        max_staleness_secs: 300,
        upstream_count: 0,
        windows: [],
        caveats: [],
      });
    }
    if (pathname === '/admin/v1/subscription-quotas/pool-history') {
      return json(200, {
        now_unix_secs: BUCKET_START + 3_600,
        windows: [],
      });
    }
    if (pathname === '/admin/v1/events/stream') {
      return route.fulfill({
        status: 200,
        headers: {
          'cache-control': 'no-cache',
          'content-type': 'text/event-stream',
        },
        body: ': connected\n\n',
      });
    }

    return json(404, {
      error: `Unexpected Overview request: ${request.method()} ${pathname}`,
    });
  });

  return {
    releaseFirstUsage,
    usageRequestTimes: () => [...usageRequestTimes],
    usageUrls: () => [...usageUrls],
  };
}

async function readCostDetails(page: Page): Promise<string[]> {
  return page
    .getByTestId('top-principal-cost-details')
    .locator('span')
    .evaluateAll((nodes) =>
      nodes
        .map((node) => node.textContent?.trim() ?? '')
        .filter((text) => text.length > 0),
    );
}

function principalCostTrigger(page: Page) {
  return page
    .getByTestId('top-principal-row')
    .getByRole('button', { name: /show breakdown$/ });
}

async function readCostSegmentColors(page: Page): Promise<string[]> {
  return principalCostTrigger(page)
    .locator('[data-cell-bar] span')
    .evaluateAll((nodes) =>
      nodes.map((node) => (node as HTMLElement).style.backgroundColor),
    );
}

function trackBrowserErrors(page: Page) {
  const pageErrors: string[] = [];
  const adminResponseErrors: string[] = [];
  page.on('pageerror', (error) => pageErrors.push(error.message));
  page.on('response', (response) => {
    const url = new URL(response.url());
    if (url.pathname.startsWith('/admin/') && !response.ok()) {
      adminResponseErrors.push(`${response.status()} ${url.pathname}`);
    }
  });
  return { adminResponseErrors, pageErrors };
}

function expectTotalsProjection(usageUrls: readonly string[]) {
  expect(usageUrls.length).toBeGreaterThanOrEqual(1);
  for (const requestedUrl of usageUrls) {
    const url = new URL(requestedUrl);
    expect(url.pathname).toBe('/admin/v1/dashboard/usage');
    expect(url.searchParams.get('range')).toBe('7d');
    expect(url.searchParams.get('step')).toBe('hour');
    expect(url.searchParams.get('group_by')).toBe('principal');
    expect(url.searchParams.get('projection')).toBe('totals');
  }
}

/**
 * The tests in this file are browser consumer-layer evidence for the Overview
 * card only. Rust tests own server cache, storage, and state-transition
 * correctness; these mocks fix the DOM contract that consumes their principal
 * totals projection.
 */
test('renders and refreshes principal total and component costs without reloading', async ({
  page,
}) => {
  test.setTimeout(45_000);
  const { adminResponseErrors, pageErrors } = trackBrowserErrors(page);
  const fixtures = await installOverviewFixtures(page, {
    holdFirstUsage: true,
    usageResponse: (requestNumber) => ({
      status: 200,
      body:
        requestNumber === 1 ? firstPrincipalUsage : refreshedPrincipalUsage,
    }),
  });

  await page.goto('/');
  await expect(page.getByText('Top principals', { exact: true })).toBeVisible();

  // Hold the first usage response long enough to prove the card keeps its
  // intended loading geometry instead of flashing its empty state.
  await expect(page.getByTestId('top-principal-skeleton-row')).toHaveCount(5);
  await expect(page.getByTestId('top-principal-row')).toHaveCount(0);
  await expect(
    page.getByText('No usage in the last 7d', { exact: true }),
  ).toHaveCount(0);
  await expect.poll(() => fixtures.usageUrls().length).toBe(1);
  fixtures.releaseFirstUsage();

  const row = page.getByTestId('top-principal-row');
  await expect(row).toBeVisible();
  await expect(row).toContainText('Principal Alpha');
  await expect(row).toContainText('$1.50');
  await expect(page.getByTestId('top-principal-skeleton-row')).toHaveCount(0);
  await expect(
    page.getByText('No usage in the last 7d', { exact: true }),
  ).toHaveCount(0);
  const costTrigger = principalCostTrigger(page);
  await expect(costTrigger).toHaveAttribute(
    'aria-label',
    'Principal Alpha cost $1.5000, show breakdown',
  );
  await costTrigger.focus();
  await page.keyboard.press('Enter');
  await expect(page.getByTestId('top-principal-cost-details')).toBeVisible();
  await expect.poll(() => readCostDetails(page)).toEqual(FIRST_COST_DETAILS);
  await expect.poll(() => readCostSegmentColors(page)).toEqual(COST_COLORS);

  // A marker on this Window distinguishes the 5-second query poll from a
  // document reload that merely happens to render the second fixture.
  await page.evaluate(() => {
    (window as Window & { principalTotalsRefreshProbe?: string })
      .principalTotalsRefreshProbe = 'alive';
  });
  const fetchCountBeforePoll = fixtures.usageUrls().length;

  await expect
    .poll(() => fixtures.usageUrls().length, {
      timeout: POLL_UPDATE_TIMEOUT_MS,
    })
    .toBeGreaterThan(fetchCountBeforePoll);
  await expect(row).toContainText('$2.00', {
    timeout: POLL_UPDATE_TIMEOUT_MS,
  });
  await expect.poll(() => readCostDetails(page)).toEqual(REFRESHED_COST_DETAILS);
  await expect.poll(() => readCostSegmentColors(page)).toEqual(COST_COLORS);
  await expect(row).not.toContainText('$1.50');
  await expect(costTrigger).toHaveAttribute(
    'aria-label',
    'Principal Alpha cost $2.0000, show breakdown',
  );

  await expect(page.getByTestId('top-principal-skeleton-row')).toHaveCount(0);
  await expect(
    page.getByText('No usage in the last 7d', { exact: true }),
  ).toHaveCount(0);
  expect(
    await page.evaluate(
      () =>
        (window as Window & { principalTotalsRefreshProbe?: string })
          .principalTotalsRefreshProbe,
    ),
  ).toBe('alive');

  const usageUrls = fixtures.usageUrls();
  expect(usageUrls.length).toBeGreaterThanOrEqual(2);
  expectTotalsProjection(usageUrls);

  const requestTimes = fixtures.usageRequestTimes();
  expect(requestTimes[1] - requestTimes[0]).toBeGreaterThanOrEqual(
    POLL_INTERVAL_MS - 500,
  );
  expect(pageErrors).toEqual([]);
  expect(adminResponseErrors).toEqual([]);
});

test('renders the Top principals empty state for an empty totals response', async ({
  page,
}) => {
  const { adminResponseErrors, pageErrors } = trackBrowserErrors(page);
  const fixtures = await installOverviewFixtures(page, {
    usageResponse: () => ({ status: 200, body: emptyPrincipalUsage }),
  });

  await page.goto('/');
  await expect(page.getByText('Top principals', { exact: true })).toBeVisible();
  await expect(
    page.getByText('No usage in the last 7d', { exact: true }),
  ).toBeVisible();
  await expect(page.getByTestId('top-principal-skeleton-row')).toHaveCount(0);
  await expect(page.getByTestId('top-principal-row')).toHaveCount(0);

  expectTotalsProjection(fixtures.usageUrls());
  expect(pageErrors).toEqual([]);
  expect(adminResponseErrors).toEqual([]);
});

test('recovers the Top principals totals after an initial usage error', async ({
  page,
}) => {
  test.setTimeout(45_000);
  const { adminResponseErrors, pageErrors } = trackBrowserErrors(page);
  const fixtures = await installOverviewFixtures(page, {
    usageResponse: (requestNumber) =>
      requestNumber <= 2
        ? {
            status: 500,
            body: { message: 'Principal totals unavailable' },
          }
        : { status: 200, body: refreshedPrincipalUsage },
  });

  await page.goto('/');
  await expect(page.getByText('Top principals', { exact: true })).toBeVisible();

  // The query client retries once. Both failed HTTP attempts must settle into
  // the existing error toast before the independent 5-second poll can recover.
  const errorToast = page.locator('[data-sonner-toast][data-type="error"]');
  await expect(errorToast).toContainText('Principal totals unavailable');
  await expect(page.getByTestId('top-principal-row')).toHaveCount(0);
  await expect(
    page.getByText('No usage in the last 7d', { exact: true }),
  ).toBeVisible();
  await expect.poll(() => [...adminResponseErrors]).toEqual([
    '500 /admin/v1/dashboard/usage',
    '500 /admin/v1/dashboard/usage',
  ]);
  expect(fixtures.usageUrls()).toHaveLength(2);

  await page.evaluate(() => {
    (window as Window & { principalTotalsErrorRecoveryProbe?: string })
      .principalTotalsErrorRecoveryProbe = 'alive';
  });

  await expect
    .poll(() => fixtures.usageUrls().length, {
      timeout: POLL_UPDATE_TIMEOUT_MS,
    })
    .toBeGreaterThan(2);

  const row = page.getByTestId('top-principal-row');
  await expect(row).toBeVisible({ timeout: POLL_UPDATE_TIMEOUT_MS });
  await expect(row).toContainText('Principal Alpha');
  await expect(row).toContainText('$2.00');
  await expect(
    page.getByText('No usage in the last 7d', { exact: true }),
  ).toHaveCount(0);
  await expect(page.getByTestId('top-principal-skeleton-row')).toHaveCount(0);

  const costTrigger = principalCostTrigger(page);
  await expect(costTrigger).toHaveAttribute(
    'aria-label',
    'Principal Alpha cost $2.0000, show breakdown',
  );
  await costTrigger.focus();
  await page.keyboard.press('Enter');
  await expect(page.getByTestId('top-principal-cost-details')).toBeVisible();
  await expect.poll(() => readCostDetails(page)).toEqual(
    REFRESHED_COST_DETAILS,
  );
  await expect.poll(() => readCostSegmentColors(page)).toEqual(COST_COLORS);

  expect(
    await page.evaluate(
      () =>
        (window as Window & { principalTotalsErrorRecoveryProbe?: string })
          .principalTotalsErrorRecoveryProbe,
    ),
  ).toBe('alive');
  expectTotalsProjection(fixtures.usageUrls());

  const requestTimes = fixtures.usageRequestTimes();
  expect(requestTimes.length).toBeGreaterThanOrEqual(3);
  expect(requestTimes[2] - requestTimes[1]).toBeGreaterThanOrEqual(
    POLL_INTERVAL_MS - 500,
  );
  expect(pageErrors).toEqual([]);
  expect(adminResponseErrors).toEqual([
    '500 /admin/v1/dashboard/usage',
    '500 /admin/v1/dashboard/usage',
  ]);
});
