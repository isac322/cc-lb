import { expect, type Page, test } from '@playwright/test';
import type {
  DashboardSummaryResponse,
  DashboardUsageResponse,
  UsageBucket,
} from '../src/lib/api';
import { fulfillAuthenticatedSession } from './support/auth-session';

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
    proxy_setup_ms_sum: 0,
    proxy_setup_ms_count: 0,
    shape_ms_sum: 0,
    shape_ms_count: 0,
    sign_ms_sum: 0,
    sign_ms_count: 0,
    upstream_ttfb_ms_sum: 0,
    upstream_ttfb_ms_count: 0,
    upstream_body_ms_sum: 0,
    upstream_body_ms_count: 0,
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
    avg_proxy_setup_ms: 0,
    avg_shape_ms: 0,
    avg_sign_ms: 0,
    avg_upstream_ttfb_ms: 0,
    avg_upstream_body_ms: 0,
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
  'Input',
  '$0.1000',
  '7%',
  'Output',
  '$0.2000',
  '13%',
  'Cache create 5m',
  '$0.3000',
  '20%',
  'Cache create 1h',
  '$0.4000',
  '27%',
  'Cache read',
  '$0.5000',
  '33%',
  'Total',
  '$1.5000',
];

const REFRESHED_COST_DETAILS = [
  'Input',
  '$0.2000',
  '10%',
  'Output',
  '$0.3000',
  '15%',
  'Cache create 5m',
  '$0.4000',
  '20%',
  'Cache create 1h',
  '$0.5000',
  '25%',
  'Cache read',
  '$0.6000',
  '30%',
  'Total',
  '$2.0000',
];

const COST_CATEGORIES = [
  'input',
  'output',
  'cache_create_5m',
  'cache_create_1h',
  'cache_read',
];

type UsageFixtureResponse = {
  status: number;
  body: unknown;
};

type OverviewFixtureOptions = {
  holdFirstUsage?: boolean;
  holdSuccessfulUsage?: boolean;
  usageResponse: (requestNumber: number) => UsageFixtureResponse;
};

type OverviewFixtures = {
  releaseFirstUsage: () => void;
  releaseSuccessfulUsage: () => void;
  usageRequestTimes: () => readonly number[];
  usageUrls: () => readonly string[];
};

async function installOverviewFixtures(
  page: Page,
  options: OverviewFixtureOptions,
): Promise<OverviewFixtures> {
  const usageRequestTimes: number[] = [];
  const usageUrls: string[] = [];
  let firstUsageReleased = false;
  let resolveFirstUsage = () => {};
  const firstUsageGate = new Promise<void>((resolve) => {
    resolveFirstUsage = resolve;
  });
  const releaseFirstUsage = () => {
    firstUsageReleased = true;
    resolveFirstUsage();
  };
  let forceUsageFailure = options.holdSuccessfulUsage === true;
  const releaseSuccessfulUsage = () => {
    forceUsageFailure = false;
  };

  await page.addInitScript(() => {
    window.localStorage.setItem('cc-lb-admin-token', 'mock-token');
  });

  await page.route('**/admin/**', async (route) => {
    if (await fulfillAuthenticatedSession(route)) return;
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
    if (pathname === '/admin/v1/principals') {
      return json(200, {
        principals: [{ id: 'principal-alpha', name: 'Principal Alpha' }],
      });
    }
    if (pathname === '/admin/v1/upstreams') {
      return json(200, { upstreams: [] });
    }
    if (pathname === '/admin/dashboard/summary') {
      return json(200, summary);
    }
    if (pathname === '/admin/usage') {
      usageUrls.push(request.url());
      const isInitialBatch =
        options.holdFirstUsage === true && !firstUsageReleased;
      if (!isInitialBatch || usageRequestTimes.length === 0) {
        usageRequestTimes.push(Date.now());
      }
      if (isInitialBatch) {
        await firstUsageGate;
      }
      const requestNumber = isInitialBatch ? 1 : usageRequestTimes.length;
      const response = options.usageResponse(
        forceUsageFailure ? 1 : requestNumber,
      );
      return json(response.status, response.body);
    }
    if (pathname === '/admin/events/recent') {
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
    if (pathname === '/admin/events/stream') {
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
    releaseSuccessfulUsage,
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

async function readCostCategories(page: Page): Promise<string[]> {
  return page
    .getByTestId('top-principal-cost-segment')
    .evaluateAll((nodes) =>
      nodes.map((node) => (node as HTMLElement).dataset.category ?? ''),
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
    expect(url.pathname).toBe('/admin/usage');
    expect(url.searchParams.get('range')).toBe('24h');
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
  await expect.poll(() => fixtures.usageUrls().length).toBeGreaterThanOrEqual(1);
  await expect(page.getByText('No usage data', { exact: true })).toHaveCount(0);
  fixtures.releaseFirstUsage();

  const row = page.getByTestId('top-principal-row');
  await expect(row).toBeVisible();
  await expect(row).toContainText('Principal Alpha');
  await expect(row).toContainText('$1.50');
  await expect(page.getByTestId('top-principal-skeleton-row')).toHaveCount(0);
  await expect(page.getByText('No usage data', { exact: true })).toHaveCount(0);
  const costMeter = page.getByTestId('top-principal-cost-meter');
  await expect(costMeter).toHaveAttribute('data-cost-components', 'complete');

  const costTrigger = page.getByTestId('top-principal-cost-trigger');
  await costTrigger.focus();
  await expect(page.getByTestId('top-principal-cost-details')).toBeVisible();
  await expect.poll(() => readCostDetails(page)).toEqual(FIRST_COST_DETAILS);
  await expect.poll(() => readCostCategories(page)).toEqual(COST_CATEGORIES);

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
  await expect.poll(() => readCostCategories(page)).toEqual(COST_CATEGORIES);
  await expect(row).not.toContainText('$1.50');
  await expect(costMeter).toHaveAttribute('data-cost-components', 'complete');

  await expect(page.getByTestId('top-principal-skeleton-row')).toHaveCount(0);
  await expect(page.getByText('No usage data', { exact: true })).toHaveCount(0);
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
  await expect(page.getByText('No usage data', { exact: true })).toBeVisible();
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
    holdSuccessfulUsage: true,
    usageResponse: (requestNumber) =>
      requestNumber === 1
        ? {
            status: 500,
            body: { message: 'Principal totals unavailable' },
          }
        : { status: 200, body: refreshedPrincipalUsage },
  });

  await page.goto('/');
  await expect(page.getByText('Top principals', { exact: true })).toBeVisible();

  // Keep returning the initial failure until the error state is visible, so
  // recovery cannot race the observable error UX even if React aborts and
  // restarts the first request.
  const errorToast = page.locator('[data-sonner-toast][data-type="error"]');
  await expect(errorToast).toContainText('Principal totals unavailable', {
    timeout: POLL_UPDATE_TIMEOUT_MS,
  });
  await expect(page.getByTestId('top-principal-row')).toHaveCount(0);
  await expect(page.getByText('No usage data', { exact: true })).toBeVisible();
  expect(adminResponseErrors.length).toBeGreaterThanOrEqual(1);
  expect(
    adminResponseErrors.every((error) => error === '500 /admin/usage'),
  ).toBe(true);
  const failureCount = adminResponseErrors.length;

  await page.evaluate(() => {
    (window as Window & { principalTotalsErrorRecoveryProbe?: string })
      .principalTotalsErrorRecoveryProbe = 'alive';
  });
  const recoveryReleasedAt = Date.now();
  fixtures.releaseSuccessfulUsage();

  await expect
    .poll(() => fixtures.usageUrls().length, {
      timeout: POLL_UPDATE_TIMEOUT_MS,
    })
    .toBeGreaterThan(failureCount);

  const row = page.getByTestId('top-principal-row');
  await expect(row).toBeVisible({ timeout: POLL_UPDATE_TIMEOUT_MS });
  await expect(row).toContainText('Principal Alpha');
  await expect(row).toContainText('$2.00');
  await expect(page.getByText('No usage data', { exact: true })).toHaveCount(0);
  await expect(page.getByTestId('top-principal-skeleton-row')).toHaveCount(0);

  const costMeter = page.getByTestId('top-principal-cost-meter');
  await expect(costMeter).toHaveAttribute('data-cost-components', 'complete');
  await page.getByTestId('top-principal-cost-trigger').focus();
  await expect(page.getByTestId('top-principal-cost-details')).toBeVisible();
  await expect.poll(() => readCostDetails(page)).toEqual(
    REFRESHED_COST_DETAILS,
  );
  await expect.poll(() => readCostCategories(page)).toEqual(COST_CATEGORIES);

  expect(
    await page.evaluate(
      () =>
        (window as Window & { principalTotalsErrorRecoveryProbe?: string })
          .principalTotalsErrorRecoveryProbe,
    ),
  ).toBe('alive');
  expectTotalsProjection(fixtures.usageUrls());

  const requestTimes = fixtures.usageRequestTimes();
  expect(requestTimes.at(-1)! - recoveryReleasedAt).toBeGreaterThanOrEqual(
    POLL_INTERVAL_MS - 500,
  );
  expect(pageErrors).toEqual([]);
  expect(adminResponseErrors).toHaveLength(failureCount);
  expect(
    adminResponseErrors.every((error) => error === '500 /admin/usage'),
  ).toBe(true);
});
