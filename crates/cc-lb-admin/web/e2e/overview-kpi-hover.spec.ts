import { expect, type Page, test } from '@playwright/test';
import type {
  DashboardSummaryResponse,
  DashboardUsageResponse,
  PoolHistoryResponse,
  UsageBucket,
} from '../src/lib/api';
import type { Theme } from '../src/lib/theme';

const TIMESTAMPS = [
  Date.UTC(2026, 7, 17, 13, 30) / 1000,
  Date.UTC(2026, 7, 17, 14, 30) / 1000,
  Date.UTC(2026, 7, 17, 15, 30) / 1000,
] as const;

function usageBucket(
  bucketStart: number,
  overrides: Partial<UsageBucket>,
): UsageBucket {
  return {
    bucket_start_unix_secs: bucketStart,
    request_count: 0,
    input_tokens: 0,
    output_tokens: 0,
    cache_creation_input_tokens: 0,
    cache_read_input_tokens: 0,
    error_count: 0,
    virtual_cost_micros: 0,
    latency_ms_sum: 0,
    latency_count: 0,
    ...overrides,
  };
}

const summaryBuckets = [
  usageBucket(TIMESTAMPS[0], {
    request_count: 120,
    input_tokens: 100,
    output_tokens: 50,
    cache_creation_input_tokens: 300,
    cache_read_input_tokens: 600,
    error_count: 6,
    virtual_cost_micros: 1_000_000,
    latency_ms_sum: 12_000,
    latency_count: 120,
  }),
  usageBucket(TIMESTAMPS[1], {
    request_count: 180,
    input_tokens: 300,
    output_tokens: 100,
    cache_creation_input_tokens: 100,
    cache_read_input_tokens: 400,
    error_count: 18,
    virtual_cost_micros: 2_500_000,
    latency_ms_sum: 36_000,
    latency_count: 180,
  }),
  usageBucket(TIMESTAMPS[2], {
    request_count: 240,
    output_tokens: 30,
    virtual_cost_micros: 4_000_000,
    latency_ms_sum: 72_000,
    latency_count: 240,
  }),
];

const summary: DashboardSummaryResponse = {
  range: '24h',
  step: 'minute',
  window_start_unix_secs: TIMESTAMPS[0],
  window_end_unix_secs: TIMESTAMPS[2] + 60,
  totals: {
    request_count: 540,
    input_tokens: 400,
    output_tokens: 180,
    cache_creation_input_tokens: 400,
    cache_read_input_tokens: 1_000,
    error_count: 24,
    error_rate: 24 / 540,
    virtual_cost_micros: 7_500_000,
    avg_latency_ms: 200,
  },
  sparkline: { buckets: summaryBuckets },
  observed: true,
};

/**
 * The same window with an empty sparkline. Totals stay put, so the page still
 * renders while the KPI charts hold zero buckets.
 */
const emptySummary: DashboardSummaryResponse = {
  ...summary,
  sparkline: { buckets: [] },
};

const principalUsage: DashboardUsageResponse = {
  range: '24h',
  step: 'hour',
  group_by: 'principal',
  window_start_unix_secs: TIMESTAMPS[0],
  window_end_unix_secs: TIMESTAMPS[2] + 60,
  observed: true,
  series: [
    {
      key: 'principal-alpha',
      buckets: [
        usageBucket(TIMESTAMPS[0], {
          request_count: 10,
          input_tokens: 50,
          output_tokens: 100,
          cache_creation_input_tokens: 150,
          cache_read_input_tokens: 800,
          virtual_cost_micros: 1_000_000,
          cost_input_micros: 300_000,
          cost_output_micros: 400_000,
          cost_cache_creation_5m_micros: 150_000,
          cost_cache_creation_1h_micros: 0,
          cost_cache_read_micros: 150_000,
        }),
        usageBucket(TIMESTAMPS[1], {
          request_count: 20,
          input_tokens: 150,
          output_tokens: 100,
          cache_creation_input_tokens: 50,
          cache_read_input_tokens: 1_200,
          virtual_cost_micros: 2_000_000,
        }),
      ],
    },
  ],
};

/**
 * The same 24h window one poll later. Every non-cache field is carried over by
 * spread, and only the cache composition is rewritten: prompt tokens still add
 * up to 1,800 across the window and to 800 in the hovered middle bucket, so the
 * tokens tile keeps its value and its `Tokens 900` row while the ratios move —
 * `Cache miss` 44.4% → 70.0% (1,260/1,800) and the hovered bucket 50.0% →
 * 80.0% (640/800). The last bucket was idle and reported no ratio at all; now it
 * reports one too, so the cache-miss line stops breaking mid-chart.
 */
const refreshedSummary: DashboardSummaryResponse = {
  ...summary,
  totals: {
    ...summary.totals,
    input_tokens: 420,
    cache_creation_input_tokens: 840,
    cache_read_input_tokens: 540,
  },
  sparkline: {
    buckets: [
      usageBucket(TIMESTAMPS[0], {
        request_count: 120,
        input_tokens: 80,
        output_tokens: 50,
        cache_creation_input_tokens: 240,
        cache_read_input_tokens: 80,
        error_count: 6,
        virtual_cost_micros: 1_000_000,
        latency_ms_sum: 12_000,
        latency_count: 120,
      }),
      usageBucket(TIMESTAMPS[1], {
        request_count: 180,
        input_tokens: 240,
        output_tokens: 100,
        cache_creation_input_tokens: 400,
        cache_read_input_tokens: 160,
        error_count: 18,
        virtual_cost_micros: 2_500_000,
        latency_ms_sum: 36_000,
        latency_count: 180,
      }),
      usageBucket(TIMESTAMPS[2], {
        request_count: 240,
        input_tokens: 100,
        output_tokens: 30,
        cache_creation_input_tokens: 200,
        cache_read_input_tokens: 300,
        virtual_cost_micros: 4_000_000,
        latency_ms_sum: 72_000,
        latency_count: 240,
      }),
    ],
  },
};

/**
 * The same principal one poll later: 30 requests, 2,600 tokens and $3.00 as
 * before — prompt tokens still total 2,400 — with only the cache split
 * rewritten, so the row's cache hit moves 83.3% → 40.0% (960/2,400) and nothing
 * else on that row is allowed to move with it.
 */
const refreshedPrincipalUsage: DashboardUsageResponse = {
  ...principalUsage,
  series: [
    {
      key: 'principal-alpha',
      buckets: [
        usageBucket(TIMESTAMPS[0], {
          request_count: 10,
          input_tokens: 240,
          output_tokens: 100,
          cache_creation_input_tokens: 240,
          cache_read_input_tokens: 320,
          virtual_cost_micros: 1_000_000,
          cost_input_micros: 600_000,
          cost_output_micros: 200_000,
          cost_cache_creation_5m_micros: 100_000,
          cost_cache_creation_1h_micros: 100_000,
          cost_cache_read_micros: 0,
        }),
        usageBucket(TIMESTAMPS[1], {
          request_count: 20,
          input_tokens: 480,
          output_tokens: 100,
          cache_creation_input_tokens: 480,
          cache_read_input_tokens: 640,
          virtual_cost_micros: 2_000_000,
        }),
      ],
    },
  ],
};

function tooltipTimestamp(unix: number): string {
  const date = new Date(unix * 1000);
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')} ${String(date.getHours()).padStart(2, '0')}:${String(date.getMinutes()).padStart(2, '0')}`;
}

const KPI_CHART_IDS = [
  'request-rate',
  'tokens',
  'cost',
  'latency',
  'error-rate',
] as const;

type KpiChartId = (typeof KPI_CHART_IDS)[number];

/**
 * Tooltip rows below the timestamp for the hovered (middle) fixture bucket, plus
 * the cache figures the first payload puts on the tokens tile and the principal
 * row. Each `REFRESHED_` value is what the second payload replaces it with.
 */
const TOKENS_VALUE_ROW = 'Tokens 900';
const CACHE_MISS_ROW = 'Cache miss 50.0%';
const REFRESHED_CACHE_MISS_ROW = 'Cache miss 80.0%';
const CACHE_MISS_AVG = 'Cache miss 44.4%';
const REFRESHED_CACHE_MISS_AVG = 'Cache miss 70.0%';
const PRINCIPAL_CACHE_HIT = '83.3% cache hit';
const REFRESHED_PRINCIPAL_CACHE_HIT = '40.0% cache hit';

/**
 * The principal's cost breakdown: label, exact value and share for every
 * request-log category, then the footer total. Only the first bucket of the
 * window records components, so its $1.00 is split and the second bucket's
 * $2.00 stays unattributed — the shape a window that straddles the upgrade has.
 * Each `REFRESHED_` list is what the second payload replaces it with, at the
 * same $3.00 total.
 */
const PRINCIPAL_COST_VALUES = [
  'Input',
  '$0.3000',
  '10%',
  'Output',
  '$0.4000',
  '13%',
  'Cache create 5m',
  '$0.1500',
  '5%',
  'Cache create 1h',
  '$0.0000',
  '—',
  'Cache read',
  '$0.1500',
  '5%',
  'Unattributed',
  '$2.0000',
  '67%',
  'Total',
  '$3.0000',
];

const REFRESHED_PRINCIPAL_COST_VALUES = [
  'Input',
  '$0.6000',
  '20%',
  'Output',
  '$0.2000',
  '7%',
  'Cache create 5m',
  '$0.1000',
  '3%',
  'Cache create 1h',
  '$0.1000',
  '3%',
  'Cache read',
  '$0.0000',
  '—',
  'Unattributed',
  '$2.0000',
  '67%',
  'Total',
  '$3.0000',
];

/**
 * Bar segments, in order, as a percentage of the filled meter: a category worth
 * $0 is drawn by nobody, and the $2.00 no category accounts for closes the bar.
 */
const PRINCIPAL_COST_SEGMENTS = [
  { category: 'input', widthPct: 10 },
  { category: 'output', widthPct: 13.33 },
  { category: 'cache_create_5m', widthPct: 5 },
  { category: 'cache_read', widthPct: 5 },
  { category: 'unattributed', widthPct: 66.67 },
];

const REFRESHED_PRINCIPAL_COST_SEGMENTS = [
  { category: 'input', widthPct: 20 },
  { category: 'output', widthPct: 6.67 },
  { category: 'cache_create_5m', widthPct: 3.33 },
  { category: 'cache_create_1h', widthPct: 3.33 },
  { category: 'unattributed', widthPct: 66.67 },
];

/**
 * Only the tokens tile carries a cache-miss row, and that row is the one value a
 * refreshed payload moves, so the hovered bucket's rows are built around it.
 */
function expectedTooltipRows(
  cacheMissRow: string,
): Record<KpiChartId, readonly string[]> {
  return {
    'request-rate': ['Req/s 3'],
    tokens: [TOKENS_VALUE_ROW, cacheMissRow],
    cost: ['Equiv $ $2.50'],
    latency: ['Avg latency 200ms'],
    'error-rate': ['Err rate 10.00%'],
  };
}

/** `useSummary` and the principal `useUsage` both poll on this cadence. */
const POLL_INTERVAL_MS = 5_000;

/**
 * Narrowly above two poll cycles: a payload swapped just after one tick still
 * lands in time, while a value that only refreshes on a reload, on a range
 * switch or on a manual refetch runs out of time here.
 */
const POLL_UPDATE_TIMEOUT_MS = 2 * POLL_INTERVAL_MS + 1_000;

/** Marker parked on the document so a reload cannot pass for a live refresh. */
type ProbedWindow = Window & { cclbPollProbe?: string };

/** Explicit themes written to `cclb.theme`; order drives the dark-vs-light check. */
const THEMES = ['dark', 'light'] as const satisfies readonly Theme[];

type ThemeChoice = (typeof THEMES)[number];

const DESKTOP_VIEWPORT = { width: 1280, height: 900 };
const MOBILE_VIEWPORT = { width: 375, height: 667 };

type Rgba = { r: number; g: number; b: number; a: number };

function parseCssColor(value: string): Rgba {
  const match = /^rgba?\(([^)]+)\)$/.exec(value.trim());
  if (!match) throw new Error(`Unsupported computed color: ${value}`);
  const parts = match[1]
    .split(/[,\s/]+/)
    .filter((part) => part.length > 0)
    .map(Number);
  if (parts.length < 3 || parts.some((part) => !Number.isFinite(part))) {
    throw new Error(`Unsupported computed color: ${value}`);
  }
  const [r, g, b] = parts;
  return { r, g, b, a: parts.length > 3 ? parts[3] : 1 };
}

/** WCAG 2.1 relative luminance. */
function relativeLuminance({ r, g, b }: Rgba): number {
  const channel = (raw: number) => {
    const srgb = raw / 255;
    return srgb <= 0.03928 ? srgb / 12.92 : ((srgb + 0.055) / 1.055) ** 2.4;
  };
  return 0.2126 * channel(r) + 0.7152 * channel(g) + 0.0722 * channel(b);
}

function contrastRatio(a: Rgba, b: Rgba): number {
  const first = relativeLuminance(a);
  const second = relativeLuminance(b);
  const lighter = Math.max(first, second);
  const darker = Math.min(first, second);
  return (lighter + 0.05) / (darker + 0.05);
}

/**
 * Route control handed back to a test: the summary and the principal-usage
 * payload can each be swapped between fetches, and every range the page asked
 * for is recorded in order — so the recorded length doubles as a fetch count —
 * letting a range switch or a polling refresh be confirmed instead of assumed.
 */
type OverviewFixtures = {
  summaryRanges: () => readonly string[];
  principalUsageRanges: () => readonly string[];
  principalUsageProjections: () => readonly string[];
  poolHistoryPointLimits: () => readonly string[];
  serveSummary: (next: DashboardSummaryResponse) => void;
  servePrincipalUsage: (next: DashboardUsageResponse) => void;
  servePoolHistory: (next: PoolHistoryResponse) => void;
};

async function installOverviewFixtures(page: Page): Promise<OverviewFixtures> {
  let servedSummary: DashboardSummaryResponse = summary;
  let servedPrincipalUsage: DashboardUsageResponse = principalUsage;
  let servedPoolHistory: PoolHistoryResponse = {
    now_unix_secs: Math.floor(Date.now() / 1000),
    windows: [],
  };
  const summaryRanges: string[] = [];
  // The Overview issues exactly one usage query: the principal grouping.
  const principalUsageRanges: string[] = [];
  const principalUsageProjections: string[] = [];
  const poolHistoryPointLimits: string[] = [];

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
    if (pathname === '/admin/v1/principals') {
      return json(200, {
        principals: [{ id: 'principal-alpha', name: 'Principal Alpha' }],
      });
    }
    if (pathname === '/admin/v1/upstreams') {
      return json(200, { upstreams: [] });
    }
    if (pathname === '/admin/v1/dashboard/summary') {
      summaryRanges.push(url.searchParams.get('range') ?? '');
      return json(200, servedSummary);
    }
    if (pathname === '/admin/v1/dashboard/usage') {
      principalUsageRanges.push(url.searchParams.get('range') ?? '');
      principalUsageProjections.push(
        url.searchParams.get('projection') ?? 'full',
      );
      return json(200, servedPrincipalUsage);
    }
    if (pathname === '/admin/v1/events/recent') {
      return json(200, { events: [], observed: true, count: 0, limit: 200 });
    }
    if (pathname === '/admin/v1/subscription-quotas/aggregate') {
      return json(200, {
        now_unix_secs: TIMESTAMPS[2],
        window_anchor_unix_secs: TIMESTAMPS[0],
        max_staleness_secs: 300,
        upstream_count: 0,
        windows: [],
        caveats: [],
      });
    }
    if (pathname === '/admin/v1/subscription-quotas/pool-history') {
      poolHistoryPointLimits.push(
        url.searchParams.get('max_points_per_series') ?? '',
      );
      return json(200, servedPoolHistory);
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
    summaryRanges: () => [...summaryRanges],
    principalUsageRanges: () => [...principalUsageRanges],
    principalUsageProjections: () => [...principalUsageProjections],
    poolHistoryPointLimits: () => [...poolHistoryPointLimits],
    serveSummary: (next) => {
      servedSummary = next;
    },
    servePrincipalUsage: (next) => {
      servedPrincipalUsage = next;
    },
    servePoolHistory: (next) => {
      servedPoolHistory = next;
    },
  };
}

/**
 * Re-boots the Overview in `theme` through the app's real startup path:
 * `initTheme()` reads `cclb.theme` before React mounts, so the value has to be
 * stored before the load that should honor it. Needs a loaded same-origin page.
 */
async function bootOverviewWithTheme(page: Page, theme: ThemeChoice) {
  await page.evaluate((next) => {
    window.localStorage.setItem('cclb.theme', next);
  }, theme);
  await page.reload();
  await expect(page.getByTestId('overview-kpi-request-rate')).toBeVisible();
  await expect
    .poll(() => page.evaluate(() => document.documentElement.dataset.theme))
    .toBe(theme);
}

/** Shapes the cache-miss wrapper is allowed to draw itself with. */
const SECONDARY_SHAPES = 'path, polyline, line, circle';

/**
 * Geometry the cache-miss wrapper actually paints over the tokens sparkline,
 * in CSS pixels and measured against the chart it is laid over. The wrapper
 * carries the test id while the drawing lives in its `<path>`/`<circle>`
 * children, so the union of those children is what a reader can see.
 */
async function readTokensSecondaryGeometry(page: Page) {
  const wrapper = page.getByTestId('overview-kpi-secondary-tokens');
  await expect(wrapper).toHaveCount(1);
  return wrapper.evaluate((element, shapes) => {
    const nodes = shapes.split(', ').includes(element.tagName.toLowerCase())
      ? [element]
      : Array.from(element.querySelectorAll(shapes));
    const chart = element
      .closest('[data-testid="overview-kpi-chart-tokens"]')
      ?.getBoundingClientRect();
    let left = Number.POSITIVE_INFINITY;
    let right = Number.NEGATIVE_INFINITY;
    let top = Number.POSITIVE_INFINITY;
    let bottom = Number.NEGATIVE_INFINITY;
    let drawn = 0;
    for (const node of nodes) {
      const rect = node.getBoundingClientRect();
      // A shape with no geometry reports an empty rect at the origin; folding
      // it into the union would stretch the union across the whole viewport.
      if (rect.width === 0 && rect.height === 0) continue;
      drawn += 1;
      left = Math.min(left, rect.left);
      right = Math.max(right, rect.right);
      top = Math.min(top, rect.top);
      bottom = Math.max(bottom, rect.bottom);
    }
    const style = nodes[0] ? window.getComputedStyle(nodes[0]) : null;
    return {
      shapeCount: nodes.length,
      drawn,
      width: drawn === 0 ? 0 : right - left,
      height: drawn === 0 ? 0 : bottom - top,
      chartWidth: chart?.width ?? 0,
      // Line segments carry the series color on `stroke`; a lone marker for an
      // isolated bucket carries it on `fill`.
      paint:
        style == null
          ? ''
          : style.stroke !== 'none' && style.stroke !== ''
            ? style.stroke
            : style.fill,
    };
  }, SECONDARY_SHAPES);
}

/**
 * The cache-miss line drawn over the tokens sparkline. The fixture's last
 * bucket is idle — no prompt tokens at all — so the series has to stop at the
 * middle bucket: a line interpolated across that gap would instead stretch
 * across the full chart width.
 */
async function expectTokensSecondarySeries(page: Page) {
  const geometry = await readTokensSecondaryGeometry(page);
  expect(geometry.shapeCount).toBeGreaterThan(0);
  expect(geometry.drawn).toBeGreaterThan(0);
  expect(geometry.chartWidth).toBeGreaterThan(0);
  expect(geometry.height).toBeGreaterThan(0);
  const spannedFraction = geometry.width / geometry.chartWidth;
  expect(spannedFraction).toBeGreaterThan(0.35);
  expect(spannedFraction).toBeLessThan(0.75);
  return geometry;
}

async function expectOverviewSummary(page: Page) {
  await expect(page.getByText(CACHE_MISS_AVG)).toBeVisible();
  await expect(page.getByTestId('top-principal-row')).toContainText(
    PRINCIPAL_CACHE_HIT,
  );
  return expectTokensSecondarySeries(page);
}

async function expectNoHorizontalOverflow(page: Page) {
  const hasHorizontalOverflow = await page.evaluate(
    () =>
      document.documentElement.scrollWidth >
      document.documentElement.clientWidth,
  );
  expect(hasHorizontalOverflow).toBe(false);
}

async function hoverChartCenter(page: Page, chartId: KpiChartId) {
  const chart = page.getByTestId(`overview-kpi-chart-${chartId}`);
  await expect(chart).toBeVisible();
  await chart.scrollIntoViewIfNeeded();
  const box = await chart.boundingBox();
  if (!box) throw new Error(`KPI chart ${chartId} has no bounding box`);
  await page.mouse.move(box.x + box.width / 2, box.y + box.height / 2);
}

async function expectSynchronizedTooltips(
  page: Page,
  options?: { readonly cacheMissRow?: string; readonly timeout?: number },
) {
  const expectedTimestamp = tooltipTimestamp(TIMESTAMPS[1]);
  const rows = expectedTooltipRows(options?.cacheMissRow ?? CACHE_MISS_ROW);
  for (const chartId of KPI_CHART_IDS) {
    const tooltip = page.getByTestId(`overview-kpi-tooltip-${chartId}`);
    await expect(tooltip).toBeVisible({ timeout: options?.timeout });
    // Exact and ordered, row by row: a different bucket, a changed formatter,
    // or an extra/missing line fails here instead of passing a substring.
    await expect(tooltip.locator('span')).toHaveText(
      [expectedTimestamp, ...rows[chartId]],
      { timeout: options?.timeout },
    );
  }
}

/** No tooltip is on screen — asserted without touching the pointer. */
async function expectTooltipsHidden(page: Page) {
  for (const chartId of KPI_CHART_IDS) {
    await expect(
      page.getByTestId(`overview-kpi-tooltip-${chartId}`),
    ).toHaveCount(0);
  }
}

async function expectTooltipsDismissed(page: Page) {
  await page.mouse.move(0, 0);
  await expectTooltipsHidden(page);
}

type RangeChoice = '1h' | '6h' | '24h' | '7d';

/**
 * Flips the range toggle without moving the pointer, so what follows is about
 * the range change and not about the mouse wandering off the chart.
 */
async function selectRange(page: Page, range: RangeChoice) {
  const control = page
    .getByLabel('Time range')
    .getByText(range, { exact: true });
  await expect(control).toBeVisible();
  await control.dispatchEvent('click');
}

/**
 * Computed colors of the open tokens tooltip: the themed surface it paints and
 * the color of every row a reader has to make out on top of it. Rows are found
 * by their exact text, so the cache-miss line — tinted apart from the rest, and
 * therefore the row that can quietly turn unreadable — is measured on itself
 * instead of on the tooltip as a whole.
 */
async function readTokensTooltipColors(page: Page) {
  const measured = await page.getByTestId('overview-kpi-tooltip-tokens')
    .evaluate(
      (element, rows) => {
        const spans = Array.from(element.querySelectorAll('span'));
        return {
          background: window.getComputedStyle(element).backgroundColor,
          rows: rows.map((text) => {
            const row = spans.find((span) => span.textContent?.trim() === text);
            if (!row) throw new Error(`Tokens tooltip has no "${text}" row`);
            return window.getComputedStyle(row).color;
          }),
        };
      },
      [TOKENS_VALUE_ROW, CACHE_MISS_ROW],
    );
  return {
    background: parseCssColor(measured.background),
    primary: parseCssColor(measured.rows[0]),
    cacheMiss: parseCssColor(measured.rows[1]),
  };
}

/**
 * Figures the open cost breakdown shows, in order. The color dots and the
 * footer spacer are empty spans, so they are dropped instead of padding the
 * expectation with blanks.
 */
async function readPrincipalCostValues(page: Page) {
  return page
    .getByTestId('top-principal-cost-details')
    .locator('span')
    .evaluateAll((nodes) =>
      nodes
        .map((node) => node.textContent?.trim() ?? '')
        .filter((text) => text.length > 0),
    );
}

/**
 * Category and declared width share of every segment drawn inside the filled
 * meter. Widths are rounded to two decimals: the proportions are the contract,
 * not however far the CSSOM chooses to serialize a repeating fraction.
 */
async function readPrincipalCostSegments(page: Page) {
  return page
    .getByTestId('top-principal-cost-segment')
    .evaluateAll((nodes) =>
      nodes.map((node) => {
        const element = node as HTMLElement;
        return {
          category: element.dataset.category ?? '',
          widthPct:
            Math.round(Number.parseFloat(element.style.width) * 100) / 100,
        };
      }),
    );
}

/**
 * Painted color of every segment. Colors are the whole encoding here, so each
 * one has to be opaque and distinct from its neighbours in the active theme.
 */
async function expectPrincipalCostPalette(page: Page) {
  const painted = await page
    .getByTestId('top-principal-cost-segment')
    .evaluateAll((nodes) =>
      nodes.map((node) => window.getComputedStyle(node).backgroundColor),
    );
  expect(painted).toHaveLength(PRINCIPAL_COST_SEGMENTS.length);
  expect(new Set(painted).size).toBe(painted.length);
  for (const color of painted) expect(parseCssColor(color).a).toBe(1);
}

/** Opens the principal cost breakdown by pointer, as a reader would. */
async function hoverPrincipalCostMeter(page: Page) {
  const trigger = page.getByTestId('top-principal-cost-trigger');
  await expect(trigger).toBeVisible();
  await trigger.scrollIntoViewIfNeeded();
  await trigger.hover();
  await expect(page.getByTestId('top-principal-cost-details')).toBeVisible();
}

test.describe('Overview KPI hover', () => {
  test('synchronizes KPI details in the dark and light themes', async ({
    page,
  }) => {
    const fixtures = await installOverviewFixtures(page);
    await page.setViewportSize(DESKTOP_VIEWPORT);
    await page.goto('/');
    // Boot once so `cclb.theme` can be stored before each themed reload.
    await expect(page.getByTestId('overview-kpi-request-rate')).toBeVisible();
    await expect
      .poll(() => fixtures.principalUsageProjections())
      .toContain('totals');
    expect(
      fixtures.principalUsageProjections().every((value) => value === 'totals'),
    ).toBe(true);
    await expect.poll(() => fixtures.poolHistoryPointLimits()).toContain('1000');

    const surfaceLuminance: number[] = [];
    for (const theme of THEMES) {
      await bootOverviewWithTheme(page, theme);
      await expectOverviewSummary(page);
      await expectNoHorizontalOverflow(page);
      // The principal cost bar carries the same category palette in either
      // theme: colors are the encoding, so they have to survive the re-skin.
      await expectPrincipalCostPalette(page);

      await hoverChartCenter(page, 'request-rate');
      await expectSynchronizedTooltips(page);
      // The cache-miss line stays drawn beneath the open tooltip.
      const series = await expectTokensSecondarySeries(page);

      // The tooltip covers the sparkline it belongs to, so it only stays
      // readable while its surface is opaque and every row on it is legible.
      const colors = await readTokensTooltipColors(page);
      expect(colors.background.a).toBe(1);
      expect(
        contrastRatio(colors.primary, colors.background),
      ).toBeGreaterThanOrEqual(4.5);
      // The cache-miss row is tinted away from the body text, so it is
      // measured on its own: the chart's own stroke color is far too weak on
      // this surface in either theme.
      expect(
        contrastRatio(colors.cacheMiss, colors.background),
      ).toBeGreaterThanOrEqual(4.5);
      // ...and it earns that contrast from a themed text color, not by
      // repainting the series line into the same dull tone.
      expect(series.paint).toMatch(/^rgba?\(/);
      const stroke = parseCssColor(series.paint);
      expect([
        colors.cacheMiss.r,
        colors.cacheMiss.g,
        colors.cacheMiss.b,
      ]).not.toEqual([stroke.r, stroke.g, stroke.b]);
      surfaceLuminance.push(relativeLuminance(colors.background));

      await expectTooltipsDismissed(page);
    }

    // Each theme re-skins the chart overlay instead of only swapping the root
    // attribute: the dark surface has to be darker than the light one.
    const [darkLuminance, lightLuminance] = surfaceLuminance;
    expect(darkLuminance).toBeLessThan(lightLuminance);
  });

  test('drops the shared hover when the range or the bucket count changes', async ({
    page,
  }) => {
    const fixtures = await installOverviewFixtures(page);
    await page.setViewportSize(DESKTOP_VIEWPORT);
    await page.goto('/');
    await expect(page.getByTestId('overview-kpi-request-rate')).toBeVisible();

    await hoverChartCenter(page, 'tokens');
    await expectSynchronizedTooltips(page);

    await selectRange(page, '6h');
    await expect.poll(() => fixtures.summaryRanges()).toContain('6h');
    // Every range is answered with the same buckets and the pointer never
    // left the chart, so only an explicit reset can take the tooltips down.
    await expectTooltipsHidden(page);

    await hoverChartCenter(page, 'tokens');
    await expectSynchronizedTooltips(page);

    // Buckets drain away under an open tooltip: the tiles survive, the charts
    // and their tooltips leave with the data.
    fixtures.serveSummary(emptySummary);
    await selectRange(page, '1h');
    await expect.poll(() => fixtures.summaryRanges()).toContain('1h');
    await expect(page.getByTestId('overview-kpi-chart-tokens')).toHaveCount(0);
    await expectTooltipsHidden(page);
    await expect(page.getByTestId('overview-kpi-tokens')).toBeVisible();

    // Buckets come back: hovering still resolves to the same shared bucket
    // across all five charts.
    fixtures.serveSummary(summary);
    await selectRange(page, '7d');
    await expect.poll(() => fixtures.summaryRanges()).toContain('7d');
    await expect(page.getByTestId('overview-kpi-chart-tokens')).toBeVisible();
    await hoverChartCenter(page, 'tokens');
    await expectSynchronizedTooltips(page);
    await expectTooltipsDismissed(page);
  });

  test('refreshes the cache metrics on its own polling cycle', async ({
    page,
  }) => {
    // Two real poll cycles of waiting on top of a cold dev-server boot.
    test.setTimeout(60_000);
    const fixtures = await installOverviewFixtures(page);
    await page.setViewportSize(DESKTOP_VIEWPORT);
    await page.goto('/');
    await expect(page.getByTestId('overview-kpi-request-rate')).toBeVisible();

    // The cache figures the first payload puts on screen, with the tokens
    // tooltip left open over the middle bucket.
    const staleSeries = await expectOverviewSummary(page);
    await hoverChartCenter(page, 'tokens');
    await expectSynchronizedTooltips(page);
    const stalePrincipalRow = await page
      .getByTestId('top-principal-row')
      .innerText();

    // Parked on this document: a reload would take it with the stale numbers,
    // so finding it later proves the fresh numbers landed in place.
    await page.evaluate(() => {
      (window as ProbedWindow).cclbPollProbe = 'alive';
    });
    const fetchesBefore = {
      summary: fixtures.summaryRanges().length,
      principalUsage: fixtures.principalUsageRanges().length,
    };

    // Both payloads change under a page that is asked to do nothing at all: no
    // reload, no range switch, no refetch, no focus event, and the pointer stays
    // where it was. Only the poll can carry this onto the screen.
    fixtures.serveSummary(refreshedSummary);
    fixtures.servePrincipalUsage(refreshedPrincipalUsage);

    await expect(page.getByText(REFRESHED_CACHE_MISS_AVG)).toBeVisible({
      timeout: POLL_UPDATE_TIMEOUT_MS,
    });
    await expect(page.getByTestId('top-principal-row')).toContainText(
      REFRESHED_PRINCIPAL_CACHE_HIT,
      { timeout: POLL_UPDATE_TIMEOUT_MS },
    );
    // The tooltip stayed open across the refresh, so it has to be the same
    // bucket — same timestamp, same `Tokens 900` — with a new cache-miss row.
    await expectSynchronizedTooltips(page, {
      cacheMissRow: REFRESHED_CACHE_MISS_ROW,
      timeout: POLL_UPDATE_TIMEOUT_MS,
    });

    // The refreshed last bucket reports a ratio, so the cache-miss line runs the
    // full chart instead of stopping halfway across it.
    await expect
      .poll(
        async () => {
          const geometry = await readTokensSecondaryGeometry(page);
          return geometry.chartWidth > 0
            ? geometry.width / geometry.chartWidth
            : 0;
        },
        { timeout: POLL_UPDATE_TIMEOUT_MS },
      )
      .toBeGreaterThan(0.9);
    // ...and it is re-plotted, not just extended: 80/80/50 climbs higher up the
    // band than the 40/50 the first payload drew.
    const liveSeries = await readTokensSecondaryGeometry(page);
    expect(liveSeries.height).toBeGreaterThan(staleSeries.height);

    // Only the cache hit moved in the principal row. The refreshed payload keeps
    // its requests, tokens and cost, so anything else changing here means the
    // row re-rendered off something other than a like-for-like refresh.
    expect(await page.getByTestId('top-principal-row').innerText()).toBe(
      stalePrincipalRow.replace(
        PRINCIPAL_CACHE_HIT,
        REFRESHED_PRINCIPAL_CACHE_HIT,
      ),
    );

    // Polling is what delivered it: both endpoints were fetched again, every
    // fetch asked for the range the page opened on, and the document that
    // rendered the stale figures is the one now showing the fresh ones.
    expect(fixtures.summaryRanges().length).toBeGreaterThan(
      fetchesBefore.summary,
    );
    expect(fixtures.principalUsageRanges().length).toBeGreaterThan(
      fetchesBefore.principalUsage,
    );
    expect(
      [
        ...fixtures.summaryRanges(),
        ...fixtures.principalUsageRanges(),
      ].filter((range) => range !== '24h'),
    ).toEqual([]);
    expect(
      await page.evaluate(() => (window as ProbedWindow).cclbPollProbe),
    ).toBe('alive');

    await expectTooltipsDismissed(page);
  });

  test('breaks principal cost into categories and repolls them under an open breakdown', async ({
    page,
  }) => {
    // Two real poll cycles of waiting on top of a cold dev-server boot.
    test.setTimeout(60_000);
    const fixtures = await installOverviewFixtures(page);
    await page.setViewportSize(DESKTOP_VIEWPORT);
    await page.goto('/');
    await expect(page.getByTestId('overview-kpi-request-rate')).toBeVisible();

    const meter = page.getByTestId('top-principal-cost-meter');
    await expect(meter).toBeVisible();
    await expect(meter).toHaveAttribute('aria-label', 'Principal Alpha cost');
    // One of the two buckets recorded components, so the bar says so instead of
    // spreading the whole $3.00 across the categories.
    await expect(meter).toHaveAttribute('data-cost-components', 'partial');
    expect(await readPrincipalCostSegments(page)).toEqual(
      PRINCIPAL_COST_SEGMENTS,
    );

    const trigger = page.getByTestId('top-principal-cost-trigger');
    await trigger.focus();
    await expect(trigger).toBeFocused();
    await expect(
      page.getByTestId('top-principal-cost-details'),
    ).toBeVisible();
    await page.keyboard.press('Escape');
    await expect(
      page.getByTestId('top-principal-cost-details'),
    ).toHaveCount(0);

    await hoverPrincipalCostMeter(page);
    await expect
      .poll(() => readPrincipalCostValues(page))
      .toEqual(PRINCIPAL_COST_VALUES);

    const stalePrincipalRow = await page
      .getByTestId('top-principal-row')
      .innerText();
    // Parked on this document: a reload would take it with the stale numbers.
    await page.evaluate(() => {
      (window as ProbedWindow).cclbPollProbe = 'alive';
    });
    const fetchesBefore = fixtures.principalUsageRanges().length;

    // The split changes at the same $3.00 total under a page that is asked to
    // do nothing: no reload, no range switch, no refetch, and the pointer stays
    // on the meter. Only the poll can carry this into the open breakdown.
    fixtures.servePrincipalUsage(refreshedPrincipalUsage);

    await expect
      .poll(() => readPrincipalCostValues(page), {
        timeout: POLL_UPDATE_TIMEOUT_MS,
      })
      .toEqual(REFRESHED_PRINCIPAL_COST_VALUES);
    // The bar was re-plotted with it: cache read empties out and cache create
    // 1h takes its place, which a memoized row would have missed.
    await expect
      .poll(() => readPrincipalCostSegments(page), {
        timeout: POLL_UPDATE_TIMEOUT_MS,
      })
      .toEqual(REFRESHED_PRINCIPAL_COST_SEGMENTS);

    // Only the cache hit moved in the row itself: the refreshed payload keeps
    // its requests, tokens and cost, and no per-category figure leaks into the
    // row text — those live in the breakdown and in the meter's value text.
    expect(await page.getByTestId('top-principal-row').innerText()).toBe(
      stalePrincipalRow.replace(
        PRINCIPAL_CACHE_HIT,
        REFRESHED_PRINCIPAL_CACHE_HIT,
      ),
    );
    expect(await meter.getAttribute('aria-valuetext')).toBe(
      'Total $3.0000; 100.0% of the largest principal; Input $0.6000, Output $0.2000, Cache create 5m $0.1000, Cache create 1h $0.1000, Cache read $0.0000, Unattributed $2.0000',
    );
    expect(fixtures.principalUsageRanges().length).toBeGreaterThan(
      fetchesBefore,
    );
    expect(
      await page.evaluate(() => (window as ProbedWindow).cclbPollProbe),
    ).toBe('alive');

    await page.mouse.move(0, 0);
    await expect(
      page.getByTestId('top-principal-cost-details'),
    ).toHaveCount(0);
  });

  test('keeps the capped 7d peak visible and the legend exact', async ({
    page,
  }) => {
    const fixtures = await installOverviewFixtures(page);
    const nowUnixSecs = Math.floor(Date.now() / 1000);
    const latestPoint = (utilizationPercent: number) => ({
      snapshot_at_unix_secs: nowUnixSecs - 60,
      utilization: utilizationPercent / 100,
      utilization_percent: utilizationPercent,
      contributing_upstreams: 1,
      eligible_upstreams: 1,
      stale_upstreams: 0,
      max_observed_at_unix_millis: (nowUnixSecs - 60) * 1000,
    });
    fixtures.servePoolHistory({
      now_unix_secs: nowUnixSecs,
      windows: [
        {
          window: '5h',
          latest: latestPoint(18),
          series: [
            {
              snapshot_at_unix_secs: nowUnixSecs - 60,
              utilization_percent: 20,
            },
          ],
        },
        {
          window: '7d',
          latest: latestPoint(35),
          series: [
            {
              snapshot_at_unix_secs: nowUnixSecs - 6 * 24 * 60 * 60,
              utilization_percent: 40,
            },
            {
              snapshot_at_unix_secs: nowUnixSecs - 60,
              utilization_percent: 38,
            },
          ],
        },
        {
          window: '7d_fable',
          latest: latestPoint(109),
          series: [
            {
              snapshot_at_unix_secs: nowUnixSecs - 6 * 24 * 60 * 60,
              utilization_percent: 137,
            },
            {
              snapshot_at_unix_secs: nowUnixSecs - 60,
              utilization_percent: 110,
            },
          ],
        },
      ],
    });

    await page.setViewportSize(DESKTOP_VIEWPORT);
    await page.goto('/');
    await expect(page.getByTestId('pool-quota-card')).toBeVisible();
    await selectRange(page, '7d');
    await expect
      .poll(() => fixtures.summaryRanges())
      .toContain('7d');

    const chart = page.getByTestId('pool-quota-chart-slot');
    await expect(chart.getByText('140%', { exact: true })).toBeVisible();
    await expect(
      page.getByTestId('pool-quota-legend-slot').filter({
        hasText: 'Fable · 109%',
      }),
    ).toBeVisible();
    await expect(
      page.getByTestId('pool-quota-legend-slot').filter({
        hasText: '7d · 35%',
      }),
    ).toBeVisible();
  });

  test('keeps the mobile Overview free of horizontal overflow', async ({
    page,
  }) => {
    await installOverviewFixtures(page);
    await page.setViewportSize(MOBILE_VIEWPORT);
    await page.goto('/');
    await expect(page.getByTestId('overview-kpi-request-rate')).toBeVisible();

    for (const theme of THEMES) {
      await bootOverviewWithTheme(page, theme);
      await expect(page.getByText(CACHE_MISS_AVG)).toBeVisible();
      await expectNoHorizontalOverflow(page);

      // The narrow layout still draws the cache-miss line, and an open tooltip
      // over a 375px card must not push the page sideways.
      await expectTokensSecondarySeries(page);
      await hoverChartCenter(page, 'tokens');
      await expectSynchronizedTooltips(page);
      await expectNoHorizontalOverflow(page);
      await expectTooltipsDismissed(page);
    }
  });
});
