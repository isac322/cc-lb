import { expect, type Page, test } from '@playwright/test';
import type {
  DashboardSummaryResponse,
  DashboardUsageResponse,
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
    avg_proxy_setup_ms: 0,
    avg_shape_ms: 0,
    avg_sign_ms: 0,
    avg_upstream_ttfb_ms: 0,
    avg_upstream_body_ms: 0,
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

/** Tooltip rows below the timestamp for the hovered (middle) fixture bucket. */
const TOKENS_VALUE_ROW = 'Tokens 900';
const CACHE_MISS_ROW = 'Cache miss 50.0%';

const EXPECTED_TOOLTIP_ROWS: Record<KpiChartId, readonly string[]> = {
  'request-rate': ['Req/s 3'],
  tokens: [TOKENS_VALUE_ROW, CACHE_MISS_ROW],
  cost: ['Equiv $ $2.50'],
  latency: ['Avg latency 200ms'],
  'error-rate': ['Err rate 10.00%'],
};

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
 * Route control handed back to a test: the summary payload can be swapped
 * between fetches, and every range the page asked for is recorded so a range
 * switch can be confirmed instead of assumed.
 */
type OverviewFixtures = {
  summaryRanges: () => readonly string[];
  serveSummary: (next: DashboardSummaryResponse) => void;
};

async function installOverviewFixtures(page: Page): Promise<OverviewFixtures> {
  let servedSummary: DashboardSummaryResponse = summary;
  const summaryRanges: string[] = [];

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
    if (pathname === '/admin/dashboard/summary') {
      summaryRanges.push(url.searchParams.get('range') ?? '');
      return json(200, servedSummary);
    }
    if (pathname === '/admin/usage') {
      return json(200, principalUsage);
    }
    if (pathname === '/admin/events/recent') {
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
      return json(200, { now_unix_secs: TIMESTAMPS[2], windows: [] });
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
    summaryRanges: () => [...summaryRanges],
    serveSummary: (next) => {
      servedSummary = next;
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
  await expect(page.getByText('Avg cache miss 44.4%')).toBeVisible();
  await expect(page.getByTestId('top-principal-row')).toContainText(
    '83.3% cache hit',
  );
  await expectTokensSecondarySeries(page);
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

async function expectSynchronizedTooltips(page: Page) {
  const expectedTimestamp = tooltipTimestamp(TIMESTAMPS[1]);
  for (const chartId of KPI_CHART_IDS) {
    const tooltip = page.getByTestId(`overview-kpi-tooltip-${chartId}`);
    await expect(tooltip).toBeVisible();
    // Exact and ordered, row by row: a different bucket, a changed formatter,
    // or an extra/missing line fails here instead of passing a substring.
    await expect(tooltip.locator('span')).toHaveText([
      expectedTimestamp,
      ...EXPECTED_TOOLTIP_ROWS[chartId],
    ]);
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

test.describe('Overview KPI hover', () => {
  test('synchronizes KPI details in the dark and light themes', async ({
    page,
  }) => {
    await installOverviewFixtures(page);
    await page.setViewportSize(DESKTOP_VIEWPORT);
    await page.goto('/');
    // Boot once so `cclb.theme` can be stored before each themed reload.
    await expect(page.getByTestId('overview-kpi-request-rate')).toBeVisible();

    const surfaceLuminance: number[] = [];
    for (const theme of THEMES) {
      await bootOverviewWithTheme(page, theme);
      await expectOverviewSummary(page);
      await expectNoHorizontalOverflow(page);

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

  test('keeps the mobile Overview free of horizontal overflow', async ({
    page,
  }) => {
    await installOverviewFixtures(page);
    await page.setViewportSize(MOBILE_VIEWPORT);
    await page.goto('/');
    await expect(page.getByTestId('overview-kpi-request-rate')).toBeVisible();

    for (const theme of THEMES) {
      await bootOverviewWithTheme(page, theme);
      await expect(page.getByText('Avg cache miss 44.4%')).toBeVisible();
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
