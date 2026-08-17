import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import type { ComponentType } from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type {
  DashboardSummaryResponse,
  DashboardUsageResponse,
  UsageBucket,
} from '../lib/api';
import * as queries from '../lib/queries';
import * as liveEvents from '../lib/useLiveEventStream';
import { PoolQuotaLegend, Route, TopPrincipalsCard, ValueTile } from './index';

vi.mock('../lib/queries', async () => {
  const actual = await vi.importActual<typeof queries>('../lib/queries');
  return {
    ...actual,
    usePrincipalNameMap: vi.fn(),
    useRecentEventsInfinite: vi.fn(),
    useSubscriptionQuotaAggregate: vi.fn(),
    useSubscriptionQuotaPoolHistory: vi.fn(),
    useSummary: vi.fn(),
    useUpstreamNameMap: vi.fn(),
    useUsage: vi.fn(),
  };
});

vi.mock('../lib/useLiveEventStream', () => ({
  useLiveEventStream: vi.fn(),
}));

const OverviewPage = Route.options.component as ComponentType;

const KPI_TIMESTAMPS = [
  Date.UTC(2026, 7, 17, 13, 30) / 1000,
  Date.UTC(2026, 7, 17, 14, 30) / 1000,
  Date.UTC(2026, 7, 17, 15, 30) / 1000,
] as const;

function kpiTooltipTimestamp(timestamp: number): string {
  const date = new Date(timestamp * 1000);
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')} ${String(date.getHours()).padStart(2, '0')}:${String(date.getMinutes()).padStart(2, '0')}`;
}

function tooltipRows(testId: string): string[] {
  return Array.from(
    screen.getByTestId(testId).querySelectorAll('span'),
    (row) => row.textContent ?? '',
  );
}

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

const SUMMARY_BUCKETS = [
  usageBucket(KPI_TIMESTAMPS[0], {
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
  usageBucket(KPI_TIMESTAMPS[1], {
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
  usageBucket(KPI_TIMESTAMPS[2], {
    request_count: 240,
    output_tokens: 30,
    virtual_cost_micros: 4_000_000,
    latency_ms_sum: 72_000,
    latency_count: 240,
  }),
];

const SUMMARY_FIXTURE: DashboardSummaryResponse = {
  range: '24h',
  step: 'minute',
  window_start_unix_secs: KPI_TIMESTAMPS[0],
  window_end_unix_secs: KPI_TIMESTAMPS[2] + 60,
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
  sparkline: { buckets: SUMMARY_BUCKETS },
  observed: true,
};

const PRINCIPAL_USAGE_FIXTURE: DashboardUsageResponse = {
  range: '24h',
  step: 'hour',
  group_by: 'principal',
  window_start_unix_secs: KPI_TIMESTAMPS[0],
  window_end_unix_secs: KPI_TIMESTAMPS[2] + 60,
  observed: true,
  series: [
    {
      key: 'principal-alpha',
      buckets: [
        usageBucket(KPI_TIMESTAMPS[0], {
          request_count: 10,
          input_tokens: 50,
          output_tokens: 100,
          cache_creation_input_tokens: 150,
          cache_read_input_tokens: 800,
          virtual_cost_micros: 1_000_000,
        }),
        usageBucket(KPI_TIMESTAMPS[1], {
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

function mockPendingOverviewQueries() {
  vi.mocked(queries.useSummary).mockReturnValue({
    data: undefined,
    isPending: true,
  } as never);
  vi.mocked(queries.useUsage).mockReturnValue({
    data: undefined,
    isPending: true,
    isPlaceholderData: false,
  } as never);
  vi.mocked(queries.useRecentEventsInfinite).mockReturnValue({
    data: { pages: [{ events: [] }] },
    fetchNextPage: vi.fn(),
    hasNextPage: false,
    isFetchingNextPage: false,
    isLoading: false,
  } as never);
  vi.mocked(queries.usePrincipalNameMap).mockReturnValue(new Map());
  vi.mocked(queries.useUpstreamNameMap).mockReturnValue(new Map());
  vi.mocked(queries.useSubscriptionQuotaAggregate).mockReturnValue({
    data: undefined,
    isPending: true,
    isPlaceholderData: false,
  } as never);
  vi.mocked(queries.useSubscriptionQuotaPoolHistory).mockReturnValue({
    data: undefined,
    isPending: true,
    isPlaceholderData: false,
  } as never);
  vi.mocked(liveEvents.useLiveEventStream).mockReturnValue({
    error: null,
    eventsMap: new Map(),
    forceReconnect: vi.fn(),
    lastActivityAt: null,
    lastCursor: null,
    malformedFrameCount: 0,
    permanentFailure: false,
    permanentFailureSince: null,
    reconnectAttempts: 0,
    status: 'idle',
    version: 0,
  });
}

function mockResolvedEmptyQuotaQueries() {
  vi.mocked(queries.useSubscriptionQuotaAggregate).mockReturnValue({
    data: { upstream_count: 0, windows: [] },
    isPending: false,
    isPlaceholderData: false,
  } as never);
  vi.mocked(queries.useSubscriptionQuotaPoolHistory).mockReturnValue({
    data: { windows: [] },
    isPending: false,
    isPlaceholderData: false,
  } as never);
}

function mockResolvedKpiQueries({
  zeroPromptDenominator = false,
}: {
  zeroPromptDenominator?: boolean;
} = {}) {
  mockPendingOverviewQueries();
  mockResolvedEmptyQuotaQueries();

  const summaryData = zeroPromptDenominator
    ? {
        ...SUMMARY_FIXTURE,
        totals: {
          ...SUMMARY_FIXTURE.totals,
          input_tokens: 0,
          cache_creation_input_tokens: 0,
          cache_read_input_tokens: 0,
        },
        sparkline: {
          buckets: SUMMARY_BUCKETS.map((bucket) => ({
            ...bucket,
            input_tokens: 0,
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 0,
          })),
        },
      }
    : SUMMARY_FIXTURE;
  const principalData = zeroPromptDenominator
    ? {
        ...PRINCIPAL_USAGE_FIXTURE,
        series: PRINCIPAL_USAGE_FIXTURE.series.map((series) => ({
          ...series,
          buckets: series.buckets.map((bucket) => ({
            ...bucket,
            input_tokens: 0,
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 0,
          })),
        })),
      }
    : PRINCIPAL_USAGE_FIXTURE;

  vi.mocked(queries.useSummary).mockReturnValue({
    data: summaryData,
    isPending: false,
  } as never);
  vi.mocked(queries.useUsage).mockReturnValue({
    data: principalData,
    isPending: false,
    isPlaceholderData: false,
  } as never);
  vi.mocked(queries.usePrincipalNameMap).mockReturnValue(
    new Map([['principal-alpha', 'Principal Alpha']]),
  );
}

beforeEach(() => {
  vi.clearAllMocks();
});

afterEach(() => {
  cleanup();
});

describe('PoolQuotaLegend', () => {
  it('renders Fable only when the selected range has Fable data', () => {
    const { rerender } = render(
      <PoolQuotaLegend
        latest={{ '5h': 20, '7d': 40, '7d_fable': null }}
        showFable={false}
      />,
    );
    expect(screen.queryByText(/Fable/)).toBeNull();

    rerender(
      <PoolQuotaLegend
        latest={{ '5h': 20, '7d': 40, '7d_fable': 28 }}
        showFable
      />,
    );
    expect(screen.getByText(/Fable/).textContent).toBe('Fable · 28%');
  });
});

describe('Overview loading geometry', () => {
  it('keeps ValueTile value, sub, and sparkline slots fixed', () => {
    const { container, rerender } = render(
      <ValueTile
        chartId="request-rate"
        chartLabel="Req/s"
        icon={<span />}
        label="requests"
        loading
        size="sm"
        spark={[]}
        sub="12 / 24h"
        value="12"
      />,
    );

    const tileClassName = container.firstElementChild?.className;
    const valueClassName = container.querySelector(
      '[data-slot="value"]',
    )?.className;
    const subClassName =
      container.querySelector('[data-slot="sub"]')?.className;
    const sparklineClassName = container.querySelector(
      '[data-slot="sparkline"]',
    )?.className;

    expect(
      container.querySelector('[data-slot="value"] .skeleton'),
    ).not.toBeNull();
    expect(
      container.querySelector('[data-slot="sub"] .skeleton'),
    ).not.toBeNull();
    expect(
      container.querySelector('[data-slot="sparkline"] .skeleton'),
    ).not.toBeNull();
    expect(valueClassName).toContain('h-5');
    expect(subClassName).toContain('h-4');
    expect(sparklineClassName).toContain('h-8');
    expect(screen.queryByText('12')).toBeNull();
    expect(screen.queryByText('12 / 24h')).toBeNull();

    rerender(
      <ValueTile
        chartId="request-rate"
        chartLabel="Req/s"
        icon={<span />}
        label="requests"
        loading={false}
        size="sm"
        spark={[]}
        sub="12 / 24h"
        value="12"
      />,
    );

    expect(container.firstElementChild?.className).toBe(tileClassName);
    expect(container.querySelector('[data-slot="value"]')?.className).toBe(
      valueClassName,
    );
    expect(container.querySelector('[data-slot="sub"]')?.className).toBe(
      subClassName,
    );
    expect(container.querySelector('[data-slot="sparkline"]')?.className).toBe(
      sparklineClassName,
    );
    expect(
      container.querySelector('[data-slot="sparkline"] .skeleton'),
    ).toBeNull();
    expect(screen.getByText('12')).toBeDefined();
    expect(screen.getByText('12 / 24h')).toBeDefined();
  });

  it('renders an isolated secondary value as a visible point', () => {
    render(
      <ValueTile
        chartId="isolated-secondary"
        chartLabel="Primary"
        icon={<span />}
        label="isolated secondary"
        secondary={{
          color: '#8b5cf6',
          format: String,
          label: 'Secondary',
          testId: 'overview-kpi-secondary-isolated',
        }}
        spark={[
          {
            secondaryValue: null,
            timestamp: KPI_TIMESTAMPS[0],
            value: 1,
          },
          {
            secondaryValue: 50,
            timestamp: KPI_TIMESTAMPS[1],
            value: 2,
          },
          {
            secondaryValue: null,
            timestamp: KPI_TIMESTAMPS[2],
            value: 3,
          },
        ]}
        value="2"
      />,
    );

    const secondary = screen.getByTestId('overview-kpi-secondary-isolated');
    const marker = secondary.querySelector('path[data-slot="secondary-point"]');
    expect(secondary.querySelectorAll('path')).toHaveLength(1);
    expect(marker?.getAttribute('d')).toBe('M 50.00,50.00 h 0.01');
  });

  it('keeps the principal card shell stable across loading, empty, and loaded states', () => {
    const { container, rerender } = render(
      <TopPrincipalsCard loading principals={[]} range="24h" />,
    );

    const cardClassName = screen
      .getByText('Top principals')
      .closest('.glass')?.className;
    const listClassName = container.querySelector(
      '[data-slot="principal-list"]',
    )?.className;
    const skeletonRows = screen.getAllByTestId('top-principal-skeleton-row');

    expect(skeletonRows).toHaveLength(5);
    for (const row of skeletonRows) {
      const skeletons = row.querySelectorAll('.skeleton');
      expect(row.className).toContain('min-h-[66px]');
      expect(row.className).toContain('px-3');
      expect(row.className).toContain('py-2');
      expect(skeletons).toHaveLength(5);
      expect(skeletons[0]?.className).toContain('h-5');
      expect(skeletons[1]?.className).toContain('h-3');
      expect(skeletons[2]?.className).toContain('h-1.5');
      expect(skeletons[3]?.className).toContain('h-5');
      expect(skeletons[4]?.className).toContain('h-3');
    }
    expect(listClassName).toContain('min-h-80');
    expect(screen.queryByText('Loading top principals…')).toBeNull();

    rerender(<TopPrincipalsCard loading={false} principals={[]} range="24h" />);

    expect(
      screen.getByText('Top principals').closest('.glass')?.className,
    ).toBe(cardClassName);
    expect(
      container.querySelector('[data-slot="principal-list"]')?.className,
    ).toBe(listClassName);
    expect(screen.getByText('No usage data')).toBeDefined();

    rerender(
      <TopPrincipalsCard
        loading={false}
        principals={[
          {
            cache_hit_ratio: null,
            cost_usd: 1.25,
            id: 'principal-1',
            max_cost: 1.25,
            name: 'Primary principal',
            primary_model: '—',
            requests: 12,
            share_pct: 100,
            tokens: 345,
          },
        ]}
        range="24h"
      />,
    );

    expect(
      screen.getByText('Top principals').closest('.glass')?.className,
    ).toBe(cardClassName);
    expect(
      container.querySelector('[data-slot="principal-list"]')?.className,
    ).toBe(listClassName);
    expect(screen.getByTestId('top-principal-row').className).toContain(
      'min-h-[66px]',
    );
    expect(screen.getByText('Primary principal')).toBeDefined();
  });

  it('renders pending Overview KPIs and principals without fake values or loading text', () => {
    mockPendingOverviewQueries();

    render(<OverviewPage />);

    for (const label of [
      'avg req/s',
      'tokens',
      'equiv $',
      'Avg latency',
      'err rate',
    ]) {
      const tile = screen.getByText(label).closest('.glass');
      expect(tile?.textContent).toBe(label);
      expect(
        tile?.querySelector('[data-slot="value"] .skeleton'),
      ).not.toBeNull();
      expect(
        tile?.querySelector('[data-slot="sparkline"] .skeleton'),
      ).not.toBeNull();
    }

    expect(screen.queryByText('Loading top principals…')).toBeNull();
    expect(screen.getAllByTestId('top-principal-skeleton-row')).toHaveLength(5);
  });

  it.each([
    ['aggregate', true, false],
    ['history', false, true],
  ])(
    'keeps Pool quota structured while the %s query is pending',
    (_queryName, aggregatePending, historyPending) => {
      mockPendingOverviewQueries();
      vi.mocked(queries.useSubscriptionQuotaAggregate).mockReturnValue({
        data: aggregatePending ? undefined : { upstream_count: 0, windows: [] },
        isPending: aggregatePending,
        isPlaceholderData: false,
      } as never);
      vi.mocked(queries.useSubscriptionQuotaPoolHistory).mockReturnValue({
        data: historyPending ? undefined : { windows: [] },
        isPending: historyPending,
        isPlaceholderData: false,
      } as never);

      render(<OverviewPage />);

      const card = screen.getByTestId('pool-quota-card');
      expect(card.getAttribute('aria-busy')).toBe('true');
      expect(card.textContent).not.toContain('no data');
      expect(card.textContent).not.toContain('No timeline data yet');

      const snapshotSlots = card.querySelectorAll(
        '[data-testid="pool-quota-snapshot-slot"]',
      );
      expect(snapshotSlots).toHaveLength(3);
      expect(
        Array.from(snapshotSlots, (slot) => slot.textContent?.trim()),
      ).toEqual(['5h pool', '7d pool', '7d (Fable) pool']);
      for (const slot of snapshotSlots) {
        expect(slot.className).toContain('min-h-[56px]');
        expect(slot.querySelectorAll('.skeleton')).toHaveLength(2);
      }

      const legendSlots = card.querySelectorAll(
        '[data-testid="pool-quota-legend-slot"]',
      );
      expect(legendSlots).toHaveLength(3);
      expect(
        Array.from(legendSlots, (slot) => slot.textContent?.trim()),
      ).toEqual(['5h', '7d', 'Fable']);
      for (const slot of legendSlots) {
        expect(slot.querySelector('.skeleton')).not.toBeNull();
      }

      const chartSlot = screen.getByTestId('pool-quota-chart-slot');
      expect(chartSlot.className).toContain('min-h-64');
      expect(chartSlot.querySelector('.skeleton')).not.toBeNull();
      const chartHeader = chartSlot.previousElementSibling;
      expect(chartHeader?.className).toContain('flex-col');
      expect(chartHeader?.className).toContain('sm:flex-row');
    },
  );

  it('keeps all three Pool quota slots mounted when pending resolves to legitimate no-data', () => {
    mockPendingOverviewQueries();

    const { rerender } = render(<OverviewPage />);
    const pendingCard = screen.getByTestId('pool-quota-card');
    const pendingSnapshotSlots = Array.from(
      pendingCard.querySelectorAll('[data-testid="pool-quota-snapshot-slot"]'),
    );
    const pendingChartSlot = screen.getByTestId('pool-quota-chart-slot');

    mockResolvedEmptyQuotaQueries();
    rerender(<OverviewPage />);

    const resolvedCard = screen.getByTestId('pool-quota-card');
    const resolvedSnapshotSlots = Array.from(
      resolvedCard.querySelectorAll('[data-testid="pool-quota-snapshot-slot"]'),
    );
    expect(resolvedCard).toBe(pendingCard);
    expect(resolvedCard.getAttribute('aria-busy')).toBe('false');
    for (let index = 0; index < resolvedSnapshotSlots.length; index += 1) {
      expect(resolvedSnapshotSlots[index]).toBe(pendingSnapshotSlots[index]);
    }
    expect(resolvedSnapshotSlots).toHaveLength(3);
    expect(
      resolvedCard.querySelectorAll('[data-testid="pool-quota-legend-slot"]'),
    ).toHaveLength(3);
    expect(screen.getByTestId('pool-quota-chart-slot')).toBe(pendingChartSlot);
    expect(resolvedCard.querySelectorAll('.skeleton')).toHaveLength(0);
    expect(resolvedCard.textContent?.match(/no data/g)).toHaveLength(3);
    expect(resolvedCard.textContent).toContain(
      'No timeline data yet for this range',
    );
  });

  it('keeps the Recent Requests scroll box height fixed while data resolves', () => {
    mockPendingOverviewQueries();
    vi.mocked(queries.useRecentEventsInfinite).mockReturnValue({
      data: undefined,
      fetchNextPage: vi.fn(),
      hasNextPage: false,
      isFetchingNextPage: false,
      isLoading: true,
    } as never);

    const { container, rerender } = render(<OverviewPage />);
    const loadingScrollBox = container.querySelector('.scroll-fade-right');

    expect(loadingScrollBox?.className).toContain('h-[50vh]');
    expect(loadingScrollBox?.className).not.toContain('max-h-[50vh]');

    vi.mocked(queries.useRecentEventsInfinite).mockReturnValue({
      data: {
        pages: [
          {
            events: [
              {
                duration_ms: 10,
                model: 'test-model',
                request_id: 'request-1',
                status: 200,
                ts: 1,
              },
            ],
          },
        ],
      },
      fetchNextPage: vi.fn(),
      hasNextPage: false,
      isFetchingNextPage: false,
      isLoading: false,
    } as never);

    rerender(<OverviewPage />);

    expect(container.querySelector('.scroll-fade-right')?.className).toBe(
      loadingScrollBox?.className,
    );
    expect(screen.getByText('test-model')).toBeDefined();
  });
});

describe('Overview KPI details', () => {
  it('renders cache averages and synchronizes all KPI tooltips by bucket index', () => {
    mockResolvedKpiQueries();
    render(<OverviewPage />);

    expect(screen.getByText('Avg cache miss 44.4%')).toBeDefined();
    expect(screen.getByTestId('top-principal-row').textContent).toContain(
      '83.3% cache hit',
    );
    const secondary = screen.getByTestId('overview-kpi-secondary-tokens');
    const secondaryPaths = Array.from(secondary.querySelectorAll('path'));
    expect(secondaryPaths).toHaveLength(1);
    expect(secondaryPaths[0]?.getAttribute('data-slot')).toBe(
      'secondary-segment',
    );
    expect(secondaryPaths[0]?.getAttribute('d')).toBe(
      'M 0.00,59.20 L 50.00,50.00',
    );
    expect(screen.queryAllByTestId(/^overview-kpi-tooltip-/)).toHaveLength(0);

    const requestChart = screen.getByTestId('overview-kpi-chart-request-rate');
    vi.spyOn(requestChart, 'getBoundingClientRect').mockReturnValue({
      bottom: 42,
      height: 32,
      left: 10,
      right: 210,
      top: 10,
      width: 200,
      x: 10,
      y: 10,
      toJSON: () => ({}),
    });
    fireEvent.mouseMove(requestChart, { clientX: -10, clientY: 26 });
    expect(tooltipRows('overview-kpi-tooltip-request-rate')).toEqual([
      kpiTooltipTimestamp(KPI_TIMESTAMPS[0]),
      'Req/s 2',
    ]);

    fireEvent.mouseMove(requestChart, { clientX: 230, clientY: 26 });
    expect(tooltipRows('overview-kpi-tooltip-request-rate')).toEqual([
      kpiTooltipTimestamp(KPI_TIMESTAMPS[2]),
      'Req/s 4',
    ]);

    fireEvent.mouseMove(requestChart, { clientX: 110, clientY: 26 });
    const tooltipTimestamp = kpiTooltipTimestamp(KPI_TIMESTAMPS[1]);
    const chartIds = [
      'request-rate',
      'tokens',
      'cost',
      'latency',
      'error-rate',
    ] as const;

    for (const chartId of chartIds) {
      expect(screen.getByTestId(`overview-kpi-${chartId}`)).toBeDefined();
      expect(screen.getByTestId(`overview-kpi-chart-${chartId}`)).toBeDefined();
    }
    expect(screen.queryAllByTestId(/^overview-kpi-tooltip-/)).toHaveLength(5);

    expect(tooltipRows('overview-kpi-tooltip-request-rate')).toEqual([
      tooltipTimestamp,
      'Req/s 3',
    ]);
    expect(tooltipRows('overview-kpi-tooltip-tokens')).toEqual([
      tooltipTimestamp,
      'Tokens 900',
      'Cache miss 50.0%',
    ]);
    expect(tooltipRows('overview-kpi-tooltip-cost')).toEqual([
      tooltipTimestamp,
      'Equiv $ $2.50',
    ]);
    expect(tooltipRows('overview-kpi-tooltip-latency')).toEqual([
      tooltipTimestamp,
      'Avg latency 200ms',
    ]);
    expect(tooltipRows('overview-kpi-tooltip-error-rate')).toEqual([
      tooltipTimestamp,
      'Err rate 10.00%',
    ]);

    fireEvent.mouseLeave(requestChart);
    expect(screen.queryAllByTestId(/^overview-kpi-tooltip-/)).toHaveLength(0);
  });

  it('renders dashes when cache ratios have a zero prompt denominator', () => {
    mockResolvedKpiQueries({ zeroPromptDenominator: true });
    render(<OverviewPage />);

    expect(screen.getByText('Avg cache miss —')).toBeDefined();
    expect(screen.getByTestId('top-principal-row').textContent).toContain(
      '— cache hit',
    );

    const requestChart = screen.getByTestId('overview-kpi-chart-request-rate');
    vi.spyOn(requestChart, 'getBoundingClientRect').mockReturnValue({
      bottom: 42,
      height: 32,
      left: 10,
      right: 210,
      top: 10,
      width: 200,
      x: 10,
      y: 10,
      toJSON: () => ({}),
    });
    fireEvent.mouseMove(requestChart, { clientX: 110, clientY: 26 });

    expect(tooltipRows('overview-kpi-tooltip-tokens')).toEqual([
      kpiTooltipTimestamp(KPI_TIMESTAMPS[1]),
      'Tokens 100',
      'Cache miss —',
    ]);
  });
});
