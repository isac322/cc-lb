import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
} from '@testing-library/react';
import type { ComponentType, ReactNode } from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type {
  AggregateResponse,
  DashboardSummaryResponse,
  DashboardUsageResponse,
  PoolHistoryResponse,
  RequestEvent,
  UsageBucket,
} from '../lib/api';
import * as queries from '../lib/queries';
import type { LiveEventMap } from '../lib/upsertReducer';
import * as liveEvents from '../lib/useLiveEventStream';
import {
  PoolQuotaLegend,
  Route,
  type TopPrincipal,
  TopPrincipalsCard,
  ValueTile,
} from './index';

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

const rechartsMock = vi.hoisted(() => ({
  areaChartRenderCount: 0,
  data: [] as readonly Record<string, unknown>[],
}));

vi.mock('recharts', () => ({
  AreaChart: ({
    children,
    className,
    data = [],
  }: {
    children?: ReactNode;
    className?: string;
    data?: readonly Record<string, unknown>[];
  }) => {
    rechartsMock.areaChartRenderCount += 1;
    rechartsMock.data = data;
    return (
      <svg className={className} data-testid="pool-quota-area-chart">
        {children}
      </svg>
    );
  },
  Area: ({ dataKey }: { dataKey: string }) => {
    const path = rechartsMock.data
      .map(
        (row) =>
          `${String(row.unix)}:${String(row[dataKey] === null ? '' : row[dataKey])}`,
      )
      .join('|');
    return <path d={path} data-testid={`pool-quota-area-${dataKey}`} />;
  },
  XAxis: ({ domain }: { domain: readonly number[] }) => (
    <g data-domain={JSON.stringify(domain)} data-testid="pool-quota-x-axis" />
  ),
  ResponsiveContainer: ({ children }: { children?: ReactNode }) => children,
  CartesianGrid: () => null,
  ReferenceArea: () => null,
  ReferenceLine: () => null,
  Tooltip: () => null,
  YAxis: () => null,
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
          cost_input_micros: 300_000,
          cost_output_micros: 400_000,
          cost_cache_creation_5m_micros: 150_000,
          cost_cache_creation_1h_micros: 0,
          cost_cache_read_micros: 150_000,
        }),
        // Rolled up before per-category cost was persisted: its $2.00 has to
        // land in the unattributed remainder instead of being split.
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

const POOL_HISTORY_NOW = Date.UTC(2026, 7, 17, 15, 30) / 1000;

function poolHistoryResponse(
  nowUnixSecs: number,
  utilizationPercent: number,
  seriesUnixSecs = nowUnixSecs - 86400 + 30,
  rangeSecs = 86400,
): PoolHistoryResponse & { readonly range_secs: number } {
  return {
    range_secs: rangeSecs,
    now_unix_secs: nowUnixSecs,
    windows: [
      { window: '5h', value: utilizationPercent },
      { window: '7d', value: utilizationPercent + 10 },
      { window: '7d_fable', value: utilizationPercent + 20 },
    ].map(({ window, value }) => ({
      window,
      latest: {
        snapshot_at_unix_secs: nowUnixSecs - 30,
        utilization: value / 100,
        utilization_percent: value,
        contributing_upstreams: 1,
        eligible_upstreams: 1,
        stale_upstreams: 0,
        max_observed_at_unix_millis: (nowUnixSecs - 30) * 1000,
      },
      series: [
        {
          snapshot_at_unix_secs: seriesUnixSecs,
          utilization_percent: value,
        },
      ],
    })),
  };
}

function aggregateResponse(
  nowUnixSecs: number,
  utilizationPercent: number,
): AggregateResponse {
  return {
    now_unix_secs: nowUnixSecs,
    window_anchor_unix_secs: nowUnixSecs,
    max_staleness_secs: 300,
    upstream_count: 1,
    caveats: [],
    windows: ['5h', '7d', '7d_fable'].map((window) => ({
      window,
      cc_window_start_unix_secs: nowUnixSecs - 3600,
      cc_window_reset_unix_secs: nowUnixSecs + 3600,
      used_tokens: 100,
      utilization: utilizationPercent / 100,
      utilization_percent: utilizationPercent,
      capacity_to_now_tokens_estimate: 500,
      projected_capacity_tokens_estimate: 1000,
      remaining_to_now_tokens_estimate: 400,
      confidence: 'high',
      contributing_upstreams: 1,
      stale_upstreams: 0,
      missing_capacity_upstreams: 0,
      provider_lots: [
        {
          upstream_id: 'upstream-1',
          upstream_name: 'Upstream One',
          window,
          source: 'oauth',
          state: 'ready',
          provider_start_unix_secs: nowUnixSecs - 3600,
          provider_reset_unix_secs: nowUnixSecs + 3600,
          observed_at_unix_millis: nowUnixSecs * 1000,
          utilization: utilizationPercent / 100,
          capacity_estimate_tokens: 1000,
          used_before_cc_window_tokens: 0,
          capacity_to_now_tokens_estimate: 500,
          projected_capacity_tokens_estimate: 1000,
          confidence: 'high',
          capacity_ratio: 1,
        },
      ],
      caveats: [],
    })),
  };
}

function mockLiveStream(eventsMap: LiveEventMap, version = 0) {
  vi.mocked(liveEvents.useLiveEventStream).mockReturnValue({
    error: null,
    eventsMap,
    forceReconnect: vi.fn(),
    lastActivityAt: null,
    lastCursor: null,
    malformedFrameCount: 0,
    permanentFailure: false,
    permanentFailureSince: null,
    reconnectAttempts: 0,
    status: 'idle',
    version,
  });
}

function finalLiveEvent(index: number): RequestEvent {
  return {
    duration_ms: 10,
    event_id: `live-${index}`,
    event_kind: 'messages',
    model: `model-${index}`,
    request_id: `request-${index}`,
    status: 200,
    ts: index,
  };
}

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
  mockLiveStream(new Map());
}

function mockResolvedEmptyQuotaQueries() {
  vi.mocked(queries.useSubscriptionQuotaAggregate).mockReturnValue({
    data: {
      now_unix_secs: POOL_HISTORY_NOW,
      window_anchor_unix_secs: POOL_HISTORY_NOW,
      max_staleness_secs: 300,
      upstream_count: 0,
      windows: [],
      caveats: [],
    },
    isPending: false,
    isPlaceholderData: false,
  } as never);
  vi.mocked(queries.useSubscriptionQuotaPoolHistory).mockReturnValue({
    data: {
      now_unix_secs: POOL_HISTORY_NOW,
      windows: [],
      range_secs: 86400,
    },
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
  rechartsMock.areaChartRenderCount = 0;
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
            cost_components_micros: null,
            cost_micros: 1_250_000,
            id: 'principal-1',
            max_cost_micros: 1_250_000,
            name: 'Primary principal',
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
        data: historyPending ? undefined : { windows: [], range_secs: 86400 },
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

  it('keeps successful quota and principal content mounted through a placeholder refresh', () => {
    vi.useFakeTimers();
    vi.setSystemTime(POOL_HISTORY_NOW * 1000);
    try {
      const initialAggregate = aggregateResponse(POOL_HISTORY_NOW, 20);
      const initialHistory = poolHistoryResponse(POOL_HISTORY_NOW, 20);
      mockResolvedKpiQueries();
      vi.mocked(queries.useSubscriptionQuotaAggregate).mockReturnValue({
        data: initialAggregate,
        isPending: false,
        isPlaceholderData: false,
      } as never);
      vi.mocked(queries.useSubscriptionQuotaPoolHistory).mockReturnValue({
        data: initialHistory,
        isPending: false,
        isPlaceholderData: false,
      } as never);

      const { rerender } = render(<OverviewPage />);
      const quotaCard = screen.getByTestId('pool-quota-card');
      const chartSlot = screen.getByTestId('pool-quota-chart-slot');
      const chartInstance = chartSlot.firstElementChild;
      expect(chartInstance).not.toBeNull();
      const principalRow = screen.getByTestId('top-principal-row');

      expect(quotaCard.getAttribute('aria-busy')).toBe('false');
      expect(quotaCard.querySelector('.skeleton')).toBeNull();
      expect(quotaCard.textContent).not.toContain(
        'No timeline data yet for this range',
      );
      expect(
        quotaCard.querySelectorAll('[data-testid="pool-quota-legend-slot"]')[0]
          ?.textContent,
      ).toBe('5h · 20%');
      expect(
        quotaCard.querySelectorAll(
          '[data-testid="pool-quota-snapshot-slot"]',
        )[0]?.textContent,
      ).toContain('20.0%');
      expect(principalRow.textContent).toContain('Principal Alpha');
      expect(principalRow.textContent).toContain('$3.00');

      vi.mocked(queries.useSubscriptionQuotaAggregate).mockReturnValue({
        data: initialAggregate,
        isPending: false,
        isPlaceholderData: true,
      } as never);
      vi.mocked(queries.useSubscriptionQuotaPoolHistory).mockReturnValue({
        data: initialHistory,
        isPending: false,
        isPlaceholderData: true,
      } as never);
      vi.mocked(queries.useUsage).mockReturnValue({
        data: PRINCIPAL_USAGE_FIXTURE,
        isPending: false,
        isPlaceholderData: true,
      } as never);

      act(() => vi.advanceTimersByTime(60_000));
      rerender(<OverviewPage />);

      expect(screen.getByTestId('pool-quota-card')).toBe(quotaCard);
      expect(screen.getByTestId('pool-quota-chart-slot')).toBe(chartSlot);
      expect(chartSlot.firstElementChild).toBe(chartInstance);
      expect(quotaCard.getAttribute('aria-busy')).toBe('false');
      expect(quotaCard.querySelector('.skeleton')).toBeNull();
      expect(quotaCard.textContent).not.toContain(
        'No timeline data yet for this range',
      );
      expect(
        quotaCard.querySelectorAll('[data-testid="pool-quota-legend-slot"]')[0]
          ?.textContent,
      ).toBe('5h · 20%');
      expect(screen.getByTestId('top-principal-row')).toBe(principalRow);
      expect(screen.queryByTestId('top-principal-skeleton-row')).toBeNull();
      expect(principalRow.textContent).toContain('$3.00');

      const refreshedPrincipalUsage: DashboardUsageResponse = {
        ...PRINCIPAL_USAGE_FIXTURE,
        series: PRINCIPAL_USAGE_FIXTURE.series.map((series) => ({
          ...series,
          buckets: series.buckets.map((bucket, index) => ({
            ...bucket,
            virtual_cost_micros: index === 0 ? 4_000_000 : 5_000_000,
          })),
        })),
      };
      vi.mocked(queries.useSubscriptionQuotaAggregate).mockReturnValue({
        data: aggregateResponse(POOL_HISTORY_NOW + 60, 55),
        isPending: false,
        isPlaceholderData: false,
      } as never);
      vi.mocked(queries.useSubscriptionQuotaPoolHistory).mockReturnValue({
        data: poolHistoryResponse(POOL_HISTORY_NOW + 60, 55),
        isPending: false,
        isPlaceholderData: false,
      } as never);
      vi.mocked(queries.useUsage).mockReturnValue({
        data: refreshedPrincipalUsage,
        isPending: false,
        isPlaceholderData: false,
      } as never);

      rerender(<OverviewPage />);

      expect(chartSlot.firstElementChild).toBe(chartInstance);
      expect(
        quotaCard.querySelectorAll('[data-testid="pool-quota-legend-slot"]')[0]
          ?.textContent,
      ).toBe('5h · 55%');
      expect(
        quotaCard.querySelectorAll(
          '[data-testid="pool-quota-snapshot-slot"]',
        )[0]?.textContent,
      ).toContain('55.0%');
      expect(screen.getByTestId('top-principal-row')).toBe(principalRow);
      expect(principalRow.textContent).toContain('$9.00');
    } finally {
      vi.useRealTimers();
    }
  });

  it('keeps the successful pool plot context while 1h expands to 7d', () => {
    const initialAggregate = aggregateResponse(POOL_HISTORY_NOW, 20);
    const initialHistory = poolHistoryResponse(
      POOL_HISTORY_NOW,
      20,
      POOL_HISTORY_NOW - 1800,
    );
    mockResolvedKpiQueries();
    vi.mocked(queries.useSubscriptionQuotaAggregate).mockReturnValue({
      data: initialAggregate,
      isPending: false,
      isPlaceholderData: false,
    } as never);
    vi.mocked(queries.useSubscriptionQuotaPoolHistory).mockReturnValue({
      data: initialHistory,
      isPending: false,
      isPlaceholderData: false,
    } as never);

    const { rerender } = render(<OverviewPage />);
    const quotaCard = screen.getByTestId('pool-quota-card');
    const chartSlot = screen.getByTestId('pool-quota-chart-slot');
    const chartInstance = chartSlot.firstElementChild;
    expect(chartInstance).not.toBeNull();
    const principalRow = screen.getByTestId('top-principal-row');

    vi.mocked(queries.useSubscriptionQuotaAggregate).mockReturnValue({
      data: initialAggregate,
      isPending: false,
      isPlaceholderData: true,
    } as never);
    vi.mocked(queries.useSubscriptionQuotaPoolHistory).mockReturnValue({
      data: initialHistory,
      isPending: false,
      isPlaceholderData: true,
    } as never);
    vi.mocked(queries.useUsage).mockReturnValue({
      data: PRINCIPAL_USAGE_FIXTURE,
      isPending: false,
      isPlaceholderData: true,
    } as never);

    fireEvent.click(screen.getByText('1h'));
    expect(screen.getByText('Trend · 24h')).toBeDefined();

    const oneHourHistory = poolHistoryResponse(
      POOL_HISTORY_NOW + 60,
      35,
      POOL_HISTORY_NOW - 900,
      3600,
    );
    vi.mocked(queries.useSubscriptionQuotaAggregate).mockReturnValue({
      data: aggregateResponse(POOL_HISTORY_NOW + 60, 35),
      isPending: false,
      isPlaceholderData: false,
    } as never);
    vi.mocked(queries.useSubscriptionQuotaPoolHistory).mockReturnValue({
      data: oneHourHistory,
      isPending: false,
      isPlaceholderData: false,
    } as never);
    vi.mocked(queries.useUsage).mockReturnValue({
      data: PRINCIPAL_USAGE_FIXTURE,
      isPending: false,
      isPlaceholderData: false,
    } as never);
    rerender(<OverviewPage />);

    expect(screen.getByText('Trend · 1h')).toBeDefined();
    const oneHourDomain = screen
      .getByTestId('pool-quota-x-axis')
      .getAttribute('data-domain');
    const oneHourPath = screen
      .getByTestId('pool-quota-area-5h')
      .getAttribute('d');
    expect(oneHourDomain).toBe(
      JSON.stringify([
        oneHourHistory.now_unix_secs - oneHourHistory.range_secs,
        oneHourHistory.now_unix_secs,
      ]),
    );

    vi.mocked(queries.useSubscriptionQuotaAggregate).mockReturnValue({
      data: aggregateResponse(POOL_HISTORY_NOW + 60, 35),
      isPending: false,
      isPlaceholderData: true,
    } as never);
    vi.mocked(queries.useSubscriptionQuotaPoolHistory).mockReturnValue({
      data: oneHourHistory,
      isPending: false,
      isPlaceholderData: true,
    } as never);
    vi.mocked(queries.useUsage).mockReturnValue({
      data: PRINCIPAL_USAGE_FIXTURE,
      isPending: false,
      isPlaceholderData: true,
    } as never);

    const sevenDayButton = screen.getByRole('button', { name: '7d' });
    fireEvent.click(sevenDayButton);

    expect(sevenDayButton.hasAttribute('data-pressed')).toBe(true);
    expect(screen.getByText('Trend · 1h')).toBeDefined();
    expect(screen.getByText('by virtual cost · 7d')).toBeDefined();
    expect(chartSlot.firstElementChild).toBe(chartInstance);
    expect(
      screen.getByTestId('pool-quota-x-axis').getAttribute('data-domain'),
    ).toBe(oneHourDomain);
    expect(screen.getByTestId('pool-quota-area-5h').getAttribute('d')).toBe(
      oneHourPath,
    );
    expect(quotaCard.querySelector('.skeleton')).toBeNull();
    expect(screen.getByTestId('top-principal-row')).toBe(principalRow);

    const sevenDayHistory = poolHistoryResponse(
      POOL_HISTORY_NOW + 120,
      55,
      POOL_HISTORY_NOW - 6 * 86400,
      604800,
    );
    vi.mocked(queries.useSubscriptionQuotaAggregate).mockReturnValue({
      data: aggregateResponse(POOL_HISTORY_NOW + 120, 55),
      isPending: false,
      isPlaceholderData: false,
    } as never);
    vi.mocked(queries.useSubscriptionQuotaPoolHistory).mockReturnValue({
      data: sevenDayHistory,
      isPending: false,
      isPlaceholderData: false,
    } as never);
    vi.mocked(queries.useUsage).mockReturnValue({
      data: PRINCIPAL_USAGE_FIXTURE,
      isPending: false,
      isPlaceholderData: false,
    } as never);

    rerender(<OverviewPage />);

    expect(screen.getByText('Trend · 7d')).toBeDefined();
    expect(chartSlot.firstElementChild).toBe(chartInstance);
    expect(
      screen.getByTestId('pool-quota-x-axis').getAttribute('data-domain'),
    ).toBe(
      JSON.stringify([
        sevenDayHistory.now_unix_secs - sevenDayHistory.range_secs,
        sevenDayHistory.now_unix_secs,
      ]),
    );
    expect(screen.getByTestId('pool-quota-area-5h').getAttribute('d')).not.toBe(
      oneHourPath,
    );
    expect(
      quotaCard.querySelectorAll('[data-testid="pool-quota-legend-slot"]')[0]
        ?.textContent,
    ).toBe('5h · 55%');
    expect(screen.getByTestId('top-principal-row')).toBe(principalRow);
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
                event_kind: 'messages',
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

  it('requests only messages events and shows no other categories from mixed data', () => {
    mockResolvedKpiQueries();
    const eventsMap: LiveEventMap = new Map([
      ['live-1', { phase: 'final', event: finalLiveEvent(1) }],
      [
        'live-2',
        {
          phase: 'final',
          event: {
            ...finalLiveEvent(2),
            event_kind: 'messages',
            source_kind: 'renewal',
          },
        },
      ],
      [
        'live-3',
        {
          phase: 'final',
          event: { ...finalLiveEvent(3), event_kind: 'count_tokens' },
        },
      ],
      [
        'live-4',
        {
          phase: 'final',
          event: {
            duration_ms: 10,
            event_id: 'live-4',
            model: 'model-4',
            request_id: 'request-4',
            status: 200,
            ts: 4,
          },
        },
      ],
    ]);
    mockLiveStream(eventsMap, 4);
    vi.mocked(queries.useRecentEventsInfinite).mockReturnValue({
      data: {
        pages: [
          {
            events: [
              {
                duration_ms: 10,
                event_kind: 'messages',
                model: 'historical-model',
                request_id: 'historical-1',
                status: 200,
                ts: 5,
              },
              {
                duration_ms: 10,
                event_kind: 'models',
                model: 'historical-models',
                request_id: 'historical-2',
                status: 200,
                ts: 4,
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

    render(<OverviewPage />);

    expect(queries.useRecentEventsInfinite).toHaveBeenCalledWith({
      event_kind: 'messages',
    });
    expect(liveEvents.useLiveEventStream).toHaveBeenCalledWith({
      event_kind: 'messages',
    });
    expect(screen.getByLabelText('View request live-1')).toBeDefined();
    expect(screen.getByLabelText('View request historical-1')).toBeDefined();
    expect(screen.queryByLabelText('View request live-2')).toBeNull();
    expect(screen.queryByLabelText('View request live-3')).toBeNull();
    expect(screen.queryByLabelText('View request live-4')).toBeNull();
    expect(screen.queryByLabelText('View request historical-2')).toBeNull();
  });

  it('keeps the pool chart memoized across live-only request updates', () => {
    mockResolvedKpiQueries();
    const eventsMap: LiveEventMap = new Map([
      ['live-1', { phase: 'final', event: finalLiveEvent(1) }],
    ]);
    mockLiveStream(eventsMap, 1);
    const { rerender } = render(<OverviewPage />);
    const initialChartRenderCount = rechartsMock.areaChartRenderCount;

    eventsMap.set('live-2', {
      phase: 'final',
      event: finalLiveEvent(2),
    });
    mockLiveStream(eventsMap, 2);
    rerender(<OverviewPage />);

    expect(initialChartRenderCount).toBeGreaterThan(0);
    expect(rechartsMock.areaChartRenderCount).toBe(initialChartRenderCount);
    expect(screen.getByLabelText('View request live-2')).toBeDefined();
  });

  it('flashes the newest live row after the twentieth insertion', () => {
    mockResolvedKpiQueries();
    const eventsMap: LiveEventMap = new Map();
    for (let index = 1; index <= 20; index += 1) {
      eventsMap.set(`live-${index}`, {
        phase: 'final',
        event: finalLiveEvent(index),
      });
    }
    mockLiveStream(eventsMap, 20);
    const { rerender } = render(<OverviewPage />);

    expect(screen.getByLabelText('View request live-1').className).toContain(
      'flash-in',
    );
    eventsMap.set('live-21', {
      phase: 'final',
      event: finalLiveEvent(21),
    });
    mockLiveStream(eventsMap, 21);
    rerender(<OverviewPage />);

    expect(screen.getByLabelText('View request live-21').className).toContain(
      'flash-in',
    );
    expect(
      screen.getByLabelText('View request live-1').className,
    ).not.toContain('flash-in');

    eventsMap.set('live-1', {
      phase: 'final',
      event: { ...finalLiveEvent(1), status: 201 },
    });
    mockLiveStream(eventsMap, 22);
    rerender(<OverviewPage />);

    expect(
      screen.getByLabelText('View request live-1').className,
    ).not.toContain('flash-in');
    expect(screen.getByLabelText('View request live-21').className).toContain(
      'flash-in',
    );
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

  it('updates cache metrics when resolved query data changes', () => {
    mockResolvedKpiQueries();
    const { rerender } = render(<OverviewPage />);

    expect(screen.getByText('Avg cache miss 44.4%')).toBeDefined();
    expect(screen.getByTestId('top-principal-row').textContent).toContain(
      '83.3% cache hit',
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
    expect(tooltipRows('overview-kpi-tooltip-tokens')).toContain(
      'Cache miss 50.0%',
    );

    const updatedSummary: DashboardSummaryResponse = {
      ...SUMMARY_FIXTURE,
      totals: {
        ...SUMMARY_FIXTURE.totals,
        input_tokens: 300,
        cache_creation_input_tokens: 300,
        cache_read_input_tokens: 1_400,
      },
      sparkline: {
        buckets: SUMMARY_BUCKETS.map((bucket, index) => ({
          ...bucket,
          input_tokens: 100,
          cache_creation_input_tokens: 100,
          cache_read_input_tokens: index === 0 ? 200 : index === 1 ? 800 : 400,
        })),
      },
    };
    const updatedPrincipalUsage: DashboardUsageResponse = {
      ...PRINCIPAL_USAGE_FIXTURE,
      series: PRINCIPAL_USAGE_FIXTURE.series.map((series) => ({
        ...series,
        buckets: series.buckets.map((bucket, index) => ({
          ...bucket,
          input_tokens: index === 0 ? 50 : 400,
          cache_creation_input_tokens: index === 0 ? 50 : 100,
          cache_read_input_tokens: index === 0 ? 400 : 1_000,
        })),
      })),
    };
    vi.mocked(queries.useSummary).mockReturnValue({
      data: updatedSummary,
      isPending: false,
    } as never);
    vi.mocked(queries.useUsage).mockReturnValue({
      data: updatedPrincipalUsage,
      isPending: false,
      isPlaceholderData: false,
    } as never);

    rerender(<OverviewPage />);

    expect(screen.queryByText('Avg cache miss 44.4%')).toBeNull();
    expect(screen.getByText('Avg cache miss 30.0%')).toBeDefined();
    expect(screen.getByTestId('top-principal-row').textContent).not.toContain(
      '83.3% cache hit',
    );
    expect(screen.getByTestId('top-principal-row').textContent).toContain(
      '70.0% cache hit',
    );
    expect(tooltipRows('overview-kpi-tooltip-tokens')).toContain(
      'Cache miss 20.0%',
    );
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

/** Request-log slice colors as jsdom serializes them. */
const CATEGORY_COLOR = {
  input: 'rgb(56, 189, 248)',
  output: 'rgb(167, 139, 250)',
  cache_create_5m: 'rgb(251, 191, 36)',
  cache_create_1h: 'rgb(180, 83, 9)',
  cache_read: 'rgb(52, 211, 153)',
  unattributed: 'rgb(107, 114, 128)',
} as const;

const COST_NOTE = 'Per-category cost not recorded for this window';

const COMPLETE_PRINCIPAL: TopPrincipal = {
  cache_hit_ratio: 0.5,
  cost_components_micros: {
    input: 400_000,
    output: 300_000,
    cache_create_5m: 100_000,
    cache_create_1h: 100_000,
    cache_read: 100_000,
  },
  cost_micros: 1_000_000,
  id: 'principal-complete',
  max_cost_micros: 1_000_000,
  name: 'Complete principal',
  requests: 12,
  share_pct: 62.5,
  tokens: 345,
};

/** $0.80 recorded, $0.60 of it attributed: the rest is a legacy remainder. */
const PARTIAL_PRINCIPAL: TopPrincipal = {
  ...COMPLETE_PRINCIPAL,
  cost_components_micros: {
    input: 200_000,
    output: 200_000,
    cache_create_5m: 0,
    cache_create_1h: 0,
    cache_read: 200_000,
  },
  cost_micros: 800_000,
  id: 'principal-partial',
  name: 'Partial principal',
};

const UNRECORDED_PRINCIPAL: TopPrincipal = {
  ...COMPLETE_PRINCIPAL,
  cache_hit_ratio: null,
  cost_components_micros: null,
  cost_micros: 500_000,
  id: 'principal-unrecorded',
  name: 'Unrecorded principal',
};

function costMeters(): HTMLElement[] {
  return screen.getAllByTestId('top-principal-cost-meter');
}

function costTriggers(): HTMLButtonElement[] {
  return screen.getAllByTestId<HTMLButtonElement>('top-principal-cost-trigger');
}

function meterFill(meter: HTMLElement): HTMLElement {
  const fill = meter.querySelector<HTMLElement>(
    '[data-slot="cost-meter-fill"]',
  );
  if (!fill) throw new Error('cost meter has no fill');
  return fill;
}

function costSegments(
  meter: HTMLElement,
): { category: string; color: string; width: string }[] {
  return Array.from(
    meter.querySelectorAll<HTMLElement>(
      '[data-testid="top-principal-cost-segment"]',
    ),
    (segment) => ({
      category: segment.dataset.category ?? '',
      color: segment.style.backgroundColor,
      width: segment.style.width,
    }),
  );
}

/** Every figure the open breakdown shows, in order. */
function costDetailValues(): string[] {
  return Array.from(
    screen.getByTestId('top-principal-cost-details').querySelectorAll('span'),
    (span) => span.textContent ?? '',
  ).filter((text) => text.length > 0);
}

function renderPrincipals(principals: readonly TopPrincipal[]) {
  return render(
    <TopPrincipalsCard loading={false} principals={principals} range="24h" />,
  );
}

describe('Top principal cost meter', () => {
  it('reads requests, tokens and cache hit with no primary-model placeholder', () => {
    const { container } = renderPrincipals([COMPLETE_PRINCIPAL]);

    expect(
      container.querySelector('[data-slot="principal-meta"]')?.textContent,
    ).toBe('12 req · 345 tok · 50.0% cache hit');
    expect(screen.getByTestId('top-principal-row').textContent).not.toContain(
      '—',
    );
    expect(screen.getByTestId('top-principal-row').className).toContain(
      'min-h-[66px]',
    );
    expect(meterFill(costMeters()[0]).parentElement?.className).toContain(
      'h-1.5',
    );
  });

  it('subdivides the filled meter in request-log order, colors and widths', () => {
    renderPrincipals([COMPLETE_PRINCIPAL]);
    const meter = costMeters()[0];

    expect(meter.dataset.costComponents).toBe('complete');
    expect(meterFill(meter).style.width).toBe('100%');
    expect(meterFill(meter).className).not.toContain('var(--color-accent)');
    expect(costSegments(meter)).toEqual([
      { category: 'input', color: CATEGORY_COLOR.input, width: '40%' },
      { category: 'output', color: CATEGORY_COLOR.output, width: '30%' },
      {
        category: 'cache_create_5m',
        color: CATEGORY_COLOR.cache_create_5m,
        width: '10%',
      },
      {
        category: 'cache_create_1h',
        color: CATEGORY_COLOR.cache_create_1h,
        width: '10%',
      },
      {
        category: 'cache_read',
        color: CATEGORY_COLOR.cache_read,
        width: '10%',
      },
    ]);
  });

  it('names the meter and carries the whole breakdown in its value text', () => {
    renderPrincipals([COMPLETE_PRINCIPAL]);
    const meter = costMeters()[0];

    expect(meter.getAttribute('role')).toBe('meter');
    expect(meter.getAttribute('aria-label')).toBe('Complete principal cost');
    expect(meter.getAttribute('aria-valuenow')).toBe('1000000');
    expect(meter.getAttribute('aria-valuemax')).toBe('1000000');
    expect(meter.getAttribute('aria-valuetext')).toBe(
      'Total $1.0000; 100.0% of the largest principal; Input $0.4000, Output $0.3000, Cache create 5m $0.1000, Cache create 1h $0.1000, Cache read $0.1000',
    );
  });

  it('shows exact values and keeps keyboard focus on the breakdown trigger', () => {
    renderPrincipals([COMPLETE_PRINCIPAL]);
    const trigger = costTriggers()[0];
    const meter = costMeters()[0];

    expect(screen.queryByTestId('top-principal-cost-details')).toBeNull();
    act(() => trigger.focus());

    expect(document.activeElement).toBe(trigger);
    expect(costDetailValues()).toEqual([
      'Input',
      '$0.4000',
      '40%',
      'Output',
      '$0.3000',
      '30%',
      'Cache create 5m',
      '$0.1000',
      '10%',
      'Cache create 1h',
      '$0.1000',
      '10%',
      'Cache read',
      '$0.1000',
      '10%',
      'Total',
      '$1.0000',
    ]);
    expect(costTriggers()[0]).toBe(trigger);
    expect(document.activeElement).toBe(trigger);
    expect(costMeters()[0]).toBe(meter);
    expect(meter.getAttribute('role')).toBe('meter');
    expect(meter.getAttribute('aria-valuetext')).toContain('Input $0.4000');
    expect(costSegments(meter).map((segment) => segment.category)).toEqual([
      'input',
      'output',
      'cache_create_5m',
      'cache_create_1h',
      'cache_read',
    ]);
  });

  it('opens the same breakdown after the hover delay', () => {
    vi.useFakeTimers();
    try {
      renderPrincipals([COMPLETE_PRINCIPAL]);
      fireEvent.pointerEnter(costTriggers()[0]);

      act(() => vi.advanceTimersByTime(199));
      expect(screen.queryByTestId('top-principal-cost-details')).toBeNull();

      act(() => vi.advanceTimersByTime(1));
      expect(costDetailValues()).toContain('Cache create 5m');
      expect(costDetailValues()).toContain('$1.0000');
    } finally {
      vi.useRealTimers();
    }
  });

  it('appends a neutral unattributed tail when the total outruns the categories', () => {
    renderPrincipals([PARTIAL_PRINCIPAL]);
    const meter = costMeters()[0];

    expect(meter.dataset.costComponents).toBe('partial');
    expect(meterFill(meter).style.width).toBe('80%');
    expect(costSegments(meter)).toEqual([
      { category: 'input', color: CATEGORY_COLOR.input, width: '25%' },
      { category: 'output', color: CATEGORY_COLOR.output, width: '25%' },
      {
        category: 'cache_read',
        color: CATEGORY_COLOR.cache_read,
        width: '25%',
      },
      {
        category: 'unattributed',
        color: CATEGORY_COLOR.unattributed,
        width: '25%',
      },
    ]);

    fireEvent.focus(costTriggers()[0]);
    expect(costDetailValues()).toEqual([
      'Input',
      '$0.2000',
      '25%',
      'Output',
      '$0.2000',
      '25%',
      'Cache create 5m',
      '$0.0000',
      '—',
      'Cache create 1h',
      '$0.0000',
      '—',
      'Cache read',
      '$0.2000',
      '25%',
      'Unattributed',
      '$0.2000',
      '25%',
      'Total',
      '$0.8000',
    ]);
  });

  it('keeps a solid accent bar and says so when no category cost was recorded', () => {
    renderPrincipals([UNRECORDED_PRINCIPAL]);
    const meter = costMeters()[0];

    expect(meter.dataset.costComponents).toBe('unavailable');
    expect(costSegments(meter)).toEqual([]);
    expect(meterFill(meter).className).toContain(
      'bg-[color:var(--color-accent)]',
    );
    expect(meter.getAttribute('aria-valuetext')).toBe(
      `Total $0.5000; 50.0% of the largest principal; ${COST_NOTE}`,
    );

    fireEvent.focus(costTriggers()[0]);
    expect(screen.getByText(COST_NOTE)).toBeDefined();
    expect(costDetailValues()).toEqual(['Total', '$0.5000']);
  });

  it('scales every meter against the largest principal below one dollar', () => {
    renderPrincipals([
      {
        ...COMPLETE_PRINCIPAL,
        cost_components_micros: {
          input: 250_000,
          output: 250_000,
          cache_create_5m: 0,
          cache_create_1h: 0,
          cache_read: 0,
        },
        cost_micros: 500_000,
        id: 'principal-top',
        max_cost_micros: 500_000,
        name: 'Top principal',
        share_pct: 80,
      },
      {
        ...UNRECORDED_PRINCIPAL,
        cost_micros: 125_000,
        max_cost_micros: 500_000,
        share_pct: 20,
      },
    ]);
    const [top, tail] = costMeters();

    expect(meterFill(top).style.width).toBe('100%');
    expect(meterFill(tail).style.width).toBe('25%');
    expect(costSegments(top)).toEqual([
      { category: 'input', color: CATEGORY_COLOR.input, width: '50%' },
      { category: 'output', color: CATEGORY_COLOR.output, width: '50%' },
    ]);
    expect(screen.getAllByTestId('top-principal-row')[0].textContent).toContain(
      '$0.50',
    );
  });

  it('aggregates bucket components and follows refreshed usage data', () => {
    mockResolvedKpiQueries();
    const { rerender } = render(<OverviewPage />);
    const meter = costMeters()[0];

    // $1.00 of the window's $3.00 carries components; the pre-upgrade bucket's
    // $2.00 stays unattributed instead of being spread over the categories.
    expect(meter.dataset.costComponents).toBe('partial');
    fireEvent.focus(costTriggers()[0]);
    expect(costDetailValues()).toEqual([
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
    ]);
    expect(screen.getByTestId('top-principal-row').textContent).toContain(
      '83.3% cache hit',
    );

    // Same window, same total, same tokens: only the recorded split moves.
    const refreshed: DashboardUsageResponse = {
      ...PRINCIPAL_USAGE_FIXTURE,
      series: PRINCIPAL_USAGE_FIXTURE.series.map((series) => ({
        ...series,
        buckets: series.buckets.map((bucket, index) =>
          index === 0
            ? {
                ...bucket,
                cost_input_micros: 500_000,
                cost_output_micros: 300_000,
                cost_cache_creation_5m_micros: 100_000,
                cost_cache_creation_1h_micros: 100_000,
                cost_cache_read_micros: 0,
              }
            : bucket,
        ),
      })),
    };
    vi.mocked(queries.useUsage).mockReturnValue({
      data: refreshed,
      isPending: false,
      isPlaceholderData: false,
    } as never);

    rerender(<OverviewPage />);

    // The breakdown was never closed or re-opened: it re-rendered in place.
    expect(costDetailValues()).toEqual([
      'Input',
      '$0.5000',
      '17%',
      'Output',
      '$0.3000',
      '10%',
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
    ]);
    expect(costSegments(costMeters()[0])).toEqual([
      {
        category: 'input',
        color: CATEGORY_COLOR.input,
        width: '16.666666666666664%',
      },
      { category: 'output', color: CATEGORY_COLOR.output, width: '10%' },
      {
        category: 'cache_create_5m',
        color: CATEGORY_COLOR.cache_create_5m,
        width: '3.3333333333333335%',
      },
      {
        category: 'cache_create_1h',
        color: CATEGORY_COLOR.cache_create_1h,
        width: '3.3333333333333335%',
      },
      {
        category: 'unattributed',
        color: CATEGORY_COLOR.unattributed,
        width: '66.66666666666666%',
      },
    ]);
    expect(screen.getByTestId('top-principal-row').textContent).toContain(
      '83.3% cache hit',
    );
  });
});
