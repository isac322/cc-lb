import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  within,
} from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import type { ComponentType, ReactNode } from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type {
  AggregateResponse,
  DashboardSummaryResponse,
  DashboardUsageResponse,
  PoolHistoryResponse,
  RequestEvent,
  Upstream,
  UsageBucket,
} from '../lib/api';
import { getWindowColor } from '../lib/colors';
import * as queries from '../lib/queries';
import type { LiveEventMap } from '../lib/upsertReducer';
import * as liveEvents from '../lib/useLiveEventStream';
import {
  Route,
  type TopPrincipal,
  TopPrincipalsSection,
  ValueTile,
} from './index';

vi.mock('../lib/queries', async () => {
  const actual = await vi.importActual<typeof queries>('../lib/queries');
  return {
    ...actual,
    usePrincipalNameMap: vi.fn(),
    useRecentEventsPage: vi.fn(),
    recentEventsPageQueryOptions: vi.fn(),
    useSubscriptionQuotaAggregate: vi.fn(),
    useSubscriptionQuotaPoolHistory: vi.fn(),
    useSummary: vi.fn(),
    useUsage: vi.fn(),
    useUpstreams: vi.fn(),
  };
});
vi.mock('../lib/useLiveEventStream', () => ({
  useLiveEventStream: vi.fn(),
}));

// The upstream strip's grid and its data hook belong to the Upstreams page;
// the Overview only decides where the strip goes and what an empty pool says.
vi.mock('../components/upstreams/UpstreamUsageTable', () => ({
  useUpstreamUsageData: () => ({
    rows: [],
    isLoading: false,
    quotaPending: false,
    statusPending: false,
    usagePending: false,
    quotaError: false,
    reconnectCount: 0,
  }),
  UpstreamUsageTable: ({ empty }: { empty?: ReactNode }) => <>{empty}</>,
}));

// The Overview links to other routes and keeps its range in the URL; render
// router links as plain anchors and back the search params with a tiny store
// so the page can mount without a RouterProvider.
const routerMock = vi.hoisted(() => ({
  search: { range: '24h' } as Record<string, unknown>,
  listeners: new Set<() => void>(),
  navigate: undefined as unknown as ReturnType<typeof vi.fn>,
}));

vi.mock('@tanstack/react-router', async () => {
  const actual = await vi.importActual<Record<string, unknown>>(
    '@tanstack/react-router',
  );
  const { useSyncExternalStore } =
    await vi.importActual<typeof import('react')>('react');
  const subscribe = (listener: () => void) => {
    routerMock.listeners.add(listener);
    return () => routerMock.listeners.delete(listener);
  };
  routerMock.navigate = vi.fn(
    ({
      search,
    }: {
      search:
        | Record<string, unknown>
        | ((prev: Record<string, unknown>) => Record<string, unknown>);
    }) => {
      routerMock.search =
        typeof search === 'function' ? search(routerMock.search) : search;
      for (const listener of routerMock.listeners) listener();
    },
  );
  return {
    ...actual,
    useNavigate: () => routerMock.navigate,
    useSearch: () => useSyncExternalStore(subscribe, () => routerMock.search),
    Link: ({
      to,
      search,
      children,
      ...rest
    }: {
      to: string;
      search?: Record<string, string>;
      children?: ReactNode;
    }) => (
      <a
        href={search ? `${to}?${new URLSearchParams(search).toString()}` : to}
        {...rest}
      >
        {children}
      </a>
    ),
  };
});

const rechartsMock = vi.hoisted(() => ({
  areaChartRenderCount: 0,
  data: [] as readonly Record<string, unknown>[],
  /** When set, the Tooltip mock renders its `content` with this hover. */
  tooltip: null as null | {
    label: number;
    payload: readonly Record<string, unknown>[];
  },
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
  Area: ({
    dataKey,
    stroke,
    strokeDasharray,
    fill,
  }: {
    dataKey: string;
    stroke?: string;
    strokeDasharray?: string;
    fill?: string;
  }) => {
    const path = rechartsMock.data
      .map(
        (row) =>
          `${String(row.unix)}:${String(row[dataKey] === null ? '' : row[dataKey])}`,
      )
      .join('|');
    // A fill-only area (no stroke) is the pool chart's fill pass.
    const role = stroke === 'none' ? 'fill' : 'area';
    return (
      <path
        d={path}
        data-testid={`pool-quota-${role}-${dataKey}`}
        fill={fill}
        stroke={stroke}
        strokeDasharray={strokeDasharray}
      />
    );
  },
  XAxis: ({ domain }: { domain: readonly number[] }) => (
    <g data-domain={JSON.stringify(domain)} data-testid="pool-quota-x-axis" />
  ),
  ResponsiveContainer: ({ children }: { children?: ReactNode }) => children,
  CartesianGrid: () => null,
  ReferenceArea: () => null,
  ReferenceLine: () => null,
  Tooltip: ({
    content,
  }: {
    content?: (props: Record<string, unknown>) => ReactNode;
  }) =>
    rechartsMock.tooltip && content ? (
      <foreignObject data-testid="pool-quota-tooltip">
        {content({ active: true, ...rechartsMock.tooltip })}
      </foreignObject>
    ) : null,
  YAxis: () => null,
}));

const RouteComponent = Route.options.component as ComponentType;

// OAuthReconnectSummary runs its own react-query hooks; give the page the
// same provider the app shell supplies.
const queryClient = new QueryClient({
  defaultOptions: { queries: { retry: false } },
});
const OverviewPage = () => (
  <QueryClientProvider client={queryClient}>
    <RouteComponent />
  </QueryClientProvider>
);

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
    screen.getByTestId(testId).children,
    (row) => row.textContent ?? '',
  );
}

function rangeOption(group: string, name: string): HTMLElement {
  return within(screen.getByRole('radiogroup', { name: group })).getByRole(
    'radio',
    { name },
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

function mockRequestEventsFeed(
  events: readonly RequestEvent[] = [],
  {
    liveEventsMap = new Map(),
    version = 0,
  }: {
    liveEventsMap?: LiveEventMap;
    version?: number;
  } = {},
) {
  vi.mocked(queries.useRecentEventsPage).mockReturnValue({
    data: {
      events,
      observed: true,
      count: events.length,
      limit: 500,
    },
    isPending: false,
    isPlaceholderData: false,
    isFetching: false,
    error: null,
    refetch: vi.fn(),
  } as never);
  vi.mocked(queries.recentEventsPageQueryOptions).mockImplementation(
    (_filters, pageParam) =>
      ({
        queryKey: ['events', pageParam],
        queryFn: async () => ({
          events: [],
          observed: true,
          count: 0,
          limit: 500,
        }),
      }) as never,
  );
  vi.mocked(liveEvents.useLiveEventStream).mockReturnValue({
    error: null,
    eventsMap: liveEventsMap,
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
  vi.mocked(queries.usePrincipalNameMap).mockReturnValue(new Map());
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
  mockRequestEventsFeed();
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

/**
 * 30 principals with requests plus 3 known principals without any: cost
 * falls from Team 01 to Team 30, requests rise, tokens peak at Team 15.
 */
function mockManyPrincipals() {
  mockResolvedKpiQueries();
  const ids = Array.from(
    { length: 30 },
    (_, index) => `${String(index + 1).padStart(2, '0')}`,
  );
  const series = ids.map((id, index) => ({
    key: `principal-${id}`,
    buckets: [
      usageBucket(KPI_TIMESTAMPS[0], {
        request_count: index + 1,
        input_tokens: 1_000 - Math.abs(index - 14) * 10,
        virtual_cost_micros: (30 - index) * 100_000,
      }),
    ],
  }));
  vi.mocked(queries.useUsage).mockReturnValue({
    data: { ...PRINCIPAL_USAGE_FIXTURE, series },
    isPending: false,
    isPlaceholderData: false,
  } as never);
  vi.mocked(queries.usePrincipalNameMap).mockReturnValue(
    new Map([
      ...ids.map((id) => [`principal-${id}`, `Team ${id}`] as const),
      ['idle-1', 'Idle one'],
      ['idle-2', 'Idle two'],
      ['idle-3', 'Idle three'],
    ]),
  );
}

beforeEach(() => {
  vi.clearAllMocks();
  routerMock.search = { range: '24h' };
  rechartsMock.areaChartRenderCount = 0;
  rechartsMock.tooltip = null;
  // OAuthReconnectSummary polls upstreams itself; default to a resolved
  // empty list so no OAuth status queries spin up. Tests that need an
  // OAuth upstream override this.
  vi.mocked(queries.useUpstreams).mockReturnValue({
    data: { upstreams: [] },
    isPending: false,
    isPlaceholderData: false,
  } as never);
});

afterEach(() => {
  cleanup();
});

describe('Overview pool quota usage', () => {
  it('leads with the pool chart, legend reads used with its reset, captions qualify it', () => {
    const base = aggregateResponse(POOL_HISTORY_NOW, 60);
    const used: Record<string, number> = { '5h': 12, '7d': 83, '7d_fable': 97 };
    const aggregate: AggregateResponse = {
      ...base,
      windows: base.windows.map((entry) => ({
        ...entry,
        utilization_percent: used[entry.window] ?? null,
        missing_capacity_upstreams: entry.window === '7d' ? 2 : 0,
        provider_lots: entry.provider_lots.map((lot) => ({
          ...lot,
          state: 'stale',
        })),
      })),
    };
    const history = poolHistoryResponse(POOL_HISTORY_NOW, 12);
    history.windows = history.windows.map((entry) => ({
      ...entry,
      latest: entry.latest
        ? { ...entry.latest, utilization_percent: used[entry.window] ?? null }
        : null,
    }));
    mockResolvedKpiQueries();
    vi.mocked(queries.useSubscriptionQuotaAggregate).mockReturnValue({
      data: aggregate,
      isPending: false,
      isPlaceholderData: false,
    } as never);
    vi.mocked(queries.useSubscriptionQuotaPoolHistory).mockReturnValue({
      data: history,
      isPending: false,
      isPlaceholderData: false,
    } as never);

    render(<OverviewPage />);

    // Usage first: the range-scoped group leads, and the sections it does
    // not govern follow it.
    expect(
      screen
        .getAllByRole('heading', { level: 2 })
        .map((heading) => heading.textContent),
    ).toEqual(['Usage', 'Upstreams', 'Latest requests (any time)']);
    const group = screen.getByRole('region', { name: 'Usage' });
    expect(
      within(group)
        .getAllByRole('heading', { level: 3 })
        .map((heading) => heading.textContent),
    ).toEqual(['Pool quota usage', 'Traffic', 'Top principals']);

    const slots = screen.getAllByTestId('pool-quota-legend-slot');
    expect(slots.map((slot) => slot.textContent)).toEqual([
      '5h · 12% used · resets in 1h',
      '7d · 83% used · resets in 1h',
      '7d (Fable) · 97% used · resets in 1h',
    ]);
    const value = (index: number) =>
      within(slots[index]!).getByText(/% used$/).className;
    expect(value(0)).toContain('text-text');
    expect(value(1)).toContain('text-warn-text');
    expect(value(2)).toContain('text-danger-text');

    const captions = screen.getByTestId('pool-quota-captions');
    expect(captions.textContent).toContain('Plan-weighted across upstreams');
    expect(captions.textContent).toContain('Capacity unknown for 2 upstreams');
    expect(captions.textContent).toContain('Stale reading');
    expect(screen.getByTestId('pool-quota-card').textContent).not.toMatch(
      /left|headroom/i,
    );
  });

  it('fills each window in its window color and paints every stroke above every fill', () => {
    mockResolvedKpiQueries();
    vi.mocked(queries.useSubscriptionQuotaPoolHistory).mockReturnValue({
      data: poolHistoryResponse(POOL_HISTORY_NOW, 12),
      isPending: false,
      isPlaceholderData: false,
    } as never);

    render(<OverviewPage />);

    const slot = screen.getByTestId('pool-quota-chart-slot');
    const paths = Array.from(slot.querySelectorAll('path'));
    const fills = paths.filter((path) =>
      path.getAttribute('data-testid')?.startsWith('pool-quota-fill-'),
    );
    const strokes = paths.filter((path) =>
      path.getAttribute('data-testid')?.startsWith('pool-quota-area-'),
    );
    expect(fills).toHaveLength(3);
    expect(strokes).toHaveLength(3);
    // No fill may cover a stroke: the fill pass paints first.
    expect(paths.indexOf(fills.at(-1)!)).toBeLessThan(
      paths.indexOf(strokes[0]!),
    );
    for (const window of ['5h', '7d', '7d_fable']) {
      const stroke = within(slot).getByTestId(`pool-quota-area-${window}`);
      expect(stroke.getAttribute('stroke')).toBe(getWindowColor(window).stroke);
      expect(stroke.getAttribute('stroke-dasharray')).toBeNull();
      expect(stroke.getAttribute('fill')).toBe('none');
      const fill = within(slot)
        .getByTestId(`pool-quota-fill-${window}`)
        .getAttribute('fill');
      const gradientId = /^url\(#(.+)\)$/.exec(fill ?? '')?.[1];
      expect(gradientId).toBeDefined();
      expect(
        slot.querySelector(`linearGradient[id="${gradientId}"]`),
      ).not.toBeNull();
    }
  });

  it('lists each window once in the hover tooltip, in legend order', () => {
    mockResolvedKpiQueries();
    vi.mocked(queries.useSubscriptionQuotaPoolHistory).mockReturnValue({
      data: poolHistoryResponse(POOL_HISTORY_NOW, 12),
      isPending: false,
      isPlaceholderData: false,
    } as never);
    // What Recharts hands custom content: an entry for every Area in paint
    // order, the fill pass (`tooltipType="none"`) included.
    const values: Record<string, number> = {
      '7d': 66,
      '7d_fable': 62,
      '5h': 14,
    };
    rechartsMock.tooltip = {
      label: POOL_HISTORY_NOW - 60,
      payload: [
        ...Object.entries(values).map(([dataKey, value]) => ({
          dataKey,
          value,
          type: 'none',
        })),
        ...Object.entries(values).map(([dataKey, value]) => ({
          dataKey,
          value,
        })),
      ],
    };

    render(<OverviewPage />);

    const rows = within(screen.getByTestId('pool-quota-tooltip')).getAllByText(
      /% used$/,
    );
    expect(rows.map((row) => row.parentElement?.textContent)).toEqual([
      '5h window14% used',
      '7d window66% used',
      '7d (Fable) window62% used',
    ]);
  });

  it('links to the upstreams page from the upstreams table section', () => {
    mockResolvedKpiQueries();
    render(<OverviewPage />);
    expect(
      screen
        .getByRole('link', { name: 'Manage upstreams' })
        .getAttribute('href'),
    ).toBe('/upstreams');
  });
});

describe('Overview loading geometry', () => {
  it('keeps ValueTile value, sub, and sparkline slots fixed', () => {
    const { container, rerender } = render(
      <ValueTile
        chartId="request-rate"
        chartLabel="Req/s"
        label="requests"
        loading
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
    expect(valueClassName).toContain('h-9');
    expect(subClassName).toContain('h-4');
    expect(sparklineClassName).toContain('h-8');
    expect(screen.queryByText('12')).toBeNull();
    expect(screen.queryByText('12 / 24h')).toBeNull();

    rerender(
      <ValueTile
        chartId="request-rate"
        chartLabel="Req/s"
        label="requests"
        loading={false}
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

  it('draws a second series as a dashed line with gaps and reads both figures with units', () => {
    render(
      <ValueTile
        activeIndex={1}
        chartId="two-series"
        formatChartValue={(value) => `${value} tokens`}
        label="two series"
        secondary={{
          color: 'var(--color-series-cache-create-5m)',
          format: (value) => `${value ?? '—'}% cache miss`,
        }}
        spark={[
          { secondaryValue: null, timestamp: KPI_TIMESTAMPS[0], value: 100 },
          { secondaryValue: 7, timestamp: KPI_TIMESTAMPS[1], value: 200 },
        ]}
        sparkColor="var(--color-text-muted)"
        value="312"
      />,
    );

    const primary = screen.getByTestId('pool-quota-area-value');
    const secondary = screen.getByTestId('pool-quota-area-secondary');
    expect(primary.getAttribute('stroke')).toBe('var(--color-text-muted)');
    expect(primary.getAttribute('stroke-dasharray')).toBeNull();
    expect(secondary.getAttribute('stroke')).toBe(
      'var(--color-series-cache-create-5m)',
    );
    expect(secondary.getAttribute('stroke-dasharray')).not.toBeNull();
    // A bucket without a ratio is a gap, not a zero.
    expect(secondary.getAttribute('d')).toBe('undefined:|undefined:7');
    expect(tooltipRows('overview-kpi-tooltip-two-series').slice(1)).toEqual([
      '200 tokens',
      '7% cache miss',
    ]);
  });

  // Mounts the whole page with 30 rows of meters and popovers: slow in jsdom.
  it('ranks the top ten by cost and expands in place to every principal with a name filter', {
    timeout: 30_000,
  }, () => {
    mockManyPrincipals();

    render(<OverviewPage />);

    const group = screen.getByRole('region', { name: 'Top principals' });
    let rows = within(group).getAllByTestId('top-principal-row');
    expect(rows).toHaveLength(10);
    expect(rows[0]?.textContent).toContain('Team 01');
    expect(rows[9]?.textContent).toContain('Team 10');
    // $3.00 of the pool's $46.50.
    expect(within(rows[0]!).getByText('6.5%')).toBeDefined();
    expect(
      within(rows[0]!)
        .getByRole('link', { name: 'Team 01' })
        .getAttribute('href'),
    ).toBe('/principals?selectedId=principal-01');
    expect(within(group).queryByText(/others/)).toBeNull();
    expect(
      within(group).getByText('3 principals had no requests in the last 24h'),
    ).toBeDefined();
    expect(
      within(group).queryByRole('searchbox', {
        name: 'Filter principals by name',
      }),
    ).toBeNull();
    expect(
      within(group)
        .getByRole('link', { name: 'View all principals' })
        .getAttribute('href'),
    ).toBe('/principals?sort=active');

    const showAll = within(group).getByRole('button', {
      name: 'Show all 30 principals',
    });
    expect(showAll.getAttribute('aria-expanded')).toBe('false');
    fireEvent.click(showAll);

    rows = within(group).getAllByTestId('top-principal-row');
    expect(rows).toHaveLength(30);
    expect(rows[29]?.textContent).toContain('Team 30');
    const filter = within(group).getByRole('searchbox', {
      name: 'Filter principals by name',
    });
    fireEvent.change(filter, { target: { value: 'team 2' } });
    expect(
      within(group)
        .getAllByTestId('top-principal-row')
        .map((row) => within(row).getByRole('link').textContent),
    ).toEqual([
      'Team 20',
      'Team 21',
      'Team 22',
      'Team 23',
      'Team 24',
      'Team 25',
      'Team 26',
      'Team 27',
      'Team 28',
      'Team 29',
    ]);
    fireEvent.change(filter, { target: { value: 'zzz' } });
    expect(within(group).queryAllByTestId('top-principal-row')).toHaveLength(0);
    expect(within(group).getByText('No principals match "zzz"')).toBeDefined();

    const showTop = within(group).getByRole('button', { name: 'Show top 10' });
    expect(showTop.getAttribute('aria-expanded')).toBe('true');
    fireEvent.click(showTop);
    expect(within(group).getAllByTestId('top-principal-row')).toHaveLength(10);
    expect(
      within(group).queryByRole('searchbox', {
        name: 'Filter principals by name',
      }),
    ).toBeNull();
  });

  // Mounts the whole page with 30 rows of meters and popovers: slow in jsdom.
  it('sorts principals from the Requests, Tokens and Cost headers', {
    timeout: 30_000,
  }, () => {
    mockManyPrincipals();

    render(<OverviewPage />);

    const group = screen.getByRole('region', { name: 'Top principals' });
    const header = (name: string) =>
      within(group).getByRole('button', { name }).closest('th')!;
    const firstRow = () =>
      within(within(group).getAllByTestId('top-principal-row')[0]!).getByRole(
        'link',
      ).textContent;

    expect(header('Cost').getAttribute('aria-sort')).toBe('descending');
    expect(header('Requests').getAttribute('aria-sort')).toBe('none');
    expect(header('Tokens').getAttribute('aria-sort')).toBe('none');
    expect(firstRow()).toBe('Team 01');

    fireEvent.click(within(group).getByRole('button', { name: 'Requests' }));
    expect(header('Requests').getAttribute('aria-sort')).toBe('descending');
    expect(header('Cost').getAttribute('aria-sort')).toBe('none');
    expect(firstRow()).toBe('Team 30');

    fireEvent.click(within(group).getByRole('button', { name: 'Requests' }));
    expect(header('Requests').getAttribute('aria-sort')).toBe('ascending');
    expect(firstRow()).toBe('Team 01');

    fireEvent.click(within(group).getByRole('button', { name: 'Tokens' }));
    expect(header('Tokens').getAttribute('aria-sort')).toBe('descending');
    expect(header('Requests').getAttribute('aria-sort')).toBe('none');
    expect(firstRow()).toBe('Team 15');

    fireEvent.click(within(group).getByRole('button', { name: 'Cost' }));
    expect(header('Cost').getAttribute('aria-sort')).toBe('descending');
    expect(firstRow()).toBe('Team 01');
  });

  it('lists only principals with requests and needs no expander for a short list', () => {
    mockResolvedKpiQueries();
    render(<OverviewPage />);

    const group = screen.getByRole('region', { name: 'Top principals' });
    expect(within(group).getAllByTestId('top-principal-row')).toHaveLength(1);
    expect(within(group).queryByRole('button', { name: /^Show / })).toBeNull();
    expect(within(group).queryByText(/had no requests/)).toBeNull();
  });

  it('keeps the principal section mounted across loading, empty and loaded states', () => {
    const { rerender } = render(
      <TopPrincipalsSection loading principals={[]} rangeWords="last 24h" />,
    );

    const section = screen.getByRole('region', { name: 'Top principals' });
    expect(screen.getAllByTestId('top-principal-skeleton-row')).toHaveLength(5);
    expect(screen.queryByText(/Loading/)).toBeNull();
    expect(screen.getByText('Ranked by virtual cost · last 24h')).toBeDefined();

    rerender(
      <TopPrincipalsSection
        loading={false}
        principals={[]}
        rangeWords="last 24h"
      />,
    );

    expect(screen.getByRole('region', { name: 'Top principals' })).toBe(
      section,
    );
    expect(screen.getByText('No usage in the last 24h')).toBeDefined();

    rerender(
      <TopPrincipalsSection
        loading={false}
        principals={[
          {
            cache_hit_ratio: null,
            cost_components_micros: null,
            cost_micros: 1_250_000,
            id: 'principal-1',
            name: 'Primary principal',
            requests: 12,
            share_pct: 100,
            tokens: 345,
          },
        ]}
        rangeWords="last 24h"
      />,
    );

    expect(screen.getByRole('region', { name: 'Top principals' })).toBe(
      section,
    );
    expect(screen.queryAllByTestId('top-principal-skeleton-row')).toHaveLength(
      0,
    );
    expect(screen.getByText('Primary principal')).toBeDefined();
  });

  it('renders pending Overview KPIs and principals without fake values or loading text', () => {
    mockPendingOverviewQueries();

    render(<OverviewPage />);

    for (const label of [
      'Requests/s',
      'Tokens',
      'Cost at list price',
      'Avg latency',
      'Error rate',
    ]) {
      const tile = within(screen.getByTestId('overview-kpi-strip'))
        .getByText(label)
        .closest('[data-slot="kpi-tile"]');
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
    'keeps the pool chart structured while the %s query is pending',
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
      // Pending is not "no reading": nothing claims the pool is empty.
      expect(card.textContent).not.toContain('No reading');
      expect(card.textContent).not.toContain('No timeline data yet');
      const legendSlots = card.querySelectorAll(
        '[data-testid="pool-quota-legend-slot"]',
      );
      expect(
        Array.from(legendSlots, (slot) => slot.textContent?.trim()),
      ).toEqual(['5h', '7d', '7d (Fable)']);
      for (const slot of legendSlots) {
        expect(slot.querySelector('.skeleton')).not.toBeNull();
      }

      const chartSlot = screen.getByTestId('pool-quota-chart-slot');
      expect(chartSlot.className).toContain('lg:h-[320px]');
      expect(chartSlot.querySelector('.skeleton')).not.toBeNull();
    },
  );

  it('keeps the pool chart mounted when pending resolves to legitimate no-data', () => {
    mockPendingOverviewQueries();

    const { rerender } = render(<OverviewPage />);
    const pendingCard = screen.getByTestId('pool-quota-card');
    const pendingChartSlot = screen.getByTestId('pool-quota-chart-slot');

    mockResolvedEmptyQuotaQueries();
    rerender(<OverviewPage />);

    const resolvedCard = screen.getByTestId('pool-quota-card');
    expect(resolvedCard).toBe(pendingCard);
    expect(resolvedCard.getAttribute('aria-busy')).toBe('false');
    const legendSlots = resolvedCard.querySelectorAll(
      '[data-testid="pool-quota-legend-slot"]',
    );
    expect(legendSlots).toHaveLength(3);
    for (const slot of legendSlots) {
      expect(slot.textContent).toContain('No reading');
    }
    expect(screen.getByTestId('pool-quota-chart-slot')).toBe(pendingChartSlot);
    expect(resolvedCard.querySelectorAll('.skeleton')).toHaveLength(0);
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
      ).toBe('5h · 20% used · resets in 1h');
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
      ).toBe('5h · 20% used · resets in 1h');
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
      ).toBe('5h · 55% used · resets in 1h');
      expect(screen.getByTestId('top-principal-row')).toBe(principalRow);
      expect(principalRow.textContent).toContain('$9.00');
    } finally {
      vi.useRealTimers();
    }
  });

  it('drives every usage query from one range and keeps the pool plot context while 1h expands to 7d', () => {
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

    expect(screen.getAllByRole('radiogroup')).toHaveLength(1);
    fireEvent.click(rangeOption('Usage range', '1h'));
    // The one control drives every usage query, in the URL, and keeps the
    // reader's place: the router must not reset the scroll to the top.
    expect(routerMock.navigate).toHaveBeenLastCalledWith(
      expect.objectContaining({ replace: true, resetScroll: false }),
    );
    expect(routerMock.search).toEqual({ range: '1h' });
    expect(vi.mocked(queries.useSummary)).toHaveBeenLastCalledWith('1h');
    expect(vi.mocked(queries.useUsage)).toHaveBeenLastCalledWith(
      '1h',
      'minute',
      'principal',
      undefined,
      'totals',
    );
    expect(
      vi.mocked(queries.useSubscriptionQuotaPoolHistory),
    ).toHaveBeenLastCalledWith(expect.objectContaining({ rangeSecs: 3600 }));
    // Every inner subtitle ends with the control's words.
    for (const subtitle of [
      'Used per window across the pool · last 1h',
      'Requests, tokens, cost, latency and errors · last 1h',
      'Ranked by virtual cost · last 1h',
    ]) {
      expect(screen.getByText(subtitle)).toBeDefined();
    }

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

    expect(
      screen.getByText('Used per window across the pool · last 1h'),
    ).toBeDefined();
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

    const sevenDayOption = rangeOption('Usage range', '7d');
    fireEvent.click(sevenDayOption);

    expect(sevenDayOption.getAttribute('aria-checked')).toBe('true');
    expect(
      screen.getByText('Used per window across the pool · last 7d'),
    ).toBeDefined();
    expect(screen.getByText('Ranked by virtual cost · last 7d')).toBeDefined();
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

    expect(
      screen.getByText('Used per window across the pool · last 7d'),
    ).toBeDefined();
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
    ).toBe('5h · 55% used · resets in 1h');
    expect(screen.getByTestId('top-principal-row')).toBe(principalRow);
  });

  it('renders the first 50 latest requests and pages through retained history', () => {
    mockResolvedKpiQueries();
    const history = Array.from({ length: 500 }, (_, index) => ({
      duration_ms: 10,
      event_kind: 'messages' as const,
      model: 'test-model',
      request_id: `request-${500 - index}`,
      status: 200,
      ts: 500 - index,
    }));
    mockRequestEventsFeed(history);
    const { rerender } = render(<OverviewPage />);

    const table = screen
      .getByLabelText('View request request-500')
      .closest('table');
    if (table === null) throw new Error('Expected latest requests table');
    const rowIds = () =>
      Array.from(
        table.querySelectorAll<HTMLTableRowElement>(
          'tbody tr[aria-label^="View request "]',
        ),
        (row) => row.getAttribute('aria-label'),
      );
    const historyPages = Array.from({ length: 10 }, (_, page) =>
      history
        .slice(page * 50, (page + 1) * 50)
        .map((event) => `View request ${event.request_id}`),
    );
    expect(rowIds()).toEqual(historyPages[0]);
    const pagination = screen.getByRole('navigation', {
      name: 'Log pagination',
    });
    const previousButton = within(pagination).getByRole('button', {
      name: 'Previous page',
    }) as HTMLButtonElement;
    const nextButton = within(pagination).getByRole('button', {
      name: 'Next page',
    }) as HTMLButtonElement;
    expect(previousButton.disabled).toBe(true);
    expect(nextButton.disabled).toBe(false);
    expect(
      screen.getByRole('link', { name: 'Open logs' }).getAttribute('href'),
    ).toBe('/logs');

    for (let page = 1; page < historyPages.length; page += 1) {
      fireEvent.click(nextButton);
      expect(rowIds()).toEqual(historyPages[page]);
    }

    const liveMap: LiveEventMap = new Map([
      ['live-501', { phase: 'final', event: finalLiveEvent(501) }],
    ]);
    mockRequestEventsFeed(history, { liveEventsMap: liveMap, version: 1 });
    rerender(<OverviewPage />);

    expect(rowIds()).toEqual(historyPages[9]);
    for (let page = 8; page >= 0; page -= 1) {
      fireEvent.click(previousButton);
      expect(rowIds()).toEqual(
        page === 0
          ? ['View request live-501', ...historyPages[0].slice(0, 49)]
          : historyPages[page],
      );
    }
    fireEvent.click(nextButton);
    expect(rowIds()).toEqual(
      history.slice(49, 99).map((event) => `View request ${event.request_id}`),
    );
    expect(queries.recentEventsPageQueryOptions).not.toHaveBeenCalled();
  });

  it('collapses the traffic strip to one line when the range has no requests', () => {
    mockResolvedKpiQueries();
    vi.mocked(queries.useSummary).mockReturnValue({
      data: {
        ...SUMMARY_FIXTURE,
        totals: { ...SUMMARY_FIXTURE.totals, request_count: 0 },
      },
      isPending: false,
    } as never);

    render(<OverviewPage />);

    const strip = screen.getByTestId('overview-kpi-strip');
    expect(strip.textContent).toBe('No requests in the last 24h');
    expect(strip.querySelector('[data-slot="kpi-tile"]')).toBeNull();
  });

  it('requests only messages events and shows no other categories from mixed data', () => {
    mockResolvedKpiQueries();
    const historical = {
      duration_ms: 10,
      event_kind: 'messages' as const,
      model: 'historical-model',
      request_id: 'historical-1',
      status: 200,
      ts: 5,
    };
    const liveMap: LiveEventMap = new Map([
      ['live-1', { phase: 'final', event: finalLiveEvent(1) }],
      [
        'live-2',
        {
          phase: 'final',
          event: { ...finalLiveEvent(2), source_kind: 'renewal' },
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
    mockRequestEventsFeed([historical], { liveEventsMap: liveMap, version: 4 });

    render(<OverviewPage />);

    expect(queries.useRecentEventsPage).toHaveBeenCalledWith(
      { event_kind: 'messages' },
      { kind: 'initial', limit: 500 },
    );
    expect(screen.getByLabelText('View request live-1')).toBeDefined();
    expect(screen.getByLabelText('View request historical-1')).toBeDefined();
    expect(screen.queryByLabelText('View request live-2')).toBeNull();
    expect(screen.queryByLabelText('View request live-3')).toBeNull();
    expect(screen.queryByLabelText('View request live-4')).toBeNull();
  });

  it('keeps live rows while the shared feed marks only new arrivals', () => {
    mockResolvedKpiQueries();
    const liveMap: LiveEventMap = new Map([
      ['live-1', { phase: 'final', event: finalLiveEvent(1) }],
    ]);
    mockRequestEventsFeed([], { liveEventsMap: liveMap, version: 1 });
    const { rerender } = render(<OverviewPage />);
    const initialChartRenderCount = rechartsMock.areaChartRenderCount;

    liveMap.set('live-2', {
      phase: 'final',
      event: finalLiveEvent(2),
    });
    mockRequestEventsFeed([], { liveEventsMap: liveMap, version: 2 });
    rerender(<OverviewPage />);

    expect(rechartsMock.areaChartRenderCount).toBe(initialChartRenderCount);
    expect(screen.getByLabelText('View request live-1')).toBeDefined();
    expect(screen.getByLabelText('View request live-2').className).toContain(
      'flash-in',
    );

    liveMap.set('live-1', {
      phase: 'final',
      event: { ...finalLiveEvent(1), status: 201 },
    });
    mockRequestEventsFeed([], { liveEventsMap: liveMap, version: 3 });
    rerender(<OverviewPage />);

    expect(
      screen.getByLabelText('View request live-1').className,
    ).not.toContain('flash-in');
    expect(
      screen.getByLabelText('View request live-2').className,
    ).not.toContain('flash-in');
  });
});

describe('Overview KPI details', () => {
  it('renders cache averages and synchronizes all KPI tooltips by bucket index', () => {
    mockResolvedKpiQueries();
    render(<OverviewPage />);

    // Tokens is the total of every token kind; cache miss is the range's
    // (fresh input + cache writes) / prompt tokens: 800 / 1,800.
    const tokensTile = screen.getByTestId('overview-kpi-tokens');
    // The tile reads compact (`2.0k`, same unit as the token tables); the
    // exact figure rides along for screen readers and hover.
    const tokensValue = tokensTile.querySelector(
      '[data-slot="value"] > span',
    ) as HTMLElement;
    expect(tokensValue.textContent).toContain('2.0k');
    expect(tokensValue.title).toBe('1,980 tokens');
    expect(tokensValue.textContent).toContain('1,980 tokens');
    expect(screen.getByTestId('overview-kpi-tokens-legend').textContent).toBe(
      'Tokens 2.0kCache miss 44.4%',
    );
    // The legend carries the cache-miss figure; no second line repeats it.
    expect(tokensTile.querySelector('[data-slot="sub"]')).toBeNull();
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
    // Bucket 1: 300 + 100 + 100 + 400 tokens; (300 + 100) / 800 missed.
    expect(tooltipRows('overview-kpi-tooltip-tokens')).toEqual([
      tooltipTimestamp,
      '900 tokens',
      '50.0% cache miss',
    ]);
    expect(tooltipRows('overview-kpi-tooltip-cost')).toEqual([
      tooltipTimestamp,
      'Cost $2.50',
    ]);
    expect(tooltipRows('overview-kpi-tooltip-latency')).toEqual([
      tooltipTimestamp,
      'Avg latency 200ms',
    ]);
    expect(tooltipRows('overview-kpi-tooltip-error-rate')).toEqual([
      tooltipTimestamp,
      'Error rate 10.00%',
    ]);

    fireEvent.mouseLeave(requestChart);
    expect(screen.queryAllByTestId(/^overview-kpi-tooltip-/)).toHaveLength(0);
  });

  it('updates cache metrics when resolved query data changes', () => {
    mockResolvedKpiQueries();
    const { rerender } = render(<OverviewPage />);

    const legend = () =>
      screen.getByTestId('overview-kpi-tokens-legend').textContent;
    expect(legend()).toContain('Cache miss 44.4%');

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
      '50.0% cache miss',
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

    // 600 / 2,000 over the range; bucket 1 is (100 + 100) / 1,000.
    expect(legend()).toContain('Cache miss 30.0%');
    expect(legend()).not.toContain('44.4%');
    expect(tooltipRows('overview-kpi-tooltip-tokens').slice(1)).toEqual([
      '1.1k tokens',
      '20.0% cache miss',
    ]);
  });

  it('renders dashes when cache ratios have a zero prompt denominator', () => {
    mockResolvedKpiQueries({ zeroPromptDenominator: true });
    render(<OverviewPage />);

    expect(
      screen.getByTestId('overview-kpi-tokens-legend').textContent,
    ).toContain('Cache miss —');

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
      '100 tokens',
      '— cache miss',
    ]);
  });
});

describe('Overview OAuth reconnect summary', () => {
  const oauthUpstream: Upstream = {
    id: 'oauth-1',
    name: 'OAuth Primary',
    kind: 'anthropic_oauth',
    enabled: true,
    spec_revision: 1,
    base_url: null,
    api_key_env: null,
    warmup_enabled: false,
    warmup_dialect_plugin: null,
    status: {
      last_apply_error: null,
      last_apply_at_unix_secs: null,
      last_warmup_at_unix_secs: null,
    },
  };

  it('warns instead of going silent when the OAuth status check fails', async () => {
    mockResolvedKpiQueries();
    vi.mocked(queries.useUpstreams).mockReturnValue({
      data: { upstreams: [oauthUpstream] },
      isPending: false,
      isPlaceholderData: false,
    } as never);

    render(<OverviewPage />);

    // jsdom cannot reach the status endpoint, so the real nudge query errors
    // and the summary must surface that rather than rendering nothing.
    expect(await screen.findByText('OAuth status check failed')).toBeDefined();
    expect(
      screen.getByTestId('overview-kpi-tokens-legend').textContent,
    ).toContain('Cache miss 44.4%');
  });
});

const COST_NOTE = 'Per-category cost not recorded for this window';

const COMPLETE_PRINCIPAL: TopPrincipal = {
  cache_hit_ratio: 0.875,
  cost_components_micros: {
    input: 400_000,
    output: 300_000,
    cache_create_5m: 100_000,
    cache_create_1h: 100_000,
    cache_read: 100_000,
  },
  cost_micros: 1_000_000,
  id: 'principal-complete',
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
  cost_components_micros: null,
  cost_micros: 500_000,
  id: 'principal-unrecorded',
  name: 'Unrecorded principal',
};

function costTrigger(name: string): HTMLButtonElement {
  return screen.getByRole<HTMLButtonElement>('button', {
    name: new RegExp(`^${name} cost .*, show breakdown$`),
  });
}

/** The cost bar's drawn segments as `color width` pairs, in paint order. */
function costBar(trigger: HTMLElement): string[] {
  return Array.from(
    trigger.querySelectorAll<HTMLElement>('[data-cell-bar] span'),
    (segment) => `${segment.style.backgroundColor} ${segment.style.width}`,
  );
}

function costTrackWidth(trigger: HTMLElement): string {
  return (
    trigger.querySelector<HTMLElement>('[data-cell-bar] > div')?.style.width ??
    ''
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
    <TopPrincipalsSection
      loading={false}
      principals={principals}
      rangeWords="last 24h"
    />,
  );
}

describe('Top principal cost breakdown and cache hit', () => {
  it('reads requests, tokens, cache hit, cost and share in one row', () => {
    renderPrincipals([COMPLETE_PRINCIPAL]);

    const cells = within(screen.getByTestId('top-principal-row')).getAllByRole(
      'cell',
    );
    expect(within(cells[0]!).getByRole('link').textContent).toBe(
      'Complete principal',
    );
    expect(cells.slice(1).map((cell) => cell.textContent)).toEqual([
      '12',
      '345',
      '87%',
      '$1.00',
      '62.5%',
    ]);
  });

  it('keeps principal costs at two decimals while exposing exact micros', () => {
    renderPrincipals([
      {
        ...COMPLETE_PRINCIPAL,
        id: 'cost-zero',
        name: 'Zero cost',
        cost_micros: 0,
      },
      {
        ...COMPLETE_PRINCIPAL,
        id: 'cost-integer',
        name: 'Integer cost',
        cost_micros: 12_000_000,
      },
      {
        ...COMPLETE_PRINCIPAL,
        id: 'cost-fraction',
        name: 'Fraction cost',
        cost_micros: 12_340_000,
      },
      {
        ...UNRECORDED_PRINCIPAL,
        id: 'cost-subcent',
        name: 'Sub-cent cost',
        cost_micros: 1_234,
      },
      {
        ...COMPLETE_PRINCIPAL,
        id: 'cost-large',
        name: 'Large cost',
        cost_micros: 12_345_678_900_000,
      },
    ]);

    expect(costTrigger('Zero cost').textContent).toContain('$0.00');
    expect(costTrigger('Integer cost').textContent).toContain('$12.00');
    expect(costTrigger('Fraction cost').textContent).toContain('$12.34');
    expect(costTrigger('Sub-cent cost').textContent).toContain('$0.00');
    expect(costTrigger('Large cost').textContent).toContain('$12,345,678.90');
    expect(costTrigger('Sub-cent cost').getAttribute('aria-label')).toContain(
      '$0.001234',
    );

    fireEvent.click(costTrigger('Sub-cent cost'));
    expect(costDetailValues()).toEqual(['Total', '$0.001234']);
  });

  it('keeps compact token units aligned and categorically distinct', () => {
    renderPrincipals([
      {
        ...COMPLETE_PRINCIPAL,
        id: 'tokens-k',
        name: 'Tokens k',
        tokens: 1_000,
      },
      {
        ...COMPLETE_PRINCIPAL,
        id: 'tokens-m',
        name: 'Tokens M',
        tokens: 1_000_000,
      },
      {
        ...COMPLETE_PRINCIPAL,
        id: 'tokens-b',
        name: 'Tokens B',
        tokens: 1_000_000_000,
      },
    ]);

    const unitStyle = (name: string, unit: string) => {
      const row = screen
        .getByRole('link', { name })
        .closest<HTMLElement>('[data-testid="top-principal-row"]');
      if (!row) throw new Error(`Missing top principal row for ${name}`);
      const tokenCell = within(row).getAllByRole('cell')[2]!;
      return Array.from(tokenCell.querySelectorAll('span'))
        .find((span) => span.textContent === unit)
        ?.getAttribute('style');
    };

    expect(unitStyle('Tokens k', 'k')).toBeTruthy();
    expect(unitStyle('Tokens M', 'M')).toBeTruthy();
    expect(unitStyle('Tokens B', 'B')).toBeTruthy();
    const colors = [
      unitStyle('Tokens k', 'k'),
      unitStyle('Tokens M', 'M'),
      unitStyle('Tokens B', 'B'),
    ];
    expect(colors[0]).not.toBe(colors[1]);
    expect(colors[0]).not.toBe(colors[2]);
    expect(colors[1]).not.toBe(colors[2]);
  });

  it('draws the cost bar in the request table categories, order and colors', () => {
    renderPrincipals([COMPLETE_PRINCIPAL]);

    expect(costBar(costTrigger('Complete principal'))).toEqual([
      'var(--color-series-cache-read) 10%',
      'var(--color-series-cache-create-5m) 10%',
      'var(--color-series-cache-create-1h) 10%',
      'var(--color-series-input) 40%',
      'var(--color-series-output) 30%',
    ]);
  });

  it('scales relative cost bars to the highest active principal', () => {
    renderPrincipals([
      { ...COMPLETE_PRINCIPAL, name: 'Highest', cost_micros: 1_000_000 },
      {
        ...PARTIAL_PRINCIPAL,
        name: 'Half',
        cost_micros: 500_000,
        cost_components_micros: {
          ...PARTIAL_PRINCIPAL.cost_components_micros!,
          input: 100_000,
          output: 100_000,
          cache_read: 300_000,
        },
      },
      {
        ...UNRECORDED_PRINCIPAL,
        name: 'Zero',
        cost_micros: 0,
        cost_components_micros: {
          input: 0,
          output: 0,
          cache_create_5m: 0,
          cache_create_1h: 0,
          cache_read: 0,
        },
      },
    ]);

    expect(costTrackWidth(costTrigger('Highest'))).toBe('100%');
    expect(costTrackWidth(costTrigger('Half'))).toBe('50%');
    expect(costTrackWidth(costTrigger('Zero'))).toBe('');
    expect(costBar(costTrigger('Half'))).toEqual([
      'var(--color-series-cache-read) 60%',
      'var(--color-series-input) 20%',
      'var(--color-series-output) 20%',
    ]);

    fireEvent.click(screen.getByRole('button', { name: 'Requests' }));
    expect(costTrackWidth(costTrigger('Half'))).toBe('50%');
  });

  it('opens the exact breakdown from the keyboard', async () => {
    const user = userEvent.setup();
    renderPrincipals([COMPLETE_PRINCIPAL]);
    const trigger = costTrigger('Complete principal');

    expect(trigger.getAttribute('aria-label')).toBe(
      'Complete principal cost $1.0000, show breakdown',
    );
    expect(screen.queryByTestId('top-principal-cost-details')).toBeNull();
    trigger.focus();
    await user.keyboard('{Enter}');

    expect(costDetailValues()).toEqual([
      'Cache read',
      '$0.1000',
      '10%',
      'Cache create 5m',
      '$0.1000',
      '10%',
      'Cache create 1h',
      '$0.1000',
      '10%',
      'Input',
      '$0.4000',
      '40%',
      'Output',
      '$0.3000',
      '30%',
      'Total',
      '$1.0000',
    ]);
  });

  it('opens the same breakdown after the hover delay', () => {
    vi.useFakeTimers();
    try {
      renderPrincipals([COMPLETE_PRINCIPAL]);
      fireEvent.pointerEnter(costTrigger('Complete principal'));

      act(() => vi.advanceTimersByTime(199));
      expect(screen.queryByTestId('top-principal-cost-details')).toBeNull();

      act(() => vi.advanceTimersByTime(1));
      expect(costDetailValues()).toContain('Cache create 5m');
      expect(costDetailValues()).toContain('$1.0000');
    } finally {
      vi.useRealTimers();
    }
  });

  it('draws and lists an unattributed remainder when the total outruns the categories', () => {
    renderPrincipals([PARTIAL_PRINCIPAL]);
    const trigger = costTrigger('Partial principal');

    expect(costBar(trigger)).toEqual([
      'var(--color-series-cache-read) 25%',
      'var(--color-series-input) 25%',
      'var(--color-series-output) 25%',
      'var(--color-neutral) 25%',
    ]);
    fireEvent.click(trigger);
    expect(costDetailValues()).toEqual([
      'Cache read',
      '$0.2000',
      '25%',
      'Input',
      '$0.2000',
      '25%',
      'Output',
      '$0.2000',
      '25%',
      'Unattributed',
      '$0.2000',
      '25%',
      'Total',
      '$0.8000',
    ]);
  });

  it('leaves bare track and says so when no category cost was recorded', () => {
    renderPrincipals([UNRECORDED_PRINCIPAL]);
    const trigger = costTrigger('Unrecorded principal');

    expect(costBar(trigger)).toEqual([]);
    fireEvent.click(trigger);
    expect(screen.getByText(COST_NOTE)).toBeDefined();
    expect(costDetailValues()).toEqual(['Total', '$0.5000']);
  });

  it('rounds the cache hit down and shows a dash without prompt tokens', () => {
    renderPrincipals([
      { ...COMPLETE_PRINCIPAL, cache_hit_ratio: 0.999 },
      {
        ...PARTIAL_PRINCIPAL,
        cache_hit_ratio: null,
      },
    ]);
    const [cached, uncached] = screen.getAllByTestId('top-principal-row');

    // A partial hit never reads as a full one.
    expect(within(cached!).getAllByRole('cell')[3]!.textContent).toBe('99%');
    expect(within(uncached!).getAllByRole('cell')[3]!.textContent).toBe(
      '—No prompt tokens',
    );
    // Phones fold the figure into the name cell's second line.
    expect(
      cached!.querySelector('[data-slot="principal-cache-hit"]')!.textContent,
    ).toBe('99% cache hit');
    expect(
      uncached!.querySelector('[data-slot="principal-cache-hit"]')!.textContent,
    ).toBe('No prompt tokens');
  });

  it('sorts by cache hit with principals that sent no prompt tokens last either way', () => {
    renderPrincipals([
      { ...COMPLETE_PRINCIPAL, id: 'a', name: 'Low', cache_hit_ratio: 0.2 },
      { ...COMPLETE_PRINCIPAL, id: 'b', name: 'None', cache_hit_ratio: null },

      { ...COMPLETE_PRINCIPAL, id: 'c', name: 'High', cache_hit_ratio: 0.9 },
    ]);
    const order = () =>
      screen
        .getAllByTestId('top-principal-row')
        .map((row) => within(row).getByRole('link').textContent);
    const header = () =>
      screen.getByRole('button', { name: 'Cache hit' }).closest('th')!;

    fireEvent.click(screen.getByRole('button', { name: 'Cache hit' }));
    expect(header().getAttribute('aria-sort')).toBe('descending');
    expect(order()).toEqual(['High', 'Low', 'None']);

    fireEvent.click(screen.getByRole('button', { name: 'Cache hit' }));
    expect(header().getAttribute('aria-sort')).toBe('ascending');
    expect(order()).toEqual(['Low', 'High', 'None']);
  });

  it('aggregates bucket components and prompt tokens and follows refreshed usage data', () => {
    mockResolvedKpiQueries();
    const { rerender } = render(<OverviewPage />);
    const row = screen.getByTestId('top-principal-row');

    // Cache read 2,000 of 2,400 prompt tokens over the two buckets.
    expect(within(row).getAllByRole('cell')[3]!.textContent).toBe('83%');
    // $1.00 of the window's $3.00 carries components; the pre-upgrade bucket's
    // $2.00 stays unattributed instead of being spread over the categories.
    fireEvent.click(costTrigger('Principal Alpha'));
    expect(costDetailValues()).toEqual([
      'Cache read',
      '$0.1500',
      '5%',
      'Cache create 5m',
      '$0.1500',
      '5%',
      'Input',
      '$0.3000',
      '10%',
      'Output',
      '$0.4000',
      '13%',
      'Unattributed',
      '$2.0000',
      '67%',
      'Total',
      '$3.0000',
    ]);
    // 1,100 + 1,500 tokens over the two buckets.
    expect(row.textContent).toContain('2.6k');

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
      'Cache create 5m',
      '$0.1000',
      '3%',
      'Cache create 1h',
      '$0.1000',
      '3%',
      'Input',
      '$0.5000',
      '17%',
      'Output',
      '$0.3000',
      '10%',
      'Unattributed',
      '$2.0000',
      '67%',
      'Total',
      '$3.0000',
    ]);
    expect(screen.getByTestId('top-principal-row').textContent).toContain(
      '2.6k',
    );
  });
});
