import { cleanup, render, screen } from '@testing-library/react';
import type { ReactNode } from 'react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import type { PoolRetentionRow } from '../components/upstreams/quotaWindowVisibility';
import type {
  AggregateProviderLotResponse,
  AggregateResponse,
  AggregateWindowResponse,
  PoolHistoryWindowResponse,
  QuotaSnapshot,
} from '../lib/api';
import * as queries from '../lib/queries';
import { successQuery } from '../lib/test-utils/hook-results';
import { ProviderPoolCard } from './ProviderPoolCard';

const NOW = 1_700_000_000;
const T1 = NOW - 7200;

vi.mock('@tanstack/react-router', async () => ({
  ...(await vi.importActual('@tanstack/react-router')),
  Link: ({ children }: { children?: ReactNode }) => <a href="/">{children}</a>,
}));
vi.mock('../lib/queries', async () => ({
  ...(await vi.importActual('../lib/queries')),
  useSubscriptionQuotaAggregate: vi.fn(),
  useSubscriptionQuotaPoolHistory: vi.fn(),
}));
// Each stroke renders its plotted points as `unix:value|…` so the test reads
// exactly what the chart was given.
const chart = vi.hoisted(() => ({
  data: [] as readonly Record<string, unknown>[],
}));
vi.mock('recharts', () => ({
  AreaChart: ({
    children,
    data = [],
  }: {
    children?: ReactNode;
    data?: readonly Record<string, unknown>[];
  }) => {
    chart.data = data;
    return <svg>{children}</svg>;
  },
  Area: ({ dataKey, stroke }: { dataKey: string; stroke?: string }) =>
    stroke === 'none' ? null : (
      <path
        data-testid={`pool-stroke-${dataKey}`}
        d={chart.data
          .filter((row) => row[dataKey] != null)
          .map((row) => `${String(row.unix)}:${String(row[dataKey])}`)
          .join('|')}
      />
    ),
  XAxis: () => null,
  YAxis: () => null,
  ResponsiveContainer: ({ children }: { children?: ReactNode }) => children,
  CartesianGrid: () => null,
  ReferenceArea: () => null,
  ReferenceLine: () => null,
  Tooltip: () => null,
}));

afterEach(cleanup);

function lot(
  window: string,
  upstream: string,
  observedAtUnixSecs: number,
  state = 'fresh',
): AggregateProviderLotResponse {
  return {
    upstream_id: upstream,
    upstream_name: upstream,
    window,
    source: 'api',
    state,
    provider_start_unix_secs: null,
    provider_reset_unix_secs: NOW + 3600,
    observed_at_unix_millis: observedAtUnixSecs * 1000,
    utilization: 0.5,
    capacity_estimate_tokens: 1000,
    used_before_cc_window_tokens: 0,
    capacity_to_now_tokens_estimate: 500,
    projected_capacity_tokens_estimate: 1000,
    confidence: 'high',
    capacity_ratio: 1,
  };
}

function aggregateWindow(
  window: string,
  lots: AggregateProviderLotResponse[],
): AggregateWindowResponse {
  return {
    window,
    cc_window_start_unix_secs: NOW - 3600,
    cc_window_reset_unix_secs: NOW + 3600,
    pool_pace_fraction: 0.5,
    next_reset_unix_secs: NOW + 3600,
    used_tokens: 100,
    utilization: 0.5,
    utilization_percent: 50,
    capacity_to_now_tokens_estimate: 500,
    projected_capacity_tokens_estimate: 1000,
    remaining_to_now_tokens_estimate: 500,
    confidence: 'high',
    contributing_upstreams: lots.length,
    stale_upstreams: 0,
    missing_capacity_upstreams: 0,
    provider_lots: lots,
    caveats: [],
  };
}

function historyWindow(
  window: string,
  points: readonly (readonly [number, number])[],
): PoolHistoryWindowResponse {
  const last = points.at(-1);
  return {
    window,
    latest: {
      snapshot_at_unix_secs: NOW,
      utilization: (last?.[1] ?? 0) / 100,
      utilization_percent: last?.[1] ?? null,
      contributing_upstreams: 2,
      eligible_upstreams: 2,
      stale_upstreams: 0,
      max_observed_at_unix_millis: NOW * 1000,
      oldest_observed_at_unix_millis: (NOW - 30) * 1000,
      max_staleness_secs: 300,
      missing_observation_upstreams: 0,
    },
    series: points.map(([snapshot_at_unix_secs, utilization_percent]) => ({
      snapshot_at_unix_secs,
      utilization_percent,
    })),
  };
}

/** One upstream's `/latest` row: 5h and weekly observation times, raw meters. */
function upstreamRow(
  id: string,
  [fiveHourAt, weeklyAt]: readonly [number, number],
  rawMeterAt?: number,
): PoolRetentionRow {
  const snapshot = (window: QuotaSnapshot['window'], at: number) => ({
    window,
    state: 'fresh' as const,
    source: 'api',
    utilization: 0.5,
    status: null,
    resets_at_unix_secs: NOW + 3600,
    surpassed_threshold: null,
    representative_claim: null,
    disabled_reason: null,
    extra_usage_enabled: null,
    extra_usage_monthly_limit: null,
    extra_usage_used_credits: null,
    observed_at_unix_millis: at * 1000,
    age_secs: NOW - at,
  });
  return {
    upstream: { id },
    windows: [
      snapshot('gemini_5h', fiveHourAt),
      snapshot('gemini_7d', weeklyAt),
    ],
    periodUnknownMeters:
      rawMeterAt == null
        ? []
        : [
            {
              group: 'gemini',
              model_id: 'gemini-unlabeled',
              remaining_fraction: 0.5,
              utilization: 0.5,
              reset_time_unix_secs: null,
              origin: 'retrieve_user_quota_summary',
              source: 'api',
              state: 'fresh',
              observed_at_unix_millis: rawMeterAt * 1000,
              age_secs: NOW - rawMeterAt,
            },
          ],
  };
}

function renderCard({
  lots,
  history,
  rows,
  pending = false,
  error = false,
}: {
  lots: Record<'gemini_5h' | 'gemini_7d', AggregateProviderLotResponse[]>;
  history: Record<'gemini_5h' | 'gemini_7d', [number, number][]>;
  rows: PoolRetentionRow[];
  pending?: boolean;
  error?: boolean;
}) {
  const aggregate: AggregateResponse = {
    now_unix_secs: NOW,
    window_anchor_unix_secs: NOW,
    max_staleness_secs: 300,
    upstream_count: 2,
    caveats: [],
    windows: [
      aggregateWindow('gemini_5h', lots.gemini_5h),
      aggregateWindow('gemini_7d', lots.gemini_7d),
    ],
  };
  vi.mocked(queries.useSubscriptionQuotaAggregate).mockReturnValue(
    successQuery(aggregate),
  );
  vi.mocked(queries.useSubscriptionQuotaPoolHistory).mockReturnValue(
    successQuery({
      now_unix_secs: NOW,
      range_secs: 86_400,
      windows: [
        historyWindow('gemini_5h', history.gemini_5h),
        historyWindow('gemini_7d', history.gemini_7d),
      ],
    }),
  );
  render(
    <ProviderPoolCard
      compact
      provider="gemini"
      hasSubscription
      rangeSecs={86_400}
      retention={{ rows, pending, error }}
    />,
  );
  const legend = (window: string) =>
    screen
      .getAllByTestId('pool-quota-legend-slot')
      .find((row) => row.dataset.window === window);
  return { legend };
}

function expectStaleLegend(row: HTMLElement | undefined) {
  expect(row?.dataset.state).toBe('stale');
  expect(row?.querySelector('[data-slot="pace-marker"]')).toBeNull();
  expect(
    row?.querySelector('[data-slot="pool-quota-legend-reset"]')?.textContent,
  ).toBe('Stale');
}

describe('Gemini pool card with an account retaining an earlier reading', () => {
  const freshLots = {
    gemini_5h: [lot('gemini_5h', 'a', NOW), lot('gemini_5h', 'b', NOW)],
    gemini_7d: [lot('gemini_7d', 'a', NOW), lot('gemini_7d', 'b', NOW)],
  };
  // A's 5h lot is its retained t1 reading, as the server serves it.
  const retainedLots = {
    gemini_5h: [lot('gemini_5h', 'a', T1), lot('gemini_5h', 'b', NOW)],
    gemini_7d: freshLots.gemini_7d,
  };

  it('keeps plotting other upstreams after the retained reading while the legend marks it stale', () => {
    // Upstream A's gemini_5h is from t1 (its later poll omitted 5h); upstream B
    // keeps reporting gemini_5h, so the pool history has valid post-t1 points.
    const fiveHour: [number, number][] = [
      [T1, 20],
      [NOW - 3600, 35],
      [NOW - 1800, 40],
    ];
    const { legend } = renderCard({
      lots: retainedLots,
      history: {
        gemini_5h: fiveHour,
        gemini_7d: [
          [T1, 90],
          [NOW - 1800, 100],
        ],
      },
      rows: [upstreamRow('a', [T1, NOW]), upstreamRow('b', [NOW, NOW])],
    });
    const plotted = screen
      .getByTestId('pool-stroke-gemini_5h')
      .getAttribute('d')
      ?.split('|')
      .map((point) => Number(point.split(':')[0]));
    for (const [unix] of fiveHour) expect(plotted).toContain(unix);
    expectStaleLegend(legend('gemini_5h'));
    expect(legend('gemini_7d')?.dataset.state).toBeUndefined();
    expect(screen.getByText('Stale reading')).toBeDefined();
  });

  it('marks both windows stale when the later poll returned only raw meters', () => {
    // t1: 5h 20%, weekly 100%; t2 returned only an unlabeled meter, so both
    // canonical rows (and their aggregate lots) still carry t1.
    const { legend } = renderCard({
      lots: {
        gemini_5h: [lot('gemini_5h', 'a', T1)],
        gemini_7d: [lot('gemini_7d', 'a', T1)],
      },
      history: {
        gemini_5h: [[NOW - 1800, 20]],
        gemini_7d: [[NOW - 1800, 100]],
      },
      rows: [upstreamRow('a', [T1, T1], NOW)],
    });
    expectStaleLegend(legend('gemini_5h'));
    expectStaleLegend(legend('gemini_7d'));
    // No current figure remains for the compact headline.
    expect(
      screen.getByLabelText('Current pool usage unavailable').textContent,
    ).toBe('—');
  });

  it('ignores an account excluded from the fresh-only figure', () => {
    // A's 5h is stale (excluded) beside a newer weekly 10%; B is fresh: 5h 90%, weekly 10%.
    const { legend } = renderCard({
      lots: {
        gemini_5h: [
          lot('gemini_5h', 'a', T1, 'stale'),
          lot('gemini_5h', 'b', NOW),
        ],
        gemini_7d: [lot('gemini_7d', 'a', NOW), lot('gemini_7d', 'b', NOW)],
      },
      history: {
        gemini_5h: [[NOW - 1800, 90]],
        gemini_7d: [[NOW - 1800, 10]],
      },
      rows: [upstreamRow('a', [T1, NOW]), upstreamRow('b', [NOW, NOW])],
    });
    expect(legend('gemini_5h')?.dataset.state).toBeUndefined();
    expect(screen.queryByText('Stale reading')).toBeNull();
    expect(
      document.querySelector('[data-slot="provider-pool-headline"]')
        ?.textContent,
    ).toBe('90%used');
  });

  it('healthy control: one poll observed together is current', () => {
    const { legend } = renderCard({
      lots: freshLots,
      history: {
        gemini_5h: [[NOW - 1800, 40]],
        gemini_7d: [[NOW - 1800, 60]],
      },
      rows: [upstreamRow('a', [NOW, NOW], NOW), upstreamRow('b', [NOW, NOW])],
    });
    expect(legend('gemini_5h')?.dataset.state).toBeUndefined();
    expect(legend('gemini_7d')?.dataset.state).toBeUndefined();
    expect(screen.queryByText('Stale reading')).toBeNull();
  });

  it('treats a lot newer than the cached /latest reading as current', () => {
    // `/latest` still caches A's retained t1 5h (weekly at now), while
    // `/aggregate` already carries A's recovery poll: a fresh 5h of 90%.
    const { legend } = renderCard({
      lots: {
        gemini_5h: [lot('gemini_5h', 'a', NOW)],
        gemini_7d: [lot('gemini_7d', 'a', NOW)],
      },
      history: {
        gemini_5h: [[NOW - 1800, 90]],
        gemini_7d: [[NOW - 1800, 10]],
      },
      rows: [upstreamRow('a', [T1, NOW - 600])],
    });
    expect(legend('gemini_5h')?.dataset.state).toBeUndefined();
    expect(screen.queryByText('Stale reading')).toBeNull();
    expect(
      document.querySelector('[data-slot="provider-pool-headline"]')
        ?.textContent,
    ).toBe('90%used');
  });

  it('says retained readings went unchecked when /latest failed, without inventing stale', () => {
    const { legend } = renderCard({
      lots: retainedLots,
      history: {
        gemini_5h: [[NOW - 1800, 40]],
        gemini_7d: [[NOW - 1800, 60]],
      },
      rows: [upstreamRow('a', [T1, NOW]), upstreamRow('b', [NOW, NOW])],
      error: true,
    });
    expect(legend('gemini_5h')?.dataset.state).toBeUndefined();
    expect(screen.queryByText('Stale reading')).toBeNull();
    const note = screen.getByText("Couldn't check for retained readings");
    expect(note.className).not.toMatch(/warn|danger/);
  });

  it('flags nothing and stays quiet while /latest is pending', () => {
    const { legend } = renderCard({
      lots: retainedLots,
      history: {
        gemini_5h: [[NOW - 1800, 40]],
        gemini_7d: [[NOW - 1800, 60]],
      },
      rows: [],
      pending: true,
    });
    expect(legend('gemini_5h')?.dataset.state).toBeUndefined();
    expect(screen.queryByText('Stale reading')).toBeNull();
    expect(
      screen.queryByText("Couldn't check for retained readings"),
    ).toBeNull();
  });
});
