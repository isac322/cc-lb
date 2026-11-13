import { cleanup, render, screen } from '@testing-library/react';
import type { ReactNode } from 'react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import type {
  AggregateProviderLotResponse,
  AggregateResponse,
  AggregateWindowResponse,
  PoolHistoryWindowResponse,
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
): AggregateProviderLotResponse {
  return {
    upstream_id: upstream,
    upstream_name: upstream,
    window,
    source: 'api',
    state: 'fresh',
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

describe('Gemini pool card with one upstream retaining an earlier reading', () => {
  it('keeps plotting other upstreams after the retained reading while the legend marks it stale', () => {
    // Upstream A's gemini_5h is from t1 (its later poll omitted 5h); upstream B
    // keeps reporting gemini_5h, so the pool history has valid post-t1 points.
    const aggregate: AggregateResponse = {
      now_unix_secs: NOW,
      window_anchor_unix_secs: NOW,
      max_staleness_secs: 300,
      upstream_count: 2,
      caveats: [],
      windows: [
        aggregateWindow('gemini_5h', [
          lot('gemini_5h', 'a', T1),
          lot('gemini_5h', 'b', NOW),
        ]),
        aggregateWindow('gemini_7d', [
          lot('gemini_7d', 'a', NOW),
          lot('gemini_7d', 'b', NOW),
        ]),
      ],
    };
    const fiveHour: [number, number][] = [
      [T1, 20],
      [NOW - 3600, 35],
      [NOW - 1800, 40],
    ];
    vi.mocked(queries.useSubscriptionQuotaAggregate).mockReturnValue(
      successQuery(aggregate),
    );
    vi.mocked(queries.useSubscriptionQuotaPoolHistory).mockReturnValue(
      successQuery({
        now_unix_secs: NOW,
        range_secs: 86_400,
        windows: [
          historyWindow('gemini_5h', fiveHour),
          historyWindow('gemini_7d', [
            [T1, 90],
            [NOW - 1800, 100],
          ]),
        ],
      }),
    );

    render(
      <ProviderPoolCard provider="gemini" hasSubscription rangeSecs={86_400} />,
    );

    const plotted = screen
      .getByTestId('pool-stroke-gemini_5h')
      .getAttribute('d')
      ?.split('|')
      .map((point) => Number(point.split(':')[0]));
    for (const [unix] of fiveHour) expect(plotted).toContain(unix);
    expect(plotted?.some((unix) => unix > T1)).toBe(true);

    const [legend] = screen
      .getAllByTestId('pool-quota-legend-slot')
      .filter((row) => row.dataset.window === 'gemini_5h');
    expect(legend?.dataset.state).toBe('stale');
    expect(legend?.querySelector('[data-slot="pace-marker"]')).toBeNull();
    expect(
      legend?.querySelector('[data-slot="pool-quota-legend-reset"]')
        ?.textContent,
    ).toBe('Stale');
    expect(screen.getByText('Stale reading')).toBeDefined();
  });
});
