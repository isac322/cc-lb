import { describe, expect, it } from 'vitest';
import type {
  AggregateProviderLotResponse,
  AggregateResponse,
  PoolHistoryWindowResponse,
} from '../lib/api';
import {
  buildPoolQuotaChartData,
  closestToLimit,
  formatResetIn,
  hasFableHistoryData,
  poolQuotaChartMax,
  poolQuotaResponseLatest,
} from './-overviewPoolQuota';

function historyWindow(
  name: string,
  values: Array<[number, number | null]>,
  latestPercent = values.at(-1)?.[1] ?? null,
): PoolHistoryWindowResponse {
  const latestTimestamp = values.at(-1)?.[0] ?? 0;
  return {
    window: name,
    latest:
      latestPercent == null
        ? null
        : {
            snapshot_at_unix_secs: latestTimestamp,
            utilization: latestPercent / 100,
            utilization_percent: latestPercent,
            contributing_upstreams: 1,
            eligible_upstreams: 1,
            stale_upstreams: 0,
            max_observed_at_unix_millis: latestTimestamp * 1000,
          },
    series: values.map(([snapshot_at_unix_secs, utilization_percent]) => ({
      snapshot_at_unix_secs,
      utilization_percent,
    })),
  };
}

describe('Overview pooled Fable history', () => {
  it('hides Fable when the selected range has no non-null Fable point', () => {
    expect(
      hasFableHistoryData([historyWindow('7d_fable', [[100, null]])]),
    ).toBe(false);
  });

  it('uses bucket peaks for chart scale and exact latest values for the legend', () => {
    const windows = [
      historyWindow('5h', [[100, 20]], 18),
      historyWindow('7d', [[100, 40]], 35),
      historyWindow('7d_fable', [[100, 130]], 110),
    ];

    expect(hasFableHistoryData(windows)).toBe(true);
    const data = buildPoolQuotaChartData(windows, true);
    expect(data).toEqual([{ unix: 100, '5h': 20, '7d': 40, '7d_fable': 130 }]);
    expect(poolQuotaResponseLatest(windows, data, true)).toEqual({
      '5h': 18,
      '7d': 35,
      '7d_fable': 110,
    });
    expect(poolQuotaChartMax(data, true)).toBe(130);

    expect(poolQuotaResponseLatest(windows, [], true)).toEqual({
      '5h': null,
      '7d': null,
      '7d_fable': null,
    });
  });
});

function lot(
  upstream_id: string,
  utilization: number | null,
  provider_reset_unix_secs: number | null = null,
): AggregateProviderLotResponse {
  return {
    upstream_id,
    upstream_name: `name-${upstream_id}`,
    window: '',
    source: 'oauth',
    state: 'fresh',
    provider_start_unix_secs: null,
    provider_reset_unix_secs,
    observed_at_unix_millis: null,
    utilization,
    capacity_estimate_tokens: null,
    used_before_cc_window_tokens: 0,
    capacity_to_now_tokens_estimate: null,
    projected_capacity_tokens_estimate: null,
    confidence: 'high',
    capacity_ratio: 1,
  };
}

function aggregate(
  windows: Record<string, AggregateProviderLotResponse[]>,
): AggregateResponse {
  return {
    now_unix_secs: 0,
    window_anchor_unix_secs: 0,
    max_staleness_secs: 0,
    upstream_count: 0,
    caveats: [],
    windows: Object.entries(windows).map(([window, provider_lots]) => ({
      window,
      cc_window_start_unix_secs: 0,
      cc_window_reset_unix_secs: 0,
      used_tokens: 0,
      utilization: null,
      utilization_percent: null,
      capacity_to_now_tokens_estimate: null,
      projected_capacity_tokens_estimate: null,
      remaining_to_now_tokens_estimate: null,
      confidence: 'high',
      contributing_upstreams: 0,
      stale_upstreams: 0,
      missing_capacity_upstreams: 0,
      provider_lots,
      caveats: [],
    })),
  };
}

describe('closestToLimit', () => {
  it('lists each upstream once under its most-used window, highest first', () => {
    const ranked = closestToLimit(
      aggregate({
        '5h': [lot('a', 0.2, 500), lot('b', 0.9, 600), lot('c', null)],
        '7d': [lot('a', 1, 700), lot('b', 0.9, 800), lot('c', null)],
        '7d_fable': [lot('a', 0.5)],
      }),
    );

    expect(
      ranked.map((entry) => [
        entry.upstreamId,
        entry.window,
        entry.utilizationPercent,
        entry.resetUnixSecs,
      ]),
    ).toEqual([
      ['a', '7d', 100, 700],
      // A tie inside one upstream keeps the earlier (shorter) window.
      ['b', '5h', 90, 600],
    ]);
  });

  it('ignores windows outside the pool set and returns nothing without data', () => {
    expect(closestToLimit(undefined)).toEqual([]);
    expect(closestToLimit(aggregate({ '7d_opus': [lot('a', 1)] }))).toEqual([]);
  });
});

describe('formatResetIn', () => {
  it('rounds down to the two largest units and reports past resets', () => {
    expect(formatResetIn(null, 0)).toBeNull();
    expect(formatResetIn(100, 100)).toBe('reset time passed');
    expect(formatResetIn(30, 0)).toBe('resets in 1m');
    expect(formatResetIn(3 * 3600 + 12 * 60 + 59, 0)).toBe('resets in 3h 12m');
    expect(formatResetIn(2 * 3600, 0)).toBe('resets in 2h');
    expect(formatResetIn(2 * 86400 + 4 * 3600 + 59 * 60, 0)).toBe(
      'resets in 2d 4h',
    );
  });
});
