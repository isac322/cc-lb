import { describe, expect, it } from 'vitest';
import type {
  AggregateProviderLotResponse,
  AggregateResponse,
  PoolHistoryWindowResponse,
} from '../lib/api';
import {
  buildPoolQuotaChartData,
  formatAgo,
  formatResetIn,
  hasFableHistoryData,
  missingCapacityUpstreams,
  oldestProviderObservation,
  poolQuotaResponseLatest,
  poolWindowResets,
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

  it('plots used per bucket and keeps exact latest utilization for the legend', () => {
    const windows = [
      historyWindow('5h', [[100, 20]], 18),
      historyWindow('7d', [[100, 40]], 35),
      historyWindow('7d_fable', [[100, 130]], 110),
    ];

    expect(hasFableHistoryData(windows)).toBe(true);
    const data = buildPoolQuotaChartData(windows, true);
    // Over-limit utilization pins to the top of the plot rather than leaving it.
    expect(data).toEqual([{ unix: 100, '5h': 20, '7d': 40, '7d_fable': 100 }]);
    expect(poolQuotaResponseLatest(windows, data, true)).toEqual({
      '5h': 18,
      '7d': 35,
      '7d_fable': 110,
    });

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
  poolUtilization: Record<string, number | null> = {},
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
      utilization_percent: poolUtilization[window] ?? null,
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

describe('poolWindowResets', () => {
  it('reads each window reset and treats an unset reset as none', () => {
    const base = aggregate({ '5h': [], '7d': [], '7d_fable': [] });
    const resets = poolWindowResets({
      ...base,
      windows: base.windows.map((entry) => ({
        ...entry,
        cc_window_reset_unix_secs: entry.window === '7d' ? 5_000 : 0,
      })),
    });
    expect(resets).toEqual({ '5h': null, '7d': 5_000, '7d_fable': null });
    expect(poolWindowResets(undefined)).toEqual({
      '5h': null,
      '7d': null,
      '7d_fable': null,
    });
  });
});

describe('missingCapacityUpstreams', () => {
  it('reports the most unsized upstreams any pool window has', () => {
    const base = aggregate({ '5h': [], '7d': [], '7d_opus': [] });
    const missing: Record<string, number> = { '5h': 1, '7d': 2, '7d_opus': 9 };
    expect(
      missingCapacityUpstreams({
        ...base,
        windows: base.windows.map((entry) => ({
          ...entry,
          missing_capacity_upstreams: missing[entry.window] ?? 0,
        })),
      }),
    ).toBe(2);
    expect(missingCapacityUpstreams(undefined)).toBe(0);
  });
});

describe('oldestProviderObservation', () => {
  function observed(
    id: string,
    observedAtSecs: number | null,
    state = 'fresh',
  ) {
    return {
      ...lot(id, 0.1),
      observed_at_unix_millis:
        observedAtSecs == null ? null : observedAtSecs * 1000,
      state,
    };
  }

  it('reports the oldest observation across pool windows', () => {
    const result = oldestProviderObservation(
      aggregate({
        '5h': [observed('a', 900), observed('b', null)],
        '7d': [observed('a', 700)],
        '7d_opus': [observed('c', 100)],
      }),
    );
    expect(result).toEqual({ observedAtUnixSecs: 700, stale: false });
  });

  it('marks the data stale by lot state or by the staleness limit', () => {
    expect(
      oldestProviderObservation(
        aggregate({ '5h': [observed('a', 900, 'stale')] }),
      ).stale,
    ).toBe(true);
    const old = {
      ...aggregate({ '7d': [observed('a', 100)] }),
      now_unix_secs: 1000,
      max_staleness_secs: 300,
    };
    expect(oldestProviderObservation(old)).toEqual({
      observedAtUnixSecs: 100,
      stale: true,
    });
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

describe('formatAgo', () => {
  it('says just now under a minute, then the two largest units', () => {
    expect(formatAgo(100, 130)).toBe('just now');
    expect(formatAgo(0, 12 * 60)).toBe('12m ago');
    expect(formatAgo(0, 86400 + 19 * 3600 + 5)).toBe('1d 19h ago');
  });
});
