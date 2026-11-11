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
  latestPoolQuotaSnapshotUnix,
  missingCapacityUpstreams,
  oldestProviderObservation,
  POOL_QUOTA_HEARTBEAT_CARRY_SECS,
  poolQuotaResponseLatest,
  poolQuotaResponseObservation,
  poolQuotaWindowsPresent,
  poolTooltipRows,
  poolWindowResets,
  visiblePoolQuotaChartData,
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

describe('Overview pooled quota history', () => {
  it('plots used per bucket and keeps exact latest utilization for the legend', () => {
    const windows = [
      historyWindow('5h', [[100, 20]], 18),
      historyWindow('7d', [[100, 40]], 35),
      historyWindow('7d_fable', [[100, 130]], 110),
    ];

    const data = buildPoolQuotaChartData(windows);
    // Over-limit utilization pins to the top of the plot rather than leaving it.
    expect(data).toEqual([{ unix: 100, '5h': 20, '7d': 40, '7d_fable': 100 }]);
    expect(poolQuotaResponseLatest(windows, data)).toEqual({
      '5h': 18,
      '7d': 35,
      '7d_fable': 110,
    });

    expect(poolQuotaResponseLatest(windows, [])).toEqual({
      '5h': null,
      '7d': null,
      '7d_fable': null,
    });
  });

  it('reads live provider latest independently of empty or null historical buckets', () => {
    for (const window of ['5h', 'gemini_5h', 'third_party_7d']) {
      const entry = historyWindow(window, [[100, null]], 23);
      for (const rows of [[], buildPoolQuotaChartData([entry])]) {
        expect(
          poolQuotaResponseLatest([entry], rows, {
            requireVisibleHistory: false,
          })[window],
        ).toBe(23);
      }
      expect(
        poolQuotaResponseLatest(
          [{ ...entry, latest: null }],
          [{ unix: 100, [window]: 88 }],
          { requireVisibleHistory: false },
        )[window],
      ).toBeNull();
    }
  });

  it('keeps chart rows sparse when the provider omits the 5h window', () => {
    const windows = [
      historyWindow('7d', [[100, 40]], 35),
      historyWindow('7d_fable', [[100, 80]], 75),
    ];

    expect(buildPoolQuotaChartData(windows)).toEqual([
      { unix: 100, '7d': 40, '7d_fable': 80 },
    ]);
    expect(
      poolQuotaResponseLatest(windows, buildPoolQuotaChartData(windows)),
    ).toEqual({
      '5h': null,
      '7d': 35,
      '7d_fable': 75,
    });
  });

  it('extends the latest in-range point to rangeEndUnix when usage does not change', () => {
    const rows = buildPoolQuotaChartData([
      historyWindow('5h', [
        [100, 12],
        [200, 25],
      ]),
      historyWindow('7d', [
        [100, 40],
        [200, 40],
      ]),
      historyWindow('7d_fable', [
        [100, 80],
        [200, 80],
      ]),
    ]);

    expect(visiblePoolQuotaChartData(rows, 50, 500)).toEqual([
      { unix: 100, '5h': 12, '7d': 40, '7d_fable': 80 },
      { unix: 200, '5h': 25, '7d': 40, '7d_fable': 80 },
      { unix: 500, '5h': 25, '7d': 40, '7d_fable': 80 },
    ]);
  });

  it('clamps the latest pre-range observation to rangeStartUnix and carries across an idle range', () => {
    const rows = buildPoolQuotaChartData([
      historyWindow('5h', [
        [10, 10],
        [80, 20],
        [150, 30],
      ]),
      historyWindow('7d', [
        [10, 35],
        [80, 45],
        [150, 55],
      ]),
    ]);

    expect(visiblePoolQuotaChartData(rows, 100, 200)).toEqual([
      { unix: 100, '5h': 20, '7d': 45 },
      { unix: 150, '5h': 30, '7d': 55 },
      { unix: 200, '5h': 30, '7d': 55 },
    ]);

    expect(visiblePoolQuotaChartData(rows, 160, 220)).toEqual([
      { unix: 160, '5h': 30, '7d': 55 },
      { unix: 220, '5h': 30, '7d': 55 },
    ]);
  });

  it('does not duplicate boundary points or fabricate readings for empty, null, or absent windows', () => {
    const sparseRows = buildPoolQuotaChartData([
      historyWindow('7d', [[100, 40]]),
      historyWindow('7d_fable', [[100, 80]]),
    ]);

    expect(visiblePoolQuotaChartData(sparseRows, 100, 100)).toEqual([
      { unix: 100, '7d': 40, '7d_fable': 80 },
    ]);
    expect(visiblePoolQuotaChartData(sparseRows, 100, 200)).toEqual([
      { unix: 100, '7d': 40, '7d_fable': 80 },
      { unix: 200, '7d': 40, '7d_fable': 80 },
    ]);
    expect(
      visiblePoolQuotaChartData(sparseRows, 80, 200).every(
        (row) => !('5h' in row),
      ),
    ).toBe(true);

    const allNullRows = buildPoolQuotaChartData([
      historyWindow('5h', [[100, null]]),
      historyWindow('7d', [[100, null]]),
    ]);
    expect(visiblePoolQuotaChartData(allNullRows, 50, 200)).toEqual([
      { unix: 100, '5h': null, '7d': null },
    ]);
    expect(visiblePoolQuotaChartData(allNullRows, 120, 200)).toEqual([]);

    const partialNullRows = buildPoolQuotaChartData([
      historyWindow('5h', [[100, 25]]),
      historyWindow('7d', [[100, null]]),
    ]);
    expect(visiblePoolQuotaChartData(partialNullRows, 80, 200)).toEqual([
      { unix: 100, '5h': 25, '7d': null },
      { unix: 200, '5h': 25, '7d': null },
    ]);

    expect(visiblePoolQuotaChartData([], 100, 200)).toEqual([]);
    expect(visiblePoolQuotaChartData(sparseRows, 200, 100)).toEqual([]);
    expect(visiblePoolQuotaChartData(sparseRows, 10, 50)).toEqual([]);
  });

  it('does not carry stale outage snapshots or bridge multi-hour leading outages', () => {
    const staleLatestUnix = 100;
    const rangeStart = 10_000;
    const rangeEnd = 13_600;
    const anchorOnlyRows = buildPoolQuotaChartData([
      historyWindow('5h', [[rangeStart - 300, 40]], 40),
    ]);

    expect(
      visiblePoolQuotaChartData(
        anchorOnlyRows,
        rangeStart,
        rangeEnd,
        staleLatestUnix,
      ),
    ).toEqual([]);

    expect(
      visiblePoolQuotaChartData(
        anchorOnlyRows,
        rangeStart,
        rangeEnd,
        rangeEnd - POOL_QUOTA_HEARTBEAT_CARRY_SECS,
      ),
    ).toEqual([
      { unix: rangeStart, '5h': 40 },
      { unix: rangeEnd, '5h': 40 },
    ]);

    const wideRangeEnd = rangeStart + 21_600;
    const leadingOutageRows = buildPoolQuotaChartData([
      historyWindow('5h', [
        [rangeStart - 600, 15],
        [rangeStart + POOL_QUOTA_HEARTBEAT_CARRY_SECS + 60, 35],
      ]),
    ]);
    expect(
      visiblePoolQuotaChartData(
        leadingOutageRows,
        rangeStart,
        wideRangeEnd,
        wideRangeEnd - 60,
      ),
    ).toEqual([
      { unix: rangeStart + POOL_QUOTA_HEARTBEAT_CARRY_SECS + 60, '5h': 35 },
      { unix: wideRangeEnd, '5h': 35 },
    ]);

    const staleTailWindows = [
      historyWindow('5h', [[rangeStart + 120, 40]], 40),
    ];
    expect(latestPoolQuotaSnapshotUnix(staleTailWindows)).toBe(
      rangeStart + 120,
    );
    expect(
      visiblePoolQuotaChartData(
        buildPoolQuotaChartData(staleTailWindows),
        rangeStart,
        rangeStart + POOL_QUOTA_HEARTBEAT_CARRY_SECS + 500,
        latestPoolQuotaSnapshotUnix(staleTailWindows),
      ),
    ).toEqual([{ unix: rangeStart + 120, '5h': 40 }]);
  });

  it('carries the raw latest utilization instead of a higher final-bucket peak', () => {
    const windows = [
      {
        window: '7d_fable',
        latest: {
          snapshot_at_unix_secs: 240,
          utilization: 0.2,
          utilization_percent: 20,
          contributing_upstreams: 1,
          eligible_upstreams: 1,
          stale_upstreams: 0,
          max_observed_at_unix_millis: 240_000,
        },
        series: [
          { snapshot_at_unix_secs: 0, utilization_percent: 10 },
          { snapshot_at_unix_secs: 90, utilization_percent: 20 },
          { snapshot_at_unix_secs: 180, utilization_percent: 40 },
        ],
      },
    ];
    const rows = buildPoolQuotaChartData(windows);

    expect(
      visiblePoolQuotaChartData(
        rows,
        60,
        300,
        latestPoolQuotaSnapshotUnix(windows),
        windows,
      ),
    ).toEqual([
      { unix: 60, '7d_fable': 10 },
      { unix: 90, '7d_fable': 20 },
      { unix: 180, '7d_fable': 40 },
      { unix: 300, '7d_fable': 20 },
    ]);
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

describe('poolQuotaWindowsPresent', () => {
  it('unions sparse history and aggregate windows without inventing absent ones', () => {
    const history = [
      historyWindow('7d', [[100, 40]]),
      historyWindow('7d_fable', [[100, 80]]),
    ];
    const mixedAggregate = aggregate({ '5h': [], '7d': [] });

    expect(poolQuotaWindowsPresent(history)).toEqual(['7d', '7d_fable']);
    expect(poolQuotaWindowsPresent(history, mixedAggregate)).toEqual([
      '5h',
      '7d',
      '7d_fable',
    ]);
    expect(poolQuotaWindowsPresent([])).toEqual([]);
  });
});

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

  it('preserves provider-reported resets for Gemini model windows', () => {
    const base = aggregate({
      gemini_5h: [],
      gemini_7d: [],
      third_party_5h: [],
      third_party_7d: [],
    });
    expect(
      poolWindowResets({
        ...base,
        windows: base.windows.map((entry, index) => ({
          ...entry,
          cc_window_reset_unix_secs: 5_000 + index,
        })),
      }),
    ).toEqual({
      gemini_5h: 5_000,
      gemini_7d: 5_001,
      third_party_5h: 5_002,
      third_party_7d: 5_003,
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

describe('poolQuotaResponseObservation', () => {
  it('uses the oldest eligible live observation, not the newest contributor or history', () => {
    const windows = [
      { window: 'gemini_5h', oldest: 900, stale: 0 },
      { window: 'third_party_7d', oldest: 300, stale: 1 },
      { window: 'unified', oldest: 100, stale: 1 },
    ].map(({ window, oldest, stale }) => {
      const entry = historyWindow(window, [[100, 70]], 20);
      if (!entry.latest) throw new Error('Expected a fixture reading');
      return {
        ...entry,
        latest: {
          ...entry.latest,
          max_observed_at_unix_millis: 1_000_000,
          oldest_observed_at_unix_millis: oldest * 1000,
          stale_upstreams: stale,
          max_staleness_secs: 300,
        },
      };
    });
    expect(
      poolQuotaResponseObservation(windows, ['gemini_5h', 'third_party_7d']),
    ).toEqual({ observedAtUnixSecs: 300, stale: true });
    expect(poolQuotaResponseObservation(windows, ['gemini_5h'])).toEqual({
      observedAtUnixSecs: 900,
      stale: false,
    });
  });

  it('does not infer observation age from a latest snapshot timestamp', () => {
    expect(
      poolQuotaResponseObservation(
        [historyWindow('5h', [[1_000, 40]])],
        ['5h'],
      ),
    ).toEqual({ observedAtUnixSecs: null, stale: false });
    expect(poolQuotaResponseObservation(undefined, ['5h'])).toEqual({
      observedAtUnixSecs: null,
      stale: false,
    });
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
  it('includes Gemini and third-party observations without letting a newer window hide an old one', () => {
    const result = oldestProviderObservation({
      ...aggregate({
        gemini_5h: [observed('a', 1000)],
        gemini_7d: [observed('a', 900)],
        third_party_5h: [observed('a', 300)],
        third_party_7d: [observed('a', 1000)],
      }),
      now_unix_secs: 1000,
      max_staleness_secs: 300,
    });
    expect(result).toEqual({ observedAtUnixSecs: 300, stale: true });
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

  it('omits the repeated sentence prefix in an aligned countdown column', () => {
    const compact = { compact: true };
    expect(formatResetIn(null, 0, compact)).toBeNull();
    expect(formatResetIn(100, 100, compact)).toBe('Passed');
    expect(formatResetIn(99, 100, compact)).toBe('Passed');
    expect(formatResetIn(30, 0, compact)).toBe('1m');
    expect(formatResetIn(4 * 3600 + 17 * 60, 0, compact)).toBe('4h 17m');
    expect(formatResetIn(2 * 3600, 0, compact)).toBe('2h');
    expect(formatResetIn(2 * 86400 + 4 * 3600 + 59 * 60, 0, compact)).toBe(
      '2d 4h',
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

describe('Gemini pool tooltips', () => {
  it('orders model windows and removes duplicate fill entries', () => {
    const payload = [
      { dataKey: 'third_party_7d' },
      { dataKey: 'gemini_7d', type: 'none' },
      { dataKey: 'gemini_7d' },
      { dataKey: 'gemini_5h' },
      { dataKey: 'third_party_5h' },
      { dataKey: 'third_party_7d' },
    ];
    expect(poolTooltipRows(payload).map((entry) => entry.dataKey)).toEqual([
      'gemini_5h',
      'gemini_7d',
      'third_party_5h',
      'third_party_7d',
    ]);
  });
});
