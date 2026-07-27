import { describe, expect, it } from 'vitest';
import type { QuotaSnapshot, SeriesResponseItem } from '../../lib/api';
import {
  isObservedAtOrAfter,
  QUOTA_WINDOW_ORDER,
  selectQuotaCardSnapshots,
  selectVisibleGraphWindows,
  seriesHasInRangeData,
} from './quotaWindowVisibility';

function snap(overrides: Partial<QuotaSnapshot>): QuotaSnapshot {
  return {
    window: '5h',
    state: 'fresh',
    source: 'api',
    utilization: 0.1,
    status: null,
    resets_at_unix_secs: null,
    surpassed_threshold: null,
    representative_claim: null,
    disabled_reason: null,
    extra_usage_enabled: null,
    extra_usage_monthly_limit: null,
    extra_usage_used_credits: null,
    observed_at_unix_millis: null,
    age_secs: null,
    ...overrides,
  };
}

function series(
  window: string,
  bucketStarts: readonly number[],
): SeriesResponseItem {
  return {
    upstream_id: '1',
    upstream_name: 'test',
    window,
    buckets: bucketStarts.map((bucket_start_unix_secs) => ({
      bucket_start_unix_secs,
      utilization_last: 0.5,
    })),
    markers: [],
  };
}

describe('isObservedAtOrAfter', () => {
  it('is true when observed exactly at the threshold (secs -> millis)', () => {
    expect(isObservedAtOrAfter(1_000_000, 1000)).toBe(true);
  });

  it('is false one millisecond before the threshold', () => {
    expect(isObservedAtOrAfter(999_999, 1000)).toBe(false);
  });

  it('is false when observed is null or undefined', () => {
    expect(isObservedAtOrAfter(null, 1000)).toBe(false);
    expect(isObservedAtOrAfter(undefined, 1000)).toBe(false);
  });
});

describe('seriesHasInRangeData', () => {
  it('is true when at least one bucket starts at or after since', () => {
    expect(seriesHasInRangeData([series('5h', [1000, 2000])], '5h', 1000)).toBe(
      true,
    );
  });

  it('is false when only a left-anchor bucket before since exists', () => {
    expect(seriesHasInRangeData([series('5h', [500])], '5h', 1000)).toBe(false);
  });

  it('is false when the window is absent from the series', () => {
    expect(seriesHasInRangeData([series('7d', [2000])], '5h', 1000)).toBe(
      false,
    );
  });

  it('is false when the series list is undefined', () => {
    expect(seriesHasInRangeData(undefined, '5h', 1000)).toBe(false);
  });
});

describe('selectVisibleGraphWindows', () => {
  const sinceUnixSecs = 1000;

  it('shows a window observed in range with in-range series data', () => {
    const result = selectVisibleGraphWindows({
      latestWindows: [
        snap({ window: '5h', observed_at_unix_millis: 1_500_000 }),
      ],
      series: [series('5h', [1200])],
      sinceUnixSecs,
    });
    expect(result).toEqual(['5h']);
  });

  it('hides a window whose latest observation predates since', () => {
    const result = selectVisibleGraphWindows({
      latestWindows: [snap({ window: '5h', observed_at_unix_millis: 900_000 })],
      series: [series('5h', [1200])],
      sinceUnixSecs,
    });
    expect(result).toEqual([]);
  });

  it('hides a recently-observed window that only has a left-anchor bucket', () => {
    const result = selectVisibleGraphWindows({
      latestWindows: [
        snap({ window: '5h', observed_at_unix_millis: 1_500_000 }),
      ],
      series: [series('5h', [500])],
      sinceUnixSecs,
    });
    expect(result).toEqual([]);
  });

  it('hides an unobserved window', () => {
    const result = selectVisibleGraphWindows({
      latestWindows: [
        snap({
          window: '5h',
          state: 'unobserved',
          observed_at_unix_millis: null,
        }),
      ],
      series: [series('5h', [1200])],
      sinceUnixSecs,
    });
    expect(result).toEqual([]);
  });

  it('hides a proven-absent window even when older series data is in range', () => {
    const result = selectVisibleGraphWindows({
      latestWindows: [
        snap({
          window: '7d',
          state: 'absent',
          observed_at_unix_millis: 1_500_000,
          utilization: null,
        }),
      ],
      series: [series('7d', [1200])],
      sinceUnixSecs,
    });
    expect(result).toEqual([]);
  });

  it('applies the same gate to the default 5h and 7d windows', () => {
    const result = selectVisibleGraphWindows({
      latestWindows: [
        snap({ window: '5h', observed_at_unix_millis: 1_500_000 }),
        snap({ window: '7d', observed_at_unix_millis: 900_000 }),
      ],
      series: [series('5h', [1200]), series('7d', [1200])],
      sinceUnixSecs,
    });
    expect(result).toEqual(['5h']);
  });

  it('shows overage only when enabled, fresh, and in-range', () => {
    const result = selectVisibleGraphWindows({
      latestWindows: [
        snap({
          window: 'overage',
          observed_at_unix_millis: 1_500_000,
          extra_usage_enabled: true,
        }),
      ],
      series: [series('overage', [1200])],
      sinceUnixSecs,
    });
    expect(result).toEqual(['overage']);
  });

  it('hides enabled overage that has no in-range series data', () => {
    const result = selectVisibleGraphWindows({
      latestWindows: [
        snap({
          window: 'overage',
          observed_at_unix_millis: 1_500_000,
          extra_usage_enabled: true,
        }),
      ],
      series: [series('overage', [500])],
      sinceUnixSecs,
    });
    expect(result).toEqual([]);
  });

  it('hides disabled overage even when fresh and in-range', () => {
    const result = selectVisibleGraphWindows({
      latestWindows: [
        snap({
          window: 'overage',
          observed_at_unix_millis: 1_500_000,
          extra_usage_enabled: false,
          extra_usage_monthly_limit: null,
        }),
      ],
      series: [series('overage', [1200])],
      sinceUnixSecs,
    });
    expect(result).toEqual([]);
  });

  it('returns visible windows ordered by QUOTA_WINDOW_ORDER', () => {
    const result = selectVisibleGraphWindows({
      latestWindows: [
        snap({ window: '7d_fable', observed_at_unix_millis: 1_500_000 }),
        snap({ window: '5h', observed_at_unix_millis: 1_500_000 }),
        snap({ window: '7d', observed_at_unix_millis: 1_500_000 }),
      ],
      series: [
        series('7d_fable', [1200]),
        series('5h', [1200]),
        series('7d', [1200]),
      ],
      sinceUnixSecs,
    });
    expect(result).toEqual(['5h', '7d', '7d_fable']);
  });
});

describe('selectQuotaCardSnapshots', () => {
  const nowUnixSecs = 1_000_000;

  it('includes unobserved 5h and 7d placeholders', () => {
    const result = selectQuotaCardSnapshots({
      latestWindows: [
        snap({
          window: '5h',
          state: 'unobserved',
          observed_at_unix_millis: null,
        }),
        snap({
          window: '7d',
          state: 'unobserved',
          observed_at_unix_millis: null,
        }),
      ],
      nowUnixSecs,
    });
    expect(result.map((s) => s.window)).toEqual(['5h', '7d']);
  });

  it('hides a proven-absent shared window', () => {
    const result = selectQuotaCardSnapshots({
      latestWindows: [
        snap({ window: '5h' }),
        snap({
          window: '7d',
          state: 'absent',
          utilization: null,
          observed_at_unix_millis: nowUnixSecs * 1000,
        }),
      ],
      nowUnixSecs,
    });
    expect(result.map((s) => s.window)).toEqual(['5h']);
  });

  it('includes a model window observed within the last week', () => {
    const result = selectQuotaCardSnapshots({
      latestWindows: [
        snap({
          window: '7d_sonnet',
          observed_at_unix_millis: (nowUnixSecs - 172_800) * 1000,
        }),
      ],
      nowUnixSecs,
    });
    expect(result.map((s) => s.window)).toEqual(['7d_sonnet']);
  });

  it('drops a model window observed more than a week ago', () => {
    const result = selectQuotaCardSnapshots({
      latestWindows: [
        snap({
          window: '7d_sonnet',
          observed_at_unix_millis: (nowUnixSecs - 691_200) * 1000,
        }),
      ],
      nowUnixSecs,
    });
    expect(result.map((s) => s.window)).toEqual([]);
  });

  it('keeps a model window observed exactly one week ago (inclusive)', () => {
    const result = selectQuotaCardSnapshots({
      latestWindows: [
        snap({
          window: '7d_fable',
          observed_at_unix_millis: (nowUnixSecs - 604_800) * 1000,
        }),
      ],
      nowUnixSecs,
    });
    expect(result.map((s) => s.window)).toEqual(['7d_fable']);
  });

  it('drops an unobserved model window', () => {
    const result = selectQuotaCardSnapshots({
      latestWindows: [
        snap({
          window: '7d_opus',
          state: 'unobserved',
          observed_at_unix_millis: null,
        }),
      ],
      nowUnixSecs,
    });
    expect(result.map((s) => s.window)).toEqual([]);
  });

  it('includes enabled non-missing overage regardless of observed age', () => {
    const result = selectQuotaCardSnapshots({
      latestWindows: [
        snap({
          window: 'overage',
          extra_usage_enabled: true,
          observed_at_unix_millis: null,
        }),
      ],
      nowUnixSecs,
    });
    expect(result.map((s) => s.window)).toEqual(['overage']);
  });

  it('drops an unobserved overage card', () => {
    const result = selectQuotaCardSnapshots({
      latestWindows: [
        snap({
          window: 'overage',
          state: 'unobserved',
          extra_usage_enabled: true,
        }),
      ],
      nowUnixSecs,
    });
    expect(result.map((s) => s.window)).toEqual([]);
  });

  it('includes a disabled overage that has a monthly limit set', () => {
    const result = selectQuotaCardSnapshots({
      latestWindows: [
        snap({
          window: 'overage',
          extra_usage_enabled: false,
          extra_usage_monthly_limit: 5000,
          observed_at_unix_millis: null,
        }),
      ],
      nowUnixSecs,
    });
    expect(result.map((s) => s.window)).toEqual(['overage']);
  });

  it('orders cards by QUOTA_WINDOW_ORDER', () => {
    const result = selectQuotaCardSnapshots({
      latestWindows: [
        snap({ window: 'overage', extra_usage_enabled: true }),
        snap({
          window: '7d_fable',
          observed_at_unix_millis: (nowUnixSecs - 1000) * 1000,
        }),
        snap({ window: '7d' }),
        snap({ window: '5h' }),
      ],
      nowUnixSecs,
    });
    expect(result.map((s) => s.window)).toEqual([
      '5h',
      '7d',
      '7d_fable',
      'overage',
    ]);
  });
});

describe('range transitions', () => {
  it('reveals a window as the selected range widens past its observation', () => {
    // Observed 3h ago relative to now = 1_000_000s.
    const nowUnixSecs = 1_000_000;
    const observedAtUnixMillis = (nowUnixSecs - 3 * 3600) * 1000;
    const latestWindows = [
      snap({ window: '5h', observed_at_unix_millis: observedAtUnixMillis }),
    ];
    const seriesData = [series('5h', [nowUnixSecs - 3 * 3600])];

    const at = (sinceUnixSecs: number) =>
      selectVisibleGraphWindows({
        latestWindows,
        series: seriesData,
        sinceUnixSecs,
      });

    expect(at(nowUnixSecs - 3600)).toEqual([]); // 1h: observed before since
    expect(at(nowUnixSecs - 21600)).toEqual(['5h']); // 6h
    expect(at(nowUnixSecs - 86400)).toEqual(['5h']); // 24h
    expect(at(nowUnixSecs - 604800)).toEqual(['5h']); // 7d
  });

  it('drops a model card once its observation crosses the one-week cutoff', () => {
    const observedAtUnixMillis = 1_000_000_000; // ms => 1_000_000 s
    const latestWindows = [
      snap({
        window: '7d_opus',
        observed_at_unix_millis: observedAtUnixMillis,
      }),
    ];

    const before = selectQuotaCardSnapshots({
      latestWindows,
      nowUnixSecs: 1_000_000 + 604_800, // exactly one week later => inclusive keep
    });
    expect(before.map((s) => s.window)).toEqual(['7d_opus']);

    const after = selectQuotaCardSnapshots({
      latestWindows,
      nowUnixSecs: 1_000_000 + 604_800 + 1, // one second past cutoff => drop
    });
    expect(after.map((s) => s.window)).toEqual([]);
  });
});

describe('QUOTA_WINDOW_ORDER', () => {
  it('lists the six detail windows in canonical order', () => {
    expect(QUOTA_WINDOW_ORDER).toEqual([
      '5h',
      '7d',
      '7d_sonnet',
      '7d_opus',
      '7d_fable',
      'overage',
    ]);
  });
});
