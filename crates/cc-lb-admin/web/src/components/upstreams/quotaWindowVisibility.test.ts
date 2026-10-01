import { describe, expect, it } from 'vitest';
import type { QuotaSnapshot, SeriesResponseItem } from '../../lib/api';
import {
  isObservedAtOrAfter,
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
          extra_usage_monthly_limit: 5000,
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
          extra_usage_monthly_limit: 5000,
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
          extra_usage_monthly_limit: 5000,
        }),
      ],
      series: [series('overage', [1200])],
      sinceUnixSecs,
    });
    expect(result).toEqual([]);
  });

  it('keeps the backend-only unified window out of rows and series', () => {
    // `unified` is not a SubscriptionQuotaWindow; the backend may still
    // report it on the wire for routing, but it never reaches the UI.
    const unified = snap({
      window: 'unified',
      observed_at_unix_millis: 1_500_000,
      status: 'allowed',
      representative_claim: '5h',
    } as unknown as Partial<QuotaSnapshot>);
    expect(
      selectVisibleGraphWindows({
        latestWindows: [unified],
        series: [series('unified', [1200])],
        sinceUnixSecs,
      }),
    ).toEqual([]);
    expect(selectQuotaCardSnapshots({ latestWindows: [unified] })).toEqual([]);
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
  const windowsOf = (latestWindows: QuotaSnapshot[]) =>
    selectQuotaCardSnapshots({ latestWindows }).map((s) => s.window);

  it('includes unobserved 5h and 7d placeholders', () => {
    expect(
      windowsOf([
        snap({ window: '5h', state: 'unobserved' }),
        snap({ window: '7d', state: 'unobserved' }),
      ]),
    ).toEqual(['5h', '7d']);
  });

  it('hides proven-absent windows', () => {
    expect(
      windowsOf([
        snap({ window: '5h' }),
        snap({ window: '7d', state: 'absent', utilization: null }),
        snap({ window: '7d_sonnet', state: 'absent', utilization: null }),
      ]),
    ).toEqual(['5h']);
  });

  it('drops unobserved windows other than 5h and 7d', () => {
    expect(
      windowsOf([
        snap({ window: '7d_opus', state: 'unobserved' }),
        snap({ window: '7d_sonnet', state: 'unobserved' }),
        snap({
          window: 'overage',
          state: 'unobserved',
          extra_usage_enabled: true,
        }),
      ]),
    ).toEqual([]);
  });

  it('keeps a legacy model window however old its last reading is', () => {
    expect(
      windowsOf([
        snap({ window: '7d_sonnet', state: 'stale', age_secs: 30 * 86_400 }),
      ]),
    ).toEqual(['7d_sonnet']);
  });

  it('lists every reported window in canonical order', () => {
    expect(
      windowsOf([
        snap({
          window: 'overage',
          extra_usage_enabled: true,
          extra_usage_monthly_limit: 5000,
        }),
        snap({ window: '7d_opus' }),
        snap({ window: '7d_sonnet' }),
        snap({ window: '7d_fable' }),
        snap({ window: '7d' }),
        snap({ window: '5h' }),
      ]),
    ).toEqual(['5h', '7d', '7d_fable', '7d_sonnet', '7d_opus', 'overage']);
  });
});

describe('Extra usage budget visibility', () => {
  it.each([
    { enabled: false, budget: 5000, visible: false },
    { enabled: null, budget: 5000, visible: false },
    { enabled: true, budget: null, visible: false },
    { enabled: true, budget: 0, visible: false },
    { enabled: true, budget: -1, visible: false },
    { enabled: true, budget: Number.NaN, visible: false },
    { enabled: true, budget: Number.POSITIVE_INFINITY, visible: false },
    { enabled: true, budget: 1, visible: true },
    { enabled: true, budget: 5000, visible: true },
  ])(
    'uses the same budget gate in quota rows and history for $enabled / $budget',
    ({ enabled, budget, visible }) => {
      const latestWindows = [
        snap({
          window: 'overage',
          extra_usage_enabled: enabled,
          extra_usage_monthly_limit: budget,
          observed_at_unix_millis: 1_500_000,
        }),
      ];
      const expected = visible ? ['overage'] : [];
      expect(
        selectQuotaCardSnapshots({ latestWindows }).map((s) => s.window),
      ).toEqual(expected);
      expect(
        selectVisibleGraphWindows({
          latestWindows,
          series: [series('overage', [1200])],
          sinceUnixSecs: 1000,
        }),
      ).toEqual(expected);
    },
  );
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
});
