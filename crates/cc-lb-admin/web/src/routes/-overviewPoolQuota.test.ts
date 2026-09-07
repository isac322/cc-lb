import { describe, expect, it } from 'vitest';
import type { PoolHistoryWindowResponse } from '../lib/api';
import {
  buildPoolQuotaChartData,
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
