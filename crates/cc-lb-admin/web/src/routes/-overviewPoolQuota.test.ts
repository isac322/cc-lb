import { describe, expect, it } from 'vitest';
import type { PoolHistoryWindowResponse } from '../lib/api';
import {
  buildPoolQuotaChartData,
  hasFableHistoryData,
  poolQuotaChartLatest,
  poolQuotaChartMax,
} from './-overviewPoolQuota';

function historyWindow(
  name: string,
  values: Array<[number, number | null]>,
): PoolHistoryWindowResponse {
  return {
    window: name,
    latest: null,
    series: values.map(([snapshot_at_unix_secs, utilization_percent]) => ({
      snapshot_at_unix_secs,
      utilization:
        utilization_percent == null ? null : utilization_percent / 100,
      utilization_percent,
      contributing_upstreams: utilization_percent == null ? 0 : 1,
      eligible_upstreams: 1,
      stale_upstreams: 0,
      max_observed_at_unix_millis: snapshot_at_unix_secs * 1000,
    })),
  };
}

describe('Overview pooled Fable history', () => {
  it('hides Fable when the selected range has no non-null Fable point', () => {
    expect(
      hasFableHistoryData([historyWindow('7d_fable', [[100, null]])]),
    ).toBe(false);
  });

  it('includes Fable in data, latest, and max calculations when present', () => {
    const windows = [
      historyWindow('5h', [[100, 20]]),
      historyWindow('7d', [[100, 40]]),
      historyWindow('7d_fable', [[100, 130]]),
    ];

    expect(hasFableHistoryData(windows)).toBe(true);
    const data = buildPoolQuotaChartData(windows, true);
    expect(data).toEqual([{ unix: 100, '5h': 20, '7d': 40, '7d_fable': 130 }]);
    expect(poolQuotaChartLatest(data, true)).toEqual({
      '5h': 20,
      '7d': 40,
      '7d_fable': 130,
    });
    expect(poolQuotaChartMax(data, true)).toBe(130);
  });
});
