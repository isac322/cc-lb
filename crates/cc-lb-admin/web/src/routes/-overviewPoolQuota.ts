import type { PoolHistoryWindowResponse } from '../lib/api';

export const POOL_QUOTA_WINDOWS = ['5h', '7d', '7d_fable'] as const;
export type PoolQuotaWindow = (typeof POOL_QUOTA_WINDOWS)[number];
export type PoolQuotaChartRow = {
  unix: number;
  '5h': number | null;
  '7d': number | null;
  '7d_fable': number | null;
};
export type PoolQuotaLatest = Omit<PoolQuotaChartRow, 'unix'>;

export function hasFableHistoryData(
  windows: readonly PoolHistoryWindowResponse[] | undefined,
): boolean {
  return (
    windows
      ?.find((entry) => entry.window === '7d_fable')
      ?.series.some((point) => point.utilization_percent != null) ?? false
  );
}

export function buildPoolQuotaChartData(
  windows: readonly PoolHistoryWindowResponse[] | undefined,
  includeFable: boolean,
): PoolQuotaChartRow[] {
  const buckets = new Map<number, PoolQuotaLatest>();
  for (const window of POOL_QUOTA_WINDOWS) {
    if (window === '7d_fable' && !includeFable) continue;
    const series =
      windows?.find((entry) => entry.window === window)?.series ?? [];
    for (const point of series) {
      const row = buckets.get(point.snapshot_at_unix_secs) ?? {
        '5h': null,
        '7d': null,
        '7d_fable': null,
      };
      if (point.utilization_percent != null) {
        row[window] = point.utilization_percent;
      }
      buckets.set(point.snapshot_at_unix_secs, row);
    }
  }
  return Array.from(buckets, ([unix, row]) => ({ unix, ...row })).sort(
    (left, right) => left.unix - right.unix,
  );
}

export function poolQuotaChartMax(
  rows: readonly PoolQuotaChartRow[],
  includeFable: boolean,
): number {
  let max = 100;
  for (const row of rows) {
    max = Math.max(max, row['5h'] ?? 0, row['7d'] ?? 0);
    if (includeFable) max = Math.max(max, row['7d_fable'] ?? 0);
  }
  return Math.ceil(max / 10) * 10;
}

export function poolQuotaResponseLatest(
  windows: readonly PoolHistoryWindowResponse[] | undefined,
  visibleRows: readonly PoolQuotaChartRow[],
  includeFable: boolean,
): PoolQuotaLatest {
  const latest: PoolQuotaLatest = {
    '5h': null,
    '7d': null,
    '7d_fable': null,
  };
  for (const window of POOL_QUOTA_WINDOWS) {
    if (window === '7d_fable' && !includeFable) continue;
    if (!visibleRows.some((row) => row[window] != null)) continue;
    latest[window] =
      windows?.find((entry) => entry.window === window)?.latest
        ?.utilization_percent ?? null;
  }
  return latest;
}
