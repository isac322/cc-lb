import type { AggregateResponse, PoolHistoryWindowResponse } from '../lib/api';

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

/** One upstream's most-used quota window, as ranked by the Overview. */
export type ClosestToLimitEntry = {
  upstreamId: string;
  upstreamName: string;
  window: PoolQuotaWindow;
  /** 0-100 */
  utilizationPercent: number;
  resetUnixSecs: number | null;
  state: string;
};

/**
 * Ranks upstreams by their highest window utilization, descending. Each
 * upstream appears once, with the window that is closest to its limit; ties
 * inside an upstream keep the window order (5h before 7d before Fable).
 * Lots without a utilization reading are skipped.
 */
export function closestToLimit(
  aggregate: AggregateResponse | undefined,
): ClosestToLimitEntry[] {
  const byUpstream = new Map<string, ClosestToLimitEntry>();
  for (const window of POOL_QUOTA_WINDOWS) {
    const entry = aggregate?.windows.find(
      (candidate) => candidate.window === window,
    );
    for (const lot of entry?.provider_lots ?? []) {
      if (lot.utilization == null || !Number.isFinite(lot.utilization)) {
        continue;
      }
      const utilizationPercent = lot.utilization * 100;
      const current = byUpstream.get(lot.upstream_id);
      if (current && current.utilizationPercent >= utilizationPercent) {
        continue;
      }
      byUpstream.set(lot.upstream_id, {
        upstreamId: lot.upstream_id,
        upstreamName: lot.upstream_name,
        window,
        utilizationPercent,
        resetUnixSecs: lot.provider_reset_unix_secs,
        state: lot.state,
      });
    }
  }
  return Array.from(byUpstream.values()).sort(
    (left, right) =>
      right.utilizationPercent - left.utilizationPercent ||
      left.upstreamName.localeCompare(right.upstreamName),
  );
}

/** "resets in 2d 4h" / "resets in 3h 12m" / "resets in 8m"; past resets say so. */
export function formatResetIn(
  resetUnixSecs: number | null,
  nowUnixSecs: number,
): string | null {
  if (resetUnixSecs == null) return null;
  const secs = resetUnixSecs - nowUnixSecs;
  if (secs <= 0) return 'reset time passed';
  const minutes = Math.max(1, Math.floor(secs / 60));
  const days = Math.floor(minutes / 1440);
  const hours = Math.floor((minutes % 1440) / 60);
  const mins = minutes % 60;
  if (days > 0) return `resets in ${days}d${hours > 0 ? ` ${hours}h` : ''}`;
  if (hours > 0) return `resets in ${hours}h${mins > 0 ? ` ${mins}m` : ''}`;
  return `resets in ${mins}m`;
}
