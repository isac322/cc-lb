import type { AggregateResponse, PoolHistoryWindowResponse } from '../lib/api';
import { headroomPct } from '../lib/quotaSeverity';

export const POOL_QUOTA_WINDOWS = ['5h', '7d', '7d_fable'] as const;
export type PoolQuotaWindow = (typeof POOL_QUOTA_WINDOWS)[number];
/** One history bucket, as pool headroom per window (0-100, higher is better). */
export type PoolQuotaChartRow = {
  unix: number;
  '5h': number | null;
  '7d': number | null;
  '7d_fable': number | null;
};
/** Latest pool utilization per window (0-100 used), for legends. */
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

/**
 * Pool history as headroom: each bucket plots `100 - used`, clamped to
 * 0-100, so the chart reads the same way as every gauge on the page.
 */
export function buildPoolQuotaChartData(
  windows: readonly PoolHistoryWindowResponse[] | undefined,
  includeFable: boolean,
): PoolQuotaChartRow[] {
  const buckets = new Map<number, Omit<PoolQuotaChartRow, 'unix'>>();
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
      const left = headroomPct(point.utilization_percent);
      if (left != null) row[window] = left;
      buckets.set(point.snapshot_at_unix_secs, row);
    }
  }
  return Array.from(buckets, ([unix, row]) => ({ unix, ...row })).sort(
    (left, right) => left.unix - right.unix,
  );
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

/**
 * The pool window that runs out first: the highest pool utilization among
 * 5h / 7d / 7d (Fable). Ties keep the shorter window. `null` without any
 * reading.
 */
export function bindingPoolWindow(
  aggregate: AggregateResponse | undefined,
): PoolQuotaWindow | null {
  let binding: PoolQuotaWindow | null = null;
  let bindingUsed = -Infinity;
  for (const window of POOL_QUOTA_WINDOWS) {
    const used = aggregate?.windows.find(
      (entry) => entry.window === window,
    )?.utilization_percent;
    if (used == null || !Number.isFinite(used)) continue;
    if (used > bindingUsed) {
      binding = window;
      bindingUsed = used;
    }
  }
  return binding;
}

/**
 * Age of the provider data behind the pool: the oldest `observed_at` across
 * every pool-window lot, and whether any lot is stale (by state, or older
 * than the server's staleness limit).
 */
export function oldestProviderObservation(
  aggregate: AggregateResponse | undefined,
): { observedAtUnixSecs: number | null; stale: boolean } {
  let oldest: number | null = null;
  let stale = false;
  for (const window of POOL_QUOTA_WINDOWS) {
    const entry = aggregate?.windows.find(
      (candidate) => candidate.window === window,
    );
    for (const lot of entry?.provider_lots ?? []) {
      if (lot.state === 'stale') stale = true;
      if (lot.observed_at_unix_millis == null) continue;
      const secs = lot.observed_at_unix_millis / 1000;
      if (oldest == null || secs < oldest) oldest = secs;
    }
  }
  if (
    aggregate &&
    oldest != null &&
    aggregate.max_staleness_secs > 0 &&
    aggregate.now_unix_secs - oldest > aggregate.max_staleness_secs
  ) {
    stale = true;
  }
  return { observedAtUnixSecs: oldest, stale };
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
 * Ranks upstreams by least headroom left (highest window utilization first).
 * Each upstream appears once, with the window that is closest to its limit;
 * ties inside an upstream keep the window order (5h before 7d before 7d
 * (Fable)). Lots without a utilization reading are skipped.
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

/** "2d 4h" / "3h 12m" / "8m": the two largest units, rounded down, at least 1m. */
export function formatDuration(secs: number): string {
  const minutes = Math.max(1, Math.floor(secs / 60));
  const days = Math.floor(minutes / 1440);
  const hours = Math.floor((minutes % 1440) / 60);
  const mins = minutes % 60;
  if (days > 0) return `${days}d${hours > 0 ? ` ${hours}h` : ''}`;
  if (hours > 0) return `${hours}h${mins > 0 ? ` ${mins}m` : ''}`;
  return `${mins}m`;
}

/** "resets in 2d 4h" / "resets in 3h 12m" / "resets in 8m"; past resets say so. */
export function formatResetIn(
  resetUnixSecs: number | null,
  nowUnixSecs: number,
): string | null {
  if (resetUnixSecs == null) return null;
  const secs = resetUnixSecs - nowUnixSecs;
  if (secs <= 0) return 'reset time passed';
  return `resets in ${formatDuration(secs)}`;
}

/** "just now" under a minute, else "12m ago" / "1d 19h ago". */
export function formatAgo(unixSecs: number, nowUnixSecs: number): string {
  const secs = nowUnixSecs - unixSecs;
  if (secs < 60) return 'just now';
  return `${formatDuration(secs)} ago`;
}
