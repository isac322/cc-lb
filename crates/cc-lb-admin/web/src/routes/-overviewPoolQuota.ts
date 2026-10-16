import type { AggregateResponse, PoolHistoryWindowResponse } from '../lib/api';

export const POOL_QUOTA_WINDOWS = ['5h', '7d', '7d_fable'] as const;
export type PoolQuotaWindow = (typeof POOL_QUOTA_WINDOWS)[number];
/** One history bucket, as pool utilization per window (0-100 used). */
export type PoolQuotaChartRow = {
  unix: number;
  '5h': number | null;
  '7d': number | null;
  '7d_fable': number | null;
};
/** Latest pool utilization per window (0-100 used), for legends. */
export type PoolQuotaLatest = Omit<PoolQuotaChartRow, 'unix'>;

/**
 * Maximum seconds a recorded pool quota snapshot is step-held across
 * intermediate timestamps emitted by faster-moving windows before a missing
 * sample is treated as an unobserved gap. Matches the 3,600s heartbeat in
 * `should_record_pool_quota_snapshot`.
 */
export const POOL_QUOTA_HEARTBEAT_SECS = 3_600;

/**
 * Pool history as used percent: each bucket plots the pool utilization,
 * clamped to 0-100 so an over-limit reading pins to the top of the plot
 * instead of leaving it. Windows that suppressed redundant writes within the
 * 3,600s heartbeat window carry forward their last observed value across
 * intermediate timestamps emitted by other windows.
 */
export function buildPoolQuotaChartData(
  windows: readonly PoolHistoryWindowResponse[] | undefined,
): PoolQuotaChartRow[] {
  type RawRow = Record<PoolQuotaWindow, number | null | undefined>;
  const buckets = new Map<number, RawRow>();
  for (const window of POOL_QUOTA_WINDOWS) {
    const series =
      windows?.find((entry) => entry.window === window)?.series ?? [];
    for (const point of series) {
      const row = buckets.get(point.snapshot_at_unix_secs) ?? {
        '5h': undefined,
        '7d': undefined,
        '7d_fable': undefined,
      };
      const used = point.utilization_percent;
      row[window] =
        used != null && Number.isFinite(used)
          ? Math.min(100, Math.max(0, used))
          : null;
      buckets.set(point.snapshot_at_unix_secs, row);
    }
  }
  const sorted = Array.from(buckets.entries()).sort(
    ([left], [right]) => left - right,
  );
  const lastSeen: Record<
    PoolQuotaWindow,
    { unix: number; value: number } | null
  > = {
    '5h': null,
    '7d': null,
    '7d_fable': null,
  };
  return sorted.map(([unix, raw]) => {
    const row: PoolQuotaChartRow = {
      unix,
      '5h': null,
      '7d': null,
      '7d_fable': null,
    };
    for (const window of POOL_QUOTA_WINDOWS) {
      const explicit = raw[window];
      if (typeof explicit === 'number') {
        row[window] = explicit;
        lastSeen[window] = { unix, value: explicit };
      } else if (explicit === null) {
        row[window] = null;
        lastSeen[window] = null;
      } else {
        const prev = lastSeen[window];
        if (prev !== null && unix - prev.unix <= POOL_QUOTA_HEARTBEAT_SECS) {
          row[window] = prev.value;
        }
      }
    }
    return row;
  });
}

export function poolQuotaResponseLatest(
  windows: readonly PoolHistoryWindowResponse[] | undefined,
  visibleRows: readonly PoolQuotaChartRow[],
): PoolQuotaLatest {
  const latest: PoolQuotaLatest = {
    '5h': null,
    '7d': null,
    '7d_fable': null,
  };
  for (const window of POOL_QUOTA_WINDOWS) {
    if (!visibleRows.some((row) => row[window] != null)) continue;
    latest[window] =
      windows?.find((entry) => entry.window === window)?.latest
        ?.utilization_percent ?? null;
  }
  return latest;
}

/**
 * When each pool window next refills (`cc_window_reset_unix_secs`), or
 * `null` for a window without a reading or a reset time.
 */
export function poolWindowResets(
  aggregate: AggregateResponse | undefined,
): Record<PoolQuotaWindow, number | null> {
  const resets: Record<PoolQuotaWindow, number | null> = {
    '5h': null,
    '7d': null,
    '7d_fable': null,
  };
  for (const window of POOL_QUOTA_WINDOWS) {
    const entry = aggregate?.windows.find(
      (candidate) => candidate.window === window,
    );
    if (entry && entry.cc_window_reset_unix_secs > 0) {
      resets[window] = entry.cc_window_reset_unix_secs;
    }
  }
  return resets;
}

/**
 * Upstreams whose share of the pool cannot be sized: the most any pool
 * window reports as `missing_capacity_upstreams`.
 */
export function missingCapacityUpstreams(
  aggregate: AggregateResponse | undefined,
): number {
  let missing = 0;
  for (const window of POOL_QUOTA_WINDOWS) {
    const entry = aggregate?.windows.find(
      (candidate) => candidate.window === window,
    );
    if (entry && entry.missing_capacity_upstreams > missing) {
      missing = entry.missing_capacity_upstreams;
    }
  }
  return missing;
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

/** Rank of a series key in legend order (5h, 7d, Fable); unknown keys last. */
function legendOrder(key: string): number {
  const index = (POOL_QUOTA_WINDOWS as readonly string[]).indexOf(key);
  return index === -1 ? POOL_QUOTA_WINDOWS.length : index;
}

/**
 * The pool chart's tooltip rows: one per window, in legend order. Each
 * window draws as two Areas (a fill pass, then a stroke pass) and Recharts
 * hands custom tooltip content an entry for both; the fill pass is marked
 * `type: 'none'` and only the default content skips it, so drop it here and
 * keep the first entry per series key.
 */
export function poolTooltipRows<
  T extends { readonly dataKey?: unknown; readonly type?: unknown },
>(payload: readonly T[]): T[] {
  const seen = new Set<string>();
  return payload
    .filter((entry) => {
      if (entry.type === 'none') return false;
      const key = String(entry.dataKey);
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    })
    .sort(
      (a, b) => legendOrder(String(a.dataKey)) - legendOrder(String(b.dataKey)),
    );
}
