import type { AggregateResponse, PoolHistoryWindowResponse } from '../lib/api';

export const POOL_QUOTA_WINDOWS = ['5h', '7d', '7d_fable'] as const;
const POOL_QUOTA_WINDOW_ORDER = [
  '5h',
  '7d',
  'gemini_5h',
  'gemini_7d',
  'third_party_5h',
  'third_party_7d',
  '7d_fable',
  '7d_sonnet',
  '7d_opus',
  'overage',
] as const;
/** Window keys used by the pool quota chart and readout. */
export type PoolQuotaWindow = string;
/** One history bucket, as pool utilization per window (0-100 used). */
export type PoolQuotaChartRow = {
  unix: number;
  [window: string]: number | null;
};
/** Latest pool utilization per window (0-100 used), for legends. */
export type PoolQuotaLatest = Record<PoolQuotaWindow, number | null>;
export interface PoolQuotaObservation {
  observedAtUnixSecs: number | null;
  stale: boolean;
}

/**
 * The default call ignores unknown provider windows; provider-specific callers
 * supply their own allowed window keys.
 */
export function poolQuotaWindowsPresent(
  history: readonly PoolHistoryWindowResponse[] | undefined,
  aggregate?: AggregateResponse,
  defaults: readonly PoolQuotaWindow[] = POOL_QUOTA_WINDOWS,
  requireData = false,
): PoolQuotaWindow[] {
  const returned = new Set<string>();
  for (const entry of history ?? []) returned.add(entry.window);
  for (const entry of aggregate?.windows ?? []) returned.add(entry.window);
  return defaults.filter((window) => {
    const historyWindow = history?.find(
      (candidate) => candidate.window === window,
    );
    // Live provider history knows current eligibility; aggregate lots may
    // include an older, now superseded provider window.
    if (historyWindow?.latest?.max_staleness_secs != null) {
      return historyWindow.latest.eligible_upstreams > 0;
    }
    const aggregateWindow = aggregate?.windows.find(
      (candidate) => candidate.window === window,
    );
    const providerLots = aggregateWindow?.provider_lots ?? [];
    if (
      providerLots.length > 0 &&
      providerLots.every((lot) => lot.state === 'absent')
    ) {
      return false;
    }
    if (!requireData) return returned.has(window);
    return (
      aggregateWindow?.utilization_percent != null ||
      (historyWindow?.latest?.eligible_upstreams ?? 0) > 0 ||
      (aggregateWindow?.cc_window_reset_unix_secs ?? 0) > 0 ||
      (historyWindow?.series.length ?? 0) > 0
    );
  });
}

function windowKeys(
  windows: readonly Pick<PoolHistoryWindowResponse, 'window'>[] | undefined,
): string[] {
  const keys: string[] = [];
  for (const entry of windows ?? []) {
    if (entry.window === 'unified') continue;
    if (!keys.includes(entry.window)) keys.push(entry.window);
  }
  return keys.length > 0 ? keys : [...POOL_QUOTA_WINDOWS];
}

const POOL_AGGREGATE_WINDOWS = new Set([
  ...POOL_QUOTA_WINDOWS,
  'gemini_5h',
  'gemini_7d',
  'third_party_5h',
  'third_party_7d',
]);

function aggregateWindowKeys(
  windows: readonly Pick<PoolHistoryWindowResponse, 'window'>[] | undefined,
): string[] {
  return windowKeys(windows).filter((window) =>
    POOL_AGGREGATE_WINDOWS.has(window),
  );
}

function clampPoolQuotaPercent(used: number | null | undefined): number | null {
  return used != null && Number.isFinite(used)
    ? Math.min(100, Math.max(0, used))
    : null;
}

/**
 * Pool history as used percent: each bucket plots the pool utilization,
 * clamped to 0-100 so an over-limit reading pins to the top of the plot
 * instead of leaving it. Rows stay sparse: a window absent from the response
 * never becomes a null-valued chart series.
 */
export function buildPoolQuotaChartData(
  windows: readonly PoolHistoryWindowResponse[] | undefined,
): PoolQuotaChartRow[] {
  const keys = windowKeys(windows);
  const buckets = new Map<number, PoolQuotaChartRow>();
  for (const window of keys) {
    const series =
      windows?.find((entry) => entry.window === window)?.series ?? [];
    for (const point of series) {
      let row = buckets.get(point.snapshot_at_unix_secs);
      if (!row) {
        row = { unix: point.snapshot_at_unix_secs };
        for (const key of keys) row[key] = null;
        buckets.set(point.snapshot_at_unix_secs, row);
      }
      row[window] = clampPoolQuotaPercent(point.utilization_percent);
      buckets.set(point.snapshot_at_unix_secs, row);
    }
  }
  return Array.from(buckets.values()).sort(
    (left, right) => left.unix - right.unix,
  );
}

/**
 * Maximum idle gap across which a pool snapshot may be step-held to the range
 * boundary: the backend's 3,600s unchanged-utilization heartbeat plus 300s of
 * cron/poll/bucket slack. Beyond this age, snapshot collection was interrupted
 * and the chart must not fabricate a live flat line across the outage.
 */
export const POOL_QUOTA_HEARTBEAT_CARRY_SECS = 3_600 + 300;

/**
 * Clip persisted history without substituting current readings or extending
 * the tail. A bounded predecessor anchors the left edge only when the next
 * persisted point confirms collection continued within one heartbeat.
 * Snapshot counts disable anchoring so a clipped point is not counted twice.
 */
export function recordedPoolQuotaChartData(
  rows: readonly PoolQuotaChartRow[],
  rangeStartUnix: number,
  rangeEndUnix: number,
  { anchorPredecessor = true }: { anchorPredecessor?: boolean } = {},
): PoolQuotaChartRow[] {
  if (rangeStartUnix > rangeEndUnix) return [];

  let predecessor: PoolQuotaChartRow | undefined;
  const visible: PoolQuotaChartRow[] = [];
  for (const row of rows) {
    if (row.unix < rangeStartUnix) {
      predecessor = row;
    } else if (row.unix <= rangeEndUnix) {
      visible.push(row);
    }
  }

  const first = visible[0];
  if (
    anchorPredecessor &&
    predecessor &&
    first &&
    first.unix > rangeStartUnix &&
    first.unix - predecessor.unix <= POOL_QUOTA_HEARTBEAT_CARRY_SECS
  ) {
    visible.unshift({ ...predecessor, unix: rangeStartUnix });
  }
  return visible;
}

function hasPoolQuotaValue(row: PoolQuotaChartRow): boolean {
  return Object.entries(row).some(
    ([key, value]) =>
      key !== 'unix' && typeof value === 'number' && Number.isFinite(value),
  );
}
export function latestPoolQuotaSnapshotUnix(
  windows: readonly PoolHistoryWindowResponse[] | undefined,
): number | null {
  let latestUnix: number | null = null;
  for (const entry of windows ?? []) {
    const ts = entry.latest?.snapshot_at_unix_secs;
    if (typeof ts === 'number' && Number.isFinite(ts)) {
      latestUnix = latestUnix === null ? ts : Math.max(latestUnix, ts);
    }
  }
  return latestUnix;
}

function buildCarriedPoolQuotaRow(
  baseRow: PoolQuotaChartRow,
  targetUnix: number,
  rangeEndUnix: number,
  windows?: readonly PoolHistoryWindowResponse[],
): PoolQuotaChartRow {
  if (!windows) {
    return { ...baseRow, unix: targetUnix };
  }
  const carried: PoolQuotaChartRow = { unix: targetUnix };
  for (const window of Object.keys(baseRow).filter((key) => key !== 'unix')) {
    if (baseRow[window] == null) {
      carried[window] = null;
      continue;
    }
    const latest = windows.find((entry) => entry.window === window)?.latest;
    const latestUnix = latest?.snapshot_at_unix_secs;
    const windowLatestIsFresh =
      typeof latestUnix === 'number' &&
      Number.isFinite(latestUnix) &&
      latestUnix <= rangeEndUnix &&
      rangeEndUnix - latestUnix <= POOL_QUOTA_HEARTBEAT_CARRY_SECS;
    carried[window] = windowLatestIsFresh
      ? clampPoolQuotaPercent(latest?.utilization_percent)
      : null;
  }
  return carried;
}

export function visiblePoolQuotaChartData(
  rows: readonly PoolQuotaChartRow[],
  rangeStartUnix: number,
  rangeEndUnix: number,
  latestSnapshotUnixSecs?: number | null,
  windows?: readonly PoolHistoryWindowResponse[],
): PoolQuotaChartRow[] {
  if (!rows.length || rangeStartUnix > rangeEndUnix) return [];

  let leadingRow: PoolQuotaChartRow | undefined;
  const visible: PoolQuotaChartRow[] = [];
  for (const row of rows) {
    if (row.unix < rangeStartUnix) {
      leadingRow = row;
    } else if (row.unix <= rangeEndUnix) {
      visible.push(row);
    }
  }

  const firstInRange = visible[0];
  const effectiveLatestUnix =
    latestSnapshotUnixSecs === undefined
      ? ((visible.at(-1) ?? leadingRow)?.unix ?? null)
      : latestSnapshotUnixSecs;
  const latestIsFresh =
    effectiveLatestUnix !== null &&
    effectiveLatestUnix <= rangeEndUnix &&
    rangeEndUnix - effectiveLatestUnix <= POOL_QUOTA_HEARTBEAT_CARRY_SECS;

  if (
    leadingRow &&
    hasPoolQuotaValue(leadingRow) &&
    firstInRange?.unix !== rangeStartUnix
  ) {
    const canClampLeading = firstInRange
      ? firstInRange.unix - leadingRow.unix <= POOL_QUOTA_HEARTBEAT_CARRY_SECS
      : latestIsFresh &&
        rangeStartUnix - leadingRow.unix <= POOL_QUOTA_HEARTBEAT_CARRY_SECS;
    if (canClampLeading) {
      const clampedRow = firstInRange
        ? { ...leadingRow, unix: rangeStartUnix }
        : buildCarriedPoolQuotaRow(
            leadingRow,
            rangeStartUnix,
            rangeEndUnix,
            windows,
          );
      if (hasPoolQuotaValue(clampedRow)) {
        visible.unshift(clampedRow);
      }
    }
  }

  const lastRow = visible.at(-1);
  if (
    lastRow &&
    lastRow.unix < rangeEndUnix &&
    hasPoolQuotaValue(lastRow) &&
    latestIsFresh
  ) {
    const tailRow = buildCarriedPoolQuotaRow(
      lastRow,
      rangeEndUnix,
      rangeEndUnix,
      windows,
    );
    if (hasPoolQuotaValue(tailRow)) {
      visible.push(tailRow);
    }
  }

  return visible;
}

export function poolQuotaResponseLatest(
  windows: readonly PoolHistoryWindowResponse[] | undefined,
  visibleRows: readonly PoolQuotaChartRow[],
  { requireVisibleHistory = true }: { requireVisibleHistory?: boolean } = {},
): PoolQuotaLatest {
  const latest: PoolQuotaLatest = {};
  for (const window of POOL_QUOTA_WINDOWS) latest[window] = null;
  for (const window of windowKeys(windows)) {
    latest[window] = null;
    if (
      requireVisibleHistory &&
      !visibleRows.some((row) => row[window] != null)
    ) {
      continue;
    }
    latest[window] =
      windows?.find((entry) => entry.window === window)?.latest
        ?.utilization_percent ?? null;
  }
  return latest;
}

/** Current provider observation age, independent of historical aggregate lots. */
export function poolQuotaResponseObservation(
  windows: readonly PoolHistoryWindowResponse[] | undefined,
  visibleWindows: readonly PoolQuotaWindow[],
): PoolQuotaObservation {
  let observedAtUnixSecs: number | null = null;
  let stale = false;
  for (const entry of windows ?? []) {
    if (!visibleWindows.includes(entry.window)) continue;
    const current = entry.latest;
    const oldest = current?.oldest_observed_at_unix_millis;
    if (oldest != null) {
      const secs = oldest / 1000;
      observedAtUnixSecs =
        observedAtUnixSecs == null ? secs : Math.min(observedAtUnixSecs, secs);
    }
    if ((current?.stale_upstreams ?? 0) > 0) stale = true;
  }
  return { observedAtUnixSecs, stale };
}

/**
 * When each pool window next refills (`cc_window_reset_unix_secs`), or
 * `null` for a window without a reading or a reset time.
 */
export function poolWindowResets(
  aggregate: AggregateResponse | undefined,
): Record<PoolQuotaWindow, number | null> {
  const resets: Record<PoolQuotaWindow, number | null> = {};
  for (const window of aggregateWindowKeys(aggregate?.windows)) {
    const entry = aggregate?.windows.find(
      (candidate) => candidate.window === window,
    );
    resets[window] =
      entry && entry.cc_window_reset_unix_secs > 0
        ? entry.cc_window_reset_unix_secs
        : null;
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
  for (const window of aggregateWindowKeys(aggregate?.windows)) {
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
  for (const window of aggregateWindowKeys(aggregate?.windows)) {
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

/** Reset sentence, or only its countdown for a column headed "Resets in". */
export function formatResetIn(
  resetUnixSecs: number | null,
  nowUnixSecs: number,
  { compact = false }: { compact?: boolean } = {},
): string | null {
  if (resetUnixSecs == null) return null;
  const secs = resetUnixSecs - nowUnixSecs;
  if (secs <= 0) return compact ? 'Passed' : 'reset time passed';
  const duration = formatDuration(secs);
  return compact ? duration : `resets in ${duration}`;
}

/** "just now" under a minute, else "12m ago" / "1d 19h ago". */
export function formatAgo(unixSecs: number, nowUnixSecs: number): string {
  const secs = nowUnixSecs - unixSecs;
  if (secs < 60) return 'just now';
  return `${formatDuration(secs)} ago`;
}

/** Rank of a series key in legend order (5h, 7d, Gemini, Fable); unknown keys last. */
function legendOrder(key: string): number {
  const index = POOL_QUOTA_WINDOW_ORDER.indexOf(
    key as (typeof POOL_QUOTA_WINDOW_ORDER)[number],
  );
  return index === -1 ? POOL_QUOTA_WINDOW_ORDER.length : index;
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
