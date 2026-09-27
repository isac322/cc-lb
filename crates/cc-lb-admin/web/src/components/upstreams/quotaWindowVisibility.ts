import type { QuotaSnapshot, SeriesResponseItem } from '../../lib/api';

export const QUOTA_WINDOW_ORDER = [
  '5h',
  '7d',
  '7d_sonnet',
  '7d_opus',
  '7d_fable',
  'overage',
] as const;

export const MODEL_SCOPED_WINDOWS = ['7d_sonnet', '7d_opus', '7d_fable'];

export const MODEL_WINDOW_MAX_AGE_SECS = 604_800;

export interface VisibleGraphWindowsInput {
  latestWindows: readonly QuotaSnapshot[] | undefined;
  series: readonly SeriesResponseItem[] | undefined;
  sinceUnixSecs: number;
}

export interface QuotaCardSnapshotsInput {
  latestWindows: readonly QuotaSnapshot[] | undefined;
  nowUnixSecs: number;
}

/**
 * Freshness gate. `observed_at_unix_millis` is in MILLISECONDS; every range
 * threshold in this module is in SECONDS, so the threshold is scaled here.
 */
export function isObservedAtOrAfter(
  observedAtUnixMillis: number | null | undefined,
  thresholdUnixSecs: number,
): boolean {
  return (
    observedAtUnixMillis != null &&
    observedAtUnixMillis >= thresholdUnixSecs * 1000
  );
}

/**
 * True when the series carries at least one bucket that actually starts inside
 * the selected range. A left-anchor bucket (before `sinceUnixSecs`) does not
 * count: it only exists to seed step interpolation and has no in-range change.
 */
export function seriesHasInRangeData(
  series: readonly SeriesResponseItem[] | undefined,
  window: string,
  sinceUnixSecs: number,
): boolean {
  if (!series) return false;
  return series.some(
    (item) =>
      item.window === window &&
      item.buckets.some(
        (bucket) => bucket.bucket_start_unix_secs >= sinceUnixSecs,
      ),
  );
}

function isOverageActive(snap: QuotaSnapshot): boolean {
  return (
    snap.extra_usage_enabled === true || snap.extra_usage_monthly_limit != null
  );
}

/**
 * Windows the detail chart should draw for the selected range: a window is
 * visible only when its latest observation lands inside the range AND the
 * series has in-range data. Overage additionally requires an active budget.
 */
export function selectVisibleGraphWindows({
  latestWindows,
  series,
  sinceUnixSecs,
}: VisibleGraphWindowsInput): string[] {
  if (!latestWindows) return [];
  const byWindow = new Map<string, QuotaSnapshot>(
    latestWindows.map((snap) => [snap.window, snap]),
  );
  return QUOTA_WINDOW_ORDER.filter((window) => {
    const snap = byWindow.get(window);
    if (!snap) return false;
    if (snap.state === 'absent' || snap.state === 'unobserved') return false;
    if (!isObservedAtOrAfter(snap.observed_at_unix_millis, sinceUnixSecs)) {
      return false;
    }
    if (!seriesHasInRangeData(series, window, sinceUnixSecs)) return false;
    if (window === 'overage') return isOverageActive(snap);
    return true;
  });
}

/**
 * Snapshots the detail card grid should render. Proven-absent windows are
 * hidden. `5h` and `7d` still show an unobserved placeholder before the first
 * successful quota lookup. Model-scoped windows are dropped when unobserved or
 * their last observation is older than a week. Overage keeps its enabled/limit
 * gate.
 */
export function selectQuotaCardSnapshots({
  latestWindows,
  nowUnixSecs,
}: QuotaCardSnapshotsInput): QuotaSnapshot[] {
  if (!latestWindows) return [];
  const byWindow = new Map<string, QuotaSnapshot>(
    latestWindows.map((snap) => [snap.window, snap]),
  );
  const staleCutoffUnixSecs = nowUnixSecs - MODEL_WINDOW_MAX_AGE_SECS;
  const result: QuotaSnapshot[] = [];
  for (const window of QUOTA_WINDOW_ORDER) {
    const snap = byWindow.get(window);
    if (!snap) continue;
    if (snap.state === 'absent') continue;
    if (window === '5h' || window === '7d') {
      result.push(snap);
      continue;
    }
    if (snap.state === 'unobserved') continue;
    if (window === 'overage') {
      if (isOverageActive(snap)) result.push(snap);
      continue;
    }
    if (
      isObservedAtOrAfter(snap.observed_at_unix_millis, staleCutoffUnixSecs)
    ) {
      result.push(snap);
    }
  }
  return result;
}
