import type { QuotaSnapshot, SeriesResponseItem } from '../../lib/api';

/**
 * Canonical window order everywhere a window list is shown: the shared
 * windows, the live model window, the legacy model windows, the unified
 * status, then extra usage.
 */
export const QUOTA_WINDOW_ORDER = [
  '5h',
  '7d',
  '7d_fable',
  '7d_sonnet',
  '7d_opus',
  'unified',
  'overage',
] as const;

export interface VisibleGraphWindowsInput {
  latestWindows: readonly QuotaSnapshot[] | undefined;
  series: readonly SeriesResponseItem[] | undefined;
  sinceUnixSecs: number;
}

export interface QuotaCardSnapshotsInput {
  latestWindows: readonly QuotaSnapshot[] | undefined;
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

export function isOverageActive(snap: QuotaSnapshot): boolean {
  const budget = snap.extra_usage_monthly_limit;
  return (
    snap.extra_usage_enabled === true &&
    budget != null &&
    Number.isFinite(budget) &&
    budget > 0
  );
}

/**
 * Windows the detail chart should draw for the selected range: a window is
 * visible only when its latest observation lands inside the range AND the
 * series has in-range data. Overage additionally requires an active budget.
 * Unified is an account restriction envelope, not a numeric quota series.
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
    if (window === 'unified') return false;
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
 * Windows the Quota rows render, in `QUOTA_WINDOW_ORDER`: every window the
 * API reports for the upstream. Proven-absent windows are hidden. `5h` and
 * `7d` still show an unobserved placeholder before the first successful
 * quota lookup; other windows need an observation. Extra usage additionally
 * requires its switch to be enabled and a finite, positive monthly budget.
 */
export function selectQuotaCardSnapshots({
  latestWindows,
}: QuotaCardSnapshotsInput): QuotaSnapshot[] {
  if (!latestWindows) return [];
  const byWindow = new Map<string, QuotaSnapshot>(
    latestWindows.map((snap) => [snap.window, snap]),
  );
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
    if (window === 'overage' && !isOverageActive(snap)) continue;
    result.push(snap);
  }
  return result;
}
