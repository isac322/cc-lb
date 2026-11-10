import type { QuotaSnapshot, SeriesResponseItem } from '../../lib/api';
import type { ProviderFamily } from '../../lib/providers';

/**
 * Canonical window order everywhere a window list is shown: the shared
 * windows, Gemini model-family windows, Claude·OSS model-family windows, the
 * legacy model windows, then extra usage. The backend's `unified`
 * account-restriction envelope is not a web-visible window: snapshots for it
 * are neither requested nor rendered.
 */
export const QUOTA_WINDOW_ORDER = [
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

function orderedWindows(latestWindows: readonly QuotaSnapshot[]): string[] {
  const known = new Set<string>(QUOTA_WINDOW_ORDER);
  return [
    ...QUOTA_WINDOW_ORDER,
    ...latestWindows
      .map((snapshot) => snapshot.window)
      .filter(
        (window, index, windows) =>
          !known.has(window) && windows.indexOf(window) === index,
      ),
  ];
}

export interface VisibleGraphWindowsInput {
  latestWindows: readonly QuotaSnapshot[] | undefined;
  series: readonly SeriesResponseItem[] | undefined;
  sinceUnixSecs: number;
}

export interface QuotaCardSnapshotsInput {
  latestWindows: readonly QuotaSnapshot[] | undefined;
  provider?: ProviderFamily;
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
  return orderedWindows(latestWindows).filter((window) => {
    const snap = byWindow.get(window);
    if (!snap) return false;
    if (window === 'unified') return false;
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
 * API reports for the upstream. Proven-absent windows are hidden. Anthropic
 * keeps its 5h and 7d unobserved placeholders; OpenAI and Gemini only show
 * windows with an actual observation. Extra usage additionally requires its
 * switch to be enabled and a finite, positive monthly budget.
 */
export function selectQuotaCardSnapshots({
  latestWindows,
  provider,
}: QuotaCardSnapshotsInput): QuotaSnapshot[] {
  if (!latestWindows) return [];
  const byWindow = new Map<string, QuotaSnapshot>(
    latestWindows.map((snap) => [snap.window, snap]),
  );
  const result: QuotaSnapshot[] = [];
  for (const window of orderedWindows(latestWindows)) {
    const snap = byWindow.get(window);
    if (!snap) continue;
    if (window === 'unified') continue;
    if (snap.state === 'absent') continue;
    if (provider !== undefined && provider !== 'anthropic') {
      if (snap.state === 'unobserved') continue;
      result.push(snap);
      continue;
    }
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
