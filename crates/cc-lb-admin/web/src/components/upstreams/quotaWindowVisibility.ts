import type {
  AggregateResponse,
  PeriodUnknownQuotaMeter,
  QuotaSnapshot,
  SeriesResponseItem,
} from '../../lib/api';
import type { ProviderFamily } from '../../lib/providers';
import { quotaPacePct } from '../../lib/quotaSeverity';
import { selectCodexCredits } from './codexCredits';

/**
 * Canonical window order everywhere a window list is shown: the shared
 * windows, Gemini model-family windows, Claude·OSS model-family windows, the
 * legacy model windows, then extra usage. The `unified` restriction envelope
 * is requested for routing truth but displayed only when it carries Codex
 * credits. Credits from Overage and Unified share one row and never a chart.
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
  'unified',
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
    if (snap.credits != null) return false;
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
 * Neutral status of a Gemini model-family window kept beside its group's
 * reported sibling: `not-reported` when the latest enumeration returned it
 * `absent`, `no-reading` when it is `unobserved` or missing.
 */
export type GeminiGroupPlaceholderState = 'not-reported' | 'no-reading';

/** Visible status copy for each placeholder state, shared by every surface. */
export const GEMINI_GROUP_PLACEHOLDER_TEXT: Readonly<
  Record<GeminiGroupPlaceholderState, string>
> = {
  'not-reported': 'Not reported',
  'no-reading': 'No reading',
};

const GEMINI_GROUP_SIBLING: Readonly<Record<string, string>> = {
  gemini_5h: 'gemini_7d',
  gemini_7d: 'gemini_5h',
  third_party_5h: 'third_party_7d',
  third_party_7d: 'third_party_5h',
};

/** The other duration of the same Gemini quota group, or null. */
export function geminiGroupSibling(window: string): string | null {
  return GEMINI_GROUP_SIBLING[window] ?? null;
}

/**
 * A Gemini group (`gemini_5h`/`gemini_7d`, `third_party_5h`/`third_party_7d`)
 * whose one window has a numeric reading keeps its other window visible as a
 * neutral placeholder: no meter, pace, reset or number, never inferred from
 * the sibling. Null when `window` is outside a Gemini group, has its own
 * snapshot to render, or its sibling has no reading either (an all-absent
 * group stays hidden).
 */
export function geminiGroupPlaceholderState(
  windows: readonly QuotaSnapshot[] | null | undefined,
  window: string,
): GeminiGroupPlaceholderState | null {
  const sibling = geminiGroupSibling(window);
  if (sibling == null || !windows) return null;
  const reading = windows.find((snap) => snap.window === sibling);
  if (
    reading == null ||
    (reading.state !== 'fresh' && reading.state !== 'stale') ||
    reading.utilization == null ||
    !Number.isFinite(reading.utilization)
  ) {
    return null;
  }
  const own = windows.find((snap) => snap.window === window);
  if (own == null || own.state === 'unobserved') return 'no-reading';
  if (own.state === 'absent') return 'not-reported';
  return null;
}

/**
 * True for a Gemini group window whose numeric (fresh or stale) reading was
 * observed before the newest observation of its group: the sibling canonical
 * window or a same-group period-unknown meter. A poll that omits a window
 * leaves its earlier reading in place, so that reading is retained, never
 * current: callers present it as stale and draw no pace.
 */
export function geminiRetainedReading(
  windows: readonly QuotaSnapshot[] | null | undefined,
  periodUnknownMeters: readonly PeriodUnknownQuotaMeter[] | null | undefined,
  window: string,
): boolean {
  const sibling = geminiGroupSibling(window);
  if (sibling == null || !windows) return false;
  const own = windows.find((snap) => snap.window === window);
  if (
    own == null ||
    (own.state !== 'fresh' && own.state !== 'stale') ||
    own.utilization == null ||
    !Number.isFinite(own.utilization) ||
    own.observed_at_unix_millis == null
  ) {
    return false;
  }
  const group = window.startsWith('gemini_') ? 'gemini' : 'third_party';
  const newest = Math.max(
    windows.find((snap) => snap.window === sibling)?.observed_at_unix_millis ??
      Number.NEGATIVE_INFINITY,
    ...(periodUnknownMeters ?? [])
      .filter((meter) => meter.group === group)
      .map((meter) => meter.observed_at_unix_millis),
  );
  return own.observed_at_unix_millis < newest;
}

/**
 * Pool counterpart of `geminiRetainedReading`: for each Gemini group window
 * where an upstream's lot was observed before that upstream's sibling lot,
 * the earliest such observation (unix seconds). The pool figure then carries
 * a retained reading: it is stale, and the plot after that time is not a
 * current reading.
 */
export function poolRetainedSince(
  aggregate: AggregateResponse | undefined,
): Partial<Record<string, number>> {
  const retained: Partial<Record<string, number>> = {};
  for (const entry of aggregate?.windows ?? []) {
    const sibling = geminiGroupSibling(entry.window);
    if (sibling == null) continue;
    const siblingLots =
      aggregate?.windows.find((candidate) => candidate.window === sibling)
        ?.provider_lots ?? [];
    for (const lot of entry.provider_lots) {
      if (
        lot.state === 'absent' ||
        lot.utilization == null ||
        lot.observed_at_unix_millis == null
      ) {
        continue;
      }
      const newer = siblingLots.find(
        (candidate) => candidate.upstream_id === lot.upstream_id,
      )?.observed_at_unix_millis;
      if (newer == null || lot.observed_at_unix_millis >= newer) continue;
      retained[entry.window] = Math.min(
        retained[entry.window] ?? Number.POSITIVE_INFINITY,
        lot.observed_at_unix_millis / 1000,
      );
    }
  }
  return retained;
}

/**
 * Pool counterpart of `geminiGroupPlaceholderState`: Gemini group windows
 * left out of the pool while their same-group sibling is shown. The legend
 * keeps them as a neutral status, never a chart series or a value inferred
 * from the sibling: `not-reported` when every provider lot is absent,
 * otherwise `no-reading`. Empty while loading.
 */
export function poolGroupPlaceholders(
  defaults: readonly string[],
  visibleWindows: readonly string[],
  aggregate: AggregateResponse | undefined,
  loading: boolean,
): Partial<Record<string, GeminiGroupPlaceholderState>> {
  const placeholders: Partial<Record<string, GeminiGroupPlaceholderState>> = {};
  if (loading) return placeholders;
  for (const window of defaults) {
    const sibling = geminiGroupSibling(window);
    if (
      sibling == null ||
      visibleWindows.includes(window) ||
      !visibleWindows.includes(sibling)
    ) {
      continue;
    }
    const lots =
      aggregate?.windows.find((entry) => entry.window === window)
        ?.provider_lots ?? [];
    placeholders[window] =
      lots.length > 0 && lots.every((lot) => lot.state === 'absent')
        ? 'not-reported'
        : 'no-reading';
  }
  return placeholders;
}

/** A Gemini group placeholder for a window the response did not carry. */
const MISSING_WINDOW_SNAPSHOT: Omit<QuotaSnapshot, 'window'> = {
  state: 'unobserved',
  source: null,
  utilization: null,
  status: null,
  resets_at_unix_secs: null,
  surpassed_threshold: null,
  representative_claim: null,
  disabled_reason: null,
  extra_usage_enabled: null,
  extra_usage_monthly_limit: null,
  extra_usage_used_credits: null,
  observed_at_unix_millis: null,
  age_secs: null,
};

/**
 * API reports for the upstream. Proven-absent windows are hidden, except a
 * Gemini group window kept as a placeholder beside its reported sibling
 * (`geminiGroupPlaceholderState`; a missing one is synthesized as
 * `unobserved`). Anthropic keeps its 5h and 7d unobserved placeholders;
 * OpenAI and Gemini otherwise only show windows with an actual observation.
 * Extra usage additionally requires its switch to be enabled and a finite,
 * positive monthly budget.
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
    if (geminiGroupPlaceholderState(latestWindows, window) != null) {
      result.push(
        snap ?? {
          ...MISSING_WINDOW_SNAPSHOT,
          window: window as QuotaSnapshot['window'],
        },
      );
      continue;
    }
    if (!snap) continue;
    // Credit observations from the API (Overage) and headers (Unified) are
    // rendered once below, with each fact retaining its source and freshness.
    if (window === 'unified' || snap.credits != null) continue;
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
  if (selectCodexCredits(latestWindows) != null) {
    const creditSnapshot = latestWindows.find(
      (snapshot) =>
        (snapshot.window === 'overage' || snapshot.window === 'unified') &&
        snapshot.credits != null &&
        snapshot.state !== 'absent' &&
        snapshot.state !== 'unobserved',
    );
    if (creditSnapshot) result.push({ ...creditSnapshot, window: 'unified' });
  }
  return result;
}

/** Keep the legend and the actual row's pace tick on the same eligibility rule. */
export function quotaCardPacePct(
  snap: QuotaSnapshot,
  nowUnixSecs: number,
): number | null {
  if (
    snap.window === 'overage' ||
    snap.credits != null ||
    snap.state === 'unobserved' ||
    snap.state === 'absent' ||
    snap.utilization == null ||
    snap.resets_at_unix_secs == null ||
    snap.resets_at_unix_secs <= nowUnixSecs
  ) {
    return null;
  }
  return quotaPacePct(snap.window, snap.resets_at_unix_secs, nowUnixSecs);
}

export function hasQuotaCardPace(
  snap: QuotaSnapshot,
  nowUnixSecs: number,
): boolean {
  return quotaCardPacePct(snap, nowUnixSecs) != null;
}

const CANONICAL_WINDOWS: Record<ProviderFamily, readonly string[]> = {
  anthropic: ['5h', '7d'],
  openai: ['5h', '7d'],
  gemini: ['gemini_5h', 'gemini_7d', 'third_party_5h', 'third_party_7d'],
};

export function selectQuotaDetailState({
  latestWindows,
  cards,
  rawMeters,
  provider,
}: {
  latestWindows: readonly QuotaSnapshot[] | null | undefined;
  cards: readonly QuotaSnapshot[];
  rawMeters: readonly PeriodUnknownQuotaMeter[];
  provider: ProviderFamily;
}): 'no-data' | 'rows' | 'not-offered' | 'no-reading' {
  if (!latestWindows) return 'no-data';
  if (cards.length > 0 || rawMeters.length > 0) return 'rows';
  if (latestWindows.length === 0) return 'no-data';
  if (
    CANONICAL_WINDOWS[provider].every((window) =>
      latestWindows.some(
        (snap) => snap.window === window && snap.state === 'absent',
      ),
    )
  ) {
    return 'not-offered';
  }
  return 'no-reading';
}
