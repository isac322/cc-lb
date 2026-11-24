import { useCallback, useSyncExternalStore } from 'react';

// The one relative time-range preset set, in display order. Every range
// control uses it; pages with unbounded history (Logs, Audit) append
// ALL_TIME_PRESET last.

export const TIME_PRESETS = ['1h', '6h', '24h', '7d'] as const;
export type TimePreset = (typeof TIME_PRESETS)[number];

export const TIME_PRESET_SECONDS: Record<TimePreset, number> = {
  '1h': 3600,
  '6h': 6 * 3600,
  '24h': 24 * 3600,
  '7d': 7 * 24 * 3600,
};

export const ALL_TIME_PRESET = { value: 'all', label: 'All time' } as const;

export const TIME_PRESET_OPTIONS: readonly {
  value: TimePreset;
  label: string;
}[] = TIME_PRESETS.map((value) => ({ value, label: value }));

/** Presets plus a trailing "All time", for pages with unbounded history. */
export const TIME_PRESET_OPTIONS_WITH_ALL: readonly {
  value: TimePreset | 'all';
  label: string;
}[] = [...TIME_PRESET_OPTIONS, ALL_TIME_PRESET];

// Shared persisted range for surfaces whose selection is not owned by the
// URL (embedded charts, dialogs). URL-driven pages keep their own search
// param — Logs in particular keeps its "All" default and never uses this.
const SHARED_TIME_RANGE_KEY = 'cclb.timeRange';
const DEFAULT_SHARED_TIME_RANGE: TimePreset = '7d';

function isTimePreset(value: string | null): value is TimePreset {
  return value !== null && (TIME_PRESETS as readonly string[]).includes(value);
}

function readStoredTimeRange(): TimePreset {
  if (typeof window === 'undefined') return DEFAULT_SHARED_TIME_RANGE;
  try {
    const stored = window.localStorage.getItem(SHARED_TIME_RANGE_KEY);
    return isTimePreset(stored) ? stored : DEFAULT_SHARED_TIME_RANGE;
  } catch {
    return DEFAULT_SHARED_TIME_RANGE;
  }
}

let rangeSnapshot: TimePreset | undefined;
const rangeListeners = new Set<() => void>();

// Newest selection wins; listeners only fire when the value actually
// changed, so repeated reads and duplicate storage events stay free.
function publishTimeRange(next: TimePreset) {
  if (next === rangeSnapshot) return;
  rangeSnapshot = next;
  for (const listener of rangeListeners) listener();
}

function onRangeStorage(event: StorageEvent) {
  if (event.key === SHARED_TIME_RANGE_KEY || event.key === null) {
    publishTimeRange(readStoredTimeRange());
  }
}

function persistTimeRange(next: TimePreset) {
  try {
    window.localStorage.setItem(SHARED_TIME_RANGE_KEY, next);
  } catch {
    // Storage may be unavailable (private mode, quota); keep the in-memory
    // selection so the control still responds for this session.
  }
  publishTimeRange(next);
}

function subscribeTimeRange(listener: () => void) {
  rangeListeners.add(listener);
  if (rangeListeners.size === 1 && typeof window !== 'undefined') {
    window.addEventListener('storage', onRangeStorage);
    rangeSnapshot = readStoredTimeRange();
  }
  return () => {
    rangeListeners.delete(listener);
    if (rangeListeners.size === 0) {
      if (typeof window !== 'undefined') {
        window.removeEventListener('storage', onRangeStorage);
      }
      rangeSnapshot = undefined;
    }
  };
}

function getTimeRangeSnapshot(): TimePreset {
  if (typeof window === 'undefined') return DEFAULT_SHARED_TIME_RANGE;
  if (rangeSnapshot === undefined) {
    rangeSnapshot = readStoredTimeRange();
  }
  return rangeSnapshot;
}

// Stable callback identity for useSyncExternalStore's server snapshot.
function getTimeRangeServerSnapshot(): TimePreset {
  return DEFAULT_SHARED_TIME_RANGE;
}

/**
 * Persisted time range shared by non-URL-owned surfaces. Defaults to 7d,
 * ignores invalid stored values, tolerates storage failures, and follows
 * cross-tab updates. Persistence happens only inside `setRange` — never in
 * an effect — so subscribing cannot write back to storage.
 */
export function useSharedTimeRange() {
  const range = useSyncExternalStore(
    subscribeTimeRange,
    getTimeRangeSnapshot,
    getTimeRangeServerSnapshot,
  );
  const setRange = useCallback(
    (next: TimePreset) => persistTimeRange(next),
    [],
  );

  return { range, setRange };
}
