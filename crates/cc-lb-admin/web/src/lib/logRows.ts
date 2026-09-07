import type { RequestEvent, RequestEventPartial } from './api';
import type { RequestEventWithPhase } from './RequestEventTypes';
import type { LiveEventMap } from './upsertReducer';

export const LOG_STATUS_CLASSES = ['2xx', '3xx', '4xx', '5xx'] as const;
export type LogStatusClass = (typeof LOG_STATUS_CLASSES)[number];
export interface LogRowFilters {
  readonly principal_id?: string;
  readonly upstream_id?: string;
  readonly session?: string;
  readonly model?: string;
  readonly status?: LogStatusClass;
  readonly source_kind?: 'all' | 'renewal';
}

type EventIdentity = {
  readonly event_id?: string;
  readonly request_id: string;
};

function eventIdentity(event: EventIdentity): string {
  return event.event_id ?? event.request_id;
}

function eventTimestamp(event: RequestEvent | RequestEventPartial): number {
  if (event.ts_ms != null) return event.ts_ms;
  if (event.ts != null) return event.ts * 1000;
  return 0;
}

export function filterLiveEventsByUnixSeconds(
  liveEvents: LiveEventMap,
  bounds: { readonly since?: number; readonly until?: number },
): LiveEventMap {
  if (bounds.since === undefined && bounds.until === undefined) {
    return liveEvents;
  }

  const filtered: LiveEventMap = new Map();
  for (const [identity, entry] of liveEvents) {
    const timestamp = eventTimestamp(entry.event);
    if (bounds.since !== undefined && timestamp < bounds.since * 1000) continue;
    if (bounds.until !== undefined && timestamp > bounds.until * 1000) continue;
    filtered.set(identity, entry);
  }
  return filtered;
}

// Map.set keeps an existing key in place, so taking the tail highlights newly
// inserted rows without making a partial-to-final update look newly arrived.
export function newestLiveEventIds(
  liveEvents: LiveEventMap,
  limit = 20,
): Set<string> {
  if (limit <= 0) return new Set();
  const entries = Array.from(liveEvents.values());
  const ids = new Set<string>();
  for (
    let index = Math.max(0, entries.length - limit);
    index < entries.length;
    index += 1
  ) {
    const entry = entries[index];
    if (entry) ids.add(eventIdentity(entry.event));
  }
  return ids;
}

export function filterLogRowsByStatusClass(
  rows: readonly RequestEventWithPhase[],
  statusClass?: LogStatusClass,
): readonly RequestEventWithPhase[] {
  if (statusClass === undefined) return rows;
  const lowerBound = Number(statusClass[0]) * 100;
  return rows.filter((row) => {
    const status =
      row._phase === 'final' ? row.status : (row.upstream_response_status ?? 0);
    return status >= lowerBound && status < lowerBound + 100;
  });
}
export function filterLogRows(
  rows: readonly RequestEventWithPhase[],
  filters: LogRowFilters,
): readonly RequestEventWithPhase[] {
  const statusRows = filterLogRowsByStatusClass(rows, filters.status);
  // Case-insensitive prefix, matching `lower(model) LIKE …` in storage and
  // `model_filter_matches` in the SSE fan-out. A row without a model never matches.
  const modelPrefix = filters.model?.trim().toLowerCase();
  return statusRows.filter((row) => {
    if (filters.principal_id && row.principal_id !== filters.principal_id) {
      return false;
    }
    if (filters.upstream_id && row.upstream_id !== filters.upstream_id) {
      return false;
    }
    if (filters.session && row.thread_id !== filters.session) {
      return false;
    }
    if (modelPrefix && !row.model?.toLowerCase().startsWith(modelPrefix)) {
      return false;
    }
    if (filters.source_kind === 'renewal') {
      return row.source_kind === 'renewal';
    }
    if (filters.source_kind !== 'all' && row.source_kind === 'renewal') {
      return false;
    }
    return true;
  });
}

// Keyed on the source event so an unchanged event yields the same row reference
// across merges, keeping memoized RequestEventRow subtrees from re-rendering on
// every live.version bump. WeakMap frees entries when a source event is evicted.
const finalRowCache = new WeakMap<RequestEvent, RequestEventWithPhase>();
const partialRowCache = new WeakMap<
  RequestEventPartial,
  RequestEventWithPhase
>();

function wrapFinal(event: RequestEvent): RequestEventWithPhase {
  const cached = finalRowCache.get(event);
  if (cached) return cached;
  const wrapped: RequestEventWithPhase = { ...event, _phase: 'final' };
  finalRowCache.set(event, wrapped);
  return wrapped;
}

function wrapPartial(event: RequestEventPartial): RequestEventWithPhase {
  const cached = partialRowCache.get(event);
  if (cached) return cached;
  const wrapped: RequestEventWithPhase = { ...event, _phase: 'partial' };
  partialRowCache.set(event, wrapped);
  return wrapped;
}

// Pick one page from sources whose ordering contract is already known. Page 0
// selects its live and historical identities from the full sorted rows. Later
// pages preserve the API's cursor order and wrap rows without another sort.
export function selectLogRowsForPage(
  sortedRows: readonly RequestEventWithPhase[],
  liveEvents: LiveEventMap | undefined,
  historicalEvents: readonly RequestEvent[],
): RequestEventWithPhase[] {
  if (liveEvents === undefined) {
    return historicalEvents.map(wrapFinal);
  }

  const pageEventIds = new Set<string>();
  for (const entry of liveEvents.values()) {
    pageEventIds.add(eventIdentity(entry.event));
  }
  for (const event of historicalEvents) {
    pageEventIds.add(eventIdentity(event));
  }
  return sortedRows.filter((row) => pageEventIds.has(eventIdentity(row)));
}

export function mergeLogRows(
  liveEvents: LiveEventMap,
  historicalEvents: readonly RequestEvent[],
): RequestEventWithPhase[] {
  const seen = new Set<string>();
  const rows: RequestEventWithPhase[] = [];

  for (const entry of liveEvents.values()) {
    const identity = eventIdentity(entry.event);
    if (seen.has(identity)) continue;
    seen.add(identity);
    rows.push(
      entry.phase === 'partial'
        ? wrapPartial(entry.event)
        : wrapFinal(entry.event),
    );
  }

  for (const event of historicalEvents) {
    const identity = eventIdentity(event);
    if (seen.has(identity)) continue;
    seen.add(identity);
    rows.push(wrapFinal(event));
  }

  rows.sort((left, right) => eventTimestamp(right) - eventTimestamp(left));
  return rows;
}
