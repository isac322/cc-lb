import type { RequestEvent, RequestEventPartial } from './api';
import type { RequestEventWithPhase } from './RequestEventTypes';
import type { LiveEventMap } from './upsertReducer';

export const MAX_RENDERED_LOG_ROWS = 500;
export const LOG_STATUS_CLASSES = ['2xx', '3xx', '4xx', '5xx'] as const;
export type LogStatusClass = (typeof LOG_STATUS_CLASSES)[number];

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

export function mergeLogRows(
  liveEvents: LiveEventMap,
  historicalEvents: readonly RequestEvent[],
): RequestEventWithPhase[] {
  const seen = new Set<string>();
  const partialRows: RequestEventWithPhase[] = [];
  const finalRows: RequestEventWithPhase[] = [];

  for (const entry of liveEvents.values()) {
    const identity = eventIdentity(entry.event);
    if (seen.has(identity)) continue;
    seen.add(identity);
    if (entry.phase === 'partial') {
      partialRows.push(wrapPartial(entry.event));
    } else {
      finalRows.push(wrapFinal(entry.event));
    }
  }

  for (const event of historicalEvents) {
    const identity = eventIdentity(event);
    if (seen.has(identity)) continue;
    seen.add(identity);
    finalRows.push(wrapFinal(event));
  }

  partialRows.sort(
    (left, right) => eventTimestamp(right) - eventTimestamp(left),
  );
  finalRows.sort((left, right) => eventTimestamp(right) - eventTimestamp(left));

  const selectedPartials = partialRows.slice(0, MAX_RENDERED_LOG_ROWS);
  const finalBudget = MAX_RENDERED_LOG_ROWS - selectedPartials.length;
  return [...selectedPartials, ...finalRows.slice(0, finalBudget)].sort(
    (left, right) => eventTimestamp(right) - eventTimestamp(left),
  );
}
