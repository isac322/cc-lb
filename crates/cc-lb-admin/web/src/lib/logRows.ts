import {
  type RequestEvent,
  type RequestEventKind,
  RequestEventKindSchema,
  type RequestEventPartial,
} from './api';
import type { RequestEventWithPhase } from './RequestEventTypes';
import type { LiveEventMap } from './upsertReducer';

export const LOG_STATUS_CLASSES = ['2xx', '3xx', '4xx', '5xx'] as const;
export type LogStatusClass = (typeof LOG_STATUS_CLASSES)[number];
/**
 * `errors` includes rows that ended abnormally — HTTP status >= 400, or a
 * recorded `error_code` on a delivered 2xx (mid-stream error, upstream
 * refusal, …). The backend applies this filter before historical pagination;
 * client-side filtering also covers live and in-flight rows.
 */
export const LOG_STATUS_FILTER_VALUES = [
  ...LOG_STATUS_CLASSES,
  'errors',
] as const;
export type LogStatusFilter = (typeof LOG_STATUS_FILTER_VALUES)[number];
export interface LogRowFilters {
  readonly principal_id?: string;
  readonly upstream_id?: string;
  readonly session?: string;
  readonly model?: string;
  readonly status?: LogStatusFilter;
  readonly event_kind?: RequestEventKind;
}

export type { RequestEventKind } from './api';

/** All request event kinds in the order the logs UI presents them. */
export const REQUEST_EVENT_KINDS: readonly RequestEventKind[] =
  RequestEventKindSchema.options;

export const REQUEST_EVENT_KIND_LABELS: Record<RequestEventKind, string> = {
  messages: 'Messages',
  count_tokens: 'Token count',
  models: 'Models',
  files: 'Files',
  other: 'Other proxy requests',
  renewal: 'Renewals',
  unclassified: 'Unclassified',
};

const REQUEST_EVENT_KIND_SET: Record<string, true> = Object.fromEntries(
  RequestEventKindSchema.options.map((kind) => [kind, true]),
);

/**
 * Effective endpoint category for a row, mirroring
 * `RequestEventKind::effective` on the backend: a `source_kind` of `renewal`
 * always wins (historical rows predate the `event_kind` column), a recorded
 * `event_kind` classifies directly, and anything else — missing or
 * unrecognized — is `unclassified`.
 */
export function effectiveRequestEventKind(event: {
  readonly source_kind?: string | null;
  readonly event_kind?: string | null;
}): RequestEventKind {
  if (event.source_kind === 'renewal') return 'renewal';
  const kind = event.event_kind;
  return kind != null && REQUEST_EVENT_KIND_SET[kind] === true
    ? (kind as RequestEventKind)
    : 'unclassified';
}

/** Whether a row's effective endpoint category is `messages`. */
export function isMessagesRequestEvent(event: {
  readonly source_kind?: string | null;
  readonly event_kind?: string | null;
}): boolean {
  return effectiveRequestEventKind(event) === 'messages';
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
/**
 * Whether a row ended abnormally: an HTTP error status (>= 400), or a
 * recorded `error_code` — which also marks 2xx rows that were delivered to
 * the client as a success but ended abnormally (mid-stream error, upstream
 * refusal, …). In-flight rows use the upstream status once it arrives.
 */
export function isErrorLogRow(row: RequestEventWithPhase): boolean {
  if (row._phase === 'final') {
    return row.status >= 400 || row.error_code != null;
  }
  return (row.upstream_response_status ?? 0) >= 400;
}

/** The one vendor prefix the model search ignores. */
const MODEL_VENDOR_PREFIX = 'claude-';

/**
 * Whether a row's model satisfies the user-typed model filter. Mirrors
 * `model_filter_matches` in `cc_lb_storage_api::storage_types_common` and the
 * pair of left-anchored `LIKE` patterns (`{core}%` or `claude-%{core}%`) the
 * historical SQL binds, so live rows, retained rows and fetched pages agree.
 *
 * The needle normalizes to lowercase without one leading `claude-`; an empty
 * remainder means the filter is absent, matching the backend's NULL LIKE bind.
 * `sonnet` therefore matches `claude-sonnet-4-5` and `claude-3-5-sonnet-…`,
 * while a row without a model never matches a present filter.
 */
function modelFilterMatches(
  needle: string | undefined,
  model: string | null | undefined,
): boolean {
  const trimmed = needle?.trim().toLowerCase() ?? '';
  const core = trimmed.startsWith(MODEL_VENDOR_PREFIX)
    ? trimmed.slice(MODEL_VENDOR_PREFIX.length)
    : trimmed;
  if (core === '') return true;
  if (model == null) return false;
  const normalized = model.toLowerCase();
  return (
    normalized.startsWith(core) ||
    (normalized.startsWith(MODEL_VENDOR_PREFIX) &&
      normalized.slice(MODEL_VENDOR_PREFIX.length).includes(core))
  );
}

export function filterLogRows(
  rows: readonly RequestEventWithPhase[],
  filters: LogRowFilters,
): readonly RequestEventWithPhase[] {
  const statusRows =
    filters.status === 'errors'
      ? rows.filter(isErrorLogRow)
      : filterLogRowsByStatusClass(rows, filters.status);
  // Live partial rows and fetched historical rows share this predicate.
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
    if (!modelFilterMatches(filters.model, row.model)) {
      return false;
    }
    if (
      filters.event_kind !== undefined &&
      effectiveRequestEventKind(row) !== filters.event_kind
    ) {
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
