import { useQueryClient } from '@tanstack/react-query';
import {
  useCallback,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
} from 'react';
import type { RequestEvent, RequestEventKind } from './api';
import {
  filterLiveEventsByUnixSeconds,
  filterLogRows,
  type LogRowFilters,
  mergeLogRows,
  selectLogRowsForPage,
} from './logRows';
import {
  getRecentEventsCursor,
  type RecentEventsPageParam,
  recentEventsPageQueryOptions,
  useRecentEventsPage,
} from './queries';
import type { RequestEventWithPhase } from './RequestEventTypes';
import {
  type ConnectionStatus,
  type LiveEventStreamState,
  useLiveEventStream,
} from './useLiveEventStream';

export type RequestEventsFeedFilters = Omit<LogRowFilters, 'event_kind'> & {
  readonly event_kind?: RequestEventKind | 'all';
  readonly since_unix_secs?: number;
  readonly until_unix_secs?: number;
};

/**
 * How the feed exposes rows to the DOM.
 *
 * - `paged`: one `pageSize` page at a time with `previousPage`/`nextPage`
 *   (the Logs surface). History is unbounded and pages are immutable
 *   snapshots.
 * - `infinite`: a stable top slice of the newest rows that grows by
 *   `pageSize` each time `loadMore` runs (driven by a scroll sentinel).
 *   Older history is fetched only when the retained rows are exhausted, and
 *   retention stops at `maxRetained`, after which `hasMore` is false.
 * - `preview`: the newest `pageSize` rows only; no paging or loading more.
 */
export type RequestEventsFeedMode = 'paged' | 'infinite' | 'preview';

export interface UseRequestEventsFeedOptions {
  readonly filters?: RequestEventsFeedFilters;
  readonly live?: boolean;
  /** Defaults to `paged`. See {@link RequestEventsFeedMode}. */
  readonly mode?: RequestEventsFeedMode;
  /**
   * Number of historical rows requested first. Defaults to 500. In
   * `infinite` mode it never exceeds `maxRetained`.
   */
  readonly initialHistoryLimit?: number;
  /**
   * Rows per page (`paged`), rows shown (`preview`), or the initial slice and
   * growth step (`infinite`). Capped at 50.
   */
  readonly pageSize?: number;
  /**
   * `infinite` only: the most rows the feed renders and the most history rows
   * it keeps. Defaults to 500. Older history is trimmed only as newer rows
   * push it past the rendered window, never from the rendered prefix.
   */
  readonly maxRetained?: number;
}

export interface RequestEventsFeedState {
  readonly mode: RequestEventsFeedMode;
  readonly rows: readonly RequestEventWithPhase[];
  readonly filteredRows: readonly RequestEventWithPhase[];
  /** Rows to render: the current page, the grown slice, or the preview. */
  readonly pageRows: readonly RequestEventWithPhase[];
  readonly firstPageEvents: readonly RequestEvent[] | undefined;
  readonly historyFilters: Record<string, string>;
  readonly loading: boolean;
  readonly isFetching: boolean;
  readonly error: Error | null;
  readonly page: number;
  readonly pageCount: number;
  readonly pageSize: number;
  /** `infinite` only: rows currently revealed; otherwise `pageRows.length`. */
  readonly visibleCount: number;
  /** `infinite` only: retention bound; otherwise `Infinity`. */
  readonly maxRetained: number;
  readonly totalRows: number;
  /** More rows are reachable via `nextPage` (paged) or `loadMore` (infinite). */
  readonly hasMore: boolean;
  /** A next page or older history request is in flight. */
  readonly loadingNext: boolean;
  readonly previousPage: () => void;
  readonly nextPage: () => Promise<void>;
  /**
   * `infinite`: reveal the next `pageSize` retained rows, fetching older
   * history first when none remain. `paged`: same as `nextPage`. `preview`:
   * no-op. Safe to call repeatedly; concurrent calls coalesce.
   */
  readonly loadMore: () => Promise<void>;
  readonly refresh: () => void;
  readonly live: LiveEventStreamState;
  readonly liveEnabled: boolean;
  readonly tailStatus: ConnectionStatus | 'failed';
  readonly statusLabel: string;
  readonly statusColor: 'neutral' | 'ok' | 'warn' | 'danger';
  readonly liveFlashIds: Set<string>;
}

const DEFAULT_INITIAL_HISTORY_LIMIT = 500;
const DEFAULT_PAGE_SIZE = 50;
const MAX_PAGE_SIZE = 50;
const DEFAULT_MAX_RETAINED = 500;
/** Mirrors `MAX_RECENT_EVENTS_LIMIT` on `/admin/v1/events/recent`. */
const MAX_HISTORY_REQUEST = 500;

type OrderedEvent = {
  readonly event_id?: string;
  readonly request_id: string;
  readonly ts?: number | null;
  readonly ts_ms?: number | null;
};

function stringFilterKey(filters: Record<string, string | undefined>): string {
  return JSON.stringify(
    Object.entries(filters)
      .filter(([, value]) => value !== undefined && value !== '')
      .sort(([left], [right]) => left.localeCompare(right)),
  );
}

function eventIdentity(event: {
  readonly event_id?: string;
  readonly request_id: string;
}): string {
  return event.event_id ?? event.request_id;
}

function eventTimestampMs(event: OrderedEvent): number {
  if (event.ts_ms != null) return event.ts_ms;
  if (event.ts != null) return event.ts * 1000;
  return 0;
}

/**
 * Newest first by timestamp, then by identity descending: the order and
 * compound `(ts_ms, event_id)` cursor of `/admin/v1/events/recent`, so
 * timestamp ties render deterministically and match pagination boundaries.
 */
function compareNewestFirst(left: OrderedEvent, right: OrderedEvent): number {
  const byTime = eventTimestampMs(right) - eventTimestampMs(left);
  if (byTime !== 0) return byTime;
  const leftId = eventIdentity(left);
  const rightId = eventIdentity(right);
  return leftId < rightId ? 1 : leftId > rightId ? -1 : 0;
}

/**
 * Merge history pages by identity, keep them newest first, and drop the oldest
 * rows past `maxRetained`. Returns `current` when nothing new arrived.
 */
function mergeHistoryEvents(
  current: readonly RequestEvent[],
  additions: readonly RequestEvent[],
  maxRetained: number,
): readonly RequestEvent[] {
  const seen = new Set(current.map(eventIdentity));
  let merged: RequestEvent[] | undefined;
  for (const event of additions) {
    const identity = eventIdentity(event);
    if (seen.has(identity)) continue;
    seen.add(identity);
    merged ??= [...current];
    merged.push(event);
  }
  if (merged === undefined) return current;
  merged.sort(compareNewestFirst);
  if (merged.length > maxRetained) merged.length = maxRetained;
  return merged;
}

function normalizeFilters(filters: RequestEventsFeedFilters): LogRowFilters & {
  readonly since_unix_secs?: number;
  readonly until_unix_secs?: number;
} {
  return {
    ...filters,
    event_kind: filters.event_kind === 'all' ? undefined : filters.event_kind,
  };
}

/** Convert the shared row filters into the historical list API query. */
export function buildRequestEventsHistoryFilters(
  input: RequestEventsFeedFilters = {},
): Record<string, string> {
  const filters = normalizeFilters(input);
  const base: Record<string, string> = {};
  if (filters.principal_id) base.principal_id = filters.principal_id;
  if (filters.upstream_id) base.upstream_id = filters.upstream_id;
  if (filters.session) base.thread_id = filters.session;
  if (filters.model) base.model = filters.model;
  if (filters.status) base.status_class = filters.status;
  if (filters.event_kind) base.event_kind = filters.event_kind;
  if (filters.since_unix_secs != null) {
    base.since_unix_secs = String(filters.since_unix_secs);
  }
  if (filters.until_unix_secs != null) {
    base.until_unix_secs = String(filters.until_unix_secs);
  }
  return base;
}

/** Convert the shared row filters into the live SSE query. */
export function buildRequestEventsLiveFilters(
  input: RequestEventsFeedFilters = {},
): Record<string, string> {
  const filters = normalizeFilters(input);
  const base: Record<string, string> = {};
  if (filters.principal_id) base.principal_id = filters.principal_id;
  if (filters.upstream_id) base.upstream_id = filters.upstream_id;
  if (filters.session) base.thread_id = filters.session;
  if (filters.model) base.model = filters.model;
  // Errors are intentionally client-side. A partial row can be corrected from
  // an upstream 4xx to a successful final response and must keep streaming.
  if (filters.status && filters.status !== 'errors') {
    base.status_class = filters.status;
  }
  if (filters.event_kind) base.event_kind = filters.event_kind;
  return base;
}

function rowFiltersFrom(input: RequestEventsFeedFilters): LogRowFilters {
  const normalized = normalizeFilters(input);
  return {
    principal_id: normalized.principal_id,
    upstream_id: normalized.upstream_id,
    session: normalized.session,
    model: normalized.model,
    status: normalized.status,
    event_kind: normalized.event_kind,
  };
}

function statusFor(
  liveEnabled: boolean,
  awaitingHistory: boolean,
  live: LiveEventStreamState,
): {
  tailStatus: ConnectionStatus | 'failed';
  statusLabel: string;
  statusColor: 'neutral' | 'ok' | 'warn' | 'danger';
} {
  // The stream waits for the history watermark before connecting; that is
  // part of connecting, not the tail being off.
  const tailStatus = !liveEnabled
    ? 'idle'
    : live.permanentFailure
      ? 'failed'
      : awaitingHistory && live.status === 'idle'
        ? 'connecting'
        : live.status;
  return {
    tailStatus,
    statusLabel: {
      idle: 'Off',
      connecting: 'Connecting…',
      live: 'Live',
      stale: 'Stale',
      reconnecting: 'Reconnecting…',
      hidden: 'Paused',
      error: 'Offline',
      failed: 'Failed',
    }[tailStatus],
    statusColor: (
      {
        idle: 'neutral',
        connecting: 'neutral',
        live: 'ok',
        stale: 'warn',
        reconnecting: 'warn',
        hidden: 'neutral',
        error: 'danger',
        failed: 'danger',
      } as const
    )[tailStatus],
  };
}

export function useRequestEventsFeed(
  options: UseRequestEventsFeedOptions = {},
): RequestEventsFeedState {
  const {
    principal_id,
    upstream_id,
    session,
    model,
    status: statusFilter,
    event_kind,
    since_unix_secs,
    until_unix_secs,
  } = options.filters ?? {};
  const filters = useMemo(
    () => ({
      principal_id,
      upstream_id,
      session,
      model,
      status: statusFilter,
      event_kind: event_kind === 'all' ? undefined : event_kind,
      since_unix_secs,
      until_unix_secs,
    }),
    [
      principal_id,
      upstream_id,
      session,
      model,
      statusFilter,
      event_kind,
      since_unix_secs,
      until_unix_secs,
    ],
  );
  const historyFilters = useMemo(
    () => buildRequestEventsHistoryFilters(filters),
    [filters],
  );
  const liveFilters = useMemo(
    () => buildRequestEventsLiveFilters(filters),
    [filters],
  );
  const rowFilters = useMemo(() => rowFiltersFrom(filters), [filters]);
  const filterKey = stringFilterKey(historyFilters);
  const liveFilterKey = stringFilterKey(liveFilters);
  const mode: RequestEventsFeedMode = options.mode ?? 'paged';
  const infinite = mode === 'infinite';
  const liveEnabled = options.live !== false && filters.until_unix_secs == null;
  const pageSize = Math.min(
    MAX_PAGE_SIZE,
    Math.max(1, Math.floor(options.pageSize ?? DEFAULT_PAGE_SIZE)),
  );
  const maxRetained = infinite
    ? Math.max(
        pageSize,
        Math.floor(options.maxRetained ?? DEFAULT_MAX_RETAINED),
      )
    : Number.POSITIVE_INFINITY;
  const initialHistoryLimit = Math.min(
    MAX_HISTORY_REQUEST,
    maxRetained,
    Math.max(
      DEFAULT_PAGE_SIZE,
      Math.floor(options.initialHistoryLimit ?? DEFAULT_INITIAL_HISTORY_LIMIT),
    ),
  );
  const queryClient = useQueryClient();
  const initialPageParam = useMemo<RecentEventsPageParam>(
    () => ({ kind: 'initial', limit: initialHistoryLimit }),
    [initialHistoryLimit],
  );
  const recent = useRecentEventsPage(historyFilters, initialPageParam);
  // Connect the tail only once the history it merges with has answered, and
  // resume it from that history's watermark: every event committed after the
  // history snapshot is replayed, so none fall between the two.
  const historySettled = recent.data !== undefined || recent.isError;
  const historyWatermark = recent.data?.cursor;
  const live = useLiveEventStream(liveFilters, {
    enabled: liveEnabled && historySettled,
    seedCursor:
      typeof historyWatermark === 'number'
        ? String(historyWatermark)
        : undefined,
  });

  const paginationIdentity = `${filterKey}|${mode}|${initialHistoryLimit}|${pageSize}|${maxRetained}`;
  const [activePaginationIdentity, setActivePaginationIdentity] =
    useState(paginationIdentity);
  const [historyEvents, setHistoryEvents] = useState<readonly RequestEvent[]>(
    [],
  );
  const [snapshots, setSnapshots] = useState<
    Array<{
      cursor: { ts_ms: number; event_id: string };
      events: readonly RequestEvent[];
    }>
  >([]);
  const [historyExhausted, setHistoryExhausted] = useState(false);
  const [exhaustedCursorKeys, setExhaustedCursorKeys] = useState<Set<string>>(
    () => new Set(),
  );
  const [page, setPage] = useState(0);
  const [visibleCount, setVisibleCount] = useState(pageSize);
  const [loadingNext, setLoadingNext] = useState(false);
  const [nextError, setNextError] = useState<Error | null>(null);
  const requestGenerationRef = useRef(0);
  const loadInFlightRef = useRef(false);
  const paginationRequestIdentityRef = useRef(paginationIdentity);
  useLayoutEffect(() => {
    if (paginationRequestIdentityRef.current === paginationIdentity) return;
    paginationRequestIdentityRef.current = paginationIdentity;
    requestGenerationRef.current += 1;
    loadInFlightRef.current = false;
  }, [paginationIdentity]);
  useEffect(
    () => () => {
      requestGenerationRef.current += 1;
    },
    [],
  );

  const paginationIdentityChanged =
    activePaginationIdentity !== paginationIdentity;
  if (paginationIdentityChanged) {
    setActivePaginationIdentity(paginationIdentity);
    setHistoryEvents([]);
    setSnapshots([]);
    setHistoryExhausted(false);
    setExhaustedCursorKeys(new Set());
    setPage(0);
    setVisibleCount(pageSize);
    setLoadingNext(false);
    setNextError(null);
  }

  const activeHistoryEvents = paginationIdentityChanged ? [] : historyEvents;
  const activeHistoryExhausted = paginationIdentityChanged
    ? false
    : historyExhausted;
  const activePage = paginationIdentityChanged ? 0 : page;
  const activeVisibleCount = paginationIdentityChanged
    ? pageSize
    : visibleCount;

  useEffect(() => {
    if (recent.data === undefined || recent.isPlaceholderData) return;
    const events = recent.data.events;
    setHistoryEvents((current) =>
      mergeHistoryEvents(current, events, maxRetained),
    );
    if (events.length < recent.data.limit) setHistoryExhausted(true);
  }, [recent.data, recent.isPlaceholderData, maxRetained]);

  // biome-ignore lint/correctness/useExhaustiveDependencies: The SSE reducer mutates eventsMap in place; version is its content revision.
  const rangedLiveEvents = useMemo(
    () =>
      filterLiveEventsByUnixSeconds(live.eventsMap, {
        since: filters.since_unix_secs,
        until: filters.until_unix_secs,
      }),
    [
      live.eventsMap,
      live.version,
      filters.since_unix_secs,
      filters.until_unix_secs,
    ],
  );
  // biome-ignore lint/correctness/useExhaustiveDependencies: Unbounded live ranges retain the mutable SSE map, so its revision invalidates merged rows.
  const rows = useMemo(
    // mergeLogRows orders by timestamp only; break ties by identity so rows
    // never swap places between renders and agree with the history cursor.
    () =>
      mergeLogRows(rangedLiveEvents, activeHistoryEvents).sort(
        compareNewestFirst,
      ),
    [rangedLiveEvents, activeHistoryEvents, live.version],
  );
  const filteredRows = useMemo(
    () => filterLogRows(rows, rowFilters),
    [rows, rowFilters],
  );

  // ── infinite ───────────────────────────────────────────────────────────
  const renderLimit = Math.min(activeVisibleCount, maxRetained);
  const historyTail = activeHistoryEvents[activeHistoryEvents.length - 1];
  const historyTailCursor =
    historyTail === undefined ? undefined : getRecentEventsCursor(historyTail);
  const historyTailTs = historyTailCursor?.ts_ms;
  const historyTailEventId = historyTailCursor?.event_id;
  const canFetchOlder =
    infinite &&
    !activeHistoryExhausted &&
    historyTailCursor !== undefined &&
    activeHistoryEvents.length < maxRetained;
  const retainedBeyondWindow = filteredRows.length > renderLimit;
  const infiniteHasMore =
    infinite &&
    renderLimit < maxRetained &&
    (retainedBeyondWindow || canFetchOlder);

  const loadOlder = useCallback(async () => {
    if (!infinite || loadInFlightRef.current) return;
    if (retainedBeyondWindow) {
      setVisibleCount((current) => Math.min(current + pageSize, maxRetained));
      return;
    }
    if (
      !canFetchOlder ||
      historyTailTs === undefined ||
      historyTailEventId === undefined
    ) {
      return;
    }
    const limit = Math.min(
      MAX_PAGE_SIZE,
      maxRetained - activeHistoryEvents.length,
    );
    const requestGeneration = requestGenerationRef.current + 1;
    requestGenerationRef.current = requestGeneration;
    loadInFlightRef.current = true;
    setLoadingNext(true);
    setNextError(null);
    try {
      const older = await queryClient.fetchQuery(
        recentEventsPageQueryOptions(historyFilters, {
          kind: 'cursor',
          limit,
          ts_ms: historyTailTs,
          event_id: historyTailEventId,
        }),
      );
      if (requestGeneration !== requestGenerationRef.current) return;
      setHistoryEvents((current) =>
        mergeHistoryEvents(current, older.events, maxRetained),
      );
      if (older.events.length < limit) setHistoryExhausted(true);
      if (older.events.length > 0) {
        setVisibleCount((current) => Math.min(current + pageSize, maxRetained));
      }
    } catch (error) {
      if (requestGeneration === requestGenerationRef.current) {
        setNextError(
          error instanceof Error ? error : new Error('Unable to load requests'),
        );
      }
    } finally {
      if (requestGeneration === requestGenerationRef.current) {
        loadInFlightRef.current = false;
        setLoadingNext(false);
      }
    }
  }, [
    activeHistoryEvents.length,
    canFetchOlder,
    historyFilters,
    historyTailEventId,
    historyTailTs,
    infinite,
    maxRetained,
    pageSize,
    queryClient,
    retainedBeyondWindow,
  ]);

  // ── paged ──────────────────────────────────────────────────────────────
  const activeSnapshots = paginationIdentityChanged ? [] : snapshots;
  const pageCount = mode === 'paged' ? activeSnapshots.length + 1 : 1;
  const clampedPage = Math.min(activePage, pageCount - 1);
  const pageRows = useMemo(() => {
    if (infinite) return filteredRows.slice(0, renderLimit);
    if (clampedPage === 0) return filteredRows.slice(0, pageSize);
    const snapshotEvents = activeSnapshots[clampedPage - 1]?.events ?? [];
    const snapshotIds = new Set(snapshotEvents.map(eventIdentity));
    const snapshotLiveEvents = new Map(
      Array.from(rangedLiveEvents.entries()).filter(([identity]) =>
        snapshotIds.has(identity),
      ),
    );
    return filterLogRows(
      selectLogRowsForPage(rows, snapshotLiveEvents, snapshotEvents),
      rowFilters,
    );
  }, [
    activeSnapshots,
    clampedPage,
    filteredRows,
    infinite,
    pageSize,
    rangedLiveEvents,
    renderLimit,
    rowFilters,
    rows,
  ]);
  const currentCursor = useMemo(
    () =>
      mode === 'paged' && pageRows.length
        ? getRecentEventsCursor(pageRows[pageRows.length - 1])
        : undefined,
    [mode, pageRows],
  );
  const currentCursorTs = currentCursor?.ts_ms;
  const currentCursorEventId = currentCursor?.event_id;
  const currentCursorKey =
    currentCursorTs === undefined || currentCursorEventId === undefined
      ? undefined
      : `${currentCursorTs}:${currentCursorEventId}`;
  const remainingHistoryEvents = useMemo(() => {
    if (currentCursorTs === undefined || currentCursorEventId === undefined) {
      return [];
    }
    // Page snapshots are immutable identity windows; choosing a future window
    // from merged rows also keeps late live finals below page zero reachable.
    return filteredRows.filter(
      (event): event is Extract<RequestEventWithPhase, { _phase: 'final' }> => {
        if (event._phase !== 'final') return false;
        const cursor = getRecentEventsCursor(event);
        return (
          cursor !== undefined &&
          (cursor.ts_ms < currentCursorTs ||
            (cursor.ts_ms === currentCursorTs &&
              cursor.event_id < currentCursorEventId))
        );
      },
    );
  }, [currentCursorEventId, currentCursorTs, filteredRows]);
  const currentCursorIsRetained = useMemo(() => {
    if (currentCursorEventId === undefined) return false;
    return (
      activeHistoryEvents.some(
        (event) => eventIdentity(event) === currentCursorEventId,
      ) || rangedLiveEvents.has(currentCursorEventId)
    );
  }, [activeHistoryEvents, currentCursorEventId, rangedLiveEvents]);
  const pagedHasMore =
    currentCursor !== undefined &&
    (clampedPage < pageCount - 1 ||
      remainingHistoryEvents.length > 0 ||
      (!activeHistoryExhausted &&
        currentCursorKey !== undefined &&
        !exhaustedCursorKeys.has(currentCursorKey)));

  const nextPage = useCallback(async () => {
    if (
      mode !== 'paged' ||
      loadingNext ||
      currentCursorTs === undefined ||
      currentCursorEventId === undefined ||
      currentCursorKey === undefined
    ) {
      return;
    }
    const cursor = {
      ts_ms: currentCursorTs,
      event_id: currentCursorEventId,
    };
    const nextPageIndex = clampedPage + 1;
    const cached = activeSnapshots[clampedPage];
    if (
      cached?.cursor.ts_ms === currentCursorTs &&
      cached.cursor.event_id === currentCursorEventId
    ) {
      requestGenerationRef.current += 1;
      setPage(nextPageIndex);
      return;
    }
    const rememberPage = (events: readonly RequestEvent[]) => {
      setSnapshots((current) => [
        ...current.slice(0, clampedPage),
        { cursor, events },
      ]);
      setPage(nextPageIndex);
    };
    if (
      currentCursorIsRetained &&
      (remainingHistoryEvents.length >= pageSize || activeHistoryExhausted)
    ) {
      const events = remainingHistoryEvents.slice(0, pageSize);
      if (events.length > 0) rememberPage(events);
      return;
    }
    if (exhaustedCursorKeys.has(currentCursorKey)) return;
    const requestGeneration = requestGenerationRef.current + 1;
    requestGenerationRef.current = requestGeneration;
    setLoadingNext(true);
    setNextError(null);
    try {
      const next = await queryClient.fetchQuery(
        recentEventsPageQueryOptions(historyFilters, {
          kind: 'cursor',
          limit: pageSize,
          ...cursor,
        }),
      );
      if (requestGeneration !== requestGenerationRef.current) return;
      if (next.events.length === 0) {
        setExhaustedCursorKeys((current) =>
          new Set(current).add(currentCursorKey),
        );
        return;
      }
      setHistoryEvents((current) =>
        mergeHistoryEvents(current, next.events, maxRetained),
      );
      if (next.events.length < next.limit) setHistoryExhausted(true);
      rememberPage(next.events);
    } catch (error) {
      if (requestGeneration === requestGenerationRef.current) {
        setNextError(
          error instanceof Error ? error : new Error('Unable to load requests'),
        );
      }
    } finally {
      if (requestGeneration === requestGenerationRef.current) {
        setLoadingNext(false);
      }
    }
  }, [
    activeHistoryExhausted,
    activeSnapshots,
    clampedPage,
    currentCursorIsRetained,
    currentCursorEventId,
    currentCursorKey,
    currentCursorTs,
    exhaustedCursorKeys,
    historyFilters,
    loadingNext,
    maxRetained,
    mode,
    pageSize,
    queryClient,
    remainingHistoryEvents,
  ]);

  const previousPage = useCallback(() => {
    if (mode !== 'paged') return;
    requestGenerationRef.current += 1;
    setLoadingNext(false);
    setNextError(null);
    setPage((current) => Math.max(0, current - 1));
  }, [mode]);

  const loadMore = useCallback(async () => {
    if (mode === 'infinite') return loadOlder();
    if (mode === 'paged') return nextPage();
  }, [loadOlder, mode, nextPage]);

  const refresh = useCallback(() => {
    requestGenerationRef.current += 1;
    loadInFlightRef.current = false;
    setLoadingNext(false);
    setNextError(null);
    void recent.refetch();
  }, [recent.refetch]);
  const flashStateRef = useRef<{
    filterKey: string | null;
    known: Set<string>;
    armed: boolean;
  }>({ filterKey: null, known: new Set(), armed: false });
  // biome-ignore lint/correctness/useExhaustiveDependencies: Arrival tracking must observe in-place SSE map updates through their revision.
  const liveFlashIds = useMemo(() => {
    const state = flashStateRef.current;
    if (state.filterKey !== liveFilterKey) {
      state.filterKey = liveFilterKey;
      state.known.clear();
      state.armed = false;
    }
    if (!state.armed) {
      for (const entry of live.eventsMap.values()) {
        state.known.add(eventIdentity(entry.event));
      }
      state.armed = true;
      return new Set<string>();
    }
    for (const identity of state.known) {
      if (!live.eventsMap.has(identity)) state.known.delete(identity);
    }
    const flashIds = new Set<string>();
    for (const entry of live.eventsMap.values()) {
      const identity = eventIdentity(entry.event);
      if (!state.known.has(identity)) flashIds.add(identity);
      state.known.add(identity);
    }
    return flashIds;
  }, [live.eventsMap, live.version, liveFilterKey]);

  const totalRows =
    mode === 'paged'
      ? Math.max(filteredRows.length, clampedPage * pageSize + pageRows.length)
      : Math.min(filteredRows.length, maxRetained);
  const status = statusFor(liveEnabled, !historySettled, live);

  return {
    mode,
    rows,
    filteredRows,
    pageRows,
    firstPageEvents: recent.data?.events,
    historyFilters,
    loading:
      (recent.isPending ||
        (recent.isFetching && activeHistoryEvents.length === 0)) &&
      filteredRows.length === 0,
    isFetching: recent.isFetching || loadingNext,
    error: nextError ?? recent.error ?? null,
    page: clampedPage,
    pageCount,
    pageSize,
    visibleCount: infinite ? renderLimit : pageRows.length,
    maxRetained,
    totalRows,
    hasMore:
      mode === 'paged' ? pagedHasMore : mode === 'infinite' && infiniteHasMore,
    loadingNext,
    previousPage,
    nextPage,
    loadMore,
    refresh,
    live,
    liveEnabled,
    ...status,
    liveFlashIds,
  };
}
