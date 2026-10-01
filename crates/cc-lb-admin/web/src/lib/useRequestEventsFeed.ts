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

export interface UseRequestEventsFeedOptions {
  readonly filters?: RequestEventsFeedFilters;
  readonly live?: boolean;
  /** Number of historical rows retained in the first request. Defaults to 500. */
  readonly initialHistoryLimit?: number;
  /** Number of rows exposed to the DOM on each page. Capped at 50. */
  readonly pageSize?: number;
}

export interface RequestEventsFeedState {
  readonly rows: readonly RequestEventWithPhase[];
  readonly filteredRows: readonly RequestEventWithPhase[];
  readonly pageRows: readonly RequestEventWithPhase[];
  readonly firstPageEvents: readonly RequestEvent[] | undefined;
  readonly historyFilters: Record<string, string>;
  readonly loading: boolean;
  readonly isFetching: boolean;
  readonly error: Error | null;
  readonly page: number;
  readonly pageCount: number;
  readonly pageSize: number;
  readonly totalRows: number;
  readonly hasMore: boolean;
  readonly loadingNext: boolean;
  readonly previousPage: () => void;
  readonly nextPage: () => Promise<void>;
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

function appendUniqueEvents(
  current: readonly RequestEvent[],
  additions: readonly RequestEvent[],
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
  return merged ?? current;
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
  live: LiveEventStreamState,
): {
  tailStatus: ConnectionStatus | 'failed';
  statusLabel: string;
  statusColor: 'neutral' | 'ok' | 'warn' | 'danger';
} {
  const tailStatus = liveEnabled
    ? live.permanentFailure
      ? 'failed'
      : live.status
    : 'idle';
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
  const liveEnabled = options.live !== false && filters.until_unix_secs == null;
  const initialHistoryLimit = Math.max(
    DEFAULT_PAGE_SIZE,
    Math.floor(options.initialHistoryLimit ?? DEFAULT_INITIAL_HISTORY_LIMIT),
  );
  const pageSize = Math.min(
    MAX_PAGE_SIZE,
    Math.max(1, Math.floor(options.pageSize ?? DEFAULT_PAGE_SIZE)),
  );
  const queryClient = useQueryClient();
  const initialPageParam = useMemo<RecentEventsPageParam>(
    () => ({ kind: 'initial', limit: initialHistoryLimit }),
    [initialHistoryLimit],
  );
  const recent = useRecentEventsPage(historyFilters, initialPageParam);
  const live = useLiveEventStream(liveFilters, { enabled: liveEnabled });

  const paginationIdentity = `${filterKey}|${initialHistoryLimit}|${pageSize}`;
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
  const [loadingNext, setLoadingNext] = useState(false);
  const [nextError, setNextError] = useState<Error | null>(null);
  const requestGenerationRef = useRef(0);
  const paginationRequestIdentityRef = useRef(paginationIdentity);
  useLayoutEffect(() => {
    if (paginationRequestIdentityRef.current === paginationIdentity) return;
    paginationRequestIdentityRef.current = paginationIdentity;
    requestGenerationRef.current += 1;
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
    setLoadingNext(false);
    setNextError(null);
  }

  const activeHistoryEvents = paginationIdentityChanged ? [] : historyEvents;
  const activeHistoryExhausted = paginationIdentityChanged
    ? false
    : historyExhausted;
  const activePage = paginationIdentityChanged ? 0 : page;

  useEffect(() => {
    if (recent.data === undefined || recent.isPlaceholderData) return;
    const events = recent.data.events;
    setHistoryEvents((current) => appendUniqueEvents(current, events));
    if (events.length < recent.data.limit) setHistoryExhausted(true);
  }, [recent.data, recent.isPlaceholderData]);

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
    () => mergeLogRows(rangedLiveEvents, activeHistoryEvents),
    [rangedLiveEvents, activeHistoryEvents, live.version],
  );
  const filteredRows = useMemo(
    () => filterLogRows(rows, rowFilters),
    [rows, rowFilters],
  );

  const activeSnapshots = paginationIdentityChanged ? [] : snapshots;
  const pageCount = activeSnapshots.length + 1;
  const clampedPage = Math.min(activePage, pageCount - 1);
  const pageRows = useMemo(() => {
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
    pageSize,
    rangedLiveEvents,
    rowFilters,
    rows,
  ]);
  const currentCursor = useMemo(
    () =>
      pageRows.length
        ? getRecentEventsCursor(pageRows[pageRows.length - 1])
        : undefined,
    [pageRows],
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
  const hasMore =
    clampedPage < pageCount - 1 ||
    remainingHistoryEvents.length > 0 ||
    (!activeHistoryExhausted &&
      currentCursorKey !== undefined &&
      !exhaustedCursorKeys.has(currentCursorKey));

  const nextPage = useCallback(async () => {
    if (
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
      setHistoryEvents((current) => appendUniqueEvents(current, next.events));
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
    pageSize,
    queryClient,
    remainingHistoryEvents,
  ]);

  const previousPage = useCallback(() => {
    requestGenerationRef.current += 1;
    setLoadingNext(false);
    setNextError(null);
    setPage((current) => Math.max(0, current - 1));
  }, []);

  const refresh = useCallback(() => {
    requestGenerationRef.current += 1;
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

  const totalRows = Math.max(
    filteredRows.length,
    clampedPage * pageSize + pageRows.length,
  );
  const status = statusFor(liveEnabled, live);

  return {
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
    totalRows,
    hasMore: hasMore && currentCursor !== undefined,
    loadingNext,
    previousPage,
    nextPage,
    refresh,
    live,
    liveEnabled,
    ...status,
    liveFlashIds,
  };
}
