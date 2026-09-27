import { Toggle as BaseToggle } from '@base-ui/react/toggle';
import { useQueryClient } from '@tanstack/react-query';
import { createFileRoute, useNavigate } from '@tanstack/react-router';
import {
  AlertTriangle,
  Download,
  RefreshCw,
  SlidersHorizontal,
  X,
} from 'lucide-react';
import type React from 'react';
import {
  type SetStateAction,
  useCallback,
  useEffect,
  useId,
  useImperativeHandle,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
} from 'react';
import * as z from 'zod';
import { LiveTailFailureBanner } from '../components/LiveTailFailureBanner';
import { LOGS_EMPTY_COPY } from '../components/onboarding/logsEmptyCopy';
import { LogsPagination } from '../components/ui/LogsPagination';
import {
  Button,
  Card,
  cx,
  Field,
  FullPage,
  INPUT_SM_CLASS,
  PageHeader,
  SegmentedControl,
  type SegmentedOption,
} from '../components/ui/primitives';
import { RequestEventsTable } from '../components/ui/RequestEventsTable';
import { Select, type SelectOption } from '../components/ui/Select';
import { SessionChip } from '../components/ui/SessionChip';
import { TimeRangeBounds } from '../components/ui/TimeRangeBounds';
import { TimeRangeStrip } from '../components/ui/TimeRangeStrip';
import {
  eventTime,
  type RecentEventsPayload,
  RequestEventKindSchema,
} from '../lib/api';
import {
  filterLiveEventsByUnixSeconds,
  filterLogRows,
  LOG_STATUS_FILTER_VALUES,
  mergeLogRows,
  newestLiveEventIds,
  REQUEST_EVENT_KIND_LABELS,
  REQUEST_EVENT_KINDS,
  selectLogRowsForPage,
} from '../lib/logRows';
import { LOGS_PAGE_SIZE } from '../lib/logsPagination';
import {
  getRecentEventsCursor,
  type RecentEventsPageParam,
  recentEventsPageQueryOptions,
  useEventsHistogram,
  usePrincipalNameMap,
  useRecentEventsPage,
  useUpstreamNameMap,
  useUpstreams,
} from '../lib/queries';
import {
  BUCKET_LADDER_MS,
  bucketCountFor,
  chooseBucketMs,
  MAX_HISTOGRAM_BUCKETS,
} from '../lib/timeBuckets';
import {
  TIME_PRESET_OPTIONS_WITH_ALL,
  TIME_PRESET_SECONDS,
  type TimePreset,
} from '../lib/timePresets';
import { MAX_FORMATTABLE_UNIX_SECONDS } from '../lib/timezone';
import { useLiveEventStream } from '../lib/useLiveEventStream';

const unixSecondsSearchParam = z.preprocess((value) => {
  if (value == null || value === '') return undefined;
  const number = Number(value);
  return Number.isSafeInteger(number) &&
    number >= 0 &&
    number <= MAX_FORMATTABLE_UNIX_SECONDS
    ? number
    : undefined;
}, z.number().optional());

export const logsSearchSchema = z
  .object({
    principal_id: z.string().optional(),
    upstream_id: z.string().optional(),
    session: z.string().optional(),
    model: z.string().optional(),
    status: z.enum(LOG_STATUS_FILTER_VALUES).optional(),
    // Old bookmarks may still carry `source_kind`; it is stripped as an
    // unknown key. Absent or invalid `event_kind` means unfiltered — the
    // list shows every endpoint category by default.
    event_kind: RequestEventKindSchema.optional().catch(undefined),
    // Accepted only so bookmarked preset URLs do not fail validateSearch and
    // fall through to the route error boundary. Normalized away on load.
    time_range: z.string().optional(),
    since_unix_secs: unixSecondsSearchParam,
    until_unix_secs: unixSecondsSearchParam,
  })
  .transform((filters) => {
    if (
      filters.since_unix_secs != null &&
      filters.until_unix_secs != null &&
      filters.since_unix_secs > filters.until_unix_secs
    ) {
      return {
        ...filters,
        since_unix_secs: undefined,
        until_unix_secs: undefined,
      };
    }
    return filters;
  });

/** Legacy preset widths, kept only to rewrite old bookmarks into absolute epochs. */
const LEGACY_PRESET_SECONDS: Record<string, number> = {
  '1h': 3600,
  '6h': 6 * 3600,
  '24h': 24 * 3600,
  '7d': 7 * 24 * 3600,
};

export const Route = createFileRoute('/logs')({
  validateSearch: logsSearchSchema,
  component: LogsPage,
});

export function buildHistoricalFilters(
  filters: z.infer<typeof logsSearchSchema>,
) {
  const { since_unix_secs, until_unix_secs } = filters;
  const base = buildLiveFilters(filters);
  // `errors` is expressible only as `status_class`, which the list and
  // histogram endpoints apply before pagination; the live stream must not
  // receive it (see `buildLiveFilters`), so it is added here instead.
  if (filters.status === 'errors') {
    base.status_class = 'errors';
  }

  if (since_unix_secs != null) {
    base.since_unix_secs = since_unix_secs.toString();
  }
  // An absent `until` means the right edge is pinned to now: the list API
  // already defaults to u64::MAX, and materializing `now` here would shift the
  // upper bound on every refetch.
  if (until_unix_secs != null) {
    base.until_unix_secs = until_unix_secs.toString();
  }

  return base;
}

export function buildLiveFilters(filters: z.infer<typeof logsSearchSchema>) {
  const base: Record<string, string> = {};
  if (filters.principal_id) base.principal_id = filters.principal_id;
  if (filters.upstream_id) base.upstream_id = filters.upstream_id;
  if (filters.session) base.thread_id = filters.session;
  if (filters.model) base.model = filters.model;
  // `errors` stays client-side (`isErrorLogRow` in filterLogRows): a partial
  // whose upstream status is corrected downward — e.g. a 401 followed by a
  // retried 200 on the same event — must keep streaming so the row updates.
  if (filters.status && filters.status !== 'errors') {
    base.status_class = filters.status;
  }
  if (filters.event_kind) base.event_kind = filters.event_kind;
  return base;
}

export function getLogsRouteState({
  userRequestedTailing,
  until_unix_secs,
}: {
  userRequestedTailing: boolean;
  until_unix_secs?: number;
}) {
  return {
    effectiveTailing: userRequestedTailing && until_unix_secs == null,
  };
}

const LOGS_TABLE_COLUMNS = { cost: true, tokens: true } as const;
const INITIAL_LOGS_PAGE_PARAM: RecentEventsPageParam = {
  kind: 'initial',
  limit: LOGS_PAGE_SIZE,
};
const LOGS_RESERVED_ROW_COUNT = 10;
// Every committed model value re-runs the historical query and reconnects the
// live SSE stream, so typing has to settle before the URL changes.
const MODEL_FILTER_DEBOUNCE_MS = 300;

/** Relative presets, shared with every range control. Each computes `since` at selection time. */
const LOGS_PRESET_SECONDS = TIME_PRESET_SECONDS;
type LogsPreset = 'all' | TimePreset;
const LOGS_PRESET_OPTIONS: readonly SegmentedOption<LogsPreset>[] =
  TIME_PRESET_OPTIONS_WITH_ALL;

/**
 * The preset an applied range still matches, or null for a custom range. A
 * preset's `since` is fixed when chosen, so it keeps matching until the window
 * has grown by more than a minute or 2% of its width, whichever is larger.
 */
export function presetFor(
  since: number | undefined,
  until: number | undefined,
  nowSecs: number,
): LogsPreset | null {
  if (until != null) return null;
  if (since == null) return 'all';
  const width = nowSecs - since;
  for (const [preset, secs] of Object.entries(LOGS_PRESET_SECONDS)) {
    if (Math.abs(width - secs) <= Math.max(60, secs * 0.02)) {
      return preset as LogsPreset;
    }
  }
  return null;
}

interface LogsTimeStripHandle {
  focusAround: (tsMs: number, radiusSecs: number) => void;
  applyPreset: (widthSecs: number | null) => void;
}

/**
 * Back to the first row of a new page. From `md` the rows scroll inside the
 * card; below it the page scrolls, so bring the rows' top back on screen when
 * the reader has scrolled past it (e.g. from the pager at the bottom).
 */
function resetRowsScroll(el: HTMLElement | null) {
  if (!el) return;
  el.scrollTop = 0;
  if (el.getBoundingClientRect().top < 0) el.scrollIntoView({ block: 'start' });
}

function LogsPage() {
  const queryClient = useQueryClient();
  const filters = Route.useSearch();
  const navigate = useNavigate({ from: Route.fullPath });
  const upstreams = useUpstreams();

  const { session: sessionFilter } = filters;
  // Absent means unfiltered: the list shows every endpoint category until the
  // operator picks one. Tests that stub useSearch get the same behavior.
  const eventKindFilter = filters.event_kind;
  const liveFilters = buildLiveFilters(filters);
  const historicalFilters = buildHistoricalFilters(filters);
  const nextPaginationIdentity = JSON.stringify([
    filters.principal_id,
    filters.upstream_id,
    filters.session,
    filters.model,
    filters.status,
    eventKindFilter,
    filters.since_unix_secs,
    filters.until_unix_secs,
  ]);

  const [userRequestedTailing, setUserRequestedTailing] = useState(true);
  const [page, setPage] = useState(0);
  const [modelDraft, setModelDraft] = useState(filters.model ?? '');
  const lastCommittedModelRef = useRef(filters.model ?? '');
  const [cursorStack, setCursorStack] = useState<RecentEventsPageParam[]>([
    INITIAL_LOGS_PAGE_PARAM,
  ]);
  const [loadedPages, setLoadedPages] = useState<
    Array<RecentEventsPayload | undefined>
  >([]);
  const [loadingNext, setLoadingNext] = useState(false);
  const [exhaustedCursorKeys, setExhaustedCursorKeys] = useState<Set<string>>(
    () => new Set(),
  );
  const scrollContainerRef = useRef<HTMLDivElement>(null);
  const paginationRequestGenerationRef = useRef(0);
  const paginationRequestIdentityRef = useRef(nextPaginationIdentity);
  const [paginationIdentity, setPaginationIdentity] = useState(
    nextPaginationIdentity,
  );
  // This checkpoint and the pagination reset are render-phase state updates,
  // so React retries them together when a concurrent render is discarded.
  const scrollResetPaginationIdentityRef = useRef(nextPaginationIdentity);
  const paginationIdentityChanged =
    paginationIdentity !== nextPaginationIdentity;
  if (paginationIdentityChanged) {
    setPaginationIdentity(nextPaginationIdentity);
    setPage(0);
    setCursorStack([INITIAL_LOGS_PAGE_PARAM]);
    setLoadedPages([]);
    setLoadingNext(false);
    setExhaustedCursorKeys(new Set());
  }

  // A changed filter identity must never subscribe with the previous page's
  // cursor while React applies the pagination reset above.
  const activePage = paginationIdentityChanged ? 0 : page;
  const activeCursorStack = paginationIdentityChanged
    ? [INITIAL_LOGS_PAGE_PARAM]
    : cursorStack;
  const activeLoadedPages = paginationIdentityChanged ? [] : loadedPages;
  const activeExhaustedCursorKeys = paginationIdentityChanged
    ? new Set<string>()
    : exhaustedCursorKeys;
  const activeLoadingNext = paginationIdentityChanged ? false : loadingNext;
  const pageCount = Math.max(1, activeCursorStack.length);
  const clampedPage = Math.min(activePage, pageCount - 1);
  const currentPageParam =
    activeCursorStack[clampedPage] ?? INITIAL_LOGS_PAGE_PARAM;
  const recent = useRecentEventsPage(historicalFilters, currentPageParam);
  const principalNameMap = usePrincipalNameMap();
  const upstreamNameMap = useUpstreamNameMap();
  const { effectiveTailing } = getLogsRouteState({
    userRequestedTailing,
    until_unix_secs: filters.until_unix_secs,
  });

  // Legacy preset bookmarks (`?time_range=24h`) are rewritten to an absolute
  // lower bound with an open right edge, preserving both the window they used
  // to mean and live tailing.
  useEffect(() => {
    if (filters.time_range == null) return;
    const width = LEGACY_PRESET_SECONDS[filters.time_range];
    navigate({
      replace: true,
      search: (prev) => ({
        ...prev,
        time_range: undefined,
        since_unix_secs:
          width == null
            ? prev.since_unix_secs
            : Math.floor(Date.now() / 1000) - width,
        until_unix_secs: width == null ? prev.until_unix_secs : undefined,
      }),
    });
  }, [filters.time_range, navigate]);

  const routeModel = filters.model ?? '';
  // Only external navigations (Clear, back/forward, a shared URL) may overwrite
  // the draft; echoing back our own debounced commit would drop keystrokes typed
  // while the router was settling.
  useEffect(() => {
    if (routeModel !== lastCommittedModelRef.current) {
      lastCommittedModelRef.current = routeModel;
      setModelDraft(routeModel);
    }
  }, [routeModel]);
  useEffect(() => {
    const next = modelDraft.trim();
    if (next === routeModel) return;
    const timer = setTimeout(() => {
      lastCommittedModelRef.current = next;
      navigate({
        search: (prev) => ({ ...prev, model: next || undefined }),
      });
    }, MODEL_FILTER_DEBOUNCE_MS);
    return () => clearTimeout(timer);
  }, [modelDraft, routeModel, navigate]);

  useEffect(() => {
    if (scrollResetPaginationIdentityRef.current === nextPaginationIdentity) {
      return;
    }
    scrollResetPaginationIdentityRef.current = nextPaginationIdentity;
    resetRowsScroll(scrollContainerRef.current);
  }, [nextPaginationIdentity]);

  // Invalidate page requests only after the identity reset commits. A render
  // mutation would survive a discarded transition while its state updates do not.
  useLayoutEffect(() => {
    if (paginationRequestIdentityRef.current === paginationIdentity) return;
    paginationRequestIdentityRef.current = paginationIdentity;
    paginationRequestGenerationRef.current += 1;
  }, [paginationIdentity]);

  useEffect(
    () => () => {
      paginationRequestGenerationRef.current += 1;
    },
    [],
  );

  useEffect(() => {
    if (recent.data === undefined || recent.isPlaceholderData) return;
    setLoadedPages((current) => {
      if (current[clampedPage] === recent.data) return current;
      const updated = current.slice(
        0,
        Math.max(current.length, clampedPage + 1),
      );
      updated[clampedPage] = recent.data;
      return updated;
    });
  }, [clampedPage, recent.data, recent.isPlaceholderData]);

  const live = useLiveEventStream(liveFilters, { enabled: effectiveTailing });
  const initialRowsLoading =
    (recent.isPending || recent.isPlaceholderData) && live.eventsMap.size === 0;
  const tailStatus = effectiveTailing
    ? live.permanentFailure
      ? 'failed'
      : live.status
    : 'idle';

  const statusLabel = {
    idle: 'Off',
    connecting: 'Connecting…',
    live: 'Live',
    stale: 'Stale',
    reconnecting: 'Reconnecting…',
    hidden: 'Paused',
    error: 'Offline',
    failed: 'Failed',
  }[tailStatus];

  const statusColor = {
    idle: 'neutral',
    connecting: 'neutral',
    live: 'ok',
    stale: 'warn',
    reconnecting: 'warn',
    hidden: 'neutral',
    error: 'danger',
    failed: 'danger',
  }[tailStatus];

  // biome-ignore lint/correctness/useExhaustiveDependencies: live.eventsMap is a stable Map ref mutated in place by useLiveEventStream; live.version is bumped on every upsert so it is the real re-run trigger.
  const rangedLiveEvents = useMemo(
    () =>
      filterLiveEventsByUnixSeconds(live.eventsMap, {
        since:
          historicalFilters.since_unix_secs === undefined
            ? undefined
            : Number(historicalFilters.since_unix_secs),
        until:
          historicalFilters.until_unix_secs === undefined
            ? undefined
            : Number(historicalFilters.until_unix_secs),
      }),
    [
      live.eventsMap,
      live.version,
      historicalFilters.since_unix_secs,
      historicalFilters.until_unix_secs,
    ],
  );

  const historicalPages = useMemo(() => {
    const pages = loadedPages.slice(0, pageCount);
    if (recent.data !== undefined && !recent.isPlaceholderData) {
      pages[clampedPage] = recent.data;
    }
    return pages;
  }, [
    clampedPage,
    loadedPages,
    pageCount,
    recent.data,
    recent.isPlaceholderData,
  ]);
  // biome-ignore lint/correctness/useExhaustiveDependencies: rangedLiveEvents can be the stable live.eventsMap ref when the range is unbounded; live.version is the mutation counter that invalidates this derived row list.
  const rows = useMemo(
    () =>
      mergeLogRows(
        rangedLiveEvents,
        historicalPages.flatMap(
          (historicalPage) => historicalPage?.events ?? [],
        ),
      ),
    [rangedLiveEvents, historicalPages, live.version],
  );

  const firstPageEvents = historicalPages[0]?.events;
  // The strip owns its pan/zoom domain and the 1 Hz follow-now timer, so the
  // tick re-renders only the strip, not the filters and table.
  const stripRef = useRef<LogsTimeStripHandle>(null);
  const focusAround = useCallback((tsMs: number, radiusSecs: number) => {
    stripRef.current?.focusAround(tsMs, radiusSecs);
  }, []);
  const activePreset = presetFor(
    filters.since_unix_secs,
    filters.until_unix_secs,
    Math.floor(Date.now() / 1000),
  );
  const [filtersOpen, setFiltersOpen] = useState(false);
  const filterPanelId = useId();

  const sessionOptions = useMemo(() => {
    const seen = new Set<string>();
    const out: string[] = [];
    for (const r of rows) {
      const id = r.thread_id;
      if (id && !seen.has(id)) {
        seen.add(id);
        out.push(id);
      }
    }
    return out;
  }, [rows]);

  const visibleRows = useMemo(
    () => filterLogRows(rows, filters),
    [rows, filters],
  );
  const currentHistoricalEvents = historicalPages[clampedPage]?.events ?? [];
  const pageRows = useMemo(
    () =>
      filterLogRows(
        selectLogRowsForPage(
          rows,
          clampedPage === 0 ? rangedLiveEvents : undefined,
          currentHistoricalEvents,
        ),
        filters,
      ).slice(0, LOGS_PAGE_SIZE),
    [clampedPage, currentHistoricalEvents, filters, rangedLiveEvents, rows],
  );
  const currentCursor = pageRows.length
    ? getRecentEventsCursor(pageRows[pageRows.length - 1])
    : undefined;
  const currentCursorKey =
    currentCursor === undefined
      ? undefined
      : `${currentCursor.ts_ms}:${currentCursor.event_id}`;
  const lastHistoricalEvents = historicalPages[pageCount - 1]?.events ?? [];
  const lastHistoricalCursor = lastHistoricalEvents.length
    ? getRecentEventsCursor(
        lastHistoricalEvents[lastHistoricalEvents.length - 1],
      )
    : undefined;
  const lastHistoricalCursorKey =
    lastHistoricalCursor === undefined
      ? undefined
      : `${lastHistoricalCursor.ts_ms}:${lastHistoricalCursor.event_id}`;
  const hasMore =
    lastHistoricalEvents.length === LOGS_PAGE_SIZE &&
    lastHistoricalCursorKey !== undefined &&
    !activeExhaustedCursorKeys.has(lastHistoricalCursorKey);

  const commitPage = (nextPage: SetStateAction<number>) => {
    resetRowsScroll(scrollContainerRef.current);
    setPage(nextPage);
  };

  const nextPage = async () => {
    if (currentCursor === undefined || currentCursorKey === undefined) return;
    const nextPageIndex = clampedPage + 1;
    const nextPageParam: RecentEventsPageParam = {
      kind: 'cursor',
      limit: LOGS_PAGE_SIZE,
      ...currentCursor,
    };
    const cachedPageParam = activeCursorStack[nextPageIndex];
    if (
      activeLoadedPages[nextPageIndex] !== undefined &&
      cachedPageParam?.kind === 'cursor' &&
      cachedPageParam.ts_ms === nextPageParam.ts_ms &&
      cachedPageParam.event_id === nextPageParam.event_id
    ) {
      paginationRequestGenerationRef.current += 1;
      commitPage(nextPageIndex);
      return;
    }

    const requestGeneration = paginationRequestGenerationRef.current + 1;
    paginationRequestGenerationRef.current = requestGeneration;
    setLoadingNext(true);
    try {
      const next = await queryClient.fetchQuery(
        recentEventsPageQueryOptions(historicalFilters, nextPageParam),
      );
      if (requestGeneration !== paginationRequestGenerationRef.current) return;
      if (next.events.length === 0) {
        setExhaustedCursorKeys((current) => {
          const updated = new Set(current);
          updated.add(currentCursorKey);
          return updated;
        });
        return;
      }
      setCursorStack((current) => [
        ...current.slice(0, nextPageIndex),
        nextPageParam,
      ]);
      setLoadedPages((current) => [...current.slice(0, nextPageIndex), next]);
      commitPage(nextPageIndex);
    } catch {
      // QueryCache owns the user-facing error toast.
    } finally {
      if (requestGeneration === paginationRequestGenerationRef.current) {
        setLoadingNext(false);
      }
    }
  };

  const refreshLogs = () => {
    paginationRequestGenerationRef.current += 1;
    setLoadingNext(false);
    void recent.refetch();
  };

  const previousPage = () => {
    paginationRequestGenerationRef.current += 1;
    setLoadingNext(false);
    commitPage((current) => Math.max(0, current - 1));
  };

  const principalSelectOptions = useMemo<SelectOption[]>(
    () =>
      Array.from(principalNameMap.entries()).map(([id, name]) => ({
        value: id,
        label: <span className="truncate">{name}</span>,
      })),
    [principalNameMap],
  );

  const upstreamSelectOptions = useMemo<SelectOption[]>(
    () =>
      (upstreams.data?.upstreams ?? []).map((u) => ({
        value: u.id,
        label: <span className="truncate">{u.name}</span>,
      })),
    [upstreams.data],
  );

  const sessionSelectOptions = useMemo<SelectOption[]>(() => {
    const list: SelectOption[] = [];
    if (sessionFilter && !sessionOptions.includes(sessionFilter)) {
      list.push({
        value: sessionFilter,
        label: <SessionChip sessionId={sessionFilter} />,
        hint: 'not in view',
      });
    }
    for (const id of sessionOptions) {
      list.push({
        value: id,
        label: <SessionChip sessionId={id} />,
      });
    }
    return list;
  }, [sessionOptions, sessionFilter]);

  const statusSelectOptions = useMemo<SelectOption[]>(
    () => [
      { value: '2xx', label: '2xx' },
      { value: '3xx', label: '3xx' },
      { value: '4xx', label: '4xx' },
      { value: '5xx', label: '5xx' },
      // Client-side only: abnormal outcomes — HTTP >= 400 or a recorded
      // error_code on a delivered 2xx (mid-stream error, upstream refusal, …).
      { value: 'errors', label: 'Errors' },
    ],
    [],
  );

  const eventKindSelectOptions = useMemo<SelectOption[]>(
    () =>
      // The empty/"all" item is the unfiltered default, so every kind —
      // including `messages` — stays selectable.
      REQUEST_EVENT_KINDS.map((kind) => ({
        value: kind,
        label: REQUEST_EVENT_KIND_LABELS[kind],
      })),
    [],
  );

  // biome-ignore lint/correctness/useExhaustiveDependencies: live.version is the mutation counter for the stable eventsMap ref.
  const recentLiveIds = useMemo(
    () => newestLiveEventIds(live.eventsMap),
    [live.eventsMap, live.version],
  );

  const setFilter = (key: keyof typeof filters, value: string) => {
    navigate({ search: { ...filters, [key]: value || undefined } });
  };

  const downloadJson = () => {
    const blob = new Blob([JSON.stringify(visibleRows, null, 2)], {
      type: 'application/json',
    });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `cc-lb-events-${new Date().toISOString().slice(0, 16)}.json`;
    a.click();
    URL.revokeObjectURL(url);
  };

  const activeFilterCount = [
    filters.principal_id,
    filters.upstream_id,
    filters.session,
    filters.model,
    filters.status,
    eventKindFilter,
    filters.since_unix_secs ?? filters.until_unix_secs,
  ].filter((value) => value != null && value !== '').length;
  const emptyCopy =
    LOGS_EMPTY_COPY[activeFilterCount > 0 ? 'filtered' : 'unfiltered'];

  // Chips name every applied filter the toolbar does not already show: a
  // preset range is visible in the segmented control, a custom one is not.
  const hasCustomRange =
    activePreset == null &&
    (filters.since_unix_secs != null || filters.until_unix_secs != null);
  const filterChips: FilterChipSpec[] = [
    ...(filters.principal_id
      ? [
          {
            key: 'principal_id' as const,
            label: 'Principal',
            value:
              principalNameMap.get(filters.principal_id) ??
              filters.principal_id,
          },
        ]
      : []),
    ...(filters.upstream_id
      ? [
          {
            key: 'upstream_id' as const,
            label: 'Upstream',
            value:
              upstreams.data?.upstreams.find(
                (u) => u.id === filters.upstream_id,
              )?.name ?? filters.upstream_id,
          },
        ]
      : []),
    ...(sessionFilter
      ? [
          {
            key: 'session' as const,
            label: 'Session',
            value: <SessionChip sessionId={sessionFilter} />,
          },
        ]
      : []),
    ...(filters.model
      ? [
          {
            key: 'model' as const,
            label: 'Model',
            value: (
              <span className="font-mono text-data">{filters.model}*</span>
            ),
          },
        ]
      : []),
    ...(filters.status
      ? [
          {
            key: 'status' as const,
            label: 'Status',
            value: filters.status === 'errors' ? 'Errors' : filters.status,
          },
        ]
      : []),
    ...(eventKindFilter
      ? [
          {
            key: 'event_kind' as const,
            label: 'Kind',
            value: REQUEST_EVENT_KIND_LABELS[eventKindFilter],
          },
        ]
      : []),
  ];
  const panelFilterCount = filterChips.length + (hasCustomRange ? 1 : 0);

  return (
    <FullPage className="gap-3">
      <LiveTailFailureBanner
        permanentFailure={live.permanentFailure}
        permanentFailureSince={live.permanentFailureSince}
        reconnectAttempts={live.reconnectAttempts}
        onRetry={live.forceReconnect}
      />
      <PageHeader
        title="Logs"
        description={
          <span className="flex flex-wrap items-center gap-x-2">
            <span>
              {visibleRows.length} requests —{' '}
              {effectiveTailing ? 'live tailing' : 'paged'}
            </span>
            {effectiveTailing &&
            tailStatus !== 'live' &&
            tailStatus !== 'idle' ? (
              <span
                className={cx(
                  'inline-flex items-center gap-1.5 text-label',
                  tailStatus === 'failed' || tailStatus === 'error'
                    ? 'text-danger-text'
                    : tailStatus === 'stale' || tailStatus === 'reconnecting'
                      ? 'text-warn-text'
                      : 'text-text-muted',
                )}
              >
                {tailStatus === 'failed' ? (
                  <AlertTriangle aria-hidden className="size-3.5" />
                ) : (
                  <span className={cx('status-dot', statusColor)} />
                )}
                {statusLabel}
              </span>
            ) : null}
          </span>
        }
        actions={
          <>
            <BaseToggle
              aria-label="Live tail logs"
              className="inline-flex items-center justify-center gap-1.5 h-8 px-3 rounded-sm border border-subtle bg-panel-strong text-[0.8125rem] font-medium text-text whitespace-nowrap transition-colors select-none hover:bg-hover-bg hover:border-subtle-strong data-[pressed]:bg-overlay-4 disabled:cursor-not-allowed disabled:opacity-40 focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-2"
              onPressedChange={setUserRequestedTailing}
              pressed={effectiveTailing}
              disabled={filters.until_unix_secs != null}
              title={
                filters.until_unix_secs != null
                  ? 'Live tail is off while the range has a fixed end. Extend the range to now to resume.'
                  : undefined
              }
            >
              <span
                aria-hidden
                className={cx(
                  'status-dot',
                  effectiveTailing === false
                    ? 'neutral'
                    : tailStatus === 'live'
                      ? 'live'
                      : statusColor,
                )}
              />
              Live tail
            </BaseToggle>
            <Button iconLeft={<RefreshCw />} onClick={refreshLogs}>
              Refresh
            </Button>
            <Button iconLeft={<Download />} onClick={downloadJson}>
              Export
            </Button>
          </>
        }
      />
      <Card className="flex-1 flex flex-col min-h-0">
        <div className="px-4 py-3 border-b border-row flex flex-col gap-3 shrink-0">
          <div className="flex flex-wrap items-center gap-2">
            <SegmentedControl<LogsPreset | 'custom'>
              ariaLabel="Time range preset"
              value={activePreset ?? 'custom'}
              options={LOGS_PRESET_OPTIONS}
              onChange={(preset) =>
                stripRef.current?.applyPreset(
                  preset === 'all' || preset === 'custom'
                    ? null
                    : LOGS_PRESET_SECONDS[preset],
                )
              }
            />
            <Button
              iconLeft={<SlidersHorizontal />}
              aria-expanded={filtersOpen}
              aria-controls={filterPanelId}
              onClick={() => setFiltersOpen((open) => !open)}
              className={filtersOpen ? 'bg-overlay-4' : undefined}
            >
              Filters
              {panelFilterCount > 0 ? (
                <span className="tabular-nums text-text-muted">
                  {panelFilterCount}
                </span>
              ) : null}
            </Button>
            {filterChips.map((chip) => (
              <FilterChip
                key={chip.key}
                label={chip.label}
                value={chip.value}
                onRemove={() => setFilter(chip.key, '')}
              />
            ))}
            {hasCustomRange ? (
              <FilterChip
                label="Range"
                value="Custom"
                onRemove={() => stripRef.current?.applyPreset(null)}
              />
            ) : null}
            {activeFilterCount > 0 ? (
              <Button
                variant="ghost"
                iconLeft={<X />}
                onClick={() => navigate({ search: {} })}
              >
                Clear
              </Button>
            ) : null}
          </div>
          <div
            id={filterPanelId}
            hidden={!filtersOpen}
            className="well grid grid-cols-2 gap-3 p-3 md:flex md:flex-wrap md:items-end"
          >
            <Field label="Principal">
              <Select
                size="sm"
                value={filters.principal_id ?? ''}
                options={principalSelectOptions}
                onChange={(v) => setFilter('principal_id', v)}
                allLabel="All principals"
                className={FILTER_CONTROL_WIDTH}
              />
            </Field>
            <Field label="Upstream">
              <Select
                size="sm"
                value={filters.upstream_id ?? ''}
                options={upstreamSelectOptions}
                onChange={(v) => setFilter('upstream_id', v)}
                allLabel="All upstreams"
                className={FILTER_CONTROL_WIDTH}
              />
            </Field>
            <Field label="Session">
              <Select
                size="sm"
                value={sessionFilter ?? ''}
                options={sessionSelectOptions}
                onChange={(v) => setFilter('session', v)}
                allLabel="All sessions"
                className={FILTER_CONTROL_WIDTH}
              />
            </Field>
            <Field label="Model">
              <input
                className={cx(
                  INPUT_SM_CLASS,
                  FILTER_CONTROL_WIDTH,
                  'font-mono placeholder:font-sans',
                )}
                value={modelDraft}
                onChange={(e) => setModelDraft(e.target.value)}
                placeholder="Model prefix"
              />
            </Field>
            <Field label="Status">
              <Select
                size="sm"
                value={filters.status ?? ''}
                options={statusSelectOptions}
                onChange={(v) => setFilter('status', v)}
                allLabel="All statuses"
                className={FILTER_CONTROL_WIDTH}
              />
            </Field>
            <Field label="Kind">
              <Select
                size="sm"
                value={eventKindFilter ?? ''}
                options={eventKindSelectOptions}
                onChange={(v) => setFilter('event_kind', v)}
                allLabel="All kinds"
                className={FILTER_CONTROL_WIDTH}
              />
            </Field>
            <div
              role="group"
              aria-label="Custom range"
              className="col-span-full flex flex-wrap items-end gap-3 md:basis-full"
            >
              <TimeRangeBounds
                since={filters.since_unix_secs}
                until={filters.until_unix_secs}
                onCommit={({ since, until }) => {
                  navigate({
                    search: (prev) => ({
                      ...prev,
                      since_unix_secs: since,
                      until_unix_secs: until,
                    }),
                  });
                }}
              />
            </div>
          </div>
        </div>
        <div className="px-4 pt-3 pb-1 border-b border-row shrink-0">
          <LogsTimeStrip
            handleRef={stripRef}
            sinceUnixSecs={filters.since_unix_secs}
            untilUnixSecs={filters.until_unix_secs}
            historicalFilters={historicalFilters}
            firstPageEvents={firstPageEvents}
          />
        </div>

        <div
          ref={scrollContainerRef}
          className="flex-1 overflow-auto min-h-0 scroll-mt-16"
        >
          <RequestEventsTable
            events={pageRows}
            onAnchorRange={focusAround}
            principalNameMap={principalNameMap}
            upstreamNameMap={upstreamNameMap}
            loading={initialRowsLoading}
            reservedRowCount={LOGS_RESERVED_ROW_COUNT}
            liveFlashIds={
              effectiveTailing && clampedPage === 0 ? recentLiveIds : undefined
            }
            columns={LOGS_TABLE_COLUMNS}
            minWidthClass="min-w-[1080px]"
            emptyTitle={emptyCopy.title}
            emptyDescription={emptyCopy.description}
            emptyAction={emptyCopy.action}
            emptyHeadingLevel={2}
          />
        </div>
        <LogsPagination
          page={clampedPage}
          pageCount={pageCount}
          totalRows={Math.max(
            visibleRows.length,
            clampedPage * LOGS_PAGE_SIZE + pageRows.length,
          )}
          pageSize={LOGS_PAGE_SIZE}
          hasMore={hasMore}
          loading={initialRowsLoading}
          loadingNext={activeLoadingNext}
          onPrev={previousPage}
          onNext={() => void nextPage()}
        />
      </Card>
    </FullPage>
  );
}

const FILTER_CONTROL_WIDTH = 'w-full md:w-40';

interface FilterChipSpec {
  key:
    | 'principal_id'
    | 'upstream_id'
    | 'session'
    | 'model'
    | 'status'
    | 'event_kind';
  label: string;
  value: React.ReactNode;
}

/** An applied filter, readable at a glance and removable in one click. */
function FilterChip({
  label,
  value,
  onRemove,
}: {
  label: string;
  value: React.ReactNode;
  onRemove: () => void;
}) {
  return (
    <span className="inline-flex h-7 max-w-full items-center gap-1.5 rounded-sm bg-overlay-3 pl-2 pr-0.5 text-caption">
      <span className="text-text-muted">{label}</span>
      <span className="min-w-0 truncate text-text">{value}</span>
      <button
        type="button"
        aria-label={`Remove ${label.toLowerCase()} filter`}
        onClick={onRemove}
        className="inline-flex size-6 shrink-0 items-center justify-center rounded-sm text-text-faint hover:bg-overlay-5 hover:text-text focus-visible:outline-2 focus-visible:outline-accent"
      >
        <X aria-hidden className="size-3.5" />
      </button>
    </span>
  );
}

function LogsTimeStrip({
  handleRef,
  sinceUnixSecs,
  untilUnixSecs,
  historicalFilters,
  firstPageEvents,
}: {
  handleRef: React.Ref<LogsTimeStripHandle>;
  sinceUnixSecs: number | undefined;
  untilUnixSecs: number | undefined;
  historicalFilters: ReturnType<typeof buildHistoricalFilters>;
  firstPageEvents: RecentEventsPayload['events'] | undefined;
}) {
  const navigate = useNavigate({ from: Route.fullPath });
  // The initial strip domain comes from the first page we already fetched, so
  // it costs no extra query and adapts to traffic density instead of assuming
  // a fixed lookback. Global min/max is deliberately never queried.
  const [view, setView] = useState<{ a: number; b: number } | null>(null);
  useEffect(() => {
    if (view != null || firstPageEvents == null) return;
    const now = Date.now();
    // A URL that pins the right edge is asking about a past window, so the
    // domain brackets that window instead of stretching to now.
    const fixedEnd = untilUnixSecs == null ? null : untilUnixSecs * 1000;
    if (firstPageEvents.length === 0) {
      const end = fixedEnd ?? now;
      setView({ a: end - 3_600_000, b: end });
      setFollowRight(fixedEnd == null);
      return;
    }
    let oldest = Number.POSITIVE_INFINITY;
    let newest = 0;
    for (const event of firstPageEvents) {
      const at = eventTime(event);
      if (at == null) continue;
      const ts = at.getTime();
      if (ts < oldest) oldest = ts;
      if (ts > newest) newest = ts;
    }
    if (newest === 0) {
      const end = fixedEnd ?? now;
      setView({ a: end - 3_600_000, b: end });
      setFollowRight(fixedEnd == null);
      return;
    }
    if (fixedEnd != null) {
      // The URL bounds are authoritative: on a dense range the first page only
      // reaches back 200 events, which would start the domain after the
      // selection begins and push the highlight off the left edge.
      const start = sinceUnixSecs == null ? oldest : sinceUnixSecs * 1000;
      const pad = Math.max(60_000, (fixedEnd - start) * 0.15);
      setView({ a: start - pad, b: Math.min(now, fixedEnd + pad) });
      setFollowRight(false);
      return;
    }
    // Otherwise the right edge is now, not the newest row: an idle proxy would
    // open on a domain that ends in the past and never shows arriving traffic.
    setView({ a: Math.min(oldest, now - 60_000), b: now });
    setFollowRight(true);
  }, [firstPageEvents, view, sinceUnixSecs, untilUnixSecs]);

  const histogramBucketMs = chooseBucketMs(
    view == null ? 3_600_000 : view.b - view.a,
  );

  // Whether the domain follows now. Held explicitly rather than inferred from
  // the current gap: a throttled background tab can leave an arbitrarily large
  // gap, and inferring "the user panned away" from that would freeze the strip
  // permanently with no way back.
  const [followRight, setFollowRight] = useState(true);
  useEffect(() => {
    if (!followRight) return;
    const timer = setInterval(() => {
      setView((current) => {
        if (current == null) return current;
        const nowMs = Date.now();
        const behind = nowMs - current.b;
        if (behind <= 0) return current;
        return { a: current.a + behind, b: nowMs };
      });
    }, 1_000);
    return () => clearInterval(timer);
  }, [followRight]);
  // Snapping the queried domain to bucket boundaries keeps bucket identities
  // (and the query key) stable across polls, so live tail grows the trailing
  // bar instead of shifting all 240 buckets sideways.
  const histogramRange = useMemo(() => {
    if (view == null) return null;
    const sinceMs = Math.floor(view.a / histogramBucketMs) * histogramBucketMs;
    const untilMs = Math.ceil(view.b / histogramBucketMs) * histogramBucketMs;
    let sinceSecs = Math.floor(sinceMs / 1000);
    const untilSecs = Math.floor(untilMs / 1000);
    // Snapping widens the window, so a domain sized right at the cap can spill
    // one bucket past it and the server would reject the range. Give up the
    // oldest buckets rather than the request.
    const overflow =
      bucketCountFor(sinceSecs, untilSecs, histogramBucketMs) -
      MAX_HISTOGRAM_BUCKETS;
    if (overflow > 0) {
      sinceSecs += (overflow * histogramBucketMs) / 1000;
    }
    return { sinceSecs, untilSecs, bucketMs: histogramBucketMs };
  }, [view, histogramBucketMs]);
  const histogram = useEventsHistogram(historicalFilters, histogramRange, {
    poll: followRight,
  });

  const selection = useMemo(() => {
    if (sinceUnixSecs == null && untilUnixSecs == null) {
      return null;
    }
    return {
      a: (sinceUnixSecs ?? 0) * 1000,
      b: (untilUnixSecs ?? Math.floor(Date.now() / 1000)) * 1000,
    };
  }, [sinceUnixSecs, untilUnixSecs]);

  // The strip is free to pan and zoom, but a domain reaching past now would
  // render dead space and put "the right edge of the strip" somewhere other
  // than now, which is what makes a pinned-to-now selection recognisable.
  // Clamp both the end and the span once, here.
  const changeView = (next: { a: number; b: number }) => {
    const nowMs = Date.now();
    // Beyond this the coarsest bucket on the ladder still needs more than
    // MAX_HISTOGRAM_BUCKETS buckets and the server rejects the range, so a
    // wheel-out would land the strip in a permanent failure state.
    const maxSpan =
      MAX_HISTOGRAM_BUCKETS * BUCKET_LADDER_MS[BUCKET_LADDER_MS.length - 1];
    const span = Math.min(next.b - next.a, maxSpan);
    const end = Math.min(next.b, nowMs);
    const clamped = { a: Math.max(0, end - span), b: end };
    setFollowRight(nowMs - clamped.b <= Math.max(histogramBucketMs, 5_000));
    setView(clamped);
  };

  const commitSelection = (sel: { a: number; b: number } | null) => {
    const nowSecs = Math.floor(Date.now() / 1000);
    const since =
      sel == null ? undefined : Math.max(0, Math.floor(sel.a / 1000));
    // A right edge inside the trailing bucket means "still pinned to now", so
    // the bound is dropped rather than frozen. It is never written ahead of now.
    const untilRaw =
      sel == null ? undefined : Math.min(nowSecs, Math.ceil(sel.b / 1000));
    const until =
      untilRaw == null || untilRaw >= nowSecs - histogramBucketMs / 1000
        ? undefined
        : untilRaw;
    if (since === sinceUnixSecs && until === untilUnixSecs) {
      return;
    }
    navigate({
      search: (prev) => ({
        ...prev,
        since_unix_secs: since,
        until_unix_secs: until,
      }),
    });
  };

  const focusAround = (tsMs: number, radiusSecs: number) => {
    const nowMs = Date.now();
    const nowSecs = Math.floor(nowMs / 1000);
    const centerSecs = Math.floor(tsMs / 1000);
    const viewEnd = Math.min(nowMs, (centerSecs + radiusSecs * 3) * 1000);
    setFollowRight(nowMs - viewEnd <= Math.max(histogramBucketMs, 5_000));
    setView({ a: (centerSecs - radiusSecs * 3) * 1000, b: viewEnd });
    // Anchoring a fresh row would otherwise write a future upper bound and
    // freeze tailing on a window that has not happened yet.
    const untilRaw = centerSecs + radiusSecs;
    navigate({
      search: (prev) => ({
        ...prev,
        since_unix_secs: centerSecs - radiusSecs,
        until_unix_secs:
          untilRaw >= nowSecs - histogramBucketMs / 1000 ? undefined : untilRaw,
      }),
    });
  };

  const applyPreset = (widthSecs: number | null) => {
    if (widthSecs == null) {
      navigate({
        search: (prev) => ({
          ...prev,
          since_unix_secs: undefined,
          until_unix_secs: undefined,
        }),
      });
      return;
    }
    const nowMs = Date.now();
    const since = Math.floor(nowMs / 1000) - widthSecs;
    // Frame the whole preset with a little lead-in so its left edge shows.
    changeView({ a: (since - Math.max(60, widthSecs * 0.1)) * 1000, b: nowMs });
    navigate({
      search: (prev) => ({
        ...prev,
        since_unix_secs: since,
        until_unix_secs: undefined,
      }),
    });
  };

  useImperativeHandle(handleRef, () => ({ focusAround, applyPreset }));

  return (
    <TimeRangeStrip
      buckets={histogram.data?.buckets ?? []}
      bucketMs={histogramBucketMs}
      view={view ?? { a: Date.now() - 3_600_000, b: Date.now() }}
      selection={selection}
      loading={histogram.data === undefined && histogram.isPending}
      failed={histogram.isError}
      onViewChange={changeView}
      onSelectionCommit={commitSelection}
    />
  );
}
