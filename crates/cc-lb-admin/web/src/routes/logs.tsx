import { Toggle as BaseToggle } from '@base-ui/react/toggle';
import { useQueryClient } from '@tanstack/react-query';
import { createFileRoute, useNavigate } from '@tanstack/react-router';
import { AlertTriangle, Download, RefreshCw, X, Zap } from 'lucide-react';
import {
  type SetStateAction,
  useEffect,
  useMemo,
  useRef,
  useState,
} from 'react';
import * as z from 'zod';
import { LiveTailFailureBanner } from '../components/LiveTailFailureBanner';
import { type FilterOption, LogSelect } from '../components/ui/LogSelect';
import { LogsPagination } from '../components/ui/LogsPagination';
import {
  Button,
  Card,
  cx,
  Field,
  FullPage,
  INPUT_CLASS,
  Section,
} from '../components/ui/primitives';
import { RequestEventsTable } from '../components/ui/RequestEventsTable';
import { SessionChip } from '../components/ui/SessionChip';
import { TimeRangeBounds } from '../components/ui/TimeRangeBounds';
import { TimeRangeStrip } from '../components/ui/TimeRangeStrip';
import { eventTime, type RecentEventsPayload } from '../lib/api';
import {
  filterLiveEventsByUnixSeconds,
  filterLogRows,
  LOG_STATUS_CLASSES,
  mergeLogRows,
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
    status: z.enum(LOG_STATUS_CLASSES).optional(),
    source_kind: z.enum(['all', 'renewal']).optional(),
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
  if (filters.status) base.status_class = filters.status;
  if (filters.source_kind) base.source_kind = filters.source_kind;
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

function LogsPage() {
  const queryClient = useQueryClient();
  const filters = Route.useSearch();
  const navigate = useNavigate({ from: Route.fullPath });
  const upstreams = useUpstreams();

  const { session: sessionFilter } = filters;
  const serverFilters = buildLiveFilters(filters);
  const historicalFilters = buildHistoricalFilters(filters);

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

  const pageCount = Math.max(1, cursorStack.length);
  const clampedPage = Math.min(page, pageCount - 1);
  const currentPageParam = cursorStack[clampedPage] ?? INITIAL_LOGS_PAGE_PARAM;
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

  // biome-ignore lint/correctness/useExhaustiveDependencies: route filter scalars intentionally reset pagination and table scroll without being read in the effect body.
  useEffect(() => {
    paginationRequestGenerationRef.current += 1;
    setPage(0);
    setCursorStack([INITIAL_LOGS_PAGE_PARAM]);
    setLoadedPages([]);
    setLoadingNext(false);
    setExhaustedCursorKeys(new Set());
    if (scrollContainerRef.current) scrollContainerRef.current.scrollTop = 0;
  }, [
    filters.principal_id,
    filters.upstream_id,
    filters.session,
    filters.model,
    filters.status,
    filters.source_kind,
    filters.since_unix_secs,
    filters.until_unix_secs,
  ]);

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

  const live = useLiveEventStream(serverFilters, { enabled: effectiveTailing });
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
    connecting: 'info',
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

  // The initial strip domain comes from the first page we already fetched, so
  // it costs no extra query and adapts to traffic density instead of assuming
  // a fixed lookback. Global min/max is deliberately never queried.
  const [view, setView] = useState<{ a: number; b: number } | null>(null);
  const firstPageEvents = historicalPages[0]?.events;
  useEffect(() => {
    if (view != null || firstPageEvents == null) return;
    const now = Date.now();
    // A URL that pins the right edge is asking about a past window, so the
    // domain brackets that window instead of stretching to now.
    const fixedEnd =
      filters.until_unix_secs == null ? null : filters.until_unix_secs * 1000;
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
      const start =
        filters.since_unix_secs == null
          ? oldest
          : filters.since_unix_secs * 1000;
      const pad = Math.max(60_000, (fixedEnd - start) * 0.15);
      setView({ a: start - pad, b: Math.min(now, fixedEnd + pad) });
      setFollowRight(false);
      return;
    }
    // Otherwise the right edge is now, not the newest row: an idle proxy would
    // open on a domain that ends in the past and never shows arriving traffic.
    setView({ a: Math.min(oldest, now - 60_000), b: now });
    setFollowRight(true);
  }, [firstPageEvents, view, filters.since_unix_secs, filters.until_unix_secs]);

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
  const histogram = useEventsHistogram(serverFilters, histogramRange, {
    poll: followRight,
  });

  const selection = useMemo(() => {
    if (filters.since_unix_secs == null && filters.until_unix_secs == null) {
      return null;
    }
    return {
      a: (filters.since_unix_secs ?? 0) * 1000,
      b: (filters.until_unix_secs ?? Math.floor(Date.now() / 1000)) * 1000,
    };
  }, [filters.since_unix_secs, filters.until_unix_secs]);

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
    if (
      since === filters.since_unix_secs &&
      until === filters.until_unix_secs
    ) {
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
  // biome-ignore lint/correctness/useExhaustiveDependencies: same stable-Map contract as rows above; the first page must re-merge whenever a live upsert bumps live.version.
  const pageRows = useMemo(
    () =>
      filterLogRows(
        mergeLogRows(
          clampedPage === 0 ? rangedLiveEvents : new Map(),
          currentHistoricalEvents,
        ),
        filters,
      ).slice(0, LOGS_PAGE_SIZE),
    [
      clampedPage,
      currentHistoricalEvents,
      filters,
      rangedLiveEvents,
      live.version,
    ],
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
    !exhaustedCursorKeys.has(lastHistoricalCursorKey);

  const commitPage = (nextPage: SetStateAction<number>) => {
    if (scrollContainerRef.current) scrollContainerRef.current.scrollTop = 0;
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
    const cachedPageParam = cursorStack[nextPageIndex];
    if (
      loadedPages[nextPageIndex] !== undefined &&
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

  const principalSelectOptions = useMemo<FilterOption[]>(
    () =>
      Array.from(principalNameMap.entries()).map(([id, name]) => ({
        value: id,
        label: <span className="truncate">{name}</span>,
      })),
    [principalNameMap],
  );

  const upstreamSelectOptions = useMemo<FilterOption[]>(
    () =>
      (upstreams.data?.upstreams ?? []).map((u) => ({
        value: u.id,
        label: <span className="font-mono truncate">{u.name}</span>,
      })),
    [upstreams.data],
  );

  const sessionSelectOptions = useMemo<FilterOption[]>(() => {
    const list: FilterOption[] = [];
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

  const statusSelectOptions = useMemo<FilterOption[]>(
    () => [
      { value: '2xx', label: <span className="font-mono">2xx</span> },
      { value: '3xx', label: <span className="font-mono">3xx</span> },
      { value: '4xx', label: <span className="font-mono">4xx</span> },
      { value: '5xx', label: <span className="font-mono">5xx</span> },
    ],
    [],
  );

  const sourceKindSelectOptions = useMemo<FilterOption[]>(
    () => [
      { value: 'all', label: <span className="font-mono">All events</span> },
      {
        value: 'renewal',
        label: <span className="font-mono">Renewals only</span>,
      },
    ],
    [],
  );

  // biome-ignore lint/correctness/useExhaustiveDependencies: same rationale — live.version is the mutation counter for the stable eventsMap ref.
  const recentLiveIds = useMemo(
    () =>
      new Set(
        Array.from(live.eventsMap.values())
          .slice(0, 20)
          .map((e) => e.event.event_id ?? e.event.request_id),
      ),
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

  return (
    <FullPage>
      <LiveTailFailureBanner
        permanentFailure={live.permanentFailure}
        permanentFailureSince={live.permanentFailureSince}
        reconnectAttempts={live.reconnectAttempts}
        onRetry={live.forceReconnect}
      />
      <Section
        title="Live Logs"
        className="flex-1 min-h-0"
        subtitle={
          <span className="flex items-center gap-2">
            <span>
              {visibleRows.length} requests —{' '}
              {effectiveTailing ? 'live tailing' : 'paged'}
            </span>
            {effectiveTailing ? (
              <span className="inline-flex items-center gap-1 px-1.5 py-0.5 text-[10px] uppercase tracking-wider border border-subtle rounded-sm">
                {tailStatus === 'failed' ? (
                  <AlertTriangle className="w-3 h-3 text-[color:var(--color-danger)]" />
                ) : (
                  <span
                    className={cx(
                      'status-dot',
                      statusColor,
                      tailStatus === 'connecting' ||
                        tailStatus === 'reconnecting'
                        ? 'animate-pulse'
                        : '',
                    )}
                  />
                )}
                <span
                  className={
                    tailStatus === 'failed'
                      ? 'text-[color:var(--color-danger)] font-bold'
                      : ''
                  }
                >
                  {statusLabel}
                </span>
              </span>
            ) : null}
          </span>
        }
        action={
          <div className="flex items-center gap-2 flex-wrap justify-end">
            <BaseToggle
              aria-label="Live tail logs"
              className="inline-flex items-center justify-center rounded-sm font-medium transition-colors select-none disabled:cursor-not-allowed disabled:opacity-50 focus-visible:outline focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-2 h-7 px-2.5 text-xs gap-1.5 bg-[color:var(--color-panel-strong)] border border-[color:var(--color-border)] text-[color:var(--color-text)] hover:bg-[color:var(--color-hover-bg)] data-[pressed]:bg-[color:var(--color-accent-dim)] data-[pressed]:text-[color:var(--color-accent)] data-[pressed]:border-[color:var(--color-accent)]"
              onPressedChange={setUserRequestedTailing}
              pressed={effectiveTailing}
              disabled={filters.until_unix_secs != null}
              title={
                filters.until_unix_secs != null
                  ? 'Live tail is off while the range has a fixed end. Extend the range to now to resume.'
                  : undefined
              }
            >
              <Zap className="w-3 h-3" />
              {effectiveTailing ? 'Stop tail' : 'Live tail'}
            </BaseToggle>
            <Button
              size="sm"
              iconLeft={<RefreshCw className="w-3 h-3" />}
              onClick={refreshLogs}
            >
              Refresh
            </Button>
            <Button
              size="sm"
              iconLeft={<Download className="w-3 h-3" />}
              onClick={downloadJson}
            >
              Export
            </Button>
          </div>
        }
      >
        <Card className="flex-1 flex flex-col min-h-0">
          <div className="p-3 border-b border-subtle flex flex-wrap gap-3 items-end shrink-0">
            <Field label="Principal">
              <LogSelect
                value={filters.principal_id ?? ''}
                options={principalSelectOptions}
                onChange={(v) => setFilter('principal_id', v)}
                allLabel="All principals"
                widthClass="w-44"
              />
            </Field>
            <Field label="Upstream">
              <LogSelect
                value={filters.upstream_id ?? ''}
                options={upstreamSelectOptions}
                onChange={(v) => setFilter('upstream_id', v)}
                allLabel="All upstreams"
                widthClass="w-44"
              />
            </Field>
            <Field label="Session">
              <LogSelect
                value={sessionFilter ?? ''}
                options={sessionSelectOptions}
                onChange={(v) => setFilter('session', v)}
                allLabel="All sessions"
                widthClass="!w-64"
              />
            </Field>
            <Field label="Model">
              <input
                className={`${INPUT_CLASS} w-44 font-mono`}
                value={modelDraft}
                onChange={(e) => setModelDraft(e.target.value)}
                placeholder="claude-sonnet-4-5"
              />
            </Field>
            <Field label="Status">
              <LogSelect
                value={filters.status ?? ''}
                options={statusSelectOptions}
                onChange={(v) => setFilter('status', v)}
                allLabel="All statuses"
                widthClass="w-32"
              />
            </Field>
            <Field label="Kind">
              <LogSelect
                value={filters.source_kind ?? ''}
                options={sourceKindSelectOptions}
                onChange={(v) => setFilter('source_kind', v)}
                allLabel="Exclude renewals"
                widthClass="w-40"
              />
            </Field>
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
            {filters.principal_id ||
            filters.upstream_id ||
            filters.session ||
            filters.model ||
            filters.status ||
            filters.source_kind ||
            filters.since_unix_secs ||
            filters.until_unix_secs ? (
              <Button
                iconLeft={<X className="w-3 h-3" />}
                onClick={() => navigate({ search: {} })}
              >
                Clear
              </Button>
            ) : null}
          </div>
          <div className="px-3 pt-2 border-b border-subtle shrink-0">
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
          </div>

          <div
            ref={scrollContainerRef}
            className="flex-1 overflow-auto min-h-0"
          >
            <RequestEventsTable
              events={pageRows}
              onAnchorRange={focusAround}
              principalNameMap={principalNameMap}
              upstreamNameMap={upstreamNameMap}
              loading={initialRowsLoading}
              reservedRowCount={LOGS_RESERVED_ROW_COUNT}
              liveFlashIds={
                effectiveTailing && clampedPage === 0
                  ? recentLiveIds
                  : undefined
              }
              columns={LOGS_TABLE_COLUMNS}
              minWidthClass="min-w-[1080px]"
              emptyTitle="No requests"
              emptyDescription="Adjust filters or enable live tail."
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
            loadingNext={loadingNext}
            onPrev={previousPage}
            onNext={() => void nextPage()}
          />
        </Card>
      </Section>
    </FullPage>
  );
}
