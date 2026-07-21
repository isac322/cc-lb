import { Toggle as BaseToggle } from '@base-ui/react/toggle';
import { createFileRoute, useNavigate } from '@tanstack/react-router';
import { AlertTriangle, Download, RefreshCw, X, Zap } from 'lucide-react';
import { useDeferredValue, useEffect, useMemo, useRef, useState } from 'react';
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
import {
  type TimeRangeMode,
  TimeRangeSelect,
} from '../components/ui/TimeRangeSelect';
import {
  filterLiveEventsByUnixSeconds,
  filterLogRowsByStatusClass,
  LOG_STATUS_CLASSES,
  mergeLogRows,
} from '../lib/logRows';
import {
  clampLogsPage,
  getLogsPageCount,
  isLastLogsPage,
  LOGS_PAGE_SIZE,
  selectLogsPageRows,
} from '../lib/logsPagination';
import {
  usePrincipalNameMap,
  useRecentEventsInfinite,
  useUpstreamNameMap,
  useUpstreams,
} from '../lib/queries';
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
    upstream: z.string().optional(),
    session: z.string().optional(),
    model: z.string().optional(),
    status: z.enum(LOG_STATUS_CLASSES).optional(),
    source_kind: z.enum(['all', 'renewal']).optional(),
    time_range: z.enum(['all', '1h', '6h', '24h', '7d', 'custom']).optional(),
    since_unix_secs: unixSecondsSearchParam,
    until_unix_secs: unixSecondsSearchParam,
  })
  .transform((filters) => {
    if (
      filters.time_range === 'custom' &&
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

export const Route = createFileRoute('/logs')({
  validateSearch: logsSearchSchema,
  component: LogsPage,
});

export function buildHistoricalFilters(
  filters: z.infer<typeof logsSearchSchema>,
) {
  const { time_range, since_unix_secs, until_unix_secs } = filters;
  const base = buildLiveFilters(filters);

  if (time_range === 'custom') {
    if (since_unix_secs != null && until_unix_secs != null) {
      base.since_unix_secs = since_unix_secs.toString();
      base.until_unix_secs = until_unix_secs.toString();
    }
  } else if (time_range && time_range !== 'all') {
    if (since_unix_secs != null) {
      base.since_unix_secs = since_unix_secs.toString();
    }
  }

  return base;
}

export function buildLiveFilters(filters: z.infer<typeof logsSearchSchema>) {
  const base: Record<string, string> = {};
  if (filters.principal_id) base.principal_id = filters.principal_id;
  if (filters.upstream) base.upstream = filters.upstream;
  if (filters.model) base.model = filters.model;
  if (filters.status) base.status_class = filters.status;
  if (filters.source_kind) base.source_kind = filters.source_kind;
  return base;
}

export function getLogsRouteState({
  sessionFilter,
  hasNextPage,
  userRequestedTailing,
  time_range,
  isLastClientPage,
}: {
  sessionFilter?: string;
  hasNextPage: boolean;
  userRequestedTailing: boolean;
  time_range?: TimeRangeMode;
  isLastClientPage: boolean;
}) {
  return {
    showSentinel: !sessionFilter && hasNextPage && isLastClientPage,
    effectiveTailing: userRequestedTailing && time_range !== 'custom',
  };
}

const LOGS_TABLE_COLUMNS = { cost: true, tokens: true } as const;

function LogsPage() {
  const filters = Route.useSearch();
  const navigate = useNavigate({ from: Route.fullPath });
  const upstreams = useUpstreams();

  const { session: sessionFilter, time_range } = filters;

  const serverFilters = buildLiveFilters(filters);

  const historicalFilters = buildHistoricalFilters(filters);

  const recent = useRecentEventsInfinite(historicalFilters);
  const principalNameMap = usePrincipalNameMap();
  const upstreamNameMap = useUpstreamNameMap();

  const [userRequestedTailing, setUserRequestedTailing] = useState(true);
  const [page, setPage] = useState(0);
  const scrollContainerRef = useRef<HTMLDivElement>(null);
  const sentinelRef = useRef<HTMLTableRowElement>(null);

  const effectiveTailing = userRequestedTailing && time_range !== 'custom';

  // biome-ignore lint/correctness/useExhaustiveDependencies: route filter scalars intentionally reset pagination and table scroll without being read in the effect body.
  useEffect(() => {
    setPage(0);
    if (scrollContainerRef.current) scrollContainerRef.current.scrollTop = 0;
  }, [
    filters.principal_id,
    filters.upstream,
    filters.session,
    filters.model,
    filters.status,
    filters.source_kind,
    filters.time_range,
    filters.since_unix_secs,
    filters.until_unix_secs,
  ]);

  const live = useLiveEventStream(serverFilters, { enabled: effectiveTailing });
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
  const rows = useMemo(() => {
    const historical = recent.isPlaceholderData
      ? []
      : (recent.data?.pages.flatMap((p) => p.events) ?? []);
    const rangedLiveEvents = filterLiveEventsByUnixSeconds(live.eventsMap, {
      since:
        historicalFilters.since_unix_secs === undefined
          ? undefined
          : Number(historicalFilters.since_unix_secs),
      until:
        historicalFilters.until_unix_secs === undefined
          ? undefined
          : Number(historicalFilters.until_unix_secs),
    });
    return mergeLogRows(rangedLiveEvents, historical);
  }, [
    live.eventsMap,
    live.version,
    recent.data,
    recent.isPlaceholderData,
    historicalFilters.since_unix_secs,
    historicalFilters.until_unix_secs,
  ]);

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

  const visibleRows = useMemo(() => {
    const statusRows = filterLogRowsByStatusClass(rows, filters.status);
    return sessionFilter
      ? statusRows.filter((row) => row.thread_id === sessionFilter)
      : statusRows;
  }, [rows, filters.status, sessionFilter]);
  const deferredVisibleRows = useDeferredValue(visibleRows);

  const pageCount = getLogsPageCount(deferredVisibleRows.length);
  const clampedPage = clampLogsPage(page, pageCount);
  const pageRows = useMemo(
    () => selectLogsPageRows(deferredVisibleRows, clampedPage),
    [deferredVisibleRows, clampedPage],
  );
  const isLastClientPage = isLastLogsPage(clampedPage, pageCount);

  // biome-ignore lint/correctness/useExhaustiveDependencies: clampedPage changes intentionally trigger the imperative scroll reset.
  useEffect(() => {
    if (scrollContainerRef.current) scrollContainerRef.current.scrollTop = 0;
  }, [clampedPage]);

  const { showSentinel } = getLogsRouteState({
    sessionFilter,
    hasNextPage: recent.hasNextPage,
    userRequestedTailing,
    time_range,
    isLastClientPage,
  });

  // biome-ignore lint/correctness/useExhaustiveDependencies: the deferred row count changes when the sentinel actually mounts; without it the observer can run once against a null ref and never attach.
  useEffect(() => {
    const el = sentinelRef.current;
    if (!el || !showSentinel || recent.isFetchingNextPage) return;
    const obs = new IntersectionObserver(
      (entries) =>
        entries.forEach((e) => {
          if (e.isIntersecting) recent.fetchNextPage();
        }),
      { root: scrollContainerRef.current, threshold: 0.1 },
    );
    obs.observe(el);
    return () => obs.disconnect();
  }, [
    showSentinel,
    recent.isFetchingNextPage,
    recent.fetchNextPage,
    pageRows.length,
  ]);

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
        value: u.name,
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
              disabled={time_range === 'custom'}
              title={
                time_range === 'custom'
                  ? 'Live tail is disabled for custom time ranges'
                  : undefined
              }
            >
              <Zap className="w-3 h-3" />
              {effectiveTailing ? 'Stop tail' : 'Live tail'}
            </BaseToggle>
            <Button
              size="sm"
              iconLeft={<RefreshCw className="w-3 h-3" />}
              onClick={() => recent.refetch()}
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
                value={filters.upstream ?? ''}
                options={upstreamSelectOptions}
                onChange={(v) => setFilter('upstream', v)}
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
                value={filters.model ?? ''}
                onChange={(e) => setFilter('model', e.target.value)}
                placeholder="claude-*"
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
            <TimeRangeSelect
              value={{
                mode: filters.time_range ?? 'all',
                since_unix_secs: filters.since_unix_secs,
                until_unix_secs: filters.until_unix_secs,
              }}
              onChange={(val) => {
                navigate({
                  search: {
                    ...filters,
                    time_range: val.mode === 'all' ? undefined : val.mode,
                    since_unix_secs: val.since_unix_secs,
                    until_unix_secs: val.until_unix_secs,
                  },
                });
              }}
            />
            {filters.principal_id ||
            filters.upstream ||
            filters.session ||
            filters.model ||
            filters.status ||
            filters.source_kind ||
            filters.time_range ? (
              <Button
                iconLeft={<X className="w-3 h-3" />}
                onClick={() => navigate({ search: {} })}
              >
                Clear
              </Button>
            ) : null}
          </div>

          <div
            ref={scrollContainerRef}
            className="flex-1 overflow-auto min-h-0"
          >
            <RequestEventsTable
              events={pageRows}
              principalNameMap={principalNameMap}
              upstreamNameMap={upstreamNameMap}
              loading={
                (recent.isPending || recent.isPlaceholderData) &&
                live.eventsMap.size === 0
              }
              liveFlashIds={effectiveTailing ? recentLiveIds : undefined}
              columns={LOGS_TABLE_COLUMNS}
              sentinelRef={showSentinel ? sentinelRef : undefined}
              loadingMore={recent.isFetchingNextPage}
              hasMore={showSentinel}
              minWidthClass="min-w-[1080px]"
              emptyTitle="No requests"
              emptyDescription="Adjust filters or enable live tail."
            />
          </div>
          {deferredVisibleRows.length > 0 && (
            <LogsPagination
              page={clampedPage}
              pageCount={pageCount}
              totalRows={deferredVisibleRows.length}
              pageSize={LOGS_PAGE_SIZE}
              onPrev={() => setPage((p) => Math.max(0, p - 1))}
              onNext={() => setPage((p) => Math.min(pageCount - 1, p + 1))}
            />
          )}
        </Card>
      </Section>
    </FullPage>
  );
}
