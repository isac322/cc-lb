import { Toggle as BaseToggle } from '@base-ui/react/toggle';
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
import { LOGS_EMPTY_COPY } from '../components/onboarding/logsEmptyCopy';
import {
  Button,
  buttonClassName,
  cx,
  Field,
  FullPage,
  INPUT_SM_CLASS,
  PageHeader,
  SegmentedControl,
  type SegmentedOption,
} from '../components/ui/primitives';
import { RequestEventsFeed } from '../components/ui/RequestEventsFeed';
import { Select, type SelectOption } from '../components/ui/Select';
import { SessionChip } from '../components/ui/SessionChip';
import { TimeRangeBounds } from '../components/ui/TimeRangeBounds';
import { TimeRangeStrip } from '../components/ui/TimeRangeStrip';
import {
  eventTime,
  type RequestEvent,
  type RequestEventKind,
  RequestEventKindSchema,
} from '../lib/api';
import {
  LOG_STATUS_FILTER_VALUES,
  REQUEST_EVENT_KIND_LABELS,
  REQUEST_EVENT_KINDS,
} from '../lib/logRows';
import { LOGS_PAGE_SIZE } from '../lib/logsPagination';
import {
  useEventsHistogram,
  usePrincipalNameMap,
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
import {
  buildRequestEventsHistoryFilters,
  buildRequestEventsLiveFilters,
  useRequestEventsFeed,
} from '../lib/useRequestEventsFeed';

const unixSecondsSearchParam = z.preprocess((value) => {
  if (value == null || value === '') return undefined;
  const number = Number(value);
  return Number.isSafeInteger(number) &&
    number >= 0 &&
    number <= MAX_FORMATTABLE_UNIX_SECONDS
    ? number
    : undefined;
}, z.number().optional());

/** `event_kind` value that lifts the default kind filter. */
const ALL_EVENT_KINDS = 'all';
/** The kind Logs lists when the URL names none: model traffic, `/v1/messages`. */
export const DEFAULT_EVENT_KIND: RequestEventKind = 'messages';

/**
 * The kind the list is filtered to: the URL's kind, the default kind when the
 * URL has none, or undefined (every kind) for `all`.
 */
export function effectiveEventKind(
  eventKind: RequestEventKind | typeof ALL_EVENT_KINDS | undefined,
): RequestEventKind | undefined {
  if (eventKind === ALL_EVENT_KINDS) return undefined;
  return eventKind ?? DEFAULT_EVENT_KIND;
}

export const logsSearchSchema = z
  .object({
    principal_id: z.string().optional(),
    upstream_id: z.string().optional(),
    session: z.string().optional(),
    model: z.string().optional(),
    status: z.enum(LOG_STATUS_FILTER_VALUES).optional(),
    // Absent or invalid `event_kind` means the default kind, `messages`
    // (`effectiveEventKind`), so the default URL stays clean; `all` is the
    // explicit opt-out that lists every endpoint category.
    event_kind: z
      .union([RequestEventKindSchema, z.literal(ALL_EVENT_KINDS)])
      .optional()
      .catch(undefined),
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

export const Route = createFileRoute('/logs')({
  validateSearch: logsSearchSchema,
  component: LogsPage,
});

export function buildHistoricalFilters(
  filters: z.infer<typeof logsSearchSchema>,
) {
  return buildRequestEventsHistoryFilters({
    ...filters,
    event_kind: effectiveEventKind(filters.event_kind),
  });
}

export function buildLiveFilters(filters: z.infer<typeof logsSearchSchema>) {
  return buildRequestEventsLiveFilters({
    ...filters,
    event_kind: effectiveEventKind(filters.event_kind),
  });
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
  const filters = Route.useSearch();
  const navigate = useNavigate({ from: Route.fullPath });
  const upstreams = useUpstreams();
  const { session: sessionFilter } = filters;
  const eventKindFilter = effectiveEventKind(filters.event_kind);
  const feedFilters = useMemo(
    () => ({ ...filters, event_kind: eventKindFilter }),
    [eventKindFilter, filters],
  );
  const [userRequestedTailing, setUserRequestedTailing] = useState(true);
  const [modelDraft, setModelDraft] = useState(filters.model ?? '');
  const lastCommittedModelRef = useRef(filters.model ?? '');
  const scrollContainerRef = useRef<HTMLDivElement>(null);
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
  const [paginationIdentity, setPaginationIdentity] = useState(
    nextPaginationIdentity,
  );
  const pendingScrollRestoreRef = useRef<number | null>(null);
  const paginationIdentityChanged =
    paginationIdentity !== nextPaginationIdentity;
  if (paginationIdentityChanged) {
    setPaginationIdentity(nextPaginationIdentity);
    pendingScrollRestoreRef.current =
      scrollContainerRef.current?.scrollTop ?? null;
  }

  const feed = useRequestEventsFeed({
    filters: feedFilters,
    live: userRequestedTailing,
    initialHistoryLimit: LOGS_PAGE_SIZE,
    pageSize: LOGS_PAGE_SIZE,
  });
  const principalNameMap = usePrincipalNameMap();
  const effectiveTailing = feed.liveEnabled;
  const {
    rows,
    tailStatus,
    statusLabel,
    statusColor,
    filteredRows: visibleRows,
    firstPageEvents,
    historyFilters: historicalFilters,
  } = feed;
  const refreshLogs = feed.refresh;

  const routeModel = filters.model ?? '';
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
        resetScroll: false,
        search: (prev) => ({ ...prev, model: next || undefined }),
      });
    }, MODEL_FILTER_DEBOUNCE_MS);
    return () => clearTimeout(timer);
  }, [modelDraft, routeModel, navigate]);

  useLayoutEffect(() => {
    const pending = pendingScrollRestoreRef.current;
    const el = scrollContainerRef.current;
    if (pending == null || !el) return;
    if (el.scrollHeight > el.clientHeight) {
      el.scrollTop = Math.min(pending, el.scrollHeight - el.clientHeight);
      pendingScrollRestoreRef.current = null;
    } else if (!feed.loading) {
      pendingScrollRestoreRef.current = null;
    }
  });

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
      // Messages is the default; the "All kinds" item (`''`) lifts the
      // filter, so every kind and the unfiltered list stay selectable.
      REQUEST_EVENT_KINDS.map((kind) => ({
        value: kind,
        label: REQUEST_EVENT_KIND_LABELS[kind],
      })),
    [],
  );

  const setFilter = (key: keyof typeof filters, value: string) => {
    navigate({
      search: { ...filters, [key]: value || undefined },
      resetScroll: false,
    });
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
    // The raw param: the default kind is not an applied filter, `all` is.
    filters.event_kind,
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
    // Only a kind that differs from the default gets a chip; removing it
    // returns to the default kind.
    ...(filters.event_kind
      ? [
          {
            key: 'event_kind' as const,
            label: 'Kind',
            value:
              eventKindFilter == null
                ? 'All kinds'
                : REQUEST_EVENT_KIND_LABELS[eventKindFilter],
          },
        ]
      : []),
  ];
  const panelFilterCount = filterChips.length + (hasCustomRange ? 1 : 0);

  return (
    // Same content width and side padding as PageContainer pages (plugins,
    // settings, audit); FullPage keeps the viewport-fit console with the rows
    // scrolling inside the card. The overrides are `lg:` variants so they win
    // over FullPage's base classes; below `lg` the viewport is narrower anyway.
    <FullPage className="gap-3 lg:max-w-[90rem] lg:px-10">
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
              // The neighbouring md secondary `Button` chrome (sizes, phone
              // touch height, disabled look); only the pressed state adds a fill.
              className={cx(
                buttonClassName('secondary', 'md'),
                'data-[pressed]:bg-panel-strong',
              )}
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
      {/* Filters and the density strip sit on the ground; only the table
          keeps a flat surface. */}
      <div className="flex flex-col gap-3 shrink-0">
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
              onClick={() => navigate({ search: {}, resetScroll: false })}
            >
              Clear
            </Button>
          ) : null}
        </div>
        <div
          id={filterPanelId}
          hidden={!filtersOpen}
          className="grid grid-cols-2 gap-3 md:flex md:flex-wrap md:items-end"
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
              // The default kind keeps the URL clean; "All kinds" is written
              // out, since an absent kind means the default.
              onChange={(v) =>
                setFilter(
                  'event_kind',
                  v === ''
                    ? ALL_EVENT_KINDS
                    : v === DEFAULT_EVENT_KIND
                      ? ''
                      : v,
                )
              }
              allLabel="All kinds"
              className={FILTER_CONTROL_WIDTH}
            />
          </Field>
          {/* Phones stack From, To and Apply at full width: two 12rem fields
              and a button beside them wrap into a ragged staircase. */}
          <div
            role="group"
            aria-label="Custom range"
            className="col-span-full flex flex-wrap items-end gap-3 md:basis-full max-md:flex-col max-md:items-stretch"
          >
            <TimeRangeBounds
              since={filters.since_unix_secs}
              until={filters.until_unix_secs}
              onCommit={({ since, until }) => {
                navigate({
                  resetScroll: false,
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
      <div className="shrink-0">
        <LogsTimeStrip
          handleRef={stripRef}
          sinceUnixSecs={filters.since_unix_secs}
          untilUnixSecs={filters.until_unix_secs}
          historicalFilters={historicalFilters}
          firstPageEvents={firstPageEvents}
        />
      </div>

      <RequestEventsFeed
        feed={feed}
        showStatus={false}
        onPageChange={() => resetRowsScroll(scrollContainerRef.current)}
        tableContainerRef={scrollContainerRef}
        onAnchorRange={focusAround}
        principalNameMap={principalNameMap}
        reservedRowCount={LOGS_RESERVED_ROW_COUNT}
        columns={LOGS_TABLE_COLUMNS}
        minWidthClass="min-w-[1080px]"
        emptyTitle={emptyCopy.title}
        emptyDescription={emptyCopy.description}
        emptyAction={emptyCopy.action}
        emptyHeadingLevel={2}
      />
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
  firstPageEvents: readonly RequestEvent[] | undefined;
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
  // The histogram spans the whole view, bounded by `histogramRange`; the
  // request overwrites the selection's since/until with it. Keeping them in the
  // query key would make every committed drag a new, empty query, blanking the
  // bars behind the fresh selection until the refetch lands.
  const {
    since_unix_secs: _selectionSince,
    until_unix_secs: _selectionUntil,
    ...histogramFilters
  } = historicalFilters;
  const histogram = useEventsHistogram(histogramFilters, histogramRange, {
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
      resetScroll: false,
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
      resetScroll: false,
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
        resetScroll: false,
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
      resetScroll: false,
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
