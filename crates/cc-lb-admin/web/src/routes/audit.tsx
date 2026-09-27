import {
  createFileRoute,
  Link,
  useNavigate,
  useRouter,
} from '@tanstack/react-router';
import { ChevronRight, RefreshCw, SlidersHorizontal, X } from 'lucide-react';
import {
  type MouseEvent,
  type ReactNode,
  useMemo,
  useRef,
  useState,
} from 'react';
import * as z from 'zod';
import { AuditEntryDrawer } from '../components/audit/AuditEntryDrawer';
import {
  AUDIT_CATEGORY_LABEL,
  AUDIT_TYPE_FILTERS,
  AUDIT_TYPE_NOUN,
  type AuditEntryLike,
  type AuditTarget,
  type AuditTypeFilter,
  auditActorLabel,
  auditCategory,
  auditDetailParts,
  auditTarget,
  humanizeAuditAction,
  type NameMaps,
  type ParsedAction,
  parseAuditAction,
  readableRoute,
  statusTextClass,
} from '../components/audit/auditEntry';
import {
  Button,
  Card,
  cx,
  EmptyState,
  Field,
  FullPage,
  IconButton,
  PageHeader,
  SegmentedControl,
  SkeletonRow,
} from '../components/ui/primitives';
import { RelativeTime } from '../components/ui/RelativeTime';
import { Select } from '../components/ui/Select';
import {
  EmptyValue,
  Table,
  TableCell,
  TableHead,
  TableHeadCell,
  TableRow,
} from '../components/ui/Table';
import { TimeRangeBounds } from '../components/ui/TimeRangeBounds';
import { eventTime } from '../lib/api';
import { useLocale, useTimezone } from '../lib/locale';
import {
  useAudit,
  usePrincipalNameMap,
  usePrincipals,
  useUpstreamNameMap,
} from '../lib/queries';
import {
  TIME_PRESET_OPTIONS_WITH_ALL,
  TIME_PRESET_SECONDS,
  TIME_PRESETS,
  type TimePreset,
} from '../lib/timePresets';
import { MAX_FORMATTABLE_UNIX_SECONDS } from '../lib/timezone';

const AUDIT_LIMIT = 200;

type RangePreset = TimePreset;

const unixSecondsSearchParam = z.preprocess((value) => {
  if (value == null || value === '') return undefined;
  const number = Number(value);
  return Number.isSafeInteger(number) &&
    number >= 0 &&
    number <= MAX_FORMATTABLE_UNIX_SECONDS
    ? number
    : undefined;
}, z.number().optional());

const auditSearchSchema = z
  .object({
    principal_id: z.string().optional(),
    since: unixSecondsSearchParam,
    until: unixSecondsSearchParam,
    // Absent means the default "write" view; the server has no action-type
    // filter, so this one is applied to the loaded entries in the browser.
    type: z.enum(AUDIT_TYPE_FILTERS).optional().catch(undefined),
    // Marks which preset produced `since`; only used to highlight it.
    range: z.enum(TIME_PRESETS).optional().catch(undefined),
  })
  .transform((filters) => {
    if (
      filters.since != null &&
      filters.until != null &&
      filters.since > filters.until
    ) {
      return {
        ...filters,
        since: undefined,
        until: undefined,
        range: undefined,
      };
    }
    if (filters.since == null || filters.until != null) {
      return { ...filters, range: undefined };
    }
    return filters;
  });

export const Route = createFileRoute('/audit')({
  validateSearch: auditSearchSchema,
  component: AuditPage,
});

const TYPE_OPTIONS: ReadonlyArray<{ value: AuditTypeFilter; label: string }> = [
  { value: 'write', label: 'Writes' },
  { value: 'read', label: 'Reads' },
  { value: 'auth', label: 'Auth' },
  { value: 'all', label: 'All' },
];

const RANGE_OPTIONS = TIME_PRESET_OPTIONS_WITH_ALL;

// Columns size to the table's own width (container query, so a collapsed
// sidebar counts), not the viewport. Below 56rem (1024px screens) Action is the
// one flexible column and the change summary rides under it. From 56rem
// (1280–1536px screens) Action is capped and the room goes to a Details column
// plus the route beside the status; Target is wide enough for a typical
// `upstream <name>` without truncating. From 80rem (1920px screens) the fixed
// columns widen, favouring Target and the route over the line-clamped Details,
// and the actor kind shows. Order: Time, Action, Target, Details, Actor,
// Request (status + route), Open.
const AUDIT_COLUMN_CLASS_NAMES = [
  'w-[6rem]',
  '@4xl:w-[13rem] @7xl:w-[16rem]',
  'w-[10.5rem] @4xl:w-[13rem] @7xl:w-[17rem]',
  'hidden @4xl:table-cell',
  'w-[8.5rem] @4xl:w-[9.5rem] @7xl:w-[12rem]',
  'w-[4.5rem] @4xl:w-[12rem] @7xl:w-[20rem]',
  'w-[3.5rem]',
] as const;

const AUDIT_SKELETON_CELL_CLASS_NAMES = [
  undefined,
  undefined,
  undefined,
  'hidden @4xl:table-cell',
] as const;

const AUDIT_SKELETON_CLASS_NAMES = [
  'w-14',
  'w-40',
  'w-28',
  'w-48',
  'w-28',
  'w-8 ml-auto @4xl:ml-0 @4xl:w-40',
  'w-6 ml-auto',
] as const;

const ENTITY_LINK_CLASS =
  'rounded-sm text-text underline decoration-border-strong underline-offset-2 hover:decoration-accent focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-2';

/**
 * The target's name, linked to its page while the entity still exists. Falls
 * back to a plain anchor outside a router (component tests mount bare).
 */
function TargetName({ target, maps }: { target: AuditTarget; maps: NameMaps }) {
  const router = useRouter({ warn: false });
  const list = target.kind === 'upstream' ? maps.upstreams : maps.principals;
  if (!target.resolved || target.id == null || !list.has(target.id)) {
    return (
      <span
        className={
          target.resolved ? 'text-text' : 'font-mono text-data text-text-muted'
        }
      >
        {target.name}
      </span>
    );
  }
  const to = target.kind === 'upstream' ? '/upstreams' : '/principals';
  // The row opens the entry drawer; following the link must not.
  const stop = (event: MouseEvent) => event.stopPropagation();
  return router ? (
    <Link
      to={to}
      search={{ selectedId: target.id }}
      className={ENTITY_LINK_CLASS}
      onClick={stop}
    >
      {target.name}
    </Link>
  ) : (
    <a
      href={`${to}?selectedId=${encodeURIComponent(target.id)}`}
      className={ENTITY_LINK_CLASS}
      onClick={stop}
    >
      {target.name}
    </a>
  );
}

const GROUP_LABEL_CLASS = 'text-label text-text-muted';

interface AuditRow {
  entry: AuditEntryLike;
  key: string;
  parsed: ParsedAction;
}

function changedFieldsSummary(parsed: ParsedAction): string | null {
  const changed = parsed.params.find(
    ([key]) => key === 'fields' || key === 'slots',
  );
  return changed ? `${changed[0]}: ${changed[1].split(',').join(', ')}` : null;
}

function AuditPage() {
  const filters = Route.useSearch();
  const navigate = useNavigate({ from: Route.fullPath });
  const principals = usePrincipals();
  const audit = useAudit({
    limit: String(AUDIT_LIMIT),
    admin_only: 'true',
    principal_id: filters.principal_id,
    since: filters.since?.toString(),
    until: filters.until?.toString(),
  });
  const principalNameMap = usePrincipalNameMap();
  const upstreamNameMap = useUpstreamNameMap();
  const maps: NameMaps = useMemo(
    () => ({ principals: principalNameMap, upstreams: upstreamNameMap }),
    [principalNameMap, upstreamNameMap],
  );
  const { effective: locale } = useLocale();
  const { effective: timezone } = useTimezone();
  const timeFormat = useMemo(
    () =>
      new Intl.DateTimeFormat(locale, {
        month: 'short',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        timeZone: timezone,
      }),
    [locale, timezone],
  );
  const [selected, setSelected] = useState<AuditEntryLike | null>(null);
  const [refreshing, setRefreshing] = useState(false);
  const [mobileFiltersOpen, setMobileFiltersOpen] = useState(false);
  const refreshInFlightRef = useRef(false);

  const typeFilter: AuditTypeFilter = filters.type ?? 'write';
  const rangeValue =
    filters.range ??
    (filters.since == null && filters.until == null ? 'all' : 'custom');

  const loadedRows = useMemo(() => {
    const entries = (audit.data?.entries ?? []) as unknown as AuditEntryLike[];
    const identityOccurrences = new Map<string, number>();
    return entries
      .filter(
        (r) => (r.admin_action ?? null) != null || (r.kind ?? null) != null,
      )
      .map((entry): AuditRow => {
        const identity = JSON.stringify([
          entry.request_id,
          entry.ts_ms ?? null,
          entry.ts ?? null,
          entry.principal_id ?? null,
          entry.actor ?? null,
          entry.route ?? null,
          entry.upstream ?? null,
          entry.admin_action ?? null,
          entry.kind ?? null,
          entry.status,
        ]);
        const occurrence = identityOccurrences.get(identity) ?? 0;
        identityOccurrences.set(identity, occurrence + 1);
        return {
          entry,
          key: `${identity}:${occurrence}`,
          parsed: parseAuditAction(entry),
        };
      })
      .sort(
        (a, b) =>
          (eventTime(b.entry)?.getTime() ?? 0) -
          (eventTime(a.entry)?.getTime() ?? 0),
      );
  }, [audit.data]);

  const visibleRows = useMemo(
    () =>
      typeFilter === 'all'
        ? loadedRows
        : loadedRows.filter(
            (row) => auditCategory(row.parsed.name) === typeFilter,
          ),
    [loadedRows, typeFilter],
  );

  const serverFilterCount =
    Number(Boolean(filters.principal_id)) +
    Number(filters.since != null || filters.until != null);
  const hasNonDefaultFilters = serverFilterCount > 0 || typeFilter !== 'write';

  const setType = (value: AuditTypeFilter) => {
    navigate({
      search: (prev) => ({
        ...prev,
        type: value === 'write' ? undefined : value,
      }),
    });
  };

  const setRange = (value: RangePreset | 'all') => {
    navigate({
      search: (prev) =>
        value === 'all'
          ? { ...prev, since: undefined, until: undefined, range: undefined }
          : {
              ...prev,
              since: Math.floor(Date.now() / 1000) - TIME_PRESET_SECONDS[value],
              until: undefined,
              range: value,
            },
    });
  };

  const refreshAudit = async () => {
    if (refreshInFlightRef.current) return;

    refreshInFlightRef.current = true;
    setRefreshing(true);
    try {
      await audit.refetch();
    } finally {
      refreshInFlightRef.current = false;
      setRefreshing(false);
    }
  };

  const [singular, plural] = AUDIT_TYPE_NOUN[typeFilter];
  const reachedLimit = (audit.data?.entries.length ?? 0) >= AUDIT_LIMIT;
  const summary = audit.isLoading ? null : (
    <>
      Showing{' '}
      <span className="font-medium text-text">{visibleRows.length}</span>{' '}
      {visibleRows.length === 1 ? singular : plural}
      {typeFilter === 'all'
        ? null
        : ` of ${loadedRows.length} loaded ${loadedRows.length === 1 ? 'entry' : 'entries'}`}
      {reachedLimit ? ` (the newest ${AUDIT_LIMIT} for these filters)` : null}
    </>
  );

  const formatTime = (entry: AuditEntryLike) => {
    const date = eventTime(entry);
    return date ? timeFormat.format(date) : '—';
  };

  const clearButton = hasNonDefaultFilters ? (
    <Button
      size="sm"
      variant="ghost"
      iconLeft={<X className="w-3 h-3" aria-hidden="true" />}
      onClick={() => navigate({ search: {} })}
    >
      Reset filters
    </Button>
  ) : null;

  let emptyState: ReactNode = null;
  if (audit.isError) {
    emptyState = (
      <EmptyState
        title="Couldn't load the audit trail"
        description={
          audit.error instanceof Error ? audit.error.message : undefined
        }
        action={
          <Button size="sm" onClick={refreshAudit}>
            Retry
          </Button>
        }
      />
    );
  } else if (visibleRows.length === 0 && loadedRows.length > 0) {
    emptyState = (
      <EmptyState
        title={`No ${plural} in the loaded entries`}
        description={`${loadedRows.length} ${loadedRows.length === 1 ? 'entry matches' : 'entries match'} the principal and time filters, but none are ${plural}.`}
        action={
          <Button size="sm" variant="primary" onClick={() => setType('all')}>
            Show all entries
          </Button>
        }
      />
    );
  } else if (visibleRows.length === 0) {
    emptyState = (
      <EmptyState
        title="No audit entries"
        description={
          serverFilterCount
            ? 'Nothing matches the principal and time filters.'
            : 'No admin actions have been recorded yet.'
        }
        action={clearButton}
      />
    );
  }

  const principalOptions = (principals.data?.principals ?? []).map((p) => ({
    value: p.id,
    label: p.name,
  }));

  return (
    <FullPage>
      <PageHeader
        title="Audit trail"
        description="Admin API actions, newest first. Open an entry for its recorded details."
        actions={
          <Button
            data-testid="audit-refresh"
            iconLeft={<RefreshCw aria-hidden="true" />}
            loading={refreshing}
            onClick={refreshAudit}
          >
            {refreshing ? 'Refreshing...' : 'Refresh'}
          </Button>
        }
      />

      {/* Filters sit on the ground; only the entries keep a flat surface. */}
      <div className="flex shrink-0 flex-col gap-3">
        <div className="flex flex-wrap items-end gap-x-6 gap-y-3">
          <div className="flex flex-col gap-1.5">
            <span className={GROUP_LABEL_CLASS} aria-hidden="true">
              Action type
            </span>
            <SegmentedControl
              ariaLabel="Action type"
              value={typeFilter}
              onChange={setType}
              options={TYPE_OPTIONS}
            />
          </div>
          <div className="flex flex-col gap-1.5">
            <span className={GROUP_LABEL_CLASS} aria-hidden="true">
              Time range
            </span>
            <SegmentedControl
              ariaLabel="Time range"
              value={rangeValue}
              onChange={(value) => {
                if (value !== 'custom') setRange(value);
              }}
              options={RANGE_OPTIONS}
            />
          </div>
          <Button
            className="md:hidden"
            aria-expanded={mobileFiltersOpen}
            aria-controls="audit-more-filters"
            iconLeft={<SlidersHorizontal aria-hidden="true" />}
            onClick={() => setMobileFiltersOpen((open) => !open)}
          >
            {serverFilterCount
              ? `More filters (${serverFilterCount})`
              : 'More filters'}
          </Button>
          <div
            id="audit-more-filters"
            className={cx(
              'w-full flex-wrap items-end gap-3 md:flex md:w-auto',
              mobileFiltersOpen ? 'flex' : 'hidden',
            )}
          >
            <div className="w-full min-w-0 sm:w-48">
              <Field label="Principal">
                <Select
                  size="sm"
                  value={filters.principal_id ?? ''}
                  options={principalOptions}
                  onChange={(value) =>
                    navigate({
                      search: (prev) => ({
                        ...prev,
                        principal_id: value || undefined,
                      }),
                    })
                  }
                  allLabel="All principals"
                  className="w-full"
                />
              </Field>
            </div>
            <TimeRangeBounds
              since={filters.since}
              until={filters.until}
              onCommit={({ since, until }) => {
                navigate({
                  search: (prev) => ({
                    ...prev,
                    since,
                    until,
                    range: undefined,
                  }),
                });
              }}
            />
          </div>
        </div>
        <div className="flex min-h-7 flex-wrap items-center justify-between gap-2">
          <p
            className="text-body text-text-muted"
            aria-live="polite"
            data-testid="audit-summary"
          >
            {summary ?? (
              <span className="skeleton inline-block h-3 w-48 align-middle" />
            )}
          </p>
          {clearButton}
        </div>
      </div>

      {/* The card is as tall as its rows; past the viewport the list scrolls. */}
      <Card className="flex min-h-0 flex-col">
        {/* Desktop table */}
        <div className="@container hidden min-h-0 overflow-auto md:block">
          <Table className="table-fixed min-w-[46rem]">
            <TableHead>
              <tr>
                <TableHeadCell className={AUDIT_COLUMN_CLASS_NAMES[0]}>
                  Time
                </TableHeadCell>
                <TableHeadCell className={AUDIT_COLUMN_CLASS_NAMES[1]}>
                  Action
                </TableHeadCell>
                <TableHeadCell className={AUDIT_COLUMN_CLASS_NAMES[2]}>
                  Target
                </TableHeadCell>
                <TableHeadCell className={AUDIT_COLUMN_CLASS_NAMES[3]}>
                  Details
                </TableHeadCell>
                <TableHeadCell className={AUDIT_COLUMN_CLASS_NAMES[4]}>
                  Actor
                </TableHeadCell>
                <TableHeadCell
                  className={cx(
                    AUDIT_COLUMN_CLASS_NAMES[5],
                    'text-right @4xl:text-left',
                  )}
                >
                  <span className="@4xl:hidden">Status</span>
                  <span className="hidden @4xl:inline">Request</span>
                </TableHeadCell>
                <TableHeadCell className={AUDIT_COLUMN_CLASS_NAMES[6]}>
                  <span className="sr-only">Open</span>
                </TableHeadCell>
              </tr>
            </TableHead>
            <tbody>
              {audit.isLoading ? (
                Array.from({ length: 5 }).map((_, i) => (
                  <SkeletonRow
                    key={i}
                    className="h-10"
                    cols={AUDIT_COLUMN_CLASS_NAMES.length}
                    cellClassNames={AUDIT_SKELETON_CELL_CLASS_NAMES}
                    skeletonClassNames={AUDIT_SKELETON_CLASS_NAMES}
                  />
                ))
              ) : emptyState ? (
                <tr>
                  <td colSpan={AUDIT_COLUMN_CLASS_NAMES.length}>
                    {emptyState}
                  </td>
                </tr>
              ) : (
                visibleRows.map(({ entry, key, parsed }) => {
                  const target = auditTarget(entry, parsed, maps);
                  const changed = changedFieldsSummary(parsed);
                  const details = auditDetailParts(entry, parsed, maps, target);
                  const category = auditCategory(parsed.name);
                  const actor = auditActorLabel(entry);
                  const actorKind = entry.actor_kind?.replaceAll('_', ' ');
                  const time = formatTime(entry);
                  const action = humanizeAuditAction(parsed.name);
                  return (
                    <TableRow
                      key={key}
                      interactive
                      className="has-[:focus-visible]:bg-overlay-3"
                      onClick={() => setSelected(entry)}
                    >
                      <TableCell className="whitespace-nowrap pr-4 text-text-muted tabular-nums">
                        <RelativeTime compact ts={eventTime(entry)} />
                      </TableCell>
                      <TableCell>
                        <div className="flex min-w-0 items-baseline gap-2">
                          <span className="truncate text-text">{action}</span>
                          {typeFilter === 'all' && category !== 'write' ? (
                            <span className="shrink-0 text-caption text-text-faint">
                              {AUDIT_CATEGORY_LABEL[category]}
                            </span>
                          ) : null}
                        </div>
                        <div className="flex min-w-0 items-baseline gap-1.5 text-caption text-text-faint">
                          <span
                            className="min-w-0 shrink-0 truncate font-mono text-data @4xl:shrink"
                            title={
                              entry.admin_action ?? entry.kind ?? undefined
                            }
                          >
                            {parsed.name}
                          </span>
                          {/* From 56rem the Details column carries this. */}
                          {changed ? (
                            <span
                              className="min-w-0 line-clamp-2 break-words @4xl:hidden"
                              title={changed}
                            >
                              · {changed}
                            </span>
                          ) : null}
                        </div>
                      </TableCell>
                      <TableCell>
                        {target ? (
                          <div
                            className="truncate"
                            title={target.id ?? target.name}
                          >
                            <span className="text-text-faint">
                              {target.kind}{' '}
                            </span>
                            <TargetName target={target} maps={maps} />
                          </div>
                        ) : (
                          <EmptyValue label="No target" />
                        )}
                      </TableCell>
                      <TableCell className={AUDIT_COLUMN_CLASS_NAMES[3]}>
                        {details.length > 0 ? (
                          <p
                            className="line-clamp-2 break-words text-caption text-text-muted"
                            title={details
                              .map(({ label, value }) => `${label}: ${value}`)
                              .join(' · ')}
                          >
                            {details.map(({ label, value }, index) => (
                              <span key={`${label}:${value}`}>
                                {index > 0 ? (
                                  <span className="text-text-faint"> · </span>
                                ) : null}
                                <span className="text-text-faint">
                                  {label}:
                                </span>{' '}
                                {value}
                              </span>
                            ))}
                          </p>
                        ) : (
                          <EmptyValue label="No recorded details" />
                        )}
                      </TableCell>
                      <TableCell>
                        <div
                          className="truncate text-text-muted"
                          title={actorKind ? `${actor} (${actorKind})` : actor}
                        >
                          {actor}
                        </div>
                        {actorKind ? (
                          <div className="hidden truncate text-caption text-text-faint @7xl:block">
                            {actorKind}
                          </div>
                        ) : null}
                      </TableCell>
                      <TableCell>
                        <div className="flex min-w-0 items-baseline justify-end gap-2 @4xl:justify-start">
                          <span
                            className={cx(
                              'shrink-0 tabular-nums',
                              statusTextClass(entry.status),
                            )}
                          >
                            {entry.status}
                          </span>
                          {/* Every admin route shares `/admin/v1`; the title
                          keeps the full path. */}
                          {entry.route ? (
                            <span
                              className="hidden min-w-0 truncate font-mono text-data text-text-muted @4xl:inline"
                              title={entry.route}
                            >
                              {readableRoute(entry.route, maps).replace(
                                /^\/admin\/v1(?=\/)/,
                                '',
                              )}
                            </span>
                          ) : null}
                        </div>
                      </TableCell>
                      <TableCell className="py-1 text-right">
                        <IconButton
                          label={`Open ${action} entry from ${time}`}
                          onClick={(event) => {
                            event.stopPropagation();
                            setSelected(entry);
                          }}
                        >
                          <ChevronRight aria-hidden="true" />
                        </IconButton>
                      </TableCell>
                    </TableRow>
                  );
                })
              )}
            </tbody>
          </Table>
        </div>

        {/* Mobile cards */}
        <div className="min-h-0 overflow-auto md:hidden">
          {audit.isLoading ? (
            <div className="flex flex-col gap-2 p-4" aria-hidden="true">
              {Array.from({ length: 4 }).map((_, i) => (
                <div key={i} className="skeleton h-16 rounded-xs" />
              ))}
            </div>
          ) : emptyState ? (
            emptyState
          ) : (
            <ul className="divide-y divide-row">
              {visibleRows.map(({ entry, key, parsed }) => {
                const target = auditTarget(entry, parsed, maps);
                const changed = changedFieldsSummary(parsed);
                return (
                  <li key={key}>
                    <button
                      type="button"
                      className="flex w-full flex-col gap-0.5 px-4 py-3 text-left text-body hover:bg-overlay-2 focus-visible:outline-2 focus-visible:-outline-offset-2 focus-visible:outline-accent"
                      onClick={() => setSelected(entry)}
                    >
                      <span className="flex w-full items-baseline justify-between gap-3">
                        <span className="truncate text-text">
                          {humanizeAuditAction(parsed.name)}
                        </span>
                        <span
                          className={cx(
                            'shrink-0 tabular-nums',
                            statusTextClass(entry.status),
                          )}
                        >
                          {entry.status}
                        </span>
                      </span>
                      <span className="flex min-w-0 items-baseline gap-1.5 text-caption text-text-faint">
                        <span className="shrink-0 font-mono text-data">
                          {parsed.name}
                        </span>
                        {changed ? (
                          <span
                            className="min-w-0 line-clamp-2 break-words"
                            title={changed}
                          >
                            · {changed}
                          </span>
                        ) : null}
                      </span>
                      {target ? (
                        <span className="truncate text-text-muted">
                          <span className="text-text-faint">
                            {target.kind}{' '}
                          </span>
                          {target.name}
                        </span>
                      ) : null}
                      <span className="flex w-full justify-between gap-3 text-caption text-text-faint">
                        <span className="truncate">
                          {auditActorLabel(entry)}
                        </span>
                        <span className="shrink-0">{formatTime(entry)}</span>
                      </span>
                    </button>
                  </li>
                );
              })}
            </ul>
          )}
        </div>
      </Card>

      <AuditEntryDrawer
        entry={selected}
        maps={maps}
        onClose={() => setSelected(null)}
      />
    </FullPage>
  );
}
