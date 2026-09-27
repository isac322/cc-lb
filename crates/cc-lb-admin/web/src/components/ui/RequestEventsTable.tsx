import type React from 'react';
import { memo, useState } from 'react';
import { eventTime } from '../../lib/api';
import { getRequestOutcome, requestOutcomeTone } from '../../lib/format';
import type { RequestEventWithPhase } from '../../lib/RequestEventTypes';
import { serviceTierBadgeText } from '../../lib/reasoningTier';
import { requestKindBadgeText } from '../../lib/requestKind';
import { CostCell } from './CostCell';
import { LatencyCell } from './latency/LatencyCell';
import { Badge, cx, EmptyState, SkeletonRow } from './primitives';
import { RelativeTime } from './RelativeTime';
import { RequestEventDrawer } from './RequestEventDrawer';
import { RequestOutcomeTableCell } from './RequestEventIdentity';
import { SessionChip } from './SessionChip';
import { Table, TableHead, TableHeadCell, TableRow } from './Table';
import { TokenCell } from './TokenCell';

export { SessionChip };

const DASH = '—';
/** Matches `TableRow`'s 40px row so loading, empty and loaded bodies agree. */
const REQUEST_EVENT_ROW_HEIGHT_REM = 2.5;
const REQUEST_EVENT_ROW_STYLE = {
  height: `${REQUEST_EVENT_ROW_HEIGHT_REM}rem`,
} satisfies React.CSSProperties;

/** Healthy by omission: a 2xx reads in muted ink; only exceptions carry tone. */
const STATUS_TONE_TEXT: Record<'ok' | 'warn' | 'danger' | 'neutral', string> = {
  ok: 'text-text-muted',
  warn: 'text-warn-text',
  danger: 'text-danger-text',
  neutral: 'text-text',
};

/*
 * Layout follows the table's own wrapper (`@container/events`), not the
 * viewport, so the same table fits a full page and a narrow detail pane.
 *
 * Under 36rem each row becomes a three-line card instead of a wide table
 * row: time, kind and status; principal, upstream and model; latency,
 * tokens and cost, each with its composition bar. Session is left to the
 * drawer. Cells keep their DOM order and are only placed on the grid.
 */
const MOBILE_ROW =
  '@max-xl/events:grid @max-xl/events:h-auto! @max-xl/events:grid-cols-[auto_minmax(0,1fr)_auto] @max-xl/events:items-center @max-xl/events:gap-x-3 @max-xl/events:gap-y-1 @max-xl/events:px-4 @max-xl/events:py-2.5';
// `p-0!` beats the table's first/last-cell inset, which only applies to rows.
const MOBILE_CELL =
  '@max-xl/events:p-0! @max-xl/events:*:p-0 @max-xl/events:max-w-none';
const MOBILE_AT = {
  time: '@max-xl/events:row-start-1 @max-xl/events:col-start-1',
  kind: '@max-xl/events:row-start-1 @max-xl/events:col-start-2 @max-xl/events:justify-self-start',
  status: '@max-xl/events:row-start-1 @max-xl/events:col-start-3',
  principal:
    '@max-xl/events:row-start-2 @max-xl/events:col-start-1 @max-xl/events:max-w-[40cqw]',
  upstream:
    '@max-xl/events:row-start-2 @max-xl/events:col-start-1 @max-xl/events:col-span-2',
  /** Reads "principal → upstream" when both cells are shown. */
  upstreamAfterPrincipal:
    '@max-xl/events:row-start-2 @max-xl/events:col-start-2 @max-xl/events:before:content-["→_"] @max-xl/events:before:text-text-faint',
  model:
    '@max-xl/events:row-start-2 @max-xl/events:col-start-3 @max-xl/events:max-w-[40cqw] @max-xl/events:text-right',
  latency:
    '@max-xl/events:row-start-3 @max-xl/events:col-start-1 @max-xl/events:text-left',
  tokens:
    '@max-xl/events:row-start-3 @max-xl/events:col-start-2 @max-xl/events:justify-self-start @max-xl/events:text-left',
  cost: '@max-xl/events:row-start-3 @max-xl/events:col-start-3',
} as const;

/** Per-column classes for the columns that give way when space runs out. */
interface ColumnFit {
  /** Principal and upstream. */
  entity: string;
  session: string;
  kind: string;
  model: string;
}

/*
 * Without a `minWidthClass` floor the table fits its container: between the
 * card layout and the full width, session drops first, then request kind,
 * then principal and upstream, and model absorbs whatever is left, truncated
 * (the full name stays in its title). Time, status, latency, tokens and cost
 * never drop.
 */
const FIT_COLUMNS: ColumnFit = {
  entity: '@xl/events:@max-[44rem]/events:hidden',
  session: '@max-[64rem]/events:hidden',
  kind: '@xl/events:@max-[52rem]/events:hidden',
  model: '@xl/events:w-full @xl/events:max-w-0',
};
/** With a floor the table keeps every column and its wrapper scrolls. */
const SCROLL_COLUMNS: ColumnFit = {
  entity: '',
  session: '',
  kind: '',
  model: 'max-w-[260px]',
};

/** Radii offered by the per-row time anchor, as [label, seconds either side]. */
const ANCHOR_RADII_SECS: ReadonlyArray<readonly [string, number]> = [
  ['±1m', 60],
  ['±5m', 300],
  ['±30m', 1800],
];

interface RequestEventsTableProps {
  events: readonly RequestEventWithPhase[];
  principalNameMap: Map<string, string>;
  upstreamNameMap: Map<string, string>;
  loading?: boolean;
  reservedRowCount?: number;
  emptyTitle?: string;
  emptyDescription?: string;
  /** Next step shown under the empty message, e.g. a link to the setup flow. */
  emptyAction?: React.ReactNode;
  /** Heading level of the empty-state title; match the surrounding outline. */
  emptyHeadingLevel?: 2 | 3 | 4;
  liveFlashIds?: Set<string>;
  columns?: {
    principal?: boolean;
    upstream?: boolean;
    session?: boolean;
    cost?: boolean;
    tokens?: boolean;
  };
  sentinelRef?: React.RefObject<HTMLTableRowElement | null>;
  loadingMore?: boolean;
  hasMore?: boolean;
  /**
   * Minimum table width for a full-page table that keeps every column and
   * scrolls horizontally. Omit it to fit the container instead, dropping the
   * least important columns as it narrows.
   */
  minWidthClass?: string;
  className?: string;
  /** Focus the surrounding time range on a row, in seconds either side. */
  onAnchorRange?: (tsMs: number, radiusSecs: number) => void;
}

interface RequestEventRowProps {
  event: RequestEventWithPhase;
  eventKey: string;
  principalName: string;
  upstreamName: string;
  showPrincipal: boolean;
  showUpstream: boolean;
  showSession: boolean;
  showKind: boolean;
  showTokens: boolean;
  showCost: boolean;
  selected: boolean;
  flash: boolean;
  selectEvent: React.Dispatch<React.SetStateAction<string | null>>;
  onAnchorRange?: (tsMs: number, radiusSecs: number) => void;
  fit: ColumnFit;
}

const RequestEventRow = memo(function RequestEventRow({
  event,
  eventKey,
  principalName,
  upstreamName,
  showPrincipal,
  showUpstream,
  showSession,
  showKind,
  showTokens,
  showCost,
  selected,
  flash,
  selectEvent,
  onAnchorRange,
  fit,
}: RequestEventRowProps) {
  const isPartial = event._phase === 'partial';
  const outcome = getRequestOutcome(
    isPartial,
    event._phase === 'final' ? event.status : 0,
    event._phase === 'final' ? event.error_code : undefined,
    event._phase === 'final' ? event.upstream_error_type : undefined,
  );
  const tone = requestOutcomeTone(outcome);
  const tierBadge = serviceTierBadgeText(event.service_tier);
  const requestKindBadge = requestKindBadgeText(event.request_kind);
  const at = eventTime(event);

  return (
    <TableRow
      interactive
      selected={selected}
      className={cx(
        'focus-visible:outline-2 focus-visible:-outline-offset-2 focus-visible:outline-accent',
        flash ? 'flash-in' : '',
        MOBILE_ROW,
      )}
      style={REQUEST_EVENT_ROW_STYLE}
      onClick={() => selectEvent(eventKey)}
      tabIndex={0}
      onKeyDown={(keyboardEvent) => {
        // Keys pressed on a control inside the row (latency breakdown, time
        // anchors) belong to that control, not to the row.
        if (keyboardEvent.target !== keyboardEvent.currentTarget) return;
        if (keyboardEvent.key === 'Enter' || keyboardEvent.key === ' ') {
          keyboardEvent.preventDefault();
          selectEvent(eventKey);
        }
      }}
      aria-selected={selected}
      aria-label={`View request ${eventKey}`}
    >
      <td
        className={cx(
          'group/ts relative px-3 py-2 text-text-muted tabular-nums whitespace-nowrap',
          MOBILE_CELL,
          MOBILE_AT.time,
        )}
      >
        {/* Errors and in-flight rows get a dot in the cell's leading inset, so
        the time text stays aligned with its header whether or not a dot shows.
        Successful rows carry no dot at all. */}
        {tone === 'ok' ? null : (
          <span
            className={cx(
              'status-dot absolute top-1/2 left-1.5 -translate-y-1/2 @max-xl/events:-left-3',
              outcome.type === 'partial' ? 'neutral animate-pulse' : tone,
            )}
          />
        )}
        <RelativeTime compact ts={at} />
        {onAnchorRange != null && at != null ? (
          <span className="absolute inset-y-0 right-0 flex items-center gap-0.5 bg-bg-sub pl-2 pr-1 opacity-0 transition-opacity group-hover/ts:opacity-100 group-focus-within/ts:opacity-100 @max-xl/events:hidden">
            {ANCHOR_RADII_SECS.map(([label, radius]) => (
              <button
                key={label}
                type="button"
                className="rounded-sm px-1 text-caption text-text-muted transition-colors hover:bg-overlay-5 hover:text-text focus-visible:outline-2 focus-visible:outline-offset-1 focus-visible:outline-accent"
                aria-label={`Show ${label} around this request`}
                title={`Show ${label} around this request`}
                onClick={(clickEvent) => {
                  clickEvent.stopPropagation();
                  onAnchorRange(at.getTime(), radius);
                }}
              >
                {label}
              </button>
            ))}
          </span>
        ) : null}
      </td>
      {showPrincipal && (
        <td
          className={cx(
            'px-3 py-2 whitespace-nowrap truncate max-w-[160px]',
            MOBILE_CELL,
            MOBILE_AT.principal,
            fit.entity,
          )}
        >
          {principalName}
        </td>
      )}
      {showUpstream && (
        <td
          className={cx(
            'px-3 py-2 whitespace-nowrap truncate max-w-[180px]',
            MOBILE_CELL,
            showPrincipal
              ? MOBILE_AT.upstreamAfterPrincipal
              : MOBILE_AT.upstream,
            fit.entity,
          )}
        >
          {upstreamName}
        </td>
      )}
      {showSession && (
        <td
          className={cx(
            'px-3 py-2 whitespace-nowrap @max-xl/events:hidden',
            fit.session,
          )}
        >
          <SessionChip sessionId={event.thread_id ?? null} />
        </td>
      )}
      {showKind && (
        <td
          className={cx(
            'px-3 py-2 whitespace-nowrap',
            MOBILE_CELL,
            MOBILE_AT.kind,
            fit.kind,
          )}
        >
          {requestKindBadge != null ? (
            <Badge className="shrink-0">{requestKindBadge}</Badge>
          ) : (
            <span className="text-text-faint">{DASH}</span>
          )}
        </td>
      )}
      <td
        className={cx(
          'px-3 py-2 text-text-muted truncate',
          MOBILE_CELL,
          MOBILE_AT.model,
          fit.model,
        )}
      >
        <span className="flex items-center gap-2 min-w-0 @max-xl/events:justify-end">
          <span
            className="truncate font-mono text-data"
            title={event.model ?? undefined}
          >
            {event.model ?? DASH}
          </span>
          {tierBadge != null && (
            <Badge tone="neutral" className="shrink-0">
              {tierBadge}
            </Badge>
          )}
        </span>
      </td>
      <td
        className={cx(
          'px-3 py-2 text-right tabular-nums whitespace-nowrap',
          MOBILE_CELL,
          MOBILE_AT.status,
          outcome.type === 'partial'
            ? 'text-text-faint'
            : STATUS_TONE_TEXT[tone],
        )}
      >
        <RequestOutcomeTableCell outcome={outcome} />
      </td>
      <LatencyCell
        event={event}
        className={cx(MOBILE_CELL, MOBILE_AT.latency)}
      />
      {showTokens && (
        <TokenCell
          event={event}
          isPartial={isPartial}
          className={cx(MOBILE_CELL, MOBILE_AT.tokens)}
        />
      )}
      {showCost && (
        <CostCell
          event={event}
          isPartial={isPartial}
          className={cx(MOBILE_CELL, MOBILE_AT.cost)}
        />
      )}
    </TableRow>
  );
});

interface ColumnSpec {
  label: string;
  numeric?: boolean;
  skeleton: string;
  /** Visibility and width classes shared by the header and every cell. */
  className?: string;
}

export const RequestEventsTable = memo(function RequestEventsTable({
  events,
  principalNameMap,
  upstreamNameMap,
  loading,
  reservedRowCount = 5,
  emptyTitle = 'No requests',
  emptyDescription,
  emptyAction,
  emptyHeadingLevel,
  liveFlashIds,
  columns,
  sentinelRef,
  loadingMore,
  hasMore,
  minWidthClass,
  className,
  onAnchorRange,
}: RequestEventsTableProps) {
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const selected = selectedId
    ? (events.find((e) => (e.event_id ?? e.request_id) === selectedId) ?? null)
    : null;
  const showPrincipal = columns?.principal ?? true;
  const showUpstream = columns?.upstream ?? true;
  const showTokens = columns?.tokens ?? true;
  const showCost = columns?.cost ?? true;
  // Optional columns earn their width only when a visible row has a value;
  // a column of dashes is noise.
  const showSession =
    (columns?.session ?? true) && events.some((e) => e.thread_id);
  const showKind = events.some((e) => requestKindBadgeText(e.request_kind));
  const fit = minWidthClass == null ? FIT_COLUMNS : SCROLL_COLUMNS;

  const columnSpecs: ColumnSpec[] = [
    { label: 'Timestamp', skeleton: 'max-w-24' },
    ...(showPrincipal
      ? [{ label: 'Principal', skeleton: 'max-w-24', className: fit.entity }]
      : []),
    ...(showUpstream
      ? [{ label: 'Upstream', skeleton: 'max-w-28', className: fit.entity }]
      : []),
    ...(showSession
      ? [{ label: 'Session', skeleton: 'max-w-24', className: fit.session }]
      : []),
    ...(showKind
      ? [{ label: 'Request kind', skeleton: 'max-w-12', className: fit.kind }]
      : []),
    { label: 'Model', skeleton: 'max-w-40', className: fit.model },
    { label: 'Status', numeric: true, skeleton: 'max-w-12 ml-auto' },
    { label: 'Latency', numeric: true, skeleton: 'max-w-16 ml-auto' },
    ...(showTokens
      ? [{ label: 'Tokens', numeric: true, skeleton: 'max-w-20 ml-auto' }]
      : []),
    ...(showCost
      ? [{ label: 'Cost', numeric: true, skeleton: 'max-w-16 ml-auto' }]
      : []),
  ];
  const colCount = columnSpecs.length;
  const reservedBodyStyle =
    loading || events.length === 0
      ? { height: `${reservedRowCount * REQUEST_EVENT_ROW_HEIGHT_REM}rem` }
      : undefined;

  return (
    <>
      <div className="@container/events">
        <Table
          className={cx(
            minWidthClass,
            // `relative` keeps absolutely positioned descendants (the
            // LatencyCell sr-only descriptions) inside the scroll wrapper;
            // without it they resolve against <body> and widen the page.
            'relative @max-xl/events:block @max-xl/events:min-w-0',
            className,
          )}
        >
          <TableHead className="@max-xl/events:hidden">
            <tr>
              {columnSpecs.map((column) => (
                <TableHeadCell
                  key={column.label}
                  numeric={column.numeric}
                  className={column.className}
                >
                  {column.label}
                </TableHeadCell>
              ))}
            </tr>
          </TableHead>
          <tbody style={reservedBodyStyle} className="@max-xl/events:block">
            {loading ? (
              Array.from({ length: reservedRowCount }).map((_, i) => (
                <SkeletonRow
                  key={i}
                  cols={colCount}
                  style={REQUEST_EVENT_ROW_STYLE}
                  // In the card layout the body is a list of cards, so the
                  // placeholder is one line of the first three cells.
                  className="@max-xl/events:flex @max-xl/events:items-center @max-xl/events:px-1"
                  cellClassNames={columnSpecs.map((column, index) =>
                    cx(
                      column.numeric ? 'text-right tabular-nums' : '',
                      column.className,
                      index < 3
                        ? '@max-xl/events:flex-1'
                        : '@max-xl/events:hidden',
                    ),
                  )}
                  skeletonClassNames={columnSpecs.map(
                    (column) => column.skeleton,
                  )}
                />
              ))
            ) : events.length ? (
              events.map((event) => {
                const eventKey = event.event_id ?? event.request_id;
                return (
                  <RequestEventRow
                    key={eventKey}
                    event={event}
                    eventKey={eventKey}
                    principalName={
                      (event.principal_id &&
                        principalNameMap.get(event.principal_id)) ??
                      event.principal_id ??
                      DASH
                    }
                    upstreamName={
                      upstreamNameMap.get(event.upstream ?? '') ??
                      event.upstream_name ??
                      event.upstream ??
                      DASH
                    }
                    showPrincipal={showPrincipal}
                    showUpstream={showUpstream}
                    showSession={showSession}
                    showKind={showKind}
                    showTokens={showTokens}
                    showCost={showCost}
                    selected={selectedId === eventKey}
                    flash={liveFlashIds?.has(eventKey) ?? false}
                    selectEvent={setSelectedId}
                    onAnchorRange={onAnchorRange}
                    fit={fit}
                  />
                );
              })
            ) : (
              <tr className="h-full">
                <td colSpan={colCount} className="px-4 align-middle">
                  <EmptyState
                    title={emptyTitle}
                    description={emptyDescription}
                    action={emptyAction}
                    headingLevel={emptyHeadingLevel}
                  />
                </td>
              </tr>
            )}
            {events.length > 0 && sentinelRef && (
              <tr ref={sentinelRef}>
                <td
                  colSpan={colCount}
                  className="px-3 py-4 text-center text-caption text-text-faint"
                >
                  {loadingMore ? 'Loading…' : hasMore ? '' : 'No more entries'}
                </td>
              </tr>
            )}
          </tbody>
        </Table>
      </div>
      <RequestEventDrawer
        event={selected}
        principalName={
          selected?.principal_id
            ? (principalNameMap.get(selected.principal_id) ?? null)
            : null
        }
        onClose={() => setSelectedId(null)}
      />
    </>
  );
});
