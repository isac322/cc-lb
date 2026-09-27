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
import { cacheHitPercent, TokenCell } from './TokenCell';

export { SessionChip };

const DASH = '—';
/** Matches `TableRow`'s 40px row so loading, empty and loaded bodies agree. */
const REQUEST_EVENT_ROW_HEIGHT_REM = 2.5;
const REQUEST_EVENT_ROW_STYLE = {
  height: `${REQUEST_EVENT_ROW_HEIGHT_REM}rem`,
} satisfies React.CSSProperties;

const STATUS_TONE_TEXT: Record<'ok' | 'warn' | 'danger' | 'neutral', string> = {
  ok: 'text-success-text',
  warn: 'text-warn-text',
  danger: 'text-danger-text',
  neutral: 'text-text',
};

/*
 * Below `md` each row becomes a three-line card instead of a 1000px-wide
 * table row: time, kind and status; principal, upstream and model; latency,
 * tokens and cost. Session and cache hit are left to the drawer. Cells keep
 * their DOM order and are only placed on the grid.
 */
const MOBILE_ROW =
  'max-md:grid max-md:h-auto! max-md:grid-cols-[auto_minmax(0,1fr)_auto] max-md:items-center max-md:gap-x-3 max-md:gap-y-1 max-md:px-4 max-md:py-2.5';
// `p-0!` beats the table's first/last-cell inset, which only applies to rows.
const MOBILE_CELL = 'max-md:p-0! max-md:*:p-0 max-md:max-w-none';
const MOBILE_AT = {
  time: 'max-md:row-start-1 max-md:col-start-1',
  kind: 'max-md:row-start-1 max-md:col-start-2 max-md:justify-self-start',
  status: 'max-md:row-start-1 max-md:col-start-3',
  principal: 'max-md:row-start-2 max-md:col-start-1 max-md:max-w-[40vw]',
  upstream: 'max-md:row-start-2 max-md:col-start-1 max-md:col-span-2',
  /** Reads "principal → upstream" when both cells are shown. */
  upstreamAfterPrincipal:
    'max-md:row-start-2 max-md:col-start-2 max-md:before:content-["→_"] max-md:before:text-text-faint',
  model:
    'max-md:row-start-2 max-md:col-start-3 max-md:max-w-[40vw] max-md:text-right',
  latency: 'max-md:row-start-3 max-md:col-start-1',
  tokens: 'max-md:row-start-3 max-md:col-start-2 max-md:justify-self-start',
  cost: 'max-md:row-start-3 max-md:col-start-3',
} as const;

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
  showCacheHit: boolean;
  showCost: boolean;
  selected: boolean;
  flash: boolean;
  selectEvent: React.Dispatch<React.SetStateAction<string | null>>;
  onAnchorRange?: (tsMs: number, radiusSecs: number) => void;
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
  showCacheHit,
  showCost,
  selected,
  flash,
  selectEvent,
  onAnchorRange,
}: RequestEventRowProps) {
  const isPartial = event._phase === 'partial';
  const outcome = getRequestOutcome(
    isPartial,
    event._phase === 'final' ? event.status : 0,
    event._phase === 'final' ? event.error_code : undefined,
    event._phase === 'final' ? event.upstream_error_type : undefined,
  );
  const tierBadge = serviceTierBadgeText(event.service_tier);
  const requestKindBadge = requestKindBadgeText(event.request_kind);
  const hit = showCacheHit ? cacheHitPercent(event) : null;
  const at = eventTime(event);

  return (
    <TableRow
      interactive
      selected={selected}
      className={cx(
        'focus:bg-overlay-2 focus-visible:outline-2 focus-visible:-outline-offset-2 focus-visible:outline-accent',
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
        <span
          className={cx(
            'status-dot mr-2',
            outcome.type === 'partial'
              ? 'neutral animate-pulse'
              : requestOutcomeTone(outcome),
          )}
        />
        <RelativeTime compact ts={at} />
        {onAnchorRange != null && at != null ? (
          <span className="absolute inset-y-0 right-0 flex items-center gap-0.5 bg-bg-sub pl-2 pr-1 opacity-0 transition-opacity group-hover/ts:opacity-100 group-focus-within/ts:opacity-100 max-md:hidden">
            {ANCHOR_RADII_SECS.map(([label, radius]) => (
              <button
                key={label}
                type="button"
                className="rounded-sm bg-overlay-3 px-1 text-caption text-text-muted hover:bg-overlay-5 hover:text-text focus-visible:outline-2 focus-visible:outline-accent"
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
          )}
        >
          {upstreamName}
        </td>
      )}
      {showSession && (
        <td className="px-3 py-2 whitespace-nowrap max-md:hidden">
          <SessionChip sessionId={event.thread_id ?? null} />
        </td>
      )}
      {showKind && (
        <td
          className={cx(
            'px-3 py-2 whitespace-nowrap',
            MOBILE_CELL,
            MOBILE_AT.kind,
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
          'px-3 py-2 text-text-muted truncate max-w-[260px]',
          MOBILE_CELL,
          MOBILE_AT.model,
        )}
      >
        <span className="flex items-center gap-2 min-w-0 max-md:justify-end">
          <span className="truncate font-mono text-data">
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
            : STATUS_TONE_TEXT[requestOutcomeTone(outcome)],
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
      {showCacheHit && (
        <td className="px-3 py-2 text-right tabular-nums whitespace-nowrap max-md:hidden">
          {hit != null ? (
            <span className="text-text-muted">{hit}%</span>
          ) : (
            <span className="text-text-faint">{DASH}</span>
          )}
        </td>
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
  minWidthClass = 'min-w-[960px]',
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
  const showCacheHit =
    showTokens && events.some((e) => cacheHitPercent(e) != null);

  const columnSpecs: ColumnSpec[] = [
    { label: 'Timestamp', skeleton: 'max-w-24' },
    ...(showPrincipal ? [{ label: 'Principal', skeleton: 'max-w-24' }] : []),
    ...(showUpstream ? [{ label: 'Upstream', skeleton: 'max-w-28' }] : []),
    ...(showSession ? [{ label: 'Session', skeleton: 'max-w-24' }] : []),
    ...(showKind ? [{ label: 'Request kind', skeleton: 'max-w-12' }] : []),
    { label: 'Model', skeleton: 'max-w-40' },
    { label: 'Status', numeric: true, skeleton: 'max-w-12 ml-auto' },
    { label: 'Latency', numeric: true, skeleton: 'max-w-16 ml-auto' },
    ...(showTokens
      ? [{ label: 'Tokens', numeric: true, skeleton: 'max-w-20 ml-auto' }]
      : []),
    ...(showCacheHit
      ? [{ label: 'Cache hit', numeric: true, skeleton: 'max-w-10 ml-auto' }]
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
      <Table
        className={cx(
          minWidthClass,
          // `relative` keeps absolutely positioned descendants (the
          // LatencyCell sr-only descriptions) inside the scroll wrapper;
          // without it they resolve against <body> and widen the page.
          'relative max-md:block max-md:min-w-0',
          className,
        )}
      >
        <TableHead className="max-md:hidden">
          <tr>
            {columnSpecs.map((column) => (
              <TableHeadCell key={column.label} numeric={column.numeric}>
                {column.label}
              </TableHeadCell>
            ))}
          </tr>
        </TableHead>
        <tbody style={reservedBodyStyle} className="max-md:block">
          {loading ? (
            Array.from({ length: reservedRowCount }).map((_, i) => (
              <SkeletonRow
                key={i}
                cols={colCount}
                style={REQUEST_EVENT_ROW_STYLE}
                // Below `md` the body is a list of cards, so the placeholder
                // is one line of the first three cells.
                className="max-md:flex max-md:items-center max-md:px-1"
                cellClassNames={columnSpecs.map((column, index) =>
                  cx(
                    column.numeric ? 'text-right tabular-nums' : '',
                    index < 3 ? 'max-md:flex-1' : 'max-md:hidden',
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
                  showCacheHit={showCacheHit}
                  showCost={showCost}
                  selected={selectedId === eventKey}
                  flash={liveFlashIds?.has(eventKey) ?? false}
                  selectEvent={setSelectedId}
                  onAnchorRange={onAnchorRange}
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
