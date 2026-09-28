import type React from 'react';
import { memo, useState } from 'react';
import { eventTime } from '../../lib/api';
import { getRequestOutcome, requestOutcomeTone } from '../../lib/format';
import type { RequestEventWithPhase } from '../../lib/RequestEventTypes';
import { serviceTierBadgeText } from '../../lib/reasoningTier';
import { CostCell } from './CostCell';
import { LatencyCell } from './latency/LatencyCell';
import { Badge, cx, EmptyState, SkeletonRow } from './primitives';
import { RelativeTime } from './RelativeTime';
import { RequestEventDrawer } from './RequestEventDrawer';
import { RequestOutcomeTableCell } from './RequestEventIdentity';
import { RequestKindBadge } from './RequestKindBadge';
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
 * Under 46rem each row becomes a three-line card instead of a wide table
 * row: time, kind, session and status; principal, upstream and model;
 * latency, tokens and cost, each with its composition bar. Cells keep their
 * DOM order and are only placed on the four-track grid.
 */
const MOBILE_ROW =
  '@max-[46rem]/events:grid @max-[46rem]/events:h-auto! @max-[46rem]/events:grid-cols-[auto_auto_minmax(0,1fr)_auto] @max-[46rem]/events:items-center @max-[46rem]/events:gap-x-2 @max-[46rem]/events:gap-y-1 @max-[46rem]/events:px-4 @max-[46rem]/events:py-2.5';
// `p-0!` beats the table's first/last-cell inset, which only applies to rows.
// `*:min-w-0` drops the metric buttons' 72px floor; the time cell instead
// holds the first track at 3.5rem (fits "23h ago" and "100 ms"), so kind
// badges line up from card to card while the upstream keeps its room.
// `max-w-none` lifts the table's `max-w-0`; principal and model re-cap
// themselves with `!` so an auto track never grows past 40cqw and squeezes
// the upstream out of the flexible track.
const MOBILE_CELL =
  '@max-[46rem]/events:p-0! @max-[46rem]/events:*:p-0 @max-[46rem]/events:*:min-w-0 @max-[46rem]/events:max-w-none';
/*
 * A card's metric button is only its figure and bar (27px); padding pulled
 * back by a negative margin makes the touch target 43px without moving the
 * card's lines.
 */
const MOBILE_METRIC = '@max-[46rem]/events:*:-my-2 @max-[46rem]/events:*:py-2';
const MOBILE_AT = {
  time: '@max-[46rem]/events:row-start-1 @max-[46rem]/events:col-start-1 @max-[46rem]/events:min-w-14',
  kind: '@max-[46rem]/events:row-start-1 @max-[46rem]/events:col-start-2 @max-[46rem]/events:justify-self-start',
  // The chip's min-content is the whole id, so the cell takes its track's
  // width outright; the chip then middle-truncates inside it.
  session:
    '@max-[46rem]/events:row-start-1 @max-[46rem]/events:col-start-3 @max-[46rem]/events:w-full @max-[46rem]/events:min-w-0',
  status: '@max-[46rem]/events:row-start-1 @max-[46rem]/events:col-start-4',
  principal:
    '@max-[46rem]/events:row-start-2 @max-[46rem]/events:col-start-1 @max-[46rem]/events:col-span-2 @max-[46rem]/events:max-w-[40cqw]!',
  upstream:
    '@max-[46rem]/events:row-start-2 @max-[46rem]/events:col-start-1 @max-[46rem]/events:col-span-3',
  /** Reads "principal → upstream" when both cells are shown. */
  upstreamAfterPrincipal:
    '@max-[46rem]/events:row-start-2 @max-[46rem]/events:col-start-3 @max-[46rem]/events:before:content-["→_"] @max-[46rem]/events:before:text-text-faint',
  model:
    '@max-[46rem]/events:row-start-2 @max-[46rem]/events:col-start-4 @max-[46rem]/events:max-w-[40cqw]! @max-[46rem]/events:text-right',
  latency:
    '@max-[46rem]/events:row-start-3 @max-[46rem]/events:col-start-1 @max-[46rem]/events:text-left',
  tokens:
    '@max-[46rem]/events:row-start-3 @max-[46rem]/events:col-start-2 @max-[46rem]/events:col-span-2 @max-[46rem]/events:justify-self-start @max-[46rem]/events:text-left',
  // The inset keeps the cost bar from reading as the end of the tokens bar.
  cost: '@max-[46rem]/events:row-start-3 @max-[46rem]/events:col-start-4 @max-[46rem]/events:ml-3',
} as const;

/*
 * Latency and cost are single figures, but their composition bars need room
 * to read, so from 46rem their cells hold 7.5rem, and 9rem once the table is
 * 80rem wide (full pages on wide screens): a little narrower than the tokens
 * cell (≈ 11.7rem). The class reaches the metric button (or the empty cell's
 * placeholder).
 */
const WIDE_METRIC_CELL =
  '@min-[46rem]/events:*:min-w-30 @min-[80rem]/events:*:min-w-36';

/** Classes that differ between a table that fits its pane and one that scrolls. */
interface ColumnFit {
  /** Principal and upstream when both are shown. */
  bothEntities: string;
  /** Session, the flexible column that absorbs spare width. */
  session: string;
  /** Model, a fixed-width column capped at a dated model id. */
  model: string;
  /** The pane band's two-line row (fit mode, 46–60rem); empty when scrolling. */
  band: {
    table: string;
    head: string;
    row: string;
    cell: string;
    skeletonRow: string;
    skeletonCell: string;
    skeletonHiddenCell: string;
    time: string;
    timeDot: string;
    timeAnchors: string;
    kind: string;
    session: string;
    entity: string;
    model: string;
    status: string;
    latency: string;
    tokens: string;
    cost: string;
  };
}

/*
 * Without a `minWidthClass` floor the table fits its container. A detail
 * pane between the card layout and 60rem (≈ 47rem on 1440px screens) cannot
 * hold every column as a table row next to readable latency and cost cells,
 * so there each row takes two lines on fixed tracks, lined up from row to row
 * and headed by the cells themselves: time, kind, session, the principal or
 * upstream, model and status; then latency under time and kind, tokens under
 * session and cost at the right edge. When both principal and upstream are
 * shown they sit out that band. From 60rem the pane gets the table, session
 * absorbing the slack and middle-truncating while model keeps its capped
 * content width.
 */
const FIT_COLUMNS: ColumnFit = {
  bothEntities: '@min-[46rem]/events:@max-[60rem]/events:hidden',
  // No right inset: the Kind cell's own left inset already separates them,
  // and the 12px lets a full 36-character id fit a 298px column.
  session:
    '@min-[60rem]/events:w-full @min-[60rem]/events:max-w-0 @min-[60rem]/events:pr-0',
  model: '@min-[60rem]/events:whitespace-nowrap @min-[60rem]/events:max-w-40',
  band: {
    table: '@min-[46rem]/events:@max-[60rem]/events:block',
    head: '@min-[46rem]/events:@max-[60rem]/events:hidden',
    row: '@min-[46rem]/events:@max-[60rem]/events:grid @min-[46rem]/events:@max-[60rem]/events:h-auto! @min-[46rem]/events:@max-[60rem]/events:grid-cols-[4rem_3.25rem_minmax(6rem,1fr)_auto_auto_auto] @min-[46rem]/events:@max-[60rem]/events:items-center @min-[46rem]/events:@max-[60rem]/events:gap-x-3 @min-[46rem]/events:@max-[60rem]/events:gap-y-1 @min-[46rem]/events:@max-[60rem]/events:px-4 @min-[46rem]/events:@max-[60rem]/events:py-2',
    cell: '@min-[46rem]/events:@max-[60rem]/events:p-0! @min-[46rem]/events:@max-[60rem]/events:*:p-0 @min-[46rem]/events:@max-[60rem]/events:min-w-0',
    skeletonRow:
      '@min-[46rem]/events:@max-[60rem]/events:flex @min-[46rem]/events:@max-[60rem]/events:items-center @min-[46rem]/events:@max-[60rem]/events:px-1',
    skeletonCell: '@min-[46rem]/events:@max-[60rem]/events:flex-1',
    skeletonHiddenCell: '@min-[46rem]/events:@max-[60rem]/events:hidden',
    time: '@min-[46rem]/events:@max-[60rem]/events:row-start-1 @min-[46rem]/events:@max-[60rem]/events:col-start-1',
    timeDot: '@min-[46rem]/events:@max-[60rem]/events:-left-3',
    timeAnchors: '@min-[46rem]/events:@max-[60rem]/events:hidden',
    kind: '@min-[46rem]/events:@max-[60rem]/events:row-start-1 @min-[46rem]/events:@max-[60rem]/events:col-start-2 @min-[46rem]/events:@max-[60rem]/events:justify-self-start',
    session:
      '@min-[46rem]/events:@max-[60rem]/events:row-start-1 @min-[46rem]/events:@max-[60rem]/events:col-start-3 @min-[46rem]/events:@max-[60rem]/events:w-full',
    entity:
      '@min-[46rem]/events:@max-[60rem]/events:row-start-1 @min-[46rem]/events:@max-[60rem]/events:col-start-4',
    model:
      '@min-[46rem]/events:@max-[60rem]/events:row-start-1 @min-[46rem]/events:@max-[60rem]/events:col-start-5 @min-[46rem]/events:@max-[60rem]/events:max-w-40 @min-[46rem]/events:@max-[60rem]/events:text-right',
    status:
      '@min-[46rem]/events:@max-[60rem]/events:row-start-1 @min-[46rem]/events:@max-[60rem]/events:col-start-6',
    latency:
      '@min-[46rem]/events:@max-[60rem]/events:row-start-2 @min-[46rem]/events:@max-[60rem]/events:col-start-1 @min-[46rem]/events:@max-[60rem]/events:col-span-2 @min-[46rem]/events:@max-[60rem]/events:text-left',
    tokens:
      '@min-[46rem]/events:@max-[60rem]/events:row-start-2 @min-[46rem]/events:@max-[60rem]/events:col-start-3 @min-[46rem]/events:@max-[60rem]/events:col-span-2 @min-[46rem]/events:@max-[60rem]/events:justify-self-start @min-[46rem]/events:@max-[60rem]/events:text-left',
    cost: '@min-[46rem]/events:@max-[60rem]/events:row-start-2 @min-[46rem]/events:@max-[60rem]/events:col-start-5 @min-[46rem]/events:@max-[60rem]/events:col-span-2 @min-[46rem]/events:@max-[60rem]/events:justify-self-end',
  },
};
/*
 * With a floor the table keeps every column at every width from 46rem;
 * session absorbs the slack and middle-truncates, so the wrapper scrolls only
 * once the floor is reached, and even then session keeps 7rem (a head, the
 * ellipsis and the tail). Model is a content column capped at 10rem: its 6rem
 * minimum fits a family and version ("sonnet-4-6"), longer dated ids truncate.
 */
const SCROLL_COLUMNS: ColumnFit = {
  bothEntities: '',
  session:
    '@min-[46rem]/events:w-full @min-[46rem]/events:max-w-0 @min-[46rem]/events:min-w-28 @min-[46rem]/events:pr-0',
  model:
    '@min-[46rem]/events:whitespace-nowrap @min-[46rem]/events:max-w-40 @min-[46rem]/events:min-w-24',
  band: {
    table: '',
    head: '',
    row: '',
    cell: '',
    skeletonRow: '',
    skeletonCell: '',
    skeletonHiddenCell: '',
    time: '',
    timeDot: '',
    timeAnchors: '',
    kind: '',
    session: '',
    entity: '',
    model: '',
    status: '',
    latency: '',
    tokens: '',
    cost: '',
  },
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
  const at = eventTime(event);

  return (
    <TableRow
      interactive
      selected={selected}
      className={cx(
        'focus-visible:outline-2 focus-visible:-outline-offset-2 focus-visible:outline-accent',
        flash ? 'flash-in' : '',
        MOBILE_ROW,
        fit.band.row,
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
          fit.band.cell,
          fit.band.time,
        )}
      >
        {/* Errors and in-flight rows get a dot in the cell's leading inset, so
        the time text stays aligned with its header whether or not a dot shows.
        Successful rows carry no dot at all. */}
        {tone === 'ok' ? null : (
          <span
            className={cx(
              'status-dot absolute top-1/2 left-1.5 -translate-y-1/2 @max-[46rem]/events:-left-3',
              fit.band.timeDot,
              outcome.type === 'partial' ? 'neutral animate-pulse' : tone,
            )}
          />
        )}
        <RelativeTime compact ts={at} />
        {onAnchorRange != null && at != null ? (
          <span
            className={cx(
              'absolute inset-y-0 right-0 flex items-center gap-0.5 bg-bg-sub pl-2 pr-1 opacity-0 transition-opacity group-hover/ts:opacity-100 group-focus-within/ts:opacity-100 @max-[46rem]/events:hidden',
              fit.band.timeAnchors,
            )}
          >
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
            fit.band.cell,
            showUpstream ? fit.bothEntities : fit.band.entity,
          )}
        >
          {principalName}
        </td>
      )}
      {showUpstream && (
        <td
          className={cx(
            'px-3 py-2 whitespace-nowrap truncate max-w-[160px]',
            MOBILE_CELL,
            showPrincipal
              ? MOBILE_AT.upstreamAfterPrincipal
              : MOBILE_AT.upstream,
            fit.band.cell,
            showPrincipal ? fit.bothEntities : fit.band.entity,
          )}
        >
          {upstreamName}
        </td>
      )}
      {showSession && (
        <td
          className={cx(
            'px-3 py-2 whitespace-nowrap',
            MOBILE_CELL,
            MOBILE_AT.session,
            fit.session,
            fit.band.cell,
            fit.band.session,
          )}
        >
          <SessionChip sessionId={event.thread_id ?? null} />
        </td>
      )}
      <td
        className={cx(
          'px-3 py-2 whitespace-nowrap',
          MOBILE_CELL,
          MOBILE_AT.kind,
          fit.band.cell,
          fit.band.kind,
        )}
      >
        <RequestKindBadge requestKind={event.request_kind} />
      </td>
      <td
        className={cx(
          'px-3 py-2 text-text-muted truncate',
          MOBILE_CELL,
          MOBILE_AT.model,
          fit.model,
          fit.band.cell,
          fit.band.model,
        )}
      >
        <span className="flex items-center gap-2 min-w-0 @max-[46rem]/events:justify-end">
          {event.model != null ? (
            <span
              className="truncate font-mono text-data"
              title={event.model}
              aria-label={event.model}
            >
              {event.model.replace(/^claude-/, '')}
            </span>
          ) : (
            <span className="font-mono text-data">{DASH}</span>
          )}
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
          fit.band.cell,
          fit.band.status,
          outcome.type === 'partial'
            ? 'text-text-faint'
            : STATUS_TONE_TEXT[tone],
        )}
      >
        <RequestOutcomeTableCell outcome={outcome} />
      </td>
      <LatencyCell
        event={event}
        className={cx(
          MOBILE_CELL,
          MOBILE_METRIC,
          MOBILE_AT.latency,
          WIDE_METRIC_CELL,
          fit.band.cell,
          fit.band.latency,
        )}
      />
      {showTokens && (
        <TokenCell
          event={event}
          isPartial={isPartial}
          className={cx(
            MOBILE_CELL,
            MOBILE_METRIC,
            MOBILE_AT.tokens,
            fit.band.cell,
            fit.band.tokens,
          )}
        />
      )}
      {showCost && (
        <CostCell
          event={event}
          isPartial={isPartial}
          className={cx(
            MOBILE_CELL,
            MOBILE_METRIC,
            MOBILE_AT.cost,
            WIDE_METRIC_CELL,
            fit.band.cell,
            fit.band.cost,
          )}
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
  // Session and kind stay even when every row is null: the dashes say the
  // requests carried none, and the columns do not jump as rows stream in.
  const showSession = columns?.session ?? true;
  const fit = minWidthClass == null ? FIT_COLUMNS : SCROLL_COLUMNS;
  const entityFit = showPrincipal && showUpstream ? fit.bothEntities : '';

  const columnSpecs: ColumnSpec[] = [
    { label: 'Timestamp', skeleton: 'max-w-24' },
    ...(showPrincipal
      ? [{ label: 'Principal', skeleton: 'max-w-24', className: entityFit }]
      : []),
    ...(showUpstream
      ? [{ label: 'Upstream', skeleton: 'max-w-28', className: entityFit }]
      : []),
    ...(showSession
      ? [{ label: 'Session', skeleton: 'max-w-40', className: fit.session }]
      : []),
    { label: 'Kind', skeleton: 'max-w-12' },
    { label: 'Model', skeleton: 'max-w-24', className: fit.model },
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
            'relative @max-[46rem]/events:block @max-[46rem]/events:min-w-0',
            fit.band.table,
            className,
          )}
        >
          <TableHead
            className={cx('@max-[46rem]/events:hidden', fit.band.head)}
          >
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
          <tbody
            style={reservedBodyStyle}
            className={cx('@max-[46rem]/events:block', fit.band.table)}
          >
            {loading ? (
              Array.from({ length: reservedRowCount }).map((_, i) => (
                <SkeletonRow
                  key={i}
                  cols={colCount}
                  style={REQUEST_EVENT_ROW_STYLE}
                  // In the card and pane-band layouts the body is a list of
                  // cards, so the placeholder is one line of the first three
                  // cells.
                  className={cx(
                    '@max-[46rem]/events:flex @max-[46rem]/events:items-center @max-[46rem]/events:px-1',
                    fit.band.skeletonRow,
                  )}
                  cellClassNames={columnSpecs.map((column, index) =>
                    cx(
                      column.numeric ? 'text-right tabular-nums' : '',
                      column.className,
                      index < 3
                        ? cx(
                            '@max-[46rem]/events:flex-1',
                            fit.band.skeletonCell,
                          )
                        : cx(
                            '@max-[46rem]/events:hidden',
                            fit.band.skeletonHiddenCell,
                          ),
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
