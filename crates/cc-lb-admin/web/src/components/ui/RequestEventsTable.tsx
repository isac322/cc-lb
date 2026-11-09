import { CircleAlert, Unplug } from 'lucide-react';
import type React from 'react';
import { memo, useState } from 'react';
import { eventTime } from '../../lib/api';
import {
  getRequestOutcome,
  type RequestOutcome,
  requestOutcomeTone,
  STATUS_TONE_TEXT_CLASS,
} from '../../lib/format';
import type { RequestEventWithPhase } from '../../lib/RequestEventTypes';
import { serviceTierBadgeText } from '../../lib/reasoningTier';
import { CostCell } from './CostCell';
import { LatencyCell } from './latency/LatencyCell';
import {
  Badge,
  cx,
  EmptyState,
  Hint,
  IconButton,
  SkeletonRow,
  Spinner,
} from './primitives';
import { RelativeTime } from './RelativeTime';
import { RequestEventDrawer } from './RequestEventDrawer';
import { RequestKindBadge } from './RequestKindBadge';
import { SessionChip } from './SessionChip';
import {
  EmptyValue,
  Table,
  TableCell,
  TableHead,
  TableHeadCell,
  TableRow,
} from './Table';
import { TokenCell } from './TokenCell';

/** = `TableRow`'s `h-10`; loading and empty bodies reserve this per row. */
const RESERVED_ROW_REM = 2.5;

/*
 * Layout follows the table's own wrapper (`@container/events`), not the
 * viewport, so every callsite gets the same rule:
 *
 * - Below 59rem each row is a three-line card on fixed tracks
 *   (4rem / 3.25rem / 1fr / 1fr / 4rem): time, kind, session and status;
 *   the principal and/or upstream and model; latency, tokens and cost.
 *   Cells keep their DOM order and are only placed on the grid.
 * - From 59rem it is a `table-fixed` table sized by its `<colgroup>`, so no
 *   column width depends on row content. Session is the one flexible column.
 */
type ColumnKey =
  | 'time'
  | 'principal'
  | 'upstream'
  | 'session'
  | 'kind'
  | 'model'
  | 'status'
  | 'latency'
  | 'tokens'
  | 'cost';

interface Column {
  label: string;
  /** Table-arrangement width in rem; null = the flexible column (Session). */
  rem: number | null;
  numeric?: boolean;
  /** Card placement, every class prefixed `@max-[59rem]/events:`. */
  card: string;
  /** Card placement when both principal and upstream are shown. */
  cardBoth?: string;
}

/** Session never shrinks below this in the table (the chip's head, ellipsis and tail). */
const SESSION_FLOOR_REM = 5;

// The literal 59rem in every `@max-/@min-[59rem]/events:` class is the
// one-entity need: Σ rem without principal or upstream + SESSION_FLOOR_REM
// (5.75 + 6.5 + 4.75 + 9 + 4 + 5.75 + 12 + 6.25 − 6.5 + 5 = 59). Change a
// width here and that literal together.
const COLUMNS = {
  time: {
    label: 'Timestamp',
    rem: 5.75,
    card: '@max-[59rem]/events:row-start-1 @max-[59rem]/events:col-start-1',
  },
  principal: {
    label: 'Principal',
    rem: 6.5,
    card: '@max-[59rem]/events:row-start-2 @max-[59rem]/events:col-start-1 @max-[59rem]/events:col-span-3',
    cardBoth:
      '@max-[59rem]/events:row-start-2 @max-[59rem]/events:col-start-1 @max-[59rem]/events:col-span-2',
  },
  upstream: {
    label: 'Upstream',
    rem: 6.5,
    card: '@max-[59rem]/events:row-start-2 @max-[59rem]/events:col-start-1 @max-[59rem]/events:col-span-3',
    // Reads "principal → upstream" when both cells are shown.
    cardBoth:
      '@max-[59rem]/events:row-start-2 @max-[59rem]/events:col-start-3 @max-[59rem]/events:before:content-["→_"] @max-[59rem]/events:before:text-text-faint',
  },
  session: {
    label: 'Session',
    rem: null,
    card: '@max-[59rem]/events:row-start-1 @max-[59rem]/events:col-start-3 @max-[59rem]/events:col-span-2 @max-[59rem]/events:w-full',
  },
  kind: {
    label: 'Kind',
    rem: 4.75,
    card: '@max-[59rem]/events:row-start-1 @max-[59rem]/events:col-start-2 @max-[59rem]/events:justify-self-start @max-[59rem]/events:max-w-full',
  },
  model: {
    label: 'Model',
    rem: 9,
    card: '@max-[59rem]/events:row-start-2 @max-[59rem]/events:col-start-4 @max-[59rem]/events:col-span-2 @max-[59rem]/events:text-right',
  },
  status: {
    label: 'Status',
    rem: 4,
    numeric: true,
    card: '@max-[59rem]/events:row-start-1 @max-[59rem]/events:col-start-5 @max-[59rem]/events:justify-self-end',
  },
  latency: {
    label: 'Latency',
    rem: 5.75,
    numeric: true,
    card: '@max-[59rem]/events:row-start-3 @max-[59rem]/events:col-start-1 @max-[59rem]/events:text-left',
  },
  tokens: {
    label: 'Tokens',
    rem: 12,
    numeric: true,
    card: '@max-[59rem]/events:row-start-3 @max-[59rem]/events:col-start-2 @max-[59rem]/events:col-span-3 @max-[59rem]/events:justify-self-start @max-[59rem]/events:text-left',
  },
  cost: {
    label: 'Cost',
    rem: 6.25,
    numeric: true,
    card: '@max-[59rem]/events:row-start-3 @max-[59rem]/events:col-start-5',
  },
} satisfies Record<ColumnKey, Column>;

const COLUMN_ORDER: readonly ColumnKey[] = [
  'time',
  'principal',
  'upstream',
  'session',
  'kind',
  'model',
  'status',
  'latency',
  'tokens',
  'cost',
];

const CARD_ROW =
  '@max-[59rem]/events:grid @max-[59rem]/events:h-auto @max-[59rem]/events:grid-cols-[4rem_3.25rem_minmax(0,1fr)_minmax(0,1fr)_4rem] @max-[59rem]/events:items-center @max-[59rem]/events:gap-x-2 @max-[59rem]/events:gap-y-1 @max-[59rem]/events:px-4 @max-[59rem]/events:py-2.5';
// `p-0!` must beat `Table`'s `[&_td:first-child]:pl-4` / `last-child:pr-4`,
// descendant selectors with higher specificity than a plain utility.
const CARD_CELL = '@max-[59rem]/events:p-0! @max-[59rem]/events:min-w-0';
/*
 * A card's metric button is only its figure and bar; `*:min-w-0` drops the
 * button's 72px floor, and padding pulled back by a negative margin makes the
 * touch target 43px without moving the card's lines.
 */
const CARD_METRIC =
  '@max-[59rem]/events:*:p-0 @max-[59rem]/events:*:min-w-0 @max-[59rem]/events:*:-my-2 @max-[59rem]/events:*:py-2';

const CLAUDE_PREFIX_RE = /^claude-/;

/** Radii offered by the per-row time anchor, as [label, seconds either side]. */
const ANCHOR_RADII_SECS: ReadonlyArray<readonly [string, number]> = [
  ['±1m', 60],
  ['±5m', 300],
  ['±30m', 1800],
];

interface RequestEventsTableProps {
  events: readonly RequestEventWithPhase[];
  principalNameMap: Map<string, string>;
  loading?: boolean;
  reservedRowCount?: number;
  emptyTitle?: string;
  emptyDescription?: string;
  /** Next step shown under the empty message, e.g. a link to the setup flow. */
  emptyAction?: React.ReactNode;
  /** Heading level of the empty-state title; match the surrounding outline. */
  emptyHeadingLevel?: 2 | 3 | 4;
  liveFlashIds?: Set<string>;
  /** Hide an entity column the surrounding page already implies. */
  columns?: {
    principal?: boolean;
    upstream?: boolean;
  };
  sentinelRef?: React.RefObject<HTMLTableRowElement | null>;
  loadingMore?: boolean;
  hasMore?: boolean;
  /** Focus the surrounding time range on a row, in seconds either side. */
  onAnchorRange?: (tsMs: number, radiusSecs: number) => void;
}

/**
 * The status as a bare HTTP number, or an icon whose `Hint` names the outcome
 * when the number alone would mislead. In-flight rows leave it empty; the
 * timestamp's spinner marks them.
 */
function RequestStatusCell({ outcome }: { outcome: RequestOutcome }) {
  const base = cx(CARD_CELL, COLUMNS.status.card);
  if (outcome.type === 'partial') {
    return (
      <TableCell numeric className={base}>
        <span className="sr-only">In progress</span>
      </TableCell>
    );
  }
  if (outcome.type === 'completed') {
    return (
      <TableCell
        numeric
        className={cx(
          base,
          STATUS_TONE_TEXT_CLASS[requestOutcomeTone(outcome)],
        )}
      >
        {outcome.status}
      </TableCell>
    );
  }
  const disconnected = outcome.type === 'client_disconnected';
  const title =
    outcome.type === 'client_disconnected'
      ? 'Client disconnected'
      : outcome.label;
  const detail =
    outcome.error_code != null && outcome.error_code !== title
      ? `HTTP ${outcome.status} · ${outcome.error_code}`
      : `HTTP ${outcome.status}`;
  const Icon = disconnected ? Unplug : CircleAlert;
  return (
    <TableCell numeric className={base}>
      <Hint
        openOnFocus={false}
        label={
          <span className="flex flex-col">
            <span>{title}</span>
            <span className="text-text-muted">{detail}</span>
          </span>
        }
      >
        {/* Tone sits on the glyph: `cx` does not merge, so a `text-*` on the
        button would race `IconButton`'s own `text-text-muted`. */}
        <IconButton
          label={`${title} (HTTP ${outcome.status})`}
          className="-my-1.5 -mr-2 align-middle max-md:-my-3 max-md:-mr-3.5"
        >
          <Icon
            strokeWidth={1.75}
            aria-hidden="true"
            className={disconnected ? 'text-warn-text' : 'text-danger-text'}
          />
        </IconButton>
      </Hint>
    </TableCell>
  );
}

interface RequestEventRowProps {
  event: RequestEventWithPhase;
  eventKey: string;
  principalName: string | null;
  upstreamName: string | null;
  showPrincipal: boolean;
  showUpstream: boolean;
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
  const tone = requestOutcomeTone(outcome);
  const tierBadge = serviceTierBadgeText(event.service_tier);
  const thinkingLevel = event.thinking_level?.trim() || null;
  const modelName = event.model?.replace(CLAUDE_PREFIX_RE, '') ?? null;
  // 11ch of the mono font keeps "sonnet-4-5…", "3-5-sonnet…" or "gemini-2.5…".
  const modelFloorCh = modelName == null ? 0 : Math.min(modelName.length, 11);
  const at = eventTime(event);
  const bothEntities = showPrincipal && showUpstream;

  return (
    <TableRow
      interactive
      selected={selected}
      className={cx(
        'focus-visible:outline-2 focus-visible:-outline-offset-2 focus-visible:outline-accent',
        flash && 'flash-in',
        CARD_ROW,
      )}
      onClick={() => selectEvent(eventKey)}
      tabIndex={0}
      onKeyDown={(keyboardEvent) => {
        // Keys pressed on a control inside the row (metric breakdowns, time
        // anchors, the status icon) belong to that control, not to the row.
        if (keyboardEvent.target !== keyboardEvent.currentTarget) return;
        if (keyboardEvent.key === 'Enter' || keyboardEvent.key === ' ') {
          keyboardEvent.preventDefault();
          selectEvent(eventKey);
        }
      }}
      aria-selected={selected}
      aria-label={`View request ${eventKey}`}
    >
      <TableCell
        className={cx(
          'group/ts relative text-text-muted tabular-nums',
          CARD_CELL,
          COLUMNS.time.card,
        )}
      >
        {/* Errors and in-flight rows get a marker in the cell's leading inset,
        so the time text stays aligned with its header whether or not one
        shows. Successful rows carry none. */}
        {tone !== 'ok' && (
          <span
            aria-hidden="true"
            className="absolute top-1/2 left-0.5 flex size-3 -translate-y-1/2 items-center justify-center @max-[59rem]/events:-left-3.5"
          >
            {outcome.type === 'partial' ? (
              <Spinner className="size-3 text-text-muted motion-reduce:animate-none" />
            ) : (
              <span className={cx('status-dot', tone)} />
            )}
          </span>
        )}
        <RelativeTime compact ts={at} className="block truncate" />
        {onAnchorRange != null && at != null && (
          <span className="absolute inset-y-0 left-0 z-1 flex items-center gap-0.5 whitespace-nowrap bg-bg-sub pl-[inherit] pr-1 opacity-0 transition-opacity group-hover/ts:opacity-100 group-focus-within/ts:opacity-100">
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
        )}
      </TableCell>
      {showPrincipal && (
        <TableCell
          className={cx(
            'truncate',
            CARD_CELL,
            bothEntities ? COLUMNS.principal.cardBoth : COLUMNS.principal.card,
          )}
          title={principalName ?? undefined}
        >
          {principalName ?? <EmptyValue label="No principal" />}
        </TableCell>
      )}
      {showUpstream && (
        <TableCell
          className={cx(
            'truncate',
            CARD_CELL,
            bothEntities ? COLUMNS.upstream.cardBoth : COLUMNS.upstream.card,
          )}
          title={upstreamName ?? undefined}
        >
          {upstreamName ?? <EmptyValue label="No upstream" />}
        </TableCell>
      )}
      <TableCell
        className={cx('overflow-hidden', CARD_CELL, COLUMNS.session.card)}
      >
        <SessionChip sessionId={event.thread_id ?? null} />
      </TableCell>
      <TableCell
        className={cx('overflow-hidden', CARD_CELL, COLUMNS.kind.card)}
      >
        <RequestKindBadge requestKind={event.request_kind} />
      </TableCell>
      <TableCell
        className={cx(
          'overflow-hidden text-text-muted',
          CARD_CELL,
          COLUMNS.model.card,
        )}
      >
        <span className="flex min-w-0 items-center gap-2 @max-[59rem]/events:justify-end">
          {event.model != null && modelName != null ? (
            // Name first, whole badges only. The name keeps 11 characters (or
            // its whole text when shorter); its shrink weight of 100 makes it
            // absorb nearly all overflow until it freezes at that floor. The
            // badge group then shrinks, and a badge that no longer fits wraps
            // onto the group's clipped second line instead of being squeezed
            // into an empty pill. The drawer always shows thinking level and
            // tier.
            <span
              className="shrink-[100] truncate font-mono text-data"
              style={{ minWidth: `${modelFloorCh}ch` }}
              title={event.model}
              aria-label={event.model}
            >
              {modelName}
            </span>
          ) : (
            <EmptyValue label="No model" />
          )}
          {(thinkingLevel != null || tierBadge != null) && (
            <span className="flex h-5 min-w-0 flex-wrap gap-x-2 overflow-hidden">
              {thinkingLevel != null && (
                <Badge tone="neutral" className="shrink-0">
                  {thinkingLevel}
                </Badge>
              )}
              {tierBadge != null && (
                <Badge tone="neutral" className="shrink-0">
                  {tierBadge}
                </Badge>
              )}
            </span>
          )}
        </span>
      </TableCell>
      <RequestStatusCell outcome={outcome} />
      <LatencyCell
        event={event}
        className={cx(CARD_METRIC, COLUMNS.latency.card)}
      />
      <TokenCell
        event={event}
        isPartial={isPartial}
        className={cx(CARD_METRIC, COLUMNS.tokens.card)}
      />
      <CostCell
        event={event}
        isPartial={isPartial}
        className={cx(CARD_METRIC, COLUMNS.cost.card)}
      />
    </TableRow>
  );
});

export const RequestEventsTable = memo(function RequestEventsTable({
  events,
  principalNameMap,
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
  onAnchorRange,
}: RequestEventsTableProps) {
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const selected = selectedId
    ? (events.find((e) => (e.event_id ?? e.request_id) === selectedId) ?? null)
    : null;
  const showPrincipal = columns?.principal ?? true;
  const showUpstream = columns?.upstream ?? true;
  const visible = COLUMN_ORDER.filter(
    (k) =>
      (k !== 'principal' || showPrincipal) &&
      (k !== 'upstream' || showUpstream),
  );
  const colCount = visible.length;
  // The table never narrows below its fixed columns plus Session's floor; with
  // both entities that is 65.5rem and the caller's slot scrolls below it.
  const minWidthRem = visible.reduce(
    (sum, k) => sum + (COLUMNS[k].rem ?? 0),
    SESSION_FLOOR_REM,
  );
  const reservedBodyStyle =
    loading || events.length === 0
      ? { height: `${reservedRowCount * RESERVED_ROW_REM}rem` }
      : undefined;

  return (
    <>
      <div className="@container/events">
        <Table
          className="table-fixed @min-[59rem]/events:min-w-(--events-min-w) @max-[59rem]/events:block"
          style={
            { '--events-min-w': `${minWidthRem}rem` } as React.CSSProperties
          }
        >
          <colgroup className="@max-[59rem]/events:hidden">
            {visible.map((k) => {
              const rem = COLUMNS[k].rem;
              return (
                <col
                  key={k}
                  style={rem == null ? undefined : { width: `${rem}rem` }}
                />
              );
            })}
          </colgroup>
          <TableHead className="@max-[59rem]/events:hidden">
            <tr>
              {visible.map((k) => {
                const column: Column = COLUMNS[k];
                return (
                  <TableHeadCell key={k} numeric={column.numeric}>
                    {column.label}
                  </TableHeadCell>
                );
              })}
            </tr>
          </TableHead>
          <tbody
            style={reservedBodyStyle}
            className="@max-[59rem]/events:block"
          >
            {loading ? (
              Array.from({ length: reservedRowCount }).map((_, i) => (
                <SkeletonRow
                  key={i}
                  cols={colCount}
                  // In the card arrangement the placeholder is one line of the
                  // first three cells.
                  className="h-10 @max-[59rem]/events:flex @max-[59rem]/events:items-center @max-[59rem]/events:px-1"
                  cellClassNames={visible.map((_, index) =>
                    index < 3
                      ? '@max-[59rem]/events:flex-1'
                      : '@max-[59rem]/events:hidden',
                  )}
                  skeletonClassNames={visible.map((k) => {
                    const column: Column = COLUMNS[k];
                    return column.numeric ? 'w-3/4 ml-auto' : 'w-3/4';
                  })}
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
                      null
                    }
                    upstreamName={event.upstream_name ?? null}
                    showPrincipal={showPrincipal}
                    showUpstream={showUpstream}
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
