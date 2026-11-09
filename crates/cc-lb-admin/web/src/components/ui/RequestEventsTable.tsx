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
import {
  PROVIDER_LABELS,
  type ProviderFamily,
  providersForRequestEventKind,
} from '../../lib/providers';
import type { RequestEventWithPhase } from '../../lib/RequestEventTypes';
import { serviceTierBadgeText } from '../../lib/reasoningTier';
import { CostCell } from './CostCell';
import { LatencyCell } from './latency/LatencyCell';
import { ProviderLogo } from './ProviderLogo';
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
 * - Below 42rem (phones and narrow split panes) each row is a three-line card
 *   on fixed tracks (4rem / 3.25rem / 1fr / 1fr / 4rem): time, kind, session
 *   and status; the principal and/or upstream and model; latency, tokens and
 *   cost. Cells keep their DOM order and are only placed on the grid.
 * - From 42rem (tablets in portrait or landscape, and desktops) it is a
 *   `table-fixed` table sized by its `<colgroup>`, so no column width depends
 *   on row content. Each column starts at its compact `rem` minimum (43.75rem
 *   with both entities so a 768px portrait tablet fits without horizontal
 *   scroll, 39.6875rem with one); the container width above the table's minimum
 *   (`100cqw` of the wrapper) is split by `grow` among the columns, and
 *   Session, which has no width of its own, takes its share plus any rounding
 *   remainder. Between 42rem and 59rem cell horizontal padding tightens so all
 *   ten columns fit comfortably.
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
  /** Table-arrangement minimum width in rem; null = Session, which takes the remainder. */
  rem: number | null;
  /**
   * Share of the width above the table's compact minimum.
   */
  grow?: number;
  numeric?: boolean;
  /** Card placement, every class prefixed `@max-[42rem]/events:`. */
  card: string;
  /** Card placement when both principal and upstream are shown. */
  cardBoth?: string;
}

/** Session never shrinks below this in the compact tablet table. */
const SESSION_FLOOR_REM = 4.125;

// The 43.75rem ten-column minimum (700px) fits inside a 768px portrait tablet
// card (`PageContainer` 2×32px gutter + 2×2px card border = 700px clientWidth);
// the 39.6875rem nine-column minimum (635px) fits single-entity detail tables.
// Below 42rem (672px) rows stack into three-line mobile cards.
const COLUMNS = {
  time: {
    label: 'Timestamp',
    rem: 4.625,
    grow: 0.4,
    card: '@max-[42rem]/events:row-start-1 @max-[42rem]/events:col-start-1',
  },
  principal: {
    label: 'Principal',
    rem: 4.0625,
    grow: 1.4,
    card: '@max-[42rem]/events:row-start-2 @max-[42rem]/events:col-start-1 @max-[42rem]/events:col-span-3',
    cardBoth:
      '@max-[42rem]/events:row-start-2 @max-[42rem]/events:col-start-1 @max-[42rem]/events:col-span-2',
  },
  upstream: {
    label: 'Upstream',
    rem: 4.0625,
    grow: 1.4,
    card: '@max-[42rem]/events:row-start-2 @max-[42rem]/events:col-start-1 @max-[42rem]/events:col-span-3',
    // Reads "principal → upstream" when both cells are shown.
    cardBoth:
      '@max-[42rem]/events:row-start-2 @max-[42rem]/events:col-start-3 @max-[42rem]/events:before:content-["→_"] @max-[42rem]/events:before:text-text-faint',
  },
  session: {
    label: 'Session',
    rem: null,
    grow: 1.4,
    card: '@max-[42rem]/events:row-start-1 @max-[42rem]/events:col-start-3 @max-[42rem]/events:col-span-2 @max-[42rem]/events:w-full',
  },
  kind: {
    label: 'Kind',
    rem: 3.25,
    grow: 0.5,
    card: '@max-[42rem]/events:row-start-1 @max-[42rem]/events:col-start-2 @max-[42rem]/events:justify-self-start @max-[42rem]/events:max-w-full',
  },
  model: {
    label: 'Model',
    rem: 4.625,
    grow: 1.7,
    card: '@max-[42rem]/events:row-start-2 @max-[42rem]/events:col-start-4 @max-[42rem]/events:col-span-2 @max-[42rem]/events:text-right',
  },
  status: {
    label: 'Status',
    rem: 2.75,
    numeric: true,
    card: '@max-[42rem]/events:row-start-1 @max-[42rem]/events:col-start-5 @max-[42rem]/events:justify-self-end',
  },
  latency: {
    label: 'Latency',
    rem: 4.125,
    grow: 0.8,
    numeric: true,
    card: '@max-[42rem]/events:row-start-3 @max-[42rem]/events:col-start-1 @max-[42rem]/events:text-left',
  },
  tokens: {
    label: 'Tokens',
    rem: 8,
    grow: 5.2,
    numeric: true,
    card: '@max-[42rem]/events:row-start-3 @max-[42rem]/events:col-start-2 @max-[42rem]/events:col-span-3 @max-[42rem]/events:justify-self-start @max-[42rem]/events:text-left',
  },
  cost: {
    label: 'Cost',
    rem: 4.125,
    grow: 0.8,
    numeric: true,
    card: '@max-[42rem]/events:row-start-3 @max-[42rem]/events:col-start-5',
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
  '@max-[42rem]/events:grid @max-[42rem]/events:h-auto @max-[42rem]/events:grid-cols-[4rem_3.25rem_minmax(0,1fr)_minmax(0,1fr)_4rem] @max-[42rem]/events:items-center @max-[42rem]/events:gap-x-2 @max-[42rem]/events:gap-y-1 @max-[42rem]/events:px-4 @max-[42rem]/events:py-2.5';
// `p-0!` must beat `Table`'s `[&_td:first-child]:pl-4` / `last-child:pr-4`,
// descendant selectors with higher specificity than a plain utility.
const CARD_CELL = '@max-[42rem]/events:p-0! @max-[42rem]/events:min-w-0';
// Between 42rem and 59rem (tablet portrait and landscape) tighten horizontal
// cell padding while keeping the first cell's 16px leading inset for the
// status dot / spinner.
const COMPACT_CELL =
  '@min-[42rem]/events:@max-[59rem]/events:px-1.5! @min-[42rem]/events:@max-[59rem]/events:first:pl-4! @min-[42rem]/events:@max-[59rem]/events:last:pr-2.5!';
// `MetricCell` puts its horizontal padding and `min-w-18` (72px) on the inner
// `<button>` / `<span>` while keeping `p-0` on the `<td>`. Target the child in
// the 42rem–59rem tablet band and keep `p-0!` on the `<td>` so `Table`'s
// `[&_td:last-child]:pr-4` does not double-pad the Cost column.
const COMPACT_METRIC =
  '@min-[42rem]/events:@max-[59rem]/events:p-0! @min-[42rem]/events:@max-[59rem]/events:*:min-w-0 @min-[42rem]/events:@max-[59rem]/events:*:px-1.5 @min-[42rem]/events:@max-[59rem]/events:last:*:pr-2.5';
/*
 * A card's metric button is only its figure and bar; `*:min-w-0` drops the
 * button's 72px floor, and padding pulled back by a negative margin makes the
 * touch target 43px without moving the card's lines.
 */
const CARD_METRIC =
  '@max-[42rem]/events:*:p-0 @max-[42rem]/events:*:min-w-0 @max-[42rem]/events:*:-my-2 @max-[42rem]/events:*:py-2';
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
  showProviderLogo?: boolean;
  /**
   * Model cells lead with the provider mark by default. Detail views can
   * suppress it when the surrounding upstream identity already supplies it.
   * Optional inventory-derived provider identity stays outside the table to
   * avoid coupling isolated request feeds to the upstream query.
   */
  providerByUpstream?: ReadonlyMap<string, ProviderFamily>;
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
  const base = cx(CARD_CELL, COMPACT_CELL, COLUMNS.status.card);
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
  provider: ProviderFamily;
  showProviderLogo: boolean;
  showPrincipal: boolean;
  showUpstream: boolean;
  selected: boolean;
  flash: boolean;
  selectEvent: React.Dispatch<React.SetStateAction<string | null>>;
  onAnchorRange?: (tsMs: number, radiusSecs: number) => void;
}

export function providerForEvent(
  event: RequestEventWithPhase,
  providerByUpstream?: ReadonlyMap<string, ProviderFamily>,
): ProviderFamily {
  const upstreamProvider =
    event.upstream_id == null
      ? undefined
      : providerByUpstream?.get(event.upstream_id);
  if (upstreamProvider != null) return upstreamProvider;
  const inferred = providersForRequestEventKind(event.event_kind);
  return inferred.length === 1 ? inferred[0] : 'anthropic';
}

const RequestEventRow = memo(function RequestEventRow({
  event,
  eventKey,
  principalName,
  upstreamName,
  provider,
  showProviderLogo,
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
  // 9ch of the mono font keeps "opus-5-5", "haiku-5-5", "sonnet-4…" in compact
  // tablet columns while yielding space before thinking/tier badges.
  const modelFloorCh = modelName == null ? 0 : Math.min(modelName.length, 9);
  const at = eventTime(event);
  const bothEntities = showPrincipal && showUpstream;

  return (
    <TableRow
      interactive
      selected={selected}
      className={cx(
        'focus-visible:outline-2 focus-visible:-outline-offset-2 focus-visible:outline-accent',
        flash && 'flash-in',
        // Pulse the cells, not the row: `flash-in` owns the row's `animation`.
        isPartial && 'motion-safe:*:animate-pulse',
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
          COMPACT_CELL,
          COLUMNS.time.card,
        )}
      >
        {/* Errors and in-flight rows get a marker in the cell's leading inset,
        so the time text stays aligned with its header whether or not one
        shows. Successful rows carry none. */}
        {tone !== 'ok' && (
          <span
            aria-hidden="true"
            className="absolute top-1/2 left-px flex size-3.5 -translate-y-1/2 items-center justify-center @max-[42rem]/events:-left-3.5"
          >
            {outcome.type === 'partial' ? (
              <Spinner className="size-3.5 text-text motion-reduce:animate-none" />
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
            COMPACT_CELL,
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
            COMPACT_CELL,
            bothEntities ? COLUMNS.upstream.cardBoth : COLUMNS.upstream.card,
          )}
          title={upstreamName ?? undefined}
        >
          {upstreamName ?? <EmptyValue label="No upstream" />}
        </TableCell>
      )}
      <TableCell
        className={cx(
          'overflow-hidden',
          CARD_CELL,
          COMPACT_CELL,
          COLUMNS.session.card,
        )}
      >
        <SessionChip sessionId={event.thread_id ?? null} />
      </TableCell>
      <TableCell
        className={cx(
          'overflow-hidden',
          CARD_CELL,
          COMPACT_CELL,
          COLUMNS.kind.card,
        )}
      >
        <RequestKindBadge requestKind={event.request_kind} />
      </TableCell>
      <TableCell
        className={cx(
          'overflow-hidden text-text-muted',
          CARD_CELL,
          COMPACT_CELL,
          COLUMNS.model.card,
          showProviderLogo && '@min-[60rem]/events:max-w-28!',
        )}
      >
        <span className="flex min-w-0 items-center gap-2 @max-[42rem]/events:justify-end">
          {showProviderLogo ? (
            <ProviderLogo
              className="shrink-0 text-text-muted"
              provider={provider}
              title={`${PROVIDER_LABELS[provider]} provider`}
            />
          ) : null}
          {event.model != null && modelName != null ? (
            // Name first, whole badges only. The name keeps 11 characters (or
            // its whole text when shorter); its shrink weight of 100 makes it
            // absorb nearly all overflow until it freezes at that floor. The
            // badge group then shrinks. A flex line always places its first
            // item even when it overflows, so a zero-width spacer leads the
            // visible line: a badge that doesn't fit beside it wraps onto the
            // clipped second line whole, never a partial pill. `-ml-2` cancels
            // the spacer's gap. The drawer always shows thinking level and
            // tier.
            <span
              className="min-w-0 shrink-[100] truncate font-mono text-data"
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
            <span className="-ml-2 flex h-5 min-w-0 flex-wrap gap-x-2 overflow-hidden">
              <span aria-hidden="true" className="h-5 w-0" />
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
        className={cx(CARD_METRIC, COMPACT_METRIC, COLUMNS.latency.card)}
      />
      <TokenCell
        event={event}
        className={cx(CARD_METRIC, COMPACT_METRIC, COLUMNS.tokens.card)}
      />
      <CostCell
        event={event}
        isPartial={isPartial}
        className={cx(CARD_METRIC, COMPACT_METRIC, COLUMNS.cost.card)}
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
  showProviderLogo = true,
  columns,
  sentinelRef,
  loadingMore,
  hasMore,
  providerByUpstream,
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
  // The table never narrows below its compact columns plus Session's floor;
  // with both entities that is 43.75rem (700px) and with one 39.6875rem (635px).
  const minWidthRem = visible.reduce(
    (sum, k) => sum + (COLUMNS[k].rem ?? 0),
    SESSION_FLOOR_REM,
  );
  const totalGrow = visible.reduce(
    (sum, k) => sum + ((COLUMNS[k] as Column).grow ?? 0),
    0,
  );
  const reservedBodyStyle =
    loading || events.length === 0
      ? { height: `${reservedRowCount * RESERVED_ROW_REM}rem` }
      : undefined;

  return (
    <>
      <div className="@container/events">
        <Table
          className="table-fixed @min-[42rem]/events:min-w-(--events-min-w) @max-[42rem]/events:block"
          style={
            { '--events-min-w': `${minWidthRem}rem` } as React.CSSProperties
          }
        >
          <colgroup className="@max-[42rem]/events:hidden">
            {visible.map((k) => {
              const { rem, grow }: Column = COLUMNS[k];
              if (rem == null) return <col key={k} />;
              const share = (grow ?? 0) / totalGrow;
              const width =
                share > 0
                  ? `calc(${rem}rem + max(0rem, 100cqw - var(--events-min-w)) * ${share})`
                  : `${rem}rem`;
              return <col key={k} style={{ width }} />;
            })}
          </colgroup>
          <TableHead className="@max-[42rem]/events:hidden">
            <tr>
              {visible.map((k) => {
                const column: Column = COLUMNS[k];
                return (
                  <TableHeadCell
                    key={k}
                    numeric={column.numeric}
                    className={COMPACT_CELL}
                  >
                    {column.label}
                  </TableHeadCell>
                );
              })}
            </tr>
          </TableHead>
          <tbody
            style={reservedBodyStyle}
            className="@max-[42rem]/events:block"
          >
            {loading ? (
              Array.from({ length: reservedRowCount }).map((_, i) => (
                <SkeletonRow
                  key={i}
                  cols={colCount}
                  // In the card arrangement the placeholder is one line of the
                  // first three cells.
                  className="h-10 @max-[42rem]/events:flex @max-[42rem]/events:items-center @max-[42rem]/events:px-1"
                  cellClassNames={visible.map((_, index) =>
                    index < 3
                      ? '@max-[42rem]/events:flex-1'
                      : '@max-[42rem]/events:hidden',
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
                    provider={providerForEvent(event, providerByUpstream)}
                    showProviderLogo={showProviderLogo}
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
        providerByUpstream={providerByUpstream}
        onClose={() => setSelectedId(null)}
      />
    </>
  );
});
