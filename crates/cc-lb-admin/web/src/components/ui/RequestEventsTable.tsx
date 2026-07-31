import type React from 'react';
import { useState } from 'react';
import { eventTime } from '../../lib/api';
import { getRequestOutcome, requestOutcomeTone } from '../../lib/format';
import type { RequestEventWithPhase } from '../../lib/RequestEventTypes';
import { serviceTierBadgeText } from '../../lib/reasoningTier';
import { requestKindBadgeText } from '../../lib/requestKind';
import { CostCell } from './CostCell';
import { LatencyCell } from './latency/LatencyCell';
import { Badge, cx, SkeletonRow } from './primitives';
import { RelativeTime } from './RelativeTime';
import { RequestEventDrawer } from './RequestEventDrawer';
import { RequestOutcomeTableCell } from './RequestEventIdentity';
import { SessionChip } from './SessionChip';
import { TokenCell } from './TokenCell';

export { SessionChip };

const DASH = '—';
const REQUEST_EVENT_ROW_HEIGHT_REM = 2.53125;
const REQUEST_EVENT_ROW_STYLE = {
  height: `${REQUEST_EVENT_ROW_HEIGHT_REM}rem`,
} satisfies React.CSSProperties;

const STATUS_TONE_TEXT: Record<'ok' | 'warn' | 'danger' | 'neutral', string> = {
  ok: 'text-[color:var(--color-ok)]',
  warn: 'text-[color:var(--color-warn)]',
  danger: 'text-[color:var(--color-danger)]',
  neutral: 'text-text',
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

export function RequestEventsTable({
  events,
  principalNameMap,
  upstreamNameMap,
  loading,
  reservedRowCount = 5,
  emptyTitle = 'No requests',
  emptyDescription,
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
  const showSession = columns?.session ?? true;
  const showTokens = columns?.tokens ?? true;
  const showCost = columns?.cost ?? true;

  const loadingCellClassNames = [
    '',
    ...(showPrincipal ? [''] : []),
    ...(showUpstream ? [''] : []),
    ...(showSession ? [''] : []),
    '',
    'text-right tabular-nums',
    'text-right tabular-nums',
    ...(showTokens ? ['text-right tabular-nums'] : []),
    ...(showCost ? ['text-right tabular-nums'] : []),
  ];
  const loadingSkeletonClassNames = [
    'max-w-24',
    ...(showPrincipal ? ['max-w-24'] : []),
    ...(showUpstream ? ['max-w-28'] : []),
    ...(showSession ? ['max-w-24'] : []),
    'max-w-40',
    'max-w-12 ml-auto',
    'max-w-16 ml-auto',
    ...(showTokens ? ['max-w-36 ml-auto'] : []),
    ...(showCost ? ['max-w-20 ml-auto'] : []),
  ];
  const colCount = loadingSkeletonClassNames.length;
  const reservedBodyStyle =
    loading || events.length === 0
      ? { height: `${reservedRowCount * REQUEST_EVENT_ROW_HEIGHT_REM}rem` }
      : undefined;

  return (
    <>
      <table
        className={cx(minWidthClass, 'w-full font-mono text-xs', className)}
      >
        <thead className="table-header sticky top-0 z-10">
          <tr className="text-[10px] uppercase tracking-wider">
            <th className="text-left px-3 py-2 whitespace-nowrap">Timestamp</th>
            {showPrincipal && (
              <th className="text-left px-3 py-2 whitespace-nowrap">
                Principal
              </th>
            )}
            {showUpstream && (
              <th className="text-left px-3 py-2 whitespace-nowrap">
                Upstream
              </th>
            )}
            {showSession && (
              <th className="text-left px-3 py-2 whitespace-nowrap">Session</th>
            )}
            <th className="text-left px-3 py-2 whitespace-nowrap">Model</th>
            <th className="text-right px-3 py-2 whitespace-nowrap">Status</th>
            <th className="text-right px-3 py-2 whitespace-nowrap">Latency</th>
            {showTokens && (
              <th className="text-right px-3 py-2 whitespace-nowrap">Token</th>
            )}
            {showCost && (
              <th className="text-right px-3 py-2 whitespace-nowrap">Cost</th>
            )}
          </tr>
        </thead>
        <tbody style={reservedBodyStyle}>
          {loading ? (
            Array.from({ length: reservedRowCount }).map((_, i) => (
              <SkeletonRow
                key={i}
                cols={colCount}
                style={REQUEST_EVENT_ROW_STYLE}
                cellClassNames={loadingCellClassNames}
                skeletonClassNames={loadingSkeletonClassNames}
              />
            ))
          ) : events.length ? (
            events.map((e) => {
              const isPartial = e._phase === 'partial';
              const key = e.event_id ?? e.request_id;
              const outcome = getRequestOutcome(
                isPartial,
                e._phase === 'final' ? e.status : 0,
                e._phase === 'final' ? e.error_code : undefined,
                e._phase === 'final' ? e.upstream_error_type : undefined,
              );
              const tierBadge = serviceTierBadgeText(e.service_tier);
              const requestKindBadge = requestKindBadgeText(e.request_kind);
              return (
                <tr
                  key={key}
                  className={cx(
                    'border-b border-row hover:bg-overlay-1 focus:bg-overlay-1 focus:outline-none cursor-pointer',
                    liveFlashIds?.has(key) ? 'flash-in' : '',
                  )}
                  style={REQUEST_EVENT_ROW_STYLE}
                  onClick={() => setSelectedId(key)}
                  tabIndex={0}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter' || e.key === ' ') {
                      e.preventDefault();
                      setSelectedId(key);
                    }
                  }}
                  aria-selected={selectedId === key}
                  aria-label={`View request ${key}`}
                >
                  <td className="group/ts relative px-3 py-2 text-text-faint whitespace-nowrap">
                    <span
                      className={cx(
                        'status-dot mr-2',
                        outcome.type === 'partial'
                          ? 'neutral animate-pulse'
                          : requestOutcomeTone(outcome),
                      )}
                    />
                    <RelativeTime compact ts={eventTime(e)} />
                    {onAnchorRange != null && eventTime(e) != null ? (
                      <span className="absolute inset-y-0 right-0 hidden items-center gap-0.5 bg-[color:var(--color-panel-strong)] pl-2 pr-1 group-hover/ts:flex group-focus-within/ts:flex">
                        {ANCHOR_RADII_SECS.map(([label, radius]) => (
                          <button
                            key={label}
                            type="button"
                            className="rounded-sm border border-subtle px-1 text-[10px] leading-4 hover:bg-[color:var(--color-hover-bg)]"
                            title={`Show ${label} around this request`}
                            onClick={(event) => {
                              event.stopPropagation();
                              const at = eventTime(e);
                              if (at != null)
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
                    <td className="px-3 py-2 whitespace-nowrap truncate max-w-[160px]">
                      {(e.principal_id &&
                        principalNameMap.get(e.principal_id)) ??
                        e.principal_id ??
                        DASH}
                    </td>
                  )}
                  {showUpstream && (
                    <td className="px-3 py-2 whitespace-nowrap truncate max-w-[180px]">
                      {upstreamNameMap.get(e.upstream ?? '') ??
                        e.upstream_name ??
                        e.upstream ??
                        DASH}
                    </td>
                  )}
                  {showSession && (
                    <td className="px-3 py-2 whitespace-nowrap">
                      <span className="flex items-center gap-1.5 min-w-0">
                        <SessionChip sessionId={e.thread_id ?? null} />
                        {requestKindBadge != null && (
                          <Badge tone="mono" className="shrink-0">
                            {requestKindBadge}
                          </Badge>
                        )}
                      </span>
                    </td>
                  )}
                  <td className="px-3 py-2 text-text-muted truncate max-w-[260px]">
                    <span className="flex items-center gap-2 min-w-0">
                      <span className="truncate">{e.model ?? DASH}</span>
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
                      outcome.type === 'partial'
                        ? 'text-text-faint'
                        : STATUS_TONE_TEXT[requestOutcomeTone(outcome)],
                    )}
                  >
                    <RequestOutcomeTableCell outcome={outcome} />
                  </td>
                  <LatencyCell event={e} isPartial={isPartial} />
                  {showTokens && <TokenCell event={e} isPartial={isPartial} />}
                  {showCost && <CostCell event={e} isPartial={isPartial} />}
                </tr>
              );
            })
          ) : (
            <tr className="h-full">
              <td
                colSpan={colCount}
                className="px-3 py-8 align-middle text-center text-text-faint text-xs"
              >
                <div className="flex flex-col items-center justify-center text-center py-4">
                  <h3 className="text-sm font-medium text-text">
                    {emptyTitle}
                  </h3>
                  {emptyDescription && (
                    <p className="mt-1 text-xs text-text-faint max-w-md">
                      {emptyDescription}
                    </p>
                  )}
                </div>
              </td>
            </tr>
          )}
          {events.length > 0 && sentinelRef && (
            <tr ref={sentinelRef}>
              <td
                colSpan={colCount}
                className="px-3 py-4 text-center text-text-faint text-[11px]"
              >
                {loadingMore ? 'Loading…' : hasMore ? '' : 'No more entries'}
              </td>
            </tr>
          )}
        </tbody>
      </table>
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
}
