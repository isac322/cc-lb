import type React from 'react';
import { useState } from 'react';
import { eventTime } from '../../lib/api';
import { getRequestOutcome, statusTone } from '../../lib/format';
import type { RequestEventWithPhase } from '../../lib/RequestEventTypes';
import {
  reasoningBadgeText,
  serviceTierBadgeText,
} from '../../lib/reasoningTier';
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

const STATUS_TONE_TEXT: Record<'ok' | 'warn' | 'danger' | 'neutral', string> = {
  ok: 'text-[color:var(--color-ok)]',
  warn: 'text-[color:var(--color-warn)]',
  danger: 'text-[color:var(--color-danger)]',
  neutral: 'text-text',
};

interface RequestEventsTableProps {
  events: readonly RequestEventWithPhase[];
  principalNameMap: Map<string, string>;
  upstreamNameMap: Map<string, string>;
  loading?: boolean;
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
}

export function RequestEventsTable({
  events,
  principalNameMap,
  upstreamNameMap,
  loading,
  emptyTitle = 'No requests',
  emptyDescription,
  liveFlashIds,
  columns,
  sentinelRef,
  loadingMore,
  hasMore,
  minWidthClass = 'min-w-[960px]',
  className,
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

  // Always shown: Timestamp, Model, Status, Latency (4)
  // Toggleable: Principal, Upstream, Session, Tokens, Cost (up to 5)
  const colCount =
    4 +
    (showPrincipal ? 1 : 0) +
    (showUpstream ? 1 : 0) +
    (showSession ? 1 : 0) +
    (showTokens ? 1 : 0) +
    (showCost ? 1 : 0);

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
        <tbody>
          {loading ? (
            Array.from({ length: 5 }).map((_, i) => (
              <SkeletonRow key={i} cols={colCount} />
            ))
          ) : events.length ? (
            events.map((e) => {
              const isPartial = e._phase === 'partial';
              const key = e.event_id ?? e.request_id;
              const outcome = getRequestOutcome(
                isPartial,
                e._phase === 'final' ? e.status : 0,
                e._phase === 'final' ? e.error_code : undefined,
              );
              const reasoningBadge = reasoningBadgeText(
                e.reasoning_effort,
                e.thinking_budget_tokens,
                e.thinking_tokens,
              );
              const tierBadge = serviceTierBadgeText(e.service_tier);
              return (
                <tr
                  key={key}
                  className={cx(
                    'border-b border-row hover:bg-overlay-1 focus:bg-overlay-1 focus:outline-none cursor-pointer',
                    liveFlashIds?.has(key) ? 'flash-in' : '',
                  )}
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
                  <td className="px-3 py-2 text-text-faint whitespace-nowrap">
                    <span
                      className={cx(
                        'status-dot mr-2',
                        isPartial
                          ? 'neutral animate-pulse'
                          : e._phase === 'final' && e.status >= 500
                            ? 'danger'
                            : e._phase === 'final' && e.status >= 400
                              ? 'warn'
                              : 'ok',
                      )}
                    />
                    <RelativeTime compact ts={eventTime(e)} />
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
                      <SessionChip sessionId={e.thread_id ?? null} />
                    </td>
                  )}
                  <td className="px-3 py-2 text-text-muted truncate max-w-[260px]">
                    <span className="flex items-center gap-2 min-w-0">
                      <span className="truncate">{e.model ?? DASH}</span>
                      {reasoningBadge != null && (
                        <Badge tone="mono" className="shrink-0">
                          {reasoningBadge}
                        </Badge>
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
                      outcome.type === 'partial'
                        ? 'text-text-faint'
                        : STATUS_TONE_TEXT[
                            statusTone(e._phase === 'final' ? e.status : 0)
                          ],
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
            <tr>
              <td
                colSpan={colCount}
                className="px-3 py-8 text-center text-text-faint text-xs"
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
