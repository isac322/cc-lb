import type React from 'react';
import { eventTime, type RequestEvent } from '../../lib/api';
import { cacheHitRatio, fmtUsd } from '../../lib/format';
import { cx, SkeletonRow } from './primitives';
import { RelativeTime } from './RelativeTime';

const DASH = '—';

interface RequestEventsTableProps {
  events: RequestEvent[];
  principalNameMap: Map<string, string>;
  upstreamNameMap: Map<string, string>;
  loading?: boolean;
  emptyTitle?: string;
  emptyDescription?: string;
  liveFlashIds?: Set<string>;
  onRowClick?: (event: RequestEvent) => void;
  columns?: {
    principal?: boolean;
    upstream?: boolean;
    cache?: boolean;
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
  onRowClick,
  columns,
  sentinelRef,
  loadingMore,
  hasMore,
  minWidthClass = 'min-w-[820px]',
  className,
}: RequestEventsTableProps) {
  const showPrincipal = columns?.principal ?? true;
  const showUpstream = columns?.upstream ?? true;
  const showTokens = columns?.tokens ?? true;
  const showCache = columns?.cache ?? false;
  const showCost = columns?.cost ?? true;

  // Always shown: Timestamp, Model, Status, Latency (4)
  // Toggleable: Principal, Upstream, Tokens, Cache, Cost (up to 5)
  const colCount =
    4 +
    (showPrincipal ? 1 : 0) +
    (showUpstream ? 1 : 0) +
    (showTokens ? 1 : 0) +
    (showCache ? 1 : 0) +
    (showCost ? 1 : 0);

  return (
    <table className={cx(minWidthClass, 'w-full font-mono text-xs', className)}>
      <thead className="table-header sticky top-0 z-10">
        <tr className="text-[10px] uppercase tracking-wider">
          <th className="text-left px-3 py-2 whitespace-nowrap">Timestamp</th>
          {showPrincipal && (
            <th className="text-left px-3 py-2 whitespace-nowrap">Principal</th>
          )}
          {showUpstream && (
            <th className="text-left px-3 py-2 whitespace-nowrap">Upstream</th>
          )}
          <th className="text-left px-3 py-2 whitespace-nowrap">Model</th>
          <th className="text-right px-3 py-2 whitespace-nowrap">Status</th>
          <th className="text-right px-3 py-2 whitespace-nowrap">Latency</th>
          {showTokens && (
            <th className="text-right px-3 py-2 whitespace-nowrap">
              Tokens I/O
            </th>
          )}
          {showCache && (
            <th className="text-right px-3 py-2 whitespace-nowrap">Cache</th>
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
            const ratio = showCache ? cacheHitRatio(e) : null;
            return (
              <tr
                key={e.request_id}
                className={cx(
                  'border-b border-row hover:bg-overlay-1',
                  onRowClick ? 'cursor-pointer' : '',
                  liveFlashIds?.has(e.request_id) ? 'flash-in' : '',
                )}
                onClick={() => onRowClick?.(e)}
              >
                <td className="px-3 py-2 text-text-faint whitespace-nowrap">
                  <span
                    className={cx(
                      'status-dot mr-2',
                      e.status >= 500
                        ? 'danger'
                        : e.status >= 400
                          ? 'warn'
                          : 'ok',
                    )}
                  />
                  <RelativeTime ts={eventTime(e)} />
                </td>
                {showPrincipal && (
                  <td className="px-3 py-2 whitespace-nowrap truncate max-w-[160px]">
                    {(e.principal_id && principalNameMap.get(e.principal_id)) ??
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
                <td className="px-3 py-2 text-text-muted truncate max-w-[260px]">
                  {e.model ?? DASH}
                </td>
                <td
                  className={cx(
                    'px-3 py-2 text-right tabular-nums whitespace-nowrap',
                    e.status >= 500
                      ? 'text-red-400'
                      : e.status >= 400
                        ? 'text-amber-400'
                        : 'text-green-400',
                  )}
                >
                  {e.status}
                </td>
                <td className="px-3 py-2 text-right tabular-nums whitespace-nowrap">
                  {e.duration_ms}ms
                </td>
                {showTokens && (
                  <td className="px-3 py-2 text-right tabular-nums whitespace-nowrap">
                    {e.input_tokens ?? 0} / {e.output_tokens ?? 0}
                  </td>
                )}
                {showCache && (
                  <td className="px-3 py-2 text-right tabular-nums text-text-muted whitespace-nowrap">
                    {ratio == null ? DASH : `${(ratio * 100).toFixed(0)}%`}
                  </td>
                )}
                {showCost && (
                  <td className="px-3 py-2 text-right tabular-nums whitespace-nowrap">
                    {fmtUsd(e.cost_usd_micros)}
                  </td>
                )}
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
                <h3 className="text-sm font-medium text-text">{emptyTitle}</h3>
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
  );
}
