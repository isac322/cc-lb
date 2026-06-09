import type React from 'react';
import { eventTime, type RequestEvent } from '../../lib/api';
import { fmtUsd, fmtUsdCompact, splitNum } from '../../lib/format';
import { cx, Hint, SkeletonRow } from './primitives';
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
  minWidthClass = 'min-w-[960px]',
  className,
}: RequestEventsTableProps) {
  const showPrincipal = columns?.principal ?? true;
  const showUpstream = columns?.upstream ?? true;
  const showTokens = columns?.tokens ?? true;
  const showCost = columns?.cost ?? true;

  // Always shown: Timestamp, Model, Status, Latency (4)
  // Toggleable: Principal, Upstream, Tokens, Cost (up to 4)
  const colCount =
    4 +
    (showPrincipal ? 1 : 0) +
    (showUpstream ? 1 : 0) +
    (showTokens ? 1 : 0) +
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
          events.map((e) => (
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
              {showTokens && <TokenCell event={e} />}
              {showCost && <CostCell event={e} />}
            </tr>
          ))
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

// ─── Token cell ──────────────────────────────────────────────────────────────

type TokenBreakdown = {
  input: number;
  output: number;
  cc_5m: number;
  cc_1h: number;
  cr: number;
};

function tokenBreakdown(e: RequestEvent): TokenBreakdown {
  const input = e.input_tokens ?? 0;
  const output = e.output_tokens ?? 0;
  const cr = e.cache_read_input_tokens ?? 0;
  // Split fields take precedence; fall back to legacy total (treated as 5m).
  const cc_5m_split = e.cache_creation_input_tokens_5m;
  const cc_1h_split = e.cache_creation_input_tokens_1h;
  if (cc_5m_split != null || cc_1h_split != null) {
    return {
      input,
      output,
      cc_5m: cc_5m_split ?? 0,
      cc_1h: cc_1h_split ?? 0,
      cr,
    };
  }
  const legacy = e.cache_creation_input_tokens ?? 0;
  return { input, output, cc_5m: legacy, cc_1h: 0, cr };
}

function hitRatioPercent(b: TokenBreakdown): number {
  const denom = b.input + b.cr;
  if (denom <= 0) return 0;
  return Math.round((b.cr / denom) * 100);
}

function TokenCell({ event }: { event: RequestEvent }) {
  const b = tokenBreakdown(event);
  const hit = hitRatioPercent(b);
  const inp = splitNum(b.input);
  const out = splitNum(b.output);

  const popover = (
    <BreakdownPopover
      title="Tokens"
      rows={[
        { label: 'Input', value: b.input, color: 'bg-sky-400', fmt: fmtTokens },
        {
          label: 'Output',
          value: b.output,
          color: 'bg-violet-400',
          fmt: fmtTokens,
        },
        {
          label: 'Cache create 5m',
          value: b.cc_5m,
          color: 'bg-amber-400',
          fmt: fmtTokens,
        },
        {
          label: 'Cache create 1h',
          value: b.cc_1h,
          color: 'bg-amber-700',
          fmt: fmtTokens,
        },
        {
          label: 'Cache read',
          value: b.cr,
          color: 'bg-emerald-400',
          fmt: fmtTokens,
        },
      ]}
    />
  );

  return (
    <td
      className="p-0 text-right whitespace-nowrap"
      onClick={(e) => e.stopPropagation()}
    >
      <Hint label={popover}>
        <div className="px-3 py-2 cursor-help inline-block w-full">
          <div className="flex items-baseline justify-end gap-0 tabular-nums">
            <span className="inline-block w-[4ch] text-right text-sky-400">
              {inp.value}
            </span>
            <span className="inline-block w-[1ch] text-text-faint">
              {inp.unit}
            </span>
            <span className="inline-block w-[1ch] text-text-faint text-center">
              /
            </span>
            <span className="inline-block w-[4ch] text-right text-violet-400">
              {out.value}
            </span>
            <span className="inline-block w-[1ch] text-text-faint">
              {out.unit}
            </span>
            <span className="inline-block ml-2 text-[10px] text-text-faint">
              hit{' '}
              <span
                className={cx(
                  'tabular-nums',
                  hit > 0 ? 'text-emerald-400' : 'text-text-faint',
                )}
              >
                {hit}%
              </span>
            </span>
          </div>
          <Sparkline
            segments={[
              { value: b.input, color: 'bg-sky-400' },
              { value: b.output, color: 'bg-violet-400' },
              { value: b.cc_5m, color: 'bg-amber-400' },
              { value: b.cc_1h, color: 'bg-amber-700' },
              { value: b.cr, color: 'bg-emerald-400' },
            ]}
          />
        </div>
      </Hint>
    </td>
  );
}

// ─── Cost cell ───────────────────────────────────────────────────────────────

type CostBreakdownT = {
  input: number;
  output: number;
  cc_5m: number;
  cc_1h: number;
  cr: number;
  total: number;
  hasComponents: boolean;
};

function costBreakdown(e: RequestEvent): CostBreakdownT {
  const input = e.cost_input_micros ?? 0;
  const output = e.cost_output_micros ?? 0;
  const cc_5m = e.cost_cache_creation_5m_micros ?? 0;
  const cc_1h = e.cost_cache_creation_1h_micros ?? 0;
  const cr = e.cost_cache_read_micros ?? 0;
  const hasComponents =
    e.cost_input_micros != null ||
    e.cost_output_micros != null ||
    e.cost_cache_creation_5m_micros != null ||
    e.cost_cache_creation_1h_micros != null ||
    e.cost_cache_read_micros != null;
  const total = e.cost_usd_micros ?? input + output + cc_5m + cc_1h + cr;
  return { input, output, cc_5m, cc_1h, cr, total, hasComponents };
}

function CostCell({ event }: { event: RequestEvent }) {
  const c = costBreakdown(event);

  // No cost data at all: render plain dash, no popover.
  if (event.cost_usd_micros == null && !c.hasComponents) {
    return (
      <td className="px-3 py-2 text-right tabular-nums whitespace-nowrap text-text-faint">
        {DASH}
      </td>
    );
  }

  const popover = (
    <BreakdownPopover
      title="Cost"
      rows={[
        {
          label: 'Input',
          value: c.input,
          color: 'bg-sky-400',
          fmt: fmtUsd,
        },
        {
          label: 'Output',
          value: c.output,
          color: 'bg-violet-400',
          fmt: fmtUsd,
        },
        {
          label: 'Cache create 5m',
          value: c.cc_5m,
          color: 'bg-amber-400',
          fmt: fmtUsd,
        },
        {
          label: 'Cache create 1h',
          value: c.cc_1h,
          color: 'bg-amber-700',
          fmt: fmtUsd,
        },
        {
          label: 'Cache read',
          value: c.cr,
          color: 'bg-emerald-400',
          fmt: fmtUsd,
        },
      ]}
      footer={
        c.hasComponents && c.total > 0
          ? { label: 'Total', value: c.total, fmt: fmtUsd }
          : null
      }
    />
  );

  return (
    <td
      className="p-0 text-right whitespace-nowrap"
      onClick={(e) => e.stopPropagation()}
    >
      <Hint label={popover}>
        <div className="px-3 py-2 cursor-help inline-block w-full">
          <div className="text-right tabular-nums">
            {fmtUsdCompact(c.total)}
          </div>
          {c.hasComponents ? (
            <Sparkline
              segments={[
                { value: c.input, color: 'bg-sky-400' },
                { value: c.output, color: 'bg-violet-400' },
                { value: c.cc_5m, color: 'bg-amber-400' },
                { value: c.cc_1h, color: 'bg-amber-700' },
                { value: c.cr, color: 'bg-emerald-400' },
              ]}
            />
          ) : (
            <div className="mt-1 h-1" />
          )}
        </div>
      </Hint>
    </td>
  );
}

// ─── Sparkline ───────────────────────────────────────────────────────────────

interface SparkSegment {
  value: number;
  color: string;
}

function Sparkline({ segments }: { segments: SparkSegment[] }) {
  const total = segments.reduce((a, s) => a + Math.max(0, s.value), 0);
  if (total <= 0) {
    return <div className="mt-1 h-1 rounded-full bg-overlay-1" />;
  }
  return (
    <div className="mt-1 h-1 w-full rounded-full overflow-hidden flex bg-overlay-1">
      {segments.map((s, i) =>
        s.value > 0 ? (
          <span
            key={i}
            className={cx('h-full', s.color)}
            style={{ width: `${(s.value / total) * 100}%` }}
          />
        ) : null,
      )}
    </div>
  );
}

// ─── Breakdown popover ───────────────────────────────────────────────────────

interface PopoverRow {
  label: string;
  value: number;
  color: string;
  fmt: (v: number) => string;
}

function BreakdownPopover({
  title,
  rows,
  footer,
}: {
  title: string;
  rows: PopoverRow[];
  footer?: { label: string; value: number; fmt: (v: number) => string } | null;
}) {
  const visible = rows.filter((r) => r.value > 0);
  const total = rows.reduce((a, r) => a + r.value, 0);
  return (
    <div className="min-w-[200px] font-mono">
      <div className="text-[10px] uppercase tracking-wider text-text-faint mb-1.5">
        {title}
      </div>
      {visible.length === 0 ? (
        <div className="text-[11px] text-text-faint">—</div>
      ) : (
        <div className="flex flex-col gap-1">
          {visible.map((r) => {
            const pct = total > 0 ? Math.round((r.value / total) * 100) : 0;
            return (
              <div
                key={r.label}
                className="flex items-center gap-2 text-[11px]"
              >
                <span
                  className={cx('h-2 w-2 rounded-full shrink-0', r.color)}
                />
                <span className="text-text-muted flex-1 truncate">
                  {r.label}
                </span>
                <span className="tabular-nums text-text w-14 text-right">
                  {r.fmt(r.value)}
                </span>
                <span className="tabular-nums text-text-faint w-9 text-right">
                  {pct}%
                </span>
              </div>
            );
          })}
        </div>
      )}
      {footer ? (
        <div className="border-t border-subtle mt-1.5 pt-1 flex items-center gap-2 text-[11px]">
          <span className="h-2 w-2 shrink-0" />
          <span className="text-text-faint flex-1">{footer.label}</span>
          <span className="tabular-nums text-text w-14 text-right">
            {footer.fmt(footer.value)}
          </span>
          <span className="w-9" />
        </div>
      ) : null}
    </div>
  );
}

function fmtTokens(v: number): string {
  const { value, unit } = splitNum(v);
  return value + unit;
}
