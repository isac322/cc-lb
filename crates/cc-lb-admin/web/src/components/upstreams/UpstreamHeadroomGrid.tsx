// The upstream list as an instrument row: a fixed end-label grid
// (Upstream | Plan | 5h | 7d | 7d (Fable)) where every window cell reads
// what is LEFT ("N% left" numeral + linear headroom meter + reset line).
// `strip` links each row to the upstream (Overview); `list` makes each row a
// selectable button (Upstreams). Below the container's wide breakpoint the
// row stacks: name and plan on top, then the three window cells, each
// repeating its label.
import { Link } from '@tanstack/react-router';
import { type ReactNode, useMemo } from 'react';
import type {
  LatestResponse,
  OrganizationMetadataInner,
  QuotaSnapshot,
} from '../../lib/api';
import { WINDOW_LABELS } from '../../lib/api';
import { sumTokens } from '../../lib/format';
import { useTimezone } from '../../lib/locale';
import {
  type OAuthReconnectNudge,
  useOAuthReconnectNudges,
} from '../../lib/oauthReconnect';
import {
  type Upstream,
  useStatus,
  useSubscriptionQuotaLatest,
  useUpstreamSubscriptionMetadata,
  useUpstreams,
  useUsage,
} from '../../lib/queries';
import {
  formatHeadroomValue,
  formatQuotaPercent,
  QUOTA_SEVERITY_TEXT_CLASS,
  quotaSeverity,
} from '../../lib/quotaSeverity';
import { HeadroomMeter } from '../ui/Gauge';
import { cx, Hint, Skeleton } from '../ui/primitives';
import { SidebarCouponNudge } from './CouponNudge';
import {
  QuotaFreshnessCaption,
  type UpstreamHealth,
  upstreamHealth,
} from './upstreamHealth';

/** The three windows every row reads, in column order. */
export const HEADROOM_GRID_WINDOWS = ['5h', '7d', '7d_fable'] as const;
export type HeadroomGridWindow = (typeof HEADROOM_GRID_WINDOWS)[number];

/**
 * The `/latest` window set the Upstreams page polls. Overview and Upstreams
 * both pass it so TanStack dedups the poll into one request.
 */
export const UPSTREAM_LATEST_WINDOWS =
  '5h,7d,overage,7d_sonnet,7d_opus,7d_fable';

export interface UpstreamUsage7d {
  cost_usd: number;
  tokens: number;
}

export interface UpstreamHeadroomRow {
  upstream: Upstream;
  /** One status for the dot: disabled, reconnect, error or active. */
  health: UpstreamHealth;
  /** OAuth reconnect nudge; `null` when none (or not OAuth). */
  nudge: OAuthReconnectNudge | null;
  /** Most recent failed reconciliation, if any. */
  runtimeError: string | null;
  /** Latest quota snapshots for this upstream; `[]` when none. */
  windows: QuotaSnapshot[];
  /** Spend and tokens over the last 7 days; `null` when unknown. */
  usage7d: UpstreamUsage7d | null;
}

export interface UpstreamHeadroomData {
  rows: UpstreamHeadroomRow[];
  /** The upstream list itself is still loading. */
  isLoading: boolean;
  quotaPending: boolean;
  statusPending: boolean;
  usagePending: boolean;
  quotaError: boolean;
  /** Rows carrying a reconnect nudge (warn or danger). */
  reconnectCount: number;
}

interface RuntimeStatusEntry {
  id: string;
  status: string;
  last_apply_error: string | null;
}

/** Pure row shaping; `useUpstreamHeadroomData` feeds it from the queries. */
export function buildUpstreamHeadroomRows({
  upstreams,
  latest,
  status,
  nudges,
  usageByUpstreamId,
}: {
  upstreams: readonly Upstream[];
  latest: LatestResponse | undefined;
  status: readonly RuntimeStatusEntry[] | undefined;
  nudges: ReadonlyMap<string, OAuthReconnectNudge>;
  usageByUpstreamId: ReadonlyMap<string, UpstreamUsage7d>;
}): UpstreamHeadroomRow[] {
  return upstreams.map((upstream) => {
    const runtime = status?.find((s) => s.id === upstream.id);
    const nudge = nudges.get(upstream.id) ?? null;
    return {
      upstream,
      health: upstreamHealth(upstream.enabled, runtime?.status, nudge),
      nudge,
      runtimeError: runtime?.last_apply_error ?? null,
      windows:
        latest?.upstreams.find((l) => l.upstream_id === upstream.id)?.windows ??
        [],
      usage7d: usageByUpstreamId.get(upstream.id) ?? null,
    };
  });
}

/**
 * Everything the grid reads: upstreams, the 5s `/latest` poll (same key as
 * the Upstreams page), runtime status, OAuth reconnect nudges and 7-day
 * spend per upstream (for API-key rows).
 */
export function useUpstreamHeadroomData(): UpstreamHeadroomData {
  const upstreamsQ = useUpstreams();
  const upstreams = useMemo(
    () => upstreamsQ.data?.upstreams ?? [],
    [upstreamsQ.data],
  );
  const upstreamIds = useMemo(
    () => upstreams.map((u) => u.id).join(','),
    [upstreams],
  );
  const latestQ = useSubscriptionQuotaLatest({
    upstreamIds,
    windows: UPSTREAM_LATEST_WINDOWS,
    source: 'merged',
    refetchInterval: 5_000,
  });
  const statusQ = useStatus();
  const usageQ = useUsage('7d', 'hour', 'upstream', undefined, 'totals');
  const { nudges } = useOAuthReconnectNudges(upstreams);

  const usageByUpstreamId = useMemo(() => {
    const m = new Map<string, UpstreamUsage7d>();
    for (const series of usageQ.data?.series ?? []) {
      let cost = 0;
      let tokens = 0;
      for (const b of series.buckets) {
        cost += (b.virtual_cost_micros ?? 0) / 1_000_000;
        tokens += sumTokens(b);
      }
      m.set(series.key, { cost_usd: cost, tokens });
    }
    return m;
  }, [usageQ.data]);

  const rows = useMemo(
    () =>
      buildUpstreamHeadroomRows({
        upstreams,
        latest: latestQ.data,
        status: statusQ.data?.upstreams,
        nudges,
        usageByUpstreamId,
      }),
    [upstreams, latestQ.data, statusQ.data, nudges, usageByUpstreamId],
  );

  return {
    rows,
    isLoading: upstreamsQ.isLoading,
    quotaPending: latestQ.data === undefined && latestQ.isPending,
    statusPending: statusQ.data === undefined && statusQ.isPending,
    usagePending: usageQ.data === undefined && usageQ.isPending,
    quotaError: latestQ.isError,
    reconnectCount: rows.filter((r) => r.nudge != null).length,
  };
}

// ─── Time helpers (shared with the Upstreams detail window cards) ───────────

const STAMP_FORMATTERS = new Map<string, Intl.DateTimeFormat>();
function stampFormatter(timeZone: string): Intl.DateTimeFormat {
  let fmt = STAMP_FORMATTERS.get(timeZone);
  if (!fmt) {
    fmt = new Intl.DateTimeFormat('en-US', {
      timeZone,
      weekday: 'short',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      hourCycle: 'h23',
    });
    STAMP_FORMATTERS.set(timeZone, fmt);
  }
  return fmt;
}

/** "Fri 10-02 00:59" in the operator's timezone. */
export function formatQuotaStamp(unixSecs: number, timeZone: string): string {
  const parts = Object.fromEntries(
    stampFormatter(timeZone)
      .formatToParts(new Date(unixSecs * 1000))
      .map((p) => [p.type, p.value]),
  );
  return `${parts.weekday} ${parts.month}-${parts.day} ${parts.hour}:${parts.minute}`;
}

/** "4d 4h" / "3h 12m" / "8m" / "<1m": the two largest units, rounded down. */
export function formatSpan(secs: number): string {
  const s = Math.max(0, Math.floor(secs));
  const d = Math.floor(s / 86_400);
  const h = Math.floor((s % 86_400) / 3_600);
  const m = Math.floor((s % 3_600) / 60);
  if (d > 0) return h > 0 ? `${d}d ${h}h` : `${d}d`;
  if (h > 0) return m > 0 ? `${h}h ${m}m` : `${h}h`;
  if (m > 0) return `${m}m`;
  return '<1m';
}

/**
 * The reset fact of a window: "Resets Fri 10-02 00:59 · in 4d 4h", "Reset
 * was due Sat 09-26 04:59" once passed, "No reset reported" without one.
 */
export function formatResetLine(
  resetUnixSecs: number | null | undefined,
  nowUnixSecs: number,
  timeZone: string,
): string {
  if (resetUnixSecs == null) return 'No reset reported';
  const stamp = formatQuotaStamp(resetUnixSecs, timeZone);
  if (resetUnixSecs <= nowUnixSecs) return `Reset was due ${stamp}`;
  return `Resets ${stamp} · in ${formatSpan(resetUnixSecs - nowUnixSecs)}`;
}

// ─── Plan label ─────────────────────────────────────────────────────────────

/** "Claude Max 20x" from the rate-limit tier, else the organization type. */
export function subscriptionPlanLabel(
  orgMeta:
    | Pick<OrganizationMetadataInner, 'organization_type' | 'rate_limit_tier'>
    | null
    | undefined,
): string | null {
  const tier = orgMeta?.rate_limit_tier ?? '';
  const type = orgMeta?.organization_type ?? '';
  const multiplier = /max_(\d+)x/i.exec(tier)?.[1];
  if (multiplier) return `Claude Max ${multiplier}x`;
  if (/max/i.test(type)) return 'Claude Max';
  if (/pro/i.test(type)) return 'Claude Pro';
  if (/team/i.test(type)) return 'Claude Team';
  if (/enterprise/i.test(type)) return 'Claude Enterprise';
  return type ? type.replace(/_/g, ' ') : null;
}

function OAuthPlan({ upstreamId }: { upstreamId: string }) {
  const meta = useUpstreamSubscriptionMetadata(upstreamId);
  const label = subscriptionPlanLabel(meta.data?.organization_metadata);
  if (meta.data === undefined && meta.isPending) {
    return <Skeleton className="h-4 w-24" />;
  }
  return <span className="text-body text-text">{label ?? 'Subscription'}</span>;
}

function PlanCell({ upstream }: { upstream: Upstream }) {
  const oauth = upstream.kind === 'anthropic_oauth';
  return (
    <div className="flex min-w-0 flex-col gap-0.5">
      {oauth ? (
        <OAuthPlan upstreamId={upstream.id} />
      ) : (
        <span className="text-body text-text">API key</span>
      )}
      <span className="truncate font-mono text-data text-text-faint">
        {upstream.kind}
      </span>
    </div>
  );
}

// ─── Cells ──────────────────────────────────────────────────────────────────

function WindowCell({
  windowName,
  snap,
  pending,
  nowUnixSecs,
  timeZone,
}: {
  windowName: HeadroomGridWindow;
  snap: QuotaSnapshot | undefined;
  pending: boolean;
  nowUnixSecs: number;
  timeZone: string;
}) {
  const label = WINDOW_LABELS[windowName];
  const used = snap?.utilization == null ? null : snap.utilization * 100;
  const severity = quotaSeverity(used);
  const status = snap?.status && snap.status !== 'allowed' ? snap.status : null;
  return (
    <div
      className="flex min-w-0 flex-col gap-1.5"
      data-testid="headroom-cell"
      data-window={windowName}
    >
      {/* Below the wide breakpoint the header row is gone: repeat the label. */}
      <span className="text-label text-text-muted @4xl:hidden">{label}</span>
      {pending ? (
        <>
          <Skeleton className="h-8 w-16" />
          <Skeleton className="h-1 w-full" />
          <Skeleton className="h-3 w-32" />
        </>
      ) : (
        <>
          <span className="flex flex-wrap items-baseline justify-between gap-x-2 tabular-nums">
            <span
              className={cx(
                'text-display leading-none',
                severity === 'warn' || severity === 'danger'
                  ? QUOTA_SEVERITY_TEXT_CLASS[severity]
                  : used == null
                    ? 'text-text-faint'
                    : 'text-text',
              )}
            >
              {formatHeadroomValue(used)}
              {used == null ? null : (
                <span className="ml-0.5 text-body-sm text-text-muted">
                  % left
                </span>
              )}
            </span>
            <span
              className={cx(
                'text-caption',
                severity === 'danger' || severity === 'warn'
                  ? QUOTA_SEVERITY_TEXT_CLASS[severity]
                  : 'text-text-muted',
              )}
            >
              {used == null
                ? 'No reading'
                : `${formatQuotaPercent(used)} used${status ? ` · ${status}` : ''}`}
            </span>
          </span>
          <HeadroomMeter label={label} usedPct={used} />
          <span className="text-caption text-text-muted">
            {snap
              ? formatResetLine(snap.resets_at_unix_secs, nowUnixSecs, timeZone)
              : 'No reading'}
          </span>
        </>
      )}
    </div>
  );
}

function formatTokens(tokens: number): string {
  if (tokens >= 1_000_000) return `${(tokens / 1_000_000).toFixed(1)}M`;
  if (tokens >= 1_000) return `${(tokens / 1_000).toFixed(1)}K`;
  return String(tokens);
}

function ApiKeyCells({
  usage,
  pending,
}: {
  usage: UpstreamUsage7d | null;
  pending: boolean;
}) {
  return (
    <div className="flex min-w-0 flex-col justify-center gap-1 @4xl:col-span-3">
      <span className="text-body text-text-muted">No subscription quota</span>
      {pending ? (
        <Skeleton className="h-3 w-32" />
      ) : (
        <Hint label="Spend and total tokens over the last 7 days">
          <span className="w-fit text-caption tabular-nums text-text-muted">
            {usage
              ? `$${usage.cost_usd.toFixed(2)} · ${formatTokens(usage.tokens)} tok · 7 days`
              : '—'}
          </span>
        </Hint>
      )}
    </div>
  );
}

function StatusLine({
  row,
  pending,
}: {
  row: UpstreamHeadroomRow;
  pending: boolean;
}) {
  if (pending) return <Skeleton className="h-4 w-28" />;
  const { health, nudge } = row;
  // A reconnect nudge is the most specific problem; otherwise the health.
  const tone = nudge?.tone ?? health.tone;
  const text = nudge?.label ?? health.label;
  return (
    <span
      className={cx(
        'inline-flex min-w-0 items-center gap-1.5 text-body-sm',
        tone === 'danger'
          ? 'text-danger-text'
          : tone === 'warn'
            ? 'text-warn-text'
            : 'text-text-muted',
      )}
      title={row.runtimeError ?? undefined}
    >
      <span aria-hidden="true" className={cx('status-dot shrink-0', tone)} />
      <span className="min-w-0 break-words">{text}</span>
    </span>
  );
}

function NameCell({
  row,
  data,
  showCoupon,
}: {
  row: UpstreamHeadroomRow;
  data: UpstreamHeadroomData;
  showCoupon: boolean;
}) {
  const oauth = row.upstream.kind === 'anthropic_oauth';
  const observed = HEADROOM_GRID_WINDOWS.flatMap((w) => {
    const snap = row.windows.find((s) => s.window === w);
    return snap ? [snap] : [];
  });
  return (
    <div className="flex min-w-0 flex-col gap-1">
      <span className="min-w-0 break-words text-title-section text-text">
        {row.upstream.name}
      </span>
      <StatusLine row={row} pending={data.statusPending} />
      {oauth && !data.quotaPending ? (
        <QuotaFreshnessCaption snapshots={observed} />
      ) : null}
      {oauth && showCoupon ? (
        <SidebarCouponNudge
          upstream={row.upstream}
          windows={data.quotaError ? [] : row.windows}
        />
      ) : null}
    </div>
  );
}

const ROW_GRID =
  'grid gap-x-6 gap-y-4 grid-cols-2 @lg:grid-cols-3 ' +
  '@4xl:grid-cols-[minmax(12rem,1.5fr)_minmax(8rem,0.9fr)_repeat(3,minmax(0,1fr))] @4xl:gap-y-0';

function RowBody({
  row,
  data,
  nowUnixSecs,
  timeZone,
  showCoupon,
}: {
  row: UpstreamHeadroomRow;
  data: UpstreamHeadroomData;
  nowUnixSecs: number;
  timeZone: string;
  showCoupon: boolean;
}) {
  const oauth = row.upstream.kind === 'anthropic_oauth';
  return (
    <>
      <div className="col-span-2 flex items-start justify-between gap-4 @lg:col-span-3 @4xl:contents">
        <NameCell row={row} data={data} showCoupon={showCoupon} />
        <PlanCell upstream={row.upstream} />
      </div>
      {oauth ? (
        <div className="col-span-2 grid grid-cols-3 gap-4 @lg:col-span-3 @4xl:contents">
          {HEADROOM_GRID_WINDOWS.map((w) => (
            <WindowCell
              key={w}
              nowUnixSecs={nowUnixSecs}
              pending={data.quotaPending}
              snap={row.windows.find((s) => s.window === w)}
              timeZone={timeZone}
              windowName={w}
            />
          ))}
        </div>
      ) : (
        <div className="col-span-2 @lg:col-span-3 @4xl:contents">
          <ApiKeyCells pending={data.usagePending} usage={row.usage7d} />
        </div>
      )}
    </>
  );
}

const ROW_CLASS =
  'w-full rounded-md border bg-panel px-4 py-4 text-left text-text transition-colors @4xl:px-5 ' +
  'focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-2';

export interface UpstreamHeadroomGridProps {
  data: UpstreamHeadroomData;
  /** `strip`: rows link to the upstream. `list`: rows select in place. */
  variant: 'strip' | 'list';
  /** `list` only: the selected upstream (accent outline, `aria-current`). */
  selectedId?: string;
  /** `list` only: called with the row's upstream id. */
  onSelect?: (id: string) => void;
  ariaLabel?: string;
  className?: string;
  /** Rendered in place of the rows when the list is empty and loaded. */
  empty?: ReactNode;
}

export function UpstreamHeadroomGrid({
  data,
  variant,
  selectedId,
  onSelect,
  ariaLabel = 'Upstreams',
  className,
  empty = null,
}: UpstreamHeadroomGridProps) {
  const { effective: timeZone } = useTimezone();
  const nowUnixSecs = Math.floor(Date.now() / 1000);

  if (data.isLoading) {
    return (
      <div className={cx('@container flex flex-col gap-2', className)}>
        {Array.from({ length: 3 }).map((_, i) => (
          <div
            key={i}
            className={cx(ROW_CLASS, ROW_GRID, 'border-border')}
            data-testid="upstream-list-loading-row"
          >
            <div className="col-span-2 flex flex-col gap-2 @lg:col-span-3 @4xl:col-span-2">
              <Skeleton className="h-5 w-32" />
              <Skeleton className="h-4 w-40" />
            </div>
            <div className="col-span-2 grid grid-cols-3 gap-4 @lg:col-span-3 @4xl:contents">
              {HEADROOM_GRID_WINDOWS.map((w) => (
                <div key={w} className="flex flex-col gap-2">
                  <Skeleton className="h-8 w-16" />
                  <Skeleton className="h-1 w-full" />
                </div>
              ))}
            </div>
          </div>
        ))}
      </div>
    );
  }
  if (data.rows.length === 0) return <>{empty}</>;

  return (
    <div className={cx('@container', className)}>
      <div
        aria-hidden="true"
        className={cx(
          'hidden px-5 pb-2 text-label text-text-faint @4xl:grid',
          ROW_GRID,
        )}
      >
        <span>Upstream</span>
        <span>Plan</span>
        {HEADROOM_GRID_WINDOWS.map((w) => (
          <span key={w}>{WINDOW_LABELS[w]}</span>
        ))}
      </div>
      <ul aria-label={ariaLabel} className="flex flex-col gap-2">
        {data.rows.map((row) => {
          const id = row.upstream.id;
          const body = (
            <RowBody
              data={data}
              nowUnixSecs={nowUnixSecs}
              row={row}
              showCoupon={variant === 'list'}
              timeZone={timeZone}
            />
          );
          if (variant === 'strip') {
            return (
              <li key={id}>
                <Link
                  className={cx(
                    ROW_CLASS,
                    ROW_GRID,
                    'border-border hover:bg-panel-strong',
                  )}
                  search={{ selectedId: id }}
                  to="/upstreams"
                >
                  {body}
                </Link>
              </li>
            );
          }
          const selected = id === selectedId;
          return (
            <li key={id}>
              <button
                aria-current={selected ? 'true' : undefined}
                className={cx(
                  ROW_CLASS,
                  ROW_GRID,
                  selected
                    ? 'border-accent shadow-[inset_3px_0_0_var(--color-accent)]'
                    : 'border-border hover:bg-panel-strong',
                )}
                onClick={() => onSelect?.(id)}
                title={row.runtimeError ?? undefined}
                type="button"
              >
                {body}
              </button>
            </li>
          );
        })}
      </ul>
    </div>
  );
}
