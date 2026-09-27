// The Overview's upstream usage table: Upstream | 5h | 7d | 7d (Fable), each
// window cell reading what is USED ("N% used" + UsageMeter + reset caption),
// like the utilization Claude reports. Rows sort by their most-used window
// (problems first, API-key and disabled upstreams last) and link to the
// upstream. Below the container's wide breakpoint a row stacks: the name
// line, then the three window cells side by side, each repeating its label.
import { Link } from '@tanstack/react-router';
import { type ReactNode, useId, useMemo, useState } from 'react';
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
  useUpstreams,
  useUsage,
} from '../../lib/queries';
import {
  formatQuotaPercent,
  QUOTA_SEVERITY_TEXT_CLASS,
  quotaSeverity,
  quotaStatusLabel,
  quotaStatusTone,
} from '../../lib/quotaSeverity';
import { cx, Skeleton } from '../ui/primitives';
import { UsageMeter } from '../ui/UsageMeter';
import { QUOTA_WINDOW_ORDER } from './quotaWindowVisibility';
import { type UpstreamHealth, upstreamHealth } from './upstreamHealth';

/** The three windows every row reads, in column order. */
const USAGE_WINDOWS = ['5h', '7d', '7d_fable'] as const;
type UsageWindow = (typeof USAGE_WINDOWS)[number];

/**
 * The `/latest` window set the Upstreams page polls: every window the
 * detail pane can show. Overview, the list and the detail pane all pass it
 * so TanStack dedups the poll into one request.
 */
export const UPSTREAM_LATEST_WINDOWS = QUOTA_WINDOW_ORDER.join(',');

export interface UpstreamUsage7d {
  cost_usd: number;
  tokens: number;
}

export interface UpstreamUsageRow {
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

export interface UpstreamUsageData {
  rows: UpstreamUsageRow[];
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

/** Pure row shaping; `useUpstreamUsageData` feeds it from the queries. */
export function buildUpstreamUsageRows({
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
}): UpstreamUsageRow[] {
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
export function useUpstreamUsageData(): UpstreamUsageData {
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
      buildUpstreamUsageRows({
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

// ─── Ordering ───────────────────────────────────────────────────────────────

/** The row's most-used window (0-100), or `null` without any reading. */
export function rowMaxUsedPct(row: UpstreamUsageRow): number | null {
  let max: number | null = null;
  for (const snap of row.windows) {
    if (!(USAGE_WINDOWS as readonly string[]).includes(snap.window)) continue;
    if (snap.utilization == null || !Number.isFinite(snap.utilization)) {
      continue;
    }
    const used = snap.utilization * 100;
    if (max == null || used > max) max = used;
  }
  return max;
}

/**
 * Usage order: upstreams that need attention, then subscription upstreams,
 * then API-key upstreams (no subscription quota), then disabled ones; inside
 * each group the most-used window first (no reading last), then by name.
 */
export function sortUpstreamRowsByUsage(
  rows: readonly UpstreamUsageRow[],
): UpstreamUsageRow[] {
  return rows
    .map((row) => {
      const attention =
        row.nudge != null ||
        row.health.tone === 'danger' ||
        row.health.tone === 'warn';
      const group = row.upstream.enabled
        ? attention
          ? 0
          : row.upstream.kind === 'anthropic_oauth'
            ? 1
            : 2
        : 3;
      return { row, group, used: rowMaxUsedPct(row) };
    })
    .sort(
      (a, b) =>
        a.group - b.group ||
        (b.used ?? -1) - (a.used ?? -1) ||
        a.row.upstream.name.localeCompare(b.row.upstream.name),
    )
    .map(({ row }) => row);
}

// ─── Cells ──────────────────────────────────────────────────────────────────

/** "resets in 3h 12m" with the absolute time for the `title`. */
function resetCaption(
  resetUnixSecs: number | null | undefined,
  nowUnixSecs: number,
  timeZone: string,
): { text: string; title?: string } {
  if (resetUnixSecs == null) return { text: 'No reset reported' };
  const title = `Resets ${formatQuotaStamp(resetUnixSecs, timeZone)}`;
  if (resetUnixSecs <= nowUnixSecs) return { text: 'Reset time passed', title };
  return {
    text: `resets in ${formatSpan(resetUnixSecs - nowUnixSecs)}`,
    title,
  };
}

function WindowCell({
  windowName,
  snap,
  pending,
  nowUnixSecs,
  timeZone,
}: {
  windowName: UsageWindow;
  snap: QuotaSnapshot | undefined;
  pending: boolean;
  nowUnixSecs: number;
  timeZone: string;
}) {
  const label = WINDOW_LABELS[windowName];
  const used =
    snap?.utilization == null || !Number.isFinite(snap.utilization)
      ? null
      : snap.utilization * 100;
  const severity = quotaSeverity(used);
  const status = snap?.status && snap.status !== 'allowed' ? snap.status : null;
  const reset = snap
    ? resetCaption(snap.resets_at_unix_secs, nowUnixSecs, timeZone)
    : { text: 'No reading' };
  return (
    <div
      className="flex min-w-0 flex-col gap-1.5"
      data-testid="usage-cell"
      data-window={windowName}
    >
      {/* Below the wide breakpoint the header row is gone: repeat the label. */}
      <span className="text-label text-text-muted @4xl:hidden">{label}</span>
      {pending ? (
        <>
          <Skeleton className="h-5 w-16" />
          <Skeleton className="h-1 w-full" />
          <Skeleton className="h-3 w-24" />
        </>
      ) : (
        <>
          <span className="flex items-baseline gap-1 tabular-nums">
            <span
              className={cx(
                'text-body font-medium',
                used == null
                  ? 'text-text-faint'
                  : severity === 'warn' || severity === 'danger'
                    ? QUOTA_SEVERITY_TEXT_CLASS[severity]
                    : 'text-text',
              )}
            >
              {formatQuotaPercent(used)}
            </span>
            {used == null ? (
              <span className="sr-only">No reading</span>
            ) : (
              <span className="text-caption text-text-muted">used</span>
            )}
          </span>
          <UsageMeter label={`${label} window used`} usedPct={used} />
          <span
            className={cx(
              'text-caption tabular-nums',
              status && quotaStatusTone(status) === 'danger'
                ? 'text-danger-text'
                : status && quotaStatusTone(status) === 'warn'
                  ? 'text-warn-text'
                  : 'text-text-muted',
            )}
            title={
              status
                ? `${quotaStatusLabel(status)} · ${reset.title ?? reset.text}`
                : (reset.title ?? reset.text)
            }
          >
            {status
              ? `${quotaStatusLabel(status)} · ${reset.text}`
              : reset.text}
          </span>
        </>
      )}
    </div>
  );
}

function NameCell({ row }: { row: UpstreamUsageRow }) {
  const { health, nudge, upstream } = row;
  // Healthy by omission: only a problem (or Disabled) earns a status line.
  const tone = nudge?.tone ?? health.tone;
  const text = nudge?.label ?? health.label;
  const showStatus = tone !== 'ok';
  return (
    <div className="flex min-w-0 flex-col gap-0.5">
      <span
        className={cx(
          'truncate text-body',
          upstream.enabled ? 'text-text' : 'text-text-muted',
        )}
        title={upstream.name}
      >
        {upstream.name}
      </span>
      {showStatus ? (
        <span
          className={cx(
            'inline-flex min-w-0 items-center gap-1.5 text-caption',
            tone === 'danger'
              ? 'text-danger-text'
              : tone === 'warn'
                ? 'text-warn-text'
                : 'text-text-muted',
          )}
          data-testid="usage-row-status"
          title={row.runtimeError ?? text}
        >
          <span
            aria-hidden="true"
            className={cx('status-dot shrink-0', tone)}
          />
          <span className="truncate">{text}</span>
        </span>
      ) : null}
    </div>
  );
}

const ROW_GRID =
  'grid gap-x-6 gap-y-3 grid-cols-3 ' +
  '@4xl:grid-cols-[minmax(12rem,1.4fr)_repeat(3,minmax(0,1fr))] @4xl:items-center @4xl:gap-y-0';

const ROW_CLASS =
  'rounded-sm px-3 py-3 text-left text-text transition-colors ' +
  'focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-2';

function RowBody({
  row,
  pending,
  nowUnixSecs,
  timeZone,
}: {
  row: UpstreamUsageRow;
  pending: boolean;
  nowUnixSecs: number;
  timeZone: string;
}) {
  return (
    <>
      <div className="col-span-3 min-w-0 @4xl:col-span-1">
        <NameCell row={row} />
      </div>
      {row.upstream.kind === 'anthropic_oauth' ? (
        USAGE_WINDOWS.map((w) => (
          <WindowCell
            key={w}
            nowUnixSecs={nowUnixSecs}
            pending={pending}
            snap={row.windows.find((s) => s.window === w)}
            timeZone={timeZone}
            windowName={w}
          />
        ))
      ) : (
        <span
          className="col-span-3 text-body-sm text-text-muted"
          data-testid="usage-no-quota"
        >
          No subscription quota
        </span>
      )}
    </>
  );
}

export interface UpstreamUsageTableProps {
  data: UpstreamUsageData;
  ariaLabel?: string;
  className?: string;
  /** Rendered in place of the rows when the list is empty and loaded. */
  empty?: ReactNode;
  /**
   * Rows shown before the "Show all N" toggle; with this many rows or fewer
   * every row shows and there is no toggle.
   */
  collapsedRows?: number;
}

/**
 * Every upstream's usage per window, most-used first. Rows link into the
 * upstream's detail. Past `collapsedRows` the rest sit behind an inline
 * "Show all N" toggle.
 */
export function UpstreamUsageTable({
  data,
  ariaLabel = 'Upstream usage per window',
  className,
  empty = null,
  collapsedRows = 8,
}: UpstreamUsageTableProps) {
  const { effective: timeZone } = useTimezone();
  const nowUnixSecs = Math.floor(Date.now() / 1000);
  const [expanded, setExpanded] = useState(false);
  const listId = useId();
  const sorted = useMemo(() => sortUpstreamRowsByUsage(data.rows), [data.rows]);

  if (data.isLoading) {
    return (
      <div className={cx('@container flex flex-col', className)}>
        {Array.from({ length: 3 }).map((_, i) => (
          <div
            key={i}
            className={cx(
              ROW_CLASS,
              ROW_GRID,
              'border-t border-row first:border-t-0',
            )}
            data-testid="upstream-list-loading-row"
          >
            <div className="col-span-3 flex flex-col gap-1.5 @4xl:col-span-1">
              <Skeleton className="h-5 w-32" />
            </div>
            {USAGE_WINDOWS.map((w) => (
              <div key={w} className="flex flex-col gap-1.5">
                <Skeleton className="h-5 w-16" />
                <Skeleton className="h-1 w-full" />
              </div>
            ))}
          </div>
        ))}
      </div>
    );
  }
  if (sorted.length === 0) return <>{empty}</>;

  const collapsible = sorted.length > collapsedRows;
  const shown =
    collapsible && !expanded ? sorted.slice(0, collapsedRows) : sorted;

  return (
    <div className={cx('@container flex flex-col', className)}>
      <div
        aria-hidden="true"
        className={cx(
          'hidden border-b border-row px-3 pb-2 text-label text-text-faint @4xl:grid',
          ROW_GRID,
        )}
      >
        <span>Upstream</span>
        {USAGE_WINDOWS.map((w) => (
          <span key={w}>{WINDOW_LABELS[w]}</span>
        ))}
      </div>
      <ul aria-label={ariaLabel} className="flex flex-col" id={listId}>
        {shown.map((row) => (
          <li
            key={row.upstream.id}
            className="border-t border-row first:border-t-0"
            data-testid="upstream-usage-row"
          >
            <Link
              className={cx(ROW_CLASS, ROW_GRID, 'hover:bg-overlay-2')}
              search={{ selectedId: row.upstream.id }}
              title={row.runtimeError ?? undefined}
              to="/upstreams"
            >
              <RowBody
                nowUnixSecs={nowUnixSecs}
                pending={data.quotaPending}
                row={row}
                timeZone={timeZone}
              />
            </Link>
          </li>
        ))}
      </ul>
      {collapsible ? (
        <div className="border-t border-row px-3 pt-3">
          <button
            aria-controls={listId}
            aria-expanded={expanded}
            className="inline-flex items-center rounded-sm text-body text-text-muted underline decoration-border-strong underline-offset-4 transition-colors hover:text-text hover:decoration-accent focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-2 max-md:min-h-11"
            data-testid="upstream-usage-toggle"
            onClick={() => setExpanded((open) => !open)}
            type="button"
          >
            {expanded ? 'Show fewer' : `Show all ${sorted.length}`}
          </button>
        </div>
      ) : null}
    </div>
  );
}
