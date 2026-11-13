// The Overview's upstream usage table: Upstream plus subscription windows,
// each cell reading what is USED ("N% used" + UsageMeter + reset caption),
// like the utilization Claude reports. Provider pages show each provider's own
// windows as columns; the all-provider Overview (`layout="shared"`) shows
// generic 5h and 7d columns into which each row maps its own windows as
// separate, sub-labeled slots. Rows sort by their most-used window (problems
// first, API-key and disabled upstreams last) and link to the upstream. Below
// the container's wide breakpoint a row stacks: the name line, then one line
// per window repeating its label.
import { Link } from '@tanstack/react-router';
import {
  type CSSProperties,
  type ReactNode,
  useCallback,
  useId,
  useMemo,
  useRef,
  useState,
} from 'react';
import type {
  LatestResponse,
  PeriodUnknownQuotaMeter,
  QuotaSnapshot,
  SubscriptionQuotaWindow,
} from '../../lib/api';
import { windowLabel } from '../../lib/api';
import {
  type CouponInventory,
  useUpstreamCouponInventory,
} from '../../lib/couponInventory';
import { sumTokens } from '../../lib/format';
import { useTimezone } from '../../lib/locale';
import {
  isTerminalOAuthReconnectReason,
  type OAuthReconnectNudge,
  useOAuthReconnectNudges,
} from '../../lib/oauthReconnect';
import { type ProviderFamily, providerOf } from '../../lib/providers';
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
  type QuotaSeverity,
  quotaPacePct,
  quotaSeverity,
} from '../../lib/quotaSeverity';
import { cx, Skeleton } from '../ui/primitives';
import { PaceLegend, UsageMeter } from '../ui/UsageMeter';
import {
  type CodexCreditFacts,
  CodexCreditSummary,
  selectCodexCredits,
} from './codexCredits';
import {
  GEMINI_GROUP_PLACEHOLDER_TEXT,
  type GeminiGroupPlaceholderState,
  geminiGroupPlaceholderState,
  geminiRetainedReading,
  QUOTA_WINDOW_ORDER,
  selectQuotaDetailState,
} from './quotaWindowVisibility';
import {
  UpstreamCouponAction,
  UpstreamCouponTrigger,
} from './UpstreamCouponAction';
import { type UpstreamHealth, upstreamHealth } from './upstreamHealth';

/** The windows shown in each provider's subscription row, in column order. */
const USAGE_WINDOWS_BY_PROVIDER: Record<
  ProviderFamily,
  readonly SubscriptionQuotaWindow[]
> = {
  anthropic: ['5h', '7d', '7d_fable'],
  openai: ['5h', '7d', 'unified'],
  gemini: ['gemini_5h', 'gemini_7d', 'third_party_5h', 'third_party_7d'],
};
type UsageWindow = SubscriptionQuotaWindow;

const DEFAULT_USAGE_WINDOWS = USAGE_WINDOWS_BY_PROVIDER.anthropic;
const PEAK_USAGE_WINDOWS: Record<string, true> = {
  '5h': true,
  '7d': true,
  gemini_5h: true,
  gemini_7d: true,
  third_party_5h: true,
  third_party_7d: true,
  '7d_fable': true,
  '7d_sonnet': true,
  '7d_opus': true,
};

/**
 * Query windows for one provider-scoped latest request. The all-provider
 * request remains available for the shared Upstreams list.
 */
export const UPSTREAM_LATEST_WINDOWS_BY_PROVIDER: Record<
  ProviderFamily,
  string
> = {
  anthropic: '5h,7d,7d_sonnet,7d_opus,7d_fable,overage',
  openai: '5h,7d,overage,unified',
  gemini: USAGE_WINDOWS_BY_PROVIDER.gemini.join(','),
};

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
  /** Provider meters without a known period (Gemini); `[]` when none. */
  periodUnknownMeters: PeriodUnknownQuotaMeter[];
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
  coupons?: ReadonlyMap<string, CouponInventory>;
  couponsLoading?: boolean;
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
    const latestRow = latest?.upstreams.find(
      (l) => l.upstream_id === upstream.id,
    );
    return {
      upstream,
      health: upstreamHealth(upstream.enabled, runtime?.status, nudge),
      nudge,
      runtimeError: runtime?.last_apply_error ?? null,
      windows: latestRow?.windows ?? [],
      periodUnknownMeters: latestRow?.period_unknown_meters ?? [],
      usage7d: usageByUpstreamId.get(upstream.id) ?? null,
    };
  });
}

export function countReconnectNudges(
  rows: readonly UpstreamUsageRow[],
): number {
  return rows.filter((row) => row.nudge != null).length;
}

/**
 * Everything the grid reads: upstreams, the 5s `/latest` poll (same key as
 * the Upstreams page), runtime status, OAuth reconnect nudges and 7-day
 * spend per upstream (for API-key rows).
 *
 * When a provider is supplied, every query and returned row is scoped to that
 * provider so provider detail pages do not mix quota pools.
 */
export function useUpstreamUsageData(
  provider?: ProviderFamily,
): UpstreamUsageData {
  const upstreamsQ = useUpstreams();
  const upstreams = useMemo(
    () =>
      (upstreamsQ.data?.upstreams ?? []).filter(
        (upstream) =>
          provider === undefined || providerOf(upstream.kind) === provider,
      ),
    [provider, upstreamsQ.data],
  );
  const coupons = useUpstreamCouponInventory(upstreams);
  const upstreamIds = useMemo(
    () => upstreams.map((u) => u.id).join(','),
    [upstreams],
  );
  const quotaWindows =
    provider === undefined
      ? UPSTREAM_LATEST_WINDOWS
      : UPSTREAM_LATEST_WINDOWS_BY_PROVIDER[provider];
  const latestQ = useSubscriptionQuotaLatest({
    upstreamIds,
    windows: quotaWindows,
    source: 'merged',
    refetchInterval: 5_000,
  });
  const statusQ = useStatus();
  const usageQ = useUsage(
    '7d',
    'hour',
    'upstream',
    provider === undefined ? undefined : { providers: [provider] },
    'totals',
  );
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
    coupons: coupons.inventories,
    couponsLoading: coupons.loading,
    isLoading: upstreamsQ.isLoading,
    quotaPending: latestQ.isLoading,
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

// ─── Ordering ───────────────────────────────────────────────────────────────

/** The row's most-used window (0-100), or `null` without any reading. */
export function rowMaxUsedPct(row: UpstreamUsageRow): number | null {
  let max: number | null = null;
  for (const snap of row.windows) {
    if (!PEAK_USAGE_WINDOWS[snap.window]) continue;
    if (snap.state === 'absent') continue;
    if (snap.utilization == null || !Number.isFinite(snap.utilization)) {
      continue;
    }
    const used = snap.utilization * 100;
    if (max == null || used > max) max = used;
  }
  return max;
}

/**
 * Usage order: terminal credential failures, then upstreams needing
 * attention, subscription upstreams, API-key upstreams, and healthy disabled
 * upstreams. Each group sorts by most-used window (no reading last), then by
 * name.
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
      const group =
        row.nudge != null && isTerminalOAuthReconnectReason(row.nudge.reason)
          ? 0
          : row.nudge != null || (row.upstream.enabled && attention)
            ? 1
            : !row.upstream.enabled
              ? 4
              : row.upstream.kind.endsWith('_oauth')
                ? 2
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

/** One window's figure, even pace, severity, caption and meter name. */
function windowReading(
  windowName: UsageWindow,
  snap: QuotaSnapshot | undefined,
  nowUnixSecs: number,
  timeZone: string,
  retained = false,
) {
  const longLabel = windowLabel(windowName, { long: true });
  const used =
    snap?.utilization == null || !Number.isFinite(snap.utilization)
      ? null
      : snap.utilization * 100;
  // A stale reading is never shown as current: its caption says `Stale` with
  // the reading's age (warn ink; observed time and reset in `title`), its
  // meter name says so, and it draws no pace tick or pace-relative severity.
  // A Gemini reading retained from an older poll than its group's latest
  // observation reads the same way, whatever its `state`.
  const stale = (snap?.state === 'stale' || retained) && used != null;
  // Even pace only while a current window runs (a reading and a reset ahead).
  const pace =
    used == null || stale
      ? null
      : quotaPacePct(windowName, snap?.resets_at_unix_secs, nowUnixSecs);
  const reset = snap
    ? resetCaption(snap.resets_at_unix_secs, nowUnixSecs, timeZone)
    : { text: 'No reading' };
  const observedUnixSecs =
    snap?.observed_at_unix_millis == null
      ? null
      : Math.floor(snap.observed_at_unix_millis / 1000);
  const observed =
    observedUnixSecs == null
      ? null
      : `Observed ${formatQuotaStamp(observedUnixSecs, timeZone)}`;
  const ageSecs =
    snap?.age_secs ??
    (observedUnixSecs == null ? null : nowUnixSecs - observedUnixSecs);
  return {
    longLabel,
    used,
    pace,
    stale,
    severity: quotaSeverity(used, pace),
    meterLabel: `${longLabel} window used${stale ? ', stale reading' : ''}`,
    caption: stale
      ? {
          text:
            ageSecs == null ? 'Stale' : `Stale · ${formatSpan(ageSecs)} ago`,
          title: ['Stale reading', observed, reset.title ?? reset.text]
            .filter((part) => part != null)
            .join(' · '),
          className: 'text-warn-text',
        }
      : {
          text: reset.text,
          title: reset.title ?? reset.text,
          className: 'text-text-muted',
        },
  };
}

/** "42% used", or "–" read as "No reading" without a finite reading. */
function UsedFigure({
  used,
  severity,
  className,
}: {
  used: number | null;
  severity: QuotaSeverity;
  className?: string;
}) {
  return (
    <span className={cx('flex items-baseline gap-1 tabular-nums', className)}>
      <span
        className={cx(
          'text-body font-medium',
          QUOTA_SEVERITY_TEXT_CLASS[severity],
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
  );
}

function WindowCell({
  windowName,
  snap,
  pending,
  nowUnixSecs,
  timeZone,
  creditFacts,
  retained = false,
}: {
  windowName: UsageWindow;
  snap: QuotaSnapshot | undefined;
  pending: boolean;
  nowUnixSecs: number;
  timeZone: string;
  creditFacts?: CodexCreditFacts | null;
  /** A Gemini reading older than its group's latest observation. */
  retained?: boolean;
}) {
  if (windowName === 'unified' && creditFacts != null) {
    return (
      <div className="min-w-0" data-testid="usage-cell" data-window="unified">
        <span className="mb-1.5 block text-label text-text-muted @4xl:hidden">
          Codex credits
        </span>
        <CodexCreditSummary facts={creditFacts} />
      </div>
    );
  }
  // Keep the desktop column aligned without presenting absent quota as an
  // unknown reading. Stacked rows omit the cell entirely.
  if (snap?.state === 'absent') {
    return (
      <div
        aria-hidden="true"
        className="hidden @4xl:block"
        data-testid="usage-absent-cell"
        data-window={windowName}
      />
    );
  }
  const label = windowLabel(windowName);
  const { longLabel, used, pace, stale, severity, meterLabel, caption } =
    windowReading(windowName, snap, nowUnixSecs, timeZone, retained);
  // Narrow containers (phones) read one line per window above its meter:
  //   [label] [N% used]            [resets in …]
  //   [meter ─────────────────────────────────]
  // From @xl the available windows sit side by side, each a small column.
  return (
    <div
      className={WINDOW_CELL_CLASS}
      data-state={stale ? 'stale' : undefined}
      data-testid="usage-cell"
      data-window={windowName}
    >
      {/* Below the wide breakpoint the header row is gone: repeat the label. */}
      <span
        className="text-label text-text-muted @4xl:hidden"
        title={longLabel !== label ? longLabel : undefined}
      >
        {label}
      </span>
      {pending ? (
        <>
          <Skeleton className="h-5 w-16" />
          <Skeleton className={cx(CAPTION_CELL_CLASS, 'h-3 w-24')} />
          <Skeleton className={cx(METER_CELL_CLASS, 'h-1 w-full')} />
        </>
      ) : (
        <>
          <UsedFigure severity={severity} used={used} />
          <span
            className={cx(
              CAPTION_CELL_CLASS,
              'text-caption tabular-nums',
              caption.className,
            )}
            title={caption.title}
          >
            {caption.text}
          </span>
          <div className={METER_CELL_CLASS}>
            <UsageMeter label={meterLabel} pacePct={pace} usedPct={used} />
          </div>
        </>
      )}
    </div>
  );
}

const WINDOW_CELL_CLASS =
  'grid min-w-0 grid-cols-[4.5rem_minmax(0,1fr)_20ch] items-baseline gap-x-3 gap-y-1.5 ' +
  '@xl:flex @xl:flex-col @xl:items-stretch @xl:gap-1.5';
// Narrow: right of the figure; from @xl: last, under the meter.
const CAPTION_CELL_CLASS = 'truncate text-left @xl:order-last';
const METER_CELL_CLASS = 'col-span-full';

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
          {!upstream.enabled && nudge ? (
            <span className="shrink-0 text-text-muted">· Disabled</span>
          ) : null}
        </span>
      ) : null}
    </div>
  );
}

// Narrow: the name, then one line per provider window. @xl: the window
// cells sit side by side under the name. @4xl: one table row with a header.
const ROW_GRID_TWO =
  'grid gap-x-6 gap-y-3 grid-cols-1 @xl:grid-cols-2 ' +
  '@4xl:grid-cols-[minmax(12rem,1.4fr)_repeat(2,minmax(0,1fr))] @4xl:items-center @4xl:gap-y-0';
const ROW_GRID_THREE =
  'grid gap-x-6 gap-y-3 grid-cols-1 @xl:grid-cols-3 ' +
  '@4xl:grid-cols-[minmax(12rem,1.4fr)_repeat(3,minmax(0,1fr))] @4xl:items-center @4xl:gap-y-0';
const ROW_GRID_MANY =
  'grid gap-x-6 gap-y-3 grid-cols-1 @xl:grid-cols-3 ' +
  '@4xl:grid-cols-[minmax(12rem,1.4fr)_repeat(var(--quota-window-count),minmax(0,1fr))] @4xl:items-center @4xl:gap-y-0';
const ROW_GRID_BY_COUNT: Record<number, string> = {
  0: 'grid grid-cols-1 gap-x-6 gap-y-3 @4xl:items-center @4xl:gap-y-0',
  1: 'grid grid-cols-1 gap-x-6 gap-y-3 @4xl:grid-cols-[minmax(12rem,1.4fr)_minmax(0,1fr)] @4xl:items-center @4xl:gap-y-0',
  2: ROW_GRID_TWO,
  3: ROW_GRID_THREE,
  4: ROW_GRID_MANY,
};

function hasReportedUsageWindow(row: UpstreamUsageRow, window: UsageWindow) {
  if (window === 'unified') {
    return (
      row.upstream.kind === 'openai_oauth' &&
      selectCodexCredits(row.windows) != null
    );
  }
  const snap = row.windows.find((candidate) => candidate.window === window);
  return (
    row.upstream.kind.endsWith('_oauth') &&
    snap != null &&
    snap.state !== 'absent' &&
    (providerOf(row.upstream.kind) === 'anthropic' ||
      snap.credits != null ||
      (snap.state !== 'unobserved' &&
        snap.utilization != null &&
        Number.isFinite(snap.utilization)))
  );
}

/**
 * `provider`: each provider's own window columns (provider pages).
 * `shared`: the all-provider Overview's generic duration columns.
 */
export type UpstreamUsageLayout = 'provider' | 'shared';

/**
 * The shared layout's duration columns. Each row maps its own windows into
 * them as separate slots; readings are never merged, summed or averaged.
 */
const SHARED_USAGE_COLUMNS = ['5h', '7d'] as const;
type SharedUsageColumn = (typeof SHARED_USAGE_COLUMNS)[number];

function isSharedUsageColumn(window: UsageWindow): window is SharedUsageColumn {
  return window === '5h' || window === '7d';
}

/** Gemini's model groups, in the order both shared columns list them. */
const GEMINI_GROUP_SLOTS: readonly {
  label: string;
  windows: Readonly<Record<SharedUsageColumn, UsageWindow>>;
}[] = [
  {
    label: 'Gemini models',
    windows: { '5h': 'gemini_5h', '7d': 'gemini_7d' },
  },
  {
    label: 'Claude & GPT-OSS',
    windows: { '5h': 'third_party_5h', '7d': 'third_party_7d' },
  },
];

interface SharedUsageSlot {
  window: UsageWindow;
  /** Scope inside the shared column; omitted for a provider's lone window. */
  subLabel?: string;
  /** Neutral placeholder in place of a reading; never a number or meter. */
  placeholder?: GeminiGroupPlaceholderState;
}

/** What a shared cell says when the row has no slot for its column. */
type SharedEmptyState = 'no-reading' | 'not-offered';

/** A current figure: observed (fresh or stale) with a finite utilization. */
function hasUsageReading(row: UpstreamUsageRow, window: UsageWindow) {
  const snap = row.windows.find((s) => s.window === window);
  return (
    snap != null &&
    (snap.state === 'fresh' || snap.state === 'stale') &&
    snap.utilization != null &&
    Number.isFinite(snap.utilization)
  );
}

/**
 * The row's slots in one shared column, in display order. A Gemini group
 * with a reading in either duration keeps a slot in both columns, so the
 * columns stay aligned; the unread side is a placeholder, never a number.
 * `[]` means the cell shows its `sharedEmptyState` instead.
 */
function sharedColumnSlots(
  row: UpstreamUsageRow,
  column: SharedUsageColumn,
): SharedUsageSlot[] {
  if (!row.upstream.kind.endsWith('_oauth')) return [];
  const provider = providerOf(row.upstream.kind);
  if (provider === 'gemini') {
    return GEMINI_GROUP_SLOTS.flatMap(
      ({ label, windows }): SharedUsageSlot[] => {
        if (!SHARED_USAGE_COLUMNS.some((c) => hasUsageReading(row, windows[c])))
          return [];
        const window = windows[column];
        return hasUsageReading(row, window)
          ? [{ window, subLabel: label }]
          : [
              {
                window,
                subLabel: label,
                placeholder:
                  geminiGroupPlaceholderState(row.windows, window) ??
                  'no-reading',
              },
            ];
      },
    );
  }
  const fable = row.windows.find((s) => s.window === '7d_fable');
  if (
    provider !== 'anthropic' ||
    column !== '7d' ||
    fable == null ||
    fable.state === 'absent'
  ) {
    return hasUsageReading(row, column) ? [{ window: column }] : [];
  }
  // Anthropic 7d with a model-specific Fable window: two separate slots.
  const allModels: SharedUsageSlot[] = hasUsageReading(row, '7d')
    ? [{ window: '7d', subLabel: 'All models' }]
    : row.windows.find((s) => s.window === '7d')?.state === 'absent'
      ? []
      : [{ window: '7d', subLabel: 'All models', placeholder: 'no-reading' }];
  return [
    ...allModels,
    hasUsageReading(row, '7d_fable')
      ? { window: '7d_fable', subLabel: 'Fable' }
      : { window: '7d_fable', subLabel: 'Fable', placeholder: 'no-reading' },
  ];
}

/**
 * Why a shared cell has no slot. "Not offered" only for proven absence: the
 * Anthropic/OpenAI window itself, or a Gemini upstream whose four canonical
 * windows are all absent with no raw meters (the detail pane's rule).
 */
function sharedEmptyState(
  row: UpstreamUsageRow,
  column: SharedUsageColumn,
): SharedEmptyState {
  const provider = providerOf(row.upstream.kind);
  const absent =
    provider === 'gemini'
      ? selectQuotaDetailState({
          latestWindows: row.windows,
          cards: [],
          rawMeters: row.periodUnknownMeters,
          provider,
        }) === 'not-offered'
      : row.windows.find((s) => s.window === column)?.state === 'absent';
  return absent ? 'not-offered' : 'no-reading';
}

const SLOT_CLASS = 'flex min-w-0 flex-col gap-1.5 py-2 first:pt-0 last:pb-0';

/** One sub-labeled window inside a shared column cell. */
function UsageSlot({
  slot,
  snap,
  nowUnixSecs,
  timeZone,
  retained,
}: {
  slot: SharedUsageSlot;
  snap: QuotaSnapshot | undefined;
  nowUnixSecs: number;
  timeZone: string;
  retained: boolean;
}) {
  const longLabel = windowLabel(slot.window, { long: true });
  // The header row is aria-hidden: name the full window for assistive tech.
  const head = (status: ReactNode) => (
    <div className="flex min-w-0 items-baseline justify-between gap-3">
      <span
        aria-hidden="true"
        className="min-w-0 truncate text-caption font-medium text-text-muted"
        title={longLabel}
      >
        {slot.subLabel}
      </span>
      <span className="sr-only">{longLabel}</span>
      {status}
    </div>
  );
  if (slot.placeholder != null) {
    return (
      <div
        className={SLOT_CLASS}
        data-state={slot.placeholder}
        data-testid="usage-slot-placeholder"
        data-window={slot.window}
      >
        {head(
          <span className="shrink-0 text-caption text-text-muted">
            {GEMINI_GROUP_PLACEHOLDER_TEXT[slot.placeholder]}
          </span>,
        )}
      </div>
    );
  }
  const { used, pace, stale, severity, meterLabel, caption } = windowReading(
    slot.window,
    snap,
    nowUnixSecs,
    timeZone,
    retained,
  );
  return (
    <div
      className={SLOT_CLASS}
      data-state={stale ? 'stale' : undefined}
      data-testid="usage-cell"
      data-window={slot.window}
    >
      {head(
        <UsedFigure className="shrink-0" severity={severity} used={used} />,
      )}
      <UsageMeter label={meterLabel} pacePct={pace} usedPct={used} />
      <span
        className={cx('truncate text-caption tabular-nums', caption.className)}
        title={caption.title}
      >
        {caption.text}
      </span>
    </div>
  );
}

/** A row's cell in one shared column: one window, or stacked slots. */
function SharedColumnCell({
  row,
  column,
  pending,
  nowUnixSecs,
  timeZone,
}: {
  row: UpstreamUsageRow;
  column: SharedUsageColumn;
  pending: boolean;
  nowUnixSecs: number;
  timeZone: string;
}) {
  const snapOf = (window: UsageWindow) =>
    row.windows.find((s) => s.window === window);
  const retainedOf = (window: UsageWindow) =>
    geminiRetainedReading(row.windows, row.periodUnknownMeters, window);
  if (pending) {
    return (
      <WindowCell
        nowUnixSecs={nowUnixSecs}
        pending
        snap={undefined}
        timeZone={timeZone}
        windowName={column}
      />
    );
  }
  const slots = sharedColumnSlots(row, column);
  const [only] = slots;
  if (only == null) {
    // The shared column always exists: say why there is no figure, neutrally.
    const state = sharedEmptyState(row, column);
    return (
      <div
        className={WINDOW_CELL_CLASS}
        data-state={state}
        data-testid="usage-cell-status"
        data-window={column}
      >
        {/* Wide rows show the header instead; keep the name for assistive tech. */}
        <span className="text-label text-text-muted @4xl:sr-only">
          {windowLabel(column)}
        </span>
        {state === 'not-offered' ? (
          <span className="text-body text-text-faint">
            <span aria-hidden="true">—</span>
            <span className="sr-only">Not offered</span>
          </span>
        ) : (
          <span className="text-caption text-text-muted">No reading</span>
        )}
      </div>
    );
  }
  if (slots.length === 1 && only.subLabel == null) {
    return (
      <WindowCell
        nowUnixSecs={nowUnixSecs}
        pending={false}
        snap={snapOf(only.window)}
        retained={retainedOf(only.window)}
        timeZone={timeZone}
        windowName={only.window}
      />
    );
  }
  return (
    <div
      className="min-w-0"
      data-column={column}
      data-testid="usage-cell-group"
    >
      {/* Below the wide breakpoint the header row is gone: repeat the label. */}
      <span className="mb-1.5 block text-label text-text-muted @4xl:hidden">
        {windowLabel(column)}
      </span>
      <div className="flex flex-col divide-y divide-row">
        {slots.map((slot) => (
          <UsageSlot
            key={slot.window}
            nowUnixSecs={nowUnixSecs}
            slot={slot}
            snap={snapOf(slot.window)}
            retained={retainedOf(slot.window)}
            timeZone={timeZone}
          />
        ))}
      </div>
    </div>
  );
}

// Stacked rows keep the section's own edge so names align with its heading;
// the table row insets its cells like other tables.
const ROW_CLASS =
  'rounded-sm px-0 py-3 text-left text-text transition-colors @4xl:px-3 ' +
  'focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-2';

function RowBody({
  row,
  pending,
  nowUnixSecs,
  timeZone,
  tableWindows,
  layout,
}: {
  row: UpstreamUsageRow;
  pending: boolean;
  nowUnixSecs: number;
  timeZone: string;
  tableWindows?: readonly UsageWindow[];
  layout: UpstreamUsageLayout;
}) {
  const provider = providerOf(row.upstream.kind);
  const windows = tableWindows ?? USAGE_WINDOWS_BY_PROVIDER[provider];
  // Fleet-level columns omit absent windows; mixed fleets retain an inert
  // desktop slot for a row that does not report the shared window.
  return (
    <>
      <div className="col-span-full min-w-0 pr-20 @4xl:col-span-1 @4xl:pr-0">
        <NameCell row={row} />
      </div>
      {row.upstream.kind.endsWith('_oauth') ? (
        windows.map((w) => {
          if (layout === 'shared' && isSharedUsageColumn(w)) {
            return (
              <SharedColumnCell
                key={w}
                column={w}
                nowUnixSecs={nowUnixSecs}
                pending={pending}
                row={row}
                timeZone={timeZone}
              />
            );
          }
          const snapshot = row.windows.find((s) => s.window === w);
          // Rule A on provider pages too: a Gemini window whose group sibling
          // has a reading stays visible as a neutral placeholder.
          const placeholder = pending
            ? null
            : geminiGroupPlaceholderState(row.windows, w);
          if (placeholder != null) {
            return (
              <div
                key={w}
                className={WINDOW_CELL_CLASS}
                data-state={placeholder}
                data-testid="usage-slot-placeholder"
                data-window={w}
              >
                {/* Wide rows show the header instead; keep the name for assistive tech. */}
                <span
                  className="text-label text-text-muted @4xl:sr-only"
                  title={windowLabel(w, { long: true })}
                >
                  {windowLabel(w)}
                </span>
                <span className="text-caption text-text-muted">
                  {GEMINI_GROUP_PLACEHOLDER_TEXT[placeholder]}
                </span>
              </div>
            );
          }
          return !USAGE_WINDOWS_BY_PROVIDER[provider].includes(w) ||
            (snapshot?.state !== 'absent' &&
              provider !== 'anthropic' &&
              !pending &&
              !hasReportedUsageWindow(row, w)) ? (
            <span
              key={w}
              aria-hidden="true"
              className="hidden min-w-0 @4xl:block"
              data-testid="usage-cell-empty"
              data-window={w}
            />
          ) : (
            <WindowCell
              key={w}
              nowUnixSecs={nowUnixSecs}
              pending={pending}
              snap={snapshot}
              retained={geminiRetainedReading(
                row.windows,
                row.periodUnknownMeters,
                w,
              )}
              timeZone={timeZone}
              windowName={w}
              creditFacts={
                w === 'unified' ? selectCodexCredits(row.windows) : null
              }
            />
          );
        })
      ) : (
        <span
          className={cx(
            'col-span-full text-body-sm text-text-muted',
            '@4xl:[grid-column:span_var(--quota-window-count)]',
          )}
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
  /**
   * Column model. Only the all-provider Overview passes `shared`; provider
   * pages keep each provider's own windows.
   */
  layout?: UpstreamUsageLayout;
}

/** The operation owner must outlive the sorted, optionally collapsed rows. */
export function UpstreamUsageTable(props: UpstreamUsageTableProps) {
  const [active, setActive] = useState<{
    row: UpstreamUsageRow;
    instance: number;
    initialOwnedCount: number | null;
    returnFocus: HTMLButtonElement;
  } | null>(null);
  const workflowOpen = useRef(false);
  const onOpenChange = useCallback((open: boolean) => {
    workflowOpen.current = open;
  }, []);
  const openCoupon = (
    row: UpstreamUsageRow,
    returnFocus: HTMLButtonElement,
  ) => {
    if (workflowOpen.current) return;
    workflowOpen.current = true;
    setActive((previous) => ({
      row,
      returnFocus,
      initialOwnedCount:
        props.data.coupons?.get(row.upstream.id)?.total ?? null,
      instance: (previous?.instance ?? 0) + 1,
    }));
  };
  const activeRow =
    active &&
    (props.data.rows.find(
      (row) => row.upstream.id === active.row.upstream.id,
    ) ??
      active.row);

  return (
    <>
      <UpstreamUsageRows {...props} onCouponClick={openCoupon} />
      {active && activeRow ? (
        <UpstreamCouponAction
          key={active.instance}
          upstream={activeRow.upstream}
          windows={activeRow.windows}
          initialOwnedCount={active.initialOwnedCount}
          returnFocus={active.returnFocus}
          onOpenChange={onOpenChange}
        />
      ) : null}
    </>
  );
}

/**
 * Every upstream's usage per window, most-used first. Rows link into the
 * upstream's detail. Past `collapsedRows` the rest sit behind an inline
 * "Show all N" toggle.
 */
function UpstreamUsageRows({
  data,
  ariaLabel = 'Upstream usage per window',
  className,
  empty = null,
  collapsedRows = 8,
  layout = 'provider',
  onCouponClick,
}: UpstreamUsageTableProps & {
  onCouponClick: (
    row: UpstreamUsageRow,
    returnFocus: HTMLButtonElement,
  ) => void;
}) {
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
              layout === 'shared' ? ROW_GRID_TWO : ROW_GRID_THREE,
              'border-t border-row first:border-t-0',
            )}
            data-testid="upstream-list-loading-row"
          >
            <div className="col-span-full flex flex-col gap-1.5 @4xl:col-span-1">
              <Skeleton className="h-5 w-32" />
            </div>
            {(layout === 'shared'
              ? SHARED_USAGE_COLUMNS
              : DEFAULT_USAGE_WINDOWS
            ).map((w) => (
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
  const hasQuotaRows = sorted.some(
    (row) =>
      row.upstream.kind.endsWith('_oauth') &&
      providerOf(row.upstream.kind) !== 'gemini',
  );
  const firstProvider = sorted[0]
    ? providerOf(sorted[0].upstream.kind)
    : undefined;
  const sameProvider =
    firstProvider !== undefined &&
    sorted.every((row) => providerOf(row.upstream.kind) === firstProvider);
  // Shared: generic 5h/7d for every provider; the Codex credits column keeps
  // its own rule (any OpenAI row while pending, a credit reading once loaded).
  const candidateWindows: readonly UsageWindow[] =
    layout === 'shared'
      ? [
          ...SHARED_USAGE_COLUMNS,
          ...(sorted.some((row) => providerOf(row.upstream.kind) === 'openai')
            ? (['unified'] as const)
            : []),
        ]
      : sameProvider && firstProvider !== undefined
        ? USAGE_WINDOWS_BY_PROVIDER[firstProvider]
        : QUOTA_WINDOW_ORDER.filter((window) =>
            sorted.some((row) =>
              USAGE_WINDOWS_BY_PROVIDER[providerOf(row.upstream.kind)].includes(
                window,
              ),
            ),
          );
  // Shared 5h/7d columns always stay, whatever the fleet reports; other
  // columns drop once loaded when no row reports them or holds a Gemini
  // group placeholder for them.
  const tableWindows = data.quotaPending
    ? candidateWindows
    : candidateWindows.filter(
        (window) =>
          (layout === 'shared' && isSharedUsageColumn(window)) ||
          sorted.some(
            (row) =>
              hasReportedUsageWindow(row, window) ||
              geminiGroupPlaceholderState(row.windows, window) != null,
          ),
      );
  const rowGrid = ROW_GRID_BY_COUNT[tableWindows.length] ?? ROW_GRID_MANY;

  return (
    <div
      className={cx('@container flex flex-col', className)}
      style={
        {
          '--quota-window-count': Math.max(1, tableWindows.length),
        } as CSSProperties
      }
    >
      <div
        aria-hidden="true"
        className={cx(
          'relative hidden border-b border-row py-2 pl-3 pr-20 text-label text-text-faint @4xl:grid',
          rowGrid,
        )}
        data-testid="upstream-usage-header"
      >
        <span>Upstream</span>
        {tableWindows.map((w) => (
          <span key={w} title={windowLabel(w, { long: true })}>
            {windowLabel(w)}
          </span>
        ))}
        <span className="absolute right-3 top-2 w-16 text-right">Coupons</span>
      </div>
      <ul aria-label={ariaLabel} className="flex flex-col" id={listId}>
        {shown.map((row) => (
          <li
            key={row.upstream.id}
            className="relative border-t border-row first:border-t-0"
            data-testid="upstream-usage-row"
          >
            <Link
              className={cx(
                ROW_CLASS,
                rowGrid,
                'hover:bg-overlay-2 @4xl:pr-20',
              )}
              search={{ selectedId: row.upstream.id }}
              title={row.runtimeError ?? undefined}
              to="/upstreams"
            >
              <RowBody
                nowUnixSecs={nowUnixSecs}
                pending={data.quotaPending}
                row={row}
                timeZone={timeZone}
                tableWindows={tableWindows}
                layout={layout}
              />
            </Link>
            <div className="absolute right-0 top-1 w-max @4xl:right-3 @4xl:top-1/2 @4xl:-translate-y-1/2">
              <UpstreamCouponTrigger
                upstream={row.upstream}
                inventory={data.coupons?.get(row.upstream.id)}
                loading={data.couponsLoading}
                onClick={(trigger) => onCouponClick(row, trigger)}
              />
            </div>
          </li>
        ))}
      </ul>
      {/* One pace key per table, beside the "Show all" toggle when there is one. */}
      {collapsible || hasQuotaRows ? (
        <div
          className={cx(
            'flex flex-wrap items-center gap-x-4 gap-y-2 border-t border-row pt-3 @4xl:px-3',
            collapsible ? 'justify-between' : 'justify-end',
          )}
        >
          {collapsible ? (
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
          ) : null}
          {hasQuotaRows ? <PaceLegend /> : null}
        </div>
      ) : null}
    </div>
  );
}
