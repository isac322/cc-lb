import { Link, useNavigate, useSearch } from '@tanstack/react-router';
import { ArrowDown, ArrowUp } from 'lucide-react';
import { memo, type ReactNode, useEffect, useMemo, useState } from 'react';
import {
  Area,
  AreaChart,
  CartesianGrid,
  Tooltip as RTooltip,
  XAxis,
  YAxis,
} from 'recharts';
import * as z from 'zod';
import {
  FirstRunChecklist,
  useFirstRunIncomplete,
} from '../../components/onboarding/FirstRunChecklist';
import {
  BreakdownPopover,
  fmtTokens,
} from '../../components/ui/BreakdownPopover';
import { CostFigure } from '../../components/ui/CostFigure';
import {
  CHART_AXIS,
  CHART_CURSOR,
  CHART_GRID,
  SeriesFillGradient,
  SPARKLINE_SECONDARY_DASH,
  Sparkline,
  useChartId,
} from '../../components/ui/charts';
import { MetricCell } from '../../components/ui/MetricCell';
import { ProviderLogo } from '../../components/ui/ProviderLogo';
import {
  Badge,
  Button,
  cx,
  Hint,
  INPUT_SM_CLASS,
  PageContainer,
  Section,
  SegmentedControl,
  Skeleton,
} from '../../components/ui/primitives';
import {
  FeedLiveStatus,
  RequestEventsFeed,
} from '../../components/ui/RequestEventsFeed';
import {
  EmptyValue,
  Table,
  TableCell,
  TableEmptyRow,
  TableHead,
  TableHeadCell,
} from '../../components/ui/Table';
import { UsageMeter } from '../../components/ui/UsageMeter';
import {
  addBucketCostMicros,
  type CostComponentMicros,
  costCategorySegments,
  emptyCostComponents,
  sumCostMicros,
} from '../../components/ui/usage/costCategories';
import {
  OAuthReconnectSummary,
  UpstreamApplyFailureSummary,
} from '../../components/upstreams/OAuthReconnectNotice';
import {
  UpstreamUsageTable,
  useUpstreamUsageData,
} from '../../components/upstreams/UpstreamUsageTable';
import {
  type AggregateResponse,
  type PoolHistoryWindowResponse,
  type RequestEventKind,
  windowLabel,
} from '../../lib/api';
import { categoricalColor, getWindowColor } from '../../lib/colors';
import {
  cacheHitRatio,
  cacheMissRatio,
  formatCount,
  formatRate,
  splitNum,
  sumTokens,
} from '../../lib/format';
import { useTimezone } from '../../lib/locale';
import {
  PROVIDER_FAMILIES,
  PROVIDER_LABELS,
  type ProviderFamily,
  requestEventKindsForProvider,
} from '../../lib/providers';
import {
  usePrincipalNameMap,
  useSubscriptionQuotaAggregate,
  useSubscriptionQuotaPoolHistory,
  useSummary,
  useUsage,
} from '../../lib/queries';
import {
  formatQuotaPercent,
  QUOTA_SEVERITY_TEXT_CLASS,
  type QuotaSeverity,
  quotaPacePct,
  quotaSeverity,
} from '../../lib/quotaSeverity';
import {
  TIME_PRESET_OPTIONS,
  TIME_PRESET_SECONDS,
  TIME_PRESETS,
  type TimePreset,
  useSharedTimeRange,
} from '../../lib/timePresets';
import { formatInTimezone } from '../../lib/timezone';
import { useRequestEventsFeed } from '../../lib/useRequestEventsFeed';
import { PrincipalUsageRow } from '../../overview/PrincipalUsageRow';
import {
  buildPoolQuotaChartData,
  formatAgo,
  formatResetIn,
  latestPoolQuotaSnapshotUnix,
  missingCapacityUpstreams,
  oldestProviderObservation,
  POOL_QUOTA_WINDOWS,
  type PoolQuotaChartRow,
  type PoolQuotaLatest,
  type PoolQuotaWindow,
  poolQuotaResponseLatest,
  poolQuotaWindowsPresent,
  poolTooltipRows,
  poolWindowResets,
  visiblePoolQuotaChartData,
} from '../../routes/-overviewPoolQuota';

export const overviewSearchSchema = z.object({
  // An explicit `?range=` owns the view; without it the shared, persisted
  // range governs, so `range` stays optional rather than URL-defaulted.
  range: z.enum(TIME_PRESETS).optional().catch(undefined),
});

type Range = TimePreset;
const RANGE_OPTIONS = TIME_PRESET_OPTIONS;
const RANGE_SECONDS = TIME_PRESET_SECONDS;
// The backend accepts one request with a comma-separated event-kind set. This
// keeps provider feeds filtered before pagination and in live-tail matching.
export const overviewEventFilters = (provider?: ProviderFamily) => ({
  event_kind_set: overviewEventKinds(provider),
});

export function overviewEventKinds(
  provider?: ProviderFamily,
): readonly RequestEventKind[] {
  const providers = provider === undefined ? PROVIDER_FAMILIES : [provider];
  return [
    ...new Set(
      providers
        .flatMap((family) => requestEventKindsForProvider(family))
        .filter((kind) => !kind.endsWith('count_tokens')),
    ),
  ];
}

const stepFor = (r: Range): 'hour' | 'minute' =>
  r === '7d' || r === '24h' ? 'hour' : 'minute';

const providerPoolWindows = (provider: ProviderFamily | undefined): string => {
  if (provider === 'openai') return '5h,7d';
  if (provider === 'gemini') return GEMINI_POOL_CHART_WINDOWS.join(',');
  return POOL_QUOTA_WINDOWS.join(',');
};
const OPENAI_POOL_CHART_WINDOWS = ['5h', '7d'] as const;
const GEMINI_POOL_CHART_WINDOWS = [
  'gemini_5h',
  'gemini_7d',
  'third_party_5h',
  'third_party_7d',
] as const;
const GEMINI_POOL_WINDOW_LABELS = {
  gemini_5h: windowLabel('gemini_5h'),
  gemini_7d: windowLabel('gemini_7d'),
  third_party_5h: windowLabel('third_party_5h'),
  third_party_7d: windowLabel('third_party_7d'),
} as const;

function presentPoolQuotaWindows(
  defaults: readonly PoolQuotaWindow[],
  aggregate: AggregateResponse | undefined,
  history: readonly PoolHistoryWindowResponse[] | undefined,
  loading: boolean,
): PoolQuotaWindow[] {
  if (loading) return [...defaults];
  return poolQuotaWindowsPresent(history, aggregate, defaults, true);
}

const POOL_HISTORY_WINDOW_QUANTUM_SECS = 1800;
const POOL_HISTORY_MAX_POINTS_PER_SERIES = 1000;

function fmtMs(n: number | undefined | null): string {
  if (n == null) return '—';
  if (n >= 1000) return `${(n / 1000).toFixed(2)}s`;
  return `${Math.round(n)}ms`;
}
function fmtChartTick(unix: number): string {
  const d = new Date(unix * 1000);
  return `${d.getMonth() + 1}/${d.getDate()} ${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`;
}
function fmtChartTooltip(unix: number): string {
  const d = new Date(unix * 1000);
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')} ${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`;
}

/** One bucket of a KPI chart: the shared hover index addresses this array. */
export type KpiChartPoint = {
  timestamp: number;
  value: number;
  /** Second series value for the bucket; null where it is undefined (gap). */
  secondaryValue?: number | null;
};

/**
 * Optional second series drawn over the primary area as a dashed line on its
 * own scale (the sparkline shows shape; the legend and readout carry figures).
 * A tile with a second series reads each readout row as a self-describing
 * figure with its unit (`formatChartValue` and `format` include the unit),
 * behind the same swatch as the legend.
 */
type KpiSecondarySeries = {
  color: string;
  format: (value: number | null) => string;
  /** Fixed scale of the second series; auto from zero when omitted. */
  domain?: [number, number];
};

const NO_KPI_POINTS: readonly KpiChartPoint[] = [];

/** One-decimal percent of a 0–100 value. */
function fmtPercent(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return '—';
  return `${value.toFixed(1)}%`;
}
function fmtCostKpi(micros: number): string {
  if (!Number.isFinite(micros) || micros < 0) return '—';
  return `$${(micros / 1_000_000).toFixed(2)}`;
}

function fmtErrorPercent(value: number): string {
  return `${value.toFixed(2)}%`;
}

// KPI series are data, not status: they draw in neutral ink. Only the error
// rate turns danger, and only when there are errors to report. Tokens is the
// one two-series tile: total tokens in the neutral KPI ink, and the cache
// miss ratio (0-100 on its own scale) as a dashed amber line, the color that
// marks cache writes in the request tables.
const KPI_SERIES_COLOR = 'var(--color-text-muted)';
const TOKENS_CACHE_MISS_COLOR = 'var(--color-series-cache-create-5m)';

const TOKENS_CACHE_MISS_SERIES: KpiSecondarySeries = {
  color: TOKENS_CACHE_MISS_COLOR,
  format: (value) => `${fmtPercent(value)} cache miss`,
  domain: [0, 100],
};

function fmtTokensReadout(value: number): string {
  return `${fmtTokens(value)} tokens`;
}

/**
 * KPI tile values never truncate: counts at or past a million shrink to
 * `splitNum`'s compact unit (same `k`/`M`/`B` the tables use) and the exact
 * figure rides in `title` and screen-reader text.
 */
function fmtRateKpi(reqPerSec: number): {
  value: string;
  exact: string | null;
} {
  const compact = reqPerSec >= 1_000_000;
  const num = splitNum(Math.round(reqPerSec));
  return {
    value: compact ? `${num.value}${num.unit}/s` : formatRate(reqPerSec),
    exact: compact ? `${formatCount(Math.round(reqPerSec))} requests/s` : null,
  };
}

/**
 * "── Tokens 45.2k  ┄┄ Cache miss 12.3%" under the Tokens value: names both
 * chart series with the range figures. Wraps to two lines when the tile is
 * narrow rather than truncating a figure.
 */
function TokensLegend({
  total,
  cacheMissPct,
}: {
  total: number;
  cacheMissPct: number | null;
}) {
  return (
    <div
      className="flex min-w-0 flex-wrap items-center gap-x-3 gap-y-0.5 text-caption tabular-nums text-text-muted"
      data-testid="overview-kpi-tokens-legend"
    >
      <span className="inline-flex h-4 items-center gap-1.5 whitespace-nowrap">
        <LegendSwatch stroke={KPI_SERIES_COLOR} strokeWidth={2} />
        Tokens <span className="text-text">{fmtTokens(total)}</span>
      </span>
      <span className="inline-flex h-4 items-center gap-1.5 whitespace-nowrap">
        <LegendSwatch
          stroke={TOKENS_CACHE_MISS_COLOR}
          strokeWidth={2}
          strokeDasharray={SPARKLINE_SECONDARY_DASH}
        />
        Cache miss <span className="text-text">{fmtPercent(cacheMissPct)}</span>
      </span>
    </div>
  );
}

/**
 * 1px left rules between the five traffic readouts at each grid width
 * (2 columns, then 3, then 5): a rule sits before every item that is not
 * first in its row.
 */
const KPI_CELL_CLASS = [
  '',
  'border-l border-row pl-4',
  'sm:border-l sm:border-row sm:pl-4',
  'border-l border-row pl-4 sm:border-l-0 sm:pl-0 xl:border-l xl:pl-4',
  'sm:border-l sm:border-row sm:pl-4',
] as const;

export function ValueTile({
  label,
  value,
  valueExact,
  legend,
  sub,
  chartId,
  spark,
  sparkColor,
  chartLabel,
  formatChartValue = String,
  secondary,
  activeIndex = null,
  onActiveIndexChange,
  loading = false,
  className,
}: {
  label: string;
  value: React.ReactNode;
  /**
   * Full-precision rendering of `value`, shown to screen readers and as the
   * hover `title` whenever the visible figure is a compacted form.
   */
  valueExact?: string | null;
  /** Series legend under the value, for a tile that draws two series. */
  legend?: React.ReactNode;
  sub?: React.ReactNode;
  chartId: string;
  spark?: readonly KpiChartPoint[];
  sparkColor?: string;
  chartLabel?: string;
  formatChartValue?: (value: number) => string;
  secondary?: KpiSecondarySeries;
  activeIndex?: number | null;
  onActiveIndexChange?: (index: number | null) => void;
  loading?: boolean;
  /** Cell geometry from the parent grid (dividers, column span). */
  className?: string;
}) {
  const points = spark ?? NO_KPI_POINTS;
  const color = sparkColor ?? KPI_SERIES_COLOR;
  const hasChart = !loading && points.length > 0;
  const activeIdx =
    hasChart && activeIndex != null
      ? Math.min(Math.max(activeIndex, 0), points.length - 1)
      : null;
  const activePoint = activeIdx == null ? null : points[activeIdx];

  const values = useMemo(() => points.map((point) => point.value), [points]);
  const secondaryValues = useMemo(
    () =>
      secondary
        ? points.map((point) => point.secondaryValue ?? null)
        : undefined,
    [points, secondary],
  );
  const secondaryColor = secondary?.color;
  const secondaryDomain = secondary?.domain;
  // Memoized so a hover on any sibling tile does not re-render Recharts.
  const chart = useMemo(
    () => (
      <Sparkline
        color={color}
        data={values}
        secondary={
          secondaryValues && secondaryColor
            ? {
                data: secondaryValues,
                color: secondaryColor,
                domain: secondaryDomain,
              }
            : undefined
        }
      />
    ),
    [color, values, secondaryValues, secondaryColor, secondaryDomain],
  );

  // Bucket position across the plot, matching Sparkline's x axis.
  const activeX =
    activeIdx == null
      ? null
      : `${points.length > 1 ? (activeIdx / (points.length - 1)) * 100 : 50}%`;

  const handleMove = (event: React.MouseEvent<HTMLDivElement>) => {
    if (!onActiveIndexChange || points.length === 0) return;
    const rect = event.currentTarget.getBoundingClientRect();
    const ratio = rect.width > 0 ? (event.clientX - rect.left) / rect.width : 0;
    const clamped = ratio < 0 ? 0 : ratio > 1 ? 1 : ratio;
    onActiveIndexChange(Math.round(clamped * (points.length - 1)));
  };

  return (
    <div
      className={cx('relative flex min-w-0 flex-col gap-1.5', className)}
      data-testid={`overview-kpi-${chartId}`}
      data-slot="kpi-tile"
    >
      <span className="truncate text-label text-text-muted">{label}</span>
      <div
        className="flex h-9 min-w-0 items-center justify-between gap-2"
        data-slot="value"
      >
        {loading ? (
          <Skeleton className="h-8 w-20" />
        ) : (
          <span
            className="truncate text-display tabular-nums text-text"
            title={valueExact ?? undefined}
          >
            {value}
            {valueExact != null ? (
              <span className="sr-only">{` (${valueExact})`}</span>
            ) : null}
          </span>
        )}
      </div>
      {legend !== undefined ? (
        loading ? (
          <Skeleton className="h-4 w-28" />
        ) : (
          legend
        )
      ) : null}
      {sub !== undefined ? (
        <div
          className="flex h-4 items-center truncate text-caption text-text-muted"
          data-slot="sub"
        >
          {loading ? <Skeleton className="h-3 w-20" /> : sub}
        </div>
      ) : null}
      {activePoint ? (
        <div
          className="glass-strong pointer-events-none absolute inset-x-2 bottom-10 z-10 flex flex-col gap-0.5 rounded-md px-2 py-1.5"
          data-testid={`overview-kpi-tooltip-${chartId}`}
        >
          <span className="truncate text-caption leading-none tabular-nums text-text-faint">
            {fmtChartTooltip(activePoint.timestamp)}
          </span>
          {secondary ? (
            <>
              <span className="inline-flex min-w-0 items-center gap-1.5 text-caption leading-none tabular-nums text-text">
                <LegendSwatch stroke={color} strokeWidth={2} />
                <span className="truncate">
                  {formatChartValue(activePoint.value)}
                </span>
              </span>
              <span className="inline-flex min-w-0 items-center gap-1.5 text-caption leading-none tabular-nums text-text">
                <LegendSwatch
                  stroke={secondary.color}
                  strokeWidth={2}
                  strokeDasharray={SPARKLINE_SECONDARY_DASH}
                />
                <span className="truncate">
                  {secondary.format(activePoint.secondaryValue ?? null)}
                </span>
              </span>
            </>
          ) : (
            <span className="truncate text-caption leading-none tabular-nums text-text">
              {`${chartLabel ?? label} ${formatChartValue(activePoint.value)}`}
            </span>
          )}
        </div>
      ) : null}
      <div className="mt-auto h-8 shrink-0 pt-1" data-slot="sparkline">
        {loading ? (
          <Skeleton className="h-7" />
        ) : hasChart ? (
          <div
            className="relative h-full w-full"
            data-testid={`overview-kpi-chart-${chartId}`}
            onMouseLeave={() => onActiveIndexChange?.(null)}
            onMouseMove={handleMove}
          >
            {chart}
            {activeX != null ? (
              <div
                aria-hidden="true"
                className="pointer-events-none absolute inset-y-0 w-px -translate-x-1/2 bg-border-strong"
                style={{ left: activeX }}
              />
            ) : null}
          </div>
        ) : null}
      </div>
    </div>
  );
}

export type TopPrincipal = {
  id: string;
  name: string;
  /** Authoritative rollup cost for the window, in micros. */
  cost_micros: number;
  /**
   * Per-category cost summed over the window, or null when no bucket recorded
   * any — legacy windows say so in the breakdown instead of an invented split.
   */
  cost_components_micros: CostComponentMicros | null;
  tokens: number;
  /**
   * Cache read over every prompt token (cache read + cache create + uncached
   * input), 0-1 (`cacheHitRatio`); null when the principal sent no prompt
   * tokens in the window.
   */
  cache_hit_ratio: number | null;
  requests: number;
  /** Share of every principal's cost in the window, 0-100. */
  share_pct: number;
};

/** Rows the ranking shows before "Show all N principals" expands it. */
export const TOP_PRINCIPAL_ROWS = 10;

type TopPrincipalSortKey = 'requests' | 'tokens' | 'hit' | 'cost';
type SortDirection = 'asc' | 'desc';

const TOP_PRINCIPAL_SORT_VALUE: Record<
  TopPrincipalSortKey,
  (principal: TopPrincipal) => number | null
> = {
  requests: (principal) => principal.requests,
  tokens: (principal) => principal.tokens,
  hit: (principal) => principal.cache_hit_ratio,
  cost: (principal) => principal.cost_micros,
};

/**
 * Sorted copy: the chosen figure, then cost (desc) and name as tie-breaks. A
 * principal without the figure (no prompt tokens for Cache hit) sorts last in
 * either direction: it has no reading to rank.
 */
function sortTopPrincipals(
  principals: readonly TopPrincipal[],
  key: TopPrincipalSortKey,
  direction: SortDirection,
): TopPrincipal[] {
  const value = TOP_PRINCIPAL_SORT_VALUE[key];
  const sign = direction === 'desc' ? -1 : 1;
  return [...principals].sort((a, b) => {
    const left = value(a);
    const right = value(b);
    const byValue =
      left == null || right == null
        ? Number(left == null) - Number(right == null)
        : sign * (left - right);
    return (
      byValue || b.cost_micros - a.cost_micros || a.name.localeCompare(b.name)
    );
  });
}

/**
 * Whole percent rounded down, like the request table's `hit`: a partial hit
 * never reads `100%`.
 */
function fmtCacheHit(ratio: number): string {
  return `${Math.floor(ratio * 100)}%`;
}

const PRINCIPAL_USD_FORMATTER = new Intl.NumberFormat('en-US', {
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});

const PRINCIPAL_EXACT_COST_FORMATTER = new Intl.NumberFormat('en-US', {
  minimumFractionDigits: 4,
  maximumFractionDigits: 6,
});

function formatPrincipalCostMicros(micros: number): string {
  const usd = micros / 1_000_000;
  if (
    !Number.isFinite(micros) ||
    micros < 0 ||
    micros > Number.MAX_SAFE_INTEGER ||
    !Number.isFinite(usd) ||
    usd > Number.MAX_SAFE_INTEGER
  ) {
    return '—';
  }
  return `$${PRINCIPAL_EXACT_COST_FORMATTER.format(usd)}`;
}

function fmtPrincipalCost(totalMicros: number): string {
  const usd = totalMicros / 1_000_000;
  if (
    !Number.isFinite(totalMicros) ||
    totalMicros < 0 ||
    totalMicros > Number.MAX_SAFE_INTEGER ||
    !Number.isFinite(usd) ||
    usd > Number.MAX_SAFE_INTEGER
  ) {
    return '—';
  }
  return `$${PRINCIPAL_USD_FORMATTER.format(usd)}`;
}

/**
 * Cache efficiency cue, not a quota or SLO: misses increase cost, so lower
 * ratios receive stronger attention while missing prompt data stays neutral.
 */
function principalCacheHitClass(ratio: number | null): string {
  if (ratio == null || !Number.isFinite(ratio)) return 'text-text-muted';
  if (ratio >= 0.9) return 'text-text';
  if (ratio >= 0.8) return 'text-warn-text';
  return 'text-danger-text';
}

/**
 * Unit-only hues are categorical, not severity signals. Keep the numeric
 * value in primary ink so scale is read from the figure first.
 */
const PRINCIPAL_TOKEN_UNIT_COLORS = {
  k: categoricalColor(200).text,
  M: categoricalColor(255).text,
  B: categoricalColor(305).text,
} as const;

function PrincipalTokenFigure({ tokens }: { tokens: number }) {
  const { value, unit } = splitNum(tokens);
  const unitColor = unit ? PRINCIPAL_TOKEN_UNIT_COLORS[unit] : undefined;
  return (
    <span className="inline-flex w-[5.5ch] shrink-0 justify-end tabular-nums">
      <span className="text-text">{value}</span>
      <span
        className="w-[1.5ch] shrink-0 text-left"
        style={unitColor ? { color: unitColor } : undefined}
      >
        {unit}
      </span>
    </span>
  );
}

const PRINCIPAL_COST_NOTE = 'Per-category cost not recorded for this window';

/**
 * The principal's cost over a bar of what it paid for, in the request
 * table's cost categories, order and colors (`costCategorySegments`), with
 * the same hover / Enter breakdown. Cost the recorded categories do not
 * account for (windows rolled up before per-category cost was persisted)
 * draws as the neutral Unattributed tail; a window with no split at all
 * leaves bare track and says so in the breakdown.
 */
function PrincipalCostCell({
  principal,
  maxTotalMicros,
}: {
  principal: TopPrincipal;
  maxTotalMicros: number;
}) {
  const totalMicros = principal.cost_micros;
  const components = principal.cost_components_micros;
  const unattributedMicros = components
    ? Math.max(0, totalMicros - sumCostMicros(components))
    : 0;
  const segments = components
    ? costCategorySegments(components, unattributedMicros)
    : [];
  // Keep the ranking scannable at two decimals; the trigger and breakdown
  // retain the full micros value, up to six fractional digits.
  const text = fmtPrincipalCost(totalMicros);
  const exactText =
    totalMicros > 0
      ? formatPrincipalCostMicros(totalMicros)
      : 'No list price for this model';

  return (
    <MetricCell
      className="@4xl/principals:w-36"
      label={`${principal.name} cost ${exactText}, show breakdown`}
      popover={
        <div data-testid="top-principal-cost-details">
          <BreakdownPopover
            title="Cost"
            note={components ? undefined : PRINCIPAL_COST_NOTE}
            rows={segments.map((segment) => ({
              label: segment.label,
              value: segment.value,
              color: segment.color,
              fmt: formatPrincipalCostMicros,
            }))}
            footer={{
              label: 'Total',
              value: totalMicros,
              fmt: formatPrincipalCostMicros,
            }}
          />
        </div>
      }
      segments={segments}
      total={totalMicros}
      maxTotal={maxTotalMicros}
    >
      <CostFigure text={text} />
    </MetricCell>
  );
}

/**
 * A sortable numeric column header: the whole label is the button, the
 * active column carries its direction arrow and `aria-sort`.
 */
function SortableHeadCell({
  label,
  sortKey,
  activeKey,
  direction,
  onSort,
  className,
}: {
  label: string;
  sortKey: TopPrincipalSortKey;
  activeKey: TopPrincipalSortKey;
  direction: SortDirection;
  onSort: (key: TopPrincipalSortKey) => void;
  className?: string;
}) {
  const active = sortKey === activeKey;
  const Arrow = direction === 'desc' ? ArrowDown : ArrowUp;
  return (
    <TableHeadCell
      aria-sort={
        active ? (direction === 'desc' ? 'descending' : 'ascending') : 'none'
      }
      // Figures get room in a wide list so they do not end up stranded far
      // right of short names.
      className={cx('@4xl/principals:w-36', className)}
      numeric
    >
      <button
        className={cx(
          'inline-flex items-center gap-1 rounded-sm transition-colors hover:text-text focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-2',
          // Phones: a 40px touch target inside the 36px header row.
          'max-md:-my-3 max-md:py-3',
          active && 'text-text',
        )}
        onClick={() => onSort(sortKey)}
        type="button"
      >
        {active ? (
          <Arrow aria-hidden="true" className="size-3" strokeWidth={1.75} />
        ) : null}
        {label}
      </button>
    </TableHeadCell>
  );
}

/**
 * Requests, Tokens and Cache hit drop below `sm`: Principal, Cost and Share
 * stay, and the cache hit folds into the name cell's second line.
 */
const TOP_PRINCIPAL_OPTIONAL_CELL = 'max-sm:hidden';

export function TopPrincipalsSection({
  rangeWords,
  principals,
  idleCount = 0,
  loading,
}: {
  /** The range as the control words it, e.g. "last 24h". */
  rangeWords: string;
  /** Every principal with requests in the range, in any order. */
  principals: readonly TopPrincipal[];
  /** Known principals with no requests in the range: a footnote, not rows. */
  idleCount?: number;
  loading: boolean;
}) {
  const [sortKey, setSortKey] = useState<TopPrincipalSortKey>('cost');
  const [direction, setDirection] = useState<SortDirection>('desc');
  const [expanded, setExpanded] = useState(false);
  const [query, setQuery] = useState('');

  // Keep the reference over the whole active dataset: sorting, expanding and
  // filtering change the visible rows, never the meaning of their bar width.
  const maxTotalMicros = useMemo(
    () =>
      principals.reduce((max, principal) => {
        const cost = Number.isFinite(principal.cost_micros)
          ? Math.max(0, principal.cost_micros)
          : 0;
        return Math.max(max, cost);
      }, 0),
    [principals],
  );

  const sorted = useMemo(
    () => sortTopPrincipals(principals, sortKey, direction),
    [principals, sortKey, direction],
  );
  const canExpand = sorted.length > TOP_PRINCIPAL_ROWS;
  const showAll = expanded && canExpand;
  const needle = query.trim().toLowerCase();
  const rows = showAll
    ? needle
      ? sorted.filter((principal) =>
          principal.name.toLowerCase().includes(needle),
        )
      : sorted
    : sorted.slice(0, TOP_PRINCIPAL_ROWS);

  const handleSort = (key: TopPrincipalSortKey) => {
    if (key === sortKey) {
      setDirection((current) => (current === 'desc' ? 'asc' : 'desc'));
    } else {
      setSortKey(key);
      setDirection('desc');
    }
  };
  const collapse = () => {
    setExpanded(false);
    setQuery('');
  };

  // From a 56rem list every column fits, so a fixed layout can honour the
  // figure column widths and hand the rest to Principal; narrower, the auto
  // layout lets Principal absorb what the visible figures leave.
  const table = (
    <Table className="@4xl/principals:table-fixed md:[&_td:first-child]:pl-6 md:[&_td:last-child]:pr-6 md:[&_th:first-child]:pl-6 md:[&_th:last-child]:pr-6">
      <TableHead sticky={showAll}>
        <tr>
          <TableHeadCell>Principal</TableHeadCell>
          <SortableHeadCell
            activeKey={sortKey}
            className={TOP_PRINCIPAL_OPTIONAL_CELL}
            direction={direction}
            label="Requests"
            onSort={handleSort}
            sortKey="requests"
          />
          <SortableHeadCell
            activeKey={sortKey}
            className={TOP_PRINCIPAL_OPTIONAL_CELL}
            direction={direction}
            label="Tokens"
            onSort={handleSort}
            sortKey="tokens"
          />
          <SortableHeadCell
            activeKey={sortKey}
            className={TOP_PRINCIPAL_OPTIONAL_CELL}
            direction={direction}
            label="Cache hit"
            onSort={handleSort}
            sortKey="hit"
          />
          <SortableHeadCell
            activeKey={sortKey}
            direction={direction}
            label="Cost"
            onSort={handleSort}
            sortKey="cost"
          />
          <TableHeadCell className="@4xl/principals:w-24" numeric>
            Share
          </TableHeadCell>
        </tr>
      </TableHead>
      <tbody>
        {rows.length === 0 ? (
          <TableEmptyRow colSpan={6}>
            {`No principals match "${query.trim()}"`}
          </TableEmptyRow>
        ) : (
          rows.map((principal) => (
            <PrincipalUsageRow
              principalId={principal.id}
              key={principal.id}
              className="cursor-pointer hover:bg-hover-bg"
              data-testid="top-principal-row"
            >
              <TableCell className="w-full max-w-0">
                {/* From `sm` to `md` the name link spans the row's 40px
                height; phones stack it over the cache-hit line instead. */}
                <Link
                  data-principal-link
                  className="block truncate rounded-sm text-text underline decoration-transparent underline-offset-4 transition-colors hover:decoration-border-strong focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-2 sm:max-md:-my-2.5 sm:max-md:py-2.5"
                  search={{ selectedId: principal.id }}
                  title={principal.name}
                  to="/principals"
                >
                  {principal.name}
                </Link>
                <span
                  className={cx(
                    'block text-caption tabular-nums sm:hidden',
                    principalCacheHitClass(principal.cache_hit_ratio),
                  )}
                  data-slot="principal-cache-hit"
                >
                  {principal.cache_hit_ratio == null
                    ? 'No prompt tokens'
                    : `${fmtCacheHit(principal.cache_hit_ratio)} cache hit`}
                </span>
              </TableCell>
              <TableCell className={TOP_PRINCIPAL_OPTIONAL_CELL} numeric>
                {formatCount(principal.requests)}
              </TableCell>
              <TableCell className={TOP_PRINCIPAL_OPTIONAL_CELL} numeric>
                <PrincipalTokenFigure tokens={principal.tokens} />
              </TableCell>
              <TableCell className={TOP_PRINCIPAL_OPTIONAL_CELL} numeric>
                {principal.cache_hit_ratio == null ? (
                  <EmptyValue label="No prompt tokens" />
                ) : (
                  <span
                    className={principalCacheHitClass(
                      principal.cache_hit_ratio,
                    )}
                  >
                    {fmtCacheHit(principal.cache_hit_ratio)}
                  </span>
                )}
              </TableCell>
              <PrincipalCostCell
                principal={principal}
                maxTotalMicros={maxTotalMicros}
              />
              <TableCell className="text-text-muted" numeric>
                {fmtPercent(principal.share_pct)}
              </TableCell>
            </PrincipalUsageRow>
          ))
        )}
      </tbody>
    </Table>
  );

  return (
    <UsageBlock
      action={
        <Link
          className={OVERVIEW_LINK_CLASS}
          search={{ sort: 'active' }}
          to="/principals"
        >
          View all principals
        </Link>
      }
      bleed
      subtitle={`Ranked by virtual cost · ${rangeWords}`}
      testId="overview-top-principals"
      title="Top principals"
    >
      {loading ? (
        <div
          aria-hidden="true"
          className="flex flex-col px-4 md:px-6"
          data-slot="principal-list"
        >
          {Array.from({ length: 5 }).map((_, index) => (
            <div
              key={index}
              className="flex h-10 items-center gap-3 border-t border-row first:border-t-0"
              data-testid="top-principal-skeleton-row"
            >
              <Skeleton className="h-4 w-2/5" />
              <Skeleton className="ml-auto h-4 w-16" />
              <Skeleton className="h-1 w-24 rounded-none" />
            </div>
          ))}
        </div>
      ) : principals.length === 0 ? (
        <p className="px-4 text-body text-text-muted md:px-6">
          {`No usage in the ${rangeWords}`}
        </p>
      ) : (
        <div
          className="@container/principals flex flex-col"
          data-slot="principal-list"
        >
          {showAll ? (
            <div className="border-b border-row px-4 pb-3 md:px-6">
              <input
                aria-label="Filter principals by name"
                className={cx(INPUT_SM_CLASS, 'w-full sm:w-64')}
                onChange={(event) => setQuery(event.target.value)}
                onKeyDown={(event) => {
                  if (event.key === 'Escape' && query !== '') {
                    event.preventDefault();
                    setQuery('');
                  }
                }}
                placeholder="Filter by name"
                type="search"
                value={query}
              />
            </div>
          ) : null}
          {showAll ? (
            <div
              className="max-h-[28rem] overflow-y-auto"
              data-testid="top-principals-scroll"
            >
              {table}
            </div>
          ) : (
            table
          )}
          {canExpand || idleCount > 0 ? (
            <div className="flex flex-wrap items-center justify-between gap-x-4 gap-y-1 border-t border-row px-4 pt-2 md:px-6">
              <span
                className="min-w-0 text-caption text-text-muted"
                data-testid="top-principals-idle-note"
              >
                {idleCount > 0
                  ? `${formatCount(idleCount)} ${idleCount === 1 ? 'principal' : 'principals'} had no requests in the ${rangeWords}`
                  : null}
              </span>
              {canExpand ? (
                <Button
                  aria-expanded={showAll}
                  onClick={showAll ? collapse : () => setExpanded(true)}
                  size="sm"
                  variant="ghost"
                >
                  {showAll
                    ? `Show top ${TOP_PRINCIPAL_ROWS}`
                    : `Show all ${formatCount(sorted.length)} principals`}
                </Button>
              ) : null}
            </div>
          ) : null}
        </div>
      )}
    </UsageBlock>
  );
}

/**
 * One region inside the Usage group: an h3 title row with a subtitle that
 * ends in the group's range words, then the content. `bleed` drops the
 * horizontal inset so a table can run edge to edge inside the frame (its
 * cells keep the same inset); below `md`, where the group has no frame,
 * the content sits at the page inset and a bled table reaches the screen
 * edges.
 */
function UsageBlock({
  title,
  subtitle,
  action,
  bleed = false,
  testId,
  children,
}: {
  title: string;
  subtitle: ReactNode;
  action?: ReactNode;
  bleed?: boolean;
  testId?: string;
  children: ReactNode;
}) {
  return (
    <section
      aria-label={title}
      className={cx(
        'flex min-w-0 flex-col gap-4 py-5 md:py-6',
        // Phones: a bled table runs to the screen edges; its cells and the
        // title keep the 16px page inset.
        bleed ? 'pb-2 max-md:-mx-4 md:pb-3' : 'md:px-6',
      )}
      data-testid={testId}
    >
      <header
        className={cx(
          'flex flex-wrap items-baseline justify-between gap-x-4 gap-y-2',
          bleed && 'px-4 md:px-6',
        )}
      >
        <div className="min-w-0 flex-1">
          <h3 className="text-title-card text-text">{title}</h3>
          <div
            className="mt-0.5 min-h-4 text-body-sm text-text-muted"
            data-slot="section-subtitle"
          >
            {subtitle}
          </div>
        </div>
        {action ? <div className="flex-shrink-0">{action}</div> : null}
      </header>
      {children}
    </section>
  );
}

/** Text link on the ground: ink with an underline rule that turns brand on hover. 44px tall on phones. */
const OVERVIEW_LINK_CLASS =
  'inline-flex items-center rounded-sm text-body text-text underline decoration-border-strong underline-offset-4 transition-colors hover:decoration-accent focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-2 max-md:min-h-11';

/** "10-01 09:00" in the viewer's timezone. */
function formatMonthDayTime(unixSecs: number, timeZone: string): string {
  return formatInTimezone(unixSecs * 1000, timeZone)
    .slice(5)
    .replace('T', ' ');
}

/** Numeral ink for a quota figure: default ink until pace-relative warn / danger. */
const QUOTA_VALUE_CLASS: Record<QuotaSeverity, string> = {
  ...QUOTA_SEVERITY_TEXT_CLASS,
  none: 'text-text-faint',
  ok: 'text-text',
};

function UpstreamsUsageSection({
  range,
  provider,
}: {
  range: Range;
  provider?: ProviderFamily;
}) {
  return <ProviderUsageAreas provider={provider} range={range} />;
}

function ProviderUsageAreas({
  range: _range,
  provider,
}: {
  range: Range;
  provider?: ProviderFamily;
}) {
  const usage = useUpstreamUsageData(provider);
  return (
    <Section
      title="Upstreams"
      subtitle={
        usage.isLoading
          ? undefined
          : `${formatCount(usage.rows.length)} configured · used per window, as last observed`
      }
      action={
        <Link className={OVERVIEW_LINK_CLASS} to="/upstreams">
          Manage upstreams
        </Link>
      }
    >
      <UpstreamUsageTable
        data={usage}
        empty={
          <p className="text-body text-text-muted">No upstreams configured.</p>
        }
      />
    </Section>
  );
}

type PoolQuotaChartProps = {
  data: PoolQuotaChartRow[];
  snapshots?: PoolQuotaChartRow[];
  rangeStartUnix: number;
  rangeEndUnix: number;
  latest: PoolQuotaLatest;
  windows?: readonly PoolQuotaWindow[];
};

/**
 * Pool windows draw as filled areas: the shared vertical gradient
 * (`SeriesFillGradient`, 2× series fill opacity at the top, transparent at
 * the bottom) under a 1.5px stroke. Fills paint in one pass and every stroke
 * in a second pass on top, so a lower window's stroke is never covered by an
 * upper window's fill.
 */
const POOL_STROKE_WIDTH = 1.5;
const POOL_QUOTA_WINDOWS_DRAW_ORDER: readonly PoolQuotaWindow[] = [
  '7d',
  '7d_fable',
  '5h',
];

/** Filled square chip in a quota window's color: its legend identity. */
function WindowChip({ window }: { window: PoolQuotaWindow }) {
  return (
    <span
      aria-hidden="true"
      className="inline-block size-2.5 shrink-0 rounded-xs"
      data-slot="window-chip"
      style={{ backgroundColor: getWindowColor(window).fill }}
    />
  );
}

function LegendSwatch({
  stroke,
  strokeWidth = 1.5,
  strokeDasharray,
}: {
  stroke: string;
  strokeWidth?: number;
  strokeDasharray?: string;
}) {
  return (
    <svg aria-hidden="true" className="shrink-0" height="4" width="16">
      <line
        stroke={stroke}
        strokeDasharray={strokeDasharray}
        strokeWidth={strokeWidth}
        x1="0"
        x2="16"
        y1="2"
        y2="2"
      />
    </svg>
  );
}

const PLAN_WEIGHT_NOTE =
  "Each upstream's usage counts in proportion to its plan size relative to Claude Pro (Pro 1×, Max 5× counts 5×). Unknown plans count as Pro. Routing is not affected.";

/**
 * The Overview's lead: how pool usage moved over the selected range, with
 * each window's current `N% used` and next reset in the legend and the
 * caveats that qualify the figures underneath.
 */
const PoolQuotaUsage = memo(function PoolQuotaUsage({
  chart,
  aggregate,
  loading,
  rangeWords,
  windowLabels,
  windows,
}: {
  chart: PoolQuotaChartProps;
  aggregate: AggregateResponse | undefined;
  loading: boolean;
  /** The group range control's words ("last 24h"); the plot keeps its last response's range. */
  rangeWords: string;
  windowLabels?: Partial<Record<PoolQuotaWindow, string>>;
  windows?: readonly PoolQuotaWindow[];
}) {
  const { effective: timeZone } = useTimezone();
  const resets = poolWindowResets(aggregate);
  const provider = oldestProviderObservation(aggregate);
  const missingCapacity = missingCapacityUpstreams(aggregate);
  const serverNow = aggregate?.now_unix_secs ?? null;
  const weighting =
    aggregate && 'weighting' in aggregate ? aggregate.weighting : 'plan';
  const scopedWindows = windows ?? chart.windows ?? POOL_QUOTA_WINDOWS;
  const snapshots = chart.snapshots ?? chart.data;
  const first = snapshots[0];
  const last = snapshots.at(-1);
  const proof =
    first && last
      ? `${formatCount(snapshots.length)} snapshots · ${formatMonthDayTime(first.unix, timeZone)} → ${formatMonthDayTime(last.unix, timeZone)}`
      : null;
  return (
    <div aria-busy={loading} className="min-w-0" data-testid="pool-quota-card">
      <UsageBlock
        title="Pool quota usage"
        subtitle={`Used per window across the pool · ${rangeWords}`}
      >
        <PoolQuotaLegend
          latest={chart.latest}
          resets={resets}
          nowUnixSecs={serverNow}
          timeZone={timeZone}
          loading={loading}
          windowLabels={windowLabels}
          windows={scopedWindows}
        />
        <div
          className="relative h-[200px] w-full min-w-0 md:h-[240px] lg:h-[320px]"
          data-testid="pool-quota-chart-slot"
        >
          {loading ? (
            <Skeleton className="absolute inset-0 h-full w-full" />
          ) : (
            <PoolQuotaThemedChart
              seriesData={chart.data}
              rangeStartUnix={chart.rangeStartUnix}
              rangeEndUnix={chart.rangeEndUnix}
              windows={scopedWindows}
              windowLabels={windowLabels}
            />
          )}
        </div>
        <div
          className="flex flex-wrap items-center gap-x-4 gap-y-1.5 text-caption tabular-nums text-text-muted"
          data-testid="pool-quota-captions"
        >
          {loading ? (
            <Skeleton as="span" className="inline-block h-3 w-48" />
          ) : (
            <>
              <Hint
                label={
                  <span className="block max-w-72 whitespace-normal leading-5">
                    {PLAN_WEIGHT_NOTE}
                  </span>
                }
              >
                <span
                  tabIndex={0}
                  className="cursor-help rounded-sm underline decoration-dotted decoration-border-strong underline-offset-4 hover:text-text"
                >
                  {weighting === 'equal'
                    ? 'Equal-weighted across upstreams'
                    : 'Plan-weighted across upstreams'}
                </span>
              </Hint>
              {missingCapacity > 0 ? (
                <span className="text-warn-text">
                  {`Capacity unknown for ${formatCount(missingCapacity)} upstream${missingCapacity === 1 ? '' : 's'}`}
                </span>
              ) : null}
              {provider.observedAtUnixSecs != null && serverNow != null ? (
                <span
                  className="inline-flex items-center gap-1.5"
                  title={formatMonthDayTime(
                    provider.observedAtUnixSecs,
                    timeZone,
                  )}
                >
                  {`Provider data ${formatAgo(provider.observedAtUnixSecs, serverNow)}`}
                  {provider.stale ? (
                    <Badge tone="warn">Stale reading</Badge>
                  ) : null}
                </span>
              ) : null}
              {proof ? <span>{proof}</span> : null}
            </>
          )}
        </div>
      </UsageBlock>
    </div>
  );
});

export const PoolQuotaThemedChart = memo(function PoolQuotaThemedChart({
  seriesData,
  rangeStartUnix,
  rangeEndUnix,
  windows: requestedWindows,
  windowLabels,
}: {
  /** Used rows (0-100). */
  seriesData: PoolQuotaChartRow[];
  rangeStartUnix: number;
  rangeEndUnix: number;
  windows?: readonly PoolQuotaWindow[];
  windowLabels?: Partial<Record<PoolQuotaWindow, string>>;
}) {
  const gradientPrefix = `pool-fill-${useChartId()}`;
  // Back to front: the long windows first, 5h (the fastest mover) on top.
  const windows = requestedWindows
    ? [
        ...POOL_QUOTA_WINDOWS_DRAW_ORDER.filter((window) =>
          requestedWindows.includes(window),
        ),
        ...requestedWindows.filter(
          (window) => !POOL_QUOTA_WINDOWS_DRAW_ORDER.includes(window),
        ),
      ]
    : POOL_QUOTA_WINDOWS_DRAW_ORDER;
  return (
    <div className="absolute inset-0 min-h-0 min-w-0">
      {!seriesData.length ? (
        <div className="pointer-events-none absolute inset-0 z-10 flex items-center justify-center text-body text-text-muted">
          No timeline data yet for this range
        </div>
      ) : null}
      <AreaChart
        responsive
        className="size-full"
        data={seriesData}
        margin={{ top: 16, right: 8, bottom: 0, left: 0 }}
      >
        <defs>
          {windows.map((window) => (
            <SeriesFillGradient
              key={window}
              color={getWindowColor(window).fill}
              id={`${gradientPrefix}-${window}`}
            />
          ))}
        </defs>
        <CartesianGrid {...CHART_GRID} />
        <XAxis
          {...CHART_AXIS}
          dataKey="unix"
          type="number"
          domain={[rangeStartUnix, rangeEndUnix]}
          allowDataOverflow
          tickFormatter={fmtChartTick}
          minTickGap={48}
          tickMargin={8}
        />
        <YAxis
          {...CHART_AXIS}
          tickFormatter={(v) => `${v}%`}
          width={44}
          tickMargin={8}
          domain={[0, 100]}
          ticks={[0, 25, 50, 75, 100]}
          allowDataOverflow={false}
        />
        <RTooltip
          cursor={CHART_CURSOR}
          content={({ active, payload, label }) => {
            if (!active || !payload?.length) return null;
            return (
              <div className="glass-strong min-w-40 rounded-md px-3 py-2 text-caption text-text">
                <div className="mb-1.5 tabular-nums text-text-muted">
                  {fmtChartTooltip(Number(label))}
                </div>
                {poolTooltipRows(payload).map((p) => {
                  const key = String(p.dataKey);
                  const shortLabel = windowLabels?.[key] ?? windowLabel(key);
                  const longLabel = windowLabel(key, { long: true });
                  return (
                    <div
                      key={key}
                      className="flex items-center justify-between gap-3 py-0.5"
                      data-slot="pool-tooltip-row"
                    >
                      <span
                        className="inline-flex items-center gap-1.5 text-text-muted"
                        title={longLabel}
                      >
                        <WindowChip window={key} />
                        {`${shortLabel} window`}
                      </span>
                      <span className="font-medium tabular-nums">
                        {typeof p.value === 'number'
                          ? `${formatQuotaPercent(p.value)} used`
                          : '—'}
                      </span>
                    </div>
                  );
                })}
              </div>
            );
          }}
        />
        {windows.map((window) => (
          <Area
            key={`fill-${window}`}
            type="monotone"
            dataKey={window}
            stroke="none"
            fill={`url(#${gradientPrefix}-${window})`}
            fillOpacity={1}
            activeDot={false}
            tooltipType="none"
            isAnimationActive={false}
            connectNulls={false}
          />
        ))}
        {windows.map((window) => (
          <Area
            key={`stroke-${window}`}
            type="monotone"
            dataKey={window}
            stroke={getWindowColor(window).stroke}
            strokeWidth={POOL_STROKE_WIDTH}
            fill="none"
            isAnimationActive={false}
            connectNulls={false}
          />
        ))}
      </AreaChart>
    </div>
  );
});

/**
 * One legend grid per surface keeps every window aligned while sizing the
 * label track only to that surface's longest label. Provider cards therefore
 * do not inherit the widest label from another provider.
 */
const POOL_QUOTA_LEGEND_GRID_CLASS =
  'grid grid-cols-[max-content_minmax(1rem,1fr)_4.5ch_8ch] items-center gap-x-2 gap-y-2 text-caption tabular-nums';
const POOL_QUOTA_LEGEND_ROW_CLASS =
  'col-span-full grid grid-cols-subgrid items-center';

export function PoolQuotaLegend({
  latest,
  resets,
  nowUnixSecs = null,
  timeZone = 'UTC',
  windows,
  loading = false,
  windowLabels,
}: {
  latest?: PoolQuotaLatest;
  resets?: Record<string, number | null>;
  nowUnixSecs?: number | null;
  timeZone?: string;
  loading?: boolean;
  windowLabels?: Partial<Record<string, string>>;
  windows?: readonly PoolQuotaWindow[];
}) {
  return (
    <div className="min-w-0" data-testid="pool-quota-legend">
      <div
        className={POOL_QUOTA_LEGEND_GRID_CLASS}
        data-slot="pool-quota-legend-grid"
      >
        <div
          aria-hidden="true"
          className={cx(POOL_QUOTA_LEGEND_ROW_CLASS, 'text-text-faint')}
          data-slot="pool-quota-legend-header"
        >
          <span className="sr-only">Window</span>
          <span />
          <span className="text-right">Used</span>
          <span className="text-right whitespace-nowrap">Resets in</span>
        </div>
        {(windows ?? POOL_QUOTA_WINDOWS).map((window) => {
          const value = latest?.[window] ?? null;
          const reset = resets?.[window] ?? null;
          const resetText =
            reset != null && nowUnixSecs != null
              ? formatResetIn(reset, nowUnixSecs)
              : null;
          const countdown =
            reset != null && nowUnixSecs != null
              ? formatResetIn(reset, nowUnixSecs, { compact: true })
              : null;
          const pace =
            nowUnixSecs != null
              ? quotaPacePct(window, reset, nowUnixSecs)
              : null;
          const shortLabel = windowLabels?.[window] ?? windowLabel(window);
          const longLabel = `${windowLabel(window, { long: true })
            .replace(/\b5h\b/g, '5-hour')
            .replace(/\b7d\b/g, '7-day')} window`;
          const valueText =
            value == null ? 'No reading' : `${formatQuotaPercent(value)} used`;
          const accessibleText = loading
            ? `${longLabel}, loading`
            : [longLabel, valueText, resetText].filter(Boolean).join(', ');
          return (
            <div
              aria-label={accessibleText}
              className={POOL_QUOTA_LEGEND_ROW_CLASS}
              key={window}
              role="group"
              title={longLabel}
              data-testid="pool-quota-legend-slot"
              data-slot="pool-quota-legend-row"
              data-window={window}
            >
              <span className="inline-flex min-w-0 items-center gap-2 text-text-muted">
                <WindowChip window={window} />
                <span className="whitespace-nowrap">{shortLabel}</span>
              </span>
              <div aria-hidden="true" data-slot="pool-quota-legend-meter">
                <UsageMeter
                  label={longLabel}
                  pacePct={loading ? null : pace}
                  usedPct={loading ? null : value}
                />
              </div>
              {loading ? (
                <Skeleton as="span" className="h-4 w-full" />
              ) : (
                <span
                  className={cx(
                    'text-right whitespace-nowrap text-label',
                    QUOTA_VALUE_CLASS[quotaSeverity(value, pace)],
                  )}
                  data-slot="pool-quota-legend-used"
                >
                  {formatQuotaPercent(value)}
                </span>
              )}
              {loading ? (
                <Skeleton as="span" className="h-4 w-full" />
              ) : (
                <span
                  className="text-right whitespace-nowrap text-text-muted"
                  data-slot="pool-quota-legend-reset"
                  title={
                    reset != null
                      ? `Resets ${formatMonthDayTime(reset, timeZone)}`
                      : undefined
                  }
                >
                  {countdown ?? '—'}
                </span>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}

export function ProviderOverviewPage({
  provider,
  providerHeading = false,
}: {
  provider?: ProviderFamily;
  /** Multi-provider page: title the usage group with the provider. */
  providerHeading?: boolean;
} = {}) {
  // One range governs the whole Usage group: pool quota usage, Traffic and
  // Top principals. An explicit `?range=` in the URL owns the view; without
  // it the shared, persisted range applies, so a preset chosen on another
  // surface follows the reader here. The page also renders under
  // `/providers/$provider`, so search and navigation are not bound to `/`.
  const { range: urlRange } = useSearch({ strict: false }) as {
    range?: TimePreset;
  };
  const shared = useSharedTimeRange();
  const range = urlRange ?? shared.range;
  const navigate = useNavigate();
  const rangeWords = `last ${range}`;
  const scopedProvider = provider;
  const providerOptions = scopedProvider
    ? { providers: [scopedProvider] as const }
    : undefined;
  // One hover index shared by every KPI chart so all five read the same bucket.
  const [activeKpiIndex, setActiveKpiIndex] = useState<number | null>(null);
  // Switching windows re-buckets every series, so an index carried over from
  // the previous window would address unrelated data: drop it with the range.
  const selectRange = (next: Range) => {
    setActiveKpiIndex(null);
    // The selection writes the shared store directly (not via an effect) so
    // every non-Logs surface follows it, and the URL keeps its explicit
    // `?range=` so the link still encodes the view.
    shared.setRange(next);
    // `resetScroll: false`: the router's scroll restoration would otherwise
    // snap the page to the top, and the control sits above what it scopes —
    // the reader changing the range from Traffic or Top principals keeps
    // their place while the figures re-scope in place.
    void navigate({
      to: '.',
      search: ((prev: Record<string, unknown>) => ({
        ...prev,
        range: next,
      })) as never,
      replace: true,
      resetScroll: false,
    });
  };

  const summary = useSummary(
    range,
    ...(providerOptions === undefined ? [] : [providerOptions]),
  );
  const principalUsage = useUsage(
    range,
    stepFor(range),
    'principal',
    undefined,
    'totals',
    ...(providerOptions === undefined ? [] : [providerOptions]),
  );
  const feed = useRequestEventsFeed({
    filters: overviewEventFilters(scopedProvider),
    providers: scopedProvider ? [scopedProvider] : undefined,
    mode: 'infinite',
    initialHistoryLimit: 500,
    pageSize: 50,
    maxRetained: 500,
    live: true,
  });
  const principalNameMap = usePrincipalNameMap();
  const poolWindows = providerPoolWindows(scopedProvider);
  const poolWindowLabels =
    scopedProvider === 'gemini' ? GEMINI_POOL_WINDOW_LABELS : undefined;
  const poolChartWindows =
    scopedProvider === undefined || scopedProvider === 'anthropic'
      ? POOL_QUOTA_WINDOWS
      : scopedProvider === 'gemini'
        ? GEMINI_POOL_CHART_WINDOWS
        : OPENAI_POOL_CHART_WINDOWS;

  const quotaAggregate = useSubscriptionQuotaAggregate({
    windows: poolWindows,
    provider: scopedProvider,
    source: 'merged',
  });

  const seriesRangeSecs = RANGE_SECONDS[range];
  const quotaPoolHistory = useSubscriptionQuotaPoolHistory({
    windows: poolWindows,
    provider: scopedProvider,
    rangeSecs: seriesRangeSecs,
    windowQuantumSecs: POOL_HISTORY_WINDOW_QUANTUM_SECS,
    maxPointsPerSeries: POOL_HISTORY_MAX_POINTS_PER_SERIES,
  });
  const quotaLoading =
    (quotaAggregate.data === undefined && quotaAggregate.isPending) ||
    (quotaPoolHistory.data === undefined && quotaPoolHistory.isPending);

  // KPI Data
  const totals = summary.data?.totals;
  const durationSecs = summary.data
    ? Math.max(
        1,
        summary.data.window_end_unix_secs - summary.data.window_start_unix_secs,
      )
    : 1;
  const reqPerSec = totals ? totals.request_count / durationSecs : 0;
  const totalTokens = totals ? sumTokens(totals) : 0;
  const totalCostMicros = totals?.virtual_cost_micros ?? 0;
  const errRate =
    totals && totals.request_count > 0
      ? (totals.error_count / totals.request_count) * 100
      : 0;
  const latency = totals?.avg_latency_ms ?? 0;
  const latencyLabel = 'Avg latency';
  // Compact tile figures: the exact count stays in `title`/screen-reader text.
  const kpiRate = fmtRateKpi(reqPerSec);
  // With zero requests, averages and ratios are undefined, not zero.
  const noTraffic = totals?.request_count === 0;
  const requestSeen =
    feed.rows.length > 0 || (totals?.request_count ?? 0) > 0
      ? true
      : feed.loading
        ? undefined
        : false;
  const firstRunIncomplete = useFirstRunIncomplete(requestSeen);

  const cacheMissAvg = totals ? cacheMissRatio(totals) : null;

  const kpiPoints = useMemo(() => {
    const buckets = summary.data?.sparkline.buckets ?? [];
    const stepSecs = summary.data?.step === 'hour' ? 3600 : 60;
    const series = {
      rate: [] as KpiChartPoint[],
      tokens: [] as KpiChartPoint[],
      cost: [] as KpiChartPoint[],
      latency: [] as KpiChartPoint[],
      error: [] as KpiChartPoint[],
    };
    for (const b of buckets) {
      const timestamp = b.bucket_start_unix_secs;
      const miss = cacheMissRatio(b);
      series.rate.push({ timestamp, value: b.request_count / stepSecs });
      // Total tokens, with the bucket's cache miss ratio (0-100) as the
      // second series; a bucket with no prompt tokens has no ratio.
      series.tokens.push({
        timestamp,
        value: sumTokens(b),
        secondaryValue: miss == null ? null : miss * 100,
      });
      series.cost.push({
        timestamp,
        value: b.virtual_cost_micros / 1_000_000,
      });
      series.latency.push({
        timestamp,
        value: b.latency_ms_sum / Math.max(1, b.latency_count),
      });
      series.error.push({
        timestamp,
        value:
          b.request_count > 0 ? (b.error_count / b.request_count) * 100 : 0,
      });
    }
    return series;
  }, [summary.data]);

  const kpiBucketCount = kpiPoints.rate.length;
  // An emptied summary drops the buckets the shared hover addressed, so the
  // index goes with them. Polling refreshes that keep the same bucket count
  // leave an active hover alone.
  useEffect(() => {
    if (kpiBucketCount === 0) setActiveKpiIndex(null);
  }, [kpiBucketCount]);

  // Anchor the visible window to the last successful response.
  // A range control may move immediately, but placeholder data keeps its own
  // plot context until the replacement response lands.
  const poolHistoryNowUnixSecs =
    quotaPoolHistory.data?.now_unix_secs ?? Math.floor(Date.now() / 1000);
  const displayedPoolHistoryRangeSecs =
    quotaPoolHistory.data?.range_secs ?? seriesRangeSecs;
  const normalizedPoolHistoryWindows = quotaPoolHistory.data?.windows;
  const normalizedPoolAggregate = quotaAggregate.data;
  const visiblePoolWindows = useMemo(
    () =>
      presentPoolQuotaWindows(
        poolChartWindows,
        normalizedPoolAggregate,
        normalizedPoolHistoryWindows,
        quotaLoading,
      ),
    [
      normalizedPoolAggregate,
      normalizedPoolHistoryWindows,
      poolChartWindows,
      quotaLoading,
    ],
  );
  const chartData = useMemo(
    () => buildPoolQuotaChartData(normalizedPoolHistoryWindows),
    [normalizedPoolHistoryWindows],
  );
  const recordedChartData = useMemo(
    () =>
      chartData
        .filter(
          (row) =>
            row.unix >=
              poolHistoryNowUnixSecs - displayedPoolHistoryRangeSecs &&
            row.unix <= poolHistoryNowUnixSecs,
        )
        .map((row) => {
          const sparseRow: PoolQuotaChartRow = { unix: row.unix };
          for (const window of visiblePoolWindows) {
            if (window in row) sparseRow[window] = row[window];
          }
          return sparseRow;
        }),
    [
      chartData,
      displayedPoolHistoryRangeSecs,
      poolHistoryNowUnixSecs,
      visiblePoolWindows,
    ],
  );
  const latestPoolQuotaSnapshotUnixSecs = useMemo(
    () => latestPoolQuotaSnapshotUnix(normalizedPoolHistoryWindows),
    [normalizedPoolHistoryWindows],
  );
  const visibleChartData = useMemo(
    () =>
      visiblePoolQuotaChartData(
        chartData,
        poolHistoryNowUnixSecs - displayedPoolHistoryRangeSecs,
        poolHistoryNowUnixSecs,
        latestPoolQuotaSnapshotUnixSecs,
        normalizedPoolHistoryWindows,
      ).map((row) => {
        const sparseRow: PoolQuotaChartRow = { unix: row.unix };
        for (const window of visiblePoolWindows) {
          if (window in row) sparseRow[window] = row[window];
        }
        return sparseRow;
      }),
    [
      chartData,
      displayedPoolHistoryRangeSecs,
      latestPoolQuotaSnapshotUnixSecs,
      normalizedPoolHistoryWindows,
      poolHistoryNowUnixSecs,
      visiblePoolWindows,
    ],
  );

  const chartLatest = useMemo(
    () =>
      poolQuotaResponseLatest(normalizedPoolHistoryWindows, visibleChartData),
    [normalizedPoolHistoryWindows, visibleChartData],
  );
  const poolQuotaChart = useMemo<PoolQuotaChartProps>(
    () => ({
      data: visibleChartData,
      snapshots: recordedChartData,
      rangeStartUnix: poolHistoryNowUnixSecs - displayedPoolHistoryRangeSecs,
      rangeEndUnix: poolHistoryNowUnixSecs,
      latest: chartLatest,
    }),
    [
      chartLatest,
      displayedPoolHistoryRangeSecs,
      poolHistoryNowUnixSecs,
      recordedChartData,
      visibleChartData,
    ],
  );

  // Principals Data: every principal with requests in the range (the table
  // sorts and pages them), and how many known principals had none.
  const principalRanking = useMemo(() => {
    const series = principalUsage.data?.series ?? [];
    const listed: Omit<TopPrincipal, 'share_pct'>[] = [];
    const listedIds = new Set<string>();
    let totalCostMicros = 0;

    for (const s of series) {
      if (!s.key) continue;
      let costMicros = 0;
      let requests = 0;
      // Window token totals per component: the cache hit is taken over the
      // whole window (cache read over every prompt token), not averaged
      // over buckets.
      const tokenTotals = {
        input_tokens: 0,
        output_tokens: 0,
        cache_creation_input_tokens: 0,
        cache_read_input_tokens: 0,
      };
      const components = emptyCostComponents();
      let recordedComponents = false;
      for (const b of s.buckets) {
        costMicros += b.virtual_cost_micros ?? 0;
        requests += b.request_count ?? 0;
        tokenTotals.input_tokens += b.input_tokens ?? 0;
        tokenTotals.output_tokens += b.output_tokens ?? 0;
        tokenTotals.cache_creation_input_tokens +=
          b.cache_creation_input_tokens ?? 0;
        tokenTotals.cache_read_input_tokens += b.cache_read_input_tokens ?? 0;
        if (addBucketCostMicros(b, components)) recordedComponents = true;
      }
      if (requests <= 0) continue;

      totalCostMicros += costMicros;
      listedIds.add(s.key);
      listed.push({
        id: s.key,
        name: principalNameMap.get(s.key) ?? s.key,
        cost_micros: costMicros,
        cost_components_micros: recordedComponents ? components : null,
        tokens: sumTokens(tokenTotals),
        cache_hit_ratio: cacheHitRatio(tokenTotals),
        requests,
      });
    }

    const principals: TopPrincipal[] = listed.map((p) => ({
      ...p,
      share_pct:
        totalCostMicros > 0 ? (p.cost_micros / totalCostMicros) * 100 : 0,
    }));
    let idleCount = 0;
    for (const id of principalNameMap.keys()) {
      if (!listedIds.has(id)) idleCount += 1;
    }
    return { principals, idleCount };
  }, [principalUsage.data, principalNameMap]);

  return (
    <PageContainer>
      <OAuthReconnectSummary />
      <UpstreamApplyFailureSummary />

      {/* The checklist is the whole first-run state: it says what fills in
      once traffic flows, so nothing else renders until it lifts. */}
      <FirstRunChecklist requestSeen={requestSeen} />

      {firstRunIncomplete ? null : (
        <>
          {/* Usage first: one frame binds the range control to everything it
          scopes (pool quota usage, traffic, top principals); the sections
          after it read "as last observed" or "any time". Below `md` the
          frame and its rules drop: the blocks sit on the ground at the page
          inset, separated by space and their own titles. */}
          <section
            aria-labelledby="overview-usage-title"
            className="min-w-0 md:rounded-md md:border md:border-subtle"
            data-testid="overview-usage-group"
          >
            <header className="flex flex-wrap items-center justify-between gap-x-6 gap-y-3 md:flex-nowrap md:border-b md:border-subtle md:px-6 md:py-4">
              <div className="min-w-0">
                <h2
                  className="flex items-center gap-2 text-title-section text-text"
                  id="overview-usage-title"
                >
                  {providerHeading && scopedProvider ? (
                    <>
                      <ProviderLogo
                        className="text-text-muted"
                        provider={scopedProvider}
                      />
                      {PROVIDER_LABELS[scopedProvider]}
                    </>
                  ) : (
                    'Usage'
                  )}
                </h2>
                <p className="mt-0.5 text-body-sm text-text-muted">
                  Pool quota usage, traffic and top principals over the selected
                  range
                </p>
              </div>
              {/* Phones: the four presets share the full width as equal
              segments, one row under the title. */}
              <SegmentedControl
                ariaLabel="Usage range"
                className="shrink-0 max-md:grid max-md:w-full max-md:grid-cols-4"
                options={RANGE_OPTIONS}
                value={range}
                onChange={selectRange}
              />
            </header>
            <div className="md:divide-y md:divide-subtle">
              <PoolQuotaUsage
                aggregate={normalizedPoolAggregate}
                chart={poolQuotaChart}
                loading={quotaLoading}
                rangeWords={rangeWords}
                windowLabels={poolWindowLabels}
                windows={visiblePoolWindows}
              />

              <UsageBlock
                subtitle={`Requests, tokens, cost, latency and errors · ${rangeWords}`}
                testId="overview-traffic"
                title="Traffic"
              >
                <div className="min-w-0" data-testid="overview-kpi-strip">
                  {/* A window without requests has nothing to chart: one line
                  instead of five readouts of zeros and dashes. */}
                  {!summary.isPending && noTraffic ? (
                    <p className="text-body text-text-muted">
                      {`No requests in the ${rangeWords}`}
                    </p>
                  ) : (
                    <div className="grid grid-cols-2 gap-y-6 sm:grid-cols-3 xl:grid-cols-5">
                      <ValueTile
                        className={KPI_CELL_CLASS[0]}
                        chartId="request-rate"
                        label="Requests/s"
                        loading={summary.isPending}
                        value={kpiRate.value}
                        valueExact={kpiRate.exact}
                        sub={`${formatCount(totals?.request_count)} in ${range}`}
                        spark={kpiPoints.rate}
                        sparkColor={KPI_SERIES_COLOR}
                        chartLabel="Req/s"
                        formatChartValue={formatRate}
                        activeIndex={activeKpiIndex}
                        onActiveIndexChange={setActiveKpiIndex}
                      />
                      <ValueTile
                        className={KPI_CELL_CLASS[1]}
                        chartId="tokens"
                        label="Tokens"
                        loading={summary.isPending}
                        value={fmtTokens(totalTokens)}
                        valueExact={`${formatCount(totalTokens)} tokens`}
                        legend={
                          <TokensLegend
                            cacheMissPct={
                              cacheMissAvg == null ? null : cacheMissAvg * 100
                            }
                            total={totalTokens}
                          />
                        }
                        spark={kpiPoints.tokens}
                        sparkColor={KPI_SERIES_COLOR}
                        formatChartValue={fmtTokensReadout}
                        secondary={TOKENS_CACHE_MISS_SERIES}
                        activeIndex={activeKpiIndex}
                        onActiveIndexChange={setActiveKpiIndex}
                      />
                      <ValueTile
                        className={KPI_CELL_CLASS[2]}
                        chartId="cost"
                        label="Cost at list price"
                        loading={summary.isPending}
                        value={fmtCostKpi(totalCostMicros)}
                        valueExact={fmtCostKpi(totalCostMicros)}
                        chartLabel="Cost"
                        spark={kpiPoints.cost}
                        sparkColor={KPI_SERIES_COLOR}
                        formatChartValue={(value) =>
                          fmtCostKpi(value * 1_000_000)
                        }
                        activeIndex={activeKpiIndex}
                        onActiveIndexChange={setActiveKpiIndex}
                      />
                      <ValueTile
                        className={KPI_CELL_CLASS[3]}
                        chartId="latency"
                        label={latencyLabel}
                        loading={summary.isPending}
                        value={fmtMs(latency)}
                        spark={kpiPoints.latency}
                        sparkColor={KPI_SERIES_COLOR}
                        formatChartValue={fmtMs}
                        activeIndex={activeKpiIndex}
                        onActiveIndexChange={setActiveKpiIndex}
                      />
                      <ValueTile
                        className={KPI_CELL_CLASS[4]}
                        chartId="error-rate"
                        label="Error rate"
                        loading={summary.isPending}
                        value={fmtErrorPercent(errRate)}
                        spark={kpiPoints.error}
                        sparkColor={
                          errRate > 0 ? 'var(--color-danger)' : KPI_SERIES_COLOR
                        }
                        formatChartValue={fmtErrorPercent}
                        activeIndex={activeKpiIndex}
                        onActiveIndexChange={setActiveKpiIndex}
                      />
                    </div>
                  )}
                </div>
              </UsageBlock>

              <TopPrincipalsSection
                idleCount={principalRanking.idleCount}
                loading={
                  principalUsage.data === undefined && principalUsage.isPending
                }
                principals={principalRanking.principals}
                rangeWords={rangeWords}
              />
            </div>
          </section>

          <UpstreamsUsageSection provider={scopedProvider} range={range} />

          {/* Latest requests: the feed is not range-scoped (newest events of any
          age), so its label must not suggest it follows the range picker. */}
          <Section
            title="Latest requests (any time)"
            subtitle={
              <span className="flex flex-wrap items-center gap-x-3 gap-y-1">
                <FeedLiveStatus feed={feed} />
                <span>
                  Newest first, not limited to the usage range — full view on
                  Logs page
                </span>
              </span>
            }
            action={
              <Link to="/logs" className={OVERVIEW_LINK_CLASS}>
                Open logs
              </Link>
            }
          >
            <RequestEventsFeed
              feed={feed}
              principalNameMap={principalNameMap}
              providerScope={scopedProvider}
              emptyTitle="No recent requests"
              tableContainerClassName="relative min-w-0 max-md:-mx-4 md:glass md:rounded-md min-h-48 h-96 max-h-[60vh] overflow-auto"
            />
          </Section>
        </>
      )}
    </PageContainer>
  );
}

export default function OverviewPage() {
  return <ProviderOverviewPage />;
}
