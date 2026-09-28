import {
  createFileRoute,
  Link,
  stripSearchParams,
  useNavigate,
  useSearch,
} from '@tanstack/react-router';
import { AlertTriangle, ArrowDown, ArrowUp } from 'lucide-react';
import { memo, type ReactNode, useEffect, useMemo, useState } from 'react';
import {
  Area,
  AreaChart,
  CartesianGrid,
  ReferenceLine,
  Tooltip as RTooltip,
  XAxis,
  YAxis,
} from 'recharts';
import * as z from 'zod';
import { LiveTailFailureBanner } from '../components/LiveTailFailureBanner';
import {
  FirstRunChecklist,
  useFirstRunIncomplete,
} from '../components/onboarding/FirstRunChecklist';
import { BreakdownPopover, fmtTokens } from '../components/ui/BreakdownPopover';
import {
  CHART_AXIS,
  CHART_CURSOR,
  CHART_GRID,
  CHART_THRESHOLD,
  SeriesFillGradient,
  SPARKLINE_SECONDARY_DASH,
  Sparkline,
  useChartId,
} from '../components/ui/charts';
import { MetricCell } from '../components/ui/MetricCell';
import {
  Badge,
  Button,
  cx,
  Hint,
  INPUT_SM_CLASS,
  PageContainer,
  PageHeader,
  Section,
  SegmentedControl,
  Skeleton,
} from '../components/ui/primitives';
import { RequestEventsTable } from '../components/ui/RequestEventsTable';
import {
  EmptyValue,
  Table,
  TableCell,
  TableEmptyRow,
  TableHead,
  TableHeadCell,
  TableRow,
} from '../components/ui/Table';
import {
  addBucketCostMicros,
  type CostComponentMicros,
  costCategorySegments,
  emptyCostComponents,
  sumCostMicros,
} from '../components/ui/usage/costCategories';
import { OAuthReconnectSummary } from '../components/upstreams/OAuthReconnectNotice';
import {
  UpstreamUsageTable,
  useUpstreamUsageData,
} from '../components/upstreams/UpstreamUsageTable';
import { type AggregateResponse, WINDOW_LABELS } from '../lib/api';
import { getWindowColor } from '../lib/colors';
import {
  cacheHitRatio,
  cacheMissRatio,
  formatCostMicros,
  formatCount,
  formatRate,
  formatUsdAmount,
  splitNum,
  sumTokens,
} from '../lib/format';
import { useTimezone } from '../lib/locale';
import {
  isMessagesRequestEvent,
  mergeLogRows,
  newestLiveEventIds,
} from '../lib/logRows';
import {
  usePrincipalNameMap,
  useRecentEventsInfinite,
  useSubscriptionQuotaAggregate,
  useSubscriptionQuotaPoolHistory,
  useSummary,
  useUsage,
} from '../lib/queries';
import {
  formatQuotaPercent,
  QUOTA_DANGER_PCT,
  QUOTA_SEVERITY_TEXT_CLASS,
  QUOTA_WARN_PCT,
  type QuotaSeverity,
  quotaSeverity,
} from '../lib/quotaSeverity';
import {
  TIME_PRESET_OPTIONS,
  TIME_PRESET_SECONDS,
  TIME_PRESETS,
  type TimePreset,
} from '../lib/timePresets';
import { formatInTimezone } from '../lib/timezone';
import { useLiveEventStream } from '../lib/useLiveEventStream';
import {
  buildPoolQuotaChartData,
  formatAgo,
  formatResetIn,
  missingCapacityUpstreams,
  oldestProviderObservation,
  POOL_QUOTA_WINDOWS,
  type PoolQuotaChartRow,
  type PoolQuotaLatest,
  type PoolQuotaWindow,
  poolQuotaResponseLatest,
  poolTooltipRows,
  poolWindowResets,
} from './-overviewPoolQuota';

const overviewSearchSchema = z.object({
  // The one range behind the Usage group (pool quota, traffic, principals).
  range: z.enum(TIME_PRESETS).default('24h').catch('24h'),
});

const OVERVIEW_SEARCH_DEFAULTS = { range: '24h' } as const;

export const Route = createFileRoute('/')({
  validateSearch: overviewSearchSchema,
  // The default range stays out of the address bar.
  search: { middlewares: [stripSearchParams(OVERVIEW_SEARCH_DEFAULTS)] },
  component: OverviewPage,
});

type Range = TimePreset;
const RANGE_OPTIONS = TIME_PRESET_OPTIONS;
const RANGE_SECONDS = TIME_PRESET_SECONDS;
const OVERVIEW_TABLE_COLUMNS = { cost: true, tokens: true } as const;
/** The Overview previews the newest requests; Logs holds the full history. */
const OVERVIEW_LATEST_ROWS = 10;
// The overview previews real user requests only: renewals and non-messages
// endpoints are excluded server-side, and merged rows are re-checked against
// the same effective-kind rule so retained data cannot leak other categories.
const OVERVIEW_EVENT_FILTERS = { event_kind: 'messages' } as const;

const stepFor = (r: Range): 'hour' | 'minute' =>
  r === '7d' || r === '24h' ? 'hour' : 'minute';

const POOL_QUOTA_QUERY_WINDOWS = POOL_QUOTA_WINDOWS.join(',');
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

/** Cost at list price: whole dollars under $1M, `$1.2M` at or past it. */
function fmtUsdKpi(usd: number): { value: string; exact: string | null } {
  const compact = usd >= 1_000_000;
  const num = splitNum(usd);
  return {
    value: compact ? `$${num.value}${num.unit}` : formatUsdAmount(usd),
    exact: compact ? formatUsdAmount(usd) : null,
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

const PRINCIPAL_COST_NOTE = 'Per-category cost not recorded for this window';

/**
 * The principal's cost over a bar of what it paid for, in the request
 * table's cost categories, order and colors (`costCategorySegments`), with
 * the same hover / Enter breakdown. Cost the recorded categories do not
 * account for (windows rolled up before per-category cost was persisted)
 * draws as the neutral Unattributed tail; a window with no split at all
 * leaves bare track and says so in the breakdown.
 */
function PrincipalCostCell({ principal }: { principal: TopPrincipal }) {
  const totalMicros = principal.cost_micros;
  const components = principal.cost_components_micros;
  const unattributedMicros = components
    ? Math.max(0, totalMicros - sumCostMicros(components))
    : 0;
  const segments = components
    ? costCategorySegments(components, unattributedMicros)
    : [];
  const text = formatUsdAmount(totalMicros / 1_000_000);

  return (
    <MetricCell
      className="@4xl/principals:w-36"
      label={`${principal.name} cost ${text}, show breakdown`}
      popover={
        <div data-testid="top-principal-cost-details">
          <BreakdownPopover
            title="Cost"
            showZeroRows={true}
            note={components ? undefined : PRINCIPAL_COST_NOTE}
            rows={segments.map((segment) => ({
              label: segment.label,
              value: segment.value,
              color: segment.color,
              fmt: formatCostMicros,
            }))}
            footer={{
              label: 'Total',
              value: totalMicros,
              fmt: formatCostMicros,
            }}
          />
        </div>
      }
      segments={segments}
      total={totalMicros}
    >
      <span className="text-text">{text}</span>
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
            <TableRow
              key={principal.id}
              className="hover:bg-hover-bg"
              data-testid="top-principal-row"
            >
              <TableCell className="w-full max-w-0">
                {/* From `sm` to `md` the name link spans the row's 40px
                height; phones stack it over the cache-hit line instead. */}
                <Link
                  className="block truncate rounded-sm text-text underline decoration-transparent underline-offset-4 transition-colors hover:decoration-border-strong focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-2 sm:max-md:-my-2.5 sm:max-md:py-2.5"
                  search={{ selectedId: principal.id }}
                  title={principal.name}
                  to="/principals"
                >
                  {principal.name}
                </Link>
                <span
                  className="block text-caption text-text-muted tabular-nums sm:hidden"
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
                {fmtTokens(principal.tokens)}
              </TableCell>
              <TableCell className={TOP_PRINCIPAL_OPTIONAL_CELL} numeric>
                {principal.cache_hit_ratio == null ? (
                  <EmptyValue label="No prompt tokens" />
                ) : (
                  fmtCacheHit(principal.cache_hit_ratio)
                )}
              </TableCell>
              <PrincipalCostCell principal={principal} />
              <TableCell className="text-text-muted" numeric>
                {fmtPercent(principal.share_pct)}
              </TableCell>
            </TableRow>
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

/** Numeral ink for a quota figure: default ink until warn / danger by used. */
const QUOTA_VALUE_CLASS: Record<QuotaSeverity, string> = {
  ...QUOTA_SEVERITY_TEXT_CLASS,
  none: 'text-text-faint',
  ok: 'text-text',
};

/** Upstream rows shown before "Show all N". */
const UPSTREAM_TABLE_ROWS = 8;

/**
 * Every upstream's usage per window, most-used first; rows link into the
 * upstream's detail.
 */
function UpstreamsUsageSection() {
  const usage = useUpstreamUsageData();
  const count = usage.rows.length;
  const subtitle = usage.isLoading
    ? undefined
    : [
        `${formatCount(count)} configured`,
        usage.reconnectCount > 0
          ? `${formatCount(usage.reconnectCount)} need reconnect`
          : null,
        'used per window, as last observed',
      ]
        .filter(Boolean)
        .join(' · ');
  return (
    <Section
      title="Upstreams"
      subtitle={subtitle}
      action={
        <Link to="/upstreams" className={OVERVIEW_LINK_CLASS}>
          Manage upstreams
        </Link>
      }
    >
      <UpstreamUsageTable
        collapsedRows={UPSTREAM_TABLE_ROWS}
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
  rangeStartUnix: number;
  rangeEndUnix: number;
  latest: PoolQuotaLatest;
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
}: {
  chart: PoolQuotaChartProps;
  aggregate: AggregateResponse | undefined;
  loading: boolean;
  /** The group range control's words ("last 24h"); the plot keeps its last response's range. */
  rangeWords: string;
}) {
  const { effective: timeZone } = useTimezone();
  const resets = poolWindowResets(aggregate);
  const provider = oldestProviderObservation(aggregate);
  const missingCapacity = missingCapacityUpstreams(aggregate);
  const serverNow = aggregate?.now_unix_secs ?? null;
  const first = chart.data[0];
  const last = chart.data.at(-1);
  const proof =
    first && last
      ? `${formatCount(chart.data.length)} snapshots · ${formatMonthDayTime(first.unix, timeZone)} → ${formatMonthDayTime(last.unix, timeZone)}`
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
                  Plan-weighted across upstreams
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

function isPoolQuotaWindow(key: string): key is PoolQuotaWindow {
  return (POOL_QUOTA_WINDOWS as readonly string[]).includes(key);
}

export function PoolQuotaThemedChart({
  seriesData,
  rangeStartUnix,
  rangeEndUnix,
}: {
  /** Used rows (0-100). */
  seriesData: PoolQuotaChartRow[];
  rangeStartUnix: number;
  rangeEndUnix: number;
}) {
  const gradientPrefix = `pool-fill-${useChartId()}`;
  // Back to front: the long windows first, 5h (the fastest mover) on top.
  const windows = POOL_QUOTA_WINDOWS_DRAW_ORDER;
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
        <ReferenceLine
          y={QUOTA_WARN_PCT}
          {...CHART_THRESHOLD.warn}
          // Both labels sit under their lines: with one above and one below
          // they collide at the 200px phone chart height.
          label={{
            position: 'insideTopRight',
            value: `${QUOTA_WARN_PCT}%`,
            fill: 'var(--color-warn-text)',
            fontSize: 12,
          }}
        />
        <ReferenceLine
          y={QUOTA_DANGER_PCT}
          {...CHART_THRESHOLD.danger}
          label={{
            position: 'insideTopRight',
            value: `${QUOTA_DANGER_PCT}%`,
            fill: 'var(--color-danger-text)',
            fontSize: 12,
          }}
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
                  return (
                    <div
                      key={key}
                      className="flex items-center justify-between gap-3 py-0.5"
                      data-slot="pool-tooltip-row"
                    >
                      <span className="inline-flex items-center gap-1.5 text-text-muted">
                        {isPoolQuotaWindow(key) ? (
                          <>
                            <WindowChip window={key} />
                            {`${WINDOW_LABELS[key]} window`}
                          </>
                        ) : (
                          key
                        )}
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
}

/**
 * The chart's header row: per window its swatch, current `N% used` in the
 * severity ink and when it resets (absolute time in the `title`), then the
 * two threshold rules.
 */
function PoolQuotaLegend({
  latest,
  resets,
  nowUnixSecs = null,
  timeZone = 'UTC',
  loading = false,
}: {
  latest?: PoolQuotaLatest;
  resets?: Record<PoolQuotaWindow, number | null>;
  nowUnixSecs?: number | null;
  timeZone?: string;
  loading?: boolean;
}) {
  return (
    <div className="flex flex-wrap items-baseline gap-x-6 gap-y-2 text-body text-text-muted">
      {POOL_QUOTA_WINDOWS.map((window) => {
        const value = latest?.[window] ?? null;
        const reset = resets?.[window] ?? null;
        const resetText =
          reset != null && nowUnixSecs != null
            ? formatResetIn(reset, nowUnixSecs)
            : null;
        return (
          <span
            key={window}
            className="inline-flex min-h-5 items-center gap-1.5"
            data-testid="pool-quota-legend-slot"
            data-window={window}
          >
            <WindowChip window={window} />
            {WINDOW_LABELS[window]}
            {loading ? (
              <Skeleton as="span" className="inline-block h-3 w-16" />
            ) : value != null ? (
              <>
                {' · '}
                <span
                  className={cx(
                    'font-medium tabular-nums',
                    QUOTA_VALUE_CLASS[quotaSeverity(value)],
                  )}
                >
                  {`${formatQuotaPercent(value)} used`}
                </span>
                {resetText ? (
                  <span
                    className="text-caption tabular-nums"
                    title={
                      reset != null
                        ? `Resets ${formatMonthDayTime(reset, timeZone)}`
                        : undefined
                    }
                  >
                    {` · ${resetText}`}
                  </span>
                ) : null}
              </>
            ) : (
              <span className="text-text-faint">{' · No reading'}</span>
            )}
          </span>
        );
      })}
      <span className="inline-flex min-h-5 items-center gap-1.5 text-caption">
        <LegendSwatch {...CHART_THRESHOLD.warn} />
        {`Warn at ${QUOTA_WARN_PCT}% used`}
      </span>
      <span className="inline-flex min-h-5 items-center gap-1.5 text-caption">
        <LegendSwatch {...CHART_THRESHOLD.danger} />
        {`Danger at ${QUOTA_DANGER_PCT}% used`}
      </span>
    </div>
  );
}

function OverviewPage() {
  // One range, in the URL, governs the whole Usage group: pool quota usage,
  // Traffic and Top principals.
  const { range } = useSearch({ from: '/' });
  const navigate = useNavigate({ from: '/' });
  const rangeWords = `last ${range}`;
  // One hover index shared by every KPI chart so all five read the same bucket.
  const [activeKpiIndex, setActiveKpiIndex] = useState<number | null>(null);
  // Switching windows re-buckets every series, so an index carried over from
  // the previous window would address unrelated data: drop it with the range.
  const selectRange = (next: Range) => {
    setActiveKpiIndex(null);
    // `resetScroll: false`: the router's scroll restoration would otherwise
    // snap the page to the top, and the control sits above what it scopes —
    // the reader changing the range from Traffic or Top principals keeps
    // their place while the figures re-scope in place.
    void navigate({
      search: (prev) => ({ ...prev, range: next }),
      replace: true,
      resetScroll: false,
    });
  };

  const summary = useSummary(range);
  const principalUsage = useUsage(
    range,
    stepFor(range),
    'principal',
    undefined,
    'totals',
  );
  const events = useRecentEventsInfinite(OVERVIEW_EVENT_FILTERS);
  const principalNameMap = usePrincipalNameMap();

  const quotaAggregate = useSubscriptionQuotaAggregate({
    windows: POOL_QUOTA_QUERY_WINDOWS,
    source: 'merged',
  });

  const seriesRangeSecs = RANGE_SECONDS[range];
  const quotaPoolHistory = useSubscriptionQuotaPoolHistory({
    windows: POOL_QUOTA_QUERY_WINDOWS,
    rangeSecs: seriesRangeSecs,
    windowQuantumSecs: POOL_HISTORY_WINDOW_QUANTUM_SECS,
    maxPointsPerSeries: POOL_HISTORY_MAX_POINTS_PER_SERIES,
  });
  const quotaLoading =
    (quotaAggregate.data === undefined && quotaAggregate.isPending) ||
    (quotaPoolHistory.data === undefined && quotaPoolHistory.isPending);

  const live = useLiveEventStream(OVERVIEW_EVENT_FILTERS);
  const streamStatus = live.status;

  // biome-ignore lint/correctness/useExhaustiveDependencies: live.eventsMap is a stable Map ref mutated in place by useLiveEventStream; live.version is bumped on every upsert so it is the real re-run trigger.
  const recentRows = useMemo(
    () =>
      mergeLogRows(
        live.eventsMap,
        events.data?.pages.flatMap((page) => page.events) ?? [],
      ).filter(isMessagesRequestEvent),
    [live.eventsMap, live.version, events.data],
  );
  const latestRows = useMemo(
    () => recentRows.slice(0, OVERVIEW_LATEST_ROWS),
    [recentRows],
  );

  // biome-ignore lint/correctness/useExhaustiveDependencies: live.version is the mutation counter for the stable eventsMap ref.
  const recentLiveIds = useMemo(
    () => newestLiveEventIds(live.eventsMap),
    [live.eventsMap, live.version],
  );

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
  const virtualUsd = totals ? totals.virtual_cost_micros / 1_000_000 : 0;
  const errRate =
    totals && totals.request_count > 0
      ? (totals.error_count / totals.request_count) * 100
      : 0;
  const latency = totals?.avg_latency_ms ?? 0;
  const latencyLabel = 'Avg latency';
  // Compact tile figures: the exact count stays in `title`/screen-reader text.
  const kpiRate = fmtRateKpi(reqPerSec);
  const kpiCost = fmtUsdKpi(virtualUsd);
  // With zero requests, averages and ratios are undefined, not zero.
  const noTraffic = totals?.request_count === 0;
  const requestSeen =
    recentRows.length > 0 || (totals?.request_count ?? 0) > 0
      ? true
      : events.isPending
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
  const chartData = useMemo(
    () => buildPoolQuotaChartData(quotaPoolHistory.data?.windows),
    [quotaPoolHistory.data],
  );
  const visibleChartData = useMemo(
    () =>
      chartData.filter(
        (row) =>
          row.unix >= poolHistoryNowUnixSecs - displayedPoolHistoryRangeSecs &&
          row.unix <= poolHistoryNowUnixSecs,
      ),
    [chartData, poolHistoryNowUnixSecs, displayedPoolHistoryRangeSecs],
  );

  const chartLatest = useMemo(
    () =>
      poolQuotaResponseLatest(quotaPoolHistory.data?.windows, visibleChartData),
    [quotaPoolHistory.data, visibleChartData],
  );
  const poolQuotaChart = useMemo<PoolQuotaChartProps>(
    () => ({
      data: visibleChartData,
      rangeStartUnix: poolHistoryNowUnixSecs - displayedPoolHistoryRangeSecs,
      rangeEndUnix: poolHistoryNowUnixSecs,
      latest: chartLatest,
    }),
    [
      chartLatest,
      displayedPoolHistoryRangeSecs,
      poolHistoryNowUnixSecs,
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
      <LiveTailFailureBanner
        permanentFailure={live.permanentFailure}
        permanentFailureSince={live.permanentFailureSince}
        reconnectAttempts={live.reconnectAttempts}
        onRetry={live.forceReconnect}
      />
      <OAuthReconnectSummary />
      <PageHeader title="Overview" />

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
                  className="text-title-section text-text"
                  id="overview-usage-title"
                >
                  Usage
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
                aggregate={quotaAggregate.data}
                chart={poolQuotaChart}
                loading={quotaLoading}
                rangeWords={rangeWords}
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
                        value={kpiCost.value}
                        valueExact={kpiCost.exact}
                        spark={kpiPoints.cost}
                        sparkColor={KPI_SERIES_COLOR}
                        chartLabel="Cost"
                        formatChartValue={formatUsdAmount}
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

          <UpstreamsUsageSection />

          {/* Latest requests: the feed is not range-scoped (newest events of any
          age), so its label must not suggest it follows the range picker. */}
          <Section
            title="Latest requests (any time)"
            subtitle={
              <span>
                Newest first, not limited to the usage range — full view on Logs
                page{streamStatus === 'live' ? ' · ' : ' '}
                {/* The dot and its word wrap as one unit, so the status never
                lands alone at the end of a wrapped line. */}
                <span className="inline-flex items-center gap-1.5 whitespace-nowrap align-middle">
                  {live.permanentFailure ? (
                    <AlertTriangle
                      aria-hidden="true"
                      strokeWidth={1.75}
                      className="w-3 h-3 text-danger-text"
                    />
                  ) : (
                    <span
                      className={cx(
                        'status-dot',
                        streamStatus === 'live'
                          ? 'live'
                          : streamStatus === 'error'
                            ? 'danger'
                            : streamStatus === 'connecting' ||
                                streamStatus === 'reconnecting'
                              ? 'warn animate-pulse'
                              : 'neutral',
                      )}
                    />
                  )}
                  {streamStatus === 'live' ? 'streaming' : null}
                </span>
              </span>
            }
            action={
              <Link to="/logs" className={OVERVIEW_LINK_CLASS}>
                Open logs
              </Link>
            }
          >
            {/* A preview, not a feed: the newest rows at their natural
            height, with the full history one click away on Logs. A card
            from `md`; on phones the rows run to the screen edges. */}
            <div className="min-w-0 max-md:-mx-4 md:glass md:rounded-md">
              <div className="relative overflow-x-auto scroll-fade-right">
                <RequestEventsTable
                  events={latestRows}
                  principalNameMap={principalNameMap}
                  loading={events.isLoading}
                  liveFlashIds={recentLiveIds}
                  columns={OVERVIEW_TABLE_COLUMNS}
                  minWidthClass="min-w-[1080px]"
                  emptyTitle="No recent requests"
                />
              </div>
            </div>
          </Section>
        </>
      )}
    </PageContainer>
  );
}
