import { Meter as BaseMeter } from '@base-ui/react/meter';
import { Popover as BasePopover } from '@base-ui/react/popover';
import { createFileRoute, Link } from '@tanstack/react-router';
import {
  Activity,
  AlertTriangle,
  ArrowUpRight,
  ChevronRight,
  Database,
  Gauge,
  Hourglass,
  ShieldCheck,
  Timer,
  TrendingUp,
  Users,
} from 'lucide-react';
import { memo, useEffect, useMemo, useRef, useState } from 'react';
import {
  Area,
  AreaChart,
  CartesianGrid,
  ReferenceLine,
  Tooltip as RTooltip,
  XAxis,
  YAxis,
} from 'recharts';
import { LiveTailFailureBanner } from '../components/LiveTailFailureBanner';
import {
  FirstRunChecklist,
  useFirstRunIncomplete,
} from '../components/onboarding/FirstRunChecklist';
import { BreakdownPopover } from '../components/ui/BreakdownPopover';
import { Sparkline } from '../components/ui/charts';
import {
  Card,
  CardHeader,
  cx,
  PageContainer,
  PageHeader,
  Section,
  SegmentedControl,
  Skeleton,
} from '../components/ui/primitives';
import { RequestEventsTable } from '../components/ui/RequestEventsTable';
import {
  addBucketCostMicros,
  type CostComponentMicros,
  costCategorySegments,
  emptyCostComponents,
  sumCostMicros,
} from '../components/ui/usage/costCategories';
import { OAuthReconnectSummary } from '../components/upstreams/OAuthReconnectNotice';
import type { AggregateResponse } from '../lib/api';
import { getWindowColor, SERIES_FILL_OPACITY } from '../lib/colors';
import {
  cacheHitRatio,
  cacheMissRatio,
  formatCostMicros,
  formatCount,
  formatRate,
  formatUsdAmount,
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
  useUpstreamNameMap,
  useUsage,
} from '../lib/queries';
import {
  QUOTA_SEVERITY_TEXT_CLASS,
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
  type ClosestToLimitEntry,
  closestToLimit,
  formatResetIn,
  POOL_QUOTA_WINDOWS,
  type PoolQuotaChartRow,
  type PoolQuotaLatest,
  type PoolQuotaWindow,
  poolQuotaChartMax,
  poolQuotaResponseLatest,
} from './-overviewPoolQuota';
export const Route = createFileRoute('/')({
  component: OverviewPage,
});

const RANGES = TIME_PRESETS;
type Range = TimePreset;
const RANGE_OPTIONS = TIME_PRESET_OPTIONS;
const RANGE_SECONDS = TIME_PRESET_SECONDS;
const OVERVIEW_TABLE_COLUMNS = { cost: true, tokens: true } as const;
// The overview previews real user requests only: renewals and non-messages
// endpoints are excluded server-side, and merged rows are re-checked against
// the same effective-kind rule so retained data cannot leak other categories.
const OVERVIEW_EVENT_FILTERS = { event_kind: 'messages' } as const;

const stepFor = (r: Range): 'hour' | 'minute' =>
  r === '7d' || r === '24h' ? 'hour' : 'minute';

function rangeLabelForSecs(rangeSecs: number): string {
  return (
    RANGES.find((candidate) => RANGE_SECONDS[candidate] === rangeSecs) ??
    `${rangeSecs}s`
  );
}

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
  secondaryValue?: number | null;
};

/** Optional second line drawn on a fixed 0–100 scale over the primary area. */
type KpiSecondarySeries = {
  testId: string;
  label: string;
  color: string;
  format: (value: number | null | undefined) => string;
};

const NO_KPI_POINTS: readonly KpiChartPoint[] = [];

/** One-decimal percent of a 0–100 value. */
function fmtPercent(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return '—';
  return `${value.toFixed(1)}%`;
}

/** One-decimal percent of a 0–1 ratio. */
function fmtRatioPercent(ratio: number | null | undefined): string {
  if (ratio == null || !Number.isFinite(ratio)) return '—';
  return fmtPercent(ratio * 100);
}

function fmtErrorPercent(value: number): string {
  return `${value.toFixed(2)}%`;
}

// The secondary series owns its geometry inside a `0 0 100 100` viewBox scaled
// with preserveAspectRatio="none", inset so a 0% or 100% bucket keeps its full
// stroke inside the 28px chart band.
const KPI_SECONDARY_TOP = 4;
const KPI_SECONDARY_BOTTOM = 96;

/** Horizontal position (percent) of bucket `index`; matches Sparkline's plot. */
function kpiPointX(index: number, count: number): number {
  return count > 1 ? (index / (count - 1)) * 100 : 50;
}

function kpiSecondaryY(value: number): number {
  const clamped = value < 0 ? 0 : value > 100 ? 100 : value;
  return (
    KPI_SECONDARY_BOTTOM -
    (clamped / 100) * (KPI_SECONDARY_BOTTOM - KPI_SECONDARY_TOP)
  );
}

// KPI series are data, not status: they draw in neutral ink. Only the error
// rate turns danger, and only when there are errors to report.
const KPI_SERIES_COLOR = 'var(--color-text-muted)';

const TOKENS_CACHE_MISS_SERIES: KpiSecondarySeries = {
  testId: 'overview-kpi-secondary-tokens',
  label: 'Cache miss',
  color: 'var(--color-accent)',
  format: fmtPercent,
};

/**
 * Hairline dividers between the five KPI cells of the traffic card at each
 * grid width (2 columns, then 3, with the last cell spanning the rest of its
 * row; then 5), so no row edge ever doubles up against the card border.
 */
const KPI_CELL_CLASS = [
  'border-row',
  'border-row border-l',
  'border-row border-t md:border-t-0 md:border-l',
  'border-row border-t border-l md:border-l-0 xl:border-t-0 xl:border-l',
  'border-row col-span-2 border-t md:border-l xl:col-span-1 xl:border-t-0',
] as const;

export function ValueTile({
  icon,
  label,
  value,
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
  icon: React.ReactNode;
  label: string;
  value: React.ReactNode;
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
  // Memoized so a hover on any sibling tile does not re-render Recharts.
  const primarySeries = useMemo(
    () => <Sparkline color={color} data={values} />,
    [color, values],
  );
  // Buckets that reported a cache-hit ratio, grouped into contiguous runs: an
  // idle bucket has no ratio, so the line breaks there instead of interpolating
  // across the gap. A run of one bucket has no segment to draw, so it is painted
  // as a round dot — a `<circle>` would be squashed into a sub-pixel ellipse by
  // preserveAspectRatio="none", while a nonzero-length subpath with round caps
  // and a non-scaling stroke stays circular in device space.
  const secondaryShapes = useMemo(() => {
    if (!secondary) return [];
    const shapes: { key: string; d: string; isPoint: boolean }[] = [];
    let run: string[] = [];
    const flush = (afterIndex: number) => {
      if (run.length === 0) return;
      const start = afterIndex - run.length;
      const coords = run.join(' L ');
      shapes.push(
        run.length === 1
          ? { key: `point-${start}`, d: `M ${coords} h 0.01`, isPoint: true }
          : { key: `segment-${start}`, d: `M ${coords}`, isPoint: false },
      );
      run = [];
    };
    for (let i = 0; i < points.length; i++) {
      const secondaryValue = points[i]?.secondaryValue;
      if (secondaryValue == null || !Number.isFinite(secondaryValue)) {
        flush(i);
        continue;
      }
      run.push(
        `${kpiPointX(i, points.length).toFixed(2)},${kpiSecondaryY(secondaryValue).toFixed(2)}`,
      );
    }
    flush(points.length);
    return shapes;
  }, [points, secondary]);

  const activeX =
    activeIdx == null ? null : `${kpiPointX(activeIdx, points.length)}%`;
  const secondaryMarker =
    secondary && activeX != null && activePoint?.secondaryValue != null
      ? {
          color: secondary.color,
          left: activeX,
          top: `${kpiSecondaryY(activePoint.secondaryValue)}%`,
        }
      : null;

  const handleMove = (event: React.MouseEvent<HTMLDivElement>) => {
    if (!onActiveIndexChange || points.length === 0) return;
    const rect = event.currentTarget.getBoundingClientRect();
    const ratio = rect.width > 0 ? (event.clientX - rect.left) / rect.width : 0;
    const clamped = ratio < 0 ? 0 : ratio > 1 ? 1 : ratio;
    onActiveIndexChange(Math.round(clamped * (points.length - 1)));
  };

  return (
    <div
      className={cx(
        'relative flex min-w-0 flex-col gap-1.5 p-3 md:p-4',
        className,
      )}
      data-testid={`overview-kpi-${chartId}`}
      data-slot="kpi-tile"
    >
      <div className="flex items-center gap-1.5 text-text-faint">
        <span className="size-3.5 [&_svg]:size-3.5">{icon}</span>
        <span className="text-label truncate">{label}</span>
      </div>
      <div
        className="flex h-7 min-w-0 items-center justify-between gap-2"
        data-slot="value"
      >
        {loading ? (
          <Skeleton className="h-6 w-20" />
        ) : (
          <span className="truncate text-2xl font-medium leading-none tabular-nums text-text">
            {value}
          </span>
        )}
      </div>
      {sub !== undefined ? (
        <div
          className="flex h-4 items-center truncate text-caption text-text-faint"
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
          <span className="truncate text-caption leading-none tabular-nums text-text">
            {`${chartLabel ?? label} ${formatChartValue(activePoint.value)}`}
          </span>
          {secondary ? (
            // Muted body text rather than the series stroke, so the row keeps
            // body-text contrast in both themes. The stroke color stays on
            // the chart itself.
            <span className="truncate text-caption leading-none tabular-nums text-text-muted">
              {`${secondary.label} ${secondary.format(activePoint.secondaryValue)}`}
            </span>
          ) : null}
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
            {primarySeries}
            {secondary && secondaryShapes.length > 0 ? (
              <svg
                aria-hidden="true"
                className="pointer-events-none absolute inset-0 h-full w-full"
                preserveAspectRatio="none"
                viewBox="0 0 100 100"
              >
                <g data-testid={secondary.testId}>
                  {secondaryShapes.map((shape) => (
                    <path
                      d={shape.d}
                      data-slot={
                        shape.isPoint ? 'secondary-point' : 'secondary-segment'
                      }
                      fill="none"
                      key={shape.key}
                      stroke={secondary.color}
                      strokeLinecap="round"
                      strokeLinejoin="round"
                      strokeOpacity={0.9}
                      strokeWidth={shape.isPoint ? 3 : 1.2}
                      vectorEffect="non-scaling-stroke"
                    />
                  ))}
                </g>
              </svg>
            ) : null}
            {activeX != null ? (
              <div
                aria-hidden="true"
                className="pointer-events-none absolute inset-y-0 w-px -translate-x-1/2 bg-border-strong"
                style={{ left: activeX }}
              />
            ) : null}
            {secondaryMarker ? (
              <div
                aria-hidden="true"
                className="pointer-events-none absolute h-1.5 w-1.5 -translate-x-1/2 -translate-y-1/2 rounded-full"
                style={{
                  backgroundColor: secondaryMarker.color,
                  left: secondaryMarker.left,
                  top: secondaryMarker.top,
                }}
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
   * any — legacy windows keep a single-tone bar instead of an invented split.
   */
  cost_components_micros: CostComponentMicros | null;
  tokens: number;
  requests: number;
  cache_hit_ratio: number | null;
  share_pct: number;
  /** Largest `cost_micros` on the card: the full-length reference for a meter. */
  max_cost_micros: number;
};

const TOP_PRINCIPAL_ROW_CLASS =
  'flex min-h-[66px] items-center gap-3 border-b border-row px-4 py-2 last:border-b-0';

const PRINCIPAL_COST_NOTE = 'Per-category cost not recorded for this window';

/**
 * Cost meter for one principal row. Length is the principal's share of the
 * largest principal; the filled part is subdivided into the request-log cost
 * categories, with a neutral tail for cost the categories do not account for.
 * Exact figures stay out of the row and live in the hover/focus breakdown and
 * in the meter's value text, so the row keeps its geometry and its reading.
 */
function PrincipalCostMeter({ principal }: { principal: TopPrincipal }) {
  const [open, setOpen] = useState(false);
  const hoverTimerRef = useRef<number | undefined>(undefined);
  useEffect(
    () => () => {
      window.clearTimeout(hoverTimerRef.current);
    },
    [],
  );
  const totalMicros = principal.cost_micros;
  const components = principal.cost_components_micros;
  const attributedMicros = components ? sumCostMicros(components) : 0;
  const unattributedMicros = components
    ? Math.max(0, totalMicros - attributedMicros)
    : 0;
  const segments = components
    ? costCategorySegments(components, unattributedMicros)
    : [];
  // Segments add up to this, so widths fill the bar exactly even if recorded
  // components run past the rollup total.
  const segmentBasisMicros = Math.max(totalMicros, attributedMicros);
  const sharePct =
    principal.max_cost_micros > 0
      ? (totalMicros / principal.max_cost_micros) * 100
      : 0;
  const valueText = [
    `Total ${formatCostMicros(totalMicros)}`,
    `${fmtPercent(sharePct)} of the largest principal`,
    components
      ? segments
          .map(
            (segment) => `${segment.label} ${formatCostMicros(segment.value)}`,
          )
          .join(', ')
      : PRINCIPAL_COST_NOTE,
  ].join('; ');

  return (
    <BasePopover.Root open={open} onOpenChange={setOpen}>
      <BasePopover.Trigger
        aria-label={`${principal.name} cost breakdown`}
        className="mt-1.5 block w-full cursor-help rounded-sm text-left focus-visible:outline focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-2"
        data-testid="top-principal-cost-trigger"
        delay={200}
        onBlur={() => setOpen(false)}
        onFocus={() => setOpen(true)}
        onPointerEnter={() => {
          window.clearTimeout(hoverTimerRef.current);
          hoverTimerRef.current = window.setTimeout(() => setOpen(true), 200);
        }}
        onPointerLeave={() => {
          window.clearTimeout(hoverTimerRef.current);
          setOpen(false);
        }}
        openOnHover
        type="button"
      >
        <BaseMeter.Root
          aria-label={`${principal.name} cost`}
          data-cost-components={
            components
              ? unattributedMicros > 0
                ? 'partial'
                : 'complete'
              : 'unavailable'
          }
          data-testid="top-principal-cost-meter"
          getAriaValueText={() => valueText}
          max={Math.max(1, principal.max_cost_micros)}
          value={totalMicros}
        >
          <BaseMeter.Track className="relative h-1.5 w-full overflow-hidden rounded-xs bg-progress-track">
            <BaseMeter.Indicator
              className={cx(
                'flex h-full overflow-hidden transition-all',
                components ? '' : 'bg-text-muted',
              )}
              data-slot="cost-meter-fill"
            >
              {segments.map((segment) =>
                segment.value <= 0 ? null : (
                  <span
                    key={segment.key}
                    className="h-full"
                    data-category={segment.key}
                    data-testid="top-principal-cost-segment"
                    style={{
                      backgroundColor: segment.color,
                      width: `${(segment.value / segmentBasisMicros) * 100}%`,
                    }}
                  />
                ),
              )}
            </BaseMeter.Indicator>
          </BaseMeter.Track>
        </BaseMeter.Root>
      </BasePopover.Trigger>
      <BasePopover.Portal>
        <BasePopover.Positioner side="top" sideOffset={4}>
          <BasePopover.Popup
            className="glass-strong z-50 rounded-md px-2 py-1.5 text-caption text-text"
            initialFocus={false}
          >
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
          </BasePopover.Popup>
        </BasePopover.Positioner>
      </BasePopover.Portal>
    </BasePopover.Root>
  );
}

export function TopPrincipalsCard({
  range,
  principals,
  loading,
}: {
  range: Range;
  principals: readonly TopPrincipal[];
  loading: boolean;
}) {
  return (
    <Card className="min-w-0 flex flex-col">
      <CardHeader
        title={
          <span className="inline-flex items-center gap-2">
            <Users className="w-3.5 h-3.5 text-text-faint" />
            Top principals
          </span>
        }
        subtitle={`By virtual cost · ${range}`}
      />
      <div className="flex-1 overflow-auto min-h-0 max-h-96 xl:max-h-none">
        <div
          className={cx(
            'flex flex-col',
            // Keep the loading and populated list at one height so rows do
            // not jump; an empty window collapses to a single line.
            (loading || principals.length > 0) && 'min-h-80',
          )}
          data-slot="principal-list"
        >
          {loading ? (
            Array.from({ length: 5 }).map((_, index) => (
              <div
                key={index}
                aria-hidden="true"
                className={TOP_PRINCIPAL_ROW_CLASS}
                data-testid="top-principal-skeleton-row"
              >
                <div className="min-w-0 flex-1">
                  <Skeleton className="h-5 w-2/5" />
                  <Skeleton className="mt-0.5 h-3 w-3/5" />
                  <Skeleton className="mt-1.5 h-1.5 rounded-xs" />
                </div>
                <div className="flex shrink-0 flex-col items-end gap-1 text-right">
                  <Skeleton className="h-5 w-16" />
                  <Skeleton className="h-3 w-10" />
                </div>
              </div>
            ))
          ) : principals.length === 0 ? (
            <div className="flex flex-1 items-center justify-center px-4 py-8 text-center text-body-sm text-text-muted">
              {`No usage in the last ${range}`}
            </div>
          ) : (
            principals.map((principal) => (
              <div
                key={principal.id}
                className={cx(
                  TOP_PRINCIPAL_ROW_CLASS,
                  'hover:bg-overlay-2 transition-colors',
                )}
                data-testid="top-principal-row"
              >
                <div className="min-w-0 flex-1">
                  <div className="truncate text-body-sm text-text">
                    {principal.name}
                  </div>
                  <div
                    className="truncate text-caption text-text-faint"
                    data-slot="principal-meta"
                  >
                    {formatCount(principal.requests)} req ·{' '}
                    {formatCount(principal.tokens)} tok ·{' '}
                    <span>
                      {`${fmtRatioPercent(principal.cache_hit_ratio)} cache hit`}
                    </span>
                  </div>
                  <PrincipalCostMeter principal={principal} />
                </div>
                <div className="text-right shrink-0">
                  <div className="text-body-sm font-medium tabular-nums text-text">
                    {formatUsdAmount(principal.cost_micros / 1_000_000)}
                  </div>
                  <div className="text-caption tabular-nums text-text-faint">
                    {principal.share_pct.toFixed(1)}%
                  </div>
                </div>
              </div>
            ))
          )}
        </div>
      </div>
    </Card>
  );
}

type AggregateWindow = AggregateResponse['windows'][number];

/**
 * Fill for a pool bar at each quota severity. The bar says how close the pool
 * is to its limit; window identity stays in the trend legend and series.
 */
const POOL_SEVERITY_FILL_VAR: Record<QuotaSeverity, string> = {
  none: 'var(--color-overlay-4)',
  ok: 'var(--color-text-muted)',
  warn: 'var(--color-warn)',
  danger: 'var(--color-danger)',
};
/** Shade steps that tell adjacent upstream segments (and their swatches) apart. */
const POOL_SEGMENT_SHADES = [100, 72, 52, 38] as const;
const POOL_WINDOW_LABEL: Record<PoolQuotaWindow, string> = {
  '5h': '5h',
  '7d': '7d',
  '7d_fable': '7d (Fable)',
};

const POOL_QUOTA_SNAPSHOT_SLOT_CLASS =
  'relative flex min-h-10 w-full flex-col justify-center';
const POOL_QUOTA_CHART_SLOT_CLASS = 'relative h-64 min-w-0 w-full';
const POOL_QUOTA_LEGEND_ITEM_CLASS = 'inline-flex min-h-4 items-center gap-1.5';

function poolSegmentColor(severity: QuotaSeverity, index: number): string {
  const shade = POOL_SEGMENT_SHADES[index % POOL_SEGMENT_SHADES.length];
  return `color-mix(in oklab, ${POOL_SEVERITY_FILL_VAR[severity]} ${shade}%, var(--color-bg))`;
}

function PoolQuotaPopoverContent({
  w,
  severity,
  activeIdx,
}: {
  w: AggregateWindow;
  severity: QuotaSeverity;
  activeIdx: number | null;
}) {
  const totalRatio = w.provider_lots.reduce(
    (sum, lot) => sum + lot.capacity_ratio,
    0,
  );
  return (
    <div className="flex flex-col gap-0.5 text-body-sm text-text">
      <div className="flex items-center gap-3 px-2 pb-1.5 mb-0.5 border-b border-row text-label text-text-faint">
        <span className="flex-1 min-w-32">Upstream</span>
        <span className="w-14 text-right">Util</span>
        <span className="w-14 text-right">Weight</span>
        <span className="w-14 text-right">Impact</span>
      </div>
      {w.provider_lots.map((lot, i) => {
        const util = lot.utilization ?? 0;
        const weightedContribution =
          totalRatio > 0 ? ((util * lot.capacity_ratio) / totalRatio) * 100 : 0;
        const utilPct = lot.utilization != null ? util * 100 : null;
        const isHovered = activeIdx === i;
        return (
          <div
            key={lot.upstream_id}
            className={cx(
              'flex items-center gap-3 px-2 py-1.5 rounded-sm transition-colors',
              isHovered && 'bg-overlay-5',
            )}
            data-testid="pool-quota-breakdown-row"
          >
            <div className="flex items-center gap-2 flex-1 min-w-32">
              <span
                aria-hidden="true"
                className="inline-block w-2.5 h-2.5 rounded-sm shrink-0"
                style={{ backgroundColor: poolSegmentColor(severity, i) }}
              />
              <span
                className={cx(
                  'truncate',
                  isHovered ? 'font-medium text-text' : 'text-text-muted',
                )}
                title={lot.upstream_name}
              >
                {lot.upstream_name}
              </span>
            </div>
            <span
              className={cx(
                'tabular-nums w-14 text-right font-medium',
                QUOTA_SEVERITY_TEXT_CLASS[quotaSeverity(utilPct)],
              )}
            >
              {utilPct != null ? `${utilPct.toFixed(1)}%` : '—'}
            </span>
            <span className="tabular-nums w-14 text-right text-text-muted">
              {lot.capacity_ratio.toFixed(1)}x
            </span>
            <span
              className={cx(
                'tabular-nums w-14 text-right',
                isHovered ? 'font-medium text-text' : 'text-text-muted',
              )}
            >
              {`${weightedContribution.toFixed(1)}%`}
            </span>
          </div>
        );
      })}
    </div>
  );
}

export function PoolQuotaStackedBar({
  window,
  w,
  loading = false,
}: {
  window: PoolQuotaWindow;
  w: AggregateWindow | undefined;
  loading?: boolean;
}) {
  const [activeIdx, setActiveIdx] = useState<number | null>(null);
  const label = POOL_WINDOW_LABEL[window];

  if (loading || !w) {
    return (
      <div className={cx(POOL_QUOTA_SNAPSHOT_SLOT_CLASS, 'gap-1.5')}>
        <div className="flex items-center justify-between">
          <span className="text-label text-text-muted">{label} pool</span>
          {loading ? (
            <Skeleton as="span" className="inline-block h-4 w-12" />
          ) : (
            <span className="text-caption text-text-faint">no data</span>
          )}
        </div>
        {loading ? (
          <Skeleton className="h-2 rounded-xs" />
        ) : (
          <div className="h-2 w-full rounded-xs bg-progress-track" />
        )}
      </div>
    );
  }

  const totalRatio = w.provider_lots.reduce(
    (sum, lot) => sum + lot.capacity_ratio,
    0,
  );
  const severity = quotaSeverity(w.utilization_percent);
  const weightedSegments = w.provider_lots
    .map((lot, index) => {
      const util = lot.utilization ?? 0;
      const weightedContribution =
        totalRatio > 0 ? ((util * lot.capacity_ratio) / totalRatio) * 100 : 0;
      return { index, lot, weightedContribution };
    })
    .filter((segment) => segment.weightedContribution > 0);
  const totalWeightedContribution = weightedSegments.reduce(
    (sum, segment) => sum + segment.weightedContribution,
    0,
  );
  const pctText =
    w.utilization_percent != null
      ? `${w.utilization_percent.toFixed(1)}%`
      : '—';

  return (
    <BasePopover.Root
      onOpenChange={(nextOpen) => {
        if (!nextOpen) setActiveIdx(null);
      }}
    >
      <BasePopover.Trigger
        aria-label={`${label} pool ${pctText}, show upstream breakdown`}
        className={cx(
          POOL_QUOTA_SNAPSHOT_SLOT_CLASS,
          'gap-1.5 text-left rounded-sm cursor-pointer focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-4',
        )}
        closeDelay={100}
        delay={100}
        openOnHover
      >
        <span className="flex items-center justify-between">
          <span className="text-label text-text-muted">{label} pool</span>
          <span
            className={cx(
              'tabular-nums font-medium text-body-sm leading-none',
              QUOTA_SEVERITY_TEXT_CLASS[severity],
            )}
            data-slot="pool-pct"
          >
            {pctText}
          </span>
        </span>
        <BaseMeter.Root
          className="h-2 w-full"
          max={100}
          value={Math.min(100, totalWeightedContribution)}
        >
          <BaseMeter.Track className="flex h-2 w-full overflow-hidden rounded-xs bg-progress-track">
            <BaseMeter.Indicator className="flex h-full gap-px">
              {weightedSegments.map(({ index, lot, weightedContribution }) => (
                <span
                  key={lot.upstream_id}
                  className={cx(
                    'h-full min-w-[2px] basis-0 transition-opacity',
                    activeIdx !== null && activeIdx !== index && 'opacity-50',
                  )}
                  data-testid="pool-quota-segment"
                  style={{
                    backgroundColor: poolSegmentColor(severity, index),
                    flexGrow: weightedContribution,
                  }}
                  onMouseEnter={() => setActiveIdx(index)}
                  onMouseLeave={() => setActiveIdx(null)}
                />
              ))}
            </BaseMeter.Indicator>
          </BaseMeter.Track>
        </BaseMeter.Root>
      </BasePopover.Trigger>

      <BasePopover.Portal>
        <BasePopover.Positioner align="start" side="bottom" sideOffset={8}>
          <BasePopover.Popup
            aria-label={`${label} pool by upstream`}
            className="glass-strong z-50 w-max min-w-[max(var(--anchor-width),22rem)] max-w-[calc(100vw-1rem)] rounded-md p-2"
            initialFocus={false}
          >
            <PoolQuotaPopoverContent
              w={w}
              severity={severity}
              activeIdx={activeIdx}
            />
          </BasePopover.Popup>
        </BasePopover.Positioner>
      </BasePopover.Portal>
    </BasePopover.Root>
  );
}

const CLOSEST_TO_LIMIT_ROWS = 5;
// Name, bar (capped so it never runs across the row), % right after the bar,
// reset text, chevron. Below md: name + % on one line, bar and reset below.
const CLOSEST_ROW_CLASS =
  'grid grid-cols-[minmax(0,1fr)_auto] md:grid-cols-[minmax(8rem,14rem)_minmax(6rem,22rem)_3rem_minmax(7rem,1fr)_1rem] items-center gap-x-3 gap-y-1.5 px-4 py-2.5';

export function ClosestToLimitCard({
  aggregate,
  loading,
}: {
  aggregate: AggregateResponse | undefined;
  loading: boolean;
}) {
  const { effective: timeZone } = useTimezone();
  const entries = useMemo(() => closestToLimit(aggregate), [aggregate]);
  const shown = entries.slice(0, CLOSEST_TO_LIMIT_ROWS);
  const hidden = entries.length - shown.length;
  return (
    <Card
      aria-busy={loading}
      className="min-w-0"
      data-testid="closest-to-limit"
    >
      <CardHeader
        align="center"
        title={
          <span className="inline-flex items-center gap-2">
            <Hourglass className="w-3.5 h-3.5 text-text-faint" />
            Closest to limit
          </span>
        }
        subtitle="Each upstream's most-used quota window"
        action={
          hidden > 0 ? (
            <Link
              to="/upstreams"
              className="text-label text-accent-text hover:underline inline-flex items-center gap-1 rounded-sm focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-2"
            >
              {`${hidden} more`} <ArrowUpRight className="w-3 h-3" />
            </Link>
          ) : undefined
        }
      />
      {loading ? (
        <ul aria-hidden="true" className="divide-y divide-row">
          {Array.from({ length: 3 }).map((_, index) => (
            <li key={index} className={CLOSEST_ROW_CLASS}>
              <Skeleton className="h-4 w-32" />
              <Skeleton className="h-4 w-10 md:order-3" />
              <Skeleton className="col-span-2 h-1.5 rounded-xs md:col-span-1 md:order-2" />
            </li>
          ))}
        </ul>
      ) : shown.length === 0 ? (
        <p className="px-4 py-8 text-center text-body-sm text-text-muted">
          No quota readings from any upstream yet.
        </p>
      ) : (
        <ul className="divide-y divide-row">
          {shown.map((entry) => (
            <ClosestToLimitRow
              key={entry.upstreamId}
              entry={entry}
              nowUnixSecs={aggregate?.now_unix_secs ?? Date.now() / 1000}
              timeZone={timeZone}
            />
          ))}
        </ul>
      )}
    </Card>
  );
}

function ClosestToLimitRow({
  entry,
  nowUnixSecs,
  timeZone,
}: {
  entry: ClosestToLimitEntry;
  nowUnixSecs: number;
  timeZone: string;
}) {
  const severity = quotaSeverity(entry.utilizationPercent);
  const reset = formatResetIn(entry.resetUnixSecs, nowUnixSecs);
  const resetAt =
    entry.resetUnixSecs != null
      ? `Resets ${formatInTimezone(entry.resetUnixSecs * 1000, timeZone).replace('T', ' ')}`
      : undefined;
  const meta = [reset, entry.state === 'stale' ? 'stale reading' : null]
    .filter(Boolean)
    .join(' · ');
  return (
    <li>
      <Link
        to="/upstreams"
        search={{ selectedId: entry.upstreamId }}
        className={cx(
          CLOSEST_ROW_CLASS,
          'group hover:bg-overlay-2 transition-colors focus-visible:outline-2 focus-visible:outline-accent focus-visible:-outline-offset-2',
        )}
        data-testid="closest-to-limit-row"
      >
        <span className="min-w-0 flex items-baseline gap-2">
          <span className="truncate text-body-sm text-text">
            {entry.upstreamName}
          </span>
          <span className="shrink-0 text-caption text-text-faint">
            {POOL_WINDOW_LABEL[entry.window]}
          </span>
        </span>
        <span
          className={cx(
            'text-right tabular-nums text-body-sm font-medium md:order-3 md:text-left',
            QUOTA_SEVERITY_TEXT_CLASS[severity],
          )}
          data-slot="closest-pct"
        >
          {`${entry.utilizationPercent.toFixed(0)}%`}
        </span>
        <span
          aria-hidden="true"
          className="col-span-2 md:col-span-1 md:order-2 h-1.5 rounded-xs bg-progress-track overflow-hidden"
        >
          <span
            className="block h-full"
            style={{
              width: `${Math.min(100, entry.utilizationPercent)}%`,
              backgroundColor: POOL_SEVERITY_FILL_VAR[severity],
            }}
          />
        </span>
        <span
          className="col-span-2 md:col-span-1 md:order-4 text-caption text-text-faint tabular-nums truncate"
          title={resetAt}
        >
          {meta || '—'}
        </span>
        <ChevronRight
          aria-hidden="true"
          className="hidden md:block md:order-5 w-3.5 h-3.5 text-text-faint group-hover:text-text"
        />
      </Link>
    </li>
  );
}

type PoolQuotaChartProps = {
  data: PoolQuotaChartRow[];
  maxValue: number;
  rangeStartUnix: number;
  rangeEndUnix: number;
  range: string;
  latest: PoolQuotaLatest;
  showFable: boolean;
};

const PoolQuotaCard = memo(function PoolQuotaCard({
  aggregate,
  chart,
  loading,
}: {
  aggregate: AggregateResponse | undefined;
  chart: PoolQuotaChartProps;
  loading: boolean;
}) {
  const w5h = aggregate?.windows.find((x) => x.window === '5h');
  const w7d = aggregate?.windows.find((x) => x.window === '7d');
  const wFable = aggregate?.windows.find((x) => x.window === '7d_fable');
  const upstreamCount = aggregate?.upstream_count ?? 0;
  const contributingCount = Math.max(
    w5h?.contributing_upstreams ?? 0,
    w7d?.contributing_upstreams ?? 0,
    wFable?.contributing_upstreams ?? 0,
  );
  return (
    <Card
      aria-busy={loading}
      className="min-w-0 flex flex-col"
      data-testid="pool-quota-card"
    >
      <CardHeader
        title={
          <span className="inline-flex items-center gap-2">
            <Gauge className="w-3.5 h-3.5 text-text-faint" />
            Pool quota
          </span>
        }
        subtitle={
          upstreamCount > 0
            ? `Plan-weighted · ${formatCount(contributingCount)} of ${formatCount(upstreamCount)} upstreams`
            : 'Plan-weighted'
        }
      />
      <div className="flex flex-col gap-4 p-4 pt-1">
        <div className="grid grid-cols-1 gap-x-10 gap-y-3 md:grid-cols-2 xl:grid-cols-3">
          {POOL_QUOTA_WINDOWS.map((window) => (
            <div
              key={window}
              data-testid="pool-quota-snapshot-slot"
              className={POOL_QUOTA_SNAPSHOT_SLOT_CLASS}
            >
              <PoolQuotaStackedBar
                window={window}
                w={aggregate?.windows.find((entry) => entry.window === window)}
                loading={loading}
              />
            </div>
          ))}
        </div>
        <div className="border-t border-row" />
        <div className="flex flex-col gap-3">
          <div className="flex flex-col items-start gap-2 sm:flex-row sm:items-center sm:justify-between">
            <div className="text-label text-text-muted">
              {`Last ${chart.range}`}
            </div>
            <PoolQuotaLegend
              latest={chart.latest}
              showFable={chart.showFable}
              loading={loading}
            />
          </div>
          <div
            className={POOL_QUOTA_CHART_SLOT_CLASS}
            data-testid="pool-quota-chart-slot"
          >
            {loading ? (
              <Skeleton className="absolute inset-0 h-full w-full" />
            ) : (
              <PoolQuotaThemedChart
                seriesData={chart.data}
                rangeStartUnix={chart.rangeStartUnix}
                rangeEndUnix={chart.rangeEndUnix}
                maxValue={chart.maxValue}
                showFable={chart.showFable}
              />
            )}
          </div>
        </div>
      </div>
    </Card>
  );
});

export function PoolQuotaThemedChart({
  seriesData,
  maxValue,
  rangeStartUnix,
  rangeEndUnix,
  showFable,
}: {
  seriesData: PoolQuotaChartRow[];
  maxValue: number;
  rangeStartUnix: number;
  rangeEndUnix: number;
  showFable: boolean;
}) {
  const c5h = getWindowColor('5h');
  const c7d = getWindowColor('7d');
  const cFable = getWindowColor('7d_fable');

  return (
    <div className="relative size-full min-h-0 min-w-0">
      {!seriesData.length ? (
        <div className="absolute inset-0 z-10 flex items-center justify-center text-body-sm text-text-muted pointer-events-none">
          No timeline data yet for this range
        </div>
      ) : null}
      <AreaChart
        responsive
        className="size-full"
        data={seriesData}
        margin={{ top: 8, right: 0, bottom: 0, left: 0 }}
      >
        <CartesianGrid vertical={false} stroke="var(--color-border-row)" />
        <XAxis
          dataKey="unix"
          type="number"
          domain={[rangeStartUnix, rangeEndUnix]}
          allowDataOverflow
          tick={{ fontSize: 11 }}
          tickFormatter={fmtChartTick}
          axisLine={false}
          tickLine={false}
          minTickGap={48}
          tickMargin={8}
        />
        <YAxis
          tick={{ fontSize: 11 }}
          tickFormatter={(v) => `${v}%`}
          axisLine={false}
          tickLine={false}
          width={44}
          tickMargin={8}
          domain={[0, maxValue]}
          ticks={maxValue <= 100 ? [0, 50, 100] : [0, 50, 100, maxValue]}
          allowDataOverflow={false}
        />
        <ReferenceLine
          y={80}
          stroke="var(--color-warn)"
          strokeOpacity={0.5}
          strokeDasharray="3 3"
          label={{
            position: 'insideTopRight',
            value: '80%',
            fill: 'var(--color-warn-text)',
            fontSize: 11,
          }}
        />
        <ReferenceLine
          y={95}
          stroke="var(--color-danger)"
          strokeOpacity={0.5}
          strokeDasharray="3 3"
          label={{
            position: 'insideTopRight',
            value: '95%',
            fill: 'var(--color-danger-text)',
            fontSize: 11,
          }}
        />
        <RTooltip
          cursor={{
            stroke: 'var(--color-border-strong)',
            strokeWidth: 1,
          }}
          content={({ active, payload, label }) => {
            if (!active || !payload?.length) return null;
            return (
              <div className="glass-strong min-w-36 rounded-md px-3 py-2 text-caption text-text">
                <div className="mb-1.5 tabular-nums text-text-muted">
                  {fmtChartTooltip(Number(label))}
                </div>
                {payload.map((p) => {
                  const w = String(p.dataKey);
                  const wLabel =
                    w === '5h'
                      ? '5h window'
                      : w === '7d'
                        ? '7d window'
                        : w === '7d_fable'
                          ? 'Fable window'
                          : w;
                  return (
                    <div
                      key={w}
                      className="flex items-center justify-between gap-3 py-0.5"
                    >
                      <span className="inline-flex items-center gap-1.5 text-text-muted">
                        <span
                          aria-hidden="true"
                          className="h-0.5 w-2.5 rounded-xs"
                          style={{
                            background:
                              typeof p.color === 'string'
                                ? p.color
                                : 'var(--color-text)',
                          }}
                        />
                        {wLabel}
                      </span>
                      <span className="font-medium tabular-nums">
                        {typeof p.value === 'number'
                          ? `${p.value.toFixed(1)}%`
                          : '—'}
                      </span>
                    </div>
                  );
                })}
              </div>
            );
          }}
        />
        {/* Series overlap, so only the 5h window carries a flat fill; the
        longer windows draw as lines. */}
        {showFable ? (
          <Area
            type="monotone"
            dataKey="7d_fable"
            stroke={cFable.stroke}
            strokeWidth={1.5}
            fill="none"
            isAnimationActive={false}
            connectNulls={false}
          />
        ) : null}
        <Area
          type="monotone"
          dataKey="7d"
          stroke={c7d.stroke}
          strokeWidth={1.5}
          fill="none"
          isAnimationActive={false}
          connectNulls={false}
        />
        <Area
          type="monotone"
          dataKey="5h"
          stroke={c5h.stroke}
          strokeWidth={1.5}
          fill={c5h.fill}
          fillOpacity={SERIES_FILL_OPACITY}
          isAnimationActive={false}
          connectNulls={false}
        />
      </AreaChart>
    </div>
  );
}

export function PoolQuotaLegend({
  latest,
  showFable,
  loading = false,
}: {
  latest?: PoolQuotaLatest;
  showFable: boolean;
  loading?: boolean;
}) {
  return (
    <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-caption text-text-muted">
      {POOL_QUOTA_WINDOWS.filter(
        (window) => showFable || window !== '7d_fable',
      ).map((window) => {
        const color = getWindowColor(window);
        const value = latest?.[window];
        const label = window === '7d_fable' ? 'Fable' : window;
        return (
          <span
            key={window}
            className={POOL_QUOTA_LEGEND_ITEM_CLASS}
            data-testid="pool-quota-legend-slot"
          >
            <span
              aria-hidden="true"
              className="h-0.5 w-2.5 rounded-xs"
              style={{ background: color.stroke }}
            />
            {label}
            {loading ? (
              <Skeleton as="span" className="inline-block h-3 w-8" />
            ) : value != null ? (
              <>
                {' · '}
                <span className="tabular-nums text-text">{`${value.toFixed(0)}%`}</span>
              </>
            ) : null}
          </span>
        );
      })}
    </div>
  );
}

function OverviewPage() {
  const [range, setRange] = useState<Range>('24h');
  // One hover index shared by every KPI chart so all five read the same bucket.
  const [activeKpiIndex, setActiveKpiIndex] = useState<number | null>(null);
  // Switching windows re-buckets every series, so an index carried over from
  // the previous window would address unrelated data: drop it with the range.
  const selectRange = (next: Range) => {
    setActiveKpiIndex(null);
    setRange(next);
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
  const upstreamNameMap = useUpstreamNameMap();

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
  const showFable = POOL_QUOTA_WINDOWS.includes('7d_fable');
  const quotaLoading =
    (quotaAggregate.data === undefined && quotaAggregate.isPending) ||
    (quotaPoolHistory.data === undefined && quotaPoolHistory.isPending);

  const live = useLiveEventStream(OVERVIEW_EVENT_FILTERS);
  const streamStatus = live.status;
  const scrollContainerRef = useRef<HTMLDivElement>(null);
  const sentinelRef = useRef<HTMLTableRowElement>(null);

  useEffect(() => {
    const el = sentinelRef.current;
    if (!el || !events.hasNextPage || events.isFetchingNextPage) return;
    const obs = new IntersectionObserver(
      (entries) =>
        entries.forEach((e) => {
          if (e.isIntersecting) events.fetchNextPage();
        }),
      { root: scrollContainerRef.current, threshold: 0.1 },
    );
    obs.observe(el);
    return () => obs.disconnect();
  }, [events.hasNextPage, events.isFetchingNextPage, events.fetchNextPage]);

  // biome-ignore lint/correctness/useExhaustiveDependencies: live.eventsMap is a stable Map ref mutated in place by useLiveEventStream; live.version is bumped on every upsert so it is the real re-run trigger.
  const recentRows = useMemo(
    () =>
      mergeLogRows(
        live.eventsMap,
        events.data?.pages.flatMap((page) => page.events) ?? [],
      ).filter(isMessagesRequestEvent),
    [live.eventsMap, live.version, events.data],
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

  // Anchor the visible window and its label to the last successful response.
  // A range control may move immediately, but placeholder data keeps its own
  // plot context until the replacement response lands.
  const poolHistoryNowUnixSecs =
    quotaPoolHistory.data?.now_unix_secs ?? Math.floor(Date.now() / 1000);
  const displayedPoolHistoryRangeSecs =
    quotaPoolHistory.data?.range_secs ?? seriesRangeSecs;
  const displayedPoolHistoryRange = rangeLabelForSecs(
    displayedPoolHistoryRangeSecs,
  );
  const chartData = useMemo(
    () => buildPoolQuotaChartData(quotaPoolHistory.data?.windows, showFable),
    [quotaPoolHistory.data, showFable],
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

  const chartMaxValue = useMemo(
    () => poolQuotaChartMax(visibleChartData, showFable),
    [visibleChartData, showFable],
  );

  const chartLatest = useMemo(
    () =>
      poolQuotaResponseLatest(
        quotaPoolHistory.data?.windows,
        visibleChartData,
        showFable,
      ),
    [quotaPoolHistory.data, visibleChartData, showFable],
  );
  const poolQuotaChart = useMemo<PoolQuotaChartProps>(
    () => ({
      data: visibleChartData,
      maxValue: chartMaxValue,
      rangeStartUnix: poolHistoryNowUnixSecs - displayedPoolHistoryRangeSecs,
      rangeEndUnix: poolHistoryNowUnixSecs,
      range: displayedPoolHistoryRange,
      latest: chartLatest,
      showFable,
    }),
    [
      chartLatest,
      chartMaxValue,
      displayedPoolHistoryRange,
      displayedPoolHistoryRangeSecs,
      poolHistoryNowUnixSecs,
      showFable,
      visibleChartData,
    ],
  );

  // Principals Data
  const topPrincipals = useMemo(() => {
    const series = principalUsage.data?.series ?? [];
    const byId = new Map<
      string,
      Omit<TopPrincipal, 'share_pct' | 'max_cost_micros'>
    >();
    let maxCostMicros = 0;
    let totalCostMicros = 0;

    for (const s of series) {
      if (!s.key) continue;
      let costMicros = 0;
      let tokens = 0;
      let requests = 0;
      let inputTokens = 0;
      let cacheCreationTokens = 0;
      let cacheReadTokens = 0;
      const components = emptyCostComponents();
      let recordedComponents = false;
      for (const b of s.buckets) {
        costMicros += b.virtual_cost_micros ?? 0;
        tokens += sumTokens(b);
        requests += b.request_count ?? 0;
        inputTokens += b.input_tokens ?? 0;
        cacheCreationTokens += b.cache_creation_input_tokens ?? 0;
        cacheReadTokens += b.cache_read_input_tokens ?? 0;
        if (addBucketCostMicros(b, components)) recordedComponents = true;
      }
      if (costMicros <= 0 && requests <= 0) continue;

      totalCostMicros += costMicros;
      if (costMicros > maxCostMicros) maxCostMicros = costMicros;

      byId.set(s.key, {
        id: s.key,
        name: principalNameMap.get(s.key) ?? s.key,
        cost_micros: costMicros,
        cost_components_micros: recordedComponents ? components : null,
        tokens,
        requests,
        cache_hit_ratio: cacheHitRatio({
          input_tokens: inputTokens,
          cache_creation_input_tokens: cacheCreationTokens,
          cache_read_input_tokens: cacheReadTokens,
        }),
      });
    }

    return Array.from(byId.values())
      .sort((a, b) => b.cost_micros - a.cost_micros)
      .slice(0, 5)
      .map((p) => ({
        ...p,
        share_pct:
          totalCostMicros > 0 ? (p.cost_micros / totalCostMicros) * 100 : 0,
        max_cost_micros: maxCostMicros,
      }));
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
      <PageHeader
        title="Overview"
        actions={
          // Nothing below reads the range until the first-run state lifts.
          firstRunIncomplete ? undefined : (
            <SegmentedControl
              ariaLabel="Time range"
              options={RANGE_OPTIONS}
              value={range}
              onChange={selectRange}
            />
          )
        }
      />

      {/* The checklist is the whole first-run state: it says what fills in
      once traffic flows, so nothing else renders until it lifts. */}
      <FirstRunChecklist requestSeen={requestSeen} />

      {firstRunIncomplete ? null : (
        <>
          <ClosestToLimitCard
            aggregate={quotaAggregate.data}
            loading={quotaLoading}
          />

          {/* Quota comes first in the DOM (and on mobile); desktop shows the
          traffic strip above it. */}
          <div className="flex flex-col gap-4 min-w-0">
            <div className="grid grid-cols-1 items-start xl:grid-cols-[2fr_1fr] gap-4 min-w-0">
              <PoolQuotaCard
                aggregate={quotaAggregate.data}
                loading={quotaLoading}
                chart={poolQuotaChart}
              />

              <TopPrincipalsCard
                range={range}
                principals={topPrincipals}
                loading={
                  principalUsage.data === undefined && principalUsage.isPending
                }
              />
            </div>

            <section
              aria-label="Traffic"
              className="glass rounded-md min-w-0 md:order-first"
              data-testid="overview-kpi-strip"
            >
              {!summary.isPending && noTraffic ? (
                <p className="border-b border-row px-3 py-2 text-caption text-text-faint md:px-4">
                  {`No traffic in the last ${range}.`}
                </p>
              ) : null}
              <div className="grid grid-cols-2 md:grid-cols-3 xl:grid-cols-5">
                <ValueTile
                  className={KPI_CELL_CLASS[0]}
                  chartId="request-rate"
                  icon={<Activity />}
                  label="Requests/s"
                  loading={summary.isPending}
                  value={formatRate(reqPerSec)}
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
                  icon={<Database />}
                  label="Tokens"
                  loading={summary.isPending}
                  value={formatCount(totalTokens)}
                  sub={`Avg cache miss ${fmtRatioPercent(cacheMissAvg)}`}
                  spark={kpiPoints.tokens}
                  sparkColor={KPI_SERIES_COLOR}
                  formatChartValue={formatCount}
                  secondary={TOKENS_CACHE_MISS_SERIES}
                  activeIndex={activeKpiIndex}
                  onActiveIndexChange={setActiveKpiIndex}
                />
                <ValueTile
                  className={KPI_CELL_CLASS[2]}
                  chartId="cost"
                  icon={<TrendingUp />}
                  label="Cost at list price"
                  loading={summary.isPending}
                  value={formatUsdAmount(virtualUsd)}
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
                  icon={<Timer />}
                  label={latencyLabel}
                  loading={summary.isPending}
                  value={noTraffic ? '—' : fmtMs(latency)}
                  spark={kpiPoints.latency}
                  sparkColor={KPI_SERIES_COLOR}
                  formatChartValue={fmtMs}
                  activeIndex={activeKpiIndex}
                  onActiveIndexChange={setActiveKpiIndex}
                />
                <ValueTile
                  className={KPI_CELL_CLASS[4]}
                  chartId="error-rate"
                  icon={<ShieldCheck />}
                  label="Error rate"
                  loading={summary.isPending}
                  value={noTraffic ? '—' : fmtErrorPercent(errRate)}
                  spark={kpiPoints.error}
                  sparkColor={
                    errRate > 0 ? 'var(--color-danger)' : KPI_SERIES_COLOR
                  }
                  formatChartValue={fmtErrorPercent}
                  activeIndex={activeKpiIndex}
                  onActiveIndexChange={setActiveKpiIndex}
                />
              </div>
            </section>
          </div>

          {/* Latest requests: the feed is not range-scoped (newest events of any
          age), so its label must not suggest it follows the range picker. */}
          <Section
            title="Latest requests (any time)"
            subtitle={
              <span className="flex items-center gap-2">
                <span>
                  Newest first, not limited to the range above — full view on
                  Logs page
                  {streamStatus === 'live' ? ' · streaming' : ''}
                </span>
                {live.permanentFailure ? (
                  <AlertTriangle className="w-3 h-3 text-danger-text" />
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
              </span>
            }
            action={
              <Link
                to="/logs"
                className="text-label text-accent-text hover:underline inline-flex items-center gap-1 rounded-sm focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-2"
              >
                See all <ArrowUpRight className="w-3 h-3" />
              </Link>
            }
          >
            <Card className="min-w-0">
              <div
                className={cx(
                  'relative overflow-auto scroll-fade-right',
                  // A fixed box keeps loading → loaded from jumping; an empty
                  // feed collapses to its one-line empty row instead of
                  // leaving a tall blank frame.
                  !events.isLoading && recentRows.length === 0
                    ? 'max-h-[50vh]'
                    : 'h-[50vh]',
                )}
                ref={scrollContainerRef}
              >
                <RequestEventsTable
                  events={recentRows}
                  principalNameMap={principalNameMap}
                  upstreamNameMap={upstreamNameMap}
                  loading={events.isLoading}
                  liveFlashIds={recentLiveIds}
                  columns={OVERVIEW_TABLE_COLUMNS}
                  sentinelRef={sentinelRef}
                  loadingMore={events.isFetchingNextPage}
                  hasMore={events.hasNextPage}
                  minWidthClass="min-w-[1080px]"
                  emptyTitle="No recent requests"
                />
              </div>
            </Card>
          </Section>
        </>
      )}
    </PageContainer>
  );
}
