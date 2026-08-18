import { Meter as BaseMeter } from '@base-ui/react/meter';
import { Popover as BasePopover } from '@base-ui/react/popover';
import { Toggle as BaseToggle } from '@base-ui/react/toggle';
import { ToggleGroup as BaseToggleGroup } from '@base-ui/react/toggle-group';
import { createFileRoute } from '@tanstack/react-router';
import {
  Activity,
  AlertTriangle,
  ArrowUpRight,
  Database,
  Gauge,
  ShieldCheck,
  Timer,
  TrendingUp,
  Users,
} from 'lucide-react';
import { useEffect, useId, useMemo, useRef, useState } from 'react';
import {
  Area,
  AreaChart,
  CartesianGrid,
  ReferenceArea,
  ReferenceLine,
  Tooltip as RTooltip,
  XAxis,
  YAxis,
} from 'recharts';
import { LiveTailFailureBanner } from '../components/LiveTailFailureBanner';
import { BreakdownPopover } from '../components/ui/BreakdownPopover';
import {
  Card,
  CardHeader,
  cx,
  PageContainer,
  Section,
  Skeleton,
  Sparkline,
} from '../components/ui/primitives';
import { RequestEventsTable } from '../components/ui/RequestEventsTable';
import {
  addBucketCostMicros,
  type CostComponentMicros,
  costCategorySegments,
  emptyCostComponents,
  sumCostMicros,
} from '../components/ui/usage/costCategories';
import { type AggregateResponse, eventTime } from '../lib/api';
import { getWindowColor } from '../lib/colors';
import {
  cacheHitRatio,
  cacheMissRatio,
  formatCostMicros,
  formatCount,
  formatRate,
  formatUsdAmount,
  sumTokens,
} from '../lib/format';
import {
  usePrincipalNameMap,
  useRecentEventsInfinite,
  useSubscriptionQuotaAggregate,
  useSubscriptionQuotaPoolHistory,
  useSummary,
  useUpstreamNameMap,
  useUsage,
} from '../lib/queries';
import type { RequestEventWithPhase } from '../lib/RequestEventTypes';
import { useLiveEventStream } from '../lib/useLiveEventStream';
import {
  buildPoolQuotaChartData,
  POOL_QUOTA_WINDOWS,
  type PoolQuotaChartRow,
  type PoolQuotaLatest,
  type PoolQuotaWindow,
  poolQuotaChartLatest,
  poolQuotaChartMax,
} from './-overviewPoolQuota';
export const Route = createFileRoute('/')({
  component: OverviewPage,
});

const RANGES = ['1h', '6h', '24h', '7d'] as const;
type Range = (typeof RANGES)[number];

const stepFor = (r: Range): 'hour' | 'minute' =>
  r === '7d' || r === '24h' ? 'hour' : 'minute';

const POOL_QUOTA_QUERY_WINDOWS = POOL_QUOTA_WINDOWS.join(',');

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

const TOKENS_COLOR = '#06b6d4';

const TOKENS_CACHE_MISS_SERIES: KpiSecondarySeries = {
  testId: 'overview-kpi-secondary-tokens',
  label: 'Cache miss',
  color: '#8b5cf6',
  format: fmtPercent,
};

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
  tone = 'neutral',
  size = 'md',
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
  tone?: 'neutral' | 'accent' | 'warn' | 'ok';
  size?: 'sm' | 'md';
}) {
  const points = spark ?? NO_KPI_POINTS;
  const color = sparkColor ?? 'var(--color-accent)';
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
        'glass rounded-sm flex flex-col gap-2 relative overflow-hidden',
        size === 'sm' ? 'p-2.5 min-h-[88px]' : 'p-3 min-h-[110px]',
      )}
      data-testid={`overview-kpi-${chartId}`}
    >
      <div className="flex items-center gap-1.5 text-text-faint">
        <span className="w-3.5 h-3.5">{icon}</span>
        <span className="text-[11px] uppercase tracking-wider truncate">
          {label}
        </span>
      </div>
      <div
        className={cx(
          'flex items-center justify-between gap-2 min-w-0',
          size === 'sm' ? 'h-5' : 'h-6',
        )}
        data-slot="value"
      >
        {loading ? (
          <Skeleton className={size === 'sm' ? 'h-5 w-20' : 'h-6 w-24'} />
        ) : (
          <span
            className={cx(
              'tabular-nums leading-none truncate',
              size === 'sm' ? 'text-xl font-medium' : 'text-2xl font-medium',
              tone === 'accent' && 'text-accent',
              tone === 'warn' && 'text-[color:var(--color-warn)]',
              tone === 'ok' && 'text-[color:var(--color-ok)]',
            )}
          >
            {value}
          </span>
        )}
      </div>
      {sub !== undefined ? (
        <div
          className="flex h-4 items-center text-[11px] text-text-faint truncate"
          data-slot="sub"
        >
          {loading ? <Skeleton className="h-3 w-20" /> : sub}
        </div>
      ) : null}
      {activePoint ? (
        <div
          className="pointer-events-none absolute inset-x-1.5 bottom-8 z-10 flex flex-col gap-0.5 rounded-sm border border-subtle-strong bg-bg-sub px-2 py-1 shadow-lg"
          data-testid={`overview-kpi-tooltip-${chartId}`}
        >
          <span className="text-[10px] leading-none text-text-faint tabular-nums truncate">
            {fmtChartTooltip(activePoint.timestamp)}
          </span>
          <span className="text-[11px] leading-none text-text tabular-nums truncate">
            {`${chartLabel ?? label} ${formatChartValue(activePoint.value)}`}
          </span>
          {secondary ? (
            // Muted body text rather than the purple series stroke: #8b5cf6 on
            // bg-bg-sub measures 4.06:1 in light theme — under the 4.5:1 floor
            // for this 11px row — and only 4.81:1 in dark, while the muted
            // token holds 7.24:1 / 8.03:1. Purple stays on the chart itself.
            <span className="text-[11px] leading-none text-text-muted tabular-nums truncate">
              {`${secondary.label} ${secondary.format(activePoint.secondaryValue)}`}
            </span>
          ) : null}
        </div>
      ) : null}
      <div
        className={cx(
          'h-8 shrink-0 pt-1 mt-auto',
          size === 'sm' ? '-mx-2.5 -mb-2.5' : '-mx-3 -mb-3',
        )}
        data-slot="sparkline"
      >
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
                className="pointer-events-none absolute inset-y-0 w-px -translate-x-1/2 bg-[color:var(--color-border-strong)]"
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
  'flex min-h-[66px] items-center gap-3 border-b border-subtle px-3 py-2 last:border-b-0';

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
          <BaseMeter.Track className="relative w-full bg-overlay-3 rounded-full overflow-hidden h-1.5">
            <BaseMeter.Indicator
              className={cx(
                'flex h-full rounded-full overflow-hidden transition-all',
                components ? '' : 'bg-[color:var(--color-accent)]',
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
            className="z-50 rounded-sm border border-subtle-strong bg-bg-sub px-2 py-1 text-[11px] text-text shadow-lg"
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
    <Card className="min-w-0 flex flex-col h-full">
      <CardHeader
        title={
          <span className="inline-flex items-center gap-2">
            <Users className="w-3.5 h-3.5 text-text-faint" />
            Top principals
          </span>
        }
        subtitle={`by virtual cost · ${range}`}
      />
      <div className="flex-1 overflow-auto min-h-0 max-h-96 xl:max-h-none">
        <div className="flex min-h-80 flex-col" data-slot="principal-list">
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
                  <Skeleton className="mt-1.5 h-1.5 rounded-full" />
                </div>
                <div className="flex shrink-0 flex-col items-end gap-1 text-right">
                  <Skeleton className="h-5 w-16" />
                  <Skeleton className="h-3 w-10" />
                </div>
              </div>
            ))
          ) : principals.length === 0 ? (
            <div className="flex flex-1 items-center justify-center p-4 text-center text-xs text-text-faint">
              No usage data
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
                  <div className="text-sm truncate">{principal.name}</div>
                  <div
                    className="text-[11px] text-text-faint truncate"
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
                  <div className="text-sm font-mono tabular-nums">
                    {formatUsdAmount(principal.cost_micros / 1_000_000)}
                  </div>
                  <div className="text-[11px] text-text-faint tabular-nums">
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

const POOL_PALETTES: Record<PoolQuotaWindow, readonly string[]> = {
  '5h': [
    'rgb(37, 99, 235)',
    'rgb(59, 130, 246)',
    'rgb(96, 165, 250)',
    'rgb(147, 197, 253)',
    'rgb(191, 219, 254)',
  ],
  '7d': [
    'rgb(124, 58, 237)',
    'rgb(139, 92, 246)',
    'rgb(167, 139, 250)',
    'rgb(196, 181, 253)',
    'rgb(221, 214, 254)',
  ],
  '7d_fable': [
    'rgb(77, 124, 15)',
    'rgb(101, 163, 13)',
    'rgb(132, 204, 22)',
    'rgb(163, 230, 53)',
    'rgb(217, 249, 157)',
  ],
};

const POOL_QUOTA_SNAPSHOT_SLOT_CLASS =
  'relative flex min-h-[56px] w-full flex-col justify-center';
const POOL_QUOTA_CHART_SLOT_CLASS = 'relative flex-1 min-h-64 min-w-0 w-full';
const POOL_QUOTA_LEGEND_ITEM_CLASS = 'inline-flex min-h-4 items-center gap-1.5';

function poolSegmentColor(window: PoolQuotaWindow, index: number): string {
  const palette = POOL_PALETTES[window];
  return palette[index % palette.length];
}

function PoolQuotaPopoverContent({
  window,
  w,
  activeIdx,
}: {
  window: PoolQuotaWindow;
  w: AggregateWindow;
  activeIdx: number | null;
}) {
  const totalRatio = w.provider_lots.reduce(
    (sum, lot) => sum + lot.capacity_ratio,
    0,
  );
  return (
    <div className="flex flex-col gap-2 text-sm text-text">
      <div className="flex items-center gap-3 px-3 pb-2 border-b border-subtle text-xs font-medium text-text-muted uppercase tracking-wider">
        <span className="flex-1">Upstream</span>
        <span className="w-16 text-right">Util</span>
        <span className="w-16 text-right">Weight</span>
        <span className="w-16 text-right">Impact</span>
      </div>
      {w.provider_lots.map((lot, i) => {
        const util = lot.utilization ?? 0;
        const weightedContribution =
          totalRatio > 0 ? ((util * lot.capacity_ratio) / totalRatio) * 100 : 0;
        const idColor = poolSegmentColor(window, i);
        const utilTextColor =
          lot.utilization != null
            ? 'var(--color-text)'
            : 'var(--color-text-muted)';
        const isHovered = activeIdx === i;
        return (
          <div
            key={i}
            className={cx(
              'flex items-center gap-3 px-3 py-2 rounded-md transition-colors',
              isHovered ? 'bg-surface-raised shadow-sm' : 'hover:bg-overlay-2',
            )}
          >
            <div className="flex items-center gap-2.5 flex-1 min-w-0">
              <span
                className="inline-block w-2.5 h-2.5 rounded-sm shrink-0"
                style={{ backgroundColor: idColor }}
              />
              <span
                className={cx(
                  'truncate',
                  isHovered ? 'font-medium text-text' : 'text-text-muted',
                )}
              >
                {lot.upstream_name}
              </span>
            </div>
            <span
              className="tabular-nums w-16 text-right font-medium"
              style={{ color: utilTextColor }}
            >
              {lot.utilization != null ? `${(util * 100).toFixed(1)}%` : '—'}
            </span>
            <span className="tabular-nums w-16 text-right text-text-muted">
              {lot.capacity_ratio.toFixed(1)}x
            </span>
            <span
              className={cx(
                'tabular-nums w-16 text-right',
                isHovered ? 'font-medium text-text' : 'text-text-muted',
              )}
            >
              {weightedContribution > 0
                ? `${weightedContribution.toFixed(1)}%`
                : '0.0%'}
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
  const [openPopover, setOpenPopover] = useState(false);
  const label = window === '7d_fable' ? '7d (Fable)' : window;

  if (loading || !w) {
    return (
      <div className={cx(POOL_QUOTA_SNAPSHOT_SLOT_CLASS, 'gap-2')}>
        <div className="flex items-center justify-between mb-1.5">
          <span className="text-xs font-medium text-text-muted">
            {label} pool
          </span>
          {loading ? (
            <Skeleton as="span" className="inline-block h-4 w-12" />
          ) : (
            <span className="text-xs text-text-faint">no data</span>
          )}
        </div>
        {loading ? (
          <Skeleton className="h-5 rounded-full" />
        ) : (
          <div className="h-5 w-full rounded-full border border-subtle bg-surface-raised" />
        )}
      </div>
    );
  }

  const totalRatio = w.provider_lots.reduce(
    (sum, lot) => sum + lot.capacity_ratio,
    0,
  );
  const usedPct = w.utilization_percent ?? 0;
  const weightedSegments = w.provider_lots
    .map((lot, index) => {
      const util = lot.utilization ?? 0;
      const weightedContribution =
        totalRatio > 0 ? ((util * lot.capacity_ratio) / totalRatio) * 100 : 0;

      return {
        idColor: poolSegmentColor(window, index),
        index,
        lot,
        weightedContribution,
      };
    })
    .filter((segment) => segment.weightedContribution > 0);
  const totalWeightedContribution = weightedSegments.reduce(
    (sum, segment) => sum + segment.weightedContribution,
    0,
  );

  const handleInteraction = (i: number | null, isClick = false) => {
    const isMobile = 'ontouchstart' in globalThis.window;
    if (isMobile && !isClick) return;
    if (isClick && isMobile) {
      if (openPopover && activeIdx === i) {
        setOpenPopover(false);
        setActiveIdx(null);
      } else {
        setOpenPopover(true);
        setActiveIdx(i);
      }
    } else if (!isMobile) {
      setActiveIdx(i);
      setOpenPopover(i !== null);
    }
  };

  const renderSegments = () =>
    weightedSegments.map(({ idColor, index, lot, weightedContribution }) => {
      const util = lot.utilization ?? 0;
      const isHovered = activeIdx === index;
      const isOtherHovered = activeIdx !== null && activeIdx !== index;
      return (
        <div
          key={index}
          className={cx(
            'h-full basis-0 border-r border-bg last:border-r-0 transition-all cursor-pointer',
            isHovered &&
              'outline outline-1 outline-white/60 outline-offset-[-1px] z-10',
            isOtherHovered && 'opacity-60',
          )}
          style={{
            backgroundColor: idColor,
            flexGrow: weightedContribution,
          }}
          title={`${lot.upstream_name} · ${lot.capacity_ratio.toFixed(1)}x · ${lot.utilization != null ? (util * 100).toFixed(1) : '—'}%`}
          onMouseEnter={() => handleInteraction(index)}
          onClick={() => handleInteraction(index, true)}
          onTouchStart={() => handleInteraction(index, true)}
        />
      );
    });

  const pctText =
    w.utilization_percent != null ? `${usedPct.toFixed(1)}%` : '—';
  return (
    <BasePopover.Root
      open={openPopover}
      onOpenChange={(nextOpen) => {
        setOpenPopover(nextOpen);
        if (!nextOpen) setActiveIdx(null);
      }}
    >
      <BasePopover.Trigger
        className={cx(
          POOL_QUOTA_SNAPSHOT_SLOT_CLASS,
          'text-left',
          openPopover && 'z-50',
        )}
        closeDelay={0}
        delay={0}
        nativeButton={false}
        openOnHover
        render={<div />}
      >
        <div className="2xl:hidden flex flex-col gap-1.5">
          <div className="flex items-center justify-between">
            <span className="text-xs font-medium text-text-muted">
              {label} pool
            </span>
            <span className="tabular-nums font-medium text-sm leading-none text-text">
              {pctText}
            </span>
          </div>
          <BaseMeter.Root
            className="h-5 w-full"
            max={100}
            value={totalWeightedContribution}
          >
            <BaseMeter.Track className="h-5 w-full flex rounded-full overflow-hidden border border-subtle bg-surface-raised">
              <BaseMeter.Indicator className="h-full flex transition-all">
                {renderSegments()}
              </BaseMeter.Indicator>
            </BaseMeter.Track>
          </BaseMeter.Root>
        </div>

        <div className="hidden 2xl:flex items-center gap-2">
          <span className="text-xs font-medium text-text-muted shrink-0">
            {label}
          </span>
          <BaseMeter.Root
            className="flex-1 h-5 min-w-0"
            max={100}
            value={totalWeightedContribution}
          >
            <BaseMeter.Track className="h-5 w-full flex rounded-full overflow-hidden border border-subtle bg-surface-raised">
              <BaseMeter.Indicator className="h-full flex transition-all">
                {renderSegments()}
              </BaseMeter.Indicator>
            </BaseMeter.Track>
          </BaseMeter.Root>
          <span className="tabular-nums font-medium text-sm leading-none text-text shrink-0">
            {pctText}
          </span>
        </div>
      </BasePopover.Trigger>

      <BasePopover.Portal>
        <BasePopover.Positioner align="start" side="bottom" sideOffset={8}>
          <BasePopover.Popup
            className="z-50 w-[var(--anchor-width)] max-w-[calc(100vw-1rem)] bg-bg-sub border border-subtle-strong rounded-md shadow-xl p-2"
            initialFocus={false}
          >
            <PoolQuotaPopoverContent
              window={window}
              w={w}
              activeIdx={activeIdx}
            />
          </BasePopover.Popup>
        </BasePopover.Positioner>
      </BasePopover.Portal>
    </BasePopover.Root>
  );
}

function PoolQuotaCard({
  aggregate,
  chart,
  loading,
}: {
  aggregate: { data: AggregateResponse | undefined };
  chart: {
    data: PoolQuotaChartRow[];
    maxValue: number;
    rangeStartUnix: number;
    rangeEndUnix: number;
    range: Range;
    latest: PoolQuotaLatest;
    showFable: boolean;
  };
  loading: boolean;
}) {
  const w5h = aggregate.data?.windows.find((x) => x.window === '5h');
  const w7d = aggregate.data?.windows.find((x) => x.window === '7d');
  const wFable = aggregate.data?.windows.find((x) => x.window === '7d_fable');
  const upstreamCount = aggregate.data?.upstream_count ?? 0;
  const contributingCount = Math.max(
    w5h?.contributing_upstreams ?? 0,
    w7d?.contributing_upstreams ?? 0,
    wFable?.contributing_upstreams ?? 0,
  );
  return (
    <Card
      aria-busy={loading}
      className="min-w-0 min-h-[41rem] flex flex-col h-full sm:min-h-0"
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
            ? `plan-weighted · ${formatCount(contributingCount)} of ${formatCount(upstreamCount)} upstreams`
            : 'plan-weighted'
        }
      />
      <div className="flex-1 flex flex-col gap-4 p-4 pt-2 min-h-0">
        <div className="flex flex-col gap-2">
          <div className="text-xs uppercase tracking-wider font-medium text-text-faint">
            Snapshot
          </div>
          <div className="grid grid-cols-1 gap-x-10 gap-y-4 md:grid-cols-2 xl:grid-cols-3">
            {POOL_QUOTA_WINDOWS.map((window) => (
              <div
                key={window}
                data-testid="pool-quota-snapshot-slot"
                className={POOL_QUOTA_SNAPSHOT_SLOT_CLASS}
              >
                <PoolQuotaStackedBar
                  window={window}
                  w={aggregate.data?.windows.find(
                    (entry) => entry.window === window,
                  )}
                  loading={loading}
                />
              </div>
            ))}
          </div>
        </div>
        <div className="h-px bg-border" />
        <div className="flex-1 flex flex-col gap-2 min-h-0">
          <div className="flex flex-col items-start gap-2 sm:flex-row sm:items-center sm:justify-between">
            <div className="text-xs uppercase tracking-wider font-medium text-text-faint">
              Trend · {chart.range}
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
}

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
  const chartId = useId();
  const c5h = getWindowColor('5h');
  const c7d = getWindowColor('7d');
  const cFable = getWindowColor('7d_fable');

  return (
    <div className="relative size-full min-h-0 min-w-0">
      {!seriesData.length ? (
        <div className="absolute inset-0 flex items-center justify-center text-xs text-text-faint pointer-events-none z-10">
          No timeline data yet for this range
        </div>
      ) : null}
      <AreaChart
        responsive
        className="size-full"
        data={seriesData}
        margin={{ top: 0, right: 0, bottom: 0, left: 0 }}
      >
        <defs>
          <linearGradient id={`${chartId}-grad-5h`} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor={c5h.stroke} stopOpacity={0.55} />
            <stop offset="100%" stopColor={c5h.stroke} stopOpacity={0} />
          </linearGradient>
          <linearGradient id={`${chartId}-grad-7d`} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor={c7d.stroke} stopOpacity={0.55} />
            <stop offset="100%" stopColor={c7d.stroke} stopOpacity={0} />
          </linearGradient>
          {showFable ? (
            <linearGradient
              id={`${chartId}-grad-fable`}
              x1="0"
              y1="0"
              x2="0"
              y2="1"
            >
              <stop offset="0%" stopColor={cFable.fill} stopOpacity={0.55} />
              <stop offset="100%" stopColor={cFable.stroke} stopOpacity={0} />
            </linearGradient>
          ) : null}
        </defs>
        <CartesianGrid stroke="var(--color-border)" />
        <XAxis
          dataKey="unix"
          type="number"
          domain={[rangeStartUnix, rangeEndUnix]}
          allowDataOverflow
          tick={{
            fill: 'var(--color-text-muted)',
            fontSize: 10,
          }}
          tickFormatter={fmtChartTick}
          axisLine={false}
          tickLine={false}
          minTickGap={40}
          tickMargin={8}
        />
        <YAxis
          tick={{
            fill: 'var(--color-text-muted)',
            fontSize: 10,
          }}
          tickFormatter={(v) => `${v}%`}
          axisLine={false}
          tickLine={false}
          width={48}
          tickMargin={8}
          domain={[0, maxValue]}
          ticks={
            maxValue <= 100
              ? [0, 25, 50, 75, 100]
              : [0, 25, 50, 75, 100, maxValue]
          }
          allowDataOverflow={false}
        />
        {maxValue > 95 ? (
          <ReferenceArea
            y1={95}
            y2={maxValue}
            fill="var(--color-danger)"
            fillOpacity={0.06}
            ifOverflow="hidden"
          />
        ) : null}
        <ReferenceLine
          y={80}
          stroke="var(--color-warn)"
          strokeOpacity={0.5}
          strokeDasharray="4 4"
          label={{
            position: 'insideBottomLeft',
            value: '80% Warn',
            fill: 'var(--color-text-muted)',
            fontSize: 11,
            opacity: 0.9,
          }}
        />
        <ReferenceLine
          y={95}
          stroke="var(--color-danger)"
          strokeOpacity={0.6}
          strokeDasharray="4 4"
          label={{
            position: 'insideBottomLeft',
            value: '95% Critical',
            fill: 'var(--color-text-muted)',
            fontSize: 11,
            opacity: 0.9,
          }}
        />
        <RTooltip
          cursor={{
            stroke: 'var(--color-accent)',
            strokeWidth: 1,
            strokeOpacity: 0.3,
          }}
          content={({ active, payload, label }) => {
            if (!active || !payload?.length) return null;
            return (
              <div
                style={{
                  background: 'var(--color-bg-sub)',
                  border: '1px solid var(--color-subtle-strong)',
                  borderRadius: 6,
                  color: 'var(--color-text)',
                  fontSize: 11,
                  padding: '8px 12px',
                  boxShadow: '0 8px 24px rgba(0,0,0,0.32)',
                  minWidth: 140,
                }}
              >
                <div
                  style={{
                    color: 'var(--color-text-muted)',
                    marginBottom: 6,
                    fontWeight: 500,
                  }}
                >
                  {fmtChartTooltip(Number(label))}
                </div>
                {payload.map((p, i) => {
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
                      key={i}
                      style={{
                        padding: '2px 0',
                        display: 'flex',
                        justifyContent: 'space-between',
                        gap: 12,
                      }}
                    >
                      <span
                        style={{
                          color:
                            typeof p.color === 'string'
                              ? p.color
                              : 'var(--color-text)',
                          fontWeight: 500,
                        }}
                      >
                        {wLabel}
                      </span>
                      <span
                        style={{
                          fontVariantNumeric: 'tabular-nums',
                          fontWeight: 500,
                        }}
                      >
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
        {showFable ? (
          <Area
            type="monotone"
            dataKey="7d_fable"
            stroke={cFable.stroke}
            strokeWidth={1.4}
            fill={`url(#${chartId}-grad-fable)`}
            fillOpacity={1}
            isAnimationActive={false}
            connectNulls={false}
          />
        ) : null}
        <Area
          type="monotone"
          dataKey="7d"
          stroke={c7d.stroke}
          strokeWidth={1.4}
          fill={`url(#${chartId}-grad-7d)`}
          fillOpacity={1}
          isAnimationActive={false}
          connectNulls={false}
        />
        <Area
          type="monotone"
          dataKey="5h"
          stroke={c5h.stroke}
          strokeWidth={1.4}
          fill={`url(#${chartId}-grad-5h)`}
          fillOpacity={1}
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
    <div className="flex flex-wrap items-center gap-3 text-[11px] text-text-faint">
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
              className="w-2 h-2 rounded-sm"
              style={{ background: color.stroke }}
            />
            {label}
            {loading ? (
              <Skeleton as="span" className="inline-block h-3 w-8" />
            ) : value != null ? (
              ` · ${value.toFixed(0)}%`
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
  const principalUsage = useUsage(range, stepFor(range), 'principal');
  const events = useRecentEventsInfinite({});
  const principalNameMap = usePrincipalNameMap();
  const upstreamNameMap = useUpstreamNameMap();

  const [nowUnixSecs, setNowUnixSecs] = useState(() =>
    Math.floor(Date.now() / 1000),
  );
  useEffect(() => {
    const interval = setInterval(() => {
      setNowUnixSecs(Math.floor(Date.now() / 1000));
    }, 60_000);
    return () => clearInterval(interval);
  }, []);

  const quotaAggregate = useSubscriptionQuotaAggregate({
    windows: POOL_QUOTA_QUERY_WINDOWS,
    source: 'merged',
  });

  const seriesRangeSecs =
    range === '1h'
      ? 3600
      : range === '6h'
        ? 21600
        : range === '24h'
          ? 86400
          : 604800;
  const quotaPoolHistory = useSubscriptionQuotaPoolHistory({
    windows: POOL_QUOTA_QUERY_WINDOWS,
    sinceUnixSecs: nowUnixSecs - seriesRangeSecs,
    untilUnixSecs: nowUnixSecs,
  });
  const showFable = POOL_QUOTA_WINDOWS.includes('7d_fable');
  const quotaLoading =
    quotaAggregate.isPending ||
    quotaAggregate.isPlaceholderData ||
    quotaPoolHistory.isPending ||
    quotaPoolHistory.isPlaceholderData;

  const live = useLiveEventStream({});
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
  const recentRows = useMemo(() => {
    const historical = events.data?.pages.flatMap((p) => p.events) ?? [];
    const seen = new Set<string>();
    const out: RequestEventWithPhase[] = [];
    for (const entry of live.eventsMap.values()) {
      const ev = entry.event;
      const key = ev.event_id ?? ev.request_id;
      if (!seen.has(key)) {
        seen.add(key);
        if (entry.phase === 'final') {
          out.push({ ...entry.event, _phase: 'final' });
        } else {
          out.push({ ...entry.event, _phase: 'partial' });
        }
      }
    }
    for (const ev of historical) {
      const key = ev.event_id ?? ev.request_id;
      if (!seen.has(key)) {
        seen.add(key);
        out.push({ ...ev, _phase: 'final' });
      }
    }
    return out.sort(
      (a, b) => (eventTime(b)?.getTime() ?? 0) - (eventTime(a)?.getTime() ?? 0),
    );
  }, [live.eventsMap, live.version, events.data]);

  // biome-ignore lint/correctness/useExhaustiveDependencies: same rationale — live.version is the mutation counter for the stable eventsMap ref.
  const recentLiveIds = useMemo(
    () =>
      new Set(
        Array.from(live.eventsMap.values())
          .slice(0, 20)
          .map((e) => e.event.event_id ?? e.event.request_id),
      ),
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

  // Chart Data
  const chartData = useMemo(
    () => buildPoolQuotaChartData(quotaPoolHistory.data?.windows, showFable),
    [quotaPoolHistory.data, showFable],
  );

  const chartMaxValue = useMemo(
    () => poolQuotaChartMax(chartData, showFable),
    [chartData, showFable],
  );

  const chartLatest = useMemo(
    () => poolQuotaChartLatest(chartData, showFable),
    [chartData, showFable],
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
      <div className="flex items-center justify-between mb-2">
        <h1 className="text-lg font-medium">Overview</h1>
        <BaseToggleGroup
          aria-label="Time range"
          className="flex flex-wrap bg-overlay-2 border border-subtle rounded-sm p-0.5"
          onValueChange={(values) => {
            const first = values[0];
            if (first) selectRange(first);
          }}
          value={[range]}
        >
          {RANGES.map((r) => (
            <BaseToggle
              key={r}
              className="px-2.5 h-7 text-xs rounded-sm transition-colors text-text-faint hover:text-text data-[pressed]:bg-[color:var(--color-overlay-6)] data-[pressed]:text-[color:var(--color-text)]"
              value={r}
            >
              {r}
            </BaseToggle>
          ))}
        </BaseToggleGroup>
      </div>

      {/* KPI Strip */}
      <div className="grid grid-cols-2 md:grid-cols-3 xl:grid-cols-5 gap-2">
        <ValueTile
          size="sm"
          chartId="request-rate"
          icon={<Activity className="w-3.5 h-3.5" />}
          label="avg req/s"
          loading={summary.isPending}
          value={formatRate(reqPerSec)}
          sub={`${formatCount(totals?.request_count)} / ${range}`}
          spark={kpiPoints.rate}
          sparkColor="var(--color-accent)"
          chartLabel="Req/s"
          formatChartValue={formatRate}
          activeIndex={activeKpiIndex}
          onActiveIndexChange={setActiveKpiIndex}
        />
        <ValueTile
          size="sm"
          chartId="tokens"
          icon={<Database className="w-3.5 h-3.5" />}
          label="tokens"
          loading={summary.isPending}
          value={formatCount(totalTokens)}
          sub={`Avg cache miss ${fmtRatioPercent(cacheMissAvg)}`}
          spark={kpiPoints.tokens}
          sparkColor={TOKENS_COLOR}
          chartLabel="Tokens"
          formatChartValue={formatCount}
          secondary={TOKENS_CACHE_MISS_SERIES}
          activeIndex={activeKpiIndex}
          onActiveIndexChange={setActiveKpiIndex}
        />
        <ValueTile
          size="sm"
          chartId="cost"
          icon={<TrendingUp className="w-3.5 h-3.5" />}
          label="equiv $"
          loading={summary.isPending}
          value={formatUsdAmount(virtualUsd)}
          spark={kpiPoints.cost}
          sparkColor="#10b981"
          tone="accent"
          chartLabel="Equiv $"
          formatChartValue={formatUsdAmount}
          activeIndex={activeKpiIndex}
          onActiveIndexChange={setActiveKpiIndex}
        />
        <ValueTile
          size="sm"
          chartId="latency"
          icon={<Timer className="w-3.5 h-3.5" />}
          label={latencyLabel}
          loading={summary.isPending}
          value={fmtMs(latency)}
          spark={kpiPoints.latency}
          sparkColor="#f59e0b"
          chartLabel={latencyLabel}
          formatChartValue={fmtMs}
          activeIndex={activeKpiIndex}
          onActiveIndexChange={setActiveKpiIndex}
        />
        <ValueTile
          size="sm"
          chartId="error-rate"
          icon={<ShieldCheck className="w-3.5 h-3.5" />}
          label="err rate"
          loading={summary.isPending}
          value={fmtErrorPercent(errRate)}
          spark={kpiPoints.error}
          sparkColor="var(--color-danger)"
          chartLabel="Err rate"
          formatChartValue={fmtErrorPercent}
          activeIndex={activeKpiIndex}
          onActiveIndexChange={setActiveKpiIndex}
        />
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-[2fr_1fr] gap-4 min-w-0">
        <PoolQuotaCard
          aggregate={quotaAggregate}
          loading={quotaLoading}
          chart={{
            data: chartData,
            maxValue: chartMaxValue,
            rangeStartUnix: nowUnixSecs - seriesRangeSecs,
            rangeEndUnix: nowUnixSecs,
            range,
            latest: chartLatest,
            showFable,
          }}
        />

        <TopPrincipalsCard
          range={range}
          principals={topPrincipals}
          loading={principalUsage.isPending || principalUsage.isPlaceholderData}
        />
      </div>

      {/* Recent Requests */}
      <Section
        title="Recent Requests"
        subtitle={
          <span className="flex items-center gap-2">
            <span>
              Live preview — full view on Logs page
              {streamStatus === 'live' ? ' · streaming' : ''}
            </span>
            {live.permanentFailure ? (
              <AlertTriangle className="w-3 h-3 text-[color:var(--color-danger)]" />
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
          <a
            href="/logs"
            className="text-xs text-accent hover:underline inline-flex items-center gap-1"
          >
            See all <ArrowUpRight className="w-3 h-3" />
          </a>
        }
      >
        <Card className="min-w-0">
          <div
            className="overflow-auto h-[50vh] scroll-fade-right"
            ref={scrollContainerRef}
          >
            <RequestEventsTable
              events={recentRows}
              principalNameMap={principalNameMap}
              upstreamNameMap={upstreamNameMap}
              loading={events.isLoading}
              liveFlashIds={recentLiveIds}
              columns={{ cost: true, tokens: true }}
              sentinelRef={sentinelRef}
              loadingMore={events.isFetchingNextPage}
              hasMore={events.hasNextPage}
              minWidthClass="min-w-[1080px]"
              emptyTitle="No recent requests"
            />
          </div>
        </Card>
      </Section>
    </PageContainer>
  );
}
