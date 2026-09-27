import { Meter as BaseMeter } from '@base-ui/react/meter';
import { Popover as BasePopover } from '@base-ui/react/popover';
import { createFileRoute, Link } from '@tanstack/react-router';
import { AlertTriangle } from 'lucide-react';
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
import {
  CHART_AXIS,
  CHART_CURSOR,
  CHART_GRID,
  CHART_THRESHOLD,
  Sparkline,
} from '../components/ui/charts';
import { ArcGauge, HeadroomMeter } from '../components/ui/Gauge';
import {
  Badge,
  Card,
  cx,
  Hint,
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
import {
  UpstreamHeadroomGrid,
  useUpstreamHeadroomData,
} from '../components/upstreams/UpstreamHeadroomGrid';
import { type AggregateResponse, WINDOW_LABELS } from '../lib/api';
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
  formatHeadroom,
  formatHeadroomValue,
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
  bindingPoolWindow,
  buildPoolQuotaChartData,
  type ClosestToLimitEntry,
  closestToLimit,
  formatAgo,
  formatDuration,
  formatResetIn,
  oldestProviderObservation,
  POOL_QUOTA_WINDOWS,
  type PoolQuotaChartRow,
  type PoolQuotaLatest,
  type PoolQuotaWindow,
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
/** The Overview previews the newest requests; Logs holds the full history. */
const OVERVIEW_LATEST_ROWS = 10;
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
// rate turns danger, and only when there are errors to report. The brand hue
// is reserved for headroom, so the cache-miss overlay is full-strength ink.
const KPI_SERIES_COLOR = 'var(--color-text-muted)';

const TOKENS_CACHE_MISS_SERIES: KpiSecondarySeries = {
  testId: 'overview-kpi-secondary-tokens',
  label: 'Cache miss',
  color: 'var(--color-text)',
  format: fmtPercent,
};

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
          <span className="truncate text-display tabular-nums text-text">
            {value}
          </span>
        )}
      </div>
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
  'flex min-h-[66px] items-center gap-3 border-t border-row py-2 first:border-t-0';

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

export function TopPrincipalsSection({
  range,
  principals,
  loading,
  className,
}: {
  range: Range;
  principals: readonly TopPrincipal[];
  loading: boolean;
  className?: string;
}) {
  return (
    <Section
      title="Top principals"
      subtitle={`By virtual cost · ${range}`}
      className={cx('min-w-0', className)}
    >
      <div className="flex flex-col" data-slot="principal-list">
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
          <p className="text-body text-text-muted">
            {`No usage in the last ${range}`}
          </p>
        ) : (
          principals.map((principal) => (
            <div
              key={principal.id}
              className={TOP_PRINCIPAL_ROW_CLASS}
              data-testid="top-principal-row"
            >
              <div className="min-w-0 flex-1">
                <div className="truncate text-body text-text">
                  {principal.name}
                </div>
                <div
                  className="truncate text-caption text-text-muted"
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
                <div className="text-body font-medium tabular-nums text-text">
                  {formatUsdAmount(principal.cost_micros / 1_000_000)}
                </div>
                <div className="text-caption tabular-nums text-text-muted">
                  {principal.share_pct.toFixed(1)}%
                </div>
              </div>
            </div>
          ))
        )}
      </div>
    </Section>
  );
}

type AggregateWindow = AggregateResponse['windows'][number];

/** Text link on the ground: ink with an underline rule that turns brand on hover. */
const OVERVIEW_LINK_CLASS =
  'rounded-sm text-body text-text underline decoration-border-strong underline-offset-4 transition-colors hover:decoration-accent focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-2';

/** "10-01 09:00" in the viewer's timezone. */
function formatMonthDayTime(unixSecs: number, timeZone: string): string {
  return formatInTimezone(unixSecs * 1000, timeZone)
    .slice(5)
    .replace('T', ' ');
}

/** Numeral ink for a quota figure: default ink until warn / danger by used. */
const HEADROOM_TONE_CLASS: Record<QuotaSeverity, string> = {
  ...QUOTA_SEVERITY_TEXT_CLASS,
  none: 'text-text',
  ok: 'text-text',
};

/** Each upstream's share of one pool window: headroom, plan weight, impact. */
function PoolBreakdownTable({ w }: { w: AggregateWindow }) {
  const totalRatio = w.provider_lots.reduce(
    (sum, lot) => sum + lot.capacity_ratio,
    0,
  );
  return (
    <div className="flex flex-col gap-0.5 text-body text-text">
      <div className="mb-0.5 flex items-center gap-3 border-b border-row px-2 pb-1.5 text-label text-text-muted">
        <span className="min-w-32 flex-1">Upstream</span>
        <span className="w-20 text-right">Left</span>
        <span className="w-14 text-right">Weight</span>
        <span className="w-14 text-right">Impact</span>
      </div>
      {w.provider_lots.map((lot) => {
        const utilPct = lot.utilization != null ? lot.utilization * 100 : null;
        const weightedContribution =
          totalRatio > 0
            ? (((lot.utilization ?? 0) * lot.capacity_ratio) / totalRatio) * 100
            : 0;
        return (
          <div
            key={lot.upstream_id}
            className="flex items-center gap-3 px-2 py-1.5"
            data-testid="pool-quota-breakdown-row"
          >
            <span
              className="min-w-32 flex-1 truncate text-text"
              title={lot.upstream_name}
            >
              {lot.upstream_name}
            </span>
            <span
              className={cx(
                'w-20 text-right font-medium tabular-nums',
                QUOTA_SEVERITY_TEXT_CLASS[quotaSeverity(utilPct)],
              )}
            >
              {formatHeadroom(utilPct)}
            </span>
            <span className="w-14 text-right tabular-nums text-text-muted">
              {lot.capacity_ratio.toFixed(1)}x
            </span>
            <span className="w-14 text-right tabular-nums text-text-muted">
              {`${weightedContribution.toFixed(1)}%`}
            </span>
          </div>
        );
      })}
    </div>
  );
}

/** Disclosure under a window gauge: the per-upstream breakdown of that window. */
function PoolBreakdownPopover({
  window,
  w,
}: {
  window: PoolQuotaWindow;
  w: AggregateWindow;
}) {
  const label = WINDOW_LABELS[window];
  return (
    <BasePopover.Root>
      <BasePopover.Trigger
        aria-label={`By upstream: ${label} headroom`}
        className="self-start rounded-sm text-body text-text-muted underline decoration-border-strong underline-offset-4 transition-colors hover:text-text hover:decoration-accent focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-2"
        closeDelay={100}
        delay={100}
        openOnHover
      >
        By upstream
      </BasePopover.Trigger>
      <BasePopover.Portal>
        <BasePopover.Positioner align="start" side="bottom" sideOffset={8}>
          <BasePopover.Popup
            aria-label={`${label} pool by upstream`}
            className="glass-strong z-50 w-max min-w-[22rem] max-w-[calc(100vw-1rem)] rounded-md p-2"
            initialFocus={false}
          >
            <PoolBreakdownTable w={w} />
          </BasePopover.Popup>
        </BasePopover.Positioner>
      </BasePopover.Portal>
    </BasePopover.Root>
  );
}

/**
 * One pool window: a sub arc gauge (a headroom meter on phones), the used
 * share and contributing upstreams, when it refills, and the per-upstream
 * breakdown. The window that runs out first carries the "binds pool" tag.
 */
function PoolWindowGauge({
  window,
  w,
  binds,
  loading,
  upstreamCount,
  nowUnixSecs,
  timeZone,
  className,
}: {
  window: PoolQuotaWindow;
  w: AggregateWindow | undefined;
  binds: boolean;
  loading: boolean;
  upstreamCount: number;
  nowUnixSecs: number | null;
  timeZone: string;
  className?: string;
}) {
  const label = WINDOW_LABELS[window];
  const used = w?.utilization_percent ?? null;
  const hasReading = used != null && Number.isFinite(used);
  const resetSecs = hasReading && w ? w.cc_window_reset_unix_secs : null;
  const resetLine =
    resetSecs != null && resetSecs > 0
      ? [
          `Resets ${formatMonthDayTime(resetSecs, timeZone)}`,
          nowUnixSecs == null
            ? null
            : resetSecs > nowUnixSecs
              ? `in ${formatDuration(resetSecs - nowUnixSecs)}`
              : 'reset time passed',
        ]
          .filter(Boolean)
          .join(' · ')
      : null;

  return (
    <div
      className={cx('flex min-w-0 flex-col gap-3', className)}
      data-testid="pool-quota-snapshot-slot"
      data-window={window}
    >
      {loading ? (
        <>
          <Skeleton className="hidden size-32 rounded-full md:block" />
          <Skeleton className="h-5 w-12" />
          <Skeleton className="h-1.5 w-full md:hidden" />
          <Skeleton className="h-4 w-40" />
        </>
      ) : (
        <>
          <div className="hidden md:block">
            <ArcGauge label={label} usedPct={used} />
          </div>
          <div className="flex flex-col gap-1.5 md:hidden">
            <span className="text-title-section text-text">{label}</span>
            <HeadroomMeter label={label} size="md" usedPct={used} />
          </div>
          <div className="flex flex-col items-start gap-1 text-body text-text-muted">
            {binds ? <Badge tone="accent">binds pool</Badge> : null}
            <span>
              {hasReading ? (
                <>
                  <span className="hidden md:inline">{`${formatQuotaPercent(used)} used · `}</span>
                  {`${formatCount(w?.contributing_upstreams ?? 0)} of ${formatCount(upstreamCount)} upstreams`}
                </>
              ) : (
                'No reading'
              )}
            </span>
            {resetLine ? (
              <span className="tabular-nums">{resetLine}</span>
            ) : null}
          </div>
          {w && w.provider_lots.length > 0 ? (
            <PoolBreakdownPopover window={window} w={w} />
          ) : null}
        </>
      )}
    </div>
  );
}

/**
 * The cluster band: one large pool-headroom dial for the window that runs
 * out first, when it refills and how old the data is beside it, then the
 * three window sub-gauges. Phones get linear meters instead of dials.
 */
export function PoolHeadroomBand({
  aggregate,
  loading,
}: {
  aggregate: AggregateResponse | undefined;
  loading: boolean;
}) {
  const { effective: timeZone } = useTimezone();
  const binding = bindingPoolWindow(aggregate);
  const bindingWindow = binding
    ? aggregate?.windows.find((entry) => entry.window === binding)
    : undefined;
  const bindingUsed = bindingWindow?.utilization_percent ?? null;
  const serverNow = aggregate?.now_unix_secs ?? null;
  const clientNow = Date.now() / 1000;
  const provider = oldestProviderObservation(aggregate);
  const refillSecs =
    bindingWindow && bindingWindow.cc_window_reset_unix_secs > 0
      ? bindingWindow.cc_window_reset_unix_secs
      : null;
  const heroCaption = binding
    ? `${WINDOW_LABELS[binding]} window binds · ${formatQuotaPercent(bindingUsed)} used`
    : 'No quota readings yet';
  const details: { term: string; value: string; stale?: boolean }[] = [
    {
      term: 'Refills',
      value:
        refillSecs == null ? '—' : formatMonthDayTime(refillSecs, timeZone),
    },
    {
      term: 'Time left',
      value:
        refillSecs == null || serverNow == null
          ? '—'
          : refillSecs > serverNow
            ? formatDuration(refillSecs - serverNow)
            : 'Refill time passed',
    },
    {
      term: 'Pool snapshot',
      value:
        serverNow == null
          ? '—'
          : `${formatInTimezone(serverNow * 1000, timeZone).slice(11)} · ${formatAgo(serverNow, clientNow)}`,
    },
    {
      term: 'Provider data',
      value:
        provider.observedAtUnixSecs == null || serverNow == null
          ? '—'
          : `${formatMonthDayTime(provider.observedAtUnixSecs, timeZone)} · ${formatAgo(provider.observedAtUnixSecs, serverNow)}`,
      stale: provider.stale,
    },
  ];

  return (
    <section
      aria-busy={loading}
      aria-labelledby="pool-headroom-title"
      className="grid grid-cols-1 gap-8 md:gap-10 lg:grid-cols-[auto_minmax(0,1fr)] lg:items-center lg:gap-12"
      data-testid="pool-headroom-band"
    >
      <h2 className="sr-only" id="pool-headroom-title">
        Pool headroom
      </h2>
      <div
        className="flex flex-col gap-6 md:flex-row md:items-center lg:flex-col lg:items-start"
        data-testid="pool-headroom-hero"
      >
        {loading ? (
          <>
            <Skeleton className="hidden size-[264px] rounded-full md:block" />
            <div className="flex flex-col gap-2 md:hidden">
              <Skeleton className="h-4 w-24" />
              <Skeleton className="h-14 w-32" />
              <Skeleton className="h-1 w-full" />
            </div>
          </>
        ) : (
          <>
            <div className="hidden md:block">
              <ArcGauge
                caption={heroCaption}
                label="Pool headroom"
                size="hero"
                usedPct={bindingUsed}
              />
            </div>
            <div className="flex flex-col gap-2 md:hidden">
              <span className="text-label text-text-muted">Pool headroom</span>
              <span
                className={cx(
                  'inline-flex items-baseline tabular-nums',
                  HEADROOM_TONE_CLASS[quotaSeverity(bindingUsed)],
                )}
              >
                <span className="text-display-hero">
                  {formatHeadroomValue(bindingUsed)}
                </span>
                {bindingUsed == null ? null : (
                  <span className="ml-0.5 text-title-section font-normal text-text-muted">
                    % left
                  </span>
                )}
              </span>
              <HeadroomMeter label="Pool headroom" usedPct={bindingUsed} />
              <span className="text-body text-text-muted">{heroCaption}</span>
            </div>
          </>
        )}
        <div className="flex flex-col gap-4">
          <dl className="grid max-w-md grid-cols-2 gap-x-8 gap-y-3">
            {details.map((detail) => (
              <div key={detail.term} className="flex min-w-0 flex-col gap-0.5">
                <dt className="text-label text-text-muted">{detail.term}</dt>
                <dd className="flex flex-wrap items-center gap-1.5 text-body tabular-nums text-text">
                  {loading ? (
                    <Skeleton as="span" className="inline-block h-4 w-24" />
                  ) : (
                    <>
                      {detail.value}
                      {detail.stale ? <Badge tone="warn">Stale</Badge> : null}
                    </>
                  )}
                </dd>
              </div>
            ))}
          </dl>
          <Hint
            label={
              <span className="block max-w-72 whitespace-normal leading-5">
                Each upstream's usage counts in proportion to its plan size
                relative to Claude Pro (Pro 1×, Max 5× counts 5×). Unknown plans
                count as Pro. Routing is not affected.
              </span>
            }
          >
            <span
              tabIndex={0}
              className="self-start cursor-help rounded-sm text-body text-text-muted underline decoration-dotted decoration-border-strong underline-offset-4 hover:text-text"
            >
              Plan-weighted across upstreams
            </span>
          </Hint>
        </div>
      </div>
      <div className="grid grid-cols-1 gap-6 md:grid-cols-3 md:gap-0">
        {POOL_QUOTA_WINDOWS.map((window, index) => (
          <PoolWindowGauge
            key={window}
            binds={window === binding}
            className={
              index > 0 ? 'md:border-l md:border-row md:pl-6' : 'md:pr-6'
            }
            loading={loading}
            nowUnixSecs={serverNow}
            timeZone={timeZone}
            upstreamCount={aggregate?.upstream_count ?? 0}
            w={aggregate?.windows.find((entry) => entry.window === window)}
            window={window}
          />
        ))}
      </div>
    </section>
  );
}

/**
 * Every upstream's headroom per window on the shared end-label grid. Rows
 * link into the upstream's detail.
 */
function UpstreamStrip() {
  const headroom = useUpstreamHeadroomData();
  const count = headroom.rows.length;
  const subtitle = headroom.isLoading
    ? undefined
    : [
        `${formatCount(count)} configured`,
        headroom.reconnectCount > 0
          ? `${formatCount(headroom.reconnectCount)} need reconnect`
          : null,
        'headroom per window, as last observed',
      ]
        .filter(Boolean)
        .join(' · ');
  return (
    <Section
      title="Upstreams"
      subtitle={subtitle}
      action={
        <Link to="/upstreams" className={OVERVIEW_LINK_CLASS}>
          Open upstreams
        </Link>
      }
    >
      <UpstreamHeadroomGrid
        ariaLabel="Upstream headroom per window"
        data={headroom}
        empty={
          <p className="text-body text-text-muted">No upstreams configured.</p>
        }
        variant="strip"
      />
    </Section>
  );
}

const CLOSEST_TO_LIMIT_ROWS = 5;
/** Closest to limit is sized by rank: the first two rows lead, the rest read as a list. */
const CLOSEST_RANK_TYPE = [
  {
    numeral: 'text-display-hero',
    unit: 'text-body md:text-title-section',
    name: 'text-title-section md:text-title-page',
  },
  { numeral: 'text-display', unit: 'text-body', name: 'text-title-section' },
] as const;
const CLOSEST_REST_TYPE = {
  numeral: 'text-body font-semibold',
  unit: 'text-body',
  name: 'text-body font-medium',
} as const;
const CLOSEST_ROW_CLASS =
  'grid grid-cols-[minmax(4.5rem,auto)_minmax(0,1fr)] items-center gap-x-5 py-4';

export function ClosestToLimit({
  aggregate,
  loading,
  className,
}: {
  aggregate: AggregateResponse | undefined;
  loading: boolean;
  className?: string;
}) {
  const { effective: timeZone } = useTimezone();
  const entries = useMemo(() => closestToLimit(aggregate), [aggregate]);
  const shown = entries.slice(0, CLOSEST_TO_LIMIT_ROWS);
  const hidden = entries.length - shown.length;
  return (
    <div
      aria-busy={loading}
      className={cx('min-w-0', className)}
      data-testid="closest-to-limit"
    >
      <Section
        title="Closest to limit"
        subtitle="Least headroom first, by each upstream's tightest window"
        action={
          hidden > 0 ? (
            <Link to="/upstreams" className={OVERVIEW_LINK_CLASS}>
              {`${hidden} more`}
            </Link>
          ) : undefined
        }
      >
        {loading ? (
          <ul aria-hidden="true" className="flex flex-col">
            {Array.from({ length: 3 }).map((_, index) => (
              <li
                key={index}
                className={cx(
                  CLOSEST_ROW_CLASS,
                  'border-t border-row first:border-t-0 first:pt-0',
                )}
              >
                <Skeleton className="h-9 w-20" />
                <Skeleton className="h-5 w-40" />
              </li>
            ))}
          </ul>
        ) : shown.length === 0 ? (
          <p className="text-body text-text-muted">
            No quota readings from any upstream yet.
          </p>
        ) : (
          <ol className="flex flex-col">
            {shown.map((entry, rank) => (
              <ClosestToLimitRow
                key={entry.upstreamId}
                entry={entry}
                rank={rank}
                nowUnixSecs={aggregate?.now_unix_secs ?? Date.now() / 1000}
                timeZone={timeZone}
              />
            ))}
          </ol>
        )}
      </Section>
    </div>
  );
}

function ClosestToLimitRow({
  entry,
  rank,
  nowUnixSecs,
  timeZone,
}: {
  entry: ClosestToLimitEntry;
  rank: number;
  nowUnixSecs: number;
  timeZone: string;
}) {
  const type = CLOSEST_RANK_TYPE[rank] ?? CLOSEST_REST_TYPE;
  const severity = quotaSeverity(entry.utilizationPercent);
  const windowLabel = WINDOW_LABELS[entry.window];
  const resetAt =
    entry.resetUnixSecs != null
      ? `Resets ${formatMonthDayTime(entry.resetUnixSecs, timeZone)}`
      : undefined;
  const meta = [
    `${windowLabel} window`,
    `${formatQuotaPercent(entry.utilizationPercent)} used`,
    formatResetIn(entry.resetUnixSecs, nowUnixSecs),
  ]
    .filter(Boolean)
    .join(' · ');
  return (
    <li className="border-t border-row first:border-t-0">
      <Link
        to="/upstreams"
        search={{ selectedId: entry.upstreamId }}
        className={cx(
          CLOSEST_ROW_CLASS,
          'group rounded-sm focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-2',
          rank === 0 && 'pt-0',
        )}
        data-rank={rank + 1}
        data-testid="closest-to-limit-row"
      >
        <span
          className={cx(
            'inline-flex items-baseline whitespace-nowrap tabular-nums',
            HEADROOM_TONE_CLASS[severity],
          )}
          data-slot="closest-headroom"
        >
          <span className={type.numeral}>
            {formatHeadroomValue(entry.utilizationPercent)}
          </span>
          <span
            className={cx(
              'ml-0.5 font-normal',
              type.unit,
              HEADROOM_TONE_CLASS[severity] === 'text-text' &&
                'text-text-muted',
            )}
          >
            % left
          </span>
        </span>
        <span className="flex min-w-0 flex-col gap-1">
          <span
            className={cx(
              'truncate text-text group-hover:underline',
              type.name,
            )}
          >
            {entry.upstreamName}
          </span>
          <span
            className="text-body tabular-nums text-text-muted"
            title={resetAt}
          >
            {meta}
          </span>
          {entry.state === 'stale' ? (
            <span className="text-body text-warn-text">Stale reading</span>
          ) : null}
          <HeadroomMeter
            className="mt-1 max-w-72"
            label={`${entry.upstreamName} ${windowLabel} headroom`}
            usedPct={entry.utilizationPercent}
          />
        </span>
      </Link>
    </li>
  );
}

type PoolQuotaChartProps = {
  data: PoolQuotaChartRow[];
  rangeStartUnix: number;
  rangeEndUnix: number;
  range: string;
  latest: PoolQuotaLatest;
  showFable: boolean;
};

/**
 * Headroom history series: 7d is the brand line, 7d (Fable) the same hue
 * dashed, 5h a thin neutral line. No fills, so crossings stay readable.
 */
const POOL_SERIES: Record<
  PoolQuotaWindow,
  { stroke: string; strokeWidth: number; strokeDasharray?: string }
> = {
  '5h': { stroke: 'var(--color-text-muted)', strokeWidth: 1 },
  '7d': { stroke: 'var(--color-accent)', strokeWidth: 1.5 },
  '7d_fable': {
    stroke: 'var(--color-accent)',
    strokeWidth: 1.5,
    strokeDasharray: '4 3',
  },
};

/** Headroom levels where the warn / danger zones begin (100 − used threshold). */
const WARN_LEFT_PCT = 100 - QUOTA_WARN_PCT;
const DANGER_LEFT_PCT = 100 - QUOTA_DANGER_PCT;

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

const PoolQuotaHistory = memo(function PoolQuotaHistory({
  chart,
  loading,
  className,
}: {
  chart: PoolQuotaChartProps;
  loading: boolean;
  className?: string;
}) {
  const { effective: timeZone } = useTimezone();
  const first = chart.data[0];
  const last = chart.data.at(-1);
  const proof =
    first && last
      ? `${formatCount(chart.data.length)} snapshots · ${formatMonthDayTime(first.unix, timeZone)} → ${formatMonthDayTime(last.unix, timeZone)}`
      : null;
  return (
    <div
      aria-busy={loading}
      className={cx('min-w-0', className)}
      data-testid="pool-quota-card"
    >
      <Section
        title="Pool quota history"
        subtitle={`Headroom, higher is better · last ${chart.range}`}
      >
        <PoolQuotaLegend
          latest={chart.latest}
          showFable={chart.showFable}
          loading={loading}
        />
        <div
          className="relative h-64 w-full min-w-0"
          data-testid="pool-quota-chart-slot"
        >
          {loading ? (
            <Skeleton className="absolute inset-0 h-full w-full" />
          ) : (
            <PoolQuotaThemedChart
              seriesData={chart.data}
              rangeStartUnix={chart.rangeStartUnix}
              rangeEndUnix={chart.rangeEndUnix}
              showFable={chart.showFable}
            />
          )}
        </div>
        {proof && !loading ? (
          <p className="text-body tabular-nums text-text-muted">{proof}</p>
        ) : null}
      </Section>
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
  showFable,
}: {
  /** Headroom rows (0-100, higher is better). */
  seriesData: PoolQuotaChartRow[];
  rangeStartUnix: number;
  rangeEndUnix: number;
  showFable: boolean;
}) {
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
        margin={{ top: 8, right: 8, bottom: 0, left: 0 }}
      >
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
          y={WARN_LEFT_PCT}
          {...CHART_THRESHOLD.warn}
          label={{
            position: 'insideTopRight',
            value: `${WARN_LEFT_PCT}% left`,
            fill: 'var(--color-warn-text)',
            fontSize: 12,
          }}
        />
        <ReferenceLine
          y={DANGER_LEFT_PCT}
          {...CHART_THRESHOLD.danger}
          label={{
            position: 'insideTopRight',
            value: `${DANGER_LEFT_PCT}% left`,
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
                {payload.map((p) => {
                  const key = String(p.dataKey);
                  return (
                    <div
                      key={key}
                      className="flex items-center justify-between gap-3 py-0.5"
                    >
                      <span className="inline-flex items-center gap-1.5 text-text-muted">
                        {isPoolQuotaWindow(key) ? (
                          <>
                            <LegendSwatch {...POOL_SERIES[key]} />
                            {`${WINDOW_LABELS[key]} window`}
                          </>
                        ) : (
                          key
                        )}
                      </span>
                      <span className="font-medium tabular-nums">
                        {typeof p.value === 'number'
                          ? formatHeadroom(100 - p.value)
                          : '—'}
                      </span>
                    </div>
                  );
                })}
              </div>
            );
          }}
        />
        {POOL_QUOTA_WINDOWS.filter(
          (window) => showFable || window !== '7d_fable',
        ).map((window) => (
          <Area
            key={window}
            type="monotone"
            dataKey={window}
            {...POOL_SERIES[window]}
            fill="none"
            isAnimationActive={false}
            connectNulls={false}
          />
        ))}
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
    <div className="flex flex-wrap items-center gap-x-5 gap-y-1.5 text-body text-text-muted">
      {POOL_QUOTA_WINDOWS.filter(
        (window) => showFable || window !== '7d_fable',
      ).map((window) => {
        const value = latest?.[window];
        return (
          <span
            key={window}
            className="inline-flex min-h-5 items-center gap-1.5"
            data-testid="pool-quota-legend-slot"
          >
            <LegendSwatch {...POOL_SERIES[window]} />
            {WINDOW_LABELS[window]}
            {loading ? (
              <Skeleton as="span" className="inline-block h-3 w-12" />
            ) : value != null ? (
              <>
                {' · '}
                <span className="tabular-nums text-text">
                  {formatHeadroom(value)}
                </span>
              </>
            ) : null}
          </span>
        );
      })}
      <span className="inline-flex min-h-5 items-center gap-1.5">
        <LegendSwatch {...CHART_THRESHOLD.warn} />
        {`Warn below ${WARN_LEFT_PCT}% left`}
      </span>
      <span className="inline-flex min-h-5 items-center gap-1.5">
        <LegendSwatch {...CHART_THRESHOLD.danger} />
        {`Danger below ${DANGER_LEFT_PCT}% left`}
      </span>
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
      rangeStartUnix: poolHistoryNowUnixSecs - displayedPoolHistoryRangeSecs,
      rangeEndUnix: poolHistoryNowUnixSecs,
      range: displayedPoolHistoryRange,
      latest: chartLatest,
      showFable,
    }),
    [
      chartLatest,
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
          {/* Instrument cluster: pool headroom first, then every upstream,
          then what runs out first beside the pool's history, then traffic. */}
          <PoolHeadroomBand
            aggregate={quotaAggregate.data}
            loading={quotaLoading}
          />

          <UpstreamStrip />

          <div className="grid min-w-0 grid-cols-1 gap-12 lg:grid-cols-[minmax(0,5fr)_minmax(0,7fr)] lg:gap-10">
            <ClosestToLimit
              aggregate={quotaAggregate.data}
              loading={quotaLoading}
            />
            <PoolQuotaHistory chart={poolQuotaChart} loading={quotaLoading} />
          </div>

          <div className="grid min-w-0 grid-cols-1 gap-12 lg:grid-cols-[minmax(0,7fr)_minmax(0,5fr)] lg:gap-10">
            <Section
              title="Traffic"
              subtitle={`Last ${range}`}
              className="min-w-0"
            >
              <div className="min-w-0" data-testid="overview-kpi-strip">
                {/* A window without requests has nothing to chart: one line
                instead of five readouts of zeros and dashes. */}
                {!summary.isPending && noTraffic ? (
                  <p className="text-body text-text-muted">
                    {`No requests in the last ${range}`}
                  </p>
                ) : (
                  <div className="grid grid-cols-2 gap-y-6 sm:grid-cols-3 xl:grid-cols-5">
                    <ValueTile
                      className={KPI_CELL_CLASS[0]}
                      chartId="request-rate"
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
            </Section>

            <TopPrincipalsSection
              range={range}
              principals={topPrincipals}
              loading={
                principalUsage.data === undefined && principalUsage.isPending
              }
            />
          </div>
          {/* Latest requests: the feed is not range-scoped (newest events of any
          age), so its label must not suggest it follows the range picker. */}
          <Section
            title="Latest requests (any time)"
            subtitle={
              <span>
                Newest first, not limited to the range above — full view on Logs
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
            height, with the full history one click away on Logs. */}
            <Card className="min-w-0">
              <div className="relative overflow-x-auto scroll-fade-right">
                <RequestEventsTable
                  events={latestRows}
                  principalNameMap={principalNameMap}
                  upstreamNameMap={upstreamNameMap}
                  loading={events.isLoading}
                  liveFlashIds={recentLiveIds}
                  columns={OVERVIEW_TABLE_COLUMNS}
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
