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
  UpstreamUsageTable,
  useUpstreamUsageData,
} from '../components/upstreams/UpstreamUsageTable';
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
  poolWindowResets,
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
// is reserved for pool quota, so the cache-miss overlay is full-strength ink.
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
  range: string;
  latest: PoolQuotaLatest;
  showFable: boolean;
};

/**
 * Pool usage series: 7d is the brand line, 7d (Fable) the same hue dashed,
 * 5h a thin neutral line. No fills, so crossings stay readable.
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
}: {
  chart: PoolQuotaChartProps;
  aggregate: AggregateResponse | undefined;
  loading: boolean;
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
      <Section
        title="Pool quota usage"
        subtitle={`Used per window across the pool · last ${chart.range}`}
      >
        <PoolQuotaLegend
          latest={chart.latest}
          resets={resets}
          nowUnixSecs={serverNow}
          timeZone={timeZone}
          showFable={chart.showFable}
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
              showFable={chart.showFable}
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
  /** Used rows (0-100). */
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
        margin={{ top: 16, right: 8, bottom: 0, left: 0 }}
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

/**
 * The chart's header row: per window its swatch, current `N% used` in the
 * severity ink and when it resets (absolute time in the `title`), then the
 * two threshold rules.
 */
export function PoolQuotaLegend({
  latest,
  resets,
  nowUnixSecs = null,
  timeZone = 'UTC',
  showFable,
  loading = false,
}: {
  latest?: PoolQuotaLatest;
  resets?: Record<PoolQuotaWindow, number | null>;
  nowUnixSecs?: number | null;
  timeZone?: string;
  showFable: boolean;
  loading?: boolean;
}) {
  return (
    <div className="flex flex-wrap items-baseline gap-x-6 gap-y-2 text-body text-text-muted">
      {POOL_QUOTA_WINDOWS.filter(
        (window) => showFable || window !== '7d_fable',
      ).map((window) => {
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
            <LegendSwatch {...POOL_SERIES[window]} />
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
          {/* Time first: how pool usage moved, then every upstream's
          usage, then traffic. */}
          <PoolQuotaUsage
            aggregate={quotaAggregate.data}
            chart={poolQuotaChart}
            loading={quotaLoading}
          />

          <UpstreamsUsageSection />

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
