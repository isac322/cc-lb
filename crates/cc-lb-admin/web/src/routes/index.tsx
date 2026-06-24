import { createFileRoute, useNavigate } from '@tanstack/react-router';
import {
  Activity,
  ArrowUpRight,
  Database,
  Gauge,
  ShieldCheck,
  Timer,
  TrendingUp,
  Users,
} from 'lucide-react';
import {
  useCallback,
  useEffect,
  useId,
  useMemo,
  useRef,
  useState,
} from 'react';
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
import {
  Card,
  CardHeader,
  cx,
  PageContainer,
  Section,
  Sparkline,
} from '../components/ui/primitives';
import { RequestEventsTable } from '../components/ui/RequestEventsTable';
import { eventTime, type RequestEvent, streamEventsFetch } from '../lib/api';
import { getWindowColor } from '../lib/colors';
import {
  usePrincipalNameMap,
  useRecentEventsInfinite,
  useSubscriptionQuotaAggregate,
  useSubscriptionQuotaPoolHistory,
  useSummary,
  useUpstreamNameMap,
  useUsage,
} from '../lib/queries';
export const Route = createFileRoute('/')({
  component: OverviewPage,
});

const RANGES = ['1h', '6h', '24h', '7d'] as const;
type Range = (typeof RANGES)[number];

function fmtCount(n: number | undefined | null): string {
  if (n == null) return '0';
  if (n >= 1e9) return `${(n / 1e9).toFixed(1)}B`;
  if (n >= 1e6) return `${(n / 1e6).toFixed(1)}M`;
  if (n >= 1e3) return `${(n / 1e3).toFixed(1)}k`;
  if (Number.isInteger(n)) return n.toString();
  return n.toFixed(1);
}
function fmtUsd(n: number | undefined | null, digits = 2): string {
  if (n == null) return '$0.00';
  if (n >= 1000) return `$${n.toFixed(0)}`;
  return `$${n.toFixed(digits)}`;
}
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

function ValueTile({
  icon,
  label,
  value,
  sub,
  spark,
  sparkColor,
  tone = 'neutral',
  size = 'md',
}: {
  icon: React.ReactNode;
  label: string;
  value: React.ReactNode;
  sub?: React.ReactNode;
  spark?: number[];
  sparkColor?: string;
  tone?: 'neutral' | 'accent' | 'warn' | 'ok';
  size?: 'sm' | 'md';
}) {
  return (
    <div
      className={cx(
        'glass rounded-sm flex flex-col gap-2 relative overflow-hidden',
        size === 'sm' ? 'p-2.5 min-h-[88px]' : 'p-3 min-h-[110px]',
      )}
    >
      <div className="flex items-center gap-1.5 text-text-faint">
        <span className="w-3.5 h-3.5">{icon}</span>
        <span className="text-[11px] uppercase tracking-wider truncate">
          {label}
        </span>
      </div>
      <div className="flex items-baseline justify-between gap-2 min-w-0">
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
      </div>
      {sub ? (
        <div className="text-[11px] text-text-faint truncate">{sub}</div>
      ) : null}
      {spark && spark.length > 0 ? (
        <div
          className={
            size === 'sm'
              ? '-mx-2.5 -mb-2.5 pt-1 mt-auto'
              : '-mx-3 -mb-3 pt-1 mt-auto'
          }
        >
          <Sparkline data={spark} color={sparkColor ?? 'var(--color-accent)'} />
        </div>
      ) : null}
    </div>
  );
}

function ProgressBar({
  pct,
  color,
  height = 'h-1.5',
}: {
  pct: number;
  color: string;
  height?: string;
}) {
  const clamped = Math.min(100, Math.max(0, pct || 0));
  return (
    <div
      className={cx(
        'relative w-full bg-overlay-3 rounded-full overflow-hidden',
        height,
      )}
    >
      <div
        className="absolute top-0 left-0 h-full rounded-full transition-all"
        style={{ width: `${clamped}%`, background: color }}
      />
    </div>
  );
}

type AggregateWindow = NonNullable<
  ReturnType<typeof useSubscriptionQuotaAggregate>['data']
>['windows'][number];

const POOL_PALETTES: Record<'5h' | '7d', readonly string[]> = {
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
};

function poolSegmentColor(window: '5h' | '7d', index: number): string {
  const palette = POOL_PALETTES[window];
  return palette[index % palette.length];
}

function PoolQuotaPopoverContent({
  window,
  w,
  activeIdx,
}: {
  window: '5h' | '7d';
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
}: {
  window: '5h' | '7d';
  w: AggregateWindow | undefined;
}) {
  const [activeIdx, setActiveIdx] = useState<number | null>(null);
  const [openPopover, setOpenPopover] = useState(false);

  if (!w) {
    return (
      <div className="flex flex-col gap-2 min-h-[56px]">
        <div className="flex items-center justify-between mb-1.5">
          <span className="text-xs font-medium text-text-muted">
            {window} pool
          </span>
          <span className="text-xs text-text-faint">no data</span>
        </div>
        <div className="h-5 w-full rounded-full border border-subtle bg-surface-raised" />
      </div>
    );
  }

  const totalRatio = w.provider_lots.reduce(
    (sum, lot) => sum + lot.capacity_ratio,
    0,
  );
  const usedPct = w.utilization_percent ?? 0;

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
    w.provider_lots.map((lot, i) => {
      const util = lot.utilization ?? 0;
      const weightedContribution =
        totalRatio > 0
          ? ((util * lot.capacity_ratio) / totalRatio) * 100
          : 0;
      if (weightedContribution <= 0) return null;

      const idColor = poolSegmentColor(window, i);
      const isHovered = activeIdx === i;
      const isOtherHovered = activeIdx !== null && activeIdx !== i;
      return (
        <div
          key={i}
          className={cx(
            'h-full border-r border-bg last:border-r-0 transition-all cursor-pointer',
            isHovered &&
              'outline outline-1 outline-white/60 outline-offset-[-1px] z-10',
            isOtherHovered && 'opacity-60',
          )}
          style={{
            width: `${weightedContribution}%`,
            backgroundColor: idColor,
          }}
          title={`${lot.upstream_name} · ${lot.capacity_ratio.toFixed(1)}x · ${lot.utilization != null ? (util * 100).toFixed(1) : '—'}%`}
          onMouseEnter={() => handleInteraction(i)}
          onClick={() => handleInteraction(i, true)}
          onTouchStart={() => handleInteraction(i, true)}
        />
      );
    });

  const pctText =
    w.utilization_percent != null ? `${usedPct.toFixed(1)}%` : '—';
  return (
    <div
      className={cx('relative', openPopover && 'z-50')}
      onMouseLeave={() => handleInteraction(null)}
    >
      <div className="2xl:hidden flex flex-col gap-1.5">
        <div className="flex items-center justify-between">
          <span className="text-xs font-medium text-text-muted">
            {window} pool
          </span>
          <span className="tabular-nums font-medium text-sm leading-none text-text">
            {pctText}
          </span>
        </div>
        <div className="h-5 w-full flex rounded-full overflow-hidden border border-subtle bg-surface-raised">
          {renderSegments()}
        </div>
      </div>

      <div className="hidden 2xl:flex items-center gap-2">
        <span className="text-xs font-medium text-text-muted shrink-0">
          {window}
        </span>
        <div className="flex-1 h-5 flex rounded-full overflow-hidden border border-subtle bg-surface-raised min-w-0">
          {renderSegments()}
        </div>
        <span className="tabular-nums font-medium text-sm leading-none text-text shrink-0">
          {pctText}
        </span>
      </div>

      {openPopover && (
        <>
          <div
            className="fixed inset-0 z-40 sm:hidden"
            onClick={() => handleInteraction(null, true)}
            onTouchStart={() => handleInteraction(null, true)}
          />
          <div className="absolute top-full left-0 mt-2 w-full z-50 bg-bg-sub border border-subtle-strong rounded-md shadow-xl p-2">
            <PoolQuotaPopoverContent
              window={window}
              w={w}
              activeIdx={activeIdx}
            />
          </div>
        </>
      )}
    </div>
  );
}

function PoolQuotaCard({
  aggregate,
  chart,
}: {
  aggregate: ReturnType<typeof useSubscriptionQuotaAggregate>;
  chart: {
    data: { unix: number; '5h': number | null; '7d': number | null }[];
    maxValue: number;
    rangeStartUnix: number;
    rangeEndUnix: number;
    range: Range;
    latest: { '5h': number | null; '7d': number | null };
  };
}) {
  const w5h = aggregate.data?.windows.find((x) => x.window === '5h');
  const w7d = aggregate.data?.windows.find((x) => x.window === '7d');
  const upstreamCount = aggregate.data?.upstream_count ?? 0;
  const contributingCount = Math.max(
    w5h?.contributing_upstreams ?? 0,
    w7d?.contributing_upstreams ?? 0,
  );
  return (
    <Card className="min-w-0 flex flex-col h-full">
      <CardHeader
        title={
          <span className="inline-flex items-center gap-2">
            <Gauge className="w-3.5 h-3.5 text-text-faint" />
            Pool quota
          </span>
        }
        subtitle={
          upstreamCount > 0
            ? `plan-weighted · ${contributingCount} of ${upstreamCount} upstreams`
            : 'plan-weighted'
        }
      />
      <div className="flex-1 flex flex-col gap-4 p-4 pt-2 min-h-0">
        <div className="flex flex-col gap-2">
          <div className="text-xs uppercase tracking-wider font-medium text-text-faint">
            Snapshot
          </div>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-x-10 gap-y-4">
            <PoolQuotaStackedBar window="5h" w={w5h} />
            <PoolQuotaStackedBar window="7d" w={w7d} />
          </div>
        </div>
        <div className="h-px bg-border" />
        <div className="flex-1 flex flex-col gap-2 min-h-0">
          <div className="flex items-center justify-between">
            <div className="text-xs uppercase tracking-wider font-medium text-text-faint">
              Trend · {chart.range}
            </div>
            <PoolQuotaLegend latest={chart.latest} />
          </div>
          <div className="flex-1 min-h-64 min-w-0 w-full relative">
            <PoolQuotaThemedChart
              seriesData={chart.data}
              rangeStartUnix={chart.rangeStartUnix}
              rangeEndUnix={chart.rangeEndUnix}
              maxValue={chart.maxValue}
            />
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
}: {
  seriesData: { unix: number; '5h': number | null; '7d': number | null }[];
  maxValue: number;
  rangeStartUnix: number;
  rangeEndUnix: number;
}) {
  const chartId = useId();
  const c5h = getWindowColor('5h');
  const c7d = getWindowColor('7d');

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
            <linearGradient
              id={`${chartId}-grad-5h`}
              x1="0"
              y1="0"
              x2="0"
              y2="1"
            >
              <stop offset="0%" stopColor={c5h.stroke} stopOpacity={0.55} />
              <stop offset="100%" stopColor={c5h.stroke} stopOpacity={0} />
            </linearGradient>
            <linearGradient
              id={`${chartId}-grad-7d`}
              x1="0"
              y1="0"
              x2="0"
              y2="1"
            >
              <stop offset="0%" stopColor={c7d.stroke} stopOpacity={0.55} />
              <stop offset="100%" stopColor={c7d.stroke} stopOpacity={0} />
            </linearGradient>
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
                      w === '5h' ? '5h window' : w === '7d' ? '7d window' : w;
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
}: {
  latest?: { '5h': number | null; '7d': number | null };
}) {
  const c5h = getWindowColor('5h');
  const c7d = getWindowColor('7d');
  const v5h = latest?.['5h'];
  const v7d = latest?.['7d'];
  return (
    <div className="flex flex-wrap items-center gap-3 text-[11px] text-text-faint">
      <span className="inline-flex items-center gap-1.5">
        <span
          className="w-2 h-2 rounded-sm"
          style={{ background: c5h.stroke }}
        />
        5h{v5h != null ? ` · ${v5h.toFixed(0)}%` : ''}
      </span>
      <span className="inline-flex items-center gap-1.5">
        <span
          className="w-2 h-2 rounded-sm"
          style={{ background: c7d.stroke }}
        />
        7d{v7d != null ? ` · ${v7d.toFixed(0)}%` : ''}
      </span>
    </div>
  );
}

function OverviewPage() {
  const navigate = useNavigate({ from: Route.fullPath });
  const [range, setRange] = useState<Range>('24h');
  const stepFor = useCallback(
    (r: Range) => (r === '7d' || r === '24h' ? 'hour' : 'minute'),
    [],
  );

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
    windows: '5h,7d',
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
    windows: '5h,7d',
    sinceUnixSecs: nowUnixSecs - seriesRangeSecs,
    untilUnixSecs: nowUnixSecs,
  });

  const [liveEvents, setLiveEvents] = useState<RequestEvent[]>([]);
  const [streamStatus, setStreamStatus] = useState<
    'idle' | 'connecting' | 'live' | 'down'
  >('idle');
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

  useEffect(() => {
    setStreamStatus('connecting');
    const close = streamEventsFetch('/admin/events/stream', {
      onConnect: () => setStreamStatus('live'),
      onEvent: (data) => {
        try {
          const parsed = JSON.parse(data) as RequestEvent;
          setLiveEvents((prev) => [parsed, ...prev].slice(0, 50));
        } catch {}
      },
      onError: () => setStreamStatus('down'),
    });
    return () => close();
  }, []);

  const recentRows = useMemo(() => {
    const historical = events.data?.pages.flatMap((p) => p.events) ?? [];
    const seen = new Set<string>();
    const out: RequestEvent[] = [];
    for (const ev of liveEvents) {
      if (!seen.has(ev.request_id)) {
        seen.add(ev.request_id);
        out.push(ev);
      }
    }
    for (const ev of historical) {
      if (!seen.has(ev.request_id)) {
        seen.add(ev.request_id);
        out.push(ev);
      }
    }
    return out.sort(
      (a, b) => (eventTime(b)?.getTime() ?? 0) - (eventTime(a)?.getTime() ?? 0),
    );
  }, [liveEvents, events.data]);

  const recentLiveIds = useMemo(
    () => new Set(liveEvents.slice(0, 20).map((e) => e.request_id)),
    [liveEvents],
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
  const totalTokens = totals ? totals.input_tokens + totals.output_tokens : 0;
  const virtualUsd = totals ? totals.virtual_cost_micros / 1_000_000 : 0;
  const errRate =
    totals && totals.request_count > 0
      ? (totals.error_count / totals.request_count) * 100
      : 0;
  const latency =
    (totals as any)?.p95_latency_ms ?? totals?.avg_latency_ms ?? 0;
  const latencyLabel =
    (totals as any)?.p95_latency_ms !== undefined ? 'p95' : 'Avg latency';

  const sparkRate =
    summary.data?.sparkline.buckets.map(
      (b) =>
        b.request_count / Math.max(1, summary.data.step === 'hour' ? 3600 : 60),
    ) ?? [];
  const sparkTokens =
    summary.data?.sparkline.buckets.map(
      (b) => b.input_tokens + b.output_tokens,
    ) ?? [];
  const sparkCost =
    summary.data?.sparkline.buckets.map(
      (b) => b.virtual_cost_micros / 1_000_000,
    ) ?? [];
  const sparkLatency =
    summary.data?.sparkline.buckets.map(
      (b) => b.latency_ms_sum / Math.max(1, b.latency_count),
    ) ?? [];
  const sparkError =
    summary.data?.sparkline.buckets.map((b) =>
      b.request_count > 0 ? (b.error_count / b.request_count) * 100 : 0,
    ) ?? [];

  // Chart Data
  const chartData = useMemo(() => {
    const w5h = quotaPoolHistory.data?.windows.find((x) => x.window === '5h');
    const w7d = quotaPoolHistory.data?.windows.find((x) => x.window === '7d');
    if (!w5h?.series.length && !w7d?.series.length) {
      return [] as { unix: number; '5h': number | null; '7d': number | null }[];
    }
    const bucketsByTime = new Map<
      number,
      { '5h': number | null; '7d': number | null }
    >();
    const addPoint = (
      window: '5h' | '7d',
      points: {
        snapshot_at_unix_secs: number;
        utilization_percent: number | null;
      }[],
    ) => {
      for (const point of points) {
        const ts = point.snapshot_at_unix_secs;
        if (!bucketsByTime.has(ts)) {
          bucketsByTime.set(ts, { '5h': null, '7d': null });
        }
        const row = bucketsByTime.get(ts);
        if (row && point.utilization_percent != null) {
          row[window] = point.utilization_percent;
        }
      }
    };
    addPoint('5h', w5h?.series ?? []);
    addPoint('7d', w7d?.series ?? []);
    return Array.from(bucketsByTime.entries())
      .map(([unix, row]) => ({ unix, '5h': row['5h'], '7d': row['7d'] }))
      .sort((a, b) => a.unix - b.unix);
  }, [quotaPoolHistory.data]);

  const chartMaxValue = useMemo(() => {
    let m = 100;
    for (const row of chartData) {
      if (row['5h'] != null && row['5h'] > m) m = row['5h'];
      if (row['7d'] != null && row['7d'] > m) m = row['7d'];
    }
    return Math.ceil(m / 10) * 10;
  }, [chartData]);

  const chartLatest = useMemo(() => {
    let latest5h: number | null = null;
    let latest7d: number | null = null;
    for (const row of chartData) {
      if (row['5h'] != null) latest5h = row['5h'];
      if (row['7d'] != null) latest7d = row['7d'];
    }
    return { '5h': latest5h, '7d': latest7d };
  }, [chartData]);

  // Principals Data
  const topPrincipals = useMemo(() => {
    const series = principalUsage.data?.series ?? [];
    const byId = new Map<string, any>();
    let maxCost = 0;
    let totalCost = 0;

    for (const s of series) {
      if (!s.key) continue;
      let cost = 0;
      let tokens = 0;
      let requests = 0;
      for (const b of s.buckets) {
        cost += (b.virtual_cost_micros ?? 0) / 1_000_000;
        tokens += (b.input_tokens ?? 0) + (b.output_tokens ?? 0);
        requests += b.request_count ?? 0;
      }
      if (cost <= 0 && requests <= 0) continue;

      totalCost += cost;
      if (cost > maxCost) maxCost = cost;

      byId.set(s.key, {
        id: s.key,
        name: principalNameMap.get(s.key) ?? s.key,
        cost_usd: cost,
        tokens,
        requests,
        primary_model: '—', // Not available in this grouping
      });
    }

    return Array.from(byId.values())
      .sort((a, b) => b.cost_usd - a.cost_usd)
      .slice(0, 5)
      .map((p) => ({
        ...p,
        share_pct: totalCost > 0 ? (p.cost_usd / totalCost) * 100 : 0,
        max_cost: maxCost,
      }));
  }, [principalUsage.data, principalNameMap]);

  return (
    <PageContainer>
      <div className="flex items-center justify-between mb-2">
        <h1 className="text-lg font-medium">Overview</h1>
        <div className="flex flex-wrap bg-overlay-2 border border-subtle rounded-sm p-0.5">
          {RANGES.map((r) => (
            <button
              key={r}
              type="button"
              onClick={() => setRange(r)}
              className={cx(
                'px-2.5 h-7 text-xs rounded-sm transition-colors',
                r === range
                  ? 'bg-overlay-6 text-text'
                  : 'text-text-faint hover:text-text',
              )}
            >
              {r}
            </button>
          ))}
        </div>
      </div>

      {/* KPI Strip */}
      <div className="grid grid-cols-2 md:grid-cols-3 xl:grid-cols-5 gap-2">
        <ValueTile
          size="sm"
          icon={<Activity className="w-3.5 h-3.5" />}
          label="req/s"
          value={reqPerSec.toFixed(1)}
          sub={`${fmtCount(totals?.request_count)} / ${range}`}
          spark={sparkRate}
          sparkColor="var(--color-accent)"
        />
        <ValueTile
          size="sm"
          icon={<Database className="w-3.5 h-3.5" />}
          label="tokens"
          value={fmtCount(totalTokens)}
          spark={sparkTokens}
          sparkColor="#06b6d4"
        />
        <ValueTile
          size="sm"
          icon={<TrendingUp className="w-3.5 h-3.5" />}
          label="equiv $"
          value={fmtUsd(virtualUsd)}
          spark={sparkCost}
          sparkColor="#10b981"
          tone="accent"
        />
        <ValueTile
          size="sm"
          icon={<Timer className="w-3.5 h-3.5" />}
          label={latencyLabel}
          value={fmtMs(latency)}
          spark={sparkLatency}
          sparkColor="#f59e0b"
        />
        <ValueTile
          size="sm"
          icon={<ShieldCheck className="w-3.5 h-3.5" />}
          label="err rate"
          value={`${errRate.toFixed(2)}%`}
          spark={sparkError}
          sparkColor="var(--color-danger)"
        />
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-[2fr_1fr] gap-4 min-w-0">
        <PoolQuotaCard
          aggregate={quotaAggregate}
          chart={{
            data: chartData,
            maxValue: chartMaxValue,
            rangeStartUnix: nowUnixSecs - seriesRangeSecs,
            rangeEndUnix: nowUnixSecs,
            range,
            latest: chartLatest,
          }}
        />

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
            <div className="flex flex-col">
              {topPrincipals.length === 0 ? (
                <div className="p-4 text-center text-xs text-text-faint">
                  No usage data
                </div>
              ) : (
                topPrincipals.map((p) => (
                  <div
                    key={p.id}
                    className="flex items-center gap-3 px-3 py-2 border-b border-subtle last:border-b-0 hover:bg-overlay-2 transition-colors"
                  >
                    <div className="min-w-0 flex-1">
                      <div className="text-sm truncate">{p.name}</div>
                      <div className="text-[11px] text-text-faint truncate">
                        {p.primary_model} · {p.requests.toLocaleString()} req ·{' '}
                        {fmtCount(p.tokens)} tok
                      </div>
                      <div className="mt-1.5">
                        <ProgressBar
                          pct={(p.cost_usd / Math.max(1, p.max_cost)) * 100}
                          color="var(--color-accent)"
                        />
                      </div>
                    </div>
                    <div className="text-right shrink-0">
                      <div className="text-sm font-mono tabular-nums">
                        {fmtUsd(p.cost_usd)}
                      </div>
                      <div className="text-[11px] text-text-faint tabular-nums">
                        {p.share_pct.toFixed(1)}%
                      </div>
                    </div>
                  </div>
                ))
              )}
            </div>
          </div>
        </Card>
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
            <span
              className={cx(
                'status-dot',
                streamStatus === 'live' ? 'live' : 'neutral',
              )}
            />
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
            className="overflow-auto max-h-[50vh] scroll-fade-right"
            ref={scrollContainerRef}
          >
            <RequestEventsTable
              events={recentRows}
              principalNameMap={principalNameMap}
              upstreamNameMap={upstreamNameMap}
              loading={events.isLoading}
              liveFlashIds={recentLiveIds}
              onRowClick={() => navigate({ to: '/logs' })}
              columns={{ cost: true, tokens: true }}
              sentinelRef={sentinelRef}
              loadingMore={events.isFetchingNextPage}
              hasMore={events.hasNextPage}
              minWidthClass="min-w-[980px]"
              emptyTitle="No recent requests"
            />
          </div>
        </Card>
      </Section>
    </PageContainer>
  );
}
