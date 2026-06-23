import { createFileRoute, useNavigate } from '@tanstack/react-router';
import {
  Activity,
  ArrowUpRight,
  Database,
  Gauge,
  LineChart as LineIcon,
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
  ResponsiveContainer,
  Tooltip as RTooltip,
  XAxis,
  YAxis,
} from 'recharts';
import {
  Card,
  CardBody,
  CardHeader,
  cx,
  PageContainer,
  Section,
  Sparkline,
  StatusBadge,
} from '../components/ui/primitives';
import { RequestEventsTable } from '../components/ui/RequestEventsTable';
import { eventTime, type RequestEvent, streamEventsFetch } from '../lib/api';
import { getWindowColor } from '../lib/colors';
import {
  usePrincipalNameMap,
  useRecentEventsInfinite,
  useSubscriptionQuotaAggregate,
  useSubscriptionQuotaSeries,
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
function fmtMin(min: number | null): string {
  if (min == null || !Number.isFinite(min)) return '—';
  if (min < 60) return `${Math.floor(min)}m`;
  if (min < 60 * 24) {
    const h = Math.floor(min / 60);
    const m = Math.floor(min - h * 60);
    return m === 0 ? `${h}h` : `${h}h ${m}m`;
  }
  const d = Math.floor(min / (60 * 24));
  const h = Math.floor((min - d * 60 * 24) / 60);
  return h === 0 ? `${d}d` : `${d}d ${h}h`;
}
function utilTone(pct: number): 'ok' | 'warn' | 'danger' {
  if (pct >= 85) return 'danger';
  if (pct >= 60) return 'warn';
  return 'ok';
}
function utilColor(pct: number): string {
  if (pct >= 85) return 'var(--color-danger)';
  if (pct >= 60) return 'var(--color-warn)';
  return 'var(--color-ok)';
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

function MetaStat({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="rounded-sm bg-overlay-1 border border-subtle p-2">
      <div className="text-text-faint text-[10px] uppercase tracking-wider">
        {label}
      </div>
      <div className="font-mono tabular-nums text-sm">{value}</div>
    </div>
  );
}

function PoolQuotaCard({
  window,
  aggregate,
}: {
  window: '5h' | '7d';
  aggregate: ReturnType<typeof useSubscriptionQuotaAggregate>;
}) {
  const w = aggregate.data?.windows.find((x) => x.window === window);
  const used_pct = w?.utilization_percent ?? 0;
  const used_tokens = w?.used_tokens ?? 0;
  const capacity_tokens =
    w?.projected_capacity_tokens_estimate ??
    w?.capacity_to_now_tokens_estimate ??
    0;

  const nowUnixSecs = Math.floor(Date.now() / 1000);
  const elapsedMin = w
    ? Math.max(1, (nowUnixSecs - w.cc_window_start_unix_secs) / 60)
    : 1;
  const burn_per_min = used_tokens / elapsedMin;
  const reset_min = w
    ? Math.max(0, (w.cc_window_reset_unix_secs - nowUnixSecs) / 60)
    : 0;
  const remainingTokens = Math.max(0, capacity_tokens - used_tokens);
  const eta_min = burn_per_min > 0 ? remainingTokens / burn_per_min : null;
  const confidence = w?.confidence ?? 'missing';

  const c = utilColor(used_pct);
  const tone = utilTone(used_pct);
  const label = `${window} pool window`;

  if (!w) {
    return (
      <Card className="relative overflow-hidden p-3 flex items-center justify-center min-h-[140px]">
        <span className="text-xs text-text-faint">No {window} quota data</span>
      </Card>
    );
  }

  return (
    <Card className="relative overflow-hidden">
      <div className="relative grid gap-3 p-3">
        <div className="flex flex-col gap-2 min-w-0">
          <div className="flex items-center justify-between gap-2">
            <span className="text-[11px] uppercase tracking-wider text-text-faint">
              {label}
            </span>
            <StatusBadge
              tone={tone === 'ok' ? 'neutral' : tone}
              label={confidence}
            />
          </div>
          <div className="flex items-baseline gap-2">
            <span
              className="tabular-nums font-medium leading-none text-3xl"
              style={{ color: c }}
            >
              {used_pct.toFixed(1)}
            </span>
            <span
              className="tabular-nums font-medium opacity-70 text-lg"
              style={{ color: c }}
            >
              %
            </span>
            <span className="text-[11px] text-text-faint ml-1 tabular-nums">
              {fmtCount(used_tokens)} / {fmtCount(capacity_tokens)} tok
            </span>
          </div>
          <ProgressBar pct={used_pct} color={c} height="h-2" />
          <div className="grid grid-cols-3 gap-2 mt-1 text-xs">
            <MetaStat label="eta to cap" value={fmtMin(eta_min)} />
            <MetaStat label="resets in" value={fmtMin(reset_min)} />
            <MetaStat label="burn" value={`${fmtCount(burn_per_min)}/min`} />
          </div>
        </div>
      </div>
    </Card>
  );
}

function PoolQuotaThemedChart({ seriesData }: { seriesData: any[] }) {
  const chartId = useId();
  const c5h = getWindowColor('5h');
  const c7d = getWindowColor('7d');

  if (!seriesData.length) {
    return (
      <div className="w-full h-[240px] flex items-center justify-center text-xs text-text-faint">
        No timeline data
      </div>
    );
  }

  return (
    <div className="w-full" style={{ height: 240, minWidth: 0 }}>
      <ResponsiveContainer width="100%" height="100%">
        <AreaChart
          data={seriesData}
          margin={{ top: 8, right: 8, bottom: 4, left: -20 }}
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
            domain={['dataMin', 'dataMax']}
            tick={{
              fill: 'var(--color-text-faint)',
              fontSize: 10,
              fontFamily: 'Geist Mono',
            }}
            tickFormatter={fmtChartTick}
            axisLine={false}
            tickLine={false}
            minTickGap={40}
          />
          <YAxis
            tick={{
              fill: 'var(--color-text-faint)',
              fontSize: 10,
              fontFamily: 'Geist Mono',
            }}
            tickFormatter={(v) => `${v}%`}
            axisLine={false}
            tickLine={false}
            width={40}
            domain={[0, 100]}
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
                    border: '1px solid var(--color-border)',
                    borderRadius: 2,
                    color: 'var(--color-text)',
                    fontSize: 11,
                    fontFamily: 'Geist Mono Variable, monospace',
                    padding: '6px 10px',
                    boxShadow: '0 4px 12px rgba(0,0,0,0.18)',
                    minWidth: 120,
                  }}
                >
                  <div
                    style={{
                      color: 'var(--color-text-faint)',
                      marginBottom: 4,
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
                          padding: '1px 0',
                          display: 'flex',
                          justifyContent: 'space-between',
                          gap: 8,
                        }}
                      >
                        <span
                          style={{
                            color:
                              typeof p.color === 'string'
                                ? p.color
                                : 'var(--color-text)',
                          }}
                        >
                          {wLabel}
                        </span>
                        <span style={{ fontVariantNumeric: 'tabular-nums' }}>
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
      </ResponsiveContainer>
    </div>
  );
}

function PoolQuotaLegend() {
  const c5h = getWindowColor('5h');
  const c7d = getWindowColor('7d');
  return (
    <div className="flex flex-wrap items-center gap-3 text-[11px] text-text-faint">
      <span className="inline-flex items-center gap-1.5">
        <span
          className="w-2 h-2 rounded-sm"
          style={{ background: c5h.stroke }}
        />
        5h window
      </span>
      <span className="inline-flex items-center gap-1.5">
        <span
          className="w-2 h-2 rounded-sm"
          style={{ background: c7d.stroke }}
        />
        7d window
      </span>
      <span className="text-text-faint">· pool aggregate</span>
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

  const quotaSeries = useSubscriptionQuotaSeries({
    windows: '5h,7d',
    source: 'merged',
    sinceUnixSecs: nowUnixSecs - 86400, // Timeline is always 24h as per mockup
    untilUnixSecs: nowUnixSecs,
    bucketSecs: 300,
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
  const util7d =
    quotaAggregate.data?.windows.find((w) => w.window === '7d')
      ?.utilization_percent ?? 0;

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
    if (!quotaSeries.data?.series.length) return [];
    const bucketsByTime = new Map<number, any>();

    for (const s of quotaSeries.data.series) {
      const w = s.window;
      for (const b of s.buckets) {
        const ts = b.bucket_start_unix_secs;
        if (!bucketsByTime.has(ts)) {
          bucketsByTime.set(ts, {
            unix: ts,
            '5h_sum': 0,
            '5h_count': 0,
            '7d_sum': 0,
            '7d_count': 0,
          });
        }
        const row = bucketsByTime.get(ts)!;
        if (b.utilization_last != null) {
          row[`${w}_sum`] += b.utilization_last * 100;
          row[`${w}_count`] += 1;
        }
      }
    }

    return Array.from(bucketsByTime.values())
      .map((row) => ({
        unix: row.unix,
        '5h': row['5h_count'] > 0 ? row['5h_sum'] / row['5h_count'] : null,
        '7d': row['7d_count'] > 0 ? row['7d_sum'] / row['7d_count'] : null,
      }))
      .sort((a, b) => a.unix - b.unix);
  }, [quotaSeries.data]);

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

      {/* 6 KPI Strip */}
      <div className="grid grid-cols-2 md:grid-cols-3 xl:grid-cols-6 gap-2">
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
          icon={<Gauge className="w-3.5 h-3.5" />}
          label="sub util"
          value={`${util7d.toFixed(0)}%`}
          sub="7d pool aggregate"
          sparkColor="#8b5cf6"
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

      {/* 2-up 5h/7d gauges */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
        <PoolQuotaCard window="5h" aggregate={quotaAggregate} />
        <PoolQuotaCard window="7d" aggregate={quotaAggregate} />
      </div>

      {/* Full-width timeline */}
      <Card>
        <CardHeader
          title={
            <span className="inline-flex items-center gap-2">
              <LineIcon className="w-3.5 h-3.5 text-text-faint" />
              Pool quota timeline
            </span>
          }
          subtitle="5h vs 7d aggregate · % of pool capacity used"
          action={<PoolQuotaLegend />}
        />
        <CardBody className="p-2 pt-1">
          <PoolQuotaThemedChart seriesData={chartData} />
        </CardBody>
      </Card>

      {/* Full-width principals */}
      <Card>
        <CardHeader
          title={
            <span className="inline-flex items-center gap-2">
              <Users className="w-3.5 h-3.5 text-text-faint" />
              Top principals
            </span>
          }
          subtitle={`by virtual cost · ${range}`}
        />
        <div className="overflow-auto max-h-[420px]">
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
