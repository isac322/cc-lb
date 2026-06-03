import { createFileRoute, useNavigate } from '@tanstack/react-router';
import { ArrowUpRight, Info } from 'lucide-react';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  Area,
  AreaChart,
  CartesianGrid,
  Legend,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import {
  Card,
  CardBody,
  CardHeader,
  cx,
  EmptyState,
  KpiTile,
  PageContainer,
  Section,
  StatusBadge,
} from '../components/ui/primitives';
import { RequestEventsTable } from '../components/ui/RequestEventsTable';
import { eventTime, type RequestEvent, streamEventsFetch } from '../lib/api';
import { getUpstreamColor } from '../lib/colors';
import {
  usePrincipalNameMap,
  usePrincipals,
  useRecentEventsInfinite,
  useSubscriptionQuotaAnalysis,
  useSubscriptionQuotaLatest,
  useSubscriptionQuotaSeries,
  useSummary,
  useUpstreamNameMap,
  useUpstreams,
  useUsage,
} from '../lib/queries';
import { useTheme } from '../lib/theme';

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
  return n.toString();
}
function fmtUsd(micros: number | undefined | null): string {
  if (micros == null) return '$0.00';
  return `$${(micros / 1_000_000).toFixed(2)}`;
}
function fmtMs(ms: number | undefined | null): string {
  if (ms == null) return '—';
  return `${Math.round(ms)}ms`;
}
function fmtPct(n: number, d: number): string {
  if (d === 0) return '0.00%';
  return `${((n / d) * 100).toFixed(2)}%`;
}

function OverviewPage() {
  const navigate = useNavigate({ from: Route.fullPath });
  const { effective } = useTheme();
  const isLight = effective === 'light';
  const tooltipBg = isLight ? '#fafafa' : '#0a0a0a';
  const tooltipBorder = isLight ? 'rgba(0,0,0,0.14)' : 'rgba(255,255,255,0.14)';
  const tooltipText = isLight ? '#0a0a0a' : '#ededed';
  const tooltipMuted = isLight ? '#4b5563' : '#9ca3af';
  const [range, setRange] = useState<Range>('24h');
  const stepFor = useCallback(
    (r: Range) => (r === '7d' || r === '24h' ? 'hour' : 'minute'),
    [],
  );

  const summary = useSummary(range);
  const principalUsage = useUsage(range, stepFor(range), 'principal');
  const events = useRecentEventsInfinite({});
  const upstreams = useUpstreams();
  const principals = usePrincipals();
  const principalNameMap = usePrincipalNameMap();
  const upstreamNameMap = useUpstreamNameMap();

  const nowUnixSecs = Math.floor(Date.now() / 1000);
  const sinceUnixSecs = useMemo(() => {
    switch (range) {
      case '1h':
        return nowUnixSecs - 3600;
      case '6h':
        return nowUnixSecs - 21600;
      case '24h':
        return nowUnixSecs - 86400;
      case '7d':
        return nowUnixSecs - 604800;
    }
  }, [range, nowUnixSecs]);
  // Backend rejects (until - since) / bucket_secs > max_points_per_series * 2.
  // Default max_points_per_series = 1000, so we target <= 500 points/series
  // for a 2-window query.
  const bucketSecsForRange = useMemo(() => {
    switch (range) {
      case '1h':
        return 60;
      case '6h':
        return 60;
      case '24h':
        return 300;
      case '7d':
        return 1800;
    }
  }, [range]);

  const quotaLatest = useSubscriptionQuotaLatest({
    windows: '5h,7d',
    source: 'merged',
  });
  const quotaSeries = useSubscriptionQuotaSeries({
    windows: '5h,7d',
    source: 'merged',
    sinceUnixSecs,
    untilUnixSecs: nowUnixSecs,
    bucketSecs: bucketSecsForRange,
  });
  const quotaAnalysis = useSubscriptionQuotaAnalysis({
    windows: '5h,7d',
    source: 'merged',
    sinceUnixSecs,
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

  const chartData = useMemo(() => {
    if (!quotaSeries.data?.series.length)
      return { rows: [], upstreams: [], markers: [] };

    const series = quotaSeries.data.series;
    const upstreamsMap = new Map<string, { id: string; name: string }>();

    for (const s of series) {
      upstreamsMap.set(s.upstream_id, {
        id: s.upstream_id,
        name: s.upstream_name,
      });
    }
    const upstreamsList = Array.from(upstreamsMap.values());

    const bucketsByTime = new Map<number, Record<string, any>>();
    const markers: { ts: number; kind: string; upstreamId: string }[] = [];

    for (const s of series) {
      const uId = s.upstream_id;
      const w = s.window;

      for (const b of s.buckets) {
        const ts = b.bucket_start_unix_secs;
        if (!bucketsByTime.has(ts)) {
          const date = new Date(ts * 1000);
          const label =
            range === '7d' || range === '24h'
              ? `${date.getMonth() + 1}/${date.getDate()} ${date.getHours()}h`
              : date.toTimeString().slice(0, 5);
          bucketsByTime.set(ts, { ts: label, unix: ts });
        }
        const row = bucketsByTime.get(ts)!;
        row[`${uId}_${w}`] =
          b.utilization_last != null ? b.utilization_last * 100 : null;
      }

      for (const m of s.markers) {
        if (m.at_unix_secs) {
          markers.push({ ts: m.at_unix_secs, kind: m.kind, upstreamId: uId });
        }
      }
    }

    const rows = Array.from(bucketsByTime.values()).sort(
      (a, b) => a.unix - b.unix,
    );

    return { rows, upstreams: upstreamsList, markers };
  }, [quotaSeries.data, range]);

  const totals = summary.data?.totals;
  const topPrincipals = useMemo(() => {
    const series = principalUsage.data?.series ?? [];
    const nameById = new Map(
      principals.data?.principals.map((p) => [p.id, p.name]) ?? [],
    );
    const byId = new Map<
      string,
      { id: string; name: string; cost_micros: number }
    >();
    for (const s of series) {
      if (!s.key) continue;
      const cost = s.buckets.reduce(
        (acc, b) => acc + (b.virtual_cost_micros ?? 0),
        0,
      );
      if (cost <= 0) continue;
      const existing = byId.get(s.key);
      if (existing) {
        existing.cost_micros += cost;
      } else {
        byId.set(s.key, {
          id: s.key,
          name: nameById.get(s.key) ?? s.key,
          cost_micros: cost,
        });
      }
    }
    return Array.from(byId.values())
      .sort((a, b) => b.cost_micros - a.cost_micros)
      .slice(0, 5);
  }, [principalUsage.data, principals.data]);

  return (
    <PageContainer>
      {/* KPIs */}
      <div className="grid grid-cols-2 md:grid-cols-3 xl:grid-cols-6 gap-3">
        <KpiTile
          label="Requests"
          value={fmtCount(totals?.request_count)}
          delta={{ value: '+12%', direction: 'up', isPositive: true }}
          hint="Total requests in selected range"
        />
        <KpiTile
          label="Avg Latency"
          value={fmtMs(totals?.avg_latency_ms)}
          delta={{ value: '-4ms', direction: 'down', isPositive: true }}
          hint="Average end-to-end latency. Down is better."
        />
        <KpiTile
          label="Error Rate"
          value={
            totals ? fmtPct(totals.error_count, totals.request_count) : '0.00%'
          }
          delta={{ value: '0%', direction: 'flat', isPositive: null }}
        />
        <KpiTile
          label="Input Tokens"
          value={fmtCount(totals?.input_tokens)}
          delta={{ value: '+3.2%', direction: 'up', isPositive: true }}
        />
        <KpiTile
          label="Output Tokens"
          value={fmtCount(totals?.output_tokens)}
          delta={{ value: '+8.5%', direction: 'up', isPositive: true }}
        />
        <KpiTile
          label="Virtual Cost"
          value={fmtUsd(totals?.virtual_cost_micros)}
          delta={{ value: '+$1.20', direction: 'up', isPositive: false }}
          hint="Cumulative micros / 1e6 USD"
        />
      </div>

      {/* Chart + side cards */}
      <div className="grid grid-cols-1 xl:grid-cols-3 gap-6">
        <Card className="xl:col-span-2 flex flex-col min-h-[360px]">
          <CardHeader
            title="Subscription Quota Forecast"
            subtitle={`5h and 7d utilization over ${range}`}
            action={
              <div className="flex flex-col sm:flex-row sm:items-center gap-2 w-full sm:w-auto min-w-0">
                <div className="flex flex-wrap bg-overlay-2 border border-subtle rounded-sm p-0.5 max-w-full">
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
            }
          />
          <CardBody className="flex-1 p-3 pt-1">
            <div className="w-full h-[300px]" style={{ minWidth: 0 }}>
              {quotaSeries.isLoading ? (
                <div className="h-full flex items-center justify-center text-text-faint text-sm">
                  Loading…
                </div>
              ) : !chartData.rows.length ? (
                <EmptyState title="No data in range" />
              ) : (
                <ResponsiveContainer width="100%" height={300} debounce={150}>
                  <AreaChart
                    data={chartData.rows}
                    margin={{ top: 8, right: 24, bottom: 4, left: 0 }}
                  >
                    <CartesianGrid stroke="var(--color-border)" />
                    <XAxis
                      dataKey="unix"
                      tick={{
                        fill: 'var(--color-text-faint)',
                        fontSize: 10,
                        fontFamily: 'Geist Mono',
                      }}
                      tickFormatter={(val) => {
                        const d = new Date(val * 1000);
                        return range === '7d' || range === '24h'
                          ? `${d.getMonth() + 1}/${d.getDate()} ${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`
                          : `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`;
                      }}
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
                      tickFormatter={(val) => `${val}%`}
                      axisLine={false}
                      tickLine={false}
                      width={40}
                      domain={[0, 100]}
                      allowDataOverflow={false}
                    />
                    <Tooltip
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
                              background: tooltipBg,
                              border: `1px solid ${tooltipBorder}`,
                              borderRadius: 2,
                              color: tooltipText,
                              fontSize: 11,
                              fontFamily: 'Geist Mono Variable, monospace',
                              padding: '6px 10px',
                              boxShadow: '0 4px 12px rgba(0,0,0,0.18)',
                              minWidth: 80,
                            }}
                          >
                            <div
                              style={{ color: tooltipMuted, marginBottom: 4 }}
                            >
                              {label}
                            </div>
                            {payload.map((p, i) => {
                              const [uId, w] = (p.dataKey as string).split('_');
                              const uName =
                                chartData.upstreams.find((u) => u.id === uId)
                                  ?.name ?? uId;
                              return (
                                <div
                                  key={i}
                                  style={{
                                    color: tooltipText,
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
                                          : tooltipText,
                                    }}
                                  >
                                    {uName} ({w})
                                  </span>
                                  <span
                                    style={{
                                      fontVariantNumeric: 'tabular-nums',
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
                    {chartData.upstreams.length > 1 ? (
                      <Legend
                        wrapperStyle={{
                          fontSize: 11,
                          fontFamily: 'Geist Mono',
                          color: 'var(--color-text-muted)',
                        }}
                        content={() => (
                          <div className="flex flex-wrap items-center justify-center gap-4 mt-2">
                            {chartData.upstreams.map((u) => (
                              <div
                                key={u.id}
                                className="flex items-center gap-1.5"
                              >
                                <div
                                  className="w-3 h-0.5"
                                  style={{
                                    backgroundColor: getUpstreamColor(u.id)
                                      .line5h,
                                  }}
                                />
                                <span className="text-[11px] text-text-muted font-mono">
                                  {u.name}
                                </span>
                              </div>
                            ))}
                          </div>
                        )}
                      />
                    ) : null}
                    {chartData.markers.map((m, i) => (
                      <ReferenceLine
                        key={`marker-${i}`}
                        x={chartData.rows.find((r) => r.unix === m.ts)?.ts}
                        stroke={getUpstreamColor(m.upstreamId).line5h}
                        strokeOpacity={0.5}
                        strokeDasharray="3 3"
                      />
                    ))}
                    {chartData.upstreams.map((u) => {
                      const colors = getUpstreamColor(u.id);
                      return [
                        <Area
                          key={`${u.id}_7d`}
                          type="stepAfter"
                          dataKey={`${u.id}_7d`}
                          stroke={colors.line7d}
                          strokeWidth={1.4}
                          strokeDasharray="3 3"
                          fill="none"
                          isAnimationActive={false}
                          connectNulls={false}
                        />,
                        <Area
                          key={`${u.id}_5h`}
                          type="stepAfter"
                          dataKey={`${u.id}_5h`}
                          stroke={colors.line5h}
                          strokeWidth={1.4}
                          fill="none"
                          isAnimationActive={false}
                          connectNulls={false}
                        />,
                      ];
                    })}
                  </AreaChart>
                </ResponsiveContainer>
              )}
            </div>
          </CardBody>
          {/* Upstream compact strips */}
          {quotaLatest.data?.upstreams.length ? (
            <div className="border-t border-subtle p-3 flex flex-col gap-2">
              {quotaLatest.data.upstreams.map((u) => {
                const analysis = quotaAnalysis.data?.upstreams.find(
                  (a) => a.upstream_id === u.upstream_id,
                );
                const w5h = u.windows.find((w) => w.window === '5h');
                const w7d = u.windows.find((w) => w.window === '7d');
                const a5h = analysis?.windows.find((w) => w.window === '5h');

                const formatEta = (secs: number | null | undefined) => {
                  if (secs == null) return '—';
                  if (secs > 86400)
                    return `${Math.floor(secs / 86400)}d ${Math.floor((secs % 86400) / 3600)}h`;
                  if (secs > 3600)
                    return `${Math.floor(secs / 3600)}h ${Math.floor((secs % 3600) / 60)}m`;
                  return `${Math.floor(secs / 60)}m`;
                };

                return (
                  <div
                    key={u.upstream_id}
                    className="flex flex-wrap items-center gap-4 text-xs bg-overlay-1 p-2 rounded-sm border border-subtle"
                  >
                    <div
                      className="font-medium min-w-[120px] truncate"
                      style={{ color: getUpstreamColor(u.upstream_id).line5h }}
                    >
                      {u.upstream_name}
                    </div>
                    <div className="flex items-center gap-1.5">
                      <span className="text-text-faint">5h:</span>
                      <span className="font-mono">
                        {w5h?.utilization != null
                          ? `${(w5h.utilization * 100).toFixed(1)}%`
                          : '—'}
                      </span>
                      <StatusBadge
                        tone={
                          w5h?.state === 'fresh'
                            ? 'ok'
                            : w5h?.state === 'stale'
                              ? 'warn'
                              : 'neutral'
                        }
                        label={w5h?.state ?? 'missing'}
                      />
                    </div>
                    <div className="flex items-center gap-1.5">
                      <span className="text-text-faint">7d:</span>
                      <span className="font-mono">
                        {w7d?.utilization != null
                          ? `${(w7d.utilization * 100).toFixed(1)}%`
                          : '—'}
                      </span>
                      <StatusBadge
                        tone={
                          w7d?.state === 'fresh'
                            ? 'ok'
                            : w7d?.state === 'stale'
                              ? 'warn'
                              : 'neutral'
                        }
                        label={w7d?.state ?? 'missing'}
                      />
                    </div>
                    <div className="flex items-center gap-1.5">
                      <span className="text-text-faint">ETA (Acct):</span>
                      <span className="font-mono">
                        {formatEta(a5h?.actual_account_burn.eta_to_limit_secs)}
                      </span>
                    </div>
                    <div className="flex items-center gap-1.5">
                      <span className="text-text-faint">ETA (Proxy):</span>
                      <span className="font-mono">
                        {formatEta(a5h?.proxy_projected_burn.eta_to_limit_secs)}
                      </span>
                    </div>
                    <div className="flex items-center gap-1.5">
                      <span className="text-text-faint">Need:</span>
                      <span className="font-mono text-amber-400">
                        {a5h?.deficit
                          ? `${a5h.deficit.recommended_multiplier}x`
                          : '—'}
                      </span>
                    </div>
                  </div>
                );
              })}
            </div>
          ) : null}
          {quotaAnalysis.data?.upstreams.some((u) =>
            u.windows.some((w) => w.caveats.length > 0),
          ) ? (
            <div className="mt-3 rounded border border-amber-400/30 bg-amber-400/10 p-3 text-xs text-amber-200">
              <div className="flex items-center gap-1.5 font-semibold mb-1">
                <Info className="w-3 h-3" />
                <span>Analysis Caveats</span>
              </div>
              <ul className="list-disc list-inside space-y-0.5">
                {Array.from(
                  new Set(
                    quotaAnalysis.data.upstreams.flatMap((u) =>
                      u.windows.flatMap((w) => w.caveats),
                    ),
                  ),
                ).map((c, i) => (
                  <li key={i}>{c}</li>
                ))}
              </ul>
            </div>
          ) : null}
        </Card>

        <div className="flex flex-col gap-4">
          <Card>
            <CardHeader
              title="Upstreams"
              subtitle={`${upstreams.data?.upstreams.length ?? 0} total`}
            />
            <CardBody className="space-y-2">
              {upstreams.isLoading ? (
                <div className="text-xs text-text-faint">Loading…</div>
              ) : (
                upstreams.data?.upstreams.slice(0, 5).map((u) => (
                  <div
                    key={u.id}
                    className="flex items-center justify-between text-xs"
                  >
                    <div className="flex items-center gap-2 min-w-0">
                      <span
                        className={cx(
                          'status-dot',
                          u.enabled ? 'ok' : 'neutral',
                        )}
                      />
                      <span className="font-mono truncate">{u.name}</span>
                    </div>
                    <span className="text-text-faint">{u.kind}</span>
                  </div>
                ))
              )}
            </CardBody>
          </Card>

          <Card>
            <CardHeader
              title="Top Principals"
              subtitle={`By cost · ${range}`}
            />
            <CardBody className="space-y-2">
              {principalUsage.isLoading ? (
                <div className="text-xs text-text-faint">Loading…</div>
              ) : topPrincipals.length ? (
                topPrincipals.map((p) => (
                  <div
                    key={p.id}
                    className="flex items-center justify-between text-xs"
                  >
                    <span className="truncate">
                      <span>{p.name}</span>
                      {p.name === p.id && (
                        <span className="ml-1 text-[10px] font-mono text-text-faint">
                          ({p.id.slice(0, 8)}…)
                        </span>
                      )}
                    </span>
                    <span className="font-mono text-text-faint tabular-nums">
                      {fmtUsd(p.cost_micros)}
                    </span>
                  </div>
                ))
              ) : (
                <div className="text-xs text-text-faint">No usage in range</div>
              )}
            </CardBody>
          </Card>
        </div>
      </div>

      {/* Live preview */}
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
              columns={{ cache: false, cost: true, tokens: true }}
              sentinelRef={sentinelRef}
              loadingMore={events.isFetchingNextPage}
              hasMore={events.hasNextPage}
              minWidthClass="min-w-[820px]"
              emptyTitle="No recent requests"
            />
          </div>
        </Card>
      </Section>
    </PageContainer>
  );
}
