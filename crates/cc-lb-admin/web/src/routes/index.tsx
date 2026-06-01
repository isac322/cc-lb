import { createFileRoute } from '@tanstack/react-router';
import { useMemo, useState } from 'react';
import {
  Area,
  AreaChart,
  CartesianGrid,
  Legend,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import { ArrowUpRight } from 'lucide-react';
import {
  Card,
  CardBody,
  CardHeader,
  EmptyState,
  KpiTile,
  PageContainer,
  Section,
  SkeletonRow,
  cx,
} from '../components/ui/primitives';
import { useRecentEvents, useSummary, useUsage, useUpstreams, usePrincipals } from '../lib/queries';
import { eventTime } from '../lib/api';
import { useTheme } from '../lib/theme';

export const Route = createFileRoute('/')({
  component: OverviewPage,
});

const RANGES = ['1h', '6h', '24h', '7d'] as const;
type Range = (typeof RANGES)[number];

const GROUPS = [
  { id: 'none', label: 'Total' },
  { id: 'model', label: 'By Model' },
  { id: 'principal', label: 'By Principal' },
  { id: 'upstream', label: 'By Upstream' },
] as const;
type Group = (typeof GROUPS)[number]['id'];

const COLORS = ['#00d4ff', '#a78bfa', '#34d399', '#fbbf24', '#f472b6', '#60a5fa', '#fb923c', '#22d3ee'];

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
  const { effective } = useTheme();
  const isLight = effective === 'light';
  const tooltipBg = isLight ? '#fafafa' : '#0a0a0a';
  const tooltipBorder = isLight ? 'rgba(0,0,0,0.14)' : 'rgba(255,255,255,0.14)';
  const tooltipText = isLight ? '#0a0a0a' : '#ededed';
  const tooltipMuted = isLight ? '#4b5563' : '#9ca3af';
  const [range, setRange] = useState<Range>('1h');
  const [group, setGroup] = useState<Group>('none');
  const stepFor = (r: Range) => (r === '7d' || r === '24h' ? 'hour' : 'minute');

  const summary = useSummary(range);
  const usage = useUsage(range, stepFor(range), group);
  const events = useRecentEvents({ limit: '5' });
  const upstreams = useUpstreams();
  const principals = usePrincipals();

  const chartData = useMemo(() => {
    if (!usage.data) return { keys: [] as string[], rows: [] as Record<string, number | string>[] };
    const series = usage.data.series;
    if (!series.length) return { keys: [], rows: [] };
    const keys = series.map((s) => s.key);
    const length = series[0]!.buckets.length;
    const rows: Record<string, number | string>[] = [];
    for (let i = 0; i < length; i++) {
      const ts = series[0]!.buckets[i]!.bucket_start_unix_secs;
      const date = new Date(ts * 1000);
      const label = stepFor(range) === 'minute' ? date.toTimeString().slice(0, 5) : `${date.getUTCMonth() + 1}/${date.getUTCDate()} ${date.getUTCHours()}h`;
      const row: Record<string, number | string> = { ts: label };
      for (const s of series) row[s.key] = s.buckets[i]?.request_count ?? 0;
      rows.push(row);
    }
    return { keys, rows };
  }, [usage.data, range]);

  const totals = summary.data?.totals;
  const topPrincipals = useMemo(() => {
    if (!principals.data) return [] as { name: string; cost: number }[];
    return [...principals.data.principals]
      .filter((p) => p.enabled)
      .map((p) => ({ name: p.name, cost: Math.floor((p.id.length * 13 + Date.now() % 1000) * 100) / 100 }))
      .sort((a, b) => b.cost - a.cost)
      .slice(0, 5);
  }, [principals.data]);

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
          value={totals ? fmtPct(totals.error_count, totals.request_count) : '0.00%'}
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
            title="Request Volume"
            subtitle={group === 'none' ? `Total requests over ${range}` : `Stacked by ${group} over ${range}`}
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
                        r === range ? 'bg-overlay-6 text-text' : 'text-text-faint hover:text-text',
                      )}
                    >
                      {r}
                    </button>
                  ))}
                </div>
                <div className="flex flex-wrap bg-overlay-2 border border-subtle rounded-sm p-0.5 max-w-full">
                  {GROUPS.map((g) => (
                    <button
                      key={g.id}
                      type="button"
                      onClick={() => setGroup(g.id)}
                      className={cx(
                        'px-2.5 h-7 text-xs rounded-sm transition-colors whitespace-nowrap',
                        g.id === group ? 'bg-overlay-6 text-text' : 'text-text-faint hover:text-text',
                      )}
                    >
                      {g.label}
                    </button>
                  ))}
                </div>
              </div>
            }
          />
          <CardBody className="flex-1 p-3 pt-1">
            <div className="w-full h-[300px]" style={{ minWidth: 0 }}>
              {usage.isLoading ? (
                <div className="h-full flex items-center justify-center text-text-faint text-sm">Loading…</div>
              ) : !chartData.rows.length ? (
                <EmptyState title="No data in range" />
              ) : (
                <ResponsiveContainer width="100%" height={300} debounce={150}>
                  <AreaChart data={chartData.rows} margin={{ top: 8, right: 24, bottom: 4, left: 0 }}>
                    <defs>
                      {chartData.keys.map((k, i) => (
                        <linearGradient key={k} id={`area-${i}`} x1="0" y1="0" x2="0" y2="1">
                          <stop offset="0%" stopColor={COLORS[i % COLORS.length]} stopOpacity={0.5} />
                          <stop offset="100%" stopColor={COLORS[i % COLORS.length]} stopOpacity={0} />
                        </linearGradient>
                      ))}
                    </defs>
                    <CartesianGrid stroke="var(--color-border)" />
                    <XAxis
                      dataKey="ts"
                      tick={{ fill: 'var(--color-text-faint)', fontSize: 10, fontFamily: 'Geist Mono' }}
                      axisLine={false}
                      tickLine={false}
                      minTickGap={40}
                    />
                    <YAxis
                      tick={{ fill: 'var(--color-text-faint)', fontSize: 10, fontFamily: 'Geist Mono' }}
                      axisLine={false}
                      tickLine={false}
                      width={36}
                      domain={[0, 'auto']}
                      allowDataOverflow={false}
                    />
                    <Tooltip
                      cursor={{ stroke: 'var(--color-accent)', strokeWidth: 1, strokeOpacity: 0.3 }}
                      content={({ active, payload, label }) => {
                        if (!active || !payload || !payload.length) return null;
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
                            <div style={{ color: tooltipMuted, marginBottom: 4 }}>{label}</div>
                            {payload.map((p, i) => (
                              <div key={i} style={{ color: tooltipText, padding: '1px 0', display: 'flex', justifyContent: 'space-between', gap: 8 }}>
                                <span style={{ color: typeof p.color === 'string' ? p.color : tooltipText }}>{p.name}</span>
                                <span style={{ fontVariantNumeric: 'tabular-nums' }}>{p.value as number}</span>
                              </div>
                            ))}
                          </div>
                        );
                      }}
                    />
                    {chartData.keys.length > 1 ? (
                      <Legend wrapperStyle={{ fontSize: 11, fontFamily: 'Geist Mono', color: 'var(--color-text-muted)' }} />
                    ) : null}
                    {chartData.keys.map((k, i) => (
                      <Area
                        key={k}
                        type="monotone"
                        dataKey={k}
                        stackId={chartData.keys.length > 1 ? 'a' : undefined}
                        stroke={COLORS[i % COLORS.length]}
                        strokeWidth={1.4}
                        fill={`url(#area-${i})`}
                        isAnimationActive={false}
                      />
                    ))}
                  </AreaChart>
                </ResponsiveContainer>
              )}
            </div>
          </CardBody>
        </Card>

        <div className="flex flex-col gap-4">
          <Card>
            <CardHeader title="Upstreams" subtitle={`${upstreams.data?.upstreams.length ?? 0} total`} />
            <CardBody className="space-y-2">
              {upstreams.isLoading ? (
                <div className="text-xs text-text-faint">Loading…</div>
              ) : (
                upstreams.data?.upstreams.slice(0, 5).map((u) => (
                  <div key={u.id} className="flex items-center justify-between text-xs">
                    <div className="flex items-center gap-2 min-w-0">
                      <span className={cx('status-dot', u.enabled ? 'ok' : 'neutral')} />
                      <span className="font-mono truncate">{u.name}</span>
                    </div>
                    <span className="text-text-faint">{u.kind}</span>
                  </div>
                ))
              )}
            </CardBody>
          </Card>

          <Card>
            <CardHeader title="Top Principals" subtitle="By cost (mock)" />
            <CardBody className="space-y-2">
              {topPrincipals.length ? (
                topPrincipals.map((p) => (
                  <div key={p.name} className="flex items-center justify-between text-xs">
                    <span className="truncate">{p.name}</span>
                    <span className="font-mono text-text-faint tabular-nums">${p.cost.toFixed(2)}</span>
                  </div>
                ))
              ) : (
                <div className="text-xs text-text-faint">No active principals</div>
              )}
            </CardBody>
          </Card>
        </div>
      </div>

      {/* Live preview */}
      <Section
        title="Recent Requests"
        subtitle="Live preview — full view on Logs page"
        action={
          <a href="/logs" className="text-xs text-accent hover:underline inline-flex items-center gap-1">
            See all <ArrowUpRight className="w-3 h-3" />
          </a>
        }
      >
        <Card className="min-w-0">
          <div className="relative">
            <div className="overflow-x-auto scroll-fade-right">
              <table className="min-w-[820px] w-full font-mono text-xs">
                <thead className="bg-overlay-1 border-b border-subtle">
                  <tr className="text-text-faint text-[10px] uppercase tracking-wider">
                    <th className="text-left px-3 py-2 whitespace-nowrap">Timestamp</th>
                    <th className="text-left px-3 py-2 whitespace-nowrap">Principal</th>
                    <th className="text-left px-3 py-2 whitespace-nowrap">Upstream</th>
                    <th className="text-left px-3 py-2 whitespace-nowrap">Model</th>
                    <th className="text-right px-3 py-2 whitespace-nowrap">Status</th>
                    <th className="text-right px-3 py-2 whitespace-nowrap">Latency</th>
                    <th className="text-right px-3 py-2 whitespace-nowrap">Tokens I/O</th>
                    <th className="text-right px-3 py-2 whitespace-nowrap">Cost</th>
                  </tr>
                </thead>
                <tbody>
                  {events.isLoading ? (
                    Array.from({ length: 5 }).map((_, i) => <SkeletonRow key={i} cols={8} />)
                  ) : events.data?.events.length ? (
                    events.data.events.map((e) => (
                      <tr key={e.request_id} className="border-b border-subtle/40 hover:bg-overlay-1">
                        <td className="px-3 py-2 text-text-faint whitespace-nowrap">{eventTime(e)?.toISOString().slice(11, 19) ?? '—'} UTC</td>
                        <td className="px-3 py-2 whitespace-nowrap">{e.principal_id ?? '—'}</td>
                        <td className="px-3 py-2 whitespace-nowrap">{e.upstream ?? '—'}</td>
                        <td className="px-3 py-2 text-text-faint truncate max-w-[260px]">{e.model ?? '—'}</td>
                        <td className={cx('px-3 py-2 text-right tabular-nums whitespace-nowrap', e.status >= 500 ? 'text-red-400' : e.status >= 400 ? 'text-amber-400' : 'text-green-400')}>{e.status}</td>
                        <td className="px-3 py-2 text-right tabular-nums whitespace-nowrap">{e.duration_ms}ms</td>
                        <td className="px-3 py-2 text-right tabular-nums whitespace-nowrap">{e.input_tokens ?? 0} / {e.output_tokens ?? 0}</td>
                        <td className="px-3 py-2 text-right tabular-nums whitespace-nowrap">{fmtUsd(e.cost_usd_micros)}</td>
                      </tr>
                    ))
                  ) : (
                    <tr><td colSpan={8} className="px-3 py-8 text-center text-text-faint text-xs">No recent requests</td></tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </Card>
      </Section>
    </PageContainer>
  );
}
