import type { DashboardSummaryResponse } from '../../lib/api';
import { formatNumber, formatPercent, microsToUsd } from '../../lib/format';
import { Card } from '../primitives/Card';
import { Sparkline } from './Sparkline';

interface KpiCardGridProps {
  summary: DashboardSummaryResponse | null;
  isLoading: boolean;
}

export function KpiCardGrid({ summary, isLoading }: KpiCardGridProps) {
  if (isLoading || !summary) {
    return (
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        {[1, 2, 3, 4].map((i) => (
          <Card key={i} className="p-4 relative overflow-hidden h-24">
            <div className="h-4 w-24 bg-graphite-800 rounded animate-pulse mb-2" />
            <div className="h-8 w-32 bg-graphite-800 rounded animate-pulse" />
          </Card>
        ))}
      </div>
    );
  }

  const { totals, sparkline, observed, range } = summary;

  if (!observed) {
    return (
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        {[
          { label: 'Requests' },
          { label: 'Tokens' },
          { label: 'Virtual Cost' },
          { label: 'Error Rate' },
        ].map((kpi, i) => (
          <Card key={i} className="p-4 relative overflow-hidden">
            <div className="text-sm text-graphite-400 font-medium">
              {kpi.label}
            </div>
            <div className="text-2xl font-semibold text-graphite-50 mt-1">
              —
            </div>
            <div className="text-xs text-graphite-300 mt-1">
              No traffic in last {range}
            </div>
          </Card>
        ))}
      </div>
    );
  }

  const cacheTotal =
    totals.cache_creation_input_tokens + totals.cache_read_input_tokens;
  const cacheBaseline = totals.input_tokens + cacheTotal;
  const cacheHitRate = cacheBaseline > 0 ? totals.cache_read_input_tokens / cacheBaseline : 0;
  const avgProxyStack =
    totals.avg_proxy_setup_ms + totals.avg_shape_ms + totals.avg_sign_ms;
  const avgUpstream = totals.avg_upstream_ttfb_ms + totals.avg_upstream_body_ms;

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <Card className="p-4 relative overflow-hidden">
          <div className="relative z-10">
            <div className="text-sm text-graphite-400 font-medium">Requests</div>
            <div className="text-2xl font-semibold text-graphite-50 mt-1">
              {formatNumber(totals.request_count)}
            </div>
          </div>
          <Sparkline
            data={sparkline.buckets}
            dataKey="request_count"
            color="#3b82f6"
          />
        </Card>

        <Card className="p-4 relative overflow-hidden">
          <div className="relative z-10">
            <div className="text-sm text-graphite-400 font-medium">Tokens</div>
            <div className="text-2xl font-semibold text-graphite-50 mt-1">
              {formatNumber(totals.input_tokens + totals.output_tokens)}
            </div>
          </div>
          <Sparkline
            data={sparkline.buckets.map((b) => ({
              ...b,
              total_tokens: b.input_tokens + b.output_tokens,
            }))}
            dataKey="total_tokens"
            color="#10b981"
          />
        </Card>

        <Card className="p-4 relative overflow-hidden">
          <div className="relative z-10">
            <div className="text-sm text-graphite-400 font-medium">
              Virtual Cost
            </div>
            <div className="text-2xl font-semibold text-graphite-50 mt-1">
              {microsToUsd(totals.virtual_cost_micros)}
            </div>
          </div>
          <Sparkline
            data={sparkline.buckets}
            dataKey="virtual_cost_micros"
            color="#f59e0b"
          />
        </Card>

        <Card className="p-4 relative overflow-hidden">
          <div className="relative z-10">
            <div className="text-sm text-graphite-400 font-medium">
              Error Rate
            </div>
            <div className="text-2xl font-semibold text-graphite-50 mt-1">
              {formatPercent(totals.error_rate)}
            </div>
          </div>
          <Sparkline
            data={sparkline.buckets}
            dataKey="error_count"
            color="#ef4444"
          />
        </Card>
      </div>

      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <Card className="p-4">
          <div className="text-xs text-graphite-400 font-medium uppercase tracking-wider">
            Cache Hit Rate
          </div>
          <div className="text-xl font-semibold text-graphite-50 mt-1">
            {formatPercent(cacheHitRate)}
          </div>
          <div className="text-[11px] text-graphite-500 mt-1 font-mono">
            {formatNumber(totals.cache_read_input_tokens)} read /{' '}
            {formatNumber(totals.cache_creation_input_tokens)} create
          </div>
        </Card>

        <Card className="p-4">
          <div className="text-xs text-graphite-400 font-medium uppercase tracking-wider">
            Avg Latency
          </div>
          <div className="text-xl font-semibold text-graphite-50 mt-1">
            {totals.avg_latency_ms > 0
              ? `${Math.round(totals.avg_latency_ms)}ms`
              : '—'}
          </div>
          <div className="text-[11px] text-graphite-500 mt-1">total wall time</div>
        </Card>

        <Card className="p-4">
          <div className="text-xs text-graphite-400 font-medium uppercase tracking-wider">
            Avg Proxy Stack
          </div>
          <div className="text-xl font-semibold text-cyan-400 mt-1">
            {avgProxyStack > 0 ? `${avgProxyStack.toFixed(1)}ms` : '—'}
          </div>
          <div className="text-[11px] text-graphite-500 mt-1 font-mono">
            setup {fmtMs(totals.avg_proxy_setup_ms)} · shape{' '}
            {fmtMs(totals.avg_shape_ms)} · sign {fmtMs(totals.avg_sign_ms)}
          </div>
        </Card>

        <Card className="p-4">
          <div className="text-xs text-graphite-400 font-medium uppercase tracking-wider">
            Avg Upstream
          </div>
          <div className="text-xl font-semibold text-purple-400 mt-1">
            {avgUpstream > 0 ? `${Math.round(avgUpstream)}ms` : '—'}
          </div>
          <div className="text-[11px] text-graphite-500 mt-1 font-mono">
            ttfb {fmtMs(totals.avg_upstream_ttfb_ms)} · body{' '}
            {fmtMs(totals.avg_upstream_body_ms)}
          </div>
        </Card>
      </div>
    </div>
  );
}

function fmtMs(value: number): string {
  if (!value || value <= 0) return '—';
  return value < 10 ? `${value.toFixed(1)}ms` : `${Math.round(value)}ms`;
}
