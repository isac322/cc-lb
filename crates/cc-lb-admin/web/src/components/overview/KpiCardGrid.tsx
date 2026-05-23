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

  return (
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
  );
}
