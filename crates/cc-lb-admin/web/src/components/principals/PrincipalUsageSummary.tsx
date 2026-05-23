import { DashboardUsageResponse } from '../../lib/api';
import { formatNumber, formatPercent, microsToUsd } from '../../lib/format';
import { MetricCard } from '../primitives/MetricCard';
import { Sparkline } from '../overview/Sparkline';

interface PrincipalUsageSummaryProps {
  usage: DashboardUsageResponse | null;
  isLoading: boolean;
}

export function PrincipalUsageSummary({ usage, isLoading }: PrincipalUsageSummaryProps) {
  if (isLoading || !usage) {
    return (
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        {[1, 2, 3, 4].map((i) => (
          <div key={i} className="h-24 bg-graphite-800 rounded animate-pulse" />
        ))}
      </div>
    );
  }

  if (!usage.observed || usage.series.length === 0) {
    return (
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <MetricCard title="Requests" value="0" />
        <MetricCard title="Tokens" value="0" />
        <MetricCard title="Error Rate" value="0%" />
        <MetricCard title="Cost" value="$0.00" />
      </div>
    );
  }

  let reqs = 0;
  let tokens = 0;
  let errors = 0;
  let cost = 0;
  
  // Aggregate across all models for this principal
  usage.series.forEach(s => {
    s.buckets.forEach(b => {
      reqs += b.request_count;
      tokens += b.input_tokens + b.output_tokens;
      errors += b.error_count;
      cost += b.virtual_cost_micros;
    });
  });

  const errorRate = reqs > 0 ? errors / reqs : 0;

  // For sparklines, we need to aggregate buckets across series by time
  const aggregatedBuckets = new Map<number, { reqs: number, tokens: number, errors: number, cost: number }>();
  
  usage.series.forEach(s => {
    s.buckets.forEach(b => {
      const existing = aggregatedBuckets.get(b.bucket_start_unix_secs) || { reqs: 0, tokens: 0, errors: 0, cost: 0 };
      existing.reqs += b.request_count;
      existing.tokens += b.input_tokens + b.output_tokens;
      existing.errors += b.error_count;
      existing.cost += b.virtual_cost_micros;
      aggregatedBuckets.set(b.bucket_start_unix_secs, existing);
    });
  });

  const sparklineData = Array.from(aggregatedBuckets.entries())
    .sort((a, b) => a[0] - b[0])
    .map(([ts, data]) => ({
      ts,
      ...data,
      errorRate: data.reqs > 0 ? data.errors / data.reqs : 0
    }));

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
      <div className="relative overflow-hidden rounded-lg">
        <MetricCard title="Requests" value={formatNumber(reqs)} />
        <Sparkline data={sparklineData} dataKey="reqs" color="#3b82f6" />
      </div>
      <div className="relative overflow-hidden rounded-lg">
        <MetricCard title="Tokens" value={formatNumber(tokens)} />
        <Sparkline data={sparklineData} dataKey="tokens" color="#8b5cf6" />
      </div>
      <div className="relative overflow-hidden rounded-lg">
        <MetricCard 
          title="Error Rate" 
          value={<span className={errorRate > 0.05 ? 'text-red-400' : ''}>{formatPercent(errorRate)}</span>} 
        />
        <Sparkline data={sparklineData} dataKey="errorRate" color={errorRate > 0.05 ? '#f87171' : '#3b82f6'} />
      </div>
      <div className="relative overflow-hidden rounded-lg">
        <MetricCard title="Cost" value={microsToUsd(cost)} />
        <Sparkline data={sparklineData} dataKey="cost" color="#10b981" />
      </div>
    </div>
  );
}
