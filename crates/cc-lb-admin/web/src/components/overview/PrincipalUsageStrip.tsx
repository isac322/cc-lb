import { Card } from '../primitives/Card';
import { DashboardUsageResponse } from '../../lib/api';
import { formatNumber, formatPercent, microsToUsd } from '../../lib/format';

interface PrincipalUsageStripProps {
  usage: DashboardUsageResponse | null;
  isLoading: boolean;
}

export function PrincipalUsageStrip({ usage, isLoading }: PrincipalUsageStripProps) {
  if (isLoading || !usage) {
    return (
      <div className="flex space-x-4 overflow-x-auto pb-4 snap-x">
        {[1, 2, 3].map((i) => (
          <Card key={i} className="p-4 min-w-[280px] flex-shrink-0 snap-start">
            <div className="h-5 w-32 bg-graphite-800 rounded animate-pulse mb-4" />
            <div className="space-y-2">
              <div className="h-4 w-full bg-graphite-800 rounded animate-pulse" />
              <div className="h-4 w-full bg-graphite-800 rounded animate-pulse" />
              <div className="h-4 w-full bg-graphite-800 rounded animate-pulse" />
            </div>
          </Card>
        ))}
      </div>
    );
  }

  if (!usage.observed || usage.series.length === 0) {
    return null;
  }

  return (
    <div className="flex space-x-4 overflow-x-auto pb-4 snap-x scrollbar-thin scrollbar-thumb-graphite-700 scrollbar-track-transparent">
      {usage.series.map((s) => {
        // Aggregate totals for this principal
        let reqs = 0;
        let tokens = 0;
        let errors = 0;
        let cost = 0;
        
        s.buckets.forEach(b => {
          reqs += b.request_count;
          tokens += b.input_tokens + b.output_tokens;
          errors += b.error_count;
          cost += b.virtual_cost_micros;
        });

        const errorRate = reqs > 0 ? errors / reqs : 0;

        return (
          <Card key={s.key} className="p-4 min-w-[280px] flex-shrink-0 snap-start border-l-4 border-l-blue-500">
            <div className="font-mono text-sm text-graphite-50 mb-3 truncate" title={s.key}>
              {s.key}
            </div>
            <div className="grid grid-cols-2 gap-y-2 gap-x-4 text-sm">
              <div>
                <div className="text-graphite-400 text-xs">Requests</div>
                <div className="font-medium text-graphite-200">{formatNumber(reqs)}</div>
              </div>
              <div>
                <div className="text-graphite-400 text-xs">Tokens</div>
                <div className="font-medium text-graphite-200">{formatNumber(tokens)}</div>
              </div>
              <div>
                <div className="text-graphite-400 text-xs">Error Rate</div>
                <div className={`font-medium ${errorRate > 0.05 ? 'text-red-400' : 'text-graphite-200'}`}>
                  {formatPercent(errorRate)}
                </div>
              </div>
              <div>
                <div className="text-graphite-400 text-xs">Cost</div>
                <div className="font-medium text-graphite-200">{microsToUsd(cost)}</div>
              </div>
            </div>
          </Card>
        );
      })}
    </div>
  );
}
