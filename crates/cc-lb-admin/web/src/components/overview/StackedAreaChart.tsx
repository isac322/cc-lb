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
import type { DashboardUsageResponse } from '../../lib/api';
import { formatNumber } from '../../lib/format';
import { Card } from '../primitives/Card';

interface StackedAreaChartProps {
  usage: DashboardUsageResponse | null;
  isLoading: boolean;
}

const COLORS = [
  '#3b82f6',
  '#10b981',
  '#f59e0b',
  '#ef4444',
  '#8b5cf6',
  '#ec4899',
  '#06b6d4',
  '#84cc16',
  '#f97316',
  '#6366f1',
];

export function StackedAreaChart({ usage, isLoading }: StackedAreaChartProps) {
  if (isLoading || !usage) {
    return (
      <Card className="p-6 h-80 flex items-center justify-center">
        <div className="animate-pulse flex flex-col items-center">
          <div className="h-4 w-32 bg-graphite-800 rounded mb-4" />
          <div className="h-48 w-full max-w-2xl bg-graphite-800/50 rounded" />
        </div>
      </Card>
    );
  }

  if (!usage.observed || usage.series.length === 0) {
    return (
      <Card className="p-6 h-80 flex items-center justify-center">
        <div className="text-center">
          <div className="text-graphite-400 mb-2">No data available</div>
          <div className="text-sm text-graphite-500">
            No traffic in last {usage.range}
          </div>
        </div>
      </Card>
    );
  }

  // Transform series data into recharts format
  // We need an array of objects where each object represents a time bucket
  // and has keys for each series (model)
  const timeBuckets = usage.series[0].buckets.map(
    (b) => b.bucket_start_unix_secs,
  );

  const chartData = timeBuckets.map((ts, i) => {
    const dataPoint: Record<string, string | number> = {
      time: new Date(ts * 1000).toLocaleTimeString([], {
        hour: '2-digit',
        minute: '2-digit',
      }),
      ts,
    };

    usage.series.forEach((s) => {
      dataPoint[s.key] = s.buckets[i].request_count;
    });

    return dataPoint;
  });

  return (
    <Card className="p-6 h-80 flex flex-col">
      <h3 className="text-sm font-medium text-graphite-50 mb-4">
        Requests by Model
      </h3>
      <div className="flex-1 min-h-0">
        <ResponsiveContainer width="100%" height="100%">
          <AreaChart
            data={chartData}
            margin={{ top: 10, right: 10, left: 0, bottom: 0 }}
          >
            <CartesianGrid
              strokeDasharray="3 3"
              stroke="#374151"
              vertical={false}
            />
            <XAxis
              dataKey="time"
              stroke="#9ca3af"
              fontSize={12}
              tickLine={false}
              axisLine={false}
              minTickGap={30}
            />
            <YAxis
              stroke="#9ca3af"
              fontSize={12}
              tickLine={false}
              axisLine={false}
              tickFormatter={(val) => formatNumber(val)}
            />
            <Tooltip
              contentStyle={{
                backgroundColor: '#1f2937',
                borderColor: '#374151',
                color: '#f9fafb',
              }}
              itemStyle={{ color: '#f9fafb' }}
              labelStyle={{ color: '#9ca3af', marginBottom: '4px' }}
            />
            <Legend
              iconType="circle"
              wrapperStyle={{ fontSize: '12px', paddingTop: '10px' }}
            />
            {usage.series.map((s, i) => (
              <Area
                key={s.key}
                type="monotone"
                dataKey={s.key}
                stackId="1"
                stroke={COLORS[i % COLORS.length]}
                fill={COLORS[i % COLORS.length]}
                fillOpacity={0.6}
              />
            ))}
          </AreaChart>
        </ResponsiveContainer>
      </div>
    </Card>
  );
}
