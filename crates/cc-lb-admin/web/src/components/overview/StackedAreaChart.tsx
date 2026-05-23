import { Activity } from 'lucide-react';
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
import { EmptyState } from '../primitives/EmptyState';

interface StackedAreaChartProps {
  usage: DashboardUsageResponse | null;
  isLoading: boolean;
}

const COLORS = [
  'var(--color-cyan-500)',
  'var(--color-graphite-500)',
  'var(--color-graphite-600)',
  'var(--color-graphite-700)',
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
        <EmptyState
          title="Waiting for first request"
          message={`No traffic in last ${usage.range}`}
          icon={Activity}
        />
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
              stroke="var(--color-graphite-800)"
              vertical={false}
            />
            <XAxis
              dataKey="time"
              stroke="var(--color-graphite-500)"
              fontSize={12}
              tickLine={false}
              axisLine={false}
              minTickGap={30}
            />
            <YAxis
              stroke="var(--color-graphite-500)"
              fontSize={12}
              tickLine={false}
              axisLine={false}
              tickFormatter={(val) => formatNumber(val)}
            />
            <Tooltip
              contentStyle={{
                backgroundColor: 'var(--color-graphite-900)',
                borderColor: 'var(--color-graphite-800)',
                color: 'var(--color-graphite-50)',
              }}
              itemStyle={{ color: 'var(--color-graphite-50)' }}
              labelStyle={{
                color: 'var(--color-graphite-400)',
                marginBottom: '4px',
              }}
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
