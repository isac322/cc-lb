import { Toggle as BaseToggle } from '@base-ui/react/toggle';
import { ToggleGroup as BaseToggleGroup } from '@base-ui/react/toggle-group';
import { useId, useMemo } from 'react';
import {
  Area,
  AreaChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import type { DashboardUsageResponse } from '../../lib/api';
import { getWindowColor } from '../../lib/colors';
import { fmtUsd, sumTokens } from '../../lib/format';
import { Card, CardBody, CardHeader, Skeleton } from '../ui/primitives';

type Props = {
  data: DashboardUsageResponse | undefined;
  isLoading: boolean;
  range: '24h' | '7d';
  onRangeChange: (r: '24h' | '7d') => void;
  metric: 'tokens' | 'cost';
  onMetricChange: (m: 'tokens' | 'cost') => void;
};

export function ApiUsageCard({
  data,
  isLoading,
  range,
  onRangeChange,
  metric,
  onMetricChange,
}: Props) {
  const chartId = useId();

  const chartData = useMemo(() => {
    if (!data?.series || data.series.length === 0) return [];

    const bucketsByTs = new Map<number, Record<string, number>>();

    for (const series of data.series) {
      const model = series.key;
      for (const bucket of series.buckets) {
        const ts = bucket.bucket_start_unix_secs;
        if (!bucketsByTs.has(ts)) {
          bucketsByTs.set(ts, { ts });
        }
        const row = bucketsByTs.get(ts)!;
        if (metric === 'tokens') {
          row[model] = sumTokens(bucket);
        } else {
          row[model] = bucket.virtual_cost_micros / 1_000_000;
        }
      }
    }

    return Array.from(bucketsByTs.values()).sort((a, b) => a.ts - b.ts);
  }, [data, metric]);

  const models = useMemo(() => {
    if (!data?.series) return [];
    return data.series.map((s) => s.key);
  }, [data]);

  const formatXAxis = (ts: number) => {
    const d = new Date(ts * 1000);
    if (range === '24h') {
      return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    }
    return d.toLocaleDateString([], { month: 'short', day: 'numeric' });
  };

  const formatYAxis = (val: number) => {
    if (metric === 'cost') {
      return fmtUsd(val * 1_000_000);
    }
    if (val >= 1_000_000) return `${(val / 1_000_000).toFixed(1)}M`;
    if (val >= 1_000) return `${(val / 1_000).toFixed(1)}K`;
    return val.toString();
  };

  const formatTooltip = (val: number) => {
    if (metric === 'cost') {
      return fmtUsd(val * 1_000_000);
    }
    return val.toLocaleString();
  };

  const toggleClass =
    'rounded px-2 py-1 text-xs font-medium transition-colors text-zinc-500 hover:text-zinc-900 dark:text-zinc-400 dark:hover:text-zinc-100 data-[pressed]:bg-white data-[pressed]:text-zinc-900 data-[pressed]:shadow-sm dark:data-[pressed]:bg-zinc-700 dark:data-[pressed]:text-zinc-100';

  const action = (
    <div className="flex items-center gap-2">
      <BaseToggleGroup
        aria-label="Time range"
        className="flex items-center rounded-md bg-zinc-100 p-0.5 dark:bg-zinc-800"
        onValueChange={(values) => {
          const first = values[0];
          if (first === '24h' || first === '7d') onRangeChange(first);
        }}
        value={[range]}
      >
        <BaseToggle className={toggleClass} value="24h">
          24h
        </BaseToggle>
        <BaseToggle className={toggleClass} value="7d">
          7d
        </BaseToggle>
      </BaseToggleGroup>
      <BaseToggleGroup
        aria-label="Metric"
        className="flex items-center rounded-md bg-zinc-100 p-0.5 dark:bg-zinc-800"
        onValueChange={(values) => {
          const first = values[0];
          if (first === 'tokens' || first === 'cost') onMetricChange(first);
        }}
        value={[metric]}
      >
        <BaseToggle className={toggleClass} value="tokens">
          Tokens
        </BaseToggle>
        <BaseToggle className={toggleClass} value="cost">
          Cost
        </BaseToggle>
      </BaseToggleGroup>
    </div>
  );

  return (
    <Card data-testid="api-usage-card">
      <CardHeader
        title="API Usage"
        subtitle="Token + cost breakdown by model"
        action={action}
      />
      <CardBody>
        <div className="flex flex-col gap-4">
          <div className="h-64 w-full">
            {isLoading ? (
              <Skeleton className="h-full w-full" />
            ) : chartData.length === 0 ? (
              <div className="flex h-full items-center justify-center text-sm text-zinc-500">
                No usage in selected range
              </div>
            ) : (
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart
                  data={chartData}
                  margin={{ top: 10, right: 10, left: 0, bottom: 0 }}
                >
                  <defs>
                    {models.map((model) => {
                      const color = getWindowColor(model).fill;
                      return (
                        <linearGradient
                          key={`${chartId}-${model}`}
                          id={`${chartId}-${model}`}
                          x1="0"
                          y1="0"
                          x2="0"
                          y2="1"
                        >
                          <stop
                            offset="5%"
                            stopColor={color}
                            stopOpacity={0.3}
                          />
                          <stop
                            offset="95%"
                            stopColor={color}
                            stopOpacity={0}
                          />
                        </linearGradient>
                      );
                    })}
                  </defs>
                  <CartesianGrid
                    strokeDasharray="3 3"
                    vertical={false}
                    stroke="currentColor"
                    className="text-zinc-200 dark:text-zinc-800"
                  />
                  <XAxis
                    dataKey="ts"
                    tickFormatter={formatXAxis}
                    tick={{ fontSize: 12 }}
                    tickLine={false}
                    axisLine={false}
                    minTickGap={30}
                    stroke="currentColor"
                    className="text-zinc-500"
                  />
                  <YAxis
                    tickFormatter={formatYAxis}
                    tick={{ fontSize: 12 }}
                    tickLine={false}
                    axisLine={false}
                    width={60}
                    stroke="currentColor"
                    className="text-zinc-500"
                  />
                  <Tooltip
                    labelFormatter={(label) => formatXAxis(label as number)}
                    formatter={(value: unknown, name: unknown) => [
                      formatTooltip(Number(value ?? 0)),
                      String(name),
                    ]}
                    contentStyle={{
                      backgroundColor: 'var(--bg-popover, #fff)',
                      borderColor: 'var(--border, #e4e4e7)',
                      borderRadius: '0.5rem',
                      fontSize: '0.875rem',
                    }}
                  />
                  {models.map((model) => {
                    const color = getWindowColor(model).fill;
                    return (
                      <Area
                        key={model}
                        type="monotone"
                        dataKey={model}
                        stackId="1"
                        stroke={color}
                        fill={`url(#${chartId}-${model})`}
                        strokeWidth={2}
                        isAnimationActive={false}
                      />
                    );
                  })}
                </AreaChart>
              </ResponsiveContainer>
            )}
          </div>
          <div
            data-testid="api-usage-legend-slot"
            className="flex flex-wrap items-center gap-4 text-sm min-h-[28px]"
          >
            {!isLoading && chartData.length > 0
              ? models.map((model) => {
                  const color = getWindowColor(model).fill;
                  const lastBucket = chartData[chartData.length - 1];
                  const val = lastBucket ? lastBucket[model] || 0 : 0;
                  return (
                    <div key={model} className="flex items-center gap-2">
                      <div
                        className="h-3 w-3 rounded-full"
                        style={{ backgroundColor: color }}
                      />
                      <span className="font-medium text-zinc-900 dark:text-zinc-100">
                        {model}
                      </span>
                      <span className="text-zinc-500">
                        {formatTooltip(val)}
                      </span>
                    </div>
                  );
                })
              : null}
          </div>
        </div>
      </CardBody>
    </Card>
  );
}
