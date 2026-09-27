import { useMemo } from 'react';
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
import { getWindowColor, SERIES_FILL_OPACITY } from '../../lib/colors';
import { fmtUsd, sumTokens } from '../../lib/format';
import { CHART_AXIS, CHART_CURSOR, CHART_GRID } from '../ui/charts';
import {
  EmptyState,
  Section,
  SegmentedControl,
  Skeleton,
} from '../ui/primitives';

// Model families reuse the quota series tokens so the same model reads in
// the same hue everywhere; anything else cycles through the remaining ones.
const MODEL_FAMILY_SERIES: [string, string][] = [
  ['opus', '7d_opus'],
  ['sonnet', '7d_sonnet'],
  ['haiku', '5h'],
  ['fable', '7d_fable'],
];
const FALLBACK_SERIES = ['7d', '5h', '7d_fable', '7d_sonnet', '7d_opus'];

function modelSeriesColor(model: string, index: number): string {
  const lower = model.toLowerCase();
  const family = MODEL_FAMILY_SERIES.find(([name]) => lower.includes(name));
  return getWindowColor(
    family?.[1] ?? FALLBACK_SERIES[index % FALLBACK_SERIES.length]!,
  ).stroke;
}

const RANGE_OPTIONS = [
  { value: '24h', label: '24h' },
  { value: '7d', label: '7d' },
] as const;
const METRIC_OPTIONS = [
  { value: 'tokens', label: 'Tokens' },
  { value: 'cost', label: 'Cost' },
] as const;

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
  const showLoading = data === undefined && isLoading;

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

  const action = (
    <div className="flex flex-wrap items-center gap-2">
      <SegmentedControl
        ariaLabel="Time range"
        value={range}
        onChange={onRangeChange}
        options={RANGE_OPTIONS}
      />
      <SegmentedControl
        ariaLabel="Metric"
        value={metric}
        onChange={onMetricChange}
        options={METRIC_OPTIONS}
      />
    </div>
  );

  return (
    <div data-testid="api-usage-card">
      <Section
        title="API usage"
        subtitle="Token and cost breakdown by model"
        action={action}
      >
        <div className="flex flex-col gap-3">
          <div
            data-testid="api-usage-legend-slot"
            className="flex flex-wrap items-center justify-end gap-x-4 gap-y-1 text-caption min-h-[28px]"
          >
            {!showLoading && chartData.length > 0
              ? models.map((model, index) => {
                  const lastBucket = chartData[chartData.length - 1];
                  const val = lastBucket ? lastBucket[model] || 0 : 0;
                  return (
                    <div key={model} className="flex items-center gap-1.5">
                      <span
                        aria-hidden="true"
                        className="h-0.5 w-2.5 shrink-0 rounded-xs"
                        style={{
                          backgroundColor: modelSeriesColor(model, index),
                        }}
                      />
                      <span className="text-text-muted">{model}</span>
                      <span className="tabular-nums text-text">
                        {formatTooltip(val)}
                      </span>
                    </div>
                  );
                })
              : null}
          </div>
          {!showLoading && chartData.length === 0 ? (
            <EmptyState title="No usage in selected range" />
          ) : (
            <div className="h-64 w-full">
              {showLoading ? (
                <Skeleton className="h-full w-full" />
              ) : (
                <ResponsiveContainer width="100%" height="100%">
                  <AreaChart
                    data={chartData}
                    margin={{ top: 10, right: 10, left: 0, bottom: 0 }}
                  >
                    <CartesianGrid {...CHART_GRID} />
                    <XAxis
                      {...CHART_AXIS}
                      dataKey="ts"
                      tickFormatter={formatXAxis}
                      minTickGap={60}
                    />
                    <YAxis
                      {...CHART_AXIS}
                      tickFormatter={formatYAxis}
                      tickCount={3}
                      width={60}
                    />
                    <Tooltip
                      labelFormatter={(label) => formatXAxis(label as number)}
                      formatter={(value: unknown, name: unknown) => [
                        formatTooltip(Number(value ?? 0)),
                        String(name),
                      ]}
                      cursor={CHART_CURSOR}
                    />
                    {models.map((model, index) => {
                      const color = modelSeriesColor(model, index);
                      return (
                        <Area
                          key={model}
                          type="monotone"
                          dataKey={model}
                          stackId="1"
                          stroke={color}
                          fill={color}
                          fillOpacity={SERIES_FILL_OPACITY}
                          strokeWidth={1.5}
                          isAnimationActive={false}
                        />
                      );
                    })}
                  </AreaChart>
                </ResponsiveContainer>
              )}
            </div>
          )}
        </div>
      </Section>
    </div>
  );
}
