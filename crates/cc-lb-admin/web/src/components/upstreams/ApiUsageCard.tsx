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
import type {
  DashboardUsageResponse,
  KeyUsageMonth,
  KeyUsageProvenance,
} from '../../lib/api';
import { getWindowColor } from '../../lib/colors';
import { fmtUsd, splitNum, sumTokens } from '../../lib/format';
import { TIME_PRESET_OPTIONS, type TimePreset } from '../../lib/timePresets';
import {
  CHART_AXIS,
  CHART_CURSOR,
  CHART_GRID,
  SeriesFillGradient,
  useChartId,
} from '../ui/charts';
import { DetailSection } from '../ui/DetailPane';
import {
  Badge,
  EmptyState,
  SegmentedControl,
  Skeleton,
} from '../ui/primitives';
import { RelativeTime } from '../ui/RelativeTime';
import { CreditStrip } from './CreditStrip';
import { QUIET_LINK_CLASS } from './connect/parts';

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

/** Anthropic reports usage without a model as `""`; it reads "Other". */
function modelLabel(model: string): string {
  return model === '' ? 'Other' : model;
}

const RANGE_OPTIONS = TIME_PRESET_OPTIONS;
const METRIC_OPTIONS = [
  { value: 'tokens', label: 'Tokens' },
  { value: 'cost', label: 'Cost' },
] as const;

type Props = {
  data: DashboardUsageResponse | undefined;
  isLoading: boolean;
  range: TimePreset;
  onRangeChange: (r: TimePreset) => void;
  metric: 'tokens' | 'cost';
  onMetricChange: (m: 'tokens' | 'cost') => void;
  /**
   * Where the figures come from and whose they are. Reported = token counts
   * from Anthropic's usage report; anything else is cc-lb's own estimate.
   * Omitted when the backend does not report provenance.
   */
  provenance?: KeyUsageProvenance;
  /** This calendar month (UTC) for the credit strip. */
  month?: KeyUsageMonth;
  monthLoading?: boolean;
  /**
   * Why the latest usage request failed. With retained figures they are
   * marked "Not refreshed"; the strip carries the one announced message.
   */
  monthError?: string | null;
  /**
   * Access shows its Usage data row (the key is usable: verified and not
   * rejected), so "Get reported usage" has somewhere to point.
   */
  usageModeShown?: boolean;
};

export function ApiUsageCard({
  data,
  isLoading,
  range,
  onRangeChange,
  metric,
  onMetricChange,
  provenance,
  month,
  monthLoading = false,
  monthError = null,
  usageModeShown = false,
}: Props) {
  const showLoading = data === undefined && isLoading;
  const chartId = useChartId();

  const chartData = useMemo(() => {
    if (!data?.series || data.series.length === 0) return [];

    const bucketsByTs = new Map<number, Record<string, number>>();

    for (const series of data.series) {
      const model = modelLabel(series.key);
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
    return data.series.map((s) => modelLabel(s.key));
  }, [data]);

  const formatXAxis = (ts: number) => {
    const d = new Date(ts * 1000);
    if (range !== '7d') {
      return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    }
    return d.toLocaleDateString([], { month: 'short', day: 'numeric' });
  };

  const formatYAxis = (val: number) => {
    if (metric === 'cost') {
      return fmtUsd(val * 1_000_000);
    }
    // Compact counts share the app's k/M/B casing (splitNum, like fmtTokens).
    const { value, unit } = splitNum(val);
    return `${value}${unit}`;
  };

  const formatTooltip = (val: number) => {
    if (metric === 'cost') {
      return fmtUsd(val * 1_000_000);
    }
    return val.toLocaleString();
  };

  // The label follows the data's source: a stale Anthropic snapshot is
  // still reported data (labeled stale), never relabeled as an estimate.
  const reported = provenance?.source === 'reported';
  const stale = reported && provenance?.stale === true;
  // Stopped (Anthropic refuses reports) is a fix-code state; a stale
  // snapshot without it is a transient outage with no fix to offer.
  const stopped = stale && provenance?.stopped === true;
  const estimated = provenance !== undefined && !reported;
  // Access already says Reported; the first poll is queued (plan R2-4).
  const pendingFirstReport =
    estimated && provenance?.pending_first_report === true;
  const lastReported = provenance?.last_reported_unix_secs ?? null;
  // A background refetch failed while the previous answer stays on screen.
  const notRefreshed = month !== undefined && monthError !== null;

  // The header holds only the provenance badge; chart controls sit in the
  // toolbar row above the chart. While the first report is on its way the
  // description says so instead of an Estimated badge contradicting Access.
  const showEstimatedBadge = estimated && !pendingFirstReport;
  const action =
    showEstimatedBadge || stale || notRefreshed ? (
      <div className="flex flex-wrap items-center gap-2">
        {showEstimatedBadge ? <Badge>Estimated</Badge> : null}
        {stale ? <Badge>Last report</Badge> : null}
        {notRefreshed ? <Badge>Not refreshed</Badge> : null}
      </div>
    ) : undefined;

  // What the figures are and whose they are. Key status and the fix that
  // makes usage reported belong to the pane notice and Access.
  const costNote = metric === 'cost' ? ' Cost computed at list price.' : '';
  const caption = reported
    ? stale
      ? `Showing the last reported figures — ${provenance?.attribution === 'workspace' ? `all usage in ${provenance.workspace ?? 'the workspace'}` : "all of this key's usage"}.${costNote}`
      : provenance?.attribution === 'workspace'
        ? `All usage in ${provenance.workspace ?? 'the workspace'} — Anthropic can't attribute it to this key.${costNote}`
        : `All of this key's usage, including traffic outside cc-lb.${costNote}`
    : provenance?.fix === 'non_anthropic_base_url'
      ? `Real usage needs api.anthropic.com Admin API access. This upstream points elsewhere → showing estimates only.${costNote}`
      : costNote.trim() || null;

  const goToUsageMode = () => {
    const row = document.querySelector<HTMLElement>(
      '[data-testid="api-key-usage-mode"]',
    );
    row?.scrollIntoView({ block: 'center' });
    row?.focus();
  };

  return (
    <DetailSection
      span="full"
      data-testid="api-usage-card"
      title="API usage"
      description={
        reported ? (
          stopped ? (
            'Last tokens reported by Anthropic · reports stopped'
          ) : stale ? (
            <>
              Last tokens reported by Anthropic · Anthropic hasn't answered
              {lastReported === null ? (
                ' lately'
              ) : (
                <>
                  {' since '}
                  <RelativeTime ts={lastReported * 1000} />
                </>
              )}
            </>
          ) : notRefreshed ? (
            'Tokens reported by Anthropic · not refreshed'
          ) : (
            'Tokens reported by Anthropic · updates about every 5 min'
          )
        ) : pendingFirstReport ? (
          'Reported figures arrive within 5 min'
        ) : estimated ? (
          <>
            Estimated from cc-lb traffic
            {usageModeShown ? (
              <>
                {' · '}
                <button
                  type="button"
                  className={QUIET_LINK_CLASS}
                  onClick={goToUsageMode}
                >
                  Get reported usage
                </button>
              </>
            ) : null}
          </>
        ) : (
          'Token and cost breakdown by model'
        )
      }
      action={action}
    >
      <div className="flex flex-col gap-4">
        <CreditStrip
          month={month}
          provenance={provenance}
          isLoading={monthLoading}
          loadError={monthError}
        />
        <div className="flex flex-col gap-3 border-t border-border-row pt-4">
          <div className="flex flex-wrap items-center justify-between gap-x-4 gap-y-2">
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
            <div
              data-testid="api-usage-legend-slot"
              className="flex min-h-[28px] flex-wrap items-center gap-x-4 gap-y-1 text-caption"
            >
              {!showLoading && chartData.length > 0
                ? models.map((model, index) => {
                    const lastBucket = chartData[chartData.length - 1];
                    const val = lastBucket ? lastBucket[model] || 0 : 0;
                    return (
                      <div key={model} className="flex items-center gap-1.5">
                        <span
                          aria-hidden="true"
                          className="size-2.5 shrink-0 rounded-xs"
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
                      width={48}
                    />
                    <Tooltip
                      labelFormatter={(label) => formatXAxis(label as number)}
                      formatter={(value: unknown, name: unknown) => [
                        formatTooltip(Number(value ?? 0)),
                        String(name),
                      ]}
                      cursor={CHART_CURSOR}
                    />
                    <defs>
                      {models.map((model, index) => (
                        <SeriesFillGradient
                          key={model}
                          id={`${chartId}-${index}`}
                          color={modelSeriesColor(model, index)}
                        />
                      ))}
                    </defs>
                    {models.map((model, index) => (
                      <Area
                        key={model}
                        type="monotone"
                        dataKey={model}
                        stackId="1"
                        stroke={modelSeriesColor(model, index)}
                        fill={`url(#${chartId}-${index})`}
                        fillOpacity={1}
                        strokeWidth={1.5}
                        isAnimationActive={false}
                      />
                    ))}
                  </AreaChart>
                </ResponsiveContainer>
              )}
            </div>
          )}
          {caption ? (
            <p className="text-caption text-text-faint">{caption}</p>
          ) : null}
        </div>
      </div>
    </DetailSection>
  );
}
