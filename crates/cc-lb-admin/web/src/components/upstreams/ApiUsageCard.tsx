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
  ProviderReportState,
} from '../../lib/api';
import { getWindowColor } from '../../lib/colors';
import { fmtUsd, splitNum, sumTokens } from '../../lib/format';
import {
  isIndeterminatePricing,
  type PricingCounts,
  pricingQualifier,
  sumBucketPricing,
} from '../../lib/pricingCompleteness';
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

const REPORT_STATE_LABEL: Record<ProviderReportState, string> = {
  reported: 'Reported',
  estimated: 'Estimated',
  not_configured: 'Not configured',
  unavailable: 'Unavailable',
  denied: 'Denied',
  not_applicable: 'Not applicable',
  stale: 'Stale',
  failed: 'Failed',
};

type Props = {
  data: DashboardUsageResponse | undefined;
  isLoading: boolean;
  range: TimePreset;
  onRangeChange: (r: TimePreset) => void;
  metric: 'tokens' | 'cost';
  onMetricChange: (m: 'tokens' | 'cost') => void;
  /**
   * Where the figures come from and whose they are. Legacy Anthropic
   * provenance and additive provider reporting states share this surface.
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

  const { chartData, pricingByTimestamp, models } = useMemo(() => {
    const bucketsByTs = new Map<number, Record<string, number>>();
    const pricingByTimestamp = new Map<number, Map<string, PricingCounts>>();
    const models = (data?.series ?? []).map((series, index) => ({
      key: `model-${index}`,
      label: modelLabel(series.key),
    }));

    for (const [index, series] of (data?.series ?? []).entries()) {
      const key = models[index].key;
      for (const bucket of series.buckets) {
        const pricing = {
          ...bucket,
          pricing_completeness:
            bucket.pricing_completeness ??
            sumBucketPricing([bucket]).pricing_completeness,
        };
        const ts = bucket.bucket_start_unix_secs;
        if (!bucketsByTs.has(ts)) {
          bucketsByTs.set(ts, { ts });
        }
        const row = bucketsByTs.get(ts)!;
        if (metric === 'cost' && provenance?.source === 'estimated') {
          let byModel = pricingByTimestamp.get(ts);
          if (!byModel) {
            byModel = new Map();
            pricingByTimestamp.set(ts, byModel);
          }
          byModel.set(key, pricing);
        }
        if (metric === 'tokens') {
          row[key] = sumTokens(bucket);
        } else if (
          !(
            provenance?.source === 'estimated' &&
            isIndeterminatePricing(pricing.pricing_completeness)
          )
        ) {
          row[key] = bucket.virtual_cost_micros / 1_000_000;
        }
      }
    }

    return {
      chartData: Array.from(bucketsByTs.values()).sort((a, b) => a.ts - b.ts),
      pricingByTimestamp,
      models,
    };
  }, [data, metric, provenance?.source]);

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

  const formatTooltip = (val: number, pricing?: PricingCounts) => {
    if (metric === 'cost') {
      const amount = fmtUsd(val * 1_000_000);
      if (pricing?.pricing_completeness !== 'partial') return amount;
      const qualifier = pricingQualifier(pricing);
      return `${amount}+${qualifier ? ` (${qualifier})` : ''}`;
    }
    return val.toLocaleString();
  };

  const nativeProvider =
    provenance?.provider === 'openai' || provenance?.provider === 'gemini';
  const providerLabel =
    provenance?.provider === 'openai'
      ? 'OpenAI'
      : provenance?.provider === 'gemini'
        ? 'Gemini'
        : provenance?.provider === 'anthropic' ||
            provenance?.provider_report_state === undefined
          ? 'Anthropic'
          : 'the provider';
  const reportState =
    provenance?.provider_report_state ??
    (provenance?.source !== 'reported' && provenance?.source !== 'estimated'
      ? provenance?.source
      : undefined);
  // A stale provider snapshot is still reported data, never an estimate.
  const reported =
    provenance?.source === 'reported' || provenance?.source === 'stale';
  const stale =
    reported && (provenance?.stale === true || reportState === 'stale');
  // Stopped (Anthropic refuses reports) is a fix-code state; a stale
  // snapshot without it is a transient outage with no fix to offer.
  const stopped = stale && provenance?.stopped === true;
  const estimated = provenance !== undefined && !reported;
  // Access already says Reported; the first poll is queued (plan R2-4).
  const pendingFirstReport =
    estimated && !nativeProvider && provenance?.pending_first_report === true;
  const lastReported = provenance?.last_reported_unix_secs ?? null;
  // A background refetch failed while the previous answer stays on screen.
  const notRefreshed = month !== undefined && monthError !== null;
  const nativeBillingStop = nativeProvider && month?.billing_stop != null;
  const monthState =
    provenance?.source === 'estimated'
      ? month?.pricing_completeness
      : undefined;
  const monthQualifier =
    monthState && month ? pricingQualifier(month) : undefined;

  // The header holds only the provenance badge; chart controls sit in the
  // toolbar row above the chart. While the first report is on its way the
  // description says so instead of an Estimated badge contradicting Access.
  const showEstimatedBadge = estimated && !pendingFirstReport;
  const action =
    showEstimatedBadge ||
    stale ||
    notRefreshed ||
    reportState ||
    nativeProvider ? (
      <div className="flex flex-wrap items-center gap-2">
        {showEstimatedBadge ? <Badge>Estimated</Badge> : null}
        {stale ? <Badge>Last report</Badge> : null}
        {reportState && reportState !== 'estimated' ? (
          <Badge>{REPORT_STATE_LABEL[reportState]}</Badge>
        ) : nativeProvider && reported && !stale ? (
          <Badge>Reported</Badge>
        ) : null}
        {notRefreshed ? <Badge>Not refreshed</Badge> : null}
      </div>
    ) : undefined;

  // What the figures are and whose they are. Key status and the fix that
  // makes usage reported belong to the pane notice and Access.
  const costNote = metric === 'cost' ? ' Cost computed at list price.' : '';
  const caption = nativeProvider
    ? reported
      ? `${stale ? 'Last reported' : 'Reported'} figures for ${provenance?.project_id ? `project ${provenance.project_id}` : 'the provider reporting scope'}.${costNote}`
      : `Proxy estimates cover cc-lb traffic only, not organization totals.${costNote}`
    : reported
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
        nativeProvider || reportState ? (
          reported ? (
            `${stale ? 'Last figures' : 'Figures'} reported by ${providerLabel}${notRefreshed ? ' · not refreshed' : ''}`
          ) : (
            `Estimated from cc-lb traffic · provider reporting ${REPORT_STATE_LABEL[reportState ?? 'estimated'].toLowerCase()}`
          )
        ) : reported ? (
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
        {nativeProvider && !nativeBillingStop ? (
          <div className="flex flex-col gap-2">
            <p className="text-caption text-text-faint">This month (UTC)</p>
            {monthLoading && !month ? (
              <Skeleton className="h-5 w-40" />
            ) : (
              <dl className="grid grid-cols-2 gap-4">
                <div>
                  <dt className="text-caption text-text-faint">Tokens used</dt>
                  <dd className="text-body font-medium tabular-nums">
                    {month ? month.tokens.toLocaleString() : 'No reading'}
                  </dd>
                </div>
                <div>
                  <dt className="text-caption text-text-faint">
                    Cost at list price
                  </dt>
                  <dd className="text-body font-medium tabular-nums">
                    {month
                      ? isIndeterminatePricing(monthState)
                        ? '—'
                        : `${fmtUsd(month.cost_usd_micros)}${monthState === 'partial' ? '+' : ''}`
                      : 'No reading'}
                  </dd>
                  {isIndeterminatePricing(monthState) ||
                  monthState === 'partial' ? (
                    <dd className="text-caption text-text-faint">
                      {monthQualifier}
                    </dd>
                  ) : null}
                </div>
              </dl>
            )}
            {monthError ? (
              <p role="alert" className="text-caption text-warn-text">
                {month ? 'Usage not refreshed' : 'Usage unavailable'}
              </p>
            ) : null}
            {provenance?.provider === 'gemini' ? (
              <a
                className={QUIET_LINK_CLASS}
                href="https://aistudio.google.com/docs/billing#monitor-usage"
                target="_blank"
                rel="noopener noreferrer"
              >
                Gemini usage and billing in AI Studio
              </a>
            ) : null}
          </div>
        ) : (
          <CreditStrip
            month={month}
            provenance={provenance}
            isLoading={monthLoading}
            loadError={monthError}
          />
        )}
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
                    const val = lastBucket ? lastBucket[model.key] || 0 : 0;
                    const pricing = lastBucket
                      ? pricingByTimestamp.get(lastBucket.ts)?.get(model.key)
                      : undefined;
                    const partial = pricing?.pricing_completeness === 'partial';
                    const qualifier = partial
                      ? pricingQualifier(pricing)
                      : undefined;
                    return (
                      <div
                        key={model.key}
                        className="flex items-center gap-1.5"
                      >
                        <span
                          aria-hidden="true"
                          className="size-2.5 shrink-0 rounded-xs"
                          style={{
                            backgroundColor: modelSeriesColor(
                              model.label,
                              index,
                            ),
                          }}
                        />
                        <span className="text-text-muted">{model.label}</span>
                        <span
                          className="tabular-nums text-text"
                          title={qualifier}
                          aria-label={
                            qualifier
                              ? `${fmtUsd(val * 1_000_000)}+ (${qualifier})`
                              : undefined
                          }
                        >
                          {metric === 'cost' &&
                          provenance?.source === 'estimated' &&
                          lastBucket?.[model.key] === undefined
                            ? '—'
                            : partial
                              ? `${fmtUsd(val * 1_000_000)}+`
                              : formatTooltip(val)}
                          {qualifier ? (
                            <span className="sr-only">{` (${qualifier})`}</span>
                          ) : null}
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
                      formatter={(value: unknown, name: unknown, item) => [
                        formatTooltip(
                          Number(value ?? 0),
                          pricingByTimestamp
                            .get(Number(item.payload?.ts))
                            ?.get(String(item.dataKey)),
                        ),
                        String(name),
                      ]}
                      cursor={CHART_CURSOR}
                    />
                    <defs>
                      {models.map((model, index) => (
                        <SeriesFillGradient
                          key={model.key}
                          id={`${chartId}-${index}`}
                          color={modelSeriesColor(model.label, index)}
                        />
                      ))}
                    </defs>
                    {models.map((model, index) => (
                      <Area
                        key={model.key}
                        type="monotone"
                        dataKey={model.key}
                        name={model.label}
                        stackId="1"
                        stroke={modelSeriesColor(model.label, index)}
                        fill={`url(#${chartId}-${index})`}
                        fillOpacity={1}
                        strokeWidth={1.5}
                        connectNulls={false}
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
