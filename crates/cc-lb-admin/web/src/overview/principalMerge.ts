import type { DashboardUsageResponse } from '../lib/api';
import { cacheHitRatio, sumTokens } from '../lib/format';
import {
  isIndeterminatePricing,
  type PricingCounts,
  sumBucketPricing,
  sumPricingCounts,
} from '../lib/pricingCompleteness';
import type { ProviderFamily } from '../lib/providers';

export interface PrincipalProviderMetric extends PricingCounts {
  readonly costMicros: number;
  readonly cacheHitRatio: number | null;
  readonly requests: number;
  readonly tokens: number;
  readonly promptTokens: number;
}

export interface MergedPrincipalUsage extends PricingCounts {
  readonly id: string;
  readonly name: string;
  readonly providers: Readonly<
    Partial<Record<ProviderFamily, PrincipalProviderMetric>>
  >;
  readonly totalCostMicros: number;
  readonly totalRequests: number;
  readonly totalTokens: number;
  readonly totalCacheHitRatio: number | null;
  readonly sharePct: number | null;
}

const EMPTY_TOKENS = {
  input_tokens: 0,
  output_tokens: 0,
  cache_creation_input_tokens: 0,
  cache_read_input_tokens: 0,
};

function metricsForSeries(
  series: DashboardUsageResponse['series'][number],
): PrincipalProviderMetric | null {
  let costMicros = 0;
  let requests = 0;
  const tokens = { ...EMPTY_TOKENS };
  for (const bucket of series.buckets) {
    costMicros += bucket.virtual_cost_micros ?? 0;
    requests += bucket.request_count ?? 0;
    tokens.input_tokens += bucket.input_tokens ?? 0;
    tokens.output_tokens += bucket.output_tokens ?? 0;
    tokens.cache_creation_input_tokens +=
      bucket.cache_creation_input_tokens ?? 0;
    tokens.cache_read_input_tokens += bucket.cache_read_input_tokens ?? 0;
  }
  if (requests <= 0) return null;
  const promptTokens =
    tokens.input_tokens +
    tokens.cache_creation_input_tokens +
    tokens.cache_read_input_tokens;
  return {
    ...sumBucketPricing(series.buckets),
    costMicros,
    cacheHitRatio: cacheHitRatio(tokens),
    requests,
    tokens: sumTokens(tokens),
    promptTokens,
  };
}

/** Merge one principal-grouped usage response per provider without dropping one-sided principals. */
export function mergePrincipalProviderUsage(
  usageByProvider: Readonly<
    Partial<Record<ProviderFamily, DashboardUsageResponse | undefined>>
  >,
  principalNames: ReadonlyMap<string, string>,
): MergedPrincipalUsage[] {
  const merged = new Map<
    string,
    {
      name: string;
      providers: Partial<Record<ProviderFamily, PrincipalProviderMetric>>;
    }
  >();
  for (const provider of ['anthropic', 'openai', 'gemini'] as const) {
    for (const series of usageByProvider[provider]?.series ?? []) {
      if (!series.key) continue;
      const metric = metricsForSeries(series);
      if (!metric) continue;
      const current = merged.get(series.key) ?? {
        name: principalNames.get(series.key) ?? series.key,
        providers: {},
      };
      current.providers[provider] = metric;
      merged.set(series.key, current);
    }
  }

  const rows = Array.from(merged, ([id, value]) => {
    const providerMetrics = Object.values(value.providers);
    const totalCostMicros = providerMetrics.reduce(
      (total, metric) => total + (metric?.costMicros ?? 0),
      0,
    );
    const totalRequests = providerMetrics.reduce(
      (total, metric) => total + (metric?.requests ?? 0),
      0,
    );
    const totalTokens = providerMetrics.reduce(
      (total, metric) => total + (metric?.tokens ?? 0),
      0,
    );
    const promptTokens = providerMetrics.reduce(
      (total, metric) => total + (metric?.promptTokens ?? 0),
      0,
    );
    const weightedCacheHit =
      promptTokens > 0
        ? providerMetrics.reduce(
            (total, metric) =>
              total +
              (metric?.cacheHitRatio == null
                ? 0
                : metric.cacheHitRatio * metric.promptTokens),
            0,
          ) / promptTokens
        : null;
    return {
      ...sumPricingCounts(
        providerMetrics.map((metric) => ({
          ...metric,
          request_count: metric.requests,
          total_tokens: metric.tokens,
          virtual_cost_micros: metric.costMicros,
        })),
      ),
      id,
      name: value.name,
      providers: value.providers,
      totalCostMicros,
      totalRequests,
      totalTokens,
      totalCacheHitRatio: weightedCacheHit,
      sharePct: null as number | null,
    } satisfies MergedPrincipalUsage;
  });
  const determinate = rows.filter(
    (row) => !isIndeterminatePricing(row.pricing_completeness),
  );
  const totalCost = determinate.reduce(
    (total, row) => total + row.totalCostMicros,
    0,
  );
  return rows.map((row) => ({
    ...row,
    sharePct:
      isIndeterminatePricing(row.pricing_completeness) ||
      determinate.length === 0
        ? null
        : totalCost > 0
          ? (row.totalCostMicros / totalCost) * 100
          : 0,
  }));
}
