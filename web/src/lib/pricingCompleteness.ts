import type { PricingCompleteness, UsageBucket } from './api';

export interface PricingCompletenessInput {
  request_count: number;
  priced_event_count: number;
  unpriced_event_count: number;
  unmetered_event_count: number;
  virtual_cost_micros: number;
}

export interface PricingCounts {
  pricing_completeness?: PricingCompleteness;
  request_count?: number;
  priced_request_count?: number;
  unpriced_request_count?: number;
  unmetered_request_count?: number;
}

/** Keep the classification branches in sync with cc-lb-storage-api. */
export function derivePricingCompleteness(
  input: PricingCompletenessInput,
): PricingCompleteness | undefined {
  if (input.request_count === 0) return undefined;
  const classified =
    input.priced_event_count +
    input.unpriced_event_count +
    input.unmetered_event_count;
  const legacy = Math.max(0, input.request_count - classified);
  if (legacy === 0) {
    if (input.unpriced_event_count > 0 && input.priced_event_count > 0)
      return 'partial';
    if (input.unpriced_event_count > 0) return 'unpriced';
    if (input.priced_event_count > 0)
      return input.virtual_cost_micros > 0 ? 'priced' : 'known_zero';
    return 'unmetered';
  }
  return input.priced_event_count > 0 || input.virtual_cost_micros > 0
    ? 'partial'
    : 'unknown';
}

export function sumBucketPricing(
  buckets: readonly UsageBucket[],
): PricingCounts {
  return sumPricingCounts(buckets);
}

export function sumPricingCounts(
  buckets: readonly (PricingCounts & {
    request_count: number;
    virtual_cost_micros: number;
  })[],
): PricingCounts {
  let request_count = 0;
  let priced_request_count = 0;
  let unpriced_request_count = 0;
  let unmetered_request_count = 0;
  let virtual_cost_micros = 0;
  for (const bucket of buckets) {
    request_count += bucket.request_count;
    priced_request_count += bucket.priced_request_count ?? 0;
    unpriced_request_count += bucket.unpriced_request_count ?? 0;
    unmetered_request_count += bucket.unmetered_request_count ?? 0;
    virtual_cost_micros += bucket.virtual_cost_micros;
  }
  return {
    request_count,
    priced_request_count,
    unpriced_request_count,
    unmetered_request_count,
    pricing_completeness: derivePricingCompleteness({
      request_count,
      priced_event_count: priced_request_count,
      unpriced_event_count: unpriced_request_count,
      unmetered_event_count: unmetered_request_count,
      virtual_cost_micros,
    }),
  };
}

export function isIndeterminatePricing(
  state: PricingCompleteness | undefined,
): boolean {
  return state === 'unpriced' || state === 'unknown';
}

export function pricingQualifier(pricing: PricingCounts): string | undefined {
  switch (pricing.pricing_completeness) {
    case 'unpriced':
      return 'List price unavailable';
    case 'unknown':
      return 'Price unknown';
    case 'unmetered':
      return 'No billable usage';
    case 'partial': {
      const priced = pricing.priced_request_count ?? 0;
      const unpriced = pricing.unpriced_request_count ?? 0;
      const unmetered = pricing.unmetered_request_count ?? 0;
      return pricing.request_count === priced + unpriced + unmetered &&
        unpriced > 0
        ? `${unpriced} unpriced`
        : 'Partly priced';
    }
    default:
      return undefined;
  }
}
