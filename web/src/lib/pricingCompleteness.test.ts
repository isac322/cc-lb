import { describe, expect, it } from 'vitest';
import type { UsageBucket } from './api';
import {
  derivePricingCompleteness,
  isIndeterminatePricing,
  type PricingCompletenessInput,
  pricingQualifier,
  sumBucketPricing,
} from './pricingCompleteness';

const input: PricingCompletenessInput = {
  request_count: 1,
  priced_event_count: 0,
  unpriced_event_count: 0,
  unmetered_event_count: 0,
  virtual_cost_micros: 0,
};

describe('pricing completeness truth table', () => {
  it.each([
    [{ request_count: 0 }, undefined],
    [{ priced_event_count: 1, virtual_cost_micros: 100 }, 'priced'],
    [{ priced_event_count: 1 }, 'known_zero'],
    [{ unpriced_event_count: 1 }, 'unpriced'],
    [{ unmetered_event_count: 1 }, 'unmetered'],
    [{}, 'unknown'],
    [{ virtual_cost_micros: 1 }, 'partial'],
    [{ request_count: 2, priced_event_count: 1 }, 'partial'],
    [{ request_count: 2, unpriced_event_count: 1 }, 'unknown'],
    [{ request_count: 2, unmetered_event_count: 1 }, 'unknown'],
    [
      { request_count: 2, priced_event_count: 1, unpriced_event_count: 1 },
      'partial',
    ],
    [
      { request_count: 2, unpriced_event_count: 1, unmetered_event_count: 1 },
      'unpriced',
    ],
    [
      { request_count: 2, priced_event_count: 1, unmetered_event_count: 1 },
      'known_zero',
    ],
    [{ priced_event_count: 2, unmetered_event_count: 1 }, 'known_zero'],
  ] as const)('classifies %j as %s', (overrides, expected) => {
    expect(derivePricingCompleteness({ ...input, ...overrides })).toBe(
      expected,
    );
  });

  it.each([
    'unpriced',
    'unknown',
    'priced',
    'known_zero',
    'partial',
    'unmetered',
    undefined,
  ] as const)('marks only unavailable amounts indeterminate: %s', (state) => {
    expect(isIndeterminatePricing(state)).toBe(
      state === 'unpriced' || state === 'unknown',
    );
  });

  it('uses the same operator copy for every surface', () => {
    expect(pricingQualifier({ pricing_completeness: 'unpriced' })).toBe(
      'List price unavailable',
    );
    expect(pricingQualifier({ pricing_completeness: 'unknown' })).toBe(
      'Price unknown',
    );
    expect(pricingQualifier({ pricing_completeness: 'unmetered' })).toBe(
      'No billable usage',
    );
    expect(
      pricingQualifier({
        pricing_completeness: 'partial',
        request_count: 5,
        priced_request_count: 1,
        unpriced_request_count: 3,
        unmetered_request_count: 1,
      }),
    ).toBe('3 unpriced');
    expect(
      pricingQualifier({
        pricing_completeness: 'partial',
        request_count: 4,
        priced_request_count: 1,
        unpriced_request_count: 1,
        unmetered_request_count: 1,
      }),
    ).toBe('Partly priced');
    expect(
      pricingQualifier({ pricing_completeness: 'priced' }),
    ).toBeUndefined();
  });

  it('sums counters across buckets, retaining legacy gaps', () => {
    const bucket: UsageBucket = {
      bucket_start_unix_secs: 0,
      request_count: 1,
      input_tokens: 100,
      output_tokens: 0,
      cache_creation_input_tokens: 0,
      cache_read_input_tokens: 0,
      error_count: 0,
      virtual_cost_micros: 0,
      latency_ms_sum: 0,
      latency_count: 0,
    };
    expect(sumBucketPricing([bucket])).toMatchObject({
      request_count: 1,
      pricing_completeness: 'unknown',
    });
    expect(sumBucketPricing([])).toMatchObject({
      request_count: 0,
      pricing_completeness: undefined,
    });
    const result = sumBucketPricing([
      {
        ...bucket,
        priced_request_count: 1,
        unpriced_request_count: 0,
        unmetered_request_count: 0,
        virtual_cost_micros: 100,
      },
      {
        ...bucket,
        priced_request_count: 0,
        unpriced_request_count: 1,
        unmetered_request_count: 0,
      },
    ]);
    expect(result).toEqual({
      request_count: 2,
      priced_request_count: 1,
      unpriced_request_count: 1,
      unmetered_request_count: 0,
      pricing_completeness: 'partial',
    });
    expect(pricingQualifier(result)).toBe('1 unpriced');
    expect(
      pricingQualifier(
        sumBucketPricing([{ ...bucket, priced_request_count: 1 }, bucket]),
      ),
    ).toBe('Partly priced');
  });
});
