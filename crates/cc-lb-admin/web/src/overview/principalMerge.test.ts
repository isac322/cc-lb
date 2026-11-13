import { describe, expect, it } from 'vitest';
import type { DashboardUsageResponse, UsageBucket } from '../lib/api';
import { mergePrincipalProviderUsage } from './principalMerge';

function response(
  key: string,
  overrides: Partial<UsageBucket>,
): DashboardUsageResponse {
  return {
    range: '24h',
    step: 'hour',
    group_by: 'principal',
    window_start_unix_secs: 0,
    window_end_unix_secs: 3600,
    observed: true,
    series: [
      {
        key,
        buckets: [
          {
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
            ...overrides,
          },
        ],
      },
    ],
  };
}

describe('principal provider pricing merge', () => {
  it('sums classified counters within a principal across providers', () => {
    const [row] = mergePrincipalProviderUsage(
      {
        anthropic: response('p', {
          priced_request_count: 1,
          virtual_cost_micros: 2_000_000,
        }),
        openai: response('p', { unpriced_request_count: 1 }),
        gemini: response('p', { unmetered_request_count: 1, input_tokens: 0 }),
      },
      new Map([['p', 'Principal']]),
    );
    expect(row.name).toBe('Principal');
    expect(row.pricing_completeness).toBe('partial');
    expect(row.priced_request_count).toBe(1);
    expect(row.unpriced_request_count).toBe(1);
    expect(row.unmetered_request_count).toBe(1);
    expect(row.totalCostMicros).toBe(2_000_000);
    expect(row.sharePct).toBe(100);
    expect(row.providers.openai?.pricing_completeness).toBe('unpriced');
  });
  it('excludes indeterminate principals from cost share', () => {
    const rows = mergePrincipalProviderUsage(
      {
        anthropic: response('known', {
          priced_request_count: 1,
          virtual_cost_micros: 1_000_000,
        }),
        openai: response('unknown', { unpriced_request_count: 1 }),
      },
      new Map(),
    );
    expect(rows.find((row) => row.id === 'known')?.sharePct).toBe(100);
    expect(rows.find((row) => row.id === 'unknown')?.sharePct).toBeNull();
    expect(
      mergePrincipalProviderUsage(
        { openai: response('unknown', { unpriced_request_count: 1 }) },
        new Map(),
      )[0].sharePct,
    ).toBeNull();
  });
  it('keeps explicit zero determinate and legacy fixtures compatible', () => {
    expect(
      mergePrincipalProviderUsage(
        { anthropic: response('zero', { priced_request_count: 1 }) },
        new Map(),
      )[0],
    ).toMatchObject({
      pricing_completeness: 'known_zero',
      totalCostMicros: 0,
      sharePct: 0,
    });
    expect(
      mergePrincipalProviderUsage(
        { anthropic: response('old', { virtual_cost_micros: 1_000_000 }) },
        new Map(),
      )[0],
    ).toMatchObject({ totalCostMicros: 1_000_000, sharePct: 100 });
    expect(
      mergePrincipalProviderUsage(
        { anthropic: response('old', {}) },
        new Map(),
      )[0].pricing_completeness,
    ).toBeUndefined();
  });
});
