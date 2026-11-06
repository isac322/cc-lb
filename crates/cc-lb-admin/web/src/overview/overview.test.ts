import { describe, expect, it } from 'vitest';
import {
  overviewEventFilters,
  overviewEventKinds,
} from '../designs/default/OverviewPage';
import type { DashboardUsageResponse } from '../lib/api';
import { qk } from '../lib/queries';
import { mergePrincipalProviderUsage } from './principalMerge';

function usage(
  series: DashboardUsageResponse['series'],
): DashboardUsageResponse {
  return {
    range: '24h',
    step: 'hour',
    group_by: 'principal',
    window_start_unix_secs: 0,
    window_end_unix_secs: 1,
    series,
    observed: true,
  };
}
function bucket(request_count: number, cost: number) {
  return {
    bucket_start_unix_secs: 0,
    request_count,
    input_tokens: 100,
    output_tokens: 20,
    cache_creation_input_tokens: 0,
    cache_read_input_tokens: 80,
    error_count: 0,
    virtual_cost_micros: cost,
    latency_ms_sum: 0,
    latency_count: 0,
  };
}

describe('multi-provider overview principal merge', () => {
  it('keeps a principal present in only one provider', () => {
    const rows = mergePrincipalProviderUsage(
      {
        anthropic: usage([
          { key: 'only-anthropic', buckets: [bucket(2, 100)] },
        ]),
        openai: usage([{ key: 'only-openai', buckets: [bucket(1, 50)] }]),
      },
      new Map([['only-anthropic', 'Anthropic user']]),
    );
    expect(rows.map((row) => row.name)).toEqual([
      'Anthropic user',
      'only-openai',
    ]);
    expect(rows[0]?.providers.anthropic?.requests).toBe(2);
    expect(rows[0]?.providers.openai).toBeUndefined();
  });

  it('falls back to the provider response key for an unknown principal name', () => {
    const rows = mergePrincipalProviderUsage(
      {
        gemini: usage([{ key: 'unknown-principal', buckets: [bucket(1, 25)] }]),
      },
      new Map(),
    );

    expect(rows[0]?.name).toBe('unknown-principal');
  });
});
describe('overview request feeds', () => {
  it('uses provider-scoped endpoint taxonomy instead of one provider kind', () => {
    expect(overviewEventFilters('gemini')).toEqual({
      event_kind_set: expect.arrayContaining([
        'gemini_generate_content',
        'gemini_predict',
      ]),
    });
    expect(overviewEventKinds('gemini')).toEqual(
      expect.arrayContaining(['gemini_generate_content', 'gemini_predict']),
    );
    expect(overviewEventKinds('openai')).toEqual(
      expect.arrayContaining([
        'openai_responses',
        'chat_completions',
        'completions',
        'embeddings',
        'images',
        'audio',
        'codex_auxiliary',
      ]),
    );
  });
});

describe('provider pool query keys', () => {
  it('separates pool aggregate and history by provider', () => {
    expect(
      qk.subscriptionQuotaAggregate({ windows: '5h', provider: 'anthropic' }),
    ).not.toEqual(
      qk.subscriptionQuotaAggregate({ windows: '5h', provider: 'openai' }),
    );
    expect(
      qk.subscriptionQuotaPoolHistory({
        windows: '5h',
        provider: 'gemini',
        rangeSecs: 3600,
      }),
    ).not.toEqual(
      qk.subscriptionQuotaPoolHistory({
        windows: '5h',
        provider: 'openai',
        rangeSecs: 3600,
      }),
    );
  });
});
