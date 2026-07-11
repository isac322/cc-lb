import { describe, expect, it } from 'vitest';
import type { QuotaSnapshot } from '../../lib/api';
import { selectSidebarQuotaWindows } from './quotaWindowVisibility';

function snap(overrides: Partial<QuotaSnapshot>): QuotaSnapshot {
  return {
    window: '5h',
    state: 'fresh',
    source: 'api',
    utilization: 0.1,
    status: null,
    resets_at_unix_secs: null,
    surpassed_threshold: null,
    representative_claim: null,
    disabled_reason: null,
    extra_usage_enabled: null,
    extra_usage_monthly_limit: null,
    extra_usage_used_credits: null,
    observed_at_unix_millis: null,
    age_secs: null,
    ...overrides,
  };
}

describe('selectSidebarQuotaWindows', () => {
  const nowUnixSecs = 1_000_000;

  it('always includes 5h and 7d even when latestWindows is undefined', () => {
    const result = selectSidebarQuotaWindows({
      latestWindows: undefined,
      nowUnixSecs,
    });
    expect(result).toEqual(['5h', '7d']);
  });

  it('always includes 5h and 7d even when missing', () => {
    const result = selectSidebarQuotaWindows({
      latestWindows: [
        snap({ window: '5h', state: 'missing', observed_at_unix_millis: null }),
        snap({ window: '7d', state: 'missing', observed_at_unix_millis: null }),
      ],
      nowUnixSecs,
    });
    expect(result).toEqual(['5h', '7d']);
  });

  it('includes 7d_fable when observed within the last week', () => {
    const result = selectSidebarQuotaWindows({
      latestWindows: [
        snap({
          window: '7d_fable',
          observed_at_unix_millis: (nowUnixSecs - 172_800) * 1000,
        }),
      ],
      nowUnixSecs,
    });
    expect(result).toEqual(['5h', '7d', '7d_fable']);
  });

  it('keeps 7d_fable when observed exactly one week ago (inclusive)', () => {
    const result = selectSidebarQuotaWindows({
      latestWindows: [
        snap({
          window: '7d_fable',
          observed_at_unix_millis: (nowUnixSecs - 604_800) * 1000,
        }),
      ],
      nowUnixSecs,
    });
    expect(result).toEqual(['5h', '7d', '7d_fable']);
  });

  it('drops 7d_fable when observed more than a week ago', () => {
    const result = selectSidebarQuotaWindows({
      latestWindows: [
        snap({
          window: '7d_fable',
          observed_at_unix_millis: (nowUnixSecs - 604_800 - 1) * 1000,
        }),
      ],
      nowUnixSecs,
    });
    expect(result).toEqual(['5h', '7d']);
  });

  it('drops 7d_fable when missing', () => {
    const result = selectSidebarQuotaWindows({
      latestWindows: [
        snap({
          window: '7d_fable',
          state: 'missing',
          observed_at_unix_millis: null,
        }),
      ],
      nowUnixSecs,
    });
    expect(result).toEqual(['5h', '7d']);
  });

  it('includes overage when active (enabled)', () => {
    const result = selectSidebarQuotaWindows({
      latestWindows: [
        snap({
          window: 'overage',
          extra_usage_enabled: true,
        }),
      ],
      nowUnixSecs,
    });
    expect(result).toEqual(['5h', '7d', 'overage']);
  });

  it('includes overage when active (monthly limit set)', () => {
    const result = selectSidebarQuotaWindows({
      latestWindows: [
        snap({
          window: 'overage',
          extra_usage_enabled: false,
          extra_usage_monthly_limit: 5000,
        }),
      ],
      nowUnixSecs,
    });
    expect(result).toEqual(['5h', '7d', 'overage']);
  });

  it('orders windows correctly', () => {
    const result = selectSidebarQuotaWindows({
      latestWindows: [
        snap({ window: 'overage', extra_usage_enabled: true }),
        snap({
          window: '7d_fable',
          observed_at_unix_millis: (nowUnixSecs - 1000) * 1000,
        }),
      ],
      nowUnixSecs,
    });
    expect(result).toEqual(['5h', '7d', '7d_fable', 'overage']);
  });
});
