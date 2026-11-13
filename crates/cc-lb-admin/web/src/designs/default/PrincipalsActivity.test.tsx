import { cleanup, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import type { DashboardUsageResponse, UsageBucket } from '../../lib/api';
import type { NativeWriteTokenFields } from '../../lib/pricingCompleteness';
import { useUsage } from '../../lib/queries';
import { successQuery } from '../../lib/test-utils/hook-results';
import type * as timePresets from '../../lib/timePresets';
import { PrincipalsActivity } from './PrincipalsActivity';

vi.mock('../../lib/queries', () => ({ useUsage: vi.fn() }));
vi.mock('../../lib/previewControls', () => ({
  usePreviewInstallation: () => ({
    mode: 'multi',
    providers: ['anthropic', 'openai'],
    stale: false,
  }),
  useProviderScope: () => ({ scope: 'all' }),
}));
vi.mock('../../lib/timePresets', async (original) => ({
  ...(await original<typeof timePresets>()),
  useSharedTimeRange: () => ({ range: '24h', setRange: vi.fn() }),
}));
afterEach(cleanup);

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
function usageResult(buckets: UsageBucket[]): ReturnType<typeof useUsage> {
  return successQuery<DashboardUsageResponse>({
    range: '24h',
    step: 'hour',
    group_by: 'provider',
    window_start_unix_secs: 0,
    window_end_unix_secs: 3600,
    series: [{ key: 'anthropic', buckets }],
    observed: true,
  });
}
function usage(buckets: UsageBucket[]) {
  vi.mocked(useUsage).mockReturnValue(usageResult(buckets));
  return render(<PrincipalsActivity principalId="principal" />);
}

describe('PrincipalsActivity cost completeness', () => {
  it.each([
    { cache_write_input_tokens: 7 },
    { cache_write_input_tokens: 0, cache_write_input_tokens_30m: 7 },
  ] satisfies NativeWriteTokenFields[])(
    'keeps legacy native-write-only consumption unknown: %j',
    (nativeWrites) => {
      usage([
        {
          ...bucket,
          input_tokens: 0,
          priced_request_count: 0,
          unpriced_request_count: 0,
          unmetered_request_count: 0,
          ...nativeWrites,
        },
      ]);
      expect(screen.getByText('Price unknown')).toBeDefined();
      expect(
        screen.getByText('Est. catalog cost').parentElement?.textContent,
      ).toContain('—');
      expect(screen.queryByText('$0.0000')).toBeNull();
    },
  );

  it.each([
    ['unpriced', 'List price unavailable'],
    ['unknown', 'Price unknown'],
  ] as const)(
    'shows %s as a neutral unavailable amount',
    (pricing_completeness, caption) => {
      usage([
        {
          ...bucket,
          pricing_completeness,
          unpriced_request_count: pricing_completeness === 'unpriced' ? 1 : 0,
        },
      ]);
      const value = screen.getByText('Est. catalog cost').parentElement;
      expect(value?.textContent).toContain('—');
      expect(value?.textContent).not.toContain('$0.0000');
      expect(screen.getByText(caption).className).toBe(
        'text-caption text-text-faint',
      );
      expect(
        screen.getByText('No recorded activity in this range.'),
      ).toBeDefined();
    },
  );
  it('combines priced and unpriced buckets into a qualified subtotal', () => {
    usage([
      { ...bucket, priced_request_count: 1, virtual_cost_micros: 2_500_000 },
      { ...bucket, unpriced_request_count: 1 },
    ]);
    expect(screen.getByText('$2.5000+')).toBeDefined();
    expect(screen.getByText('1 unpriced')).toBeDefined();
  });
  it('preserves a known zero and legacy recorded costs', () => {
    const view = usage([{ ...bucket, priced_request_count: 1 }]);
    expect(screen.getByText('$0.0000')).toBeDefined();
    expect(screen.queryByText('List price unavailable')).toBeNull();
    view.unmount();
    usage([{ ...bucket, virtual_cost_micros: 2_500_000 }]);
    expect(screen.getByText('$2.5000')).toBeDefined();
    expect(screen.queryByText('Partly priced')).toBeNull();
  });
});
