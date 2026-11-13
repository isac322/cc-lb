import { cleanup, render, screen } from '@testing-library/react';
import type { ReactNode } from 'react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import type {
  DashboardUsageResponse,
  KeyUsageMonth,
  KeyUsageProvenance,
  UsageBucket,
} from '../../../lib/api';
import { ApiUsageCard } from '../ApiUsageCard';

const captured = vi.hoisted(() => ({ data: [] as Record<string, number>[] }));
vi.mock('recharts', () => ({
  ResponsiveContainer: ({ children }: { children: ReactNode }) => children,
  AreaChart: ({
    data,
    children,
  }: {
    data: typeof captured.data;
    children: ReactNode;
  }) => {
    captured.data = data;
    return <div>{children}</div>;
  },
  Area: () => null,
  CartesianGrid: () => null,
  XAxis: () => null,
  YAxis: () => null,
  Tooltip: () => null,
}));
afterEach(() => {
  cleanup();
  captured.data = [];
});

const provenance: KeyUsageProvenance = {
  source: 'estimated',
  provider: 'openai',
  stale: false,
  stopped: false,
  last_reported_unix_secs: null,
  pending_first_report: false,
  attribution: 'api_key',
  workspace: null,
  verified: true,
  key_class: 'org_scoped',
  fix: null,
};
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
const data: DashboardUsageResponse = {
  range: '24h',
  step: 'hour',
  group_by: 'model',
  window_start_unix_secs: 0,
  window_end_unix_secs: 3600,
  observed: true,
  series: [
    {
      key: 'model',
      buckets: [
        {
          ...bucket,
          pricing_completeness: 'priced',
          virtual_cost_micros: 2_500_000,
        },
        {
          ...bucket,
          bucket_start_unix_secs: 60,
          pricing_completeness: 'unpriced',
        },
        {
          ...bucket,
          bucket_start_unix_secs: 120,
          pricing_completeness: 'unknown',
        },
      ],
    },
  ],
};
const month: KeyUsageMonth = {
  start_unix_secs: 1793491200,
  end_unix_secs: 1796083200,
  tokens: 100,
  cost_usd_micros: 0,
  billing_stop: null,
};
const props = {
  data,
  isLoading: false,
  range: '24h' as const,
  onRangeChange: () => {},
  metric: 'cost' as const,
  onMetricChange: () => {},
  provenance,
};

describe('ApiUsageCard estimated pricing', () => {
  it('omits model keys for unpriced and unknown cost buckets', () => {
    render(
      <ApiUsageCard
        {...props}
        month={{ ...month, pricing_completeness: 'unpriced' }}
      />,
    );
    expect(captured.data).toEqual([
      { ts: 0, model: 2.5 },
      { ts: 60 },
      { ts: 120 },
    ]);
    expect(screen.getByText('List price unavailable').className).toBe(
      'text-caption text-text-faint',
    );
    const cost = screen.getByText('Cost at list price').parentElement;
    expect(cost?.textContent).toContain('—');
    expect(cost?.textContent).not.toContain('$0.0000');
  });
  it('uses the same unknown and partial month captions as the credit strip', () => {
    const view = render(
      <ApiUsageCard
        {...props}
        month={{ ...month, pricing_completeness: 'unknown' }}
      />,
    );
    expect(screen.getByText('Price unknown')).toBeDefined();
    view.rerender(
      <ApiUsageCard
        {...props}
        month={{
          ...month,
          pricing_completeness: 'partial',
          cost_usd_micros: 2_500_000,
        }}
      />,
    );
    expect(screen.getByText('$2.5000+')).toBeDefined();
    expect(screen.getByText('Partly priced')).toBeDefined();
  });
  it('keeps provider-reported org bucket values unchanged', () => {
    render(
      <ApiUsageCard
        {...props}
        provenance={{ ...provenance, source: 'reported' }}
        month={{ ...month, pricing_completeness: 'unknown' }}
      />,
    );
    expect(captured.data).toEqual([
      { ts: 0, model: 2.5 },
      { ts: 60, model: 0 },
      { ts: 120, model: 0 },
    ]);
    expect(screen.queryByText('Price unknown')).toBeNull();
    expect(
      screen.getByText('Cost at list price').parentElement?.textContent,
    ).toContain('$0.0000');
  });
  it('keeps token charts populated when list price is unavailable', () => {
    render(<ApiUsageCard {...props} metric="tokens" month={month} />);
    expect(captured.data.map((point) => point.model)).toEqual([100, 100, 100]);
  });
});
