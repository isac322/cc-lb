import { cleanup, render, screen, within } from '@testing-library/react';
import type { ReactNode } from 'react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import type {
  DashboardUsageResponse,
  KeyUsageMonth,
  KeyUsageProvenance,
  UsageBucket,
} from '../../../lib/api';
import { sumBucketPricing } from '../../../lib/pricingCompleteness';
import { ApiUsageCard } from '../ApiUsageCard';

type TooltipFormatter = (
  value: unknown,
  name: unknown,
  item: { payload: { ts: number } },
) => [string, string];
const captured = vi.hoisted(() => ({
  data: [] as Record<string, number>[],
  formatter: undefined as TooltipFormatter | undefined,
}));
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
  Tooltip: ({ formatter }: { formatter: TooltipFormatter }) => {
    captured.formatter = formatter;
    return null;
  },
}));
afterEach(() => {
  cleanup();
  captured.data = [];
  captured.formatter = undefined;
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
  it.each([
    ['legacy all-zero/native omitted', {}, '—', 'Price unknown'],
    [
      'legacy plus unpriced',
      { request_count: 2, unpriced_request_count: 1 },
      '—',
      'Price unknown',
    ],
    [
      'legacy plus auxiliary',
      { request_count: 2, unmetered_request_count: 1 },
      '—',
      'Price unknown',
    ],
    [
      'legacy plus priced zero',
      { request_count: 2, priced_request_count: 1 },
      '$0.0000+',
      'Partly priced',
    ],
    [
      'positive legacy',
      { virtual_cost_micros: 2_500_000 },
      '$2.5000+',
      'Partly priced',
    ],
  ] as const)(
    'keeps %s honest in the month, legend and chart',
    (_name, overrides, figure, caption) => {
      const legacy = { ...bucket, input_tokens: 0, ...overrides };
      const pricing = sumBucketPricing([legacy]);
      render(
        <ApiUsageCard
          {...props}
          data={{ ...data, series: [{ key: 'model', buckets: [legacy] }] }}
          month={{
            ...month,
            ...pricing,
            cost_usd_micros: legacy.virtual_cost_micros,
          }}
        />,
      );
      expect(
        screen.getByText('Cost at list price').parentElement?.textContent,
      ).toContain(figure);
      expect(screen.getByText(caption)).toBeDefined();
      expect(screen.getByTestId('api-usage-legend-slot').textContent).toContain(
        figure,
      );
      expect(captured.data).toEqual([
        figure === '—'
          ? { ts: 0 }
          : { ts: 0, model: legacy.virtual_cost_micros / 1_000_000 },
      ]);
      if (figure !== '—') {
        expect(
          captured.formatter?.(
            legacy.virtual_cost_micros / 1_000_000,
            'model',
            { payload: { ts: 0 } },
          ),
        ).toEqual([`${figure} (${caption})`, 'model']);
      }
      expect(screen.queryByText('No billable usage')).toBeNull();
    },
  );

  it('qualifies each partial estimated point in the legend and chart tooltip', () => {
    const partial: UsageBucket = {
      ...bucket,
      bucket_start_unix_secs: 60,
      request_count: 2,
      virtual_cost_micros: 2_500_000,
      pricing_completeness: 'partial',
      priced_request_count: 1,
      unpriced_request_count: 1,
      unmetered_request_count: 0,
    };
    render(
      <ApiUsageCard
        {...props}
        data={{
          ...data,
          series: [
            { key: 'model', buckets: [data.series[0].buckets[0], partial] },
            {
              key: 'zero',
              buckets: [
                {
                  ...partial,
                  pricing_completeness: 'known_zero',
                  virtual_cost_micros: 0,
                },
              ],
            },
          ],
        }}
      />,
    );
    const legend = screen.getByTestId('api-usage-legend-slot');
    expect(within(legend).getByText('$2.5000+')).toBeDefined();
    const qualified = within(legend).getByTitle('1 unpriced');
    expect(qualified.getAttribute('aria-label')).toBe('$2.5000+ (1 unpriced)');
    expect(qualified.textContent).toBe('$2.5000+ (1 unpriced)');
    expect(within(legend).getByText('$0.0000')).toBeDefined();
    expect(captured.formatter?.(2.5, 'model', { payload: { ts: 60 } })).toEqual(
      ['$2.5000+ (1 unpriced)', 'model'],
    );
    expect(captured.formatter?.(2.5, 'model', { payload: { ts: 0 } })).toEqual([
      '$2.5000',
      'model',
    ]);
    expect(captured.formatter?.(0, 'zero', { payload: { ts: 60 } })).toEqual([
      '$0.0000',
      'zero',
    ]);
  });

  it.each(['reported', 'stale'] as const)(
    'leaves partial %s legend and tooltip values unchanged',
    (source) => {
      render(
        <ApiUsageCard
          {...props}
          provenance={{ ...provenance, source }}
          data={{
            ...data,
            series: [
              {
                key: 'model',
                buckets: [
                  {
                    ...bucket,
                    pricing_completeness: 'partial',
                    virtual_cost_micros: 2_500_000,
                  },
                ],
              },
            ],
          }}
        />,
      );
      const legend = screen.getByTestId('api-usage-legend-slot');
      expect(within(legend).getByText('$2.5000')).toBeDefined();
      expect(within(legend).queryByText('$2.5000+')).toBeNull();
      expect(
        captured.formatter?.(2.5, 'model', { payload: { ts: 0 } }),
      ).toEqual(['$2.5000', 'model']);
    },
  );

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
  it.each(['known_zero', 'unmetered'] as const)(
    'preserves the fully classified %s estimated zero',
    (state) => {
      render(
        <ApiUsageCard
          {...props}
          data={{
            ...data,
            series: [
              {
                key: 'model',
                buckets: [
                  {
                    ...bucket,
                    input_tokens: 0,
                    pricing_completeness: state,
                    priced_request_count: state === 'known_zero' ? 1 : 0,
                    unmetered_request_count: state === 'unmetered' ? 1 : 0,
                  },
                ],
              },
            ],
          }}
          month={{ ...month, pricing_completeness: state }}
        />,
      );
      expect(
        screen.getByText('Cost at list price').parentElement?.textContent,
      ).toContain('$0.0000');
      expect(screen.getByTestId('api-usage-legend-slot').textContent).toContain(
        '$0.0000',
      );
      expect(captured.data).toEqual([{ ts: 0, model: 0 }]);
      expect(screen.queryByText('Price unknown')).toBeNull();
      expect(screen.queryByText('Partly priced')).toBeNull();
    },
  );
  it('keeps token charts populated when list price is unavailable', () => {
    render(<ApiUsageCard {...props} metric="tokens" month={month} />);
    expect(captured.data.map((point) => point.model)).toEqual([100, 100, 100]);
  });
});
