import {
  cleanup,
  fireEvent,
  render,
  screen,
  within,
} from '@testing-library/react';
import { type ReactNode, useState } from 'react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import type {
  DashboardUsageResponse,
  KeyUsageMonth,
  KeyUsageProvenance,
  UsageBucket,
} from '../../../lib/api';
import { sumBucketPricing } from '../../../lib/pricingCompleteness';
import { ApiUsageCard } from '../ApiUsageCard';

type ChartPoint = Record<string, unknown>;
type ChartDataKey = string | number | ((point: ChartPoint) => unknown);
type ChartSeries = { dataKey: ChartDataKey; name?: string };
const { getValueByDataKey } = await vi.importActual<{
  getValueByDataKey: (point: ChartPoint, key: ChartDataKey) => unknown;
}>('recharts/lib/util/ChartUtils');

type TooltipFormatter = (
  value: unknown,
  name: unknown,
  item: { payload: ChartPoint; dataKey: ChartDataKey },
) => [string, string];
const captured = vi.hoisted(() => ({
  data: [] as ChartPoint[],
  series: [] as ChartSeries[],
  xDataKey: undefined as ChartDataKey | undefined,
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
  Area: ({ dataKey, name }: ChartSeries) => {
    captured.series.push({ dataKey, name });
    return null;
  },
  CartesianGrid: () => null,
  XAxis: ({ dataKey }: { dataKey: ChartDataKey }) => {
    captured.xDataKey = dataKey;
    return null;
  },
  YAxis: () => null,
  Tooltip: ({ formatter }: { formatter: TooltipFormatter }) => {
    captured.formatter = formatter;
    const [readings, setReadings] = useState<string[]>([]);
    return (
      <>
        <button
          type="button"
          onClick={() => {
            setReadings(
              captured.data.flatMap((point) =>
                captured.series.flatMap((series) => {
                  const value = getValueByDataKey(point, series.dataKey);
                  if (value == null) return [];
                  const [amount, name] = formatter(
                    value,
                    series.name ?? series.dataKey,
                    { payload: point, dataKey: series.dataKey },
                  );
                  return [`${name}: ${amount}`];
                }),
              ),
            );
          }}
        >
          Show chart tooltip
        </button>
        <div data-testid="api-chart-tooltip">
          {readings.map((reading, index) => (
            <span key={index}>{reading}</span>
          ))}
        </div>
      </>
    );
  },
}));
afterEach(() => {
  cleanup();
  captured.data = [];
  captured.series = [];
  captured.xDataKey = undefined;
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

function plottedValues(seriesIndex = 0) {
  return captured.data.map((point) =>
    getValueByDataKey(point, captured.series[seriesIndex].dataKey),
  );
}

function chartTimestamps() {
  return captured.data.map((point) =>
    getValueByDataKey(point, captured.xDataKey!),
  );
}

function tooltipReading(timestamp: number, label: string) {
  const series = captured.series.find(
    (series) => (series.name ?? series.dataKey) === label,
  )!;
  const point = captured.data.find(
    (point) => getValueByDataKey(point, captured.xDataKey!) === timestamp,
  )!;
  return captured.formatter?.(getValueByDataKey(point, series.dataKey), label, {
    payload: point,
    dataKey: series.dataKey,
  });
}

describe('ApiUsageCard estimated pricing', () => {
  describe('arbitrary model keys stay separate from chart metadata', () => {
    for (const model of [
      'ts',
      '__proto__',
      'constructor',
      'model.version',
      'model[0]',
    ]) {
      it.each([
        ['unknown', 'estimated', 'cost', 0, '—'],
        ['unpriced', 'estimated', 'cost', 0, '—'],
        ['known_zero', 'estimated', 'cost', 0, '$0.0000'],
        ['priced', 'estimated', 'cost', 2_500_000, '$2.5000'],
        ['unknown', 'reported', 'cost', 2_500_000, '$2.5000'],
        ['unknown', 'stale', 'cost', 2_500_000, '$2.5000'],
        ['unknown', 'estimated', 'tokens', 0, '100'],
        ['unknown', 'reported', 'tokens', 0, '100'],
      ] as const)(
        `${model}: %s %s %s preserves timestamps and honest readings`,
        (state, source, metric, cost, expected) => {
          const timestamp = 1_794_654_000;
          render(
            <ApiUsageCard
              {...props}
              metric={metric}
              provenance={{ ...provenance, source }}
              data={{
                ...data,
                series: [
                  {
                    key: model,
                    buckets: [
                      {
                        ...bucket,
                        bucket_start_unix_secs: timestamp,
                        pricing_completeness: state,
                        virtual_cost_micros: cost,
                      },
                    ],
                  },
                ],
              }}
            />,
          );
          const legend = screen.getByTestId('api-usage-legend-slot');
          expect(
            within(legend).getByText(model, { exact: true }),
          ).toBeDefined();
          expect(
            within(legend).getByText(expected, { exact: true }),
          ).toBeDefined();
          expect(legend.textContent).not.toContain('1794654000');
          expect(legend.textContent).not.toContain('NaN');
          expect(captured.xDataKey).toBeDefined();
          expect(getValueByDataKey(captured.data[0], captured.xDataKey!)).toBe(
            timestamp,
          );
          expect(captured.series).toHaveLength(1);
          const plotted = getValueByDataKey(
            captured.data[0],
            captured.series[0].dataKey,
          );
          const unavailable =
            metric === 'cost' &&
            source === 'estimated' &&
            (state === 'unknown' || state === 'unpriced');
          if (unavailable) {
            expect(plotted == null).toBe(true);
          } else {
            expect(plotted).toBe(metric === 'tokens' ? 100 : cost / 1_000_000);
          }
          fireEvent.click(
            screen.getByRole('button', { name: 'Show chart tooltip' }),
          );
          const tooltip = screen.getByTestId('api-chart-tooltip');
          expect(tooltip.textContent).toBe(
            unavailable ? '' : `${model}: ${expected}`,
          );
          expect(tooltip.textContent).not.toContain('1794654000');
          expect(tooltip.textContent).not.toContain('NaN');
        },
      );
    }

    it('qualifies partial ts independently from an ordinary model at the same timestamp', () => {
      const timestamp = 1_794_654_000;
      render(
        <ApiUsageCard
          {...props}
          data={{
            ...data,
            series: [
              {
                key: 'ts',
                buckets: [
                  {
                    ...bucket,
                    bucket_start_unix_secs: timestamp,
                    request_count: 2,
                    virtual_cost_micros: 2_500_000,
                    pricing_completeness: 'partial',
                    priced_request_count: 1,
                    unpriced_request_count: 1,
                  },
                ],
              },
              {
                key: 'ordinary',
                buckets: [
                  {
                    ...bucket,
                    bucket_start_unix_secs: timestamp,
                    virtual_cost_micros: 1_000_000,
                    pricing_completeness: 'priced',
                    priced_request_count: 1,
                  },
                ],
              },
            ],
          }}
        />,
      );
      expect(chartTimestamps()).toEqual([timestamp]);
      expect(plottedValues(0)).toEqual([2.5]);
      expect(plottedValues(1)).toEqual([1]);
      const legend = screen.getByTestId('api-usage-legend-slot');
      const partial = within(legend).getByText('ts', { exact: true })
        .parentElement!;
      const ordinary = within(legend).getByText('ordinary', { exact: true })
        .parentElement!;
      expect(partial.textContent).toBe('ts$2.5000+ (1 unpriced)');
      expect(ordinary.textContent).toBe('ordinary$1.0000');
      expect(tooltipReading(timestamp, 'ts')).toEqual([
        '$2.5000+ (1 unpriced)',
        'ts',
      ]);
      expect(tooltipReading(timestamp, 'ordinary')).toEqual([
        '$1.0000',
        'ordinary',
      ]);
      fireEvent.click(
        screen.getByRole('button', { name: 'Show chart tooltip' }),
      );
      expect(screen.getByTestId('api-chart-tooltip').textContent).toBe(
        'ts: $2.5000+ (1 unpriced)ordinary: $1.0000',
      );
    });
  });

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
      expect(chartTimestamps()).toEqual([0]);
      expect(plottedValues()).toEqual([
        figure === '—' ? undefined : legacy.virtual_cost_micros / 1_000_000,
      ]);
      if (figure !== '—') {
        expect(tooltipReading(0, 'model')).toEqual([
          `${figure} (${caption})`,
          'model',
        ]);
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
    expect(tooltipReading(60, 'model')).toEqual([
      '$2.5000+ (1 unpriced)',
      'model',
    ]);
    expect(tooltipReading(0, 'model')).toEqual(['$2.5000', 'model']);
    expect(tooltipReading(60, 'zero')).toEqual(['$0.0000', 'zero']);
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
      expect(tooltipReading(0, 'model')).toEqual(['$2.5000', 'model']);
    },
  );

  it('leaves gaps for unpriced and unknown cost buckets', () => {
    render(
      <ApiUsageCard
        {...props}
        month={{ ...month, pricing_completeness: 'unpriced' }}
      />,
    );
    expect(chartTimestamps()).toEqual([0, 60, 120]);
    expect(plottedValues()).toEqual([2.5, undefined, undefined]);
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
    expect(chartTimestamps()).toEqual([0, 60, 120]);
    expect(plottedValues()).toEqual([2.5, 0, 0]);
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
      expect(chartTimestamps()).toEqual([0]);
      expect(plottedValues()).toEqual([0]);
      expect(screen.queryByText('Price unknown')).toBeNull();
      expect(screen.queryByText('Partly priced')).toBeNull();
    },
  );
  it('keeps token charts populated when list price is unavailable', () => {
    render(<ApiUsageCard {...props} metric="tokens" month={month} />);
    expect(plottedValues()).toEqual([100, 100, 100]);
  });
});
