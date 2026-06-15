// Regression: ApiUsageCard rendered its legend (Tokens/Cost colour swatches)
// only inside the loaded branch. When loading finished, the legend popped
// into existence below the chart, pushing every section underneath down by
// its own height (~28px). Loading → loaded therefore caused a visible jump
// for every component below the card. This test pins the fix: a legend slot
// reserved in every state (loading, empty, loaded) with `min-h-[28px]`.

import { cleanup, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, test } from 'vitest';
import type { DashboardUsageResponse } from '../../../lib/api';
import { ApiUsageCard } from '../ApiUsageCard';

afterEach(cleanup);

const baseProps = {
  range: '24h' as const,
  onRangeChange: () => {},
  metric: 'tokens' as const,
  onMetricChange: () => {},
};

const emptyData: DashboardUsageResponse = {
  range: '24h',
  step: 'hour',
  group_by: 'model',
  series: [],
} as unknown as DashboardUsageResponse;

const populatedData: DashboardUsageResponse = {
  range: '24h',
  step: 'hour',
  group_by: 'model',
  series: [
    {
      key: 'claude-sonnet-4',
      buckets: [
        {
          bucket_start_unix_secs: 1_700_000_000,
          input_tokens: 100,
          output_tokens: 50,
          virtual_cost_micros: 1_500_000,
        },
      ],
    },
  ],
} as unknown as DashboardUsageResponse;

describe('ApiUsageCard legend slot reservation', () => {
  test('reserves legend slot while loading', () => {
    render(<ApiUsageCard data={undefined} isLoading {...baseProps} />);
    const slot = screen.getByTestId('api-usage-legend-slot');
    expect(slot.className).toContain('min-h-[28px]');
  });

  test('reserves legend slot when data is empty', () => {
    render(<ApiUsageCard data={emptyData} isLoading={false} {...baseProps} />);
    const slot = screen.getByTestId('api-usage-legend-slot');
    expect(slot.className).toContain('min-h-[28px]');
  });

  test('renders legend slot with model name when data has series', () => {
    render(
      <ApiUsageCard data={populatedData} isLoading={false} {...baseProps} />,
    );
    const slot = screen.getByTestId('api-usage-legend-slot');
    expect(slot.className).toContain('min-h-[28px]');
    expect(slot.textContent).toContain('claude-sonnet-4');
  });
});
