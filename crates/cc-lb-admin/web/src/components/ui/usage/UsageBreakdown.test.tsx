import { cleanup, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it } from 'vitest';
import type { RequestEventWithPhase } from '../../../lib/RequestEventTypes';
import { CostBreakdown } from './UsageBreakdown';

const event = {
  request_id: 'cost-test',
  ts: 1,
  status: 200,
  duration_ms: 1,
  _phase: 'final',
} satisfies RequestEventWithPhase;
afterEach(cleanup);

describe('CostBreakdown recorded amounts', () => {
  it.each([null, undefined])(
    'keeps missing cost %s unavailable',
    (cost_usd_micros) => {
      render(<CostBreakdown event={{ ...event, cost_usd_micros }} />);
      expect(screen.getByText('List price unavailable').className).toBe(
        'text-body-sm text-text-muted',
      );
      expect(screen.queryByText('No cost')).toBeNull();
      expect(screen.queryByText('Total')).toBeNull();
      expect(screen.queryByText('$0.0000')).toBeNull();
    },
  );
  it('reserves No cost for an explicit zero', () => {
    render(<CostBreakdown event={{ ...event, cost_usd_micros: 0 }} />);
    expect(screen.getByText('No cost')).toBeDefined();
  });
  it('shows a recorded total without inventing a category split', () => {
    render(<CostBreakdown event={{ ...event, cost_usd_micros: 2_500_000 }} />);
    expect(screen.getByText('Total')).toBeDefined();
    expect(screen.getByText('$2.5000')).toBeDefined();
    expect(screen.getByText('Per-category cost not recorded')).toBeDefined();
    expect(screen.queryByText('No cost')).toBeNull();
  });
  it('preserves priced category and total rows', () => {
    render(
      <CostBreakdown
        event={{
          ...event,
          cost_usd_micros: 2_500_000,
          cost_input_micros: 1_000_000,
          cost_output_micros: 1_500_000,
        }}
      />,
    );
    expect(screen.getByText('$1.0000')).toBeDefined();
    expect(screen.getByText('$1.5000')).toBeDefined();
    expect(screen.getByText('$2.5000')).toBeDefined();
    expect(screen.queryByText('Per-category cost not recorded')).toBeNull();
  });
});
