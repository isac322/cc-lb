import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it } from 'vitest';
import type { RequestEventWithPhase } from '../../lib/RequestEventTypes';
import { CostCell } from './CostCell';

afterEach(cleanup);

const baseEvent = {
  ts: 1,
  request_id: 'req-cost',
  status: 200,
  duration_ms: 100,
  _phase: 'final',
} satisfies RequestEventWithPhase;

function renderCost(event: RequestEventWithPhase) {
  render(
    <table>
      <tbody>
        <tr>
          <CostCell event={event} />
        </tr>
      </tbody>
    </table>,
  );
  const trigger = screen.getByRole('button', {
    name: /Cost .* show breakdown/,
  });
  fireEvent.click(trigger);
}

describe('CostCell breakdown', () => {
  it('omits absent and zero categories while keeping positive amounts', () => {
    renderCost({
      ...baseEvent,
      cost_usd_micros: 6_000,
      cost_input_micros: 1_000,
      cost_output_micros: 2_000,
      cost_cache_creation_5m_micros: 0,
      cost_cache_read_micros: 3_000,
    });

    expect(screen.getByText('Input')).toBeDefined();
    expect(screen.getByText('Output')).toBeDefined();
    expect(screen.getByText('Cache read')).toBeDefined();
    expect(screen.queryByText('Cache create 5m')).toBeNull();
    expect(screen.queryByText('Cache create 1h')).toBeNull();
  });

  it('shows a positive one-hour cache cost with its authoritative amount', () => {
    renderCost({
      ...baseEvent,
      cost_usd_micros: 4_000,
      cost_input_micros: 1_000,
      cost_cache_creation_1h_micros: 3_000,
    });

    expect(screen.getByText('Cache create 1h')).toBeDefined();
    expect(screen.getByText('$0.0030')).toBeDefined();
    expect(screen.queryByText('Cache create 5m')).toBeNull();
  });
});
