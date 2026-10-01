import {
  cleanup,
  fireEvent,
  render,
  screen,
  within,
} from '@testing-library/react';
import { afterEach, describe, expect, it } from 'vitest';
import type { RequestEventWithPhase } from '../../lib/RequestEventTypes';
import { BreakdownPopover } from './BreakdownPopover';
import { CostCell } from './CostCell';

afterEach(cleanup);

const baseEvent = {
  ts: 1,
  request_id: 'req-cost',
  status: 200,
  duration_ms: 100,
  _phase: 'final',
} satisfies RequestEventWithPhase;

function renderCostCell(event: RequestEventWithPhase) {
  return render(
    <table>
      <tbody>
        <tr>
          <CostCell event={event} />
        </tr>
      </tbody>
    </table>,
  );
}

function renderCost(event: RequestEventWithPhase) {
  renderCostCell(event);
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

describe('CostCell alignment', () => {
  it('ends the figure on the cell edge while staying a breakdown button', () => {
    const { container } = renderCostCell({
      ...baseEvent,
      cost_usd_micros: 4_000,
    });

    const cell = container.querySelector('td');
    const classes = cell?.className.split(/\s+/) ?? [];
    // The trigger drops only its right inset, so the last digit lands on the
    // cell's own (header-matching) right edge.
    expect(classes).toContain('text-right');
    expect(classes).toContain('*:pr-0!');

    const trigger = within(cell as HTMLElement).getByRole('button', {
      name: 'Cost $0.0040, show breakdown',
    });
    expect(trigger.parentElement).toBe(cell);
    expect(trigger.getAttribute('type')).toBe('button');
  });

  it('keeps the empty placeholder on the same edge', () => {
    const { container } = renderCostCell(baseEvent);

    const cell = container.querySelector('td');
    expect(cell?.className.split(/\s+/)).toContain('*:pr-0!');
    expect(cell?.textContent).toBe('—');
    expect(screen.queryByRole('button')).toBeNull();
  });
});

describe('CostCell breakdown layout', () => {
  it.each([
    ['zero', 0, '$0.0000'],
    ['small', 1, '$0.0000'],
    ['large', 123_456_789_012, '$123,456.7890'],
  ])('shows a %s total unclipped', (_size, micros, expected) => {
    renderCost({ ...baseEvent, cost_usd_micros: micros });

    const value = screen
      .getAllByText(expected)
      .find((element) => element.className.includes('min-w-14'));
    expect(value).toBeDefined();
    const className = value?.className ?? '';
    // `min-w-14` is a floor; a fixed `w-14` would clip long amounts.
    expect(className.split(/\s+/)).not.toContain('w-14');
    expect(className).toContain('shrink-0');
    expect(className).toContain('whitespace-nowrap');
    expect(className).toContain('text-right');
  });

  it('puts long micro-dollar values in the same decimal column', () => {
    render(
      <BreakdownPopover
        title="Cost"
        rows={[
          { label: 'Input', value: 1_500_000, color: 'red', fmt: () => '$1.5' },
          {
            label: 'Output',
            value: 5,
            color: 'blue',
            fmt: () => '$0.000005',
          },
        ]}
        footer={{ label: 'Total', value: 1_500_005, fmt: () => '$1.500005' }}
      />,
    );

    const short = screen.getByText('$1.5');
    const long = screen.getByText('$0.000005');
    const total = screen.getByText('$1.500005');

    // The short figure is padded with five invisible digits to the shared
    // six-digit fraction, so all three decimal points share one column.
    const pad = short.querySelector('[data-slot="decimal-pad"]');
    expect(pad?.textContent).toBe('00000');
    expect(pad?.className).toContain('invisible');
    expect(pad?.getAttribute('aria-hidden')).toBe('true');
    expect(long.querySelector('[data-slot="decimal-pad"]')).toBeNull();
    expect(total.querySelector('[data-slot="decimal-pad"]')).toBeNull();

    for (const value of [short, long, total]) {
      expect(value.className.split(/\s+/)).not.toContain('w-14');
      expect(value.className).toContain('text-right');
      expect(value.className).toContain('tabular-nums');
    }
  });

  it('leaves figures without a decimal point unpadded', () => {
    render(
      <BreakdownPopover
        title="Tokens"
        rows={[
          { label: 'Input', value: 2_800, color: 'red', fmt: () => '2.8k' },
          { label: 'Output', value: 812, color: 'blue', fmt: () => '812' },
        ]}
      />,
    );

    expect(
      screen.getByText('812').querySelector('[data-slot="decimal-pad"]'),
    ).toBeNull();
  });
});
