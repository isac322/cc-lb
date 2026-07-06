import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it } from 'vitest';
import {
  RequestEventsTable,
  type RequestEventWithPhase,
} from './RequestEventsTable';

describe('RequestEventsTable', () => {
  const principalNameMap = new Map<string, string>();
  const upstreamNameMap = new Map<string, string>();

  it('renders two events with the same request_id but distinct event_id as separate rows', () => {
    const events: RequestEventWithPhase[] = [
      {
        event_id: 'evt_1',
        request_id: 'req_same',
        ts: '2026-07-04T00:00:00Z',
        ts_ms: 1718553120000,
        status: 200,
        _phase: 'final',
      } as unknown as RequestEventWithPhase,
      {
        event_id: 'evt_2',
        request_id: 'req_same',
        ts: '2026-07-04T00:00:01Z',
        ts_ms: 1718553121000,
        status: 500,
        _phase: 'final',
      } as unknown as RequestEventWithPhase,
    ];

    render(
      <RequestEventsTable
        events={events}
        principalNameMap={principalNameMap}
        upstreamNameMap={upstreamNameMap}
      />,
    );

    // Should render two rows (plus header and sentinel)
    const rows = screen.getAllByRole('row');
    // Header + 2 data rows
    expect(rows.length).toBeGreaterThanOrEqual(3);

    // Check that both statuses are rendered
    expect(screen.getByText('200')).toBeDefined();
    expect(screen.getByText('500')).toBeDefined();
  });

  it('handles JS int overflow gracefully', () => {
    const events: RequestEventWithPhase[] = [
      {
        event_id: 'evt_overflow',
        request_id: 'req_overflow',
        ts: '2026-07-04T00:00:00Z',
        ts_ms: 1718553120000,
        status: 200,
        duration_ms: 2 ** 53,
        input_tokens: 2 ** 53,
        output_tokens: 2 ** 53,
        cost_usd_micros: 2 ** 53,
        _phase: 'final',
      } as unknown as RequestEventWithPhase,
    ];

    render(
      <RequestEventsTable
        events={events}
        principalNameMap={principalNameMap}
        upstreamNameMap={upstreamNameMap}
      />,
    );

    const text = screen.getAllByRole('table')[0].textContent || '';
    expect(text).not.toContain('NaN');
    expect(text).not.toContain('Infinity');
    expect(text).not.toMatch(/(?<![a-zA-Z])-0(?![a-zA-Z0-9])/);
    expect(text).not.toContain('undefined');

    // Should contain em-dash for overflowed values
    const dashes = screen.getAllByText('—');
    expect(dashes.length).toBeGreaterThan(0);
  });

  describe('Cost Tooltip', () => {
    it('renders 5 rows with $0.0000 formatting for each sub-cost and sums correctly', async () => {
      const user = userEvent.setup();
      const events: RequestEventWithPhase[] = [
        {
          event_id: 'evt_cost_1',
          request_id: 'req_cost_1',
          ts: '2026-07-04T00:00:00Z',
          ts_ms: 1718553120000,
          status: 200,
          cost_input_micros: 1000,
          cost_output_micros: 2000,
          cost_cache_creation_5m_micros: 0,
          cost_cache_creation_1h_micros: 0,
          cost_cache_read_micros: 500,
          cost_usd_micros: 3500,
          _phase: 'final',
        } as unknown as RequestEventWithPhase,
      ];

      render(
        <RequestEventsTable
          events={events}
          principalNameMap={principalNameMap}
          upstreamNameMap={upstreamNameMap}
        />,
      );

      // Find the cost cell (it should display $0.0035)
      const costCell = screen.getByText('$0.0035');
      expect(costCell).toBeDefined();

      // Hover over the cost cell to trigger the tooltip
      await user.hover(costCell);

      // Wait for the popover to appear (Hint has a 200ms delay)
      const inputLabel = await screen.findAllByText(
        'Input',
        {},
        { timeout: 1000 },
      );
      expect(inputLabel.length).toBeGreaterThan(0);

      // Check tooltip contents
      const tooltips = await screen.findAllByText('Cost');
      expect(tooltips.length).toBeGreaterThan(0);

      // Check all 5 rows are present
      expect(screen.getAllByText('Output').length).toBeGreaterThan(0);
      expect(screen.getAllByText('Cache create 5m').length).toBeGreaterThan(0);
      expect(screen.getAllByText('Cache create 1h').length).toBeGreaterThan(0);
      expect(screen.getAllByText('Cache read').length).toBeGreaterThan(0);

      // Check values
      expect(screen.getAllByText('$0.0010').length).toBeGreaterThan(0);
      expect(screen.getAllByText('$0.0020').length).toBeGreaterThan(0);
      expect(screen.getAllByText('$0.0005').length).toBeGreaterThan(0);

      // Check that 0 values are formatted as $0.0000
      const zeroValues = screen.getAllByText('$0.0000');
      expect(zeroValues.length).toBeGreaterThanOrEqual(2);

      // Check total
      expect(screen.getAllByText('Total').length).toBeGreaterThan(0);
      // The total value $0.0035 should appear twice (once in cell, once in tooltip)
      const totals = screen.getAllByText('$0.0035');
      expect(totals.length).toBeGreaterThanOrEqual(2);
    });

    it('shows em-dash for partial rows', async () => {
      const user = userEvent.setup();
      const events: RequestEventWithPhase[] = [
        {
          event_id: 'evt_cost_partial',
          request_id: 'req_cost_partial',
          ts: '2026-07-04T00:00:00Z',
          ts_ms: 1718553120000,
          status: 200,
          cost_input_micros: 0,
          cost_output_micros: 0,
          cost_cache_creation_5m_micros: 0,
          cost_cache_creation_1h_micros: 0,
          cost_cache_read_micros: 0,
          cost_usd_micros: 0,
          _phase: 'partial',
        } as unknown as RequestEventWithPhase,
      ];

      render(
        <RequestEventsTable
          events={events}
          principalNameMap={principalNameMap}
          upstreamNameMap={upstreamNameMap}
        />,
      );

      // Find the cost cell (it should display Est. —)
      const costCell = screen.getByText('Est. —');
      expect(costCell).toBeDefined();

      // Hover over the cost cell to trigger the tooltip
      await user.hover(costCell);

      // Wait for the popover to appear
      const inputLabel = await screen.findAllByText(
        'Input',
        {},
        { timeout: 1000 },
      );
      expect(inputLabel.length).toBeGreaterThan(0);

      // Check tooltip contents
      const tooltips = await screen.findAllByText('Estimated Cost');
      expect(tooltips.length).toBeGreaterThan(0);

      // Check all 5 rows are present
      expect(screen.getAllByText('Output').length).toBeGreaterThan(0);
      expect(screen.getAllByText('Cache create 5m').length).toBeGreaterThan(0);
      expect(screen.getAllByText('Cache create 1h').length).toBeGreaterThan(0);
      expect(screen.getAllByText('Cache read').length).toBeGreaterThan(0);

      // Check that values are em-dashes
      const dashes = screen.getAllByText('—');
      // 5 rows + 1 total + 1 cell = 7 dashes
      expect(dashes.length).toBeGreaterThanOrEqual(7);
    });
  });
});
