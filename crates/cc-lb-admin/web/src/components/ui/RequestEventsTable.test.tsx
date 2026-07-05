import { render, screen } from '@testing-library/react';
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
});
