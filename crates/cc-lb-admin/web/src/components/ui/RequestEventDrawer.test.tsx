import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import { RequestEventDrawer } from './RequestEventDrawer';
import type { RequestEventWithPhase } from './RequestEventsTable';

describe('RequestEventDrawer', () => {
  it('updates terminal-only sections when transitioning from partial to final', () => {
    const partialEvent: RequestEventWithPhase = {
      event_id: 'evt_1',
      request_id: 'req_1',
      ts: '2026-07-04T00:00:00Z',
      ts_ms: 1718553120000,
      status: 200,
      _phase: 'partial',
    } as unknown as RequestEventWithPhase;

    const { rerender } = render(
      <RequestEventDrawer
        event={partialEvent}
        principalName={null}
        onClose={() => {}}
      />,
    );

    // While partial, it should show "Live" badge and "In progress" status
    expect(screen.getByText('Live')).toBeDefined();
    expect(screen.getByText('In progress')).toBeDefined();

    // Update to final event
    const finalEvent: RequestEventWithPhase = {
      ...partialEvent,
      _phase: 'final',
      duration_ms: 150,
      error_code: 'upstream_timeout',
    } as unknown as RequestEventWithPhase;

    rerender(
      <RequestEventDrawer
        event={finalEvent}
        principalName={null}
        onClose={() => {}}
      />,
    );

    // "Live" badge and "In progress" should be gone
    expect(screen.queryByText('Live')).toBeNull();
    expect(screen.queryByText('In progress')).toBeNull();

    // Terminal sections should appear
    expect(screen.getByText('upstream_timeout')).toBeDefined();
    expect(screen.getAllByText('150 ms').length).toBeGreaterThan(0);
  });
});
