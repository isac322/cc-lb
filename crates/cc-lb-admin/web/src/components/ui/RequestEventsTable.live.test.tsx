import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import {
  cleanup,
  type RenderOptions,
  render as rtlRender,
  screen,
} from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import type { ReactElement } from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { RequestEventWithPhase } from '../../lib/RequestEventTypes';
import { RequestEventsTable } from './RequestEventsTable';

function render(ui: ReactElement, options?: RenderOptions) {
  const queryClient = new QueryClient();
  return rtlRender(ui, {
    wrapper: ({ children }) => (
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    ),
    ...options,
  });
}

describe('RequestEventsTable - Live & Outcomes', () => {
  beforeEach(() => {
    vi.stubGlobal(
      'fetch',
      vi.fn(() => Promise.reject(new Error('network disabled in test'))),
    );
  });

  afterEach(() => {
    cleanup();
    vi.unstubAllGlobals();
  });

  const principalNameMap = new Map<string, string>();

  describe('RequestOutcome', () => {
    it('shows Client disconnected in the status icon popover for 499 + client_closed_request', async () => {
      const user = userEvent.setup();
      const events: RequestEventWithPhase[] = [
        {
          event_id: 'evt_499',
          request_id: 'req_499',
          ts: 1718553120,
          ts_ms: 1718553120000,
          status: 499,
          error_code: 'client_closed_request',
          duration_ms: 100,
          _phase: 'final',
        } satisfies RequestEventWithPhase,
      ];

      render(
        <RequestEventsTable
          events={events}
          principalNameMap={principalNameMap}
        />,
      );

      const icon = screen.getByRole('button', {
        name: 'Client disconnected (HTTP 499)',
      });
      expect(screen.queryByText('Client disconnected')).toBeNull();
      expect(screen.queryByText('499')).toBeNull();

      await user.hover(icon);

      expect(
        await screen.findByText('Client disconnected', {}, { timeout: 1000 }),
      ).toBeDefined();
      expect(
        screen.getByText('HTTP 499 · client_closed_request'),
      ).toBeDefined();
    });

    it('renders numeric status for 499 with other error code', () => {
      const events: RequestEventWithPhase[] = [
        {
          event_id: 'evt_499_other',
          request_id: 'req_499_other',
          ts: 1718553120,
          ts_ms: 1718553120000,
          status: 499,
          error_code: 'other_error',
          duration_ms: 100,
          _phase: 'final',
        } satisfies RequestEventWithPhase,
      ];

      render(
        <RequestEventsTable
          events={events}
          principalNameMap={principalNameMap}
        />,
      );

      expect(screen.queryByText('Client disconnected')).toBeNull();
      expect(screen.getByText('499')).toBeDefined();
    });

    it('renders numeric status for 0/terminal_dropped', () => {
      const events: RequestEventWithPhase[] = [
        {
          event_id: 'evt_0',
          request_id: 'req_0',
          ts: 1718553120,
          ts_ms: 1718553120000,
          status: 0,
          error_code: 'terminal_dropped',
          duration_ms: 100,
          _phase: 'final',
        } satisfies RequestEventWithPhase,
      ];

      render(
        <RequestEventsTable
          events={events}
          principalNameMap={principalNameMap}
        />,
      );

      expect(screen.queryByText('Client disconnected')).toBeNull();
      expect(screen.getAllByText('0').length).toBeGreaterThan(0);
    });

    it('renders numeric status for 504/tower_timeout', () => {
      const events: RequestEventWithPhase[] = [
        {
          event_id: 'evt_504',
          request_id: 'req_504',
          ts: 1718553120,
          ts_ms: 1718553120000,
          status: 504,
          error_code: 'tower_timeout',
          duration_ms: 100,
          _phase: 'final',
        } satisfies RequestEventWithPhase,
      ];

      render(
        <RequestEventsTable
          events={events}
          principalNameMap={principalNameMap}
        />,
      );

      expect(screen.queryByText('Client disconnected')).toBeNull();
      expect(screen.getByText('504')).toBeDefined();
    });
  });

  it('replaces an in-progress row with the final upstream stream error', () => {
    const partialEvent = {
      event_id: 'evt_stream_error',
      request_id: 'req_stream_error',
      ts: 1718553120,
      ts_ms: 1718553120000,
      status: 0,
      duration_ms: 0,
      _phase: 'partial',
    } satisfies RequestEventWithPhase;

    const { container, rerender } = render(
      <RequestEventsTable
        events={[partialEvent]}
        principalNameMap={principalNameMap}
      />,
    );

    const headers = Array.from(
      container.querySelectorAll('thead th'),
      (th) => th.textContent,
    );
    const row = screen.getByLabelText('View request evt_stream_error');
    const cells = row.querySelectorAll('td');
    const statusCell = cells[headers.indexOf('Status')];
    const timeCell = cells[headers.indexOf('Timestamp')];
    // In flight: the status is visually empty and only screen readers hear it;
    // the spinner before the time marks the row instead of a dot.
    expect(statusCell.textContent).toBe('In progress');
    expect(statusCell.querySelector('.sr-only')?.textContent).toBe(
      'In progress',
    );
    expect(timeCell.querySelector('svg')).not.toBeNull();
    expect(timeCell.querySelector('.status-dot')).toBeNull();

    const finalEvent = {
      ...partialEvent,
      status: 200,
      duration_ms: 12,
      error_code: 'upstream_stream_error',
      upstream_error_type: 'overloaded_error',
      upstream_error_message: 'Overloaded',
      _phase: 'final',
    } satisfies RequestEventWithPhase;

    rerender(
      <RequestEventsTable
        events={[finalEvent]}
        principalNameMap={principalNameMap}
      />,
    );

    expect(screen.queryByText('In progress')).toBeNull();
    expect(
      screen.getByRole('button', { name: 'overloaded_error (HTTP 200)' }),
    ).toBeDefined();
    expect(container.querySelector('.status-dot')?.classList).toContain(
      'danger',
    );
  });

  it('keeps an HTTP upstream error as its numeric status', () => {
    const events: RequestEventWithPhase[] = [
      {
        event_id: 'evt_http_error',
        request_id: 'req_http_error',
        ts: 1718553120,
        ts_ms: 1718553120000,
        status: 429,
        error_code: 'upstream_4xx',
        upstream_error_type: 'rate_limit_error',
        duration_ms: 100,
        _phase: 'final',
      } satisfies RequestEventWithPhase,
    ];

    render(
      <RequestEventsTable
        events={events}
        principalNameMap={principalNameMap}
      />,
    );

    expect(screen.getByText('429')).toBeDefined();
    expect(screen.queryByText('rate_limit_error')).toBeNull();
  });

  describe('Drawer Live Updates', () => {
    it('updates an already-open drawer when the event transitions from partial to richer partial to final', async () => {
      cleanup();
      const user = userEvent.setup();
      const partialEvent = {
        event_id: 'evt_live',
        request_id: 'req_live_test',
        ts: 1718553120,
        ts_ms: 1718553120000,
        status: 0,
        duration_ms: 0,
        _phase: 'partial',
      } satisfies RequestEventWithPhase;

      const { rerender } = render(
        <RequestEventsTable
          events={[partialEvent]}
          principalNameMap={principalNameMap}
        />,
      );

      // Open the drawer
      const row = screen.getAllByRole('row')[1];
      if (!row) throw new Error('Row not found');
      await user.click(row);

      // Drawer should show "In progress"
      expect(screen.getAllByText('In progress').length).toBeGreaterThan(0);

      // Rerender with richer partial
      const richerPartialEvent = {
        ...partialEvent,
        thread_id: 'sess_123',
        model: 'claude-3-opus',
        principal_id: 'prin_456',
      } satisfies RequestEventWithPhase;

      rerender(
        <RequestEventsTable
          events={[richerPartialEvent]}
          principalNameMap={principalNameMap}
        />,
      );

      // Drawer should update without reopening
      expect(screen.getAllByText('sess_123').length).toBeGreaterThan(0);
      expect(screen.getAllByText('claude-3-opus').length).toBeGreaterThan(0);
      expect(screen.getAllByText('prin_456').length).toBeGreaterThan(0);
      expect(screen.getAllByText('In progress').length).toBeGreaterThan(0);

      // Rerender with final event
      const finalEvent = {
        ...richerPartialEvent,
        status: 200,
        duration_ms: 1500,
        _phase: 'final',
      } satisfies RequestEventWithPhase;

      rerender(
        <RequestEventsTable
          events={[finalEvent]}
          principalNameMap={principalNameMap}
        />,
      );

      // Drawer should show final status and duration
      expect(screen.getAllByText('200').length).toBeGreaterThan(0);
      const drawerText = screen.getByRole('dialog').textContent || '';
      expect(drawerText).toMatch(/1,?500\s*ms/);

      // Rerender without the event (e.g. it was removed or scrolled out)
      rerender(
        <RequestEventsTable events={[]} principalNameMap={principalNameMap} />,
      );

      // Drawer should close cleanly (or at least not show the stale event)
      expect(screen.queryByText('sess_123')).toBeNull();
    });

    it('keeps requested Gemini thinking level when the final update adds observed service tier', async () => {
      cleanup();
      const partialEvent = {
        event_id: 'evt_badges',
        request_id: 'req_badges',
        ts: 1718553120,
        ts_ms: 1718553120000,
        status: 0,
        duration_ms: 0,
        model: 'gemini-2.5-pro',
        thinking_budget_tokens: 18000,
        thinking_level: 'low',
        _phase: 'partial',
      } satisfies RequestEventWithPhase;

      const { rerender } = render(
        <RequestEventsTable
          events={[partialEvent]}
          principalNameMap={principalNameMap}
        />,
      );

      // No service_tier yet → no tier badge
      expect(screen.queryByText('priority')).toBeNull();
      expect(screen.getByText('low')).toBeDefined();

      // Rerender with final event that includes service_tier
      const finalEvent = {
        ...partialEvent,
        status: 200,
        duration_ms: 1500,
        service_tier: 'priority',
        thinking_tokens: 8200,
        _phase: 'final',
      } satisfies RequestEventWithPhase;

      rerender(
        <RequestEventsTable
          events={[finalEvent]}
          principalNameMap={principalNameMap}
        />,
      );

      // Tier badge appears; reasoning is never shown in the table
      expect(screen.getByText('priority')).toBeDefined();
      expect(screen.getByText('low')).toBeDefined();
      expect(screen.queryByText('high · 8.2k')).toBeNull();
    });
  });
});
