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
  const upstreamNameMap = new Map<string, string>();

  describe('RequestOutcome', () => {
    it('renders Client disconnected for 499 + client_closed_request', () => {
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
          upstreamNameMap={upstreamNameMap}
        />,
      );

      expect(screen.getByText('Client disconnected')).toBeDefined();
      expect(screen.queryByText('499')).toBeNull();
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
          upstreamNameMap={upstreamNameMap}
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
          upstreamNameMap={upstreamNameMap}
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
          upstreamNameMap={upstreamNameMap}
        />,
      );

      expect(screen.queryByText('Client disconnected')).toBeNull();
      expect(screen.getByText('504')).toBeDefined();
    });
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
          upstreamNameMap={upstreamNameMap}
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
          upstreamNameMap={upstreamNameMap}
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
          upstreamNameMap={upstreamNameMap}
        />,
      );

      // Drawer should show final status and duration
      expect(screen.getAllByText('200').length).toBeGreaterThan(0);
      const drawerText = screen.getByRole('dialog').textContent || '';
      expect(drawerText).toMatch(/1,?500\s*ms/);

      // Rerender without the event (e.g. it was removed or scrolled out)
      rerender(
        <RequestEventsTable
          events={[]}
          principalNameMap={principalNameMap}
          upstreamNameMap={upstreamNameMap}
        />,
      );

      // Drawer should close cleanly (or at least not show the stale event)
      expect(screen.queryByText('sess_123')).toBeNull();
    });

    it('shows the service-tier badge only after the final update provides service_tier', async () => {
      cleanup();
      const partialEvent = {
        event_id: 'evt_badges',
        request_id: 'req_badges',
        ts: 1718553120,
        ts_ms: 1718553120000,
        status: 0,
        duration_ms: 0,
        model: 'claude-3-5-sonnet',
        thinking_budget_tokens: 18000,
        _phase: 'partial',
      } satisfies RequestEventWithPhase;

      const { rerender } = render(
        <RequestEventsTable
          events={[partialEvent]}
          principalNameMap={principalNameMap}
          upstreamNameMap={upstreamNameMap}
        />,
      );

      // No service_tier yet → no tier badge
      expect(screen.queryByText('priority')).toBeNull();

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
          upstreamNameMap={upstreamNameMap}
        />,
      );

      // Tier badge appears; reasoning is never shown in the table
      expect(screen.getByText('priority')).toBeDefined();
      expect(screen.queryByText('high · 8.2k')).toBeNull();
    });
  });
});
