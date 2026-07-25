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
import { Sparkline } from './Sparkline';

function render(ui: ReactElement, options?: RenderOptions) {
  const queryClient = new QueryClient();
  return rtlRender(ui, {
    wrapper: ({ children }) => (
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    ),
    ...options,
  });
}

describe('RequestEventsTable', () => {
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

  it('renders two events with the same request_id but distinct event_id as separate rows', () => {
    const events: RequestEventWithPhase[] = [
      {
        event_id: 'evt_1',
        request_id: 'req_same',
        ts: 1718553120,
        ts_ms: 1718553120000,
        status: 200,
        duration_ms: 100,
        _phase: 'final',
      } satisfies RequestEventWithPhase,
      {
        event_id: 'evt_2',
        request_id: 'req_same',
        ts: 1718553121,
        ts_ms: 1718553121000,
        status: 500,
        duration_ms: 150,
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

    // Should render two rows (plus header and sentinel)
    const rows = screen.getAllByRole('row');
    // Header + 2 data rows
    expect(rows.length).toBeGreaterThanOrEqual(3);

    // Check that both statuses are rendered
    expect(screen.getByText('200')).toBeDefined();
    expect(screen.getByText('500')).toBeDefined();
  });

  it('renders only explicit request-kind badges', () => {
    const events: RequestEventWithPhase[] = [
      {
        event_id: 'evt_advisor',
        request_id: 'req_advisor',
        ts: 1718553120,
        status: 200,
        duration_ms: 100,
        model: 'claude-sonnet',
        request_kind: 'advisor',
        _phase: 'final',
      },
      {
        event_id: 'evt_unclassified',
        request_id: 'req_unclassified',
        ts: 1718553121,
        status: 200,
        duration_ms: 100,
        model: 'claude-sonnet',
        _phase: 'final',
      },
    ];

    render(
      <RequestEventsTable
        events={events}
        principalNameMap={principalNameMap}
        upstreamNameMap={upstreamNameMap}
      />,
    );

    expect(screen.getByText('advisor')).toBeDefined();
    expect(screen.queryByText('main')).toBeNull();
  });

  it('supports keyboard navigation and correctly identifies events with colliding request_id', async () => {
    const user = userEvent.setup();
    const events: RequestEventWithPhase[] = [
      {
        event_id: 'evt_1',
        request_id: 'req_same',
        ts: 1718553120,
        ts_ms: 1718553120000,
        status: 200,
        duration_ms: 100,
        _phase: 'final',
      } satisfies RequestEventWithPhase,
      {
        event_id: 'evt_2',
        request_id: 'req_same',
        ts: 1718553121,
        ts_ms: 1718553121000,
        status: 500,
        duration_ms: 150,
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

    const rows = screen.getAllByRole('row');
    // rows[0] is header, rows[1] is evt_1, rows[2] is evt_2
    const row1 = rows[1];
    const row2 = rows[2];

    expect(row1.getAttribute('tabIndex')).toBe('0');
    expect(row2.getAttribute('tabIndex')).toBe('0');

    // Initially neither is selected
    expect(row1.getAttribute('aria-selected')).toBe('false');
    expect(row2.getAttribute('aria-selected')).toBe('false');

    row1.focus();
    await user.keyboard('{Enter}');

    expect(row1.getAttribute('aria-selected')).toBe('true');
    expect(row2.getAttribute('aria-selected')).toBe('false');

    const dialog1 = screen.getByRole('dialog');
    expect(dialog1).toBeDefined();
    expect(dialog1.textContent).toContain('200');

    await user.keyboard('{Escape}');
    expect(screen.queryByRole('dialog')).toBeNull();

    row2.focus();
    await user.keyboard(' ');

    expect(row1.getAttribute('aria-selected')).toBe('false');
    expect(row2.getAttribute('aria-selected')).toBe('true');

    const dialog2 = screen.getByRole('dialog');
    expect(dialog2).toBeDefined();
    expect(dialog2.textContent).toContain('500');

    await user.keyboard('{Escape}');
    expect(screen.queryByRole('dialog')).toBeNull();
  });

  it('handles JS int overflow gracefully', () => {
    const events: RequestEventWithPhase[] = [
      {
        event_id: 'evt_overflow',
        request_id: 'req_overflow',
        ts: 1718553120,
        ts_ms: 1718553120000,
        status: 200,
        duration_ms: 2 ** 53,
        input_tokens: 2 ** 53,
        output_tokens: 2 ** 53,
        cost_usd_micros: 2 ** 53,
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
          ts: 1718553120,
          ts_ms: 1718553120000,
          status: 200,
          duration_ms: 100,
          cost_input_micros: 1000,
          cost_output_micros: 2000,
          cost_cache_creation_5m_micros: 0,
          cost_cache_creation_1h_micros: 0,
          cost_cache_read_micros: 500,
          cost_usd_micros: 3500,
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
          ts: 1718553120,
          ts_ms: 1718553120000,
          status: 200,
          duration_ms: 0,
          cost_input_micros: 0,
          cost_output_micros: 0,
          cost_cache_creation_5m_micros: 0,
          cost_cache_creation_1h_micros: 0,
          cost_cache_read_micros: 0,
          cost_usd_micros: 0,
          _phase: 'partial',
        } satisfies RequestEventWithPhase,
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

  describe('Badges', () => {
    it('does not render the reasoning badge in the table even when reasoning fields are set', () => {
      const events: RequestEventWithPhase[] = [
        {
          event_id: 'evt_reasoning',
          request_id: 'req_reasoning',
          ts: 1718553120,
          ts_ms: 1718553120000,
          status: 200,
          duration_ms: 100,
          model: 'claude-3-5-sonnet',
          reasoning_effort: 'max',
          thinking_budget_tokens: 18000,
          thinking_tokens: 8200,
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

      expect(screen.queryByText('max · 8.2k')).toBeNull();
      expect(
        screen.queryByText(/low ·|medium ·|high ·|xhigh ·|max ·/),
      ).toBeNull();
    });

    it('renders no reasoning badge when thinking_budget_tokens, reasoning_effort, and thinking_tokens are omitted', () => {
      const events: RequestEventWithPhase[] = [
        {
          event_id: 'evt_no_reasoning',
          request_id: 'req_no_reasoning',
          ts: 1718553120,
          ts_ms: 1718553120000,
          status: 200,
          duration_ms: 100,
          model: 'claude-3-5-sonnet',
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

      expect(
        screen.queryByText(/low ·|medium ·|high ·|xhigh ·|max ·/),
      ).toBeNull();
    });

    it('renders priority badge when service_tier is priority', () => {
      const events: RequestEventWithPhase[] = [
        {
          event_id: 'evt_fast',
          request_id: 'req_fast',
          ts: 1718553120,
          ts_ms: 1718553120000,
          status: 200,
          duration_ms: 100,
          model: 'claude-3-5-sonnet',
          service_tier: 'priority',
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

      expect(screen.getByText('priority')).toBeDefined();
    });

    it('renders no badge when service_tier is standard', () => {
      const events: RequestEventWithPhase[] = [
        {
          event_id: 'evt_standard',
          request_id: 'req_standard',
          ts: 1718553120,
          ts_ms: 1718553120000,
          status: 200,
          duration_ms: 100,
          model: 'claude-3-5-sonnet',
          service_tier: 'standard',
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

      expect(screen.queryByText('standard')).toBeNull();
      expect(screen.queryByText('priority')).toBeNull();
    });

    it('renders batch badge when service_tier is batch', () => {
      const events: RequestEventWithPhase[] = [
        {
          event_id: 'evt_batch',
          request_id: 'req_batch',
          ts: 1718553120,
          ts_ms: 1718553120000,
          status: 200,
          duration_ms: 100,
          model: 'claude-3-5-sonnet',
          service_tier: 'batch',
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

      expect(screen.getByText('batch')).toBeDefined();
      expect(screen.queryByText('priority')).toBeNull();
    });

    it('renders flex badge when service_tier is flex', () => {
      const events: RequestEventWithPhase[] = [
        {
          event_id: 'evt_flex',
          request_id: 'req_flex',
          ts: 1718553120,
          ts_ms: 1718553120000,
          status: 200,
          duration_ms: 100,
          model: 'claude-3-5-sonnet',
          service_tier: 'flex',
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

      expect(screen.getByText('flex')).toBeDefined();
      expect(screen.queryByText('priority')).toBeNull();
    });

    it('renders no priority badge when service_tier is omitted', () => {
      const events: RequestEventWithPhase[] = [
        {
          event_id: 'evt_no_tier',
          request_id: 'req_no_tier',
          ts: 1718553120,
          ts_ms: 1718553120000,
          status: 200,
          duration_ms: 100,
          model: 'claude-3-5-sonnet',
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

      expect(screen.queryByText('priority')).toBeNull();
      expect(screen.queryByText('standard')).toBeNull();
      expect(screen.queryByText('batch')).toBeNull();
    });
  });

  describe('Sparkline', () => {
    it('applies Tailwind bg-* utility colors through className', () => {
      const { container } = render(
        <Sparkline segments={[{ value: 10, color: 'bg-sky-400' }]} />,
      );

      const segments = Array.from(container.querySelectorAll('span'));

      expect(segments).toHaveLength(1);
      expect(segments[0]?.className).toContain('bg-sky-400');
      expect(segments[0]?.style.backgroundColor).toBe('');
    });

    it('applies raw CSS colors through inline style', () => {
      const { container } = render(
        <Sparkline segments={[{ value: 20, color: '#ff0000' }]} />,
      );

      const segments = Array.from(container.querySelectorAll('span'));

      expect(segments).toHaveLength(1);
      expect(segments[0]?.className).not.toContain('#ff0000');
      expect(segments[0]?.style.backgroundColor).toBe('rgb(255, 0, 0)');
    });

    it('keeps compound Tailwind bg-* utility colors in className', () => {
      const compoundColor =
        'bg-slate-700/30 bg-[repeating-linear-gradient(45deg,_transparent_0_4px,_rgba(255,255,255,0.05)_4px_8px)]';
      const { container } = render(
        <Sparkline segments={[{ value: 30, color: compoundColor }]} />,
      );

      const segments = Array.from(container.querySelectorAll('span'));

      expect(segments).toHaveLength(1);
      expect(segments[0]?.className).toContain('bg-slate-700/30');
      expect(segments[0]?.className).toContain(
        'bg-[repeating-linear-gradient(45deg,_transparent_0_4px,_rgba(255,255,255,0.05)_4px_8px)]',
      );
      expect(segments[0]?.style.backgroundColor).toBe('');
    });

    it('renders empty track when total is zero or negative', () => {
      const { container } = render(
        <Sparkline
          segments={[
            { value: 0, color: 'bg-sky-400' },
            { value: -10, color: '#ff0000' },
          ]}
        />,
      );

      const segments = container.querySelectorAll('span');
      expect(segments).toHaveLength(0);

      const track = container.querySelector('div');
      expect(track?.className).toContain('bg-overlay-1');
    });
  });
});
