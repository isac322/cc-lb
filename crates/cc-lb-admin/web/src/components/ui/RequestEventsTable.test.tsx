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
import type * as FormatModule from '../../lib/format';
import type { RequestEventWithPhase } from '../../lib/RequestEventTypes';
import { RequestEventsTable } from './RequestEventsTable';

type GetRequestOutcome = typeof FormatModule.getRequestOutcome;

const { requestOutcomeWork } = vi.hoisted(() => ({
  requestOutcomeWork: vi.fn(),
}));

vi.mock('../../lib/format', async (importOriginal) => {
  const actual = await importOriginal<typeof FormatModule>();
  return {
    ...actual,
    getRequestOutcome: (...args: Parameters<GetRequestOutcome>) => {
      requestOutcomeWork();
      return actual.getRequestOutcome(...args);
    },
  };
});

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
    requestOutcomeWork.mockClear();
  });

  afterEach(() => {
    cleanup();
    vi.unstubAllGlobals();
  });

  const principalNameMap = new Map<string, string>();
  const upstreamNameMap = new Map<string, string>();

  it('skips unchanged row work across identical and single-event updates', () => {
    const events: RequestEventWithPhase[] = Array.from(
      { length: 200 },
      (_, index) => ({
        event_id: `evt_${index}`,
        request_id: `req_${index}`,
        ts: 1718553120 + index,
        ts_ms: 1718553120000 + index * 1000,
        status: 200,
        duration_ms: 100 + index,
        _phase: 'final' as const,
      }),
    );
    const { container, rerender } = render(
      <RequestEventsTable
        events={events}
        principalNameMap={principalNameMap}
        upstreamNameMap={upstreamNameMap}
      />,
    );

    expect(requestOutcomeWork).toHaveBeenCalledTimes(200);
    const unchangedRow = container.querySelector<HTMLTableRowElement>(
      'tr[aria-label="View request evt_72"]',
    );
    const changedRow = container.querySelector<HTMLTableRowElement>(
      'tr[aria-label="View request evt_73"]',
    );
    if (!unchangedRow || !changedRow) throw new Error('Expected request rows');

    requestOutcomeWork.mockClear();
    rerender(
      <RequestEventsTable
        events={events}
        principalNameMap={principalNameMap}
        upstreamNameMap={upstreamNameMap}
      />,
    );
    expect(requestOutcomeWork).not.toHaveBeenCalled();

    const changedEvents = [...events];
    const changedEvent = changedEvents[73];
    if (!changedEvent) throw new Error('Expected changed event');
    changedEvents[73] = { ...changedEvent, status: 500 };

    requestOutcomeWork.mockClear();
    rerender(
      <RequestEventsTable
        events={changedEvents}
        principalNameMap={principalNameMap}
        upstreamNameMap={upstreamNameMap}
      />,
    );

    expect(requestOutcomeWork).toHaveBeenCalledTimes(1);
    expect(
      container.querySelector('tr[aria-label="View request evt_72"]'),
    ).toBe(unchangedRow);
    expect(
      container.querySelector('tr[aria-label="View request evt_73"]'),
    ).toBe(changedRow);
    expect(changedRow.textContent).toContain('500');
  });

  it('matches visible columns in reserved loading rows', () => {
    const { container } = render(
      <RequestEventsTable
        events={[]}
        principalNameMap={principalNameMap}
        upstreamNameMap={upstreamNameMap}
        loading
        reservedRowCount={3}
        columns={{ session: false }}
      />,
    );

    const headerCells = container.querySelectorAll('thead th');
    const body = container.querySelector('tbody');
    const rows = container.querySelectorAll('tbody > tr');
    // Session is off; kind stays even with no rows yet.
    const expectedSkeletonWidths = [
      'max-w-24',
      'max-w-24',
      'max-w-28',
      'max-w-12',
      'max-w-40',
      'max-w-12',
      'max-w-16',
      'max-w-20',
      'max-w-16',
    ];

    expect(headerCells).toHaveLength(9);
    expect(rows).toHaveLength(3);
    expect(body?.style.height).toBe('7.5rem');

    for (const row of rows) {
      expect(row.className).toContain('border-b');
      expect(row.className).toContain('border-row');
      expect((row as HTMLTableRowElement).style.height).toBe('2.5rem');
      const cells = row.querySelectorAll('td');
      expect(cells).toHaveLength(headerCells.length);

      cells.forEach((cell, index) => {
        const skeleton = cell.querySelector('.skeleton');
        expect(skeleton?.className).toContain(expectedSkeletonWidths[index]);

        if (index >= 5) {
          expect(cell.className).toContain('text-right');
          expect(cell.className).toContain('tabular-nums');
          expect(skeleton?.className).toContain('ml-auto');
        }
      });
    }
  });

  it('uses the same row height for loading and loaded events', () => {
    const event = {
      event_id: 'evt_row_height',
      request_id: 'req_row_height',
      ts: 1718553120,
      ts_ms: 1718553120000,
      status: 200,
      duration_ms: 100,
      _phase: 'final',
    } satisfies RequestEventWithPhase;
    const { container, rerender } = render(
      <RequestEventsTable
        events={[]}
        principalNameMap={principalNameMap}
        upstreamNameMap={upstreamNameMap}
        loading
        reservedRowCount={1}
      />,
    );
    const loadingRow = container.querySelector(
      'tbody > tr',
    ) as HTMLTableRowElement;
    const loadingRowHeight = loadingRow.style.height;

    expect(loadingRowHeight).toBe('2.5rem');

    rerender(
      <RequestEventsTable
        events={[event]}
        principalNameMap={principalNameMap}
        upstreamNameMap={upstreamNameMap}
      />,
    );

    const loadedRow = container.querySelector(
      'tbody > tr[aria-label="View request evt_row_height"]',
    ) as HTMLTableRowElement;
    expect(loadedRow.style.height).toBe(loadingRowHeight);
  });

  it('keeps the empty state at the reserved loading body height', () => {
    const { container, rerender } = render(
      <RequestEventsTable
        events={[]}
        principalNameMap={principalNameMap}
        upstreamNameMap={upstreamNameMap}
        loading
        reservedRowCount={4}
      />,
    );

    const loadingHeight = container.querySelector('tbody')?.style.height;
    expect(loadingHeight).toBe('10rem');

    rerender(
      <RequestEventsTable
        events={[]}
        principalNameMap={principalNameMap}
        upstreamNameMap={upstreamNameMap}
        reservedRowCount={4}
      />,
    );

    const emptyBody = container.querySelector('tbody');
    const emptyRow = emptyBody?.querySelector('tr');
    expect(emptyBody?.style.height).toBe(loadingHeight);
    expect(emptyRow?.className).toContain('h-full');
    // Every column, session and kind included, is present with no rows.
    expect(emptyRow?.querySelector('td')?.getAttribute('colspan')).toBe('10');
    expect(screen.getByText('No requests')).toBeDefined();
  });

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

  it('renders explicit request-kind badges in a dedicated column', () => {
    const events: RequestEventWithPhase[] = [
      {
        event_id: 'evt_advisor',
        request_id: 'req_advisor',
        ts: 1718553120,
        status: 200,
        duration_ms: 100,
        model: 'claude-sonnet',
        request_kind: 'advisor',
        thread_id: 'session-advisor',
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

    const { container } = render(
      <RequestEventsTable
        events={events}
        principalNameMap={principalNameMap}
        upstreamNameMap={upstreamNameMap}
      />,
    );

    const sessionChip = screen.getByTitle('session-advisor');
    const requestKindBadge = screen.getByText('adv');
    expect(sessionChip.closest('td')).not.toBe(requestKindBadge.closest('td'));
    expect(screen.getByRole('columnheader', { name: 'Kind' })).toBeDefined();
    expect(screen.queryByText('advisor')).toBeNull();
    expect(screen.queryByText('main')).toBeNull();

    const kindColumnIndex = [
      ...container.querySelectorAll('thead th'),
    ].findIndex((th) => th.textContent === 'Kind');
    const unclassifiedRow = container.querySelectorAll('tbody > tr')[1];
    expect(
      unclassifiedRow.querySelectorAll('td')[kindColumnIndex].textContent,
    ).toBe('—');
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

  it('leaves Enter on a control inside the row to that control', async () => {
    const user = userEvent.setup();
    render(
      <RequestEventsTable
        events={[
          {
            event_id: 'evt_nested',
            request_id: 'req_nested',
            ts: 1718553120,
            ts_ms: 1718553120000,
            status: 200,
            duration_ms: 100,
            _phase: 'final',
          } satisfies RequestEventWithPhase,
        ]}
        principalNameMap={principalNameMap}
        upstreamNameMap={upstreamNameMap}
      />,
    );

    screen.getByRole('button', { name: /^Latency .* show breakdown$/ }).focus();
    await user.keyboard('{Enter}');

    expect(screen.queryByRole('dialog', { name: 'Request detail' })).toBeNull();
    expect(screen.getByText('Latency by responsibility')).toBeDefined();
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

  it('keeps the session and kind columns when every row is null', () => {
    const base = {
      ts: 1718553120,
      status: 200,
      duration_ms: 100,
      _phase: 'final',
    } as const;
    const { container } = render(
      <RequestEventsTable
        events={[
          { ...base, event_id: 'evt_a', request_id: 'req_a', input_tokens: 10 },
          { ...base, event_id: 'evt_b', request_id: 'req_b', input_tokens: 20 },
        ]}
        principalNameMap={principalNameMap}
        upstreamNameMap={upstreamNameMap}
      />,
    );
    const headers = Array.from(
      container.querySelectorAll('thead th'),
      (th) => th.textContent,
    );

    expect(headers).toEqual(expect.arrayContaining(['Session', 'Kind']));
    for (const column of ['Session', 'Kind']) {
      const index = headers.indexOf(column);
      for (const row of container.querySelectorAll('tbody > tr')) {
        expect(row.querySelectorAll('td')[index].textContent).toBe('—');
      }
    }
  });

  it('shows the cache hit for every row with prompt tokens, including 0%', () => {
    const base = {
      ts: 1718553120,
      status: 200,
      duration_ms: 100,
      _phase: 'final',
    } as const;
    render(
      <RequestEventsTable
        events={[
          {
            ...base,
            event_id: 'evt_a',
            request_id: 'req_a',
            input_tokens: 10,
            cache_read_input_tokens: 30,
          },
          { ...base, event_id: 'evt_b', request_id: 'req_b', input_tokens: 20 },
          {
            ...base,
            event_id: 'evt_c',
            request_id: 'req_c',
            input_tokens: 1,
            cache_read_input_tokens: 999,
          },
          { ...base, event_id: 'evt_d', request_id: 'req_d' },
        ]}
        principalNameMap={principalNameMap}
        upstreamNameMap={upstreamNameMap}
      />,
    );

    const tokens = (name: string) => screen.getByRole('button', { name });
    expect(
      tokens('Tokens 40 in, 0 out, cache hit 75%, show breakdown').textContent,
    ).toContain('75%');
    expect(
      tokens('Tokens 20 in, 0 out, cache hit 0%, show breakdown').textContent,
    ).toContain('0%');
    // A partial hit never rounds up to a full one.
    expect(
      tokens('Tokens 1.0k in, 0 out, cache hit 99%, show breakdown')
        .textContent,
    ).toContain('99%');
    // No prompt tokens: nothing to hit, so no percentage.
    expect(
      tokens('Tokens 0 in, 0 out, show breakdown').textContent,
    ).not.toContain('%');
  });

  it('gives rows of one session the same chip color and other sessions another', () => {
    const base = {
      ts: 1718553120,
      status: 200,
      duration_ms: 100,
      _phase: 'final',
    } as const;
    render(
      <RequestEventsTable
        events={[
          { ...base, event_id: 'a1', request_id: 'a1', thread_id: 'sess-a-1' },
          { ...base, event_id: 'b1', request_id: 'b1', thread_id: 'sess-b-2' },
          { ...base, event_id: 'a2', request_id: 'a2', thread_id: 'sess-a-1' },
        ]}
        principalNameMap={principalNameMap}
        upstreamNameMap={upstreamNameMap}
      />,
    );

    const [a1, a2] = screen.getAllByTitle('sess-a-1');
    const [b1] = screen.getAllByTitle('sess-b-2');
    expect(a1.getAttribute('style')).toBe(a2.getAttribute('style'));
    expect(a1.getAttribute('style')).not.toBe(b1.getAttribute('style'));
  });

  it('shortens the model id only in fit tables, keeping the full id in the title', () => {
    const events = [
      {
        event_id: 'evt_m',
        request_id: 'req_m',
        ts: 1718553120,
        status: 200,
        duration_ms: 100,
        model: 'claude-sonnet-4-6',
        _phase: 'final',
      } satisfies RequestEventWithPhase,
    ];

    const { container: fit } = render(
      <RequestEventsTable
        events={events}
        principalNameMap={principalNameMap}
        upstreamNameMap={upstreamNameMap}
      />,
    );
    expect(
      fit.querySelector('tbody span[title="claude-sonnet-4-6"]')?.textContent,
    ).toBe('sonnet-4-6');

    const { container: scroll } = render(
      <RequestEventsTable
        events={events}
        principalNameMap={principalNameMap}
        upstreamNameMap={upstreamNameMap}
        minWidthClass="min-w-[1080px]"
      />,
    );
    expect(
      scroll.querySelector('tbody span[title="claude-sonnet-4-6"]')
        ?.textContent,
    ).toBe('claude-sonnet-4-6');
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
      const tooltips = await screen.findAllByText('Estimated cost');
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
});
