import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import {
  cleanup,
  type RenderOptions,
  render as rtlRender,
  screen,
  within,
} from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import type { ReactElement } from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { type RequestEventKind, RequestEventKindSchema } from '../../lib/api';
import type * as FormatModule from '../../lib/format';
import type { RequestEventWithPhase } from '../../lib/RequestEventTypes';
import { providerForEvent, RequestEventsTable } from './RequestEventsTable';

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
  it('infers every request kind through the shared provider taxonomy', () => {
    const expected: Record<
      RequestEventKind,
      'anthropic' | 'openai' | 'gemini'
    > = {
      messages: 'anthropic',
      count_tokens: 'anthropic',
      openai_responses: 'openai',
      gemini_generate_content: 'gemini',
      gemini_count_tokens: 'gemini',
      chat_completions: 'anthropic',
      completions: 'anthropic',
      embeddings: 'anthropic',
      moderations: 'openai',
      audio: 'openai',
      images: 'openai',
      codex_auxiliary: 'openai',
      gemini_predict: 'gemini',
      models: 'anthropic',
      files: 'anthropic',
      other: 'anthropic',
      renewal: 'anthropic',
      unclassified: 'anthropic',
    };
    for (const kind of RequestEventKindSchema.options) {
      expect(
        providerForEvent({
          event_kind: kind,
        } as RequestEventWithPhase),
      ).toBe(expected[kind]);
    }
  });
  it('uses the active provider scope for shared request kinds without inventory identity', () => {
    const event = {
      event_kind: 'chat_completions',
    } as RequestEventWithPhase;
    expect(providerForEvent(event, undefined, 'openai')).toBe('openai');
    expect(providerForEvent(event, undefined, 'gemini')).toBe('gemini');
  });
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

  it('skips unchanged row work across identical and single-event updates', () => {
    // Row skipping is a per-row property: one changed row between two
    // unchanged neighbours proves it. Mounting a large table only adds
    // jsdom render time that runner CPU throttling can push past the timeout.
    const events: RequestEventWithPhase[] = Array.from(
      { length: 3 },
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
      />,
    );

    expect(requestOutcomeWork).toHaveBeenCalledTimes(3);
    const rowFor = (eventId: string) =>
      container.querySelector<HTMLTableRowElement>(
        `tr[aria-label="View request ${eventId}"]`,
      );
    const unchangedBefore = rowFor('evt_0');
    const changedRow = rowFor('evt_1');
    const unchangedAfter = rowFor('evt_2');
    if (!unchangedBefore || !changedRow || !unchangedAfter) {
      throw new Error('Expected request rows');
    }

    requestOutcomeWork.mockClear();
    rerender(
      <RequestEventsTable
        events={events}
        principalNameMap={principalNameMap}
      />,
    );
    expect(requestOutcomeWork).not.toHaveBeenCalled();

    const changedEvents = [...events];
    const changedEvent = changedEvents[1];
    if (!changedEvent) throw new Error('Expected changed event');
    changedEvents[1] = { ...changedEvent, status: 500 };

    requestOutcomeWork.mockClear();
    rerender(
      <RequestEventsTable
        events={changedEvents}
        principalNameMap={principalNameMap}
      />,
    );

    expect(requestOutcomeWork).toHaveBeenCalledTimes(1);
    expect(rowFor('evt_0')).toBe(unchangedBefore);
    expect(rowFor('evt_1')).toBe(changedRow);
    expect(rowFor('evt_2')).toBe(unchangedAfter);
    expect(changedRow.textContent).toContain('500');
  });

  it('matches visible columns in reserved loading rows', () => {
    const { container } = render(
      <RequestEventsTable
        events={[]}
        principalNameMap={principalNameMap}
        loading
        reservedRowCount={3}
        columns={{ principal: false }}
      />,
    );

    const headerCells = container.querySelectorAll('thead th');
    const body = container.querySelector('tbody');
    const rows = container.querySelectorAll('tbody > tr');

    // Principal is off; session and kind stay even with no rows yet.
    expect(headerCells).toHaveLength(9);
    expect(rows).toHaveLength(3);
    expect(body?.style.height).toBe('7.5rem');
    for (const row of rows) {
      expect(row.querySelectorAll('td')).toHaveLength(headerCells.length);
    }
  });

  it('reserves the same space for loading and empty bodies, and none once loaded', () => {
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
        loading
        reservedRowCount={2}
      />,
    );
    expect(container.querySelector('tbody')?.style.height).toBe('5rem');

    rerender(
      <RequestEventsTable
        events={[]}
        principalNameMap={principalNameMap}
        reservedRowCount={2}
      />,
    );
    expect(container.querySelector('tbody')?.style.height).toBe('5rem');

    rerender(
      <RequestEventsTable
        events={[event]}
        principalNameMap={principalNameMap}
        reservedRowCount={2}
      />,
    );
    expect(screen.getByLabelText('View request evt_row_height')).toBeDefined();
    expect(container.querySelector('tbody')?.style.height).toBe('');
  });

  it('keeps the empty state at the reserved loading body height', () => {
    const { container, rerender } = render(
      <RequestEventsTable
        events={[]}
        principalNameMap={principalNameMap}
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
        reservedRowCount={4}
      />,
    );

    const emptyBody = container.querySelector('tbody');
    const emptyRow = emptyBody?.querySelector('tr');
    expect(emptyBody?.style.height).toBe(loadingHeight);
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
      />,
    );

    const [a1, a2] = screen.getAllByTitle('sess-a-1');
    const [b1] = screen.getAllByTitle('sess-b-2');
    expect(a1.getAttribute('style')).toBe(a2.getAttribute('style'));
    expect(a1.getAttribute('style')).not.toBe(b1.getAttribute('style'));
  });

  it('drops a leading claude- from model ids, keeping the full id', () => {
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
      {
        event_id: 'evt_other',
        request_id: 'req_other',
        ts: 1718553121,
        status: 200,
        duration_ms: 100,
        model: 'my-claude-proxy',
        _phase: 'final',
      } satisfies RequestEventWithPhase,
    ];

    const { container } = render(
      <RequestEventsTable
        events={events}
        principalNameMap={principalNameMap}
      />,
    );
    const model = container.querySelector(
      'tbody span[title="claude-sonnet-4-6"]',
    );
    expect(model?.textContent).toBe('sonnet-4-6');
    expect(model?.getAttribute('aria-label')).toBe('claude-sonnet-4-6');
    // Only a leading prefix goes.
    expect(
      container.querySelector('tbody span[title="my-claude-proxy"]')
        ?.textContent,
    ).toBe('my-claude-proxy');
  });

  describe('Column widths', () => {
    const shortEvents: RequestEventWithPhase[] = [
      {
        event_id: 'evt_short',
        request_id: 'req_short',
        ts: 1718553120,
        ts_ms: 1718553120000,
        status: 200,
        duration_ms: 100,
        principal_id: 'p_short',
        upstream_name: 'up',
        thread_id: 'sess-1',
        request_kind: 'main',
        model: 'claude-haiku-4-5',
        _phase: 'final',
      },
    ];
    const longPrincipalId = 'p_long';
    const longEvents: RequestEventWithPhase[] = [
      {
        event_id: 'evt_long',
        request_id: 'req_long',
        ts: 1718553121,
        ts_ms: 1718553121000,
        status: 200,
        duration_ms: 100,
        principal_id: longPrincipalId,
        upstream_name: 'u'.repeat(60),
        thread_id: '01234567-89ab-cdef-0123-456789abcdef',
        request_kind: 'custom-kind-abcdefgh',
        model: `claude-${'m'.repeat(33)}`,
        thinking_level: 'high',
        service_tier: 'priority',
        _phase: 'final',
      },
      {
        event_id: 'evt_partial',
        request_id: 'req_partial',
        ts: 1718553122,
        ts_ms: 1718553122000,
        status: 0,
        duration_ms: 0,
        _phase: 'partial',
      },
      {
        event_id: 'evt_disconnect',
        request_id: 'req_disconnect',
        ts: 1718553123,
        ts_ms: 1718553123000,
        status: 499,
        error_code: 'client_closed_request',
        duration_ms: 100,
        _phase: 'final',
      },
      {
        event_id: 'evt_semantic',
        request_id: 'req_semantic',
        ts: 1718553124,
        ts_ms: 1718553124000,
        status: 200,
        error_code: 'upstream_stream_error',
        upstream_error_type: 'overloaded_error',
        duration_ms: 100,
        _phase: 'final',
      },
    ];
    const names = new Map([
      ['p_short', 'alice'],
      [longPrincipalId, 'P'.repeat(60)],
    ]);

    it('never derives a column width from row content', () => {
      const { container, rerender } = render(
        <RequestEventsTable events={shortEvents} principalNameMap={names} />,
      );
      const before = container.querySelector('colgroup')?.outerHTML;
      expect(before).toBeDefined();

      rerender(
        <RequestEventsTable
          events={[...longEvents, ...shortEvents]}
          principalNameMap={names}
        />,
      );

      expect(container.querySelector('colgroup')?.outerHTML).toBe(before);
      const headers = Array.from(
        container.querySelectorAll('thead th'),
        (th) => th.textContent,
      );
      const cols = container.querySelectorAll('colgroup > col');
      expect(cols).toHaveLength(headers.length);
    });

    it('sets the table minimum to the visible columns plus the session floor and reserves card fallback below 42rem', () => {
      const { container, rerender } = render(
        <RequestEventsTable
          events={shortEvents}
          principalNameMap={names}
          columns={{ principal: false }}
        />,
      );
      const table = container.querySelector('table');
      expect(table?.style.getPropertyValue('--events-min-w')).toBe(
        '39.6875rem',
      );
      expect(table?.className).toContain(
        '@min-[42rem]/events:min-w-(--events-min-w)',
      );
      expect(table?.className).toContain('@max-[42rem]/events:block');
      expect(container.querySelector('thead')?.className).toContain(
        '@max-[42rem]/events:hidden',
      );

      rerender(
        <RequestEventsTable events={shortEvents} principalNameMap={names} />,
      );
      expect(table?.style.getPropertyValue('--events-min-w')).toBe('43.75rem');
    });
  });

  describe('Status cell', () => {
    const statusCell = (container: HTMLElement, eventId: string) => {
      const headers = Array.from(
        container.querySelectorAll('thead th'),
        (th) => th.textContent,
      );
      const row = screen.getByLabelText(`View request ${eventId}`);
      return row.querySelectorAll('td')[headers.indexOf('Status')];
    };
    const visibleText = (cell: Element) => {
      const clone = cell.cloneNode(true) as Element;
      for (const hidden of clone.querySelectorAll('.sr-only')) hidden.remove();
      return clone.textContent?.trim() ?? '';
    };

    it('never prints words, only an HTTP number or nothing', () => {
      const base = {
        ts: 1718553120,
        ts_ms: 1718553120000,
        duration_ms: 100,
      } as const;
      const events: RequestEventWithPhase[] = [
        ...[200, 404, 503].map((status) => ({
          ...base,
          event_id: `s${status}`,
          request_id: `s${status}`,
          status,
          _phase: 'final' as const,
        })),
        {
          ...base,
          event_id: 's0',
          request_id: 's0',
          status: 0,
          error_code: 'terminal_dropped',
          _phase: 'final',
        },
        {
          ...base,
          event_id: 's504',
          request_id: 's504',
          status: 504,
          error_code: 'tower_timeout',
          _phase: 'final',
        },
        {
          ...base,
          event_id: 's499',
          request_id: 's499',
          status: 499,
          error_code: 'other_error',
          _phase: 'final',
        },
        {
          ...base,
          event_id: 'sdisc',
          request_id: 'sdisc',
          status: 499,
          error_code: 'client_closed_request',
          _phase: 'final',
        },
        {
          ...base,
          event_id: 'ssem',
          request_id: 'ssem',
          status: 200,
          error_code: 'upstream_stream_error',
          upstream_error_type: 'overloaded_error',
          _phase: 'final',
        },
        {
          ...base,
          event_id: 'spart',
          request_id: 'spart',
          status: 0,
          duration_ms: 0,
          _phase: 'partial',
        },
      ];
      const { container } = render(
        <RequestEventsTable
          events={events}
          principalNameMap={principalNameMap}
        />,
      );

      // Every fixture's event_id equals its request_id, the row's label key.
      for (const event of events) {
        expect(visibleText(statusCell(container, event.request_id))).toMatch(
          /^(\d{1,3})?$/,
        );
      }
    });

    it('keeps the status icon a control that does not select the row', async () => {
      const user = userEvent.setup();
      const base = {
        ts: 1718553120,
        duration_ms: 100,
        _phase: 'final',
      } as const;
      render(
        <RequestEventsTable
          events={[
            {
              ...base,
              event_id: 'evt_disc',
              request_id: 'req_disc',
              status: 499,
              error_code: 'client_closed_request',
            },
            {
              ...base,
              event_id: 'evt_sem',
              request_id: 'req_sem',
              status: 200,
              error_code: 'upstream_stream_error',
              upstream_error_type: 'overloaded_error',
            },
          ]}
          principalNameMap={principalNameMap}
        />,
      );

      await user.click(
        screen.getByRole('button', { name: 'Client disconnected (HTTP 499)' }),
      );
      expect(
        screen
          .getByLabelText('View request evt_disc')
          .getAttribute('aria-selected'),
      ).toBe('false');
      expect(
        screen.queryByRole('dialog', { name: 'Request detail' }),
      ).toBeNull();
      // The click opens the icon's Hint. Base UI moves focus into that open
      // popup, so dismiss it before using the next row's icon.
      expect(
        await screen.findByText('HTTP 499 · client_closed_request'),
      ).toBeDefined();
      await user.keyboard('{Escape}');
      expect(screen.queryByText('HTTP 499 · client_closed_request')).toBeNull();

      screen
        .getByRole('button', { name: 'overloaded_error (HTTP 200)' })
        .focus();
      await user.keyboard('{Enter}');

      expect(
        await screen.findByText(
          'HTTP 200 · upstream_stream_error',
          {},
          { timeout: 1000 },
        ),
      ).toBeDefined();
      expect(
        screen
          .getByLabelText('View request evt_sem')
          .getAttribute('aria-selected'),
      ).toBe('false');
      expect(
        screen.queryByRole('dialog', { name: 'Request detail' }),
      ).toBeNull();
    });
  });

  it('titles a long custom request kind with the raw kind', () => {
    render(
      <RequestEventsTable
        events={[
          {
            event_id: 'evt_kind',
            request_id: 'req_kind',
            ts: 1718553120,
            status: 200,
            duration_ms: 100,
            request_kind: 'custom-kind-abcdefgh',
            _phase: 'final',
          },
        ]}
        principalNameMap={principalNameMap}
      />,
    );

    expect(screen.getByTitle('custom-kind-abcdefgh')).toBeDefined();
  });

  it('names empty principal, upstream and model cells for screen readers', () => {
    render(
      <RequestEventsTable
        events={[
          {
            event_id: 'evt_nulls',
            request_id: 'req_nulls',
            ts: 1718553120,
            status: 200,
            duration_ms: 100,
            _phase: 'final',
          },
        ]}
        principalNameMap={principalNameMap}
      />,
    );

    expect(screen.getByText('No principal')).toBeDefined();
    expect(screen.getByText('No upstream')).toBeDefined();
    expect(screen.getByText('No model')).toBeDefined();
  });

  it('keeps the provider logo inside the bounded model column', () => {
    const model = 'gemini-3.1-pro-preview-long-model-identifier';
    const { container } = render(
      <RequestEventsTable
        events={[
          {
            event_id: 'bounded-model',
            request_id: 'bounded-model-request',
            status: 200,
            duration_ms: 100,
            ts: 1718553120,
            event_kind: 'gemini_generate_content',
            model,
            _phase: 'final',
          },
        ]}
        principalNameMap={principalNameMap}
      />,
    );
    const modelText = container.querySelector(`span[title="${model}"]`);
    expect(modelText?.className).toContain('min-w-0');
    expect(modelText?.className).toContain('truncate');
    const modelCell = modelText?.closest('td');
    expect(modelCell?.className).toContain('@min-[60rem]/events:max-w-28!');
    expect(modelCell?.querySelector('svg')).not.toBeNull();
  });

  describe('Cost Tooltip', () => {
    it('shows positive cost categories and keeps the authoritative total', async () => {
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
        />,
      );

      const costCell = screen.getByRole('button', {
        name: 'Cost $0.0035, show breakdown',
      });

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

      // Explicitly reported zero categories remain visible so the tooltip
      // distinguishes a measured zero from an unavailable component.
      expect(screen.getByText('Cache create 5m')).toBeDefined();
      expect(screen.getByText('Cache create 1h')).toBeDefined();

      // Absent categories stay out of the popover.
      expect(screen.queryByText('Cache write 30m')).toBeNull();
      expect(screen.queryByText('Cache write 12h')).toBeNull();

      // Check values
      expect(screen.getAllByText('$0.0010').length).toBeGreaterThan(0);
      expect(screen.getAllByText('$0.0020').length).toBeGreaterThan(0);
      expect(screen.getAllByText('$0.0005').length).toBeGreaterThan(0);

      // Reported zero-value rows remain visible; unavailable categories do not.
      // Check total
      expect(screen.getAllByText('Total').length).toBeGreaterThan(0);
      // The exact total remains in the accessible trigger and tooltip.
      const totals = screen.getAllByText('$0.0035');
      expect(totals.length).toBeGreaterThanOrEqual(1);
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
        />,
      );

      const costCell = screen.getByRole('button', {
        name: 'Cost Est. —, show breakdown',
      });

      // Hover over the cost cell to trigger the tooltip
      await user.hover(costCell);

      // Wait for the popover to appear
      const estimateLabels = await screen.findAllByText(
        'Estimated cost',
        {},
        { timeout: 1000 },
      );
      expect(estimateLabels.length).toBeGreaterThan(0);

      // Check tooltip contents
      const tooltips = await screen.findAllByText('Estimated cost');
      expect(tooltips.length).toBeGreaterThan(0);

      // Partial rows retain their component labels, while zero estimates use
      // the em dash marker instead of presenting a measured value.
      for (const label of [
        'Input',
        'Output',
        'Cache create 5m',
        'Cache create 1h',
        'Cache read',
      ]) {
        const labelElement = screen.getByText(label);
        expect(labelElement.parentElement?.textContent).toContain('—');
      }
    });
  });

  describe('Badges', () => {
    it('shows the requested Gemini thinking level alongside the observed tier and retains reasoning in the drawer', async () => {
      const user = userEvent.setup();
      const event = {
        event_id: 'evt_gemini_metadata',
        request_id: 'req_gemini_metadata',
        ts: 1718553120,
        ts_ms: 1718553120000,
        status: 200,
        duration_ms: 100,
        model: 'gemini-2.5-pro',
        thinking_level: 'high',
        thinking_budget_tokens: 18000,
        thinking_tokens: 8200,
        service_tier: 'priority',
        _phase: 'final',
      } satisfies RequestEventWithPhase;

      render(
        <RequestEventsTable
          events={[event]}
          principalNameMap={principalNameMap}
        />,
      );

      const row = screen.getByRole('row', {
        name: 'View request evt_gemini_metadata',
      });
      expect(within(row).getByText('high')).toBeDefined();
      expect(within(row).getByText('priority')).toBeDefined();
      expect(within(row).queryByText('high · 8.2k')).toBeNull();

      await user.click(row);

      expect(
        screen.getByText('Thinking level').parentElement?.textContent,
      ).toContain('high');
      expect(
        screen.getByText('Reasoning').parentElement?.textContent,
      ).toContain('high · 8.2k');
      expect(
        screen.getByText('Service tier').parentElement?.textContent,
      ).toContain('priority');
    });

    it.each([
      ['claude-sonnet-4-5', undefined],
      ['gpt-5', undefined],
      ['gemini-2.5-pro', null],
      ['gemini-2.5-pro', ''],
      ['gemini-2.5-pro', '  '],
    ] as const)(
      'does not infer a thinking-level badge for %s when level is %s',
      (model, thinkingLevel) => {
        render(
          <RequestEventsTable
            events={[
              {
                event_id: 'evt_no_level',
                request_id: 'req_no_level',
                ts: 1718553120,
                status: 200,
                duration_ms: 100,
                model,
                thinking_level: thinkingLevel,
                reasoning_effort: 'max',
                thinking_budget_tokens: 18000,
                service_tier: 'priority',
                _phase: 'final',
              },
            ]}
            principalNameMap={principalNameMap}
          />,
        );

        expect(screen.queryByText('high')).toBeNull();
        expect(screen.queryByText('max')).toBeNull();
        expect(screen.getByText('priority')).toBeDefined();
      },
    );

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
        />,
      );

      expect(screen.getByText('flex')).toBeDefined();
      expect(screen.queryByText('priority')).toBeNull();
    });

    it('hides the Gemini unspecified service tier badge', () => {
      const events: RequestEventWithPhase[] = [
        {
          event_id: 'evt_gemini_unspecified',
          request_id: 'req_gemini_unspecified',
          ts: 1718553120,
          ts_ms: 1718553120000,
          status: 200,
          duration_ms: 100,
          model: 'gemini-2.5-pro',
          service_tier: 'unspecified',
          _phase: 'final',
        } satisfies RequestEventWithPhase,
      ];

      render(
        <RequestEventsTable
          events={events}
          principalNameMap={principalNameMap}
        />,
      );

      expect(screen.queryByText('unspecified')).toBeNull();
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
        />,
      );

      expect(screen.queryByText('priority')).toBeNull();
      expect(screen.queryByText('standard')).toBeNull();
      expect(screen.queryByText('batch')).toBeNull();
    });
  });
});
