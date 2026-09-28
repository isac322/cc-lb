import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import {
  cleanup,
  fireEvent,
  render,
  screen,
  within,
} from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import * as queries from '../../../../lib/queries';
import { cacheKeepaliveApiRows } from '../../../../lib/test-utils/cacheKeepalive-fixtures';
import * as visibilityManager from '../../../../lib/visibilityManager';
import { cacheKeepaliveRows } from '../__fixtures__/cacheKeepaliveFixtures';
import { CacheKeepaliveSessionsDrawer } from '../CacheKeepaliveSessionsDrawer';

const mockPrincipal: queries.Principal = {
  id: 'p-123',
  name: 'Test Principal',
  kind: 'machine',
  enabled: true,
  revision: 42,
  allowed_models: [],
  allowed_upstreams: [],
  default_limits: [],
  cache_keepalive: {
    enabled: true,
    refresh_lead_time_5m_secs: 45,
    refresh_lead_time_1h_secs: 400,
    max_refreshes_per_session: 10,
    max_total_duration_secs: 7200,
    snapshot_max_bytes: 256000,
    classifier: {
      extra_wait_for_user_tools: [],
      treat_end_turn_as_ambiguous: true,
    },
  },
};

function renderWithProviders(ui: React.ReactElement) {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return render(
    <QueryClientProvider client={queryClient}>{ui}</QueryClientProvider>,
  );
}

describe('CacheKeepaliveSessionsDrawer', () => {
  afterEach(() => {
    cleanup();
    document.body.innerHTML = '';
    window.PAUSE_ANIMATIONS = false;
  });

  beforeEach(() => {
    vi.spyOn(visibilityManager, 'useVisibility').mockReturnValue({
      visible: true,
      online: true,
      hiddenSince: null,
      gracePeriodElapsed: false,
    });

    vi.spyOn(queries, 'useCacheKeepaliveSessions').mockReturnValue({
      data: {
        pages: [
          {
            summary: {
              renewing_now: 1,
              sessions_last_5m: 2,
              renewals_fired: 3,
              cost_saved: 4.56,
            },
            rows: cacheKeepaliveRows.map((r) => ({
              ...r,
              net_pnl: r.netPnl,
              max_attempts: r.maxAttempts,
              last_message_at_ms: 1234567890,
              session_key_hash: r.id,
            })),
            next_cursor: null,
          },
        ],
      },
      isLoading: false,
      hasNextPage: false,
      isFetchingNextPage: false,
      fetchNextPage: vi.fn(),
    } as unknown as ReturnType<typeof queries.useCacheKeepaliveSessions>);

    vi.spyOn(queries, 'useCacheKeepaliveSessionDetail').mockReturnValue({
      data: {
        id: 'a1f39c2b7e04',
        last_message_at_ms: 1234567890,
        state: 'renewed',
        ttl: '5m',
        attempts: 8,
        max_attempts: 12,
        reason: 'agent-in-turn (tool_use: `bash`)',
        generation: 1,
        upstream: 'anthropic',
        error: null,
        net_pnl: 0.102,
        session_key_hash: 'a1f39c2b7e04',
        renewal_tokens: 20000,
        total_avoided: 0.15,
        total_spent: 0.048,
        total_renewals: 8,
        is_last_pending: false,
        turns: [
          {
            turn_number: 1,
            renewals: 8,
            followed_up: true,
            label: 'agent-in-turn',
            time_ms: 1234567890,
            pnl: 0.102,
            pending: false,
          },
        ],
        config_snapshot: {
          lead_5m: 30,
          lead_1h: 300,
          max_renewals: 12,
          max_duration: 14400,
          snapshot_bytes: 524288,
        },
        raw_record: {
          session_key_hash: 'a1f39c2b7e04',
          principal_id: 'p-123',
          upstream_id: 'anthropic',
          generation: 1,
          renewal_count: 8,
          ttl: '5m',
          status: 'renewed',
          enqueue_state: 'enqueued',
        },
      },
      isLoading: false,
      isError: false,
    } as unknown as ReturnType<typeof queries.useCacheKeepaliveSessionDetail>);
  });

  it('renders five structured session skeleton rows with reserved list geometry', () => {
    vi.spyOn(queries, 'useCacheKeepaliveSessions').mockReturnValue({
      data: undefined,
      isLoading: true,
      hasNextPage: false,
      isFetchingNextPage: false,
      fetchNextPage: vi.fn(),
    } as never);

    renderWithProviders(
      <CacheKeepaliveSessionsDrawer
        open={true}
        onOpenChange={() => {}}
        principal={mockPrincipal}
      />,
    );

    const region = screen.getByTestId('cache-keepalive-session-list-region');
    const rows = screen.getAllByTestId('cache-keepalive-session-skeleton-row');

    expect(region.className).toContain('min-h-[340px]');
    expect(rows).toHaveLength(5);
    expect(rows[0].parentElement?.tagName).toBe('UL');
    expect(rows[0].parentElement?.className).toContain('flex');
    expect(rows[0].parentElement?.className).toContain('flex-col');
    rows.forEach((row) => {
      expect(row.className).toContain('border-b');
      expect(row.className).toContain('min-h-[68px]');
      expect(row.className).toContain('px-4');
      expect(row.className).toContain('py-3');
      expect(row.querySelectorAll('.skeleton')).toHaveLength(6);
    });
    expect(screen.queryByText('0 renewals fired')).toBeNull();
    expect(screen.queryByText(/\$0\.00 saved/)).toBeNull();
  });

  it('Given in-place update, When data changes, Then row updates in place', () => {
    vi.spyOn(queries, 'useCacheKeepaliveSessions').mockReturnValue({
      data: {
        pages: [
          {
            summary: {
              renewing_now: 1,
              sessions_last_5m: 2,
              renewals_fired: 3,
              cost_saved: 4.56,
            },
            rows: cacheKeepaliveRows.map((r) => ({
              ...r,
              net_pnl: r.netPnl,
              max_attempts: r.maxAttempts,
              last_message_at_ms: 1234567890,
              session_key_hash: r.id,
            })),
            next_cursor: null,
          },
        ],
      },
      isLoading: false,
      hasNextPage: false,
      isFetchingNextPage: false,
      fetchNextPage: vi.fn(),
    } as unknown as ReturnType<typeof queries.useCacheKeepaliveSessions>);

    vi.spyOn(queries, 'useCacheKeepaliveSessionDetail').mockReturnValue({
      data: {
        id: 'a1f39c2b7e04',
        last_message_at_ms: 1234567890,
        state: 'renewed',
        ttl: '5m',
        attempts: 8,
        max_attempts: 12,
        reason: 'agent-in-turn (tool_use: `bash`)',
        generation: 1,
        upstream: 'anthropic',
        error: null,
        net_pnl: 0.102,
        session_key_hash: 'a1f39c2b7e04',
        renewal_tokens: 20000,
        total_avoided: 0.15,
        total_spent: 0.048,
        total_renewals: 8,
        is_last_pending: false,
        turns: [
          {
            turn_number: 1,
            renewals: 8,
            followed_up: true,
            label: 'agent-in-turn',
            time_ms: 1234567890,
            pnl: 0.102,
            pending: false,
          },
        ],
        config_snapshot: {
          lead_5m: 30,
          lead_1h: 300,
          max_renewals: 12,
          max_duration: 14400,
          snapshot_bytes: 524288,
        },
        raw_record: {
          session_key_hash: 'a1f39c2b7e04',
          principal_id: 'p-123',
          upstream_id: 'anthropic',
          generation: 1,
          renewal_count: 8,
          ttl: '5m',
          status: 'renewed',
          enqueue_state: 'enqueued',
        },
      },
      isLoading: false,
      isError: false,
    } as unknown as ReturnType<typeof queries.useCacheKeepaliveSessionDetail>);
  });

  it('renders dialog with correct role, accessible name, and handles Escape/close', () => {
    const onOpenChange = vi.fn();
    renderWithProviders(
      <CacheKeepaliveSessionsDrawer
        open={true}
        onOpenChange={onOpenChange}
        principal={mockPrincipal}
      />,
    );

    const dialog = screen.getByRole('dialog', {
      name: 'Cache keepalive sessions',
    });

    fireEvent.click(within(dialog).getByRole('button', { name: 'Close' }));
    expect(onOpenChange).toHaveBeenCalledWith(false);

    onOpenChange.mockClear();
    fireEvent.keyDown(dialog, { key: 'Escape', code: 'Escape' });
    expect(onOpenChange).toHaveBeenCalledWith(false);
  });

  it('exposes session rows as focusable buttons that open detail on activation', () => {
    renderWithProviders(
      <CacheKeepaliveSessionsDrawer
        open={true}
        onOpenChange={() => {}}
        principal={mockPrincipal}
      />,
    );

    const rowBtn = screen.getByText('a1f39c2b7e04').closest('button')!;
    expect(rowBtn.tagName).toBe('BUTTON');
    rowBtn.focus();
    expect(document.activeElement).toBe(rowBtn);
    fireEvent.click(rowBtn);

    expect(screen.getByText('Session detail')).toBeDefined();
  });

  it('status filter is a select that narrows the sessions query', async () => {
    renderWithProviders(
      <CacheKeepaliveSessionsDrawer
        open={true}
        onOpenChange={() => {}}
        principal={mockPrincipal}
      />,
    );

    expect(screen.queryByRole('radiogroup', { name: 'Session status' })).toBe(
      null,
    );
    const trigger = screen.getByRole('combobox', { name: 'Session status' });
    expect(trigger.textContent).toContain('All statuses');

    const user = userEvent.setup();
    await user.click(trigger);
    await user.click(await screen.findByRole('option', { name: 'Renewed' }));

    expect(trigger.textContent).toContain('Renewed');
    expect(
      vi.mocked(queries.useCacheKeepaliveSessions),
    ).toHaveBeenLastCalledWith(
      'p-123',
      { horizon: '24h', status: 'renewed', error: undefined },
      true,
    );
  });

  it('applies responsive layout classes when a row is selected', () => {
    renderWithProviders(
      <CacheKeepaliveSessionsDrawer
        open={true}
        onOpenChange={() => {}}
        principal={mockPrincipal}
      />,
    );

    const row = screen.getByText('a1f39c2b7e04');
    fireEvent.click(row);

    const listContainer = row.closest('.overflow-y-auto');
    expect(listContainer?.className).toContain('max-[960px]:hidden');
    expect(listContainer?.className).toContain('w-[440px]');
  });

  it('returns from a detail query error through the responsive Back action', () => {
    vi.mocked(queries.useCacheKeepaliveSessionDetail).mockReturnValue({
      data: undefined,
      isLoading: false,
      isError: true,
    } as never);
    renderWithProviders(
      <CacheKeepaliveSessionsDrawer
        open={true}
        onOpenChange={() => {}}
        principal={mockPrincipal}
      />,
    );

    const row = screen.getByText('a1f39c2b7e04');
    fireEvent.click(row);

    const listContainer = row.closest('.overflow-y-auto');
    expect(listContainer?.className).toContain('max-[960px]:hidden');
    expect(screen.getByTestId('session-detail-error')).toBeDefined();

    fireEvent.click(screen.getByRole('button', { name: 'Back to sessions' }));

    expect(screen.queryByTestId('session-detail-error')).toBeNull();
    expect(listContainer?.className).toContain('flex-1');
    expect(listContainer?.className).not.toContain('max-[960px]:hidden');
  });

  it('renders drawer title, principal name, and time range with 24h default', () => {
    renderWithProviders(
      <CacheKeepaliveSessionsDrawer
        open={true}
        onOpenChange={() => {}}
        principal={mockPrincipal}
      />,
    );

    const dialog = screen.getByRole('dialog', {
      name: 'Cache keepalive sessions',
    });
    expect(within(dialog).getAllByText('Test Principal')[0]).toBeDefined();

    const range = within(
      screen.getByRole('radiogroup', { name: 'Time range' }),
    );
    expect(
      range.getByRole('radio', { name: '24h' }).getAttribute('aria-checked'),
    ).toBe('true');
    expect(
      range.getByRole('radio', { name: '7d' }).getAttribute('aria-checked'),
    ).toBe('false');
    expect(range.getByRole('radio', { name: 'All' })).toBeDefined();

    expect(within(dialog).getByRole('button', { name: 'Close' })).toBeDefined();
  });

  it('lists every status in the status filter', async () => {
    renderWithProviders(
      <CacheKeepaliveSessionsDrawer
        open={true}
        onOpenChange={() => {}}
        principal={mockPrincipal}
      />,
    );

    const user = userEvent.setup();
    await user.click(screen.getByRole('combobox', { name: 'Session status' }));
    const options = await screen.findAllByRole('option');
    expect(options.map((option) => option.textContent)).toEqual([
      'All statuses',
      'Renewed',
      'Scheduled',
      'Capped',
      'Expired',
      'Not tracked',
      'Error',
    ]);
  });

  it('renders row full session id first, not truncated, with status badge, optional Error chip, Net P&L, reason-only second line', () => {
    renderWithProviders(
      <CacheKeepaliveSessionsDrawer
        open={true}
        onOpenChange={() => {}}
        principal={mockPrincipal}
      />,
    );

    // Check first row (Renewed)
    expect(screen.getByText('a1f39c2b7e04')).toBeDefined();
    const renewedRow = screen.getByText('a1f39c2b7e04').closest('li');
    expect(renewedRow).not.toBeNull();
    expect(
      within(renewedRow as HTMLElement).getByText('Renewed'),
    ).toBeDefined();
    expect(screen.getByText('+$0.102')).toBeDefined();
    expect(screen.getByText('agent-in-turn (tool_use: `bash`)')).toBeDefined();

    // Check error row
    expect(screen.getByText('error-overlaps-reason')).toBeDefined();
    const errorRow = screen.getByText('error-overlaps-reason').closest('li');
    expect(errorRow).not.toBeNull();
    expect(within(errorRow as HTMLElement).getByText('Error')).toBeDefined();
    expect(screen.getByText('−$0.006')).toBeDefined();
  });

  it('renders retry tick segments correctly based on state', () => {
    renderWithProviders(
      <CacheKeepaliveSessionsDrawer
        open={true}
        onOpenChange={() => {}}
        principal={mockPrincipal}
      />,
    );

    // Renewed row has 8/12 ticks
    expect(screen.getAllByText('8/12')[0]).toBeDefined();
    // Capped row has 12/12 ticks
    expect(screen.getAllByText('12/12')[0]).toBeDefined();
    // Expired row has 8/12 ticks
    expect(screen.getAllByText('8/12')[1]).toBeDefined();

    // Scheduled and Not tracked should not have ticks
    expect(screen.queryByText('0/12')).toBeNull();
  });

  it('dedupes error and reason', () => {
    renderWithProviders(
      <CacheKeepaliveSessionsDrawer
        open={true}
        onOpenChange={() => {}}
        principal={mockPrincipal}
      />,
    );

    // error-overlaps-reason row has error === reason
    expect(
      screen.getAllByText('renewal dispatch unavailable')[0],
    ).toBeDefined();
    expect(
      screen.queryByText(
        'renewal dispatch unavailable · renewal dispatch unavailable',
      ),
    ).toBeNull();
  });

  it('defaults to full-width list with no detail pane before row selection', () => {
    renderWithProviders(
      <CacheKeepaliveSessionsDrawer
        open={true}
        onOpenChange={() => {}}
        principal={mockPrincipal}
      />,
    );
    expect(
      screen.getByTestId('cache-keepalive-session-list-region').className,
    ).toContain('min-h-[340px]');

    expect(screen.queryByText('Session detail pane (Todo 7)')).toBeNull();

    const row = screen.getByText('a1f39c2b7e04');
    fireEvent.click(row);

    expect(screen.getByText('Session detail')).toBeDefined();
  });

  it('renders exhaustion state', () => {
    renderWithProviders(
      <CacheKeepaliveSessionsDrawer
        open={true}
        onOpenChange={() => {}}
        principal={mockPrincipal}
      />,
    );

    expect(screen.getAllByText('· No more sessions ·')[0]).toBeDefined();
  });

  it('renders loading older sessions state', () => {
    vi.spyOn(queries, 'useCacheKeepaliveSessions').mockReturnValue({
      data: {
        pages: [
          {
            summary: {
              renewing_now: 1,
              sessions_last_5m: 2,
              renewals_fired: 3,
              cost_saved: 4.56,
            },
            rows: cacheKeepaliveRows.map((r) => ({
              ...r,
              net_pnl: r.netPnl,
              max_attempts: r.maxAttempts,
              last_message_at_ms: 1234567890,
              session_key_hash: r.id,
            })),
            next_cursor: 'cursor',
          },
        ],
      },
      isLoading: false,
      hasNextPage: true,
      isFetchingNextPage: false,
      fetchNextPage: vi.fn(),
    } as unknown as ReturnType<typeof queries.useCacheKeepaliveSessions>);

    renderWithProviders(
      <CacheKeepaliveSessionsDrawer
        open={true}
        onOpenChange={() => {}}
        principal={mockPrincipal}
      />,
    );

    expect(screen.getByText('Loading older sessions...')).toBeDefined();
  });

  it('replaces the filters with one line when keepalive is off', () => {
    vi.spyOn(queries, 'useCacheKeepaliveSessions').mockReturnValue({
      data: {
        pages: [
          {
            summary: {
              renewing_now: 0,
              sessions_last_5m: 0,
              renewals_fired: 0,
              cost_saved: 0,
            },
            rows: [],
            next_cursor: null,
          },
        ],
      },
      isLoading: false,
      hasNextPage: false,
      isFetchingNextPage: false,
      fetchNextPage: vi.fn(),
    } as unknown as ReturnType<typeof queries.useCacheKeepaliveSessions>);

    renderWithProviders(
      <CacheKeepaliveSessionsDrawer
        open={true}
        onOpenChange={() => {}}
        principal={{
          ...mockPrincipal,
          cache_keepalive: mockPrincipal.cache_keepalive && {
            ...mockPrincipal.cache_keepalive,
            enabled: false,
          },
        }}
      />,
    );

    expect(screen.getByTestId('cache-keepalive-sessions-off')).toBeDefined();
    expect(screen.queryByRole('combobox', { name: 'Session status' })).toBe(
      null,
    );
    expect(screen.queryByRole('radiogroup', { name: 'Time range' })).toBe(null);
    expect(screen.getByText('No sessions recorded')).toBeDefined();
  });

  it('renders no sessions empty state', () => {
    vi.spyOn(queries, 'useCacheKeepaliveSessions').mockReturnValue({
      data: {
        pages: [
          {
            summary: {
              renewing_now: 0,
              sessions_last_5m: 0,
              renewals_fired: 0,
              cost_saved: 0,
            },
            rows: [],
            next_cursor: null,
          },
        ],
      },
      isLoading: false,
      hasNextPage: false,
      isFetchingNextPage: false,
      fetchNextPage: vi.fn(),
    } as unknown as ReturnType<typeof queries.useCacheKeepaliveSessions>);

    renderWithProviders(
      <CacheKeepaliveSessionsDrawer
        open={true}
        onOpenChange={() => {}}
        principal={mockPrincipal}
      />,
    );

    expect(screen.getByText('No sessions')).toBeDefined();
    expect(
      screen.getByTestId('cache-keepalive-session-list-region').className,
    ).toContain('min-h-[340px]');
  });

  it('renders no matching sessions empty state', async () => {
    vi.spyOn(queries, 'useCacheKeepaliveSessions').mockReturnValue({
      data: {
        pages: [
          {
            summary: {
              renewing_now: 0,
              sessions_last_5m: 0,
              renewals_fired: 0,
              cost_saved: 0,
            },
            rows: [],
            next_cursor: null,
          },
        ],
      },
      isLoading: false,
      hasNextPage: false,
      isFetchingNextPage: false,
      fetchNextPage: vi.fn(),
    } as unknown as ReturnType<typeof queries.useCacheKeepaliveSessions>);

    renderWithProviders(
      <CacheKeepaliveSessionsDrawer
        open={true}
        onOpenChange={() => {}}
        principal={mockPrincipal}
      />,
    );

    const user = userEvent.setup();
    await user.click(screen.getByRole('combobox', { name: 'Session status' }));
    await user.click(await screen.findByRole('option', { name: 'Renewed' }));

    expect(screen.getByText('No matching sessions')).toBeDefined();
  });

  it('Given in-place update, When data changes, Then row updates in place', () => {
    const { rerender } = renderWithProviders(
      <CacheKeepaliveSessionsDrawer
        open={true}
        onOpenChange={() => {}}
        principal={mockPrincipal}
      />,
    );

    expect(screen.getAllByText('8/12')[0]).toBeDefined();

    vi.spyOn(queries, 'useCacheKeepaliveSessions').mockReturnValue({
      data: {
        pages: [
          {
            summary: {
              renewing_now: 1,
              sessions_last_5m: 2,
              renewals_fired: 3,
              cost_saved: 4.56,
            },
            rows: cacheKeepaliveRows.map((r) => ({
              ...r,
              net_pnl: r.netPnl,
              max_attempts: r.maxAttempts,
              last_message_at_ms: 1234567890,
              session_key_hash: r.id,
              attempts: r.id === 'a1f39c2b7e04' ? 9 : r.attempts,
            })),
            next_cursor: null,
          },
        ],
      },
      isLoading: false,
      hasNextPage: false,
      isFetchingNextPage: false,
      fetchNextPage: vi.fn(),
    } as unknown as ReturnType<typeof queries.useCacheKeepaliveSessions>);

    rerender(
      <QueryClientProvider
        client={
          new QueryClient({ defaultOptions: { queries: { retry: false } } })
        }
      >
        <CacheKeepaliveSessionsDrawer
          open={true}
          onOpenChange={() => {}}
          principal={mockPrincipal}
        />
      </QueryClientProvider>,
    );

    expect(screen.getAllByText('9/12')[0]).toBeDefined();
  });

  it('reads all FLIP geometry before writes and only animates moved rows', () => {
    const initialRows = cacheKeepaliveApiRows.slice(0, 2);
    let currentRows = initialRows;
    vi.mocked(queries.useCacheKeepaliveSessions).mockImplementation(
      () =>
        ({
          data: {
            pages: [
              {
                summary: {
                  renewing_now: 1,
                  sessions_last_5m: 2,
                  renewals_fired: 3,
                  cost_saved: 4.56,
                },
                rows: currentRows,
                next_cursor: null,
              },
            ],
          },
          isLoading: false,
          hasNextPage: false,
          isFetchingNextPage: false,
          fetchNextPage: vi.fn(),
        }) as never,
    );

    const beforePositions = {
      a1f39c2b7e04: 0,
      '7b204de1c83f': 100,
    } satisfies Record<string, number>;
    const afterPositions = {
      a1f39c2b7e04: 100,
      '7b204de1c83f': 0,
    } satisfies Record<string, number>;
    const expectedTranslateY = {
      a1f39c2b7e04: beforePositions.a1f39c2b7e04 - afterPositions.a1f39c2b7e04,
      '7b204de1c83f':
        beforePositions['7b204de1c83f'] - afterPositions['7b204de1c83f'],
    } satisfies Record<string, number>;
    let positions: Record<string, number> = beforePositions;
    const events: string[] = [];
    const rectSpy = vi
      .spyOn(HTMLElement.prototype, 'getBoundingClientRect')
      .mockImplementation(function (this: HTMLElement) {
        const key = this.dataset.key;
        if (key) events.push(`read:${key}`);
        const top = key ? (positions[key] ?? 0) : 0;
        return {
          x: 0,
          y: top,
          top,
          left: 0,
          right: 440,
          bottom: top + 68,
          width: 440,
          height: 68,
          toJSON: () => ({}),
        } as DOMRect;
      });
    const rafSpy = vi
      .spyOn(window, 'requestAnimationFrame')
      .mockImplementation(() => 1);
    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });
    const drawer = () => (
      <QueryClientProvider client={queryClient}>
        <CacheKeepaliveSessionsDrawer
          open={true}
          onOpenChange={() => {}}
          principal={mockPrincipal}
        />
      </QueryClientProvider>
    );
    const { rerender } = render(drawer());

    const transforms = new Map<string, string>();
    const renderedRows = screen
      .getByTestId('cache-keepalive-session-list-region')
      .querySelectorAll<HTMLElement>('li[data-key]');
    for (const row of renderedRows) {
      const key = row.dataset.key;
      if (!key) continue;
      transforms.set(key, row.style.transform);
      Object.defineProperty(row.style, 'transform', {
        configurable: true,
        get: () => transforms.get(key) ?? '',
        set: (value: string) => {
          transforms.set(key, value);
          events.push(`write:${key}`);
        },
      });
    }

    currentRows = [initialRows[1]!, initialRows[0]!];
    positions = afterPositions;
    events.length = 0;
    rafSpy.mockClear();
    rerender(drawer());

    expect(events.slice(0, 2)).toEqual([
      'read:7b204de1c83f',
      'read:a1f39c2b7e04',
    ]);
    expect(events.slice(2)).toEqual([
      'write:7b204de1c83f',
      'write:a1f39c2b7e04',
    ]);
    expect(transforms.get('7b204de1c83f')).toBe(
      `translateY(${expectedTranslateY['7b204de1c83f']}px)`,
    );
    expect(transforms.get('a1f39c2b7e04')).toBe(
      `translateY(${expectedTranslateY.a1f39c2b7e04}px)`,
    );
    expect(rafSpy).toHaveBeenCalledTimes(2);

    currentRows = currentRows.map((row) => ({
      ...row,
      reason: `${row.reason} updated`,
    }));
    events.length = 0;
    rafSpy.mockClear();
    rerender(drawer());

    expect(events).toEqual(['read:7b204de1c83f', 'read:a1f39c2b7e04']);
    expect(rafSpy).not.toHaveBeenCalled();

    rectSpy.mockRestore();
    rafSpy.mockRestore();
  });

  it('Given PAUSE_ANIMATIONS=true, When data changes, Then no animation is applied', () => {
    window.PAUSE_ANIMATIONS = true;
    const { rerender } = renderWithProviders(
      <CacheKeepaliveSessionsDrawer
        open={true}
        onOpenChange={() => {}}
        principal={mockPrincipal}
      />,
    );

    vi.spyOn(queries, 'useCacheKeepaliveSessions').mockReturnValue({
      data: {
        pages: [
          {
            summary: {
              renewing_now: 1,
              sessions_last_5m: 2,
              renewals_fired: 3,
              cost_saved: 4.56,
            },
            rows: [
              {
                id: 'new-session',
                state: 'scheduled',
                last_message_at_ms: 1234567890,
                ttl: null,
                attempts: null,
                max_attempts: 12,
                reason: 'test',
                generation: 1,
                upstream: null,
                error: null,
                net_pnl: 0,
                session_key_hash: 'hash',
              },
              ...cacheKeepaliveRows.map((r) => ({
                ...r,
                net_pnl: r.netPnl,
                max_attempts: r.maxAttempts,
                last_message_at_ms: 1234567890,
                session_key_hash: r.id,
              })),
            ],
            next_cursor: null,
          },
        ],
      },
      isLoading: false,
      hasNextPage: false,
      isFetchingNextPage: false,
      fetchNextPage: vi.fn(),
    } as unknown as ReturnType<typeof queries.useCacheKeepaliveSessions>);

    rerender(
      <QueryClientProvider
        client={
          new QueryClient({ defaultOptions: { queries: { retry: false } } })
        }
      >
        <CacheKeepaliveSessionsDrawer
          open={true}
          onOpenChange={() => {}}
          principal={mockPrincipal}
        />
      </QueryClientProvider>,
    );

    const newRow = screen.getByText('new-session').closest('li');
    expect(newRow?.style.transition).toBe('');
  });
});
