import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import type { CacheKeepaliveDetail } from '../../../../lib/cacheKeepaliveApi';
import * as queries from '../../../../lib/queries';
import type { PolledDataResult } from '../../../../lib/usePolledData';
import { SessionDetailPane } from '../SessionDetailPane';

function renderWithProviders(ui: React.ReactElement) {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return render(
    <QueryClientProvider client={queryClient}>{ui}</QueryClientProvider>,
  );
}

describe('SessionDetailPane', () => {
  afterEach(() => {
    cleanup();
    document.body.innerHTML = '';
  });

  it('keeps the detail structure mounted while the query is pending', () => {
    vi.spyOn(queries, 'useCacheKeepaliveSessionDetail').mockReturnValue({
      data: undefined,
      isLoading: true,
      isError: false,
    } as unknown as PolledDataResult<CacheKeepaliveDetail, Error>);

    const { container } = renderWithProviders(
      <SessionDetailPane
        principalId="p-123"
        sessionId="a1f39c2b7e04"
        onClose={() => {}}
      />,
    );

    const pane = screen.getByTestId('session-detail-loading');
    expect(pane.className).toBe('flex-1 min-w-0 overflow-y-auto');
    expect(pane.getAttribute('aria-busy')).toBe('true');
    expect(pane.getAttribute('role')).toBe('region');
    expect(pane.getAttribute('aria-label')).toBe('Session detail');

    const content = screen.getByTestId('session-detail-loading-content');
    expect(content.className).toBe('flex flex-col gap-3 p-3');

    const header = screen.getByText('Session detail').parentElement;
    expect(header?.className).toContain('sticky');
    expect(header?.className).toContain('px-3');
    expect(screen.getByText('Close ▶')).toBeDefined();
    expect(
      screen.getByRole('button', { name: 'Back to sessions' }),
    ).toBeDefined();
    expect(screen.getByRole('status').textContent).toBe(
      'Loading session detail',
    );

    const overview = screen.getByTestId('session-detail-overview-skeleton');
    expect(overview.className).toContain('p-3');
    expect(screen.getByText('Net P&L')).toBeDefined();

    const metadata = screen.getByTestId('session-detail-metadata-skeleton');
    expect(metadata.className).toContain('grid-cols-[100px_minmax(0,1fr)]');
    expect(metadata.querySelectorAll('dt')).toHaveLength(7);
    expect(metadata.querySelectorAll('dd')).toHaveLength(7);

    const turns = screen.getByTestId('session-detail-turns-skeleton');
    expect(turns.className).toContain('mt-2');
    expect(screen.getByText('Message-by-message')).toBeDefined();
    const turnCards = screen.getAllByTestId('session-detail-turn-skeleton');
    expect(turnCards).toHaveLength(3);
    expect(turnCards[0].className).toContain('px-3');
    expect(turnCards[0].className).toContain('py-2.5');
    expect(turnCards[1].className).toContain('px-3');
    expect(turnCards[1].className).toContain('py-2');

    expect(container.querySelector('svg.animate-spin')).toBeNull();
  });

  it('keeps the named detail region and Back action available on query error', () => {
    const onClose = vi.fn();
    vi.spyOn(queries, 'useCacheKeepaliveSessionDetail').mockReturnValue({
      data: undefined,
      isLoading: false,
      isError: true,
    } as unknown as PolledDataResult<CacheKeepaliveDetail, Error>);

    renderWithProviders(
      <SessionDetailPane
        principalId="p-123"
        sessionId="missing"
        onClose={onClose}
      />,
    );

    const pane = screen.getByTestId('session-detail-error');
    expect(pane.getAttribute('role')).toBe('region');
    expect(pane.getAttribute('aria-label')).toBe('Session detail');
    expect(pane.getAttribute('aria-busy')).toBe('false');
    expect(screen.getByRole('alert').textContent).toContain(
      'Failed to load session detail.',
    );

    const mobileBackLabel = screen.getByText('◀ Back');
    expect(mobileBackLabel.className).toContain('max-[960px]:inline');
    fireEvent.click(screen.getByRole('button', { name: 'Back to sessions' }));
    expect(onClose).toHaveBeenCalledOnce();
  });

  it('renders Renewed active pending with exact labels and formatting', () => {
    vi.spyOn(queries, 'useCacheKeepaliveSessionDetail').mockReturnValue({
      data: {
        id: 'a1f39c2b7e04',
        last_message_at_ms: 1234567890,
        state: 'renewed',
        ttl: '5m',
        attempts: 99,
        max_attempts: 12,
        reason: 'agent-in-turn (tool_use: `bash`)',
        generation: 1,
        upstream: 'anthropic',
        error: null,
        net_pnl: 0.0936,
        session_key_hash: 'a1f39c2b7e04',
        renewal_tokens: 20000,
        total_avoided: 0.15,
        total_spent: 0.0564,
        total_renewals: 8,
        is_last_pending: true,
        turns: [
          {
            turn_number: 3,
            renewals: 1,
            followed_up: false,
            label: 'agent-in-turn',
            time_ms: 1234567890,
            pnl: -0.006,
            pending: true,
          },
          {
            turn_number: 2,
            renewals: 3,
            followed_up: true,
            label: 'agent-in-turn',
            time_ms: 1234567000,
            pnl: 0.057,
            pending: false,
          },
          {
            turn_number: 1,
            renewals: 4,
            followed_up: true,
            label: 'agent-in-turn',
            time_ms: 1234566000,
            pnl: 0.0426,
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

    renderWithProviders(
      <SessionDetailPane
        principalId="p-123"
        sessionId="a1f39c2b7e04"
        onClose={() => {}}
      />,
    );

    expect(screen.getByText('Session detail')).toBeDefined();
    expect(screen.getByText('Close ▶')).toBeDefined();
    const pane = screen.getByTestId('session-detail-content');
    expect(pane.getAttribute('role')).toBe('region');
    expect(pane.getAttribute('aria-busy')).toBe('false');
    expect(pane.getAttribute('aria-label')).toBe('Session detail');

    // Exact overview labels
    expect(screen.getByText('Session ID')).toBeDefined();
    expect(screen.getByText('Upstream')).toBeDefined();
    expect(screen.getByText('TTL')).toBeDefined();
    expect(screen.getByText('Generation')).toBeDefined();
    expect(screen.getByText('First seen')).toBeDefined();
    expect(screen.getByText('Expires')).toBeDefined();
    expect(screen.getByText('Total renewals')).toBeDefined();

    // No separate session hash field
    expect(screen.queryByText('session hash')).toBeNull();

    // Net P&L formatting
    expect(screen.getByText('+$0.0936')).toBeDefined();
    expect(
      screen.getByText(
        'saved $0.150 in avoided cache re-creation · spent $0.056 on 8 renewals · current turn pending',
      ),
    ).toBeDefined();

    // Turns
    expect(screen.getByText('Current turn · Live')).toBeDefined();
    const totalRenewalsLabel = screen.getByText('Total renewals');
    expect(totalRenewalsLabel.nextElementSibling?.textContent).toBe('8');
    const turn3 = screen.getByText(/Turn 3/);
    const turn2 = screen.getByText(/Turn 2/);
    const turn1 = screen.getByText(/Turn 1/);
    expect(turn3.compareDocumentPosition(turn2)).toBe(
      Node.DOCUMENT_POSITION_FOLLOWING,
    );
    expect(turn2.compareDocumentPosition(turn1)).toBe(
      Node.DOCUMENT_POSITION_FOLLOWING,
    );
    expect(screen.getByText('−$0.006 pending')).toBeDefined();
    expect(screen.getByText('waiting for follow-up')).toBeDefined();

    expect(screen.getByText('+$0.057')).toBeDefined();
    expect(screen.getAllByText('cache used by follow-up')).toHaveLength(2);

    // Collapsibles
    expect(screen.getByText('Config in effect at schedule time')).toBeDefined();
    expect(screen.getByText('Raw session record')).toBeDefined();
  });

  it('renders Scheduled zero-renewal pending', () => {
    vi.spyOn(queries, 'useCacheKeepaliveSessionDetail').mockReturnValue({
      data: {
        id: 'sched-1',
        last_message_at_ms: 1234567890,
        state: 'scheduled',
        ttl: '5m',
        attempts: 0,
        max_attempts: 12,
        reason: 'agent-in-turn',
        generation: 1,
        upstream: 'anthropic',
        error: null,
        net_pnl: 0,
        session_key_hash: 'sched-1',
        renewal_tokens: 20000,
        total_avoided: 0,
        total_spent: 0,
        total_renewals: 0,
        is_last_pending: true,
        turns: [
          {
            turn_number: 1,
            renewals: 0,
            followed_up: false,
            label: 'agent-in-turn',
            time_ms: 1234567890,
            pnl: 0,
            pending: true,
          },
        ],
        config_snapshot: null,
        raw_record: {
          session_key_hash: 'sched-1',
          principal_id: 'p-123',
          upstream_id: 'anthropic',
          generation: 1,
          renewal_count: 0,
          ttl: '5m',
          status: 'scheduled',
          enqueue_state: 'enqueued',
        },
      },
      isLoading: false,
      isError: false,
    } as unknown as ReturnType<typeof queries.useCacheKeepaliveSessionDetail>);

    renderWithProviders(
      <SessionDetailPane
        principalId="p-123"
        sessionId="sched-1"
        onClose={() => {}}
      />,
    );

    expect(screen.getByText('Scheduled')).toBeDefined();
    expect(screen.getByText('Current turn · Live')).toBeDefined();
    expect(screen.getByText('−$0.000 pending')).toBeDefined();
  });

  it('renders Capped final neutral', () => {
    vi.spyOn(queries, 'useCacheKeepaliveSessionDetail').mockReturnValue({
      data: {
        id: 'cap-1',
        last_message_at_ms: 1234567890,
        state: 'capped',
        ttl: '5m',
        attempts: 12,
        max_attempts: 12,
        reason: 'max renewals reached',
        generation: 1,
        upstream: 'anthropic',
        error: null,
        net_pnl: -0.072,
        session_key_hash: 'cap-1',
        renewal_tokens: 20000,
        total_avoided: 0,
        total_spent: 0.072,
        total_renewals: 12,
        is_last_pending: false,
        turns: [
          {
            turn_number: 1,
            renewals: 12,
            followed_up: false,
            label: 'agent-in-turn',
            time_ms: 1234567890,
            pnl: -0.072,
            pending: false,
          },
        ],
        config_snapshot: null,
        raw_record: {
          session_key_hash: 'cap-1',
          principal_id: 'p-123',
          upstream_id: 'anthropic',
          generation: 1,
          renewal_count: 12,
          ttl: '5m',
          status: 'capped',
          enqueue_state: 'enqueued',
        },
      },
      isLoading: false,
      isError: false,
    } as unknown as ReturnType<typeof queries.useCacheKeepaliveSessionDetail>);

    renderWithProviders(
      <SessionDetailPane
        principalId="p-123"
        sessionId="cap-1"
        onClose={() => {}}
      />,
    );

    expect(screen.getAllByText('Capped')[0]).toBeDefined();
    expect(screen.getByText('Final turn')).toBeDefined();
    expect(screen.getAllByText('−$0.072')[0]).toBeDefined();
    expect(screen.getAllByText('Expired')[0]).toBeDefined(); // Terminal state uses Expired instead of Expires
  });

  it('renders Expired final amber', () => {
    vi.spyOn(queries, 'useCacheKeepaliveSessionDetail').mockReturnValue({
      data: {
        id: 'exp-1',
        last_message_at_ms: 1234567890,
        state: 'expired',
        ttl: '5m',
        attempts: 8,
        max_attempts: 12,
        reason: 'TTL expired before follow-up',
        generation: 1,
        upstream: 'anthropic',
        error: null,
        net_pnl: -0.048,
        session_key_hash: 'exp-1',
        renewal_tokens: 20000,
        total_avoided: 0,
        total_spent: 0.048,
        total_renewals: 8,
        is_last_pending: false,
        turns: [
          {
            turn_number: 1,
            renewals: 8,
            followed_up: false,
            label: 'agent-in-turn',
            time_ms: 1234567890,
            pnl: -0.048,
            pending: false,
          },
        ],
        config_snapshot: null,
        raw_record: {
          session_key_hash: 'exp-1',
          principal_id: 'p-123',
          upstream_id: 'anthropic',
          generation: 1,
          renewal_count: 8,
          ttl: '5m',
          status: 'expired',
          enqueue_state: 'enqueued',
        },
      },
      isLoading: false,
      isError: false,
    } as unknown as ReturnType<typeof queries.useCacheKeepaliveSessionDetail>);

    renderWithProviders(
      <SessionDetailPane
        principalId="p-123"
        sessionId="exp-1"
        onClose={() => {}}
      />,
    );

    expect(screen.getAllByText('Expired')[0]).toBeDefined();
    expect(screen.getByText('Final turn')).toBeDefined();
    expect(screen.getAllByText('−$0.048')[0]).toBeDefined();
  });

  it('renders Not tracked zero/-', () => {
    vi.spyOn(queries, 'useCacheKeepaliveSessionDetail').mockReturnValue({
      data: {
        id: 'not-1',
        last_message_at_ms: 1234567890,
        state: 'not_tracked',
        ttl: null,
        attempts: null,
        max_attempts: 12,
        reason: 'user turn (stop_reason=end_turn)',
        generation: 1,
        upstream: 'anthropic',
        error: null,
        net_pnl: 0,
        session_key_hash: 'not-1',
        renewal_tokens: 0,
        total_avoided: 0,
        total_spent: 0,
        total_renewals: 0,
        is_last_pending: false,
        turns: [],
        config_snapshot: null,
        raw_record: {
          session_key_hash: 'not-1',
          principal_id: 'p-123',
          upstream_id: 'anthropic',
          generation: 1,
          renewal_count: 0,
          ttl: null,
          status: 'not_tracked',
        },
      },
      isLoading: false,
      isError: false,
    } as unknown as ReturnType<typeof queries.useCacheKeepaliveSessionDetail>);

    renderWithProviders(
      <SessionDetailPane
        principalId="p-123"
        sessionId="not-1"
        onClose={() => {}}
      />,
    );

    expect(screen.getByText('Not tracked')).toBeDefined();
    expect(screen.getByText('$0.00')).toBeDefined();
    expect(screen.getByText('no renewals fired')).toBeDefined();
    expect(screen.getAllByText('-')[0]).toBeDefined();
    expect(screen.queryByText(/Turn 1/)).toBeNull();
  });

  it('renders error banner with deduped reason', () => {
    vi.spyOn(queries, 'useCacheKeepaliveSessionDetail').mockReturnValue({
      data: {
        id: 'err-1',
        last_message_at_ms: 1234567890,
        state: 'renewed',
        ttl: '5m',
        attempts: 1,
        max_attempts: 12,
        reason: 'renewal dispatch unavailable',
        generation: 1,
        upstream: 'anthropic',
        error: 'renewal dispatch unavailable',
        net_pnl: -0.006,
        session_key_hash: 'err-1',
        renewal_tokens: 20000,
        total_avoided: 0,
        total_spent: 0.006,
        total_renewals: 1,
        is_last_pending: true,
        turns: [
          {
            turn_number: 1,
            renewals: 1,
            followed_up: false,
            label: 'agent-in-turn',
            time_ms: 1234567890,
            pnl: -0.006,
            pending: true,
          },
        ],
        config_snapshot: null,
        raw_record: {
          session_key_hash: 'err-1',
          principal_id: 'p-123',
          upstream_id: 'anthropic',
          generation: 1,
          renewal_count: 1,
          ttl: '5m',
          status: 'renewed',
        },
      },
      isLoading: false,
      isError: false,
    } as unknown as ReturnType<typeof queries.useCacheKeepaliveSessionDetail>);

    renderWithProviders(
      <SessionDetailPane
        principalId="p-123"
        sessionId="err-1"
        onClose={() => {}}
      />,
    );

    // Error banner should show the error text
    expect(screen.getByText(/renewal dispatch unavailable/)).toBeDefined();
  });
});
