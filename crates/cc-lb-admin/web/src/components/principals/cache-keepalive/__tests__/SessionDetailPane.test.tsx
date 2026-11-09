import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { ApiError } from '../../../../lib/api';
import type { CacheKeepaliveDetail } from '../../../../lib/cacheKeepaliveApi';
import * as queries from '../../../../lib/queries';
import { cacheKeepaliveRenewedDetailFixture } from '../../../../lib/test-utils/cacheKeepalive-fixtures';
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
    vi.restoreAllMocks();
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
    const closeLabel = screen.getByText('Close', { selector: 'span' });
    expect(closeLabel.querySelector('svg.lucide-chevron-right')).not.toBeNull();
    expect(
      screen.getByRole('button', { name: 'Back to sessions' }),
    ).toBeDefined();
    expect(screen.getByRole('status').textContent).toBe(
      'Loading session detail',
    );

    expect(
      screen.getByTestId('session-detail-overview-skeleton'),
    ).toBeDefined();
    expect(screen.getByText('Net P&L')).toBeDefined();

    const metadata = screen.getByTestId('session-detail-metadata-skeleton');
    expect(metadata.className).toContain('grid-cols-[100px_minmax(0,1fr)]');
    expect(metadata.querySelectorAll('dt')).toHaveLength(7);
    expect(metadata.querySelectorAll('dd')).toHaveLength(7);

    const turns = screen.getByTestId('session-detail-turns-skeleton');
    expect(turns.className).toContain('mt-2');
    expect(screen.getByText('Message-by-message')).toBeDefined();
    const turnRows = screen.getAllByTestId('session-detail-turn-skeleton');
    expect(turnRows).toHaveLength(3);
    // Skeleton rows share the loaded turn rows' vertical rhythm.
    for (const row of turnRows) {
      expect(row.className).toContain('py-2.5');
    }

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

    const mobileBackLabel = screen.getByText('Back', { selector: 'span' });
    expect(mobileBackLabel.className).toContain('max-[960px]:inline-flex');
    expect(
      mobileBackLabel.querySelector('svg.lucide-chevron-left'),
    ).not.toBeNull();
    expect(screen.queryByText(/[▶◀]/)).toBeNull();
    fireEvent.click(
      screen.getAllByRole('button', { name: 'Back to sessions' })[
        screen.getAllByRole('button', { name: 'Back to sessions' }).length - 1
      ]!,
    );
    expect(onClose).toHaveBeenCalledOnce();
  });

  it('renders Renewed active pending with exact labels and formatting', () => {
    vi.spyOn(queries, 'useCacheKeepaliveSessionDetail').mockReturnValue({
      data: {
        id: 'a1f39c2b7e04',
        last_message_at_ms: 1234567890,
        state: 'renewed',
        is_active: true,
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
        total_renewals_current_cycle: 8,
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
    expect(screen.getByText('Close', { selector: 'span' })).toBeDefined();
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
    expect(screen.getByText('Last message')).toBeDefined();
    expect(screen.getByText('Current-cycle renewals')).toBeDefined();

    // No separate session hash field
    expect(screen.queryByText('session hash')).toBeNull();

    // Net P&L formatting
    expect(screen.getByText('+$0.0936')).toBeDefined();
    const breakdown = screen.getByText(/saved \$0\.150/);
    expect(breakdown.textContent).toMatch(/spent \$0\.056/);
    expect(breakdown.textContent).toMatch(/across retained history/);
    expect(breakdown.textContent).toContain('current turn pending');
    expect(breakdown.textContent).not.toContain(
      'spent $0.056 on 8 current-cycle renewals',
    );

    // Turns
    expect(screen.getByText('Current turn · Live')).toBeDefined();
    const totalRenewalsLabel = screen.getByText('Current-cycle renewals');
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
        is_active: true,
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
        total_renewals_current_cycle: 0,
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
    expect(screen.getByText('+$0.000 pending')).toBeDefined();
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
        total_renewals_current_cycle: 12,
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
    expect(screen.getByText('Last message')).toBeDefined();
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
        total_renewals_current_cycle: 8,
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

  it('renders Not tracked with neutral unknown economics', () => {
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
        net_pnl: null,
        session_key_hash: 'not-1',
        renewal_tokens: 0,
        total_avoided: 0,
        total_spent: 0,
        total_renewals_current_cycle: 0,
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

    const netPnlLabel = screen.getByText('Net P&L');
    expect(netPnlLabel.nextElementSibling?.textContent).toBe('—');
    expect(screen.getByText('no renewals fired')).toBeDefined();
    expect(screen.getAllByText('-')[0]).toBeDefined();
    expect(screen.queryByText(/Turn 1/)).toBeNull();
  });
  it('shows observed terminal hit and miss spend as numeric losses, not unavailable evidence', () => {
    const detail: CacheKeepaliveDetail = {
      ...cacheKeepaliveRenewedDetailFixture,
      id: 'terminal-unfollowed',
      state: 'expired',
      is_active: false,
      reason: 'cache miss',
      attempts: 1,
      net_pnl: -0.021,
      total_spent: 0.021,
      total_avoided: 0,
      evidence_status: 'observed',
      total_renewals_current_cycle: 1,
      is_last_pending: false,
      turns: [
        {
          turn_number: 2,
          renewals: 0,
          followed_up: false,
          label: 'no follow-up (loss)',
          time_ms: 1_700_000_060_000,
          pnl: -0.015,
          pending: false,
          evidence_status: 'observed',
        },
        {
          turn_number: 1,
          renewals: 1,
          followed_up: false,
          label: 'no follow-up (loss)',
          time_ms: 1_700_000_000_000,
          pnl: -0.006,
          pending: false,
          evidence_status: 'observed',
        },
      ],
      raw_record: {
        ...cacheKeepaliveRenewedDetailFixture.raw_record,
        renewal_count: 1,
        status: 'expired',
      },
    };
    vi.spyOn(queries, 'useCacheKeepaliveSessionDetail').mockReturnValue({
      data: detail,
      isLoading: false,
      isError: false,
    } as unknown as PolledDataResult<CacheKeepaliveDetail, Error>);

    renderWithProviders(
      <SessionDetailPane
        principalId="p-123"
        sessionId={detail.id}
        onClose={() => {}}
      />,
    );

    expect(
      screen.getByRole('region', { name: 'Session detail' }),
    ).toBeDefined();
    expect(screen.getByText('Net P&L').nextElementSibling?.textContent).toBe(
      '−$0.021',
    );
    expect(screen.getByText('−$0.015')).toBeDefined();
    expect(screen.getByText('−$0.006')).toBeDefined();
    expect(screen.getAllByText('no follow-up (loss)')).toHaveLength(2);
    expect(screen.queryByText(/follow-up unavailable/)).toBeNull();
    expect(screen.queryByText('waiting for follow-up')).toBeNull();
    expect(
      screen.getAllByText(/^Renewals:/).map((label) => label.textContent),
    ).toEqual(['Renewals: 0', 'Renewals: 1']);
    expect(
      screen.getByText('Current-cycle renewals').nextElementSibling
        ?.textContent,
    ).toBe('1');
  });

  it('keeps retained economics separate from a replacement cycle with no renewals', () => {
    const detail: CacheKeepaliveDetail = {
      ...cacheKeepaliveRenewedDetailFixture,
      id: 'replacement-cycle',
      state: 'scheduled',
      is_active: true,
      attempts: 0,
      net_pnl: 0.054,
      total_spent: 0.021,
      total_avoided: 0.075,
      evidence_status: 'observed',
      total_renewals_current_cycle: 0,
      is_last_pending: false,
      turns: [],
      raw_record: {
        ...cacheKeepaliveRenewedDetailFixture.raw_record,
        generation: 8,
        renewal_count: 0,
        status: 'scheduled',
      },
      generation: 8,
    };
    vi.spyOn(queries, 'useCacheKeepaliveSessionDetail').mockReturnValue({
      data: detail,
      isLoading: false,
      isError: false,
    } as unknown as PolledDataResult<CacheKeepaliveDetail, Error>);

    renderWithProviders(
      <SessionDetailPane
        principalId="p-123"
        sessionId={detail.id}
        onClose={() => {}}
      />,
    );

    expect(screen.getByText('Net P&L').nextElementSibling?.textContent).toBe(
      '+$0.054',
    );
    const breakdown = screen.getByText(/saved \$0\.075/);
    expect(breakdown.textContent).toMatch(/spent \$0\.021/);
    expect(breakdown.textContent).toMatch(/across retained history/);
    expect(breakdown.textContent).not.toContain(
      'spent $0.021 on 0 current-cycle renewals',
    );
    expect(
      screen.getByText('Current-cycle renewals').nextElementSibling
        ?.textContent,
    ).toBe('0');
    expect(
      screen.getByRole('heading', { name: 'Message-by-message' }),
    ).toBeDefined();
    expect(screen.getByText('· no renewals')).toBeDefined();
    expect(screen.queryByText(/^Turn \d/)).toBeNull();
    expect(screen.queryByText(/current turn pending/)).toBeNull();
  });

  it('labels unknown follow-up evidence as unavailable, not as a loss', () => {
    const detail: CacheKeepaliveDetail = {
      ...cacheKeepaliveRenewedDetailFixture,
      state: 'expired',
      is_active: false,
      net_pnl: null,
      total_spent: 0.021,
      total_avoided: null,
      evidence_status: 'unknown',
      total_renewals_current_cycle: 1,
      is_last_pending: false,
      turns: [
        {
          turn_number: 1,
          renewals: 1,
          followed_up: null,
          label: 'agent-in-turn',
          time_ms: 1_700_000_000_000,
          pnl: null,
          pending: false,
          evidence_status: 'unknown',
        },
      ],
    };
    vi.spyOn(queries, 'useCacheKeepaliveSessionDetail').mockReturnValue({
      data: detail,
      isLoading: false,
      isError: false,
    } as unknown as ReturnType<typeof queries.useCacheKeepaliveSessionDetail>);

    renderWithProviders(
      <SessionDetailPane
        principalId="p-123"
        sessionId="unknown-evidence"
        onClose={() => {}}
      />,
    );

    expect(
      screen.getByText('follow-up unavailable (evidence unknown)'),
    ).toBeDefined();
    expect(screen.queryByText('no follow-up (loss)')).toBeNull();
    expect(screen.getByText('Net P&L').nextElementSibling?.textContent).toBe(
      '—',
    );
    expect(screen.getAllByText('-')[0]).toBeDefined();
    const breakdown = screen.getByText(/evidence unknown/, { selector: 'p' });
    expect(breakdown.textContent).toContain('spent $0.021');
    expect(breakdown.textContent).toMatch(/across retained history/);
    expect(screen.queryByText(/saved \$0\.000/)).toBeNull();
    expect(screen.queryByText('waiting for follow-up')).toBeNull();
  });

  it('keeps pending unknown P&L nullable instead of displaying zero', () => {
    const detail: CacheKeepaliveDetail = {
      ...cacheKeepaliveRenewedDetailFixture,
      is_active: true,
      net_pnl: null,
      total_spent: 0.006,
      total_avoided: null,
      evidence_status: 'pending',
      total_renewals_current_cycle: 1,
      is_last_pending: true,
      turns: [
        {
          turn_number: 1,
          renewals: 1,
          followed_up: false,
          label: 'agent-in-turn',
          time_ms: 1_700_000_000_000,
          pending: true,
          pnl: null,
          evidence_status: 'pending',
        },
      ],
    };
    vi.spyOn(queries, 'useCacheKeepaliveSessionDetail').mockReturnValue({
      data: detail,
      isLoading: false,
      isError: false,
    } as unknown as PolledDataResult<CacheKeepaliveDetail, Error>);

    renderWithProviders(
      <SessionDetailPane
        principalId="p-123"
        sessionId="pending-unknown-economics"
        onClose={() => {}}
      />,
    );

    expect(screen.getByText('— pending')).toBeDefined();
    expect(screen.queryByText('$0.000 pending')).toBeNull();
    expect(screen.getByText('waiting for follow-up')).toBeDefined();
    expect(screen.getByText('Current turn · Live')).toBeDefined();
    expect(screen.getByText('Net P&L').nextElementSibling?.textContent).toBe(
      '—',
    );
    expect(screen.queryByText(/follow-up unavailable/)).toBeNull();
    expect(screen.queryByText('no follow-up (loss)')).toBeNull();
  });
  it('uses singular wording for one current-cycle renewal', () => {
    const detail = {
      ...cacheKeepaliveRenewedDetailFixture,
      is_active: false,
      total_renewals_current_cycle: 1,
      turns: [
        {
          ...cacheKeepaliveRenewedDetailFixture.turns[0],
          pending: false,
        },
      ],
    };
    vi.spyOn(queries, 'useCacheKeepaliveSessionDetail').mockReturnValue({
      data: detail,
      isLoading: false,
      isError: false,
    } as never);

    renderWithProviders(
      <SessionDetailPane
        principalId="p-123"
        sessionId="one-renewal"
        onClose={() => {}}
      />,
    );

    expect(
      screen.getByText('Current-cycle renewals').nextElementSibling
        ?.textContent,
    ).toBe('1');
    const breakdown = screen.getByText(/saved \$0\.120/);
    expect(breakdown.textContent).toMatch(/across retained history/);
    expect(breakdown.textContent).toMatch(/1 current-cycle renewal/);
  });
  it('replaces stale detail with a stable not-found state after a terminal 404', () => {
    const refetch = vi.fn();
    vi.spyOn(queries, 'useCacheKeepaliveSessionDetail').mockReturnValue({
      data: cacheKeepaliveRenewedDetailFixture,
      isLoading: false,
      isError: true,
      error: new ApiError(404, 'not_found', {}, 'Session not found'),
      refetch,
    } as unknown as ReturnType<typeof queries.useCacheKeepaliveSessionDetail>);

    renderWithProviders(
      <SessionDetailPane
        principalId="p-123"
        sessionId="gone"
        onClose={() => {}}
      />,
    );

    expect(
      screen.getByText('This session is no longer available.'),
    ).toBeDefined();
    expect(
      screen.queryByText('Refresh failed. Showing the last successful detail.'),
    ).toBeNull();
    expect(screen.queryByText('a1f39c2b7e04')).toBeNull();
    fireEvent.click(
      screen.getByRole('button', { name: 'Retry session detail' }),
    );
    expect(refetch).toHaveBeenCalledTimes(1);
  });
  it('resets config and raw disclosures when session identity changes', () => {
    vi.spyOn(queries, 'useCacheKeepaliveSessionDetail').mockReturnValue({
      data: cacheKeepaliveRenewedDetailFixture,
      isLoading: false,
      isError: false,
    } as unknown as ReturnType<typeof queries.useCacheKeepaliveSessionDetail>);
    const onClose = vi.fn();
    const { rerender } = renderWithProviders(
      <SessionDetailPane
        principalId="p-123"
        sessionId="session-a"
        onClose={onClose}
      />,
    );

    fireEvent.click(screen.getByRole('button', { name: /Config in effect/ }));
    fireEvent.click(screen.getByRole('button', { name: /Raw session record/ }));
    expect(screen.getByText('Lead time · 5m')).toBeDefined();

    rerender(
      <QueryClientProvider
        client={
          new QueryClient({ defaultOptions: { queries: { retry: false } } })
        }
      >
        <SessionDetailPane
          principalId="p-123"
          sessionId="session-b"
          onClose={onClose}
        />
      </QueryClientProvider>,
    );

    expect(screen.queryByText('Lead time · 5m')).toBeNull();
    expect(screen.queryByText(/"session_key_hash"/)).toBeNull();
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
        total_renewals_current_cycle: 1,
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

  it('settles a missing detail as not found with explicit recovery actions', () => {
    const refetch = vi.fn();
    vi.spyOn(queries, 'useCacheKeepaliveSessionDetail').mockReturnValue({
      data: undefined,
      isLoading: false,
      isError: true,
      error: new ApiError(404, 'not_found', {}, 'Session not found'),
      refetch,
    } as unknown as PolledDataResult<CacheKeepaliveDetail, Error>);

    renderWithProviders(
      <SessionDetailPane
        principalId="p-123"
        sessionId="missing"
        onClose={() => {}}
      />,
    );

    expect(screen.getByRole('alert').textContent).toContain(
      'This session is no longer available.',
    );
    expect(
      screen.getByRole('button', { name: 'Retry session detail' }),
    ).toBeDefined();
    fireEvent.click(
      screen.getByRole('button', { name: 'Retry session detail' }),
    );
    expect(refetch).toHaveBeenCalledTimes(1);
    expect(
      screen.getAllByRole('button', { name: 'Back to sessions' })[
        screen.getAllByRole('button', { name: 'Back to sessions' }).length - 1
      ],
    ).toBeDefined();
  });
  it('renders English tooltips for D3 time-rail points', () => {
    vi.spyOn(queries, 'useCacheKeepaliveSessionDetail').mockReturnValue({
      data: {
        ...cacheKeepaliveRenewedDetailFixture,
        gaps: [
          {
            cycle_id: 'cycle-1',
            pause_at_ms: 1_700_000_000_000,
            cached_tokens: 20_000,
            pulses: [
              {
                pulse_number: 1,
                time_ms: 1_700_000_240_000,
                offset_sec: 240,
                since_prev_renewal_sec: null,
                spent_usd: 0.05,
                hit: true,
              },
            ],
            ending_kind: 'followup_hit',
            ending_at_ms: 1_700_000_600_000,
            total_duration_sec: 600,
            since_last_renewal_sec: 360,
            spent_usd: 0.05,
            avoided_usd: 0.15,
            net_pnl_usd: 0.1,
          },
        ],
      },
      isLoading: false,
      isError: false,
    } as unknown as PolledDataResult<CacheKeepaliveDetail, Error>);

    renderWithProviders(
      <SessionDetailPane
        principalId="p-123"
        sessionId="a1f39c2b7e04"
        onClose={() => {}}
      />,
    );

    const rail = screen.getByTestId('session-detail-time-rail');
    for (const pointId of ['start', 'pulse-1', 'end']) {
      fireEvent.focus(rail.querySelector(`[data-rail-point="${pointId}"]`)!);
      expect(screen.getByRole('tooltip').textContent).toContain('Timestamp');
      expect(screen.getByRole('tooltip').textContent).toContain(
        'Since last request',
      );
      expect(screen.getByRole('tooltip').textContent).toContain(
        'Since last renewal',
      );
      fireEvent.blur(rail.querySelector(`[data-rail-point="${pointId}"]`)!);
    }
  });
});
