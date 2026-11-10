import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { CacheKeepaliveRow } from '../../../../lib/cacheKeepaliveApi';
import * as queries from '../../../../lib/queries';
import * as visibilityManager from '../../../../lib/visibilityManager';
import { cacheKeepaliveRows } from '../__fixtures__/cacheKeepaliveFixtures';
import {
  CacheKeepaliveSessionsDrawer,
  extractTriggerLabel,
} from '../CacheKeepaliveSessionsDrawer';

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
    min_cache_tokens: 10000,
    classifier: {
      extra_wait_for_user_tools: [],
      treat_end_turn_as_ambiguous: true,
    },
  },
};

const summaryBase = {
  renewing_now: 1,
  sessions_last_5m: 2,
  retained_successful_hits: 17,
  cost_saved: 4.56,
  total_spent: 4.18,
  total_avoided: 16.65,
  unlisted_sessions: 0,
  unlisted_renewals: 0,
  unlisted_spent: 0,
  evidence_status: 'observed' as const,
  unknown_cycles: 0,
  retained_from_ms: 1_699_395_200_000,
};

function renderWithProviders(ui: React.ReactElement) {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return render(
    <QueryClientProvider client={queryClient}>{ui}</QueryClientProvider>,
  );
}

describe('extractTriggerLabel', () => {
  const notTracked = (reason: string, displayReason: string) =>
    ({
      id: 'blank-display-reason',
      state: 'not_tracked',
      reason,
      display_reason: displayReason,
    }) as unknown as CacheKeepaliveRow;

  it('falls back to reason when the persisted display reason is blank', () => {
    for (const blank of ['', '   ']) {
      expect(
        extractTriggerLabel(
          notTracked(
            'cached prefix too small (5000 tokens < minimum 10000)',
            blank,
          ),
        ),
      ).toBe('Prefix < min tokens');
      expect(
        extractTriggerLabel(
          notTracked('agent-in-turn (tool_use: `Bash`) — first renewal', blank),
        ),
      ).toBe('Bash');
    }
  });
});

describe('CacheKeepaliveSessionsDrawer (D3 Split Time-Rail)', () => {
  afterEach(() => {
    cleanup();
    document.body.innerHTML = '';
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
            summary: summaryBase,
            rows: cacheKeepaliveRows.map((r) => ({
              ...r,
              net_pnl: r.netPnl,
              is_active: r.state === 'renewed' || r.state === 'scheduled',
              max_attempts: r.maxAttempts,
              last_message_at_ms: 1_700_000_000_000,
              session_key_hash: r.id,
              retained_renewals: r.attempts ?? 0,
            })),
            next_cursor: null,
          },
        ],
      },
      isLoading: false,
      isError: false,
      hasNextPage: false,
      isFetchingNextPage: false,
      fetchNextPage: vi.fn(),
    } as never);

    vi.spyOn(queries, 'useCacheKeepaliveSessionDetail').mockReturnValue({
      data: {
        id: 'a1f39c2b7e04',
        last_message_at_ms: 1_700_000_600_000,
        state: 'renewed',
        is_active: true,
        ttl: '5m',
        attempts: 3,
        retained_renewals: 3,
        max_attempts: 12,
        reason: 'agent-in-turn (tool_use: `bash`)',
        generation: 1,
        upstream: 'anthropic',
        error: null,
        net_pnl: 9.4802,
        session_key_hash: 'a1f39c2b7e04',
        renewal_tokens: 604_506,
        total_avoided: 9.88,
        total_spent: 0.4,
        total_renewals_current_cycle: 3,
        is_last_pending: false,
        turns: [],
        gaps: [
          {
            cycle_id: 'cycle-1',
            pause_at_ms: 1_700_000_000_000,
            cached_tokens: 604_506,
            pulses: [
              {
                pulse_number: 1,
                time_ms: 1_700_000_270_000,
                offset_sec: 270,
                since_prev_renewal_sec: null,
                spent_usd: 0.1209,
                hit: true,
              },
            ],
            ending_kind: 'followup_hit',
            ending_at_ms: 1_700_000_323_000,
            total_duration_sec: 323,
            since_last_renewal_sec: 53,
            spent_usd: 0.1209,
            avoided_usd: 3.0225,
            net_pnl_usd: 2.9016,
          },
        ],
        config_snapshot: null,
        raw_record: {
          session_key_hash: 'a1f39c2b7e04',
          principal_id: 'p-123',
          upstream_id: 'anthropic',
          generation: 1,
          renewal_count: 3,
          ttl: '5m',
          status: 'renewed',
          enqueue_state: 'enqueued',
        },
      },
      isLoading: false,
      isError: false,
    } as never);
  });

  it('renders dialog with title, principal name, and single-line summary bar', () => {
    const onOpenChange = vi.fn();
    renderWithProviders(
      <CacheKeepaliveSessionsDrawer
        open={true}
        onOpenChange={onOpenChange}
        principal={mockPrincipal}
      />,
    );

    expect(
      screen.getByRole('dialog', { name: /Cache keepalive sessions/i }),
    ).toBeDefined();
    expect(screen.getByText('Test Principal')).toBeDefined();
    const eq = screen.getByTestId('cache-keepalive-pnl-equation');
    expect(eq.textContent).toContain('Saved $16.65 · Spent $4.18');

    fireEvent.keyDown(document, { key: 'Escape' });
    expect(onOpenChange).toHaveBeenCalledWith(false);
  });

  it('groups tracked terminal reasons under Agent in-turn and never under TTL expired or Max renewals reached', () => {
    renderWithProviders(
      <CacheKeepaliveSessionsDrawer
        open={true}
        onOpenChange={() => {}}
        principal={mockPrincipal}
      />,
    );

    const region = screen.getByTestId('cache-keepalive-session-list-region');
    const groupNodes = Array.from(
      region.querySelectorAll('[data-trigger-group]'),
    ).map((el) => el.getAttribute('data-trigger-group'));

    expect(groupNodes).toContain('Agent in-turn');
    expect(groupNodes).not.toContain('TTL expired');
    expect(groupNodes).not.toContain('Max renewals reached');
    expect(groupNodes).not.toContain('Max duration reached');
  });

  it('collapses and expands trigger groups on click', async () => {
    const user = userEvent.setup();
    renderWithProviders(
      <CacheKeepaliveSessionsDrawer
        open={true}
        onOpenChange={() => {}}
        principal={mockPrincipal}
      />,
    );

    const region = screen.getByTestId('cache-keepalive-session-list-region');
    const agentGroup = region.querySelector(
      '[data-trigger-group="Agent in-turn"]',
    )!;
    const toggleBtn = within(agentGroup as HTMLElement).getAllByRole(
      'button',
    )[0]!;

    expect(toggleBtn.getAttribute('aria-expanded')).toBe('true');
    expect(
      agentGroup.querySelectorAll('[data-session-id]').length,
    ).toBeGreaterThan(0);

    await user.click(toggleBtn);
    expect(toggleBtn.getAttribute('aria-expanded')).toBe('false');
    expect(agentGroup.querySelectorAll('[data-session-id]').length).toBe(0);

    await user.click(toggleBtn);
    expect(toggleBtn.getAttribute('aria-expanded')).toBe('true');
    expect(
      agentGroup.querySelectorAll('[data-session-id]').length,
    ).toBeGreaterThan(0);
  });

  it('aligns group and item decimal points in separate scopes without trailing zero padding', () => {
    renderWithProviders(
      <CacheKeepaliveSessionsDrawer
        open={true}
        onOpenChange={() => {}}
        principal={mockPrincipal}
      />,
    );

    const region = screen.getByTestId('cache-keepalive-session-list-region');
    expect(
      region.querySelectorAll('[data-decimal-dot="group"]').length,
    ).toBeGreaterThan(0);
    expect(
      region.querySelectorAll('[data-decimal-dot="item"]').length,
    ).toBeGreaterThan(0);
    expect(screen.getAllByText('+$0.0936').length).toBeGreaterThanOrEqual(1);
    expect(screen.queryByText(/\.\d*00$/)).toBeNull();
  });

  it('keeps long tool triggers truncated while USD values stay nowrap and decimal-aligned', () => {
    const longTrigger = 'mcp__plugin_supabase_supabase__execute_sql';
    const rows = cacheKeepaliveRows.map((r, idx) => ({
      ...r,
      reason:
        idx === 0
          ? `agent-in-turn (tool_use: \`${longTrigger}\`)`
          : (r.reason as string),
      net_pnl: r.netPnl,
      is_active: r.state === 'renewed' || r.state === 'scheduled',
      max_attempts: r.maxAttempts,
      last_message_at_ms: 1_700_000_000_000,
      session_key_hash: r.id,
      retained_renewals: r.attempts ?? 0,
    }));

    vi.spyOn(queries, 'useCacheKeepaliveSessions').mockReturnValue({
      data: {
        pages: [{ summary: summaryBase, rows, next_cursor: null }],
      },
      isLoading: false,
      isError: false,
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
    const group = region.querySelector(
      `[data-trigger-group="${longTrigger}"]`,
    ) as HTMLElement;
    expect(group).toBeDefined();

    const triggerChip = within(group).getByText(longTrigger);
    expect(triggerChip.className).toContain('truncate');
    expect(triggerChip.className).toContain('font-mono');

    const groupUsd = group.querySelector(
      '[data-decimal-scope="group"]',
    ) as HTMLElement;
    const itemUsd = group.querySelector(
      '[data-decimal-scope="item"]',
    ) as HTMLElement;
    for (const usd of [groupUsd, itemUsd]) {
      expect(usd.className).toContain('whitespace-nowrap');
      expect(usd.className).toContain('shrink-0');
      expect(usd.querySelector('[data-decimal-dot]')).not.toBeNull();
    }
    expect(group.textContent).not.toMatch(/\.\d*00/);
  });

  it('selects a session row and renders its D3 time-rail in the right split pane', async () => {
    const user = userEvent.setup();
    renderWithProviders(
      <CacheKeepaliveSessionsDrawer
        open={true}
        onOpenChange={() => {}}
        principal={mockPrincipal}
      />,
    );

    const sessionBtn = document.querySelector(
      '[data-session-id="f4b71a0c9d52"]',
    ) as HTMLElement;
    await user.click(sessionBtn);

    expect(sessionBtn.getAttribute('aria-current')).toBe('true');
    expect(screen.getByTestId('session-detail-time-rail')).toBeDefined();
  });

  it('shows off-state banner when keepalive is disabled for the principal', () => {
    renderWithProviders(
      <CacheKeepaliveSessionsDrawer
        open={true}
        onOpenChange={() => {}}
        principal={{
          ...mockPrincipal,
          cache_keepalive: {
            ...mockPrincipal.cache_keepalive!,
            enabled: false,
          },
        }}
      />,
    );

    expect(screen.getByTestId('cache-keepalive-sessions-off')).toBeDefined();
  });
  it('keeps principal-wide OpenAI rows and applies tracked filtering for both TTLs', () => {
    const rows = [
      {
        id: 'openai-30m',
        last_message_at_ms: 1_700_000_000_000,
        relative_time: '1m ago',
        state: 'renewed' as const,
        is_active: false,
        ttl: '30m' as const,
        attempts: 1,
        max_attempts: 12,
        reason: 'OpenAI Responses 30m renewal',
        display_reason: null,
        generation: 1,
        upstream: 'openai',
        error: null,
        net_pnl: null,
        total_spent: 0,
        total_avoided: null,
        evidence_status: 'none' as const,
        session_key_hash: 'openai-30m',
        retained_renewals: 1,
      },
      {
        id: 'openai-12h',
        last_message_at_ms: 1_700_000_000_000,
        relative_time: '1m ago',
        state: 'not_tracked' as const,
        is_active: false,
        ttl: '12h' as const,
        attempts: null,
        max_attempts: 12,
        reason: '12h retention recorded without read re-anchoring',
        display_reason: null,
        generation: 1,
        upstream: 'openai',
        error: null,
        net_pnl: null,
        total_spent: 0,
        total_avoided: null,
        evidence_status: 'none' as const,
        session_key_hash: 'openai-12h',
      },
      {
        id: 'openai-5m-untracked',
        last_message_at_ms: 1_700_000_000_000,
        relative_time: '1m ago',
        state: 'not_tracked' as const,
        is_active: false,
        ttl: '5m' as const,
        attempts: null,
        max_attempts: 12,
        reason: '5m retention not tracked',
        display_reason: null,
        generation: 1,
        upstream: 'openai',
        error: null,
        net_pnl: null,
        total_spent: 0,
        total_avoided: null,
        evidence_status: 'none' as const,
        session_key_hash: 'openai-5m-untracked',
      },
    ];
    vi.mocked(queries.useCacheKeepaliveSessions).mockReturnValue({
      data: {
        pages: [{ summary: summaryBase, rows, next_cursor: null }],
      },
      isLoading: false,
      isError: false,
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

    expect(screen.getByTestId('cache-keepalive-scope').textContent).toContain(
      'Principal-wide history',
    );
    expect(queries.useCacheKeepaliveSessions).toHaveBeenCalledWith(
      'p-123',
      { horizon: '24h', status: 'all' },
      true,
    );
    expect(screen.getByTitle('openai-30m')).toBeDefined();
    expect(screen.getByTitle('openai-12h')).toBeDefined();
    expect(screen.queryByTitle('openai-5m-untracked')).toBeNull();

    cleanup();
    renderWithProviders(
      <CacheKeepaliveSessionsDrawer
        open={true}
        onOpenChange={() => {}}
        principal={{
          ...mockPrincipal,
          cache_keepalive: {
            ...mockPrincipal.cache_keepalive!,
            enabled: false,
          },
        }}
      />,
    );
    expect(screen.getByTitle('openai-12h')).toBeDefined();
    expect(screen.getByTestId('cache-keepalive-sessions-off')).toBeDefined();
  });
  it('keeps paging when a page contains only hidden not-tracked rows', async () => {
    const fetchNextPage = vi.fn();
    vi.mocked(queries.useCacheKeepaliveSessions).mockReturnValue({
      data: {
        pages: [
          {
            summary: summaryBase,
            rows: [
              {
                id: 'openai-5m-hidden',
                last_message_at_ms: 1_700_000_000_000,
                state: 'not_tracked',
                is_active: false,
                ttl: '5m',
                attempts: null,
                max_attempts: 12,
                reason: 'ordinary not-tracked row',
                generation: 1,
                upstream: 'openai',
                error: null,
                net_pnl: null,
                total_spent: 0,
                total_avoided: null,
                evidence_status: 'none',
                session_key_hash: 'openai-5m-hidden',
              },
            ],
            next_cursor: 'next-page',
          },
        ],
      },
      isLoading: false,
      isError: false,
      hasNextPage: true,
      isFetchingNextPage: false,
      fetchNextPage,
    } as never);

    renderWithProviders(
      <CacheKeepaliveSessionsDrawer
        open={true}
        onOpenChange={() => {}}
        principal={mockPrincipal}
      />,
    );

    await waitFor(() => expect(fetchNextPage).toHaveBeenCalledOnce());
  });
});

it('renders the non-clickable deleted-session aggregate with real money', () => {
  vi.mocked(queries.useCacheKeepaliveSessions).mockReturnValue({
    data: {
      pages: [
        {
          summary: {
            ...summaryBase,
            unlisted_sessions: 2,
            unlisted_renewals: 5,
            unlisted_spent: 0.03,
          },
          rows: [],
          next_cursor: null,
        },
      ],
    },
    isLoading: false,
    isError: false,
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

  const deleted = screen.getByTestId('cache-keepalive-deleted-sessions');
  expect(deleted.textContent).toContain('Deleted sessions (2)');
  expect(deleted.textContent).toContain('5 renewals');
  expect(deleted.textContent).toContain('Spent $0.03');
  expect(deleted.textContent).toContain('−$0.03');
  expect(deleted.querySelector('button')).toBeNull();
});
