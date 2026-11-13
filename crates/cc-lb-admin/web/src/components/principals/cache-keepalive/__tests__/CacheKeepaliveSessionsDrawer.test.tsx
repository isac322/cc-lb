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
import type { CacheKeepaliveRow } from '../../../../lib/cacheKeepaliveApi';
import * as queries from '../../../../lib/queries';
import { apiRowFromFixture } from '../../../../lib/test-utils/cacheKeepalive-fixtures';
import * as visibilityManager from '../../../../lib/visibilityManager';
import { Drawer } from '../../../ui/primitives';
import { cacheKeepaliveRows } from '../__fixtures__/cacheKeepaliveFixtures';
import {
  CacheKeepaliveSessionsDrawer,
  splitUsdNoTrailingZeros,
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
  unlisted_net: 0,
  evidence_status: 'observed' as const,
  unknown_cycles: 0,
  not_needed_cycles: 0,
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
            groups: [
              {
                key: 'Bash',
                label: 'Bash',
                kind: 'tool',
                sessions: 1,
                net_pnl: 0.102,
                total_spent: 0.048,
                total_avoided: 0.12,
              },
              {
                key: 'Edit',
                label: 'Edit',
                kind: 'tool',
                sessions: 1,
                net_pnl: 0,
                total_spent: 0,
                total_avoided: 0.12,
              },
              {
                key: 'Agent in-turn',
                label: 'Agent in-turn',
                kind: 'state',
                sessions: 4,
                net_pnl: 0.1512,
                total_spent: 0.204,
                total_avoided: 0.48,
              },
              {
                key: 'stop_reason=end_turn',
                label: 'stop_reason=end_turn',
                kind: 'state',
                sessions: 1,
                net_pnl: 0,
                total_spent: 0,
                total_avoided: 0,
              },
            ],
            rows: cacheKeepaliveRows.map((r) => ({
              ...r,
              net_pnl: r.netPnl,
              is_active: r.state === 'renewed' || r.state === 'scheduled',
              max_attempts: r.maxAttempts,
              last_message_at_ms: 1_700_000_000_000,
              session_key_hash: r.id,
              retained_renewals: r.attempts ?? 0,
              group_key: r.groupKey,
              group_label: r.groupLabel,
              group_kind: r.groupKind,
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
        follow_up_outcomes: {
          not_needed: 0,
          observed: 1,
          pending: 0,
          no_follow_up: 0,
          unknown: 0,
        },
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
            natural_expiry_ms: 1_700_000_300_000,
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

  it('renders dialog with title, principal name, and quiet three-value summary strip', () => {
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
    const metrics = within(eq).getAllByRole('term');
    expect(metrics.map((term) => term.textContent)).toEqual([
      'Net',
      'Saved',
      'Spent',
    ]);
    expect(
      within(eq)
        .getAllByRole('definition')
        .map((value) => value.textContent),
    ).toEqual(['+$4.56', '$16.65', '$4.18']);
    expect(within(eq).queryByText(/Follow-up|Price unknown/)).toBeNull();
    expect(within(eq).queryByText(/^Showing /)).toBeNull();
    expect(within(eq).getByRole('radio', { name: 'All time' })).toBeDefined();

    fireEvent.keyDown(document, { key: 'Escape' });
    expect(onOpenChange).toHaveBeenCalledWith(false);
  });

  it('omits complete-list filler and gives the two desktop panes their own scroll containers', () => {
    renderWithProviders(
      <CacheKeepaliveSessionsDrawer
        open
        principal={mockPrincipal}
        onOpenChange={() => {}}
      />,
    );
    const body = screen.getByTestId('cache-keepalive-sessions-body');
    expect(body.parentElement?.className).toContain('overflow-hidden');
    const region = screen.getByTestId('cache-keepalive-session-list-region');
    expect(region.className).toContain('md:overflow-hidden');
    const grid = region.firstElementChild!;
    expect(grid.className).toContain('md:h-full');
    for (const pane of Array.from(grid.children)) {
      expect(pane.className).toContain('md:overflow-y-auto');
      expect(pane.className).toContain('min-h-0');
    }
    expect(screen.queryByText(/^All \d+ sessions shown$/)).toBeNull();
    expect(screen.queryByTestId('cache-keepalive-session-count')).toBeNull();
    expect(
      screen.queryByRole('button', { name: 'Load more sessions' }),
    ).toBeNull();
    const detail = screen.getByTestId('session-detail-content');
    const header = detail.children[1];
    expect(header.className).toContain('md:sticky');
    expect(header.className).toContain('md:top-0');
    const back = within(detail).getByRole('button', {
      name: 'Back to sessions',
    });
    expect(back.className).toContain('max-md:min-h-11');
    expect(back.querySelector('svg')).not.toBeNull();
    const label = grid.children[0].querySelector('[data-session-id] .truncate');
    expect(label).not.toBeNull();
  });

  it.each(['error', 'loading', 'empty', 'off'] as const)(
    'keeps the non-split %s branch scrollable on desktop',
    (branch) => {
      const original = vi
        .mocked(queries.useCacheKeepaliveSessions)
        .getMockImplementation()!('p-123', { horizon: '24h' }, true);
      const refetch = vi.fn();
      vi.mocked(queries.useCacheKeepaliveSessions).mockReturnValue({
        ...original,
        data:
          branch === 'error' || branch === 'loading'
            ? undefined
            : {
                pages: [
                  {
                    summary: summaryBase,
                    groups: [],
                    rows: [],
                    next_cursor: null,
                  },
                ],
                pageParams: [null],
              },
        isError: branch === 'error',
        isLoading: branch === 'loading',
        refetch,
      } as never);
      renderWithProviders(
        <CacheKeepaliveSessionsDrawer
          open
          principal={
            branch === 'off'
              ? {
                  ...mockPrincipal,
                  cache_keepalive: {
                    ...mockPrincipal.cache_keepalive!,
                    enabled: false,
                  },
                }
              : mockPrincipal
          }
          onOpenChange={() => {}}
        />,
      );
      const region = screen.getByTestId('cache-keepalive-session-list-region');
      expect(region.className).toContain('overflow-y-auto');
      expect(region.className).not.toContain('md:overflow-hidden');
      if (branch === 'error') {
        fireEvent.click(
          within(region).getByRole('button', { name: 'Retry sessions' }),
        );
        expect(refetch).toHaveBeenCalledOnce();
      }
    },
  );

  it('keeps the shared Drawer body scrollable unless content explicitly owns scrolling', () => {
    render(
      <Drawer open onOpenChange={() => {}} title="Other drawer" width="md">
        <div data-testid="other-drawer-content">Content</div>
      </Drawer>,
    );
    const body = screen.getByTestId('other-drawer-content').parentElement!;
    expect(body.className).toContain('overflow-y-auto');
    expect(body.className).not.toContain('overflow-hidden');
  });

  it.each([
    [null, 'observed', 0, 'Price unknown'],
    [4.56, 'pending', 0, 'Follow-up pending'],
    [-0.006, 'observed', 1, '1 not needed'],
    [null, 'unknown', 2, 'Price unknown · 2 not needed'],
  ] as const)(
    'qualifies summary economics only when required (%s, %s, %s)',
    (net, evidence, notNeeded, label) => {
      const original = vi
        .mocked(queries.useCacheKeepaliveSessions)
        .getMockImplementation()!('p-123', { horizon: '24h' }, true);
      if (!original.data) throw new Error('Expected mocked session data');
      original.data.pageParams ??= [null];
      original.data.pages = original.data.pages.map((page) => ({
        ...page,
        summary: {
          ...summaryBase,
          cost_saved: net,
          evidence_status: evidence,
          not_needed_cycles: notNeeded,
        },
      }));
      renderWithProviders(
        <CacheKeepaliveSessionsDrawer
          open
          principal={mockPrincipal}
          onOpenChange={() => {}}
        />,
      );
      expect(
        within(screen.getByTestId('cache-keepalive-pnl-equation')).getByText(
          label,
        ),
      ).toBeDefined();
    },
  );

  it('keeps error-overlap rows danger toned without replacing their lifecycle label', () => {
    renderWithProviders(
      <CacheKeepaliveSessionsDrawer
        open
        principal={mockPrincipal}
        onOpenChange={() => {}}
      />,
    );
    const errorRow = cacheKeepaliveRows.find((row) => row.error != null)!;
    const row = screen.getByTitle(errorRow.id);
    expect(row.querySelector('.status-dot')?.className).toContain('danger');
    const status = within(row).getByRole('status', {
      name: 'Session state: Warm',
    });
    expect(status.firstElementChild?.className).toContain('text-danger-text');
  });

  it.each([
    [84.9, '+$84.90'],
    [0.09996, '+$0.10'],
    [0.0326, '+$0.0326'],
  ] as const)(
    'uses the same rounded money precision in aligned list values (%s)',
    (amount, expected) => {
      const parts = splitUsdNoTrailingZeros(amount, true);
      expect(
        `${parts.intPart}${parts.hasDot ? '.' : ''}${parts.fracDigits}`,
      ).toBe(expected);
    },
  );

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

  it('aligns group and item decimal points in separate scopes with cents for larger amounts', () => {
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
    expect(
      Array.from(region.querySelectorAll('[data-decimal-scope="item"]')).some(
        (value) => value.textContent === '+$0.0936',
      ),
    ).toBe(true);
    expect(
      region.querySelector('[data-decimal-scope]')?.className,
    ).not.toContain('font-mono');
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
      group_key: idx === 0 ? longTrigger : r.groupKey,
      group_label: idx === 0 ? longTrigger : r.groupLabel,
      group_kind: idx === 0 ? 'tool' : r.groupKind,
    }));

    vi.spyOn(queries, 'useCacheKeepaliveSessions').mockReturnValue({
      data: {
        pages: [
          {
            summary: summaryBase,
            groups: [
              {
                key: longTrigger,
                label: longTrigger,
                kind: 'tool',
                sessions: 1,
                net_pnl: 0.102,
                total_spent: 0.048,
                total_avoided: 0.12,
              },
              {
                key: 'Agent in-turn',
                label: 'Agent in-turn',
                kind: 'state',
                sessions: 4,
                net_pnl: 0.1512,
                total_spent: 0.204,
                total_avoided: 0.48,
              },
              {
                key: 'stop_reason=end_turn',
                label: 'stop_reason=end_turn',
                kind: 'state',
                sessions: 1,
                net_pnl: 0,
                total_spent: 0,
                total_avoided: 0,
              },
            ],
            rows,
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
    expect(groupUsd.textContent).toBe('+$0.10');
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

  it('keys detail by the selected row lifecycle and same-generation economics', () => {
    const rowForState = (
      is_active: boolean,
      generation: number,
      net_pnl = 0.102,
    ) => ({
      id: 'a1f39c2b7e04',
      last_message_at_ms: 1_700_000_000_000,
      state: 'renewed' as const,
      is_active,
      ttl: '5m' as const,
      attempts: 3,
      max_attempts: 12,
      reason: 'agent-in-turn (tool_use: `bash`)',
      display_reason: null,
      generation,
      upstream: 'anthropic',
      error: null,
      net_pnl,
      total_spent: 0.048,
      total_avoided: 0.12,
      evidence_status: 'observed' as const,
      session_key_hash: 'a1f39c2b7e04',
      retained_renewals: 3,
      group_key: 'Bash',
      group_label: 'Bash',
      group_kind: 'tool' as const,
    });
    const listResult = (row: CacheKeepaliveRow) => ({
      data: {
        pages: [
          {
            summary: summaryBase,
            groups: [
              {
                key: 'Bash',
                label: 'Bash',
                kind: 'tool' as const,
                sessions: 1,
                net_pnl: 0.102,
                total_spent: 0.048,
                total_avoided: 0.12,
              },
            ],
            rows: [row],
            next_cursor: null,
          },
        ],
      },
      isLoading: false,
      isError: false,
      hasNextPage: false,
      isFetchingNextPage: false,
      fetchNextPage: vi.fn(),
    });
    vi.mocked(queries.useCacheKeepaliveSessions).mockReturnValue(
      listResult(rowForState(false, 1)) as never,
    );

    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });
    const initialFingerprint = JSON.stringify([
      1,
      false,
      'renewed',
      'observed',
      0.102,
      0.048,
      0.12,
      3,
    ]);
    const { rerender } = render(
      <QueryClientProvider client={queryClient}>
        <CacheKeepaliveSessionsDrawer
          open={true}
          onOpenChange={() => {}}
          principal={mockPrincipal}
        />
      </QueryClientProvider>,
    );
    expect(queries.useCacheKeepaliveSessionDetail).toHaveBeenLastCalledWith(
      'p-123',
      'a1f39c2b7e04',
      '24h',
      initialFingerprint,
      rowForState(false, 1),
    );

    for (const netPnl of [0.102, 9]) {
      vi.mocked(queries.useCacheKeepaliveSessions).mockReturnValue(
        listResult(rowForState(true, 2, netPnl)) as never,
      );
      rerender(
        <QueryClientProvider client={queryClient}>
          <CacheKeepaliveSessionsDrawer
            open={true}
            onOpenChange={() => {}}
            principal={mockPrincipal}
          />
        </QueryClientProvider>,
      );

      expect(queries.useCacheKeepaliveSessionDetail).toHaveBeenLastCalledWith(
        'p-123',
        'a1f39c2b7e04',
        '24h',
        JSON.stringify([
          2,
          true,
          'renewed',
          'observed',
          netPnl,
          0.048,
          0.12,
          3,
        ]),
        rowForState(true, 2, netPnl),
      );
    }
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
  it('keeps server-visible OpenAI rows without client-side filtering', () => {
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
        pages: [
          {
            summary: summaryBase,
            groups: [
              {
                key: 'Agent in-turn',
                label: 'Agent in-turn',
                kind: 'state',
                sessions: 3,
                net_pnl: null,
                total_spent: 0,
                total_avoided: null,
              },
            ],
            rows: rows.map((row) => ({
              ...row,
              group_key: 'Agent in-turn',
              group_label: 'Agent in-turn',
              group_kind: 'state',
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

    renderWithProviders(
      <CacheKeepaliveSessionsDrawer
        open={true}
        onOpenChange={() => {}}
        principal={mockPrincipal}
      />,
    );

    expect(screen.queryByText(/Principal-wide|All providers/)).toBeNull();
    expect(queries.useCacheKeepaliveSessions).toHaveBeenCalledWith(
      'p-123',
      { horizon: '24h', status: 'visible' },
      true,
    );
    expect(screen.getByTitle('openai-30m')).toBeDefined();
    expect(screen.getByTitle('openai-12h')).toBeDefined();
    expect(screen.getByTitle('openai-5m-untracked')).toBeDefined();

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
    expect(queries.useCacheKeepaliveSessions).toHaveBeenLastCalledWith(
      'p-123',
      { horizon: '24h', status: 'all' },
      true,
    );
  });

  it.each([
    ['renewed', 'Warm', 'live'],
    ['scheduled', 'Warm', 'live'],
    ['in_request', 'In use', 'neutral'],
    ['capped', 'At limit', 'neutral'],
    ['expired', 'Expired', 'neutral'],
    ['cancelled', 'Returned', 'ok'],
    ['failed', 'Failed', 'danger'],
    ['not_tracked', 'Not tracked', 'neutral'],
  ] as const)(
    'keeps a server-visible %s row without inventing a renewal or cancellation',
    (state, badgeLabel, dotTone) => {
      const row: CacheKeepaliveRow = {
        ...apiRowFromFixture(cacheKeepaliveRows[0], 0),
        id: 'request-lifecycle',
        state,
        is_active: false,
        retained_renewals: 0,
        attempts: 0,
        error: null,
        net_pnl: state === 'in_request' || state === 'expired' ? 0.12 : 0,
      };
      vi.mocked(queries.useCacheKeepaliveSessions).mockReturnValue({
        data: {
          pages: [
            {
              summary: summaryBase,
              groups: [
                {
                  key: row.group_key,
                  label: row.group_label,
                  kind: row.group_kind,
                  sessions: 1,
                  net_pnl: row.net_pnl,
                  total_spent: row.total_spent,
                  total_avoided: row.total_avoided,
                },
              ],
              rows: [row],
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

      const button = screen.getByTitle(row.id);
      expect(within(button).getByText('0 renewals')).toBeDefined();
      expect(
        button.querySelector('.status-dot')?.classList.contains(dotTone),
      ).toBe(true);
      const badge = within(button).getByRole('status', {
        name: `Session state: ${badgeLabel}`,
      });
      expect(badge.textContent).toBe(badgeLabel);
      expect(within(button).getAllByRole('status')).toHaveLength(1);
      const toneClass =
        dotTone === 'danger'
          ? 'text-danger-text'
          : dotTone === 'live'
            ? 'text-accent-text'
            : dotTone === 'ok'
              ? 'text-traffic-success-text'
              : 'text-text-muted';
      expect(badge.firstElementChild?.className).toContain(toneClass);
      if (state === 'in_request' || state === 'expired') {
        expect(within(button).queryByText(/Cancelled|Failed/)).toBeNull();
      }
      expect(queries.useCacheKeepaliveSessions).toHaveBeenCalledWith(
        'p-123',
        { horizon: '24h', status: 'visible' },
        true,
      );
    },
  );
  it('loads the next page only after the explicit control is clicked', async () => {
    const user = userEvent.setup();
    const fetchNextPage = vi.fn();
    vi.mocked(queries.useCacheKeepaliveSessions).mockReturnValue({
      data: {
        pages: [
          {
            summary: summaryBase,
            groups: [
              {
                key: 'Other',
                label: 'Other',
                kind: 'state',
                sessions: 2,
                net_pnl: null,
                total_spent: 0,
                total_avoided: null,
              },
            ],
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
                group_key: 'Other',
                group_label: 'Other',
                group_kind: 'state',
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

    expect(
      screen.getByTestId('cache-keepalive-session-count').textContent,
    ).toBe('1 of 2 sessions shown');
    expect(fetchNextPage).not.toHaveBeenCalled();
    await user.click(
      screen.getByRole('button', { name: /Load more sessions/i }),
    );
    expect(fetchNextPage).toHaveBeenCalledOnce();
  });
  it('disambiguates colliding thread prefixes and preserves labels across polls and loaded pages', () => {
    const ids = [
      'p-123/thread:01a1c484-1111-aaaaaa',
      'p-123/thread:01a1c484-2222-bbbbbb',
      'p-123/thread:01a1c484-3333-aaaaaa',
    ];
    const rows = ids.map((id, index) => ({
      ...apiRowFromFixture(cacheKeepaliveRows[0], index),
      id,
    }));
    const setRows = (pages: CacheKeepaliveRow[][]) => {
      vi.mocked(queries.useCacheKeepaliveSessions).mockReturnValue({
        data: {
          pages: pages.map((rows) => ({
            rows,
            summary: summaryBase,
            next_cursor: null,
            groups: [
              {
                key: rows[0]?.group_key ?? 'Bash',
                label: 'Bash',
                kind: 'tool',
                sessions: 3,
                net_pnl: 0,
                total_spent: 0,
                total_avoided: 0,
              },
            ],
          })),
        },
        isLoading: false,
        isError: false,
        hasNextPage: false,
        isFetchingNextPage: false,
        fetchNextPage: vi.fn(),
      } as never);
    };
    const client = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });
    const ui = () => (
      <QueryClientProvider client={client}>
        <CacheKeepaliveSessionsDrawer
          open
          principal={mockPrincipal}
          onOpenChange={() => {}}
        />
      </QueryClientProvider>
    );
    setRows([rows.slice(0, 2)]);
    const { rerender } = render(ui());
    expect(
      within(screen.getByTitle(ids[0])).getByText('01a1c484…aaaaaa'),
    ).toBeDefined();
    expect(
      within(screen.getByTitle(ids[1])).getByText('01a1c484…bbbbbb'),
    ).toBeDefined();
    setRows([rows.slice(0, 2), [rows[2]]]);
    rerender(ui());
    expect(
      within(screen.getByTitle(ids[0])).getByText('01a1c484…aaaaaa'),
    ).toBeDefined();
    expect(
      within(screen.getByTitle(ids[1])).getByText('01a1c484…bbbbbb'),
    ).toBeDefined();
    expect(
      within(screen.getByTitle(ids[2])).queryByText('01a1c484…aaaaaa'),
    ).toBeNull();
    setRows([[rows[1]]]);
    rerender(ui());
    expect(
      within(screen.getByTitle(ids[1])).getByText('01a1c484…bbbbbb'),
    ).toBeDefined();
    setRows([[rows[2], rows[0], rows[1]]]);
    rerender(ui());
    expect(
      within(screen.getByTitle(ids[0])).getByText('01a1c484…aaaaaa'),
    ).toBeDefined();
    expect(
      within(screen.getByTitle(ids[1])).getByText('01a1c484…bbbbbb'),
    ).toBeDefined();
  });
  it('upgrades a bare prefix label when a peer sharing the prefix loads later', () => {
    const ids = [
      'p-123/thread:01a1c484-1111-aaaaaa',
      'p-123/thread:01a1c484-2222-bbbbbb',
    ];
    const rows = ids.map((id, index) => ({
      ...apiRowFromFixture(cacheKeepaliveRows[0], index),
      id,
    }));
    const setRows = (visible: CacheKeepaliveRow[]) => {
      vi.mocked(queries.useCacheKeepaliveSessions).mockReturnValue({
        data: {
          pages: [
            {
              rows: visible,
              summary: summaryBase,
              next_cursor: null,
              groups: [
                {
                  key: visible[0]?.group_key ?? 'Bash',
                  label: 'Bash',
                  kind: 'tool',
                  sessions: visible.length,
                  net_pnl: 0,
                  total_spent: 0,
                  total_avoided: 0,
                },
              ],
            },
          ],
        },
        isLoading: false,
        isError: false,
        hasNextPage: false,
        isFetchingNextPage: false,
        fetchNextPage: vi.fn(),
      } as never);
    };
    const client = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });
    const ui = () => (
      <QueryClientProvider client={client}>
        <CacheKeepaliveSessionsDrawer
          open
          principal={mockPrincipal}
          onOpenChange={() => {}}
        />
      </QueryClientProvider>
    );
    // Given: a shorter range shows only one of the two sessions.
    setRows([rows[0]]);
    const { rerender } = render(ui());
    expect(
      within(screen.getByTitle(ids[0])).getByText('01a1c484'),
    ).toBeDefined();
    // When: a wider range loads the second session with the same prefix.
    setRows(rows);
    rerender(ui());
    // Then: neither row keeps the ambiguous bare prefix.
    expect(
      within(screen.getByTitle(ids[0])).getByText('01a1c484…aaaaaa'),
    ).toBeDefined();
    expect(
      within(screen.getByTitle(ids[1])).getByText('01a1c484…bbbbbb'),
    ).toBeDefined();
    // And: returning to the shorter range keeps the upgraded label stable.
    setRows([rows[0]]);
    rerender(ui());
    expect(
      within(screen.getByTitle(ids[0])).getByText('01a1c484…aaaaaa'),
    ).toBeDefined();
  });
  it.each([
    ['short', ['session-2', 'session-3']],
    ['exactly prefix plus tail length', ['12345678aaaaaa', '12345678bbbbbb']],
  ] as const)(
    'shows full %s ids instead of overlapping prefix/tail text',
    (_name, rawIds) => {
      const rows = rawIds.map((raw, index) => ({
        ...apiRowFromFixture(cacheKeepaliveRows[0], index),
        id: `p-123/thread:${raw}`,
      }));
      vi.mocked(queries.useCacheKeepaliveSessions).mockReturnValue({
        data: {
          pages: [
            {
              rows,
              summary: summaryBase,
              next_cursor: null,
              groups: [
                {
                  key: rows[0].group_key,
                  label: rows[0].group_label,
                  kind: rows[0].group_kind,
                  sessions: rows.length,
                  net_pnl: 0,
                  total_spent: 0,
                  total_avoided: 0,
                },
              ],
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
          open
          principal={mockPrincipal}
          onOpenChange={() => {}}
        />,
      );
      rows.forEach((row, index) => {
        const button = screen.getByTitle(row.id);
        expect(within(button).getByText(rawIds[index])).toBeDefined();
        expect(within(button).queryByText(/…/)).toBeNull();
      });
    },
  );
});

it('renders the non-clickable deleted-session aggregate with real money', () => {
  vi.spyOn(queries, 'useCacheKeepaliveSessions').mockReturnValue({
    data: {
      pages: [
        {
          summary: {
            ...summaryBase,
            unlisted_sessions: 2,
            unlisted_renewals: 5,
            unlisted_spent: 0.03,
            unlisted_net: -0.03,
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

it('shows an unknown deleted-session net when a deleted renewal has no known cost', () => {
  cleanup();
  vi.spyOn(queries, 'useCacheKeepaliveSessions').mockReturnValue({
    data: {
      pages: [
        {
          summary: {
            ...summaryBase,
            cost_saved: null,
            total_avoided: null,
            unlisted_sessions: 1,
            unlisted_renewals: 2,
            unlisted_spent: 0.01,
            unlisted_net: null,
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
  expect(deleted.textContent).not.toContain('−$0.01');
  expect(deleted.textContent).toContain('Spent $0.01');
});

it('seeds the 24h card summary cache in the list-response shape the card selects from', () => {
  cleanup();
  vi.spyOn(queries, 'useCacheKeepaliveSessions').mockReturnValue({
    data: {
      pages: [
        {
          summary: { ...summaryBase, cost_saved: 7.5 },
          groups: [],
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
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });

  render(
    <QueryClientProvider client={queryClient}>
      <CacheKeepaliveSessionsDrawer
        open={true}
        onOpenChange={() => {}}
        principal={mockPrincipal}
      />
    </QueryClientProvider>,
  );

  const cached = queryClient.getQueryData<{ summary: { cost_saved: number } }>(
    queries.qk.cacheKeepaliveSummary(mockPrincipal.id),
  );
  expect(cached?.summary.cost_saved).toBe(7.5);
});
