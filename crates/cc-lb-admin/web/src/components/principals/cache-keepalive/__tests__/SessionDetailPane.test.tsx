import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import {
  cleanup,
  fireEvent,
  render,
  screen,
  within,
} from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { ApiError } from '../../../../lib/api';
import {
  type CacheKeepaliveDetail,
  CacheKeepaliveDetailSchema,
} from '../../../../lib/cacheKeepaliveApi';
import * as locale from '../../../../lib/locale';
import * as queries from '../../../../lib/queries';
import { cacheKeepaliveRenewedDetailFixture } from '../../../../lib/test-utils/cacheKeepalive-fixtures';
import { successQuery } from '../../../../lib/test-utils/hook-results';
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

describe('SessionDetailPane (D3 Time-Rail)', () => {
  afterEach(() => {
    cleanup();
    document.body.innerHTML = '';
    vi.restoreAllMocks();
  });

  it('renders loading skeleton while detail query is pending', () => {
    vi.spyOn(queries, 'useCacheKeepaliveSessionDetail').mockReturnValue({
      data: undefined,
      isLoading: true,
      isError: false,
    } as unknown as PolledDataResult<CacheKeepaliveDetail, Error>);

    renderWithProviders(
      <SessionDetailPane
        principalId="p-123"
        sessionId="a1f39c2b7e04"
        onClose={() => {}}
      />,
    );

    const pane = screen.getByTestId('session-detail-loading');
    expect(pane.getAttribute('aria-busy')).toBe('true');
    expect(screen.getByRole('status').textContent).toBe(
      'Loading session detail',
    );
  });

  it('renders error recovery state and invokes onClose when Back is clicked', () => {
    const onClose = vi.fn();
    vi.spyOn(queries, 'useCacheKeepaliveSessionDetail').mockReturnValue({
      data: undefined,
      isLoading: false,
      isError: true,
      error: new ApiError(404, 'not_found', null, 'Not found'),
      refetch: vi.fn(),
    } as unknown as PolledDataResult<CacheKeepaliveDetail, Error>);

    renderWithProviders(
      <SessionDetailPane
        principalId="p-123"
        sessionId="missing"
        onClose={onClose}
      />,
    );

    expect(screen.getByTestId('session-detail-error')).toBeDefined();
    expect(screen.getByRole('alert').textContent).toContain(
      'This session is no longer available.',
    );
    fireEvent.click(screen.getByRole('button', { name: 'Back to sessions' }));
    expect(onClose).toHaveBeenCalledOnce();
  });

  it('renders D3 time-rail with blue bg-accent pulse ticks and English hover tooltips', () => {
    vi.spyOn(queries, 'useCacheKeepaliveSessionDetail').mockReturnValue({
      data: {
        follow_up_outcomes: {
          observed: 1,
          pending: 0,
          no_follow_up: 0,
          unknown: 0,
        },
        id: 'p-123/thread:3c5cd0f1-4d8e-4be7-b0ec-7fd81793fede',
        last_message_at_ms: 1_700_000_323_000,
        state: 'renewed',
        is_active: false,
        ttl: '5m',
        attempts: 1,
        retained_renewals: 3,
        max_attempts: 12,
        reason: 'agent-in-turn (tool_use: `Bash`)',
        generation: 1,
        upstream: 'bale-max2',
        error: null,
        net_pnl: 9.4802,
        session_key_hash: '3c5cd0f1',
        renewal_tokens: 604_506,
        total_avoided: 9.8753,
        total_spent: 0.3951,
        total_renewals_current_cycle: 1,
        is_last_pending: false,
        evidence_status: 'observed',
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
          session_key_hash: '3c5cd0f1',
          principal_id: 'p-123',
          upstream_id: 'bale-max2',
          generation: 1,
          renewal_count: 1,
          ttl: '5m',
          status: 'renewed',
          enqueue_state: 'pending',
        },
      },
      isLoading: false,
      isError: false,
    } as unknown as PolledDataResult<CacheKeepaliveDetail, Error>);

    renderWithProviders(
      <SessionDetailPane
        principalId="p-123"
        sessionId="p-123/thread:3c5cd0f1-4d8e-4be7-b0ec-7fd81793fede"
        onClose={() => {}}
      />,
    );

    const rail = screen.getByTestId('session-detail-time-rail');
    const pulseBtn = rail.querySelector('[data-rail-point="pulse-1"]')!;
    const pulseTick = pulseBtn.querySelector('span')!;
    expect(pulseTick.className).toContain('bg-accent');
    expect(pulseTick.className).not.toContain('bg-bg');

    for (const pointId of ['start', 'pulse-1', 'end']) {
      const btn = rail.querySelector(`[data-rail-point="${pointId}"]`)!;
      fireEvent.focus(btn);
      const tip = screen.getByRole('tooltip');
      expect(tip.textContent).toContain('Timestamp');
      expect(tip.textContent).toContain('Since last request');
      expect(tip.textContent).toContain('Since last renewal');
      fireEvent.blur(btn);
    }
  });
  it('maps capped 12-renewal pulses linearly across the interior rail', () => {
    const pulses = Array.from({ length: 12 }, (_, index) => ({
      pulse_number: index + 1,
      time_ms: 1_700_000_000_000 + (index + 1) * 270_000,
      offset_sec: index === 10 ? 2980 : index === 11 ? 3250 : (index + 1) * 270,
      since_prev_renewal_sec: null,
      spent_usd: 0.1,
      hit: false,
    }));

    vi.spyOn(queries, 'useCacheKeepaliveSessionDetail').mockReturnValue({
      data: {
        follow_up_outcomes: {
          observed: 0,
          pending: 0,
          no_follow_up: 1,
          unknown: 0,
        },
        id: 'p-123/thread:capped',
        last_message_at_ms: 1_700_003_250_000,
        state: 'capped',
        is_active: false,
        ttl: '5m',
        attempts: 12,
        retained_renewals: 12,
        max_attempts: 12,
        reason: 'agent-in-turn',
        generation: 1,
        upstream: 'anthropic',
        error: null,
        net_pnl: -1.2,
        session_key_hash: 'capped',
        renewal_tokens: 604_506,
        total_avoided: 0,
        total_spent: 1.2,
        total_renewals_current_cycle: 12,
        is_last_pending: false,
        evidence_status: 'observed',
        turns: [],
        gaps: [
          {
            cycle_id: 'capped-cycle',
            pause_at_ms: 1_700_000_000_000,
            cached_tokens: 604_506,
            pulses,
            ending_kind: 'capped_no_followup',
            ending_at_ms: 1_700_003_250_000,
            total_duration_sec: 3250,
            since_last_renewal_sec: 0,
            spent_usd: 1.2,
            avoided_usd: 0,
            net_pnl_usd: -1.2,
          },
        ],
        config_snapshot: null,
        raw_record: {
          session_key_hash: 'capped',
          principal_id: 'p-123',
          upstream_id: 'anthropic',
          generation: 1,
          renewal_count: 12,
          ttl: '5m',
          status: 'capped',
          enqueue_state: 'pending',
        },
      },
      isLoading: false,
      isError: false,
    } as unknown as PolledDataResult<CacheKeepaliveDetail, Error>);

    renderWithProviders(
      <SessionDetailPane
        principalId="p-123"
        sessionId="p-123/thread:capped"
        onClose={() => {}}
      />,
    );

    const rail = screen.getByTestId('session-detail-time-rail');
    const pulse11 = rail.querySelector('[data-rail-point="pulse-11"]')!;
    const pulse12 = rail.querySelector('[data-rail-point="pulse-12"]')!;
    const pulse11Pct = Number.parseFloat(
      (pulse11 as HTMLElement).style.left.replace('%', ''),
    );
    const pulse12Pct = Number.parseFloat(
      (pulse12 as HTMLElement).style.left.replace('%', ''),
    );

    expect(pulse11Pct).toBeCloseTo(85.855, 2);
    expect(pulse12Pct).toBe(93);
    expect(pulse11Pct).not.toBeCloseTo(91.7, 1);
    expect(pulse11Pct).not.toBe(pulse12Pct);
  });
  it('uses the 30m renewal window for OpenAI Responses sessions', () => {
    vi.spyOn(queries, 'useCacheKeepaliveSessionDetail').mockReturnValue({
      data: {
        follow_up_outcomes: {
          observed: 0,
          pending: 1,
          no_follow_up: 0,
          unknown: 0,
        },
        id: 'openai-30m',
        last_message_at_ms: 1_700_000_323_000,
        state: 'scheduled',
        is_active: true,
        ttl: '30m',
        attempts: 0,
        retained_renewals: 0,
        max_attempts: 12,
        reason: 'waiting for follow-up',
        generation: 1,
        upstream: 'openai',
        error: null,
        net_pnl: null,
        session_key_hash: 'openai-30m',
        renewal_tokens: 604_506,
        total_avoided: null,
        total_spent: 0,
        total_renewals_current_cycle: 0,
        is_last_pending: true,
        evidence_status: 'none',
        turns: [],
        gaps: [],
        config_snapshot: {
          lead_5m: 30,
          lead_1h: 300,
          max_renewals: 12,
          max_duration: 14_400,
          snapshot_bytes: 8_388_608,
        },
        raw_record: {
          session_key_hash: 'openai-30m',
          principal_id: 'p-123',
          upstream_id: 'openai',
          generation: 1,
          renewal_count: 0,
          ttl: '30m',
          status: 'scheduled',
          enqueue_state: 'pending',
        },
      },
      isLoading: false,
      isError: false,
    } as unknown as PolledDataResult<CacheKeepaliveDetail, Error>);

    renderWithProviders(
      <SessionDetailPane
        principalId="p-123"
        sessionId="openai-30m"
        onClose={() => {}}
      />,
    );

    expect(screen.getByTestId('session-detail-time-rail')).toBeDefined();
    expect(screen.getByText('Next renewal at +29m 30s')).toBeDefined();
  });

  it('labels 12h retention without rendering a periodic renewal rail', () => {
    vi.spyOn(queries, 'useCacheKeepaliveSessionDetail').mockReturnValue({
      data: {
        follow_up_outcomes: {
          observed: 0,
          pending: 0,
          no_follow_up: 0,
          unknown: 0,
        },
        id: 'openai-12h',
        last_message_at_ms: 1_700_000_323_000,
        state: 'not_tracked',
        is_active: false,
        ttl: '12h',
        attempts: 0,
        retained_renewals: 0,
        max_attempts: 12,
        reason: 'openai 12h retention is recorded without read re-anchoring',
        generation: 1,
        upstream: 'openai',
        error: null,
        net_pnl: null,
        session_key_hash: 'openai-12h',
        renewal_tokens: 604_506,
        total_avoided: null,
        total_spent: 0,
        total_renewals_current_cycle: 0,
        is_last_pending: false,
        evidence_status: 'none',
        turns: [],
        gaps: [],
        config_snapshot: {
          lead_5m: 30,
          lead_1h: 300,
          max_renewals: 12,
          max_duration: 14_400,
          snapshot_bytes: 8_388_608,
        },
        raw_record: {
          session_key_hash: 'openai-12h',
          principal_id: 'p-123',
          upstream_id: 'openai',
          generation: 1,
          renewal_count: 0,
          ttl: '12h',
          status: 'not_tracked',
          enqueue_state: null,
        },
      },
      isLoading: false,
      isError: false,
    } as unknown as PolledDataResult<CacheKeepaliveDetail, Error>);

    renderWithProviders(
      <SessionDetailPane
        principalId="p-123"
        sessionId="openai-12h"
        onClose={() => {}}
      />,
    );

    expect(
      screen
        .getByTestId('cache-keepalive-12h-retention')
        .textContent?.includes(
          '12h retention is recorded without read re-anchoring',
        ),
    ).toBe(true);
    expect(screen.queryByTestId('session-detail-time-rail')).toBeNull();
    expect(screen.queryByText(/Next renewal/)).toBeNull();
    expect(screen.getAllByText('—').length).toBeGreaterThan(0);
  });

  it.each([
    ['scheduled', true, 'scheduled_waiting', true],
    ['renewed', true, 'scheduled_waiting', true],
    ['renewed', true, 'followup_hit', false],
    ['scheduled', false, 'scheduled_waiting', false],
    ['in_request', false, 'scheduled_waiting', false],
    ['expired', false, 'scheduled_waiting', false],
    ['cancelled', false, 'scheduled_waiting', false],
    ['failed', false, 'scheduled_waiting', false],
    ['capped', false, 'scheduled_waiting', false],
    ['not_tracked', false, 'scheduled_waiting', false],
  ] as const)(
    'only advertises a next renewal for an active waiting rail (%s, active=%s, ending=%s)',
    (state, isActive, endingKind, expectsNextRenewal) => {
      vi.spyOn(queries, 'useCacheKeepaliveSessionDetail').mockReturnValue(
        successQuery<CacheKeepaliveDetail>({
          ...cacheKeepaliveRenewedDetailFixture,
          state,
          is_active: isActive,
          net_pnl: 0.12,
          total_spent: 0.03,
          total_avoided: 0.15,
          turns: [],
          gaps: [
            {
              cycle_id: 'retained-cycle',
              pause_at_ms: 1_700_000_000_000,
              cached_tokens: 20_000,
              pulses: [],
              ending_kind: endingKind,
              ending_at_ms: 1_700_000_300_000,
              total_duration_sec: 300,
              since_last_renewal_sec: null,
              spent_usd: 0,
              avoided_usd: 0.12,
              net_pnl_usd: 0.12,
            },
          ],
        }),
      );

      renderWithProviders(
        <SessionDetailPane
          principalId="p-123"
          sessionId="zero-renewal-history"
          onClose={() => {}}
        />,
      );

      expect(screen.getByTestId('session-detail-time-rail')).toBeDefined();
      expect(screen.queryByText(/Next renewal/) !== null).toBe(
        expectsNextRenewal,
      );
      if (!expectsNextRenewal) {
        expect(screen.getByText('0 renewals')).toBeDefined();
      }
      expect(screen.getByText('Net +$0.12')).toBeDefined();
    },
  );

  it.each([false, true])(
    'explains an in-request session without claiming cancellation (retained history: %s)',
    (withHistory) => {
      vi.spyOn(queries, 'useCacheKeepaliveSessionDetail').mockReturnValue(
        successQuery<CacheKeepaliveDetail>({
          ...cacheKeepaliveRenewedDetailFixture,
          state: 'in_request',
          is_active: false,
          cancel_reason: null,
          reason: 'A real request is in progress',
          turns: [],
          gaps: withHistory
            ? [
                {
                  cycle_id: 'retained-cycle',
                  pause_at_ms: 1_700_000_000_000,
                  cached_tokens: 20_000,
                  pulses: [],
                  ending_kind: 'followup_hit',
                  ending_at_ms: 1_700_000_300_000,
                  total_duration_sec: 300,
                  since_last_renewal_sec: null,
                  spent_usd: 0,
                  avoided_usd: 0.12,
                  net_pnl_usd: 0.12,
                },
              ]
            : [],
        }),
      );

      renderWithProviders(
        <SessionDetailPane
          principalId="p-123"
          sessionId="request-session"
          onClose={() => {}}
        />,
      );

      const badge = screen.getByText('In request');
      expect(badge.className).toContain('bg-overlay-5');
      expect(badge.className).toContain('text-text-muted');
      expect(
        screen.getByText(
          'A real request is in progress. Keepalive renewals are paused until it completes.',
        ),
      ).toBeDefined();
      expect(
        screen.queryByText(/Cancelled|Failed|Next renewal|first renewal in/),
      ).toBeNull();
      expect(screen.queryByTestId('session-detail-time-rail') !== null).toBe(
        withHistory,
      );
    },
  );

  it.each([false, true])(
    'shows the expired request deadline neutrally (retained history: %s)',
    (withHistory) => {
      vi.spyOn(queries, 'useCacheKeepaliveSessionDetail').mockReturnValue(
        successQuery<CacheKeepaliveDetail>({
          ...cacheKeepaliveRenewedDetailFixture,
          state: 'expired',
          is_active: false,
          cancel_reason: null,
          reason: 'Request did not complete before its deadline',
          error: null,
          turns: withHistory ? cacheKeepaliveRenewedDetailFixture.turns : [],
          gaps: [],
        }),
      );

      renderWithProviders(
        <SessionDetailPane
          principalId="p-123"
          sessionId="expired-request"
          onClose={() => {}}
        />,
      );

      const reason = screen.getByText(
        'Request did not complete before its deadline',
      );
      expect(reason.className).toContain('text-text-muted');
      expect(screen.queryByText('In request')).toBeNull();
      expect(
        screen.queryByText(/Cancelled|Failed|aborted|Next renewal/),
      ).toBeNull();
      expect(screen.queryByTestId('session-detail-time-rail') !== null).toBe(
        withHistory,
      );
    },
  );

  it.each([
    ['new_request', 'Cancelled: new request'],
    ['user_turn_detected', 'Cancelled: user turn detected'],
    ['ambiguous_turn', 'Cancelled: ambiguous turn'],
    ['retention_recorded', 'Cancelled: retention recorded'],
    ['renewal_not_beneficial', 'Cancelled: renewal not beneficial'],
    ['snapshot_too_large', 'Cancelled: snapshot too large'],
    ['cached_prefix_too_small', 'Cancelled: cached prefix too small'],
    [
      'zero_token_renewal_unsupported',
      'Cancelled: zero-token renewal unsupported',
    ],
    [
      'completed_not_renewable',
      'Cancelled: request completed, renewal not possible',
    ],
    [null, 'Cancelled'],
    [undefined, 'Cancelled'],
  ] as const)(
    'shows cancellation cause %s instead of a stale scheduling reason',
    (cancelReason, expected) => {
      vi.spyOn(queries, 'useCacheKeepaliveSessionDetail').mockReturnValue(
        successQuery<CacheKeepaliveDetail>(
          CacheKeepaliveDetailSchema.parse({
            ...cacheKeepaliveRenewedDetailFixture,
            state: 'cancelled',
            is_active: false,
            cancel_reason: cancelReason,
            reason: cancelReason
              ? expected
              : 'agent-in-turn — first renewal in 4m 30s',
            gaps: [],
          }),
        ),
      );

      renderWithProviders(
        <SessionDetailPane
          principalId="p-123"
          sessionId="cancelled-session"
          onClose={() => {}}
        />,
      );

      expect(screen.getByText(expected)).toBeDefined();
      expect(screen.queryByText(/first renewal in/)).toBeNull();
      expect(screen.getByText(expected).className).toContain('text-text-muted');
      expect(screen.queryByText('In request')).toBeNull();
      expect(screen.queryByText('Failed')).toBeNull();
    },
  );

  it.each([
    ['new_request', 'Cancelled: new request'],
    [
      'completed_not_renewable',
      'Cancelled: request completed, renewal not possible',
    ],
  ] as const)(
    'keeps %s visible alongside retained renewal history',
    (cancelReason, expected) => {
      vi.spyOn(queries, 'useCacheKeepaliveSessionDetail').mockReturnValue(
        successQuery<CacheKeepaliveDetail>({
          ...cacheKeepaliveRenewedDetailFixture,
          state: 'cancelled',
          is_active: false,
          cancel_reason: cancelReason,
          reason: expected,
          gaps: [
            {
              cycle_id: 'retained-cycle',
              pause_at_ms: 1_700_000_000_000,
              cached_tokens: 20_000,
              pulses: [],
              ending_kind: 'ttl_expired_no_followup',
              ending_at_ms: 1_700_000_300_000,
              total_duration_sec: 300,
              since_last_renewal_sec: null,
              spent_usd: 0,
              avoided_usd: null,
              net_pnl_usd: null,
            },
          ],
        }),
      );

      renderWithProviders(
        <SessionDetailPane
          principalId="p-123"
          sessionId="cancelled-session"
          onClose={() => {}}
        />,
      );

      expect(screen.getByText(expected)).toBeDefined();
      expect(screen.getByText(expected).className).toContain('text-text-muted');
      expect(screen.queryByText('In request')).toBeNull();
      expect(screen.queryByText('Failed')).toBeNull();
      expect(screen.getByTestId('session-detail-time-rail')).toBeDefined();
      expect(screen.queryByText(/first renewal in/)).toBeNull();
    },
  );

  it('shows every horizon cycle in the renewal table and matches the timeline cycle labels', () => {
    const base = 1_700_000_000_000;
    const turn = (
      cycleId: string | null,
      timeMs: number,
      hit: boolean,
      pnl: number | null,
    ) => ({
      cycle_id: cycleId,
      turn_number: 1,
      time_ms: timeMs,
      renewals: hit ? 1 : 0,
      followed_up: hit ? true : null,
      label: 'agent-in-turn',
      pnl,
      pending: false,
      evidence_status: hit ? ('observed' as const) : ('unknown' as const),
    });
    const gap = (cycleId: string, pauseMs: number) => ({
      cycle_id: cycleId,
      pause_at_ms: pauseMs,
      cached_tokens: 20_000,
      pulses: [
        {
          pulse_number: 1,
          time_ms: pauseMs + 270_000,
          offset_sec: 270,
          since_prev_renewal_sec: null,
          spent_usd: 0.02,
          hit: true,
        },
      ],
      ending_kind: 'followup_hit' as const,
      ending_at_ms: pauseMs + 290_000,
      total_duration_sec: 290,
      since_last_renewal_sec: 20,
      spent_usd: 0.02,
      avoided_usd: 0.12,
      net_pnl_usd: 0.1,
    });
    vi.spyOn(queries, 'useCacheKeepaliveSessionDetail').mockReturnValue(
      successQuery<CacheKeepaliveDetail>({
        ...cacheKeepaliveRenewedDetailFixture,
        total_renewals_current_cycle: 1,
        turns: [
          turn('current-cycle', base + 870_000, false, null),
          turn(null, base + 570_000, true, 0.03),
          turn('previous-cycle', base + 270_000, true, 0.1),
        ],
        gaps: [
          gap('previous-cycle', base),
          gap('current-cycle', base + 600_000),
        ],
      }),
    );

    renderWithProviders(
      <SessionDetailPane
        principalId="p-123"
        sessionId="multi-cycle"
        onClose={() => {}}
      />,
    );

    expect(
      screen.getByRole('region', { name: 'Cycle 1 timeline' }),
    ).toBeDefined();
    expect(
      screen.getByRole('region', { name: 'Cycle 2 timeline' }),
    ).toBeDefined();
    const tables = screen.getAllByRole('table');
    expect(tables.map((table) => table.getAttribute('aria-label'))).toEqual([
      'Cycle 1 renewals',
      'Cycle 2 renewals',
      'Unknown cycle renewals',
    ]);
    for (const table of tables) {
      expect(within(table).getAllByRole('row')).toHaveLength(2);
      expect(within(table).getByText('#1')).toBeDefined();
    }
    expect(within(tables[0]).getByText('Hit')).toBeDefined();
    expect(within(tables[0]).getByText('+$0.1')).toBeDefined();
    expect(within(tables[1]).getByText('Miss')).toBeDefined();
    expect(within(tables[1]).getByText('Unknown')).toBeDefined();
    expect(within(tables[1]).getByText('—')).toBeDefined();
    expect(within(tables[2]).getByText('+$0.03')).toBeDefined();
  });

  it('does not invent renewal table rows from a zero-turn scheduled timeline', () => {
    vi.spyOn(queries, 'useCacheKeepaliveSessionDetail').mockReturnValue(
      successQuery<CacheKeepaliveDetail>({
        ...cacheKeepaliveRenewedDetailFixture,
        state: 'scheduled',
        turns: [],
        gaps: [],
      }),
    );
    renderWithProviders(
      <SessionDetailPane
        principalId="p-123"
        sessionId="scheduled-only"
        onClose={() => {}}
      />,
    );
    expect(screen.queryByRole('table')).toBeNull();
    expect(screen.getByTestId('session-detail-time-rail')).toBeDefined();
  });
  it.each([
    ['UTC', '2026-11-10T23:58:00Z', '2026-11-11T00:02:00Z'],
    ['Asia/Seoul', '2026-11-10T14:58:00Z', '2026-11-10T15:02:00Z'],
  ])(
    'includes local calendar dates across midnight in %s',
    (timeZone, pause, end) => {
      vi.spyOn(Date, 'now').mockReturnValue(Date.parse('2026-11-11T12:00:00Z'));
      vi.spyOn(locale, 'useTimezone').mockReturnValue({
        timezone: timeZone,
        effective: timeZone,
        setTimezone: vi.fn(),
      });
      vi.spyOn(queries, 'useCacheKeepaliveSessionDetail').mockReturnValue(
        successQuery<CacheKeepaliveDetail>({
          ...cacheKeepaliveRenewedDetailFixture,
          turns: [
            {
              ...cacheKeepaliveRenewedDetailFixture.turns[0],
              cycle_id: 'midnight',
              turn_number: 1,
              time_ms: Date.parse(pause) + 30_000,
            },
            {
              ...cacheKeepaliveRenewedDetailFixture.turns[0],
              cycle_id: 'midnight',
              turn_number: 2,
              time_ms: Date.parse(end) - 30_000,
            },
          ],
          gaps: [
            {
              cycle_id: 'midnight',
              pause_at_ms: Date.parse(pause),
              cached_tokens: 20_000,
              pulses: [],
              ending_kind: 'followup_hit',
              ending_at_ms: Date.parse(end),
              total_duration_sec: 240,
              since_last_renewal_sec: null,
              spent_usd: 0,
              avoided_usd: 0,
              net_pnl_usd: 0,
            },
          ],
        }),
      );
      renderWithProviders(
        <SessionDetailPane
          principalId="p-123"
          sessionId="midnight"
          onClose={() => {}}
        />,
      );
      expect(screen.getByText('11-10 23:58:00')).toBeDefined();
      expect(screen.getByText('11-11 00:02:00')).toBeDefined();
      const table = screen.getByRole('table', { name: 'Cycle 1 renewals' });
      expect(within(table).getByText('11-10 23:58:30')).toBeDefined();
      expect(within(table).getByText('11-11 00:01:30')).toBeDefined();
    },
  );

  it('keeps same-day local timestamps compact', () => {
    vi.spyOn(Date, 'now').mockReturnValue(Date.parse('2026-11-11T12:00:00Z'));
    vi.spyOn(locale, 'useTimezone').mockReturnValue({
      timezone: 'UTC',
      effective: 'UTC',
      setTimezone: vi.fn(),
    });
    vi.spyOn(queries, 'useCacheKeepaliveSessionDetail').mockReturnValue(
      successQuery<CacheKeepaliveDetail>({
        ...cacheKeepaliveRenewedDetailFixture,
        turns: [
          {
            ...cacheKeepaliveRenewedDetailFixture.turns[0],
            cycle_id: 'today',
            time_ms: Date.parse('2026-11-11T09:02:00Z'),
          },
        ],
        gaps: [
          {
            cycle_id: 'today',
            pause_at_ms: Date.parse('2026-11-11T09:00:00Z'),
            cached_tokens: 20_000,
            pulses: [],
            ending_kind: 'followup_hit',
            ending_at_ms: Date.parse('2026-11-11T09:04:00Z'),
            total_duration_sec: 240,
            since_last_renewal_sec: null,
            spent_usd: 0,
            avoided_usd: 0,
            net_pnl_usd: 0,
          },
        ],
      }),
    );
    renderWithProviders(
      <SessionDetailPane
        principalId="p-123"
        sessionId="today"
        onClose={() => {}}
      />,
    );
    expect(screen.getByText('09:00:00')).toBeDefined();
    expect(screen.getByText('09:04:00')).toBeDefined();
    expect(
      within(screen.getByRole('table', { name: 'Cycle 1 renewals' })).getByText(
        '09:02:00',
      ),
    ).toBeDefined();
    expect(screen.queryByText(/11-11 /)).toBeNull();
  });

  it('shows server outcome counts instead of flattening mixed cycles to unknown', () => {
    vi.spyOn(queries, 'useCacheKeepaliveSessionDetail').mockReturnValue(
      successQuery<CacheKeepaliveDetail>({
        ...cacheKeepaliveRenewedDetailFixture,
        evidence_status: 'unknown',
        follow_up_outcomes: {
          observed: 0,
          no_follow_up: 10,
          unknown: 1,
          pending: 2,
        },
      }),
    );
    renderWithProviders(
      <SessionDetailPane
        principalId="p-123"
        sessionId="mixed-outcomes"
        onClose={() => {}}
      />,
    );
    expect(screen.getByLabelText('Follow-up outcomes').textContent).toBe(
      'Observed 0 · No follow-up 10 · Unknown 1 · Pending 2',
    );
    expect(screen.queryByText('Follow-up unknown ·')).toBeNull();
  });
});
