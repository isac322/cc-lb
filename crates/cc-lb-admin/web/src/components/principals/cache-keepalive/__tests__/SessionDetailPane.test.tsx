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
import {
  fmtUsdPlain,
  fmtUsdSigned,
  SessionDetailPane,
} from '../SessionDetailPane';

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
    expect(screen.getByText('Net —')).toBeDefined();
    expect(screen.getByText('Saved — · Spent $0')).toBeDefined();
    expect(screen.queryByRole('table')).toBeNull();
  });

  it.each([
    ['scheduled', true, 'scheduled_waiting', true],
    ['renewed', true, 'scheduled_waiting', true],
    ['renewed', true, 'followup_hit', false],
    ['scheduled', false, 'scheduled_waiting', false],
    ['in_request', false, 'scheduled_waiting', false],
    ['expired', false, 'scheduled_waiting', false],
    ['in_request', false, 'followup_pending', false],
    ['expired', false, 'followup_pending', false],
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
        expect(screen.getByText('No renewals · 5m')).toBeDefined();
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

      const badge = screen.getByText('In use');
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

  it.each(['in_request', 'expired'] as const)(
    'keeps %s follow-up evidence pending without a terminal rail segment',
    (state) => {
      const detail = CacheKeepaliveDetailSchema.parse({
        ...cacheKeepaliveRenewedDetailFixture,
        state,
        is_active: false,
        is_last_pending: true,
        evidence_status: 'pending',
        follow_up_outcomes: {
          observed: 0,
          pending: 1,
          no_follow_up: 0,
          unknown: 0,
        },
        gaps: [
          {
            cycle_id: 'paused-cycle',
            pause_at_ms: 1_700_000_000_000,
            cached_tokens: 20_000,
            pulses: [
              {
                pulse_number: 1,
                time_ms: 1_700_000_270_000,
                offset_sec: 270,
                since_prev_renewal_sec: null,
                spent_usd: 0.006,
                hit: true,
              },
            ],
            ending_kind: 'followup_pending',
            ending_at_ms: 1_700_000_370_000,
            total_duration_sec: 370,
            since_last_renewal_sec: 100,
            spent_usd: 0.006,
            avoided_usd: 0,
            net_pnl_usd: -0.006,
          },
        ],
        turns: [],
      });
      vi.spyOn(queries, 'useCacheKeepaliveSessionDetail').mockReturnValue(
        successQuery(detail),
      );
      renderWithProviders(
        <SessionDetailPane
          principalId="p-123"
          sessionId="paused-cycle"
          onClose={() => {}}
        />,
      );
      const rail = screen.getByTestId('session-detail-time-rail');
      expect(within(rail).getByText(/Follow-up pending/)).toBeDefined();
      expect(within(rail).queryByText(/No follow-up/)).toBeNull();
      expect(screen.queryByText(/Next renewal/)).toBeNull();
      const endpoint = within(rail).getByRole('button', {
        name: /Waiting for follow-up evidence/,
      });
      fireEvent.focus(endpoint);
      expect(screen.getByRole('tooltip').textContent).toContain(
        'Waiting for follow-up evidence',
      );
      expect(screen.getByRole('tooltip').textContent).not.toContain(
        'No follow-up',
      );
    },
  );

  it('shows an expired paid evidence window even while renewal scheduling remains active', () => {
    vi.spyOn(queries, 'useCacheKeepaliveSessionDetail').mockReturnValue(
      successQuery(
        CacheKeepaliveDetailSchema.parse({
          ...cacheKeepaliveRenewedDetailFixture,
          state: 'renewed',
          is_active: true,
          is_last_pending: false,
          evidence_status: 'observed',
          follow_up_outcomes: {
            observed: 0,
            pending: 0,
            no_follow_up: 1,
            unknown: 0,
          },
          gaps: [
            {
              cycle_id: 'overdue-active',
              pause_at_ms: 1_700_000_000_000,
              cached_tokens: 20_000,
              pulses: [
                {
                  pulse_number: 1,
                  time_ms: 1_700_000_270_000,
                  offset_sec: 270,
                  since_prev_renewal_sec: null,
                  spent_usd: 0.006,
                  hit: true,
                },
              ],
              ending_kind: 'ttl_expired_no_followup',
              ending_at_ms: 1_700_000_570_000,
              total_duration_sec: 570,
              since_last_renewal_sec: 300,
              spent_usd: 0.006,
              avoided_usd: 0,
              net_pnl_usd: -0.006,
            },
          ],
          turns: [],
        }),
      ),
    );
    renderWithProviders(
      <SessionDetailPane
        principalId="p-123"
        sessionId="overdue-active"
        onClose={() => {}}
      />,
    );
    const rail = screen.getByTestId('session-detail-time-rail');
    expect(within(rail).getByText(/No follow-up/)).toBeDefined();
    expect(within(rail).queryByText(/Follow-up pending|Waiting/)).toBeNull();
    expect(
      within(rail).getByRole('button', { name: /No follow-up.*TTL expired/ }),
    ).toBeDefined();
  });

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
      expect(screen.queryByText('In use')).toBeNull();
      expect(
        screen.queryByText(/Cancelled|Failed|aborted|Next renewal/),
      ).toBeNull();
      expect(screen.queryByTestId('session-detail-time-rail') !== null).toBe(
        withHistory,
      );
    },
  );

  it.each([
    ['new_request', 'A new request arrived'],
    ['user_turn_detected', 'The agent is waiting for the user'],
    ['ambiguous_turn', 'The agent turn could not be determined'],
    ['retention_recorded', 'Retention recorded without renewal'],
    [
      'renewal_not_beneficial',
      'Refresh expected value is unknown or non-positive',
    ],
    ['snapshot_too_large', 'The request snapshot is too large'],
    ['cached_prefix_too_small', 'The cached prefix is too small'],
    ['zero_token_renewal_unsupported', 'Zero-token renewals are unsupported'],
    ['completed_not_renewable', 'Request completed, renewal not possible'],
    [null, 'Returned'],
    [undefined, 'Returned'],
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
      expect(screen.queryByText('Cancelled', { exact: true })).toBeNull();
      expect(screen.getByText(expected).className).toContain('text-text-muted');
      expect(screen.queryByText('In use')).toBeNull();
      expect(screen.queryByText('Failed')).toBeNull();
    },
  );

  it.each([
    ['new_request', 'A new request arrived'],
    ['completed_not_renewable', 'Request completed, renewal not possible'],
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
      expect(screen.queryByText('In use')).toBeNull();
      expect(screen.queryByText('Failed')).toBeNull();
      expect(screen.getByTestId('session-detail-time-rail')).toBeDefined();
      expect(screen.queryByText(/first renewal in/)).toBeNull();
    },
  );

  it('keeps matched renewal facts on cycle rails and unmatched evidence visible', () => {
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
    expect(screen.queryByRole('table')).toBeNull();
    expect(screen.queryByRole('heading', { name: 'Renewals' })).toBeNull();
    for (const [cycle, net, followUp] of [
      ['Cycle 1', '+$0.10', 'Observed'],
      ['Cycle 2', '—', 'Unknown'],
    ]) {
      const timeline = screen.getByRole('region', {
        name: `${cycle} timeline`,
      });
      fireEvent.click(
        within(timeline).getByRole('button', { name: /Renewal #1:/ }),
      );
      const tip = screen.getByRole('tooltip');
      expect(within(tip).getByText(net)).toBeDefined();
      expect(within(tip).getByText(followUp)).toBeDefined();
      fireEvent.keyDown(document, { key: 'Escape' });
    }
    const additional = screen.getByRole('region', {
      name: 'Additional renewal evidence',
    });
    expect(within(additional).getByText('Hit')).toBeDefined();
    expect(within(additional).getByText('Net +$0.03')).toBeDefined();
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
      expect(screen.getByText('Next message 11-11 00:02:00')).toBeDefined();
      const additional = screen.getByRole('region', {
        name: 'Additional renewal evidence',
      });
      const times = Array.from(additional.querySelectorAll('time'));
      expect(times.map((time) => time.dateTime)).toEqual([
        new Date(Date.parse(pause) + 30_000).toISOString(),
        new Date(Date.parse(end) - 30_000).toISOString(),
      ]);
      expect(times[0].textContent).toContain('10 Nov 2026');
      expect(times[1].textContent).toContain('11 Nov 2026');
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
    expect(screen.getByText('Next message 09:04:00')).toBeDefined();
    const additional = screen.getByRole('region', {
      name: 'Additional renewal evidence',
    });
    const evidenceTime = additional.querySelector('time')?.textContent;
    expect(evidenceTime).toContain('11 Nov 2026');
    expect(evidenceTime).toContain('09:02:00');
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
    const outcomes = screen.getByLabelText('Follow-up outcomes');
    expect(outcomes.getAttribute('title')).toBe(
      '0 observed · 10 no follow-up · 1 unknown · 2 pending',
    );
    expect(
      within(outcomes).getByText(
        'Follow-up: 10 no follow-up · 1 unknown · 2 pending',
      ),
    ).toBeDefined();
    expect(outcomes.querySelector('.sr-only')?.textContent).toBe(
      '0 observed · 10 no follow-up · 1 unknown · 2 pending',
    );
    expect(screen.queryByText('Follow-up unknown ·')).toBeNull();
  });

  it.each([
    [84.9, '+$84.90', '$84.90'],
    [0.09996, '+$0.10', '$0.10'],
    [0.0326, '+$0.0326', '$0.0326'],
    [-84.9, '−$84.90', '$84.90'],
    [0, '$0', '$0'],
    [null, '—', '—'],
  ] as const)(
    'formats money with rounding-aware cents (%s)',
    (amount, signed, plain) => {
      expect(fmtUsdSigned(amount)).toBe(signed);
      expect(fmtUsdPlain(amount)).toBe(plain);
    },
  );

  it.each([true, false])(
    'moves renewal facts into a pinnable pulse popover (hit=%s)',
    (hit) => {
      vi.spyOn(locale, 'useTimezone').mockReturnValue({
        timezone: 'UTC',
        effective: 'UTC',
        setTimezone: vi.fn(),
      });
      const pulseMs = 1794373221291;
      const detail: CacheKeepaliveDetail = {
        ...cacheKeepaliveRenewedDetailFixture,
        turns: [
          {
            ...cacheKeepaliveRenewedDetailFixture.turns[0],
            cycle_id: 'real-cycle',
            turn_number: 1,
            time_ms: 1794373222000,
            renewals: hit ? 1 : 0,
            pnl: hit ? 0.0326 : -0.0123,
            followed_up: hit,
            pending: false,
          },
        ],
        gaps: [
          {
            cycle_id: 'real-cycle',
            pause_at_ms: pulseMs - 270_000,
            cached_tokens: 20_000,
            pulses: [
              {
                pulse_number: 1,
                time_ms: pulseMs,
                offset_sec: 270,
                since_prev_renewal_sec: null,
                spent_usd: 0.0123,
                hit,
              },
            ],
            ending_kind: 'followup_hit',
            ending_at_ms: pulseMs + 30_000,
            total_duration_sec: 300,
            since_last_renewal_sec: 30,
            spent_usd: 0.0123,
            avoided_usd: 0.0449,
            net_pnl_usd: 0.0326,
          },
        ],
      };
      vi.spyOn(queries, 'useCacheKeepaliveSessionDetail').mockReturnValue(
        successQuery(detail),
      );
      renderWithProviders(
        <SessionDetailPane
          principalId="p-123"
          sessionId="real-shape"
          onClose={() => {}}
        />,
      );
      expect(screen.queryByRole('table')).toBeNull();
      expect(screen.queryByRole('heading', { name: 'Renewals' })).toBeNull();
      expect(
        screen.queryByRole('region', { name: 'Additional renewal evidence' }),
      ).toBeNull();
      const pulse = screen.getByRole('button', { name: /Renewal #1:/ });
      expect(pulse.querySelector('span')?.className).toContain(
        hit ? 'bg-accent' : 'bg-warn',
      );
      fireEvent.click(pulse);
      expect(pulse.getAttribute('aria-pressed')).toBe('true');
      fireEvent.blur(pulse);
      const tip = screen.getByRole('tooltip');
      expect(pulse.getAttribute('aria-describedby')).toBe(tip.id);
      expect(within(tip).getByText(hit ? 'Hit' : 'Miss')).toBeDefined();
      expect(within(tip).getByText('Spent')).toBeDefined();
      expect(within(tip).getByText('$0.0123')).toBeDefined();
      expect(within(tip).getByText('Net')).toBeDefined();
      expect(
        within(tip).getByText(hit ? '+$0.0326' : '−$0.0123'),
      ).toBeDefined();
      expect(within(tip).getByText('Follow-up')).toBeDefined();
      expect(within(tip).getByText(hit ? 'Observed' : 'None')).toBeDefined();
      expect(tip.querySelector('time')?.dateTime).toBe(
        new Date(pulseMs).toISOString(),
      );
      expect(tip.querySelector('time')?.textContent).toContain('11 Nov 2026');
      expect(within(tip).getByText('+4m 30s')).toBeDefined();
      expect(within(tip).getByText('— (first renewal)')).toBeDefined();
      fireEvent.pointerDown(tip);
      expect(screen.getByRole('tooltip')).toBeDefined();
      fireEvent.keyDown(document, { key: 'Escape' });
      expect(screen.queryByRole('tooltip')).toBeNull();
      fireEvent.click(pulse);
      fireEvent.click(pulse);
      expect(screen.queryByRole('tooltip')).toBeNull();
      fireEvent.click(pulse);
      fireEvent.pointerDown(document.body);
      expect(screen.queryByRole('tooltip')).toBeNull();
    },
  );

  it('matches cycle ordinals before nearby timestamps and only uses unmatched turns within three seconds', () => {
    const base = 1794373221291;
    const turn = (
      cycle_id: string,
      turn_number: number,
      time_ms: number,
      pnl: number,
    ) => ({
      ...cacheKeepaliveRenewedDetailFixture.turns[0],
      cycle_id,
      turn_number,
      time_ms,
      pnl,
      followed_up: true,
      pending: false,
    });
    vi.spyOn(queries, 'useCacheKeepaliveSessionDetail').mockReturnValue(
      successQuery<CacheKeepaliveDetail>({
        ...cacheKeepaliveRenewedDetailFixture,
        turns: [
          turn('other-cycle', 1, base, 9),
          turn('matching-cycle', 2, base + 2000, 0.12),
          turn('matching-cycle', 9, base - 2500, 0.0326),
          turn('matching-cycle', 10, base + 24_000, 7),
        ],
        gaps: [
          {
            cycle_id: 'matching-cycle',
            pause_at_ms: base - 270_000,
            cached_tokens: 20_000,
            pulses: [
              {
                pulse_number: 99,
                time_ms: base,
                offset_sec: 270,
                since_prev_renewal_sec: null,
                spent_usd: 0.01,
                hit: true,
              },
              {
                pulse_number: 2,
                time_ms: base + 10_000,
                offset_sec: 280,
                since_prev_renewal_sec: 10,
                spent_usd: 0.02,
                hit: true,
              },
              {
                pulse_number: 3,
                time_ms: base + 20_000,
                offset_sec: 290,
                since_prev_renewal_sec: 10,
                spent_usd: 0.03,
                hit: true,
              },
            ],
            ending_kind: 'followup_hit',
            ending_at_ms: base + 50_000,
            total_duration_sec: 320,
            since_last_renewal_sec: 30,
            spent_usd: 0.06,
            avoided_usd: 0.2,
            net_pnl_usd: 0.14,
          },
        ],
      }),
    );
    renderWithProviders(
      <SessionDetailPane
        principalId="p-123"
        sessionId="matching"
        onClose={() => {}}
      />,
    );
    const pulses = screen
      .getByTestId('session-detail-time-rail')
      .querySelectorAll('[data-rail-point^="pulse-"]');
    for (const [index, expected] of ['+$0.0326', '+$0.12', '—'].entries()) {
      fireEvent.click(pulses[index]);
      const tip = screen.getByRole('tooltip');
      expect(within(tip).getByText(expected)).toBeDefined();
      fireEvent.keyDown(document, { key: 'Escape' });
    }
    const additional = screen.getByRole('region', {
      name: 'Additional renewal evidence',
    });
    expect(within(additional).getByText('Net +$9.00')).toBeDefined();
    expect(within(additional).getByText('Net +$7.00')).toBeDefined();
  });

  it('marks legacy synthesized per-renewal spending as estimated', () => {
    const first = cacheKeepaliveRenewedDetailFixture.turns[0];
    vi.spyOn(queries, 'useCacheKeepaliveSessionDetail').mockReturnValue(
      successQuery<CacheKeepaliveDetail>({
        ...cacheKeepaliveRenewedDetailFixture,
        total_spent: 0.0326,
        turns: [{ ...first, cycle_id: null, turn_number: 4, pending: true }],
        gaps: [],
      }),
    );
    renderWithProviders(
      <SessionDetailPane
        principalId="p-123"
        sessionId="legacy"
        onClose={() => {}}
      />,
    );
    fireEvent.click(screen.getByRole('button', { name: /Renewal #4:/ }));
    const tip = screen.getByRole('tooltip');
    expect(within(tip).getByText('≈ $0.0326')).toBeDefined();
    expect(within(tip).getByTitle('Estimated per-renewal spend')).toBeDefined();
    expect(within(tip).getByText('Pending')).toBeDefined();
    expect(screen.queryByRole('table')).toBeNull();
  });

  it('retains a renewal evidence table when the session has no periodic rail', () => {
    vi.spyOn(queries, 'useCacheKeepaliveSessionDetail').mockReturnValue(
      successQuery<CacheKeepaliveDetail>({
        ...cacheKeepaliveRenewedDetailFixture,
        ttl: '12h',
        gaps: [],
        turns: [
          {
            ...cacheKeepaliveRenewedDetailFixture.turns[0],
            cycle_id: 'historical',
            pending: false,
            followed_up: null,
            pnl: null,
          },
        ],
      }),
    );
    renderWithProviders(
      <SessionDetailPane
        principalId="p-123"
        sessionId="12h-history"
        onClose={() => {}}
      />,
    );
    expect(screen.queryByTestId('session-detail-time-rail')).toBeNull();
    expect(
      screen.getByRole('table', { name: 'Cycle 1 renewals' }),
    ).toBeDefined();
    expect(screen.getByText('Unknown', { exact: true })).toBeDefined();
    expect(
      within(screen.getByRole('table', { name: 'Cycle 1 renewals' })).getByText(
        '—',
      ),
    ).toBeDefined();
  });

  it.each([0, 2000])(
    'matches distinct null-cycle gaps to their own renewal evidence (turn delay %sms)',
    (delay) => {
      const pulseTimes = [1794373221291, 1794373821291];
      vi.spyOn(queries, 'useCacheKeepaliveSessionDetail').mockReturnValue(
        successQuery<CacheKeepaliveDetail>({
          ...cacheKeepaliveRenewedDetailFixture,
          turns: pulseTimes.map((time, index) => ({
            ...cacheKeepaliveRenewedDetailFixture.turns[0],
            cycle_id: null,
            turn_number: index + 1,
            time_ms: time + delay,
            pnl: index === 0 ? 0.0326 : -0.0123,
            followed_up: index === 0,
            pending: false,
          })),
          gaps: pulseTimes.map((time) => ({
            cycle_id: null,
            pause_at_ms: time - 270_000,
            cached_tokens: 20_000,
            pulses: [
              {
                pulse_number: 1,
                time_ms: time,
                offset_sec: 270,
                since_prev_renewal_sec: null,
                spent_usd: 0.0123,
                hit: true,
              },
            ],
            ending_kind: 'ttl_expired_no_followup',
            ending_at_ms: time + 300_000,
            total_duration_sec: 570,
            since_last_renewal_sec: 300,
            spent_usd: 0.0123,
            avoided_usd: 0,
            net_pnl_usd: -0.0123,
          })),
        }),
      );
      renderWithProviders(
        <SessionDetailPane
          principalId="p-123"
          sessionId="null-cycles"
          onClose={() => {}}
        />,
      );
      const timelines = screen.getAllByRole('region', {
        name: 'Unknown cycle timeline',
      });
      for (const [index, timeline] of timelines.entries()) {
        fireEvent.click(
          within(timeline).getByRole('button', { name: /Renewal #/ }),
        );
        const tip = screen.getByRole('tooltip');
        expect(
          within(tip).getByText(index === 0 ? '+$0.0326' : '−$0.0123'),
        ).toBeDefined();
        expect(
          within(tip).getByText(index === 0 ? 'Observed' : 'None'),
        ).toBeDefined();
        fireEvent.keyDown(document, { key: 'Escape' });
      }
      expect(
        screen.queryByRole('region', { name: 'Additional renewal evidence' }),
      ).toBeNull();
    },
  );

  it.each(['cycle-b', null])(
    'keeps interleaved renewal ordinals isolated from cycle %s',
    (secondCycle) => {
      const base = 1794373221291;
      const identities = [
        {
          cycle: 'cycle-a',
          time: base + 10_000,
          net: 0.0326,
          followedUp: true,
        },
        {
          cycle: secondCycle,
          time: base + 2000,
          net: -0.0123,
          followedUp: false,
        },
      ];
      vi.spyOn(queries, 'useCacheKeepaliveSessionDetail').mockReturnValue(
        successQuery<CacheKeepaliveDetail>({
          ...cacheKeepaliveRenewedDetailFixture,
          turns: identities.map(({ cycle, time, net, followedUp }) => ({
            ...cacheKeepaliveRenewedDetailFixture.turns[0],
            cycle_id: cycle,
            turn_number: 2,
            time_ms: time,
            pnl: net,
            followed_up: followedUp,
            pending: false,
          })),
          gaps: identities.map(({ cycle, time, net }) => ({
            cycle_id: cycle,
            pause_at_ms: time - 270_000,
            cached_tokens: 20_000,
            pulses: [
              {
                pulse_number: 2,
                time_ms: time,
                offset_sec: 270,
                since_prev_renewal_sec: null,
                spent_usd: 0.0123,
                hit: true,
              },
            ],
            ending_kind: 'ttl_expired_no_followup',
            ending_at_ms: time + 300_000,
            total_duration_sec: 570,
            since_last_renewal_sec: 300,
            spent_usd: 0.0123,
            avoided_usd: 0,
            net_pnl_usd: net,
          })),
        }),
      );
      renderWithProviders(
        <SessionDetailPane
          principalId="p-123"
          sessionId="interleaved-cycles"
          onClose={() => {}}
        />,
      );
      const timelines = within(
        screen.getByTestId('session-detail-time-rail'),
      ).getAllByRole('region');
      for (const [index, timeline] of timelines.entries()) {
        fireEvent.click(
          within(timeline).getByRole('button', { name: /Renewal #2:/ }),
        );
        const tip = screen.getByRole('tooltip');
        expect(
          within(tip).getByText(index === 0 ? '+$0.0326' : '−$0.0123'),
        ).toBeDefined();
        expect(
          within(tip).getByText(index === 0 ? 'Observed' : 'None'),
        ).toBeDefined();
        fireEvent.keyDown(document, { key: 'Escape' });
      }
      expect(
        screen.queryByRole('region', { name: 'Additional renewal evidence' }),
      ).toBeNull();
    },
  );

  it('clears pinned renewal details when the session changes despite identical gap identities', () => {
    const pulseMs = 1794373221291;
    const detail: CacheKeepaliveDetail = {
      ...cacheKeepaliveRenewedDetailFixture,
      id: 'session-a',
      gaps: [
        {
          cycle_id: null,
          pause_at_ms: pulseMs - 270_000,
          cached_tokens: 20_000,
          pulses: [
            {
              pulse_number: 1,
              time_ms: pulseMs,
              offset_sec: 270,
              since_prev_renewal_sec: null,
              spent_usd: 0.0123,
              hit: true,
            },
          ],
          ending_kind: 'ttl_expired_no_followup',
          ending_at_ms: pulseMs + 300_000,
          total_duration_sec: 570,
          since_last_renewal_sec: 300,
          spent_usd: 0.0123,
          avoided_usd: 0,
          net_pnl_usd: -0.0123,
        },
      ],
      turns: [],
    };
    const query = vi
      .spyOn(queries, 'useCacheKeepaliveSessionDetail')
      .mockReturnValue(successQuery(detail));
    const client = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });
    const view = (sessionId: string) => (
      <QueryClientProvider client={client}>
        <SessionDetailPane
          principalId="p-123"
          sessionId={sessionId}
          onClose={() => {}}
        />
      </QueryClientProvider>
    );
    const { rerender } = render(view('session-a'));
    fireEvent.click(screen.getByRole('button', { name: /Renewal #1:/ }));
    expect(screen.getByRole('tooltip')).toBeDefined();
    rerender(view('session-a'));
    expect(screen.getByRole('tooltip')).toBeDefined();
    query.mockReturnValue(successQuery({ ...detail, id: 'session-b' }));
    rerender(view('session-b'));
    expect(screen.queryByRole('tooltip')).toBeNull();
    expect(
      screen
        .getByRole('button', { name: /Renewal #1:/ })
        .getAttribute('aria-pressed'),
    ).toBe('false');
  });
});
