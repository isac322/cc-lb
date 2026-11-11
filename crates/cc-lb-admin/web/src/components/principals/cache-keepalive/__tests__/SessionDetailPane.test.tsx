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
    ['new_request', 'Cancelled: new request'],
    ['user_turn_detected', 'Cancelled: user turn detected'],
    ['snapshot_too_large', 'Cancelled: snapshot too large'],
    ['cached_prefix_too_small', 'Cancelled: cached prefix too small'],
    [
      'zero_token_renewal_unsupported',
      'Cancelled: zero-token renewal unsupported',
    ],
    [null, 'Cancelled'],
    [undefined, 'Cancelled'],
  ] as const)(
    'shows cancellation cause %s instead of a stale scheduling reason',
    (cancelReason, expected) => {
      vi.spyOn(queries, 'useCacheKeepaliveSessionDetail').mockReturnValue({
        data: {
          ...cacheKeepaliveRenewedDetailFixture,
          state: 'cancelled',
          is_active: false,
          cancel_reason: cancelReason,
          reason: cancelReason
            ? expected
            : 'agent-in-turn — first renewal in 4m 30s',
          gaps: [],
        },
        isLoading: false,
        isError: false,
      } as unknown as PolledDataResult<CacheKeepaliveDetail, Error>);

      renderWithProviders(
        <SessionDetailPane
          principalId="p-123"
          sessionId="cancelled-session"
          onClose={() => {}}
        />,
      );

      expect(screen.getByText(expected)).toBeDefined();
      expect(screen.queryByText(/first renewal in/)).toBeNull();
    },
  );

  it('keeps the cancellation cause visible alongside retained renewal history', () => {
    vi.spyOn(queries, 'useCacheKeepaliveSessionDetail').mockReturnValue({
      data: {
        ...cacheKeepaliveRenewedDetailFixture,
        state: 'cancelled',
        is_active: false,
        cancel_reason: 'new_request',
        reason: 'Cancelled: new request',
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
      },
      isLoading: false,
      isError: false,
    } as unknown as PolledDataResult<CacheKeepaliveDetail, Error>);

    renderWithProviders(
      <SessionDetailPane
        principalId="p-123"
        sessionId="cancelled-session"
        onClose={() => {}}
      />,
    );

    expect(screen.getByText('Cancelled: new request')).toBeDefined();
    expect(screen.getByTestId('session-detail-time-rail')).toBeDefined();
    expect(screen.queryByText(/first renewal in/)).toBeNull();
  });
});
