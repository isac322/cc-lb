import {
  cleanup,
  fireEvent,
  render,
  screen,
  within,
} from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { ApiError } from '../../lib/api';
import { deriveCouponNudge } from '../../lib/couponNudge';
import { couponInventory } from '../../lib/couponPresentation';
import {
  type LimitResetHistoryItem,
  type LimitResetHistoryResponse,
  type LimitResetsResponse,
  useClaimLimitReset,
  useLimitResetHistory,
} from '../../lib/limitResets';
import {
  failedQuery,
  idleMutation,
  pendingQuery,
  successQuery,
} from '../../lib/test-utils/hook-results';
import { makeOauthUpstream } from '../../lib/test-utils/warmup-fixtures';
import { type CouponNudgeState, useCouponNudge } from './CouponNudge';
import { LimitResetAction } from './LimitResetAction';
import { ResetHistoryPanel } from './ResetHistoryPanel';

vi.mock('../../lib/limitResets', async () => {
  const actual = await vi.importActual<Record<string, unknown>>(
    '../../lib/limitResets',
  );
  return {
    ...actual,
    useLimitResetHistory: vi.fn(),
    useClaimLimitReset: vi.fn(),
    loadPendingLimitResetOp: vi.fn(() => null),
  };
});
vi.mock('./CouponNudge', async () => {
  const actual =
    await vi.importActual<Record<string, unknown>>('./CouponNudge');
  return { ...actual, useCouponNudge: vi.fn() };
});
vi.mock('../ui/RelativeTime', () => ({
  RelativeTime: ({ ts }: { ts: number }) => (
    <time>{new Date(ts).toISOString()}</time>
  ),
}));

function attempt(
  overrides: Partial<LimitResetHistoryItem> = {},
): LimitResetHistoryItem {
  return {
    id: 'audit-row',
    recorded_at_unix_millis: Date.parse('2026-11-12T12:00:00Z'),
    upstream_id: 'upstream-a',
    provider: 'anthropic',
    coupon_type: 'weekly',
    gives_back_weekly: false,
    result: 'reset',
    usage_refresh: 'refreshed',
    actor: null,
    request_id: null,
    cleared: ['7d'],
    windows_reset: null,
    ...overrides,
  };
}

function setHistory(
  data: LimitResetHistoryResponse | undefined,
  error: Error | null = null,
) {
  vi.mocked(useLimitResetHistory).mockReturnValue(
    error
      ? failedQuery<LimitResetHistoryResponse>(error)
      : data
        ? successQuery(data)
        : pendingQuery<LimitResetHistoryResponse>(),
  );
}

beforeEach(() => {
  vi.clearAllMocks();
  setHistory({ items: [], has_more: false, limit: 100 });
});
afterEach(cleanup);

describe('recorded reset activity', () => {
  it('shows an uncertain legacy attempt without claiming consumption or offering a retry', () => {
    setHistory({
      items: [
        attempt({
          result: 'unknown',
          provider: null,
          coupon_type: null,
          usage_refresh: null,
        }),
      ],
      has_more: false,
      limit: 100,
    });
    render(<ResetHistoryPanel upstreamId="upstream-a" open />);
    expect(screen.getByText(/Outcome unknown/)).toBeDefined();
    expect(screen.queryByText('Coupon used')).toBeNull();
    expect(
      screen.queryByRole('button', { name: /Use coupon|Retry request/ }),
    ).toBeNull();
    expect(screen.getByText('Activity recorded by cc-lb')).toBeDefined();
    expect(screen.getByText('2026-11-12T12:00:00.000Z')).toBeDefined();
  });

  it('keeps consumption success distinct from a failed usage refresh', () => {
    setHistory({
      items: [attempt({ usage_refresh: 'failed' })],
      has_more: false,
      limit: 100,
    });
    render(<ResetHistoryPanel upstreamId="upstream-a" open />);
    expect(screen.getByText('Coupon used')).toBeDefined();
    expect(screen.queryByText(/Outcome unknown/)).toBeNull();
  });

  it('distinguishes already-applied and not-needed outcomes from a new consumption', () => {
    setHistory({
      items: [
        attempt({ id: 'already', result: 'already_used' }),
        attempt({ id: 'not-needed', result: 'not_limited' }),
      ],
      has_more: false,
      limit: 100,
    });
    render(<ResetHistoryPanel upstreamId="upstream-a" open />);
    const rows = screen.getAllByRole('listitem');
    expect(
      within(rows[0]).getByText(/no additional coupon used/),
    ).toBeDefined();
    expect(within(rows[1]).getByText(/no coupon used/)).toBeDefined();
    expect(screen.queryByText('Coupon used')).toBeNull();
  });

  it('shows the recorded-attempt empty state instead of inventory or granted credits', () => {
    render(<ResetHistoryPanel upstreamId="upstream-a" open />);
    expect(
      screen.getByText('No reset attempts recorded in cc-lb.'),
    ).toBeDefined();
    expect(screen.queryByRole('list')).toBeNull();
  });

  it('does not enable the history query while closed', () => {
    render(<ResetHistoryPanel upstreamId="upstream-a" open={false} />);
    expect(useLimitResetHistory).toHaveBeenCalledWith('upstream-a', false, 100);
  });

  it('reports loading and permission failure without presenting an empty ledger', () => {
    setHistory(undefined);
    const view = render(<ResetHistoryPanel upstreamId="upstream-a" open />);
    expect(
      screen.getByRole('status', { name: 'Loading reset activity' }),
    ).toBeDefined();
    expect(
      screen.queryByText('No reset attempts recorded in cc-lb.'),
    ).toBeNull();
    setHistory(undefined, new ApiError(403, null, null, 'Forbidden'));
    view.rerender(<ResetHistoryPanel upstreamId="upstream-a" open />);
    expect(screen.getByText(/does not have permission/)).toBeDefined();
    expect(
      screen.queryByText('No reset attempts recorded in cc-lb.'),
    ).toBeNull();
  });

  it('expands the real recent-history bound up to 1000, then states the limit honestly', () => {
    vi.mocked(useLimitResetHistory).mockImplementation(
      (_upstreamId, _enabled, limit = 100) =>
        successQuery({ items: [attempt()], has_more: true, limit }),
    );
    render(<ResetHistoryPanel upstreamId="upstream-a" open />);
    for (let limit = 200; limit <= 1000; limit += 100) {
      fireEvent.click(
        screen.getByRole('button', { name: 'Load older activity' }),
      );
      expect(useLimitResetHistory).toHaveBeenLastCalledWith(
        'upstream-a',
        true,
        limit,
      );
    }
    expect(
      screen.queryByRole('button', { name: 'Load older activity' }),
    ).toBeNull();
    expect(
      screen.getByText(/most recent 1,000 recorded attempts/),
    ).toBeDefined();
  });
});

describe('reset activity entry availability', () => {
  it('opens recorded history without credentials and never dispatches a coupon claim', async () => {
    const mutate = vi.fn();
    vi.mocked(useClaimLimitReset).mockReturnValue(idleMutation({ mutate }));
    const state: CouponNudgeState = {
      resetsQ: pendingQuery<LimitResetsResponse>(),
      nowMs: Date.now(),
      suppressed: true,
      nudge: null,
      derived: null,
      claimInFlight: false,
    };
    vi.mocked(useCouponNudge).mockReturnValue(state);
    render(
      <LimitResetAction
        upstream={makeOauthUpstream()}
        credentialsStored={false}
      />,
    );
    const entry = screen.getByRole('button', { name: 'Reset quota' });
    expect((entry as HTMLButtonElement).disabled).toBe(false);
    fireEvent.click(entry);
    expect(
      await screen.findByRole('dialog', { name: 'Reset activity' }),
    ).toBeDefined();
    expect(useLimitResetHistory).toHaveBeenLastCalledWith('oauth-1', true, 100);
    expect(
      screen.getByText('No reset attempts recorded in cc-lb.'),
    ).toBeDefined();
    expect(mutate).not.toHaveBeenCalled();
  });

  it('keeps recorded history reachable while the provider inventory is loading', async () => {
    const mutate = vi.fn();
    vi.mocked(useClaimLimitReset).mockReturnValue(idleMutation({ mutate }));
    const state: CouponNudgeState = {
      resetsQ: pendingQuery<LimitResetsResponse>(),
      nowMs: Date.now(),
      suppressed: true,
      nudge: null,
      derived: null,
      claimInFlight: false,
    };
    vi.mocked(useCouponNudge).mockReturnValue(state);
    render(
      <LimitResetAction
        upstream={makeOauthUpstream()}
        credentialsStored
        renderTrigger={({ onClick, disabled, loading }) => (
          <button
            type="button"
            disabled={disabled || loading}
            onClick={onClick}
          >
            Coupons
          </button>
        )}
      />,
    );
    fireEvent.click(screen.getByRole('button', { name: 'Coupons' }));
    expect(
      await screen.findByRole('dialog', { name: 'Reset activity' }),
    ).toBeDefined();
    expect(screen.queryByRole('button', { name: 'View coupons' })).toBeNull();
    expect(
      screen.queryByRole('dialog', { name: 'Use a reset coupon?' }),
    ).toBeNull();
    expect(mutate).not.toHaveBeenCalled();
  });
});

describe('manual native coupon availability', () => {
  it.each([
    {
      used: 20,
      failedRefresh: false,
      observed: true,
      coverageUnreported: false,
      sibling: 'none',
    },
    {
      used: 20.001,
      failedRefresh: false,
      observed: true,
      coverageUnreported: false,
      sibling: 'none',
    },
    {
      used: 21,
      failedRefresh: true,
      observed: true,
      coverageUnreported: false,
      sibling: 'none',
    },
    {
      used: 21,
      failedRefresh: true,
      observed: true,
      coverageUnreported: false,
      sibling: 'positive-entry',
    },
    {
      used: 21,
      failedRefresh: false,
      observed: false,
      coverageUnreported: false,
      sibling: 'none',
    },
    {
      used: 21,
      failedRefresh: false,
      observed: true,
      coverageUnreported: true,
      sibling: 'none',
    },
    {
      used: 21,
      failedRefresh: false,
      observed: true,
      coverageUnreported: false,
      sibling: 'missing',
    },
    {
      used: 21,
      failedRefresh: false,
      observed: true,
      coverageUnreported: false,
      sibling: 'stale',
    },
  ])(
    'gates manual redemption for $used used, failed refresh $failedRefresh, observed $observed, unknown coverage $coverageUnreported, sibling $sibling',
    ({ used, failedRefresh, observed, coverageUnreported, sibling }) => {
      const mutate = vi.fn();
      vi.mocked(useClaimLimitReset).mockReturnValue(idleMutation({ mutate }));
      const data: LimitResetsResponse = {
        provider: 'openai',
        account_id: 'native-account',
        organization_id: null,
        cedar_ember: null,
        openai: {
          available_count: 1,
          credits: [
            {
              id: 'native-credit',
              reset_type: 'codex_rate_limits',
              status: 'available',
              is_supported_by_plan: true,
              granted_at: null,
              expires_at: null,
              title: null,
              description: null,
            },
          ],
          manual_redemption: observed
            ? {
                eligible: used > 20 && !coverageUnreported,
                reason: coverageUnreported
                  ? 'coverage_unreported'
                  : used > 20
                    ? null
                    : 'below_redemption_threshold',
                windows: coverageUnreported
                  ? []
                  : [
                      {
                        window: '7d',
                        used_percent: used,
                        resets_at_unix_secs: null,
                        duration_seconds: 604800,
                      },
                      ...(sibling === 'stale'
                        ? [
                            {
                              window: '5h' as const,
                              used_percent: null,
                              resets_at_unix_secs: null,
                              duration_seconds: 18000,
                            },
                          ]
                        : []),
                    ],
              }
            : null,
        },
      };
      const inventory = couponInventory(data);
      const failure = failedQuery<LimitResetsResponse>(
        new ApiError(503, null, null, 'Usage refresh unavailable'),
      );
      if (!failure.isError) throw new Error('Expected a failed query fixture');
      const state: CouponNudgeState = {
        resetsQ: failedRefresh
          ? { ...failure, data, isLoadingError: false, isRefetchError: true }
          : successQuery(data),
        nowMs: Date.now(),
        suppressed: true,
        nudge: null,
        derived: deriveCouponNudge(inventory ?? undefined, [], Date.now()),
        claimInFlight: false,
      };
      vi.mocked(useCouponNudge).mockReturnValue(state);
      render(
        <LimitResetAction
          upstream={makeOauthUpstream({ kind: 'openai_oauth' })}
          credentialsStored
          defaultOpen
          initialOwnedCount={sibling === 'positive-entry' ? 1 : undefined}
        />,
      );
      if (failedRefresh && sibling !== 'positive-entry') {
        expect(
          screen.getByRole('dialog', { name: 'Reset activity' }),
        ).toBeDefined();
        expect(
          screen.getByText('Coupon inventory could not be loaded.'),
        ).toBeDefined();
        expect(screen.queryByRole('button', { name: 'Use coupon' })).toBeNull();
        expect(
          screen.queryByRole('button', { name: 'View coupons' }),
        ).toBeNull();
        expect(mutate).not.toHaveBeenCalled();
        return;
      }
      const useCoupon = screen.getByRole('button', { name: 'Use coupon' });
      const enabled =
        used > 20 && observed && !failedRefresh && !coverageUnreported;
      expect((useCoupon as HTMLButtonElement).disabled).toBe(!enabled);
      if (!enabled) fireEvent.click(useCoupon);
      expect(mutate).not.toHaveBeenCalled();
      if (used === 20)
        expect(screen.getByText(/less than 80% remaining/)).toBeDefined();
      if (failedRefresh) {
        expect(
          screen.getByRole('dialog', { name: 'Use a reset coupon?' }),
        ).toBeDefined();
        expect(screen.getByText('Could not load reset status')).toBeDefined();
        expect(screen.queryByText(/21% used/)).toBeNull();
      }
      if (coverageUnreported) {
        expect(
          screen.getByText(/Covered allowances could not be confirmed/),
        ).toBeDefined();
        expect(
          screen.queryByText(/not supported by the current plan/),
        ).toBeNull();
      }
      if (sibling === 'stale') {
        expect((useCoupon as HTMLButtonElement).disabled).toBe(false);
        expect(screen.getByText('Unknown')).toBeDefined();
        expect(screen.getByText('5-hour')).toBeDefined();
      }
      if (sibling === 'missing') {
        expect((useCoupon as HTMLButtonElement).disabled).toBe(false);
        expect(screen.queryByText('5-hour')).toBeNull();
      }
    },
  );
});
