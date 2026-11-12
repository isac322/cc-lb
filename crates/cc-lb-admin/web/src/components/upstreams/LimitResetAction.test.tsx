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
import { ApiError } from '../../lib/api';
import { deriveCouponNudge } from '../../lib/couponNudge';
import {
  type CouponQuotaWindow,
  couponInventory,
} from '../../lib/couponPresentation';
import {
  type LimitResetGrant,
  type LimitResetsResponse,
  loadPendingLimitResetOp,
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

vi.mock('../../lib/limitResets', async () => ({
  ...(await vi.importActual<Record<string, unknown>>('../../lib/limitResets')),
  useClaimLimitReset: vi.fn(),
  useLimitResetHistory: vi.fn(),
  loadPendingLimitResetOp: vi.fn(),
  savePendingLimitResetOp: vi.fn(),
}));
vi.mock('./CouponNudge', async () => ({
  ...(await vi.importActual<Record<string, unknown>>('./CouponNudge')),
  useCouponNudge: vi.fn(),
}));
vi.mock('../ui/RelativeTime', () => ({
  RelativeTime: ({ ts }: { ts: number | Date }) => (
    <time>{(ts instanceof Date ? ts : new Date(ts)).toISOString()}</time>
  ),
  ResetCountdown: () => <span>Natural reset</span>,
}));
const NOW = Date.parse('2026-11-20T12:00:00Z');
const HOUR = 3600;
const mutate = vi.fn();
function grant(overrides: Partial<LimitResetGrant> = {}): LimitResetGrant {
  return {
    id: 'credit',
    label: 'Reset',
    type: null,
    gives_back_weekly: false,
    unavailable_reason: null,
    resets_total: 1,
    resets_left: 1,
    starts_at: null,
    ends_at: new Date(NOW + 7 * 86400000).toISOString(),
    clears: ['5h'],
    paused: false,
    usable_now: true,
    use_requires_limit: false,
    percent_used: {},
    blocking: [],
    ...overrides,
  };
}
function inventory(
  provider: 'anthropic' | 'openai',
  grants: LimitResetGrant[] = [grant()],
): LimitResetsResponse {
  return {
    provider,
    account_id: 'account',
    organization_id: provider === 'anthropic' ? 'org' : null,
    cedar_ember:
      provider === 'anthropic'
        ? {
            eligible: true,
            ineligible_reason: null,
            at_limit: null,
            grants,
            next_grant_id: null,
            weekly_resets_at: null,
            cooldown_until: null,
          }
        : null,
    openai:
      provider === 'openai'
        ? {
            available_count: grants.reduce(
              (sum, item) => sum + item.resets_left,
              0,
            ),
            manual_redemption: {
              eligible: true,
              reason: null,
              windows: [
                {
                  window: '7d',
                  used_percent: 80,
                  resets_at_unix_secs: NOW / 1000 + 86400,
                  duration_seconds: 604800,
                },
              ],
            },
            credits: grants.map((item) => ({
              id: item.id,
              reset_type: 'codex_rate_limits',
              status: item.resets_left > 0 ? 'available' : 'redeemed',
              is_supported_by_plan: item.usable_now,
              granted_at: null,
              expires_at: item.ends_at,
              title: null,
              description: null,
            })),
          }
        : null,
  };
}
function quota(used = 25, resetIn = 2 * HOUR): CouponQuotaWindow[] {
  return [
    {
      window: '5h',
      utilization: used / 100,
      state: 'fresh',
      resets_at_unix_secs: NOW / 1000 + resetIn,
    },
  ];
}
function observe(
  data: LimitResetsResponse | undefined,
  windows: CouponQuotaWindow[] = [],
  options: {
    pending?: boolean;
    error?: boolean;
    inFlight?: boolean;
    nowMs?: number;
  } = {},
) {
  let resetsQ =
    data === undefined || options.pending
      ? pendingQuery<LimitResetsResponse>()
      : successQuery(data);
  if (options.error) {
    const failed = failedQuery<LimitResetsResponse>(
      new ApiError(503, null, null, 'Inventory unavailable'),
    );
    if (!failed.isError) throw new Error('Expected a failed query fixture');
    resetsQ =
      data === undefined
        ? failed
        : {
            ...failed,
            data,
            isLoadingError: false,
            isRefetchError: true,
          };
  }
  const nowMs = options.nowMs ?? NOW;
  const state: CouponNudgeState = {
    resetsQ,
    nowMs,
    suppressed: true,
    nudge: null,
    derived: deriveCouponNudge(
      couponInventory(data) ?? undefined,
      windows,
      nowMs,
    ),
    claimInFlight: options.inFlight ?? false,
  };
  vi.mocked(useCouponNudge).mockReturnValue(state);
}
beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(loadPendingLimitResetOp).mockReturnValue(null);
  vi.mocked(useClaimLimitReset).mockReturnValue(idleMutation({ mutate }));
  vi.mocked(useLimitResetHistory).mockReturnValue(
    successQuery({ items: [], has_more: false, limit: 100 }),
  );
});
afterEach(cleanup);

for (const provider of ['anthropic', 'openai'] as const) {
  const upstream = makeOauthUpstream({
    kind: provider === 'openai' ? 'openai_oauth' : 'anthropic_oauth',
  });
  describe(`${provider} coupon entry`, () => {
    it.each([
      'zero',
      'expired',
      'depleted',
      'missing',
      'error',
      'disconnected',
    ] as const)('opens History for %s without a POST', async (state) => {
      const data =
        state === 'missing'
          ? { ...inventory(provider), cedar_ember: null, openai: null }
          : inventory(
              provider,
              state === 'zero'
                ? []
                : [
                    grant(
                      state === 'expired'
                        ? { ends_at: new Date(NOW - 1000).toISOString() }
                        : state === 'depleted'
                          ? { resets_left: 0 }
                          : {},
                    ),
                  ],
            );
      if (provider === 'openai' && state === 'expired' && data.openai)
        data.openai.available_count = 0;
      observe(data, [], { error: state === 'error' });
      render(
        <LimitResetAction
          upstream={upstream}
          credentialsStored={state !== 'disconnected'}
          defaultOpen
        />,
      );
      expect(
        await screen.findByRole('dialog', { name: 'Reset activity' }),
      ).toBeDefined();
      expect(
        screen.queryByRole('button', {
          name: /View coupons|Back to coupon|Use coupon/,
        }),
      ).toBeNull();
      expect(useLimitResetHistory).toHaveBeenCalledWith(upstream.id, true, 100);
      expect(mutate).not.toHaveBeenCalled();
    });

    it('keeps positive but blocked inventory in the confirmation', async () => {
      observe(
        inventory(provider, [
          grant({ usable_now: false, blocking: ['requires_limit'] }),
        ]),
      );
      render(
        <LimitResetAction upstream={upstream} credentialsStored defaultOpen />,
      );
      expect(
        await screen.findByRole('dialog', { name: 'Use a reset coupon?' }),
      ).toBeDefined();
      expect(
        (
          screen.getByRole('button', {
            name: 'Use coupon',
          }) as HTMLButtonElement
        ).disabled,
      ).toBe(true);
    });

    it('keeps late credential and inventory discovery on History until explicitly opened', async () => {
      observe(undefined, [], { pending: true });
      const view = render(
        <LimitResetAction
          upstream={upstream}
          credentialsStored={undefined}
          defaultOpen
        />,
      );
      expect(
        screen.getByRole('dialog', { name: 'Reset activity' }),
      ).toBeDefined();
      expect(
        screen.getByText('Checking current coupon inventory…'),
      ).toBeDefined();
      expect(screen.queryByRole('button', { name: 'Use coupon' })).toBeNull();
      observe(inventory(provider, []));
      view.rerender(
        <LimitResetAction upstream={upstream} credentialsStored defaultOpen />,
      );
      expect(
        await screen.findByRole('dialog', { name: 'Reset activity' }),
      ).toBeDefined();
      observe(inventory(provider));
      view.rerender(
        <LimitResetAction upstream={upstream} credentialsStored defaultOpen />,
      );
      expect(
        screen.getByRole('dialog', { name: 'Reset activity' }),
      ).toBeDefined();
      fireEvent.click(screen.getByRole('button', { name: 'View coupons' }));
      expect(
        await screen.findByRole('dialog', { name: 'Use a reset coupon?' }),
      ).toBeDefined();
      observe(inventory(provider, []));
      view.rerender(
        <LimitResetAction upstream={upstream} credentialsStored defaultOpen />,
      );
      expect(
        screen.getByRole('dialog', { name: 'Use a reset coupon?' }),
      ).toBeDefined();
      expect(mutate).not.toHaveBeenCalled();
    });

    it.each([1, 0, null])(
      'uses the clicked table inventory %s before asynchronous queries arrive',
      async (count) => {
        observe(undefined, [], { pending: true });
        render(
          <LimitResetAction
            upstream={upstream}
            credentialsStored={undefined}
            initialOwnedCount={count}
            defaultOpen
          />,
        );
        expect(
          await screen.findByRole('dialog', {
            name:
              count != null && count > 0
                ? 'Use a reset coupon?'
                : 'Reset activity',
          }),
        ).toBeDefined();
        expect(mutate).not.toHaveBeenCalled();
      },
    );

    it('prioritizes an unresolved operation over empty or disconnected inventory', async () => {
      vi.mocked(loadPendingLimitResetOp).mockReturnValue({
        upstream_id: upstream.id,
        account_id: 'account',
        organization_id: provider === 'anthropic' ? 'org' : null,
        grant_id: 'credit',
        request_id: 'pending-request',
        started_at_unix_secs: NOW / 1000,
      });
      observe(inventory(provider, []));
      render(
        <LimitResetAction
          upstream={upstream}
          credentialsStored={false}
          initialOwnedCount={0}
          defaultOpen
        />,
      );
      expect(
        await screen.findByRole('dialog', { name: 'Reset not confirmed' }),
      ).toBeDefined();
      expect(screen.queryByRole('button', { name: 'Use coupon' })).toBeNull();
      fireEvent.click(screen.getByRole('button', { name: 'History' }));
      expect(
        await screen.findByRole('dialog', { name: 'Reset activity' }),
      ).toBeDefined();
      fireEvent.click(
        screen.getByRole('button', { name: 'Review pending reset' }),
      );
      expect(
        await screen.findByRole('dialog', { name: 'Reset not confirmed' }),
      ).toBeDefined();
      expect(mutate).not.toHaveBeenCalled();
    });
  });
}

describe('approved nonblocking guidance', () => {
  it('keeps caution guidance enabled and frozen during a claim', async () => {
    const data = inventory('anthropic');
    const windows = quota(100, 8 * 60);
    observe(data, windows);
    const surface = (observations: CouponQuotaWindow[]) => (
      <LimitResetAction
        upstream={makeOauthUpstream()}
        credentialsStored
        defaultOpen
        quotaWindows={observations}
      />
    );
    const view = render(surface(windows));
    const group = await screen.findByRole('group', { name: 'Coupon guidance' });
    expect(within(group).getByRole('status').getAttribute('data-tone')).toBe(
      'warning',
    );
    const guidance = group.textContent;
    const confirm = screen.getByRole('button', { name: 'Use coupon' });
    expect((confirm as HTMLButtonElement).disabled).toBe(false);
    fireEvent.click(confirm);
    expect(mutate).toHaveBeenCalledTimes(1);
    observe(inventory('anthropic', []), quota(0), { inFlight: true });
    view.rerender(surface(quota(0)));
    expect(
      screen.getByRole('group', { name: 'Coupon guidance' }).textContent,
    ).toBe(guidance);
    expect(
      screen.getByRole('dialog', { name: 'Use a reset coupon?' }),
    ).toBeDefined();
    fireEvent.click(screen.getByRole('button', { name: 'Use coupon' }));
    expect(mutate).toHaveBeenCalledTimes(1);
  });

  it('updates advice and exact expiry when selecting another type', async () => {
    const user = userEvent.setup();
    const expiry = new Date(NOW + 10 * 86400000).toISOString();
    const data = inventory('anthropic', [
      grant(),
      grant({ id: 'weekly', type: 'weekly', clears: ['7d'], ends_at: expiry }),
    ]);
    const windows = [
      ...quota(),
      {
        window: '7d',
        utilization: 0,
        state: 'fresh' as const,
        resets_at_unix_secs: NOW / 1000 + 4 * 86400,
      },
    ];
    observe(data, windows);
    render(
      <LimitResetAction
        upstream={makeOauthUpstream()}
        credentialsStored
        defaultOpen
        quotaWindows={windows}
      />,
    );
    const before = (
      await screen.findByRole('group', { name: 'Coupon guidance' })
    ).textContent;
    await user.click(screen.getByRole('combobox', { name: 'Coupon type' }));
    await user.click(await screen.findByRole('option', { name: /Weekly/ }));
    expect(
      screen.getByRole('group', { name: 'Coupon guidance' }).textContent,
    ).not.toBe(before);
    expect(screen.getByText(expiry)).toBeDefined();
    expect(
      (screen.getByRole('button', { name: 'Use coupon' }) as HTMLButtonElement)
        .disabled,
    ).toBe(false);
  });

  it('uses informational styling for useful-now without changing eligibility', async () => {
    observe(inventory('anthropic'), quota(100, 3 * HOUR));
    render(
      <LimitResetAction
        upstream={makeOauthUpstream()}
        credentialsStored
        defaultOpen
        quotaWindows={quota(100, 3 * HOUR)}
      />,
    );
    const group = await screen.findByRole('group', { name: 'Coupon guidance' });
    expect(within(group).getByRole('status').getAttribute('data-tone')).toBe(
      'info',
    );
    expect(
      (screen.getByRole('button', { name: 'Use coupon' }) as HTMLButtonElement)
        .disabled,
    ).toBe(false);
  });
});

describe('coupon workflow focus ownership', () => {
  it.each([true, false])(
    'returns detail focus after closing History with positive inventory %s',
    async (positive) => {
      const user = userEvent.setup();
      observe(inventory('anthropic', positive ? [grant()] : []), quota());
      render(
        <LimitResetAction
          upstream={makeOauthUpstream()}
          credentialsStored
          quotaWindows={quota()}
        />,
      );
      const opener = screen.getByRole('button', { name: /^Reset quota/ });
      await user.click(opener);
      if (positive) {
        await screen.findByRole('dialog', { name: 'Use a reset coupon?' });
        await user.click(screen.getByRole('button', { name: 'History' }));
      }
      const history = await screen.findByRole('dialog', {
        name: 'Reset activity',
      });
      await waitFor(() =>
        expect(history.contains(document.activeElement)).toBe(true),
      );
      expect(document.activeElement).not.toBe(opener);
      await user.tab();
      // A single-control drawer wraps through Base UI's focus guard, whose
      // enqueueFocus settles on the next animation frame.
      await waitFor(() =>
        expect(history.contains(document.activeElement)).toBe(true),
      );
      await user.keyboard('{Escape}');
      await waitFor(() => expect(screen.queryByRole('dialog')).toBeNull());
      await waitFor(() => expect(document.activeElement).toBe(opener));
      expect(mutate).not.toHaveBeenCalled();
    },
  );

  it('keeps focus in the workflow when returning from History to confirmation', async () => {
    const user = userEvent.setup();
    observe(inventory('anthropic'), quota());
    render(
      <LimitResetAction
        upstream={makeOauthUpstream()}
        credentialsStored
        quotaWindows={quota()}
      />,
    );
    const opener = screen.getByRole('button', { name: /^Reset quota/ });
    await user.click(opener);
    await user.click(screen.getByRole('button', { name: 'History' }));
    await screen.findByRole('dialog', { name: 'Reset activity' });
    await user.click(screen.getByRole('button', { name: 'View coupons' }));
    const confirmation = await screen.findByRole('dialog', {
      name: 'Use a reset coupon?',
    });
    await waitFor(() =>
      expect(confirmation.contains(document.activeElement)).toBe(true),
    );
    expect(document.activeElement).not.toBe(opener);
    await user.keyboard('{Escape}');
    await waitFor(() => expect(screen.queryByRole('dialog')).toBeNull());
    await waitFor(() => expect(document.activeElement).toBe(opener));
    expect(mutate).not.toHaveBeenCalled();
  });
});

describe('OpenAI inventory aging between polls', () => {
  const upstream = makeOauthUpstream({ kind: 'openai_oauth' });
  it('opens History when the complete cached credit list has expired without another provider poll', async () => {
    const expiresAt = NOW + 1000;
    const cached = inventory('openai', [
      grant({ ends_at: new Date(expiresAt).toISOString() }),
    ]);
    observe(cached, [], { nowMs: expiresAt + 1 });
    render(
      <LimitResetAction upstream={upstream} credentialsStored defaultOpen />,
    );
    expect(
      await screen.findByRole('dialog', { name: 'Reset activity' }),
    ).toBeDefined();
    expect(screen.getByText('No active coupons available.')).toBeDefined();
    expect(screen.queryByRole('button', { name: 'Use coupon' })).toBeNull();
    expect(mutate).not.toHaveBeenCalled();
  });

  it('keeps a confirmation already being read open when its last credit expires', async () => {
    const expiresAt = NOW + 1000;
    const cached = inventory('openai', [
      grant({ ends_at: new Date(expiresAt).toISOString() }),
    ]);
    observe(cached);
    const view = render(
      <LimitResetAction upstream={upstream} credentialsStored defaultOpen />,
    );
    expect(
      await screen.findByRole('dialog', { name: 'Use a reset coupon?' }),
    ).toBeDefined();
    observe(cached, [], { nowMs: expiresAt + 1 });
    view.rerender(
      <LimitResetAction upstream={upstream} credentialsStored defaultOpen />,
    );
    expect(
      screen.getByRole('dialog', { name: 'Use a reset coupon?' }),
    ).toBeDefined();
    expect(
      (screen.getByRole('button', { name: 'Use coupon' }) as HTMLButtonElement)
        .disabled,
    ).toBe(true);
    expect(mutate).not.toHaveBeenCalled();
  });

  it('retains the native summary for a capped credit list instead of inventing zero inventory', async () => {
    const expiresAt = NOW + 1000;
    const cached = inventory('openai', [
      grant({ ends_at: new Date(expiresAt).toISOString() }),
    ]);
    if (!cached.openai) throw new Error('Missing native fixture');
    cached.openai.available_count = 2;
    observe(cached, [], { nowMs: expiresAt + 1 });
    render(
      <LimitResetAction upstream={upstream} credentialsStored defaultOpen />,
    );
    expect(
      await screen.findByRole('dialog', { name: 'Use a reset coupon?' }),
    ).toBeDefined();
    expect(
      (screen.getByRole('button', { name: 'Use coupon' }) as HTMLButtonElement)
        .disabled,
    ).toBe(true);
    expect(mutate).not.toHaveBeenCalled();
  });
});
