import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import type { ReactNode } from 'react';
import { Toaster } from 'sonner';
import { afterEach, describe, expect, it, vi } from 'vitest';
import type { QuotaSnapshot, UpstreamOAuthStatusResponse } from '../../lib/api';
import {
  type CouponInventoryResponse,
  couponInventoryForUpstream,
} from '../../lib/couponInventory';
import {
  clearPendingLimitResetOp,
  LIMIT_RESET_CLAIM_MUTATION_KEY,
  type LimitResetClaimResponse,
  type LimitResetsResponse,
  loadPendingLimitResetOp,
} from '../../lib/limitResets';
import type { Upstream } from '../../lib/queries';
import {
  buildUpstreamUsageRows,
  type UpstreamUsageData,
  UpstreamUsageTable,
} from './UpstreamUsageTable';

// Navigation is unrelated to action ownership; all query, action, modal and
// mutation lifecycle code stays real. Only HTTP responses are controlled below.
vi.mock('@tanstack/react-router', () => ({
  Link: ({
    to,
    search,
    children,
    ...props
  }: {
    to: string;
    search?: Record<string, string>;
    children?: ReactNode;
  }) => (
    <a
      href={search ? `${to}?${new URLSearchParams(search).toString()}` : to}
      {...props}
    >
      {children}
    </a>
  ),
}));

const ACTION_STATUS_PATH =
  /^\/admin\/v1\/upstreams\/([^/]+)\/(oauth\/status|limit-resets)$/;
const CONFIRM_ACTION_NAME = /^(Reset quota|Use coupon)$/;
const TARGET = 'pending-reset-target';
const clients: QueryClient[] = [];

const resetInventory: LimitResetsResponse = {
  provider: 'anthropic',
  account_id: 'target-account',
  organization_id: 'target-organization',
  openai: null,
  cedar_ember: {
    eligible: true,
    ineligible_reason: null,
    at_limit: true,
    grants: [
      {
        id: 'one-reset',
        label: 'Weekly reset',
        type: 'weekly',
        gives_back_weekly: false,
        unavailable_reason: null,
        resets_total: 1,
        resets_left: 1,
        starts_at: null,
        ends_at: null,
        clears: ['7d'],
        paused: false,
        usable_now: true,
        use_requires_limit: true,
        percent_used: { '7d': 100 },
        blocking: [],
      },
    ],
    next_grant_id: 'one-reset',
    weekly_resets_at: null,
    cooldown_until: null,
  },
};

const credential: UpstreamOAuthStatusResponse = {
  upstream_id: TARGET,
  kind: 'anthropic_oauth',
  has_credentials: true,
  status: 'connected',
  expires_at_unix_secs: null,
  refresh_token_present: false,
  refresh_token_expires_at_unix_secs: null,
  mode: null,
  can_refresh: false,
  scopes: [],
};

const successfulReset: LimitResetClaimResponse = {
  result: 'reset',
  reason: null,
  cleared: ['7d'],
  resets_left: 0,
  weekly_resets_at: null,
  cooldown_until: null,
  windows_reset: 1,
  usage_refresh: 'refreshed',
};

function jsonResponse(value: unknown) {
  return new Response(JSON.stringify(value), {
    status: 200,
    headers: { 'content-type': 'application/json' },
  });
}

function snapshot(utilization: number): QuotaSnapshot {
  return {
    window: '7d',
    state: 'fresh',
    source: 'api',
    utilization,
    status: null,
    resets_at_unix_secs: Math.floor(Date.now() / 1000) + 24 * 3600,
    surpassed_threshold: false,
    representative_claim: null,
    disabled_reason: null,
    extra_usage_enabled: null,
    extra_usage_monthly_limit: null,
    extra_usage_used_credits: null,
    observed_at_unix_millis: Date.now(),
    age_secs: 0,
  };
}

function usageData(targetUtilization: number): UpstreamUsageData {
  const upstreams: Upstream[] = [
    TARGET,
    ...Array.from({ length: 8 }, (_, index) => `other-${index}`),
  ].map((id) => ({
    id,
    name: id,
    kind: 'anthropic_oauth',
    enabled: true,
    base_url: null,
    spec_revision: 1,
    warmup_enabled: false,
    warmup_dialect_plugin: null,
    status: {
      last_apply_error: null,
      last_apply_at_unix_secs: null,
      last_warmup_at_unix_secs: null,
    },
  }));
  const fleet: CouponInventoryResponse = {
    upstreams: [{ upstream_id: TARGET, ...resetInventory }],
  };
  return {
    rows: buildUpstreamUsageRows({
      upstreams,
      latest: {
        now_unix_secs: Math.floor(Date.now() / 1000),
        max_staleness_secs: 300,
        upstreams: upstreams.map((upstream) => ({
          upstream_id: upstream.id,
          upstream_name: upstream.name,
          windows: [snapshot(upstream.id === TARGET ? targetUtilization : 0.5)],
        })),
      },
      status: upstreams.map((upstream) => ({
        id: upstream.id,
        status: 'active',
        last_apply_error: null,
      })),
      nudges: new Map(),
      usageByUpstreamId: new Map(),
    }),
    coupons: new Map(
      upstreams.map((upstream) => [
        upstream.id,
        couponInventoryForUpstream({ upstream, fleet, nowMs: Date.now() }),
      ]),
    ),
    couponsLoading: false,
    isLoading: false,
    quotaPending: false,
    statusPending: false,
    usagePending: false,
    quotaError: false,
    reconnectCount: 0,
  };
}

afterEach(() => {
  cleanup();
  for (const client of clients.splice(0)) client.clear();
  clearPendingLimitResetOp(TARGET);
  vi.unstubAllGlobals();
});

describe('Overview coupon action lifetime', () => {
  it.each(['reset', 'unknown'] as const)(
    'keeps a pending claim and its %s outcome when quota polling hides its row',
    async (result) => {
      clearPendingLimitResetOp(TARGET);
      const user = userEvent.setup();
      const client = new QueryClient({
        defaultOptions: {
          queries: { retry: false, gcTime: Infinity },
          mutations: { retry: false, gcTime: Infinity },
        },
      });
      clients.push(client);
      const { promise: pendingClaim, resolve: resolveClaim } =
        Promise.withResolvers<Response>();
      const claimRequests: RequestInit[] = [];
      vi.stubGlobal(
        'fetch',
        vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
          const path = String(input);
          if (
            path === `/admin/v1/upstreams/${TARGET}/limit-resets/claim` &&
            init?.method === 'POST'
          ) {
            claimRequests.push(init);
            return pendingClaim;
          }
          if (path === `/admin/v1/upstreams/${TARGET}/oauth/status`)
            return jsonResponse(credential);
          if (path === `/admin/v1/upstreams/${TARGET}/limit-resets`)
            return jsonResponse(resetInventory);
          throw new Error(
            `Unexpected HTTP request: ${init?.method ?? 'GET'} ${path}`,
          );
        }),
      );
      const surface = (data: UpstreamUsageData) => (
        <QueryClientProvider client={client}>
          <UpstreamUsageTable collapsedRows={8} data={data} />
          <Toaster />
        </QueryClientProvider>
      );
      const view = render(surface(usageData(1)));
      const otherTrigger = screen.getByRole('button', {
        name: 'Coupons for other-0: unknown',
      });
      try {
        expect(screen.getAllByTestId('upstream-usage-row')).toHaveLength(8);
        expect(
          within(screen.getAllByTestId('upstream-usage-row')[0]!).getByRole(
            'button',
            {
              name: `Coupons for ${TARGET}: 1`,
            },
          ),
        ).toBeTruthy();
        await user.click(
          screen.getByRole('button', { name: `Coupons for ${TARGET}: 1` }),
        );
        const dialog = await screen.findByRole('dialog');
        const confirm = within(dialog).getByRole('button', {
          name: CONFIRM_ACTION_NAME,
        });
        await waitFor(() =>
          expect(confirm.hasAttribute('disabled')).toBe(false),
        );
        await user.click(confirm);
        await waitFor(() => expect(claimRequests).toHaveLength(1));
        // Even a programmatic activation cannot replace an open operation owner.
        fireEvent.click(otherTrigger);
        expect(
          within(screen.getByRole('dialog')).getByText(TARGET),
        ).toBeTruthy();
        expect(loadPendingLimitResetOp(TARGET)?.grant_id).toBe('one-reset');
        await user.keyboard('{Escape}');
        expect(screen.queryByRole('dialog')).not.toBeNull();
        expect(
          client.isMutating({ mutationKey: LIMIT_RESET_CLAIM_MUTATION_KEY }),
        ).toBe(1);

        // A concurrent quota observation changes rank before the POST answers.
        // This must remove the list row without destroying its operation owner.
        view.rerender(surface(usageData(0)));
        expect(screen.getAllByTestId('upstream-usage-row')).toHaveLength(8);
        for (const row of screen.getAllByTestId('upstream-usage-row')) {
          expect(
            within(row).queryByRole('button', {
              name: `Coupons for ${TARGET}: 1`,
            }),
          ).toBeNull();
        }
        expect(
          client.isMutating({ mutationKey: LIMIT_RESET_CLAIM_MUTATION_KEY }),
        ).toBe(1);
        expect.soft(screen.queryByRole('dialog')).not.toBeNull();
        expect(loadPendingLimitResetOp(TARGET)).not.toBeNull();

        // Loading or temporarily empty list data must not own the overlay either.
        view.rerender(surface({ ...usageData(0), isLoading: true, rows: [] }));
        expect(screen.queryByRole('dialog')).not.toBeNull();
        view.rerender(surface({ ...usageData(0), rows: [] }));
        expect(screen.queryByRole('dialog')).not.toBeNull();

        await act(async () => {
          resolveClaim(jsonResponse({ ...successfulReset, result }));
        });
        await waitFor(() =>
          expect(
            client.isMutating({
              mutationKey: LIMIT_RESET_CLAIM_MUTATION_KEY,
            }),
          ).toBe(0),
        );
        expect(claimRequests).toHaveLength(1);
        if (result === 'reset') {
          expect.soft(loadPendingLimitResetOp(TARGET)).toBeNull();
          expect(
            await screen.findByText(
              'Reset completed. Usage reloaded from the provider.',
            ),
          ).toBeTruthy();
          await waitFor(() => expect(screen.queryByRole('dialog')).toBeNull());
          expect(screen.queryByText('Reset not confirmed')).toBeNull();
        } else {
          expect(loadPendingLimitResetOp(TARGET)).not.toBeNull();
          const unresolvedDialog = await screen.findByRole('dialog');
          const repeatAction = within(unresolvedDialog).queryByRole('button', {
            name: CONFIRM_ACTION_NAME,
          });
          expect(
            repeatAction === null || repeatAction.hasAttribute('disabled'),
          ).toBe(true);
          expect(claimRequests).toHaveLength(1);
        }
      } finally {
        await act(async () => {
          resolveClaim(jsonResponse(successfulReset));
        });
      }
    },
  );
  it('closes, reopens, and switches upstreams without submitting a claim', async () => {
    const user = userEvent.setup();
    const client = new QueryClient({
      defaultOptions: { queries: { retry: false, gcTime: Infinity } },
    });
    clients.push(client);
    const requests: string[] = [];
    vi.stubGlobal(
      'fetch',
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const path = String(input);
        requests.push(path);
        if (
          path.endsWith('/limit-resets/history?limit=100') &&
          init?.method !== 'POST'
        )
          return jsonResponse({ items: [], has_more: false, limit: 100 });
        const match = ACTION_STATUS_PATH.exec(path);
        if (init?.method === 'POST' || !match) {
          throw new Error(
            `Unexpected HTTP request: ${init?.method ?? 'GET'} ${path}`,
          );
        }
        return jsonResponse(
          match[2] === 'oauth/status'
            ? { ...credential, upstream_id: match[1] }
            : resetInventory,
        );
      }),
    );
    render(
      <QueryClientProvider client={client}>
        <UpstreamUsageTable data={usageData(1)} />
      </QueryClientProvider>,
    );
    const targetTrigger = screen.getByRole('button', {
      name: `Coupons for ${TARGET}: 1`,
    });
    const otherTrigger = screen.getByRole('button', {
      name: 'Coupons for other-0: unknown',
    });
    for (const [trigger, name] of [
      [targetTrigger, TARGET],
      [targetTrigger, TARGET],
      [otherTrigger, 'other-0'],
    ] as const) {
      await user.click(trigger);
      const dialog = await screen.findByRole('dialog');
      expect(within(dialog).getByText(name)).toBeTruthy();
      if (name === TARGET) {
        await waitFor(() =>
          expect(
            within(dialog)
              .getByRole('button', { name: CONFIRM_ACTION_NAME })
              .hasAttribute('disabled'),
          ).toBe(false),
        );
        await user.click(
          within(dialog).getByRole('button', { name: 'History' }),
        );
        const history = await screen.findByRole('dialog', {
          name: 'Reset activity',
        });
        await waitFor(() =>
          expect(history.contains(document.activeElement)).toBe(true),
        );
        expect(document.activeElement).not.toBe(trigger);
        await user.tab();
        expect(history.contains(document.activeElement)).toBe(true);
      } else {
        expect(
          await screen.findByRole('dialog', { name: 'Reset activity' }),
        ).toBeTruthy();
        expect(
          within(screen.getByRole('dialog')).queryByRole('button', {
            name: CONFIRM_ACTION_NAME,
          }),
        ).toBeNull();
      }
      await user.keyboard('{Escape}');
      await waitFor(() => expect(screen.queryByRole('dialog')).toBeNull());
      await waitFor(() => expect(document.activeElement).toBe(trigger));
    }
    expect(requests.some((path) => path.endsWith('/claim'))).toBe(false);
    expect(loadPendingLimitResetOp(TARGET)).toBeNull();
  });
});
