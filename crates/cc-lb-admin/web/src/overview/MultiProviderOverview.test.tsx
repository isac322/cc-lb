import type * as ReactRouterModule from '@tanstack/react-router';
import {
  cleanup,
  fireEvent,
  render,
  screen,
  within,
} from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import {
  type AnchorHTMLAttributes,
  createElement,
  type ReactNode,
} from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { DashboardUsageResponse } from '../lib/api';
import type { CouponInventoryResponse } from '../lib/couponInventory';
import * as couponInventory from '../lib/couponInventory';
import type * as CouponNudgeModule from '../lib/couponNudge';
import type * as ProvidersModule from '../lib/providers';
import type { ProviderFamily } from '../lib/providers';
import * as queries from '../lib/queries';
import {
  failedQuery,
  pendingQuery,
  successQuery,
} from '../lib/test-utils/hook-results';
import { MultiProviderOverview } from './MultiProviderOverview';

const overviewMocks = vi.hoisted(() => ({
  providers: ['anthropic', 'openai', 'gemini'] as ProviderFamily[],
  navigate: vi.fn(),
}));

vi.mock('@tanstack/react-router', async (importOriginal) => ({
  ...(await importOriginal<typeof ReactRouterModule>()),
  useNavigate: () => overviewMocks.navigate,
  useSearch: () => ({ range: '24h' }),
  Link: ({
    to,
    search,
    children,
    ...props
  }: AnchorHTMLAttributes<HTMLAnchorElement> & {
    to: string;
    search?: Record<string, string>;
    children?: ReactNode;
  }) =>
    createElement(
      'a',
      {
        ...props,
        href: search ? `${to}?${new URLSearchParams(search).toString()}` : to,
      },
      children,
    ),
}));

vi.mock('../lib/queries', async (importOriginal) => ({
  ...(await importOriginal<typeof queries>()),
  usePrincipalNameMap: vi.fn(),
  usePrincipals: vi.fn(),
  useUpstreams: vi.fn(),
  useUsage: vi.fn(),
}));

vi.mock('../lib/providers', async (importOriginal) => ({
  ...(await importOriginal<typeof ProvidersModule>()),
  useInstallation: () => ({ providers: overviewMocks.providers }),
}));

vi.mock('../lib/couponInventory', async (importOriginal) => ({
  ...(await importOriginal<typeof couponInventory>()),
  useCouponInventory: vi.fn(),
}));

vi.mock('../lib/couponNudge', async (importOriginal) => ({
  ...(await importOriginal<typeof CouponNudgeModule>()),
  useCouponNow: () => Date.parse('2026-11-12T12:00:00Z'),
}));

// Keep the actual page, principal table, row and coupon popover real. Pool
// charts and the request stream are independent sections of this regression.
vi.mock('./ProviderPoolCard', () => ({ ProviderPoolCard: () => null }));
vi.mock('../lib/useRequestEventsFeed', () => ({
  useRequestEventsFeed: () => ({}),
}));
vi.mock('../components/ui/RequestEventsFeed', () => ({
  FeedLiveStatus: () => null,
  RequestEventsFeed: () => null,
}));

afterEach(cleanup);

function usage(
  series: DashboardUsageResponse['series'],
): DashboardUsageResponse {
  return {
    range: '24h',
    step: 'hour',
    group_by: 'principal',
    window_start_unix_secs: 0,
    window_end_unix_secs: 1,
    series,
    observed: true,
  };
}
function bucket(request_count: number, cost: number) {
  return {
    bucket_start_unix_secs: 0,
    request_count,
    input_tokens: 100,
    output_tokens: 20,
    cache_creation_input_tokens: 0,
    cache_read_input_tokens: 80,
    error_count: 0,
    virtual_cost_micros: cost,
    latency_ms_sum: 0,
    latency_count: 0,
  };
}

const COUPON_FLEET: CouponInventoryResponse = {
  upstreams: [
    ...['claude-main', 'claude-duplicate'].map((upstream_id) => ({
      upstream_id,
      provider: 'anthropic' as const,
      account_id: 'same-account',
      organization_id: 'same-organization',
      openai: null,
      cedar_ember: {
        eligible: true,
        ineligible_reason: null,
        at_limit: null,
        grants: [
          {
            id: 'weekly-grant',
            label: 'Provider weekly reset',
            type: 'weekly',
            gives_back_weekly: false,
            unavailable_reason: null,
            resets_total: 4,
            resets_left: 3,
            starts_at: null,
            ends_at: null,
            clears: ['7d'],
            paused: true,
            usable_now: false,
            use_requires_limit: true,
            percent_used: {},
            blocking: [],
          },
        ],
        next_grant_id: null,
        weekly_resets_at: null,
        cooldown_until: null,
      },
    })),
    {
      upstream_id: 'codex-main',
      provider: 'openai',
      account_id: 'codex-account',
      organization_id: null,
      cedar_ember: null,
      openai: { available_count: 2, credits: [] },
    },
  ],
};

describe('actual multi-provider Overview coupon surface', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    overviewMocks.providers = ['anthropic', 'openai', 'gemini'];
    const principals = [
      {
        id: 'all-access',
        name: 'All access',
        kind: 'human' as const,
        enabled: true,
        revision: 1,
        allowed_models: [],
        allowed_upstreams: [],
        default_limits: [],
      },
      {
        id: 'restricted',
        name: 'Restricted',
        kind: 'machine' as const,
        enabled: true,
        revision: 1,
        allowed_models: [],
        allowed_upstreams: ['claude-main'],
        default_limits: [],
      },
    ];
    vi.mocked(queries.usePrincipalNameMap).mockReturnValue(
      new Map(principals.map((principal) => [principal.id, principal.name])),
    );
    vi.mocked(queries.usePrincipals).mockReturnValue(
      successQuery({ principals }),
    );
    vi.mocked(queries.useUpstreams).mockReturnValue(
      successQuery({ upstreams: [] }),
    );
    vi.mocked(queries.useUsage).mockImplementation(
      (_range, _step, _group, _upstream, _projection, options) =>
        successQuery(
          usage(
            options?.providers?.length
              ? principals.map((principal) => ({
                  key: principal.id,
                  buckets: [bucket(1, 100)],
                }))
              : [],
          ),
        ),
    );
    vi.mocked(couponInventory.useCouponInventory).mockReturnValue(
      successQuery(COUPON_FLEET),
    );
  });

  it('renders allowlist-scoped, account-deduplicated reset counts on the real page', () => {
    render(createElement(MultiProviderOverview));

    const rows = screen.getAllByTestId('multi-provider-principal-row');
    expect(rows).toHaveLength(2);
    expect(
      within(rows[0]).getByTestId('coupon-inventory-badge').textContent,
    ).toBe('5 resets');
    expect(
      within(rows[1]).getByTestId('coupon-inventory-badge').textContent,
    ).toBe('3 resets');
    expect(couponInventory.useCouponInventory).toHaveBeenCalledTimes(1);
    expect(queries.usePrincipals).toHaveBeenCalledTimes(1);
  });

  it.each([
    { providers: ['anthropic'], total: '3 resets' },
    { providers: ['openai'], total: '2 resets' },
    { providers: ['gemini'], total: 'Resets not reported' },
  ] satisfies { providers: ProviderFamily[]; total: string }[])(
    'keeps inventory within the visible $providers scope',
    ({ providers, total }) => {
      overviewMocks.providers = providers;
      render(createElement(MultiProviderOverview));
      const row = screen.getAllByTestId('multi-provider-principal-row')[0];
      expect(
        within(row).getByTestId('coupon-inventory-badge').textContent,
      ).toBe(total);
    },
  );

  it.each(['pointer', 'keyboard'] as const)(
    'opens type details with %s without triggering principal navigation',
    async (interaction) => {
      const user = userEvent.setup();
      render(createElement(MultiProviderOverview));
      const row = screen.getAllByTestId('multi-provider-principal-row')[0];
      const badge = within(row).getByTestId('coupon-inventory-badge');
      const nameLink = within(row).getByRole('link', { name: 'All access' });
      expect(badge.closest('a')).toBeNull();
      expect(nameLink.getAttribute('href')).toBe(
        '/principals?selectedId=all-access',
      );

      if (interaction === 'pointer') {
        await user.click(badge);
      } else {
        nameLink.focus();
        await user.tab();
        expect(document.activeElement).toBe(
          within(row).getByTestId('coupon-inventory-badge'),
        );
        await user.keyboard('{Enter}');
      }
      const details = await screen.findByTestId('coupon-inventory-details');
      expect(details.textContent).toContain('Weekly reset');
      expect(details.textContent).toContain('3 resets');
      expect(details.textContent).toContain('Type not reported');
      expect(
        within(details).getAllByRole('link', { name: 'claude-main' }).length,
      ).toBeGreaterThan(0);
      fireEvent.click(within(details).getByText('Resets on allowed upstreams'));
      expect(overviewMocks.navigate).not.toHaveBeenCalled();

      fireEvent.click(within(row).getAllByRole('cell')[1]);
      expect(overviewMocks.navigate).toHaveBeenCalledWith({
        to: '/principals',
        search: { selectedId: 'all-access' },
      });
    },
  );

  it('shows unknown rather than zero while reset inventory loads or fails', () => {
    vi.mocked(couponInventory.useCouponInventory).mockReturnValue(
      pendingQuery(),
    );
    const page = render(createElement(MultiProviderOverview));
    expect(screen.getAllByTestId('coupon-inventory-badge')[0].textContent).toBe(
      'Resets loading…',
    );
    vi.mocked(couponInventory.useCouponInventory).mockReturnValue(
      failedQuery(new Error('Unavailable')),
    );
    page.rerender(createElement(MultiProviderOverview));
    expect(screen.getAllByTestId('coupon-inventory-badge')[0].textContent).toBe(
      'Resets unknown',
    );
  });

  it('does not assume unrestricted access before principal allowlists load', () => {
    vi.mocked(queries.usePrincipals).mockReturnValue(pendingQuery());
    render(createElement(MultiProviderOverview));
    for (const badge of screen.getAllByTestId('coupon-inventory-badge')) {
      expect(badge.textContent).toBe('Resets loading…');
    }
  });

  it('does not present placeholder fleet data as a current inventory', () => {
    const snapshot = successQuery(COUPON_FLEET);
    if (!snapshot.isSuccess) throw new Error('Expected a successful fixture');
    vi.mocked(couponInventory.useCouponInventory).mockReturnValue({
      ...snapshot,
      isPlaceholderData: true,
    });
    render(createElement(MultiProviderOverview));
    for (const badge of screen.getAllByTestId('coupon-inventory-badge')) {
      expect(badge.textContent).toBe('Resets unknown');
    }
  });
});
