import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type * as ReactRouterModule from '@tanstack/react-router';
import { cleanup, render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import {
  type AnchorHTMLAttributes,
  createElement,
  type ReactNode,
} from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { DashboardUsageResponse } from '../lib/api';
import type { CouponInventoryResponse } from '../lib/couponInventory';
import type * as CouponNudgeModule from '../lib/couponNudge';
import type * as LimitResetsModule from '../lib/limitResets';
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
  inventoryQuery: vi.fn(),
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
  useStatus: vi.fn(),
  useSubscriptionQuotaLatest: vi.fn(),
  useUpstreamOAuthStatus: vi.fn(),
}));
vi.mock('../lib/oauthReconnect', () => ({
  useOAuthReconnectNudges: () => ({ nudges: new Map() }),
  isTerminalOAuthReconnectReason: () => false,
}));
vi.mock('../lib/providers', async (importOriginal) => ({
  ...(await importOriginal<typeof ProvidersModule>()),
  useInstallation: () => ({ providers: overviewMocks.providers }),
}));
// Mock the polling boundary: the real inventory hook calls its module-local
// fleet reader, so replacing only that exported reader cannot intercept it.
vi.mock('../lib/usePolledData', () => ({
  usePolledData: () => overviewMocks.inventoryQuery(),
}));
vi.mock('../lib/couponNudge', async (importOriginal) => ({
  ...(await importOriginal<typeof CouponNudgeModule>()),
  useCouponNow: () => Date.parse('2026-11-12T12:00:00Z'),
}));
vi.mock('../lib/limitResets', async (importOriginal) => ({
  ...(await importOriginal<typeof LimitResetsModule>()),
  useLimitResets: () => successQuery(COUPON_FLEET.upstreams[0]),
}));
// The actual Overview, shared upstream table and action dialog stay real.
vi.mock('./ProviderPoolCard', () => ({ ProviderPoolCard: () => null }));
vi.mock('../lib/useRequestEventsFeed', () => ({
  useRequestEventsFeed: () => ({}),
}));
vi.mock('../components/ui/RequestEventsFeed', () => ({
  FeedLiveStatus: () => null,
  RequestEventsFeed: () => null,
}));
afterEach(cleanup);

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

function page() {
  return createElement(
    QueryClientProvider,
    { client: new QueryClient() },
    createElement(MultiProviderOverview),
  );
}

describe('actual Overview upstream coupon surface', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    overviewMocks.providers = ['anthropic', 'openai', 'gemini'];
    vi.mocked(queries.usePrincipalNameMap).mockReturnValue(
      new Map([['person', 'Person']]),
    );
    vi.mocked(queries.useUpstreams).mockReturnValue(
      successQuery({
        upstreams: [
          ['claude-main', 'anthropic_oauth'],
          ['claude-duplicate', 'anthropic_oauth'],
          ['codex-main', 'openai_oauth'],
          ['key', 'anthropic_api_key'],
        ].map(([id, kind]) => ({
          id,
          name: id,
          kind,
          enabled: true,
          spec_revision: 1,
          base_url: null,
          warmup_enabled: false,
          warmup_dialect_plugin: null,
          status: {
            last_apply_error: null,
            last_apply_at_unix_secs: null,
            last_warmup_at_unix_secs: null,
          },
        })) as queries.Upstream[],
      }),
    );
    vi.mocked(queries.useSubscriptionQuotaLatest).mockReturnValue(
      successQuery({
        upstreams: [],
        now_unix_secs: 0,
        max_staleness_secs: 300,
      }),
    );
    vi.mocked(queries.useStatus).mockReturnValue(
      successQuery({
        version: 'test',
        git_sha: 'test',
        uptime_secs: 0,
        generation: 1,
        build: { rust_version: 'test', profile: 'test', target: 'test' },
        upstreams: ['claude-main', 'claude-duplicate', 'codex-main', 'key'].map(
          (id) => ({
            id,
            name: id,
            status: 'active',
            last_apply_at_unix_secs: null,
            last_apply_error: null,
          }),
        ),
      }),
    );
    vi.mocked(queries.useUpstreamOAuthStatus).mockReturnValue(
      successQuery({
        upstream_id: 'claude-main',
        kind: 'anthropic_oauth',
        has_credentials: true,
        status: 'connected',
        expires_at_unix_secs: null,
        refresh_token_present: false,
        refresh_token_expires_at_unix_secs: null,
        mode: null,
        can_refresh: false,
        scopes: [],
      }),
    );
    vi.mocked(queries.useUsage).mockReturnValue(
      successQuery({
        range: '24h',
        step: 'hour',
        group_by: 'principal',
        window_start_unix_secs: 0,
        window_end_unix_secs: 1,
        series: [],
        observed: true,
      } as DashboardUsageResponse),
    );
    overviewMocks.inventoryQuery.mockReturnValue(successQuery(COUPON_FLEET));
  });

  it('mounts one shared upstream table between principal usage and latest requests', () => {
    render(page());
    const headings = screen
      .getAllByRole('heading')
      .map((heading) => heading.textContent);
    expect(headings.indexOf('Upstreams')).toBeGreaterThan(
      headings.indexOf('Principal usage'),
    );
    expect(headings.indexOf('Upstreams')).toBeLessThan(
      headings.indexOf('Latest requests (any time)'),
    );
    expect(screen.getAllByText('Coupons')).toHaveLength(1);
    const rows = screen.getAllByTestId('upstream-usage-row');
    expect(rows).toHaveLength(4);
    for (const id of ['claude-main', 'claude-duplicate']) {
      const link = screen.getByRole('link', { name: new RegExp(`^${id}`) });
      expect(link.getAttribute('href')).toBe(`/upstreams?selectedId=${id}`);
      // The all-provider Overview keeps its shared columns before any reading.
      expect(
        Array.from(
          link.querySelectorAll('[data-testid="usage-cell-status"]'),
          (cell) =>
            `${cell.getAttribute('data-window')}:${cell.getAttribute('data-state')}`,
        ),
      ).toEqual(['5h:no-reading', '7d:no-reading']);
      expect(
        within(link.closest('li')!).getByRole('button', {
          name: `Coupons for ${id}: 3`,
        }).textContent,
      ).toBe('×3');
    }
    expect(
      screen.getByRole('button', { name: 'Coupons for codex-main: 2' })
        .textContent,
    ).toBe('×2');
    expect(queries.usePrincipals).not.toHaveBeenCalled();
  });

  it.each(['pointer', 'Enter', 'Space'] as const)(
    'opens the shared current action with %s without navigating',
    async (interaction) => {
      const user = userEvent.setup();
      render(page());
      const button = screen.getByRole('button', {
        name: 'Coupons for claude-main: 3',
      });
      expect(button.closest('a')).toBeNull();
      if (interaction === 'pointer') await user.click(button);
      else {
        button.focus();
        await user.keyboard(interaction === 'Enter' ? '{Enter}' : ' ');
      }
      expect(await screen.findByRole('dialog')).toBeTruthy();
      expect(overviewMocks.navigate).not.toHaveBeenCalled();
    },
  );

  it('preserves ordinary upstream navigation and leaves unsupported API keys empty', () => {
    render(page());
    const link = screen.getByRole('link', { name: /^key/ });
    expect(link.getAttribute('href')).toBe('/upstreams?selectedId=key');
    expect(within(link.closest('li')!).queryByRole('button')).toBeNull();
  });

  it('keeps loading, unknown and known zero distinct', () => {
    overviewMocks.inventoryQuery.mockReturnValue(pendingQuery());
    const view = render(page());
    expect(
      screen.getByRole('button', { name: 'Coupons for claude-main: loading' }),
    ).toBeTruthy();
    overviewMocks.inventoryQuery.mockReturnValue(
      failedQuery(new Error('Unavailable')),
    );
    view.rerender(page());
    expect(
      screen.getByRole('button', { name: 'Coupons for claude-main: unknown' })
        .textContent,
    ).toBe('—');
    overviewMocks.inventoryQuery.mockReturnValue(
      successQuery({
        upstreams: [
          {
            ...COUPON_FLEET.upstreams[0]!,
            cedar_ember: {
              ...COUPON_FLEET.upstreams[0]!.cedar_ember!,
              grants: [],
            },
          },
        ],
      }),
    );
    view.rerender(page());
    expect(
      screen.getByRole('button', { name: 'Coupons for claude-main: 0' })
        .textContent,
    ).toBe('×0');
  });

  it('does not present placeholder data as current inventory', () => {
    const snapshot = successQuery(COUPON_FLEET);
    if (!snapshot.isSuccess) {
      throw new Error('Expected a successful inventory fixture');
    }
    overviewMocks.inventoryQuery.mockReturnValue({
      ...snapshot,
      isPlaceholderData: true,
    });
    render(page());
    expect(
      screen.getByRole('button', { name: 'Coupons for claude-main: unknown' }),
    ).toBeTruthy();
  });
});
