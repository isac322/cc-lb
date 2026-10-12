// The API-key detail pane wired end to end below the data hooks: key notices
// (K1–K3) and their single owner, notice order, the usage card's provenance
// next to Access, and the OAuth pane's surfaces. Every hook is mocked; the
// pane components render for real.

import {
  QueryClient,
  QueryClientProvider,
  type UseMutationResult,
  type UseQueryResult,
} from '@tanstack/react-query';
import {
  cleanup,
  fireEvent,
  render,
  screen,
  within,
} from '@testing-library/react';
import type { ReactNode } from 'react';
import { toast } from 'sonner';
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest';
import {
  ApiError,
  type ApiKeyAccess,
  type PreflightResponse,
  type UpstreamKeyUsageResponse,
} from '../lib/api';
import type { Upstream } from '../lib/queries';
import * as queries from '../lib/queries';
import {
  failedMutation,
  failedQuery,
  idleMutation,
  pendingMutation,
  pendingQuery,
  successQuery,
} from '../lib/test-utils/hook-results';
import { DetailView, Route } from './upstreams';

vi.mock('@tanstack/react-router', async () => {
  const actual = (await vi.importActual('@tanstack/react-router')) as Record<
    string,
    unknown
  >;
  return { ...actual, useNavigate: () => vi.fn() };
});

vi.mock('../lib/useRequestEventsFeed', () => ({
  useRequestEventsFeed: vi.fn(() => ({})),
}));

vi.mock('../components/ui/RequestEventsFeed', () => ({
  FeedLiveStatus: () => null,
  RequestEventsFeed: () => null,
}));

vi.mock('../components/upstreams/InlineNameEditor', () => ({
  InlineNameEditor: ({ upstream }: { upstream: { name: string } }) => (
    <span>{upstream.name}</span>
  ),
}));

vi.mock('../components/upstreams/warmup/WarmupCardMinimal', () => ({
  WarmupCardMinimal: () => null,
}));

vi.mock('../components/upstreams/LimitResetAction', () => ({
  LimitResetAction: () => null,
}));

vi.mock('../components/upstreams/connect/UpstreamConnectDialog', () => ({
  UpstreamConnectDialog: ({
    target,
  }: {
    target: { mode: string } | null;
  }): ReactNode =>
    target ? (
      <div data-testid="connect-dialog" data-mode={target.mode} />
    ) : null,
}));

vi.mock('../lib/queries', async () => {
  const actual = (await vi.importActual('../lib/queries')) as typeof queries;
  return {
    ...actual,
    useDeleteUpstream: vi.fn(),
    usePrincipalNameMap: vi.fn(),
    useRecheckApiKey: vi.fn(),
    useStatus: vi.fn(),
    useSubscriptionQuotaLatest: vi.fn(),
    useSubscriptionQuotaSeries: vi.fn(),
    useTriggerSubscriptionMetadataRefresh: vi.fn(),
    useUpdateUpstream: vi.fn(),
    useUpdateUpstreamWarmupSettings: vi.fn(),
    useUpstreamKeyUsage: vi.fn(),
    useUpstreamOAuthStatus: vi.fn(),
    useUpstreamSubscriptionMetadata: vi.fn(),
    useUpstreamWorkspaces: vi.fn(),
    useUpstreams: vi.fn(),
  };
});

const NOW = Math.floor(Date.now() / 1000);

const ORG_OK: ApiKeyAccess = {
  verified: true,
  checked_at_unix_secs: NOW - 3_600,
  key_class: 'org_scoped',
  inference: 'ok',
  workspace: { mode: 'selected', id: 'wrkspc_01Prod00aa', name: 'Production' },
  workspace_list: 'ok',
  usage_report: 'ok',
  key_type: 'service_account',
  last_error: null,
};
const REJECTED: ApiKeyAccess = {
  ...ORG_OK,
  inference: 'denied',
  usage_report: 'not_applicable',
  last_error: 'Anthropic answered 401 - invalid x-api-key.',
};
const CHECK_FAILED: ApiKeyAccess = {
  ...ORG_OK,
  verified: false,
  key_class: 'unknown',
  key_type: null,
  inference: 'unverified',
  workspace_list: 'not_applicable',
  usage_report: 'not_applicable',
  last_error: "Couldn't reach your endpoint — the key wasn't checked.",
};

const BASE: Upstream = {
  id: 'upstream-key',
  name: 'Key upstream',
  kind: 'anthropic_api_key',
  enabled: true,
  spec_revision: 3,
  base_url: null,
  warmup_enabled: false,
  warmup_dialect_plugin: null,
  status: {
    last_apply_error: null,
    last_apply_at_unix_secs: null,
    last_warmup_at_unix_secs: null,
  },
};

function keyUpstream(
  access: ApiKeyAccess | null,
  overrides: Partial<Upstream> = {},
): Upstream {
  return access === null
    ? { ...BASE, ...overrides }
    : { ...BASE, api_key_access: access, ...overrides };
}

const OAUTH: Upstream = {
  ...BASE,
  id: 'upstream-oauth',
  name: 'OAuth upstream',
  kind: 'anthropic_oauth',
};

function usage(
  overrides: Partial<UpstreamKeyUsageResponse> = {},
): UpstreamKeyUsageResponse {
  return {
    range: '24h',
    step: 'hour',
    group_by: 'model',
    window_start_unix_secs: NOW - 86_400,
    window_end_unix_secs: NOW,
    series: [],
    observed: true,
    source: 'real',
    attribution: 'api_key',
    attribution_workspace_id: null,
    stale: false,
    last_reported_unix_secs: NOW,
    totals: { cost_usd_micros: 0, cost_is_estimate: true },
    fix: null,
    month: {
      start_unix_secs: Date.UTC(2026, 9, 1) / 1000,
      end_unix_secs: Date.UTC(2026, 10, 1) / 1000,
      tokens: 1_200,
      cost_usd_micros: 40_000,
      billing_stop: null,
    },
    ...overrides,
  };
}

// The hook's result contract; the mock below must stay assignable to it.
type RecheckMutation = UseMutationResult<PreflightResponse, Error, string>;

const recheckMutate = vi.fn<RecheckMutation['mutate']>();

// The pane's Re-check state; Access's own mutation stays idle.
type RecheckState = 'idle' | 'pending' | { error: Error };

function recheckResult(state: RecheckState): RecheckMutation {
  const fns = { mutate: recheckMutate };
  if (state === 'idle') return idleMutation(fns);
  if (state === 'pending') return pendingMutation('upstream-key', fns);
  return failedMutation(state.error, 'upstream-key', fns);
}

function mockRecheck(paneState: RecheckState = 'idle') {
  // DetailView calls the hook first, then Access in the same render pass;
  // each owns its own mutation, so only the pane's carries `paneState`.
  let call = 0;
  vi.mocked(queries.useRecheckApiKey).mockImplementation(() => {
    const pane = call % 2 === 0;
    call += 1;
    return recheckResult(pane ? paneState : 'idle');
  });
}

const queryClient = new QueryClient({
  defaultOptions: { queries: { retry: false } },
});

function renderPane(
  upstream: Upstream,
  keyUsage: UseQueryResult<UpstreamKeyUsageResponse> = successQuery(usage()),
) {
  vi.mocked(queries.useUpstreams).mockReturnValue(
    successQuery({ upstreams: [upstream] }),
  );
  vi.mocked(queries.useUpstreamKeyUsage).mockReturnValue(keyUsage);
  return render(
    <QueryClientProvider client={queryClient}>
      <DetailView upstream={upstream} onBack={vi.fn()} onConnect={vi.fn()} />
    </QueryClientProvider>,
  );
}

function notices(): HTMLElement[] {
  return [...document.querySelectorAll<HTMLElement>('[data-tone]')];
}

beforeEach(() => {
  vi.clearAllMocks();
  queryClient.clear();
  Object.assign(Route, { useSearch: () => ({}) });
  Element.prototype.scrollIntoView = vi.fn();
  mockRecheck();
  vi.mocked(queries.useUpdateUpstream).mockReturnValue(idleMutation());
  vi.mocked(queries.useUpstreamWorkspaces).mockReturnValue(pendingQuery());
  vi.mocked(queries.useDeleteUpstream).mockReturnValue(idleMutation());
  vi.mocked(queries.useUpdateUpstreamWarmupSettings).mockReturnValue(
    idleMutation(),
  );
  vi.mocked(queries.useTriggerSubscriptionMetadataRefresh).mockReturnValue(
    idleMutation(),
  );
  vi.mocked(queries.usePrincipalNameMap).mockReturnValue(new Map());
  vi.mocked(queries.useStatus).mockReturnValue(pendingQuery());
  vi.mocked(queries.useSubscriptionQuotaLatest).mockReturnValue(pendingQuery());
  vi.mocked(queries.useSubscriptionQuotaSeries).mockReturnValue(pendingQuery());
  vi.mocked(queries.useUpstreamSubscriptionMetadata).mockReturnValue(
    pendingQuery(),
  );
  vi.mocked(queries.useUpstreamOAuthStatus).mockReturnValue(pendingQuery());
});

afterEach(cleanup);

describe('UI-K-01 K1 rejected key', () => {
  test('danger notice owns Replace key; Access keeps the badge and Re-check', () => {
    renderPane(
      keyUpstream(REJECTED),
      successQuery(usage({ source: 'estimated', fix: null })),
    );
    const alert = screen
      .getAllByRole('alert')
      .find((el) => el.textContent?.includes('Anthropic rejected this key'));
    expect(alert?.textContent).toContain(
      'Anthropic answered 401 - invalid x-api-key. Replace the key → requests through this upstream succeed again.',
    );
    // One Replace key in the whole pane: the notice's.
    const replace = screen.getAllByRole('button', { name: 'Replace key' });
    expect(replace).toHaveLength(1);
    expect(alert?.contains(replace[0] ?? null)).toBe(true);

    const access = screen.getByTestId('api-key-access');
    expect(within(access).getByRole('status').textContent).toBe('Key rejected');
    expect(
      within(access).getByRole('button', { name: 'Re-check' }),
    ).toBeDefined();
    expect(within(access).getByTestId('upstream-endpoint-row')).toBeDefined();
    expect(within(access).queryByTestId('api-key-usage-mode')).toBeNull();
    expect(access.textContent).toContain(
      'Workspace and usage reports appear after a successful check.',
    );

    const card = screen.getByTestId('api-usage-card');
    expect(
      within(card).queryByRole('button', { name: 'Get reported usage' }),
    ).toBeNull();

    fireEvent.click(replace[0] as HTMLElement);
    expect(screen.getByTestId('connect-dialog').dataset.mode).toBe('replace');
  });
});

describe('UI-K-02 K2 never checked', () => {
  test('a warning (not an alert) with the only Re-check in the pane', () => {
    renderPane(keyUpstream(null), successQuery(usage({ source: 'estimated' })));
    const notice = notices().find((el) =>
      el.textContent?.includes('Key not checked yet'),
    );
    expect(notice?.getAttribute('role')).toBe('status');
    expect(notice?.textContent).toContain(
      'Re-check → see what this key can do at Anthropic.',
    );
    const recheck = screen.getAllByRole('button', { name: 'Re-check' });
    expect(recheck).toHaveLength(1);
    expect(notice?.contains(recheck[0] ?? null)).toBe(true);
    fireEvent.click(recheck[0] as HTMLElement);
    expect(recheckMutate).toHaveBeenCalledWith('upstream-key');

    const access = screen.getByTestId('api-key-access');
    expect(access.textContent).toContain('Never checked');
    expect(within(access).getByRole('status').textContent).toBe(
      'Key not verified',
    );
  });

  test('pending: Checking…', () => {
    mockRecheck('pending');
    renderPane(keyUpstream(null), successQuery(usage({ source: 'estimated' })));
    expect(screen.getByRole('button', { name: 'Checking…' })).toBeDefined();
  });
});

describe('UI-K-03 K3 last check failed', () => {
  test('endpoint-aware last_error, and a failed Re-check alerts inside the notice', () => {
    const toastError = vi.spyOn(toast, 'error');
    mockRecheck({
      error: new ApiError(504, 'probe_unreachable', null, 'timed out'),
    });
    renderPane(
      keyUpstream(CHECK_FAILED, { base_url: 'http://127.0.0.1:9' }),
      successQuery(
        usage({ source: 'estimated', fix: { code: 'non_anthropic_base_url' } }),
      ),
    );
    const notice = notices().find((el) =>
      el.textContent?.includes('Last check failed'),
    );
    expect(notice?.getAttribute('role')).toBe('status');
    expect(notice?.textContent).toContain(
      "Couldn't reach your endpoint — the key wasn't checked. Re-check → confirm the key works.",
    );
    const alerts = screen.getAllByRole('alert');
    expect(alerts).toHaveLength(1);
    expect(alerts[0]?.textContent).toBe('Re-check failed: timed out');
    expect(notice?.contains(alerts[0] ?? null)).toBe(true);
    expect(toastError).not.toHaveBeenCalled();
  });
});

describe('UI-K-04 one owner and order', () => {
  test('key notice, then apply error, then the one Stopped banner', () => {
    // The apply error is the runtime status's, as the header reads it.
    vi.mocked(queries.useStatus).mockReturnValue(
      successQuery({
        version: '0.0.0',
        git_sha: 'test',
        uptime_secs: 60,
        build: { rust_version: '1.0.0', profile: 'test', target: 'test' },
        generation: 1,
        upstreams: [
          {
            id: 'upstream-key',
            name: 'Key upstream',
            status: 'active',
            last_apply_at_unix_secs: NOW,
            last_apply_error: 'signer build failed',
          },
        ],
      }),
    );
    renderPane(
      keyUpstream(REJECTED),
      successQuery(usage({ stale: true, fix: { code: 'usage_poll_failed' } })),
    );
    const titles = notices().map((el) =>
      [
        'Anthropic rejected this key',
        'Last apply failed',
        'Usage reports stopped',
      ].find((t) => el.textContent?.includes(t)),
    );
    expect(titles.filter(Boolean)).toEqual([
      'Anthropic rejected this key',
      'Last apply failed',
      'Usage reports stopped',
    ]);
  });

  test('the header adds no second "Key not verified"', () => {
    renderPane(
      keyUpstream(CHECK_FAILED),
      successQuery(usage({ source: 'estimated' })),
    );
    expect(screen.getAllByText('Key not verified')).toHaveLength(1);
    expect(
      within(screen.getByTestId('api-key-access')).getByText(
        'Key not verified',
      ),
    ).toBeDefined();
  });
});

describe('UI-U-11 Stopped and recovery', () => {
  test('401/403: banner with the role fix, Last report card, Access Stopped', () => {
    const view = renderPane(
      keyUpstream(ORG_OK),
      successQuery(usage({ stale: true, fix: { code: 'usage_poll_failed' } })),
    );
    const banner = notices().find((el) =>
      el.textContent?.includes('Usage reports stopped'),
    );
    expect(banner?.textContent).toContain(
      "Raise the linked account's org role → get reported usage.",
    );
    const card = screen.getByTestId('api-usage-card');
    expect(within(card).getByText('Last report')).toBeDefined();
    expect(card.textContent).toContain(
      "Showing the last reported figures — all of this key's usage.",
    );
    expect(screen.getByTestId('api-key-usage-mode').textContent).toContain(
      'Stopped',
    );

    // Re-check restored the role; the immediate poll answered.
    vi.mocked(queries.useUpstreamKeyUsage).mockReturnValue(
      successQuery(usage()),
    );
    view.rerender(
      <QueryClientProvider client={queryClient}>
        <DetailView
          upstream={keyUpstream(ORG_OK)}
          onBack={vi.fn()}
          onConnect={vi.fn()}
        />
      </QueryClientProvider>,
    );
    expect(screen.queryByText('Usage reports stopped')).toBeNull();
    expect(screen.getByTestId('api-key-usage-mode').textContent).toContain(
      'Reported',
    );
  });
});

describe('UI-U-14 a transient outage never shows a role or key fix', () => {
  test('stale without a fix: no banner, no fix, Access still Reported', () => {
    renderPane(
      keyUpstream(ORG_OK),
      successQuery(usage({ stale: true, fix: null })),
    );
    expect(screen.queryByText('Usage reports stopped')).toBeNull();
    const pane = document.body.textContent ?? '';
    expect(pane).not.toContain('org role');
    expect(pane).not.toContain('All workspaces key →');
    const card = screen.getByTestId('api-usage-card');
    expect(within(card).getByText('Last report')).toBeDefined();
    expect(card.textContent).toContain("Anthropic hasn't answered since");
    const mode = screen.getByTestId('api-key-usage-mode');
    expect(mode.textContent).toContain('Reported');
    expect(mode.textContent).not.toContain('Stopped');
  });
});

describe('UI-U-15 first poll pending', () => {
  test('Access says Reported; the card says figures arrive within 5 min', () => {
    renderPane(
      keyUpstream(ORG_OK),
      successQuery(usage({ source: 'estimated', fix: null })),
    );
    expect(screen.getByTestId('api-key-usage-mode').textContent).toContain(
      'Reported',
    );
    const card = screen.getByTestId('api-usage-card');
    expect(card.textContent).toContain('Reported figures arrive within 5 min');
    expect(
      within(card).queryByRole('button', { name: 'Get reported usage' }),
    ).toBeNull();
    expect(within(card).queryByText('Estimated')).toBeNull();
  });
});

describe('UI-U-03 Get reported usage pointer', () => {
  test('scrolls Access → Usage data into view and focuses it', () => {
    renderPane(
      keyUpstream({ ...ORG_OK, usage_report: 'denied' }),
      successQuery(
        usage({ source: 'estimated', fix: { code: 'role_insufficient' } }),
      ),
    );
    const card = screen.getByTestId('api-usage-card');
    fireEvent.click(
      within(card).getByRole('button', { name: 'Get reported usage' }),
    );
    const row = screen.getByTestId('api-key-usage-mode');
    expect(document.activeElement).toBe(row);
    expect(row.scrollIntoView).toHaveBeenCalledWith({ block: 'center' });
    // The fix sentence appears once: in Access, never in the card.
    expect(card.textContent).not.toContain('org role');
    expect(row.textContent).toContain("Raise the linked account's org role");
  });
});

describe('UI-U-09 one surface for a usage load failure', () => {
  test('exactly one alert, no zero figures', () => {
    renderPane(
      keyUpstream(ORG_OK),
      failedQuery(new ApiError(500, null, null, 'Request failed (500)')),
    );
    const card = screen.getByTestId('api-usage-card');
    const alerts = within(card).getAllByRole('alert');
    expect(alerts).toHaveLength(1);
    expect(alerts[0]?.textContent).toBe(
      "Couldn't load this month: Request failed (500)",
    );
    for (const label of ['Tokens used', 'Credits used']) {
      const cell = card.querySelector(`[data-cell="${label}"]`);
      expect(cell?.textContent).not.toMatch(/\d/);
    }
  });
});

describe('UI-AC-05 one inline editor at a time', () => {
  test('opening Change workspace closes the Endpoint editor', () => {
    vi.mocked(queries.useUpstreamWorkspaces).mockReturnValue(
      successQuery({ status: 'denied' }),
    );
    renderPane(keyUpstream(ORG_OK));
    fireEvent.click(screen.getByRole('button', { name: 'Change endpoint' }));
    expect(screen.getByLabelText('Base URL')).toBeDefined();
    fireEvent.click(screen.getByRole('button', { name: 'Change workspace' }));
    expect(screen.queryByLabelText('Base URL')).toBeNull();
    expect(screen.getByLabelText('Workspace ID')).toBeDefined();
    expect(screen.getAllByRole('button', { name: 'Save' })).toHaveLength(1);
  });
});

describe('OAU-02 / UI-O-01 the OAuth pane', () => {
  test('no API usage card, credit strip or Access section', () => {
    renderPane(OAUTH);
    expect(screen.queryByTestId('api-usage-card')).toBeNull();
    expect(screen.queryByTestId('api-key-credits')).toBeNull();
    expect(screen.queryByTestId('api-key-access')).toBeNull();
    expect(screen.getByText('Quota')).toBeDefined();
  });

  test('Credential ends with a skeleton Endpoint row while loading', () => {
    vi.mocked(queries.useUpstreamOAuthStatus).mockReturnValue(pendingQuery());
    renderPane(OAUTH);
    expect(screen.queryByTestId('upstream-endpoint-row')).toBeNull();
    expect(screen.getByText('Endpoint')).toBeDefined();
  });

  test('default and custom endpoints; no orphan "Use a custom endpoint"', () => {
    vi.mocked(queries.useUpstreamOAuthStatus).mockReturnValue(
      failedQuery(new ApiError(500, null, null, 'Request failed (500)')),
    );
    const view = renderPane(OAUTH);
    let row = screen.getByTestId('upstream-endpoint-row');
    expect(row.textContent).toContain('Anthropic (default)');
    expect(
      within(row).getByRole('button', { name: 'Change endpoint' }),
    ).toBeDefined();
    view.unmount();

    renderPane({ ...OAUTH, base_url: 'https://proxy.example.com' });
    row = screen.getByTestId('upstream-endpoint-row');
    expect(row.textContent).toContain('https://proxy.example.com');
    expect(row.textContent).toContain('Requests go here instead of Anthropic.');
    expect(screen.queryByText('Use a custom endpoint')).toBeNull();
  });
});
