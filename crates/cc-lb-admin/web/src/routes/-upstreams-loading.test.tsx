import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { cleanup, render, screen, within } from '@testing-library/react';
import type React from 'react';
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest';
import type { Upstream } from '../lib/queries';
import * as queries from '../lib/queries';
import { Route } from './upstreams';

let searchState: { selectedId?: string } = {};
const navigateMock = vi.fn();

vi.mock('@tanstack/react-router', async () => {
  const actual = (await vi.importActual('@tanstack/react-router')) as Record<
    string,
    unknown
  >;
  return {
    ...actual,
    useNavigate: () => navigateMock,
  };
});

vi.mock('../lib/queries', async () => {
  const actual = (await vi.importActual('../lib/queries')) as typeof queries;
  return {
    ...actual,
    useCompleteOauthDraft: vi.fn(),
    useCreateFromOauthDraft: vi.fn(),
    useCreateUpstream: vi.fn(),
    useDeleteUpstream: vi.fn(),
    useOAuthComplete: vi.fn(),
    useOAuthStart: vi.fn(),
    usePrincipalNameMap: vi.fn(),
    useRecentEvents: vi.fn(),
    useStartOauthDraft: vi.fn(),
    useStatus: vi.fn(),
    useSubscriptionQuotaAnalysis: vi.fn(),
    useSubscriptionQuotaLatest: vi.fn(),
    useSubscriptionQuotaSeries: vi.fn(),
    useTriggerSubscriptionMetadataRefresh: vi.fn(),
    useUpdateUpstreamWarmupSettings: vi.fn(),
    useUpstreamNameMap: vi.fn(),
    useUpstreamOAuthStatus: vi.fn(),
    useUpstreamSubscriptionMetadata: vi.fn(),
    useUpstreams: vi.fn(),
    useUsage: vi.fn(),
  };
});

vi.mock('../components/upstreams/InlineNameEditor', () => ({
  InlineNameEditor: ({ upstream }: { upstream: { name: string } }) => (
    <span>{upstream.name}</span>
  ),
}));

vi.mock('../components/upstreams/warmup/WarmupCardMinimal', () => ({
  WarmupCardMinimal: () => (
    <div data-testid="warmup-card-stub" className="min-h-28" />
  ),
}));

const queryClient = new QueryClient({
  defaultOptions: { queries: { retry: false } },
});
const Component = Route.options.component as React.ComponentType;

const upstream: Upstream = {
  id: 'upstream-1',
  name: 'OAuth Primary',
  kind: 'anthropic_oauth',
  enabled: true,
  spec_revision: 1,
  base_url: null,
  api_key_env: null,
  warmup_enabled: true,
  warmup_dialect_plugin: null,
  status: {
    last_apply_error: null,
    last_apply_at_unix_secs: null,
    last_warmup_at_unix_secs: null,
  },
};

const apiKeyUpstream: Upstream = {
  ...upstream,
  id: 'upstream-2',
  name: 'API Key Backup',
  kind: 'anthropic_api_key',
  warmup_enabled: false,
};

function mutationResult() {
  return {
    mutate: vi.fn(),
    isPending: false,
    reset: vi.fn(),
  };
}

function renderRoute() {
  Object.assign(Route, { useSearch: () => searchState });
  return render(
    <QueryClientProvider client={queryClient}>
      <Component />
    </QueryClientProvider>,
  );
}

beforeEach(() => {
  vi.clearAllMocks();
  queryClient.clear();
  searchState = { selectedId: upstream.id };

  vi.mocked(queries.useUpstreams).mockReturnValue({
    data: { upstreams: [upstream, apiKeyUpstream] },
    isLoading: false,
    isPending: false,
    isPlaceholderData: false,
  } as never);
  vi.mocked(queries.useSubscriptionQuotaLatest).mockReturnValue({
    data: undefined,
    isLoading: true,
    isPending: true,
    isPlaceholderData: false,
  } as never);
  vi.mocked(queries.useSubscriptionQuotaSeries).mockReturnValue({
    data: undefined,
    isLoading: true,
    isPending: true,
    isPlaceholderData: false,
  } as never);
  vi.mocked(queries.useSubscriptionQuotaAnalysis).mockReturnValue({
    data: undefined,
    isLoading: true,
    isPending: true,
    isPlaceholderData: false,
  } as never);
  vi.mocked(queries.useUsage).mockReturnValue({
    data: undefined,
    isLoading: true,
    isPending: true,
    isPlaceholderData: false,
  } as never);
  vi.mocked(queries.useStatus).mockReturnValue({
    data: undefined,
    isLoading: true,
    isPending: true,
    isPlaceholderData: false,
  } as never);
  vi.mocked(queries.useUpstreamSubscriptionMetadata).mockReturnValue({
    data: undefined,
    isLoading: true,
    isPending: true,
    isPlaceholderData: false,
  } as never);
  vi.mocked(queries.useUpstreamOAuthStatus).mockReturnValue({
    data: undefined,
    isLoading: true,
    isPending: true,
    isPlaceholderData: false,
  } as never);
  vi.mocked(queries.useRecentEvents).mockReturnValue({
    data: undefined,
    isLoading: true,
    isPending: true,
    isPlaceholderData: false,
  } as never);
  vi.mocked(queries.usePrincipalNameMap).mockReturnValue(new Map());
  vi.mocked(queries.useUpstreamNameMap).mockReturnValue(
    new Map([
      [upstream.id, upstream.name],
      [apiKeyUpstream.id, apiKeyUpstream.name],
    ]),
  );

  vi.mocked(queries.useUpdateUpstreamWarmupSettings).mockReturnValue(
    mutationResult() as never,
  );
  vi.mocked(queries.useDeleteUpstream).mockReturnValue(
    mutationResult() as never,
  );
  vi.mocked(queries.useOAuthStart).mockReturnValue(mutationResult() as never);
  vi.mocked(queries.useOAuthComplete).mockReturnValue(
    mutationResult() as never,
  );
  vi.mocked(queries.useTriggerSubscriptionMetadataRefresh).mockReturnValue(
    mutationResult() as never,
  );
  vi.mocked(queries.useCreateUpstream).mockReturnValue(
    mutationResult() as never,
  );
  vi.mocked(queries.useStartOauthDraft).mockReturnValue(
    mutationResult() as never,
  );
  vi.mocked(queries.useCompleteOauthDraft).mockReturnValue(
    mutationResult() as never,
  );
  vi.mocked(queries.useCreateFromOauthDraft).mockReturnValue(
    mutationResult() as never,
  );
});

afterEach(cleanup);

describe('/upstreams cold-load geometry', () => {
  test('renders a structured detail shell before desktop auto-selection', () => {
    searchState = {};
    vi.mocked(queries.useUpstreams).mockReturnValue({
      data: undefined,
      isLoading: true,
      isPending: true,
      isPlaceholderData: false,
    } as never);

    renderRoute();

    const shell = screen.getByTestId('upstream-detail-loading-shell');
    expect(shell.className).toContain('contents');
    expect(
      within(shell).getByTestId('upstream-detail-loading-metadata').className,
    ).toContain('min-h-9');
    expect(
      within(shell).getByTestId('quota-history-legend-slot').className,
    ).toContain('min-h-5');
    expect(
      within(shell).getByTestId('quota-snapshot-grid').className,
    ).toContain('min-h-[203px]');
    const rangeControl = within(shell).getByTestId(
      'quota-history-range-control',
    );
    expect(rangeControl.className).toContain('p-0.5');
    expect(rangeControl.children).toHaveLength(4);
    for (const rangeItem of rangeControl.children) {
      expect(rangeItem.className).toContain('h-7');
    }
    expect(within(shell).queryByText('Timestamp')).toBeNull();
    expect(
      within(shell).queryByTestId('recent-requests-table-slot'),
    ).toBeNull();
    expect(within(shell).queryByTestId('warmup-card')).toBeNull();
    expect(within(shell).queryByTestId('oauth-status-card-body')).toBeNull();
    expect(screen.getAllByTestId('upstream-list-loading-row')).toHaveLength(3);
    expect(screen.queryByText('Select an upstream')).toBeNull();
  });

  test('reserves metadata, chart legend, snapshots, OAuth, and requests while queries are pending', () => {
    renderRoute();

    const metadata = screen.getByTestId('upstream-metadata-strip');
    expect(metadata.className).toContain('min-h-9');
    const metadataLoading = within(metadata).getByTestId(
      'upstream-metadata-loading',
    );
    expect(metadataLoading.querySelectorAll('.skeleton')).toHaveLength(5);

    const legend = screen.getByTestId('quota-history-legend-slot');
    expect(legend.className).toContain('min-h-5');
    expect(legend.querySelectorAll('.skeleton')).toHaveLength(2);

    const snapshotGrid = screen.getByTestId('quota-snapshot-grid');
    expect(snapshotGrid.className).toContain('min-h-[203px]');
    expect(
      within(snapshotGrid).getAllByTestId('quota-snapshot-skeleton-card'),
    ).toHaveLength(3);
    expect(screen.queryByText(/No subscription quota data/)).toBeNull();

    const oauthBody = screen.getByTestId('oauth-status-card-body');
    expect(oauthBody.className).toContain('min-h-28');
    const oauthGrid = within(oauthBody).getByTestId(
      'oauth-status-loading-grid',
    );
    expect(oauthGrid.className).toContain('min-h-20');
    expect(oauthGrid.className).toContain('space-y-3');

    const requestSlot = screen.getByTestId('recent-requests-table-slot');
    expect(requestSlot.className).toContain('min-h-48');
    expect(within(requestSlot).getByText('Timestamp')).toBeDefined();
    expect(requestSlot.querySelectorAll('tbody tr')).toHaveLength(5);
    expect(screen.queryByText(/Loading/)).toBeNull();
    expect(screen.queryByText('—%')).toBeNull();
    const oauthListRow = screen.getByRole('button', {
      name: /OAuth Primary/,
    });
    expect(
      oauthListRow.querySelectorAll('.skeleton').length,
    ).toBeGreaterThanOrEqual(5);
    const apiKeyListRow = screen.getByRole('button', {
      name: /API Key Backup/,
    });
    expect(
      apiKeyListRow.querySelectorAll('.skeleton').length,
    ).toBeGreaterThanOrEqual(2);
  });

  test('retains the reserved slots after empty and loaded queries resolve', () => {
    vi.mocked(queries.useSubscriptionQuotaLatest).mockReturnValue({
      data: {
        now_unix_secs: 1_800_000_000,
        max_staleness_secs: 300,
        upstreams: [],
      },
      isLoading: false,
      isPending: false,
      isPlaceholderData: false,
    } as never);
    vi.mocked(queries.useSubscriptionQuotaSeries).mockReturnValue({
      data: { series: [] },
      isLoading: false,
      isPending: false,
      isPlaceholderData: false,
    } as never);
    vi.mocked(queries.useSubscriptionQuotaAnalysis).mockReturnValue({
      data: { upstreams: [] },
      isLoading: false,
      isPending: false,
      isPlaceholderData: false,
    } as never);
    vi.mocked(queries.useUpstreamSubscriptionMetadata).mockReturnValue({
      data: {
        upstream_id: upstream.id,
        subscription_metadata: null,
        organization_metadata: null,
      },
      isLoading: false,
      isPending: false,
      isPlaceholderData: false,
    } as never);
    vi.mocked(queries.useUpstreamOAuthStatus).mockReturnValue({
      data: {
        upstream_id: upstream.id,
        kind: upstream.kind,
        has_credentials: true,
        status: 'valid',
        expires_at_unix_secs: 2_000_000_000,
        refresh_token_present: true,
        scopes: ['user:inference'],
      },
      isLoading: false,
      isPending: false,
      isPlaceholderData: false,
    } as never);
    vi.mocked(queries.useRecentEvents).mockReturnValue({
      data: { events: [] },
      isLoading: false,
      isPending: false,
      isPlaceholderData: false,
    } as never);

    renderRoute();

    expect(screen.getByTestId('upstream-metadata-strip').className).toContain(
      'min-h-9',
    );
    expect(screen.getByTestId('quota-history-legend-slot').className).toContain(
      'min-h-5',
    );
    expect(screen.getByTestId('quota-snapshot-grid').className).toContain(
      'min-h-[203px]',
    );
    const rangeControl = screen.getByTestId('quota-history-range-control');
    expect(rangeControl.className).toContain('p-0.5');
    expect(rangeControl.querySelectorAll('button')).toHaveLength(4);
    for (const rangeItem of rangeControl.querySelectorAll('button')) {
      expect(rangeItem.className).toContain('h-7');
    }
    expect(screen.getByTestId('oauth-status-card-body').className).toContain(
      'min-h-28',
    );
    const loadedOauthGrid = screen.getByTestId('oauth-status-loaded-grid');
    expect(loadedOauthGrid.className).toContain('min-h-20');
    expect(loadedOauthGrid.className).toContain('space-y-3');
    expect(
      screen.getByTestId('recent-requests-table-slot').className,
    ).toContain('min-h-48');
  });
});
