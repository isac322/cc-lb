import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import {
  cleanup,
  fireEvent,
  render,
  screen,
  within,
} from '@testing-library/react';
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

const secondOauthUpstream: Upstream = {
  ...upstream,
  id: 'upstream-3',
  name: 'OAuth Secondary',
  spec_revision: 2,
};

const NOW_UNIX_SECS = Date.UTC(2026, 5, 18, 0, 0, 1) / 1000;

function queryResult(data: unknown, overrides: Record<string, unknown> = {}) {
  return {
    data,
    isLoading: false,
    isPending: false,
    isPlaceholderData: false,
    ...overrides,
  } as never;
}

function quotaSnapshot(window: '5h' | '7d', utilization: number) {
  return {
    window,
    state: 'fresh',
    source: 'api',
    utilization,
    status: null,
    resets_at_unix_secs: NOW_UNIX_SECS + 3_600,
    surpassed_threshold: false,
    representative_claim: null,
    disabled_reason: null,
    extra_usage_enabled: null,
    extra_usage_monthly_limit: null,
    extra_usage_used_credits: null,
    observed_at_unix_millis: NOW_UNIX_SECS * 1_000,
    age_secs: 0,
  };
}

function quotaLatestData(
  target: Upstream,
  utilization: number,
  additionalUpstreams: unknown[] = [],
) {
  return {
    now_unix_secs: NOW_UNIX_SECS,
    max_staleness_secs: 300,
    upstreams: [
      {
        upstream_id: target.id,
        upstream_name: target.name,
        windows: [
          quotaSnapshot('5h', utilization),
          quotaSnapshot('7d', utilization / 2),
        ],
      },
      ...additionalUpstreams,
    ],
  };
}

function quotaSeriesData(
  target: Upstream,
  utilization: number,
  rangeSecs = 604_800,
) {
  const since = NOW_UNIX_SECS - rangeSecs;
  return {
    since_unix_secs: since,
    until_unix_secs: NOW_UNIX_SECS,
    bucket_secs: 1_800,
    source: 'merged',
    series: [
      {
        upstream_id: target.id,
        upstream_name: target.name,
        window: '5h',
        buckets: [
          {
            bucket_start_unix_secs: since + 1_800,
            utilization_last: utilization,
          },
        ],
        markers: [],
      },
      {
        upstream_id: target.id,
        upstream_name: target.name,
        window: '7d',
        buckets: [
          {
            bucket_start_unix_secs: since + 1_800,
            utilization_last: utilization / 2,
          },
        ],
        markers: [],
      },
    ],
  };
}

function quotaAnalysisData(target: Upstream, utilization: number) {
  const burn = {
    utilization_per_second: 0.0001,
    utilization_per_hour: 0.36,
    eta_to_limit_secs: 3_600,
    resets_before_limit: false,
    confidence: 'high',
    sample_count: 2,
    reason: null,
  };
  return {
    since_unix_secs: NOW_UNIX_SECS - 604_800,
    until_unix_secs: NOW_UNIX_SECS,
    now_unix_secs: NOW_UNIX_SECS,
    max_staleness_secs: 300,
    upstreams: [
      {
        upstream_id: target.id,
        upstream_name: target.name,
        windows: [
          {
            window: '5h',
            current_utilization: utilization,
            resets_at_unix_secs: NOW_UNIX_SECS + 3_600,
            data_state: 'fresh',
            actual_account_burn: burn,
            proxy_projected_burn: {
              proxy_tokens_per_second: 10,
              proxy_tokens_per_hour: 36_000,
              effective_limit_tokens_estimate: 100_000,
              utilization_per_hour: 0.36,
              eta_to_limit_secs: 3_600,
              resets_before_limit: false,
              confidence: 'high',
              sample_count: 2,
              reason: null,
            },
            deficit: null,
            caveats: [],
          },
        ],
      },
    ],
  };
}

function metadataData(target: Upstream, plan: string, accountEmail: string) {
  return {
    upstream_id: target.id,
    subscription_metadata: {
      organization_role: 'member',
      workspace_role: 'user',
    },
    organization_metadata: {
      organization_type: plan,
      rate_limit_tier: 'tier-1',
      account_display_name: target.name,
      account_email: accountEmail,
      organization_name: `${target.name} Org`,
      billing_type: 'invoice',
      has_extra_usage_enabled: false,
    },
  };
}

function oauthStatusData(
  target: Upstream,
  scope: string,
  overrides: Record<string, unknown> = {},
) {
  return {
    upstream_id: target.id,
    kind: target.kind,
    has_credentials: true,
    status: 'valid',
    expires_at_unix_secs: 2_000_000_000,
    refresh_token_present: true,
    refresh_token_expires_at_unix_secs: 2_100_000_000,
    mode: 'refreshing',
    can_refresh: true,
    scopes: [scope],
    ...overrides,
  };
}

function usageData(key: string, bucketTokens: number, costMicros: number) {
  return {
    range: '24h',
    step: 'hour',
    group_by: 'model',
    series: [
      {
        key,
        buckets: [
          {
            bucket_start_unix_secs: NOW_UNIX_SECS - 3_600,
            request_count: 1,
            input_tokens: bucketTokens,
            output_tokens: 0,
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 0,
            error_count: 0,
            virtual_cost_micros: costMicros,
            latency_ms_sum: 100,
            latency_count: 1,
            proxy_setup_ms_sum: 0,
            proxy_setup_ms_count: 0,
            shape_ms_sum: 0,
            shape_ms_count: 0,
            sign_ms_sum: 0,
            sign_ms_count: 0,
            upstream_ttfb_ms_sum: 0,
            upstream_ttfb_ms_count: 0,
            upstream_body_ms_sum: 0,
            upstream_body_ms_count: 0,
          },
        ],
      },
    ],
  };
}

function recentData(target: Upstream, model: string) {
  return {
    events: [
      {
        event_id: `event-${model}`,
        request_id: `request-${model}`,
        ts: NOW_UNIX_SECS - 30,
        ts_ms: (NOW_UNIX_SECS - 30) * 1_000,
        upstream: target.id,
        event_kind: 'messages',
        model,
        status: 200,
        duration_ms: 100,
      },
    ],
  };
}

function mutationResult() {
  return {
    mutate: vi.fn(),
    isPending: false,
    reset: vi.fn(),
  };
}

function routeElement() {
  Object.assign(Route, { useSearch: () => searchState });
  return (
    <QueryClientProvider client={queryClient}>
      <Component />
    </QueryClientProvider>
  );
}

function renderRoute() {
  return render(routeElement());
}

beforeEach(() => {
  vi.useFakeTimers({
    toFake: ['Date'],
    now: new Date('2026-06-18T00:00:01.000Z'),
  });
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

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
  vi.useRealTimers();
});

describe('/upstreams quota request cadence', () => {
  test('keeps stable series and analysis range identities across wall-clock ticks', () => {
    vi.setSystemTime(new Date('2026-06-18T00:00:01.000Z'));
    renderRoute();

    const firstSeriesParams = vi
      .mocked(queries.useSubscriptionQuotaSeries)
      .mock.calls.at(-1)?.[0];
    const firstAnalysisParams = vi
      .mocked(queries.useSubscriptionQuotaAnalysis)
      .mock.calls.at(-1)?.[0];

    cleanup();
    vi.clearAllMocks();
    vi.setSystemTime(new Date('2026-06-18T00:01:01.000Z'));
    renderRoute();

    const secondSeriesParams = vi
      .mocked(queries.useSubscriptionQuotaSeries)
      .mock.calls.at(-1)?.[0];
    const secondAnalysisParams = vi
      .mocked(queries.useSubscriptionQuotaAnalysis)
      .mock.calls.at(-1)?.[0];

    expect(firstSeriesParams).toMatchObject({
      rangeSecs: 604800,
      bucketSecs: 1800,
    });
    expect(firstAnalysisParams).toMatchObject({ rangeSecs: 604800 });
    expect(secondSeriesParams).toEqual(firstSeriesParams);
    expect(secondAnalysisParams).toEqual(firstAnalysisParams);
  });

  test('updates both stable range identities when the visible range changes', () => {
    renderRoute();

    fireEvent.click(screen.getByRole('button', { name: '1h' }));

    const seriesParams = vi
      .mocked(queries.useSubscriptionQuotaSeries)
      .mock.calls.at(-1)?.[0];
    const analysisParams = vi
      .mocked(queries.useSubscriptionQuotaAnalysis)
      .mock.calls.at(-1)?.[0];

    expect(seriesParams).toMatchObject({
      rangeSecs: 3600,
      bucketSecs: 60,
    });
    expect(analysisParams).toMatchObject({ rangeSecs: 3600 });
  });
});

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
        refresh_token_expires_at_unix_secs: 2_100_000_000,
        mode: 'refreshing',
        can_refresh: true,
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

describe('/upstreams refresh retention', () => {
  test('keeps OAuth detail data during a delayed range refresh and swaps it only after success', () => {
    let phase: 'old' | 'new' = 'old';
    let isPlaceholderData = false;
    const phaseData = {
      old: {
        utilization: 0.42,
        accountEmail: 'old@example.com',
        scope: 'old-scope',
        requestModel: 'old-quota-model',
      },
      new: {
        utilization: 0.68,
        accountEmail: 'new@example.com',
        scope: 'new-scope',
        requestModel: 'new-quota-model',
      },
    } as const;

    vi.mocked(queries.useStatus).mockImplementation(() =>
      queryResult(
        {
          upstreams: [
            {
              id: upstream.id,
              status: 'active',
              last_apply_error: null,
            },
            {
              id: apiKeyUpstream.id,
              status: 'active',
              last_apply_error: null,
            },
          ],
        },
        { isPlaceholderData },
      ),
    );
    vi.mocked(queries.useSubscriptionQuotaLatest).mockImplementation(() =>
      queryResult(quotaLatestData(upstream, phaseData[phase].utilization), {
        isPlaceholderData,
      }),
    );
    vi.mocked(queries.useSubscriptionQuotaSeries).mockImplementation(() =>
      queryResult(
        quotaSeriesData(
          upstream,
          phaseData[phase].utilization,
          phase === 'old' ? 604_800 : 3_600,
        ),
        { isPlaceholderData },
      ),
    );
    vi.mocked(queries.useSubscriptionQuotaAnalysis).mockImplementation(() =>
      queryResult(quotaAnalysisData(upstream, phaseData[phase].utilization), {
        isPlaceholderData,
      }),
    );
    vi.mocked(queries.useUpstreamSubscriptionMetadata).mockImplementation(() =>
      queryResult(
        metadataData(
          upstream,
          phase === 'old' ? 'Pro-old' : 'Pro-new',
          phaseData[phase].accountEmail,
        ),
        { isPlaceholderData },
      ),
    );
    vi.mocked(queries.useUpstreamOAuthStatus).mockImplementation(() =>
      queryResult(oauthStatusData(upstream, phaseData[phase].scope), {
        isPlaceholderData,
      }),
    );
    vi.mocked(queries.useRecentEvents).mockImplementation(() =>
      queryResult(recentData(upstream, phaseData[phase].requestModel), {
        isPlaceholderData,
      }),
    );

    const view = renderRoute();

    expect(screen.getByText(/old@example\.com/)).toBeDefined();
    expect(screen.getByText('old-scope')).toBeDefined();
    expect(screen.getByText('old-quota-model')).toBeDefined();
    expect(screen.getByTestId('quota-snapshot-grid').textContent).toContain(
      '42.0%',
    );
    const oauthSidebarRow = screen.getByRole('button', {
      name: /OAuth Primary/,
    });
    expect(oauthSidebarRow.textContent).toContain('42%');
    expect(
      screen.getByTestId('quota-history-legend-slot').textContent,
    ).toContain('5h');

    isPlaceholderData = true;
    const oneHourRange = screen.getByRole('button', { name: '1h' });
    fireEvent.click(oneHourRange);
    expect(oneHourRange.getAttribute('aria-pressed')).toBe('true');

    expect(screen.getByText(/old@example\.com/)).toBeDefined();
    expect(screen.getByText('old-scope')).toBeDefined();
    expect(screen.getByText('old-quota-model')).toBeDefined();
    expect(screen.getByTestId('quota-snapshot-grid').textContent).toContain(
      '42.0%',
    );
    expect(
      screen.getByTestId('quota-history-legend-slot').textContent,
    ).toContain('5h');
    expect(
      screen
        .getByTestId('upstream-metadata-strip')
        .querySelectorAll('.skeleton'),
    ).toHaveLength(0);
    expect(
      screen
        .getByTestId('quota-history-legend-slot')
        .querySelectorAll('.skeleton'),
    ).toHaveLength(0);
    expect(
      screen.getByTestId('quota-snapshot-grid').querySelectorAll('.skeleton'),
    ).toHaveLength(0);
    expect(
      screen
        .getByTestId('oauth-status-card-body')
        .querySelectorAll('.skeleton'),
    ).toHaveLength(0);
    expect(
      screen
        .getByTestId('recent-requests-table-slot')
        .querySelectorAll('.skeleton'),
    ).toHaveLength(0);
    expect(oauthSidebarRow.querySelectorAll('.skeleton')).toHaveLength(0);

    phase = 'new';
    isPlaceholderData = false;
    view.rerender(routeElement());

    expect(screen.queryByText(/old@example\.com/)).toBeNull();
    expect(screen.queryByText('old-scope')).toBeNull();
    expect(screen.queryByText('old-quota-model')).toBeNull();
    expect(screen.getByText(/new@example\.com/)).toBeDefined();
    expect(screen.getByText('new-scope')).toBeDefined();
    expect(screen.getByText('new-quota-model')).toBeDefined();
    expect(screen.getByTestId('quota-snapshot-grid').textContent).toContain(
      '68.0%',
    );
  });

  test('keeps API usage, sidebar totals, and recent rows during placeholder refresh', () => {
    searchState = { selectedId: apiKeyUpstream.id };
    let phase: 'old' | 'new' = 'old';
    let isPlaceholderData = false;

    vi.mocked(queries.useSubscriptionQuotaLatest).mockReturnValue(
      queryResult({
        now_unix_secs: NOW_UNIX_SECS,
        max_staleness_secs: 300,
        upstreams: [],
      }),
    );
    vi.mocked(queries.useStatus).mockReturnValue(
      queryResult({
        upstreams: [
          {
            id: upstream.id,
            status: 'active',
            last_apply_error: null,
          },
          {
            id: apiKeyUpstream.id,
            status: 'active',
            last_apply_error: null,
          },
        ],
      }),
    );
    vi.mocked(queries.useUsage).mockImplementation((_range, _step, groupBy) => {
      const old = phase === 'old';
      const data =
        groupBy === 'upstream'
          ? usageData(
              apiKeyUpstream.id,
              old ? 100 : 200,
              old ? 1_500_000 : 2_500_000,
            )
          : usageData(
              old ? 'old-usage-model' : 'new-usage-model',
              old ? 100 : 200,
              old ? 1_500_000 : 2_500_000,
            );
      return queryResult(data, { isPlaceholderData });
    });
    vi.mocked(queries.useRecentEvents).mockImplementation(() =>
      queryResult(
        recentData(
          apiKeyUpstream,
          phase === 'old' ? 'old-request-model' : 'new-request-model',
        ),
        { isPlaceholderData },
      ),
    );

    const view = renderRoute();

    expect(screen.getByText('old-usage-model')).toBeDefined();
    expect(screen.getByText('old-request-model')).toBeDefined();
    const apiSidebarRow = screen.getByRole('button', {
      name: /API Key Backup/,
    });
    expect(apiSidebarRow.textContent).toContain('$1.50 · 100 tok');

    isPlaceholderData = true;
    fireEvent.click(screen.getByRole('button', { name: '7d' }));

    expect(screen.getByText('old-usage-model')).toBeDefined();
    expect(screen.getByText('old-request-model')).toBeDefined();
    expect(apiSidebarRow.textContent).toContain('$1.50 · 100 tok');
    expect(
      screen.getByTestId('api-usage-card').querySelectorAll('.skeleton'),
    ).toHaveLength(0);
    expect(
      screen
        .getByTestId('recent-requests-table-slot')
        .querySelectorAll('.skeleton'),
    ).toHaveLength(0);
    expect(apiSidebarRow.querySelectorAll('.skeleton')).toHaveLength(0);

    phase = 'new';
    isPlaceholderData = false;
    view.rerender(routeElement());

    expect(screen.queryByText('old-usage-model')).toBeNull();
    expect(screen.queryByText('old-request-model')).toBeNull();
    expect(screen.getByText('new-usage-model')).toBeDefined();
    expect(screen.getByText('new-request-model')).toBeDefined();
    expect(apiSidebarRow.textContent).toContain('$2.50 · 200 tok');
  });

  test('resets detail-local state and never renders the previous OAuth identity after selection changes', () => {
    vi.mocked(queries.useUpstreams).mockReturnValue(
      queryResult({ upstreams: [upstream, secondOauthUpstream] }),
    );
    vi.mocked(queries.useStatus).mockReturnValue(
      queryResult({
        upstreams: [
          {
            id: upstream.id,
            status: 'active',
            last_apply_error: null,
          },
          {
            id: secondOauthUpstream.id,
            status: 'active',
            last_apply_error: null,
          },
        ],
      }),
    );
    vi.mocked(queries.useSubscriptionQuotaLatest).mockReturnValue(
      queryResult(quotaLatestData(upstream, 0.42)),
    );
    vi.mocked(queries.useSubscriptionQuotaSeries).mockImplementation(
      (options) =>
        options.upstreamIds === upstream.id
          ? queryResult(quotaSeriesData(upstream, 0.42))
          : queryResult(undefined, { isLoading: true, isPending: true }),
    );
    vi.mocked(queries.useSubscriptionQuotaAnalysis).mockImplementation(
      (options) =>
        options.upstreamIds === upstream.id
          ? queryResult(quotaAnalysisData(upstream, 0.42))
          : queryResult(undefined, { isLoading: true, isPending: true }),
    );
    vi.mocked(queries.useUpstreamSubscriptionMetadata).mockImplementation(
      (upstreamId) =>
        upstreamId === upstream.id
          ? queryResult(
              metadataData(
                upstream,
                'Identity-old',
                'identity-old@example.com',
              ),
            )
          : queryResult(undefined, { isLoading: true, isPending: true }),
    );
    vi.mocked(queries.useUpstreamOAuthStatus).mockImplementation(
      (upstreamId) =>
        upstreamId === upstream.id
          ? queryResult(oauthStatusData(upstream, 'identity-old-scope'))
          : queryResult(undefined, { isLoading: true, isPending: true }),
    );
    vi.mocked(queries.useRecentEvents).mockImplementation((options) =>
      options.upstream_id === upstream.id
        ? queryResult(recentData(upstream, 'identity-old-model'))
        : queryResult(undefined, { isLoading: true, isPending: true }),
    );

    const view = renderRoute();
    expect(screen.getByText(/identity-old@example\.com/)).toBeDefined();
    expect(screen.getByText('identity-old-scope')).toBeDefined();
    expect(screen.getByText('identity-old-model')).toBeDefined();

    const oneHourRange = screen.getByRole('button', { name: '1h' });
    fireEvent.click(oneHourRange);
    expect(oneHourRange.getAttribute('aria-pressed')).toBe('true');

    searchState = { selectedId: secondOauthUpstream.id };
    view.rerender(routeElement());

    const resetRangeControl = screen.getByTestId('quota-history-range-control');
    expect(
      within(resetRangeControl)
        .getByRole('button', { name: '7d' })
        .getAttribute('aria-pressed'),
    ).toBe('true');
    expect(
      within(resetRangeControl)
        .getByRole('button', { name: '1h' })
        .getAttribute('aria-pressed'),
    ).toBe('false');
    expect(screen.queryByText(/identity-old@example\.com/)).toBeNull();
    expect(screen.queryByText('identity-old-scope')).toBeNull();
    expect(screen.queryByText('identity-old-model')).toBeNull();
    expect(screen.queryByText('42.0%')).toBeNull();
    expect(screen.getByTestId('upstream-metadata-loading')).toBeDefined();
    expect(
      screen
        .getByTestId('quota-history-legend-slot')
        .querySelectorAll('.skeleton'),
    ).toHaveLength(2);
    expect(screen.getByTestId('oauth-status-loading-grid')).toBeDefined();
    expect(
      screen
        .getByTestId('recent-requests-table-slot')
        .querySelectorAll('tbody tr'),
    ).toHaveLength(5);
    expect(
      screen.getByText(
        `No subscription quota data for ${secondOauthUpstream.name}`,
      ),
    ).toBeDefined();
  });
});

describe('/upstreams mutation pending UX', () => {
  test('create pending locks resubmission and modal dismissal while showing progress', () => {
    const mutate = vi.fn();
    vi.mocked(queries.useCreateUpstream).mockReturnValue({
      mutate,
      isPending: false,
      reset: vi.fn(),
    } as never);

    const view = renderRoute();
    fireEvent.click(screen.getByRole('button', { name: 'New' }));

    let dialog = screen.getByRole('dialog', { name: 'New upstream' });
    fireEvent.click(
      within(dialog).getByRole('radio', { name: /Anthropic API Key/ }),
    );
    fireEvent.click(within(dialog).getByRole('button', { name: 'Continue' }));
    fireEvent.change(within(dialog).getByPlaceholderText('anthropic-prod'), {
      target: { value: 'api-key-primary' },
    });
    fireEvent.change(within(dialog).getByPlaceholderText('sk-ant-...'), {
      target: { value: 'sk-ant-test' },
    });
    fireEvent.click(within(dialog).getByRole('button', { name: 'Create' }));
    expect(mutate).toHaveBeenCalledTimes(1);

    vi.mocked(queries.useCreateUpstream).mockReturnValue({
      mutate,
      isPending: true,
      reset: vi.fn(),
    } as never);
    view.rerender(routeElement());

    dialog = screen.getByRole('dialog', { name: 'New upstream' });
    const creating = within(dialog).getByRole('button', {
      name: 'Creating...',
    });
    expect(creating.hasAttribute('disabled')).toBe(true);
    expect(creating.getAttribute('aria-busy')).toBe('true');
    expect(creating.querySelector('svg.animate-spin')).not.toBeNull();
    expect(
      within(dialog)
        .getByRole('button', { name: 'Back' })
        .hasAttribute('disabled'),
    ).toBe(true);
    expect(
      within(dialog)
        .getByRole('button', { name: 'Close dialog' })
        .hasAttribute('disabled'),
    ).toBe(true);
    for (const control of [
      ...within(dialog).getAllByRole('textbox'),
      within(dialog).getByPlaceholderText('sk-ant-...'),
      within(dialog).getByRole('button', {
        name: 'Use environment variable instead',
      }),
    ]) {
      expect(control.hasAttribute('disabled')).toBe(true);
    }

    fireEvent.click(creating);
    fireEvent.keyDown(dialog, { key: 'Escape', code: 'Escape' });
    expect(mutate).toHaveBeenCalledTimes(1);
    expect(screen.getByRole('dialog', { name: 'New upstream' })).toBeDefined();
  });

  test('OAuth final save retains its account context and submits once while pending', () => {
    vi.spyOn(window, 'open').mockImplementation(() => null);
    const startDraft = vi.fn(
      (
        _vars: undefined,
        options?: {
          onSuccess?: (result: {
            authorize_url: string;
            state_token: string;
          }) => void;
        },
      ) => {
        options?.onSuccess?.({
          authorize_url: 'https://example.com/authorize',
          state_token: 'draft-state-token',
        });
      },
    );
    const completeDraft = vi.fn(
      (
        _body: unknown,
        options?: {
          onSuccess?: (result: {
            state_token: string;
            suggested_name: string;
            subscription_metadata: null;
            organization_metadata: null;
            mode: string;
            long_lived_fallback: boolean;
            fallback_reason: 'rejected' | 'clamped' | null;
            granted_expires_in_secs: number | null;
          }) => void;
        },
      ) => {
        options?.onSuccess?.({
          state_token: 'draft-state-token',
          suggested_name: 'OAuth Account',
          subscription_metadata: null,
          organization_metadata: null,
          mode: 'long_lived_365d',
          long_lived_fallback: false,
          fallback_reason: null,
          granted_expires_in_secs: null,
        });
      },
    );
    const createFromDraft = vi.fn();
    vi.mocked(queries.useStartOauthDraft).mockReturnValue({
      mutate: startDraft,
      isPending: false,
      reset: vi.fn(),
    } as never);
    vi.mocked(queries.useCompleteOauthDraft).mockReturnValue({
      mutate: completeDraft,
      isPending: false,
      reset: vi.fn(),
    } as never);
    vi.mocked(queries.useCreateFromOauthDraft).mockReturnValue({
      mutate: createFromDraft,
      isPending: false,
      reset: vi.fn(),
    } as never);

    const view = renderRoute();
    fireEvent.click(screen.getByRole('button', { name: 'New' }));

    let dialog = screen.getByRole('dialog', { name: 'New upstream' });
    fireEvent.click(within(dialog).getByRole('button', { name: 'Continue' }));
    fireEvent.click(
      within(dialog).getByRole('button', {
        name: 'Authorize with Anthropic',
      }),
    );
    fireEvent.change(within(dialog).getByPlaceholderText('paste code...'), {
      target: { value: 'oauth-code' },
    });
    fireEvent.click(
      within(dialog).getByRole('button', {
        name: 'Verify and fetch account',
      }),
    );
    fireEvent.click(within(dialog).getByRole('button', { name: 'Save' }));

    expect(startDraft).toHaveBeenCalledTimes(1);
    // The draft-start request carries no mode: cc-lb always requests the
    // long-lived grant and decides the outcome itself.
    const draftVars = startDraft.mock.calls[0]?.[0] as unknown;
    expect(draftVars).toBeUndefined();
    expect(completeDraft).toHaveBeenCalledTimes(1);
    expect(createFromDraft).toHaveBeenCalledTimes(1);
    expect(createFromDraft).toHaveBeenCalledWith(
      {
        state_token: 'draft-state-token',
        name: 'OAuth Account',
      },
      expect.anything(),
    );

    vi.mocked(queries.useCreateFromOauthDraft).mockReturnValue({
      mutate: createFromDraft,
      isPending: true,
      reset: vi.fn(),
    } as never);
    view.rerender(routeElement());

    dialog = screen.getByRole('dialog', { name: 'New upstream' });
    expect(within(dialog).getByText('Account Preview')).toBeDefined();
    const saving = within(dialog).getByRole('button', { name: 'Saving...' });
    expect(saving.hasAttribute('disabled')).toBe(true);
    expect(saving.getAttribute('aria-busy')).toBe('true');
    expect(saving.querySelector('svg.animate-spin')).not.toBeNull();
    expect(
      within(dialog)
        .getByRole('button', { name: 'Back' })
        .hasAttribute('disabled'),
    ).toBe(true);
    expect(
      within(dialog)
        .getByRole('button', { name: 'Close dialog' })
        .hasAttribute('disabled'),
    ).toBe(true);
    expect(within(dialog).getByRole('textbox').hasAttribute('disabled')).toBe(
      true,
    );

    fireEvent.click(saving);
    fireEvent.keyDown(dialog, { key: 'Escape', code: 'Escape' });
    expect(createFromDraft).toHaveBeenCalledTimes(1);
    expect(screen.getByRole('dialog', { name: 'New upstream' })).toBeDefined();
  });

  test('toggle pending shows the requested progress and prevents a second change', () => {
    const mutate = vi.fn();
    vi.mocked(queries.useUpdateUpstreamWarmupSettings).mockReturnValue({
      mutate,
      isPending: false,
      variables: undefined,
      reset: vi.fn(),
    } as never);

    const view = renderRoute();
    const toggle = screen.getByRole('switch', { name: 'Enabled' });
    fireEvent.click(toggle);

    expect(mutate).toHaveBeenCalledTimes(1);
    expect(mutate).toHaveBeenCalledWith(
      {
        id: upstream.id,
        body: { enabled: false, warmup_enabled: false },
        spec_revision: upstream.spec_revision,
      },
      expect.anything(),
    );

    vi.mocked(queries.useUpdateUpstreamWarmupSettings).mockReturnValue({
      mutate,
      isPending: true,
      variables: { body: { enabled: false } },
      reset: vi.fn(),
    } as never);
    view.rerender(routeElement());

    const pendingToggle = screen.getByRole('switch', { name: 'Enabled' });
    expect(pendingToggle.hasAttribute('disabled')).toBe(true);
    const status = screen.getByTestId('upstream-enabled-pending');
    expect(status.getAttribute('role')).toBe('status');
    expect(status.textContent).toContain('Disabling...');
    expect(status.querySelector('svg.animate-spin')).not.toBeNull();

    fireEvent.click(pendingToggle);
    expect(mutate).toHaveBeenCalledTimes(1);
  });

  test('OAuth start shows progress and completion locks the authorization modal', () => {
    vi.mocked(queries.useUpstreamOAuthStatus).mockReturnValue({
      data: {
        upstream_id: upstream.id,
        kind: upstream.kind,
        has_credentials: false,
        status: 'missing',
        expires_at_unix_secs: null,
        refresh_token_present: false,
        refresh_token_expires_at_unix_secs: null,
        mode: null,
        can_refresh: false,
        scopes: [],
      },
      isLoading: false,
      isPending: false,
      isPlaceholderData: false,
    } as never);
    const start = vi.fn();
    const complete = vi.fn();
    vi.mocked(queries.useOAuthStart).mockReturnValue({
      mutate: start,
      isPending: false,
      reset: vi.fn(),
    } as never);
    vi.mocked(queries.useOAuthComplete).mockReturnValue({
      mutate: complete,
      isPending: false,
      reset: vi.fn(),
    } as never);

    const view = renderRoute();

    // The reconnect notice is the prominent entry point; the OAuth card keeps
    // its own Connect button, so scope the click to the notice's status role.
    const reconnectNotice = screen.getByRole('status');
    expect(
      within(reconnectNotice).getByText('OAuth not connected'),
    ).toBeDefined();
    fireEvent.click(
      within(reconnectNotice).getByRole('button', { name: 'Connect' }),
    );

    // Connect only opens the modal; the start mutation fires from the
    // Generate button, so its pending state lives there.
    let dialog = screen.getByRole('dialog', {
      name: 'OAuth Authorization',
    });
    expect(
      within(dialog).getByRole('button', {
        name: 'Generate authorization URL',
      }),
    ).toBeDefined();
    expect(start).not.toHaveBeenCalled();

    vi.mocked(queries.useOAuthStart).mockReturnValue({
      mutate: start,
      isPending: true,
      reset: vi.fn(),
    } as never);
    view.rerender(routeElement());

    const starting = within(dialog).getByRole('button', {
      name: 'Starting...',
    });
    expect(starting.hasAttribute('disabled')).toBe(true);
    expect(starting.getAttribute('aria-busy')).toBe('true');
    expect(starting.querySelector('svg.animate-spin')).not.toBeNull();
    expect(start).not.toHaveBeenCalled();

    const startResolved = vi.fn(
      (
        _vars: { id: string },
        options?: {
          onSuccess?: (result: {
            authorize_url: string;
            state_token: string;
            revision: number;
          }) => void;
        },
      ) => {
        options?.onSuccess?.({
          authorize_url: 'https://example.com/authorize',
          state_token: 'oauth-state-token',
          revision: 2,
        });
      },
    );
    vi.mocked(queries.useOAuthStart).mockReturnValue({
      mutate: startResolved,
      isPending: false,
      reset: vi.fn(),
    } as never);
    view.rerender(routeElement());
    fireEvent.click(
      within(dialog).getByRole('button', {
        name: 'Generate authorization URL',
      }),
    );
    expect(startResolved).toHaveBeenCalledTimes(1);
    // The start request carries no mode: cc-lb always requests the
    // long-lived grant and decides the outcome itself.
    expect(startResolved).toHaveBeenCalledWith(
      { id: upstream.id },
      expect.anything(),
    );
    expect(
      within(dialog).getByText('https://example.com/authorize'),
    ).toBeDefined();

    fireEvent.change(within(dialog).getByPlaceholderText(/paste code/), {
      target: { value: 'oauth-code' },
    });
    fireEvent.click(within(dialog).getByRole('button', { name: 'Complete' }));
    expect(complete).toHaveBeenCalledTimes(1);
    expect(complete).toHaveBeenCalledWith(
      {
        id: upstream.id,
        state_token: 'oauth-state-token',
        code: 'oauth-code',
      },
      expect.anything(),
    );

    vi.mocked(queries.useOAuthComplete).mockReturnValue({
      mutate: complete,
      isPending: true,
      reset: vi.fn(),
    } as never);
    view.rerender(routeElement());

    dialog = screen.getByRole('dialog', { name: 'OAuth Authorization' });
    const completing = within(dialog).getByRole('button', {
      name: 'Completing...',
    });
    expect(completing.hasAttribute('disabled')).toBe(true);
    expect(completing.getAttribute('aria-busy')).toBe('true');
    expect(completing.querySelector('svg.animate-spin')).not.toBeNull();
    expect(
      within(dialog)
        .getByRole('button', { name: 'Cancel' })
        .hasAttribute('disabled'),
    ).toBe(true);
    expect(
      within(dialog)
        .getByRole('button', { name: 'Close dialog' })
        .hasAttribute('disabled'),
    ).toBe(true);
    expect(within(dialog).getByRole('textbox').hasAttribute('disabled')).toBe(
      true,
    );

    fireEvent.click(completing);
    fireEvent.click(within(dialog).getByRole('button', { name: 'Cancel' }));
    fireEvent.keyDown(dialog, { key: 'Escape', code: 'Escape' });
    expect(complete).toHaveBeenCalledTimes(1);
    expect(
      screen.getByRole('dialog', { name: 'OAuth Authorization' }),
    ).toBeDefined();
  });

  test('delete confirmation remains open and locked until deletion succeeds', () => {
    const mutate = vi.fn();
    vi.mocked(queries.useDeleteUpstream).mockReturnValue({
      mutate,
      isPending: false,
      reset: vi.fn(),
    } as never);

    const view = renderRoute();
    fireEvent.click(screen.getByRole('button', { name: 'Delete' }));

    let dialog = screen.getByRole('alertdialog', {
      name: 'Delete upstream?',
    });
    fireEvent.click(within(dialog).getByRole('button', { name: 'Delete' }));
    expect(mutate).toHaveBeenCalledTimes(1);
    expect(
      screen.getByRole('alertdialog', { name: 'Delete upstream?' }),
    ).toBeDefined();

    vi.mocked(queries.useDeleteUpstream).mockReturnValue({
      mutate,
      isPending: true,
      reset: vi.fn(),
    } as never);
    view.rerender(routeElement());

    dialog = screen.getByRole('alertdialog', { name: 'Delete upstream?' });
    expect(within(dialog).getByText('OAuth Primary')).toBeDefined();
    expect(
      within(dialog)
        .getByRole('button', { name: 'Cancel' })
        .hasAttribute('disabled'),
    ).toBe(true);
    const deleting = within(dialog).getByRole('button', {
      name: 'Deleting...',
    });
    expect(deleting.hasAttribute('disabled')).toBe(true);
    expect(deleting.getAttribute('aria-busy')).toBe('true');
    expect(deleting.querySelector('svg.animate-spin')).not.toBeNull();

    fireEvent.click(deleting);
    fireEvent.click(within(dialog).getByRole('button', { name: 'Cancel' }));
    fireEvent.keyDown(dialog, { key: 'Escape', code: 'Escape' });
    expect(mutate).toHaveBeenCalledTimes(1);
    expect(
      screen.getByRole('alertdialog', { name: 'Delete upstream?' }),
    ).toBeDefined();
  });
});

describe('/upstreams OAuth card', () => {
  function mockOAuthStatus(refreshTokenExpiresAt: number | null) {
    vi.mocked(queries.useUpstreamOAuthStatus).mockReturnValue({
      data: {
        upstream_id: upstream.id,
        kind: upstream.kind,
        has_credentials: true,
        status: 'valid',
        expires_at_unix_secs: 2_000_000_000,
        refresh_token_present: true,
        refresh_token_expires_at_unix_secs: refreshTokenExpiresAt,
        mode: 'refreshing',
        can_refresh: true,
        scopes: ['user:inference'],
      },
      isLoading: false,
      isPending: false,
      isPlaceholderData: false,
    } as never);
  }

  test('shows the refresh-token expiry as a relative time when known', () => {
    mockOAuthStatus(Math.floor(Date.now() / 1000) + 29 * 24 * 60 * 60);

    renderRoute();

    const expiry = screen.getByTestId('oauth-refresh-token-expiry');
    expect(expiry.textContent).toContain('expires');
    expect(expiry.textContent).toMatch(/in \d+ (weeks?|months?|days?)/);
  });

  test('omits the refresh-token expiry when the endpoint never reported one', () => {
    mockOAuthStatus(null);

    renderRoute();

    expect(screen.queryByTestId('oauth-refresh-token-expiry')).toBeNull();
    expect(screen.getByTestId('oauth-status-loaded-grid')).toBeDefined();
  });

  test('shows the reconnect notice while the live record reports a renewal failure and clears it once resolved', () => {
    mockOAuthStatus(2_100_000_000);
    // The runtime /status snapshot reports no error; only the upstream
    // record's live last_apply_error drives the classifier.
    vi.mocked(queries.useStatus).mockReturnValue({
      data: {
        upstreams: [
          {
            id: upstream.id,
            name: upstream.name,
            status: 'applied',
            last_apply_at_unix_secs: NOW_UNIX_SECS,
            last_apply_error: null,
          },
        ],
      },
      isLoading: false,
      isPending: false,
      isPlaceholderData: false,
    } as never);
    const failingUpstream: Upstream = {
      ...upstream,
      status: { ...upstream.status, last_apply_error: 'status_401' },
    };
    vi.mocked(queries.useUpstreams).mockReturnValue({
      data: { upstreams: [failingUpstream, apiKeyUpstream] },
      isLoading: false,
      isPending: false,
      isPlaceholderData: false,
    } as never);

    const view = renderRoute();

    const notice = screen.getByRole('alert');
    expect(within(notice).getByText('Token renewal failed')).toBeDefined();
    expect(
      within(notice).getByRole('button', { name: 'Reconnect' }),
    ).toBeDefined();

    vi.mocked(queries.useUpstreams).mockReturnValue({
      data: { upstreams: [upstream, apiKeyUpstream] },
      isLoading: false,
      isPending: false,
      isPlaceholderData: false,
    } as never);
    view.rerender(routeElement());

    expect(screen.queryByRole('alert')).toBeNull();
  });
});

describe('/upstreams long-lived OAuth credential', () => {
  test('shows a stored-but-unused refresh token and no expiry for a long-lived credential', () => {
    vi.mocked(queries.useUpstreamOAuthStatus).mockReturnValue(
      queryResult(
        oauthStatusData(upstream, 'user:inference', {
          mode: 'long_lived_365d',
          can_refresh: false,
          refresh_token_expires_at_unix_secs: null,
        }),
      ),
    );

    renderRoute();

    expect(screen.queryByTestId('oauth-refresh-token-expiry')).toBeNull();
    const refreshRow = screen.getByText('Refresh token')
      .parentElement as HTMLElement;
    expect(within(refreshRow).getByText('stored, unused')).toBeDefined();
  });

  test('keeps the healthy Long-lived badge when the stored refresh-token clock has lapsed', () => {
    const now = Math.floor(Date.now() / 1000);
    vi.mocked(queries.useUpstreamOAuthStatus).mockReturnValue(
      queryResult(
        oauthStatusData(upstream, 'user:inference', {
          mode: 'long_lived_365d',
          can_refresh: false,
          expires_at_unix_secs: now + 330 * 24 * 60 * 60,
          refresh_token_expires_at_unix_secs: now - 24 * 60 * 60,
        }),
      ),
    );

    renderRoute();

    expect(screen.getByText('Long-lived')).toBeDefined();
    expect(screen.queryByText('Login expired')).toBeNull();
    expect(screen.queryByTestId('oauth-refresh-token-expiry')).toBeNull();
  });

  test('labels the credential mode for long-lived and refreshing credentials', () => {
    vi.mocked(queries.useUpstreamOAuthStatus).mockReturnValue(
      queryResult(
        oauthStatusData(upstream, 'user:inference', {
          mode: 'long_lived_365d',
          can_refresh: false,
          refresh_token_expires_at_unix_secs: null,
        }),
      ),
    );

    renderRoute();
    expect(
      within(screen.getByTestId('oauth-credential-mode')).getByText(
        '365-day token',
      ),
    ).toBeDefined();

    cleanup();
    vi.mocked(queries.useUpstreamOAuthStatus).mockReturnValue(
      queryResult(oauthStatusData(upstream, 'user:inference')),
    );

    renderRoute();
    expect(
      within(screen.getByTestId('oauth-credential-mode')).getByText(
        'Refreshing',
      ),
    ).toBeDefined();
  });

  test('shows the fallback notice only when the long-lived exchange fell back', () => {
    vi.spyOn(window, 'open').mockImplementation(() => null);
    let fallbackReason: 'rejected' | 'clamped' | null = null;
    let grantedExpiresInSecs: number | null = null;
    const startDraft = vi.fn(
      (
        _vars: undefined,
        options?: {
          onSuccess?: (result: {
            authorize_url: string;
            state_token: string;
          }) => void;
        },
      ) => {
        options?.onSuccess?.({
          authorize_url: 'https://example.com/authorize',
          state_token: 'draft-state-token',
        });
      },
    );
    const completeDraft = vi.fn(
      (
        _body: unknown,
        options?: {
          onSuccess?: (result: {
            state_token: string;
            suggested_name: string;
            subscription_metadata: null;
            organization_metadata: null;
            mode: string;
            long_lived_fallback: boolean;
            fallback_reason: 'rejected' | 'clamped' | null;
            granted_expires_in_secs: number | null;
          }) => void;
        },
      ) => {
        options?.onSuccess?.({
          state_token: 'draft-state-token',
          suggested_name: 'OAuth Account',
          subscription_metadata: null,
          organization_metadata: null,
          mode: fallbackReason ? 'refreshing' : 'long_lived_365d',
          long_lived_fallback: fallbackReason !== null,
          fallback_reason: fallbackReason,
          granted_expires_in_secs: grantedExpiresInSecs,
        });
      },
    );
    vi.mocked(queries.useStartOauthDraft).mockReturnValue({
      mutate: startDraft,
      isPending: false,
      reset: vi.fn(),
    } as never);
    vi.mocked(queries.useCompleteOauthDraft).mockReturnValue({
      mutate: completeDraft,
      isPending: false,
      reset: vi.fn(),
    } as never);

    const completeHandshake = () => {
      fireEvent.click(screen.getByRole('button', { name: 'New' }));
      const dialog = screen.getByRole('dialog', { name: 'New upstream' });
      fireEvent.click(within(dialog).getByRole('button', { name: 'Continue' }));
      fireEvent.click(
        within(dialog).getByRole('button', {
          name: 'Authorize with Anthropic',
        }),
      );
      fireEvent.change(within(dialog).getByPlaceholderText('paste code...'), {
        target: { value: 'oauth-code' },
      });
      fireEvent.click(
        within(dialog).getByRole('button', {
          name: 'Verify and fetch account',
        }),
      );
      return dialog;
    };

    renderRoute();
    let dialog = completeHandshake();
    expect(within(dialog).getByText('Account Preview')).toBeDefined();
    expect(
      within(dialog).queryByTestId('oauth-long-lived-fallback-notice'),
    ).toBeNull();

    cleanup();
    fallbackReason = 'rejected';
    renderRoute();
    dialog = completeHandshake();
    expect(within(dialog).getByText('Account Preview')).toBeDefined();
    let notice = within(dialog).getByTestId('oauth-long-lived-fallback-notice');
    expect(notice.getAttribute('data-reason')).toBe('rejected');

    cleanup();
    fallbackReason = 'clamped';
    grantedExpiresInSecs = 28_800;
    renderRoute();
    dialog = completeHandshake();
    expect(within(dialog).getByText('Account Preview')).toBeDefined();
    notice = within(dialog).getByTestId('oauth-long-lived-fallback-notice');
    expect(notice.getAttribute('data-reason')).toBe('clamped');
    // The granted lifetime (28800s = 8 hours) is surfaced to the operator.
    expect(notice.textContent).toContain('expires in 8 hours');
  });

  test('shows the fallback notice when a reconnect falls back to a refreshing credential', () => {
    vi.mocked(queries.useUpstreamOAuthStatus).mockReturnValue({
      data: {
        upstream_id: upstream.id,
        kind: upstream.kind,
        has_credentials: false,
        status: 'missing',
        expires_at_unix_secs: null,
        refresh_token_present: false,
        refresh_token_expires_at_unix_secs: null,
        mode: null,
        can_refresh: false,
        scopes: [],
      },
      isLoading: false,
      isPending: false,
      isPlaceholderData: false,
    } as never);
    const start = vi.fn(
      (
        _vars: { id: string },
        options?: {
          onSuccess?: (result: {
            authorize_url: string;
            state_token: string;
            revision: number;
          }) => void;
        },
      ) => {
        options?.onSuccess?.({
          authorize_url: 'https://example.com/authorize',
          state_token: 'oauth-state-token',
          revision: 2,
        });
      },
    );
    let completeData:
      | {
          upstream_id: string;
          expires_at_unix_secs: number;
          access_token_fingerprint: string;
          mode: string;
          long_lived_fallback: boolean;
          fallback_reason: 'rejected' | 'clamped' | null;
          granted_expires_in_secs: number | null;
        }
      | undefined;
    const complete = vi.fn(
      (
        _vars: { id: string; state_token: string; code: string },
        options?: { onSuccess?: (result: typeof completeData) => void },
      ) => {
        completeData = {
          upstream_id: upstream.id,
          expires_at_unix_secs: NOW_UNIX_SECS + 3600,
          access_token_fingerprint: 'fp',
          mode: 'refreshing',
          long_lived_fallback: true,
          fallback_reason: 'clamped',
          granted_expires_in_secs: 3600,
        };
        options?.onSuccess?.(completeData);
      },
    );
    vi.mocked(queries.useOAuthStart).mockReturnValue({
      mutate: start,
      isPending: false,
      reset: vi.fn(),
    } as never);
    vi.mocked(queries.useOAuthComplete).mockImplementation(
      () =>
        ({
          mutate: complete,
          isPending: false,
          isSuccess: completeData !== undefined,
          data: completeData,
          reset: vi.fn(),
        }) as never,
    );

    const view = renderRoute();

    // The reconnect notice is the prominent entry point, same as the
    // pending-UX test above.
    const reconnectNotice = screen.getByRole('status');
    fireEvent.click(
      within(reconnectNotice).getByRole('button', { name: 'Connect' }),
    );

    const dialog = screen.getByRole('dialog', { name: 'OAuth Authorization' });
    fireEvent.click(
      within(dialog).getByRole('button', {
        name: 'Generate authorization URL',
      }),
    );
    fireEvent.change(within(dialog).getByPlaceholderText(/paste code/), {
      target: { value: 'oauth-code' },
    });
    fireEvent.click(within(dialog).getByRole('button', { name: 'Complete' }));
    expect(complete).toHaveBeenCalledTimes(1);

    view.rerender(routeElement());

    // The modal stays open so the operator sees the demotion, and the notice
    // reports the same reason the wizard would.
    const openDialog = screen.getByRole('dialog', {
      name: 'OAuth Authorization',
    });
    const notice = within(openDialog).getByTestId(
      'oauth-long-lived-fallback-notice',
    );
    expect(notice.getAttribute('data-reason')).toBe('clamped');
  });

  test('nudges reconnect on the access-token clock and ignores the stored refresh token', () => {
    const now = Math.floor(Date.now() / 1000);
    vi.mocked(queries.useUpstreamOAuthStatus).mockReturnValue(
      queryResult(
        oauthStatusData(upstream, 'user:inference', {
          mode: 'long_lived_365d',
          can_refresh: false,
          expires_at_unix_secs: now + 10 * 24 * 60 * 60,
          refresh_token_expires_at_unix_secs: null,
        }),
      ),
    );

    renderRoute();

    // Inside the 14-day window the access-token deadline drives the nudge.
    const notice = screen.getByRole('status');
    expect(
      within(notice).getByText('Long-lived token expiring soon'),
    ).toBeDefined();
    expect(
      within(notice).getByRole('button', { name: 'Reconnect' }),
    ).toBeDefined();
    expect(screen.getByText('Login expiring')).toBeDefined();

    cleanup();
    vi.mocked(queries.useUpstreamOAuthStatus).mockReturnValue(
      queryResult(
        oauthStatusData(upstream, 'user:inference', {
          mode: 'long_lived_365d',
          can_refresh: false,
          expires_at_unix_secs: now + 330 * 24 * 60 * 60,
          refresh_token_expires_at_unix_secs: now - 24 * 60 * 60,
        }),
      ),
    );

    renderRoute();

    // A lapsed refresh-token clock must not nudge a credential that never
    // uses it.
    expect(screen.queryByRole('status')).toBeNull();
    expect(screen.getByText('Long-lived')).toBeDefined();
  });
});
