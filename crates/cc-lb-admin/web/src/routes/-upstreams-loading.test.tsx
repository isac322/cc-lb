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
  test('aligns analysis ranges at the 120-second boundary', () => {
    const boundaryUnixSecs =
      new Date('2026-06-18T00:02:00.000Z').getTime() / 1000;
    const rangeSecs = 3600;

    const justBefore = queries.alignSubscriptionQuotaAnalysisKeyRange(
      boundaryUnixSecs - 1 - rangeSecs,
      boundaryUnixSecs - 1,
    );
    const atBoundary = queries.alignSubscriptionQuotaAnalysisKeyRange(
      boundaryUnixSecs - rangeSecs,
      boundaryUnixSecs,
    );
    const justBeforeNextBoundary =
      queries.alignSubscriptionQuotaAnalysisKeyRange(
        boundaryUnixSecs + 119 - rangeSecs,
        boundaryUnixSecs + 119,
      );

    expect(justBefore).toEqual({
      sinceUnixSecs: boundaryUnixSecs - 120 - rangeSecs,
      untilUnixSecs: boundaryUnixSecs - 120,
    });
    expect(atBoundary).toEqual({
      sinceUnixSecs: boundaryUnixSecs - rangeSecs,
      untilUnixSecs: boundaryUnixSecs,
    });
    expect(justBeforeNextBoundary).toEqual(atBoundary);
  });

  test('keeps the analysis key stable across a 60-second tick while requests follow the visible domain', () => {
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

    if (
      !firstSeriesParams ||
      !firstAnalysisParams ||
      !secondSeriesParams ||
      !secondAnalysisParams
    ) {
      throw new Error('quota hooks were not called');
    }

    expect(secondAnalysisParams.untilUnixSecs).toBe(
      firstAnalysisParams.untilUnixSecs + 60,
    );
    expect(secondAnalysisParams.sinceUnixSecs).toBe(
      firstAnalysisParams.sinceUnixSecs + 60,
    );
    expect(secondAnalysisParams).toMatchObject({
      sinceUnixSecs: secondSeriesParams.sinceUnixSecs,
      untilUnixSecs: secondSeriesParams.untilUnixSecs,
    });
    expect(secondSeriesParams.untilUnixSecs).toBe(
      firstSeriesParams.untilUnixSecs + 60,
    );
    expect(secondSeriesParams.sinceUnixSecs).toBe(
      firstSeriesParams.sinceUnixSecs + 60,
    );

    const firstRequest =
      queries.buildSubscriptionQuotaAnalysisRequest(firstAnalysisParams);
    const secondRequest =
      queries.buildSubscriptionQuotaAnalysisRequest(secondAnalysisParams);
    expect(secondRequest.queryKey).toEqual(firstRequest.queryKey);
    expect(secondRequest.path).not.toBe(firstRequest.path);
    expect(secondRequest.path).toContain(
      `since_unix_secs=${secondAnalysisParams.sinceUnixSecs}`,
    );
    expect(secondRequest.path).toContain(
      `until_unix_secs=${secondAnalysisParams.untilUnixSecs}`,
    );
  });

  test('uses the latest exact range when the 120-second analysis quantum advances', () => {
    const firstUntilUnixSecs =
      new Date('2026-06-18T00:00:01.000Z').getTime() / 1000;
    const rangeSecs = 604800;
    const firstParams = {
      sinceUnixSecs: firstUntilUnixSecs - rangeSecs,
      untilUnixSecs: firstUntilUnixSecs,
    };
    const secondParams = {
      sinceUnixSecs: firstParams.sinceUnixSecs + 120,
      untilUnixSecs: firstParams.untilUnixSecs + 120,
    };

    const firstRequest =
      queries.buildSubscriptionQuotaAnalysisRequest(firstParams);
    const secondRequest =
      queries.buildSubscriptionQuotaAnalysisRequest(secondParams);

    expect(secondRequest.queryKey).not.toEqual(firstRequest.queryKey);
    expect(secondRequest.path).toContain(
      `since_unix_secs=${secondParams.sinceUnixSecs}`,
    );
    expect(secondRequest.path).toContain(
      `until_unix_secs=${secondParams.untilUnixSecs}`,
    );
  });

  test('keeps exact request bounds when the visible range changes', () => {
    renderRoute();

    fireEvent.click(screen.getByRole('button', { name: '1h' }));

    const seriesParams = vi
      .mocked(queries.useSubscriptionQuotaSeries)
      .mock.calls.at(-1)?.[0];
    const analysisParams = vi
      .mocked(queries.useSubscriptionQuotaAnalysis)
      .mock.calls.at(-1)?.[0];

    if (!seriesParams || !analysisParams) {
      throw new Error('quota hooks were not called');
    }
    expect(analysisParams).toMatchObject({
      sinceUnixSecs: seriesParams.sinceUnixSecs,
      untilUnixSecs: seriesParams.untilUnixSecs,
    });
    expect(analysisParams.untilUnixSecs - analysisParams.sinceUnixSecs).toBe(
      3600,
    );

    const request =
      queries.buildSubscriptionQuotaAnalysisRequest(analysisParams);
    const keyParams = request.queryKey[2];
    const alignedUntilUnixSecs =
      Math.floor(analysisParams.untilUnixSecs / 120) * 120;
    expect(keyParams).toMatchObject({
      sinceUnixSecs: alignedUntilUnixSecs - 3600,
      untilUnixSecs: alignedUntilUnixSecs,
    });
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
        _body: undefined,
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
          }) => void;
        },
      ) => {
        options?.onSuccess?.({
          state_token: 'draft-state-token',
          suggested_name: 'OAuth Account',
          subscription_metadata: null,
          organization_metadata: null,
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
      isPending: true,
      reset: vi.fn(),
    } as never);
    vi.mocked(queries.useOAuthComplete).mockReturnValue({
      mutate: complete,
      isPending: false,
      reset: vi.fn(),
    } as never);

    const view = renderRoute();
    const starting = screen.getByRole('button', { name: 'Starting...' });
    expect(starting.hasAttribute('disabled')).toBe(true);
    expect(starting.getAttribute('aria-busy')).toBe('true');
    expect(starting.querySelector('svg.animate-spin')).not.toBeNull();

    vi.mocked(queries.useOAuthStart).mockReturnValue({
      mutate: vi.fn(
        (
          _id: string,
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
      ),
      isPending: false,
      reset: vi.fn(),
    } as never);
    view.rerender(routeElement());
    fireEvent.click(screen.getByRole('button', { name: 'Connect' }));

    let dialog = screen.getByRole('dialog', {
      name: 'OAuth Authorization',
    });
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
});
