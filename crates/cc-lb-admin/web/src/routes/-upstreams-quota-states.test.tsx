import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { cleanup, render, screen, within } from '@testing-library/react';
import type { ComponentType } from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  absentWindows,
  ID,
  latestRow,
  NAME,
  NOW,
  quotaSnapshot,
  rawMeter,
} from '../../e2e/quotaSettledStatesFixture';
import type {
  DashboardUsageResponse,
  LatestResponse,
  SeriesResponse,
  SubscriptionMetadataResponse,
  UpstreamKeyUsageResponse,
  UpstreamLatestSnapshots,
  UpstreamOAuthStatusResponse,
} from '../lib/api';
import type { Upstream } from '../lib/queries';
import * as queries from '../lib/queries';
import {
  failedQuery,
  pendingQuery,
  successQuery,
} from '../lib/test-utils/hook-results';
import { Route } from './upstreams';

const Component = Route.options.component as ComponentType;

vi.mock('@tanstack/react-router', async () => ({
  ...(await vi.importActual('@tanstack/react-router')),
  useNavigate: () => vi.fn(),
}));
vi.mock('../lib/queries', async () => ({
  ...(await vi.importActual('../lib/queries')),
  useUpstreams: vi.fn(),
  useStatus: vi.fn(),
  useUsage: vi.fn(),
  useSubscriptionQuotaLatest: vi.fn(),
  useSubscriptionQuotaSeries: vi.fn(),
  useUpstreamOAuthStatus: vi.fn(),
  useUpstreamSubscriptionMetadata: vi.fn(),
  useUpstreamKeyUsage: vi.fn(),
  usePrincipalNameMap: () => new Map(),
}));
vi.mock('../lib/useRequestEventsFeed', () => ({
  useRequestEventsFeed: () => ({
    rows: [],
    filteredRows: [],
    pageRows: [],
    firstPageEvents: [],
    historyFilters: {},
    loading: false,
    isFetching: false,
    error: null,
    page: 0,
    pageCount: 1,
    pageSize: 50,
    totalRows: 0,
    hasMore: false,
    loadingNext: false,
    previousPage: vi.fn(),
    nextPage: vi.fn(),
    refresh: vi.fn(),
    live: {},
    liveEnabled: true,
    tailStatus: 'live',
    statusLabel: 'Live',
    statusColor: 'ok',
    liveFlashIds: new Set(),
  }),
}));
vi.mock('../components/upstreams/InlineNameEditor', () => ({
  InlineNameEditor: ({ upstream }: { upstream: Upstream }) => (
    <span>{upstream.name}</span>
  ),
}));
vi.mock('../components/upstreams/LimitResetAction', () => ({
  LimitResetAction: () => null,
}));
vi.mock('../components/upstreams/warmup/WarmupCardMinimal', () => ({
  WarmupCardMinimal: () => null,
}));

const upstream: Upstream = {
  id: ID,
  name: NAME,
  kind: 'gemini_oauth',
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
};
type UpstreamsData = { upstreams: Upstream[] };
type StatusData = {
  version: string;
  git_sha: string;
  uptime_secs: number;
  build: { rust_version: string; profile: string; target: string };
  generation: number;
  upstreams: Array<{
    id: string;
    name: string;
    status: string;
    last_apply_at_unix_secs: number | null;
    last_apply_error: string | null;
  }>;
};
type UsageData = DashboardUsageResponse;
type KeyUsageData = UpstreamKeyUsageResponse;
type QuotaLatestData = LatestResponse;
type QuotaSeriesData = SeriesResponse;
type OAuthStatusData = UpstreamOAuthStatusResponse;
type SubscriptionMetadataData = SubscriptionMetadataResponse;

let client: QueryClient;
function setLatest(latest: UpstreamLatestSnapshots | null, pending = false) {
  vi.mocked(queries.useSubscriptionQuotaLatest).mockReturnValue(
    pending
      ? pendingQuery<QuotaLatestData>()
      : successQuery<QuotaLatestData>({
          now_unix_secs: NOW,
          max_staleness_secs: 300,
          upstreams: latest == null ? [] : [latest],
        }),
  );
}
function element() {
  return (
    <QueryClientProvider client={client}>
      <Component />
    </QueryClientProvider>
  );
}
function quotaSection() {
  const section = screen
    .getByRole('heading', { name: /^Quota$/ })
    .closest('section');
  if (!section) throw new Error('Quota section missing');
  return within(section);
}

beforeEach(() => {
  vi.useFakeTimers({ toFake: ['Date'], now: new Date(NOW * 1000) });
  vi.clearAllMocks();
  window.localStorage.clear();
  client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  Object.assign(Route, { useSearch: () => ({ selectedId: ID }) });
  vi.mocked(queries.useUpstreams).mockReturnValue(
    successQuery<UpstreamsData>({ upstreams: [upstream] }),
  );
  vi.mocked(queries.useStatus).mockReturnValue(pendingQuery<StatusData>());
  vi.mocked(queries.useUsage).mockReturnValue(pendingQuery<UsageData>());
  vi.mocked(queries.useUpstreamKeyUsage).mockReturnValue(
    failedQuery<KeyUsageData>(
      new Error('API usage is not used in this OAuth test'),
    ),
  );
  vi.mocked(queries.useSubscriptionQuotaSeries).mockReturnValue(
    pendingQuery<QuotaSeriesData>(),
  );
  vi.mocked(queries.useUpstreamOAuthStatus).mockReturnValue(
    pendingQuery<OAuthStatusData>(),
  );
  vi.mocked(queries.useUpstreamSubscriptionMetadata).mockReturnValue(
    pendingQuery<SubscriptionMetadataData>(),
  );
  setLatest(null);
});
afterEach(() => {
  cleanup();
  client.clear();
  vi.useRealTimers();
});

describe('QA-20: real upstream detail quota state branches', () => {
  it.each([
    ['missing upstream', null, `No subscription quota data for ${NAME}`],
    ['empty windows', latestRow([]), `No subscription quota data for ${NAME}`],
    ['all canonical absent', latestRow(absentWindows('gemini')), 'Not offered'],
    [
      'partial absence',
      latestRow(absentWindows('gemini').slice(1)),
      'No quota reading yet',
    ],
    [
      'all unobserved',
      latestRow(
        absentWindows('gemini').map((snap) => ({
          ...snap,
          state: 'unobserved' as const,
        })),
      ),
      'No quota reading yet',
    ],
  ] as const)(
    '%s has a truthful empty state without legend or freshness',
    (_name, latest, title) => {
      setLatest(latest);
      render(element());
      const section = quotaSection();
      expect(section.getByText(title)).not.toBeNull();
      expect(section.queryByTestId('quota-snapshot-grid')).toBeNull();
      expect(section.queryByTestId('quota-freshness')).toBeNull();
      expect(section.queryByText('even pace', { exact: true })).toBeNull();
    },
  );

  it('keeps skeletons quiet until the first response', () => {
    setLatest(null, true);
    render(element());
    const section = quotaSection();
    expect(section.getByTestId('quota-snapshot-grid')).not.toBeNull();
    expect(
      section.queryByText(
        /No subscription quota data|Not offered|No quota reading yet/,
      ),
    ).toBeNull();
    expect(section.queryByTestId('quota-freshness')).toBeNull();
    expect(section.queryByText('even pace', { exact: true })).toBeNull();
  });

  it.each([{ windows: [] }, { windows: absentWindows('gemini') }])(
    'renders raw-only quota over empty or absent canonical windows',
    ({ windows }) => {
      setLatest(latestRow(windows, [rawMeter()]));
      render(element());
      const section = quotaSection();
      expect(section.getAllByRole('listitem')).toHaveLength(1);
      expect(section.getByText('Period unknown')).not.toBeNull();
      expect(section.getByTestId('quota-freshness')).not.toBeNull();
      expect(section.queryByText('even pace', { exact: true })).toBeNull();
    },
  );

  it('keeps same-model meters with different known periods as separate rows', () => {
    setLatest(
      latestRow(
        [],
        [
          rawMeter({
            model_id: 'gemini-pro',
            bucket_id: null,
            known_period: '5h',
            reset_time_unix_secs: null,
          }),
          rawMeter({
            model_id: 'gemini-pro',
            bucket_id: null,
            known_period: '7d',
            reset_time_unix_secs: null,
          }),
        ],
      ),
    );
    render(element());
    const section = quotaSection();
    expect(section.getAllByRole('listitem')).toHaveLength(2);
    expect(
      section.getByRole('meter', { name: 'gemini-pro 5h reset unavailable' }),
    ).not.toBeNull();
    expect(
      section.getByRole('meter', { name: 'gemini-pro 7d reset unavailable' }),
    ).not.toBeNull();
  });

  it('updates raw, canonical and empty branches without remounting the route', () => {
    setLatest(latestRow(absentWindows('gemini')));
    const view = render(element());
    expect(quotaSection().getByText('Not offered')).not.toBeNull();
    setLatest(latestRow([quotaSnapshot()], [rawMeter()]));
    view.rerender(element());
    let section = quotaSection();
    const rows = section.getAllByRole('listitem');
    expect(rows[0].getAttribute('data-window')).toBe('gemini_5h');
    // The unreported weekly sibling stays as a neutral placeholder (rule A).
    expect(rows[1].getAttribute('data-window')).toBe('gemini_7d');
    expect(rows[1].getAttribute('data-placeholder')).toBe('no-reading');
    expect(rows[2].getAttribute('data-period-unknown')).toBe('gemini');
    expect(section.getByText('even pace', { exact: true })).not.toBeNull();
    setLatest(
      latestRow([quotaSnapshot({ utilization: 0, resets_at_unix_secs: null })]),
    );
    view.rerender(element());
    section = quotaSection();
    expect(section.getByText('Not started')).not.toBeNull();
    expect(section.queryByText('even pace', { exact: true })).toBeNull();
    setLatest(latestRow(absentWindows('gemini')));
    view.rerender(element());
    expect(quotaSection().getByText('Not offered')).not.toBeNull();
    expect(quotaSection().queryByTestId('quota-freshness')).toBeNull();
  });
});

describe('Gemini group placeholder in the detail pane and list row', () => {
  const weekly = quotaSnapshot({
    window: 'gemini_7d',
    utilization: 1,
    resets_at_unix_secs: NOW + 86_400,
  });
  function listFacts() {
    const facts = screen.getByTestId('upstream-row-quota');
    return {
      facts,
      cell: (window: string) => {
        const cell = facts.querySelector(`[data-window="${window}"]`);
        if (!(cell instanceof HTMLElement)) throw new Error(`${window} cell`);
        return cell;
      },
    };
  }

  it.each([
    [
      'weekly 100% with 5h absent (missing or disabled)',
      quotaSnapshot({
        window: 'gemini_5h',
        state: 'absent',
        utilization: null,
        resets_at_unix_secs: null,
      }),
      'not-reported',
      'Not reported',
    ],
    [
      'weekly 100% with 5h unobserved',
      quotaSnapshot({
        window: 'gemini_5h',
        state: 'unobserved',
        utilization: null,
        resets_at_unix_secs: null,
        observed_at_unix_millis: null,
      }),
      'no-reading',
      'No reading',
    ],
  ] as const)(
    'QA-1/3: %s keeps a neutral 5h placeholder',
    (_name, fiveHour, state, text) => {
      setLatest(
        latestRow([fiveHour, weekly, ...absentWindows('gemini').slice(2)]),
      );
      render(element());
      const section = quotaSection();
      const rows = section.getAllByRole('listitem');
      expect(rows.map((row) => row.getAttribute('data-window'))).toEqual([
        'gemini_5h',
        'gemini_7d',
      ]);
      expect(rows[0].getAttribute('data-placeholder')).toBe(state);
      expect(rows[0].textContent).toBe(`Gemini 5h${text}`);
      expect(rows[0].querySelector('[role="meter"]')).toBeNull();
      expect(rows[1].textContent).toContain('100% used');
      expect(section.getAllByRole('meter')).toHaveLength(1);
      expect(section.queryByText('Not offered')).toBeNull();

      const { cell } = listFacts();
      expect(cell('gemini_5h').getAttribute('data-placeholder')).toBe(state);
      expect(cell('gemini_5h').textContent).toBe(`Gemini 5h—${text}`);
      expect(cell('gemini_5h').getAttribute('aria-hidden')).toBeNull();
      expect(cell('gemini_7d').textContent).toBe('Gemini 7d100%');
    },
  );

  it('QA-2: both windows reported render normal meters without placeholders', () => {
    setLatest(
      latestRow([
        quotaSnapshot({ window: 'gemini_5h', utilization: 0.4 }),
        weekly,
      ]),
    );
    render(element());
    const section = quotaSection();
    expect(section.getAllByRole('meter')).toHaveLength(2);
    expect(document.querySelector('[data-placeholder]')).toBeNull();
  });

  it('QA-4: a group without readings stays hidden in the pane and list', () => {
    setLatest(
      latestRow([
        ...absentWindows('gemini').slice(0, 2),
        quotaSnapshot({ window: 'third_party_5h', utilization: 0.9 }),
        quotaSnapshot({ window: 'third_party_7d', utilization: 0.3 }),
      ]),
    );
    render(element());
    const section = quotaSection();
    expect(
      section
        .getAllByRole('listitem')
        .map((row) => row.getAttribute('data-window')),
    ).toEqual(['third_party_5h', 'third_party_7d']);
    expect(document.querySelector('[data-placeholder]')).toBeNull();
    const { facts } = listFacts();
    expect(facts.querySelector('[data-window="gemini_5h"]')).toBeNull();
    expect(facts.querySelector('[data-window="gemini_7d"]')).toBeNull();
  });
});
