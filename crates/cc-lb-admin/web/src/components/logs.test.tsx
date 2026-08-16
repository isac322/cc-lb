import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { filterLogRows } from '../lib/logRows';
import type { RequestEventWithPhase } from '../lib/RequestEventTypes';
import { Route } from '../routes/logs';

const queryMocks = vi.hoisted(() => ({
  fetchRecentEventsPage: vi.fn(),
  pageCache: new Map<string, unknown>(),
}));

const liveState = vi.hoisted(() => ({
  eventsMap: new Map(),
  version: 0,
}));

const routerMocks = vi.hoisted(() => ({ navigate: vi.fn() }));

const mockEvents = Array.from({ length: 120 }, (_, i) => ({
  request_id: `req-${i}`,
  thread_id: i % 2 === 0 ? 'session-a' : 'session-b',
  ts: (1_000 - i) / 1_000,
  ts_ms: 1_000 - i,
  model: 'claude-3',
  status: 200,
  tokens: 100,
  cost: 0.01,
}));
type TestPageParam =
  | { readonly kind: 'initial'; readonly limit: number }
  | {
      readonly kind: 'cursor';
      readonly limit: number;
      readonly ts_ms: number;
      readonly event_id: string;
    };

function makeRecentPage(
  filters: Record<string, string | undefined>,
  pageParam: TestPageParam,
) {
  const cacheKey = JSON.stringify([filters, pageParam]);
  const cached = queryMocks.pageCache.get(cacheKey);
  if (cached !== undefined) return cached;
  const filtered = filters.thread_id
    ? mockEvents.filter((event) => event.thread_id === filters.thread_id)
    : mockEvents;
  const start =
    pageParam.kind === 'initial'
      ? 0
      : filtered.findIndex(
          (event) =>
            event.ts_ms < pageParam.ts_ms ||
            (event.ts_ms === pageParam.ts_ms &&
              event.request_id < pageParam.event_id),
        );
  const pageStart = start < 0 ? filtered.length : start;
  const events = filtered.slice(pageStart, pageStart + pageParam.limit);
  const page = {
    events,
    observed: events.length > 0,
    count: events.length,
    limit: pageParam.limit,
  };
  queryMocks.pageCache.set(cacheKey, page);
  return page;
}

vi.mock('../lib/queries', () => ({
  getRecentEventsCursor: (event: {
    ts?: number | null;
    ts_ms?: number | null;
    event_id?: string;
    request_id: string;
  }) => ({
    ts_ms: event.ts_ms ?? (event.ts ?? 0) * 1_000,
    event_id: event.event_id ?? event.request_id,
  }),
  recentEventsPageQueryOptions: (
    filters: Record<string, string | undefined>,
    pageParam: TestPageParam,
  ) => ({
    queryKey: [
      'events',
      filters,
      'page',
      pageParam.limit,
      pageParam.kind === 'cursor' ? pageParam.ts_ms : null,
      pageParam.kind === 'cursor' ? pageParam.event_id : null,
    ],
    queryFn: () => queryMocks.fetchRecentEventsPage(filters, pageParam),
  }),
  useUpstreams: () => ({ data: { upstreams: [] } }),
  usePrincipalNameMap: () => new Map(),
  useUpstreamNameMap: () => new Map(),
  useRecentEventsPage: (
    filters: Record<string, string | undefined>,
    pageParam: TestPageParam,
  ) => ({
    data: makeRecentPage(filters, pageParam),
    isPlaceholderData: false,
    isPending: false,
    refetch: vi.fn(),
  }),
  useEventsHistogram: () => ({
    data: { buckets: [], bucket_ms: 60_000, bucket_count: 0 },
    isFetching: false,
    isError: false,
  }),
}));

vi.mock('../lib/useLiveEventStream', () => ({
  useLiveEventStream: () => ({
    eventsMap: liveState.eventsMap,
    version: liveState.version,
    status: 'idle',
    permanentFailure: false,
  }),
}));

vi.mock('@tanstack/react-router', async (importOriginal) => {
  const actual =
    await importOriginal<typeof import('@tanstack/react-router')>();
  return {
    ...actual,
    useNavigate: () => routerMocks.navigate,
  };
});

global.URL.createObjectURL = vi.fn(() => 'blob:test');
global.URL.revokeObjectURL = vi.fn();

global.IntersectionObserver = class IntersectionObserver {
  observe() {}
  unobserve() {}
  disconnect() {}
} as unknown as typeof global.IntersectionObserver;

describe('LogsPage', () => {
  beforeEach(() => {
    queryMocks.fetchRecentEventsPage.mockImplementation(
      async (
        filters: Record<string, string | undefined>,
        pageParam: TestPageParam,
      ) => makeRecentPage(filters, pageParam),
    );
  });

  afterEach(() => {
    cleanup();
    vi.restoreAllMocks();
    queryMocks.fetchRecentEventsPage.mockReset();
    queryMocks.pageCache.clear();
    liveState.eventsMap.clear();
    liveState.version = 0;
    routerMocks.navigate.mockReset();
  });

  it('applies every row filter individually and in combination', () => {
    const target = {
      ts: null,
      request_id: 'target',
      principal_id: 'principal-target',
      upstream_id: 'upstream-target',
      thread_id: 'thread-target',
      model: 'model-target',
      status: 429,
      duration_ms: 0,
      source_kind: 'renewal',
      _phase: 'final',
    } as RequestEventWithPhase;
    const rows = [
      target,
      {
        ...target,
        request_id: 'wrong-principal',
        principal_id: 'principal-other',
      },
      {
        ...target,
        request_id: 'wrong-upstream',
        upstream_id: 'upstream-other',
      },
      { ...target, request_id: 'wrong-session', thread_id: 'thread-other' },
      { ...target, request_id: 'wrong-model', model: 'other-model-target' },
      { ...target, request_id: 'wrong-status', status: 200 },
      { ...target, request_id: 'wrong-source', source_kind: 'request' },
    ] as RequestEventWithPhase[];

    for (const [filters, excludedRequestId] of [
      [
        { principal_id: 'principal-target', source_kind: 'all' },
        'wrong-principal',
      ],
      [
        { upstream_id: 'upstream-target', source_kind: 'all' },
        'wrong-upstream',
      ],
      [{ session: 'thread-target', source_kind: 'all' }, 'wrong-session'],
      [{ model: 'model-target', source_kind: 'all' }, 'wrong-model'],
      [{ model: 'MODEL-tar', source_kind: 'all' }, 'wrong-model'],
      [{ status: '4xx', source_kind: 'all' }, 'wrong-status'],
      [{ source_kind: 'renewal' }, 'wrong-source'],
    ] as const) {
      const requestIds = filterLogRows(rows, filters).map(
        (row) => row.request_id,
      );
      expect(requestIds).toContain('target');
      expect(requestIds).not.toContain(excludedRequestId);
    }

    expect(
      filterLogRows(rows, {
        principal_id: 'principal-target',
        upstream_id: 'upstream-target',
        session: 'thread-target',
        model: 'model-target',
        status: '4xx',
        source_kind: 'renewal',
      }).map((row) => row.request_id),
    ).toEqual(['target']);
  });

  it('renders Session select with an enforced w-64 and Clear button with h-9', async () => {
    const queryClient = new QueryClient();

    vi.spyOn(Route, 'useSearch').mockReturnValue({ session: '123' });

    const LogsPage = Route.options.component;
    if (LogsPage === undefined) {
      throw new Error('Expected logs route component');
    }

    render(
      <QueryClientProvider client={queryClient}>
        <LogsPage />
      </QueryClientProvider>,
    );

    const clearButton = await screen.findByRole('button', { name: /Clear/i });

    expect(clearButton.className).not.toContain('h-7');
    expect(clearButton.className).toContain('h-9');

    const sessionSelect = screen.getByText('123').closest('button');
    expect(sessionSelect).not.toBeNull();
    expect(sessionSelect?.className).toContain('!w-64');
  });

  it('commits the model filter once typing settles', () => {
    vi.useFakeTimers();
    try {
      const queryClient = new QueryClient();
      vi.spyOn(Route, 'useSearch').mockReturnValue({});

      const LogsPage = Route.options.component;
      if (LogsPage === undefined) {
        throw new Error('Expected logs route component');
      }

      render(
        <QueryClientProvider client={queryClient}>
          <LogsPage />
        </QueryClientProvider>,
      );

      const modelInput = screen.getByPlaceholderText('claude-sonnet-4-5');
      for (const value of ['c', 'cl', 'claude ']) {
        fireEvent.change(modelInput, { target: { value } });
        act(() => {
          vi.advanceTimersByTime(200);
        });
      }
      expect(routerMocks.navigate).not.toHaveBeenCalled();

      act(() => {
        vi.advanceTimersByTime(300);
      });
      expect(routerMocks.navigate).toHaveBeenCalledTimes(1);
      const { search } = routerMocks.navigate.mock.calls[0][0];
      expect(search({ status: '4xx' })).toEqual({
        status: '4xx',
        model: 'claude',
      });
    } finally {
      vi.useRealTimers();
    }
  });

  // This remains the page-render throughput outlier: prior shared-runner
  // measurements reached 7.2s under two pinned cores plus competing CPU load.
  // It now also awaits the intentional per-page fetch transitions; the 30s
  // budget preserves measured CI headroom rather than masking an async race.
  // Re-check that gap before budgeting another test in this file.
  it('paginates rows correctly and clamps on filter change', async () => {
    const queryClient = new QueryClient();
    let currentSearch: Record<string, string> = {};
    vi.spyOn(Route, 'useSearch').mockImplementation(() => currentSearch);

    const LogsPage = Route.options.component;
    if (LogsPage === undefined) {
      throw new Error('Expected logs route component');
    }

    const { rerender } = render(
      <QueryClientProvider client={queryClient}>
        <LogsPage />
      </QueryClientProvider>,
    );

    const getRowCount = () => {
      const tbody = document.querySelector('tbody');
      return tbody ? tbody.querySelectorAll('tr').length : 0;
    };

    expect(screen.getByText('Showing 1–50 of 50+')).toBeDefined();
    expect(screen.queryByText(/^Page /)).toBeNull();
    expect(getRowCount()).toBe(50);

    const scroller = document.querySelector<HTMLDivElement>(
      'div.flex-1.overflow-auto.min-h-0',
    );
    if (scroller === null) throw new Error('Expected log table scroller');
    scroller.scrollTop = 100;

    const nextBtn = screen.getByRole('button', { name: /Next page/i });
    fireEvent.click(nextBtn);
    await waitFor(() =>
      expect(screen.getByText('Showing 51–100 of 100+')).toBeDefined(),
    );
    expect(getRowCount()).toBe(50);
    expect(scroller.scrollTop).toBe(0);

    fireEvent.click(nextBtn);
    await waitFor(() =>
      expect(screen.getByText('Showing 101–120 of 120')).toBeDefined(),
    );
    expect(getRowCount()).toBe(20);

    const prevBtn = screen.getByRole('button', { name: /Previous page/i });
    fireEvent.click(prevBtn);
    await waitFor(() =>
      expect(screen.getByText('Showing 51–100 of 120')).toBeDefined(),
    );
    expect(getRowCount()).toBe(50);

    scroller.scrollTop = 100;
    currentSearch = { session: 'session-a' };
    rerender(
      <QueryClientProvider client={queryClient}>
        <LogsPage />
      </QueryClientProvider>,
    );

    await waitFor(() =>
      expect(screen.getByText('Showing 1–50 of 50+')).toBeDefined(),
    );
    expect(getRowCount()).toBe(50);
    expect(scroller.scrollTop).toBe(0);
  }, 30_000);

  it('keeps historical pages stable while live rows continue arriving', async () => {
    const queryClient = new QueryClient();
    vi.spyOn(Route, 'useSearch').mockReturnValue({});

    const LogsPage = Route.options.component;
    if (LogsPage === undefined) {
      throw new Error('Expected logs route component');
    }
    const { rerender } = render(
      <QueryClientProvider client={queryClient}>
        <LogsPage />
      </QueryClientProvider>,
    );

    fireEvent.click(screen.getByRole('button', { name: /Next page/i }));
    await waitFor(() =>
      expect(screen.getByLabelText('View request req-50')).toBeDefined(),
    );

    liveState.eventsMap.set('live-new', {
      phase: 'final',
      event: {
        ...mockEvents[0],
        request_id: 'live-new',
        ts: 2,
        ts_ms: 2_000,
      },
    });
    liveState.version = 1;
    rerender(
      <QueryClientProvider client={queryClient}>
        <LogsPage />
      </QueryClientProvider>,
    );

    expect(screen.getByLabelText('View request req-50')).toBeDefined();
    expect(screen.queryByLabelText('View request live-new')).toBeNull();
  });

  it('moves an arriving live row onto the first page and the displaced historical tail onto the next page', async () => {
    const queryClient = new QueryClient();
    vi.spyOn(Route, 'useSearch').mockReturnValue({});

    const LogsPage = Route.options.component;
    if (LogsPage === undefined) {
      throw new Error('Expected logs route component');
    }
    const { rerender } = render(
      <QueryClientProvider client={queryClient}>
        <LogsPage />
      </QueryClientProvider>,
    );

    expect(screen.queryByLabelText('View request live-new')).toBeNull();

    liveState.eventsMap.set('live-new', {
      phase: 'final',
      event: {
        ...mockEvents[0],
        request_id: 'live-new',
        ts: 2,
        ts_ms: 2_000,
      },
    });
    liveState.version = 1;
    rerender(
      <QueryClientProvider client={queryClient}>
        <LogsPage />
      </QueryClientProvider>,
    );

    expect(screen.getByLabelText('View request live-new')).toBeDefined();
    expect(screen.queryByLabelText('View request req-49')).toBeNull();

    fireEvent.click(screen.getByRole('button', { name: /Next page/i }));

    await waitFor(() =>
      expect(queryMocks.fetchRecentEventsPage).toHaveBeenCalledWith(
        {},
        {
          kind: 'cursor',
          limit: 50,
          ts_ms: mockEvents[48].ts_ms,
          event_id: mockEvents[48].request_id,
        },
      ),
    );
    expect(screen.getByLabelText('View request req-49')).toBeDefined();
  });

  it('exports all visible rows, not just the current page', async () => {
    const queryClient = new QueryClient();
    vi.spyOn(Route, 'useSearch').mockReturnValue({});

    const LogsPage = Route.options.component;
    if (LogsPage === undefined) {
      throw new Error('Expected logs route component');
    }

    render(
      <QueryClientProvider client={queryClient}>
        <LogsPage />
      </QueryClientProvider>,
    );

    const nextButton = screen.getByRole('button', { name: /Next page/i });
    fireEvent.click(nextButton);
    await waitFor(() =>
      expect(screen.getByText('Showing 51–100 of 100+')).toBeDefined(),
    );
    fireEvent.click(nextButton);
    await waitFor(() =>
      expect(screen.getByText('Showing 101–120 of 120')).toBeDefined(),
    );

    const exportBtn = screen.getByRole('button', { name: /Export/i });

    let capturedBlob: Blob | undefined;
    vi.spyOn(URL, 'createObjectURL').mockImplementation((object) => {
      if (!(object instanceof Blob)) throw new Error('Expected Blob export');
      capturedBlob = object;
      return 'blob:test';
    });

    const clickSpy = vi
      .spyOn(HTMLAnchorElement.prototype, 'click')
      .mockImplementation(() => {});

    fireEvent.click(exportBtn);

    expect(clickSpy).toHaveBeenCalled();
    expect(capturedBlob).toBeDefined();

    if (capturedBlob === undefined) throw new Error('Expected exported Blob');
    const text = await capturedBlob.text();
    const exportedRows: unknown = JSON.parse(text);
    expect(Array.isArray(exportedRows)).toBe(true);
    if (Array.isArray(exportedRows)) expect(exportedRows.length).toBe(120);

    const tbody = document.querySelector('tbody');
    expect(tbody?.querySelectorAll('tr').length).toBe(20);
  });
});
