import {
  keepPreviousData,
  QueryClient,
  QueryClientProvider,
} from '@tanstack/react-query';
import {
  createMemoryHistory,
  createRootRouteWithContext,
  createRouter,
  defaultStringifySearch,
  Outlet,
  RouterProvider,
} from '@tanstack/react-router';
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from '@testing-library/react';
import { useSyncExternalStore } from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { RecentEventsPayload, RequestEvent } from '../lib/api';
import { filterLogRows } from '../lib/logRows';
import type * as queries from '../lib/queries';
import type { RequestEventWithPhase } from '../lib/RequestEventTypes';
import type { LiveEventMap } from '../lib/upsertReducer';
import { Route } from '../routes/logs';

const queryMocks = vi.hoisted(() => ({
  fetchRecentEventsPage: vi.fn(),
}));

const liveState = vi.hoisted(() => ({
  eventsMap: new Map() as LiveEventMap,
  listeners: new Set<() => void>(),
  version: 0,
}));

const queryClients: QueryClient[] = [];

const mockEvents = Array.from({ length: 120 }, (_, i) => ({
  request_id: `req-${i}`,
  thread_id: i % 2 === 0 ? 'session-a' : 'session-b',
  ts: (1_000 - i) / 1_000,
  ts_ms: 1_000 - i,
  model: 'claude-3',
  event_kind: 'messages' as const,
  status: 200,
  duration_ms: 10,
  input_tokens: 100,
  cost_usd_micros: 10_000,
}));

function applyRecentFilters(
  events: readonly RequestEvent[],
  filters: Record<string, string | undefined>,
): RequestEvent[] {
  // Mirrors the backend model contract (`model_filter_matches`): lowercase the
  // needle minus a leading `claude-`; a row matches when its model starts with
  // the core or is a `claude-*` id containing it. An absent core means no
  // filter; a present one never matches a model-less row.
  const core = filters.model
    ?.trim()
    .toLowerCase()
    .replace(/^claude-/, '');
  return events.filter((event) => {
    if (filters.thread_id && event.thread_id !== filters.thread_id) {
      return false;
    }
    if (core) {
      const model = event.model?.toLowerCase() ?? '';
      const matches =
        model.startsWith(core) ||
        (model.startsWith('claude-') && model.slice(7).includes(core));
      if (!matches) return false;
    }
    if (
      filters.status_class &&
      filters.status_class !== `${Math.floor(event.status / 100)}xx`
    ) {
      return false;
    }
    return true;
  });
}

function makeRecentPage(
  filters: Record<string, string | undefined>,
  pageParam: queries.RecentEventsPageParam,
  sourceEvents: readonly RequestEvent[] = mockEvents,
): RecentEventsPayload {
  const filtered = applyRecentFilters(sourceEvents, filters);
  const start =
    pageParam.kind === 'initial'
      ? 0
      : filtered.findIndex((event) => {
          const eventTsMs = event.ts_ms ?? (event.ts ?? 0) * 1000;
          return (
            eventTsMs < pageParam.ts_ms ||
            (eventTsMs === pageParam.ts_ms &&
              event.request_id < pageParam.event_id)
          );
        });
  const pageStart = start < 0 ? filtered.length : start;
  const pageEvents = filtered.slice(pageStart, pageStart + pageParam.limit);
  return {
    events: pageEvents,
    observed: pageEvents.length > 0,
    count: pageEvents.length,
    limit: pageParam.limit,
  };
}

vi.mock('../lib/queries', async () => {
  const actual = await vi.importActual<typeof queries>('../lib/queries');
  return {
    ...actual,
    useUpstreams: () => ({ data: { upstreams: [] } }),
    usePrincipalNameMap: () => new Map(),
    useEventsHistogram: () => ({
      data: { buckets: [], bucket_ms: 60_000, bucket_count: 0 },
      isFetching: false,
      isError: false,
    }),
  };
});

vi.mock('../lib/useLiveEventStream', () => {
  const subscribe = (listener: () => void) => {
    liveState.listeners.add(listener);
    return () => liveState.listeners.delete(listener);
  };
  const getSnapshot = () => liveState.version;
  return {
    useLiveEventStream: () => ({
      eventsMap: liveState.eventsMap,
      version: useSyncExternalStore(subscribe, getSnapshot, getSnapshot),
      status: 'idle',
      permanentFailure: false,
    }),
  };
});

global.URL.createObjectURL = vi.fn(() => 'blob:test');
global.URL.revokeObjectURL = vi.fn();

global.IntersectionObserver = class IntersectionObserver {
  observe() {}
  unobserve() {}
  disconnect() {}
} as unknown as typeof global.IntersectionObserver;

async function renderLogs(search: Record<string, string> = {}) {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: {
        retry: false,
        placeholderData: keepPreviousData,
      },
    },
  });
  queryClients.push(queryClient);
  const rootRoute = createRootRouteWithContext<{
    queryClient: QueryClient;
  }>()({ component: Outlet });
  // File routes receive this metadata from routeTree.gen.ts in the app.
  Object.assign(Route.options, {
    id: '/logs',
    path: '/logs',
    getParentRoute: () => rootRoute,
  });
  const searchStr = defaultStringifySearch(search);
  const router = createRouter({
    routeTree: rootRoute.addChildren([Route]),
    context: { queryClient },
    history: createMemoryHistory({
      initialEntries: [`/logs${searchStr}`],
    }),
  });
  await router.load();
  const view = render(
    <QueryClientProvider client={queryClient}>
      <RouterProvider router={router} />
    </QueryClientProvider>,
  );
  await screen.findByRole('heading', { name: 'Logs', level: 1 });
  return { ...view, router };
}

function rowIds(table: HTMLElement) {
  return Array.from(
    table.querySelectorAll<HTMLTableRowElement>(
      'tbody tr[aria-label^="View request "]',
    ),
    (row) => row.getAttribute('aria-label'),
  );
}

function expectedRows(events: readonly RequestEvent[]) {
  return events.map((event) => `View request ${event.request_id}`);
}

function publishLiveEvent(event: RequestEvent) {
  act(() => {
    liveState.eventsMap.set(event.event_id ?? event.request_id, {
      phase: 'final',
      event,
    });
    liveState.version += 1;
    for (const listener of liveState.listeners) listener();
  });
}

describe('LogsPage', () => {
  beforeEach(() => {
    queryMocks.fetchRecentEventsPage.mockImplementation(makeRecentPage);
    vi.stubGlobal(
      'fetch',
      vi.fn(async (input: RequestInfo | URL) => {
        const url = new URL(String(input), 'http://localhost');
        if (url.pathname !== '/admin/v1/events/recent') {
          throw new Error(`Unexpected request: ${url.pathname}`);
        }
        const { limit, until_ts_ms, until_event_id, ...filters } =
          Object.fromEntries(url.searchParams);
        const pageParam: queries.RecentEventsPageParam =
          until_ts_ms === undefined || until_event_id === undefined
            ? { kind: 'initial', limit: Number(limit) }
            : {
                kind: 'cursor',
                limit: Number(limit),
                ts_ms: Number(until_ts_ms),
                event_id: until_event_id,
              };
        return new Response(
          JSON.stringify(
            await queryMocks.fetchRecentEventsPage(filters, pageParam),
          ),
          { headers: { 'Content-Type': 'application/json' } },
        );
      }),
    );
  });

  afterEach(() => {
    cleanup();
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
    queryMocks.fetchRecentEventsPage.mockReset();
    for (const queryClient of queryClients) queryClient.clear();
    queryClients.length = 0;
    liveState.eventsMap.clear();
    liveState.listeners.clear();
    liveState.version = 0;
    vi.useRealTimers();
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
      event_kind: 'renewal',
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
      {
        ...target,
        request_id: 'wrong-kind',
        source_kind: 'proxy',
        event_kind: 'messages',
      },
    ] as RequestEventWithPhase[];

    for (const [filters, excludedRequestId] of [
      [
        { principal_id: 'principal-target', event_kind: 'renewal' },
        'wrong-principal',
      ],
      [
        { upstream_id: 'upstream-target', event_kind: 'renewal' },
        'wrong-upstream',
      ],
      [{ session: 'thread-target', event_kind: 'renewal' }, 'wrong-session'],
      [{ model: 'model-target', event_kind: 'renewal' }, 'wrong-model'],
      [{ model: 'MODEL-tar', event_kind: 'renewal' }, 'wrong-model'],
      [{ status: '4xx', event_kind: 'renewal' }, 'wrong-status'],
      [{ event_kind: 'renewal' }, 'wrong-kind'],
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
        event_kind: 'renewal',
      }).map((row) => row.request_id),
    ).toEqual(['target']);
  });

  it.each([
    ['sonnet', ['sonnet-modern', 'sonnet-legacy', 'sonnet-bare']],
    ['SONNET', ['sonnet-modern', 'sonnet-legacy', 'sonnet-bare']],
    [' sonnet-4-5 ', ['sonnet-modern', 'sonnet-bare']],
    ['3-5-sonnet', ['sonnet-legacy']],
    ['claude-sonnet', ['sonnet-modern', 'sonnet-legacy', 'sonnet-bare']],
    ['claude-3-5-sonnet-20241022', ['sonnet-legacy']],
    ['sonnet-modern', []],
  ] as const)(
    'filters models without requiring the vendor prefix: %s',
    (model, expected) => {
      const rows = [
        { request_id: 'sonnet-modern', model: 'claude-sonnet-4-5-20250929' },
        { request_id: 'sonnet-legacy', model: 'claude-3-5-sonnet-20241022' },
        { request_id: 'sonnet-bare', model: 'Sonnet-4-5' },
        { request_id: 'other', model: 'claude-opus-4-5-20251101' },
        { request_id: 'model-less', model: null },
      ] as RequestEventWithPhase[];

      expect(
        filterLogRows(rows, { model }).map((row) => row.request_id),
      ).toEqual(expected);
      // A whitespace needle and a bare vendor prefix are both absent filters.
      for (const absent of ['', '  ', 'claude-', 'CLAUDE- ']) {
        expect(filterLogRows(rows, { model: absent })).toHaveLength(
          rows.length,
        );
      }
    },
  );

  it('filters arriving live rows with the same fuzzy model predicate', async () => {
    const events = [
      {
        ts: 5,
        ts_ms: 5_000,
        request_id: 'historical-sonnet',
        model: 'claude-3-5-sonnet-20241022',
        status: 200,
        duration_ms: 10,
        event_kind: 'messages' as const,
      },
    ];
    queryMocks.fetchRecentEventsPage.mockImplementation((filters, pageParam) =>
      makeRecentPage(filters, pageParam, events),
    );
    await renderLogs({ model: 'sonnet' });

    const table = screen.getByRole('table');
    await waitFor(() =>
      expect(rowIds(table)).toEqual(['View request historical-sonnet']),
    );

    publishLiveEvent({
      ...mockEvents[0],
      request_id: 'live-sonnet',
      model: 'claude-sonnet-4-5-20250929',
      ts: 6,
      ts_ms: 6_000,
    });
    publishLiveEvent({
      ...mockEvents[0],
      request_id: 'live-other',
      model: 'other-vendor-model',
      ts: 7,
      ts_ms: 7_000,
    });

    await waitFor(() =>
      expect(rowIds(table)).toEqual([
        'View request live-sonnet',
        'View request historical-sonnet',
      ]),
    );
  });

  it('shows applied filters as removable chips behind a collapsed panel', async () => {
    const { router } = await renderLogs({ session: '123', status: '4xx' });
    const filtersButton = screen.getByRole('button', { name: /^Filters/ });
    expect(filtersButton.getAttribute('aria-expanded')).toBe('false');
    expect(filtersButton.textContent).toContain('2');

    fireEvent.click(
      screen.getByRole('button', { name: 'Remove session filter' }),
    );
    await waitFor(() => {
      expect(router.state.location.search.session).toBeUndefined();
      expect(
        screen.queryByRole('button', { name: 'Remove session filter' }),
      ).toBeNull();
    });
    expect(router.state.location.search.status).toBe('4xx');
    fireEvent.click(filtersButton);
    expect(filtersButton.getAttribute('aria-expanded')).toBe('true');
  });

  it('commits the model filter once typing settles', async () => {
    const { router } = await renderLogs({ status: '4xx' });
    const table = screen.getByRole('table');
    await waitFor(() => expect(table.querySelector('tbody h2')).not.toBeNull());
    vi.useFakeTimers();
    fireEvent.click(screen.getByRole('button', { name: /^Filters/ }));
    const modelInput = screen.getByRole('textbox', { name: 'Model' });
    for (const value of ['c', 'cl', 'claude ']) {
      fireEvent.change(modelInput, { target: { value } });
      await act(async () => {
        await vi.advanceTimersByTimeAsync(200);
      });
    }
    expect(router.state.location.search.model).toBeUndefined();
    expect(
      queryMocks.fetchRecentEventsPage.mock.calls.filter(
        ([filters]) => filters.model !== undefined,
      ),
    ).toEqual([]);

    await act(async () => {
      await vi.advanceTimersByTimeAsync(300);
    });
    expect(router.state.location.search).toMatchObject({
      status: '4xx',
      model: 'claude',
    });
    expect(
      queryMocks.fetchRecentEventsPage.mock.calls
        .map(([filters]) => filters.model)
        .filter((model) => model !== undefined),
    ).toEqual(['claude']);
  });

  it('paginates row identities and resets the page on a filter change', async () => {
    const { router } = await renderLogs();
    const table = screen.getByRole('table');
    await waitFor(() =>
      expect(rowIds(table)).toEqual(expectedRows(mockEvents.slice(0, 50))),
    );
    const prevBtn = screen.getByRole('button', {
      name: 'Previous page',
    }) as HTMLButtonElement;
    const nextBtn = screen.getByRole('button', {
      name: 'Next page',
    }) as HTMLButtonElement;
    expect(prevBtn.disabled).toBe(true);
    const scroller = table.closest<HTMLDivElement>(
      'div.flex-1.overflow-auto.min-h-0',
    );
    if (scroller === null) throw new Error('Expected log table scroller');
    scroller.scrollTop = 100;
    fireEvent.click(nextBtn);
    await waitFor(() =>
      expect(rowIds(table)).toEqual(expectedRows(mockEvents.slice(50, 100))),
    );
    expect(scroller.scrollTop).toBe(0);

    fireEvent.click(nextBtn);
    await waitFor(() =>
      expect(rowIds(table)).toEqual(expectedRows(mockEvents.slice(100))),
    );
    expect(nextBtn.disabled).toBe(true);
    scroller.scrollTop = 100;
    fireEvent.click(prevBtn);
    expect(rowIds(table)).toEqual(expectedRows(mockEvents.slice(50, 100)));
    expect(nextBtn.disabled).toBe(false);
    expect(scroller.scrollTop).toBe(0);

    scroller.scrollTop = 100;
    fireEvent.click(nextBtn);
    expect(rowIds(table)).toEqual(expectedRows(mockEvents.slice(100)));
    expect(scroller.scrollTop).toBe(0);
    scroller.scrollTop = 100;
    await act(async () => {
      await router.navigate({
        to: '/logs',
        search: { session: 'session-a' },
        resetScroll: false,
      });
    });
    const sessionRows = mockEvents.filter(
      (event) => event.thread_id === 'session-a',
    );
    await waitFor(() =>
      expect(rowIds(table)).toEqual(expectedRows(sessionRows.slice(0, 50))),
    );
    expect(prevBtn.disabled).toBe(true);
    expect(nextBtn.disabled).toBe(false);
    expect(scroller.scrollTop).toBe(100);

    fireEvent.click(nextBtn);
    await waitFor(() =>
      expect(rowIds(table)).toEqual(expectedRows(sessionRows.slice(50))),
    );
    expect(nextBtn.disabled).toBe(true);
    fireEvent.click(prevBtn);
    expect(rowIds(table)).toEqual(expectedRows(sessionRows.slice(0, 50)));
  }, 30_000);

  it('keeps historical pages stable while live rows continue arriving', async () => {
    await renderLogs();
    const table = screen.getByRole('table');
    await waitFor(() =>
      expect(rowIds(table)).toEqual(expectedRows(mockEvents.slice(0, 50))),
    );
    fireEvent.click(screen.getByRole('button', { name: 'Next page' }));
    const historicalPage = expectedRows(mockEvents.slice(50, 100));
    await waitFor(() => expect(rowIds(table)).toEqual(historicalPage));

    publishLiveEvent({
      ...mockEvents[0],
      request_id: 'live-new',
      ts: 2,
      ts_ms: 2_000,
    });
    expect(rowIds(table)).toEqual(historicalPage);
  });

  it('moves an arriving live row onto the first page and the displaced historical tail onto the next page', async () => {
    await renderLogs();
    const table = screen.getByRole('table');
    await waitFor(() =>
      expect(rowIds(table)).toEqual(expectedRows(mockEvents.slice(0, 50))),
    );
    publishLiveEvent({
      ...mockEvents[0],
      request_id: 'live-new',
      ts: 2,
      ts_ms: 2_000,
    });
    expect(rowIds(table)).toEqual([
      'View request live-new',
      ...expectedRows(mockEvents.slice(0, 49)),
    ]);
    fireEvent.click(screen.getByRole('button', { name: 'Next page' }));
    await waitFor(() =>
      expect(rowIds(table)).toEqual(expectedRows(mockEvents.slice(49, 99))),
    );
    expect(queryMocks.fetchRecentEventsPage).toHaveBeenLastCalledWith(
      { event_kind: 'messages' },
      {
        kind: 'cursor',
        limit: 50,
        ts_ms: mockEvents[48].ts_ms,
        event_id: mockEvents[48].request_id,
      },
    );
  });

  it('exports all retained rows, not just the current page', async () => {
    await renderLogs();
    const table = screen.getByRole('table');
    await waitFor(() =>
      expect(rowIds(table)).toEqual(expectedRows(mockEvents.slice(0, 50))),
    );
    const nextButton = screen.getByRole('button', { name: 'Next page' });
    fireEvent.click(nextButton);
    await waitFor(() =>
      expect(rowIds(table)).toEqual(expectedRows(mockEvents.slice(50, 100))),
    );
    fireEvent.click(nextButton);
    await waitFor(() =>
      expect(rowIds(table)).toEqual(expectedRows(mockEvents.slice(100))),
    );
    let capturedBlob: Blob | undefined;
    vi.spyOn(URL, 'createObjectURL').mockImplementation((object) => {
      if (!(object instanceof Blob)) throw new Error('Expected Blob export');
      capturedBlob = object;
      return 'blob:test';
    });
    vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(() => {});
    fireEvent.click(screen.getByRole('button', { name: /Export/i }));
    if (capturedBlob === undefined) throw new Error('Expected exported Blob');
    const exportedRows: RequestEvent[] = JSON.parse(await capturedBlob.text());
    expect(exportedRows.map((row) => row.request_id)).toEqual(
      mockEvents.map((row) => row.request_id),
    );
    expect(rowIds(table)).toEqual(expectedRows(mockEvents.slice(100)));
  });
});
