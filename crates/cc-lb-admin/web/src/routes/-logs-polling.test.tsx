import {
  keepPreviousData,
  QueryClient,
  QueryClientProvider,
} from '@tanstack/react-query';
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from '@testing-library/react';
import type React from 'react';
import { Suspense, startTransition } from 'react';
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest';
import type * as RequestEventsTableModule from '../components/ui/RequestEventsTable';
import type { EventsHistogramPayload, RecentEventsPayload } from '../lib/api';
import * as queries from '../lib/queries';
import { useEventsHistogram } from '../lib/queries';
import type { LiveEventMap } from '../lib/upsertReducer';
import { Route } from './logs';

interface HistogramResult {
  data: EventsHistogramPayload | undefined;
  isError: boolean;
  isFetching: boolean;
  isPending: boolean;
}

interface RecentResult {
  data: RecentEventsPayload | undefined;
  isPlaceholderData: boolean;
  isPending: boolean;
  refetch: () => unknown;
}

const routeState = vi.hoisted(() => ({
  cursorRecent: undefined as unknown as RecentResult,
  histogram: undefined as unknown as HistogramResult,
  live: {
    eventsMap: new Map() as LiveEventMap,
    forceReconnect: vi.fn(),
    permanentFailure: false,
    permanentFailureSince: null,
    reconnectAttempts: 0,
    status: 'idle' as const,
    version: 0,
  },
  navigate: vi.fn(),
  nextPage: undefined as unknown as
    | RecentEventsPayload
    | Promise<RecentEventsPayload>,
  recent: undefined as unknown as RecentResult,
  renderRealTable: false,
  recentCalls: [] as Array<{
    filters: Record<string, string | undefined>;
    pageParam: queries.RecentEventsPageParam;
  }>,
  histogramCalls: [] as Array<Record<string, string | undefined>>,
  liveCalls: [] as Array<Record<string, string | undefined>>,
  search: {} as Record<string, string | number | undefined>,
  suspendInitialForPrincipal: undefined as string | undefined,
  suspendedInitialPromise: new Promise<never>(() => {}),
}));

vi.mock('@tanstack/react-router', async () => {
  const actual = await vi.importActual<Record<string, unknown>>(
    '@tanstack/react-router',
  );
  return {
    ...actual,
    useNavigate: () => routeState.navigate,
  };
});

vi.mock('../lib/queries', async () => {
  const actual = await vi.importActual<typeof queries>('../lib/queries');
  return {
    ...actual,
    getRecentEventsCursor: (event: {
      event_id?: string;
      request_id: string;
      ts?: number | null;
      ts_ms?: number | null;
    }) => ({
      event_id: event.event_id ?? event.request_id,
      ts_ms: event.ts_ms ?? (event.ts ?? 0) * 1_000,
    }),
    recentEventsPageQueryOptions: vi.fn(
      (
        filters: Record<string, string | undefined>,
        pageParam: queries.RecentEventsPageParam,
      ) => ({
        queryKey: ['logs-page-test', filters, pageParam],
        queryFn: async () => routeState.nextPage,
      }),
    ),
    useEventsHistogram: vi.fn((filters: Record<string, string | undefined>) => {
      routeState.histogramCalls.push(filters);
      return routeState.histogram;
    }),
    usePrincipalNameMap: () => new Map(),
    useRecentEventsPage: (
      filters: Record<string, string | undefined>,
      pageParam: queries.RecentEventsPageParam,
    ) => {
      routeState.recentCalls.push({ filters, pageParam });
      if (
        routeState.suspendInitialForPrincipal !== undefined &&
        pageParam.kind === 'initial' &&
        filters.principal_id === routeState.suspendInitialForPrincipal
      ) {
        routeState.suspendInitialForPrincipal = undefined;
        throw routeState.suspendedInitialPromise;
      }
      return pageParam.kind === 'cursor'
        ? routeState.cursorRecent
        : routeState.recent;
    },
    useUpstreams: () => ({ data: { upstreams: [] } }),
  };
});

vi.mock('../lib/useLiveEventStream', () => ({
  useLiveEventStream: (filters: Record<string, string | undefined>) => {
    routeState.liveCalls.push(filters);
    return routeState.live;
  },
}));

vi.mock('../components/ui/RequestEventsTable', async (importOriginal) => {
  const actual = await importOriginal<typeof RequestEventsTableModule>();
  return {
    ...actual,
    RequestEventsTable: (
      props: React.ComponentProps<typeof actual.RequestEventsTable>,
    ) => {
      if (routeState.renderRealTable) {
        return <actual.RequestEventsTable {...props} />;
      }
      const { events, liveFlashIds, loading } = props;
      return (
        <div data-loading={loading ? 'true' : 'false'} data-testid="logs-table">
          {events.map((event) => {
            const id = event.event_id ?? event.request_id;
            return (
              <span
                data-flash={liveFlashIds?.has(id) ? 'true' : 'false'}
                data-testid="logs-row"
                key={id}
              >
                {id}
              </span>
            );
          })}
        </div>
      );
    },
  };
});
const LogsPage = Route.options.component as React.ComponentType;

const actualQueries = await vi.importActual<typeof queries>('../lib/queries');

/** The real query hook, run against a stubbed fetch. */
function useRealEventsHistogram(
  ...args: Parameters<typeof actualQueries.useEventsHistogram>
) {
  routeState.histogramCalls.push(args[0]);
  return actualQueries.useEventsHistogram(...args);
}

function page(requestId?: string, principalId = 'principal-a') {
  const events = requestId
    ? [
        {
          ts: 1_700_000_000,
          request_id: requestId,
          principal_id: principalId,
          status: 200,
          duration_ms: 25,
          event_kind: 'messages' as const,
        },
      ]
    : [];
  return {
    events,
    observed: events.length > 0,
    count: events.length,
    limit: 50,
  };
}

function pageFromEvents(events: RecentEventsPayload['events']) {
  return {
    events,
    observed: events.length > 0,
    count: events.length,
    limit: 50,
  };
}

function fullPage(prefix: string, principalId = 'principal-a') {
  return pageFromEvents(
    Array.from({ length: 50 }, (_, index) => ({
      ts: 1_700_000_000 - index,
      request_id: `${prefix}-${index + 1}`,
      principal_id: principalId,
      status: 200,
      duration_ms: 25,
      event_kind: 'messages' as const,
    })),
  );
}

function renderLogs(
  queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  }),
) {
  const view = render(
    <QueryClientProvider client={queryClient}>
      <Suspense fallback={<div data-testid="logs-suspended" />}>
        <LogsPage />
      </Suspense>
    </QueryClientProvider>,
  );
  return {
    ...view,
    rerenderLogs: () =>
      view.rerender(
        <QueryClientProvider client={queryClient}>
          <Suspense fallback={<div data-testid="logs-suspended" />}>
            <LogsPage />
          </Suspense>
        </QueryClientProvider>,
      ),
  };
}

describe('logs polling surfaces', () => {
  beforeEach(() => {
    vi.useFakeTimers({
      toFake: ['Date'],
      now: new Date('2026-09-07T12:00:00.000Z'),
    });
    vi.clearAllMocks();
    routeState.live = {
      eventsMap: new Map(),
      forceReconnect: vi.fn(),
      permanentFailure: false,
      permanentFailureSince: null,
      reconnectAttempts: 0,
      status: 'idle',
      version: 0,
    };
    routeState.recentCalls = [];
    routeState.histogramCalls = [];
    vi.mocked(useEventsHistogram).mockReset();
    routeState.liveCalls = [];
    routeState.search = {};
    routeState.suspendInitialForPrincipal = undefined;
    routeState.renderRealTable = false;
    routeState.suspendedInitialPromise = new Promise<never>(() => {});
    vi.spyOn(Route, 'useSearch').mockImplementation(
      () => routeState.search as never,
    );
    routeState.recent = {
      data: page(),
      isPlaceholderData: false,
      isPending: false,
      refetch: vi.fn(),
    };
    routeState.cursorRecent = {
      data: page(),
      isPlaceholderData: false,
      isPending: false,
      refetch: vi.fn(),
    };
    routeState.nextPage = page();
    routeState.histogram = {
      data: undefined,
      isError: false,
      isFetching: true,
      isPending: true,
    };
  });

  afterEach(() => {
    cleanup();
    vi.restoreAllMocks();
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  test('blocks only the first histogram load and keeps refresh states undimmed', () => {
    const view = renderLogs();

    expect(screen.getByTestId('time-range-strip-loading')).toBeDefined();
    expect(screen.queryByTestId('time-range-strip-error')).toBeNull();

    routeState.histogram = {
      data: { buckets: [], bucket_count: 0, bucket_ms: 60_000 },
      isError: false,
      isFetching: false,
      isPending: false,
    };
    view.rerenderLogs();

    expect(screen.queryByTestId('time-range-strip-loading')).toBeNull();
    expect(screen.queryByTestId('time-range-strip-error')).toBeNull();

    const successfulData = {
      buckets: [
        {
          bucket_start_unix_secs: 1_700_000_000,
          total_count: 4,
          error_count: 1,
        },
      ],
      bucket_count: 1,
      bucket_ms: 60_000,
    };
    routeState.histogram = {
      data: successfulData,
      isError: false,
      isFetching: true,
      isPending: false,
    };
    view.rerenderLogs();

    expect(screen.queryByTestId('time-range-strip-loading')).toBeNull();
    expect(screen.getByRole('img', { name: /Request density/ })).toBeDefined();

    routeState.histogram = {
      data: successfulData,
      isError: true,
      isFetching: false,
      isPending: false,
    };
    view.rerenderLogs();

    const refreshError = screen.getByTestId('time-range-strip-error');
    expect(refreshError.className).toContain('right-2');
    expect(refreshError.className).not.toContain('inset-0');
    expect(screen.queryByTestId('time-range-strip-loading')).toBeNull();
    expect(screen.getByRole('img', { name: /Request density/ })).toBeDefined();

    routeState.histogram = {
      data: undefined,
      isError: true,
      isFetching: false,
      isPending: false,
    };
    view.rerenderLogs();

    expect(screen.getByText('Failed to load request density')).toBeDefined();
    expect(screen.queryByTestId('time-range-strip-loading')).toBeNull();
  });

  test.each(['principal_id', 'upstream_id'] as const)(
    'does not show placeholder rows or an empty state across a %s change',
    async (filterKey) => {
      const scope = filterKey === 'principal_id' ? 'principal' : 'upstream';
      const firstEvent = {
        ts: 1_700_000_000,
        request_id: `${scope}-a-request`,
        principal_id: 'principal-a',
        upstream_id: 'upstream-a',
        status: 200,
        duration_ms: 25,
        event_kind: 'messages' as const,
      };
      const secondEvent = {
        ...firstEvent,
        request_id: `${scope}-b-request`,
        principal_id: 'principal-b',
        upstream_id: 'upstream-b',
      };
      const requests: Array<{
        url: URL;
        resolve: (response: Response) => void;
      }> = [];
      vi.stubGlobal(
        'fetch',
        vi.fn(
          (input: RequestInfo | URL) =>
            new Promise<Response>((resolve) => {
              requests.push({
                url: new URL(String(input), 'http://localhost'),
                resolve,
              });
            }),
        ),
      );
      vi.spyOn(queries, 'useRecentEventsPage').mockImplementation(
        actualQueries.useRecentEventsPage,
      );
      routeState.renderRealTable = true;
      routeState.search = { [filterKey]: `${scope}-a` };
      routeState.histogram = {
        data: { buckets: [], bucket_count: 0, bucket_ms: 60_000 },
        isError: false,
        isFetching: false,
        isPending: false,
      };
      const queryClient = new QueryClient({
        defaultOptions: {
          queries: { retry: false, placeholderData: keepPreviousData },
        },
      });
      const view = renderLogs(queryClient);
      const pagination = () =>
        screen.getByRole('navigation', { name: 'Log pagination' });
      const emptyHeading = () =>
        within(screen.getByRole('table')).queryByRole('heading', { level: 2 });
      const resolvePage = async (
        request: (typeof requests)[number],
        events: RecentEventsPayload['events'],
      ) => {
        await act(async () => {
          request.resolve(
            new Response(JSON.stringify(pageFromEvents(events)), {
              headers: { 'Content-Type': 'application/json' },
            }),
          );
        });
      };

      await waitFor(() => expect(requests).toHaveLength(1));
      expect(pagination().getAttribute('aria-busy')).toBe('true');
      expect(emptyHeading()).toBeNull();
      await resolvePage(requests[0], [firstEvent]);
      await waitFor(() =>
        expect(
          screen.getByLabelText(`View request ${firstEvent.request_id}`),
        ).toBeDefined(),
      );

      // The stream may still expose its old map until its scope-reset effect runs.
      routeState.live.eventsMap.set(firstEvent.request_id, {
        phase: 'final',
        event: firstEvent,
      });
      routeState.live.version = 1;
      routeState.search = { [filterKey]: `${scope}-b` };
      view.rerenderLogs();

      await waitFor(() => expect(requests).toHaveLength(2));
      expect(requests[1].url.searchParams.get(filterKey)).toBe(`${scope}-b`);
      expect(screen.queryAllByLabelText(/^View request /)).toEqual([]);
      expect(pagination().getAttribute('aria-busy')).toBe('true');
      expect(emptyHeading()).toBeNull();

      await resolvePage(requests[1], [secondEvent]);
      await waitFor(() =>
        expect(
          screen.getByLabelText(`View request ${secondEvent.request_id}`),
        ).toBeDefined(),
      );
      expect(
        screen.queryByLabelText(`View request ${firstEvent.request_id}`),
      ).toBeNull();
      expect(pagination().getAttribute('aria-busy')).toBe('false');

      routeState.search = { [filterKey]: `${scope}-empty` };
      view.rerenderLogs();
      await waitFor(() => expect(requests).toHaveLength(3));
      expect(screen.queryAllByLabelText(/^View request /)).toEqual([]);
      expect(pagination().getAttribute('aria-busy')).toBe('true');
      expect(emptyHeading()).toBeNull();

      await resolvePage(requests[2], []);
      await waitFor(() => expect(emptyHeading()).not.toBeNull());
      expect(screen.queryAllByLabelText(/^View request /)).toEqual([]);
      expect(pagination().getAttribute('aria-busy')).toBe('false');
      view.unmount();
      queryClient.clear();
    },
  );

  test('uses the initial cursor and keeps scroll for a changed filter identity', async () => {
    const firstPage = fullPage('principal-a-request');
    const secondPage = page('principal-a-next');
    routeState.recent = {
      data: firstPage,
      isPlaceholderData: false,
      isPending: false,
      refetch: vi.fn(),
    };
    routeState.cursorRecent = {
      data: secondPage,
      isPlaceholderData: false,
      isPending: false,
      refetch: vi.fn(),
    };
    routeState.nextPage = secondPage;
    routeState.histogram = {
      data: { buckets: [], bucket_count: 0, bucket_ms: 60_000 },
      isError: false,
      isFetching: false,
      isPending: false,
    };
    const view = renderLogs();

    fireEvent.click(screen.getByRole('button', { name: 'Next page' }));
    await waitFor(() =>
      expect(screen.getByText('principal-a-next')).toBeDefined(),
    );

    const scrollContainer = screen.getByTestId('logs-table').parentElement;
    if (scrollContainer == null) {
      throw new Error('Expected the logs table scroll container');
    }
    scrollContainer.scrollTop = 120;
    view.rerenderLogs();
    expect(scrollContainer.scrollTop).toBe(120);
    expect(screen.getByText('principal-a-next')).toBeDefined();

    routeState.recentCalls = [];
    routeState.search = { principal_id: 'principal-b' };
    routeState.recent = {
      data: page('principal-b-request', 'principal-b'),
      isPlaceholderData: false,
      isPending: false,
      refetch: vi.fn(),
    };
    view.rerenderLogs();

    await waitFor(() =>
      expect(screen.getByText('principal-b-request')).toBeDefined(),
    );
    // A filter change re-scopes the rows in place; the scroller stays.
    expect(scrollContainer.scrollTop).toBe(120);
    const changedFilterCalls = routeState.recentCalls.filter(
      ({ filters }) => filters.principal_id === 'principal-b',
    );
    expect(
      changedFilterCalls.filter(({ pageParam }) => pageParam.kind === 'cursor'),
    ).toHaveLength(0);
    expect(
      changedFilterCalls.some(({ pageParam }) => pageParam.kind === 'initial'),
    ).toBe(true);
  });

  test('retries the initial page after a filter render is discarded', async () => {
    const firstPage = fullPage('principal-a-request');
    const secondPage = fullPage('principal-a-next');
    routeState.recent = {
      data: firstPage,
      isPlaceholderData: false,
      isPending: false,
      refetch: vi.fn(),
    };
    routeState.cursorRecent = {
      data: secondPage,
      isPlaceholderData: false,
      isPending: false,
      refetch: vi.fn(),
    };
    routeState.nextPage = secondPage;
    routeState.histogram = {
      data: { buckets: [], bucket_count: 0, bucket_ms: 60_000 },
      isError: false,
      isFetching: false,
      isPending: false,
    };
    const view = renderLogs();

    fireEvent.click(screen.getByRole('button', { name: 'Next page' }));
    await waitFor(() =>
      expect(screen.getByText('principal-a-next-1')).toBeDefined(),
    );
    routeState.nextPage = new Promise<RecentEventsPayload>(() => {});
    fireEvent.click(screen.getByRole('button', { name: 'Next page' }));
    expect(
      screen.getByRole('button', { name: 'Loading next page' }),
    ).toBeDefined();

    routeState.recentCalls = [];
    routeState.search = { principal_id: 'principal-b' };
    routeState.recent = {
      data: page('principal-b-request', 'principal-b'),
      isPlaceholderData: false,
      isPending: false,
      refetch: vi.fn(),
    };
    routeState.suspendInitialForPrincipal = 'principal-b';

    startTransition(() => view.rerenderLogs());
    await waitFor(() =>
      expect(
        routeState.recentCalls.some(
          ({ filters, pageParam }) =>
            filters.principal_id === 'principal-b' &&
            pageParam.kind === 'initial',
        ),
      ).toBe(true),
    );

    routeState.live = {
      ...routeState.live,
      version: routeState.live.version + 1,
    };
    view.rerenderLogs();

    await waitFor(() =>
      expect(screen.getByText('principal-b-request')).toBeDefined(),
    );
    const changedFilterCalls = routeState.recentCalls.filter(
      ({ filters }) => filters.principal_id === 'principal-b',
    );
    expect(
      changedFilterCalls.filter(({ pageParam }) => pageParam.kind === 'cursor'),
    ).toHaveLength(0);
    expect(
      changedFilterCalls.filter(({ pageParam }) => pageParam.kind === 'initial')
        .length,
    ).toBeGreaterThanOrEqual(2);
    expect(
      screen.queryByRole('button', { name: 'Loading next page' }),
    ).toBeNull();
  });

  test('keeps visible page, count, and export in the same merged order', () => {
    // Kind-less fixtures: lift the default messages filter.
    routeState.search = { event_kind: 'all' };
    routeState.recent = {
      data: pageFromEvents([
        {
          ts: 1_700_000_300,
          request_id: 'historical-new',
          thread_id: 'session-new',
          status: 200,
          duration_ms: 25,
        },
        {
          ts: 1_700_000_200,
          request_id: 'duplicate',
          thread_id: 'session-duplicate',
          status: 200,
          duration_ms: 25,
        },
        {
          ts: 1_700_000_100,
          request_id: 'historical-old',
          thread_id: 'session-old',
          status: 200,
          duration_ms: 25,
        },
      ]),
      isPlaceholderData: false,
      isPending: false,
      refetch: vi.fn(),
    };
    routeState.live = {
      ...routeState.live,
      eventsMap: new Map([
        [
          'duplicate',
          {
            phase: 'final',
            event: {
              ts: 1_700_000_250,
              request_id: 'duplicate',
              thread_id: 'session-duplicate',
              status: 201,
              duration_ms: 20,
            },
          },
        ],
        [
          'live-new',
          {
            phase: 'final',
            event: {
              ts: 1_700_000_400,
              request_id: 'live-new',
              thread_id: 'session-live',
              status: 200,
              duration_ms: 10,
            },
          },
        ],
      ]),
      version: 2,
    };
    routeState.histogram = {
      data: { buckets: [], bucket_count: 0, bucket_ms: 60_000 },
      isError: false,
      isFetching: false,
      isPending: false,
    };
    let exportedJson = '';
    vi.stubGlobal(
      'Blob',
      class {
        constructor(parts: BlobPart[]) {
          exportedJson = String(parts[0]);
        }
      },
    );
    const createObjectUrlDescriptor = Object.getOwnPropertyDescriptor(
      URL,
      'createObjectURL',
    );
    const revokeObjectUrlDescriptor = Object.getOwnPropertyDescriptor(
      URL,
      'revokeObjectURL',
    );
    Object.defineProperty(URL, 'createObjectURL', {
      configurable: true,
      value: vi.fn(() => 'blob:logs-export'),
    });
    Object.defineProperty(URL, 'revokeObjectURL', {
      configurable: true,
      value: vi.fn(),
    });
    const anchorClick = vi
      .spyOn(HTMLAnchorElement.prototype, 'click')
      .mockImplementation(() => undefined);

    try {
      renderLogs();
      expect(
        screen.getAllByTestId('logs-row').map((row) => row.textContent),
      ).toEqual(['live-new', 'historical-new', 'duplicate', 'historical-old']);
      expect(screen.getByText('4 requests — live tailing')).toBeDefined();

      fireEvent.click(screen.getByRole('button', { name: 'Export' }));
      expect(anchorClick).toHaveBeenCalledOnce();
      const exported = JSON.parse(exportedJson) as Array<{
        request_id: string;
        status: number;
      }>;
      expect(exported.map((row) => row.request_id)).toEqual([
        'live-new',
        'historical-new',
        'duplicate',
        'historical-old',
      ]);
      expect(
        exported.find((row) => row.request_id === 'duplicate')?.status,
      ).toBe(201);
    } finally {
      if (createObjectUrlDescriptor) {
        Object.defineProperty(
          URL,
          'createObjectURL',
          createObjectUrlDescriptor,
        );
      } else {
        Reflect.deleteProperty(URL, 'createObjectURL');
      }
      if (revokeObjectUrlDescriptor) {
        Object.defineProperty(
          URL,
          'revokeObjectURL',
          revokeObjectUrlDescriptor,
        );
      } else {
        Reflect.deleteProperty(URL, 'revokeObjectURL');
      }
    }
  });

  test('sends event_kind to recent, histogram, and live queries and partitions rows', async () => {
    routeState.search = { event_kind: 'renewal' };
    routeState.recent = {
      data: pageFromEvents([
        {
          ts: 1_700_000_000,
          request_id: 'renewal-request',
          status: 200,
          duration_ms: 25,
          source_kind: 'renewal',
        },
        {
          ts: 1_700_000_001,
          request_id: 'messages-request',
          status: 200,
          duration_ms: 25,
          event_kind: 'messages',
        },
      ]),
      isPlaceholderData: false,
      isPending: false,
      refetch: vi.fn(),
    };
    routeState.histogram = {
      data: { buckets: [], bucket_count: 0, bucket_ms: 60_000 },
      isError: false,
      isFetching: false,
      isPending: false,
    };

    renderLogs();

    await waitFor(() =>
      expect(screen.getByText('renewal-request')).toBeDefined(),
    );
    expect(screen.queryByText('messages-request')).toBeNull();

    for (const call of routeState.recentCalls) {
      expect(call.filters.event_kind).toBe('renewal');
    }
    expect(routeState.recentCalls.length).toBeGreaterThan(0);
    for (const filters of routeState.histogramCalls) {
      expect(filters.event_kind).toBe('renewal');
    }
    expect(routeState.histogramCalls.length).toBeGreaterThan(0);
    for (const filters of routeState.liveCalls) {
      expect(filters.event_kind).toBe('renewal');
    }
    expect(routeState.liveCalls.length).toBeGreaterThan(0);
  });

  test('lists only messages requests when the URL names no kind', async () => {
    routeState.search = {};
    routeState.recent = {
      data: pageFromEvents([
        {
          ts: 1_700_000_000,
          request_id: 'count-tokens-request',
          status: 200,
          duration_ms: 25,
          event_kind: 'count_tokens',
        },
        {
          ts: 1_700_000_001,
          request_id: 'messages-request',
          status: 200,
          duration_ms: 25,
          event_kind: 'messages',
        },
      ]),
      isPlaceholderData: false,
      isPending: false,
      refetch: vi.fn(),
    };

    renderLogs();

    await waitFor(() =>
      expect(screen.getByText('messages-request')).toBeDefined(),
    );
    expect(screen.queryByText('count-tokens-request')).toBeNull();
    expect(routeState.recentCalls.length).toBeGreaterThan(0);
    for (const call of routeState.recentCalls) {
      expect(call.filters.event_kind).toBe('messages');
    }
    expect(routeState.liveCalls.length).toBeGreaterThan(0);
    for (const filters of routeState.liveCalls) {
      expect(filters.event_kind).toBe('messages');
    }
    // The default kind is not an applied filter: no chip.
    expect(
      screen.queryByRole('button', { name: 'Remove kind filter' }),
    ).toBeNull();
  });

  test('sends the typed model needle unchanged to recent, histogram, and live queries', async () => {
    routeState.search = { model: 'sonnet' };
    routeState.recent = {
      data: pageFromEvents([
        {
          ts: 1_700_000_001,
          request_id: 'sonnet-request',
          model: 'claude-3-5-sonnet-20241022',
          status: 200,
          duration_ms: 25,
          event_kind: 'messages',
        },
        {
          ts: 1_700_000_000,
          request_id: 'opus-request',
          model: 'claude-opus-4-5-20251101',
          status: 200,
          duration_ms: 25,
          event_kind: 'messages',
        },
      ]),
      isPlaceholderData: false,
      isPending: false,
      refetch: vi.fn(),
    };

    renderLogs();

    await waitFor(() =>
      expect(screen.getByText('sonnet-request')).toBeDefined(),
    );
    // A stale retained row outside the fuzzy match stays hidden.
    expect(screen.queryByText('opus-request')).toBeNull();
    // The server owns normalization; the client never prepends `claude-`.
    for (const calls of [
      routeState.recentCalls.map(({ filters }) => filters),
      routeState.histogramCalls,
      routeState.liveCalls,
    ]) {
      expect(calls.length).toBeGreaterThan(0);
      for (const filters of calls) expect(filters.model).toBe('sonnet');
    }
  });

  test('opens on All time even when a shared range preference is stored', () => {
    localStorage.setItem('cclb.timeRange', '24h');
    try {
      routeState.search = {};
      renderLogs();

      const range = within(
        screen.getByRole('radiogroup', { name: 'Time range preset' }),
      );
      expect(
        range
          .getByRole('radio', { name: 'All time' })
          .getAttribute('aria-checked'),
      ).toBe('true');
      for (const filters of [
        ...routeState.recentCalls.map((call) => call.filters),
        ...routeState.liveCalls,
      ]) {
        expect(filters.since_unix_secs).toBeUndefined();
        expect(filters.until_unix_secs).toBeUndefined();
      }
      expect(routeState.recentCalls.length).toBeGreaterThan(0);
    } finally {
      localStorage.removeItem('cclb.timeRange');
    }
  });

  test('removes a live error row promptly when its partial corrects to 200', async () => {
    routeState.search = { status: 'errors', event_kind: 'all' };
    const partialAt = (status: number) => ({
      eventsMap: new Map([
        [
          'live-retry',
          {
            phase: 'partial' as const,
            event: {
              event_id: 'live-retry',
              request_id: 'live-retry',
              ts: 1_700_000_002,
              ts_ms: 1_700_000_002_000,
              upstream_response_status: status,
            },
          },
        ],
      ]),
    });
    const { rerenderLogs } = renderLogs();
    await waitFor(() => expect(screen.getByTestId('logs-table')).toBeDefined());

    routeState.live = { ...routeState.live, ...partialAt(401), version: 1 };
    rerenderLogs();
    await waitFor(() => expect(screen.getByText('live-retry')).toBeDefined());

    routeState.live = { ...routeState.live, ...partialAt(200), version: 2 };
    rerenderLogs();
    await waitFor(() => expect(screen.queryByText('live-retry')).toBeNull());
  });

  test('keeps the histogram bars when a strip selection is committed', async () => {
    const nowSecs = Math.floor(Date.now() / 1000);
    vi.mocked(useEventsHistogram).mockImplementation(useRealEventsHistogram);
    routeState.recent = {
      data: pageFromEvents([
        {
          ts: nowSecs - 1_800,
          request_id: 'recent-request',
          status: 200,
          duration_ms: 25,
          event_kind: 'messages',
        },
      ]),
      isPlaceholderData: false,
      isPending: false,
      refetch: vi.fn(),
    };
    const histogramUrls: string[] = [];
    vi.stubGlobal(
      'fetch',
      vi.fn((input: RequestInfo | URL) => {
        const url = String(input);
        if (!url.includes('/admin/v1/events/histogram')) {
          return new Promise<Response>(() => {});
        }
        histogramUrls.push(url);
        // Only the first load answers; any later request stays in flight, as
        // a refetch does while the user looks at the strip.
        if (histogramUrls.length > 1) return new Promise<Response>(() => {});
        return Promise.resolve(
          new Response(
            JSON.stringify({
              buckets: [
                {
                  bucket_start_unix_secs: nowSecs - 1_800,
                  total_count: 4,
                  error_count: 1,
                },
              ],
              bucket_count: 1,
              bucket_ms: 60_000,
            }),
            { headers: { 'Content-Type': 'application/json' } },
          ),
        );
      }),
    );

    const view = renderLogs();
    await waitFor(() => expect(histogramUrls).toHaveLength(1));
    await waitFor(() =>
      expect(screen.queryByTestId('time-range-strip-loading')).toBeNull(),
    );

    // What a drag release commits: the URL gains the selection's bounds.
    routeState.search = {
      since_unix_secs: nowSecs - 2_400,
      until_unix_secs: nowSecs - 1_200,
    };
    view.rerenderLogs();

    // The strip still covers the same view, so the bars it already has stay
    // on screen instead of blanking behind the new selection.
    expect(screen.queryByTestId('time-range-strip-loading')).toBeNull();
    expect(histogramUrls).toHaveLength(1);
  });
});
