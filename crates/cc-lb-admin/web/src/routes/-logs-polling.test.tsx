import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from '@testing-library/react';
import type React from 'react';
import { Suspense, startTransition } from 'react';
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest';
import type { EventsHistogramPayload, RecentEventsPayload } from '../lib/api';
import type * as queries from '../lib/queries';
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
    useEventsHistogram: (filters: Record<string, string | undefined>) => {
      routeState.histogramCalls.push(filters);
      return routeState.histogram;
    },
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
    useUpstreamNameMap: () => new Map(),
    useUpstreams: () => ({ data: { upstreams: [] } }),
  };
});

vi.mock('../lib/useLiveEventStream', () => ({
  useLiveEventStream: (filters: Record<string, string | undefined>) => {
    routeState.liveCalls.push(filters);
    return routeState.live;
  },
}));

vi.mock('../components/ui/RequestEventsTable', () => ({
  RequestEventsTable: ({
    events,
    liveFlashIds,
    loading,
  }: {
    events: ReadonlyArray<{ event_id?: string; request_id: string }>;
    liveFlashIds?: Set<string>;
    loading?: boolean;
  }) => (
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
  ),
}));
const LogsPage = Route.options.component as React.ComponentType;

function page(requestId?: string, principalId = 'principal-a') {
  const events = requestId
    ? [
        {
          ts: 1_700_000_000,
          request_id: requestId,
          principal_id: principalId,
          status: 200,
          duration_ms: 25,
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
    })),
  );
}

function renderLogs() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
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
    routeState.liveCalls = [];
    routeState.search = {};
    routeState.suspendInitialForPrincipal = undefined;
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

  test('does not cache placeholder pages across a filter identity change', async () => {
    routeState.recent = {
      data: page('principal-a-request', 'principal-a'),
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
    const view = renderLogs();

    await waitFor(() =>
      expect(screen.getByText('principal-a-request')).toBeDefined(),
    );

    routeState.search = { principal_id: 'principal-b' };
    routeState.recent = {
      data: page('principal-a-request', 'principal-a'),
      isPlaceholderData: true,
      isPending: false,
      refetch: vi.fn(),
    };
    view.rerenderLogs();

    await waitFor(() =>
      expect(screen.queryByText('principal-a-request')).toBeNull(),
    );
    expect(screen.getByTestId('logs-table').dataset.loading).toBe('true');

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
    expect(screen.queryByText('principal-a-request')).toBeNull();
  });

  test('uses the initial cursor and resets scroll for a changed filter identity', async () => {
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
    expect(scrollContainer.scrollTop).toBe(0);
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
});
