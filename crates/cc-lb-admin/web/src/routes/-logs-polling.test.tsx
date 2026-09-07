import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { cleanup, render, screen, waitFor } from '@testing-library/react';
import type React from 'react';
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest';
import type { EventsHistogramPayload, RecentEventsPayload } from '../lib/api';
import type * as queries from '../lib/queries';
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
  histogram: undefined as unknown as HistogramResult,
  navigate: vi.fn(),
  recent: undefined as unknown as RecentResult,
  search: {} as Record<string, string | number | undefined>,
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
    recentEventsPageQueryOptions: vi.fn(),
    useEventsHistogram: () => routeState.histogram,
    usePrincipalNameMap: () => new Map(),
    useRecentEventsPage: () => routeState.recent,
    useUpstreamNameMap: () => new Map(),
    useUpstreams: () => ({ data: { upstreams: [] } }),
  };
});

vi.mock('../lib/useLiveEventStream', () => ({
  useLiveEventStream: () => ({
    eventsMap: new Map(),
    forceReconnect: vi.fn(),
    permanentFailure: false,
    permanentFailureSince: null,
    reconnectAttempts: 0,
    status: 'idle',
    version: 0,
  }),
}));

vi.mock('../components/ui/RequestEventsTable', () => ({
  RequestEventsTable: ({
    events,
    loading,
  }: {
    events: ReadonlyArray<{ request_id: string }>;
    loading?: boolean;
  }) => (
    <div data-loading={loading ? 'true' : 'false'} data-testid="logs-table">
      {events.map((event) => (
        <span key={event.request_id}>{event.request_id}</span>
      ))}
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

function renderLogs() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  const view = render(
    <QueryClientProvider client={queryClient}>
      <LogsPage />
    </QueryClientProvider>,
  );
  return {
    ...view,
    rerenderLogs: () =>
      view.rerender(
        <QueryClientProvider client={queryClient}>
          <LogsPage />
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
    routeState.search = {};
    vi.spyOn(Route, 'useSearch').mockImplementation(() => routeState.search);
    routeState.recent = {
      data: page(),
      isPlaceholderData: false,
      isPending: false,
      refetch: vi.fn(),
    };
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
});
