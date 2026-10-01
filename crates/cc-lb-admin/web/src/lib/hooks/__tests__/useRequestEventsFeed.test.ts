// @vitest-environment jsdom
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { act, renderHook, waitFor } from '@testing-library/react';
import { createElement, type ReactNode } from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type {
  RecentEventsPayload,
  RequestEvent,
  RequestEventUpdate,
} from '../../api';
import { useRequestEventsFeed } from '../../useRequestEventsFeed';

const mocks = vi.hoisted(() => {
  const live = {
    eventsMap: new Map(),
    version: 0,
    status: 'live' as const,
    lastActivityAt: null,
    lastCursor: null,
    error: null,
    malformedFrameCount: 0,
    permanentFailure: false,
    permanentFailureSince: null,
    reconnectAttempts: 0,
    forceReconnect: vi.fn(),
  };
  const recent = {
    data: undefined as RecentEventsPayload | undefined,
    isPending: true,
    isPlaceholderData: false,
    isFetching: true,
    error: null as Error | null,
    refetch: vi.fn(),
  };
  return {
    live,
    recent,
    useLiveEventStream: vi.fn(() => live),
    useRecentEventsPage: vi.fn(() => recent),
    recentEventsPageQueryOptions: vi.fn((filters, pageParam) => ({
      queryKey: ['events', filters, pageParam],
      queryFn: async () => ({
        events: [],
        observed: true,
        count: 0,
        limit: 500,
      }),
    })),
  };
});

vi.mock('../../useLiveEventStream', () => ({
  useLiveEventStream: mocks.useLiveEventStream,
}));

vi.mock('../../queries', () => ({
  getRecentEventsCursor: (event: RequestEvent) => ({
    ts_ms: event.ts_ms ?? 0,
    event_id: event.event_id ?? event.request_id,
  }),
  recentEventsPageQueryOptions: mocks.recentEventsPageQueryOptions,
  useRecentEventsPage: mocks.useRecentEventsPage,
}));

function makeEvent(index: number): RequestEvent {
  return {
    event_id: `event-${index}`,
    request_id: `request-${index}`,
    ts: index / 1000,
    ts_ms: index,
    principal_id: 'principal-a',
    status: 200,
    duration_ms: 10,
  };
}

function makePartial(index: number): RequestEventUpdate {
  return {
    phase: 'partial',
    payload: {
      event_id: `event-${index}`,
      request_id: `request-${index}`,
      ts: index / 1000,
      ts_ms: index,
      principal_id: 'principal-a',
    },
  };
}

function makeFinal(index: number): RequestEventUpdate {
  return {
    phase: 'final',
    payload: {
      event: makeEvent(index),
      cursor: index,
    },
  };
}

function makeWrapper(client: QueryClient) {
  return function Wrapper({ children }: { readonly children: ReactNode }) {
    return createElement(QueryClientProvider, { client }, children);
  };
}

describe('useRequestEventsFeed', () => {
  beforeEach(() => {
    mocks.live.eventsMap.clear();
    mocks.live.version = 0;
    mocks.recent.data = {
      events: Array.from({ length: 500 }, (_, offset) =>
        makeEvent(500 - offset),
      ),
      observed: true,
      count: 500,
      limit: 500,
    };
    mocks.recent.isPending = false;
    mocks.recent.isFetching = false;
    mocks.recent.error = null;
    mocks.recent.refetch.mockReset();
    mocks.recentEventsPageQueryOptions.mockClear();
    mocks.useRecentEventsPage.mockClear();
    mocks.useLiveEventStream.mockClear();
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  it('retains 500 history rows in 50-row pages and anchors the next cursor to history', async () => {
    const nextBatch: RecentEventsPayload = {
      events: [makeEvent(0)],
      observed: true,
      count: 1,
      limit: 500,
    };
    const client = new QueryClient();
    client.fetchQuery = vi.fn(
      async () => nextBatch,
    ) as typeof client.fetchQuery;
    const { result, rerender } = renderHook(
      () =>
        useRequestEventsFeed({
          filters: { principal_id: 'principal-a' },
          initialHistoryLimit: 500,
          pageSize: 50,
        }),
      { wrapper: makeWrapper(client) },
    );

    await waitFor(() => expect(result.current.rows).toHaveLength(500));
    expect(result.current.pageRows).toHaveLength(50);
    expect(result.current.pageRows[49]?.request_id).toBe('request-451');

    for (let page = 1; page < 10; page += 1) {
      await act(async () => {
        await result.current.nextPage();
      });
    }
    expect(result.current.page).toBe(9);
    expect(result.current.pageRows[0]?.request_id).toBe('request-50');
    expect(result.current.pageRows[49]?.request_id).toBe('request-1');

    mocks.live.eventsMap.set('event-1001', {
      phase: 'final',
      event: makeEvent(1001),
    });
    mocks.live.version += 1;
    act(() => rerender());
    expect(result.current.pageRows[0]?.request_id).toBe('request-50');
    expect(result.current.pageRows[49]?.request_id).toBe('request-1');
    expect(result.current.filteredRows[0]?.request_id).toBe('request-1001');

    await act(async () => {
      await result.current.nextPage();
    });
    expect(result.current.page).toBe(10);
    expect(result.current.pageRows[0]?.request_id).toBe('request-0');
    const cursor = mocks.recentEventsPageQueryOptions.mock.calls.at(-1)?.[1];
    expect(cursor).toMatchObject({ kind: 'cursor', event_id: 'event-1' });

    for (let page = 0; page < 10; page += 1) {
      await act(async () => {
        await result.current.previousPage();
      });
    }
    await act(async () => {
      await result.current.nextPage();
    });
    expect(result.current.page).toBe(1);
    expect(result.current.pageRows[0]?.request_id).toBe('request-451');
  });
  it('pages a late final whose timestamp falls below the initial 50 rows', async () => {
    const client = new QueryClient();
    const { result, rerender } = renderHook(
      () =>
        useRequestEventsFeed({
          filters: { principal_id: 'principal-a' },
          initialHistoryLimit: 500,
          pageSize: 50,
        }),
      { wrapper: makeWrapper(client) },
    );

    await waitFor(() => expect(result.current.rows).toHaveLength(500));
    mocks.live.eventsMap.set('late-event', {
      phase: 'final',
      event: {
        ...makeEvent(100),
        event_id: 'late-event',
        request_id: 'late-request',
      },
    });
    mocks.live.version += 1;
    act(() => rerender());

    const pagedRows = [];
    for (let page = 0; page < 8; page += 1) {
      await act(async () => {
        await result.current.nextPage();
      });
      pagedRows.push(...result.current.pageRows);
    }
    expect(pagedRows.some((row) => row.request_id === 'late-request')).toBe(
      true,
    );
  });

  it('flashes actual scoped arrivals once and does not flash initial history, page changes, or finalization', async () => {
    const client = new QueryClient();
    const { result, rerender } = renderHook(
      () =>
        useRequestEventsFeed({
          filters: { principal_id: 'principal-a' },
          initialHistoryLimit: 500,
          pageSize: 50,
        }),
      { wrapper: makeWrapper(client) },
    );

    await waitFor(() => expect(result.current.rows).toHaveLength(500));
    expect(result.current.liveFlashIds).toEqual(new Set());
    expect(mocks.useLiveEventStream).toHaveBeenCalledWith(
      { principal_id: 'principal-a' },
      { enabled: true },
    );

    await act(async () => {
      await result.current.nextPage();
    });
    expect(result.current.liveFlashIds).toEqual(new Set());

    mocks.live.eventsMap.set('event-1001', {
      phase: 'partial',
      event: makePartial(1001).payload,
    });
    mocks.live.version += 1;
    act(() => rerender());
    expect(result.current.liveFlashIds).toEqual(new Set(['event-1001']));
    expect(
      result.current.rows.find((row) => row.request_id === 'request-1001')
        ?._phase,
    ).toBe('partial');

    mocks.live.eventsMap.set('event-1001', {
      phase: 'final',
      event: makeFinal(1001).payload.event,
    });
    mocks.live.version += 1;
    act(() => rerender());
    expect(result.current.liveFlashIds).toEqual(new Set());
    expect(
      result.current.rows.find((row) => row.request_id === 'request-1001')
        ?._phase,
    ).toBe('final');
  });
});
