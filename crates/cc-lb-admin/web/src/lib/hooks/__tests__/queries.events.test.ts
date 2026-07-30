import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { act, renderHook, waitFor } from '@testing-library/react';
import { createElement, type ReactNode } from 'react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import type { RecentEventsPayload, RequestEvent } from '../../api';
import {
  type RecentEventsPageParam,
  useRecentEventsInfinite,
  useRecentEventsPage,
} from '../../queries';

function makePage(page: number, limit: number): RecentEventsPayload {
  return {
    events: Array.from({ length: limit }, (_, index) => {
      const sequence = page * 200 + index;
      return {
        event_id: `event-${sequence}`,
        request_id: `request-${sequence}`,
        ts: sequence / 1000,
        ts_ms: sequence,
        status: 200,
        duration_ms: 10,
      } satisfies RequestEvent;
    }).reverse(),
    observed: true,
    count: limit,
    limit,
  };
}

function jsonResponse(body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status: 200,
    headers: { 'Content-Type': 'application/json' },
  });
}

function makeWrapper(client: QueryClient) {
  return function Wrapper({ children }: { readonly children: ReactNode }) {
    return createElement(QueryClientProvider, { client }, children);
  };
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe('useRecentEventsInfinite', () => {
  it('continues cursor pagination past 500 events until the backend is exhausted', async () => {
    const requestedLimits: number[] = [];
    const pageSizes = [200, 200, 200, 75];
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const rawUrl =
        typeof input === 'string'
          ? input
          : input instanceof URL
            ? input.toString()
            : input.url;
      const url = new URL(rawUrl, 'http://localhost');
      const limit = Number(url.searchParams.get('limit'));
      requestedLimits.push(limit);
      const pageIndex = requestedLimits.length - 1;
      return jsonResponse({
        ...makePage(pageIndex, pageSizes[pageIndex] ?? 0),
        limit,
      });
    });
    vi.stubGlobal('fetch', fetchMock);
    const client = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });
    const { result } = renderHook(() => useRecentEventsInfinite({}), {
      wrapper: makeWrapper(client),
    });
    await waitFor(() => expect(result.current.data?.pages).toHaveLength(1));

    for (let page = 2; page <= 4; page += 1) {
      await act(async () => {
        await result.current.fetchNextPage();
      });
      await waitFor(() =>
        expect(result.current.data?.pages).toHaveLength(page),
      );
    }

    expect(
      result.current.data?.pages.flatMap(({ events }) => events),
    ).toHaveLength(675);
    expect(requestedLimits).toEqual([200, 200, 200, 200]);
    expect(result.current.hasNextPage).toBe(false);
    await act(async () => {
      await result.current.fetchNextPage();
    });
    expect(fetchMock).toHaveBeenCalledTimes(4);
  });
  it('uses the requested page size and carries the visible cursor forward', async () => {
    const requestedUrls: URL[] = [];
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const rawUrl =
        typeof input === 'string'
          ? input
          : input instanceof URL
            ? input.toString()
            : input.url;
      const url = new URL(rawUrl, 'http://localhost');
      requestedUrls.push(url);
      return jsonResponse(makePage(requestedUrls.length - 1, 50));
    });
    vi.stubGlobal('fetch', fetchMock);
    const client = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });
    const initialPageParam: RecentEventsPageParam = {
      kind: 'initial',
      limit: 50,
    };
    const { result, rerender } = renderHook(
      ({ pageParam }: { pageParam: RecentEventsPageParam }) =>
        useRecentEventsPage({}, pageParam),
      {
        initialProps: {
          pageParam: initialPageParam,
        } as { pageParam: RecentEventsPageParam },
        wrapper: makeWrapper(client),
      },
    );
    await waitFor(() => expect(result.current.data?.events).toHaveLength(50));

    rerender({
      pageParam: {
        kind: 'cursor',
        limit: 50,
        ts_ms: 0,
        event_id: 'event-0',
      },
    });
    await waitFor(() => expect(requestedUrls).toHaveLength(2));

    expect(requestedUrls).toHaveLength(2);
    expect(requestedUrls.map((url) => url.searchParams.get('limit'))).toEqual([
      '50',
      '50',
    ]);
    expect(requestedUrls[1].searchParams.get('until_ts_ms')).toBe('0');
    expect(requestedUrls[1].searchParams.get('until_event_id')).toBe('event-0');
  });
});
