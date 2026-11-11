import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { act, renderHook, waitFor } from '@testing-library/react';
import { createElement, type ReactNode } from 'react';
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest';
import type { CacheKeepaliveListResponse } from '../../cacheKeepaliveApi';
import {
  POLLING_INTERVALS,
  qk,
  useCacheKeepaliveSessionDetail,
  useCacheKeepaliveSessions,
  useCacheKeepaliveSummary,
} from '../../queries';
import {
  cacheKeepaliveListResponseFixture,
  cacheKeepaliveRenewedDetailFixture,
  cacheKeepaliveSummaryFixture,
  cacheKeepaliveSummaryOnlyResponseFixture,
} from '../../test-utils/cacheKeepalive-fixtures';
import * as visibilityManager from '../../visibilityManager';

const PRINCIPAL_ID = 'principal-1';
const SESSION_ID = 'session/a%2Bb+c&d?e#한글';
const SESSION_DETAIL_PATH = `/admin/v1/principals/${PRINCIPAL_ID}/cache-keepalive/detail`;

function makeClient() {
  return new QueryClient({
    defaultOptions: {
      queries: { retry: false },
      mutations: { retry: false },
    },
  });
}

function makeWrapper(client: QueryClient) {
  return function Wrapper({ children }: { children: ReactNode }) {
    return createElement(QueryClientProvider, { client }, children);
  };
}

function jsonResponse(body: unknown, init?: ResponseInit) {
  return new Response(JSON.stringify(body), {
    status: 200,
    headers: { 'Content-Type': 'application/json' },
    ...init,
  });
}

function stubFetchOnce(body: unknown, init?: ResponseInit) {
  const fetchMock = vi.fn(async () => jsonResponse(body, init));
  vi.stubGlobal('fetch', fetchMock);
  return fetchMock;
}

function urlFrom(fetchMock: ReturnType<typeof vi.fn>) {
  const [input] = fetchMock.mock.calls[0] as [RequestInfo | URL];
  if (typeof input === 'string') {
    return input;
  }
  return input instanceof URL ? input.toString() : input.url;
}

beforeEach(() => {
  localStorage.clear();
});

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe('useCacheKeepaliveSummary', () => {
  test('fetches limit=0 and exposes the decoded card summary', async () => {
    const fetchMock = stubFetchOnce(cacheKeepaliveSummaryOnlyResponseFixture);
    const { result } = renderHook(
      () => useCacheKeepaliveSummary(PRINCIPAL_ID),
      { wrapper: makeWrapper(makeClient()) },
    );

    await waitFor(() => expect(result.current.isSuccess).toBe(true));

    expect(urlFrom(fetchMock)).toBe(
      `/admin/v1/principals/${PRINCIPAL_ID}/cache-keepalive?limit=0&horizon=24h`,
    );
    expect(result.current.data).toEqual(cacheKeepaliveSummaryFixture);
  });

  test('does not fetch while keep-alive is disabled', async () => {
    const fetchMock = stubFetchOnce(cacheKeepaliveSummaryOnlyResponseFixture);
    const { result, rerender } = renderHook(
      ({ enabled }) => useCacheKeepaliveSummary(PRINCIPAL_ID, enabled),
      {
        initialProps: { enabled: false },
        wrapper: makeWrapper(makeClient()),
      },
    );

    expect(fetchMock).not.toHaveBeenCalled();
    expect(result.current.fetchStatus).toBe('idle');

    rerender({ enabled: true });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });
});

describe('useCacheKeepaliveSessions', () => {
  test('does not fetch until its drawer is open', async () => {
    const fetchMock = stubFetchOnce(cacheKeepaliveListResponseFixture);
    const { result, rerender } = renderHook(
      ({ enabled }) => useCacheKeepaliveSessions(PRINCIPAL_ID, {}, enabled),
      {
        initialProps: { enabled: false },
        wrapper: makeWrapper(makeClient()),
      },
    );

    expect(fetchMock).not.toHaveBeenCalled();
    expect(result.current.fetchStatus).toBe('idle');

    rerender({ enabled: true });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(urlFrom(fetchMock)).toBe(
      `/admin/v1/principals/${PRINCIPAL_ID}/cache-keepalive`,
    );
  });

  test('fetches the session list and decodes every frozen fixture row', async () => {
    const fetchMock = stubFetchOnce(cacheKeepaliveListResponseFixture);
    const { result } = renderHook(
      () => useCacheKeepaliveSessions(PRINCIPAL_ID),
      { wrapper: makeWrapper(makeClient()) },
    );

    await waitFor(() => expect(result.current.isSuccess).toBe(true));

    expect(urlFrom(fetchMock)).toBe(
      `/admin/v1/principals/${PRINCIPAL_ID}/cache-keepalive`,
    );
    const firstPage = result.current.data?.pages[0];
    expect(firstPage?.rows).toHaveLength(7);
    expect(firstPage?.rows.map((row) => row.state)).toEqual([
      'renewed',
      'scheduled',
      'capped',
      'capped',
      'expired',
      'not_tracked',
      'renewed',
    ]);
    expect(result.current.hasNextPage).toBe(true);
  });
});

describe('head-only session polling', () => {
  let requests: string[];
  let head: CacheKeepaliveListResponse;
  let visible: boolean;

  beforeEach(() => {
    vi.useFakeTimers();
    requests = [];
    visible = true;
    head = {
      ...cacheKeepaliveListResponseFixture,
      next_cursor: 'cursor-1',
    };
    vi.spyOn(visibilityManager, 'useVisibility').mockImplementation(() => ({
      visible,
      online: true,
      hiddenSince: visible ? null : 1,
      gracePeriodElapsed: !visible,
    }));
    vi.stubGlobal(
      'fetch',
      vi.fn(async (input: string) => {
        const cursor = new URL(input, 'http://localhost').searchParams.get(
          'cursor',
        );
        requests.push(cursor ?? 'head');
        return jsonResponse(
          cursor === null
            ? head
            : {
                summary: null,
                groups: null,
                rows: [{ ...head.rows[0], id: cursor }],
                next_cursor: cursor === 'cursor-1' ? 'cursor-2' : null,
              },
        );
      }),
    );
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  test('three loaded pages receive one overview request per unchanged poll and no cursor reads', async () => {
    const { result } = renderHook(
      () => useCacheKeepaliveSessions(PRINCIPAL_ID),
      { wrapper: makeWrapper(makeClient()) },
    );
    await act(() => vi.advanceTimersByTimeAsync(1));
    await act(async () => {
      await result.current.fetchNextPage();
    });
    await act(() => vi.advanceTimersByTimeAsync(1));
    await act(async () => {
      await result.current.fetchNextPage();
    });
    await act(() => vi.advanceTimersByTimeAsync(1));
    expect(result.current.data?.pages).toHaveLength(3);
    requests.length = 0;
    const laterPages = result.current.data?.pages.slice(1);
    head = {
      ...head,
      rows: head.rows.map((row, index) =>
        index === 0 ? { ...row, generation: 99 } : row,
      ),
    };

    await act(() => vi.advanceTimersByTimeAsync(15_000));

    expect(requests).toEqual(['head', 'head', 'head']);
    expect(result.current.data?.pages.slice(1)).toEqual(laterPages);
    expect(result.current.data?.pages[0].rows[0].generation).toBe(99);
  });

  test('clock-only sessions_last_5m changes update the head without fetching loaded tail pages', async () => {
    const { result } = renderHook(
      () => useCacheKeepaliveSessions(PRINCIPAL_ID),
      { wrapper: makeWrapper(makeClient()) },
    );
    await act(() => vi.advanceTimersByTimeAsync(1));
    await act(async () => {
      await result.current.fetchNextPage();
    });
    await act(() => vi.advanceTimersByTimeAsync(1));
    const laterPages = result.current.data?.pages.slice(1);
    requests.length = 0;
    head = {
      ...head,
      summary: {
        ...cacheKeepaliveSummaryFixture,
        sessions_last_5m: cacheKeepaliveSummaryFixture.sessions_last_5m + 1,
      },
    };

    await act(() => vi.advanceTimersByTimeAsync(15_000));

    expect(requests).toEqual(['head', 'head', 'head']);
    expect(result.current.data?.pages.slice(1)).toEqual(laterPages);
    expect(result.current.data?.pages[0].summary?.sessions_last_5m).toBe(
      head.summary?.sessions_last_5m,
    );
  });

  test.each(['total_spent', 'renewing_now', 'groups'] as const)(
    'a changed %s refreshes the loaded tail exactly once',
    async (aggregate) => {
      const { result } = renderHook(
        () => useCacheKeepaliveSessions(PRINCIPAL_ID),
        { wrapper: makeWrapper(makeClient()) },
      );
      await act(() => vi.advanceTimersByTimeAsync(1));
      await act(async () => {
        await result.current.fetchNextPage();
      });
      await act(() => vi.advanceTimersByTimeAsync(1));
      await act(async () => {
        await result.current.fetchNextPage();
      });
      await act(() => vi.advanceTimersByTimeAsync(1));
      requests.length = 0;
      head =
        aggregate !== 'groups'
          ? {
              ...head,
              summary: { ...cacheKeepaliveSummaryFixture, [aggregate]: 10 },
            }
          : {
              ...head,
              groups:
                head.groups?.map((group) => ({
                  ...group,
                  sessions: group.sessions + 1,
                })) ?? null,
            };

      await act(() => vi.advanceTimersByTimeAsync(15_000));

      expect(requests).toEqual([
        'head',
        'cursor-1',
        'cursor-2',
        'head',
        'head',
      ]);
      expect(result.current.data?.pages).toHaveLength(3);
      expect(result.current.data?.pageParams).toEqual([
        undefined,
        'cursor-1',
        'cursor-2',
      ]);
    },
  );

  test('a moved head boundary refreshes the loaded chain once even when aggregates are equal', async () => {
    const { result } = renderHook(
      () => useCacheKeepaliveSessions(PRINCIPAL_ID),
      { wrapper: makeWrapper(makeClient()) },
    );
    await act(() => vi.advanceTimersByTimeAsync(1));
    await act(async () => {
      await result.current.fetchNextPage();
    });
    await act(() => vi.advanceTimersByTimeAsync(1));
    await act(async () => {
      await result.current.fetchNextPage();
    });
    await act(() => vi.advanceTimersByTimeAsync(1));
    requests.length = 0;
    const server = globalThis.fetch;
    head = {
      ...head,
      rows: head.rows.map((row, index) =>
        index === head.rows.length - 1
          ? { ...row, id: 'shifted-boundary' }
          : row,
      ),
      next_cursor: 'shifted-1',
    };
    vi.stubGlobal(
      'fetch',
      vi.fn(async (input: string) => {
        const cursor = new URL(input, 'http://localhost').searchParams.get(
          'cursor',
        );
        if (cursor?.startsWith('shifted-')) {
          requests.push(cursor);
          return jsonResponse({
            summary: null,
            groups: null,
            rows: [{ ...head.rows[0], id: cursor }],
            next_cursor: cursor === 'shifted-1' ? 'shifted-2' : 'shifted-3',
          });
        }
        return server(input);
      }),
    );

    await act(() => vi.advanceTimersByTimeAsync(15_000));

    expect(requests).toEqual([
      'head',
      'shifted-1',
      'shifted-2',
      'head',
      'head',
    ]);
    expect(result.current.data?.pageParams).toEqual([
      undefined,
      'shifted-1',
      'shifted-2',
    ]);
    await act(async () => {
      await result.current.fetchNextPage();
    });
    await act(() => vi.advanceTimersByTimeAsync(1));
    expect(requests.at(-1)).toBe('shifted-3');
    expect(result.current.data?.pages).toHaveLength(4);
  });

  test.each(['1h', '6h', '24h', '7d'] as const)(
    'rolling %s cursor anchors do not refresh an unchanged page boundary',
    async (horizon) => {
      const cursorPages = new Map<string, number>();
      let horizonStart = 1_700_000_000_000;
      const cursorForPage = (page: number) => {
        const row =
          page === 0
            ? head.rows.at(-1)
            : {
                id: `tail-${page}`,
                last_message_at_ms: 1_700_086_000_000 - page,
              };
        const token = btoa(
          JSON.stringify({
            horizon,
            principal_id: PRINCIPAL_ID,
            horizon_start_ms: horizonStart,
            filter: 'all',
            last_message_at_ms: row?.last_message_at_ms,
            entry_id: `session:${row?.id}`,
          }),
        )
          .replaceAll('+', '-')
          .replaceAll('/', '_')
          .replaceAll('=', '');
        cursorPages.set(token, page + 1);
        return token;
      };
      vi.stubGlobal(
        'fetch',
        vi.fn(async (input: string) => {
          const cursor = new URL(input, 'http://localhost').searchParams.get(
            'cursor',
          );
          if (cursor === null) {
            requests.push('head');
            horizonStart += 5_000;
            return jsonResponse({ ...head, next_cursor: cursorForPage(0) });
          }
          const page = cursorPages.get(cursor);
          requests.push(`tail-${page}`);
          return jsonResponse({
            summary: null,
            groups: null,
            rows: [{ ...head.rows[0], id: `tail-${page}` }],
            next_cursor: page === 1 ? cursorForPage(1) : null,
          });
        }),
      );
      const { result } = renderHook(
        () => useCacheKeepaliveSessions(PRINCIPAL_ID, { horizon }),
        { wrapper: makeWrapper(makeClient()) },
      );
      await act(() => vi.advanceTimersByTimeAsync(1));
      await act(async () => {
        await result.current.fetchNextPage();
      });
      await act(() => vi.advanceTimersByTimeAsync(1));
      await act(async () => {
        await result.current.fetchNextPage();
      });
      await act(() => vi.advanceTimersByTimeAsync(1));
      const originalParams = result.current.data?.pageParams;
      requests.length = 0;

      await act(() => vi.advanceTimersByTimeAsync(15_000));

      expect(requests).toEqual(['head', 'head', 'head']);
      expect(result.current.data?.pageParams).toEqual(originalParams);
      expect(result.current.data?.pages[0].next_cursor).not.toBe(
        originalParams?.[1],
      );
      requests.length = 0;
      head = {
        ...head,
        rows: head.rows.map((row, index) =>
          index === head.rows.length - 1
            ? { ...row, last_message_at_ms: row.last_message_at_ms + 1 }
            : row,
        ),
      };

      await act(() => vi.advanceTimersByTimeAsync(15_000));

      expect(requests).toEqual(['head', 'tail-1', 'tail-2', 'head', 'head']);
      expect(result.current.data?.pages).toHaveLength(3);
      expect(result.current.data?.pageParams).not.toEqual(originalParams);
    },
  );

  test('a queued Load more is dropped when its horizon is no longer selected', async () => {
    const client = makeClient();
    const { result, rerender } = renderHook(
      ({ horizon }: { horizon: '24h' | 'all' }) =>
        useCacheKeepaliveSessions(PRINCIPAL_ID, { horizon }),
      {
        initialProps: { horizon: '24h' },
        wrapper: makeWrapper(client),
      },
    );
    await act(() => vi.advanceTimersByTimeAsync(1));
    const original24h = client.getQueryData(
      qk.cacheKeepaliveSessions(PRINCIPAL_ID, { horizon: '24h' }),
    );
    const { promise, resolve } = Promise.withResolvers<Response>();
    const server = globalThis.fetch;
    const requestsByHorizon: string[] = [];
    let holdPoll = true;
    vi.stubGlobal(
      'fetch',
      vi.fn(async (input: string) => {
        const url = new URL(input, 'http://localhost');
        const scope = url.searchParams.get('horizon');
        requestsByHorizon.push(
          `${scope}:${url.searchParams.get('cursor') ?? 'head'}`,
        );
        if (scope === '24h' && holdPoll) {
          holdPoll = false;
          return promise;
        }
        return server(input);
      }),
    );
    await act(() => vi.advanceTimersByTimeAsync(5_000));
    let append: Promise<unknown> | undefined;
    act(() => {
      append = result.current.fetchNextPage();
    });
    rerender({ horizon: 'all' });
    await act(() => vi.advanceTimersByTimeAsync(1));
    await act(async () => {
      resolve(jsonResponse(head));
      await append;
    });
    await act(() => vi.advanceTimersByTimeAsync(1));

    expect(requestsByHorizon).toEqual(['24h:head', 'all:head']);
    expect(
      client.getQueryData(
        qk.cacheKeepaliveSessions(PRINCIPAL_ID, { horizon: '24h' }),
      ),
    ).toEqual(original24h);
    expect(result.current.data?.pages).toHaveLength(1);
  });

  test('unchanged totals and boundary retain stale tail rows until the sheet reopens', async () => {
    const { result, rerender } = renderHook(
      ({ enabled }) => useCacheKeepaliveSessions(PRINCIPAL_ID, {}, enabled),
      { initialProps: { enabled: true }, wrapper: makeWrapper(makeClient()) },
    );
    await act(() => vi.advanceTimersByTimeAsync(1));
    await act(async () => {
      await result.current.fetchNextPage();
    });
    await act(() => vi.advanceTimersByTimeAsync(1));
    await act(async () => {
      await result.current.fetchNextPage();
    });
    await act(() => vi.advanceTimersByTimeAsync(1));
    head = {
      ...head,
      rows: head.rows.map((row, index) =>
        index === 0 ? { ...row, state: 'expired', is_active: false } : row,
      ),
    };
    requests.length = 0;

    await act(() => vi.advanceTimersByTimeAsync(5_000));

    expect(requests).toEqual(['head']);
    expect(result.current.data?.pages[0].rows[0].state).toBe('expired');
    expect(result.current.data?.pages[1].rows[0].state).toBe('renewed');
    rerender({ enabled: false });
    requests.length = 0;
    await act(() => vi.advanceTimersByTimeAsync(10_000));
    expect(requests).toEqual([]);
    rerender({ enabled: true });
    await act(() => vi.advanceTimersByTimeAsync(10_001));

    expect(requests).toEqual(['head', 'cursor-1', 'cursor-2', 'head', 'head']);
    expect(result.current.data?.pages[1].rows[0].state).toBe('expired');
    expect(result.current.data?.pages).toHaveLength(3);
  });

  test('single-page polling uses one request per interval and Load more still appends', async () => {
    const { result } = renderHook(
      () => useCacheKeepaliveSessions(PRINCIPAL_ID),
      { wrapper: makeWrapper(makeClient()) },
    );
    await act(() => vi.advanceTimersByTimeAsync(1));
    expect(requests).toEqual(['head']);
    requests.length = 0;
    head = {
      ...head,
      summary: { ...cacheKeepaliveSummaryFixture, total_spent: 10 },
    };

    await act(() => vi.advanceTimersByTimeAsync(15_000));

    expect(requests).toEqual(['head', 'head', 'head']);
    expect(result.current.data?.pages).toHaveLength(1);
    await act(async () => {
      await result.current.fetchNextPage();
    });
    await act(() => vi.advanceTimersByTimeAsync(1));
    expect(requests).toEqual(['head', 'head', 'head', 'cursor-1']);
    expect(result.current.data?.pages).toHaveLength(2);
  });

  test('a failed initial overview can be retried without a second list request', async () => {
    const server = globalThis.fetch;
    vi.stubGlobal(
      'fetch',
      vi
        .fn()
        .mockResolvedValueOnce(
          jsonResponse({ error: 'unavailable' }, { status: 503 }),
        )
        .mockImplementation(server),
    );
    const { result } = renderHook(
      () => useCacheKeepaliveSessions(PRINCIPAL_ID),
      { wrapper: makeWrapper(makeClient()) },
    );
    await act(() => vi.advanceTimersByTimeAsync(1));
    expect(result.current.isError).toBe(true);

    await act(async () => {
      await result.current.refetch();
    });
    await act(() => vi.advanceTimersByTimeAsync(1));

    expect(result.current.isError).toBe(false);
    expect(result.current.data?.pages[0]).toEqual(head);
    expect(requests).toEqual(['head']);
  });

  test('a failed changed-aggregate tail refresh remains eligible for the next poll', async () => {
    const { result } = renderHook(
      () => useCacheKeepaliveSessions(PRINCIPAL_ID),
      { wrapper: makeWrapper(makeClient()) },
    );
    await act(() => vi.advanceTimersByTimeAsync(1));
    await act(async () => {
      await result.current.fetchNextPage();
    });
    await act(() => vi.advanceTimersByTimeAsync(1));
    requests.length = 0;
    head = {
      ...head,
      summary: { ...cacheKeepaliveSummaryFixture, total_spent: 10 },
    };
    const server = globalThis.fetch;
    let failOnce = true;
    vi.stubGlobal(
      'fetch',
      vi.fn(async (input: string) => {
        if (input.includes('cursor=') && failOnce) {
          failOnce = false;
          requests.push('cursor-1');
          return jsonResponse({ error: 'unavailable' }, { status: 503 });
        }
        return server(input);
      }),
    );

    await act(() => vi.advanceTimersByTimeAsync(10_000));

    expect(requests).toEqual(['head', 'cursor-1', 'head', 'cursor-1']);
    expect(result.current.data?.pages[0]).toEqual(head);
    expect(result.current.isError).toBe(false);
  });

  test.each(['append-first', 'poll-first'] as const)(
    'overlapping Load more and a changed overview preserve both operations (%s)',
    async (order) => {
      const { result } = renderHook(
        () => useCacheKeepaliveSessions(PRINCIPAL_ID),
        { wrapper: makeWrapper(makeClient()) },
      );
      await act(() => vi.advanceTimersByTimeAsync(1));
      if (order === 'poll-first') {
        await act(async () => {
          await result.current.fetchNextPage();
        });
        await act(() => vi.advanceTimersByTimeAsync(1));
      }
      requests.length = 0;
      const { promise, resolve } = Promise.withResolvers<Response>();
      const server = globalThis.fetch;
      let holdOnce = true;
      vi.stubGlobal(
        'fetch',
        vi.fn(async (input: string) => {
          if (input.includes('cursor=') && holdOnce) {
            holdOnce = false;
            requests.push('cursor-1');
            return promise;
          }
          return server(input);
        }),
      );
      head = {
        ...head,
        summary: { ...cacheKeepaliveSummaryFixture, total_spent: 10 },
      };
      let append: Promise<unknown> = Promise.resolve();
      if (order === 'append-first') {
        act(() => {
          append = result.current.fetchNextPage();
        });
        await act(() => vi.advanceTimersByTimeAsync(1));
      }
      await act(() => vi.advanceTimersByTimeAsync(5_000));
      if (order === 'poll-first') {
        act(() => {
          append = result.current.fetchNextPage();
        });
      }
      await act(async () => {
        resolve(
          jsonResponse({
            summary: null,
            groups: null,
            rows: [{ ...head.rows[0], id: 'cursor-1' }],
            next_cursor: 'cursor-2',
          }),
        );
        await append;
      });
      await act(() => vi.advanceTimersByTimeAsync(1));

      expect(requests).toEqual(
        order === 'append-first'
          ? ['cursor-1', 'head', 'cursor-1']
          : ['head', 'cursor-1', 'cursor-2'],
      );
      expect(result.current.data?.pages).toHaveLength(
        order === 'append-first' ? 2 : 3,
      );
      expect(result.current.data?.pages[0]).toEqual(head);
    },
  );

  test('Load more requested at the poll snapshot boundary survives the tail write', async () => {
    const client = makeClient();
    const { result } = renderHook(
      () => useCacheKeepaliveSessions(PRINCIPAL_ID),
      { wrapper: makeWrapper(client) },
    );
    await act(() => vi.advanceTimersByTimeAsync(1));
    await act(async () => {
      await result.current.fetchNextPage();
    });
    await act(() => vi.advanceTimersByTimeAsync(1));
    requests.length = 0;
    head = {
      ...head,
      summary: { ...cacheKeepaliveSummaryFixture, total_spent: 10 },
    };
    let armed = true;
    let append: Promise<unknown> | undefined;
    const read = client.getQueryData.bind(client);
    const readAtBoundary: typeof client.getQueryData = <
      TData,
      TKey extends readonly unknown[],
      TInferred,
    >(
      key: TKey,
    ) => {
      const value = read<TData, TKey, TInferred>(key);
      if (armed && key.length === 4 && key[1] === 'sessions') {
        armed = false;
        append = result.current.fetchNextPage();
      }
      return value;
    };
    vi.spyOn(client, 'getQueryData').mockImplementation(readAtBoundary);

    await act(() => vi.advanceTimersByTimeAsync(15_000));
    await act(async () => {
      await append;
    });
    await act(() => vi.advanceTimersByTimeAsync(1));

    expect(armed).toBe(false);
    expect(requests).toEqual(['head', 'cursor-1', 'cursor-2', 'head', 'head']);
    expect(result.current.data?.pages).toHaveLength(3);
    expect(result.current.data?.pageParams).toEqual([
      undefined,
      'cursor-1',
      'cursor-2',
    ]);
    expect(result.current.data?.pages[2].rows[0].id).toBe('cursor-2');
  });

  test('hidden tabs stop polling and visible tabs resume', async () => {
    const { rerender } = renderHook(
      () => useCacheKeepaliveSessions(PRINCIPAL_ID),
      { wrapper: makeWrapper(makeClient()) },
    );
    await act(() => vi.advanceTimersByTimeAsync(1));
    requests.length = 0;
    visible = false;
    rerender();

    await act(() => vi.advanceTimersByTimeAsync(15_000));

    expect(requests).toEqual([]);
    visible = true;
    rerender();
    await act(() => vi.advanceTimersByTimeAsync(5_000));
    expect(requests).toEqual(['head']);
  });
});

describe('useCacheKeepaliveSessionDetail', () => {
  test('fetches one session detail and decodes its turns', async () => {
    const fetchMock = stubFetchOnce(cacheKeepaliveRenewedDetailFixture);
    const { result } = renderHook(
      () => useCacheKeepaliveSessionDetail(PRINCIPAL_ID, SESSION_ID, '24h'),
      { wrapper: makeWrapper(makeClient()) },
    );

    await waitFor(() => expect(result.current.isSuccess).toBe(true));

    const detailUrl = new URL(urlFrom(fetchMock), 'http://localhost');
    expect(detailUrl.pathname).toBe(SESSION_DETAIL_PATH);
    expect(detailUrl.searchParams.get('id')).toBe(SESSION_ID);
    expect(detailUrl.searchParams.get('horizon')).toBe('24h');
    expect([...detailUrl.searchParams.keys()]).toEqual(['id', 'horizon']);
    expect(result.current.data?.turns).toHaveLength(3);
    expect(result.current.data?.config_snapshot?.snapshot_bytes).toBe(524_288);
  });

  test('selecting a different session never shows the previous session while it loads', async () => {
    const { promise: secondResponse, resolve: resolveSecond } =
      Promise.withResolvers<Response>();
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(jsonResponse(cacheKeepaliveRenewedDetailFixture))
      .mockImplementationOnce(() => secondResponse);
    vi.stubGlobal('fetch', fetchMock);
    const { result, rerender } = renderHook(
      ({ sessionId }) =>
        useCacheKeepaliveSessionDetail(PRINCIPAL_ID, sessionId, '24h'),
      {
        initialProps: { sessionId: SESSION_ID },
        wrapper: makeWrapper(makeClient()),
      },
    );
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    rerender({ sessionId: 'other-session' });
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));
    expect(result.current.data).toBeUndefined();
    expect(result.current.isLoading).toBe(true);
    resolveSecond(
      jsonResponse({
        ...cacheKeepaliveRenewedDetailFixture,
        id: 'other-session',
      }),
    );
    await waitFor(() => expect(result.current.data?.id).toBe('other-session'));
  });

  test('an advancing list fingerprint starts a new request while the first detail is in flight', async () => {
    const { promise: firstResponse, resolve: resolveFirst } =
      Promise.withResolvers<Response>();
    const updated = { ...cacheKeepaliveRenewedDetailFixture, generation: 8 };
    const fetchMock = vi
      .fn()
      .mockImplementationOnce(() => firstResponse)
      .mockResolvedValue(jsonResponse(updated));
    vi.stubGlobal('fetch', fetchMock);
    const client = makeClient();
    const { result, rerender } = renderHook(
      ({ fingerprint }) =>
        useCacheKeepaliveSessionDetail(
          PRINCIPAL_ID,
          SESSION_ID,
          '24h',
          fingerprint,
        ),
      {
        initialProps: { fingerprint: 'generation-7' },
        wrapper: makeWrapper(client),
      },
    );
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));
    rerender({ fingerprint: 'generation-8' });
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(result.current.data?.generation).toBe(8));
    resolveFirst(jsonResponse(cacheKeepaliveRenewedDetailFixture));
    await waitFor(() =>
      expect(
        client.getQueryState(
          qk.cacheKeepaliveSessionDetail(
            PRINCIPAL_ID,
            SESSION_ID,
            '24h',
            'generation-7',
          ),
        )?.status,
      ).toBe('success'),
    );
    expect(result.current.data?.generation).toBe(8);
  });

  test('same-generation economics changes refetch, but an unchanged fingerprint does not', async () => {
    const original = {
      ...cacheKeepaliveRenewedDetailFixture,
      is_active: false,
    };
    const updated = { ...original, net_pnl: 9, total_avoided: 10 };
    const { promise: nextResponse, resolve: resolveNext } =
      Promise.withResolvers<Response>();
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(jsonResponse(original))
      .mockImplementationOnce(() => nextResponse);
    vi.stubGlobal('fetch', fetchMock);
    const { result, rerender } = renderHook(
      ({ fingerprint }) =>
        useCacheKeepaliveSessionDetail(
          PRINCIPAL_ID,
          SESSION_ID,
          '24h',
          fingerprint,
        ),
      {
        initialProps: { fingerprint: 'economics-before' },
        wrapper: makeWrapper(makeClient()),
      },
    );
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    rerender({ fingerprint: 'economics-before' });
    expect(fetchMock).toHaveBeenCalledTimes(1);
    rerender({ fingerprint: 'economics-after' });
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));
    expect(result.current.data).toEqual(original);
    expect(result.current.isPlaceholderData).toBe(true);
    resolveNext(jsonResponse(updated));
    await waitFor(() => expect(result.current.data?.net_pnl).toBe(9));
    expect(result.current.data?.generation).toBe(original.generation);
    rerender({ fingerprint: 'economics-after' });
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  test('stays idle and issues no request when sessionId is null', () => {
    const fetchMock = stubFetchOnce(cacheKeepaliveRenewedDetailFixture);
    const { result } = renderHook(
      () => useCacheKeepaliveSessionDetail(PRINCIPAL_ID, null),
      { wrapper: makeWrapper(makeClient()) },
    );

    expect(fetchMock).not.toHaveBeenCalled();
    expect(result.current.fetchStatus).toBe('idle');
    expect(result.current.data).toBeUndefined();
  });
});

describe('in-request detail polling', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.spyOn(visibilityManager, 'useVisibility').mockReturnValue({
      visible: true,
      online: true,
      hiddenSince: null,
      gracePeriodElapsed: false,
    });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  test.each(['scheduled', 'cancelled', 'expired'] as const)(
    'polls an inactive in-request projection until it becomes %s',
    async (nextState) => {
      const inRequest = {
        ...cacheKeepaliveRenewedDetailFixture,
        state: 'in_request',
        is_active: false,
        evidence_status: 'none',
        cancel_reason: null,
        reason: 'A real request is in progress',
        error: null,
      };
      const resolved = {
        ...inRequest,
        state: nextState,
        is_active: nextState === 'scheduled',
        reason:
          nextState === 'expired'
            ? 'Request did not complete before its deadline'
            : nextState === 'cancelled'
              ? 'Cancelled: user turn detected'
              : 'agent-in-turn — first renewal in 4m 30s',
      };
      const fetchMock = vi
        .fn()
        .mockResolvedValueOnce(jsonResponse(inRequest))
        .mockImplementation(async () => jsonResponse(resolved));
      vi.stubGlobal('fetch', fetchMock);
      const { result, unmount } = renderHook(
        () => useCacheKeepaliveSessionDetail(PRINCIPAL_ID, SESSION_ID),
        { wrapper: makeWrapper(makeClient()) },
      );
      await act(() => vi.advanceTimersByTimeAsync(1));
      expect(result.current.data?.state).toBe('in_request');

      await act(() =>
        vi.advanceTimersByTimeAsync(
          POLLING_INTERVALS.CACHE_KEEPALIVE_DETAIL_MS,
        ),
      );
      expect(result.current.data?.state).toBe(nextState);
      expect(fetchMock).toHaveBeenCalledTimes(2);

      await act(() =>
        vi.advanceTimersByTimeAsync(
          POLLING_INTERVALS.CACHE_KEEPALIVE_DETAIL_MS,
        ),
      );
      expect(fetchMock).toHaveBeenCalledTimes(
        nextState === 'scheduled' ? 3 : 2,
      );
      unmount();
    },
  );
});
