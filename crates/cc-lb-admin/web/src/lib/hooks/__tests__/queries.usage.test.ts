import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { renderHook, waitFor } from '@testing-library/react';
import { createElement, type ReactNode } from 'react';
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest';
import { qk, useUsage } from '../../queries';

function makeClient() {
  return new QueryClient({
    defaultOptions: {
      queries: { retry: false },
    },
  });
}

function makeWrapper(client: QueryClient) {
  return function Wrapper({ children }: { children: ReactNode }) {
    return createElement(QueryClientProvider, { client }, children);
  };
}

function emptyUsageResponse() {
  return new Response(
    JSON.stringify({
      range: '6h',
      step: 'minute',
      group_by: 'principal',
      window_start_unix_secs: 0,
      window_end_unix_secs: 21_600,
      series: [],
      observed: false,
    }),
    {
      status: 200,
      headers: { 'Content-Type': 'application/json' },
    },
  );
}

beforeEach(() => {
  localStorage.clear();
});

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe('useUsage', () => {
  test('requests and caches the totals projection independently', async () => {
    const fetchMock = vi.fn(async (_input: RequestInfo | URL) =>
      emptyUsageResponse(),
    );
    vi.stubGlobal('fetch', fetchMock);

    const { result } = renderHook(
      () => useUsage('6h', 'minute', 'principal', undefined, 'totals'),
      { wrapper: makeWrapper(makeClient()) },
    );

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [input] = fetchMock.mock.calls[0];
    const url =
      typeof input === 'string'
        ? input
        : input instanceof URL
          ? input.toString()
          : input.url;
    expect(url).toBe(
      '/admin/usage?range=6h&step=minute&group_by=principal&projection=totals',
    );
    expect(qk.usage('6h', 'minute', 'principal')).not.toEqual(
      qk.usage('6h', 'minute', 'principal', undefined, 'totals'),
    );
  });
});
