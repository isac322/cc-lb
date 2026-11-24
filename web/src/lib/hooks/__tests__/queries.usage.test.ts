import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { renderHook, waitFor } from '@testing-library/react';
import { createElement, type ReactNode } from 'react';
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest';
import { qk, useSummary, useUsage } from '../../queries';

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
      '/admin/v1/dashboard/usage?range=6h&step=minute&group_by=principal&projection=totals',
    );
    expect(qk.usage('6h', 'minute', 'principal')).not.toEqual(
      qk.usage('6h', 'minute', 'principal', undefined, 'totals'),
    );
  });
  test('sends provider families and isolates provider-grouped cache entries', async () => {
    const fetchMock = vi.fn(async (_input: RequestInfo | URL) =>
      emptyUsageResponse(),
    );
    vi.stubGlobal('fetch', fetchMock);
    const client = makeClient();
    const { result, rerender } = renderHook(
      ({ providers }: { providers: ('anthropic' | 'openai')[] }) =>
        useUsage('6h', 'minute', 'provider', { providers }),
      {
        initialProps: { providers: ['anthropic'] },
        wrapper: makeWrapper(client),
      },
    );
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(String(fetchMock.mock.calls[0][0])).toContain(
      'group_by=provider&providers=anthropic',
    );
    rerender({ providers: ['openai'] });
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));
    expect(String(fetchMock.mock.calls[1][0])).toContain('providers=openai');
    expect(
      qk.usage(
        '6h',
        'minute',
        'provider',
        undefined,
        undefined,
        'anthropic',
        undefined,
      ),
    ).not.toEqual(
      qk.usage(
        '6h',
        'minute',
        'provider',
        undefined,
        undefined,
        'openai',
        undefined,
      ),
    );
  });
  test('sends principal_id and isolates principal ledger cache entries', async () => {
    const fetchMock = vi.fn(async (_input: RequestInfo | URL) =>
      emptyUsageResponse(),
    );
    vi.stubGlobal('fetch', fetchMock);
    const { result } = renderHook(
      () =>
        useUsage('6h', 'minute', 'model', {
          principalId: 'principal-123',
        }),
      { wrapper: makeWrapper(makeClient()) },
    );
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(String(fetchMock.mock.calls[0][0])).toContain(
      'group_by=model&principal_id=principal-123',
    );
    expect(
      qk.usage('6h', 'minute', 'model', undefined, undefined, undefined),
    ).not.toEqual(
      qk.usage(
        '6h',
        'minute',
        'model',
        undefined,
        undefined,
        undefined,
        'principal-123',
      ),
    );
  });

  test('does not turn an empty provider intersection into an unfiltered read', () => {
    const fetchMock = vi.fn(async (_input: RequestInfo | URL) =>
      emptyUsageResponse(),
    );
    vi.stubGlobal('fetch', fetchMock);
    const { result } = renderHook(
      () => useUsage('6h', 'minute', 'principal', { providers: [] }),
      { wrapper: makeWrapper(makeClient()) },
    );
    expect(result.current.fetchStatus).toBe('idle');
    expect(fetchMock).not.toHaveBeenCalled();
  });

  test('summary sends the same provider filter and separates its cache key', async () => {
    const fetchMock = vi.fn(
      async (_input: RequestInfo | URL) =>
        new Response(
          JSON.stringify({
            range: '6h',
            step: 'minute',
            window_start_unix_secs: 0,
            window_end_unix_secs: 21_600,
            observed: false,
            totals: {},
            sparklines: {},
          }),
          { status: 200, headers: { 'Content-Type': 'application/json' } },
        ),
    );
    vi.stubGlobal('fetch', fetchMock);
    const { result } = renderHook(
      () => useSummary('6h', { providers: ['gemini'] }),
      { wrapper: makeWrapper(makeClient()) },
    );
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(String(fetchMock.mock.calls[0][0])).toContain(
      'range=6h&providers=gemini',
    );
    expect(qk.summary('6h', 'gemini')).not.toEqual(qk.summary('6h', 'openai'));
  });
});
