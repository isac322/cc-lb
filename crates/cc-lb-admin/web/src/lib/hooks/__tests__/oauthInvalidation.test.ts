import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { renderHook } from '@testing-library/react';
import { createElement, type ReactNode } from 'react';
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest';
import { qk, useOAuthComplete, useOAuthStart } from '../../queries';

const UPSTREAM_ID = '00000000-0000-0000-0000-000000000001';

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

beforeEach(() => {
  vi.stubGlobal(
    'fetch',
    vi.fn(async (input: RequestInfo | URL) => {
      const url =
        typeof input === 'string'
          ? input
          : input instanceof URL
            ? input.toString()
            : input.url;
      if (url.endsWith('/oauth/start')) {
        return jsonResponse({
          authorize_url: 'https://example.test/auth',
          state_token: 'state',
          revision: 1,
        });
      }
      if (url.endsWith('/oauth/complete')) {
        return jsonResponse({
          upstream_id: UPSTREAM_ID,
          expires_at_unix_secs: 9_999_999_999,
          access_token_fingerprint: 'abcd1234',
        });
      }
      return new Response('not found', { status: 404 });
    }),
  );
});

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe('OAuth invalidation contract', () => {
  test('useOAuthComplete success invalidates oauth status, runtime status, subscription metadata, and upstreams', async () => {
    const client = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });
    const invalidateSpy = vi.spyOn(client, 'invalidateQueries');
    const { result } = renderHook(() => useOAuthComplete(), {
      wrapper: makeWrapper(client),
    });

    await result.current.mutateAsync({
      id: UPSTREAM_ID,
      state_token: 'state',
      code: 'code',
    });
    const invalidatedKeys = invalidateSpy.mock.calls.map(
      ([arg]) => arg?.queryKey,
    );
    expect(invalidatedKeys).toEqual(
      expect.arrayContaining([
        qk.upstreams,
        qk.status,
        qk.upstreamOauthStatus(UPSTREAM_ID),
        qk.upstreamSubscriptionMetadata(UPSTREAM_ID),
      ]),
    );
  });

  test('useOAuthStart success invalidates nothing - reconnect alone must not flip OAuth Status', async () => {
    const client = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });
    const invalidateSpy = vi.spyOn(client, 'invalidateQueries');
    const { result } = renderHook(() => useOAuthStart(), {
      wrapper: makeWrapper(client),
    });

    await result.current.mutateAsync(UPSTREAM_ID);

    expect(invalidateSpy).not.toHaveBeenCalled();
  });
});
