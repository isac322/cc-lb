import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { renderHook } from '@testing-library/react';
import type { PropsWithChildren } from 'react';
import { describe, expect, it } from 'vitest';
import type { UpstreamOAuthStatusResponse } from './api';
import { useOAuthReconnectNudges } from './oauthReconnect';
import { qk, type Upstream } from './queries';

describe('provider reconnect polling', () => {
  it('includes OpenAI and Gemini accounts, including disabled ones', () => {
    const client = new QueryClient({
      defaultOptions: { queries: { retry: false, staleTime: Infinity } },
    });
    const upstreams: Upstream[] = (
      ['openai_oauth', 'gemini_oauth'] as const
    ).map((kind) => ({
      id: kind,
      name: kind,
      kind,
      enabled: false,
      spec_revision: 1,
      base_url: null,
      warmup_enabled: false,
      warmup_dialect_plugin: null,
      status: {
        last_apply_error: 'status_401',
        last_apply_at_unix_secs: null,
        last_warmup_at_unix_secs: null,
      },
    }));
    for (const upstream of upstreams) {
      client.setQueryData(qk.upstreamOauthStatus(upstream.id), {
        upstream_id: upstream.id,
        kind: upstream.kind,
        has_credentials: true,
        status: 'active',
        expires_at_unix_secs: null,
        refresh_token_present: true,
        refresh_token_expires_at_unix_secs: null,
        scopes: [],
        mode: 'refreshing',
        can_refresh: true,
      } satisfies UpstreamOAuthStatusResponse);
    }
    const wrapper = ({ children }: PropsWithChildren) => (
      <QueryClientProvider client={client}>{children}</QueryClientProvider>
    );
    const { result, unmount } = renderHook(
      () => useOAuthReconnectNudges(upstreams),
      { wrapper },
    );

    expect([...result.current.nudges.keys()]).toEqual([
      'openai_oauth',
      'gemini_oauth',
    ]);
    for (const nudge of result.current.nudges.values()) {
      expect(nudge.label).toBe('Sign-in rejected');
    }
    unmount();
    client.clear();
  });
});
