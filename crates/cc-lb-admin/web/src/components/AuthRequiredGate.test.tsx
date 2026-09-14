// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { AUTH_TOKEN_KEY } from '../lib/auth';
import { useAuthSessionContext } from '../lib/authSession';
import { AuthRequiredGate } from './AuthRequiredGate';

function SessionConsumer() {
  const session = useAuthSessionContext();
  return (
    <span>
      {session ? `${session.subject}:${session.auth_mode}` : 'no session'}
    </span>
  );
}

function unauthorizedResponse(authMode: 'static_token' | 'external') {
  return new Response(
    JSON.stringify({
      error: 'unauthorized',
      auth_mode: authMode,
    }),
    {
      status: 401,
      headers: { 'Content-Type': 'application/json' },
    },
  );
}

describe('AuthRequiredGate', () => {
  beforeEach(() => {
    localStorage.clear();
  });

  afterEach(() => {
    cleanup();
    localStorage.clear();
    vi.unstubAllGlobals();
  });

  it('renders protected content with the authenticated session context', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue(
        new Response(
          JSON.stringify({
            authority: 'https://access.example.com',
            subject: 'alice',
            kind: 'human',
            provider_id: 'cloudflare',
            email: 'alice@example.com',
            display_name: 'Alice',
            expires_at_unix_secs: 2_000_000_000,
            auth_mode: 'external',
          }),
          {
            status: 200,
            headers: { 'Content-Type': 'application/json' },
          },
        ),
      ),
    );

    render(
      <AuthRequiredGate>
        <div>Protected content</div>
        <SessionConsumer />
      </AuthRequiredGate>,
    );

    expect(await screen.findByText('Protected content')).toBeDefined();
    expect(screen.getByText('alice:external')).toBeDefined();
  });

  it('renders the token form when the session is unauthorized and no token is stored', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue(
        new Response(
          JSON.stringify({
            error: 'unauthorized',
            auth_mode: 'static_token',
          }),
          {
            status: 401,
            headers: { 'Content-Type': 'application/json' },
          },
        ),
      ),
    );

    render(
      <AuthRequiredGate>
        <div>Protected content</div>
      </AuthRequiredGate>,
    );

    expect(
      await screen.findByRole('heading', { name: 'Admin token required' }),
    ).toBeDefined();
    expect(screen.getByLabelText('Bearer token')).toBeDefined();
    expect(screen.getByRole('button', { name: 'Sign in' })).toBeDefined();
    expect(screen.queryByText('Protected content')).toBeNull();
  });

  it('shows feedback when a submitted token is rejected', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn().mockImplementation(() => unauthorizedResponse('static_token')),
    );

    render(
      <AuthRequiredGate>
        <div>Protected content</div>
      </AuthRequiredGate>,
    );

    const input = await screen.findByLabelText('Bearer token');
    fireEvent.change(input, { target: { value: 'wrong-token' } });
    fireEvent.click(screen.getByRole('button', { name: 'Sign in' }));

    expect(await screen.findByText('Admin token was rejected')).toBeDefined();
    expect(screen.queryByText('Protected content')).toBeNull();
  });

  it('recovers a mixed-mode session by removing a stale local token and retrying once', async () => {
    localStorage.setItem(AUTH_TOKEN_KEY, 'stale-token');
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            error: 'unauthorized',
            auth_mode: 'external',
          }),
          {
            status: 401,
            headers: { 'Content-Type': 'application/json' },
          },
        ),
      )
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            authority: 'https://access.example.com',
            subject: 'alice',
            kind: 'human',
            provider_id: 'cloudflare',
            email: 'alice@example.com',
            display_name: 'Alice',
            expires_at_unix_secs: 2_000_000_000,
            auth_mode: 'external',
          }),
          {
            status: 200,
            headers: { 'Content-Type': 'application/json' },
          },
        ),
      );
    vi.stubGlobal('fetch', fetchMock);

    render(
      <AuthRequiredGate>
        <div>Protected content</div>
      </AuthRequiredGate>,
    );

    expect(await screen.findByText('Protected content')).toBeDefined();
    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(
      new Headers(fetchMock.mock.calls[0]?.[1]?.headers).get('Authorization'),
    ).toBe('Bearer stale-token');
    expect(
      new Headers(fetchMock.mock.calls[1]?.[1]?.headers).has('Authorization'),
    ).toBe(false);
    expect(localStorage.getItem(AUTH_TOKEN_KEY)).toBeNull();
    expect(
      screen.queryByRole('heading', { name: 'Admin token required' }),
    ).toBeNull();
  });

  it('stops after one token-free external retry and never shows the token form', async () => {
    localStorage.setItem(AUTH_TOKEN_KEY, 'stale-token');
    const fetchMock = vi
      .fn()
      .mockImplementation(() => unauthorizedResponse('external'));
    vi.stubGlobal('fetch', fetchMock);

    render(
      <AuthRequiredGate>
        <div>Protected content</div>
      </AuthRequiredGate>,
    );

    expect(
      await screen.findByRole('heading', {
        name: 'External authentication required',
      }),
    ).toBeDefined();
    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(
      new Headers(fetchMock.mock.calls[0]?.[1]?.headers).get('Authorization'),
    ).toBe('Bearer stale-token');
    expect(
      new Headers(fetchMock.mock.calls[1]?.[1]?.headers).has('Authorization'),
    ).toBe(false);
    expect(localStorage.getItem(AUTH_TOKEN_KEY)).toBeNull();
    expect(screen.queryByLabelText('Bearer token')).toBeNull();
  });

  it('shows external-authentication recovery when external credentials are missing', async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          error: 'unauthorized',
          auth_mode: 'external',
        }),
        {
          status: 401,
          headers: { 'Content-Type': 'application/json' },
        },
      ),
    );
    vi.stubGlobal('fetch', fetchMock);

    render(
      <AuthRequiredGate>
        <div>Protected content</div>
      </AuthRequiredGate>,
    );

    expect(
      await screen.findByRole('heading', {
        name: 'External authentication required',
      }),
    ).toBeDefined();
    expect(screen.getByRole('button', { name: 'Retry' })).toBeDefined();
    expect(screen.queryByLabelText('Bearer token')).toBeNull();
    expect(screen.queryByText('Protected content')).toBeNull();
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });
});
