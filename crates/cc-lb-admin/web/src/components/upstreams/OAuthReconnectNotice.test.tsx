import { cleanup, render, screen } from '@testing-library/react';
import type { ReactNode } from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type * as OAuthReconnectModule from '../../lib/oauthReconnect';
import {
  type OAuthReconnectNudge,
  useOAuthReconnectNudges,
} from '../../lib/oauthReconnect';
import type { Upstream } from '../../lib/queries';
import { useUpstreams } from '../../lib/queries';
import { OAuthReconnectSummary } from './OAuthReconnectNotice';

vi.mock('@tanstack/react-router', () => ({
  useNavigate: () => vi.fn(),
}));

vi.mock('../../lib/oauthReconnect', async (importOriginal) => {
  const actual = await importOriginal<typeof OAuthReconnectModule>();
  return {
    ...actual,
    useOAuthReconnectNudges: vi.fn(),
  };
});
vi.mock('../../lib/queries', () => ({
  useUpstreams: vi.fn(),
}));

vi.mock('../ui/primitives', () => ({
  Button: ({ children }: { children?: ReactNode }) => (
    <button type="button">{children}</button>
  ),
  Notice: ({
    action,
    children,
    title,
  }: {
    action?: ReactNode;
    children?: ReactNode;
    title?: ReactNode;
  }) => (
    <section>
      <h2>{title}</h2>
      <div>{children}</div>
      {action}
    </section>
  ),
}));

vi.mock('../ui/RelativeTime', () => ({
  RelativeTime: () => null,
}));

afterEach(() => {
  cleanup();
});

const disabledOAuth = {
  id: 'oauth-disabled',
  name: 'Disabled OAuth',
  kind: 'anthropic_oauth',
  enabled: false,
} as Upstream;

const expiredNudge: OAuthReconnectNudge = {
  reason: 'refresh_token_expired',
  tone: 'danger',
  label: 'Refresh token expired',
  description: 'Reconnect the account.',
  actionLabel: 'Reconnect',
  expiresAt: null,
};
const enabledUnreadableOAuth = {
  id: 'oauth-unreadable',
  name: 'Unreadable OAuth',
  kind: 'anthropic_oauth',
  enabled: true,
} as Upstream;

const unreadableNudge: OAuthReconnectNudge = {
  reason: 'credentials_unreadable',
  tone: 'danger',
  label: 'Stored credentials unreadable',
  description: 'Reconnect the account.',
  actionLabel: 'Reconnect',
  expiresAt: null,
};

const healthy = {
  nudges: new Map<string, OAuthReconnectNudge>(),
  isPending: false,
  isError: false,
};

describe('OAuthReconnectSummary', () => {
  beforeEach(() => {
    vi.mocked(useUpstreams).mockReturnValue({
      data: { upstreams: [disabledOAuth] },
      isPending: false,
      isError: false,
    } as never);
    vi.mocked(useOAuthReconnectNudges).mockReturnValue(healthy);
  });

  it('surfaces a disabled expired OAuth upstream in the overview', () => {
    vi.mocked(useOAuthReconnectNudges).mockReturnValue({
      ...healthy,
      nudges: new Map([[disabledOAuth.id, expiredNudge]]),
    });

    render(<OAuthReconnectSummary />);

    expect(
      screen.getByText('1 OAuth connection needs attention'),
    ).toBeDefined();
    expect(
      screen.getByText('Refresh token expired: Disabled OAuth'),
    ).toBeDefined();
  });
  it('puts terminal rejected credentials before ordinary danger in the overview', () => {
    vi.mocked(useUpstreams).mockReturnValue({
      data: { upstreams: [disabledOAuth, enabledUnreadableOAuth] },
      isPending: false,
      isError: false,
    } as never);
    vi.mocked(useOAuthReconnectNudges).mockReturnValue({
      ...healthy,
      nudges: new Map<string, OAuthReconnectNudge>([
        [disabledOAuth.id, { ...expiredNudge, reason: 'renewal_rejected' }],
        [enabledUnreadableOAuth.id, unreadableNudge],
      ]),
    });

    render(<OAuthReconnectSummary />);

    expect(screen.getByText('Disabled OAuth, Unreadable OAuth')).toBeDefined();
  });

  it('surfaces status query errors instead of claiming healthy', () => {
    vi.mocked(useOAuthReconnectNudges).mockReturnValue({
      ...healthy,
      isError: true,
    });

    render(<OAuthReconnectSummary />);

    expect(screen.getByText('OAuth status check failed')).toBeDefined();
  });

  it('stays quiet while status is loading and clears after reconnect', () => {
    vi.mocked(useUpstreams).mockReturnValue({
      data: undefined,
      isPending: true,
      isError: false,
    } as never);
    vi.mocked(useOAuthReconnectNudges).mockReturnValue(healthy);
    const { rerender } = render(<OAuthReconnectSummary />);
    expect(screen.queryByText('OAuth status check failed')).toBeNull();

    vi.mocked(useUpstreams).mockReturnValue({
      data: { upstreams: [disabledOAuth] },
      isPending: false,
      isError: false,
    } as never);
    vi.mocked(useOAuthReconnectNudges).mockReturnValue({
      ...healthy,
      nudges: new Map([[disabledOAuth.id, expiredNudge]]),
    });
    rerender(<OAuthReconnectSummary />);
    expect(
      screen.getByText('Refresh token expired: Disabled OAuth'),
    ).toBeDefined();

    vi.mocked(useOAuthReconnectNudges).mockReturnValue(healthy);
    rerender(<OAuthReconnectSummary />);
    expect(
      screen.queryByText('Refresh token expired: Disabled OAuth'),
    ).toBeNull();
  });
});
