import { describe, expect, it } from 'vitest';
import type { OAuthReconnectNudge } from '../../lib/oauthReconnect';
import { upstreamHealth } from './upstreamHealth';

const expiredNudge: OAuthReconnectNudge = {
  reason: 'refresh_token_expired',
  tone: 'danger',
  label: 'Refresh token expired',
  description: 'Reconnect the account.',
  actionLabel: 'Reconnect',
  expiresAt: null,
};

describe('upstreamHealth', () => {
  it('keeps a disabled OAuth upstream visibly in reconnect danger', () => {
    expect(upstreamHealth(false, 'active', expiredNudge)).toEqual({
      tone: 'danger',
      label: 'Refresh token expired',
    });
  });

  it('keeps a healthy disabled non-OAuth upstream neutral', () => {
    expect(upstreamHealth(false, 'active', null)).toEqual({
      tone: 'neutral',
      label: 'Disabled',
    });
  });

  it('lets a reconnect nudge outrank runtime state', () => {
    const nudge: OAuthReconnectNudge = {
      ...expiredNudge,
      reason: 'refresh_token_expiring',
      tone: 'warn',
      label: 'Refresh token expiring soon',
    };
    expect(upstreamHealth(true, 'error', nudge)).toEqual({
      tone: 'warn',
      label: 'Refresh token expiring soon',
    });
  });
});
