import { cleanup, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import type { OAuthReconnectNudge } from '../../lib/oauthReconnect';
import {
  type FreshnessSnapshot,
  QuotaFreshnessCaption,
  upstreamHealth,
} from './upstreamHealth';

afterEach(cleanup);

vi.mock('./QuotaObservedAt', () => ({
  QuotaObservedAt: ({ snapshot }: { snapshot: FreshnessSnapshot }) => (
    <span>{snapshot.observed_at_unix_millis}</span>
  ),
}));

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

describe('QA-21: quota freshness across canonical and raw readings', () => {
  it('uses the oldest stale reading even when a fresh canonical reading is older', () => {
    render(
      <QuotaFreshnessCaption
        snapshots={[
          { state: 'fresh', observed_at_unix_millis: 1000 },
          { state: 'stale', observed_at_unix_millis: 3000 },
          { state: 'stale', observed_at_unix_millis: 2000 },
        ]}
      />,
    );
    const caption = screen.getByTestId('quota-freshness');
    expect(caption.textContent).toBe('Stale:Updated 2000');
    expect(caption.className).toContain('text-warn-text');
  });

  it('uses the oldest fresh reading, including raw-only rows', () => {
    const { rerender } = render(
      <QuotaFreshnessCaption
        snapshots={[
          { state: 'fresh', observed_at_unix_millis: 3000 },
          { state: 'fresh', observed_at_unix_millis: 2000 },
        ]}
      />,
    );
    expect(screen.getByTestId('quota-freshness').textContent).toBe(
      'Updated 2000',
    );
    rerender(
      <QuotaFreshnessCaption
        snapshots={[{ state: 'fresh', observed_at_unix_millis: 1000 }]}
      />,
    );
    expect(screen.getByTestId('quota-freshness').textContent).toBe(
      'Updated 1000',
    );
  });

  it('ignores absent, unobserved and missing-timestamp entries', () => {
    render(
      <QuotaFreshnessCaption
        snapshots={[
          { state: 'absent', observed_at_unix_millis: 1000 },
          { state: 'unobserved', observed_at_unix_millis: null },
          { state: 'fresh', observed_at_unix_millis: null },
          { state: 'stale', observed_at_unix_millis: null },
        ]}
      />,
    );
    expect(screen.queryByTestId('quota-freshness')).toBeNull();
  });
});
