import { describe, expect, it } from 'vitest';
import type { UpstreamOAuthStatusResponse } from './api';
import { classifyOAuthReconnect } from './oauthReconnect';

// Fixed clock: the classifier takes nowSecs explicitly, so tests never touch
// Date.now().
const NOW = 1_800_000_000;
const DAY = 24 * 60 * 60;

function oauthStatus(
  overrides: Partial<UpstreamOAuthStatusResponse> = {},
): UpstreamOAuthStatusResponse {
  return {
    upstream_id: 'up-1',
    kind: 'anthropic_oauth',
    has_credentials: true,
    status: 'active',
    expires_at_unix_secs: NOW + 30 * DAY,
    refresh_token_present: true,
    refresh_token_expires_at_unix_secs: NOW + 60 * DAY,
    scopes: [],
    mode: 'refreshing',
    can_refresh: true,
    ...overrides,
  };
}

function longLived(
  overrides: Partial<UpstreamOAuthStatusResponse> = {},
): UpstreamOAuthStatusResponse {
  // The backend stores the refresh token but reports no expiry for it and
  // never refreshes; the access token is the only real deadline.
  return oauthStatus({
    mode: 'long_lived_365d',
    can_refresh: false,
    refresh_token_expires_at_unix_secs: null,
    ...overrides,
  });
}

describe('classifyOAuthReconnect — long-lived credentials', () => {
  it('ignores a lapsed refresh-token clock entirely', () => {
    // ~11 months out on the access token, refresh token present but already
    // past its expiry: the refresh clock must not influence the nudge.
    const status = longLived({
      expires_at_unix_secs: NOW + 335 * DAY,
      refresh_token_expires_at_unix_secs: NOW - DAY,
    });
    expect(classifyOAuthReconnect(status, null, NOW)).toBeNull();
  });

  it('ignores a missing refresh token while the access token is valid', () => {
    const status = longLived({
      expires_at_unix_secs: NOW + 335 * DAY,
      refresh_token_present: false,
    });
    expect(classifyOAuthReconnect(status, null, NOW)).toBeNull();
  });

  it('warns 10 days before the access token expires, keyed on the access-token deadline', () => {
    const accessExpiresAt = NOW + 10 * DAY;
    const nudge = classifyOAuthReconnect(
      longLived({ expires_at_unix_secs: accessExpiresAt }),
      null,
      NOW,
    );
    expect(nudge).toMatchObject({
      tone: 'warn',
      label: 'Long-lived token expiring soon',
      actionLabel: 'Reconnect',
      expiresAt: accessExpiresAt,
    });
  });

  it('warns at exactly the 14-day boundary but not at 15 days', () => {
    const atBoundary = classifyOAuthReconnect(
      longLived({ expires_at_unix_secs: NOW + 14 * DAY }),
      null,
      NOW,
    );
    expect(atBoundary?.tone).toBe('warn');
    expect(atBoundary?.expiresAt).toBe(NOW + 14 * DAY);

    expect(
      classifyOAuthReconnect(
        longLived({ expires_at_unix_secs: NOW + 15 * DAY }),
        null,
        NOW,
      ),
    ).toBeNull();
  });

  it('reports a terminal danger once the access token has expired', () => {
    const accessExpiresAt = NOW - 1;
    const nudge = classifyOAuthReconnect(
      longLived({ expires_at_unix_secs: accessExpiresAt }),
      null,
      NOW,
    );
    expect(nudge).toMatchObject({
      tone: 'danger',
      label: 'Long-lived token expired',
      actionLabel: 'Reconnect',
      expiresAt: accessExpiresAt,
    });
    // There is no renewal path for a long-lived grant — the description must
    // say so rather than hint at automatic recovery.
    expect(nudge?.description).toContain('cannot be refreshed');
  });

  it('invents no countdown when the access-token expiry is unknown', () => {
    expect(
      classifyOAuthReconnect(
        longLived({ expires_at_unix_secs: null }),
        null,
        NOW,
      ),
    ).toBeNull();
  });

  it('reports a rejected credential before access-token expiry nudges', () => {
    const status = longLived({
      expires_at_unix_secs: NOW + 10 * DAY,
    });
    const nudge = classifyOAuthReconnect(status, 'status_401', NOW);
    expect(nudge).toMatchObject({
      tone: 'danger',
      label: 'Long-lived credential rejected',
      description:
        'The long-lived OAuth credential was rejected. Reconnect the account to restore requests.',
      actionLabel: 'Reconnect',
      expiresAt: null,
    });
  });

  it('keeps the corrupted branch ahead of the mode branch', () => {
    const nudge = classifyOAuthReconnect(
      oauthStatus({
        status: 'corrupted',
        mode: null,
        can_refresh: false,
        expires_at_unix_secs: null,
        refresh_token_present: false,
        refresh_token_expires_at_unix_secs: null,
      }),
      null,
      NOW,
    );
    expect(nudge).toMatchObject({
      tone: 'danger',
      label: 'Stored credentials unreadable',
      actionLabel: 'Reconnect',
      expiresAt: null,
    });
  });
});

describe('classifyOAuthReconnect — refreshing credentials', () => {
  it('warns while the refresh token expires within 3 days', () => {
    const refreshExpiresAt = NOW + 2 * DAY;
    const nudge = classifyOAuthReconnect(
      oauthStatus({ refresh_token_expires_at_unix_secs: refreshExpiresAt }),
      null,
      NOW,
    );
    expect(nudge).toMatchObject({
      tone: 'warn',
      label: 'Refresh token expiring soon',
      actionLabel: 'Reconnect',
      expiresAt: refreshExpiresAt,
    });
  });

  it('reports danger once the refresh token has expired', () => {
    const refreshExpiresAt = NOW - 1;
    const nudge = classifyOAuthReconnect(
      oauthStatus({ refresh_token_expires_at_unix_secs: refreshExpiresAt }),
      null,
      NOW,
    );
    expect(nudge).toMatchObject({
      reason: 'refresh_token_expired',
      tone: 'danger',
      label: 'Refresh token expired',
      actionLabel: 'Reconnect',
      expiresAt: refreshExpiresAt,
    });
  });

  it('recognizes the durable refresh_token_expired marker without a provider body', () => {
    const nudge = classifyOAuthReconnect(
      oauthStatus({ refresh_token_expires_at_unix_secs: NOW + 10 * DAY }),
      'refresh_token_expired',
      NOW,
    );
    expect(nudge).toMatchObject({
      reason: 'refresh_token_expired',
      tone: 'danger',
      label: 'Refresh token expired',
      actionLabel: 'Reconnect',
      expiresAt: NOW + 10 * DAY,
    });
  });

  it('does not turn transient renewal failures into reconnect nudges', () => {
    expect(classifyOAuthReconnect(oauthStatus(), 'network', NOW)).toBeNull();
    expect(
      classifyOAuthReconnect(oauthStatus(), 'status_400_retryable', NOW),
    ).toBeNull();
    expect(classifyOAuthReconnect(oauthStatus(), 'status_429', NOW)).toBeNull();
    expect(classifyOAuthReconnect(oauthStatus(), 'status_500', NOW)).toBeNull();
  });
  it('clears a durable reconnect nudge when the refreshed status is healthy', () => {
    const expired = classifyOAuthReconnect(
      oauthStatus({ refresh_token_expires_at_unix_secs: NOW + 10 * DAY }),
      'refresh_token_expired',
      NOW,
    );
    expect(expired?.reason).toBe('refresh_token_expired');
    expect(
      classifyOAuthReconnect(
        oauthStatus({ refresh_token_expires_at_unix_secs: NOW + 60 * DAY }),
        null,
        NOW,
      ),
    ).toBeNull();
  });

  it('does not fabricate a nudge while OAuth status is unavailable', () => {
    expect(
      classifyOAuthReconnect(undefined, 'refresh_token_expired', NOW),
    ).toBeNull();
  });

  it('reports danger when the latest renewal attempt was rejected', () => {
    const nudge = classifyOAuthReconnect(oauthStatus(), 'status_400', NOW);
    expect(nudge).toMatchObject({
      tone: 'danger',
      label: 'Token renewal failed',
      actionLabel: 'Reconnect',
      expiresAt: null,
    });
  });

  it('ignores a lapsed access token while the refresh token stays healthy', () => {
    const status = oauthStatus({
      expires_at_unix_secs: NOW - DAY,
      refresh_token_expires_at_unix_secs: NOW + 60 * DAY,
    });
    expect(classifyOAuthReconnect(status, null, NOW)).toBeNull();
  });

  it('escalates a missing refresh token with the access-token state', () => {
    const expiredAccess = NOW - DAY;
    const danger = classifyOAuthReconnect(
      oauthStatus({
        refresh_token_present: false,
        refresh_token_expires_at_unix_secs: null,
        expires_at_unix_secs: expiredAccess,
      }),
      null,
      NOW,
    );
    expect(danger).toMatchObject({
      tone: 'danger',
      label: 'Access token expired',
      actionLabel: 'Reconnect',
      expiresAt: expiredAccess,
    });

    const validAccess = NOW + DAY;
    const warn = classifyOAuthReconnect(
      oauthStatus({
        refresh_token_present: false,
        refresh_token_expires_at_unix_secs: null,
        expires_at_unix_secs: validAccess,
      }),
      null,
      NOW,
    );
    expect(warn).toMatchObject({
      tone: 'warn',
      label: 'No refresh token',
      actionLabel: 'Reconnect',
      expiresAt: validAccess,
    });
  });

  it('prompts a first Connect when no credentials are stored', () => {
    const nudge = classifyOAuthReconnect(
      oauthStatus({
        status: 'missing',
        has_credentials: false,
        mode: null,
        can_refresh: false,
        expires_at_unix_secs: null,
        refresh_token_present: false,
        refresh_token_expires_at_unix_secs: null,
      }),
      null,
      NOW,
    );
    expect(nudge).toMatchObject({
      tone: 'warn',
      label: 'OAuth not connected',
      actionLabel: 'Connect',
      expiresAt: null,
    });
  });
});

describe('classifyOAuthReconnect — provider-specific renewal', () => {
  it.each(['openai_oauth', 'gemini_oauth'])(
    'does not apply the Anthropic yearly deadline to %s',
    (kind) => {
      expect(
        classifyOAuthReconnect(
          oauthStatus({
            kind,
            mode: 'long_lived_365d',
            expires_at_unix_secs: NOW + DAY,
            refresh_token_expires_at_unix_secs: null,
          }),
          null,
          NOW,
        ),
      ).toBeNull();
    },
  );

  it('does not invent an OpenAI refresh deadline from an access-token lifetime', () => {
    const status = oauthStatus({
      kind: 'openai_oauth',
      expires_at_unix_secs: NOW + 10 * DAY,
      refresh_token_expires_at_unix_secs: null,
    });
    expect(classifyOAuthReconnect(status, null, NOW)).toBeNull();
    expect(
      classifyOAuthReconnect(
        { ...status, expires_at_unix_secs: NOW - 1 },
        null,
        NOW,
      ),
    ).toBeNull();
  });

  it.each([
    'refresh_token_reused',
    'refresh_token_invalidated',
    'invalid_grant',
  ])('recognizes an OpenAI terminal renewal rejection: %s', (error) => {
    const nudge = classifyOAuthReconnect(
      oauthStatus({
        kind: 'openai_oauth',
        refresh_token_expires_at_unix_secs: null,
      }),
      error,
      NOW,
    );
    expect(nudge).toMatchObject({
      reason: 'renewal_rejected',
      tone: 'danger',
      actionLabel: 'Reconnect',
      expiresAt: null,
    });
    expect(nudge?.description).not.toMatch(/30 days|365-day|long-lived/i);
  });
  it.each(['openai_oauth', 'gemini_oauth'] as const)(
    'keeps terminal reconnect markers visible for %s',
    (kind) => {
      expect(
        classifyOAuthReconnect(oauthStatus({ kind }), 'status_401', NOW),
      ).toMatchObject({
        reason: 'renewal_rejected',
        tone: 'danger',
        actionLabel: 'Reconnect',
      });
      expect(
        classifyOAuthReconnect(
          oauthStatus({ kind }),
          'refresh_token_expired',
          NOW,
        ),
      ).toMatchObject({
        reason: 'refresh_token_expired',
        tone: 'danger',
        actionLabel: 'Reconnect',
      });
      expect(
        classifyOAuthReconnect(oauthStatus({ kind }), 'network', NOW),
      ).toBeNull();
    },
  );
});

describe('classifyOAuthReconnect — access-only provider imports', () => {
  it.each(['openai_oauth', 'gemini_oauth'])(
    '%s stays quiet while an intentionally non-refreshing token is valid',
    (kind) => {
      expect(
        classifyOAuthReconnect(
          oauthStatus({
            kind,
            mode: 'long_lived_365d',
            can_refresh: false,
            refresh_token_present: false,
            refresh_token_expires_at_unix_secs: null,
            expires_at_unix_secs: NOW + DAY,
          }),
          null,
          NOW,
        ),
      ).toBeNull();
    },
  );

  it.each(['openai_oauth', 'gemini_oauth'])(
    '%s invents no warning for an access-only token with unknown expiry',
    (kind) => {
      expect(
        classifyOAuthReconnect(
          oauthStatus({
            kind,
            mode: 'long_lived_365d',
            can_refresh: false,
            refresh_token_present: false,
            refresh_token_expires_at_unix_secs: null,
            expires_at_unix_secs: null,
          }),
          null,
          NOW,
        ),
      ).toBeNull();
    },
  );

  it.each(['openai_oauth', 'gemini_oauth'])(
    '%s requires reconnect once an access-only token reaches its deadline',
    (kind) => {
      for (const expiresAt of [NOW, NOW - DAY]) {
        expect(
          classifyOAuthReconnect(
            oauthStatus({
              kind,
              status: 'expired',
              mode: 'long_lived_365d',
              can_refresh: false,
              refresh_token_present: false,
              refresh_token_expires_at_unix_secs: null,
              expires_at_unix_secs: expiresAt,
            }),
            null,
            NOW,
          ),
        ).toMatchObject({
          reason: 'access_token_expired',
          tone: 'danger',
          label: 'Sign-in expired',
          actionLabel: 'Reconnect',
          expiresAt: null,
        });
      }
    },
  );

  it.each(['openai_oauth', 'gemini_oauth'])(
    '%s preserves rejected-sign-in recovery for an access-only token',
    (kind) => {
      expect(
        classifyOAuthReconnect(
          oauthStatus({
            kind,
            mode: 'long_lived_365d',
            can_refresh: false,
            refresh_token_present: false,
            refresh_token_expires_at_unix_secs: null,
            expires_at_unix_secs: NOW + DAY,
          }),
          'status_401',
          NOW,
        ),
      ).toMatchObject({
        reason: 'renewal_rejected',
        tone: 'danger',
        label: 'Sign-in rejected',
        actionLabel: 'Reconnect',
      });
    },
  );

  it.each(['openai_oauth', 'gemini_oauth'])(
    '%s still warns when a refresh-capable credential loses its refresh token',
    (kind) => {
      expect(
        classifyOAuthReconnect(
          oauthStatus({
            kind,
            can_refresh: true,
            refresh_token_present: false,
            refresh_token_expires_at_unix_secs: null,
            expires_at_unix_secs: NOW + DAY,
          }),
          null,
          NOW,
        ),
      ).toMatchObject({
        reason: 'refresh_token_missing',
        tone: 'warn',
        label: 'Sign-in incomplete',
        actionLabel: 'Reconnect',
      });
    },
  );
});

describe('concise provider reconnect copy', () => {
  it.each(['openai_oauth', 'gemini_oauth'])(
    '%s keeps each actionable reason without credential mechanics',
    (kind) => {
      const cases: {
        overrides: Partial<UpstreamOAuthStatusResponse>;
        error: string | null;
        label: string;
        action: 'Connect' | 'Reconnect';
        deadline: number | null;
      }[] = [
        {
          overrides: { status: 'corrupted' },
          error: null,
          label: 'Sign-in unavailable',
          action: 'Reconnect',
          deadline: null,
        },
        {
          overrides: { status: 'missing', has_credentials: false },
          error: null,
          label: 'Account not connected',
          action: 'Connect',
          deadline: null,
        },
        {
          overrides: { refresh_token_expires_at_unix_secs: NOW - 1 },
          error: null,
          label: 'Sign-in expired',
          action: 'Reconnect',
          deadline: null,
        },
        {
          overrides: {},
          error: 'status_401',
          label: 'Sign-in rejected',
          action: 'Reconnect',
          deadline: null,
        },
        {
          overrides: { refresh_token_expires_at_unix_secs: NOW + DAY },
          error: null,
          label: 'Sign-in expires soon',
          action: 'Reconnect',
          deadline: NOW + DAY,
        },
        {
          overrides: {
            refresh_token_present: false,
            refresh_token_expires_at_unix_secs: null,
            expires_at_unix_secs: NOW - 1,
          },
          error: null,
          label: 'Sign-in expired',
          action: 'Reconnect',
          deadline: null,
        },
        {
          overrides: {
            refresh_token_present: false,
            refresh_token_expires_at_unix_secs: null,
            expires_at_unix_secs: null,
          },
          error: null,
          label: 'Sign-in incomplete',
          action: 'Reconnect',
          deadline: null,
        },
      ];
      for (const { overrides, error, label, action, deadline } of cases) {
        const nudge = classifyOAuthReconnect(
          oauthStatus({ kind, ...overrides }),
          error,
          NOW,
        );
        expect(nudge).toMatchObject({
          label,
          actionLabel: action,
          expiresAt: deadline,
        });
        expect(nudge?.description).not.toMatch(
          /OAuth|refresh token|access token|renew|rotat|long-lived|365-day/i,
        );
      }
    },
  );
});
