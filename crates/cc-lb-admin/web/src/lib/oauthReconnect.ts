// OAuth reconnect nudge classification + polling.
// Single source of truth for deciding when an OAuth upstream needs the user to
// re-run the connect flow. Anthropic's long-lived grant uses the access-token
// deadline; refreshing credentials use a refresh-token deadline only when the
// provider reports one. OpenAI and Google do not inherit Anthropic's yearly
// grant lifecycle. Consumed by the per-upstream detail notice and the global
// summary; both share the same query keys as useUpstreamOAuthStatus so the
// existing completion invalidation clears every surface at once.

import {
  type UseQueryResult,
  useQueries,
  useQueryClient,
} from '@tanstack/react-query';
import { useCallback, useEffect, useMemo, useRef } from 'react';
import {
  getJson,
  type Upstream,
  type UpstreamOAuthStatusResponse,
} from './api';
import { qk } from './queries';
import { useVisibility } from './visibilityManager';

export type OAuthReconnectReason =
  | 'credentials_unreadable'
  | 'not_connected'
  | 'long_lived_rejected'
  | 'long_lived_expired'
  | 'long_lived_expiring'
  | 'refresh_token_expired'
  | 'renewal_rejected'
  | 'refresh_token_expiring'
  | 'access_token_expired'
  | 'refresh_token_missing';

export interface OAuthReconnectNudge {
  reason: OAuthReconnectReason;
  tone: 'warn' | 'danger';
  label: string;
  description: string;
  actionLabel: 'Connect' | 'Reconnect';
  /** Relevant deadline (unix secs) when one is known; null when unknown. */
  expiresAt: number | null;
}

/** Terminal credential failures outrank quota and ordinary health danger. */
export function isTerminalOAuthReconnectReason(
  reason: OAuthReconnectReason,
): boolean {
  return (
    reason === 'refresh_token_expired' ||
    reason === 'renewal_rejected' ||
    reason === 'long_lived_rejected'
  );
}

const OAUTH_STATUS_POLL_MS = 30_000;

// Refreshing credentials live on the refresh token's multi-week clock, so
// "soon" is days, not minutes — warn while the refresh token is inside this
// window. Once it lapses the connection can no longer renew itself, and the
// upstream must be reauthorized.
export const REFRESH_EXPIRING_SOON_SECS = 3 * 24 * 60 * 60;

// A long-lived 365-day grant has no refresh token to renew it, so the access
// token's own expiry IS the login deadline. Two weeks gives an operator room
// to schedule the yearly reauthorization without an outage.
export const LONG_LIVED_EXPIRING_SOON_SECS = 14 * 24 * 60 * 60;

// The refresh worker persists status_400 only for invalid_grant, status_401
// for unauthorized credentials, and refresh_token_expired for a known lapsed
// deadline. Other HTTP, network and local failures remain transient.

export function classifyOAuthReconnect(
  status: UpstreamOAuthStatusResponse | undefined,
  lastApplyError: string | null | undefined,
  nowSecs: number,
): OAuthReconnectNudge | null {
  // No data yet (or query failed): never masquerade as either healthy or
  // broken — the caller distinguishes loading/error separately.
  if (!status) return null;
  // Non-OAuth upstreams are never queried, but stay honest if one arrives.
  if (status.status === 'wrong_kind') return null;
  const concise = status.kind !== 'anthropic_oauth';

  // Corrupted is checked before has_credentials so an undecryptable row can
  // never fall into the "not connected" branch below regardless of how the
  // backend reports has_credentials for it.
  if (status.status === 'corrupted') {
    return {
      reason: 'credentials_unreadable',
      tone: 'danger',
      label: concise ? 'Sign-in unavailable' : 'Stored credentials unreadable',
      description: concise
        ? 'The saved sign-in could not be read. Reconnect the account.'
        : 'The stored OAuth credentials could not be decrypted. Reconnect the account to restore requests.',
      actionLabel: 'Reconnect',
      expiresAt: null,
    };
  }

  if (status.status === 'missing' || !status.has_credentials) {
    return {
      reason: 'not_connected',
      tone: 'warn',
      label: concise ? 'Account not connected' : 'OAuth not connected',
      description: concise
        ? 'Connect an account to enable requests.'
        : 'No OAuth credentials are stored for this upstream. Connect an account to enable requests.',
      actionLabel: 'Connect',
      expiresAt: null,
    };
  }

  // A long-lived credential never refreshes: the refresh token is stored but
  // unusable, and no renewal ever runs. An authoritative authentication
  // failure still means the credential was rejected and must be reconnected.
  // Check it before access-token expiry so the actionable cause wins.
  if (status.kind === 'anthropic_oauth' && status.mode === 'long_lived_365d') {
    if (lastApplyError === 'status_400' || lastApplyError === 'status_401') {
      return {
        reason: 'long_lived_rejected',
        tone: 'danger',
        label: 'Long-lived credential rejected',
        description:
          'The long-lived OAuth credential was rejected. Reconnect the account to restore requests.',
        actionLabel: 'Reconnect',
        expiresAt: null,
      };
    }

    // Ignore the refresh token entirely and drive the nudge off the access
    // token, which is the only real deadline.
    const accessExpiresAt = status.expires_at_unix_secs;
    // An unknown deadline gets no invented countdown.
    if (accessExpiresAt == null) return null;
    if (accessExpiresAt <= nowSecs) {
      return {
        reason: 'long_lived_expired',
        tone: 'danger',
        label: 'Long-lived token expired',
        description:
          'The 365-day OAuth access token has expired. It cannot be refreshed, so reconnect the account to restore requests.',
        actionLabel: 'Reconnect',
        expiresAt: accessExpiresAt,
      };
    }
    if (accessExpiresAt - nowSecs <= LONG_LIVED_EXPIRING_SOON_SECS) {
      return {
        reason: 'long_lived_expiring',
        tone: 'warn',
        label: 'Long-lived token expiring soon',
        description:
          'The 365-day OAuth access token expires within 2 weeks and cannot be refreshed. Reconnect before the deadline to avoid interruption.',
        actionLabel: 'Reconnect',
        expiresAt: accessExpiresAt,
      };
    }
    return null;
  }

  const refreshExpiresAt = status.refresh_token_expires_at_unix_secs;
  if (
    lastApplyError === 'refresh_token_expired' ||
    (refreshExpiresAt != null && refreshExpiresAt <= nowSecs)
  ) {
    return {
      reason: 'refresh_token_expired',
      tone: 'danger',
      label: concise ? 'Sign-in expired' : 'Refresh token expired',
      description: concise
        ? 'The account sign-in has expired. Reconnect to restore requests.'
        : 'The OAuth refresh token has expired, so the connection can no longer renew itself. Reconnect the account to restore renewal.',
      actionLabel: 'Reconnect',
      expiresAt: concise ? null : refreshExpiresAt,
    };
  }

  if (
    lastApplyError === 'status_400' ||
    lastApplyError === 'status_401' ||
    (status.kind === 'openai_oauth' &&
      (lastApplyError === 'refresh_token_reused' ||
        lastApplyError === 'refresh_token_invalidated' ||
        lastApplyError === 'invalid_grant'))
  ) {
    return {
      reason: 'renewal_rejected',
      tone: 'danger',
      label: concise ? 'Sign-in rejected' : 'Token renewal failed',
      description: concise
        ? 'The provider rejected this account sign-in. Reconnect to restore requests.'
        : 'The latest token renewal attempt failed. Reconnect the account to restore automatic renewal.',
      actionLabel: 'Reconnect',
      expiresAt: null,
    };
  }

  if (
    refreshExpiresAt != null &&
    refreshExpiresAt - nowSecs <= REFRESH_EXPIRING_SOON_SECS
  ) {
    return {
      reason: 'refresh_token_expiring',
      tone: 'warn',
      label: concise ? 'Sign-in expires soon' : 'Refresh token expiring soon',
      description: concise
        ? 'Reconnect before the deadline to keep requests working.'
        : 'The OAuth refresh token expires within 3 days. Reconnect before the deadline to avoid interruption.',
      actionLabel: 'Reconnect',
      expiresAt: refreshExpiresAt,
    };
  }

  // Access-only provider imports intentionally cannot refresh. Their access
  // deadline still matters, but a missing refresh token is only a warning for
  // credentials that are expected to renew.
  if (!status.refresh_token_present || (concise && !status.can_refresh)) {
    const accessExpiresAt = status.expires_at_unix_secs;
    if (accessExpiresAt != null && accessExpiresAt <= nowSecs) {
      return {
        reason: 'access_token_expired',
        tone: 'danger',
        label: concise ? 'Sign-in expired' : 'Access token expired',
        description: concise
          ? 'The account sign-in has expired. Reconnect to restore requests.'
          : 'The OAuth access token has expired and no refresh token is stored to renew it. Reconnect the account to restore requests.',
        actionLabel: 'Reconnect',
        expiresAt: concise ? null : accessExpiresAt,
      };
    }
    if (concise && !status.can_refresh) return null;
    return {
      reason: 'refresh_token_missing',
      tone: 'warn',
      label: concise ? 'Sign-in incomplete' : 'No refresh token',
      description: concise
        ? 'This sign-in cannot stay connected. Reconnect to keep requests working.'
        : 'The stored OAuth credentials have no refresh token, so the connection cannot renew itself. Reconnect the account before the access token expires.',
      actionLabel: 'Reconnect',
      expiresAt: accessExpiresAt,
    };
  }

  // Access-token expiry alone is not a reconnect signal while a refresh token
  // remains usable, and an unknown refresh expiry gets no countdown.
  return null;
}

export interface OAuthReconnectNudgesResult {
  nudges: ReadonlyMap<string, OAuthReconnectNudge>;
  isPending: boolean;
  isError: boolean;
}

const NO_UPSTREAMS: readonly Upstream[] = [];

/**
 * Polls /oauth/status for every OAuth-kind upstream, enabled and disabled.
 * Shares query keys with useUpstreamOAuthStatus, so completion invalidation
 * refreshes every reconnect surface.
 */
export function useOAuthReconnectNudges(
  upstreams: readonly Upstream[] = NO_UPSTREAMS,
): OAuthReconnectNudgesResult {
  const queryClient = useQueryClient();
  const visibility = useVisibility();
  const isHidden = visibility.gracePeriodElapsed || !visibility.visible;

  const oauthUpstreams = useMemo(
    () => upstreams.filter((u) => u.kind.endsWith('_oauth')),
    [upstreams],
  );

  const queries = useMemo(
    () =>
      oauthUpstreams.map((u) => ({
        queryKey: qk.upstreamOauthStatus(u.id),
        queryFn: ({ signal }: { signal: AbortSignal }) =>
          getJson<UpstreamOAuthStatusResponse>(
            `/admin/v1/upstreams/${u.id}/oauth/status`,
            { signal },
          ),
        refetchInterval: isHidden ? (false as const) : OAUTH_STATUS_POLL_MS,
      })),
    [oauthUpstreams, isHidden],
  );

  // Match usePolledData: force a refresh when the tab becomes visible again so
  // nudges never sit on stale data after a hidden pause.
  const wasHiddenRef = useRef(isHidden);
  useEffect(() => {
    if (wasHiddenRef.current && !isHidden) {
      for (const u of oauthUpstreams) {
        queryClient.invalidateQueries({
          queryKey: qk.upstreamOauthStatus(u.id),
        });
      }
    }
    wasHiddenRef.current = isHidden;
  }, [isHidden, oauthUpstreams, queryClient]);

  const combine = useCallback(
    (results: UseQueryResult<UpstreamOAuthStatusResponse>[]) => {
      const nowSecs = Math.floor(Date.now() / 1000);
      const nudges: Record<string, OAuthReconnectNudge> = {};
      let isPending = false;
      let isError = false;
      results.forEach((result, index) => {
        const upstream = oauthUpstreams[index];
        if (!upstream) return;
        if (result.isPending) isPending = true;
        if (result.isError) isError = true;
        const nudge = classifyOAuthReconnect(
          result.data,
          upstream.status.last_apply_error,
          nowSecs,
        );
        if (nudge) nudges[upstream.id] = nudge;
      });
      return { nudges, isPending, isError };
    },
    [oauthUpstreams],
  );

  const combined = useQueries({ queries, combine });

  // useQueries structurally shares the combined plain object; wrap it in the
  // contracted Map only when the underlying nudges actually change.
  const nudges = useMemo(
    () => new Map<string, OAuthReconnectNudge>(Object.entries(combined.nudges)),
    [combined.nudges],
  );

  return {
    nudges,
    isPending: combined.isPending,
    isError: combined.isError,
  };
}
