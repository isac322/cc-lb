// OAuth reconnect nudge classification + polling.
// Single source of truth for deciding when an OAuth upstream needs the user to
// re-run the connect flow. The deadline clock depends on the credential mode:
// refreshing credentials are driven by the refresh token's expiry, while a
// long-lived 365-day grant is driven by the access token itself since nothing
// ever renews it. Consumed by the per-upstream detail notice and the global
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

export interface OAuthReconnectNudge {
  tone: 'warn' | 'danger';
  label: string;
  description: string;
  actionLabel: 'Connect' | 'Reconnect';
  /** Relevant deadline (unix secs) when one is known; null when unknown. */
  expiresAt: number | null;
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

// The refresh worker records failures in last_apply_error via reason_for():
// "status_<code>" for token-endpoint rejections, plus network/parse/cancelled/
// decrypt/encrypt/storage/missing_credentials. Only an exact 400/401 is
// treated as an authoritative renewal failure; every other reason is
// transient or local and must not be presented as a reconnect requirement.

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

  // Corrupted is checked before has_credentials so an undecryptable row can
  // never fall into the "not connected" branch below regardless of how the
  // backend reports has_credentials for it.
  if (status.status === 'corrupted') {
    return {
      tone: 'danger',
      label: 'Stored credentials unreadable',
      description:
        'The stored OAuth credentials could not be decrypted. Reconnect the account to restore requests.',
      actionLabel: 'Reconnect',
      expiresAt: null,
    };
  }

  if (status.status === 'missing' || !status.has_credentials) {
    return {
      tone: 'warn',
      label: 'OAuth not connected',
      description:
        'No OAuth credentials are stored for this upstream. Connect an account to enable requests.',
      actionLabel: 'Connect',
      expiresAt: null,
    };
  }

  // A long-lived credential never refreshes: the refresh token is stored but
  // unusable, and no renewal ever runs, so last_apply_error stays empty.
  // Ignore the refresh token entirely and drive the nudge off the access
  // token, which is the only real deadline.
  if (status.mode === 'long_lived_365d') {
    const accessExpiresAt = status.expires_at_unix_secs;
    // An unknown deadline gets no invented countdown.
    if (accessExpiresAt == null) return null;
    if (accessExpiresAt <= nowSecs) {
      return {
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
  if (refreshExpiresAt != null && refreshExpiresAt <= nowSecs) {
    return {
      tone: 'danger',
      label: 'Refresh token expired',
      description:
        'The OAuth refresh token has expired, so the connection can no longer renew itself. Reconnect the account to restore renewal.',
      actionLabel: 'Reconnect',
      expiresAt: refreshExpiresAt,
    };
  }

  if (lastApplyError === 'status_400' || lastApplyError === 'status_401') {
    return {
      tone: 'danger',
      label: 'Token renewal failed',
      description:
        'The latest token renewal attempt failed. Reconnect the account to restore automatic renewal.',
      actionLabel: 'Reconnect',
      expiresAt: null,
    };
  }

  if (
    refreshExpiresAt != null &&
    refreshExpiresAt - nowSecs <= REFRESH_EXPIRING_SOON_SECS
  ) {
    return {
      tone: 'warn',
      label: 'Refresh token expiring soon',
      description:
        'The OAuth refresh token expires within 3 days. Reconnect before the deadline to avoid interruption.',
      actionLabel: 'Reconnect',
      expiresAt: refreshExpiresAt,
    };
  }

  // A missing refresh token means renewal is impossible. Once the access
  // token is also past its expiry the connection is already broken — danger;
  // otherwise warn without claiming an outage.
  if (!status.refresh_token_present) {
    const accessExpiresAt = status.expires_at_unix_secs;
    if (accessExpiresAt != null && accessExpiresAt <= nowSecs) {
      return {
        tone: 'danger',
        label: 'Access token expired',
        description:
          'The OAuth access token has expired and no refresh token is stored to renew it. Reconnect the account to restore requests.',
        actionLabel: 'Reconnect',
        expiresAt: accessExpiresAt,
      };
    }
    return {
      tone: 'warn',
      label: 'No refresh token',
      description:
        'The stored OAuth credentials have no refresh token, so the connection cannot renew itself. Reconnect the account before the access token expires.',
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
 * Polls /oauth/status for every OAuth-kind upstream in the list (enabled and
 * disabled alike — callers decide which to surface). Shares query keys with
 * useUpstreamOAuthStatus, so completion invalidation refreshes both.
 */
export function useOAuthReconnectNudges(
  upstreams: readonly Upstream[] = NO_UPSTREAMS,
): OAuthReconnectNudgesResult {
  const queryClient = useQueryClient();
  const visibility = useVisibility();
  const isHidden = visibility.gracePeriodElapsed || !visibility.visible;

  const oauthUpstreams = useMemo(
    () => upstreams.filter((u) => u.kind === 'anthropic_oauth'),
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
