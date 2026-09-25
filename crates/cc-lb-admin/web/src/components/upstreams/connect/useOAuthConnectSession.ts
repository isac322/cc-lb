// Engine behind the upstream connect dialog's Claude sign-in:
//
// - The authorize URL is fetched *before* the operator clicks, so "Sign in" is
//   a plain <a target="_blank"> and can never be popup-blocked.
// - The 15-minute PKCE window is tracked; an expired session is replaced with
//   one click (never silently, so an open Anthropic tab is not orphaned).
// - Internal plumbing (state token) never reaches the screen.
// - Reconnect verifies *which* Claude account came back by refreshing the
//   upstream's subscription metadata and comparing it to the account that was
//   connected before.

import { useQueryClient } from '@tanstack/react-query';
import { useCallback, useEffect, useRef, useState } from 'react';
import {
  ApiError,
  type DraftCompleteResponse,
  type OAuthFallbackReason,
  type OAuthTokenMode,
  type OrganizationMetadataInner,
  triggerSubscriptionMetadataRefresh,
} from '../../../lib/api';
import {
  qk,
  useCompleteOauthDraft,
  useOAuthComplete,
  useOAuthStart,
  useStartOauthDraft,
} from '../../../lib/queries';
import { stateOfAuthorizeUrl } from './parseOAuthPaste';

/** Mirrors PKCE_FLOW_TTL_SECS in crates/cc-lb-admin/src/v1/oauth.rs. */
export const OAUTH_SESSION_TTL_MS = 900_000;

export type OAuthConnectTarget =
  | { mode: 'create' }
  | { mode: 'reconnect'; upstreamId: string };

export type SessionPhase = 'starting' | 'ready' | 'expired' | 'start_failed';

export interface AccountIdentity {
  email: string | null;
  displayName: string | null;
  accountUuid: string | null;
  organizationName: string | null;
  organizationType: string | null;
  rateLimitTier: string | null;
}

export type IdentityVerdict =
  | 'same'
  | 'different'
  | 'unknown'
  | 'first_connect';

export interface ConnectOutcome {
  mode: OAuthTokenMode;
  longLivedFallback: boolean;
  fallbackReason: OAuthFallbackReason | null;
  grantedExpiresInSecs: number | null;
  account: AccountIdentity | null;
  /** Create only: the draft to finalize with `createFromDraft`. */
  draft: DraftCompleteResponse | null;
  verdict: IdentityVerdict;
  /** Reconnect only: the account connected before this sign-in. */
  previousAccount: AccountIdentity | null;
}

export function identityOf(
  org: OrganizationMetadataInner | null | undefined,
): AccountIdentity | null {
  if (!org) return null;
  return {
    email: org.account_email,
    displayName: org.account_display_name,
    accountUuid: org.account_uuid,
    organizationName: org.organization_name,
    organizationType: org.organization_type,
    rateLimitTier: org.rate_limit_tier,
  };
}

export function compareIdentity(
  before: AccountIdentity | null,
  after: AccountIdentity | null,
): IdentityVerdict {
  if (!before || (!before.accountUuid && !before.email)) return 'first_connect';
  if (!after || (!after.accountUuid && !after.email)) return 'unknown';
  if (before.accountUuid && after.accountUuid) {
    return before.accountUuid === after.accountUuid ? 'same' : 'different';
  }
  return before.email === after.email ? 'same' : 'different';
}

export function errorMessage(error: unknown): string {
  if (error instanceof ApiError) {
    return error.message || `Request failed (${error.status})`;
  }
  return error instanceof Error ? error.message : String(error);
}

interface Session {
  authorizeUrl: string;
  stateToken: string;
  /** `state` as Anthropic will echo it back in `code#state`. */
  oauthState: string | null;
  expiresAtMs: number;
}

export interface OAuthConnectSession {
  phase: SessionPhase;
  /** Plain link target for "Sign in with Claude"; null until the session starts. */
  authorizeUrl: string | null;
  /** `state` Anthropic will echo in `code#state`; compare pastes against it. */
  oauthState: string | null;
  remainingMs: number;
  startError: string | null;
  /** Discard the current sign-in and issue a fresh 15-minute one. */
  restart: () => Promise<void>;
  /** Exchange a code. Resolves null on failure (see `completeError`). */
  complete: (code: string) => Promise<ConnectOutcome | null>;
  completing: boolean;
  completeError: string | null;
  clearCompleteError: () => void;
  outcome: ConnectOutcome | null;
}

export function useOAuthConnectSession(
  target: OAuthConnectTarget,
  options: {
    /** Account connected before a reconnect, for the identity comparison. */
    previousAccount?: AccountIdentity | null;
    /** Defer starting until the operator picks the Claude path. */
    enabled?: boolean;
  } = {},
): OAuthConnectSession {
  const { previousAccount = null, enabled = true } = options;
  const qc = useQueryClient();
  const startDraft = useStartOauthDraft();
  const completeDraft = useCompleteOauthDraft();
  const startUpstream = useOAuthStart();
  const completeUpstream = useOAuthComplete();
  const [session, setSession] = useState<Session | null>(null);
  const [phase, setPhase] = useState<SessionPhase>('starting');
  const [startError, setStartError] = useState<string | null>(null);
  const [completing, setCompleting] = useState(false);
  const [completeError, setCompleteError] = useState<string | null>(null);
  const [outcome, setOutcome] = useState<ConnectOutcome | null>(null);
  const [nowMs, setNowMs] = useState(() => Date.now());
  const generation = useRef(0);
  const upstreamId = target.mode === 'reconnect' ? target.upstreamId : null;
  const startDraftAsync = startDraft.mutateAsync;
  const completeDraftAsync = completeDraft.mutateAsync;
  const startUpstreamAsync = startUpstream.mutateAsync;
  const completeUpstreamAsync = completeUpstream.mutateAsync;

  const start = useCallback(async () => {
    const mine = ++generation.current;
    setPhase('starting');
    setStartError(null);
    setCompleteError(null);
    // A new sign-in invalidates any exchanged-but-unsaved result.
    setOutcome(null);
    try {
      const res =
        upstreamId === null
          ? await startDraftAsync()
          : await startUpstreamAsync({ id: upstreamId });
      if (mine !== generation.current) return;
      setSession({
        authorizeUrl: res.authorize_url,
        stateToken: res.state_token,
        oauthState: stateOfAuthorizeUrl(res.authorize_url),
        expiresAtMs: Date.now() + OAUTH_SESSION_TTL_MS,
      });
      setPhase('ready');
    } catch (error) {
      if (mine !== generation.current) return;
      setStartError(errorMessage(error));
      setPhase('start_failed');
    }
  }, [upstreamId, startDraftAsync, startUpstreamAsync]);

  useEffect(() => {
    if (!enabled) return;
    setOutcome(null);
    setSession(null);
    void start();
    return () => {
      generation.current++;
    };
  }, [enabled, start]);

  useEffect(() => {
    if (phase !== 'ready') return;
    const id = window.setInterval(() => setNowMs(Date.now()), 1000);
    return () => window.clearInterval(id);
  }, [phase]);

  useEffect(() => {
    if (phase === 'ready' && session && nowMs >= session.expiresAtMs) {
      setPhase('expired');
    }
  }, [phase, session, nowMs]);

  const complete = useCallback(
    async (code: string): Promise<ConnectOutcome | null> => {
      if (!session || phase !== 'ready' || completing) return null;
      setCompleting(true);
      setCompleteError(null);
      try {
        let next: ConnectOutcome;
        if (upstreamId === null) {
          const draft = await completeDraftAsync({
            state_token: session.stateToken,
            code,
          });
          next = {
            mode: draft.mode,
            longLivedFallback: draft.long_lived_fallback,
            fallbackReason: draft.fallback_reason,
            grantedExpiresInSecs: draft.granted_expires_in_secs,
            account: identityOf(draft.organization_metadata),
            draft,
            verdict: 'first_connect',
            previousAccount: null,
          };
        } else {
          const id = upstreamId;
          // Stores the credential and invalidates every OAuth-derived query.
          const res = await completeUpstreamAsync({
            id,
            state_token: session.stateToken,
            code,
          });
          // The credential is stored; now learn which account it belongs to.
          let account: AccountIdentity | null = null;
          try {
            const meta = await triggerSubscriptionMetadataRefresh(id);
            qc.setQueryData(qk.upstreamSubscriptionMetadata(id), meta);
            account = identityOf(meta.organization_metadata);
          } catch {
            account = null;
          }
          next = {
            mode: res.mode,
            longLivedFallback: res.long_lived_fallback,
            fallbackReason: res.fallback_reason,
            grantedExpiresInSecs: res.granted_expires_in_secs,
            account,
            draft: null,
            verdict: compareIdentity(previousAccount, account),
            // Snapshot: the live metadata query flips to the new account.
            previousAccount,
          };
        }
        setOutcome(next);
        return next;
      } catch (error) {
        const message = errorMessage(error);
        setCompleteError(message);
        // A consumed or expired state can never succeed again; offer a restart.
        if (/expired|already used|restart/i.test(message)) setPhase('expired');
        return null;
      } finally {
        setCompleting(false);
      }
    },
    [
      session,
      phase,
      completing,
      upstreamId,
      completeDraftAsync,
      completeUpstreamAsync,
      qc,
      previousAccount,
    ],
  );

  const remainingMs = session ? Math.max(0, session.expiresAtMs - nowMs) : 0;

  return {
    phase,
    authorizeUrl: session?.authorizeUrl ?? null,
    oauthState: session?.oauthState ?? null,
    remainingMs,
    startError,
    restart: start,
    complete,
    completing,
    completeError,
    clearCompleteError: () => setCompleteError(null),
    outcome,
  };
}

export function formatRemaining(ms: number): string {
  const total = Math.ceil(ms / 1000);
  const m = Math.floor(total / 60);
  const s = total % 60;
  return `${m}:${String(s).padStart(2, '0')}`;
}
