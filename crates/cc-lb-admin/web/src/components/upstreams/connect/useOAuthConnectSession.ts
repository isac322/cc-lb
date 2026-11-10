// Engine behind every subscription sign-in in the upstream connect dialog.
// Anthropic uses a paste-code flow; Gemini uses browser PKCE; OpenAI supports both device and browser sign-in.

import { useQueryClient } from '@tanstack/react-query';
import { useCallback, useEffect, useRef, useState } from 'react';
import {
  ApiError,
  apiErrorDetail,
  type DraftCompleteResponse,
  getJson,
  type OAuthCompletePendingResponse,
  type OAuthFallbackReason,
  type OAuthFlow,
  type OAuthProvider,
  type OAuthStartFlow,
  type OAuthStartResponse,
  type OAuthTokenMode,
  type OrganizationMetadataInner,
  type SubscriptionMetadataResponse,
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
  | { mode: 'create'; provider?: OAuthProvider }
  | { mode: 'reconnect'; upstreamId: string; provider?: OAuthProvider };

export type SessionPhase = 'starting' | 'ready' | 'expired' | 'start_failed';

export interface AccountIdentity {
  email: string | null;
  displayName: string | null;
  accountUuid: string | null;
  organizationName: string | null;
  organizationType: string | null;
  rateLimitTier: string | null;
  plan: string | null;
  projectId: string | null;
}
export type IdentityVerdict =
  | 'same'
  | 'different'
  | 'unknown'
  | 'first_connect';

export interface ConnectOutcome {
  provider: OAuthProvider;
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
    plan: null,
    projectId: null,
  };
}
export function identityOfResult(
  result:
    | Pick<
        DraftCompleteResponse,
        | 'organization_metadata'
        | 'account_email'
        | 'account_id'
        | 'plan'
        | 'project_id'
      >
    | null
    | undefined,
): AccountIdentity | null {
  if (!result) return null;
  const fromOrg = identityOf(result.organization_metadata);
  if (fromOrg) {
    return {
      ...fromOrg,
      email: fromOrg.email ?? result.account_email ?? null,
      accountUuid: fromOrg.accountUuid ?? result.account_id ?? null,
      rateLimitTier: fromOrg.rateLimitTier ?? result.plan ?? null,
      plan: result.plan ?? fromOrg.plan,
      projectId: result.project_id ?? fromOrg.projectId,
    };
  }
  if (!result.account_email && !result.account_id && !result.project_id)
    return null;
  return {
    email: result.account_email ?? null,
    displayName: null,
    accountUuid: result.account_id ?? null,
    organizationName: null,
    organizationType: null,
    rateLimitTier: result.plan ?? null,
    plan: result.plan ?? null,
    projectId: result.project_id ?? null,
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

export function oauthCompleteErrorMessage(
  error: unknown,
  provider: OAuthProvider,
): string {
  if (!(error instanceof ApiError)) return errorMessage(error);
  const detail = apiErrorDetail(error);
  const body =
    error.body && typeof error.body === 'object'
      ? (error.body as Record<string, unknown>)
      : null;
  const providerError =
    body && typeof body.provider_error === 'string'
      ? body.provider_error
      : null;
  const providerName =
    provider === 'gemini'
      ? 'Google'
      : provider === 'openai'
        ? 'ChatGPT'
        : 'Anthropic';
  if (provider === 'gemini' && error.code === 'missing_project_id') {
    return `This Google account has no Gemini Code Assist project${
      detail ? `: ${detail}` : ''
    }`;
  }
  if (
    error.code === 'token_exchange_rejected' ||
    error.code === 'invalid_grant' ||
    providerError === 'invalid_grant'
  ) {
    const code =
      providerError ?? (error.code === 'invalid_grant' ? error.code : null);
    return `${providerName} rejected the code${code ? ` (${code})` : ''}${
      detail ? `: ${detail}` : ''
    }`;
  }
  const labels: Record<string, string> = {
    token_endpoint_unreachable:
      'The provider token service could not be reached',
    invalid_token_response: 'The provider returned an invalid token response',
    userinfo_failed: 'The provider could not load account details',
    load_code_assist_failed: 'Google Code Assist could not load this account',
    onboard_user_failed: 'Google Code Assist could not provision this account',
    missing_account_identity: 'The provider did not return account identity',
  };
  const label = error.code ? labels[error.code] : undefined;
  if (label) return `${label}${detail ? `: ${detail}` : ''}`;
  if (error.code && detail) return `${error.code}: ${detail}`;
  if (error.code) return error.code;
  return error.message || `Request failed (${error.status})`;
}

interface Session {
  authorizeUrl: string | null;
  verificationUrl: string | null;
  userCode: string | null;
  stateToken: string;
  oauthState: string | null;
  flow: OAuthFlow;
  pollIntervalMs: number;
  expiresAtMs: number;
}

export interface OAuthConnectSession {
  provider: OAuthProvider;
  flow: OAuthFlow | null;
  phase: SessionPhase;
  authorizeUrl: string | null;
  verificationUrl: string | null;
  userCode: string | null;
  pollIntervalSecs: number;
  oauthState: string | null;
  remainingMs: number;
  startError: string | null;
  startNotice: string | null;
  restart: () => Promise<void>;
  startBrowser: () => Promise<void>;
  complete: (code?: string) => Promise<ConnectOutcome | null>;
  completing: boolean;
  polling: boolean;
  completeError: string | null;
  clearCompleteError: () => void;
  outcome: ConnectOutcome | null;
}

function isPending(
  value: DraftCompleteResponse | OAuthCompletePendingResponse | unknown,
): value is OAuthCompletePendingResponse {
  if (value === null || typeof value !== 'object' || !('status' in value)) {
    return false;
  }
  return value.status === 'pending';
}

export function useOAuthConnectSession(
  target: OAuthConnectTarget,
  options: {
    previousAccount?: AccountIdentity | null;
    enabled?: boolean;
  } = {},
): OAuthConnectSession {
  const { previousAccount = null, enabled = true } = options;
  const qc = useQueryClient();
  const startDraft = useStartOauthDraft();
  const completeDraft = useCompleteOauthDraft();
  const startUpstream = useOAuthStart();
  const completeUpstream = useOAuthComplete();
  const provider = target.provider ?? 'anthropic';
  const upstreamId = target.mode === 'reconnect' ? target.upstreamId : null;
  const [session, setSession] = useState<Session | null>(null);
  const [phase, setPhase] = useState<SessionPhase>('starting');
  const [startError, setStartError] = useState<string | null>(null);
  const [startNotice, setStartNotice] = useState<string | null>(null);
  const [completing, setCompleting] = useState(false);
  const [polling, setPolling] = useState(false);
  const [completeError, setCompleteError] = useState<string | null>(null);
  const [outcome, setOutcome] = useState<ConnectOutcome | null>(null);
  const [nowMs, setNowMs] = useState(() => Date.now());
  const generation = useRef(0);
  const pollIntervalMsRef = useRef(0);
  const completeRef = useRef<OAuthConnectSession['complete'] | null>(null);
  const phaseRef = useRef<SessionPhase>(phase);
  const outcomeRef = useRef<ConnectOutcome | null>(outcome);
  const startDraftAsync = startDraft.mutateAsync;
  const completeDraftAsync = completeDraft.mutateAsync;
  const startUpstreamAsync = startUpstream.mutateAsync;
  const completeUpstreamAsync = completeUpstream.mutateAsync;

  const start = useCallback(
    async (
      requestedFlow?: OAuthStartFlow,
      notice: string | null = null,
    ): Promise<void> => {
      const mine = ++generation.current;
      setPhase('starting');
      setStartError(null);
      setStartNotice(notice);
      setCompleteError(null);
      setOutcome(null);
      try {
        const requestStart = (flow?: OAuthStartFlow) => {
          const providerBody = provider === 'anthropic' ? {} : { provider };
          const flowBody = flow ? { flow } : {};
          if (upstreamId === null && provider === 'anthropic' && !flow) {
            return startDraftAsync();
          }
          return upstreamId === null
            ? startDraftAsync({ ...providerBody, ...flowBody })
            : startUpstreamAsync({
                id: upstreamId,
                ...providerBody,
                ...flowBody,
              });
        };
        let res: OAuthStartResponse;
        try {
          res = await requestStart(requestedFlow);
        } catch (error) {
          if (
            provider !== 'openai' ||
            requestedFlow === 'browser' ||
            !(error instanceof ApiError) ||
            error.status !== 404 ||
            error.code !== 'device_login_unavailable'
          ) {
            throw error;
          }
          setStartNotice(
            'Device sign-in is turned off by this workspace, so browser sign-in is ready instead.',
          );
          res = await requestStart('browser');
        }
        if (mine !== generation.current) return;
        const expiresAtMs = res.expires_at_unix_secs
          ? res.expires_at_unix_secs * 1000
          : Date.now() + OAUTH_SESSION_TTL_MS;
        const pollIntervalMs = Math.max(1, res.poll_interval_secs ?? 5) * 1000;
        pollIntervalMsRef.current = pollIntervalMs;
        setSession({
          authorizeUrl: res.authorize_url,
          verificationUrl: res.verification_url ?? null,
          userCode: res.user_code ?? null,
          stateToken: res.state_token,
          oauthState: res.authorize_url
            ? stateOfAuthorizeUrl(res.authorize_url)
            : null,
          flow: res.flow,
          pollIntervalMs,
          expiresAtMs,
        });
        setPhase('ready');
      } catch (error) {
        if (mine !== generation.current) return;
        setStartError(errorMessage(error));
        setPhase('start_failed');
      }
    },
    [provider, upstreamId, startDraftAsync, startUpstreamAsync],
  );

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
    async (code?: string): Promise<ConnectOutcome | null> => {
      if (!session || phase !== 'ready' || completing) return null;
      const completionGeneration = generation.current;
      setCompleting(true);
      setCompleteError(null);
      try {
        const response =
          upstreamId === null
            ? await completeDraftAsync({
                state_token: session.stateToken,
                code,
              })
            : await completeUpstreamAsync({
                id: upstreamId,
                state_token: session.stateToken,
                code,
              });
        if (completionGeneration !== generation.current) return null;
        if (isPending(response)) {
          const pollIntervalMs =
            Math.max(
              1,
              response.poll_interval_secs ?? session.pollIntervalMs / 1000,
            ) * 1000;
          pollIntervalMsRef.current = pollIntervalMs;
          setSession((current) =>
            current && current.pollIntervalMs !== pollIntervalMs
              ? { ...current, pollIntervalMs }
              : current,
          );
          setPolling(true);
          return null;
        }
        const result = response as DraftCompleteResponse;
        let account = upstreamId === null ? identityOfResult(result) : null;
        if (upstreamId !== null) {
          try {
            const meta =
              provider === 'anthropic'
                ? await triggerSubscriptionMetadataRefresh(upstreamId)
                : await getJson<SubscriptionMetadataResponse>(
                    `/admin/v1/upstreams/${upstreamId}/subscription-metadata`,
                  );
            qc.setQueryData(qk.upstreamSubscriptionMetadata(upstreamId), meta);
            account = identityOfResult(meta);
          } catch {
            account = null;
          }
        }
        const next: ConnectOutcome = {
          provider,
          mode: result.mode,
          longLivedFallback: result.long_lived_fallback,
          fallbackReason: result.fallback_reason,
          grantedExpiresInSecs: result.granted_expires_in_secs,
          account,
          draft: upstreamId === null ? result : null,
          verdict:
            upstreamId === null
              ? 'first_connect'
              : compareIdentity(previousAccount, account),
          previousAccount: upstreamId === null ? null : previousAccount,
        };
        setPolling(false);
        setOutcome(next);
        return next;
      } catch (error) {
        if (completionGeneration !== generation.current) return null;
        const message = oauthCompleteErrorMessage(error, provider);
        setCompleteError(message);
        setPolling(false);
        if (/expired|already used|restart|denied/i.test(message))
          setPhase('expired');
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
      provider,
    ],
  );

  completeRef.current = complete;
  phaseRef.current = phase;
  outcomeRef.current = outcome;

  useEffect(() => {
    if (phase !== 'ready' || session?.flow !== 'device_code' || outcome) return;
    let cancelled = false;
    let timeoutId: number | undefined;
    const poll = async () => {
      if (cancelled) return;
      const result = await completeRef.current?.();
      if (
        cancelled ||
        result ||
        phaseRef.current !== 'ready' ||
        outcomeRef.current
      )
        return;
      timeoutId = window.setTimeout(
        () => void poll(),
        pollIntervalMsRef.current,
      );
    };
    void poll();
    return () => {
      cancelled = true;
      if (timeoutId !== undefined) window.clearTimeout(timeoutId);
    };
  }, [phase, session?.flow, outcome]);

  const remainingMs = session ? Math.max(0, session.expiresAtMs - nowMs) : 0;
  return {
    provider,
    flow: session?.flow ?? null,
    phase,
    authorizeUrl: session?.authorizeUrl ?? null,
    verificationUrl: session?.verificationUrl ?? null,
    userCode: session?.userCode ?? null,
    pollIntervalSecs: session ? session.pollIntervalMs / 1000 : 0,
    oauthState: session?.oauthState ?? null,
    remainingMs,
    startError,
    startNotice,
    restart: () => start(),
    startBrowser: () => start('browser'),
    complete,
    completing,
    polling,
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
