// Typed client for the per-upstream limit-reset ("coupon") endpoints.
// Kept out of api.ts so the coupon surface stays self-contained; it reuses
// fetchWithAuth/getJson so auth, timeouts and ApiError semantics are identical
// to the rest of the admin API.
//
// Anthropic owns all coupon state: this client stores no quota numbers and
// keeps no idempotency ledger. The only local record is the pending-op
// marker below, which preserves evidence of a claim whose outcome never
// reached the client.

import { useMutation, useQueryClient } from '@tanstack/react-query';
import { ApiError, getJson, postJson } from './api';
import { qk } from './queries';
import { usePolledData } from './usePolledData';

export interface LimitResetGrant {
  id: string;
  label: string;
  /** Null when the provider does not report a total; resets_left stays
   *  authoritative. */
  resets_total: number | null;
  resets_left: number;
  /** ISO 8601, null when the provider does not report a start. */
  starts_at: string | null;
  /** ISO 8601, null when the grant does not expire */
  ends_at: string | null;
  /** Subscription quota window names this reset clears, e.g. '5h', '7d'. */
  clears: string[];
  paused: boolean;
  usable_now: boolean;
  /** Server requires the account to be at its limit before a reset applies. */
  use_requires_limit: boolean;
  /** Per-window utilization 0–100 at observation time. */
  percent_used: Record<string, number>;
  /** Machine-readable reasons the grant cannot be used right now. */
  blocking: string[];
}

export interface CedarEmberLimitResets {
  eligible: boolean;
  ineligible_reason: string | null;
  at_limit: boolean;
  grants: LimitResetGrant[];
  /** Server-recommended grant to offer; null when nothing is usable. */
  next_grant_id: string | null;
  /** ISO 8601, null when the provider does not report a weekly reset. */
  weekly_resets_at: string | null;
  /** ISO 8601 */
  cooldown_until: string | null;
}

export interface LimitResetsResponse {
  account_id: string | null;
  organization_id: string | null;
  /** Null when no fresh, identity-bound coupon observation is available. */
  cedar_ember: CedarEmberLimitResets | null;
}

export type LimitResetClaimResult =
  | 'reset'
  | 'already_used'
  | 'not_limited'
  | 'cooldown'
  | 'ineligible'
  | 'unavailable'
  | 'unknown';

export interface LimitResetClaimRequest {
  account_id: string;
  organization_id: string;
  grant_id: string;
  /** Provider idempotency key for this claim attempt. */
  request_id: string;
}

export interface LimitResetClaimResponse {
  result: LimitResetClaimResult;
  reason: string | null;
  cleared: string[];
  resets_left: number | null;
  weekly_resets_at: string | null;
  cooldown_until: string | null;
}

export const limitResetKeys = {
  all: ['limit-resets'] as const,
  detail: (upstreamId: string) => ['limit-resets', upstreamId] as const,
};

/** Read the snapshot populated by the shared quota poll; this GET does not
 *  contact the provider. Match the background observation cadence. */
const LIMIT_RESET_POLL_MS = 60_000;

/** Mutation key so coupon surfaces can lock while a claim is in flight
 *  (useIsMutating). */
export const LIMIT_RESET_CLAIM_MUTATION_KEY = ['limitResetClaim'] as const;

export function useLimitResets(upstreamId: string | null) {
  return usePolledData(
    {
      queryKey: limitResetKeys.detail(upstreamId ?? ''),
      queryFn: ({ signal }) =>
        getJson<LimitResetsResponse>(
          `/admin/v1/upstreams/${upstreamId}/limit-resets`,
          { signal },
        ),
      enabled: Boolean(upstreamId),
      // A 404 means this backend does not serve limit resets at all; retrying
      // cannot change that, so surface "unsupported" immediately.
      retry: (failureCount, error) =>
        !(error instanceof ApiError && error.status === 404) &&
        failureCount < 3,
    },
    LIMIT_RESET_POLL_MS,
  );
}

export function useClaimLimitReset() {
  const qc = useQueryClient();
  return useMutation({
    mutationKey: LIMIT_RESET_CLAIM_MUTATION_KEY,
    mutationFn: ({
      upstreamId,
      body,
    }: {
      upstreamId: string;
      body: LimitResetClaimRequest;
    }) =>
      postJson<LimitResetClaimResponse, LimitResetClaimRequest>(
        `/admin/v1/upstreams/${upstreamId}/limit-resets/claim`,
        body,
      ),
    // Whatever the outcome, the quota picture may have moved: refetch the real
    // latest/series/analysis data (never optimistic zeros) plus the sidebar
    // sources so a consumed reset is visible everywhere.
    onSettled: (_data, _error, vars) => {
      qc.invalidateQueries({
        queryKey: limitResetKeys.detail(vars.upstreamId),
      });
      qc.invalidateQueries({ queryKey: ['subscription-quota'] });
      qc.invalidateQueries({ queryKey: qk.upstreams });
      qc.invalidateQueries({ queryKey: qk.status });
    },
  });
}

/** crypto.randomUUID is unavailable on insecure (non-localhost http) origins,
 *  so fall back to a getRandomValues-built v4 UUID for LAN deployments. */
export function newLimitResetRequestId(): string {
  const c = globalThis.crypto;
  if (typeof c?.randomUUID === 'function') return c.randomUUID();
  if (typeof c?.getRandomValues === 'function') {
    const bytes = c.getRandomValues(new Uint8Array(16));
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    const hex = Array.from(bytes, (b) => b.toString(16).padStart(2, '0'));
    return `${hex.slice(0, 4).join('')}-${hex.slice(4, 6).join('')}-${hex
      .slice(6, 8)
      .join('')}-${hex.slice(8, 10).join('')}-${hex.slice(10).join('')}`;
  }
  return `req-${Date.now().toString(36)}-${Math.random()
    .toString(36)
    .slice(2, 10)}`;
}

/** A claim whose outcome never reached the client (HTTP 5xx, network drop,
 *  request timeout) or was reported as unknown. Persisted BEFORE dispatch so
 *  a reload mid-flight still shows the request evidence. The marker is only
 *  evidence: the provider does not guarantee that replaying request_id is
 *  free, so the UI offers a status refresh and an explicit dismissal, never
 *  an automatic resubmit. */
export interface PendingLimitResetOp {
  upstream_id: string;
  account_id: string;
  organization_id: string;
  grant_id: string;
  request_id: string;
  started_at_unix_secs: number;
}

const PENDING_OP_PREFIX = 'cclb:limit-reset-pending:';

export function loadPendingLimitResetOp(
  upstreamId: string,
): PendingLimitResetOp | null {
  try {
    const raw = localStorage.getItem(`${PENDING_OP_PREFIX}${upstreamId}`);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as Partial<PendingLimitResetOp>;
    if (
      typeof parsed.upstream_id !== 'string' ||
      typeof parsed.account_id !== 'string' ||
      typeof parsed.organization_id !== 'string' ||
      typeof parsed.grant_id !== 'string' ||
      typeof parsed.request_id !== 'string'
    ) {
      return null;
    }
    return parsed as PendingLimitResetOp;
  } catch {
    return null;
  }
}

export function savePendingLimitResetOp(op: PendingLimitResetOp): void {
  try {
    localStorage.setItem(
      `${PENDING_OP_PREFIX}${op.upstream_id}`,
      JSON.stringify(op),
    );
  } catch {
    // Storage unavailable (private mode, quota): the in-memory state still
    // drives the UI for this session.
  }
}

export function clearPendingLimitResetOp(upstreamId: string): void {
  try {
    localStorage.removeItem(`${PENDING_OP_PREFIX}${upstreamId}`);
  } catch {
    // ignore
  }
}

/** True when the claim may have been applied but the outcome is unknown:
 *  any 5xx (a sent request may have consumed the reset), or any non-HTTP
 *  failure — network drop, abort/timeout, or an unreadable response body —
 *  which by definition happened after dispatch. 4xx responses are definite
 *  rejections that consumed nothing. */
export function isUnknownClaimError(error: unknown): boolean {
  if (error instanceof ApiError) return error.status >= 500;
  return true;
}

/** Extract a machine or human reason from an error body shaped like the
 *  claim contract ({reason} or {error}). */
export function claimErrorReason(error: unknown): string | null {
  if (!(error instanceof ApiError)) return null;
  const body = error.body;
  if (body && typeof body === 'object') {
    const reason = (body as { reason?: unknown }).reason;
    if (typeof reason === 'string' && reason) return reason;
    const code = (body as { error?: unknown }).error;
    if (typeof code === 'string' && code) return code;
  }
  return null;
}
