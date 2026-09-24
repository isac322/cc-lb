// Header-level limit-reset ("coupon") action for the upstream detail view.
// Renders a small secondary button next to Delete plus a direct confirmation
// dialog; it owns its query, claim mutation and unknown-outcome marker so the
// surrounding page stays untouched.

import { ExternalLink } from 'lucide-react';
import { type ReactNode, useEffect, useRef, useState } from 'react';
import { toast } from 'sonner';
import { ApiError } from '../../lib/api';
import { formatWait, type NudgeQuotaWindow } from '../../lib/couponNudge';
import {
  claimErrorReason,
  clearPendingLimitResetOp,
  isUnknownClaimError,
  type LimitResetGrant,
  loadPendingLimitResetOp,
  newLimitResetRequestId,
  type PendingLimitResetOp,
  savePendingLimitResetOp,
  useClaimLimitReset,
} from '../../lib/limitResets';
import type { Upstream } from '../../lib/queries';
import { Button, INPUT_CLASS, Modal, Notice } from '../ui/primitives';
import { RelativeTime, ResetCountdown } from '../ui/RelativeTime';
import { CouponActionButton, useCouponNudge } from './CouponNudge';

const SURFACE_URL = 'https://claude.ai/settings/usage';

// Coupon copy uses spelled-out window names; the compressed '5h'/'7d' labels
// belong to the existing quota UI, which stays the sole source of usage data.
const COUPON_WINDOW_LABELS: Record<string, string> = {
  '5h': '5-hour',
  '7d': 'Weekly',
  '7d_sonnet': 'Weekly (Sonnet)',
  '7d_opus': 'Weekly (Opus)',
  '7d_fable': 'Weekly (Fable)',
  '7d_overage_included': 'Weekly (overage included)',
  '7d_cowork': 'Weekly (Cowork)',
  '7d_omelette': 'Weekly (Omelette)',
  '7d_oauth_apps': 'Weekly (OAuth apps)',
  overage: 'Extra Usage',
  unified: 'Unified',
};

function windowLabel(name: string): string {
  return COUPON_WINDOW_LABELS[name] ?? name.replaceAll('_', ' ');
}

function windowList(names: string[]): string {
  return names.map(windowLabel).join(', ');
}

interface GrantBlock {
  /** Plain-text explanation, reused as the disabled-button popover. */
  label: string;
  /** True when the fix lives on claude.ai rather than in this app. */
  surface: boolean;
  /** True for the cooldown block, which the dialog shows as a countdown. */
  cooldown?: boolean;
}

function grantBlock(grant: LimitResetGrant, nowMs: number): GrantBlock | null {
  // usable_now can be stale relative to ends_at — a date-expired (or
  // unparseable) grant is never claimable regardless of the snapshot.
  if (grant.ends_at != null) {
    const endsMs = Date.parse(grant.ends_at);
    if (Number.isNaN(endsMs) || endsMs <= nowMs)
      return {
        label: 'This coupon expired and can no longer be used',
        surface: false,
      };
  }
  if (grant.paused)
    return { label: 'The provider paused this coupon', surface: false };
  for (const reason of grant.blocking) {
    switch (reason) {
      case 'expired':
        return {
          label: 'This coupon expired and can no longer be used',
          surface: false,
        };
      case 'cooldown':
        return {
          label: 'Resets are on cooldown',
          surface: false,
          cooldown: true,
        };
      case 'surface_restricted':
        return {
          label: 'This coupon can only be used on claude.ai',
          surface: true,
        };
      case 'requires_limit':
        return {
          label:
            'This coupon becomes usable once the account reaches its limit',
          surface: false,
        };
      default:
        return {
          label: 'This coupon is not usable right now',
          surface: false,
        };
    }
  }
  return grant.usable_now
    ? null
    : { label: 'This coupon is not usable right now', surface: false };
}

function blockingReason(
  grant: LimitResetGrant,
  cooldownUntil: string | null,
  nowMs: number,
): { text: ReactNode; surface: boolean } | null {
  const block = grantBlock(grant, nowMs);
  if (block == null) return null;
  // The dialog shows the live countdown for a cooldown; the shared label
  // stays plain text so the disabled-button popover can reuse it.
  if (block.cooldown && cooldownUntil) {
    return {
      text: (
        <>
          Cooldown ends <ResetCountdown ts={new Date(cooldownUntil)} />
        </>
      ),
      surface: block.surface,
    };
  }
  return { text: block.label, surface: block.surface };
}

export function LimitResetAction({
  upstream,
  quotaWindows = [],
}: {
  upstream: Upstream;
  /** Quota snapshots the page already renders — the nudge reads the same
   *  numbers, never a second source. */
  quotaWindows?: readonly NudgeQuotaWindow[];
}) {
  const isOauth = upstream.kind === 'anthropic_oauth';
  const { resetsQ, nowMs, suppressed, nudge, derived, claimInFlight } =
    useCouponNudge(isOauth ? upstream.id : null, quotaWindows);
  const claim = useClaimLimitReset();

  const [dialogOpen, setDialogOpen] = useState(false);
  const [selectedGrantId, setSelectedGrantId] = useState<string | null>(null);
  // One request_id per confirmation, generated when the dialog opens.
  const [requestId, setRequestId] = useState(() => newLimitResetRequestId());
  const [pendingOp, setPendingOp] = useState<PendingLimitResetOp | null>(() =>
    loadPendingLimitResetOp(upstream.id),
  );
  // Frozen confirmation view captured when a fresh claim is dispatched: the
  // modal keeps showing exactly what the user confirmed while the request is
  // in flight instead of swapping to the pending-op surface mid-request.
  const [inFlightView, setInFlightView] = useState<{
    grant: LimitResetGrant;
    atLimit: boolean;
  } | null>(null);
  // Two-step dismissal for an unresolved marker: the first click arms it and
  // shows the warning, the second clears the record.
  const [dismissArmed, setDismissArmed] = useState(false);
  // Synchronous latch: the shared claimInFlight flag only reflects on the
  // next render, so a ref guards against two dispatches in the same tick.
  const dispatchLatch = useRef(false);

  const data = resetsQ.data;
  const ember = data?.cedar_ember ?? null;

  // A persisted marker belongs to the account that started it. Counts can
  // never prove this operation's outcome, so the marker clears itself only
  // when the account or organization changes — a stale marker from a
  // previous account must never follow the new one. Everything else waits
  // for the user to refresh the status or dismiss the record.
  useEffect(() => {
    if (!pendingOp || !data?.account_id || !data.organization_id) return;
    if (
      pendingOp.account_id !== data.account_id ||
      pendingOp.organization_id !== data.organization_id
    ) {
      clearPendingLimitResetOp(upstream.id);
      setPendingOp(null);
    }
  }, [pendingOp, data, upstream.id]);

  useEffect(() => {
    if (!pendingOp) setDismissArmed(false);
  }, [pendingOp]);

  // The action stays visible in every state so the user can always inspect
  // why a reset is or isn't available — errors and ineligibility are
  // explained in the dialog, never hidden and never shown as "0". A
  // confirmed empty/unusable state disables the button instead (the
  // disabledReason popover explains why), while a read error or an
  // unresolved pending op keeps it clickable for inspection.
  const queryError = resetsQ.isError;
  const loading = resetsQ.isPending && !data;
  const eligible = ember?.eligible === true;

  const grants = ember?.grants ?? [];
  // Expired/depleted/not-yet-started grants never appear as a usable count or
  // a selectable option — activeGrants is the single filtered list.
  const selectableGrants = derived?.activeGrants ?? [];
  const hiddenGrantCount = grants.length - selectableGrants.length;
  const recommended = grants.find((g) => g.id === ember?.next_grant_id);
  // Account-level cooldown blocks every grant, matching the nudge's usable
  // definition (an unparseable cooldown fails closed).
  const cooldownMs =
    ember?.cooldown_until != null ? Date.parse(ember.cooldown_until) : null;
  const cooldownActive =
    cooldownMs != null && (Number.isNaN(cooldownMs) || cooldownMs > nowMs);
  const grantReady = (
    g: LimitResetGrant | undefined | null,
  ): g is LimitResetGrant =>
    g?.usable_now === true &&
    !g.paused &&
    g.blocking.length === 0 &&
    !cooldownActive &&
    selectableGrants.includes(g);
  const readyGrants = selectableGrants.filter(grantReady);
  const earliestEnding = (list: readonly LimitResetGrant[]) => {
    let earliest: LimitResetGrant | undefined;
    let earliestMs = Infinity;
    for (const grant of list) {
      const endsMs =
        grant.ends_at == null ? Infinity : Date.parse(grant.ends_at);
      if (earliest == null || endsMs < earliestMs) {
        earliest = grant;
        earliestMs = endsMs;
      }
    }
    return earliest;
  };
  // Prefer the soonest-expiring usable coupon; undated coupons come last.
  // If none are usable, retain an active grant to explain its blocking reason.
  const defaultGrant =
    earliestEnding(readyGrants) ??
    (recommended != null && selectableGrants.includes(recommended)
      ? recommended
      : (selectableGrants.at(0) ?? null));
  const selected =
    selectableGrants.find((g) => g.id === selectedGrantId) ?? defaultGrant;
  const selectedBlocking = selected
    ? blockingReason(selected, ember?.cooldown_until ?? null, nowMs)
    : null;
  const selectedUsable =
    eligible && selected != null && grantReady(selected) && !selectedBlocking;

  // Usable coupons exist only on authoritative successful data: the account
  // is eligible and at least one active grant is consumable right now.
  const usableCoupons = eligible && readyGrants.length > 0;
  // Disabled while a claim is in flight, during the initial load (unless a
  // pending op still needs its resolution surface), or once data proves
  // there is no usable coupon. Read errors and pending ops stay enabled —
  // the dialog is where the user inspects the state.
  const actionDisabled =
    claimInFlight ||
    (pendingOp == null &&
      (loading || (!queryError && data != null && !usableCoupons)));

  const openDialog = () => {
    if (actionDisabled) return;
    setSelectedGrantId(null);
    setRequestId(newLimitResetRequestId());
    setDismissArmed(false);
    setDialogOpen(true);
  };

  if (!isOauth) return null;

  const runClaim = (
    body: { grant_id: string; request_id: string },
    // Fresh claims freeze the confirmation the user just approved so the
    // dialog body cannot shift while the request is in flight.
    freeze: { grant: LimitResetGrant; atLimit: boolean },
  ) => {
    // The account/org pair submitted with the claim must come from the same
    // payload the user reviewed; the server revalidates it against the live
    // profile and rejects a stale identity with 409.
    if (!data?.account_id || !data.organization_id) return;
    // Latch: a claim already in flight must never dispatch a second request.
    // The shared flag covers remounts; the ref covers same-tick repeats.
    if (claimInFlight || dispatchLatch.current) return;
    dispatchLatch.current = true;
    setInFlightView(freeze);
    // Persist BEFORE dispatch: a reload while the request is in flight loses
    // the response, so the marker must already exist to keep the evidence.
    const op: PendingLimitResetOp = {
      upstream_id: upstream.id,
      account_id: data.account_id,
      organization_id: data.organization_id,
      grant_id: body.grant_id,
      request_id: body.request_id,
      started_at_unix_secs: Math.floor(Date.now() / 1000),
    };
    savePendingLimitResetOp(op);
    setPendingOp(op);
    claim.mutate(
      {
        upstreamId: upstream.id,
        body: {
          account_id: data.account_id,
          organization_id: data.organization_id,
          grant_id: body.grant_id,
          request_id: body.request_id,
        },
      },
      {
        onSuccess: (res) => {
          switch (res.result) {
            case 'reset':
              clearPendingLimitResetOp(upstream.id);
              setPendingOp(null);
              setDialogOpen(false);
              toast.success(
                res.cleared.length
                  ? `Usage reset for ${windowList(res.cleared)}`
                  : 'Quota reset applied',
              );
              break;
            case 'already_used':
              clearPendingLimitResetOp(upstream.id);
              setPendingOp(null);
              setDialogOpen(false);
              toast(
                'This reset was already applied. No additional coupon was used.',
              );
              break;
            case 'unknown':
              toast.error(
                'Reset result not confirmed. The request is saved; reopen this dialog to review or dismiss it.',
              );
              break;
            default:
              clearPendingLimitResetOp(upstream.id);
              setPendingOp(null);
              setDialogOpen(false);
              toast.error(
                `Reset not applied: ${res.reason ?? 'the server declined it'}`,
              );
          }
        },
        onError: (error) => {
          if (isUnknownClaimError(error)) {
            toast.error(
              'Could not confirm the reset. The request is saved; reopen this dialog to review or dismiss it.',
            );
            return;
          }
          // Definite outcome: the server rejected the request without
          // consuming anything, so the marker is resolved.
          clearPendingLimitResetOp(upstream.id);
          setPendingOp(null);
          setDialogOpen(false);
          if (error instanceof ApiError && error.status === 409) {
            toast.error(
              claimErrorReason(error) === 'stale_identity'
                ? 'Reset not applied: the account changed. Review the refreshed status.'
                : `Reset not applied: ${
                    claimErrorReason(error) ?? 'the coupon state changed'
                  }`,
            );
            return;
          }
          toast.error(
            error instanceof Error ? error.message : 'Reset request failed',
          );
        },
        onSettled: () => {
          dispatchLatch.current = false;
          setInFlightView(null);
        },
      },
    );
  };

  const dismissPendingOp = () => {
    clearPendingLimitResetOp(upstream.id);
    setPendingOp(null);
    setDismissArmed(false);
    setDialogOpen(false);
  };

  // The modal has exactly two surfaces. The pending-op surface appears only
  // once a claim's outcome is actually unknown — a marker persisted while a
  // fresh claim is still in flight (inFlightView set) must not swap the
  // confirmation the user is looking at.
  const pendingView = pendingOp != null && inFlightView == null;
  // While a fresh claim is in flight the dialog renders the frozen grant the
  // user confirmed; otherwise the live selection.
  const displayGrant = inFlightView?.grant ?? selected;
  const displayBlocking = inFlightView ? null : selectedBlocking;

  // Popover copy for the disabled button: the direct reason the action is
  // unavailable. Only set when data actually proves the state is unusable.
  const disabledReason =
    !claimInFlight &&
    pendingOp == null &&
    !loading &&
    !queryError &&
    data != null &&
    !usableCoupons
      ? ember == null
        ? 'No current reset coupon data. Status updates with quota polling.'
        : (() => {
            const candidate = defaultGrant ?? grants.at(0);
            const block = candidate ? grantBlock(candidate, nowMs) : null;
            return block
              ? `${block.label}.`
              : 'No reset coupons available right now.';
          })()
      : undefined;

  const showNudge = !suppressed && nudge != null && nudge.kind !== 'quiet';
  const buttonKind = showNudge ? nudge.kind : 'quiet';
  const buttonTitle = showNudge
    ? (nudge.detail ?? nudge.label ?? undefined)
    : undefined;
  const buttonCount =
    (!pendingOp || inFlightView != null) &&
    !loading &&
    !queryError &&
    eligible &&
    derived != null &&
    derived.activeCount > 0
      ? derived.activeCount
      : null;
  // Constant label while a claim is in flight — the button's own loading
  // spinner signals progress, so the ticket outline never resizes.
  const buttonLabel = pendingView
    ? 'Reset not confirmed'
    : loading
      ? 'Reset quota · …'
      : queryError || !eligible
        ? 'Reset unavailable'
        : 'Reset quota';
  // Expiry rides beside the button whenever a usable grant ends inside the
  // notice window — including while a limit recommendation owns the tone.
  const expiryLabel =
    nudge?.expiresSoonAt != null
      ? `Expires ${formatWait(nudge.expiresSoonAt - nowMs)}`
      : undefined;

  return (
    <>
      <CouponActionButton
        kind={buttonKind}
        count={buttonCount}
        disabled={actionDisabled}
        disabledReason={disabledReason}
        loading={claimInFlight}
        expiryLabel={expiryLabel}
        title={buttonTitle}
        onClick={openDialog}
      >
        {buttonLabel}
      </CouponActionButton>

      <Modal
        open={dialogOpen}
        onOpenChange={(open) => {
          if (!open && !claimInFlight) setDialogOpen(false);
        }}
        title={
          pendingView ? 'Reset not confirmed' : 'Reset subscription quota?'
        }
        description={upstream.name}
        preventDismiss={claimInFlight}
        footer={
          <>
            <Button
              variant="ghost"
              autoFocus
              disabled={claimInFlight}
              onClick={() => setDialogOpen(false)}
            >
              Cancel
            </Button>
            {pendingView ? (
              <>
                <Button
                  variant={dismissArmed ? 'danger' : 'ghost'}
                  disabled={claimInFlight}
                  onClick={() =>
                    dismissArmed ? dismissPendingOp() : setDismissArmed(true)
                  }
                >
                  {dismissArmed ? 'Dismiss anyway' : 'Dismiss record'}
                </Button>
                <Button
                  variant="secondary"
                  loading={resetsQ.isFetching}
                  disabled={claimInFlight}
                  onClick={() => resetsQ.refetch()}
                >
                  Refresh status
                </Button>
              </>
            ) : (
              <Button
                variant="secondary"
                loading={claimInFlight}
                disabled={!selectedUsable || !data || queryError}
                onClick={() => {
                  if (!selected || !data || !selectedUsable || queryError)
                    return;
                  runClaim(
                    {
                      grant_id: selected.id,
                      request_id: requestId,
                    },
                    { grant: selected, atLimit: ember?.at_limit === true },
                  );
                }}
              >
                Reset quota
              </Button>
            )}
          </>
        }
      >
        <div className="flex flex-col gap-3 text-sm text-text-muted">
          {pendingView && pendingOp ? (
            <>
              <Notice tone="warning" title="Reset result not confirmed">
                We could not confirm whether this request used a coupon.
                Refreshing the status shows the current coupons and counts, but
                the provider cannot confirm this individual request — a changed
                count is not proof it was consumed. Requested{' '}
                <RelativeTime
                  ts={pendingOp.started_at_unix_secs * 1000}
                  compact
                />
                .
                <details className="mt-1">
                  <summary className="cursor-pointer text-text-faint">
                    Request details
                  </summary>
                  <code className="mt-0.5 block break-all font-mono text-[10px]">
                    {pendingOp.request_id}
                  </code>
                </details>
              </Notice>
              {dismissArmed ? (
                <Notice tone="danger" title="Dismiss this record?">
                  The request stays unconfirmed. If it did consume a coupon,
                  that use is not shown here. This only clears the local record.
                </Notice>
              ) : null}
            </>
          ) : null}
          {queryError ? (
            <Notice
              tone="danger"
              title="Could not load reset status"
              action={
                <Button
                  size="sm"
                  variant="secondary"
                  loading={resetsQ.isFetching}
                  onClick={() => resetsQ.refetch()}
                >
                  Retry
                </Button>
              }
            >
              {resetsQ.error instanceof ApiError && resetsQ.error.status === 404
                ? 'This server does not support limit resets.'
                : resetsQ.error instanceof Error
                  ? resetsQ.error.message
                  : 'The reset status request failed.'}
            </Notice>
          ) : null}

          {data && !eligible && ember != null ? (
            <p className="text-xs text-text-faint">
              {ember.ineligible_reason === 'api_key_upstream'
                ? 'Limit resets are only available for OAuth subscription upstreams.'
                : 'This account is not eligible for limit resets.'}{' '}
              <a
                href={SURFACE_URL}
                target="_blank"
                rel="noreferrer"
                className="inline-flex items-center gap-0.5 underline decoration-text-faint underline-offset-2 hover:text-text"
              >
                Manage on claude.ai
                <ExternalLink className="h-3 w-3" />
              </a>
            </p>
          ) : null}

          {data && ember == null ? (
            <p className="text-xs text-text-faint">
              No current reset coupon data. Status updates with quota polling.
            </p>
          ) : null}

          {loading ? (
            <p className="text-xs text-text-faint">
              Checking reset availability…
            </p>
          ) : null}

          {pendingView || !ember ? null : ember.grants.length === 0 ? (
            <p className="text-xs text-text-faint">
              No reset coupons available.
              {ember.weekly_resets_at ? (
                <>
                  {' '}
                  Weekly usage resets{' '}
                  <RelativeTime ts={new Date(ember.weekly_resets_at)} />.
                </>
              ) : null}
            </p>
          ) : selectableGrants.length === 0 ? (
            <p className="text-xs text-text-faint">
              No active reset coupons.
              {ember.weekly_resets_at ? (
                <>
                  {' '}
                  Weekly usage resets{' '}
                  <RelativeTime ts={new Date(ember.weekly_resets_at)} />.
                </>
              ) : null}
            </p>
          ) : (
            <>
              {selectableGrants.length > 1 ? (
                <select
                  aria-label="Reset coupon"
                  className={INPUT_CLASS}
                  value={displayGrant?.id ?? ''}
                  disabled={claimInFlight || pendingView}
                  onChange={(e) => setSelectedGrantId(e.target.value)}
                >
                  {selectableGrants.map((grant) => (
                    <option key={grant.id} value={grant.id}>
                      {grant.label}: {grant.resets_left}
                      {grant.resets_total != null
                        ? `/${grant.resets_total}`
                        : ''}{' '}
                      left
                    </option>
                  ))}
                </select>
              ) : null}
              {hiddenGrantCount > 0 ? (
                <p className="text-[11px] text-text-faint">
                  Only active coupons are shown.
                </p>
              ) : null}
              {displayGrant ? (
                <dl className="grid grid-cols-[auto_1fr] gap-x-4 gap-y-1 text-xs">
                  <dt className="text-text-faint">Coupon</dt>
                  <dd className="min-w-0 break-words text-text">
                    {displayGrant.label} · {displayGrant.resets_left}
                    {displayGrant.resets_total != null
                      ? `/${displayGrant.resets_total}`
                      : ''}{' '}
                    left
                  </dd>
                  <dt className="text-text-faint">Clears</dt>
                  <dd className="min-w-0 break-words text-text">
                    {windowList(displayGrant.clears)}
                  </dd>
                  {displayGrant.ends_at ? (
                    <>
                      <dt className="text-text-faint">Expires</dt>
                      <dd className="min-w-0 break-words text-text">
                        <RelativeTime ts={new Date(displayGrant.ends_at)} />
                      </dd>
                    </>
                  ) : null}
                  {displayBlocking ? (
                    <>
                      <dt className="text-text-faint">Status</dt>
                      <dd className="min-w-0 break-words text-[color:var(--color-warn-text)]">
                        {displayBlocking.text}
                        {displayBlocking.surface ? (
                          <>
                            {' '}
                            <a
                              href={SURFACE_URL}
                              target="_blank"
                              rel="noreferrer"
                              className="inline-flex items-center gap-0.5 underline decoration-text-faint underline-offset-2 hover:text-text"
                            >
                              Open claude.ai
                              <ExternalLink className="h-3 w-3" />
                            </a>
                          </>
                        ) : null}
                      </dd>
                    </>
                  ) : null}
                </dl>
              ) : null}
              {derived != null && derived.usableCount < derived.activeCount ? (
                <p className="text-[11px] text-text-faint">
                  {derived.usableCount === 0
                    ? 'These coupons cannot be used right now. See the status above.'
                    : `${derived.usableCount} of ${derived.activeCount} coupons can be used right now.`}
                </p>
              ) : null}
            </>
          )}

          {ember?.cooldown_until ? (
            <p className="text-xs text-text-faint">
              Resets on cooldown until{' '}
              <ResetCountdown ts={new Date(ember.cooldown_until)} />.
            </p>
          ) : null}

          {displayGrant &&
          (inFlightView != null || (!pendingView && selectedUsable)) ? (
            <>
              <p className="text-xs text-text-faint">
                This uses one reset coupon to reset usage for your{' '}
                {windowList(displayGrant.clears)}{' '}
                {displayGrant.clears.length > 1 ? 'limits' : 'limit'}. It cannot
                be undone, and your weekly reset schedule stays the same.
              </p>
              {!(inFlightView?.atLimit ?? ember?.at_limit) ? (
                <Notice tone="warning" title="Not at the limit yet">
                  You can still use this coupon now, or save it for later.
                </Notice>
              ) : null}
            </>
          ) : null}
          <a
            href={SURFACE_URL}
            target="_blank"
            rel="noreferrer"
            className="inline-flex items-center gap-1 text-[11px] text-text-faint hover:text-text"
          >
            Manage on claude.ai
            <ExternalLink className="h-3 w-3" />
          </a>
        </div>
      </Modal>
    </>
  );
}
