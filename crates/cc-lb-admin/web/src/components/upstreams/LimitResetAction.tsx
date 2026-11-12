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
  type CouponPresentation,
  chooseCouponGrant,
  couponClaimMessage,
  couponEffectDescription,
  couponInventory,
  couponOptionLabel,
  couponProductLabel,
  couponScopeLabel,
  couponUsageLabel,
  openAIResetLabel,
  presentCoupon,
} from '../../lib/couponPresentation';
import {
  claimErrorReason,
  clearPendingLimitResetOp,
  isUnknownClaimError,
  type LimitResetGrant,
  loadPendingLimitResetOp,
  newUuid,
  type PendingLimitResetOp,
  savePendingLimitResetOp,
  useClaimLimitReset,
} from '../../lib/limitResets';
import type { Upstream } from '../../lib/queries';
import { Button, Modal, Notice } from '../ui/primitives';
import { RelativeTime, ResetCountdown } from '../ui/RelativeTime';
import { Select } from '../ui/Select';
import { CouponActionButton, useCouponNudge } from './CouponNudge';

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
      case 'unsupported_plan':
        return {
          label: 'This coupon is not supported by the current plan',
          surface: false,
        };
      case 'plan_support_unreported':
        return {
          label: 'Plan support not reported',
          surface: false,
        };
      default:
        return {
          label:
            grant.unavailable_reason ?? 'This coupon is not usable right now',
          surface: false,
        };
    }
  }
  return grant.usable_now
    ? null
    : {
        label:
          grant.unavailable_reason ?? 'This coupon is not usable right now',
        surface: false,
      };
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
  credentialsStored,
  quotaWindows = [],
  renderTrigger,
  defaultOpen = false,
}: {
  upstream: Upstream;
  /** Whether OAuth credentials are stored (`has_credentials`); undefined
   *  while the OAuth status loads. Resets are claimed with the stored
   *  credential, so without one there is nothing to check or claim. */
  credentialsStored: boolean | undefined;
  /** Quota snapshots the page already renders — the nudge reads the same
   *  numbers, never a second source. */
  quotaWindows?: readonly NudgeQuotaWindow[];
  /** Compact entry points reuse this same action and confirmation flow. */
  renderTrigger?: (props: {
    onClick: () => void;
    disabled: boolean;
    loading: boolean;
  }) => ReactNode;
  defaultOpen?: boolean;
}) {
  const isOpenAI = upstream.kind === 'openai_oauth';
  const isOauth = upstream.kind === 'anthropic_oauth' || isOpenAI;
  const surfaceUrl = isOpenAI
    ? 'https://chatgpt.com/codex/settings/usage'
    : 'https://claude.ai/settings/usage';
  const providerSurface = isOpenAI ? 'ChatGPT usage' : 'claude.ai';
  const notConnected = isOauth && credentialsStored === false;
  const { resetsQ, nowMs, suppressed, nudge, derived, claimInFlight } =
    useCouponNudge(
      isOauth && credentialsStored === true ? upstream.id : null,
      quotaWindows,
    );
  const claim = useClaimLimitReset();

  const [dialogOpen, setDialogOpen] = useState(defaultOpen);
  const [selectedGrantId, setSelectedGrantId] = useState<string | null>(null);
  // One request_id per confirmation, generated when the dialog opens.
  const [requestId, setRequestId] = useState(() => newUuid());
  const [pendingOp, setPendingOp] = useState<PendingLimitResetOp | null>(() =>
    loadPendingLimitResetOp(upstream.id),
  );
  // Frozen confirmation view captured when a fresh claim is dispatched: the
  // modal keeps showing exactly what the user confirmed while the request is
  // in flight instead of swapping to the pending-op surface mid-request.
  const [inFlightView, setInFlightView] = useState<{
    grant: LimitResetGrant;
    presentation: CouponPresentation;
    grants: LimitResetGrant[];
    activeCount: number;
    usableCount: number;
    hiddenGrantCount: number;
    cooldownUntil: string | null;
  } | null>(null);
  // Two-step dismissal for an unresolved marker: the first click arms it and
  // shows the warning, the second clears the record.
  const [dismissArmed, setDismissArmed] = useState(false);
  // Synchronous latch: the shared claimInFlight flag only reflects on the
  // next render, so a ref guards against two dispatches in the same tick.
  const dispatchLatch = useRef(false);

  const data = resetsQ.data;
  const ember = couponInventory(data);

  // A persisted marker belongs to the account that started it. Counts can
  // never prove this operation's outcome, so the marker clears itself only
  // when the account or organization changes — a stale marker from a
  // previous account must never follow the new one. Everything else waits
  // for the user to refresh the status or dismiss the record.
  useEffect(() => {
    if (!pendingOp || !data?.account_id) return;
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

  // Keep the action available for inspecting inventory, provider gates and
  // unknown outcomes. Missing or failed observations are never shown as zero.
  const queryError = resetsQ.isError;
  const loading = !notConnected && resetsQ.isPending && !data;
  const eligible = ember?.eligible === true;

  const grants = ember?.grants ?? [];
  // Expired/depleted/not-yet-started grants never appear as a usable count or
  // a selectable option — activeGrants is the single filtered list.
  const selectableGrants = derived?.activeGrants ?? [];
  const hiddenGrantCount = grants.length - selectableGrants.length;
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
  const observationOptions = { providerFresh: !queryError };
  const selected = chooseCouponGrant(
    selectableGrants,
    readyGrants,
    quotaWindows,
    selectedGrantId,
    observationOptions,
  );
  const selectedPresentation = selected
    ? presentCoupon(selected, quotaWindows, observationOptions)
    : null;
  const selectedBlocking = selected
    ? blockingReason(selected, ember?.cooldown_until ?? null, nowMs)
    : null;
  const selectedUsable =
    eligible &&
    Boolean(data?.account_id && (isOpenAI || data.organization_id)) &&
    selected != null &&
    grantReady(selected) &&
    !selectedBlocking;

  // Keep inventory inspectable even when no coupon can currently be used.
  // Only connection, loading and an in-flight claim block opening the dialog.
  const actionDisabled =
    claimInFlight || notConnected || (pendingOp == null && loading);

  const openDialog = () => {
    if (actionDisabled) return;
    setSelectedGrantId(null);
    setRequestId(newUuid());
    setDismissArmed(false);
    setDialogOpen(true);
  };

  if (!isOauth) return null;

  const runClaim = (
    body: { grant_id: string; request_id: string },
    // Fresh claims freeze the confirmation the user just approved so the
    // dialog body cannot shift while the request is in flight.
    freeze: NonNullable<typeof inFlightView>,
  ) => {
    // The account/org pair submitted with the claim must come from the same
    // payload the user reviewed; the server revalidates it against the live
    // profile and rejects a stale identity with 409.
    if (!data?.account_id || (!isOpenAI && !data.organization_id)) return;
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
            case 'already_used':
              clearPendingLimitResetOp(upstream.id);
              setPendingOp(null);
              setDialogOpen(false);
              if (res.usage_refresh === 'failed')
                toast.warning(couponClaimMessage(res));
              else toast.success(couponClaimMessage(res));
              break;
            case 'not_limited':
              clearPendingLimitResetOp(upstream.id);
              setPendingOp(null);
              setDialogOpen(false);
              toast(couponClaimMessage(res));
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
  const displayPresentation =
    inFlightView?.presentation ?? selectedPresentation;
  const displayGrants = inFlightView?.grants ?? selectableGrants;
  const displayActiveCount = inFlightView?.activeCount ?? derived?.activeCount;
  const displayUsableCount = inFlightView?.usableCount ?? derived?.usableCount;
  const displayHiddenCount = inFlightView?.hiddenGrantCount ?? hiddenGrantCount;
  const displayCooldown = inFlightView
    ? inFlightView.cooldownUntil
    : ember?.cooldown_until;

  const disabledReason = notConnected
    ? 'Connect the account first: resets are claimed with its stored credentials.'
    : undefined;

  const showNudge = !suppressed && nudge != null && nudge.kind !== 'quiet';
  const buttonKind = showNudge ? nudge.kind : 'quiet';
  const buttonTitle = showNudge
    ? (nudge.detail ?? nudge.label ?? undefined)
    : undefined;
  const ownedCount =
    inFlightView?.activeCount ??
    (isOpenAI ? data?.openai?.available_count : derived?.activeCount);
  const buttonCount =
    (!pendingOp || inFlightView != null) &&
    !loading &&
    !queryError &&
    eligible &&
    ownedCount != null &&
    ownedCount > 0
      ? ownedCount
      : null;
  // Constant label while a claim is in flight — the button's own loading
  // spinner signals progress, so the ticket outline never resizes. The
  // initial status check uses the same spinner+label instead of a ghost
  // placeholder, so the action stays readable from first paint.
  const buttonLabel = pendingView ? 'Reset not confirmed' : 'Reset quota';
  // Expiry rides beside the button whenever a usable grant ends inside the
  // notice window — including while a limit recommendation owns the tone.
  const expiryLabel =
    nudge?.expiresSoonAt != null
      ? `Expires ${formatWait(nudge.expiresSoonAt - nowMs)}`
      : undefined;

  return (
    <>
      {renderTrigger ? (
        renderTrigger({
          onClick: openDialog,
          disabled: actionDisabled,
          loading: claimInFlight || loading,
        })
      ) : (
        <CouponActionButton
          kind={buttonKind}
          count={buttonCount}
          disabled={actionDisabled}
          disabledReason={disabledReason}
          loading={claimInFlight || loading}
          expiryLabel={expiryLabel}
          title={buttonTitle}
          onClick={openDialog}
        >
          {buttonLabel}
        </CouponActionButton>
      )}

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
              autoFocus
              disabled={claimInFlight}
              onClick={() => setDialogOpen(false)}
            >
              Cancel
            </Button>
            {pendingView ? (
              <>
                <Button
                  variant={dismissArmed ? 'danger' : 'secondary'}
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
                variant="primary"
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
                    {
                      grant: structuredClone(selected),
                      presentation: presentCoupon(
                        selected,
                        quotaWindows,
                        observationOptions,
                      ),
                      grants: structuredClone(selectableGrants),
                      activeCount: ownedCount ?? 0,
                      usableCount: derived?.usableCount ?? 0,
                      hiddenGrantCount,
                      cooldownUntil: ember?.cooldown_until ?? null,
                    },
                  );
                }}
              >
                Reset quota
              </Button>
            )}
          </>
        }
      >
        <div className="flex flex-col gap-3 text-body-sm text-text-muted">
          {isOpenAI && data?.openai && !inFlightView ? (
            <>
              <p className="text-text">
                {data.openai.available_count} banked reset coupons available.{' '}
                {data.openai.applicable_available_count == null
                  ? 'The number applicable to this account is not reported.'
                  : `${data.openai.applicable_available_count} applicable to this account.`}
              </p>
              <details>
                <summary className="cursor-pointer text-text-muted">
                  Coupon inventory and history ({data.openai.credits.length})
                </summary>
                <ul className="mt-2 flex flex-col gap-3">
                  {data.openai.credits.map((credit) => (
                    <li key={credit.id} className="min-w-0">
                      <p className="text-text">
                        {openAIResetLabel(credit.reset_type)}
                      </p>
                      {credit.title ? (
                        <p className="text-text-faint">{credit.title}</p>
                      ) : null}
                      <p>
                        {credit.status} ·{' '}
                        {credit.is_supported_by_plan === false
                          ? 'Not supported by this plan'
                          : credit.is_supported_by_plan === true
                            ? 'Plan supported'
                            : 'Plan support not reported'}
                      </p>
                      {credit.granted_at ? (
                        <p>
                          Granted{' '}
                          <RelativeTime ts={new Date(credit.granted_at)} />
                        </p>
                      ) : null}
                      {credit.expires_at ? (
                        <p>
                          Expires{' '}
                          <RelativeTime ts={new Date(credit.expires_at)} />
                        </p>
                      ) : null}
                      <code className="break-all font-mono text-data text-text-faint">
                        {credit.id}
                      </code>
                    </li>
                  ))}
                </ul>
              </details>
            </>
          ) : null}
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
                  <code className="mt-0.5 block break-all font-mono text-data">
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
          {queryError && !inFlightView ? (
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

          {!inFlightView && data && !eligible && ember != null ? (
            <p className="text-body-sm text-text-muted">
              {ember.ineligible_reason === 'api_key_upstream'
                ? 'Limit resets are only available for OAuth subscription upstreams.'
                : 'This account is not eligible for limit resets.'}{' '}
              <a
                href={surfaceUrl}
                target="_blank"
                rel="noreferrer"
                className="inline-flex items-center gap-0.5 underline decoration-text-faint underline-offset-2 hover:text-text"
              >
                Manage on {providerSurface}
                <ExternalLink className="h-3 w-3" />
              </a>
            </p>
          ) : null}

          {!inFlightView && data && ember == null ? (
            <p className="text-body-sm text-text-muted">
              No current reset coupon data. Status updates with quota polling.
            </p>
          ) : null}

          {loading && !inFlightView ? (
            <p className="text-body-sm text-text-muted">
              Checking reset availability…
            </p>
          ) : null}
          {!pendingView &&
          !inFlightView &&
          !selected &&
          selectedGrantId != null ? (
            <Notice
              tone="warning"
              title="Selected coupon is no longer available"
            >
              Choose another coupon to continue. No reset has been submitted.
            </Notice>
          ) : null}

          {pendingView || (!ember && !inFlightView) ? null : !inFlightView &&
            ember?.grants.length === 0 ? (
            <p className="text-body-sm text-text-muted">
              No reset coupons available.
              {ember?.weekly_resets_at ? (
                <>
                  {' '}
                  Weekly usage resets{' '}
                  <RelativeTime ts={new Date(ember.weekly_resets_at)} />.
                </>
              ) : null}
            </p>
          ) : displayGrants.length === 0 ? (
            <p className="text-body-sm text-text-muted">
              No active reset coupons.
              {ember?.weekly_resets_at ? (
                <>
                  {' '}
                  Weekly usage resets{' '}
                  <RelativeTime ts={new Date(ember.weekly_resets_at)} />.
                </>
              ) : null}
            </p>
          ) : (
            <>
              {displayGrants.length > 1 || selectedGrantId != null ? (
                <Select
                  aria-label="Reset coupon"
                  className="w-full"
                  value={displayGrant?.id ?? ''}
                  disabled={claimInFlight || pendingView}
                  onChange={setSelectedGrantId}
                  placeholder="Choose a coupon"
                  options={displayGrants.map((grant, index) => ({
                    value: grant.id,
                    label: `${couponOptionLabel(grant)} · Coupon ${index + 1}`,
                  }))}
                />
              ) : null}
              {displayHiddenCount > 0 ? (
                <p className="text-caption text-text-faint">
                  Only active coupons are shown.
                </p>
              ) : null}
              {displayGrant ? (
                <dl className="grid grid-cols-[auto_1fr] gap-x-4 gap-y-1.5 text-body-sm">
                  <dt className="text-label text-text-faint">Reset</dt>
                  <dd className="min-w-0 break-words text-text">
                    {couponProductLabel(displayGrant)}
                  </dd>
                  <dt className="text-label text-text-faint">Provider title</dt>
                  <dd className="min-w-0 break-words text-text">
                    {displayGrant.label}
                  </dd>
                  <dt className="text-label text-text-faint">Scope</dt>
                  <dd className="min-w-0 break-words text-text">
                    {displayGrant.openai_credit?.reset_type ===
                    'codex_rate_limits'
                      ? 'Main Codex allowance'
                      : couponScopeLabel(displayGrant)}
                  </dd>
                  <dt className="text-label text-text-faint">
                    Provider details
                  </dt>
                  <dd className="min-w-0">
                    <details>
                      <summary className="cursor-pointer">
                        Exact scope and coupon ID
                      </summary>
                      <p className="break-all font-mono text-data">
                        {displayGrant.openai_credit?.reset_type ??
                          (displayGrant.clears.join(', ') || 'Not reported')}
                      </p>
                      <p className="break-all font-mono text-data">
                        {displayGrant.id}
                      </p>
                    </details>
                  </dd>
                  {displayPresentation?.targets.map((reading) => (
                    <div key={reading.window} className="contents">
                      <dt className="text-label text-text-faint">
                        {reading.label} usage
                      </dt>
                      <dd className="min-w-0 break-words text-text">
                        {couponUsageLabel(reading)}
                      </dd>
                    </div>
                  ))}
                  {displayGrant.openai_credit ? (
                    <>
                      <dt className="text-label text-text-faint">
                        Plan support
                      </dt>
                      <dd className="text-text">
                        {displayGrant.openai_credit.is_supported_by_plan ===
                        true
                          ? 'Supported'
                          : displayGrant.openai_credit.is_supported_by_plan ===
                              false
                            ? 'Not supported by this plan'
                            : 'Not reported by OpenAI'}
                      </dd>
                      {displayGrant.openai_credit.description ? (
                        <>
                          <dt className="text-label text-text-faint">
                            Provider note
                          </dt>
                          <dd className="min-w-0 break-words text-text">
                            {displayGrant.openai_credit.description}
                          </dd>
                        </>
                      ) : null}
                    </>
                  ) : null}
                  {displayGrant.ends_at ? (
                    <>
                      <dt className="text-label text-text-faint">Expires</dt>
                      <dd className="min-w-0 break-words text-text">
                        <RelativeTime ts={new Date(displayGrant.ends_at)} />
                      </dd>
                    </>
                  ) : null}
                  {displayBlocking ? (
                    <>
                      <dt className="text-label text-text-faint">
                        Provider status
                      </dt>
                      <dd className="min-w-0 break-words text-warn-text">
                        {displayBlocking.text}
                        {displayBlocking.surface ? (
                          <>
                            {' '}
                            <a
                              href={surfaceUrl}
                              target="_blank"
                              rel="noreferrer"
                              className="inline-flex items-center gap-0.5 underline decoration-text-faint underline-offset-2 hover:text-text"
                            >
                              Open {providerSurface}
                              <ExternalLink className="h-3 w-3" />
                            </a>
                          </>
                        ) : null}
                      </dd>
                    </>
                  ) : null}
                  {!displayBlocking ? (
                    <>
                      <dt className="text-label text-text-faint">
                        Provider status
                      </dt>
                      <dd className="min-w-0 break-words text-text">
                        {inFlightView || selectedUsable
                          ? 'Usable now'
                          : 'Not usable now'}
                      </dd>
                    </>
                  ) : null}
                </dl>
              ) : null}
              {displayPresentation &&
              displayPresentation.fullNonTargets.length > 0 ? (
                <Notice tone="warning" title="Additional full usage">
                  {displayPresentation.fullNonTargets
                    .map(
                      (reading) =>
                        `${reading.label}: ${couponUsageLabel(reading)}`,
                    )
                    .join('; ')}
                  . These are additional usage readings, not a prediction of the
                  coupon's effects.
                </Notice>
              ) : null}
              {displayActiveCount != null &&
              displayUsableCount != null &&
              displayUsableCount < displayActiveCount ? (
                <p className="text-caption text-text-faint">
                  {displayUsableCount === 0
                    ? 'These coupons cannot be used right now. See the status above.'
                    : `${displayUsableCount} of ${displayActiveCount} coupons can be used right now.`}
                </p>
              ) : null}
            </>
          )}

          {displayCooldown ? (
            <p className="text-body-sm text-text-muted">
              Resets on cooldown until{' '}
              <ResetCountdown ts={new Date(displayCooldown)} />.
            </p>
          ) : null}

          {displayGrant &&
          (inFlightView != null || (!pendingView && selectedUsable)) ? (
            <p className="text-body-sm text-text-muted">
              {couponEffectDescription(
                displayGrant,
                isOpenAI ? 'openai' : 'anthropic',
              )}
            </p>
          ) : null}
          <a
            href={surfaceUrl}
            target="_blank"
            rel="noreferrer"
            className="inline-flex items-center gap-1 text-caption text-text-faint hover:text-text"
          >
            Manage on {providerSurface}
            <ExternalLink className="h-3 w-3" />
          </a>
        </div>
      </Modal>
    </>
  );
}
