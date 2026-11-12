// Header-level limit-reset ("coupon") action for the upstream detail view.
// Renders a small secondary button next to Delete plus a direct confirmation
// dialog; it owns its query, claim mutation and unknown-outcome marker so the
// surrounding page stays untouched.

import { ChevronRight, ExternalLink, History, Ticket } from 'lucide-react';
import {
  type ReactNode,
  useCallback,
  useEffect,
  useRef,
  useState,
} from 'react';
import { toast } from 'sonner';
import { ApiError } from '../../lib/api';
import { type CouponAdvice, deriveCouponAdvice } from '../../lib/couponAdvice';
import { formatWait, type NudgeQuotaWindow } from '../../lib/couponNudge';
import {
  type CouponEffects as CouponEffectsView,
  type CouponGroup,
  type CouponPresentation,
  chooseCouponGrant,
  chooseCouponGroupGrant,
  couponClaimMessage,
  couponGroupKey,
  couponInventory,
  couponScopeLabel,
  couponUsageLabel,
  groupCouponGrants,
  presentCoupon,
  presentCouponEffects,
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
import { Button, Drawer, Modal, Notice } from '../ui/primitives';
import { RelativeTime, ResetCountdown } from '../ui/RelativeTime';
import { Select } from '../ui/Select';
import { CouponEffects } from './CouponEffects';
import { CouponActionButton, useCouponNudge } from './CouponNudge';
import { ResetHistoryPanel } from './ResetHistoryPanel';

const ADVICE_TITLES: Record<CouponAdvice['kind'], string> = {
  nothing_to_restore: 'No usage to restore',
  wait: 'You can wait',
  save: 'Enough unused quota',
  expiry: 'Check coupon expiry',
  unknown: 'Not enough information',
  scope: 'Does not restore this limit',
  useful_now: 'Restores a full limit',
};

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
      case 'coverage_unreported':
        return {
          label: 'Covered allowances could not be confirmed.',
          surface: false,
        };
      case 'manual_usage_not_needed':
        return {
          label:
            'At least one covered allowance must have less than 80% remaining (more than 20% used).',
          surface: false,
        };
      case 'manual_usage_unknown':
        return {
          label:
            'Manual reset eligibility is not confirmed. Refresh usage before using a coupon.',
          surface: false,
        };
      case 'unsupported_reset_type':
        return {
          label: 'This reset type is not supported here.',
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

function CouponTypeCount({ group }: { group: CouponGroup }) {
  return (
    <span
      className={`inline-flex min-w-0 items-center gap-2 ${group.fable ? 'text-series-fable' : 'text-text'}`}
    >
      <Ticket
        className="size-4 shrink-0"
        strokeWidth={1.75}
        aria-hidden="true"
      />
      <span>{group.label}</span>
      <span className="tabular-nums">×{group.remaining}</span>
    </span>
  );
}

export function LimitResetAction({
  upstream,
  credentialsStored,
  quotaWindows = [],
  renderTrigger,
  defaultOpen = false,
  onOpenChange,
  initialOwnedCount,
  returnFocus,
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
  onOpenChange?: (open: boolean) => void;
  initialOwnedCount?: number | null;
  returnFocus?: HTMLElement | null;
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

  const [dialogOpen, updateDialogOpen] = useState(defaultOpen);
  const workflowOpenRef = useRef(defaultOpen);
  const returnFocusRef = useRef<HTMLElement | null>(returnFocus ?? null);
  const setDialogOpen = useCallback((open: boolean) => {
    workflowOpenRef.current = open;
    updateDialogOpen(open);
  }, []);
  const restoreFocus = useCallback(() => {
    // A surface swap unmounts one overlay while the workflow stays open.
    // Only a real close returns focus to the original coupon entry point.
    if (workflowOpenRef.current) return false;
    return returnFocusRef.current?.isConnected ? returnFocusRef.current : true;
  }, []);
  useEffect(() => {
    onOpenChange?.(dialogOpen);
  }, [dialogOpen, onOpenChange]);
  const [pendingOp, setPendingOp] = useState<PendingLimitResetOp | null>(() =>
    loadPendingLimitResetOp(upstream.id),
  );
  const [surface, setSurface] = useState<'coupon' | 'history'>(() => {
    if (pendingOp != null) return 'coupon';
    if (credentialsStored === false) return 'history';
    if (initialOwnedCount !== undefined)
      return initialOwnedCount != null && initialOwnedCount > 0
        ? 'coupon'
        : 'history';
    const owned = isOpenAI
      ? resetsQ.data?.openai?.available_count
      : derived?.activeCount;
    return credentialsStored === true &&
      !resetsQ.isError &&
      owned != null &&
      owned > 0
      ? 'coupon'
      : 'history';
  });
  const [selectedGroupKey, setSelectedGroupKey] = useState<string | null>(null);
  const [selectedGrantId, setSelectedGrantId] = useState<string | null>(null);
  // One request_id per confirmation, generated when the dialog opens.
  const [requestId, setRequestId] = useState(() => newUuid());
  // Frozen confirmation view captured when a fresh claim is dispatched: the
  // modal keeps showing exactly what the user confirmed while the request is
  // in flight instead of swapping to the pending-op surface mid-request.
  const [inFlightView, setInFlightView] = useState<{
    grant: LimitResetGrant;
    presentation: CouponPresentation;
    effects: CouponEffectsView;
    advice: CouponAdvice | null;
    grants: LimitResetGrant[];
    activeCount: number;
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

  // Expired/depleted/not-yet-started grants never appear as a usable count or
  // a selectable option — activeGrants is the single filtered list.
  const selectableGrants = derived?.activeGrants ?? [];
  const ownedInventoryCount = isOpenAI
    ? data?.openai?.available_count
    : derived?.activeCount;
  const hasOwnedCoupons =
    credentialsStored === true &&
    !queryError &&
    ownedInventoryCount != null &&
    ownedInventoryCount > 0;
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
  const provider = isOpenAI ? 'openai' : 'anthropic';
  const groups = groupCouponGrants(selectableGrants, provider);
  const initialGrant = chooseCouponGrant(
    selectableGrants,
    readyGrants,
    quotaWindows,
    null,
    observationOptions,
  );
  const selectedGroup = groups.find(
    (group) =>
      group.key ===
      (selectedGroupKey ??
        (initialGrant ? couponGroupKey(initialGrant, provider) : null)),
  );
  const selected =
    selectedGrantId != null
      ? (selectedGroup?.grants.find((grant) => grant.id === selectedGrantId) ??
        null)
      : selectedGroup
        ? chooseCouponGroupGrant(selectedGroup, readyGrants)
        : null;
  const selectionKey = selectedGroup?.key;
  const selectionId = selected?.id;
  // Pin the exact credit being reviewed. Polling must never silently replace
  // a disappeared credit, even with another credit of the same type.
  useEffect(() => {
    if (
      !dialogOpen ||
      selectedGroupKey != null ||
      !selectionKey ||
      !selectionId
    )
      return;
    setSelectedGroupKey(selectionKey);
    setSelectedGrantId(selectionId);
  }, [dialogOpen, selectedGroupKey, selectionKey, selectionId]);
  const selectedPresentation = selected
    ? presentCoupon(selected, quotaWindows, observationOptions)
    : null;
  const selectedEffects = selected
    ? presentCouponEffects(selected, quotaWindows, {
        ...observationOptions,
        provider,
        weeklyResetsAt: ember?.weekly_resets_at,
      })
    : null;
  const selectedBlocking = selected
    ? blockingReason(selected, ember?.cooldown_until ?? null, nowMs)
    : null;
  const selectedUsable =
    credentialsStored === true &&
    eligible &&
    Boolean(data?.account_id && (isOpenAI || data.organization_id)) &&
    selected != null &&
    grantReady(selected) &&
    !selectedBlocking;
  const selectedAdvice =
    selected && selectedEffects
      ? deriveCouponAdvice({
          grant: selected,
          effects: selectedEffects,
          nowMs,
          blocked: !selectedUsable || queryError || pendingOp != null,
        })
      : null;

  // Recorded activity is local admin data, independent of provider credentials
  // and inventory loading. Only an actual claim locks the entry point.
  const actionDisabled = claimInFlight;

  const openDialog = (event?: { currentTarget: EventTarget | null }) => {
    if (actionDisabled) return;
    returnFocusRef.current =
      returnFocus ??
      (event?.currentTarget instanceof HTMLElement
        ? event.currentTarget
        : document.activeElement instanceof HTMLElement
          ? document.activeElement
          : null);
    setSelectedGrantId(null);
    setSelectedGroupKey(null);
    setSurface(pendingOp != null || hasOwnedCoupons ? 'coupon' : 'history');
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
    if (credentialsStored !== true) return;
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
  const displayExpiry = displayGrant?.ends_at
    ? new Date(displayGrant.ends_at)
    : null;
  const displayBlocking = inFlightView ? null : selectedBlocking;
  const displayPresentation =
    inFlightView?.presentation ?? selectedPresentation;
  const displayEffects = inFlightView?.effects ?? selectedEffects;
  const displayAdvice = inFlightView ? inFlightView.advice : selectedAdvice;
  const historyOpen = surface === 'history' && inFlightView == null;
  const displayGrants = inFlightView?.grants ?? selectableGrants;
  const displayGroups = groupCouponGrants(displayGrants, provider);
  const displayGroup = displayGrant
    ? displayGroups.find(
        (group) => group.key === couponGroupKey(displayGrant, provider),
      )
    : null;
  const unlistedCount = isOpenAI
    ? Math.max(
        0,
        (inFlightView?.activeCount ?? data?.openai?.available_count ?? 0) -
          displayGroups.reduce((count, group) => count + group.remaining, 0),
      )
    : 0;

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
  // Inventory loading must not disable access to recorded activity.
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
          loading: claimInFlight,
        })
      ) : (
        <CouponActionButton
          kind={buttonKind}
          count={buttonCount}
          disabled={actionDisabled}
          loading={claimInFlight}
          expiryLabel={expiryLabel}
          title={buttonTitle}
          onClick={openDialog}
        >
          {buttonLabel}
        </CouponActionButton>
      )}

      {historyOpen ? (
        <Drawer
          open={dialogOpen}
          finalFocus={restoreFocus}
          onOpenChange={(open) => {
            if (!open) {
              setSurface('history');
              setDialogOpen(false);
            }
          }}
          title="Reset activity"
          description={upstream.name}
          width="md"
          footer={
            hasOwnedCoupons || pendingOp != null ? (
              <Button
                size="sm"
                disabled={claimInFlight}
                onClick={() => setSurface('coupon')}
              >
                {pendingOp != null ? 'Review pending reset' : 'View coupons'}
              </Button>
            ) : undefined
          }
        >
          <p className="px-4 pt-4 text-body-sm text-text-muted">
            {notConnected
              ? 'Connect this account to check current coupons.'
              : queryError
                ? 'Coupon inventory could not be loaded.'
                : credentialsStored === undefined ||
                    (resetsQ.isPending && !data)
                  ? 'Checking current coupon inventory…'
                  : ownedInventoryCount === 0
                    ? 'No active coupons available.'
                    : hasOwnedCoupons
                      ? 'Coupons are available for this account.'
                      : 'Coupon inventory is not reported.'}
          </p>
          <ResetHistoryPanel upstreamId={upstream.id} open={dialogOpen} />
        </Drawer>
      ) : (
        <Modal
          open={dialogOpen}
          finalFocus={restoreFocus}
          onOpenChange={(open) => {
            if (!open && !claimInFlight) {
              setDialogOpen(false);
              setSurface('history');
            }
          }}
          title={pendingView ? 'Reset not confirmed' : 'Use a reset coupon?'}
          description={upstream.name}
          size="md"
          preventDismiss={claimInFlight}
          footer={
            <div className="flex w-full flex-col gap-3 text-body-sm text-text-muted">
              {!pendingView && displayGrant ? (
                <>
                  <p className="text-caption text-text-faint">
                    Uses 1 coupon. Cannot be undone.
                  </p>
                  {displayBlocking ? (
                    <Notice tone="warning" title="Not usable now">
                      {displayBlocking.text}
                      {displayBlocking.surface ? (
                        <a
                          href={surfaceUrl}
                          target="_blank"
                          rel="noreferrer"
                          className="ml-1 inline-flex items-center gap-1 underline underline-offset-2 hover:text-text"
                        >
                          Open {providerSurface}
                          <ExternalLink className="size-3" aria-hidden="true" />
                        </a>
                      ) : null}
                    </Notice>
                  ) : cooldownActive && !inFlightView ? (
                    <Notice tone="warning" title="Resets on cooldown">
                      {ember?.cooldown_until ? (
                        <>
                          Available{' '}
                          <ResetCountdown ts={new Date(ember.cooldown_until)} />
                        </>
                      ) : (
                        'Try after the cooldown ends.'
                      )}
                    </Notice>
                  ) : !selectedUsable && !inFlightView && eligible ? (
                    <Notice tone="warning" title="Not usable now">
                      The account identity or coupon availability is not
                      confirmed.
                    </Notice>
                  ) : displayAdvice ? (
                    <div role="group" aria-label="Coupon guidance">
                      <Notice
                        tone={
                          displayAdvice.kind === 'useful_now'
                            ? 'info'
                            : 'warning'
                        }
                        title={ADVICE_TITLES[displayAdvice.kind]}
                        role="status"
                      >
                        {displayAdvice.message}
                      </Notice>
                    </div>
                  ) : null}
                </>
              ) : null}
              <div className="flex w-full flex-wrap items-center justify-end gap-2">
                <Button
                  variant="ghost"
                  size="sm"
                  className="mr-auto"
                  disabled={claimInFlight}
                  onClick={() => setSurface('history')}
                >
                  <History aria-hidden="true" />
                  History
                </Button>
                <Button
                  autoFocus
                  size="sm"
                  disabled={claimInFlight}
                  onClick={() => setDialogOpen(false)}
                >
                  Cancel
                </Button>
                {pendingView ? (
                  <>
                    <Button
                      size="sm"
                      variant={dismissArmed ? 'danger' : 'secondary'}
                      disabled={claimInFlight}
                      onClick={() =>
                        dismissArmed
                          ? dismissPendingOp()
                          : setDismissArmed(true)
                      }
                    >
                      {dismissArmed ? 'Dismiss anyway' : 'Dismiss record'}
                    </Button>
                    <Button
                      size="sm"
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
                    size="sm"
                    loading={claimInFlight}
                    disabled={!selectedUsable || !data || queryError}
                    onClick={() => {
                      if (!selected || !data || !selectedUsable || queryError)
                        return;
                      runClaim(
                        { grant_id: selected.id, request_id: requestId },
                        {
                          grant: structuredClone(selected),
                          presentation: presentCoupon(
                            selected,
                            quotaWindows,
                            observationOptions,
                          ),
                          effects: presentCouponEffects(
                            selected,
                            quotaWindows,
                            {
                              ...observationOptions,
                              provider,
                              weeklyResetsAt: ember?.weekly_resets_at,
                            },
                          ),
                          advice: selectedAdvice ? { ...selectedAdvice } : null,
                          grants: structuredClone(selectableGrants),
                          activeCount: ownedCount ?? 0,
                        },
                      );
                    }}
                  >
                    Use coupon
                  </Button>
                )}
              </div>
            </div>
          }
        >
          <div className="flex flex-col gap-4 text-body-sm text-text-muted">
            {pendingView && pendingOp ? (
              <>
                <Notice tone="warning" title="A coupon may have been used">
                  This request has no confirmed outcome. Check History or
                  refresh status; a changed coupon count does not prove the
                  result. No request will be retried.
                </Notice>
                <p className="text-caption text-text-faint">
                  Requested{' '}
                  <RelativeTime ts={pendingOp.started_at_unix_secs * 1000} />
                </p>
                <details className="group">
                  <summary className="flex min-h-11 cursor-pointer list-none items-center gap-1 text-text-muted md:min-h-8">
                    <ChevronRight
                      className="size-3 transition-transform group-open:rotate-90"
                      aria-hidden="true"
                    />
                    Details
                  </summary>
                  <p className="text-caption text-text-faint">
                    Local request reference
                  </p>
                  <code className="block break-all font-mono text-data">
                    {pendingOp.request_id}
                  </code>
                </details>
                {dismissArmed ? (
                  <Notice tone="danger" title="Dismiss this record?">
                    This only clears the local warning. It does not undo a
                    coupon use or confirm the outcome.
                  </Notice>
                ) : null}
              </>
            ) : (
              <>
                {queryError && !inFlightView ? (
                  <Notice
                    tone="danger"
                    title="Could not load reset status"
                    action={
                      <Button
                        size="sm"
                        loading={resetsQ.isFetching}
                        onClick={() => resetsQ.refetch()}
                      >
                        Retry
                      </Button>
                    }
                  >
                    {resetsQ.error instanceof ApiError &&
                    resetsQ.error.status === 404
                      ? 'This server does not support limit resets.'
                      : resetsQ.error instanceof Error
                        ? resetsQ.error.message
                        : 'The reset status request failed.'}
                  </Notice>
                ) : null}
                {!inFlightView && data && !eligible && ember ? (
                  <p>This account is not eligible for limit resets.</p>
                ) : null}
                {!inFlightView && data && !ember ? (
                  <p>
                    No current coupon data. Status updates with quota polling.
                  </p>
                ) : null}
                {notConnected && !inFlightView ? (
                  <Notice tone="warning" title="Account not connected">
                    Connect the account before using a coupon. Recorded activity
                    remains available in History.
                  </Notice>
                ) : null}
                {loading && !inFlightView ? (
                  <p role="status">Checking reset availability…</p>
                ) : null}
                {!inFlightView && !selected && selectedGroupKey != null ? (
                  <Notice
                    tone="warning"
                    title="Selected coupon is no longer available"
                  >
                    Choose a coupon again to continue. No reset has been
                    submitted.
                  </Notice>
                ) : null}
                <div className="space-y-2">
                  {displayGroups.length > 1 ||
                  unlistedCount > 0 ||
                  (!displayGrant && displayGroups.length > 0) ? (
                    <Select
                      aria-label="Coupon type"
                      className="w-full max-md:min-h-11"
                      value={displayGroup?.key ?? ''}
                      disabled={claimInFlight}
                      onChange={(key) => {
                        const group = groups.find((item) => item.key === key);
                        if (!group) return;
                        setSelectedGroupKey(key);
                        setSelectedGrantId(
                          chooseCouponGroupGrant(group, readyGrants)?.id ??
                            null,
                        );
                      }}
                      placeholder="Choose a coupon"
                      options={[
                        ...displayGroups.map((group) => ({
                          value: group.key,
                          label: <CouponTypeCount group={group} />,
                        })),
                        ...(unlistedCount > 0
                          ? [
                              {
                                value: 'unlisted',
                                label: (
                                  <span className="inline-flex items-center gap-2">
                                    <Ticket
                                      className="size-4"
                                      strokeWidth={1.75}
                                      aria-hidden="true"
                                    />
                                    Type not reported ×{unlistedCount}
                                  </span>
                                ),
                                disabled: true,
                              },
                            ]
                          : []),
                      ]}
                    />
                  ) : displayGroup ? (
                    <CouponTypeCount group={displayGroup} />
                  ) : ember && !loading ? (
                    <p>No active reset coupons available.</p>
                  ) : null}
                  {displayGrant ? (
                    <dl className="text-caption text-text-faint">
                      <div className="flex items-baseline gap-2">
                        <dt>Expires</dt>
                        <dd className="text-text-muted">
                          {displayExpiry ? (
                            <RelativeTime ts={displayExpiry} />
                          ) : (
                            'No expiry reported'
                          )}
                        </dd>
                      </div>
                    </dl>
                  ) : null}
                </div>
                {displayGrant ? (
                  <>
                    {displayEffects ? (
                      <CouponEffects effects={displayEffects} />
                    ) : null}
                    <details className="group">
                      <summary className="flex min-h-11 cursor-pointer list-none items-center gap-1 text-text-muted md:min-h-8">
                        <ChevronRight
                          className="size-3 transition-transform group-open:rotate-90"
                          aria-hidden="true"
                        />
                        Details
                      </summary>
                      <div className="space-y-3 pt-2 text-caption">
                        <p>{displayGrant.label}</p>
                        <p>
                          Scope:{' '}
                          {displayGrant.openai_credit?.reset_type ===
                          'codex_rate_limits'
                            ? 'Main Codex allowance'
                            : couponScopeLabel(displayGrant)}
                        </p>
                        {displayGrant.clears.length > 0 ? (
                          <p>
                            Provider scope reference:{' '}
                            <code className="font-mono text-data">
                              {displayGrant.clears.join(', ')}
                            </code>
                          </p>
                        ) : null}
                        {displayGrant.openai_credit?.description ? (
                          <p>{displayGrant.openai_credit.description}</p>
                        ) : null}
                        {displayGrant.openai_credit ? (
                          <p>
                            Plan support:{' '}
                            {displayGrant.openai_credit.is_supported_by_plan ===
                            true
                              ? 'Supported'
                              : displayGrant.openai_credit
                                    .is_supported_by_plan === false
                                ? 'Not supported'
                                : 'Not reported'}
                          </p>
                        ) : null}
                        {displayGrant.openai_manual_redemption?.reason ? (
                          <p>
                            Manual eligibility reference:{' '}
                            <code className="font-mono text-data">
                              {displayGrant.openai_manual_redemption.reason}
                            </code>
                          </p>
                        ) : null}
                        {displayPresentation ? (
                          <ul className="space-y-1">
                            {[
                              ...displayPresentation.targets,
                              ...displayPresentation.fullNonTargets,
                            ].map((reading) => (
                              <li key={reading.window}>
                                {reading.label}: {couponUsageLabel(reading)}
                              </li>
                            ))}
                          </ul>
                        ) : null}
                        {displayGroup && displayGroup.grants.length > 1 ? (
                          <p>
                            The earliest-expiring usable coupon of this type is
                            selected.
                          </p>
                        ) : null}
                        <p className="text-text-faint">Coupon reference</p>
                        <code className="block break-all font-mono text-data">
                          {displayGrant.id}
                        </code>
                        <a
                          href={surfaceUrl}
                          target="_blank"
                          rel="noreferrer"
                          className="inline-flex min-h-11 items-center gap-1 underline underline-offset-2 hover:text-text md:min-h-8"
                        >
                          Manage on {providerSurface}
                          <ExternalLink className="size-3" aria-hidden="true" />
                        </a>
                      </div>
                    </details>
                  </>
                ) : null}
              </>
            )}
          </div>
        </Modal>
      )}
    </>
  );
}
