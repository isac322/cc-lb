// Visual presentation for the limit-reset ("coupon") nudge: a ticket-shaped
// header button (real notches + dashed count compartment) plus a
// non-interactive badge inside the sidebar upstream row.
// All decisions come from lib/couponNudge.deriveCouponNudge; these components
// only render the result and never consume or mutate coupon state. Quota
// stays the single source of truth for usage numbers.

import { Popover as BasePopover } from '@base-ui/react/popover';
import { useIsMutating } from '@tanstack/react-query';
import { Ticket } from 'lucide-react';
import { type ReactNode, useEffect, useRef, useState } from 'react';
import {
  type CouponNudge,
  type CouponNudgeKind,
  deriveCouponNudge,
  type NudgeQuotaWindow,
  useCouponNow,
} from '../../lib/couponNudge';
import {
  LIMIT_RESET_CLAIM_MUTATION_KEY,
  type LimitResetsResponse,
  loadPendingLimitResetOp,
  useLimitResets,
} from '../../lib/limitResets';
import type { Upstream } from '../../lib/queries';
import type { PolledDataResult } from '../../lib/usePolledData';
import { cx, Spinner } from '../ui/primitives';

// Tone is carried by icon + border + a short reason, never by color alone.
// `fill`/`stroke` feed the SVG ticket outline; `fill` is a group-hover pair
// so the shape follows the same hover wash as the button.
const TONE: Record<
  CouponNudgeKind,
  {
    icon: string;
    text: string;
    divider: string;
    fill: string;
    stroke: string;
  }
> = {
  quiet: {
    icon: 'text-text-faint',
    text: 'text-text-faint',
    divider: 'border-[color:var(--color-border)]',
    fill: 'fill-[var(--color-panel-strong)] group-hover/coupon:fill-[var(--color-hover-bg)]',
    stroke: 'stroke-[var(--color-border)]',
  },
  expiry: {
    icon: 'text-[color:var(--color-warn-text)]',
    text: 'text-[color:var(--color-warn-text)]',
    divider: 'border-[color:var(--color-warn)]/40',
    fill: 'fill-[var(--color-panel-strong)] group-hover/coupon:fill-[var(--color-hover-bg)]',
    stroke: 'stroke-[color-mix(in_oklab,var(--color-warn)_50%,transparent)]',
  },
  // A usable reset is news, not a warning: ink and the strong line, no
  // brand hue (the accent is reserved for gauges and the one primary action).
  limit: {
    icon: 'text-text',
    text: 'text-text',
    divider: 'border-[color:var(--color-border-strong)]',
    fill: 'fill-[var(--color-panel-strong)] group-hover/coupon:fill-[var(--color-hover-bg)]',
    stroke: 'stroke-[var(--color-border-strong)]',
  },
};

export interface CouponNudgeState {
  resetsQ: PolledDataResult<LimitResetsResponse, Error>;
  nowMs: number;
  /** Pending, unknown, or unavailable state suppresses recommendations. */
  suppressed: boolean;
  nudge: CouponNudge | null;
  /** Counts and active-grant selection remain available without a nudge. */
  derived: CouponNudge | null;
  /** A claim for this upstream is in flight right now (survives remounts,
   *  unlike useMutation().isPending on a fresh hook instance). */
  claimInFlight: boolean;
}

/** Shared nudge state for every coupon surface of one upstream. Pass null
 *  upstreamId for non-OAuth upstreams — the query stays disabled. */
export function useCouponNudge(
  upstreamId: string | null,
  windows: readonly NudgeQuotaWindow[],
): CouponNudgeState {
  const resetsQ = useLimitResets(upstreamId);
  const nowMs = useCouponNow();
  const inFlight =
    useIsMutating({
      mutationKey: LIMIT_RESET_CLAIM_MUTATION_KEY,
      predicate: (mutation) =>
        (mutation.state.variables as { upstreamId?: string } | undefined)
          ?.upstreamId === upstreamId,
    }) > 0;

  const data = resetsQ.data;
  const ember = data?.cedar_ember;
  // A persisted marker only suppresses while it still matches the account it
  // was written for — the same rule LimitResetAction applies. localStorage
  // is re-read on every render; the shared clock tick and query updates keep
  // this fresh without a storage event (same-window writes do not fire one).
  const pendingOp = upstreamId ? loadPendingLimitResetOp(upstreamId) : null;
  const pendingOpActive =
    pendingOp != null &&
    (data == null ||
      (pendingOp.account_id === data.account_id &&
        pendingOp.organization_id === data.organization_id));

  const suppressed =
    inFlight || pendingOpActive || resetsQ.isError || resetsQ.isPending;
  const derived =
    upstreamId && ember ? deriveCouponNudge(ember, windows, nowMs) : null;
  return {
    resetsQ,
    nowMs,
    suppressed,
    nudge: suppressed ? null : derived,
    derived,
    claimInFlight: inFlight,
  };
}

const ACTION_BASE =
  'inline-flex h-8 max-md:h-10 items-center rounded-sm border text-xs font-medium transition-colors select-none ' +
  'disabled:control-disabled ' +
  'focus-visible:outline focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-2';

/** Ticket-outline path for the header button: rounded corners plus a
 *  semicircular notch cut into the top and bottom edges at the count
 *  divider. `x` is the divider's center in border-box coordinates; null
 *  yields a plain rounded rectangle. Arc sweep 0 dips inward on both
 *  traversals (top edge runs left→right, bottom right→left). */
function ticketOutlinePath(
  w: number,
  h: number,
  x: number | null,
  r = 4,
  n = 5,
): string {
  if (x == null) {
    return (
      `M${r} 0 H${w - r} A${r} ${r} 0 0 1 ${w} ${r} V${h - r} ` +
      `A${r} ${r} 0 0 1 ${w - r} ${h} H${r} A${r} ${r} 0 0 1 0 ${h - r} ` +
      `V${r} A${r} ${r} 0 0 1 ${r} 0 Z`
    );
  }
  const nx = Math.min(Math.max(x, r + n + 1), w - r - n - 1);
  return (
    `M${nx - n} 0 A${n} ${n} 0 0 0 ${nx + n} 0 H${w - r} ` +
    `A${r} ${r} 0 0 1 ${w} ${r} V${h - r} A${r} ${r} 0 0 1 ${w - r} ${h} ` +
    `H${nx + n} A${n} ${n} 0 0 0 ${nx - n} ${h} H${r} ` +
    `A${r} ${r} 0 0 1 0 ${h - r} V${r} A${r} ${r} 0 0 1 ${r} 0 Z`
  );
}

/** Header coupon button: a real ticket outline (inward notches at the
 *  divider) drawn as an SVG shape, so the notches are transparent rather
 *  than painted holes.
 *
 *  `loading` swaps the ticket icon for a same-size spinner and disables the
 *  button; callers keep the label constant so the width never shifts.
 *  `expiryLabel` renders as compact text immediately after the button —
 *  outside the ticket outline so it never moves the notches or divider.
 *
 *  `disabledReason` wraps a genuinely disabled button in a focusable
 *  popover trigger: a native disabled button swallows pointer events and
 *  leaves the tab order, so the wrapper (not a nested button) carries
 *  hover/focus/touch and explains why the action is unavailable. */
export function CouponActionButton({
  kind,
  count,
  disabled,
  disabledReason,
  loading,
  expiryLabel,
  title,
  onClick,
  children,
}: {
  kind: CouponNudgeKind;
  /** Remaining resets across unexpired grants; null hides the count. */
  count: number | null;
  disabled?: boolean;
  /** Why the action is unavailable; shown in a popover while disabled. */
  disabledReason?: string;
  /** In-flight claim: spinner replaces the icon, label stays put. */
  loading?: boolean;
  /** Compact expiry text rendered adjacent to the button (e.g. "Expires in
   *  ~2h"); visible without hover. */
  expiryLabel?: string;
  title?: string;
  onClick?: () => void;
  children: ReactNode;
}) {
  const tone = TONE[kind];
  const isDisabled = Boolean(disabled || loading);
  const wrapped = Boolean(isDisabled && disabledReason);
  // Disabled (not in-flight): the ticket reads inert — no fill, a dashed
  // disabled line and disabled ink. A loading claim keeps its normal look.
  const inert = isDisabled && !loading;
  const [reasonOpen, setReasonOpen] = useState(false);
  const suppressFocusOpen = useRef(false);

  // DOM nodes live in state (not refs) so the measurement effect re-runs
  // whenever the button remounts — e.g. the disabled-reason wrapper toggles —
  // or the count compartment mounts/unmounts. The ResizeObserver then covers
  // count/label width changes without re-rendering.
  const [btnNode, setBtnNode] = useState<HTMLButtonElement | null>(null);
  const [dividerNode, setDividerNode] = useState<HTMLSpanElement | null>(null);
  const [geo, setGeo] = useState<{ w: number; h: number; x: number | null }>({
    w: 0,
    h: 0,
    x: null,
  });

  // Measure the button and divider so the SVG outline can place its notches
  // exactly on the dashed divider line (the compartment's left border edge).
  useEffect(() => {
    if (!btnNode) return;
    const measure = () => {
      const rect = btnNode.getBoundingClientRect();
      const x = dividerNode
        ? dividerNode.getBoundingClientRect().left - rect.left
        : null;
      setGeo((prev) =>
        prev.w === rect.width && prev.h === rect.height && prev.x === x
          ? prev
          : { w: rect.width, h: rect.height, x },
      );
    };
    measure();
    const observer = new ResizeObserver(measure);
    observer.observe(btnNode);
    if (dividerNode) observer.observe(dividerNode);
    return () => observer.disconnect();
  }, [btnNode, dividerNode]);

  const button = (
    <button
      type="button"
      ref={setBtnNode}
      disabled={isDisabled}
      aria-busy={loading || undefined}
      title={title}
      onClick={onClick}
      className={cx(
        ACTION_BASE,
        // The SVG ticket is the outline; keep the native border out of it
        // (`control-disabled` would add a dashed rectangle on top).
        'group/coupon relative items-stretch p-0 border-transparent disabled:border-transparent!',
        inert ? 'text-text-disabled' : 'text-text',
        wrapped && 'pointer-events-none',
      )}
    >
      {geo.w > 0 ? (
        <svg
          aria-hidden
          className="absolute -inset-px overflow-visible"
          viewBox={`0 0 ${geo.w} ${geo.h}`}
          preserveAspectRatio="none"
        >
          <path
            d={ticketOutlinePath(geo.w, geo.h, geo.x)}
            className={
              inert
                ? 'fill-transparent stroke-[var(--color-border-disabled)]'
                : cx(tone.fill, tone.stroke)
            }
            strokeDasharray={inert ? '3 3' : undefined}
            strokeWidth={1}
          />
        </svg>
      ) : null}
      <span className="relative inline-flex items-center gap-1.5 px-2.5">
        {loading ? (
          <Spinner className={cx('h-3.5 w-3.5 shrink-0', tone.icon)} />
        ) : (
          <Ticket
            className={cx(
              'h-3.5 w-3.5 shrink-0',
              inert ? 'text-text-disabled' : tone.icon,
            )}
          />
        )}
        {children}
      </span>
      {count != null ? (
        <span
          ref={setDividerNode}
          className={cx(
            'relative inline-flex items-center border-l border-dashed px-2 tabular-nums',
            inert
              ? 'border-border-disabled text-text-disabled'
              : cx(tone.divider, tone.icon),
          )}
        >
          {count}
        </span>
      ) : null}
    </button>
  );

  const action = !wrapped ? (
    button
  ) : (
    <BasePopover.Root
      open={reasonOpen}
      onOpenChange={(nextOpen) => {
        // After an Escape/outside dismissal the wrapper still holds focus;
        // block focus from reopening it until the user deliberately presses.
        if (!nextOpen) suppressFocusOpen.current = true;
        setReasonOpen(nextOpen);
      }}
    >
      <BasePopover.Trigger
        nativeButton={false}
        openOnHover
        delay={150}
        closeDelay={80}
        onFocus={() => {
          if (suppressFocusOpen.current) return;
          setReasonOpen(true);
        }}
        onBlur={() => {
          suppressFocusOpen.current = false;
          setReasonOpen(false);
        }}
        render={
          <span
            className={cx(
              'inline-flex cursor-not-allowed rounded-sm',
              'focus-visible:outline focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-2',
            )}
          />
        }
      >
        {button}
      </BasePopover.Trigger>
      <BasePopover.Portal>
        <BasePopover.Positioner
          className="z-50"
          side="bottom"
          align="end"
          sideOffset={6}
          collisionPadding={8}
        >
          <BasePopover.Popup className="max-w-[240px] rounded-md glass-strong px-2.5 py-1.5 text-caption text-text">
            <BasePopover.Description>{disabledReason}</BasePopover.Description>
          </BasePopover.Popup>
        </BasePopover.Positioner>
      </BasePopover.Portal>
    </BasePopover.Root>
  );

  if (!expiryLabel) return action;
  return (
    <span className="inline-flex items-center gap-2">
      {action}
      <span className="shrink-0 whitespace-nowrap text-caption text-warn-text">
        {expiryLabel}
      </span>
    </span>
  );
}

/** Compact expiry for the sidebar badge: "<1m" / "45m" / "2h" / "3d". */
function formatExpiryShort(ms: number): string {
  if (ms < 60_000) return '<1m';
  const mins = Math.round(ms / 60_000);
  if (mins < 60) return `${mins}m`;
  const hours = Math.round(mins / 60);
  if (hours < 48) return `${hours}h`;
  return `${Math.round(hours / 24)}d`;
}

/** Non-interactive badge inside a sidebar upstream row (the row itself is the
 *  button — this must never be or contain a button). Rendered only when there
 *  is something to say; a soon expiry is shown even when the limit hint is
 *  also active. */
export function SidebarCouponNudge({
  upstream,
  windows,
}: {
  upstream: Upstream;
  windows: readonly NudgeQuotaWindow[];
}) {
  const isOauth = upstream.kind === 'anthropic_oauth';
  const { nudge, nowMs } = useCouponNudge(
    isOauth ? upstream.id : null,
    windows,
  );
  const expiryMs =
    nudge?.expiresSoonAt != null ? nudge.expiresSoonAt - nowMs : null;
  if (!nudge || (nudge.kind === 'quiet' && expiryMs == null)) return null;
  const tone = TONE[nudge.kind];
  return (
    <div className={cx('flex items-center gap-1.5 text-caption', tone.text)}>
      <Ticket className="h-3 w-3 shrink-0" />
      {nudge.label ? <span className="truncate">{nudge.label}</span> : null}
      {nudge.activeCount > 0 ? (
        <span className="shrink-0 tabular-nums">×{nudge.activeCount}</span>
      ) : null}
      {expiryMs != null ? (
        <span className="shrink-0 whitespace-nowrap text-[color:var(--color-warn-text)]">
          · exp {formatExpiryShort(expiryMs)}
        </span>
      ) : null}
    </div>
  );
}
