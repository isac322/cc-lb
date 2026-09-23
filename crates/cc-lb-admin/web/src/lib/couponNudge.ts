// Pure derivation for the coupon ("limit reset") nudge. Counts and
// recommendations come only from the limit-resets payload plus the quota
// snapshots the page already renders — quota stays the single source of
// truth for usage and nothing here invents numbers, consumes grants, or
// pressures the user.
//
// Thresholds are display hints, not provider policy: an expiry notice
// appears within 24h of ends_at, and a limit hint only when the natural
// reset is at least 1h away.

import { useSyncExternalStore } from 'react';
import type { CedarEmberLimitResets, LimitResetGrant } from './limitResets';

/** Structural subset of QuotaSnapshot the nudge derivation needs. */
export interface NudgeQuotaWindow {
  window: string;
  /** 0–1 fraction; null when the window carries no utilization number. */
  utilization: number | null;
  state: string;
  resets_at_unix_secs: number | null;
}

export type CouponNudgeKind = 'quiet' | 'expiry' | 'limit';

export interface CouponNudge {
  /** Owned grants that still count: started, unexpired, resets remaining. */
  activeGrants: LimitResetGrant[];
  /** Remaining resets across activeGrants. */
  activeCount: number;
  /** Remaining resets across grants actually usable right now. */
  usableCount: number;
  kind: CouponNudgeKind;
  label: string | null;
  detail: string | null;
  /** ISO ends_at of the soonest-expiring usable grant; null when unlimited. */
  earliestExpiry: string | null;
  /** ms epoch of the soonest usable-grant expiry inside the 24h notice
   *  window. Independent of `kind` — a 'limit' or 'quiet' nudge can still
   *  carry expiry so surfaces render it alongside the recommendation.
   *  Null when no usable grant expires soon. */
  expiresSoonAt: number | null;
}

const MINUTE_MS = 60_000;
const HOUR_MS = 60 * MINUTE_MS;
const EXPIRY_NOTICE_MS = 24 * HOUR_MS;
const LIMIT_MIN_WAIT_MS = HOUR_MS;

function parseMs(iso: string): number | null {
  const ms = Date.parse(iso);
  return Number.isNaN(ms) ? null : ms;
}

/** Owned and still countable: started, not ended, resets left, valid dates.
 *  A null starts_at means the provider did not report one; the grant counts
 *  as started rather than being hidden. */
function isActiveGrant(grant: LimitResetGrant, nowMs: number): boolean {
  if (grant.resets_left <= 0) return false;
  if (grant.starts_at != null) {
    const startsMs = parseMs(grant.starts_at);
    if (startsMs == null || startsMs > nowMs) return false;
  }
  if (grant.ends_at != null) {
    const endsMs = parseMs(grant.ends_at);
    if (endsMs == null || endsMs <= nowMs) return false;
  }
  // An 'expired' blocking reason is the server stating a past ends_at; other
  // blocking reasons leave the grant countable even though it is unusable.
  return !grant.blocking.includes('expired');
}

/** Actually consumable right now: eligible account, no pause/block/cooldown. */
function isUsableGrant(
  grant: LimitResetGrant,
  ember: CedarEmberLimitResets,
  nowMs: number,
): boolean {
  if (!ember.eligible || !grant.usable_now || grant.paused) return false;
  if (grant.blocking.length > 0) return false;
  if (ember.cooldown_until != null) {
    const cooldownMs = parseMs(ember.cooldown_until);
    // An unparseable cooldown fails closed rather than urging consumption.
    if (cooldownMs == null || cooldownMs > nowMs) return false;
  }
  return true;
}

/** "in ~4h" / "in ~35m" / "in ~3d" / "in under a minute". Exported so the
 *  action button can render the same compact expiry phrasing. */
export function formatWait(ms: number): string {
  if (ms < MINUTE_MS) return 'in under a minute';
  const mins = Math.round(ms / MINUTE_MS);
  if (mins < 60) return `in ~${mins}m`;
  const hours = Math.round(mins / 60);
  if (hours < 48) return `in ~${hours}h`;
  return `in ~${Math.round(hours / 24)}d`;
}

export function deriveCouponNudge(
  ember: CedarEmberLimitResets | undefined,
  windows: readonly NudgeQuotaWindow[],
  nowMs: number,
): CouponNudge {
  const activeGrants = (ember?.grants ?? []).filter((g) =>
    isActiveGrant(g, nowMs),
  );
  const usableGrants =
    ember == null
      ? []
      : activeGrants.filter((g) => isUsableGrant(g, ember, nowMs));
  let earliestExpiry: string | null = null;
  let earliestExpiryMs = Infinity;
  for (const g of usableGrants) {
    if (g.ends_at == null) continue;
    const ms = parseMs(g.ends_at);
    if (ms != null && ms < earliestExpiryMs) {
      earliestExpiryMs = ms;
      earliestExpiry = g.ends_at;
    }
  }

  const base = {
    activeGrants,
    activeCount: activeGrants.reduce((sum, g) => sum + g.resets_left, 0),
    usableCount: usableGrants.reduce((sum, g) => sum + g.resets_left, 0),
    earliestExpiry,
    // Orthogonal to kind: expiry stays visible even when a limit
    // recommendation wins the headline slot.
    expiresSoonAt:
      earliestExpiryMs !== Infinity &&
      earliestExpiryMs - nowMs <= EXPIRY_NOTICE_MS
        ? earliestExpiryMs
        : null,
  };
  if (usableGrants.length === 0) {
    return { ...base, kind: 'quiet', label: null, detail: null };
  }

  // Limit first: a window this coupon clears is freshly observed at 100%.
  // Stale/absent/unobserved quota, windows the grant does not clear, and
  // non-finite numbers never produce usage advice — 'fresh' alone cannot
  // create truth. One full window with a long observed wait is enough; a
  // sibling window resetting soon does not suppress the hint.
  const byWindow = new Map(windows.map((w) => [w.window, w]));
  let limitedWindow: string | null = null;
  let limitedWaitMs = -1;
  for (const g of usableGrants) {
    for (const name of g.clears) {
      const w = byWindow.get(name);
      if (w?.state !== 'fresh') continue;
      if (w.utilization == null || !Number.isFinite(w.utilization)) continue;
      if (w.utilization < 1 || w.resets_at_unix_secs == null) continue;
      const wait = w.resets_at_unix_secs * 1000 - nowMs;
      // A natural reset inside the hour is left to happen on its own.
      if (wait >= LIMIT_MIN_WAIT_MS && wait > limitedWaitMs) {
        limitedWaitMs = wait;
        limitedWindow = name;
      }
    }
  }
  if (limitedWindow != null) {
    return {
      ...base,
      kind: 'limit',
      label: 'At your limit',
      detail: `${limitedWindow} window resets on its own ${formatWait(limitedWaitMs)} — a reset clears it now.`,
    };
  }

  // Expiry is informational only — stated as a fact, never "use it now",
  // even when usage is low.
  if (earliestExpiryMs - nowMs <= EXPIRY_NOTICE_MS) {
    return {
      ...base,
      kind: 'expiry',
      label: 'Reset expiring soon',
      detail: `Earliest reset expires ${formatWait(earliestExpiryMs - nowMs)}.`,
    };
  }

  return { ...base, kind: 'quiet', label: null, detail: null };
}

// ── Shared ticking clock ─────────────────────────────────────────────────────
// One interval for every nudge consumer so expiry boundaries and countdown
// text re-evaluate as time passes without each component owning a timer.

const NOW_TICK_MS = 1_000;
let nowSnapshot = Date.now();
let nowTimer: ReturnType<typeof setInterval> | null = null;
const nowListeners = new Set<() => void>();

function subscribeNow(listener: () => void): () => void {
  nowListeners.add(listener);
  if (nowTimer == null) {
    nowTimer = setInterval(() => {
      nowSnapshot = Date.now();
      for (const l of nowListeners) l();
    }, NOW_TICK_MS);
  }
  return () => {
    nowListeners.delete(listener);
    if (nowListeners.size === 0 && nowTimer != null) {
      clearInterval(nowTimer);
      nowTimer = null;
    }
  };
}

/** Current time in ms, re-rendered every second while subscribed. */
export function useCouponNow(): number {
  return useSyncExternalStore(
    subscribeNow,
    () => nowSnapshot,
    () => nowSnapshot,
  );
}
