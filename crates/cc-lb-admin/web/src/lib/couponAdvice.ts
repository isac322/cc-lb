import { WINDOW_DURATION_SECS } from './colors';
import { formatWait } from './couponNudge';
import type { CouponEffectRow, CouponEffects } from './couponPresentation';
import type { LimitResetGrant } from './limitResets';

export type CouponAdviceKind =
  | 'scope'
  | 'nothing_to_restore'
  | 'wait'
  | 'save'
  | 'useful_now'
  | 'expiry'
  | 'unknown';

export interface CouponAdvice {
  kind: CouponAdviceKind;
  message: string;
}

export interface CouponAdviceInput {
  grant: LimitResetGrant;
  effects: CouponEffects;
  nowMs: number;
  /** Provider gates and unresolved operations always take precedence. */
  blocked?: boolean;
}

const WAIT_SECONDS = 3600;
const SAVE_PACE_MARGIN = 10;

function freshUsage(row: CouponEffectRow): number | null {
  const value = row.current.percentUsed;
  return row.current.state === 'fresh' &&
    value != null &&
    Number.isFinite(value) &&
    value >= 0
    ? value
    : null;
}

function durationSeconds(
  grant: LimitResetGrant,
  row: CouponEffectRow,
): number | null {
  const duration = grant.openai_credit
    ? grant.openai_manual_redemption?.windows.find(
        (window) => window.window === row.window,
      )?.duration_seconds
    : WINDOW_DURATION_SECS[row.window];
  return duration != null && Number.isFinite(duration) && duration > 0
    ? duration
    : null;
}

function resetSeconds(row: CouponEffectRow, nowSeconds: number): number | null {
  const reset = row.resetsAtUnixSecs;
  return reset != null && Number.isFinite(reset) && reset > nowSeconds
    ? reset
    : null;
}

/** Nonblocking guidance about this exact selected grant. This never chooses a
 * coupon, predicts exhaustion, changes eligibility, or estimates a Fable rebate. */
export function deriveCouponAdvice({
  grant,
  effects,
  nowMs,
  blocked = false,
}: CouponAdviceInput): CouponAdvice | null {
  if (blocked) return null;
  const nowSeconds = nowMs / 1000;
  const rows = effects.rows;
  const unaddressed = rows.filter(
    (row) =>
      (row.window === '5h' || row.window === '7d') &&
      row.effect === 'unchanged' &&
      (freshUsage(row) ?? -1) >= 100,
  );
  if (unaddressed.length > 0) {
    return {
      kind: 'scope',
      message: `${unaddressed.map((row) => row.label).join(' and ')} is full and will not be restored by this coupon. Other limits may still prevent use.`,
    };
  }

  const restored = rows.filter((row) => row.effect === 'reset');
  const uncertainEffects = rows.some(
    (row) =>
      row.effect === 'unknown' ||
      row.effect === 'conditional_5h' ||
      row.effect === 'fable_weekly' ||
      row.effect === 'fable_scoped',
  );
  const allUsageKnown = restored.every((row) => freshUsage(row) != null);
  if (
    restored.length > 0 &&
    !uncertainEffects &&
    allUsageKnown &&
    restored.every((row) => freshUsage(row) === 0)
  ) {
    return {
      kind: 'nothing_to_restore',
      message: 'There is no usage to restore right now.',
    };
  }

  const positive = restored.filter((row) => (freshUsage(row) ?? 0) > 0);
  const expiry = grant.ends_at == null ? NaN : Date.parse(grant.ends_at) / 1000;
  const expiryKnown = Number.isFinite(expiry) && expiry > nowSeconds;
  const hasFullTarget = restored.some((row) => (freshUsage(row) ?? -1) >= 100);
  const resetTimes = positive.map((row) => resetSeconds(row, nowSeconds));
  const allResetsKnown = resetTimes.every((reset) => reset != null);
  const lastsBeyondResets =
    expiryKnown &&
    allResetsKnown &&
    resetTimes.every((reset) => reset != null && expiry > reset);
  const completeBenefit =
    !uncertainEffects && allUsageKnown && positive.length > 0;

  if (
    completeBenefit &&
    lastsBeyondResets &&
    resetTimes.every(
      (reset) => reset != null && reset - nowSeconds <= WAIT_SECONDS,
    )
  ) {
    const waitUntil = resetTimes.reduce<number>(
      (latest, reset) => Math.max(latest, reset ?? nowSeconds),
      nowSeconds,
    );
    return {
      kind: 'wait',
      message: `If you can wait, keep this coupon: the affected usage resets naturally ${formatWait((waitUntil - nowSeconds) * 1000)}.`,
    };
  }

  if (
    completeBenefit &&
    !hasFullTarget &&
    lastsBeyondResets &&
    positive.every((row) => {
      const used = freshUsage(row);
      const reset = resetSeconds(row, nowSeconds);
      const duration = durationSeconds(grant, row);
      if (used == null || reset == null || duration == null) return false;
      const elapsed = duration - (reset - nowSeconds);
      if (elapsed <= 0 || elapsed >= duration) return false;
      const pace = (elapsed / duration) * 100;
      return pace - used >= SAVE_PACE_MARGIN;
    })
  ) {
    return {
      kind: 'save',
      message:
        'Usage is at least 10 percentage points below an even pace, and this coupon remains valid after the natural resets.',
    };
  }

  if (hasFullTarget) {
    const expiresBeforeReset =
      expiryKnown &&
      restored.some((row) => {
        const reset = resetSeconds(row, nowSeconds);
        return (
          (freshUsage(row) ?? -1) >= 100 && reset != null && expiry <= reset
        );
      });
    return {
      kind: 'useful_now',
      message: `This coupon can restore a full limit if you need to keep working.${expiresBeforeReset ? ' It expires by the time that limit resets naturally.' : ''}`,
    };
  }

  const expiryBeforeReset =
    expiryKnown &&
    rows.some((row) => {
      if (row.effect === 'unchanged') return false;
      const reset = resetSeconds(row, nowSeconds);
      return reset != null && expiry <= reset;
    });
  if (expiryBeforeReset) {
    return {
      kind: 'expiry',
      message:
        'This coupon expires by the next natural reset of an affected limit. Review whether you will need it before it expires.',
    };
  }

  if (
    rows.length === 0 ||
    uncertainEffects ||
    !allUsageKnown ||
    !expiryKnown ||
    (positive.length > 0 &&
      (!allResetsKnown ||
        positive.some((row) => durationSeconds(grant, row) == null)))
  ) {
    const fableUnknown = rows.some(
      (row) => row.effect === 'fable_weekly' || row.effect === 'fable_scoped',
    );
    const stale = rows.some(
      (row) => row.effect !== 'unchanged' && row.current.state === 'stale',
    );
    return {
      kind: 'unknown',
      message: fableUnknown
        ? 'The amount of weekly Fable usage this coupon returns is not reported, so its benefit cannot be estimated.'
        : stale
          ? 'Usage is stale. Refresh usage before deciding whether to keep this coupon.'
          : !expiryKnown
            ? 'The coupon expiry is not reported, so there is not enough information to recommend when to use it.'
            : 'There is not enough current usage or reset information to recommend when to use this coupon.',
    };
  }
  return null;
}
