import { WINDOW_DURATION_SECS } from './colors';
import { formatWait } from './couponNudge';
import {
  type CouponEffectRow,
  type CouponEffects,
  isFableRebateGrant,
} from './couponPresentation';
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
 * coupon, predicts exhaustion, or changes provider eligibility. */
export function deriveCouponAdvice({
  grant,
  effects,
  nowMs,
  blocked = false,
}: CouponAdviceInput): CouponAdvice | null {
  if (blocked) return null;
  const nowSeconds = nowMs / 1000;
  const rows = effects.rows;
  const restored = rows.filter((row) => row.effect === 'reset');
  const fableWeeklyRow =
    rows.find((row) => row.effect === 'fable_weekly') ?? null;
  const fableBucketRow = fableWeeklyRow
    ? (rows.find(
        (row) => row.window === '7d_fable' && row.effect === 'reset',
      ) ?? null)
    : null;
  const fableWeeklyUsed = fableWeeklyRow ? freshUsage(fableWeeklyRow) : null;
  const fableBucketUsed = fableBucketRow ? freshUsage(fableBucketRow) : null;
  const fableNoBucket = fableWeeklyRow != null && fableBucketRow == null;
  const fableZeroOnFullWeekly =
    fableWeeklyRow != null &&
    (fableWeeklyUsed ?? -1) >= 100 &&
    fableBucketUsed === 0;

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
  if (fableZeroOnFullWeekly) {
    return {
      kind: 'scope',
      message:
        'Weekly is full, and Weekly (Fable) is at 0% used so this coupon has no Fable usage to restore.',
    };
  }

  if (
    restored.length > 0 &&
    !fableNoBucket &&
    (fableWeeklyRow == null || fableWeeklyUsed != null) &&
    restored.every((row) => freshUsage(row) === 0)
  ) {
    return {
      kind: 'nothing_to_restore',
      message: 'There is no usage to restore right now.',
    };
  }

  const allUsageKnown =
    restored.every((row) => freshUsage(row) != null) &&
    (fableWeeklyRow == null || (fableWeeklyUsed != null && !fableNoBucket));
  const positive = restored.filter((row) => (freshUsage(row) ?? 0) > 0);
  const timingRows = [
    ...positive,
    ...(fableWeeklyRow != null && (fableBucketUsed ?? 0) > 0
      ? [fableWeeklyRow]
      : []),
  ];
  const expiry = grant.ends_at == null ? NaN : Date.parse(grant.ends_at) / 1000;
  const expiryKnown = Number.isFinite(expiry) && expiry > nowSeconds;
  // For Fable rebates, 7d_fable alone does not restore shared Weekly when 7d < 100%;
  // only non-Fable reset targets count as direct full limits here.
  const directRestored =
    isFableRebateGrant(grant) || fableWeeklyRow != null
      ? restored.filter((row) => row.window !== '7d_fable')
      : restored;
  const hasDirectFullTarget = directRestored.some(
    (row) => (freshUsage(row) ?? -1) >= 100,
  );
  const fableUnblocksWeekly =
    fableWeeklyRow != null &&
    (fableWeeklyUsed ?? -1) >= 100 &&
    (fableBucketUsed ?? 0) > 0;
  const hasFullTarget = hasDirectFullTarget || fableUnblocksWeekly;
  const resetTimes = timingRows.map((row) => resetSeconds(row, nowSeconds));
  const allResetsKnown = resetTimes.every((reset) => reset != null);
  const lastsBeyondResets =
    expiryKnown &&
    allResetsKnown &&
    resetTimes.every((reset) => reset != null && expiry > reset);
  const completeBenefit = allUsageKnown && positive.length > 0;

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
    timingRows.every((row) => {
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
    const fullRows: CouponEffectRow[] = [
      ...directRestored.filter((row) => (freshUsage(row) ?? -1) >= 100),
      ...(fableUnblocksWeekly && fableWeeklyRow ? [fableWeeklyRow] : []),
    ];
    const expiresBeforeReset =
      expiryKnown &&
      fullRows.some((row) => {
        const reset = resetSeconds(row, nowSeconds);
        return reset != null && expiry <= reset;
      });
    return {
      kind: 'useful_now',
      message:
        fableUnblocksWeekly && !hasDirectFullTarget
          ? `This coupon resets Weekly (Fable) and restores the Fable portion of your full Weekly limit.${expiresBeforeReset ? ' It expires by the time that limit resets naturally.' : ''}`
          : `This coupon can restore a full limit if you need to keep working.${expiresBeforeReset ? ' It expires by the time that limit resets naturally.' : ''}`,
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
    !allUsageKnown ||
    !expiryKnown ||
    (timingRows.length > 0 &&
      (!allResetsKnown ||
        timingRows.some((row) => durationSeconds(grant, row) == null)))
  ) {
    const stale = rows.some(
      (row) => row.effect !== 'unchanged' && row.current.state === 'stale',
    );
    return {
      kind: 'unknown',
      message: stale
        ? 'Usage is stale. Refresh usage before deciding whether to keep this coupon.'
        : !expiryKnown
          ? 'The coupon expiry is not reported, so there is not enough information to recommend when to use it.'
          : 'There is not enough current usage or reset information to recommend when to use this coupon.',
    };
  }
  return null;
}
