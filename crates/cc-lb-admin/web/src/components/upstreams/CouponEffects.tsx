import { ArrowRight } from 'lucide-react';
import { getWindowColor } from '../../lib/colors';
import type {
  CouponEffectRow,
  CouponEffects as Effects,
} from '../../lib/couponPresentation';
import { formatQuotaPercent } from '../../lib/quotaSeverity';
import { ResetCountdown } from '../ui/RelativeTime';

function afterUse(row: CouponEffectRow): string {
  switch (row.effect) {
    case 'reset':
      return '0% used';
    case 'unchanged':
      return 'Unchanged';
    case 'fable_weekly':
      return 'Fable portion restored';
    case 'fable_scoped':
      return 'Fable usage restored';
    case 'conditional_5h':
      return 'Fully refilled if this account has a 5-hour limit.';
    case 'unknown':
      return 'Effect not reported';
  }
}

function ResetSchedule({ row }: { row: CouponEffectRow }) {
  switch (row.schedule) {
    case 'unchanged':
      return row.resetsAtUnixSecs != null ? (
        <>
          <ResetCountdown ts={row.resetsAtUnixSecs * 1000} /> · unchanged
        </>
      ) : (
        'Reset time unchanged'
      );
    case 'confirmed_after_use':
      return 'Reset time · Confirmed after use';
    case 'updated_after_use':
      return 'Reset time · Updated after use';
    case 'unknown':
      return 'Reset time not reported';
  }
}

/** An expected before/after view, never an optimistic quota update. */
export function CouponEffects({ effects }: { effects: Effects }) {
  if (effects.rows.length === 0) {
    return (
      <p className="text-body-sm text-text-muted">
        Quota effects are not reported for this coupon.
      </p>
    );
  }
  return (
    <div className="space-y-3">
      <div
        aria-hidden="true"
        className="grid grid-cols-[minmax(0,1fr)_1rem_minmax(0,1.3fr)] gap-2 text-caption text-text-faint"
      >
        <span>Current usage</span>
        <span />
        <span>After use</span>
      </div>
      <dl aria-label="Expected quota changes" className="space-y-4">
        {effects.rows.map((row) => (
          <div key={row.window} className="space-y-1">
            <dt className="flex items-center gap-2 text-label text-text-muted">
              <span
                aria-hidden="true"
                className="size-2 shrink-0"
                style={{ backgroundColor: getWindowColor(row.window).stroke }}
              />
              {row.label}
            </dt>
            <dd className="space-y-1">
              {row.effect === 'conditional_5h' ? (
                <p className="text-body-sm text-text">{afterUse(row)}</p>
              ) : (
                <div className="grid grid-cols-[minmax(0,1fr)_1rem_minmax(0,1.3fr)] items-start gap-2 text-body-sm text-text">
                  <span>
                    <span className="sr-only">Current usage: </span>
                    {row.current.state === 'fresh' &&
                    row.current.percentUsed != null
                      ? `${formatQuotaPercent(row.current.percentUsed)} used`
                      : 'Unknown'}
                    {row.current.state === 'stale' ? (
                      <span className="text-text-faint"> · stale</span>
                    ) : null}
                  </span>
                  <ArrowRight
                    className="mt-0.5 size-4 text-text-faint"
                    strokeWidth={1.75}
                    aria-hidden="true"
                  />
                  <span>
                    <span className="sr-only">After use: </span>
                    {afterUse(row)}
                  </span>
                </div>
              )}
              {row.effect === 'fable_weekly' ? (
                <p className="text-caption text-text-faint">
                  Other-model weekly usage unchanged.
                </p>
              ) : null}
              <p className="text-caption text-text-faint">
                <ResetSchedule row={row} />
              </p>
            </dd>
          </div>
        ))}
      </dl>
      {effects.otherLimitsUnchanged ? (
        <p className="text-caption text-text-faint">
          Other model and feature limits unchanged.
        </p>
      ) : null}
    </div>
  );
}
