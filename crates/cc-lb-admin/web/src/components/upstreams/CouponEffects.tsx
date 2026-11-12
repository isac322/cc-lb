import { ArrowRight } from 'lucide-react';
import { getWindowColor } from '../../lib/colors';
import type {
  CouponEffectRow,
  CouponEffects as Effects,
} from '../../lib/couponPresentation';
import { formatQuotaPercent } from '../../lib/quotaSeverity';
import { ResetCountdown } from '../ui/RelativeTime';

const COMPARISON_GRID =
  'grid grid-cols-[5.5rem_4rem_0.75rem_minmax(0,1fr)] gap-x-2 md:grid-cols-[6.5rem_4rem_1rem_minmax(0,1fr)] md:gap-x-3';

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
    <div>
      <div
        aria-hidden="true"
        className={`${COMPARISON_GRID} pb-2 text-caption text-text-faint`}
      >
        <span>Quota</span>
        <span className="text-right">Now</span>
        <span />
        <span>After use</span>
      </div>
      <dl
        aria-label="Expected quota changes"
        className="divide-y divide-row border-t border-row"
      >
        {effects.rows.map((row) => (
          <div
            key={row.window}
            className={`${COMPARISON_GRID} items-baseline gap-y-1 py-3`}
          >
            <dt className="flex min-w-0 items-baseline gap-2 text-label text-text">
              <span
                aria-hidden="true"
                className="size-2 shrink-0 self-center"
                style={{ backgroundColor: getWindowColor(row.window).stroke }}
              />
              <span className="min-w-0">{row.label}</span>
            </dt>
            <dd className="contents">
              <span className="min-w-0 text-right text-body-sm tabular-nums text-text-muted">
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
                className="mt-1 size-3 self-start text-text-faint md:size-4"
                strokeWidth={1.75}
                aria-hidden="true"
              />
              <span
                className={`min-w-0 break-words ${
                  row.effect === 'unchanged' || row.effect === 'unknown'
                    ? 'text-body-sm text-text-muted'
                    : 'text-body font-medium text-text'
                }`}
              >
                <span className="sr-only">After use: </span>
                {afterUse(row)}
              </span>
              <p className="col-span-4 text-caption text-text-faint">
                <ResetSchedule row={row} />
              </p>
              {row.effect === 'fable_weekly' ? (
                <p className="col-span-4 text-caption text-text-faint">
                  Other-model weekly usage unchanged.
                </p>
              ) : null}
            </dd>
          </div>
        ))}
      </dl>
      {effects.otherLimitsUnchanged ? (
        <p className="mt-2 text-caption text-text-faint">
          Other model and feature limits unchanged.
        </p>
      ) : null}
    </div>
  );
}
