// Linear quota usage: the fill is what has been USED of a window, matching
// the utilization Claude reports. Neutral ink below 80% used, warn from 80%,
// danger from 95%, with a 1px mark where the warn zone starts.
// Static: nothing animates, so reduced motion needs no special case.
import {
  formatQuotaPercent,
  QUOTA_SEVERITY_TEXT_CLASS,
  QUOTA_WARN_PCT,
  quotaSeverity,
} from '../../lib/quotaSeverity';
import { cx } from './primitives';

export interface UsageMeterProps {
  /** Window utilization 0-100; `null`/`undefined` renders an empty track. */
  usedPct: number | null | undefined;
  /** `sm`: 4px bar for dense lists. `md`: 6px bar with `N% used` above it. */
  size?: 'sm' | 'md';
  /** Accessible name of the meter, e.g. the window ("7d"). */
  label?: string;
  className?: string;
}

const FILL_CLASS = {
  none: 'bg-text-faint',
  ok: 'bg-text-muted',
  warn: 'bg-warn',
  danger: 'bg-danger',
} as const;

export function UsageMeter({
  usedPct,
  size = 'sm',
  label = 'Usage',
  className,
}: UsageMeterProps) {
  const used =
    usedPct == null || !Number.isFinite(usedPct)
      ? null
      : Math.min(100, Math.max(0, usedPct));
  const severity = quotaSeverity(used);
  return (
    <div
      aria-label={label}
      aria-valuemax={100}
      aria-valuemin={0}
      aria-valuenow={used ?? undefined}
      aria-valuetext={
        used === null ? 'No reading' : `${formatQuotaPercent(used)} used`
      }
      className={cx('flex min-w-0 flex-col gap-1.5', className)}
      role="meter"
    >
      {size === 'md' ? (
        <span
          className={cx(
            'text-title-card tabular-nums',
            severity === 'warn' || severity === 'danger'
              ? QUOTA_SEVERITY_TEXT_CLASS[severity]
              : 'text-text',
          )}
        >
          {used === null ? '—' : `${formatQuotaPercent(used)} used`}
        </span>
      ) : null}
      <span
        className={cx(
          'relative block w-full bg-progress-track',
          size === 'md' ? 'h-1.5' : 'h-1',
        )}
      >
        {used !== null && used > 0 ? (
          <span
            className={cx('absolute inset-y-0 left-0', FILL_CLASS[severity])}
            style={{ width: `${Math.max(used, 1)}%` }}
          />
        ) : null}
        <span
          aria-hidden="true"
          className="absolute -inset-y-0.5 w-px bg-text-faint"
          style={{ left: `${QUOTA_WARN_PCT}%` }}
        />
      </span>
    </div>
  );
}
