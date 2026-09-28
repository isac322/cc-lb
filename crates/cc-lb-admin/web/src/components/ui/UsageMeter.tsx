// Linear quota usage: the fill is what has been USED of a window, matching
// the utilization Claude reports. Neutral ink below 80% used, warn from 80%,
// danger from 95%. The only mark on the track is the optional even-pace
// tick: where usage would sit if spread evenly over the window.
// Static: nothing animates, so reduced motion needs no special case.
import {
  formatQuotaPercent,
  QUOTA_SEVERITY_TEXT_CLASS,
  quotaSeverity,
} from '../../lib/quotaSeverity';
import { cx } from './primitives';

export interface UsageMeterProps {
  /** Window utilization 0-100; `null`/`undefined` renders an empty track. */
  usedPct: number | null | undefined;
  /**
   * Even pace 0-100 (`quotaPacePct`): the share of the window elapsed. Draws
   * a thin ink tick across the track; `null`/`undefined` draws none.
   */
  pacePct?: number | null;
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

// A 2px ink tick, 2px taller than the track on each side, centred on its
// position. Ink (not a severity or window color) so it reads on any fill.
const PACE_TICK_CLASS =
  'pointer-events-none absolute -inset-y-0.5 w-0.5 -translate-x-1/2 bg-text';

function clampPct(pct: number | null | undefined): number | null {
  return pct == null || !Number.isFinite(pct)
    ? null
    : Math.min(100, Math.max(0, pct));
}

export function UsageMeter({
  usedPct,
  pacePct,
  size = 'sm',
  label = 'Usage',
  className,
}: UsageMeterProps) {
  const used = clampPct(usedPct);
  const pace = clampPct(pacePct);
  const severity = quotaSeverity(used);
  const valueText = [
    used === null ? 'No reading' : `${formatQuotaPercent(used)} used`,
    pace === null ? null : `even pace ${formatQuotaPercent(pace)}`,
  ]
    .filter(Boolean)
    .join(', ');
  return (
    <div
      aria-label={label}
      aria-valuemax={100}
      aria-valuemin={0}
      aria-valuenow={used ?? undefined}
      aria-valuetext={valueText}
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
        {pace !== null ? (
          <span
            aria-hidden="true"
            className={PACE_TICK_CLASS}
            data-pace-pct={pace}
            data-slot="pace-marker"
            style={{ left: `${pace}%` }}
          />
        ) : null}
      </span>
    </div>
  );
}

/**
 * The once-per-section key for the pace tick: a small tick glyph and
 * "even pace", with the rule in its `title`.
 */
export function PaceLegend({ className }: { className?: string }) {
  return (
    <span
      className={cx(
        'inline-flex items-center gap-1.5 text-caption text-text-muted',
        className,
      )}
      data-testid="pace-legend"
      title="Even pace: where usage would be if spread evenly over the window (time elapsed since the window started)"
    >
      <span aria-hidden="true" className="inline-block h-3 w-0.5 bg-text" />
      even pace
    </span>
  );
}
