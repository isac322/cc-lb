import { cx } from './primitives';

export interface StackedBarSegment {
  value: number;
  /** A CSS color (`var(--color-series-input)`) or a Tailwind `bg-*` class list. */
  color: string;
}

const HEIGHTS = { xs: 'h-1', sm: 'h-1.5', md: 'h-2' } as const;

/**
 * Meter-style stacked bar (DESIGN.md §5 Meters): 4px (`xs`, the composition
 * bar under a request-table metric), 6px (`sm`) or 8px (`md`), a flat track
 * with a 2px radius, square segments separated by a 1px gap so the track shows
 * between them. Segments scale within their own bar; `maxTotal` optionally
 * compares the outer width across rows. Renders nothing without a positive total.
 */
export function StackedBar({
  segments,
  total: explicitTotal,
  maxTotal,
  size = 'md',
  ariaLabel,
  className,
}: {
  segments: readonly StackedBarSegment[];
  /** Denominator when the segments do not cover the whole (defaults to their sum). */
  total?: number;
  /**
   * Optional outer denominator for relative bars. Segment widths still use
   * `total`; this only scales the track itself within its containing cell.
   */
  maxTotal?: number;
  size?: keyof typeof HEIGHTS;
  ariaLabel?: string;
  className?: string;
}) {
  const segmentTotal = segments.reduce(
    (sum, segment) => sum + Math.max(0, segment.value),
    0,
  );
  const total = Math.max(segmentTotal, explicitTotal ?? 0);
  const relativeScale =
    maxTotal == null
      ? null
      : maxTotal > 0 && Number.isFinite(maxTotal)
        ? Math.min(1, Math.max(0, explicitTotal ?? segmentTotal) / maxTotal)
        : 0;
  if (total <= 0 || relativeScale === 0) return null;
  return (
    <div
      role={ariaLabel ? 'img' : undefined}
      aria-label={ariaLabel}
      aria-hidden={ariaLabel ? undefined : true}
      className={cx(
        'flex w-full gap-px overflow-hidden rounded-xs bg-progress-track',
        HEIGHTS[size],
        className,
      )}
      style={
        relativeScale == null ? undefined : { width: `${relativeScale * 100}%` }
      }
    >
      {segments.map((segment, index) => {
        if (segment.value <= 0) return null;
        const isTailwindBg = /(^|\s)bg-/.test(segment.color);
        return (
          <span
            // Segments are positional; a zero segment is skipped, not reordered.
            key={index}
            className={cx(
              'h-full min-w-[2px]',
              isTailwindBg ? segment.color : undefined,
            )}
            style={{
              width: `${(segment.value / total) * 100}%`,
              backgroundColor: isTailwindBg ? undefined : segment.color,
            }}
          />
        );
      })}
    </div>
  );
}
