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
 * between them. Segments always scale within their own bar, never across rows.
 * Renders nothing when every segment is zero.
 */
export function StackedBar({
  segments,
  total: explicitTotal,
  size = 'md',
  ariaLabel,
  className,
}: {
  segments: readonly StackedBarSegment[];
  /** Denominator when the segments do not cover the whole (defaults to their sum). */
  total?: number;
  size?: keyof typeof HEIGHTS;
  ariaLabel?: string;
  className?: string;
}) {
  const segmentTotal = segments.reduce(
    (sum, segment) => sum + Math.max(0, segment.value),
    0,
  );
  const total = Math.max(segmentTotal, explicitTotal ?? 0);
  if (total <= 0) return null;
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
