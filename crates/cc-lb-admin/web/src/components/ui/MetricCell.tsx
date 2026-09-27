import type { ReactNode } from 'react';
import { cx, Hint } from './primitives';
import { StackedBar, type StackedBarSegment } from './StackedBar';

/**
 * Tabular total plus the 4px bar slot under it. The slot is always reserved,
 * so a row whose bar is empty keeps its number on the same baseline as its
 * neighbours; with the cell's 6px insets the stack fits the 40px row.
 */
function MetricStack({
  children,
  segments,
  total,
  pulse,
}: {
  children: ReactNode;
  segments: readonly StackedBarSegment[];
  total?: number;
  pulse?: boolean;
}) {
  return (
    <>
      <span
        className={cx(
          'block tabular-nums leading-5',
          pulse ? 'animate-pulse' : '',
        )}
      >
        {children}
      </span>
      <span className="mt-[3px] block h-1" data-cell-bar="">
        <StackedBar size="xs" segments={segments} total={total} />
      </span>
    </>
  );
}

/**
 * One request-table metric (latency, tokens, cost): the total on top and its
 * composition as a thin stacked bar underneath, scaled within the cell. Hover
 * opens `popover`; the cell is a button, so keyboard users open the same
 * breakdown with Enter or Space. Clicks never reach the row.
 */
export function MetricCell({
  className,
  label,
  describedBy,
  popover,
  segments,
  total,
  pulse,
  children,
}: {
  className?: string;
  /** Accessible name of the trigger, ending in "show breakdown". */
  label: string;
  describedBy?: string;
  popover: ReactNode;
  segments: readonly StackedBarSegment[];
  /** Denominator when the segments do not cover the whole. */
  total?: number;
  pulse?: boolean;
  children: ReactNode;
}) {
  return (
    <td
      className={cx('p-0 text-right whitespace-nowrap', className)}
      onClick={(event) => event.stopPropagation()}
    >
      <Hint label={popover} openOnFocus={false}>
        <button
          type="button"
          aria-label={label}
          aria-describedby={describedBy}
          className="block w-full min-w-18 cursor-help rounded-sm border-0 bg-transparent px-3 py-1.5 text-inherit [text-align:inherit] focus-visible:outline-2 focus-visible:-outline-offset-2 focus-visible:outline-accent"
        >
          <MetricStack segments={segments} total={total} pulse={pulse}>
            {children}
          </MetricStack>
        </button>
      </Hint>
    </td>
  );
}

/** A metric the request never recorded: a dash over an empty bar slot. */
export function EmptyMetricCell({ className }: { className?: string }) {
  return (
    <td className={cx('p-0 text-right whitespace-nowrap', className)}>
      <span className="block min-w-18 px-3 py-1.5">
        <MetricStack segments={[]}>
          <span className="text-text-faint">—</span>
        </MetricStack>
      </span>
    </td>
  );
}
