import { cx } from './primitives';

/**
 * Displays an already formatted currency amount while softening only the
 * right-hand padding zeros in its fractional part. The decimal point and all
 * significant fraction digits stay primary; whole-number zeroes never mute.
 */
export function CostFigure({
  text,
  className,
}: {
  text: string;
  className?: string;
}) {
  const decimalIndex = text.lastIndexOf('.');
  if (decimalIndex < 0) {
    return (
      <span data-slot="cost" className={cx('text-text', className)}>
        {text}
      </span>
    );
  }

  const integer = text.slice(0, decimalIndex);
  const fraction = text.slice(decimalIndex + 1);
  let significantEnd = fraction.length;
  while (significantEnd > 0 && fraction[significantEnd - 1] === '0') {
    significantEnd -= 1;
  }

  return (
    <span data-slot="cost" className={cx('text-text', className)}>
      <span>{integer}</span>
      <span>.</span>
      {significantEnd > 0 ? (
        <span>{fraction.slice(0, significantEnd)}</span>
      ) : null}
      {significantEnd < fraction.length ? (
        <span className="text-text-muted">
          {fraction.slice(significantEnd)}
        </span>
      ) : null}
    </span>
  );
}
