import { splitNum } from '../../lib/format';
import { cx } from './primitives';

interface PopoverRow {
  label: string;
  value: number;
  color: string;
  fmt: (v: number) => string;
}

/** Digits after the decimal point of a formatted figure, or 0 without one. */
function fractionDigits(text: string): number {
  const decimal = text.lastIndexOf('.');
  return decimal < 0 ? 0 : text.length - decimal - 1;
}

/**
 * A formatted figure padded on the right with invisible zeros up to `fraction`
 * digits, so right-aligned figures of different precision ("$1.5",
 * "$0.000005") put their decimal points in one column. The padding is
 * tabular-width, hidden from assistive tech, and outside the figure's own
 * text node, so the figure still reads as itself.
 */
function DecimalFigure({ text, fraction }: { text: string; fraction: number }) {
  const pad = text.includes('.') ? fraction - fractionDigits(text) : 0;
  return (
    <>
      {text}
      {pad > 0 ? (
        <span aria-hidden="true" className="invisible" data-slot="decimal-pad">
          {'0'.repeat(pad)}
        </span>
      ) : null}
    </>
  );
}

/*
 * The value column is sized by its widest figure (never a fixed width that a
 * long micro-dollar amount spills out of) and right-aligned, with decimals
 * padded to a shared precision; the label truncates instead.
 */
const VALUE_CLASS =
  'min-w-14 shrink-0 whitespace-nowrap text-right tabular-nums text-text';

export function BreakdownPopover({
  title,
  rows,
  footer,
  showZeroRows,
  isPartial,
  note,
}: {
  title: string;
  rows: PopoverRow[];
  footer?: { label: string; value: number; fmt: (v: number) => string } | null;
  showZeroRows?: boolean;
  isPartial?: boolean;
  /** Replaces the em dash when no row is visible: why the rows are missing. */
  note?: string;
}) {
  const visible = showZeroRows ? rows : rows.filter((r) => r.value > 0);
  const total = rows.reduce((a, r) => a + r.value, 0);
  const rowTexts = visible.map((r) =>
    r.value <= 0 && isPartial ? '—' : r.fmt(r.value),
  );
  const footerText = footer
    ? isPartial && footer.value <= 0
      ? '—'
      : footer.fmt(footer.value)
    : null;
  // Every value line ends on the same right edge (the share column follows),
  // so a shared fraction width is all decimal alignment needs.
  const fraction = Math.max(
    0,
    ...rowTexts.map(fractionDigits),
    footerText == null ? 0 : fractionDigits(footerText),
  );
  return (
    <div className="min-w-[200px] py-1">
      <div className="text-label text-text mb-1.5">{title}</div>
      {visible.length === 0 ? (
        note ? (
          <div className="max-w-[220px] whitespace-normal text-caption text-text-muted">
            {note}
          </div>
        ) : (
          <div className="text-caption text-text-faint">—</div>
        )
      ) : (
        <div className="flex flex-col gap-1">
          {visible.map((r, index) => {
            const pct = total > 0 ? Math.round((r.value / total) * 100) : 0;
            const isZero = r.value <= 0;
            return (
              <div
                key={r.label}
                className={cx(
                  'flex items-center gap-2 text-caption',
                  isZero ? 'opacity-50' : '',
                )}
              >
                <span
                  className="h-2 w-2 rounded-xs shrink-0"
                  style={{ backgroundColor: r.color }}
                />
                <span className="text-text-muted min-w-0 flex-1 truncate">
                  {r.label}
                </span>
                <span className={VALUE_CLASS}>
                  <DecimalFigure text={rowTexts[index]} fraction={fraction} />
                </span>
                <span className="tabular-nums text-text-faint w-9 shrink-0 text-right">
                  {isZero ? '—' : `${pct}%`}
                </span>
              </div>
            );
          })}
        </div>
      )}
      {footer && footerText != null ? (
        <div className="border-t border-row mt-1.5 pt-1 flex items-center gap-2 text-caption">
          <span className="h-2 w-2 shrink-0" />
          <span className="text-text-muted min-w-0 flex-1 truncate">
            {footer.label}
          </span>
          <span className={VALUE_CLASS}>
            <DecimalFigure text={footerText} fraction={fraction} />
          </span>
          <span className="w-9 shrink-0" />
        </div>
      ) : null}
    </div>
  );
}

export function fmtTokens(v: number): string {
  const { value, unit } = splitNum(v);
  return value + unit;
}
