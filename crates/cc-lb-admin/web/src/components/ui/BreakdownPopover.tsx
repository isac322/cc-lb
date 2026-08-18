import { splitNum } from '../../lib/format';
import { cx } from './primitives';

interface PopoverRow {
  label: string;
  value: number;
  color: string;
  fmt: (v: number) => string;
}

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
  return (
    <div className="min-w-[200px] font-mono">
      <div className="text-[10px] uppercase tracking-wider text-text-faint mb-1.5">
        {title}
      </div>
      {visible.length === 0 ? (
        note ? (
          <div className="max-w-[220px] whitespace-normal text-[11px] leading-snug text-text-muted">
            {note}
          </div>
        ) : (
          <div className="text-[11px] text-text-faint">—</div>
        )
      ) : (
        <div className="flex flex-col gap-1">
          {visible.map((r) => {
            const pct = total > 0 ? Math.round((r.value / total) * 100) : 0;
            const isZero = r.value <= 0;
            return (
              <div
                key={r.label}
                className={cx(
                  'flex items-center gap-2 text-[11px]',
                  isZero ? 'opacity-50' : '',
                )}
              >
                <span
                  className="h-2 w-2 rounded-full shrink-0"
                  style={{ backgroundColor: r.color }}
                />
                <span className="text-text-muted flex-1 truncate">
                  {r.label}
                </span>
                <span className="tabular-nums text-text w-14 text-right">
                  {isZero && isPartial ? '—' : r.fmt(r.value)}
                </span>
                <span className="tabular-nums text-text-faint w-9 text-right">
                  {isZero ? '—' : `${pct}%`}
                </span>
              </div>
            );
          })}
        </div>
      )}
      {footer ? (
        <div className="border-t border-subtle mt-1.5 pt-1 flex items-center gap-2 text-[11px]">
          <span className="h-2 w-2 shrink-0" />
          <span className="text-text-faint flex-1">{footer.label}</span>
          <span className="tabular-nums text-text w-14 text-right">
            {isPartial && footer.value <= 0 ? '—' : footer.fmt(footer.value)}
          </span>
          <span className="w-9" />
        </div>
      ) : null}
    </div>
  );
}

export function fmtTokens(v: number): string {
  const { value, unit } = splitNum(v);
  return value + unit;
}
