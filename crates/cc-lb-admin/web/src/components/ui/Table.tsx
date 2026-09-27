import type {
  HTMLAttributes,
  ReactNode,
  TdHTMLAttributes,
  ThHTMLAttributes,
} from 'react';
import { cx } from './primitives';

/**
 * Table primitives (DESIGN.md §5 Tables). One header style everywhere:
 * `text-label` sans, sentence case, `text-faint`, 32px, opaque sticky
 * `bg-sub` with a `border-strong` rule. Body is 13px sans; mono only for
 * ID / model / path / hash columns. Numbers right-align with tabular figures.
 *
 * Tables inside a card drop the card's body padding; the first and last
 * cells keep a 16px inset so the header aligns with the card title.
 */
export function Table({
  className,
  ...rest
}: HTMLAttributes<HTMLTableElement>) {
  return (
    <table
      className={cx(
        'relative w-full border-collapse text-body-sm text-text [&_th:first-child]:pl-4 [&_th:last-child]:pr-4 [&_td:first-child]:pl-4 [&_td:last-child]:pr-4',
        className,
      )}
      {...rest}
    />
  );
}

/** Sticky, opaque header group. Pass `sticky={false}` for short tables. */
export function TableHead({
  className,
  sticky = true,
  ...rest
}: HTMLAttributes<HTMLTableSectionElement> & { sticky?: boolean }) {
  return (
    <thead
      className={cx('table-header', sticky && 'sticky top-0 z-10', className)}
      {...rest}
    />
  );
}

export function TableHeadCell({
  className,
  numeric = false,
  scope = 'col',
  ...rest
}: ThHTMLAttributes<HTMLTableCellElement> & {
  /** Right-align to sit over a numeric column. */
  numeric?: boolean;
}) {
  return (
    <th
      scope={scope}
      className={cx('table-th', numeric && 'text-right', className)}
      {...rest}
    />
  );
}

/**
 * 40px row (32px with `dense`), `border-row` divider, `overlay-2` hover,
 * `accent-dim` when selected.
 */
export function TableRow({
  className,
  selected = false,
  dense = false,
  interactive = false,
  ...rest
}: HTMLAttributes<HTMLTableRowElement> & {
  selected?: boolean;
  dense?: boolean;
  /** Adds the pointer cursor and hover fill for clickable rows. */
  interactive?: boolean;
}) {
  return (
    <tr
      className={cx(
        'border-b border-row last:border-b-0 transition-colors',
        dense ? 'h-8' : 'h-10',
        interactive && 'cursor-pointer hover:bg-overlay-2',
        selected && 'bg-accent-dim',
        className,
      )}
      {...rest}
    />
  );
}

export function TableCell({
  className,
  numeric = false,
  mono = false,
  ...rest
}: TdHTMLAttributes<HTMLTableCellElement> & {
  /** Right-aligned tabular figures. Put units in `text-muted` at the same size. */
  numeric?: boolean;
  /** Machine strings only: IDs, model IDs, paths, hashes (12px mono). */
  mono?: boolean;
}) {
  return (
    <td
      className={cx(
        'px-3 py-2 align-middle',
        numeric && 'text-right tabular-nums whitespace-nowrap',
        mono && 'font-mono text-data',
        className,
      )}
      {...rest}
    />
  );
}

/** The `—` placeholder for a null value. */
export function EmptyValue({ label = 'No value' }: { label?: string }) {
  return (
    <span className="relative text-text-faint">
      <span aria-hidden="true">—</span>
      <span className="sr-only">{label}</span>
    </span>
  );
}

/** One muted line spanning the table, for an empty or filtered-out body. */
export function TableEmptyRow({
  colSpan,
  children,
}: {
  colSpan: number;
  children: ReactNode;
}) {
  return (
    <tr>
      <td
        colSpan={colSpan}
        className="px-4 py-8 text-center text-body-sm text-text-muted"
      >
        {children}
      </td>
    </tr>
  );
}
