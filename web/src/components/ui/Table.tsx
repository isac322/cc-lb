import type {
  HTMLAttributes,
  ReactNode,
  TdHTMLAttributes,
  ThHTMLAttributes,
} from 'react';
import { cx } from './primitives';

/**
 * Table primitives. One header style everywhere: 12px/500 sans, sentence
 * case, `text-faint`, 36px, opaque sticky panel with a 1px line. Body is
 * 14px sans; mono only for ID / model / path / hash columns. Numbers
 * right-align with tabular figures.
 *
 * Tables inside a card drop the card's body padding; the first and last
 * cells keep a 16px inset so the header aligns with the card title. On
 * phones the gutters between columns tighten from 24 to 16px so the text
 * columns keep their width.
 */
export function Table({
  className,
  ...rest
}: HTMLAttributes<HTMLTableElement>) {
  return (
    <table
      className={cx(
        'relative w-full border-collapse text-body text-text max-md:[&_th]:px-2 [&_th:first-child]:pl-4 [&_th:last-child]:pr-4 [&_td:first-child]:pl-4 [&_td:last-child]:pr-4',
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
 * 40px row (32px with `dense`), 1px `border-row` divider, hover fill, and
 * when selected the neutral selection fill (no edge rule).
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
        interactive && 'cursor-pointer hover:bg-hover-bg',
        selected && 'bg-selected hover:bg-selected',
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
        'px-3 max-md:px-2 py-2 align-middle',
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
