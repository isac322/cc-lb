import { ChevronLeft, ChevronRight } from 'lucide-react';
import { Button } from './primitives';

export interface LogsPaginationProps {
  readonly page: number;
  readonly pageCount: number;
  readonly totalRows: number;
  readonly pageSize: number;
  readonly onPrev: () => void;
  readonly onNext: () => void;
}

export function LogsPagination({
  page,
  pageCount,
  totalRows,
  pageSize,
  onPrev,
  onNext,
}: LogsPaginationProps) {
  const startRow = totalRows === 0 ? 0 : page * pageSize + 1;
  const endRow = Math.min((page + 1) * pageSize, totalRows);

  return (
    <nav
      aria-label="Log pagination"
      className="flex items-center justify-between gap-4 px-4 py-3 border-t border-subtle bg-bg-sub"
    >
      <div
        aria-live="polite"
        className="text-xs text-text-muted flex items-center gap-4"
      >
        <span>
          Showing {startRow}–{endRow} of {totalRows}
        </span>
        <span className="hidden sm:inline">
          Page {page + 1} of {pageCount}
        </span>
      </div>
      <div className="flex items-center gap-2">
        <Button
          variant="secondary"
          size="sm"
          onClick={onPrev}
          disabled={page <= 0}
          aria-label="Previous page"
          iconLeft={<ChevronLeft className="w-4 h-4" />}
        >
          Prev
        </Button>
        <Button
          variant="secondary"
          size="sm"
          onClick={onNext}
          disabled={page >= pageCount - 1}
          aria-label="Next page"
          iconRight={<ChevronRight className="w-4 h-4" />}
        >
          Next
        </Button>
      </div>
    </nav>
  );
}
