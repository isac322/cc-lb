import { ChevronLeft, ChevronRight } from 'lucide-react';
import { Button, Skeleton, Spinner } from './primitives';

export interface LogsPaginationProps {
  readonly page: number;
  readonly pageCount: number;
  readonly totalRows: number;
  readonly pageSize: number;
  readonly hasMore: boolean;
  readonly loadingNext?: boolean;
  readonly onPrev: () => void;
  readonly onNext: () => void;
  readonly loading?: boolean;
}

export function LogsPagination({
  page,
  pageCount,
  totalRows,
  pageSize,
  hasMore,
  loadingNext = false,
  onPrev,
  onNext,
  loading = false,
}: LogsPaginationProps) {
  const startRow = totalRows === 0 ? 0 : page * pageSize + 1;
  const endRow = Math.min((page + 1) * pageSize, totalRows);
  const totalLabel = hasMore ? `${totalRows}+` : String(totalRows);
  const showNextPending = !loading && loadingNext;
  return (
    <nav
      aria-label="Log pagination"
      aria-busy={loading || showNextPending}
      className="h-12 shrink-0 flex items-center justify-between gap-4 px-4 border-t border-row"
    >
      <div
        aria-live="polite"
        className="text-body-sm text-text-muted tabular-nums flex items-center gap-4"
      >
        {loading ? (
          <div className="w-28 sm:w-36">
            <Skeleton className="h-3" />
          </div>
        ) : (
          <span>
            Showing {startRow}–{endRow} of {totalLabel}
          </span>
        )}
      </div>
      <div className="flex items-center gap-2">
        <Button
          variant="secondary"
          size="sm"
          onClick={onPrev}
          disabled={loading || page <= 0}
          aria-label="Previous page"
          iconLeft={<ChevronLeft />}
        >
          Prev
        </Button>
        <Button
          variant="secondary"
          size="sm"
          onClick={onNext}
          disabled={
            loading || showNextPending || (page >= pageCount - 1 && !hasMore)
          }
          aria-label={showNextPending ? 'Loading next page' : 'Next page'}
          aria-busy={showNextPending}
          iconRight={
            showNextPending ? (
              <span aria-hidden="true" className="inline-flex h-4 w-4">
                <Spinner className="size-3.5 text-text-muted" />
              </span>
            ) : (
              <ChevronRight />
            )
          }
        >
          Next
        </Button>
      </div>
    </nav>
  );
}
