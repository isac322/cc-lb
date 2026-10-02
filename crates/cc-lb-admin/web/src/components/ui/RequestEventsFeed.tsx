import {
  type ComponentProps,
  type RefObject,
  useCallback,
  useEffect,
  useRef,
} from 'react';
import type { RequestEventsFeedState } from '../../lib/useRequestEventsFeed';
import { LiveTailFailureBanner } from '../LiveTailFailureBanner';
import { LogsPagination } from './LogsPagination';
import { Notice, StatusBadge } from './primitives';
import { RequestEventsTable } from './RequestEventsTable';

export type RequestEventsFeedTableProps = Omit<
  ComponentProps<typeof RequestEventsTable>,
  | 'events'
  | 'loading'
  | 'liveFlashIds'
  | 'sentinelRef'
  | 'loadingMore'
  | 'hasMore'
>;

export interface RequestEventsFeedProps extends RequestEventsFeedTableProps {
  readonly feed: RequestEventsFeedState;
  readonly showLiveFailureBanner?: boolean;
  /**
   * Show page controls. Only `paged` feeds have pages, so this defaults to on
   * for them and is ignored for `infinite` and `preview` feeds.
   */
  readonly showPagination?: boolean;
  readonly onPageChange?: (page: number) => void;
  /**
   * Scroll container around the table. `infinite` feeds load more when the
   * end of the table scrolls into view inside it, so give it a bounded height
   * and overflow for an in-place scroll slot.
   */
  readonly tableContainerRef?: RefObject<HTMLDivElement | null>;
  readonly tableContainerClassName?: string;
}

/**
 * The feed's tail state as a `StatusBadge` with a polite live region, for the
 * owning page to place in its own section heading or toolbar. The feed table
 * never renders it itself.
 */
export function FeedLiveStatus({ feed }: { feed: RequestEventsFeedState }) {
  return (
    <span
      role="status"
      aria-live="polite"
      data-feed-status={feed.tailStatus}
      className="inline-flex items-center"
    >
      <StatusBadge tone={feed.statusColor} label={feed.statusLabel} />
    </span>
  );
}

/** Start loading a little before the end of the table is visible. */
const LOAD_MORE_MARGIN_PX = 160;

export function RequestEventsFeed({
  feed,
  showLiveFailureBanner = true,
  showPagination = true,
  onPageChange,
  tableContainerRef,
  tableContainerClassName = 'flex-1 overflow-auto min-h-0 scroll-mt-16',
  ...tableProps
}: RequestEventsFeedProps) {
  const infinite = feed.mode === 'infinite';
  const containerRef = useRef<HTMLDivElement | null>(null);
  const sentinelRef = useRef<HTMLTableRowElement | null>(null);
  const setContainer = useCallback(
    (node: HTMLDivElement | null) => {
      containerRef.current = node;
      if (tableContainerRef) tableContainerRef.current = node;
    },
    [tableContainerRef],
  );

  const { hasMore, loadingNext, loadMore } = feed;
  const visibleRowCount = feed.pageRows.length;
  // The observer only reports transitions, so after each growth step check
  // again whether the sentinel is still in view and keep filling the slot.
  const loadMoreIfSentinelVisible = useCallback(() => {
    const sentinel = sentinelRef.current;
    const container = containerRef.current;
    if (!infinite || !hasMore || loadingNext || !sentinel || !container) {
      return;
    }
    const slot = container.getBoundingClientRect();
    const end = sentinel.getBoundingClientRect();
    if (
      end.top <= slot.bottom + LOAD_MORE_MARGIN_PX &&
      end.bottom >= slot.top - LOAD_MORE_MARGIN_PX
    ) {
      void loadMore();
    }
  }, [hasMore, infinite, loadMore, loadingNext]);

  // biome-ignore lint/correctness/useExhaustiveDependencies: The sentinel row mounts and moves as rows render; re-observe when their count changes.
  useEffect(() => {
    const sentinel = sentinelRef.current;
    const container = containerRef.current;
    if (!infinite || !sentinel || !container) return;
    if (typeof IntersectionObserver === 'undefined') {
      loadMoreIfSentinelVisible();
      return;
    }
    const observer = new IntersectionObserver(
      (entries) => {
        if (entries.some((entry) => entry.isIntersecting)) {
          loadMoreIfSentinelVisible();
        }
      },
      { root: container, rootMargin: `${LOAD_MORE_MARGIN_PX}px 0px` },
    );
    observer.observe(sentinel);
    return () => observer.disconnect();
  }, [infinite, loadMoreIfSentinelVisible, visibleRowCount]);

  const movePrevious = () => {
    const nextPage = Math.max(0, feed.page - 1);
    onPageChange?.(nextPage);
    feed.previousPage();
  };
  const moveNext = () => {
    const nextPage = feed.page + 1;
    onPageChange?.(nextPage);
    void feed.nextPage();
  };

  return (
    <div className="flex min-h-0 flex-1 flex-col">
      {showLiveFailureBanner && feed.liveEnabled && (
        <LiveTailFailureBanner
          permanentFailure={feed.live.permanentFailure}
          permanentFailureSince={feed.live.permanentFailureSince}
          reconnectAttempts={feed.live.reconnectAttempts}
          onRetry={feed.live.forceReconnect}
        />
      )}
      {feed.error && !feed.loading && (
        <Notice
          tone="danger"
          title="Unable to load request history"
          className="mx-4 mb-3 shrink-0"
        >
          {feed.error.message}
        </Notice>
      )}
      <div ref={setContainer} className={tableContainerClassName}>
        <RequestEventsTable
          {...tableProps}
          events={feed.pageRows}
          loading={feed.loading}
          liveFlashIds={feed.liveFlashIds}
          sentinelRef={infinite ? sentinelRef : undefined}
          loadingMore={infinite && loadingNext}
          hasMore={infinite && hasMore}
        />
      </div>
      {showPagination && feed.mode === 'paged' && (
        <LogsPagination
          page={feed.page}
          pageCount={feed.pageCount}
          totalRows={feed.totalRows}
          pageSize={feed.pageSize}
          hasMore={feed.hasMore}
          loadingNext={feed.loadingNext}
          loading={feed.loading}
          onPrev={movePrevious}
          onNext={moveNext}
        />
      )}
    </div>
  );
}
