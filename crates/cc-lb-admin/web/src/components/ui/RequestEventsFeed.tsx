import type { ComponentProps, RefObject } from 'react';
import type { RequestEventsFeedState } from '../../lib/useRequestEventsFeed';
import { LiveTailFailureBanner } from '../LiveTailFailureBanner';
import { LogsPagination } from './LogsPagination';
import { Notice } from './primitives';
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
  readonly showStatus?: boolean;
  readonly showLiveFailureBanner?: boolean;
  readonly showPagination?: boolean;
  readonly onPageChange?: (page: number) => void;
  readonly tableContainerRef?: RefObject<HTMLDivElement | null>;
  readonly tableContainerClassName?: string;
}

const STATUS_TEXT_CLASS = {
  neutral: 'text-text-muted',
  ok: 'text-success-text',
  warn: 'text-warn-text',
  danger: 'text-danger-text',
} as const;

export function RequestEventsFeed({
  feed,
  showStatus = true,
  showLiveFailureBanner = true,
  showPagination = true,
  onPageChange,
  tableContainerRef,
  tableContainerClassName = 'flex-1 overflow-auto min-h-0 scroll-mt-16',
  ...tableProps
}: RequestEventsFeedProps) {
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
      {showStatus && (
        <div
          className="flex shrink-0 items-center gap-2 px-4 py-2 text-caption"
          data-feed-status={feed.tailStatus}
          role="status"
          aria-live="polite"
        >
          <span
            aria-hidden="true"
            className={`size-1.5 rounded-full bg-current ${STATUS_TEXT_CLASS[feed.statusColor]}`}
          />
          <span className={STATUS_TEXT_CLASS[feed.statusColor]}>
            {feed.statusLabel}
          </span>
        </div>
      )}
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
      <div ref={tableContainerRef} className={tableContainerClassName}>
        <RequestEventsTable
          {...tableProps}
          events={feed.pageRows}
          loading={feed.loading}
          liveFlashIds={feed.liveFlashIds}
        />
      </div>
      {showPagination && (
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
