import { ChevronRight, Ticket } from 'lucide-react';
import { useState } from 'react';
import { ApiError } from '../../lib/api';
import {
  type LimitResetHistoryItem,
  useLimitResetHistory,
} from '../../lib/limitResets';
import { Button, Notice, Skeleton } from '../ui/primitives';
import { RelativeTime } from '../ui/RelativeTime';

function historyType(item: LimitResetHistoryItem): string {
  if (item.coupon_type === 'full' && item.gives_back_weekly === true)
    return 'Fable';
  if (item.coupon_type === 'weekly') return 'Weekly';
  if (item.coupon_type === 'codex_rate_limits') return 'Codex';
  return 'Reset';
}

function historyOutcome(item: LimitResetHistoryItem): string {
  switch (item.result) {
    case 'reset':
      return 'Coupon used';
    case 'already_used':
      return 'Already applied · no additional coupon used';
    case 'not_limited':
      return 'Not needed · no coupon used';
    case 'cooldown':
      return 'Cooldown · no coupon used';
    case 'ineligible':
      return 'Account ineligible · no coupon used';
    case 'unavailable':
      return 'Unavailable · no coupon used';
    case 'unknown':
      return 'Outcome unknown · may have used a coupon';
  }
}

/** Recorded cc-lb attempts only. Coupon inventory is not a redemption ledger. */
export function ResetHistoryPanel({
  upstreamId,
  open,
}: {
  upstreamId: string;
  open: boolean;
}) {
  const [limit, setLimit] = useState(100);
  const historyQ = useLimitResetHistory(upstreamId, open, limit);
  const data = historyQ.data;

  return (
    <section aria-label="Recorded reset attempts" className="p-4">
      <p className="mb-4 text-caption text-text-faint">
        Activity recorded by cc-lb
      </p>
      {historyQ.isPending && !data ? (
        <div
          role="status"
          aria-label="Loading reset activity"
          className="space-y-4"
        >
          {[0, 1, 2].map((row) => (
            <div key={row} className="space-y-2">
              <Skeleton className="h-4 w-28" />
              <Skeleton className="h-4 w-48" />
            </div>
          ))}
        </div>
      ) : historyQ.isError ? (
        <Notice
          tone="danger"
          title="Could not load reset activity"
          action={
            <Button
              size="sm"
              loading={historyQ.isFetching}
              onClick={() => historyQ.refetch()}
            >
              Retry
            </Button>
          }
        >
          {historyQ.error instanceof ApiError && historyQ.error.status === 403
            ? 'Your admin account does not have permission to read reset activity.'
            : historyQ.error instanceof ApiError &&
                historyQ.error.status === 404
              ? 'This server does not support recorded reset activity.'
              : historyQ.error instanceof Error
                ? historyQ.error.message
                : 'Recorded attempts could not be retrieved.'}
        </Notice>
      ) : data?.items.length === 0 ? (
        <p className="py-8 text-center text-body-sm text-text-muted">
          No reset attempts recorded in cc-lb.
        </p>
      ) : data ? (
        <>
          <ol className="divide-y divide-row">
            {data.items.map((item) => (
              <li key={item.id} className="min-w-0 py-4 first:pt-0">
                <div className="flex flex-wrap items-center justify-between gap-x-4 gap-y-1 text-body-sm">
                  <span
                    className={`inline-flex items-center gap-2 ${historyType(item) === 'Fable' ? 'text-series-fable' : 'text-text'}`}
                  >
                    <Ticket
                      className="size-4"
                      strokeWidth={1.75}
                      aria-hidden="true"
                    />
                    {historyType(item)}
                  </span>
                  <span className="text-caption text-text-faint">
                    <RelativeTime ts={item.recorded_at_unix_millis} />
                  </span>
                </div>
                <p className="mt-2 text-body-sm text-text">
                  {historyOutcome(item)}
                </p>
                {item.usage_refresh === 'failed' ? (
                  <p className="mt-1 text-body-sm text-warn-text">
                    Usage not refreshed.
                  </p>
                ) : null}
                {item.actor || item.request_id ? (
                  <details className="group mt-1 text-caption text-text-muted">
                    <summary className="flex min-h-11 cursor-pointer list-none items-center gap-1 md:min-h-8">
                      <ChevronRight
                        className="size-3 transition-transform group-open:rotate-90"
                        aria-hidden="true"
                      />
                      Details
                    </summary>
                    <dl className="space-y-2 pb-2">
                      {item.actor ? (
                        <div>
                          <dt className="text-text-faint">Requested by</dt>
                          <dd className="break-words">{item.actor}</dd>
                        </div>
                      ) : null}
                      {item.request_id ? (
                        <div>
                          <dt className="text-text-faint">Request reference</dt>
                          <dd className="break-all font-mono text-data">
                            {item.request_id}
                          </dd>
                        </div>
                      ) : null}
                    </dl>
                  </details>
                ) : null}
              </li>
            ))}
          </ol>
          {data.has_more ? (
            <div className="mt-4 border-t border-row pt-4">
              {limit < 1000 ? (
                <Button
                  loading={historyQ.isFetching}
                  onClick={() =>
                    setLimit((current) => Math.min(1000, current + 100))
                  }
                >
                  Load older activity
                </Button>
              ) : data.limit < 1000 ? (
                <p role="status" className="text-caption text-text-faint">
                  Loading older activity…
                </p>
              ) : (
                <p className="text-caption text-text-faint">
                  Showing the most recent 1,000 recorded attempts. Older
                  activity is not shown here.
                </p>
              )}
            </div>
          ) : null}
        </>
      ) : null}
    </section>
  );
}
