import { Copy } from 'lucide-react';
import { useRef } from 'react';
import { fmtBytes, fmtMs } from '../../lib/format';
import { useRequestEventDetail } from '../../lib/queries';
import type { RequestEventWithPhase } from '../../lib/RequestEventTypes';
import { useCopyButton } from '../../lib/useCopyButton';
import { LatencyTimeline } from './latency/LatencyTimeline';
import { Badge, Drawer, Skeleton } from './primitives';
import {
  DetailSection,
  KvRow,
  RequestEventIdentity,
} from './RequestEventIdentity';
import { CostBreakdown, TokenBreakdown } from './usage/UsageBreakdown';

const DASH = '—';

export function RequestEventDrawer({
  event,
  principalName,
  onClose,
}: {
  event: RequestEventWithPhase | null;
  principalName: string | null;
  onClose: () => void;
}) {
  return (
    <Drawer
      open={!!event}
      onOpenChange={(open) => {
        if (!open) onClose();
      }}
      title="Request detail"
      width="lg"
    >
      {event ? (
        <RequestDetail event={event} principalName={principalName} />
      ) : null}
    </Drawer>
  );
}

function RequestDetail({
  event,
  principalName,
}: {
  event: RequestEventWithPhase;
  principalName: string | null;
}) {
  const { copy } = useCopyButton();
  const isPartial = event._phase === 'partial';
  const snapshotKey =
    event.event_id ??
    `${event.request_id}:${event.ts_ms ?? event.ts ?? 'unknown'}`;
  const lastKnownEventRef = useRef<{
    key: string;
    event: RequestEventWithPhase;
  } | null>(null);

  // Partial and slim final list payloads describe the same request
  // incrementally. Preserve fields already shown for that identity until the
  // detail payload arrives, but reset immediately when the drawer switches to
  // another request.
  const previousEvent =
    lastKnownEventRef.current?.key === snapshotKey
      ? lastKnownEventRef.current.event
      : null;
  const listEvent = previousEvent
    ? ({ ...previousEvent, ...event } as RequestEventWithPhase)
    : event;

  // The list view only carries the fields it displays (see RequestEventListItem
  // in cc-lb-storage-api); fetch the full payload here, on the single-item
  // detail path, so latency/body/error diagnostics render for finalized rows
  // regardless of whether this row came from a slim list page or an
  // already-complete live-tail update.
  const detailId = isPartial
    ? null
    : (event.event_id ?? (event.request_id || null));
  const detail = useRequestEventDetail(detailId);
  const merged: RequestEventWithPhase = detail.data
    ? ({
        ...listEvent,
        ...detail.data,
        _phase: 'final',
      } as RequestEventWithPhase)
    : listEvent;
  lastKnownEventRef.current = { key: snapshotKey, event: merged };

  const isDetailPending = Boolean(detailId) && detail.isPending;
  const hasLatencyTimelineData = isPartial
    ? (merged.elapsed_ms ?? 0) > 0
    : merged._phase === 'final' && merged.duration_ms > 0;
  const hasUpstreamFailure = Boolean(
    merged.upstream_error_type || merged.upstream_error_message,
  );

  const hasAnyToken =
    (merged.input_tokens ?? 0) > 0 ||
    (merged.output_tokens ?? 0) > 0 ||
    (merged.cache_creation_input_tokens ?? 0) > 0 ||
    (merged.cache_read_input_tokens ?? 0) > 0;

  const principalLabel = principalName ?? merged.principal_id ?? DASH;

  return (
    <>
      <div className="px-4 py-3 border-b border-row min-w-0">
        <div className="text-body text-text truncate">
          {principalLabel} → {merged.upstream_name ?? merged.upstream ?? DASH}
        </div>
        <div className="mt-1 flex items-center gap-1.5 flex-wrap min-w-0">
          <span
            className="font-mono text-data text-text-faint break-all min-w-0"
            title={merged.request_id}
          >
            {merged.request_id}
          </span>
          <button
            type="button"
            aria-label="Copy request id"
            className="shrink-0 -m-1.5 p-1.5 rounded-sm text-text-faint hover:text-text focus-visible:outline-2 focus-visible:outline-accent"
            onClick={() => copy(merged.request_id, 'Request ID')}
          >
            <Copy className="w-3 h-3" />
          </button>
          {isPartial && (
            <Badge tone="neutral" className="animate-pulse shrink-0">
              Live
            </Badge>
          )}
        </div>
      </div>
      <div className="overflow-x-hidden p-4 pb-8 space-y-6 text-body-sm min-w-0">
        <RequestEventIdentity
          event={merged}
          principalLabel={principalLabel}
          isPartial={isPartial}
          isDetailPending={isDetailPending}
        />

        {merged._phase === 'final' && hasUpstreamFailure ? (
          <DetailSection title="Upstream failure">
            <div className="rounded-sm bg-danger/8 p-3 space-y-2 min-w-0">
              {merged.upstream_error_type ? (
                <div className="font-mono text-data text-danger-text break-all min-w-0">
                  {merged.upstream_error_type}
                </div>
              ) : null}
              {merged.upstream_error_message ? (
                <div className="text-text-muted whitespace-pre-wrap break-words select-text min-w-0">
                  {merged.upstream_error_message}
                </div>
              ) : null}
            </div>
          </DetailSection>
        ) : null}

        {hasAnyToken ? (
          <div className="grid grid-cols-1 min-[420px]:grid-cols-2 gap-x-6 gap-y-6 min-w-0">
            <DetailSection title="Tokens">
              <TokenBreakdown event={merged} />
            </DetailSection>
            <DetailSection title={isPartial ? 'Estimated cost' : 'Cost'}>
              <CostBreakdown event={merged} />
            </DetailSection>
          </div>
        ) : null}

        <DetailSection title="Latency">
          <div className="space-y-1.5 mb-3">
            <KvRow
              label="Total"
              value={
                <span className="tabular-nums">
                  {isPartial
                    ? merged.elapsed_ms != null
                      ? fmtMs(merged.elapsed_ms)
                      : DASH
                    : merged._phase === 'final'
                      ? fmtMs(merged.duration_ms)
                      : DASH}
                </span>
              }
            />
            {merged._phase === 'final' &&
            (merged.body_bytes != null || isDetailPending) ? (
              <KvRow
                label="Body bytes"
                value={
                  merged.body_bytes != null ? (
                    <span className="tabular-nums">
                      {fmtBytes(merged.body_bytes)}
                    </span>
                  ) : (
                    <Skeleton className="ml-auto h-3 w-20" />
                  )
                }
              />
            ) : null}
          </div>
          <LatencyTimeline
            event={merged}
            isPartial={isPartial}
            isLoading={isDetailPending && !hasLatencyTimelineData}
          />
        </DetailSection>
      </div>
    </>
  );
}
