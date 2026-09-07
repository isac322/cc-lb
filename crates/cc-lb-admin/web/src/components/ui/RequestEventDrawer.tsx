import { Dialog as BaseDialog } from '@base-ui/react/dialog';
import { Copy, X } from 'lucide-react';
import { type ReactNode, useRef } from 'react';
import { fmtBytes, fmtMs } from '../../lib/format';
import { useRequestEventDetail } from '../../lib/queries';
import { useCopyButton } from '../../lib/useCopyButton';
import { LatencyTimeline } from './latency/LatencyTimeline';
import { Badge, Skeleton } from './primitives';
import { CostPie } from './usage/CostPie';
import { useActiveSlice } from './usage/PieChart';
import { TokenPie } from './usage/TokenPie';

const DASH = '—';

import type { RequestEventWithPhase } from '../../lib/RequestEventTypes';
import {
  DetailSection,
  KvRow,
  RequestEventIdentity,
} from './RequestEventIdentity';

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
    <BaseDialog.Root
      onOpenChange={(open) => {
        if (!open) onClose();
      }}
      open={!!event}
    >
      <BaseDialog.Portal>
        <BaseDialog.Backdrop className="fixed inset-0 z-40 bg-drawer-backdrop transition-opacity duration-200 ease-out data-[ending-style]:opacity-0 data-[starting-style]:opacity-0" />
        <BaseDialog.Popup className="fixed right-0 top-0 bottom-0 w-full max-w-lg bg-bg-sub border-l border-subtle z-50 flex flex-col outline-none transition-transform duration-200 ease-out data-[ending-style]:translate-x-full data-[starting-style]:translate-x-full overflow-x-hidden">
          <BaseDialog.Title className="sr-only">
            Request detail
          </BaseDialog.Title>
          <BaseDialog.Description className="sr-only">
            Detail view of a single request event
          </BaseDialog.Description>
          {event ? (
            <RequestDetail
              event={event}
              onClose={onClose}
              principalName={principalName}
            />
          ) : null}
        </BaseDialog.Popup>
      </BaseDialog.Portal>
    </BaseDialog.Root>
  );
}

function RequestDetail({
  event,
  principalName,
  onClose,
}: {
  event: RequestEventWithPhase;
  principalName: string | null;
  onClose: () => void;
}) {
  const { copy } = useCopyButton();
  const usageControl = useActiveSlice();
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
      <div className="p-4 border-b border-subtle flex items-start justify-between gap-3">
        <div className="min-w-0 flex-1">
          <div className="flex items-center gap-2 mb-1 flex-wrap min-w-0">
            <span
              className="font-mono text-xs text-text-faint break-all min-w-0"
              title={merged.request_id}
            >
              {merged.request_id}
            </span>
            <button
              type="button"
              aria-label="Copy request id"
              className="text-text-faint hover:text-text shrink-0"
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
          <div className="text-sm truncate">
            {principalLabel} → {merged.upstream_name ?? merged.upstream ?? DASH}
          </div>
        </div>
        <button
          type="button"
          aria-label="Close"
          onClick={onClose}
          className="text-text-muted hover:text-text shrink-0"
        >
          <X className="w-4 h-4" />
        </button>
      </div>
      <div className="flex-1 overflow-y-auto overflow-x-hidden p-4 pb-8 space-y-5 text-xs min-w-0">
        <RequestEventIdentity
          event={merged}
          principalLabel={principalLabel}
          isPartial={isPartial}
          isDetailPending={isDetailPending}
        />

        {merged._phase === 'final' && hasUpstreamFailure ? (
          <DetailSection title="Upstream Failure">
            <div className="bg-overlay-2 border border-subtle rounded p-3 space-y-2 min-w-0">
              {merged.upstream_error_type ? (
                <div className="font-mono text-danger break-all min-w-0">
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
          <div className="grid grid-cols-1 min-[420px]:grid-cols-2 gap-3 min-w-0">
            <DetailSection title="Tokens">
              <TokenPie event={merged} control={usageControl} />
            </DetailSection>
            <DetailSection title={isPartial ? 'Estimated Cost' : 'Cost'}>
              <CostPie event={merged} control={usageControl} />
            </DetailSection>
          </div>
        ) : null}

        {merged._phase === 'final' &&
        (merged.body_bytes != null || isDetailPending) ? (
          <KvRow
            label="Body bytes"
            value={
              merged.body_bytes != null ? (
                <MonoNum>{fmtBytes(merged.body_bytes)}</MonoNum>
              ) : (
                <Skeleton className="ml-auto h-3 w-20" />
              )
            }
          />
        ) : null}

        <DetailSection title="Latency">
          <div className="flex items-center justify-between text-[11px] mb-2">
            <span className="text-text-faint">Total</span>
            <MonoNum>
              {isPartial
                ? merged.elapsed_ms != null
                  ? fmtMs(merged.elapsed_ms)
                  : DASH
                : merged._phase === 'final'
                  ? fmtMs(merged.duration_ms)
                  : DASH}
            </MonoNum>
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

function MonoNum({ children }: { children: ReactNode }) {
  return <span className="font-mono tabular-nums">{children}</span>;
}
