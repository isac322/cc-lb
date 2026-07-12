import { Dialog as BaseDialog } from '@base-ui/react/dialog';
import { Copy, X } from 'lucide-react';
import type { ReactNode } from 'react';
import { fmtBytes, fmtMs } from '../../lib/format';
import { useCopyButton } from '../../lib/useCopyButton';
import { LatencyTimeline } from './latency/LatencyTimeline';
import { Badge } from './primitives';
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

  const hasAnyToken =
    (event.input_tokens ?? 0) > 0 ||
    (event.output_tokens ?? 0) > 0 ||
    (event.cache_creation_input_tokens ?? 0) > 0 ||
    (event.cache_read_input_tokens ?? 0) > 0;

  const principalLabel = principalName ?? event.principal_id ?? DASH;
  const isPartial = event._phase === 'partial';

  return (
    <>
      <div className="p-4 border-b border-subtle flex items-start justify-between gap-3">
        <div className="min-w-0 flex-1">
          <div className="flex items-center gap-2 mb-1 flex-wrap min-w-0">
            <span
              className="font-mono text-xs text-text-faint break-all min-w-0"
              title={event.request_id}
            >
              {event.request_id}
            </span>
            <button
              type="button"
              aria-label="Copy request id"
              className="text-text-faint hover:text-text shrink-0"
              onClick={() => copy(event.request_id, 'Request ID')}
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
            {principalLabel} → {event.upstream_name ?? event.upstream ?? DASH}
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
          event={event}
          principalLabel={principalLabel}
          isPartial={isPartial}
        />

        {event._phase === 'final' &&
        (event.upstream_error_type || event.upstream_error_message) ? (
          <DetailSection title="Upstream Failure">
            <div className="bg-overlay-2 border border-subtle rounded p-3 space-y-2 min-w-0">
              {event.upstream_error_type ? (
                <div className="font-mono text-danger break-all min-w-0">
                  {event.upstream_error_type}
                </div>
              ) : null}
              {event.upstream_error_message ? (
                <div className="text-text-muted whitespace-pre-wrap break-words select-text min-w-0">
                  {event.upstream_error_message}
                </div>
              ) : null}
            </div>
          </DetailSection>
        ) : null}

        {hasAnyToken ? (
          <div className="grid grid-cols-1 min-[420px]:grid-cols-2 gap-3 min-w-0">
            <DetailSection title="Tokens">
              <TokenPie event={event} control={usageControl} />
            </DetailSection>
            <DetailSection title={isPartial ? 'Estimated Cost' : 'Cost'}>
              <CostPie event={event} control={usageControl} />
            </DetailSection>
          </div>
        ) : null}

        {event._phase === 'final' && event.body_bytes != null ? (
          <KvRow
            label="Body bytes"
            value={<MonoNum>{fmtBytes(event.body_bytes)}</MonoNum>}
          />
        ) : null}

        <DetailSection title="Latency">
          <div className="flex items-center justify-between text-[11px] mb-2">
            <span className="text-text-faint">Total</span>
            <MonoNum>
              {isPartial
                ? event.elapsed_ms != null
                  ? fmtMs(event.elapsed_ms)
                  : DASH
                : event._phase === 'final'
                  ? fmtMs(event.duration_ms)
                  : DASH}
            </MonoNum>
          </div>
          <LatencyTimeline event={event} isPartial={isPartial} />
        </DetailSection>
      </div>
    </>
  );
}

function MonoNum({ children }: { children: ReactNode }) {
  return <span className="font-mono tabular-nums">{children}</span>;
}
