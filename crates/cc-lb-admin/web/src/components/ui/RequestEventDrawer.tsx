import { Dialog as BaseDialog } from '@base-ui/react/dialog';
import { Copy, X } from 'lucide-react';
import type { ReactNode } from 'react';
import { eventTime } from '../../lib/api';
import { fmtBytes, fmtMs, statusTone } from '../../lib/format';
import { useCopyButton } from '../../lib/useCopyButton';
import { LatencyTimeline } from './latency/LatencyTimeline';
import { Badge, Hint } from './primitives';
import { RelativeTime } from './RelativeTime';
import { SessionChip } from './RequestEventsTable';
import { CostPie } from './usage/CostPie';
import { useActiveSlice } from './usage/PieChart';
import { TokenPie } from './usage/TokenPie';

const DASH = '—';

import type { RequestEventWithPhase } from './RequestEventsTable';

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
        <BaseDialog.Popup className="fixed right-0 top-0 bottom-0 w-full max-w-lg bg-bg-sub border-l border-subtle z-50 flex flex-col outline-none transition-transform duration-200 ease-out data-[ending-style]:translate-x-full data-[starting-style]:translate-x-full">
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
    event.input_tokens != null ||
    event.output_tokens != null ||
    event.cache_creation_input_tokens != null ||
    event.cache_read_input_tokens != null ||
    event.body_bytes != null;

  const principalLabel = principalName ?? event.principal_id ?? DASH;
  const isPartial = event._phase === 'partial';

  return (
    <>
      <div className="p-4 border-b border-subtle flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="flex items-center gap-2 mb-1">
            <span
              className="font-mono text-xs text-text-faint truncate max-w-[280px]"
              title={event.request_id}
            >
              {event.request_id}
            </span>
            <button
              type="button"
              aria-label="Copy request id"
              className="text-text-faint hover:text-text"
              onClick={() => copy(event.request_id, 'Request ID')}
            >
              <Copy className="w-3 h-3" />
            </button>
            {isPartial && (
              <Badge tone="neutral" className="animate-pulse">
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
      <div className="flex-1 overflow-y-auto p-4 pb-8 space-y-5 text-xs">
        <DetailSection title="Identity">
          <div className="space-y-1.5">
            <KvRow
              label="Timestamp"
              value={<RelativeTime ts={eventTime(event)} />}
            />
            <KvRow
              label="Principal"
              value={
                <span className="flex items-center gap-2 justify-end">
                  <span
                    className="font-mono truncate max-w-[220px]"
                    title={event.principal_id ?? ''}
                  >
                    {principalLabel}
                  </span>
                  {event.principal_kind ? (
                    <Badge tone="mono">{event.principal_kind}</Badge>
                  ) : null}
                </span>
              }
            />
            <KvRow
              label="Key ID"
              value={
                event.key_id ? (
                  <span className="flex items-center gap-1 justify-end">
                    <Hint label={event.key_id}>
                      <span className="font-mono truncate max-w-[180px] cursor-help">
                        {truncateMid(event.key_id, 16)}
                      </span>
                    </Hint>
                    <button
                      type="button"
                      aria-label="Copy key id"
                      className="text-text-faint hover:text-text"
                      onClick={() => copy(event.key_id ?? '', 'Key ID')}
                    >
                      <Copy className="w-3 h-3" />
                    </button>
                  </span>
                ) : (
                  DASH
                )
              }
            />
            <KvRow
              label="Upstream"
              value={
                <span className="font-mono">
                  {event.upstream_name ?? DASH}
                  {event.upstream && event.upstream !== event.upstream_name ? (
                    <span className="text-text-faint ml-2">
                      ({event.upstream})
                    </span>
                  ) : null}
                </span>
              }
            />
            <KvRow
              label="Session"
              value={
                event.thread_id ? (
                  <span className="flex items-center gap-1 justify-end">
                    <SessionChip sessionId={event.thread_id} />
                    <button
                      type="button"
                      aria-label="Copy session id"
                      className="text-text-faint hover:text-text"
                      onClick={() => copy(event.thread_id ?? '', 'Session ID')}
                    >
                      <Copy className="w-3 h-3" />
                    </button>
                  </span>
                ) : (
                  DASH
                )
              }
            />
            <KvRow
              label="Model"
              value={<span className="font-mono">{event.model ?? DASH}</span>}
            />
            <KvRow
              label="Status"
              value={
                isPartial ? (
                  <span className="inline-flex items-center gap-1.5 text-text-faint">
                    <span className="w-3 h-3 border-2 border-text-faint border-t-transparent rounded-full animate-spin" />
                    In progress
                  </span>
                ) : (
                  <Badge tone={statusTone(event.status)}>{event.status}</Badge>
                )
              }
            />
            {event.error_code ? (
              <KvRow
                label="Error"
                value={
                  <span className="text-red-400 font-mono">
                    {event.error_code}
                  </span>
                }
              />
            ) : null}
          </div>
        </DetailSection>

        <div className="grid grid-cols-2 gap-3">
          {hasAnyToken ? (
            <DetailSection title="Tokens">
              <TokenPie event={event} control={usageControl} />
            </DetailSection>
          ) : null}
          <DetailSection title={isPartial ? 'Estimated Cost' : 'Cost'}>
            <CostPie event={event} control={usageControl} />
          </DetailSection>
        </div>

        {event.body_bytes != null ? (
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
                : fmtMs(event.duration_ms)}
            </MonoNum>
          </div>
          <LatencyTimeline event={event} isPartial={isPartial} />
        </DetailSection>
      </div>
    </>
  );
}

function DetailSection({
  title,
  children,
}: {
  title: string;
  children: ReactNode;
}) {
  return (
    <div>
      <div className="text-[10px] uppercase tracking-wider text-text-faint mb-2 border-b border-subtle pb-1">
        {title}
      </div>
      {children}
    </div>
  );
}

function KvRow({ label, value }: { label: string; value: ReactNode }) {
  return (
    <div className="flex items-center justify-between gap-3 min-h-[18px]">
      <span className="text-text-faint">{label}</span>
      <span className="text-right">{value}</span>
    </div>
  );
}

function MonoNum({ children }: { children: ReactNode }) {
  return <span className="font-mono tabular-nums">{children}</span>;
}

function truncateMid(s: string, max: number): string {
  if (s.length <= max) return s;
  const half = Math.floor((max - 1) / 2);
  return `${s.slice(0, half)}…${s.slice(-half)}`;
}
