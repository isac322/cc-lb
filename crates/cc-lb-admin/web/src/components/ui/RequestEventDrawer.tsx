import { Dialog as BaseDialog } from '@base-ui/react/dialog';
import { Copy, X } from 'lucide-react';
import type { ReactNode } from 'react';
import { eventTime, type RequestEvent } from '../../lib/api';
import { fmtBytes, fmtMs, fmtN, fmtUsd, statusTone } from '../../lib/format';
import { useCopyButton } from '../../lib/useCopyButton';
import { LatencyTimeline } from './latency/LatencyTimeline';
import { Badge, Hint } from './primitives';
import { RelativeTime } from './RelativeTime';

const DASH = '—';

export function RequestEventDrawer({
  event,
  principalName,
  onClose,
}: {
  event: RequestEvent | null;
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
  event: RequestEvent;
  principalName: string | null;
  onClose: () => void;
}) {
  const { copy } = useCopyButton();

  const hasAnyToken =
    event.input_tokens != null ||
    event.output_tokens != null ||
    event.cache_creation_input_tokens != null ||
    event.cache_read_input_tokens != null ||
    event.body_bytes != null;

  const totalTokens = (event.input_tokens ?? 0) + (event.output_tokens ?? 0);

  const hasCacheCreationSplit =
    event.cache_creation_input_tokens_5m != null ||
    event.cache_creation_input_tokens_1h != null;

  const hasCostBreakdown =
    event.cost_input_micros != null ||
    event.cost_output_micros != null ||
    event.cost_cache_creation_5m_micros != null ||
    event.cost_cache_creation_1h_micros != null ||
    event.cost_cache_read_micros != null;

  const principalLabel = principalName ?? event.principal_id ?? DASH;

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
            label="Model"
            value={<span className="font-mono">{event.model ?? DASH}</span>}
          />
          <KvRow
            label="Status"
            value={
              <Badge tone={statusTone(event.status)}>{event.status}</Badge>
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
        </DetailSection>

        {hasAnyToken ? (
          <DetailSection title="Tokens">
            <KvRow
              label="Input"
              value={<MonoNum>{fmtN(event.input_tokens)}</MonoNum>}
            />
            <KvRow
              label="Output"
              value={<MonoNum>{fmtN(event.output_tokens)}</MonoNum>}
            />
            {hasCacheCreationSplit ? (
              <>
                <KvRow
                  label="Cache create 5m"
                  value={
                    <MonoNum>
                      {fmtN(event.cache_creation_input_tokens_5m ?? 0)}
                    </MonoNum>
                  }
                />
                <KvRow
                  label="Cache create 1h"
                  value={
                    <MonoNum>
                      {fmtN(event.cache_creation_input_tokens_1h ?? 0)}
                    </MonoNum>
                  }
                />
              </>
            ) : (
              <KvRow
                label="Cache creation"
                value={
                  <MonoNum>{fmtN(event.cache_creation_input_tokens)}</MonoNum>
                }
              />
            )}
            <KvRow
              label="Cache read"
              value={<MonoNum>{fmtN(event.cache_read_input_tokens)}</MonoNum>}
            />
            <KvRow
              label="Total (in+out)"
              value={<MonoNum>{fmtN(totalTokens)}</MonoNum>}
            />
            <KvRow
              label="Body bytes"
              value={<MonoNum>{fmtBytes(event.body_bytes)}</MonoNum>}
            />
          </DetailSection>
        ) : null}

        <DetailSection title="Cost">
          {hasCostBreakdown ? (
            <>
              <KvRow
                label="Input"
                value={<MonoNum>{fmtUsd(event.cost_input_micros)}</MonoNum>}
              />
              <KvRow
                label="Output"
                value={<MonoNum>{fmtUsd(event.cost_output_micros)}</MonoNum>}
              />
              <KvRow
                label="Cache create 5m"
                value={
                  <MonoNum>
                    {fmtUsd(event.cost_cache_creation_5m_micros)}
                  </MonoNum>
                }
              />
              <KvRow
                label="Cache create 1h"
                value={
                  <MonoNum>
                    {fmtUsd(event.cost_cache_creation_1h_micros)}
                  </MonoNum>
                }
              />
              <KvRow
                label="Cache read"
                value={
                  <MonoNum>{fmtUsd(event.cost_cache_read_micros)}</MonoNum>
                }
              />
            </>
          ) : null}
          <KvRow
            label="Total (USD)"
            value={<MonoNum>{fmtUsd(event.cost_usd_micros)}</MonoNum>}
          />
        </DetailSection>

        <DetailSection title="Latency">
          <div className="flex items-center justify-between text-[11px] mb-2">
            <span className="text-text-faint">Total</span>
            <MonoNum>{fmtMs(event.duration_ms)}</MonoNum>
          </div>
          <LatencyTimeline event={event} />
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
      <div className="space-y-1.5">{children}</div>
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
