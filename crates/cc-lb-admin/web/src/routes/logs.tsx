import { Dialog as BaseDialog } from '@base-ui/react/dialog';
import { Toggle as BaseToggle } from '@base-ui/react/toggle';
import { createFileRoute, useNavigate } from '@tanstack/react-router';
import { Copy, Download, RefreshCw, X, Zap } from 'lucide-react';
import { useEffect, useMemo, useRef, useState } from 'react';
import { z } from 'zod';
import {
  Badge,
  Button,
  Card,
  cx,
  Field,
  FullPage,
  Hint,
  INPUT_CLASS,
  Section,
} from '../components/ui/primitives';
import { RelativeTime } from '../components/ui/RelativeTime';
import { RequestEventsTable } from '../components/ui/RequestEventsTable';
import {
  ApiError,
  eventTime,
  type RequestEvent,
  streamEventsFetch,
} from '../lib/api';
import { fmtBytes, fmtMs, fmtN, fmtUsd, statusTone } from '../lib/format';
import {
  usePrincipalNameMap,
  useRecentEventsInfinite,
  useUpstreamNameMap,
  useUpstreams,
} from '../lib/queries';
import { useCopyButton } from '../lib/useCopyButton';

import { computeLatencySections } from './computeLatencySections';

const logsSearchSchema = z.object({
  principal_id: z.string().optional(),
  upstream: z.string().optional(),
  model: z.string().optional(),
  status: z.string().optional(),
  since: z.string().optional(),
  until: z.string().optional(),
});

export const Route = createFileRoute('/logs')({
  validateSearch: logsSearchSchema,
  component: LogsPage,
});

const DASH = '—';

function LogsPage() {
  const filters = Route.useSearch();
  const navigate = useNavigate({ from: Route.fullPath });
  const upstreams = useUpstreams();
  const recent = useRecentEventsInfinite(filters);
  const principalNameMap = usePrincipalNameMap();
  const upstreamNameMap = useUpstreamNameMap();

  const [tailing, setTailing] = useState(true);
  const [liveRows, setLiveRows] = useState<RequestEvent[]>([]);
  const [selected, setSelected] = useState<RequestEvent | null>(null);
  const [tailStatus, setTailStatus] = useState<
    'idle' | 'connecting' | 'live' | 'reconnecting' | 'down'
  >('idle');
  const scrollContainerRef = useRef<HTMLDivElement>(null);
  const sentinelRef = useRef<HTMLTableRowElement>(null);

  useEffect(() => {
    const el = sentinelRef.current;
    if (!el || !recent.hasNextPage || recent.isFetchingNextPage) return;
    const obs = new IntersectionObserver(
      (entries) =>
        entries.forEach((e) => {
          if (e.isIntersecting) recent.fetchNextPage();
        }),
      { root: scrollContainerRef.current, threshold: 0.1 },
    );
    obs.observe(el);
    return () => obs.disconnect();
  }, [recent.hasNextPage, recent.isFetchingNextPage, recent.fetchNextPage]);

  useEffect(() => {
    if (!tailing) {
      setTailStatus('idle');
      return;
    }
    setTailStatus('connecting');
    const close = streamEventsFetch('/admin/events/stream', {
      onConnect: () => setTailStatus('live'),
      onEvent: (data) => {
        try {
          const parsed = JSON.parse(data) as RequestEvent;
          setLiveRows((prev) => [parsed, ...prev].slice(0, 500));
        } catch {
          /* ignore: malformed SSE chunk */
        }
      },
      onError: (err) => {
        if (err instanceof ApiError && err.status === 401) {
          setTailStatus('down');
          setTailing(false);
          return;
        }
        setTailStatus('reconnecting');
      },
    });
    return () => close();
  }, [tailing]);

  const rows = useMemo(() => {
    // While filters change, `recent.data` still holds the previous filter's
    // pages (queryClient default `placeholderData: keepPreviousData`). Treat
    // that as empty so we don't show the old filter's rows under the new
    // filter's subtitle.
    const historical = recent.isPlaceholderData
      ? []
      : (recent.data?.pages.flatMap((p) => p.events) ?? []);
    const seen = new Set<string>();
    const out: RequestEvent[] = [];
    for (const ev of liveRows) {
      if (!seen.has(ev.request_id)) {
        seen.add(ev.request_id);
        out.push(ev);
      }
    }
    for (const ev of historical) {
      if (!seen.has(ev.request_id)) {
        seen.add(ev.request_id);
        out.push(ev);
      }
    }
    return out.sort(
      (a, b) => (eventTime(b)?.getTime() ?? 0) - (eventTime(a)?.getTime() ?? 0),
    );
  }, [liveRows, recent.data, recent.isPlaceholderData]);

  const recentLiveIds = useMemo(
    () => new Set(liveRows.slice(0, 20).map((e) => e.request_id)),
    [liveRows],
  );

  const setFilter = (key: keyof typeof filters, value: string) => {
    navigate({ search: { ...filters, [key]: value || undefined } });
  };

  const downloadJson = () => {
    const blob = new Blob([JSON.stringify(rows, null, 2)], {
      type: 'application/json',
    });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `cc-lb-events-${new Date().toISOString().slice(0, 16)}.json`;
    a.click();
    URL.revokeObjectURL(url);
  };

  return (
    <FullPage>
      <Section
        title="Live Logs"
        className="flex-1 min-h-0"
        subtitle={
          <span className="flex items-center gap-2">
            <span>
              {rows.length} requests — {tailing ? 'live tailing' : 'paged'}
            </span>
            {tailing ? (
              <span className="inline-flex items-center gap-1 px-1.5 py-0.5 text-[10px] uppercase tracking-wider border border-subtle rounded-sm">
                <span
                  className={cx(
                    'status-dot',
                    tailStatus === 'live'
                      ? 'live'
                      : tailStatus === 'down'
                        ? 'danger'
                        : 'neutral',
                  )}
                />
                <span>{tailStatus}</span>
              </span>
            ) : null}
          </span>
        }
        action={
          <div className="flex items-center gap-2 flex-wrap justify-end">
            <BaseToggle
              aria-label="Live tail logs"
              className="inline-flex items-center justify-center rounded-sm font-medium transition-colors select-none disabled:cursor-not-allowed disabled:opacity-50 focus-visible:outline focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-2 h-7 px-2.5 text-xs gap-1.5 bg-[color:var(--color-panel-strong)] border border-[color:var(--color-border)] text-[color:var(--color-text)] hover:bg-[color:var(--color-hover-bg)] data-[pressed]:bg-[color:var(--color-accent-dim)] data-[pressed]:text-[color:var(--color-accent)] data-[pressed]:border-[color:var(--color-accent)]"
              onPressedChange={setTailing}
              pressed={tailing}
            >
              <Zap className="w-3 h-3" />
              {tailing ? 'Stop tail' : 'Live tail'}
            </BaseToggle>
            <Button
              size="sm"
              iconLeft={<RefreshCw className="w-3 h-3" />}
              onClick={() => recent.refetch()}
            >
              Refresh
            </Button>
            <Button
              size="sm"
              iconLeft={<Download className="w-3 h-3" />}
              onClick={downloadJson}
            >
              Export
            </Button>
          </div>
        }
      >
        <Card className="flex-1 flex flex-col min-h-0">
          <div className="p-3 border-b border-subtle flex flex-wrap gap-3 items-end shrink-0">
            <Field label="Principal">
              <select
                className={`${INPUT_CLASS} w-44`}
                value={filters.principal_id ?? ''}
                onChange={(e) => setFilter('principal_id', e.target.value)}
              >
                <option value="">All principals</option>
                {Array.from(principalNameMap.entries()).map(([id, name]) => (
                  <option key={id} value={id}>
                    {name}
                  </option>
                ))}
              </select>
            </Field>
            <Field label="Upstream">
              <select
                className={`${INPUT_CLASS} w-44`}
                value={filters.upstream ?? ''}
                onChange={(e) => setFilter('upstream', e.target.value)}
              >
                <option value="">All upstreams</option>
                {upstreams.data?.upstreams.map((u) => (
                  <option key={u.id} value={u.name}>
                    {u.name}
                  </option>
                ))}
              </select>
            </Field>
            <Field label="Model">
              <input
                className={`${INPUT_CLASS} w-44 font-mono`}
                value={filters.model ?? ''}
                onChange={(e) => setFilter('model', e.target.value)}
                placeholder="claude-*"
              />
            </Field>
            <Field label="Status">
              <select
                className={`${INPUT_CLASS} w-32`}
                value={filters.status ?? ''}
                onChange={(e) => setFilter('status', e.target.value)}
              >
                <option value="">All statuses</option>
                <option value="200">2xx</option>
                <option value="429">429</option>
                <option value="500">5xx</option>
              </select>
            </Field>
            <Field label="Since">
              <input
                type="datetime-local"
                lang="en"
                className={`${INPUT_CLASS} w-48`}
                value={filters.since ?? ''}
                onChange={(e) => setFilter('since', e.target.value)}
              />
            </Field>
            <Field label="Until">
              <input
                type="datetime-local"
                lang="en"
                className={`${INPUT_CLASS} w-48`}
                value={filters.until ?? ''}
                onChange={(e) => setFilter('until', e.target.value)}
              />
            </Field>
            {filters.principal_id ||
            filters.upstream ||
            filters.model ||
            filters.status ||
            filters.since ||
            filters.until ? (
              <Button
                size="sm"
                iconLeft={<X className="w-3 h-3" />}
                onClick={() => navigate({ search: {} })}
              >
                Clear
              </Button>
            ) : null}
          </div>

          <div
            ref={scrollContainerRef}
            className="flex-1 overflow-auto min-h-0"
          >
            <RequestEventsTable
              events={rows}
              principalNameMap={principalNameMap}
              upstreamNameMap={upstreamNameMap}
              loading={
                (recent.isPending || recent.isPlaceholderData) &&
                liveRows.length === 0
              }
              liveFlashIds={tailing ? recentLiveIds : undefined}
              onRowClick={setSelected}
              columns={{ cost: true, tokens: true }}
              sentinelRef={sentinelRef}
              loadingMore={recent.isFetchingNextPage}
              hasMore={recent.hasNextPage}
              minWidthClass="min-w-[980px]"
              emptyTitle="No requests"
              emptyDescription="Adjust filters or enable live tail."
            />
          </div>
        </Card>
      </Section>

      <BaseDialog.Root
        onOpenChange={(open) => {
          if (!open) setSelected(null);
        }}
        open={!!selected}
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
            {selected ? (
              <RequestDetail
                event={selected}
                onClose={() => setSelected(null)}
                principalName={
                  selected.principal_id
                    ? (principalNameMap.get(selected.principal_id) ?? null)
                    : null
                }
              />
            ) : null}
          </BaseDialog.Popup>
        </BaseDialog.Portal>
      </BaseDialog.Root>
    </FullPage>
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

  const isStream =
    event.stream_total_ms != null ||
    (event.sse_event_count ?? 0) > 0 ||
    event.stream_first_content_delta_ms != null;

  const { sections, unaccounted } = computeLatencySections(event);

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
                    onClick={() => copy(event.key_id!, 'Key ID')}
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

        <DetailSection title="Latency breakdown">
          <KvRow
            label="Total"
            value={<MonoNum>{fmtMs(event.duration_ms)}</MonoNum>}
          />
          {sections.map((s) => (
            <div key={s.title} className="mt-2">
              <div className="flex items-center gap-2 text-[11px] uppercase tracking-wider text-text-faint mb-1">
                <span>{s.title}</span>
                {s.warmPool ? (
                  <span className="px-1.5 py-0.5 rounded-sm bg-emerald-500/20 text-emerald-300 text-[10px]">
                    Warm pool
                  </span>
                ) : null}
              </div>
              {s.rows.map((r) => (
                <KvRow
                  key={r.label}
                  label={r.label}
                  value={<MonoNum>{fmtMs(r.value)}</MonoNum>}
                />
              ))}
            </div>
          ))}
          {unaccounted > 10 ? (
            <KvRow
              label="Unaccounted"
              value={<MonoNum>{fmtMs(unaccounted)}</MonoNum>}
            />
          ) : null}
        </DetailSection>

        {isStream ? (
          <DetailSection title="Stream metrics">
            <KvRow
              label="TTFT (first content delta)"
              value={
                <span className="font-mono tabular-nums text-accent">
                  {fmtMs(event.stream_first_content_delta_ms)}
                </span>
              }
            />
            <KvRow
              label="Inter-token avg"
              value={<MonoNum>{fmtMs(event.inter_token_avg_ms)}</MonoNum>}
            />
            <KvRow
              label="Message start"
              value={<MonoNum>{fmtMs(event.stream_message_start_ms)}</MonoNum>}
            />
            <KvRow
              label="Content block start"
              value={
                <MonoNum>{fmtMs(event.stream_content_block_start_ms)}</MonoNum>
              }
            />
            <KvRow
              label="Last content delta"
              value={
                <MonoNum>{fmtMs(event.stream_last_content_delta_ms)}</MonoNum>
              }
            />
            <KvRow
              label="Message stop"
              value={<MonoNum>{fmtMs(event.stream_message_stop_ms)}</MonoNum>}
            />
            <KvRow
              label="Last chunk"
              value={<MonoNum>{fmtMs(event.stream_last_chunk_ms)}</MonoNum>}
            />
            <KvRow
              label="Total stream"
              value={<MonoNum>{fmtMs(event.stream_total_ms)}</MonoNum>}
            />
            <KvRow
              label="SSE events"
              value={<MonoNum>{fmtN(event.sse_event_count)}</MonoNum>}
            />
            <KvRow
              label="Content deltas"
              value={<MonoNum>{fmtN(event.content_delta_count)}</MonoNum>}
            />
            <KvRow
              label="Pings"
              value={<MonoNum>{fmtN(event.ping_count)}</MonoNum>}
            />
            <KvRow
              label="Body chunks"
              value={<MonoNum>{fmtN(event.body_chunk_count)}</MonoNum>}
            />
          </DetailSection>
        ) : null}
      </div>
    </>
  );
}

function DetailSection({
  title,
  children,
}: {
  title: string;
  children: React.ReactNode;
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

function KvRow({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="flex items-center justify-between gap-3 min-h-[18px]">
      <span className="text-text-faint">{label}</span>
      <span className="text-right">{value}</span>
    </div>
  );
}

function MonoNum({ children }: { children: React.ReactNode }) {
  return <span className="font-mono tabular-nums">{children}</span>;
}

function truncateMid(s: string, max: number): string {
  if (s.length <= max) return s;
  const half = Math.floor((max - 1) / 2);
  return `${s.slice(0, half)}…${s.slice(-half)}`;
}
