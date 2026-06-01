import { createFileRoute, useNavigate } from '@tanstack/react-router';
import { useEffect, useMemo, useState } from 'react';
import { z } from 'zod';
import { Drawer } from 'vaul';
import { Copy, Download, RefreshCw, X, Zap } from 'lucide-react';
import { toast } from 'sonner';
import {
  Badge,
  Button,
  Card,
  EmptyState,
  Field,
  Hint,
  INPUT_CLASS,
  PageContainer,
  Section,
  cx,
} from '../components/ui/primitives';
import { useRecentEvents, usePrincipals, useUpstreams } from '../lib/queries';
import { ApiError, eventTime, streamEventsFetch, type RequestEvent } from '../lib/api';

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

function fmtMs(v: number | null | undefined): string {
  return v == null ? DASH : `${Math.round(v).toLocaleString()}ms`;
}
function fmtN(v: number | null | undefined): string {
  return v == null ? DASH : v.toLocaleString();
}
function fmtBytes(v: number | null | undefined): string {
  if (v == null) return DASH;
  if (v < 1024) return `${v} B`;
  if (v < 1024 * 1024) return `${(v / 1024).toFixed(1)} KB`;
  return `${(v / (1024 * 1024)).toFixed(2)} MB`;
}
function fmtUsd(micros: number | null | undefined): string {
  if (micros == null) return DASH;
  const usd = micros / 1_000_000;
  return `$${usd.toFixed(4)}`;
}
function statusTone(s: number): 'ok' | 'warn' | 'danger' | 'neutral' {
  if (s >= 500) return 'danger';
  if (s >= 400) return 'warn';
  if (s >= 200 && s < 300) return 'ok';
  return 'neutral';
}
function cacheHitRatio(e: RequestEvent): number | null {
  const read = e.cache_read_input_tokens ?? 0;
  const input = e.input_tokens ?? 0;
  const denom = read + input;
  if (denom <= 0) return null;
  return read / denom;
}
function copyText(value: string, label: string) {
  navigator.clipboard.writeText(value).then(
    () => toast.success(`${label} copied`),
    () => toast.error(`Failed to copy ${label}`),
  );
}

function LogsPage() {
  const filters = Route.useSearch();
  const navigate = useNavigate({ from: Route.fullPath });
  const principals = usePrincipals();
  const upstreams = useUpstreams();
  const recent = useRecentEvents({ ...filters, limit: '200' });

  const [tailing, setTailing] = useState(false);
  const [liveRows, setLiveRows] = useState<RequestEvent[]>([]);
  const [selected, setSelected] = useState<RequestEvent | null>(null);
  const [tailStatus, setTailStatus] = useState<'idle' | 'connecting' | 'live' | 'reconnecting' | 'down'>('idle');

  const principalNameById = useMemo(() => {
    const m = new Map<string, string>();
    for (const p of principals.data?.principals ?? []) m.set(p.id, p.name);
    return m;
  }, [principals.data]);

  useEffect(() => {
    if (!tailing) { setTailStatus('idle'); return; }
    setLiveRows([]);
    setTailStatus('connecting');
    const close = streamEventsFetch('/admin/events/stream', {
      onConnect: () => setTailStatus('live'),
      onEvent: (data) => {
        try {
          const parsed = JSON.parse(data) as RequestEvent;
          setLiveRows((prev) => [parsed, ...prev].slice(0, 200));
        } catch {/* ignore: malformed SSE chunk */}
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

  const rows = tailing ? liveRows : recent.data?.events ?? [];

  const setFilter = (key: keyof typeof filters, value: string) => {
    navigate({ search: { ...filters, [key]: value || undefined } });
  };

  const downloadJson = () => {
    const blob = new Blob([JSON.stringify(rows, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `cc-lb-events-${new Date().toISOString().slice(0, 16)}.json`;
    a.click();
    URL.revokeObjectURL(url);
  };

  return (
    <PageContainer>
      <Section
        title="Live Logs"
        subtitle={
          <span className="flex items-center gap-2">
            <span>{rows.length} requests — {tailing ? 'live tailing' : 'paged'}</span>
            {tailing ? (
              <span className="inline-flex items-center gap-1 px-1.5 py-0.5 text-[10px] uppercase tracking-wider border border-subtle rounded-sm">
                <span className={cx('status-dot', tailStatus === 'live' ? 'live' : tailStatus === 'down' ? 'danger' : 'neutral')} />
                <span>{tailStatus}</span>
              </span>
            ) : null}
          </span>
        }
        action={
          <div className="flex items-center gap-2 flex-wrap justify-end">
            <Button size="sm" variant={tailing ? 'accent' : 'secondary'} iconLeft={<Zap className="w-3 h-3" />} onClick={() => setTailing((t) => !t)}>
              {tailing ? 'Stop tail' : 'Live tail'}
            </Button>
            <Button size="sm" iconLeft={<RefreshCw className="w-3 h-3" />} onClick={() => recent.refetch()}>Refresh</Button>
            <Button size="sm" iconLeft={<Download className="w-3 h-3" />} onClick={downloadJson}>Export</Button>
          </div>
        }>
        <Card>
          <div className="p-3 border-b border-subtle flex flex-wrap gap-3 items-end">
            <Field label="Principal">
              <select className={INPUT_CLASS + ' w-44'} value={filters.principal_id ?? ''} onChange={(e) => setFilter('principal_id', e.target.value)}>
                <option value="">All principals</option>
                {principals.data?.principals.map((p) => <option key={p.id} value={p.id}>{p.name}</option>)}
              </select>
            </Field>
            <Field label="Upstream">
              <select className={INPUT_CLASS + ' w-44'} value={filters.upstream ?? ''} onChange={(e) => setFilter('upstream', e.target.value)}>
                <option value="">All upstreams</option>
                {upstreams.data?.upstreams.map((u) => <option key={u.id} value={u.name}>{u.name}</option>)}
              </select>
            </Field>
            <Field label="Model">
              <input className={INPUT_CLASS + ' w-44 font-mono'} value={filters.model ?? ''} onChange={(e) => setFilter('model', e.target.value)} placeholder="claude-*" />
            </Field>
            <Field label="Status">
              <select className={INPUT_CLASS + ' w-32'} value={filters.status ?? ''} onChange={(e) => setFilter('status', e.target.value)}>
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
                className={INPUT_CLASS + ' w-48'}
                value={filters.since ?? ''}
                onChange={(e) => setFilter('since', e.target.value)}
              />
            </Field>
            <Field label="Until">
              <input
                type="datetime-local"
                lang="en"
                className={INPUT_CLASS + ' w-48'}
                value={filters.until ?? ''}
                onChange={(e) => setFilter('until', e.target.value)}
              />
            </Field>
            {(filters.principal_id || filters.upstream || filters.model || filters.status || filters.since || filters.until) ? (
              <Button size="sm" iconLeft={<X className="w-3 h-3" />} onClick={() => navigate({ search: {} })}>Clear</Button>
            ) : null}
          </div>

          <div className="overflow-x-auto" style={{ maxHeight: 'calc(100vh - 280px)' }}>
            <table className="min-w-[980px] w-full font-mono text-xs">
              <thead className="bg-panel-strong border-b border-subtle sticky top-0 z-10 backdrop-blur-md">
                <tr className="text-text-faint text-[10px] uppercase tracking-wider">
                  <th className="text-left px-3 py-2">Timestamp</th>
                  <th className="text-left px-3 py-2">Principal</th>
                  <th className="text-left px-3 py-2">Upstream</th>
                  <th className="text-left px-3 py-2">Model</th>
                  <th className="text-right px-3 py-2">Status</th>
                  <th className="text-right px-3 py-2">Latency</th>
                  <th className="text-right px-3 py-2">Tokens I/O</th>
                  <th className="text-right px-3 py-2">Cache</th>
                  <th className="text-right px-3 py-2">Cost</th>
                </tr>
              </thead>
              <tbody>
                {rows.length ? rows.map((e, i) => {
                  const ratio = cacheHitRatio(e);
                  return (
                    <tr
                      key={e.request_id}
                      className={cx('border-b border-subtle/40 hover:bg-overlay-3 cursor-pointer', tailing && i === 0 ? 'flash-in' : '')}
                      onClick={() => setSelected(e)}
                    >
                      <td className="px-3 py-2 text-text-faint whitespace-nowrap">
                        <span className={cx('status-dot mr-2', e.status >= 500 ? 'danger' : e.status >= 400 ? 'warn' : 'ok')} />
                        {eventTime(e)?.toISOString().slice(11, 19) ?? DASH} UTC
                      </td>
                      <td className="px-3 py-2 truncate max-w-[160px]">
                        {(e.principal_id && principalNameById.get(e.principal_id)) ?? e.principal_id ?? DASH}
                      </td>
                      <td className="px-3 py-2 truncate max-w-[180px]">{e.upstream_name ?? e.upstream ?? DASH}</td>
                      <td className="px-3 py-2 text-text-muted truncate max-w-[260px]">{e.model ?? DASH}</td>
                      <td className={cx('px-3 py-2 text-right tabular-nums', e.status >= 500 ? 'text-red-400' : e.status >= 400 ? 'text-amber-400' : 'text-green-400')}>{e.status}</td>
                      <td className="px-3 py-2 text-right tabular-nums">{e.duration_ms}ms</td>
                      <td className="px-3 py-2 text-right tabular-nums">{e.input_tokens ?? 0} / {e.output_tokens ?? 0}</td>
                      <td className="px-3 py-2 text-right tabular-nums text-text-muted">
                        {ratio == null ? DASH : `${(ratio * 100).toFixed(0)}%`}
                      </td>
                      <td className="px-3 py-2 text-right tabular-nums">${((e.cost_usd_micros ?? 0) / 1_000_000).toFixed(4)}</td>
                    </tr>
                  );
                }) : (
                  <tr><td colSpan={9}><EmptyState title="No requests" description="Adjust filters or enable live tail." /></td></tr>
                )}
              </tbody>
            </table>
          </div>
        </Card>
      </Section>

      <Drawer.Root open={!!selected} onOpenChange={(o) => { if (!o) setSelected(null); }} direction="right">
        <Drawer.Portal>
          <Drawer.Overlay className="fixed inset-0 z-40 bg-drawer-backdrop" />
          <Drawer.Content className="fixed right-0 top-0 bottom-0 w-full max-w-lg bg-bg-sub border-l border-subtle z-50 flex flex-col">
            <Drawer.Title className="sr-only">Request detail</Drawer.Title>
            <Drawer.Description className="sr-only">Detail view of a single request event</Drawer.Description>
            {selected ? (
              <RequestDetail
                event={selected}
                principalName={selected.principal_id ? principalNameById.get(selected.principal_id) ?? null : null}
                onClose={() => setSelected(null)}
              />
            ) : null}
          </Drawer.Content>
        </Drawer.Portal>
      </Drawer.Root>
    </PageContainer>
  );
}

function RequestDetail({ event, principalName, onClose }: { event: RequestEvent; principalName: string | null; onClose: () => void }) {
  const ts = eventTime(event);
  const tsIso = ts?.toISOString() ?? null;
  const tsLocal = ts?.toLocaleString() ?? null;

  const hasAnyToken =
    event.input_tokens != null ||
    event.output_tokens != null ||
    event.cache_creation_input_tokens != null ||
    event.cache_read_input_tokens != null ||
    event.body_bytes != null;

  const totalTokens =
    (event.input_tokens ?? 0) +
    (event.output_tokens ?? 0);

  const isStream =
    (event.stream_total_ms != null) ||
    ((event.sse_event_count ?? 0) > 0) ||
    (event.stream_first_content_delta_ms != null);

  // Latency breakdown — sum known stages to compute "Unaccounted".
  const stages: { label: string; value?: number }[] = [
    { label: 'Proxy setup', value: event.proxy_setup_ms },
    { label: 'Shape', value: event.shape_ms },
    { label: 'Sign', value: event.sign_ms },
    { label: 'Upstream TTFB', value: event.upstream_ttfb_ms },
    { label: 'Upstream body', value: event.upstream_body_ms },
    { label: 'First body chunk', value: event.first_body_chunk_ms },
  ];
  const knownSum = stages.reduce((a, s) => a + (s.value ?? 0), 0);
  const unaccounted = Math.max(0, (event.duration_ms ?? 0) - knownSum);

  const principalLabel = principalName ?? event.principal_id ?? DASH;

  return (
    <>
      <div className="p-4 border-b border-subtle flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="flex items-center gap-2 mb-1">
            <span className="font-mono text-xs text-text-faint truncate max-w-[280px]" title={event.request_id}>
              {event.request_id}
            </span>
            <button
              type="button"
              aria-label="Copy request id"
              className="text-text-faint hover:text-text"
              onClick={() => copyText(event.request_id, 'Request ID')}
            >
              <Copy className="w-3 h-3" />
            </button>
          </div>
          <div className="text-sm truncate">{principalLabel} → {event.upstream_name ?? event.upstream ?? DASH}</div>
        </div>
        <button type="button" aria-label="Close" onClick={onClose} className="text-text-muted hover:text-text shrink-0">
          <X className="w-4 h-4" />
        </button>
      </div>
      <div className="flex-1 overflow-y-auto p-4 space-y-5 text-xs">
        <DetailSection title="Identity">
          <KvRow label="Timestamp" value={
            tsLocal ? (
              <Hint label={tsIso ?? ''}>
                <span className="font-mono cursor-help">{tsLocal}</span>
              </Hint>
            ) : DASH
          } />
          <KvRow label="Principal" value={
            <span className="flex items-center gap-2 justify-end">
              <span className="font-mono truncate max-w-[220px]" title={event.principal_id ?? ''}>{principalLabel}</span>
              {event.principal_kind ? <Badge tone="mono">{event.principal_kind}</Badge> : null}
            </span>
          } />
          <KvRow label="Key ID" value={
            event.key_id ? (
              <span className="flex items-center gap-1 justify-end">
                <Hint label={event.key_id}>
                  <span className="font-mono truncate max-w-[180px] cursor-help">{truncateMid(event.key_id, 16)}</span>
                </Hint>
                <button
                  type="button"
                  aria-label="Copy key id"
                  className="text-text-faint hover:text-text"
                  onClick={() => copyText(event.key_id!, 'Key ID')}
                >
                  <Copy className="w-3 h-3" />
                </button>
              </span>
            ) : DASH
          } />
          <KvRow label="Upstream" value={
            <span className="font-mono">
              {event.upstream_name ?? DASH}
              {event.upstream && event.upstream !== event.upstream_name ? (
                <span className="text-text-faint ml-2">({event.upstream})</span>
              ) : null}
            </span>
          } />
          <KvRow label="Model" value={<span className="font-mono">{event.model ?? DASH}</span>} />
          <KvRow label="Status" value={
            <Badge tone={statusTone(event.status)}>{event.status}</Badge>
          } />
          {event.error_code ? (
            <KvRow label="Error" value={<span className="text-red-400 font-mono">{event.error_code}</span>} />
          ) : null}
        </DetailSection>

        {hasAnyToken ? (
          <DetailSection title="Tokens">
            <KvRow label="Input" value={<MonoNum>{fmtN(event.input_tokens)}</MonoNum>} />
            <KvRow label="Output" value={<MonoNum>{fmtN(event.output_tokens)}</MonoNum>} />
            <KvRow label="Cache creation" value={<MonoNum>{fmtN(event.cache_creation_input_tokens)}</MonoNum>} />
            <KvRow label="Cache read" value={<MonoNum>{fmtN(event.cache_read_input_tokens)}</MonoNum>} />
            <KvRow label="Total (in+out)" value={<MonoNum>{fmtN(totalTokens)}</MonoNum>} />
            <KvRow label="Body bytes" value={<MonoNum>{fmtBytes(event.body_bytes)}</MonoNum>} />
          </DetailSection>
        ) : null}

        <DetailSection title="Cost">
          <KvRow label="Cost (USD)" value={<MonoNum>{fmtUsd(event.cost_usd_micros)}</MonoNum>} />
        </DetailSection>

        <DetailSection title="Latency breakdown">
          <KvRow label="Total" value={<MonoNum>{fmtMs(event.duration_ms)}</MonoNum>} />
          {stages.map((s) => (
            <KvRow key={s.label} label={s.label} value={<MonoNum>{fmtMs(s.value)}</MonoNum>} />
          ))}
          <KvRow
            label="Unaccounted"
            value={<MonoNum>{event.duration_ms != null ? fmtMs(unaccounted) : DASH}</MonoNum>}
          />
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
            <KvRow label="Inter-token avg" value={<MonoNum>{fmtMs(event.inter_token_avg_ms)}</MonoNum>} />
            <KvRow label="Message start" value={<MonoNum>{fmtMs(event.stream_message_start_ms)}</MonoNum>} />
            <KvRow label="Content block start" value={<MonoNum>{fmtMs(event.stream_content_block_start_ms)}</MonoNum>} />
            <KvRow label="Last content delta" value={<MonoNum>{fmtMs(event.stream_last_content_delta_ms)}</MonoNum>} />
            <KvRow label="Message stop" value={<MonoNum>{fmtMs(event.stream_message_stop_ms)}</MonoNum>} />
            <KvRow label="Last chunk" value={<MonoNum>{fmtMs(event.stream_last_chunk_ms)}</MonoNum>} />
            <KvRow label="Total stream" value={<MonoNum>{fmtMs(event.stream_total_ms)}</MonoNum>} />
            <KvRow label="SSE events" value={<MonoNum>{fmtN(event.sse_event_count)}</MonoNum>} />
            <KvRow label="Content deltas" value={<MonoNum>{fmtN(event.content_delta_count)}</MonoNum>} />
            <KvRow label="Pings" value={<MonoNum>{fmtN(event.ping_count)}</MonoNum>} />
            <KvRow label="Body chunks" value={<MonoNum>{fmtN(event.body_chunk_count)}</MonoNum>} />
          </DetailSection>
        ) : null}
      </div>
    </>
  );
}

function DetailSection({ title, children }: { title: string; children: React.ReactNode }) {
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
