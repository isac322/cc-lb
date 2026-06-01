import { createFileRoute, useNavigate } from '@tanstack/react-router';
import { useEffect, useState } from 'react';
import { z } from 'zod';
import { Drawer } from 'vaul';
import { Download, RefreshCw, X, Zap } from 'lucide-react';
import {
  Button,
  Card,
  EmptyState,
  Field,
  INPUT_CLASS,
  PageContainer,
  Section,
  cx,
} from '../components/ui/primitives';
import { useRecentEvents, usePrincipals, useUpstreams } from '../lib/queries';
import { streamEventsFetch, type RequestEvent } from '../lib/api';

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

function LogsPage() {
  const filters = Route.useSearch();
  const navigate = useNavigate({ from: Route.fullPath });
  const principals = usePrincipals();
  const upstreams = useUpstreams();
  const recent = useRecentEvents({ ...filters, limit: '200' });

  const [tailing, setTailing] = useState(false);
  const [liveRows, setLiveRows] = useState<RequestEvent[]>([]);
  const [selected, setSelected] = useState<RequestEvent | null>(null);
  const [tailStatus, setTailStatus] = useState<'idle' | 'connecting' | 'live' | 'down'>('idle');

  useEffect(() => {
    if (!tailing) { setTailStatus('idle'); return; }
    setLiveRows([]);
    setTailStatus('connecting');
    const close = streamEventsFetch('/admin/events/stream', {
      onConnect: () => setTailStatus('live'),
      onEvent: (ev) => {
        try {
          const parsed = JSON.parse(ev.data) as RequestEvent;
          setLiveRows((prev) => [parsed, ...prev].slice(0, 200));
        } catch {/* ignore: malformed SSE chunk */}
      },
      onError: () => { setTailStatus('down'); setTailing(false); },
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
            <table className="min-w-[920px] w-full font-mono text-xs">
              <thead className="bg-panel-strong border-b border-subtle sticky top-0 z-10 backdrop-blur-md">
                <tr className="text-text-faint text-[10px] uppercase tracking-wider">
                  <th className="text-left px-3 py-2">Timestamp</th>
                  <th className="text-left px-3 py-2">Principal</th>
                  <th className="text-left px-3 py-2">Upstream</th>
                  <th className="text-left px-3 py-2">Model</th>
                  <th className="text-right px-3 py-2">Status</th>
                  <th className="text-right px-3 py-2">Latency</th>
                  <th className="text-right px-3 py-2">Tokens I/O</th>
                  <th className="text-right px-3 py-2">Cost</th>
                </tr>
              </thead>
              <tbody>
                {rows.length ? rows.map((e, i) => (
                  <tr
                    key={e.request_id}
                    className={cx('border-b border-subtle/40 hover:bg-overlay-3 cursor-pointer', tailing && i === 0 ? 'flash-in' : '')}
                    onClick={() => setSelected(e)}
                  >
                    <td className="px-3 py-2 text-text-faint whitespace-nowrap">
                      <span className={cx('status-dot mr-2', e.status >= 500 ? 'danger' : e.status >= 400 ? 'warn' : 'ok')} />
                      {new Date(e.ts * 1000).toISOString().slice(11, 19)} UTC
                    </td>
                    <td className="px-3 py-2 truncate max-w-[160px]">{e.principal_id ?? '—'}</td>
                    <td className="px-3 py-2 truncate max-w-[180px]">{e.upstream ?? '—'}</td>
                    <td className="px-3 py-2 text-text-muted truncate max-w-[260px]">{e.model ?? '—'}</td>
                    <td className={cx('px-3 py-2 text-right tabular-nums', e.status >= 500 ? 'text-red-400' : e.status >= 400 ? 'text-amber-400' : 'text-green-400')}>{e.status}</td>
                    <td className="px-3 py-2 text-right tabular-nums">{e.duration_ms}ms</td>
                    <td className="px-3 py-2 text-right tabular-nums">{e.input_tokens ?? 0} / {e.output_tokens ?? 0}</td>
                    <td className="px-3 py-2 text-right tabular-nums">${((e.cost_usd_micros ?? 0) / 1_000_000).toFixed(4)}</td>
                  </tr>
                )) : (
                  <tr><td colSpan={8}><EmptyState title="No requests" description="Adjust filters or enable live tail." /></td></tr>
                )}
              </tbody>
            </table>
          </div>
        </Card>
      </Section>

      <Drawer.Root open={!!selected} onOpenChange={(o) => { if (!o) setSelected(null); }} direction="right">
        <Drawer.Portal>
          <Drawer.Overlay className="fixed inset-0 z-40 bg-drawer-backdrop" />
          <Drawer.Content className="fixed right-0 top-0 bottom-0 w-full max-w-md bg-bg-sub border-l border-subtle z-50 flex flex-col">
            <Drawer.Title className="sr-only">Request detail</Drawer.Title>
            <Drawer.Description className="sr-only">Detail view of a single request event</Drawer.Description>
            {selected ? (
              <>
                <div className="p-4 border-b border-subtle flex items-start justify-between">
                  <div>
                    <div className="font-mono text-xs text-text-faint">{selected.request_id}</div>
                    <div className="text-sm">{selected.principal_id ?? '—'} → {selected.upstream ?? '—'}</div>
                  </div>
                  <button type="button" aria-label="Close" onClick={() => setSelected(null)} className="text-text-muted hover:text-text">
                    <X className="w-4 h-4" />
                  </button>
                </div>
                <div className="flex-1 overflow-y-auto p-4 space-y-4 text-xs font-mono">
                  <Row label="Timestamp" value={new Date(selected.ts * 1000).toISOString()} />
                  <Row label="Status" value={String(selected.status)} />
                  <Row label="Model" value={selected.model ?? '—'} />
                  <Row label="Tokens (in/out)" value={`${selected.input_tokens ?? 0} / ${selected.output_tokens ?? 0}`} />
                  <Row label="Cost (USD)" value={`$${((selected.cost_usd_micros ?? 0) / 1_000_000).toFixed(6)}`} />
                  <div className="border-t border-subtle pt-3">
                    <div className="text-[10px] uppercase tracking-wider text-text-faint mb-2">Latency breakdown</div>
                    <Row label="Total" value={`${selected.duration_ms}ms`} />
                    <Row label="Queue" value={selected.proxy_setup_ms != null ? `${selected.proxy_setup_ms}ms` : (selected as { queue_ms?: number }).queue_ms != null ? `${(selected as { queue_ms?: number }).queue_ms}ms` : '—'} />
                    <Row label="Sign" value={selected.sign_ms != null ? `${selected.sign_ms}ms` : '—'} />
                    <Row label="Upstream TTFB" value={selected.upstream_ttfb_ms != null ? `${selected.upstream_ttfb_ms}ms` : '—'} />
                  </div>
                </div>
              </>
            ) : null}
          </Drawer.Content>
        </Drawer.Portal>
      </Drawer.Root>
    </PageContainer>
  );
}

function Row({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="flex items-center justify-between">
      <span className="text-text-faint">{label}</span>
      <span>{value}</span>
    </div>
  );
}
