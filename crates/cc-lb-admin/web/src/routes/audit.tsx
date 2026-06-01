import { createFileRoute, useNavigate } from '@tanstack/react-router';
import { Filter, RefreshCw, X } from 'lucide-react';
import { useMemo, useState } from 'react';
import { z } from 'zod';
import {
  Badge,
  Button,
  Card,
  CardBody,
  CardHeader,
  cx,
  EmptyState,
  Field,
  FullPage,
  INPUT_CLASS,
  Modal,
  Section,
  Skeleton,
} from '../components/ui/primitives';
import { RelativeTime } from '../components/ui/RelativeTime';
import { eventTime } from '../lib/api';
import {
  useAudit,
  usePrincipalNameMap,
  usePrincipals,
  useUpstreamNameMap,
  useUpstreams,
} from '../lib/queries';

const auditSearchSchema = z.object({
  principal_id: z.string().optional(),
  upstream: z.string().optional(),
  route: z.string().optional(),
  status_class: z.enum(['2xx', '4xx', '5xx']).optional(),
});

export const Route = createFileRoute('/audit')({
  validateSearch: auditSearchSchema,
  component: AuditPage,
});

interface AuditEntryLike {
  request_id: string;
  ts?: number | null;
  ts_ms?: number | null;
  principal_id?: string | null;
  route?: string | null;
  upstream?: string | null;
  status: number;
  actor?: string | null;
  admin_action?: string | null;
  kind?: string | null;
  payload?: Record<string, unknown> | null;
  [k: string]: unknown;
}

function cleanAuditPayload(entry: AuditEntryLike): Record<string, unknown> {
  const DROP = new Set([
    'model',
    'input_tokens',
    'output_tokens',
    'duration_ms',
    'body_bytes',
    'cost_usd_micros',
    'cache_creation_input_tokens',
    'cache_read_input_tokens',
    'agent_label',
    'api_key_id',
  ]);
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(entry)) {
    if (DROP.has(k)) continue;
    if (v == null) continue;
    if (typeof v === 'number' && v === 0 && k !== 'status') continue;
    out[k] = v;
  }
  return out;
}

function AuditPage() {
  const filters = Route.useSearch();
  const navigate = useNavigate({ from: Route.fullPath });
  const principals = usePrincipals();
  const upstreams = useUpstreams();
  const audit = useAudit({ limit: '200', ...filters });
  const principalNameMap = usePrincipalNameMap();
  const upstreamNameMap = useUpstreamNameMap();
  const [selected, setSelected] = useState<AuditEntryLike | null>(null);

  const setFilter = <K extends keyof typeof filters>(key: K, value: string) => {
    navigate({ search: { ...filters, [key]: value || undefined } });
  };

  const rows = (audit.data?.entries ?? []) as unknown as AuditEntryLike[];
  const adminRows = rows.filter(
    (r) => (r.admin_action ?? null) != null || (r.kind ?? null) != null,
  );
  const sortedRows = useMemo(
    () =>
      [...adminRows].sort(
        (a, b) =>
          (eventTime(b)?.getTime() ?? 0) - (eventTime(a)?.getTime() ?? 0),
      ),
    [adminRows],
  );

  const activeFilterCount = Object.values(filters).filter(Boolean).length;

  return (
    <FullPage>
      <Section
        title="Audit Trail"
        className="flex-1 min-h-0"
        subtitle={`${sortedRows.length} admin actions · ${activeFilterCount ? `${activeFilterCount} filter${activeFilterCount > 1 ? 's' : ''} active` : 'unfiltered'}`}
        action={
          <div className="flex items-center gap-2">
            <Button
              size="sm"
              iconLeft={<RefreshCw className="w-3 h-3" />}
              onClick={() => audit.refetch()}
            >
              Refresh
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
                {principals.data?.principals.map((p) => (
                  <option key={p.id} value={p.id}>
                    {p.name}
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
            <Field label="Route">
              <input
                className={`${INPUT_CLASS} w-44 font-mono`}
                value={filters.route ?? ''}
                onChange={(e) => setFilter('route', e.target.value)}
                placeholder="/v1/messages"
              />
            </Field>
            <Field label="Status">
              <select
                className={`${INPUT_CLASS} w-32`}
                value={filters.status_class ?? ''}
                onChange={(e) => setFilter('status_class', e.target.value)}
              >
                <option value="">All</option>
                <option value="2xx">2xx</option>
                <option value="4xx">4xx</option>
                <option value="5xx">5xx</option>
              </select>
            </Field>
            {activeFilterCount ? (
              <Button
                size="sm"
                iconLeft={<X className="w-3 h-3" />}
                onClick={() => navigate({ search: {} })}
              >
                Clear
              </Button>
            ) : null}
          </div>

          <div className="flex-1 overflow-auto min-h-0">
            <table className="min-w-[1080px] w-full font-mono text-xs">
              <thead className="table-header sticky top-0 z-10">
                <tr className="text-[10px] uppercase tracking-wider">
                  <th className="text-left px-3 py-2">
                    Timestamp <span className="font-mono ml-1">↓</span>
                  </th>
                  <th className="text-left px-3 py-2">Principal</th>
                  <th className="text-left px-3 py-2">Actor</th>
                  <th className="text-left px-3 py-2">Route</th>
                  <th className="text-left px-3 py-2">Upstream</th>
                  <th className="text-left px-3 py-2">Action</th>
                  <th className="text-left px-3 py-2">Kind</th>
                  <th className="text-right px-3 py-2">Status</th>
                  <th className="text-right px-3 py-2">Detail</th>
                </tr>
              </thead>
              <tbody>
                {audit.isLoading ? (
                  Array.from({ length: 5 }).map((_, i) => (
                    <tr key={i}>
                      <td colSpan={9} className="px-3 py-2">
                        <Skeleton />
                      </td>
                    </tr>
                  ))
                ) : sortedRows.length ? (
                  sortedRows.map((e) => (
                    <tr
                      key={e.request_id}
                      className="border-b border-row hover:bg-overlay-3 cursor-pointer"
                      onClick={() => setSelected(e)}
                    >
                      <td className="px-3 py-2 text-text-faint whitespace-nowrap">
                        <RelativeTime ts={eventTime(e)} />
                      </td>
                      <td className="px-3 py-2 truncate max-w-[160px]">
                        {principalNameMap.get(e.principal_id ?? '') ??
                          e.principal_id ??
                          '—'}
                      </td>
                      <td className="px-3 py-2 font-mono truncate max-w-[140px]">
                        {e.actor ?? '—'}
                      </td>
                      <td className="px-3 py-2 text-text-muted">
                        {e.route ?? '—'}
                      </td>
                      <td className="px-3 py-2 truncate max-w-[180px]">
                        {upstreamNameMap.get(e.upstream ?? '') ??
                          e.upstream ??
                          '—'}
                      </td>
                      <td
                        className="px-3 py-2 font-mono truncate max-w-[280px]"
                        title={e.admin_action ?? undefined}
                      >
                        {e.admin_action ?? '—'}
                      </td>
                      <td className="px-3 py-2 font-mono truncate max-w-[160px]">
                        {e.kind ?? '—'}
                      </td>
                      <td
                        className={cx(
                          'px-3 py-2 text-right tabular-nums',
                          e.status >= 500
                            ? 'text-red-400'
                            : e.status >= 400
                              ? 'text-amber-400'
                              : 'text-green-400',
                        )}
                      >
                        {e.status}
                      </td>
                      <td className="px-3 py-2 text-right">
                        <Badge tone="neutral">view</Badge>
                      </td>
                    </tr>
                  ))
                ) : (
                  <tr>
                    <td colSpan={9}>
                      <EmptyState
                        title="No audit entries"
                        description="No entries match the current filters."
                        action={
                          activeFilterCount ? (
                            <Button
                              onClick={() => navigate({ search: {} })}
                              variant="primary"
                              iconLeft={<Filter className="w-3 h-3" />}
                            >
                              Clear filters
                            </Button>
                          ) : undefined
                        }
                      />
                    </td>
                  </tr>
                )}
                {sortedRows.length > 0 && (
                  <tr>
                    <td
                      colSpan={9}
                      className="px-3 py-4 text-center text-text-faint text-[11px]"
                    >
                      No more entries
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </Card>
      </Section>

      <Modal
        open={!!selected}
        onOpenChange={(o) => {
          if (!o) setSelected(null);
        }}
        title={selected ? `Audit entry ${selected.request_id}` : ''}
        size="lg"
        footer={<Button onClick={() => setSelected(null)}>Close</Button>}
      >
        {selected ? (
          <div className="space-y-3 text-xs font-mono">
            <Card>
              <CardHeader title="Summary" />
              <CardBody className="space-y-2">
                <Row
                  label="Timestamp"
                  value={<RelativeTime ts={eventTime(selected)} />}
                />
                <Row
                  label="Principal"
                  value={
                    principalNameMap.get(selected.principal_id ?? '') ??
                    selected.principal_id ??
                    '—'
                  }
                />
                <Row label="Actor" value={selected.actor ?? '—'} />
                <Row label="Route" value={selected.route ?? '—'} />
                <Row
                  label="Upstream"
                  value={
                    upstreamNameMap.get(selected.upstream ?? '') ??
                    selected.upstream ??
                    '—'
                  }
                />
                <Row
                  label="Admin action"
                  value={selected.admin_action ?? '—'}
                />
                <Row label="Kind" value={selected.kind ?? '—'} />
                <Row label="Status" value={String(selected.status)} />
              </CardBody>
            </Card>
            <Card>
              <CardHeader
                title="Raw payload"
                subtitle="Redacted keys are pre-stripped server-side"
              />
              <CardBody>
                <pre className="overflow-x-auto text-[11px] leading-relaxed">
                  {JSON.stringify(cleanAuditPayload(selected), null, 2)}
                </pre>
              </CardBody>
            </Card>
          </div>
        ) : null}
      </Modal>
    </FullPage>
  );
}

function Row({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="flex items-center justify-between gap-3">
      <span className="text-text-faint">{label}</span>
      <span className="text-right truncate max-w-[60%]">{value}</span>
    </div>
  );
}
