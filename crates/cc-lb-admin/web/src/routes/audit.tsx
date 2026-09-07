import { createFileRoute, useNavigate } from '@tanstack/react-router';
import { Filter, RefreshCw, X } from 'lucide-react';
import { useMemo, useRef, useState } from 'react';
import * as z from 'zod';
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
  SkeletonRow,
} from '../components/ui/primitives';
import { RelativeTime } from '../components/ui/RelativeTime';
import { TimeRangeBounds } from '../components/ui/TimeRangeBounds';
import { eventTime } from '../lib/api';
import {
  useAudit,
  usePrincipalNameMap,
  usePrincipals,
  useUpstreamNameMap,
} from '../lib/queries';
import { MAX_FORMATTABLE_UNIX_SECONDS } from '../lib/timezone';

const unixSecondsSearchParam = z.preprocess((value) => {
  if (value == null || value === '') return undefined;
  const number = Number(value);
  return Number.isSafeInteger(number) &&
    number >= 0 &&
    number <= MAX_FORMATTABLE_UNIX_SECONDS
    ? number
    : undefined;
}, z.number().optional());

const auditSearchSchema = z
  .object({
    principal_id: z.string().optional(),
    since: unixSecondsSearchParam,
    until: unixSecondsSearchParam,
  })
  .transform((filters) => {
    if (
      filters.since != null &&
      filters.until != null &&
      filters.since > filters.until
    ) {
      return {
        ...filters,
        since: undefined,
        until: undefined,
      };
    }
    return filters;
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

const AUDIT_COLUMN_CLASS_NAMES = [
  'w-36 whitespace-nowrap',
  'w-40 max-w-40 truncate',
  'w-36 max-w-36 truncate',
  'w-40',
  'w-44 max-w-44 truncate',
  'w-72 max-w-72 truncate',
  'w-40 max-w-40 truncate',
  'w-20 text-right',
  'w-20 text-right',
] as const;

const AUDIT_SKELETON_CLASS_NAMES = [
  'w-24',
  'w-28',
  'w-24',
  'w-28',
  'w-32',
  'w-48',
  'w-28',
  'w-10 ml-auto',
  'w-10 ml-auto',
] as const;
const AUDIT_COUNT_SLOT_CLASS =
  'inline-flex min-w-5 shrink-0 items-center justify-end';

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
  const audit = useAudit({
    limit: '200',
    principal_id: filters.principal_id,
    since: filters.since?.toString(),
    until: filters.until?.toString(),
  });
  const principalNameMap = usePrincipalNameMap();
  const upstreamNameMap = useUpstreamNameMap();
  const [selected, setSelected] = useState<AuditEntryLike | null>(null);
  const [refreshing, setRefreshing] = useState(false);
  const refreshInFlightRef = useRef(false);

  const setPrincipalFilter = (value: string) => {
    navigate({
      search: { ...filters, principal_id: value || undefined },
    });
  };

  const rows = (audit.data?.entries ?? []) as unknown as AuditEntryLike[];
  const adminRows = rows.filter(
    (r) => (r.admin_action ?? null) != null || (r.kind ?? null) != null,
  );
  const renderRows = useMemo(() => {
    const identityOccurrences = new Map<string, number>();
    return adminRows
      .map((entry) => {
        const identity = JSON.stringify([
          entry.request_id,
          entry.ts_ms ?? null,
          entry.ts ?? null,
          entry.principal_id ?? null,
          entry.actor ?? null,
          entry.route ?? null,
          entry.upstream ?? null,
          entry.admin_action ?? null,
          entry.kind ?? null,
          entry.status,
        ])!;
        const occurrence = identityOccurrences.get(identity) ?? 0;
        identityOccurrences.set(identity, occurrence + 1);
        return {
          entry,
          key: `${identity}:${occurrence}`,
        };
      })
      .sort(
        (a, b) =>
          (eventTime(b.entry)?.getTime() ?? 0) -
          (eventTime(a.entry)?.getTime() ?? 0),
      );
  }, [adminRows]);

  const activeFilterCount =
    Number(Boolean(filters.principal_id)) +
    Number(filters.since != null) +
    Number(filters.until != null);

  const refreshAudit = async () => {
    if (refreshInFlightRef.current) return;

    refreshInFlightRef.current = true;
    setRefreshing(true);
    try {
      await audit.refetch();
    } finally {
      refreshInFlightRef.current = false;
      setRefreshing(false);
    }
  };

  return (
    <FullPage>
      <Section
        title="Audit Trail"
        className="flex-1 min-h-0"
        subtitle={
          <span className="inline-flex items-center gap-1.5">
            <span
              className={AUDIT_COUNT_SLOT_CLASS}
              data-testid="audit-count-slot"
            >
              {audit.isLoading ? (
                <span className="skeleton h-3 flex-1" aria-hidden="true" />
              ) : (
                renderRows.length
              )}
            </span>
            <span>
              admin actions ·{' '}
              {activeFilterCount
                ? `${activeFilterCount} filter${activeFilterCount > 1 ? 's' : ''} active`
                : 'unfiltered'}
            </span>
          </span>
        }
        action={
          <div className="flex items-center gap-2">
            <Button
              size="sm"
              data-testid="audit-refresh"
              iconLeft={<RefreshCw className="w-3 h-3" />}
              loading={refreshing}
              onClick={refreshAudit}
            >
              {refreshing ? 'Refreshing...' : 'Refresh'}
            </Button>
          </div>
        }
      >
        <Card className="flex-1 flex flex-col min-h-0">
          <div className="flex shrink-0 flex-wrap items-end gap-3 border-b border-subtle p-3">
            <div className="w-full min-w-0 sm:w-auto">
              <Field label="Principal">
                <select
                  className={`${INPUT_CLASS} w-full sm:w-44`}
                  value={filters.principal_id ?? ''}
                  onChange={(e) => setPrincipalFilter(e.target.value)}
                >
                  <option value="">All principals</option>
                  {principals.data?.principals.map((p) => (
                    <option key={p.id} value={p.id}>
                      {p.name}
                    </option>
                  ))}
                </select>
              </Field>
            </div>
            <TimeRangeBounds
              since={filters.since}
              until={filters.until}
              onCommit={({ since, until }) => {
                navigate({
                  search: (prev) => ({
                    ...prev,
                    since,
                    until,
                  }),
                });
              }}
            />
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
            <table className="table-fixed min-w-[1080px] w-full font-mono text-xs">
              <colgroup>
                {AUDIT_COLUMN_CLASS_NAMES.map((className, index) => (
                  <col key={index} className={className} />
                ))}
              </colgroup>
              <thead className="table-header sticky top-0 z-10">
                <tr className="text-[10px] uppercase tracking-wider">
                  <th
                    className={cx(
                      'text-left px-3 py-2',
                      AUDIT_COLUMN_CLASS_NAMES[0],
                    )}
                  >
                    Timestamp <span className="font-mono ml-1">↓</span>
                  </th>
                  <th
                    className={cx(
                      'text-left px-3 py-2',
                      AUDIT_COLUMN_CLASS_NAMES[1],
                    )}
                  >
                    Principal
                  </th>
                  <th
                    className={cx(
                      'text-left px-3 py-2',
                      AUDIT_COLUMN_CLASS_NAMES[2],
                    )}
                  >
                    Actor
                  </th>
                  <th
                    className={cx(
                      'text-left px-3 py-2',
                      AUDIT_COLUMN_CLASS_NAMES[3],
                    )}
                  >
                    Route
                  </th>
                  <th
                    className={cx(
                      'text-left px-3 py-2',
                      AUDIT_COLUMN_CLASS_NAMES[4],
                    )}
                  >
                    Upstream
                  </th>
                  <th
                    className={cx(
                      'text-left px-3 py-2',
                      AUDIT_COLUMN_CLASS_NAMES[5],
                    )}
                  >
                    Action
                  </th>
                  <th
                    className={cx(
                      'text-left px-3 py-2',
                      AUDIT_COLUMN_CLASS_NAMES[6],
                    )}
                  >
                    Kind
                  </th>
                  <th className={cx('px-3 py-2', AUDIT_COLUMN_CLASS_NAMES[7])}>
                    Status
                  </th>
                  <th className={cx('px-3 py-2', AUDIT_COLUMN_CLASS_NAMES[8])}>
                    Detail
                  </th>
                </tr>
              </thead>
              <tbody>
                {audit.isLoading ? (
                  Array.from({ length: 5 }).map((_, i) => (
                    <SkeletonRow
                      key={i}
                      cols={AUDIT_COLUMN_CLASS_NAMES.length}
                      cellClassNames={AUDIT_COLUMN_CLASS_NAMES}
                      skeletonClassNames={AUDIT_SKELETON_CLASS_NAMES}
                    />
                  ))
                ) : renderRows.length ? (
                  renderRows.map(({ entry: e, key }) => (
                    <tr
                      key={key}
                      className="border-b border-row hover:bg-overlay-3 cursor-pointer"
                      onClick={() => setSelected(e)}
                    >
                      <td
                        className={cx(
                          'px-3 py-2 text-text-faint',
                          AUDIT_COLUMN_CLASS_NAMES[0],
                        )}
                      >
                        <RelativeTime compact ts={eventTime(e)} />
                      </td>
                      <td
                        className={cx('px-3 py-2', AUDIT_COLUMN_CLASS_NAMES[1])}
                      >
                        {principalNameMap.get(e.principal_id ?? '') ??
                          e.principal_id ??
                          '—'}
                      </td>
                      <td
                        className={cx(
                          'px-3 py-2 font-mono',
                          AUDIT_COLUMN_CLASS_NAMES[2],
                        )}
                      >
                        {e.actor ?? '—'}
                      </td>
                      <td
                        className={cx(
                          'px-3 py-2 text-text-muted',
                          AUDIT_COLUMN_CLASS_NAMES[3],
                        )}
                      >
                        {e.route ?? '—'}
                      </td>
                      <td
                        className={cx('px-3 py-2', AUDIT_COLUMN_CLASS_NAMES[4])}
                      >
                        {upstreamNameMap.get(e.upstream ?? '') ??
                          e.upstream ??
                          '—'}
                      </td>
                      <td
                        className={cx(
                          'px-3 py-2 font-mono',
                          AUDIT_COLUMN_CLASS_NAMES[5],
                        )}
                        title={e.admin_action ?? undefined}
                      >
                        {e.admin_action ?? '—'}
                      </td>
                      <td
                        className={cx(
                          'px-3 py-2 font-mono',
                          AUDIT_COLUMN_CLASS_NAMES[6],
                        )}
                      >
                        {e.kind ?? '—'}
                      </td>
                      <td
                        className={cx(
                          'px-3 py-2 tabular-nums',
                          AUDIT_COLUMN_CLASS_NAMES[7],
                          e.status >= 500
                            ? 'text-red-400'
                            : e.status >= 400
                              ? 'text-amber-400'
                              : 'text-green-400',
                        )}
                      >
                        {e.status}
                      </td>
                      <td
                        className={cx('px-3 py-2', AUDIT_COLUMN_CLASS_NAMES[8])}
                      >
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
                {renderRows.length > 0 && (
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
