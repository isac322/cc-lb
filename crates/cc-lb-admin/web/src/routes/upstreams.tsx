import { useQueryClient } from '@tanstack/react-query';
import { createFileRoute, useNavigate } from '@tanstack/react-router';
import {
  ChevronLeft,
  ExternalLink,
  Info,
  KeyRound,
  Pencil,
  Plus,
  Trash2,
} from 'lucide-react';
import { useEffect, useMemo, useState } from 'react';
import {
  Area,
  AreaChart,
  CartesianGrid,
  Legend,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import { toast } from 'sonner';
import { z } from 'zod';
import {
  Badge,
  Button,
  Card,
  CardBody,
  CardHeader,
  ConfirmDialog,
  cx,
  EmptyState,
  Field,
  INPUT_CLASS,
  Modal,
  QuotaMiniChart,
  Section,
  Skeleton,
  StatusBadge,
} from '../components/ui/primitives';
import { RelativeTime } from '../components/ui/RelativeTime';
import { ApiError, deleteJson, eventTime, getJson } from '../lib/api';
import { getUpstreamColor } from '../lib/colors';
import {
  qk,
  type Upstream,
  useCreateUpstream,
  useDeleteUpstream,
  useOAuthComplete,
  useOAuthStart,
  usePrincipalNameMap,
  useRecentEvents,
  useStatus,
  useSubscriptionQuotaAnalysis,
  useSubscriptionQuotaLatest,
  useSubscriptionQuotaSeries,
  useToggleUpstream,
  useUpdateUpstream,
  useUpstreamOAuthStatus,
  useUpstreams,
} from '../lib/queries';

const upstreamSearchSchema = z.object({ selectedId: z.string().optional() });

export const Route = createFileRoute('/upstreams')({
  validateSearch: upstreamSearchSchema,
  component: UpstreamsPage,
});

function UpstreamsPage() {
  const { selectedId } = Route.useSearch();
  const navigate = useNavigate({ from: Route.fullPath });
  const upstreams = useUpstreams();

  const nowUnixSecs = Math.floor(Date.now() / 1000);
  const sinceUnixSecs = nowUnixSecs - 21600; // 6h

  const upstreamIds = useMemo(() => {
    return upstreams.data?.upstreams.map((u) => u.id).join(',') ?? '';
  }, [upstreams.data]);

  const quotaSeries = useSubscriptionQuotaSeries({
    upstreamIds,
    windows: '5h,7d',
    source: 'merged',
    sinceUnixSecs,
    untilUnixSecs: nowUnixSecs,
    bucketSecs: 300,
  });

  const quotaLatest = useSubscriptionQuotaLatest({
    upstreamIds,
    windows: '5h,7d',
    source: 'merged',
  });

  // /admin/v1/status reports per-upstream runtime state incl. OAuth binding.
  const status = useStatus();
  const statusByUpstreamId = useMemo(() => {
    const m = new Map<
      string,
      { status: string; last_apply_error: string | null }
    >();
    for (const u of status.data?.upstreams ?? []) {
      m.set(u.id, { status: u.status, last_apply_error: u.last_apply_error });
    }
    return m;
  }, [status.data]);
  const [createOpen, setCreateOpen] = useState(false);
  // While an OAuth upstream is created but its /oauth/complete hasn't succeeded
  // yet, the row exists in the DB (we need its id for /oauth/start) but should
  // be hidden from the list. CreateUpstreamModal calls the setter on POST
  // success and clears it on completion/cancel.
  const [pendingCreatedId, setPendingCreatedId] = useState<string | null>(null);

  const quotaDataById = useMemo(() => {
    const map = new Map<
      string,
      { i: number; val5h: number | null; val7d: number | null }[]
    >();
    for (const s of quotaSeries.data?.series ?? []) {
      const uId = s.upstream_id;
      if (!map.has(uId)) {
        map.set(uId, []);
      }
      const arr = map.get(uId)!;

      for (let i = 0; i < s.buckets.length; i++) {
        if (!arr[i]) {
          arr[i] = { i, val5h: null, val7d: null };
        }
        const b = s.buckets[i]!;
        if (s.window === '5h') {
          arr[i]!.val5h =
            b.utilization_last != null ? b.utilization_last * 100 : null;
        } else if (s.window === '7d') {
          arr[i]!.val7d =
            b.utilization_last != null ? b.utilization_last * 100 : null;
        }
      }
    }
    return map;
  }, [quotaSeries.data]);

  const visibleUpstreams = useMemo(
    () =>
      (upstreams.data?.upstreams ?? []).filter(
        (u) => u.id !== pendingCreatedId,
      ),
    [upstreams.data, pendingCreatedId],
  );

  const selected = visibleUpstreams.find((u) => u.id === selectedId) ?? null;
  const select = (id: string | undefined) =>
    navigate({ search: id ? { selectedId: id } : {} });

  useEffect(() => {
    if (!upstreams.isLoading && !selected && visibleUpstreams.length > 0) {
      if (window.matchMedia('(min-width: 768px)').matches) {
        navigate({
          search: { selectedId: visibleUpstreams[0].id },
          replace: true,
        });
      }
    }
  }, [upstreams.isLoading, selected, visibleUpstreams, navigate]);

  return (
    <div className="h-[calc(100dvh-3rem)] min-h-0 flex w-full max-w-[120rem] mx-auto">
      {/* List pane */}
      <aside
        className={cx(
          'border-r border-subtle flex flex-col min-h-0 w-full md:w-[360px] shrink-0',
          selected ? 'hidden md:flex' : 'flex',
        )}
      >
        <div className="h-12 px-4 flex items-center justify-between border-b border-subtle shrink-0">
          <div>
            <h1 className="text-sm font-medium">Upstreams</h1>
            <p className="text-[11px] text-text-faint">
              {visibleUpstreams.length} total
            </p>
          </div>
          <Button
            id="btn-new-upstream"
            size="sm"
            variant="primary"
            iconLeft={<Plus className="w-3 h-3" />}
            onClick={() => setCreateOpen(true)}
          >
            New
          </Button>
        </div>
        <div className="flex-1 overflow-y-auto p-2 pb-8 space-y-1">
          {upstreams.isLoading ? (
            Array.from({ length: 3 }).map((_, i) => (
              <Skeleton key={i} className="h-20" />
            ))
          ) : visibleUpstreams.length ? (
            visibleUpstreams.map((u) => {
              const chartData = quotaDataById.get(u.id) ?? [];
              const latest = quotaLatest.data?.upstreams.find(
                (l) => l.upstream_id === u.id,
              );
              const w5h = latest?.windows.find((w) => w.window === '5h');
              const w7d = latest?.windows.find((w) => w.window === '7d');

              const getPctColor = (util: number | null | undefined) => {
                if (util == null) return 'text-text-faint';
                if (util < 0.7) return 'text-green-400';
                if (util < 0.9) return 'text-amber-400';
                return 'text-red-400';
              };

              const runtimeStatus = statusByUpstreamId.get(u.id);
              const dotTone = !u.enabled
                ? 'neutral'
                : runtimeStatus?.status === 'error'
                  ? 'danger'
                  : runtimeStatus?.status === 'active'
                    ? 'ok'
                    : 'neutral';
              return (
                <button
                  key={u.id}
                  type="button"
                  onClick={() => select(u.id)}
                  className={cx(
                    'w-full text-left p-3 rounded-sm border transition-colors',
                    u.id === selectedId
                      ? 'border-accent/40 bg-accent/5 text-text'
                      : 'border-subtle hover:bg-overlay-3 text-text',
                  )}
                  title={runtimeStatus?.last_apply_error ?? undefined}
                >
                  <div className="flex items-center justify-between gap-2 mb-1.5">
                    <div className="flex items-center gap-2 min-w-0">
                      <span className={cx('status-dot', dotTone)} />
                      <span className="font-medium text-sm truncate">
                        {u.name}
                      </span>
                    </div>
                    <div className="flex items-center gap-2 text-[10px] font-mono shrink-0">
                      <span className={getPctColor(w5h?.utilization)}>
                        {w5h?.utilization != null
                          ? `${(w5h.utilization * 100).toFixed(1)}%`
                          : '—'}
                      </span>
                      <span className="text-text-faint">/</span>
                      <span className={getPctColor(w7d?.utilization)}>
                        {w7d?.utilization != null
                          ? `${(w7d.utilization * 100).toFixed(1)}%`
                          : '—'}
                      </span>
                    </div>
                  </div>
                  <div className="flex items-center gap-3 mt-2 pb-1">
                    <div className="flex-1 min-w-0" style={{ minHeight: 32 }}>
                      <QuotaMiniChart
                        data={chartData}
                        color5h={getUpstreamColor(u.id).line5h}
                        color7d={getUpstreamColor(u.id).line7d}
                      />
                    </div>
                    <div className="text-right text-[11px] text-text-faint shrink-0">
                      <div className="font-mono tabular-nums">
                        rev {u.revision}
                      </div>
                    </div>
                  </div>
                </button>
              );
            })
          ) : (
            <EmptyState
              title="No upstreams"
              description="Create your first upstream to start routing traffic."
              action={
                <Button variant="primary" onClick={() => setCreateOpen(true)}>
                  New upstream
                </Button>
              }
            />
          )}
        </div>
      </aside>

      {/* Detail pane */}
      <section
        className={cx(
          'flex-1 flex flex-col bg-bg min-w-0',
          selected ? 'flex' : 'hidden md:flex',
        )}
      >
        {selected ? (
          <DetailView upstream={selected} onBack={() => select(undefined)} />
        ) : (
          <div className="flex-1 flex items-center justify-center">
            <EmptyState
              title="Select an upstream"
              description="Pick an upstream from the list to see its configuration, OAuth state, and recent requests."
            />
          </div>
        )}
      </section>

      <CreateUpstreamModal
        open={createOpen}
        onOpenChange={setCreateOpen}
        onPendingCreatedIdChange={setPendingCreatedId}
      />
    </div>
  );
}

// Mirrors credentials.tsx fmtRelExpiry "warn" window; never synthesize success.
const OAUTH_EXPIRING_SOON_SECS = 600;

type OAuthBadge = { tone: 'ok' | 'warn' | 'danger' | 'neutral'; label: string };

function oauthBadge(entry: {
  status: string;
  expires_at_unix_secs: number | null;
}): OAuthBadge {
  if (entry.status === 'corrupted')
    return { tone: 'danger', label: 'Refresh failed' };
  if (entry.status === 'missing')
    return { tone: 'neutral', label: 'No credentials' };
  const exp = entry.expires_at_unix_secs;
  if (exp == null) return { tone: 'neutral', label: 'Unknown' };
  const now = Math.floor(Date.now() / 1000);
  if (exp <= now) return { tone: 'danger', label: 'Expired' };
  if (exp - now < OAUTH_EXPIRING_SOON_SECS)
    return { tone: 'warn', label: 'Expiring soon' };
  return { tone: 'ok', label: 'Active' };
}

function DetailView({
  upstream,
  onBack,
}: {
  upstream: Upstream;
  onBack: () => void;
}) {
  const toggle = useToggleUpstream();
  const del = useDeleteUpstream();
  const update = useUpdateUpstream();
  const oauthStart = useOAuthStart();
  const oauthComplete = useOAuthComplete();
  const upstreamOAuthQ = useUpstreamOAuthStatus(
    upstream.kind === 'anthropic_oauth' ? upstream.id : null,
  );
  const statusQ = useStatus();
  const principalNameMap = usePrincipalNameMap();
  const upstreamRuntimeStatus = useMemo(
    () => statusQ.data?.upstreams.find((u) => u.id === upstream.id) ?? null,
    [statusQ.data, upstream.id],
  );
  const [editOpen, setEditOpen] = useState(false);
  const [oauthOpen, setOauthOpen] = useState(false);
  const [oauthState, setOauthState] = useState<{
    authorize_url?: string;
    state_token?: string;
    code?: string;
  }>({});
  const [name, setName] = useState(upstream.name);
  const [baseUrl, setBaseUrl] = useState(upstream.base_url ?? '');
  const [editApiKeyValue, setEditApiKeyValue] = useState('');
  const [editApiKeyEnv, setEditApiKeyEnv] = useState(
    upstream.api_key_env ?? '',
  );
  const [editUseEnvVar, setEditUseEnvVar] = useState(false);
  const [confirmDeleteOpen, setConfirmDeleteOpen] = useState(false);
  const [range, setRange] = useState<'1h' | '6h' | '24h' | '7d'>('7d');

  const nowUnixSecs = Math.floor(Date.now() / 1000);
  const sinceUnixSecs = useMemo(() => {
    switch (range) {
      case '1h':
        return nowUnixSecs - 3600;
      case '6h':
        return nowUnixSecs - 21600;
      case '24h':
        return nowUnixSecs - 86400;
      case '7d':
        return nowUnixSecs - 604800;
    }
  }, [range, nowUnixSecs]);
  // Backend rejects (until - since) / bucket_secs > max_points_per_series * 2.
  // Default max_points_per_series = 1000, so we target <= 500 points/series
  // for a 2-window query.
  const bucketSecsForRange = useMemo(() => {
    switch (range) {
      case '1h':
        return 60;
      case '6h':
        return 60;
      case '24h':
        return 300;
      case '7d':
        return 1800;
    }
  }, [range]);

  const quotaLatest = useSubscriptionQuotaLatest({
    upstreamIds: upstream.id,
    windows: '5h,7d,overage,7d_sonnet,7d_opus',
    source: 'merged',
  });
  const quotaSeries = useSubscriptionQuotaSeries({
    upstreamIds: upstream.id,
    windows: '5h,7d',
    source: 'merged',
    sinceUnixSecs,
    untilUnixSecs: nowUnixSecs,
    bucketSecs: bucketSecsForRange,
  });
  const quotaAnalysis = useSubscriptionQuotaAnalysis({
    upstreamIds: upstream.id,
    windows: '5h,7d',
    source: 'merged',
    sinceUnixSecs,
    untilUnixSecs: nowUnixSecs,
  });

  const chartData = useMemo(() => {
    if (!quotaSeries.data?.series.length) return { rows: [], markers: [] };

    const series = quotaSeries.data.series;
    const bucketsByTime = new Map<number, Record<string, any>>();
    const markers: { ts: number; kind: string }[] = [];

    for (const s of series) {
      const w = s.window;

      for (const b of s.buckets) {
        const ts = b.bucket_start_unix_secs;
        if (!bucketsByTime.has(ts)) {
          const date = new Date(ts * 1000);
          const label =
            range === '7d' || range === '24h'
              ? `${date.getMonth() + 1}/${date.getDate()} ${date.getHours()}h`
              : date.toTimeString().slice(0, 5);
          bucketsByTime.set(ts, { ts: label, unix: ts });
        }
        const row = bucketsByTime.get(ts)!;
        row[w] = b.utilization_last != null ? b.utilization_last * 100 : null;
      }

      for (const m of s.markers) {
        if (m.at_unix_secs) {
          markers.push({ ts: m.at_unix_secs, kind: m.kind });
        }
      }
    }

    const rows = Array.from(bucketsByTime.values()).sort(
      (a, b) => a.unix - b.unix,
    );
    return { rows, markers };
  }, [quotaSeries.data, range]);

  useEffect(() => {
    if (editOpen) {
      setName(upstream.name);
      setBaseUrl(upstream.base_url ?? '');
      setEditApiKeyValue('');
      setEditApiKeyEnv(upstream.api_key_env ?? '');
      setEditUseEnvVar(false);
    }
  }, [editOpen, upstream]);

  // The backend `/admin/events/recent?upstream=` param only accepts the
  // RequestEventUpstream class enum (`anthropic_direct` / `custom_anthropic_spec`),
  // not an upstream display name or id. Passing the name returns 400
  // `invalid_upstream`, so we fetch unfiltered and narrow client-side by
  // `upstream_name`.
  const recent = useRecentEvents({ limit: '50' });
  const recentForUpstream = useMemo(
    () =>
      (recent.data?.events ?? [])
        .filter((e) => e.upstream_name === upstream.name)
        .slice(0, 5),
    [recent.data, upstream.name],
  );

  return (
    <>
      <header className="px-4 md:px-6 py-4 border-b border-subtle flex items-start justify-between gap-3 flex-wrap shrink-0">
        <div className="min-w-0">
          <button
            type="button"
            onClick={onBack}
            className="md:hidden inline-flex items-center gap-1 text-xs text-text-faint hover:text-text mb-1"
          >
            <ChevronLeft className="w-3 h-3" /> Back
          </button>
          <div className="flex items-center gap-3 flex-wrap">
            <h2 className="text-lg font-medium text-text truncate">
              {upstream.name}
            </h2>
            <StatusBadge
              tone={upstream.enabled ? 'ok' : 'neutral'}
              label={upstream.enabled ? 'Enabled' : 'Disabled'}
            />
            <Badge tone="mono">{upstream.kind}</Badge>
          </div>
          <div className="text-xs text-text-faint font-mono mt-0.5">
            ID {upstream.id} · rev {upstream.revision}
          </div>
        </div>
        <div className="flex items-center gap-2 shrink-0">
          <Button
            size="sm"
            iconLeft={<Pencil className="w-3 h-3" />}
            onClick={() => setEditOpen(true)}
          >
            Edit
          </Button>
          <Button
            size="sm"
            onClick={() =>
              toggle.mutate(
                {
                  id: upstream.id,
                  enabled: !upstream.enabled,
                  revision: upstream.revision,
                },
                {
                  onSuccess: () =>
                    toast.success(
                      upstream.enabled
                        ? 'Upstream disabled'
                        : 'Upstream enabled',
                    ),
                },
              )
            }
          >
            {upstream.enabled ? 'Disable' : 'Enable'}
          </Button>
          <Button
            size="sm"
            variant="danger"
            iconLeft={<Trash2 className="w-3 h-3" />}
            onClick={() => setConfirmDeleteOpen(true)}
          >
            Delete
          </Button>
        </div>
      </header>

      <div className="flex-1 overflow-y-auto p-4 md:p-6 pb-8 md:pb-12 space-y-6">
        <Section title="Subscription Quota">
          <Card>
            <CardHeader
              title="Quota Forecast"
              action={
                <div className="flex flex-wrap bg-overlay-2 border border-subtle rounded-sm p-0.5 max-w-full">
                  {(['1h', '6h', '24h', '7d'] as const).map((r) => (
                    <button
                      key={r}
                      type="button"
                      onClick={() => setRange(r)}
                      className={cx(
                        'px-2.5 h-7 text-xs rounded-sm transition-colors',
                        r === range
                          ? 'bg-overlay-6 text-text'
                          : 'text-text-faint hover:text-text',
                      )}
                    >
                      {r}
                    </button>
                  ))}
                </div>
              }
            />
            <CardBody className="p-3 pt-1">
              <div className="w-full h-[240px]" style={{ minWidth: 0 }}>
                {quotaSeries.isLoading ? (
                  <div className="h-full flex items-center justify-center text-text-faint text-sm">
                    Loading…
                  </div>
                ) : !chartData.rows.length ? (
                  <EmptyState title="No data in range" />
                ) : (
                  <ResponsiveContainer width="100%" height={240} debounce={150}>
                    <AreaChart
                      data={chartData.rows}
                      margin={{ top: 8, right: 24, bottom: 4, left: 0 }}
                    >
                      <CartesianGrid stroke="var(--color-border)" />
                      <XAxis
                        dataKey="unix"
                        tick={{
                          fill: 'var(--color-text-faint)',
                          fontSize: 10,
                          fontFamily: 'Geist Mono',
                        }}
                        tickFormatter={(val) => {
                          const d = new Date(val * 1000);
                          return range === '7d' || range === '24h'
                            ? `${d.getMonth() + 1}/${d.getDate()} ${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`
                            : `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`;
                        }}
                        axisLine={false}
                        tickLine={false}
                        minTickGap={40}
                      />
                      <YAxis
                        tick={{
                          fill: 'var(--color-text-faint)',
                          fontSize: 10,
                          fontFamily: 'Geist Mono',
                        }}
                        tickFormatter={(val) => `${val}%`}
                        axisLine={false}
                        tickLine={false}
                        width={40}
                        domain={[0, 100]}
                        allowDataOverflow={false}
                      />
                      <Tooltip
                        cursor={{
                          stroke: 'var(--color-accent)',
                          strokeWidth: 1,
                          strokeOpacity: 0.3,
                        }}
                        content={({ active, payload, label }) => {
                          if (!active || !payload?.length) return null;
                          return (
                            <div
                              style={{
                                background: 'var(--color-bg-sub)',
                                border: '1px solid var(--color-border)',
                                borderRadius: 2,
                                color: 'var(--color-text)',
                                fontSize: 11,
                                fontFamily: 'Geist Mono Variable, monospace',
                                padding: '6px 10px',
                                boxShadow: '0 4px 12px rgba(0,0,0,0.18)',
                                minWidth: 80,
                              }}
                            >
                              <div
                                style={{
                                  color: 'var(--color-text-faint)',
                                  marginBottom: 4,
                                }}
                              >
                                {label}
                              </div>
                              {payload.map((p, i) => (
                                <div
                                  key={i}
                                  style={{
                                    color: 'var(--color-text)',
                                    padding: '1px 0',
                                    display: 'flex',
                                    justifyContent: 'space-between',
                                    gap: 8,
                                  }}
                                >
                                  <span
                                    style={{
                                      color:
                                        typeof p.color === 'string'
                                          ? p.color
                                          : 'var(--color-text)',
                                    }}
                                  >
                                    {String(p.dataKey)}
                                  </span>
                                  <span
                                    style={{
                                      fontVariantNumeric: 'tabular-nums',
                                    }}
                                  >
                                    {typeof p.value === 'number'
                                      ? `${p.value.toFixed(1)}%`
                                      : '—'}
                                  </span>
                                </div>
                              ))}
                            </div>
                          );
                        }}
                      />
                      <Legend
                        wrapperStyle={{
                          fontSize: 11,
                          fontFamily: 'Geist Mono',
                          color: 'var(--color-text-muted)',
                        }}
                        content={() => (
                          <div className="flex flex-wrap items-center justify-center gap-4 mt-2">
                            <div className="flex items-center gap-1.5">
                              <div
                                className="w-3 h-0.5"
                                style={{
                                  backgroundColor: getUpstreamColor(upstream.id)
                                    .line5h,
                                }}
                              />
                              <span className="text-[11px] text-text-muted font-mono">
                                5h
                              </span>
                            </div>
                            <div className="flex items-center gap-1.5">
                              <div
                                className="w-3 h-0.5 border-t border-dashed"
                                style={{
                                  borderColor: getUpstreamColor(upstream.id)
                                    .line7d,
                                }}
                              />
                              <span className="text-[11px] text-text-muted font-mono">
                                7d
                              </span>
                            </div>
                          </div>
                        )}
                      />
                      {chartData.markers.map((m, i) => (
                        <ReferenceLine
                          key={`marker-${i}`}
                          x={chartData.rows.find((r) => r.unix === m.ts)?.ts}
                          stroke={getUpstreamColor(upstream.id).line5h}
                          strokeOpacity={0.5}
                          strokeDasharray="3 3"
                        />
                      ))}
                      <Area
                        type="stepAfter"
                        dataKey="7d"
                        stroke={getUpstreamColor(upstream.id).line7d}
                        strokeWidth={1.4}
                        strokeDasharray="3 3"
                        fill="none"
                        isAnimationActive={false}
                        connectNulls={false}
                      />
                      <Area
                        type="stepAfter"
                        dataKey="5h"
                        stroke={getUpstreamColor(upstream.id).line5h}
                        strokeWidth={1.4}
                        fill="none"
                        isAnimationActive={false}
                        connectNulls={false}
                      />
                    </AreaChart>
                  </ResponsiveContainer>
                )}
              </div>
            </CardBody>
          </Card>

          {(() => {
            const latest = quotaLatest.data?.upstreams[0];
            const analysis = quotaAnalysis.data?.upstreams[0];
            const a5h = analysis?.windows.find((w) => w.window === '5h');
            const caveats = a5h?.caveats ?? [];

            const formatEta = (secs: number | null | undefined) => {
              if (secs == null) return '—';
              if (secs > 86400)
                return `${Math.floor(secs / 86400)}d ${Math.floor((secs % 86400) / 3600)}h`;
              if (secs > 3600)
                return `${Math.floor(secs / 3600)}h ${Math.floor((secs % 3600) / 60)}m`;
              return `${Math.floor(secs / 60)}m`;
            };

            return (
              <div className="space-y-4">
                {caveats.length > 0 && (
                  <div className="bg-amber-500/10 border border-amber-500/20 rounded-sm p-3 text-xs text-amber-400 flex flex-col gap-1">
                    <div className="font-medium flex items-center gap-1.5">
                      <Info className="w-3.5 h-3.5" />
                      Analysis Caveats
                    </div>
                    <ul className="list-disc list-inside opacity-90 ml-1">
                      {caveats.map((c, i) => (
                        <li key={i}>{c}</li>
                      ))}
                    </ul>
                  </div>
                )}

                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-5 gap-3">
                  {['5h', '7d', 'overage', '7d_sonnet', '7d_opus'].map((w) => {
                    const snap = latest?.windows.find((x) => x.window === w);
                    if (!snap && w !== '5h' && w !== '7d') return null;
                    return (
                      <Card key={w}>
                        <CardBody className="p-3 flex flex-col gap-1.5">
                          <div className="flex items-center justify-between">
                            <span className="text-[11px] uppercase tracking-wider text-text-faint">
                              {w}
                            </span>
                            <StatusBadge
                              tone={
                                snap?.state === 'fresh'
                                  ? 'ok'
                                  : snap?.state === 'stale'
                                    ? 'warn'
                                    : 'neutral'
                              }
                              label={snap?.state ?? 'missing'}
                            />
                          </div>
                          <div className="text-xl font-medium tabular-nums">
                            {snap?.utilization != null
                              ? `${(snap.utilization * 100).toFixed(1)}%`
                              : '—'}
                          </div>
                          <div className="text-[10px] text-text-faint font-mono">
                            {snap?.resets_at_unix_secs
                              ? `Resets: ${new Date(snap.resets_at_unix_secs * 1000).toLocaleString()}`
                              : 'No reset info'}
                          </div>
                          {w === 'overage' && snap?.extra_usage_enabled && (
                            <div className="text-[10px] text-text-faint font-mono mt-1">
                              Used: ${snap.extra_usage_used_credits?.toFixed(2)}{' '}
                              / ${snap.extra_usage_monthly_limit?.toFixed(2)}
                            </div>
                          )}
                        </CardBody>
                      </Card>
                    );
                  })}
                </div>

                <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                  <Card>
                    <CardHeader title="Time to 5h limit" />
                    <CardBody className="p-3 grid grid-cols-2 gap-4">
                      <div>
                        <div className="text-[11px] uppercase tracking-wider text-text-faint mb-1">
                          Account-wide
                        </div>
                        <div className="text-lg font-medium tabular-nums">
                          {formatEta(
                            a5h?.actual_account_burn.eta_to_limit_secs,
                          )}
                        </div>
                        <div className="text-[10px] text-text-faint mt-1">
                          Confidence:{' '}
                          <span className="text-text">
                            {a5h?.actual_account_burn.confidence ?? '—'}
                          </span>
                        </div>
                      </div>
                      <div>
                        <div className="text-[11px] uppercase tracking-wider text-text-faint mb-1">
                          This proxy
                        </div>
                        <div className="text-lg font-medium tabular-nums">
                          {formatEta(
                            a5h?.proxy_projected_burn.eta_to_limit_secs,
                          )}
                        </div>
                        <div className="text-[10px] text-text-faint mt-1">
                          Confidence:{' '}
                          <span className="text-text">
                            {a5h?.proxy_projected_burn.confidence ?? '—'}
                          </span>
                        </div>
                      </div>
                    </CardBody>
                  </Card>

                  <Card>
                    <CardHeader title="Quota deficit" />
                    <CardBody className="p-3">
                      {a5h?.deficit ? (
                        <div className="flex flex-col gap-2">
                          <div className="flex items-center justify-between">
                            <span className="text-sm text-text-faint">
                              Shortfall
                            </span>
                            <span className="font-mono text-amber-400">
                              {Math.round(
                                a5h.deficit.shortfall_tokens,
                              ).toLocaleString()}{' '}
                              tokens
                            </span>
                          </div>
                          <div className="flex items-center justify-between">
                            <span className="text-sm text-text-faint">
                              Recommended Multiplier
                            </span>
                            <span className="font-mono text-amber-400">
                              {a5h.deficit.recommended_multiplier}x
                            </span>
                          </div>
                          <div className="text-[10px] text-text-faint mt-1">
                            Confidence:{' '}
                            <span className="text-text">
                              {a5h.deficit.confidence}
                            </span>
                          </div>
                        </div>
                      ) : (
                        <div className="text-sm text-text-faint h-full flex items-center">
                          No deficit detected or insufficient data.
                        </div>
                      )}
                    </CardBody>
                  </Card>
                </div>
              </div>
            );
          })()}
        </Section>

        <Card>
          <CardHeader title="Configuration" />
          <CardBody className="grid grid-cols-1 md:grid-cols-2 gap-4 text-sm">
            <div>
              <div className="text-[11px] text-text-faint uppercase tracking-wider">
                Kind
              </div>
              <div className="font-mono mt-0.5">{upstream.kind}</div>
            </div>
            <div>
              <div className="text-[11px] text-text-faint uppercase tracking-wider">
                Base URL
              </div>
              <div className="font-mono mt-0.5 break-all">
                {upstream.base_url ?? '—'}
              </div>
            </div>
            {upstream.api_key_env ? (
              <div>
                <div className="text-[11px] text-text-faint uppercase tracking-wider">
                  API Key Env
                </div>
                <div className="font-mono mt-0.5">{upstream.api_key_env}</div>
              </div>
            ) : null}
            {upstream.shape_plugin ? (
              <div>
                <div className="text-[11px] text-text-faint uppercase tracking-wider">
                  Shape Plugin
                </div>
                <div className="font-mono mt-0.5">
                  {upstream.shape_plugin.wasm_registry_id}
                </div>
              </div>
            ) : null}
          </CardBody>
        </Card>

        {upstream.kind === 'anthropic_oauth'
          ? (() => {
              const principalEntry = upstreamOAuthQ.data?.has_credentials
                ? upstreamOAuthQ.data
                : null;
              const runtimeStatus = upstreamRuntimeStatus?.status;
              const hasBoundToken = Boolean(principalEntry);
              const badge: OAuthBadge = upstreamOAuthQ.isLoading
                ? { tone: 'neutral', label: 'Loading' }
                : principalEntry
                  ? oauthBadge(principalEntry)
                  : { tone: 'neutral', label: 'Not connected' };
              return (
                <Card>
                  <CardHeader
                    title="OAuth Status"
                    subtitle={
                      hasBoundToken ? 'Bound on this upstream' : 'Not connected'
                    }
                    action={
                      <Button
                        size="sm"
                        iconLeft={<KeyRound className="w-3 h-3" />}
                        onClick={() => {
                          oauthStart.mutate(upstream.id, {
                            onSuccess: (res) => {
                              setOauthState({
                                authorize_url: res.authorize_url,
                                state_token: res.state_token,
                                code: '',
                              });
                              setOauthOpen(true);
                            },
                          });
                        }}
                      >
                        {hasBoundToken
                          ? 'Reconnect via OAuth'
                          : 'Connect via OAuth'}
                      </Button>
                    }
                  />
                  <CardBody className="text-sm">
                    {statusQ.isLoading ? (
                      <Skeleton className="h-12" />
                    ) : !hasBoundToken ? (
                      <div className="flex flex-wrap items-center gap-3">
                        <StatusBadge tone={badge.tone} label={badge.label} />
                        <p className="text-xs text-text-faint">
                          Run "Connect via OAuth" to authorize this upstream.
                        </p>
                      </div>
                    ) : (
                      <div className="space-y-3">
                        <div className="flex flex-wrap items-center gap-2">
                          <StatusBadge tone={badge.tone} label={badge.label} />
                          <span className="text-[11px] text-text-faint font-mono">
                            runtime: {runtimeStatus}
                          </span>
                        </div>
                        {principalEntry ? (
                          <>
                            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                              <div>
                                <div className="text-[11px] text-text-faint uppercase tracking-wider">
                                  Expires
                                </div>
                                <div className="font-mono mt-0.5">
                                  <RelativeTime
                                    ts={
                                      principalEntry.expires_at_unix_secs
                                        ? new Date(
                                            principalEntry.expires_at_unix_secs *
                                              1000,
                                          )
                                        : null
                                    }
                                  />
                                </div>
                              </div>
                              <div>
                                <div className="text-[11px] text-text-faint uppercase tracking-wider">
                                  Refresh token
                                </div>
                                <div className="mt-0.5">
                                  {principalEntry.refresh_token_present ? (
                                    'present'
                                  ) : (
                                    <span className="text-amber-400">
                                      missing
                                    </span>
                                  )}
                                </div>
                              </div>
                            </div>
                            {principalEntry.scopes.length ? (
                              <div>
                                <div className="text-[11px] text-text-faint uppercase tracking-wider">
                                  Scopes
                                </div>
                                <div className="font-mono mt-0.5 break-all text-xs">
                                  {principalEntry.scopes.join(', ')}
                                </div>
                              </div>
                            ) : null}
                          </>
                        ) : (
                          <p className="text-xs text-text-faint">
                            Token is bound on the upstream but no credential
                            details are available right now.
                          </p>
                        )}
                      </div>
                    )}
                  </CardBody>
                </Card>
              );
            })()
          : null}

        <Card>
          <CardHeader
            title="Recent Requests"
            subtitle={`Last 5 against ${upstream.name}`}
          />
          <div className="overflow-x-auto">
            <table className="w-full font-mono text-xs">
              <thead className="table-header sticky top-0 z-10">
                <tr className="text-[10px] uppercase tracking-wider">
                  <th className="text-left px-3 py-2">Time</th>
                  <th className="text-left px-3 py-2">Principal</th>
                  <th className="text-left px-3 py-2">Model</th>
                  <th className="text-right px-3 py-2">Status</th>
                  <th className="text-right px-3 py-2">Latency</th>
                </tr>
              </thead>
              <tbody>
                {recentForUpstream.length ? (
                  recentForUpstream.map((e) => (
                    <tr
                      key={e.request_id}
                      className="border-b border-row hover:bg-overlay-1"
                    >
                      <td className="px-3 py-2 text-text-faint whitespace-nowrap">
                        <RelativeTime ts={eventTime(e)} />
                      </td>
                      <td className="px-3 py-2">
                        {principalNameMap.get(e.principal_id ?? '') ??
                          e.principal_id ??
                          '—'}
                      </td>
                      <td className="px-3 py-2 text-text-faint truncate max-w-[200px]">
                        {e.model ?? '—'}
                      </td>
                      <td
                        className={cx(
                          'px-3 py-2 text-right',
                          e.status >= 500
                            ? 'text-red-400'
                            : e.status >= 400
                              ? 'text-amber-400'
                              : 'text-green-400',
                        )}
                      >
                        {e.status}
                      </td>
                      <td className="px-3 py-2 text-right tabular-nums">
                        {e.duration_ms}ms
                      </td>
                    </tr>
                  ))
                ) : (
                  <tr>
                    <td
                      colSpan={5}
                      className="px-3 py-6 text-center text-text-faint text-xs"
                    >
                      No recent requests for this upstream
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </Card>
      </div>

      <Modal
        open={editOpen}
        onOpenChange={setEditOpen}
        title="Edit upstream"
        footer={
          <>
            <Button onClick={() => setEditOpen(false)}>Cancel</Button>
            <Button
              variant="primary"
              onClick={() => {
                const trimmedName = name.trim();
                const trimmedBase = baseUrl.trim();
                const trimmedKeyValue = editApiKeyValue.trim();
                const trimmedKeyEnv = editApiKeyEnv.trim();
                const isApiKey = upstream.kind === 'anthropic_api_key';
                update.mutate(
                  {
                    id: upstream.id,
                    body: {
                      name: trimmedName,
                      base_url: trimmedBase === '' ? null : trimmedBase,
                      api_key_value:
                        isApiKey && !editUseEnvVar && trimmedKeyValue !== ''
                          ? trimmedKeyValue
                          : null,
                      api_key_env:
                        isApiKey && editUseEnvVar && trimmedKeyEnv !== ''
                          ? trimmedKeyEnv
                          : null,
                    },
                    revision: upstream.revision,
                  },
                  {
                    onSuccess: () => {
                      toast.success('Upstream updated');
                      setEditOpen(false);
                    },
                  },
                );
              }}
            >
              Save
            </Button>
          </>
        }
      >
        <div className="space-y-3">
          <Field label="Name" required>
            <input
              className={INPUT_CLASS}
              value={name}
              onChange={(e) => setName(e.target.value)}
            />
          </Field>
          <Field label="Base URL">
            <input
              className={INPUT_CLASS}
              value={baseUrl}
              onChange={(e) => setBaseUrl(e.target.value)}
            />
          </Field>
          {upstream.kind === 'anthropic_api_key' ? (
            editUseEnvVar ? (
              <Field
                label="API Key Env Var"
                hint="Name of an env var on the server holding the new API key. Leave unchanged to keep the existing credential."
              >
                <input
                  className={`${INPUT_CLASS} font-mono`}
                  value={editApiKeyEnv}
                  onChange={(e) => setEditApiKeyEnv(e.target.value)}
                  placeholder="ANTHROPIC_API_KEY"
                />
                <button
                  type="button"
                  className="mt-1 text-[11px] text-text-faint hover:text-text underline underline-offset-2"
                  onClick={() => setEditUseEnvVar(false)}
                >
                  Paste new API key instead
                </button>
              </Field>
            ) : (
              <Field
                label="Rotate API Key"
                hint="Paste a new API key to replace the stored one. Leave blank to keep the existing credential."
              >
                <input
                  type="password"
                  autoComplete="off"
                  spellCheck={false}
                  className={`${INPUT_CLASS} font-mono`}
                  value={editApiKeyValue}
                  onChange={(e) => setEditApiKeyValue(e.target.value)}
                  placeholder="sk-ant-..."
                />
                <button
                  type="button"
                  className="mt-1 text-[11px] text-text-faint hover:text-text underline underline-offset-2"
                  onClick={() => setEditUseEnvVar(true)}
                >
                  Use env var instead
                </button>
              </Field>
            )
          ) : null}
        </div>
      </Modal>

      <Modal
        open={oauthOpen}
        onOpenChange={setOauthOpen}
        title="OAuth Authorization"
        description="Open the authorize URL, then paste the code below."
        size="lg"
        footer={
          <>
            <Button onClick={() => setOauthOpen(false)}>Cancel</Button>
            <Button
              variant="primary"
              disabled={!oauthState.code || !oauthState.state_token}
              onClick={() => {
                oauthComplete.mutate(
                  {
                    id: upstream.id,
                    state_token: oauthState.state_token!,
                    code: oauthState.code!,
                  },
                  {
                    onSuccess: () => {
                      toast.success('OAuth connected');
                      setOauthOpen(false);
                    },
                  },
                );
              }}
            >
              Complete
            </Button>
          </>
        }
      >
        <div className="space-y-3">
          <p className="text-xs text-text-faint">
            1. Open the authorization URL below. 2. Approve access. 3. Copy the
            returned code and paste it here.
          </p>
          <Field label="Authorize URL">
            <code className="block p-2 text-xs font-mono bg-overlay-2 border border-subtle rounded-sm break-all select-all">
              {oauthState.authorize_url ?? ''}
            </code>
            <div className="mt-2">
              <Button
                size="sm"
                variant="primary"
                disabled={!oauthState.authorize_url}
                iconLeft={<ExternalLink className="w-3 h-3" />}
                onClick={() => {
                  if (oauthState.authorize_url) {
                    window.open(
                      oauthState.authorize_url,
                      '_blank',
                      'noopener,noreferrer',
                    );
                  }
                }}
              >
                Open authorization URL
              </Button>
            </div>
          </Field>
          <Field label="State Token">
            <code className="block p-2 text-xs font-mono bg-overlay-2 border border-subtle rounded-sm break-all select-all">
              {oauthState.state_token ?? ''}
            </code>
          </Field>
          <Field label="Authorization Code" required>
            <input
              className={`${INPUT_CLASS} font-mono`}
              value={oauthState.code ?? ''}
              onChange={(e) =>
                setOauthState((s) => ({ ...s, code: e.target.value }))
              }
              placeholder="paste code…"
            />
          </Field>
        </div>
      </Modal>

      <ConfirmDialog
        open={confirmDeleteOpen}
        onOpenChange={setConfirmDeleteOpen}
        title="Delete upstream?"
        description={
          <>
            <span className="font-mono">{upstream.name}</span> will be
            permanently removed. This cannot be undone.
          </>
        }
        confirmLabel="Delete"
        destructive
        onConfirm={() =>
          del.mutate(
            { id: upstream.id, revision: upstream.revision },
            {
              onSuccess: () => {
                toast.success('Upstream deleted');
                onBack();
              },
            },
          )
        }
      />
    </>
  );
}

// Invariant: `succeeded` MUST be set before any onOpenChange(false) on a
// successful path, otherwise handleOpenChange will treat the close as a
// cancel and delete the freshly-created upstream.
function CreateUpstreamModal({
  open,
  onOpenChange,
  onPendingCreatedIdChange,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onPendingCreatedIdChange: (id: string | null) => void;
}) {
  const create = useCreateUpstream();
  const oauthStart = useOAuthStart();
  const oauthComplete = useOAuthComplete();
  const qc = useQueryClient();
  const [step, setStep] = useState<'configure' | 'authorize'>('configure');
  const [name, setName] = useState('');
  const [kind, setKind] = useState<
    'anthropic_api_key' | 'anthropic_oauth' | 'custom'
  >('anthropic_api_key');
  const [baseUrl, setBaseUrl] = useState('https://api.anthropic.com');
  const [apiKeyValue, setApiKeyValue] = useState('');
  const [apiKeyEnv, setApiKeyEnv] = useState('ANTHROPIC_API_KEY');
  const [useEnvVar, setUseEnvVar] = useState(false);
  const [created, setCreated] = useState<{
    id: string;
    revision: number;
  } | null>(null);
  const [authState, setAuthState] = useState<{
    authorize_url: string;
    state_token: string;
  } | null>(null);
  const [code, setCode] = useState('');
  const [succeeded, setSucceeded] = useState(false);

  useEffect(() => {
    if (!open) {
      setStep('configure');
      setName('');
      setKind('anthropic_api_key');
      setBaseUrl('https://api.anthropic.com');
      setApiKeyValue('');
      setApiKeyEnv('ANTHROPIC_API_KEY');
      setUseEnvVar(false);
      setCreated(null);
      setAuthState(null);
      setCode('');
      setSucceeded(false);
      onPendingCreatedIdChange(null);
    }
  }, [open, onPendingCreatedIdChange]);

  // Bypasses useDeleteUpstream so the global 409 toast stays quiet, and
  // refetches the current revision once on 409/428 before retrying. 404
  // on either step is treated as success (already deleted).
  const cleanupCreatedUpstream = async (id: string, revision: number) => {
    const tryDelete = (rev: number) =>
      deleteJson(`/admin/v1/upstreams/${id}`, { ifMatch: rev });
    try {
      await tryDelete(revision);
    } catch (err) {
      if (!(err instanceof ApiError)) throw err;
      if (err.status === 404) return;
      if (err.status !== 409 && err.status !== 428) throw err;
      const current = await getJson<Upstream>(
        `/admin/v1/upstreams/${id}`,
      ).catch((e) => {
        if (e instanceof ApiError && e.status === 404) return null;
        throw e;
      });
      if (!current) return;
      try {
        await tryDelete(current.revision);
      } catch (retryErr) {
        if (retryErr instanceof ApiError && retryErr.status === 404) return;
        throw retryErr;
      }
    }
  };

  const handleOpenChange = (next: boolean) => {
    if (!next && created && !succeeded) {
      const { id, revision } = created;
      cleanupCreatedUpstream(id, revision)
        .then(() => {
          toast.info('Upstream creation cancelled');
        })
        .catch((err) => {
          const message =
            err instanceof ApiError
              ? err.message || `Request failed (${err.status})`
              : err instanceof Error
                ? err.message
                : String(err);
          toast.error(`Failed to clean up unfinished upstream: ${message}`);
        })
        .finally(() => {
          qc.invalidateQueries({ queryKey: qk.upstreams });
        });
    }
    onOpenChange(next);
  };

  const submitConfigure = () => {
    const trimmedName = name.trim();
    const trimmedBase = baseUrl.trim();
    const trimmedEnv = apiKeyEnv.trim();
    const trimmedValue = apiKeyValue.trim();
    const isApiKey = kind === 'anthropic_api_key';
    create.mutate(
      {
        name: trimmedName,
        kind,
        base_url: trimmedBase === '' ? null : trimmedBase,
        api_key_value:
          isApiKey && !useEnvVar && trimmedValue !== '' ? trimmedValue : null,
        api_key_env:
          isApiKey && useEnvVar && trimmedEnv !== '' ? trimmedEnv : null,
      },
      {
        onSuccess: (upstream) => {
          if (kind !== 'anthropic_oauth') {
            setSucceeded(true);
            toast.success('Upstream created');
            onOpenChange(false);
            return;
          }
          setCreated({ id: upstream.id, revision: upstream.revision });
          onPendingCreatedIdChange(upstream.id);
          oauthStart.mutate(upstream.id, {
            onSuccess: (res) => {
              setCreated({ id: upstream.id, revision: res.revision });
              setAuthState({
                authorize_url: res.authorize_url,
                state_token: res.state_token,
              });
              setStep('authorize');
            },
          });
        },
      },
    );
  };

  const submitAuthorize = () => {
    if (!created || !authState || !code.trim()) return;
    oauthComplete.mutate(
      { id: created.id, state_token: authState.state_token, code: code.trim() },
      {
        onSuccess: () => {
          setSucceeded(true);
          toast.success('OAuth connected');
          onOpenChange(false);
        },
      },
    );
  };

  return (
    <Modal
      open={open}
      onOpenChange={handleOpenChange}
      title={step === 'configure' ? 'New upstream' : 'Authorize OAuth'}
      description={
        step === 'configure'
          ? 'Register an Anthropic API key, OAuth principal, or custom backend.'
          : 'Open the authorize URL, then paste the returned code below.'
      }
      size={step === 'authorize' ? 'lg' : undefined}
      footer={
        step === 'configure' ? (
          <>
            <Button onClick={() => handleOpenChange(false)}>Cancel</Button>
            <Button
              variant="primary"
              disabled={
                !name.trim() ||
                create.isPending ||
                oauthStart.isPending ||
                (kind === 'anthropic_api_key' &&
                  (useEnvVar ? !apiKeyEnv.trim() : !apiKeyValue.trim()))
              }
              onClick={submitConfigure}
            >
              {kind === 'anthropic_oauth' ? 'Create & Authorize' : 'Create'}
            </Button>
          </>
        ) : (
          <>
            <Button onClick={() => handleOpenChange(false)}>Cancel</Button>
            <Button
              variant="primary"
              disabled={
                !code.trim() ||
                !authState?.state_token ||
                oauthComplete.isPending
              }
              onClick={submitAuthorize}
            >
              Complete
            </Button>
          </>
        )
      }
    >
      {step === 'configure' ? (
        <div className="space-y-3">
          <Field
            label="Name"
            required
            hint="A unique label, e.g. anthropic-prod"
          >
            <input
              className={INPUT_CLASS}
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="anthropic-prod"
            />
          </Field>
          <Field label="Kind" required>
            <select
              className={INPUT_CLASS}
              value={kind}
              onChange={(e) => setKind(e.target.value as typeof kind)}
            >
              <option value="anthropic_api_key">Anthropic API Key</option>
              <option value="anthropic_oauth">Anthropic OAuth</option>
              <option value="custom">Custom backend</option>
            </select>
          </Field>
          <Field label="Base URL">
            <input
              className={INPUT_CLASS}
              value={baseUrl}
              onChange={(e) => setBaseUrl(e.target.value)}
            />
          </Field>
          {kind === 'anthropic_api_key' ? (
            useEnvVar ? (
              <Field
                label="API Key Env Var"
                hint="Name of an env var on the server holding the API key"
                required
              >
                <input
                  className={`${INPUT_CLASS} font-mono`}
                  value={apiKeyEnv}
                  onChange={(e) => setApiKeyEnv(e.target.value)}
                  placeholder="ANTHROPIC_API_KEY"
                />
                <button
                  type="button"
                  className="mt-1 text-[11px] text-text-faint hover:text-text underline underline-offset-2"
                  onClick={() => setUseEnvVar(false)}
                >
                  Paste API key instead
                </button>
              </Field>
            ) : (
              <Field
                label="API Key"
                hint="Pasted plaintext is encrypted at rest"
                required
              >
                <input
                  type="password"
                  autoComplete="off"
                  spellCheck={false}
                  className={`${INPUT_CLASS} font-mono`}
                  value={apiKeyValue}
                  onChange={(e) => setApiKeyValue(e.target.value)}
                  placeholder="sk-ant-..."
                />
                <button
                  type="button"
                  className="mt-1 text-[11px] text-text-faint hover:text-text underline underline-offset-2"
                  onClick={() => setUseEnvVar(true)}
                >
                  Use env var instead
                </button>
              </Field>
            )
          ) : null}
          {kind === 'anthropic_oauth' ? (
            <p className="text-xs text-text-faint">
              The dialog will advance to the authorization step after creation.
              Closing it before you paste the code removes the upstream.
            </p>
          ) : null}
        </div>
      ) : (
        <div className="space-y-3">
          <p className="text-xs text-text-faint">
            1. Open the authorization URL below. 2. Approve access. 3. Copy the
            returned code and paste it here.
          </p>
          <Field label="Authorize URL">
            <code className="block p-2 text-xs font-mono bg-overlay-2 border border-subtle rounded-sm break-all select-all">
              {authState?.authorize_url ?? ''}
            </code>
            <div className="mt-2">
              <Button
                size="sm"
                variant="primary"
                disabled={!authState?.authorize_url}
                iconLeft={<ExternalLink className="w-3 h-3" />}
                onClick={() => {
                  if (authState?.authorize_url) {
                    window.open(
                      authState.authorize_url,
                      '_blank',
                      'noopener,noreferrer',
                    );
                  }
                }}
              >
                Open authorization URL
              </Button>
            </div>
          </Field>
          <Field label="State Token">
            <code className="block p-2 text-xs font-mono bg-overlay-2 border border-subtle rounded-sm break-all select-all">
              {authState?.state_token ?? ''}
            </code>
          </Field>
          <Field label="Authorization Code" required>
            <input
              className={`${INPUT_CLASS} font-mono`}
              value={code}
              onChange={(e) => setCode(e.target.value)}
              placeholder="paste code…"
            />
          </Field>
        </div>
      )}
    </Modal>
  );
}
