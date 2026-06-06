// biome-ignore-all lint/suspicious/noExplicitAny: legitimate any for loose chart/metadata shapes
import { createFileRoute, useNavigate } from '@tanstack/react-router';
import {
  ChevronLeft,
  ExternalLink,
  Info,
  KeyRound,
  Plus,
  RefreshCw,
  Trash2,
} from 'lucide-react';
import { useEffect, useMemo, useState } from 'react';
import {
  Area,
  AreaChart,
  CartesianGrid,
  Label,
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
  Hint,
  INPUT_CLASS,
  Modal,
  Section,
  Skeleton,
  StatusBadge,
} from '../components/ui/primitives';
import { RelativeTime } from '../components/ui/RelativeTime';
import { ApiUsageCard } from '../components/upstreams/ApiUsageCard';
import { InlineNameEditor } from '../components/upstreams/InlineNameEditor';
import { SettingsCard } from '../components/upstreams/SettingsCard';
import { ApiError, eventTime } from '../lib/api';
import { getWindowColor, WINDOW_DURATION_SECS } from '../lib/colors';
import {
  type Upstream,
  useCompleteOauthDraft,
  useCreateFromOauthDraft,
  useCreateUpstream,
  useDeleteUpstream,
  useOAuthComplete,
  useOAuthStart,
  usePrincipalNameMap,
  useRecentEvents,
  useStartOauthDraft,
  useStatus,
  useSubscriptionQuotaAnalysis,
  useSubscriptionQuotaLatest,
  useSubscriptionQuotaSeries,
  useToggleUpstream,
  useTriggerSubscriptionMetadataRefresh,
  useUpstreamOAuthStatus,
  useUpstreamSubscriptionMetadata,
  useUpstreams,
  useUsage,
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

  const upstreamIds = useMemo(() => {
    return upstreams.data?.upstreams.map((u) => u.id).join(',') ?? '';
  }, [upstreams.data]);

  const quotaLatest = useSubscriptionQuotaLatest({
    upstreamIds,
    windows: '5h,7d,overage',
    source: 'merged',
  });

  const listUsage = useUsage('7d', 'hour', 'upstream');
  const usageByUpstreamId = useMemo(() => {
    const m = new Map<string, { cost_usd: number; tokens: number }>();
    for (const series of listUsage.data?.series ?? []) {
      let cost = 0;
      let tokens = 0;
      for (const b of series.buckets) {
        cost += (b.virtual_cost_micros ?? 0) / 1_000_000;
        tokens += (b.input_tokens ?? 0) + (b.output_tokens ?? 0);
      }
      m.set(series.key, { cost_usd: cost, tokens });
    }
    return m;
  }, [listUsage.data]);

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
              const latest = quotaLatest.data?.upstreams.find(
                (l) => l.upstream_id === u.id,
              );
              const runtimeStatus = statusByUpstreamId.get(u.id);
              const dotTone = !u.enabled
                ? 'neutral'
                : runtimeStatus?.status === 'error'
                  ? 'danger'
                  : runtimeStatus?.status === 'active'
                    ? 'ok'
                    : 'neutral';
              const barWindows = ['5h', '7d'];
              const overage = latest?.windows.find(
                (w) => w.window === 'overage',
              );
              if (
                overage &&
                (overage.extra_usage_enabled ||
                  overage.extra_usage_monthly_limit != null)
              ) {
                barWindows.push('overage');
              }
              return (
                <button
                  key={u.id}
                  type="button"
                  onClick={() => select(u.id)}
                  className={cx(
                    '@container w-full text-left p-3 rounded-sm border transition-colors',
                    u.id === selectedId
                      ? 'border-accent/40 bg-accent/5 text-text'
                      : 'border-subtle hover:bg-overlay-3 text-text',
                  )}
                  title={runtimeStatus?.last_apply_error ?? undefined}
                >
                  <div className="flex items-center justify-between gap-2 mb-2">
                    <div className="flex items-center gap-2 min-w-0">
                      <span className={cx('status-dot', dotTone)} />
                      <span className="font-medium text-sm truncate">
                        {u.name}
                      </span>
                    </div>
                    <div className="hidden @[240px]:flex items-center shrink-0">
                      <Badge tone="mono">{u.kind}</Badge>
                    </div>
                  </div>
                  {u.kind === 'anthropic_oauth' ? (
                    <div className="flex flex-col gap-1.5 w-full">
                      {barWindows.map((windowName) => {
                        const snap = latest?.windows.find(
                          (w) => w.window === windowName,
                        );
                        const color = getWindowColor(windowName);
                        const label = windowLabel(windowName);
                        let utilization = snap?.utilization ?? null;
                        if (
                          windowName === 'overage' &&
                          utilization == null &&
                          snap?.extra_usage_monthly_limit != null &&
                          snap.extra_usage_monthly_limit > 0 &&
                          snap.extra_usage_used_credits != null
                        ) {
                          utilization =
                            snap.extra_usage_used_credits /
                            snap.extra_usage_monthly_limit;
                        }
                        const pct =
                          utilization == null
                            ? '—%'
                            : `${(utilization * 100).toFixed(0)}%`;
                        const stateDot =
                          snap?.state === 'fresh'
                            ? 'bg-green-400'
                            : snap?.state === 'stale'
                              ? 'bg-amber-400'
                              : '';
                        return (
                          <div
                            key={windowName}
                            className="flex items-center gap-2 w-full text-[10px] font-mono"
                          >
                            <div className="w-8 shrink-0 text-text-faint truncate">
                              {windowName === 'overage' ? 'Extra' : label}
                            </div>
                            <div className="flex-1 h-[5px] bg-progress-track rounded-full overflow-hidden">
                              {utilization != null && (
                                <div
                                  className="h-full rounded-full"
                                  style={{
                                    width: `${Math.min(100, Math.max(0, utilization * 100))}%`,
                                    backgroundColor: color.stroke,
                                  }}
                                />
                              )}
                            </div>
                            <div className="w-8 shrink-0 text-right tabular-nums">
                              {pct}
                            </div>
                            <div className="hidden @[240px]:flex w-2 shrink-0 justify-end">
                              {stateDot && (
                                <Hint
                                  label={`${snap?.state} · ${snap?.source} · ${snap?.age_secs ?? 0}s`}
                                >
                                  <div
                                    className={cx(
                                      'w-1.5 h-1.5 rounded-full',
                                      stateDot,
                                    )}
                                  />
                                </Hint>
                              )}
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  ) : (
                    <div className="flex items-center gap-2 text-[10px] font-mono text-text-faint">
                      <Hint label="Spend and total tokens over the last 7 days">
                        {(() => {
                          const usage = usageByUpstreamId.get(u.id);
                          if (!usage) {
                            return <span>—</span>;
                          }
                          const tokens =
                            usage.tokens >= 1_000_000
                              ? `${(usage.tokens / 1_000_000).toFixed(1)}M`
                              : usage.tokens >= 1_000
                                ? `${(usage.tokens / 1_000).toFixed(1)}K`
                                : String(usage.tokens);
                          return (
                            <span className="text-text">
                              ${usage.cost_usd.toFixed(2)} · {tokens} tok
                            </span>
                          );
                        })()}
                      </Hint>
                    </div>
                  )}
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

type ChartRow = { ts: string; unix: number } & Record<
  string,
  number | null | string
>;

type ChartMarker = { ts: number; kind: string; window: string };

const DETAIL_WINDOWS = ['5h', '7d', '7d_sonnet', '7d_opus', 'overage'];
const SNAPSHOT_ORDER = ['5h', '7d', '7d_sonnet', '7d_opus', 'overage'];

function windowLabel(windowName: string): string {
  switch (windowName) {
    case '7d_sonnet':
      return '7d (Sonnet)';
    case '7d_opus':
      return '7d (Opus)';
    case 'overage':
      return 'Extra Usage';
    default:
      return windowName;
  }
}

function formatEta(secs: number | null | undefined): string {
  if (secs == null) return '—';
  if (secs <= 0) return 'Already saturated';
  if (secs > 86400) {
    return `${Math.floor(secs / 86400)}d ${Math.floor((secs % 86400) / 3600)}h`;
  }
  if (secs > 3600) {
    return `${Math.floor(secs / 3600)}h ${Math.floor((secs % 3600) / 60)}m`;
  }
  return `${Math.floor(secs / 60)}m`;
}

function SnapshotStatusComposite({ snap }: { snap: any }) {
  const age =
    snap.age_secs == null
      ? 'recently'
      : snap.age_secs < 60
        ? `${snap.age_secs}s old`
        : snap.age_secs < 3600
          ? `${Math.floor(snap.age_secs / 60)}m old`
          : snap.age_secs < 86400
            ? `${Math.floor(snap.age_secs / 3600)}h old`
            : `${Math.floor(snap.age_secs / 86400)}d old`;
  const source =
    snap.source === 'api' ? 'API' : snap.source === 'header' ? 'Header' : '—';
  const label =
    snap.state === 'fresh'
      ? 'live'
      : snap.state === 'stale'
        ? 'stale'
        : 'no data';
  const dot =
    snap.state === 'fresh'
      ? 'bg-green-500'
      : snap.state === 'stale'
        ? 'bg-amber-500'
        : 'bg-gray-400';
  return (
    <Hint
      label={`observed at ${
        snap.observed_at_unix_millis
          ? new Date(snap.observed_at_unix_millis).toLocaleString()
          : 'unknown'
      } · source: ${source} · age: ${age}`}
    >
      <div className="flex items-center gap-1.5 mt-0.5">
        <div
          className={cx('w-2 h-2 rounded-full', dot)}
          style={
            snap.state === 'fresh'
              ? { animation: 'pulse-glow 2s infinite' }
              : undefined
          }
        />
        <span className="text-[9px] text-text-faint font-mono">
          {label} · {source} · {age}
        </span>
      </div>
    </Hint>
  );
}

function PromotionalCreditsBadge({ orgMeta }: { orgMeta: any }) {
  const hasAny =
    orgMeta?.overage_credit_granted === true ||
    orgMeta?.overage_credit_eligible === true ||
    orgMeta?.overage_credit_available === true ||
    (orgMeta?.overage_credit_amount_minor_units != null &&
      orgMeta.overage_credit_amount_minor_units > 0);
  if (!hasAny) {
    return null;
  }
  const amount = orgMeta.overage_credit_amount_minor_units;
  const currency = orgMeta.overage_credit_currency ?? 'USD';
  const amountText =
    amount == null
      ? '—'
      : `${(amount / 100).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })} ${currency}`;
  const tone = orgMeta.overage_credit_granted ? 'ok' : 'neutral';
  const status = orgMeta.overage_credit_granted
    ? 'Granted'
    : orgMeta.overage_credit_eligible
      ? 'Eligible'
      : 'Available';
  return (
    <div className="rounded-sm border border-subtle bg-overlay-1 px-3 py-2 text-xs font-mono flex items-center gap-2">
      <span className="text-text-faint">Promo credits:</span>
      <span className="text-text">{amountText}</span>
      <span className="text-text-muted/50">·</span>
      <StatusBadge tone={tone} label={status} />
    </div>
  );
}

function TrialBanner({ orgMeta }: { orgMeta: any }) {
  if (!orgMeta?.claude_code_trial_ends_at) return null;
  return (
    <div className="rounded-sm border border-accent/30 bg-accent/10 p-3 text-xs text-accent">
      Claude Code trial ends{' '}
      <RelativeTime ts={orgMeta.claude_code_trial_ends_at * 1000} />
    </div>
  );
}

function PaymentWarning({ orgMeta }: { orgMeta: any }) {
  if (!orgMeta?.payment_auth_hosted_invoice_url) return null;
  return (
    <div className="rounded-sm border border-amber-400/30 bg-amber-400/10 p-3 text-xs text-amber-200 flex items-center justify-between gap-3">
      <span>Payment authorization is required for this organization.</span>
      <a
        href={orgMeta.payment_auth_hosted_invoice_url}
        target="_blank"
        rel="noreferrer"
        className="inline-flex items-center gap-1 text-amber-100 hover:underline"
      >
        Open invoice <ExternalLink className="w-3 h-3" />
      </a>
    </div>
  );
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
  const oauthStart = useOAuthStart();
  const oauthComplete = useOAuthComplete();
  const subscriptionMetadataQ = useUpstreamSubscriptionMetadata(upstream.id);
  const triggerSubscriptionMetadataRefresh =
    useTriggerSubscriptionMetadataRefresh();
  const upstreamOAuthQ = useUpstreamOAuthStatus(
    upstream.kind === 'anthropic_oauth' ? upstream.id : null,
  );
  const statusQ = useStatus();
  const principalNameMap = usePrincipalNameMap();
  const upstreamRuntimeStatus = useMemo(
    () => statusQ.data?.upstreams.find((u) => u.id === upstream.id) ?? null,
    [statusQ.data, upstream.id],
  );
  const [oauthOpen, setOauthOpen] = useState(false);
  const [oauthState, setOauthState] = useState<{
    authorize_url?: string;
    state_token?: string;
    code?: string;
  }>({});
  const [confirmDeleteOpen, setConfirmDeleteOpen] = useState(false);
  const [range, setRange] = useState<'1h' | '6h' | '24h' | '7d'>('7d');
  const [showMoreMeta, setShowMoreMeta] = useState(false);

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

  const [apiUsageRange, setApiUsageRange] = useState<'24h' | '7d'>('24h');
  const [apiUsageMetric, setApiUsageMetric] = useState<'tokens' | 'cost'>(
    'tokens',
  );
  const apiUsageQ = useUsage(
    apiUsageRange,
    'hour',
    'model',
    upstream.kind === 'anthropic_oauth' ? undefined : upstream.id,
  );

  const chartData = useMemo(() => {
    if (!quotaSeries.data?.series.length)
      return { rows: [] as ChartRow[], markers: [] as ChartMarker[] };

    const bucketsByTime = new Map<number, ChartRow>();
    const markers: ChartMarker[] = [];

    for (const series of quotaSeries.data.series) {
      const windowName = series.window;
      for (const bucket of series.buckets) {
        const ts = bucket.bucket_start_unix_secs;
        if (!bucketsByTime.has(ts)) {
          const date = new Date(ts * 1000);
          const label =
            range === '7d' || range === '24h'
              ? `${date.getMonth() + 1}/${date.getDate()} ${date.getHours()}h`
              : date.toTimeString().slice(0, 5);
          bucketsByTime.set(ts, { ts: label, unix: ts });
        }
        const row = bucketsByTime.get(ts)!;
        row[windowName] =
          bucket.utilization_last != null
            ? bucket.utilization_last * 100
            : null;
      }

      for (const marker of series.markers) {
        if (marker.at_unix_secs) {
          markers.push({
            ts: marker.at_unix_secs,
            kind: marker.kind,
            window: windowName,
          });
        }
      }
    }

    const latestWindows = quotaLatest.data?.upstreams[0]?.windows;
    if (latestWindows) {
      for (const snap of latestWindows) {
        const duration = WINDOW_DURATION_SECS[snap.window];
        if (snap.resets_at_unix_secs && duration) {
          markers.push({
            ts: snap.resets_at_unix_secs - duration,
            kind: 'start',
            window: snap.window,
          });
        }
      }
    }

    return {
      rows: Array.from(bucketsByTime.values()).sort((a, b) => a.unix - b.unix),
      markers,
    };
  }, [quotaSeries.data, quotaLatest.data, range]);

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

  const isOauth = upstream.kind === 'anthropic_oauth';
  const subMeta = subscriptionMetadataQ.data?.subscription_metadata;
  const orgMeta = subscriptionMetadataQ.data?.organization_metadata;

  const renderHeader = () => {
    const fields: {
      label: string;
      value: React.ReactNode;
      tooltip: string;
    }[] = [];
    let primaryLabels: Set<string>;

    const commonIdFields: typeof fields = [
      {
        label: 'ID',
        value: <span className="font-mono">{upstream.id.slice(0, 8)}…</span>,
        tooltip: `Internal upstream identifier (${upstream.id})`,
      },
    ];

    if (isOauth) {
      primaryLabels = new Set([
        'ID',
        'Plan',
        'Rate',
        'Extra Usage Billing',
        'Account',
      ]);
      fields.push(...commonIdFields);
      if (orgMeta?.organization_type)
        fields.push({
          label: 'Plan',
          value: orgMeta.organization_type,
          tooltip: 'Anthropic subscription tier',
        });
      if (orgMeta?.rate_limit_tier)
        fields.push({
          label: 'Rate',
          value: orgMeta.rate_limit_tier,
          tooltip:
            'Rate-limit tier (Max 5x = base plan, Max 20x = power user, Pro = Pro plan)',
        });
      if (orgMeta?.has_extra_usage_enabled != null)
        fields.push({
          label: 'Extra Usage Billing',
          value: orgMeta.has_extra_usage_enabled ? 'Enabled' : 'Disabled',
          tooltip:
            'Whether overage spending beyond plan quota is enabled (paid extra)',
        });
      if (orgMeta?.account_display_name || orgMeta?.account_email)
        fields.push({
          label: 'Account',
          value: `${orgMeta.account_display_name || 'Unknown'} (${orgMeta.account_email || 'unknown'})`,
          tooltip: 'OAuth token account identity',
        });
      if (orgMeta?.organization_name)
        fields.push({
          label: 'Org',
          value: orgMeta.organization_name,
          tooltip: 'Anthropic organization name',
        });
      if (subMeta?.organization_role)
        fields.push({
          label: 'Role',
          value: subMeta.organization_role,
          tooltip: 'Your role within the organization',
        });
      if (subMeta?.workspace_role)
        fields.push({
          label: 'Seat',
          value: subMeta.workspace_role,
          tooltip: 'Seat tier within team plans',
        });
      if (orgMeta?.subscription_created_at_unix_secs)
        fields.push({
          label: 'Subscribed',
          value: (
            <RelativeTime
              ts={new Date(orgMeta.subscription_created_at_unix_secs * 1000)}
            />
          ),
          tooltip: 'When this organization first subscribed',
        });
      if (orgMeta?.billing_type)
        fields.push({
          label: 'Billing',
          value: orgMeta.billing_type,
          tooltip: 'How the subscription is billed',
        });
    } else {
      primaryLabels = new Set(['ID', 'Base URL']);
      fields.push(...commonIdFields);
      if (upstream.base_url)
        fields.push({
          label: 'Base URL',
          value: <span className="font-mono">{upstream.base_url}</span>,
          tooltip: 'Endpoint base URL for upstream requests',
        });
      if (upstream.api_key_env)
        fields.push({
          label: 'API Key',
          value: <span className="font-mono">env:{upstream.api_key_env}</span>,
          tooltip: `Loaded from the ${upstream.api_key_env} environment variable on the server`,
        });
      else if (upstream.kind === 'anthropic_api_key')
        fields.push({
          label: 'API Key',
          value: 'literal',
          tooltip: 'Stored inline (literal API key)',
        });
      else if (upstream.kind === 'custom')
        fields.push({
          label: 'Credentials',
          value: 'passthrough',
          tooltip:
            "Custom upstreams forward the downstream client's API key directly. No credential is stored.",
        });
      if (upstream.shape_plugin?.registry_id)
        fields.push({
          label: 'Plugin',
          value: (
            <span className="font-mono">
              {upstream.shape_plugin.registry_id}
            </span>
          ),
          tooltip: 'WASM shape plugin handling request/response transforms',
        });
      if (upstreamRuntimeStatus?.last_apply_error)
        fields.push({
          label: 'Apply error',
          value: (
            <span className="text-red-400">
              {upstreamRuntimeStatus.last_apply_error}
            </span>
          ),
          tooltip: 'Most recent failed reconciliation',
        });
    }

    const visibleFields = isOauth
      ? fields.filter((f) => showMoreMeta || primaryLabels.has(f.label))
      : fields;
    const hiddenCount = isOauth
      ? fields.filter((f) => !primaryLabels.has(f.label)).length
      : 0;

    return (
      <div className="sticky top-0 z-30 bg-bg-sub border-b border-subtle backdrop-blur-sm">
        <div className="px-4 md:px-6 py-3 flex items-start justify-between gap-3 flex-wrap shrink-0">
          <div className="min-w-0">
            <button
              type="button"
              onClick={onBack}
              className="md:hidden inline-flex items-center gap-1 text-xs text-text-faint hover:text-text mb-1"
            >
              <ChevronLeft className="w-3 h-3" /> Back
            </button>
            <div className="flex items-center gap-3 flex-wrap">
              <InlineNameEditor upstream={upstream} />
              <Hint
                label={
                  upstream.enabled ? 'Click to disable' : 'Click to enable'
                }
              >
                <button
                  type="button"
                  role="switch"
                  aria-checked={upstream.enabled}
                  disabled={toggle.isPending}
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
                  className="group inline-flex items-center gap-2 h-7 px-2 rounded-sm transition-colors focus:outline-none focus:ring-2 focus:ring-accent/40 disabled:opacity-50 disabled:cursor-not-allowed hover:bg-overlay-3"
                >
                  <div
                    className={cx(
                      'relative inline-flex h-4 w-8 shrink-0 items-center rounded-full transition-colors duration-200 ease-in-out border',
                      upstream.enabled
                        ? 'bg-emerald-500 border-emerald-500'
                        : 'bg-overlay-5 border-subtle-strong group-hover:border-text-muted',
                    )}
                  >
                    <span
                      className={cx(
                        'pointer-events-none inline-block h-3 w-3 transform rounded-full bg-white shadow-sm transition-transform duration-200 ease-in-out',
                        upstream.enabled ? 'translate-x-4' : 'translate-x-0.5',
                      )}
                    />
                  </div>
                  <span
                    className={cx(
                      'text-[11px] font-mono uppercase tracking-wider',
                      upstream.enabled
                        ? 'text-emerald-400'
                        : 'text-text-muted group-hover:text-text',
                    )}
                  >
                    {upstream.enabled ? 'Enabled' : 'Disabled'}
                  </span>
                </button>
              </Hint>
              <Badge tone="mono">{upstream.kind}</Badge>
            </div>
          </div>
          <div className="flex items-center gap-2 shrink-0">
            <Button
              size="sm"
              variant="danger"
              iconLeft={<Trash2 className="w-3 h-3" />}
              onClick={() => setConfirmDeleteOpen(true)}
            >
              Delete
            </Button>
          </div>
        </div>
        {fields.length > 0 && (
          <div className="px-4 md:px-6 py-2 border-t border-subtle bg-overlay-1 flex items-center gap-3">
            <div className="flex-1 min-w-0 overflow-x-auto whitespace-nowrap scrollbar-none">
              <div className="flex items-center gap-2 text-xs font-mono text-text-faint">
                {visibleFields.map((f, i) => (
                  <span key={f.label} className="flex items-center gap-2">
                    <Hint label={f.tooltip}>
                      <span className="cursor-help border-b border-dotted border-text-faint/30 hover:text-text transition-colors">
                        <span className="text-text-muted">{f.label}:</span>{' '}
                        <span className="text-text">{f.value}</span>
                      </span>
                    </Hint>
                    {i < visibleFields.length - 1 && (
                      <span className="text-text-muted/50">·</span>
                    )}
                  </span>
                ))}
              </div>
            </div>
            <div className="flex items-center gap-2 shrink-0">
              {hiddenCount > 0 && (
                <button
                  type="button"
                  onClick={() => setShowMoreMeta((v) => !v)}
                  className="text-[11px] font-mono text-text-faint hover:text-text underline underline-offset-2"
                >
                  {showMoreMeta ? 'Less' : `+${hiddenCount} more`}
                </button>
              )}
              {isOauth && (
                <Hint label="Refresh subscription metadata">
                  <button
                    type="button"
                    disabled={triggerSubscriptionMetadataRefresh.isPending}
                    onClick={() => {
                      triggerSubscriptionMetadataRefresh.mutate(upstream.id, {
                        onSuccess: () => toast.success('Metadata refreshed'),
                        onError: (error) => {
                          const message =
                            error instanceof ApiError
                              ? error.message ||
                                `Request failed (${error.status})`
                              : error instanceof Error
                                ? error.message
                                : String(error);
                          toast.error(`Metadata refresh failed: ${message}`);
                        },
                      });
                    }}
                    className="text-text-faint hover:text-text disabled:opacity-50"
                    aria-label="Refresh subscription metadata"
                  >
                    <RefreshCw
                      className={cx(
                        'w-3.5 h-3.5',
                        triggerSubscriptionMetadataRefresh.isPending &&
                          'animate-spin',
                      )}
                    />
                  </button>
                </Hint>
              )}
            </div>
          </div>
        )}
      </div>
    );
  };

  return (
    <>
      {renderHeader()}

      <div className="flex-1 overflow-y-auto p-4 md:p-6 pb-8 md:pb-12 space-y-6">
        {isOauth &&
        (orgMeta?.claude_code_trial_ends_at ||
          orgMeta?.payment_auth_hosted_invoice_url ||
          orgMeta?.overage_credit_granted ||
          orgMeta?.overage_credit_eligible ||
          (orgMeta as any)?.overage_credit_available ||
          (orgMeta?.overage_credit_amount_minor_units != null &&
            orgMeta.overage_credit_amount_minor_units > 0)) ? (
          <div className="space-y-2">
            <TrialBanner orgMeta={orgMeta} />
            <PaymentWarning orgMeta={orgMeta} />
            <PromotionalCreditsBadge orgMeta={orgMeta} />
          </div>
        ) : null}

        {isOauth && (
          <Section title="Subscription Quota">
            <Card>
              <CardHeader
                title="Quota History"
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
                    <ResponsiveContainer
                      width="100%"
                      height={240}
                      debounce={150}
                    >
                      <AreaChart
                        data={chartData.rows}
                        margin={{ top: 28, right: 24, bottom: 4, left: 0 }}
                      >
                        <defs>
                          {DETAIL_WINDOWS.map((windowName) => {
                            const color = getWindowColor(windowName);
                            return (
                              <linearGradient
                                key={windowName}
                                id={`quota-detail-grad-${windowName}`}
                                x1="0"
                                y1="0"
                                x2="0"
                                y2="1"
                              >
                                <stop
                                  offset="0%"
                                  stopColor={color.stroke}
                                  stopOpacity={0.55}
                                />
                                <stop
                                  offset="100%"
                                  stopColor={color.stroke}
                                  stopOpacity={0}
                                />
                              </linearGradient>
                            );
                          })}
                        </defs>
                        <CartesianGrid stroke="var(--color-border)" />
                        <XAxis
                          dataKey="unix"
                          type="number"
                          domain={[sinceUnixSecs, nowUnixSecs]}
                          tick={{
                            fill: 'var(--color-text-faint)',
                            fontSize: 10,
                            fontFamily: 'Geist Mono',
                          }}
                          tickFormatter={(val) => {
                            const d = new Date(Number(val) * 1000);
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
                            const first =
                              quotaLatest.data?.upstreams[0]?.windows[0];
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
                                {payload.map((p, i) => {
                                  const key = String(p.dataKey);
                                  return (
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
                                      <div
                                        style={{
                                          display: 'flex',
                                          alignItems: 'center',
                                          gap: 6,
                                        }}
                                      >
                                        <div
                                          style={{
                                            width: 8,
                                            height: 8,
                                            borderRadius: '50%',
                                            backgroundColor:
                                              getWindowColor(key).fill,
                                          }}
                                        />
                                        <span>{windowLabel(key)}</span>
                                      </div>
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
                                  );
                                })}
                                {first?.source && (
                                  <div
                                    style={{
                                      marginTop: 6,
                                      paddingTop: 6,
                                      borderTop:
                                        '1px solid var(--color-border)',
                                      color: 'var(--color-text-faint)',
                                      fontSize: 10,
                                    }}
                                  >
                                    from: {first.source} · observed{' '}
                                    {first.age_secs == null
                                      ? 'recently'
                                      : `${first.age_secs}s ago`}
                                  </div>
                                )}
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
                          content={() => {
                            const latest = quotaLatest.data?.upstreams[0];
                            if (!latest) return null;
                            const windows = ['5h', '7d', '7d_sonnet'];
                            const opus = latest.windows.find(
                              (w) => w.window === '7d_opus',
                            );
                            if (opus && opus.state !== 'missing')
                              windows.push('7d_opus');
                            const overage = latest.windows.find(
                              (w) => w.window === 'overage',
                            );
                            if (
                              overage &&
                              (overage.extra_usage_enabled ||
                                overage.extra_usage_monthly_limit != null)
                            )
                              windows.push('overage');
                            return (
                              <div className="flex flex-wrap items-center justify-center gap-4 mt-2">
                                {windows.map((windowName) => (
                                  <div
                                    key={windowName}
                                    className="flex items-center gap-1.5"
                                  >
                                    <div
                                      className="w-3 h-0.5"
                                      style={{
                                        backgroundColor:
                                          getWindowColor(windowName).stroke,
                                      }}
                                    />
                                    <span className="text-[11px] text-text-muted font-mono">
                                      {windowLabel(windowName)}
                                    </span>
                                  </div>
                                ))}
                              </div>
                            );
                          }}
                        />
                        {(() => {
                          const visible = 2 * (nowUnixSecs - sinceUnixSecs);
                          const markers: ChartMarker[] = [];
                          const paired = new Map<
                            string,
                            { start?: ChartMarker; reset?: ChartMarker }
                          >();
                          for (const marker of chartData.markers) {
                            if (!paired.has(marker.window))
                              paired.set(marker.window, {});
                            const bucket = paired.get(marker.window)!;
                            if (marker.kind === 'start') bucket.start = marker;
                            else if (marker.kind === 'reset')
                              bucket.reset = marker;
                            else markers.push(marker);
                          }
                          for (const pair of paired.values()) {
                            if (
                              pair.start &&
                              pair.reset &&
                              Math.abs(pair.reset.ts - pair.start.ts) /
                                visible <
                                0.25
                            )
                              markers.push(pair.reset);
                            else {
                              if (pair.start) markers.push(pair.start);
                              if (pair.reset) markers.push(pair.reset);
                            }
                          }
                          return markers.map((marker, i) => {
                            if (
                              marker.ts < sinceUnixSecs ||
                              marker.ts >
                                nowUnixSecs + (nowUnixSecs - sinceUnixSecs)
                            )
                              return null;
                            const color = getWindowColor(marker.window);
                            return (
                              <ReferenceLine
                                key={`marker-${i}`}
                                x={marker.ts}
                                stroke={color.stroke}
                                strokeOpacity={0.6}
                                strokeDasharray={
                                  marker.kind === 'start' ? '4 6' : '2 4'
                                }
                              >
                                <Label
                                  value={
                                    marker.kind === 'start'
                                      ? `${windowLabel(marker.window)} start`
                                      : `${windowLabel(marker.window)} reset`
                                  }
                                  position="top"
                                  fontSize={10}
                                  fill={color.stroke}
                                />
                              </ReferenceLine>
                            );
                          });
                        })()}
                        {(() => {
                          const latest = quotaLatest.data?.upstreams[0];
                          if (!latest) return null;
                          const windows = ['5h', '7d', '7d_sonnet'];
                          const opus = latest.windows.find(
                            (w) => w.window === '7d_opus',
                          );
                          if (opus && opus.state !== 'missing')
                            windows.push('7d_opus');
                          const overage = latest.windows.find(
                            (w) => w.window === 'overage',
                          );
                          if (
                            overage &&
                            (overage.extra_usage_enabled ||
                              overage.extra_usage_monthly_limit != null)
                          )
                            windows.push('overage');
                          return windows.map((windowName) => (
                            <Area
                              key={windowName}
                              type="monotone"
                              dataKey={windowName}
                              stroke={getWindowColor(windowName).stroke}
                              strokeWidth={1.4}
                              fill={`url(#quota-detail-grad-${windowName})`}
                              fillOpacity={1}
                              isAnimationActive={false}
                              connectNulls={false}
                            />
                          ));
                        })()}
                      </AreaChart>
                    </ResponsiveContainer>
                  )}
                </div>
              </CardBody>
            </Card>

            {(() => {
              const latest = quotaLatest.data?.upstreams[0];
              if (!latest) {
                return (
                  <Card>
                    <CardBody className="p-6">
                      <EmptyState
                        title={`No subscription quota data for ${upstream.name}`}
                      />
                    </CardBody>
                  </Card>
                );
              }
              const analysis = quotaAnalysis.data?.upstreams[0];
              const a5h = analysis?.windows.find((w) => w.window === '5h');
              const caveats = Array.from(
                new Set(analysis?.windows.flatMap((w) => w.caveats) ?? []),
              ).filter(
                (c) =>
                  c.toLowerCase().trim() !==
                  'capacity is inferred from proxy tokens and quota utilization; anthropic quota units are not directly exposed',
              );
              return (
                <div className="space-y-4">
                  <div className="grid gap-3 [grid-template-columns:repeat(auto-fit,minmax(240px,1fr))]">
                    {(['5h', '7d', '7d_sonnet', '7d_opus', 'overage'] as const)
                      .map((windowName) =>
                        latest.windows.find(
                          (snap) => snap.window === windowName,
                        ),
                      )
                      .filter((snap): snap is NonNullable<typeof snap> => {
                        if (!snap || snap.state === 'missing') return false;
                        if (
                          snap.window === 'overage' &&
                          !snap.extra_usage_enabled &&
                          snap.extra_usage_monthly_limit == null
                        ) {
                          return false;
                        }
                        return true;
                      })
                      .sort(
                        (a, b) =>
                          SNAPSHOT_ORDER.indexOf(a.window) -
                          SNAPSHOT_ORDER.indexOf(b.window),
                      )
                      .map((snap) => {
                        const color = getWindowColor(snap.window);
                        const isOverage =
                          snap.window === 'overage' &&
                          (snap.extra_usage_enabled ||
                            snap.extra_usage_monthly_limit != null);
                        const windowAnalysis = analysis?.windows.find(
                          (w) => w.window === snap.window,
                        );
                        const actualBurn =
                          windowAnalysis?.actual_account_burn
                            .utilization_per_second;
                        const projBurn =
                          windowAnalysis?.proxy_projected_burn
                            .utilization_per_hour;
                        const eta =
                          windowAnalysis?.actual_account_burn.eta_to_limit_secs;
                        const waitingForGrowth =
                          !isOverage &&
                          (!windowAnalysis ||
                            windowAnalysis.actual_account_burn.reason ===
                              'insufficient_growth_intervals');
                        return (
                          <Card key={snap.window}>
                            <CardBody className="p-3 flex flex-col gap-1.5">
                              <div className="flex items-center justify-between">
                                <span className="text-[11px] uppercase tracking-wider text-text-faint">
                                  {windowLabel(snap.window)}
                                </span>
                              </div>
                              <SnapshotStatusComposite snap={snap} />
                              {isOverage ? (
                                snap.extra_usage_monthly_limit != null &&
                                snap.extra_usage_used_credits != null ? (
                                  <>
                                    <div className="text-xl font-medium tabular-nums">
                                      {(
                                        (snap.extra_usage_used_credits /
                                          snap.extra_usage_monthly_limit) *
                                        100
                                      ).toFixed(1)}
                                      %
                                    </div>
                                    <div className="w-full h-1 bg-progress-track rounded-full overflow-hidden mt-1">
                                      <div
                                        className="h-full rounded-full"
                                        style={{
                                          width: `${Math.min(100, Math.max(0, (snap.extra_usage_used_credits / snap.extra_usage_monthly_limit) * 100))}%`,
                                          backgroundColor: color.fill,
                                        }}
                                      />
                                    </div>
                                    <div className="text-sm font-medium tabular-nums mt-1">
                                      $
                                      {(
                                        snap.extra_usage_used_credits / 100
                                      ).toFixed(2)}{' '}
                                      / $
                                      {(
                                        snap.extra_usage_monthly_limit / 100
                                      ).toLocaleString('en-US', {
                                        minimumFractionDigits: 2,
                                        maximumFractionDigits: 2,
                                      })}{' '}
                                      USD
                                    </div>
                                  </>
                                ) : (
                                  <span className="text-sm text-text-faint">
                                    Enabled — no limit set
                                  </span>
                                )
                              ) : (
                                <div className="text-xl font-medium tabular-nums">
                                  {snap.utilization == null
                                    ? (snap.status ?? '—')
                                    : `${(snap.utilization * 100).toFixed(1)}%`}
                                </div>
                              )}
                              {!isOverage && snap.utilization != null && (
                                <div className="w-full h-1 bg-progress-track rounded-full overflow-hidden mt-1">
                                  <div
                                    className="h-full rounded-full"
                                    style={{
                                      width: `${Math.min(100, Math.max(0, snap.utilization * 100))}%`,
                                      backgroundColor: color.fill,
                                    }}
                                  />
                                </div>
                              )}
                              {snap.resets_at_unix_secs ? (
                                <div className="text-[10px] text-text-faint font-mono mt-1">
                                  resets{' '}
                                  <RelativeTime
                                    ts={snap.resets_at_unix_secs * 1000}
                                  />
                                </div>
                              ) : null}
                              {!isOverage && (
                                <div className="border-t border-subtle pt-1.5 mt-1.5 flex flex-col gap-1">
                                  <div className="flex items-baseline justify-between gap-2">
                                    <span className="text-[10px] uppercase tracking-wider text-text-faint">
                                      ETA to limit
                                    </span>
                                    <span className="font-mono tabular-nums text-sm">
                                      {formatEta(eta)}
                                    </span>
                                  </div>
                                  <div className="flex items-center justify-between gap-2 text-[10px] font-mono text-text-faint">
                                    <span>burn</span>
                                    <span className="tabular-nums">
                                      {actualBurn == null
                                        ? '—'
                                        : `${(actualBurn * 60 * 100).toFixed(2)}%/min`}
                                      {' · proj '}
                                      {projBurn == null
                                        ? '—'
                                        : `${((projBurn / 60) * 100).toFixed(2)}%/min`}
                                    </span>
                                  </div>
                                  {waitingForGrowth && (
                                    <div className="text-[10px] text-amber-400">
                                      Waiting for utilization to rise — burn
                                      appears once growth is observed.
                                    </div>
                                  )}
                                </div>
                              )}
                            </CardBody>
                          </Card>
                        );
                      })}
                  </div>
                  {a5h?.deficit && (
                    <Card>
                      <CardHeader title="Quota deficit" />
                      <CardBody className="p-3">
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
                      </CardBody>
                    </Card>
                  )}
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
                </div>
              );
            })()}
          </Section>
        )}

        {!isOauth && (
          <Section title="API Usage">
            <ApiUsageCard
              data={apiUsageQ.data}
              isLoading={apiUsageQ.isLoading}
              range={apiUsageRange}
              onRangeChange={setApiUsageRange}
              metric={apiUsageMetric}
              onMetricChange={setApiUsageMetric}
            />
          </Section>
        )}

        {!isOauth && <SettingsCard upstream={upstream} />}

        <Card>
          <CardHeader
            title="Recent Requests"
            subtitle={
              recentForUpstream.length === 0
                ? `No recent requests against ${upstream.name}`
                : `Last ${recentForUpstream.length} against ${upstream.name}`
            }
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
      </div>

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
  const startDraft = useStartOauthDraft();
  const completeDraft = useCompleteOauthDraft();
  const createFromDraft = useCreateFromOauthDraft();

  const [step, setStep] = useState<
    'type' | 'configure_non_oauth' | 'oauth_handshake' | 'oauth_confirm'
  >('type');
  const [kind, setKind] = useState<
    'anthropic_api_key' | 'anthropic_oauth' | 'custom'
  >('anthropic_oauth');

  // Non-OAuth state
  const [name, setName] = useState('');
  const [baseUrl, setBaseUrl] = useState('https://api.anthropic.com');
  const [apiKeyValue, setApiKeyValue] = useState('');
  const [apiKeyEnv, setApiKeyEnv] = useState('ANTHROPIC_API_KEY');
  const [useEnvVar, setUseEnvVar] = useState(false);

  // OAuth state
  const [authState, setAuthState] = useState<{
    authorize_url: string;
    state_token: string;
  } | null>(null);
  const [code, setCode] = useState('');
  const [draftResult, setDraftResult] = useState<any>(null);
  const [oauthName, setOauthName] = useState('');
  const [oauthError, setOauthError] = useState<string | null>(null);

  useEffect(() => {
    if (!open) {
      setStep('type');
      setKind('anthropic_oauth');
      setName('');
      setBaseUrl('https://api.anthropic.com');
      setApiKeyValue('');
      setApiKeyEnv('ANTHROPIC_API_KEY');
      setUseEnvVar(false);
      setAuthState(null);
      setCode('');
      setDraftResult(null);
      setOauthName('');
      setOauthError(null);
      onPendingCreatedIdChange(null);
    }
  }, [open, onPendingCreatedIdChange]);

  const handleOpenChange = (next: boolean) => {
    onOpenChange(next);
  };

  const submitNonOauth = () => {
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
        onSuccess: () => {
          toast.success('Upstream created');
          onOpenChange(false);
        },
      },
    );
  };

  const handleAuthorizeClick = () => {
    setOauthError(null);
    startDraft.mutate(undefined, {
      onSuccess: (res: any) => {
        setAuthState(res);
        window.open(res.authorize_url, '_blank');
      },
      onError: (err: any) => {
        setOauthError(err instanceof Error ? err.message : String(err));
      },
    });
  };

  const handleVerifyCode = () => {
    if (!authState || !code.trim()) return;
    setOauthError(null);
    completeDraft.mutate(
      { state_token: authState.state_token, code: code.trim() },
      {
        onSuccess: (res: any) => {
          setDraftResult(res);
          setOauthName(res.suggested_name || '');
          setStep('oauth_confirm');
        },
        onError: (err: any) => {
          setOauthError(err instanceof Error ? err.message : String(err));
        },
      },
    );
  };

  const submitOauthConfirm = () => {
    if (!authState || !oauthName.trim()) return;
    createFromDraft.mutate(
      { state_token: authState.state_token, name: oauthName.trim() },
      {
        onSuccess: () => {
          toast.success('OAuth upstream created');
          onOpenChange(false);
        },
        onError: (err: any) => {
          toast.error(err instanceof Error ? err.message : String(err));
        },
      },
    );
  };

  const renderStepContent = () => {
    if (step === 'type') {
      return (
        <div className="space-y-4">
          <div className="space-y-2">
            <label className="flex items-start gap-3 p-3 border border-subtle rounded-md cursor-pointer hover:bg-overlay-1 transition-colors">
              <input
                type="radio"
                name="kind"
                value="anthropic_oauth"
                checked={kind === 'anthropic_oauth'}
                onChange={() => setKind('anthropic_oauth')}
                className="mt-1"
              />
              <div>
                <div className="font-medium text-text">
                  Anthropic OAuth (Recommended)
                </div>
                <div className="text-xs text-text-faint mt-1">
                  Use a Claude Max/Pro/Team/Enterprise subscription via OAuth.
                  Recommended — automatic plan detection and quota tracking via
                  Anthropic's official APIs.
                </div>
              </div>
            </label>
            <label className="flex items-start gap-3 p-3 border border-subtle rounded-md cursor-pointer hover:bg-overlay-1 transition-colors">
              <input
                type="radio"
                name="kind"
                value="anthropic_api_key"
                checked={kind === 'anthropic_api_key'}
                onChange={() => setKind('anthropic_api_key')}
                className="mt-1"
              />
              <div>
                <div className="font-medium text-text">Anthropic API Key</div>
                <div className="text-xs text-text-faint mt-1">
                  Use a workspace API key (sk-ant-api03-...). Pay-as-you-go
                  billing per token.
                </div>
              </div>
            </label>
            <label className="flex items-start gap-3 p-3 border border-subtle rounded-md cursor-pointer hover:bg-overlay-1 transition-colors">
              <input
                type="radio"
                name="kind"
                value="custom"
                checked={kind === 'custom'}
                onChange={() => setKind('custom')}
                className="mt-1"
              />
              <div>
                <div className="font-medium text-text">Custom Backend</div>
                <div className="text-xs text-text-faint mt-1">
                  Custom Anthropic-compatible endpoint. Provide your own base
                  URL; the client API key is passed through (no credential is
                  stored).
                </div>
              </div>
            </label>
          </div>
        </div>
      );
    }

    if (step === 'configure_non_oauth') {
      return (
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
                  Use literal value instead
                </button>
              </Field>
            ) : (
              <Field
                label="API Key Value"
                hint="Literal sk-ant-... key"
                required
              >
                <input
                  type="password"
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
                  Use environment variable instead
                </button>
              </Field>
            )
          ) : null}
        </div>
      );
    }

    if (step === 'oauth_handshake') {
      return (
        <div className="space-y-4">
          <p className="text-sm text-text-faint">
            We'll authorize a Claude account via Anthropic. Click Authorize,
            complete the flow, then paste the code below.
          </p>

          <Button
            variant="primary"
            onClick={handleAuthorizeClick}
            disabled={startDraft.isPending}
          >
            {startDraft.isPending ? 'Starting...' : 'Authorize with Anthropic'}
          </Button>

          {authState && (
            <div className="mt-4 space-y-3 p-4 border border-subtle rounded-md bg-overlay-1">
              <Field label="Authorization Code" required>
                <textarea
                  className={`${INPUT_CLASS} font-mono min-h-[80px]`}
                  value={code}
                  onChange={(e) => setCode(e.target.value)}
                  placeholder="paste code..."
                />
              </Field>
              {oauthError && (
                <div className="text-sm text-red-400">{oauthError}</div>
              )}
              <Button
                onClick={handleVerifyCode}
                disabled={!code.trim() || completeDraft.isPending}
              >
                {completeDraft.isPending
                  ? 'Verifying...'
                  : 'Verify and fetch account'}
              </Button>
            </div>
          )}
        </div>
      );
    }

    if (step === 'oauth_confirm') {
      const orgMeta = draftResult?.organization_metadata;
      const subMeta = draftResult?.subscription_metadata;

      const fields: { label: string; value: React.ReactNode }[] = [];
      if (orgMeta?.organization_type)
        fields.push({ label: 'Plan', value: orgMeta.organization_type });
      if (orgMeta?.rate_limit_tier)
        fields.push({ label: 'Rate', value: orgMeta.rate_limit_tier });
      if (subMeta?.organization_role)
        fields.push({ label: 'Role', value: subMeta.organization_role });
      if (orgMeta?.organization_name)
        fields.push({ label: 'Org', value: orgMeta.organization_name });
      if (orgMeta?.subscription_created_at_unix_secs)
        fields.push({
          label: 'Subscribed',
          value: (
            <RelativeTime
              ts={new Date(orgMeta.subscription_created_at_unix_secs * 1000)}
            />
          ),
        });
      if (orgMeta?.billing_type)
        fields.push({ label: 'Billing', value: orgMeta.billing_type });
      if (orgMeta?.has_extra_usage_enabled != null)
        fields.push({
          label: 'Extra Usage',
          value: orgMeta.has_extra_usage_enabled ? 'Enabled' : 'Disabled',
        });
      if (orgMeta?.account_display_name || orgMeta?.account_email)
        fields.push({
          label: 'Account',
          value: `${orgMeta.account_display_name || 'Unknown'} (${orgMeta.account_email || 'unknown'})`,
        });

      return (
        <div className="space-y-4">
          <div className="p-4 border border-subtle rounded-md bg-overlay-1 space-y-3">
            <h3 className="text-sm font-medium text-text">Account Preview</h3>
            <div className="grid grid-cols-2 gap-2 text-xs font-mono">
              {fields.map((f) => (
                <div key={f.label} className="flex flex-col">
                  <span className="text-text-faint">{f.label}</span>
                  <span className="text-text truncate">{f.value}</span>
                </div>
              ))}
            </div>
          </div>

          <Field label="Upstream Name" required>
            <input
              className={INPUT_CLASS}
              value={oauthName}
              onChange={(e) => setOauthName(e.target.value)}
            />
          </Field>
        </div>
      );
    }
  };

  const renderFooter = () => {
    if (step === 'type') {
      return (
        <>
          <Button onClick={() => handleOpenChange(false)}>Cancel</Button>
          <Button
            variant="primary"
            onClick={() =>
              setStep(
                kind === 'anthropic_oauth'
                  ? 'oauth_handshake'
                  : 'configure_non_oauth',
              )
            }
          >
            Continue
          </Button>
        </>
      );
    }
    if (step === 'configure_non_oauth') {
      return (
        <>
          <Button onClick={() => setStep('type')}>Back</Button>
          <Button
            variant="primary"
            disabled={
              !name.trim() ||
              create.isPending ||
              (kind === 'anthropic_api_key' &&
                (useEnvVar ? !apiKeyEnv.trim() : !apiKeyValue.trim()))
            }
            onClick={submitNonOauth}
          >
            Create
          </Button>
        </>
      );
    }
    if (step === 'oauth_handshake') {
      return (
        <>
          <Button onClick={() => setStep('type')}>Back</Button>
        </>
      );
    }
    if (step === 'oauth_confirm') {
      return (
        <>
          <Button onClick={() => setStep('oauth_handshake')}>Back</Button>
          <Button
            variant="primary"
            disabled={!oauthName.trim() || createFromDraft.isPending}
            onClick={submitOauthConfirm}
          >
            Save
          </Button>
        </>
      );
    }
  };

  return (
    <Modal
      open={open}
      onOpenChange={handleOpenChange}
      title="New upstream"
      description="Register an Anthropic API key, OAuth principal, or custom backend."
      size={
        step === 'oauth_handshake' || step === 'oauth_confirm'
          ? 'lg'
          : undefined
      }
      footer={renderFooter()}
    >
      {renderStepContent()}
    </Modal>
  );
}
