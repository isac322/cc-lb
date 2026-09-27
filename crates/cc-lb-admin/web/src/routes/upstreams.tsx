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
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  Area,
  AreaChart,
  CartesianGrid,
  Label,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import { toast } from 'sonner';
import * as z from 'zod';
import { UpstreamsListEmpty } from '../components/onboarding/ListEmptyStates';
import {
  CHART_AXIS,
  CHART_CURSOR,
  CHART_GRID,
  CHART_THRESHOLD,
} from '../components/ui/charts';
import { ArcGauge, HeadroomMeter } from '../components/ui/Gauge';
import {
  Button,
  ConfirmDialog,
  cx,
  EmptyState,
  Hint,
  Notice,
  PageContainer,
  PageHeader,
  Section,
  SegmentedControl,
  Skeleton,
  Spinner,
  StatusBadge,
  ToggleSwitch,
} from '../components/ui/primitives';
import {
  RelativeOffsetTime,
  RelativeTime,
  ResetCountdown,
} from '../components/ui/RelativeTime';
import { RequestEventsTable } from '../components/ui/RequestEventsTable';
import { ApiUsageCard } from '../components/upstreams/ApiUsageCard';
import {
  buildQuotaChartData,
  type ChartMarker,
} from '../components/upstreams/buildQuotaChartData';
import {
  UpstreamConnectDialog,
  type UpstreamConnectTarget,
} from '../components/upstreams/connect/UpstreamConnectDialog';
import { InlineNameEditor } from '../components/upstreams/InlineNameEditor';
import { LimitResetAction } from '../components/upstreams/LimitResetAction';
import { OAuthReconnectNotice } from '../components/upstreams/OAuthReconnectNotice';
import { QuotaObservedAt } from '../components/upstreams/QuotaObservedAt';
import {
  selectQuotaCardSnapshots,
  selectVisibleGraphWindows,
} from '../components/upstreams/quotaWindowVisibility';
import { SettingsCard } from '../components/upstreams/SettingsCard';
import {
  formatQuotaStamp,
  subscriptionPlanLabel,
  UpstreamHeadroomGrid,
  useUpstreamHeadroomData,
} from '../components/upstreams/UpstreamHeadroomGrid';
import { upstreamHealth } from '../components/upstreams/upstreamHealth';
import { WarmupCardMinimal } from '../components/upstreams/warmup/WarmupCardMinimal';
import {
  ApiError,
  type OrganizationMetadataInner,
  type QuotaSnapshot,
  type SubscriptionQuotaWindow,
  type UpstreamOAuthStatusResponse,
  WINDOW_LABELS,
} from '../lib/api';
import { getWindowColor } from '../lib/colors';
import { DEFAULT_ANTHROPIC_BASE_URL } from '../lib/constants';
import { fmtChartTooltipTs } from '../lib/format';
import { useTimezone } from '../lib/locale';
import { isMessagesRequestEvent } from '../lib/logRows';
import {
  classifyOAuthReconnect,
  LONG_LIVED_EXPIRING_SOON_SECS,
  REFRESH_EXPIRING_SOON_SECS,
} from '../lib/oauthReconnect';
import {
  type UpdateUpstreamWarmupSettingsRequest,
  type Upstream,
  useDeleteUpstream,
  usePrincipalNameMap,
  useRecentEvents,
  useStatus,
  useSubscriptionQuotaAnalysis,
  useSubscriptionQuotaLatest,
  useSubscriptionQuotaSeries,
  useTriggerSubscriptionMetadataRefresh,
  useUpdateUpstreamWarmupSettings,
  useUpstreamNameMap,
  useUpstreamOAuthStatus,
  useUpstreamSubscriptionMetadata,
  useUpstreams,
  useUsage,
} from '../lib/queries';
import {
  formatHeadroom,
  formatQuotaPercent,
  QUOTA_DANGER_PCT,
  QUOTA_WARN_PCT,
} from '../lib/quotaSeverity';
import {
  TIME_PRESET_OPTIONS,
  TIME_PRESETS,
  type TimePreset,
} from '../lib/timePresets';

const upstreamSearchSchema = z.object({
  selectedId: z.string().optional(),
  action: z.enum(['new', 'reconnect']).optional(),
});

export const Route = createFileRoute('/upstreams')({
  validateSearch: upstreamSearchSchema,
  component: UpstreamsPage,
});

function UpstreamsPage() {
  const { selectedId, action } = Route.useSearch();
  const navigate = useNavigate({ from: Route.fullPath });
  // Upstreams, the 5s /latest poll, runtime status, reconnect nudges and
  // 7-day spend in one place; the DetailView polls /latest with the same
  // key, so TanStack dedups it into one request.
  const headroom = useUpstreamHeadroomData();

  // One dialog serves every entry point: "Add upstream" (header, empty state,
  // and the ?action=new deep link used by the command palette) and
  // Connect/Reconnect on an OAuth upstream (card button, reconnect notice,
  // ?action=reconnect).
  const [connectTarget, setConnectTarget] =
    useState<UpstreamConnectTarget | null>(null);
  const openCreate = useCallback(
    () => setConnectTarget({ mode: 'create' }),
    [],
  );
  const handledCreateAction = useRef(false);
  useEffect(() => {
    if (action !== 'new') {
      handledCreateAction.current = false;
      return;
    }
    if (handledCreateAction.current) return;
    handledCreateAction.current = true;
    openCreate();
    navigate({
      replace: true,
      search: (previous) => ({ ...previous, action: undefined }),
    });
  }, [action, navigate, openCreate]);

  const visibleUpstreams = useMemo(
    () => headroom.rows.map((row) => row.upstream),
    [headroom.rows],
  );

  const selected = visibleUpstreams.find((u) => u.id === selectedId) ?? null;
  const select = (id: string | undefined) =>
    navigate({ search: id ? { selectedId: id } : {} });

  useEffect(() => {
    if (!headroom.isLoading && !selected && visibleUpstreams.length > 0) {
      if (window.matchMedia('(min-width: 768px)').matches) {
        navigate({
          search: { selectedId: visibleUpstreams[0].id },
          replace: true,
        });
      }
    }
  }, [headroom.isLoading, selected, visibleUpstreams, navigate]);

  return (
    <PageContainer>
      <PageHeader title="Upstreams" />
      {/* Phones show the list or the selected upstream, never both, so the
          page stays short; from md the detail sits under the list. */}
      <section
        aria-labelledby="upstreams-list-title"
        className={cx('flex-col gap-4', selected ? 'hidden md:flex' : 'flex')}
      >
        <header className="flex flex-wrap items-center justify-between gap-x-6 gap-y-3">
          <div className="flex min-w-0 flex-wrap items-baseline gap-x-3 gap-y-1">
            <h2
              id="upstreams-list-title"
              className="text-title-section text-text"
            >
              Upstreams
            </h2>
            <p className="flex min-h-5 items-center text-body-sm text-text-muted">
              {headroom.isLoading ? (
                <Skeleton as="span" className="h-3 w-28" />
              ) : (
                [
                  `${visibleUpstreams.length} configured`,
                  headroom.reconnectCount > 0
                    ? `${headroom.reconnectCount} need${headroom.reconnectCount === 1 ? 's' : ''} reconnect`
                    : null,
                ]
                  .filter(Boolean)
                  .join(' · ')
              )}
            </p>
          </div>
          <Button variant="primary" iconLeft={<Plus />} onClick={openCreate}>
            Add upstream
          </Button>
        </header>
        <UpstreamHeadroomGrid
          data={headroom}
          empty={<UpstreamsListEmpty onCreate={openCreate} />}
          onSelect={select}
          selectedId={selectedId}
          variant="list"
        />
      </section>

      {selected ? (
        <DetailView
          key={selected.id}
          upstream={selected}
          onBack={() => select(undefined)}
          onConnect={() =>
            setConnectTarget({ mode: 'reconnect', upstream: selected })
          }
        />
      ) : headroom.isLoading ? (
        <UpstreamDetailLoadingShell />
      ) : visibleUpstreams.length ? (
        <div className="hidden md:block">
          <EmptyState
            headingLevel={2}
            title="Select an upstream"
            description="Pick an upstream from the list to see its configuration, OAuth state, and recent requests."
          />
        </div>
      ) : null}

      <UpstreamConnectDialog
        target={connectTarget}
        onClose={() => setConnectTarget(null)}
        onCreated={(created) => select(created.id)}
      />
    </PageContainer>
  );
}

const QUOTA_HISTORY_RANGES = TIME_PRESETS;
const QUOTA_HISTORY_RANGE_OPTIONS = TIME_PRESET_OPTIONS;
// Loading placeholder mirrors SegmentedControl (size md) so nothing shifts.
const QUOTA_HISTORY_RANGE_GROUP_CLASS =
  'inline-flex items-center gap-0.5 rounded-sm border border-subtle bg-overlay-2 p-0.5';
const QUOTA_HISTORY_RANGE_ITEM_CLASS =
  'h-9 md:h-[1.625rem] px-2.5 text-xs rounded-sm';
// Window cards reserve one gauge row (dial, label, reset and observed facts,
// ETA and burn) so the loaded cards never push the chart down.
const QUOTA_WINDOW_GRID_CLASS =
  'grid min-h-[22rem] gap-x-10 gap-y-8 sm:gap-y-10 [grid-template-columns:repeat(auto-fill,minmax(13rem,1fr))]';
const QUOTA_CHART_HEIGHT = 280;
const DETAIL_GRID_CLASS =
  'grid gap-x-12 gap-y-12 lg:grid-cols-[minmax(0,1fr)_20rem] xl:grid-cols-[minmax(0,1fr)_22rem]';
/** The one series drawn dashed, so 7d and 7d (Fable) stay apart without a new hue. */
const DASHED_WINDOW = '7d_fable';
/** Headroom thresholds on the chart: warn below 20% left, danger below 5%. */
const HEADROOM_THRESHOLDS = [
  { y: 100 - QUOTA_WARN_PCT, tone: 'warn' },
  { y: 100 - QUOTA_DANGER_PCT, tone: 'danger' },
] as const;

function IdentitySkeleton() {
  return (
    <div data-testid="upstream-metadata-loading" className="flex flex-col">
      {['w-28', 'w-24', 'w-32', 'w-40', 'w-36'].map((width) => (
        <div
          key={width}
          className="flex min-h-10 items-center border-t border-border-row py-2"
        >
          <Skeleton className={cx('h-3', width)} />
        </div>
      ))}
    </div>
  );
}

function QuotaWindowCardSkeleton() {
  return (
    <div
      data-testid="quota-snapshot-skeleton-card"
      className="flex flex-col gap-3"
    >
      <Skeleton className="size-32 rounded-full" />
      <Skeleton className="h-5 w-16" />
      <Skeleton className="h-4 w-28" />
      <Skeleton className="mt-2 h-4 w-40" />
      <Skeleton className="h-4 w-36" />
    </div>
  );
}

function UpstreamDetailLoadingShell() {
  return (
    <div
      data-testid="upstream-detail-loading-shell"
      aria-busy="true"
      aria-label="Loading upstream details"
      className="flex flex-col gap-12"
    >
      <div className="flex flex-col gap-2">
        <Skeleton className="h-4 w-28" />
        <Skeleton className="h-8 w-56" />
        <Skeleton as="span" className="h-4 w-48" />
      </div>
      <div className={DETAIL_GRID_CLASS}>
        <div className="flex min-w-0 flex-col gap-12">
          <Section title="Quota windows">
            <div
              data-testid="quota-snapshot-grid"
              className={QUOTA_WINDOW_GRID_CLASS}
            >
              {Array.from({ length: 3 }).map((_, index) => (
                <QuotaWindowCardSkeleton key={index} />
              ))}
            </div>
          </Section>
          <Section
            title="Quota history"
            action={
              <div
                aria-hidden="true"
                className={QUOTA_HISTORY_RANGE_GROUP_CLASS}
                data-testid="quota-history-range-control"
              >
                {QUOTA_HISTORY_RANGES.map((range) => (
                  <span
                    key={range}
                    className={cx(
                      'skeleton inline-flex items-center justify-center',
                      QUOTA_HISTORY_RANGE_ITEM_CLASS,
                    )}
                  >
                    <span className="invisible">{range}</span>
                  </span>
                ))}
              </div>
            }
          >
            <div>
              <div
                data-testid="quota-history-legend-slot"
                className="mb-3 flex min-h-5 flex-wrap items-center gap-x-5 gap-y-1"
              >
                <Skeleton className="h-3 w-20" />
                <Skeleton className="h-3 w-20" />
              </div>
              <Skeleton
                className="w-full"
                style={{ height: QUOTA_CHART_HEIGHT }}
              />
            </div>
          </Section>
        </div>
        <aside aria-label="Identity" className="flex flex-col gap-4">
          <Skeleton className="h-5 w-20" />
          <div
            data-testid="upstream-detail-loading-metadata"
            className="min-h-9"
          >
            <IdentitySkeleton />
          </div>
        </aside>
      </div>
    </div>
  );
}

function WindowFact({
  label,
  children,
}: {
  label: string;
  children: React.ReactNode;
}) {
  return (
    <div className="grid grid-cols-[6rem_minmax(0,1fr)] items-baseline gap-x-3">
      <dt className="text-label text-text-muted">{label}</dt>
      <dd className="min-w-0 text-body-sm tabular-nums text-text">
        {children}
      </dd>
    </div>
  );
}

type OAuthBadge = { tone: 'ok' | 'warn' | 'danger' | 'neutral'; label: string };

// Refresh tokens live on a multi-week clock, so "soon" is measured in days.
function refreshTokenExpiryTone(
  expiresAtUnixSecs: number,
): 'ok' | 'warn' | 'danger' {
  const now = Math.floor(Date.now() / 1000);
  if (expiresAtUnixSecs <= now) return 'danger';
  if (expiresAtUnixSecs - now < REFRESH_EXPIRING_SOON_SECS) return 'warn';
  return 'ok';
}

// This badge describes the login lifecycle, not the access token. For a
// refreshing credential a lapsed access token is routine and renews silently
// while a refresh token exists, so the refresh-token deadline drives the badge.
// A long-lived credential never refreshes, so its own access-token expiry IS
// the login deadline and the refresh token is ignored entirely.
function oauthBadge(entry: UpstreamOAuthStatusResponse): OAuthBadge {
  if (entry.status === 'corrupted')
    return { tone: 'danger', label: 'Reconnect required' };
  const now = Math.floor(Date.now() / 1000);
  if (entry.mode === 'long_lived_365d') {
    const exp = entry.expires_at_unix_secs;
    if (exp == null) return { tone: 'neutral', label: 'Unknown' };
    if (exp <= now) return { tone: 'danger', label: 'Login expired' };
    if (exp - now < LONG_LIVED_EXPIRING_SOON_SECS)
      return { tone: 'warn', label: 'Login expiring' };
    return { tone: 'ok', label: 'Long-lived' };
  }
  if (!entry.refresh_token_present)
    return { tone: 'warn', label: 'Refresh missing' };
  const refreshExp = entry.refresh_token_expires_at_unix_secs;
  if (refreshExp == null) return { tone: 'ok', label: 'Connected' };
  if (refreshExp <= now) return { tone: 'danger', label: 'Login expired' };
  if (refreshExp - now < REFRESH_EXPIRING_SOON_SECS)
    return { tone: 'warn', label: 'Login expiring' };
  return { tone: 'ok', label: 'Connected' };
}

function isQuotaWindow(name: string): name is SubscriptionQuotaWindow {
  return name in WINDOW_LABELS;
}

/** Canonical window names come from `WINDOW_LABELS`; the overage window is
 *  spelled in sentence case here. */
function windowLabel(windowName: string): string {
  if (windowName === 'overage') return 'Extra usage';
  return isQuotaWindow(windowName) ? WINDOW_LABELS[windowName] : windowName;
}

/** Card-header caption: when the reading was taken, with a warn dot only
 *  when it is stale. The source sits in the hint. */
function SnapshotStatusComposite({ snap }: { snap: QuotaSnapshot }) {
  const source =
    snap.source === 'api' ? 'API' : snap.source === 'header' ? 'Header' : '—';
  const observed = snap.state === 'fresh' || snap.state === 'stale';
  return (
    <Hint label={`Source: ${source}`}>
      <span
        tabIndex={0}
        className="relative inline-flex cursor-help items-center gap-1.5 self-start whitespace-nowrap rounded-sm text-caption text-text-faint"
      >
        {snap.state === 'stale' ? (
          <>
            <span aria-hidden="true" className="status-dot warn" />
            <span className="sr-only">Stale:</span>
          </>
        ) : null}
        {observed ? (
          <span>
            Updated <QuotaObservedAt snapshot={snap} />
          </span>
        ) : (
          'No data'
        )}
      </span>
    </Hint>
  );
}

function PromotionalCreditsBadge({
  orgMeta,
}: {
  orgMeta: OrganizationMetadataInner | null | undefined;
}) {
  const hasAny =
    orgMeta?.overage_credit_granted === true ||
    orgMeta?.overage_credit_eligible === true ||
    (orgMeta?.overage_credit_amount_minor_units != null &&
      orgMeta.overage_credit_amount_minor_units > 0);
  if (!hasAny || !orgMeta) {
    return null;
  }
  const amount = orgMeta.overage_credit_amount_minor_units;
  const currency = orgMeta.overage_credit_currency ?? 'USD';
  const amountText =
    amount == null
      ? '—'
      : `${(amount / 100).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })} ${currency}`;
  const status = orgMeta.overage_credit_granted
    ? 'Granted'
    : orgMeta.overage_credit_eligible
      ? 'Eligible'
      : 'Available';
  return (
    <Notice tone={orgMeta.overage_credit_granted ? 'success' : 'info'}>
      Promo credits:{' '}
      <span className="tabular-nums text-text">{amountText}</span> · {status}
    </Notice>
  );
}

function TrialBanner({
  orgMeta,
}: {
  orgMeta: OrganizationMetadataInner | null | undefined;
}) {
  if (!orgMeta?.claude_code_trial_ends_at) return null;
  return (
    <Notice>
      Claude Code trial ends{' '}
      <RelativeTime ts={orgMeta.claude_code_trial_ends_at * 1000} />
    </Notice>
  );
}

function PaymentWarning({
  orgMeta,
}: {
  orgMeta: OrganizationMetadataInner | null | undefined;
}) {
  if (!orgMeta?.payment_auth_hosted_invoice_url) return null;
  return (
    <Notice
      tone="warning"
      action={
        <a
          href={orgMeta.payment_auth_hosted_invoice_url}
          target="_blank"
          rel="noreferrer"
          className="inline-flex items-center gap-1 text-text underline-offset-2 hover:underline"
        >
          Open invoice <ExternalLink className="w-3 h-3" />
        </a>
      }
    >
      Payment authorization is required for this organization.
    </Notice>
  );
}

function DetailView({
  upstream,
  onBack,
  onConnect,
}: {
  upstream: Upstream;
  onBack: () => void;
  /** Opens the shared connect dialog at the Claude sign-in step. */
  onConnect: () => void;
}) {
  const { action } = Route.useSearch();
  const navigate = useNavigate({ from: Route.fullPath });
  const toggle = useUpdateUpstreamWarmupSettings();
  const del = useDeleteUpstream();
  const subscriptionMetadataQ = useUpstreamSubscriptionMetadata(upstream.id);
  const triggerSubscriptionMetadataRefresh =
    useTriggerSubscriptionMetadataRefresh();
  const upstreamOAuthQ = useUpstreamOAuthStatus(
    upstream.kind === 'anthropic_oauth' ? upstream.id : null,
  );
  const statusQ = useStatus();
  const principalNameMap = usePrincipalNameMap();
  const upstreamNameMap = useUpstreamNameMap();
  const upstreamRuntimeStatus = useMemo(
    () => statusQ.data?.upstreams.find((u) => u.id === upstream.id) ?? null,
    [statusQ.data, upstream.id],
  );
  const [confirmDeleteOpen, setConfirmDeleteOpen] = useState(false);
  // Requested `enabled` value awaiting confirmation. `enabled` outlives
  // `open` so the dialog copy stays put during its close transition.
  const [enabledConfirm, setEnabledConfirm] = useState({
    open: false,
    enabled: false,
  });
  const confirmEnabled = enabledConfirm.enabled;
  const [range, setRange] = useState<TimePreset>('7d');
  const [isolatedWindow, setIsolatedWindow] = useState<string | null>(null);
  const isOauth = upstream.kind === 'anthropic_oauth';

  // ?action=reconnect deep link: consume the param exactly once, then open the
  // connect dialog. The ref survives StrictMode's double effect pass;
  // DetailView is keyed by upstream id, so a selection change remounts and
  // re-arms it. Cancelling the dialog never touches the nudge itself.
  const handledReconnectAction = useRef(false);
  useEffect(() => {
    if (action !== 'reconnect') {
      handledReconnectAction.current = false;
      return;
    }
    if (handledReconnectAction.current) return;
    handledReconnectAction.current = true;
    navigate({
      replace: true,
      search: (previous) => ({ ...previous, action: undefined }),
    });
    if (isOauth) onConnect();
  }, [action, isOauth, onConnect, navigate]);

  // Keep a separate wall clock for snapshot freshness/countdowns. Series and
  // analysis requests use stable range keys and resolve their own absolute
  // bounds when each request starts.
  const [nowUnixSecs, setNowUnixSecs] = useState(() =>
    Math.floor(Date.now() / 1000),
  );
  useEffect(() => {
    const interval = setInterval(() => {
      setNowUnixSecs(Math.floor(Date.now() / 1000));
    }, 60_000);
    return () => clearInterval(interval);
  }, []);
  const { effective: timeZone } = useTimezone();
  const rangeSecs = useMemo(() => {
    switch (range) {
      case '1h':
        return 3600;
      case '6h':
        return 21600;
      case '24h':
        return 86400;
      case '7d':
        return 604800;
    }
  }, [range]);
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

  // Use the same upstreamIds + windows as UpstreamsPage so TanStack dedups
  // the /latest poll into a single request. selectedLatest extracts our row.
  const allUpstreams = useUpstreams();
  const allUpstreamIds = useMemo(
    () => allUpstreams.data?.upstreams.map((u) => u.id).join(',') ?? '',
    [allUpstreams.data],
  );
  // Disabling this upstream leaves the other enabled ones serving the pool.
  const poolImpact = allUpstreams.data
    ? {
        total: allUpstreams.data.upstreams.length,
        remaining: allUpstreams.data.upstreams.filter(
          (u) => u.enabled && u.id !== upstream.id,
        ).length,
      }
    : null;
  const quotaLatest = useSubscriptionQuotaLatest({
    upstreamIds: allUpstreamIds,
    windows: '5h,7d,overage,7d_sonnet,7d_opus,7d_fable',
    source: 'merged',
    refetchInterval: 5_000,
  });
  const selectedLatest = useMemo(
    () =>
      quotaLatest.data?.upstreams.find((u) => u.upstream_id === upstream.id) ??
      null,
    [quotaLatest.data, upstream.id],
  );
  const quotaSeries = useSubscriptionQuotaSeries({
    upstreamIds: upstream.id,
    windows: '5h,7d,7d_sonnet,7d_opus,7d_fable,overage',
    source: 'merged',
    rangeSecs,
    bucketSecs: bucketSecsForRange,
  });
  const quotaAnalysis = useSubscriptionQuotaAnalysis({
    upstreamIds: upstream.id,
    windows: '5h,7d,7d_sonnet,7d_opus,7d_fable,overage',
    source: 'merged',
    rangeSecs,
  });
  const seriesSinceUnixSecs =
    quotaSeries.data?.since_unix_secs ?? nowUnixSecs - rangeSecs;
  const seriesUntilUnixSecs = quotaSeries.data?.until_unix_secs ?? nowUnixSecs;

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
    return buildQuotaChartData(
      quotaSeries.data?.series,
      selectedLatest?.windows,
      range,
    );
  }, [quotaSeries.data, selectedLatest, range]);

  const visibleGraphWindows = useMemo(
    () =>
      selectVisibleGraphWindows({
        latestWindows: selectedLatest?.windows,
        series: quotaSeries.data?.series,
        sinceUnixSecs: seriesSinceUnixSecs,
      }),
    [selectedLatest, quotaSeries.data, seriesSinceUnixSecs],
  );

  const effectiveIsolatedWindow =
    isolatedWindow && visibleGraphWindows.includes(isolatedWindow)
      ? isolatedWindow
      : null;

  // The chart plots headroom (100 - used), like every other quota figure.
  const chartRows = useMemo(
    () =>
      chartData.rows
        .filter((row) => row.unix >= seriesSinceUnixSecs)
        .map((row) => {
          const next: typeof row = { ...row };
          for (const [key, value] of Object.entries(row)) {
            if (key !== 'unix' && typeof value === 'number') {
              next[key] = Math.min(100, Math.max(0, 100 - value));
            }
          }
          return next;
        }),
    [chartData.rows, seriesSinceUnixSecs],
  );

  const xDomain = useMemo<[number, number]>(
    () => [seriesSinceUnixSecs, seriesUntilUnixSecs],
    [seriesSinceUnixSecs, seriesUntilUnixSecs],
  );

  const recent = useRecentEvents({
    upstream_id: upstream.id,
    limit: '5',
    event_kind: 'messages',
  });
  const recentForUpstream = useMemo(() => {
    return (recent.data?.events ?? [])
      .filter(isMessagesRequestEvent)
      .map((e) => ({
        ...e,
        _phase: 'final' as const,
      }));
  }, [recent.data]);

  const metadataPending =
    isOauth &&
    subscriptionMetadataQ.data === undefined &&
    subscriptionMetadataQ.isPending;
  const quotaLatestPending =
    quotaLatest.data === undefined && quotaLatest.isPending;
  const quotaHistoryPending =
    quotaLatestPending ||
    (quotaSeries.data === undefined && quotaSeries.isPending);
  const quotaAnalysisPending =
    quotaAnalysis.data === undefined && quotaAnalysis.isPending;
  const analysis = quotaAnalysis.data?.upstreams[0];
  const a5h = analysis?.windows.find((w) => w.window === '5h');
  const caveats = Array.from(
    new Set(analysis?.windows.flatMap((w) => w.caveats) ?? []),
  ).filter(
    (c) =>
      c.toLowerCase().trim() !==
      'capacity is inferred from proxy tokens and quota utilization; anthropic quota units are not directly exposed',
  );
  const oauthStatusPending =
    isOauth && upstreamOAuthQ.data === undefined && upstreamOAuthQ.isPending;
  const recentPending = recent.data === undefined && recent.isPending;
  const subMeta = subscriptionMetadataQ.data?.subscription_metadata;
  const orgMeta = subscriptionMetadataQ.data?.organization_metadata;
  const principalEntry = upstreamOAuthQ.data?.has_credentials
    ? upstreamOAuthQ.data
    : null;
  const hasBoundToken = Boolean(principalEntry);
  const oauthStatusBadge: OAuthBadge = principalEntry
    ? oauthBadge(principalEntry)
    : { tone: 'neutral', label: 'Not connected' };
  const reconnectNudge = isOauth
    ? classifyOAuthReconnect(
        upstreamOAuthQ.data,
        upstream.status.last_apply_error,
        nowUnixSecs,
      )
    : null;
  const headerHealth = upstreamHealth(
    upstream.enabled,
    upstreamRuntimeStatus?.status,
    reconnectNudge,
  );

  // The toggle PATCH is not optimistic, so `upstream.enabled` still holds the
  // pre-request value while it is in flight; the requested value lives in the
  // in-flight mutation variables.
  const toggleRequestedEnabled =
    toggle.variables?.body.enabled ?? !upstream.enabled;
  const togglePendingLabel = toggle.isPending
    ? toggleRequestedEnabled
      ? 'Enabling...'
      : 'Disabling...'
    : null;

  const fields: {
    label: string;
    value: React.ReactNode;
    tooltip: string;
  }[] = [];

  const commonIdFields: typeof fields = [
    {
      label: 'ID',
      value: (
        <span className="break-all font-mono text-data">{upstream.id}</span>
      ),
      tooltip: 'Internal upstream identifier',
    },
  ];

  if (isOauth) {
    fields.push(...commonIdFields);
    if (orgMeta?.organization_type)
      fields.push({
        label: 'Plan',
        value: (
          <span className="font-mono text-data">
            {orgMeta.organization_type}
          </span>
        ),
        tooltip: 'Anthropic subscription tier',
      });
    if (orgMeta?.rate_limit_tier)
      fields.push({
        label: 'Rate',
        value: (
          <span className="font-mono text-data">{orgMeta.rate_limit_tier}</span>
        ),
        tooltip:
          'Rate-limit tier (Max 5x = base plan, Max 20x = power user, Pro = Pro plan)',
      });
    if (orgMeta?.has_extra_usage_enabled != null)
      fields.push({
        label: 'Extra usage billing',
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
        value: (
          <span className="font-mono text-data">{orgMeta.billing_type}</span>
        ),
        tooltip: 'How the subscription is billed',
      });
  } else {
    fields.push(...commonIdFields);
    fields.push({
      label: 'Base URL',
      value: (
        <span className="font-mono text-data">
          {upstream.base_url || DEFAULT_ANTHROPIC_BASE_URL}
        </span>
      ),
      tooltip: upstream.base_url
        ? 'Endpoint base URL for upstream requests'
        : `Endpoint base URL for upstream requests (default: ${DEFAULT_ANTHROPIC_BASE_URL})`,
    });
    if (upstream.api_key_env)
      fields.push({
        label: 'API key',
        value: (
          <span className="font-mono text-data">
            env:{upstream.api_key_env}
          </span>
        ),
        tooltip: `Loaded from the ${upstream.api_key_env} environment variable on the server`,
      });
    else if (upstream.kind === 'anthropic_api_key')
      fields.push({
        label: 'API key',
        value: 'Literal',
        tooltip: 'Stored inline (literal API key)',
      });
    if (upstreamRuntimeStatus?.last_apply_error)
      fields.push({
        label: 'Apply error',
        value: (
          <span className="text-danger-text">
            {upstreamRuntimeStatus.last_apply_error}
          </span>
        ),
        tooltip: 'Most recent failed reconciliation',
      });
  }

  const planSummary = isOauth
    ? [subscriptionPlanLabel(orgMeta), orgMeta?.organization_name]
        .filter(Boolean)
        .join(' · ') || 'Claude subscription'
    : 'API key';

  const identity = (
    <aside
      aria-labelledby="upstream-identity-title"
      className="flex min-w-0 flex-col gap-4"
    >
      <header className="flex min-h-7 items-center justify-between gap-3">
        <h2
          id="upstream-identity-title"
          className="text-title-section text-text"
        >
          Identity
        </h2>
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
                        ? error.message || `Request failed (${error.status})`
                        : error instanceof Error
                          ? error.message
                          : String(error);
                    toast.error(`Metadata refresh failed: ${message}`);
                  },
                });
              }}
              className="inline-flex size-7 items-center justify-center rounded-sm text-text-muted hover:bg-hover-bg hover:text-text disabled:opacity-40"
              aria-label="Refresh subscription metadata"
            >
              <RefreshCw
                strokeWidth={1.75}
                className={cx(
                  'size-3.5',
                  triggerSubscriptionMetadataRefresh.isPending &&
                    'animate-spin',
                )}
              />
            </button>
          </Hint>
        )}
      </header>
      <div data-testid="upstream-metadata-strip" className="min-h-9">
        {metadataPending ? (
          <IdentitySkeleton />
        ) : (
          <dl className="flex flex-col">
            {fields.map((f) => (
              <div
                key={f.label}
                className="grid grid-cols-[minmax(0,8rem)_minmax(0,1fr)] items-baseline gap-x-4 border-t border-border-row py-2.5"
              >
                <dt className="text-label text-text-muted">
                  <Hint label={f.tooltip}>
                    <span
                      tabIndex={0}
                      className="inline-flex cursor-help items-center gap-1 rounded-sm hover:text-text"
                    >
                      {f.label}
                      <Info
                        aria-hidden="true"
                        strokeWidth={1.75}
                        className="size-3 opacity-60"
                      />
                    </span>
                  </Hint>
                </dt>
                <dd className="min-w-0 break-words text-body text-text">
                  {f.value}
                </dd>
              </div>
            ))}
          </dl>
        )}
      </div>
    </aside>
  );

  const quotaWindows = (
    <Section
      title="Quota windows"
      subtitle="Headroom per window with its reset time"
    >
      {quotaLatestPending ? (
        <div
          data-testid="quota-snapshot-grid"
          className={QUOTA_WINDOW_GRID_CLASS}
        >
          {Array.from({ length: 3 }).map((_, index) => (
            <QuotaWindowCardSkeleton key={index} />
          ))}
        </div>
      ) : !selectedLatest ? (
        <div
          data-testid="quota-snapshot-grid"
          className={QUOTA_WINDOW_GRID_CLASS}
        >
          <div className="col-span-full flex items-center justify-center">
            <EmptyState
              title={`No subscription quota data for ${upstream.name}`}
            />
          </div>
        </div>
      ) : (
        <div
          data-testid="quota-snapshot-grid"
          className={QUOTA_WINDOW_GRID_CLASS}
        >
          {selectQuotaCardSnapshots({
            latestWindows: selectedLatest.windows,
            nowUnixSecs,
          }).map((snap) => {
            const isOverage =
              snap.window === 'overage' &&
              (snap.extra_usage_enabled ||
                snap.extra_usage_monthly_limit != null);
            const overageLimit = snap.extra_usage_monthly_limit;
            const overageUsed = snap.extra_usage_used_credits;
            const usedPct = isOverage
              ? overageLimit != null && overageUsed != null && overageLimit > 0
                ? (overageUsed / overageLimit) * 100
                : null
              : snap.utilization == null
                ? null
                : snap.utilization * 100;
            const windowAnalysis = analysis?.windows.find(
              (w) => w.window === snap.window,
            );
            const actualBurn =
              windowAnalysis?.actual_account_burn.utilization_per_second;
            const projBurn =
              windowAnalysis?.proxy_projected_burn.utilization_per_hour;
            const eta = windowAnalysis?.actual_account_burn.eta_to_limit_secs;
            const windowNotStarted =
              !isOverage &&
              (snap.utilization == null ||
                snap.resets_at_unix_secs == null ||
                snap.resets_at_unix_secs <= nowUnixSecs);
            const waitingForGrowth =
              !isOverage &&
              !windowNotStarted &&
              (!windowAnalysis ||
                windowAnalysis.actual_account_burn.reason ===
                  'insufficient_growth_intervals');
            const status =
              snap.status && snap.status !== 'allowed' ? snap.status : null;
            let caption: string;
            if (isOverage) {
              caption =
                overageLimit != null && overageUsed != null
                  ? `$${((overageLimit - overageUsed) / 100).toFixed(2)} of $${(
                      overageLimit / 100
                    ).toLocaleString('en-US', {
                      minimumFractionDigits: 2,
                      maximumFractionDigits: 2,
                    })} USD left`
                  : 'Enabled — no limit set';
            } else if (usedPct == null) {
              caption = snap.status ?? 'No reading';
            } else {
              caption = `${formatQuotaPercent(usedPct)} used${status ? ` · ${status}` : ''}`;
            }
            return (
              <article
                key={snap.window}
                data-window={snap.window}
                className="flex min-w-0 flex-col gap-4"
              >
                {/* Phones read a linear meter; the dial needs the room. */}
                <div className="hidden sm:block">
                  <ArcGauge
                    caption={caption}
                    label={windowLabel(snap.window)}
                    usedPct={usedPct}
                  />
                </div>
                <div className="flex flex-col gap-2 sm:hidden">
                  <span className="text-title-section text-text">
                    {windowLabel(snap.window)}
                  </span>
                  <HeadroomMeter
                    label={windowLabel(snap.window)}
                    size="md"
                    usedPct={usedPct}
                  />
                  {isOverage || usedPct == null || status ? (
                    <span className="text-body-sm text-text-muted">
                      {caption}
                    </span>
                  ) : null}
                </div>
                <dl className="flex flex-col gap-2">
                  {!isOverage || snap.resets_at_unix_secs ? (
                    <WindowFact label="Reset">
                      {windowNotStarted ? (
                        'Not started — begins on first request or warm-up'
                      ) : snap.resets_at_unix_secs ? (
                        <>
                          {formatQuotaStamp(snap.resets_at_unix_secs, timeZone)}
                          <span className="block text-caption text-text-muted">
                            <ResetCountdown
                              compact
                              ts={snap.resets_at_unix_secs * 1000}
                            />
                          </span>
                        </>
                      ) : (
                        'No reset reported'
                      )}
                    </WindowFact>
                  ) : null}
                  {!isOverage && !windowNotStarted && (
                    <>
                      <WindowFact label="ETA to limit">
                        {quotaAnalysisPending ? (
                          <Skeleton className="h-5 w-24" />
                        ) : eta != null && eta <= 0 ? (
                          'Already saturated'
                        ) : (
                          <RelativeOffsetTime compact offsetSeconds={eta} />
                        )}
                      </WindowFact>
                      <WindowFact label="Burn">
                        {quotaAnalysisPending ? (
                          <Skeleton className="h-4 w-28" />
                        ) : (
                          <>
                            {actualBurn == null
                              ? '—'
                              : `${(actualBurn * 60 * 100).toFixed(2)}%/min`}
                            <span className="block text-caption text-text-muted">
                              {'projected '}
                              {projBurn == null
                                ? '—'
                                : `${((projBurn / 60) * 100).toFixed(2)}%/min`}
                            </span>
                          </>
                        )}
                      </WindowFact>
                    </>
                  )}
                </dl>
                {!quotaAnalysisPending && waitingForGrowth && (
                  <p className="text-body-sm text-warn-text">
                    Waiting for utilization to rise — burn appears once growth
                    is observed.
                  </p>
                )}
                <SnapshotStatusComposite snap={snap} />
              </article>
            );
          })}
        </div>
      )}
    </Section>
  );

  const quotaHistory = (
    <Section
      title="Quota history"
      subtitle="Headroom per window; higher is better"
      action={
        <div data-testid="quota-history-range-control">
          <SegmentedControl
            ariaLabel="Quota history range"
            value={range}
            onChange={setRange}
            options={QUOTA_HISTORY_RANGE_OPTIONS}
          />
        </div>
      }
    >
      <div>
        <div
          data-testid="quota-history-legend-slot"
          className="mb-3 flex min-h-5 flex-wrap items-center gap-x-5 gap-y-1"
        >
          {quotaHistoryPending ? (
            <>
              <Skeleton className="h-3 w-20" />
              <Skeleton className="h-3 w-20" />
            </>
          ) : chartData.rows.length > 0 && visibleGraphWindows.length > 0 ? (
            <>
              {visibleGraphWindows.map((windowName) => {
                const dimmed =
                  effectiveIsolatedWindow !== null &&
                  effectiveIsolatedWindow !== windowName;
                const current = selectedLatest?.windows.find(
                  (w) => w.window === windowName,
                )?.utilization;
                return (
                  <button
                    key={windowName}
                    type="button"
                    onClick={() =>
                      setIsolatedWindow((prev) =>
                        prev === windowName ? null : windowName,
                      )
                    }
                    aria-pressed={effectiveIsolatedWindow === windowName}
                    className={cx(
                      'flex min-h-6 cursor-pointer items-center gap-1.5 rounded-sm text-body-sm transition-opacity',
                      dimmed ? 'opacity-40 hover:opacity-70' : '',
                    )}
                  >
                    <span
                      aria-hidden="true"
                      className="w-3 border-t-2"
                      style={{
                        borderColor: getWindowColor(windowName).stroke,
                        borderStyle:
                          windowName === DASHED_WINDOW ? 'dashed' : 'solid',
                      }}
                    />
                    <span className="text-text-muted">
                      {windowLabel(windowName)}
                    </span>
                    {current != null ? (
                      <span className="tabular-nums text-text">
                        {formatHeadroom(current * 100)}
                      </span>
                    ) : null}
                  </button>
                );
              })}
              {HEADROOM_THRESHOLDS.map(({ y, tone }) => (
                <span
                  key={tone}
                  className="flex items-center gap-1.5 text-body-sm text-text-muted"
                >
                  <span
                    aria-hidden="true"
                    className={cx(
                      'w-3 border-t border-dashed',
                      tone === 'warn' ? 'border-warn' : 'border-danger',
                    )}
                  />
                  {tone === 'warn' ? 'Warn' : 'Danger'} below {y}% left
                </span>
              ))}
            </>
          ) : null}
        </div>
        <div
          className="w-full"
          style={{ minWidth: 0, height: QUOTA_CHART_HEIGHT }}
        >
          {quotaHistoryPending ? (
            <Skeleton className="h-full w-full" />
          ) : !chartRows.length || !visibleGraphWindows.length ? (
            <EmptyState title="No data in range" />
          ) : (
            <ResponsiveContainer width="100%" height={QUOTA_CHART_HEIGHT}>
              <AreaChart
                data={chartRows}
                margin={{ top: 20, right: 12, bottom: 0, left: 0 }}
              >
                <CartesianGrid {...CHART_GRID} />
                <XAxis
                  {...CHART_AXIS}
                  dataKey="unix"
                  type="number"
                  domain={xDomain}
                  // Four ticks inset from the edges so the first and last
                  // labels never clip.
                  ticks={[1, 3, 5, 7].map(
                    (i) =>
                      seriesSinceUnixSecs +
                      ((seriesUntilUnixSecs - seriesSinceUnixSecs) * i) / 8,
                  )}
                  interval={0}
                  tickFormatter={(val) => {
                    const d = new Date(Number(val) * 1000);
                    return range === '7d' || range === '24h'
                      ? `${d.getMonth() + 1}/${d.getDate()} ${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`
                      : `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`;
                  }}
                />
                <YAxis
                  {...CHART_AXIS}
                  tickFormatter={(val) => `${val}%`}
                  ticks={[0, 25, 50, 75, 100]}
                  width={44}
                  domain={[0, 100]}
                  allowDataOverflow={false}
                />
                {HEADROOM_THRESHOLDS.map(({ y, tone }) => (
                  <ReferenceLine
                    key={y}
                    y={y}
                    {...CHART_THRESHOLD[tone]}
                    ifOverflow="extendDomain"
                  />
                ))}
                <Tooltip
                  cursor={CHART_CURSOR}
                  content={({ active, payload, label }) => {
                    if (!active || !payload?.length) return null;
                    const first = selectedLatest?.windows[0];
                    return (
                      <div className="glass-strong min-w-40 rounded-md px-3 py-2 text-body-sm text-text">
                        <div className="mb-1 text-caption text-text-muted">
                          {fmtChartTooltipTs(Number(label))}
                        </div>
                        {payload.map((p, i) => {
                          const key = String(p.dataKey);
                          return (
                            <div
                              key={i}
                              className="flex items-center justify-between gap-4 py-px"
                            >
                              <span className="flex items-center gap-1.5 text-text-muted">
                                <span
                                  aria-hidden="true"
                                  className="w-3 border-t-2"
                                  style={{
                                    borderColor: getWindowColor(key).stroke,
                                    borderStyle:
                                      key === DASHED_WINDOW
                                        ? 'dashed'
                                        : 'solid',
                                  }}
                                />
                                {windowLabel(key)}
                              </span>
                              <span className="tabular-nums">
                                {typeof p.value === 'number'
                                  ? formatHeadroom(100 - p.value)
                                  : '—'}
                              </span>
                            </div>
                          );
                        })}
                        {first?.source && (
                          <div className="mt-1.5 border-t border-subtle pt-1.5 text-caption text-text-muted">
                            Source: {first.source} · observed{' '}
                            <QuotaObservedAt snapshot={first} />
                          </div>
                        )}
                      </div>
                    );
                  }}
                />
                {(() => {
                  const visible =
                    2 * (seriesUntilUnixSecs - seriesSinceUnixSecs);
                  const markers: ChartMarker[] = [];
                  const paired = new Map<
                    string,
                    { start?: ChartMarker; reset?: ChartMarker }
                  >();
                  for (const marker of chartData.markers) {
                    let bucket = paired.get(marker.window);
                    if (bucket === undefined) {
                      bucket = {};
                      paired.set(marker.window, bucket);
                    }
                    if (marker.kind === 'start') bucket.start = marker;
                    else if (marker.kind === 'reset') bucket.reset = marker;
                    else markers.push(marker);
                  }
                  for (const pair of paired.values()) {
                    if (
                      pair.start &&
                      pair.reset &&
                      Math.abs(pair.reset.ts - pair.start.ts) / visible < 0.25
                    )
                      markers.push(pair.reset);
                    else {
                      if (pair.start) markers.push(pair.start);
                      if (pair.reset) markers.push(pair.reset);
                    }
                  }
                  const span = seriesUntilUnixSecs - seriesSinceUnixSecs;
                  // Markers that land on (nearly) the same instant
                  // share one line and one label, so "7d start" and
                  // "7d (Fable) start" never print over each other.
                  const groups: {
                    ts: number;
                    kind: ChartMarker['kind'];
                    windows: string[];
                  }[] = [];
                  for (const marker of [...markers].sort(
                    (a, b) => a.ts - b.ts,
                  )) {
                    if (
                      marker.ts < seriesSinceUnixSecs ||
                      marker.ts > seriesUntilUnixSecs + span ||
                      !visibleGraphWindows.includes(marker.window) ||
                      (effectiveIsolatedWindow !== null &&
                        effectiveIsolatedWindow !== marker.window)
                    )
                      continue;
                    const group = groups.find(
                      (g) =>
                        g.kind === marker.kind &&
                        Math.abs(g.ts - marker.ts) <= span * 0.01,
                    );
                    if (group) group.windows.push(marker.window);
                    else
                      groups.push({
                        ts: marker.ts,
                        kind: marker.kind,
                        windows: [marker.window],
                      });
                  }
                  // Labels of neighbouring lines alternate between
                  // above the plot and just inside its top edge.
                  let previousTs = Number.NEGATIVE_INFINITY;
                  let previousRaised = false;
                  return groups.map((group) => {
                    const crowded = group.ts - previousTs < span * 0.15;
                    const raised = !(crowded && previousRaised);
                    previousTs = group.ts;
                    previousRaised = raised;
                    const firstWindow = group.windows[0] ?? '';
                    const names = group.windows.map(windowLabel).join(', ');
                    return (
                      <ReferenceLine
                        key={`marker-${group.kind}-${group.ts}`}
                        x={group.ts}
                        stroke={getWindowColor(firstWindow).stroke}
                        strokeOpacity={0.5}
                        strokeDasharray={group.kind === 'start' ? '4 6' : '2 4'}
                      >
                        <Label
                          value={
                            group.kind === 'start'
                              ? `${names} start`
                              : `${names} reset`
                          }
                          position={raised ? 'top' : 'insideTop'}
                          fontSize={12}
                          fill="var(--color-text-muted)"
                        />
                      </ReferenceLine>
                    );
                  });
                })()}
                {visibleGraphWindows.map((windowName) => (
                  <Area
                    key={windowName}
                    type="monotone"
                    dataKey={windowName}
                    stroke={getWindowColor(windowName).stroke}
                    strokeWidth={1.5}
                    strokeDasharray={
                      windowName === DASHED_WINDOW ? '5 3' : undefined
                    }
                    fill="none"
                    isAnimationActive={false}
                    connectNulls={false}
                    hide={
                      effectiveIsolatedWindow !== null &&
                      effectiveIsolatedWindow !== windowName
                    }
                  />
                ))}
              </AreaChart>
            </ResponsiveContainer>
          )}
        </div>
        {quotaHistoryPending ? null : (
          <p className="mt-3 text-caption text-text-muted">
            {formatQuotaStamp(seriesSinceUnixSecs, timeZone)} →{' '}
            {formatQuotaStamp(seriesUntilUnixSecs, timeZone)}
            {selectedLatest?.windows[0] ? (
              <>
                {' · provider data observed '}
                <QuotaObservedAt snapshot={selectedLatest.windows[0]} />
              </>
            ) : null}
          </p>
        )}
      </div>
    </Section>
  );

  return (
    <>
      <section
        aria-labelledby="upstream-detail-title"
        className="flex flex-col gap-12"
      >
        <div className="flex flex-col gap-3">
          <button
            type="button"
            onClick={onBack}
            className="-ml-1 inline-flex min-h-9 w-fit items-center gap-1 rounded-sm px-1 text-body text-text-muted hover:text-text md:hidden"
          >
            <ChevronLeft className="size-4" strokeWidth={1.75} /> All upstreams
          </button>
          <div className="flex flex-wrap items-end justify-between gap-x-6 gap-y-4">
            <div className="min-w-0">
              <p className="text-label text-text-muted">Selected upstream</p>
              <h2 id="upstream-detail-title" className="mt-1 break-words">
                <InlineNameEditor upstream={upstream} />
              </h2>
              <p className="mt-1 flex min-h-5 items-center text-body text-text-muted">
                {metadataPending ? (
                  <Skeleton as="span" className="h-4 w-48" />
                ) : (
                  planSummary
                )}
              </p>
            </div>
            <div className="flex flex-wrap items-center gap-3">
              <ToggleSwitch
                variant="compact"
                role="switch"
                aria-label="Enabled"
                label={upstream.enabled ? 'Enabled' : 'Disabled'}
                checked={upstream.enabled}
                disabled={toggle.isPending}
                onChange={(e) =>
                  setEnabledConfirm({ open: true, enabled: e.target.checked })
                }
                className="flex-row-reverse"
              />
              {/* One danger surface per problem: when the reconnect notice
                  below owns the diagnosis, the head stays quiet. */}
              {headerHealth.tone === 'danger' && !reconnectNudge ? (
                <StatusBadge tone="danger" label={headerHealth.label} />
              ) : null}
              {togglePendingLabel ? (
                <span
                  role="status"
                  aria-live="polite"
                  data-testid="upstream-enabled-pending"
                  className="inline-flex items-center gap-1.5 text-body-sm text-text-muted"
                >
                  <Spinner className="w-3 h-3 text-text-muted" />
                  {togglePendingLabel}
                </span>
              ) : null}
              <LimitResetAction
                key={upstream.id}
                upstream={upstream}
                quotaWindows={
                  quotaLatest.isError ? [] : (selectedLatest?.windows ?? [])
                }
              />
              <Button
                size="sm"
                variant="danger"
                iconLeft={<Trash2 />}
                onClick={() => setConfirmDeleteOpen(true)}
              >
                Delete
              </Button>
            </div>
          </div>
        </div>

        {reconnectNudge ? (
          <OAuthReconnectNotice
            nudge={reconnectNudge}
            onReconnect={onConnect}
          />
        ) : null}

        {isOauth &&
        (orgMeta?.claude_code_trial_ends_at ||
          orgMeta?.payment_auth_hosted_invoice_url ||
          orgMeta?.overage_credit_granted ||
          orgMeta?.overage_credit_eligible ||
          (orgMeta?.overage_credit_amount_minor_units != null &&
            orgMeta.overage_credit_amount_minor_units > 0)) ? (
          <div className="space-y-2">
            <TrialBanner orgMeta={orgMeta} />
            <PaymentWarning orgMeta={orgMeta} />
            <PromotionalCreditsBadge orgMeta={orgMeta} />
          </div>
        ) : null}

        <div className={DETAIL_GRID_CLASS}>
          <div className="flex min-w-0 flex-col gap-12">
            {isOauth ? (
              <>
                {quotaWindows}
                {quotaHistory}
              </>
            ) : (
              <ApiUsageCard
                data={apiUsageQ.data}
                isLoading={apiUsageQ.data === undefined && apiUsageQ.isPending}
                range={apiUsageRange}
                onRangeChange={setApiUsageRange}
                metric={apiUsageMetric}
                onMetricChange={setApiUsageMetric}
              />
            )}
          </div>
          {identity}
        </div>

        {isOauth ? (
          <div className="grid gap-12 2xl:grid-cols-2">
            <WarmupCardMinimal
              upstream={upstream}
              credentialNoticeShown={reconnectNudge != null}
            />
            <Section
              title="OAuth status"
              subtitle={
                oauthStatusPending ? (
                  <Skeleton className="h-3 w-32" />
                ) : hasBoundToken ? (
                  'Bound on this upstream'
                ) : (
                  'Not connected'
                )
              }
              action={
                <div className="flex items-center gap-3">
                  {oauthStatusPending ? (
                    <>
                      <Skeleton className="h-5 w-24" />
                      <Skeleton className="h-7 w-24" />
                    </>
                  ) : (
                    <>
                      <StatusBadge
                        tone={oauthStatusBadge.tone}
                        label={oauthStatusBadge.label}
                      />
                      <Button
                        size="sm"
                        iconLeft={<KeyRound />}
                        onClick={onConnect}
                      >
                        {hasBoundToken ? 'Reconnect' : 'Connect'}
                      </Button>
                    </>
                  )}
                </div>
              }
            >
              <div
                data-testid="oauth-status-card-body"
                className="min-h-28 text-body-sm"
              >
                {oauthStatusPending ? (
                  <div
                    data-testid="oauth-status-loading-grid"
                    className="min-h-20 space-y-3"
                  >
                    <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
                      {Array.from({ length: 2 }).map((_, index) => (
                        <div key={index}>
                          <Skeleton className="h-3 w-20" />
                          <Skeleton className="mt-1 h-5 w-28" />
                        </div>
                      ))}
                    </div>
                    <div>
                      <Skeleton className="h-3 w-16" />
                      <Skeleton className="mt-1 h-3 w-full max-w-56" />
                    </div>
                  </div>
                ) : !hasBoundToken ? (
                  <p className="text-body-sm text-text-muted">
                    Use Connect to authorize this upstream with a Claude
                    account.
                  </p>
                ) : principalEntry ? (
                  <div
                    data-testid="oauth-status-loaded-grid"
                    className="min-h-20 space-y-3"
                  >
                    <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
                      <div>
                        <div className="text-label text-text-faint">
                          Access token
                        </div>
                        <div className="mt-1">
                          {principalEntry.expires_at_unix_secs ? (
                            <span>
                              Expires{' '}
                              <RelativeTime
                                ts={
                                  new Date(
                                    principalEntry.expires_at_unix_secs * 1000,
                                  )
                                }
                              />
                            </span>
                          ) : (
                            <span className="text-text-faint">—</span>
                          )}
                        </div>
                        {principalEntry.mode === 'refreshing' ? (
                          <div className="mt-0.5 text-caption text-text-faint">
                            Renews automatically while the refresh token is
                            valid.
                          </div>
                        ) : null}
                      </div>
                      <div>
                        <div className="text-label text-text-faint">
                          Credential mode
                        </div>
                        <div
                          data-testid="oauth-credential-mode"
                          className="mt-1"
                        >
                          {principalEntry.mode === 'long_lived_365d' ? (
                            <>
                              <span>365-day token</span>
                              <div className="mt-0.5 text-caption text-text-faint">
                                Never refreshed — reauthorize once a year
                              </div>
                            </>
                          ) : principalEntry.mode === 'refreshing' ? (
                            <>
                              <span>Refreshing</span>
                              <div className="mt-0.5 text-caption text-text-faint">
                                Auto-refreshes; reauthorize about every 30 days
                              </div>
                            </>
                          ) : (
                            <span className="text-text-faint">Unknown</span>
                          )}
                        </div>
                      </div>
                      <div>
                        <div className="text-label text-text-faint">
                          Refresh token
                        </div>
                        <div className="mt-1">
                          {principalEntry.can_refresh ? (
                            principalEntry.refresh_token_present ? (
                              'Present'
                            ) : (
                              <span className="text-warn-text">Missing</span>
                            )
                          ) : principalEntry.refresh_token_present ? (
                            <span className="text-text-muted">
                              Stored, unused
                            </span>
                          ) : (
                            <span className="text-text-muted">Not stored</span>
                          )}
                        </div>
                        {/* The refresh-token clock only matters when the
                            credential can actually be refreshed; for a
                            long-lived credential it would paint a false
                            danger badge. */}
                        {principalEntry.can_refresh &&
                        principalEntry.refresh_token_expires_at_unix_secs !=
                          null ? (
                          <div
                            data-testid="oauth-refresh-token-expiry"
                            className="mt-0.5 text-caption text-text-faint"
                          >
                            Expires{' '}
                            <span
                              className={
                                {
                                  ok: 'text-text-muted',
                                  warn: 'text-warn-text',
                                  danger: 'text-danger-text',
                                }[
                                  refreshTokenExpiryTone(
                                    principalEntry.refresh_token_expires_at_unix_secs,
                                  )
                                ]
                              }
                            >
                              <RelativeTime
                                ts={
                                  new Date(
                                    principalEntry.refresh_token_expires_at_unix_secs *
                                      1000,
                                  )
                                }
                              />
                            </span>
                          </div>
                        ) : null}
                      </div>
                    </div>
                    {principalEntry.scopes.length ? (
                      <div>
                        <div className="text-label text-text-faint">Scopes</div>
                        <div className="mt-1 break-all font-mono text-data text-text-muted">
                          {principalEntry.scopes.join(', ')}
                        </div>
                      </div>
                    ) : null}
                  </div>
                ) : (
                  <p className="text-body-sm text-text-muted">
                    Token is bound on the upstream but no credential details are
                    available right now.
                  </p>
                )}
              </div>
            </Section>
          </div>
        ) : (
          <SettingsCard upstream={upstream} />
        )}

        <div data-testid="recent-requests-card">
          <Section
            title="Recent requests"
            subtitle={
              recentPending ? (
                <Skeleton className="h-3 w-52" />
              ) : recentForUpstream.length === 0 ? (
                `No recent requests against ${upstream.name}`
              ) : (
                `Last ${recentForUpstream.length} against ${upstream.name}`
              )
            }
          >
            <div
              data-testid="recent-requests-table-slot"
              className="min-h-48 overflow-x-auto"
            >
              <RequestEventsTable
                events={recentForUpstream}
                principalNameMap={principalNameMap}
                upstreamNameMap={upstreamNameMap}
                loading={recentPending}
                columns={{
                  upstream: false,
                  cost: true,
                  tokens: true,
                }}
                minWidthClass="min-w-[920px]"
                emptyTitle="No recent requests for this upstream"
              />
            </div>
          </Section>
        </div>

        {selectedLatest && (a5h?.deficit || caveats.length > 0) && (
          <Section title="Quota analysis">
            <div className="flex flex-col gap-6">
              {a5h?.deficit && (
                <div className="flex flex-col gap-2">
                  <h3 className="text-title-card text-text">Quota deficit</h3>
                  <dl className="grid max-w-md grid-cols-[auto_1fr] items-baseline gap-x-6 gap-y-2 text-body">
                    <dt className="text-label text-text-muted">Shortfall</dt>
                    <dd className="text-right tabular-nums text-warn-text">
                      {Math.round(
                        a5h.deficit.shortfall_tokens,
                      ).toLocaleString()}{' '}
                      <span className="text-text-muted">tokens</span>
                    </dd>
                    <dt className="text-label text-text-muted">
                      Recommended multiplier
                    </dt>
                    <dd className="text-right tabular-nums text-warn-text">
                      {a5h.deficit.recommended_multiplier}×
                    </dd>
                    <dt className="text-label text-text-muted">Confidence</dt>
                    <dd className="text-right text-text">
                      {a5h.deficit.confidence}
                    </dd>
                  </dl>
                </div>
              )}
              {caveats.length > 0 && (
                <Notice tone="warning" title="Analysis caveats">
                  <ul className="list-disc list-inside ml-1">
                    {caveats.map((c) => {
                      // The backend names its config key; operators read
                      // the actual freshness limit instead.
                      const limit = quotaAnalysis.data?.max_staleness_secs;
                      const text = c
                        .toLowerCase()
                        .startsWith(
                          'latest observation is older than max_staleness_secs',
                        )
                        ? `Latest reading is older than ${
                            limit == null
                              ? 'the freshness limit'
                              : limit >= 60
                                ? `${Math.round(limit / 60)} min`
                                : `${limit} s`
                          }; analysis may be outdated`
                        : c.charAt(0).toUpperCase() + c.slice(1);
                      return <li key={c}>{text}</li>;
                    })}
                  </ul>
                </Notice>
              )}
            </div>
          </Section>
        )}
      </section>

      <ConfirmDialog
        open={confirmDeleteOpen}
        onOpenChange={setConfirmDeleteOpen}
        title="Delete upstream?"
        description={
          <>
            <span className="font-medium text-text">{upstream.name}</span> will
            be permanently removed. This cannot be undone.
          </>
        }
        confirmLabel={del.isPending ? 'Deleting...' : 'Delete'}
        destructive
        pending={del.isPending}
        closeOnConfirm={false}
        onConfirm={() =>
          del.mutate(
            { id: upstream.id, spec_revision: upstream.spec_revision },
            {
              onSuccess: () => {
                setConfirmDeleteOpen(false);
                toast.success('Upstream deleted');
                onBack();
              },
            },
          )
        }
      />

      <ConfirmDialog
        open={enabledConfirm.open}
        onOpenChange={(open) =>
          setEnabledConfirm((prev) => ({ ...prev, open }))
        }
        title={confirmEnabled ? 'Enable upstream?' : 'Disable upstream?'}
        description={
          confirmEnabled ? (
            <>
              <span className="font-medium text-text">{upstream.name}</span>{' '}
              will start receiving requests again.
              {upstream.warmup_enabled
                ? null
                : ' Warm-up is turned back on too.'}
            </>
          ) : (
            <>
              <span className="font-medium text-text">{upstream.name}</span>{' '}
              will stop receiving requests.
              {upstream.warmup_enabled ? ' Warm-up is turned off too.' : null}
              {poolImpact ? (
                <span
                  className="block mt-2 text-text-muted"
                  data-testid="upstream-disable-pool-impact"
                >
                  {poolImpact.remaining === 0
                    ? `No other upstream is enabled — requests will fail until one is.`
                    : `${poolImpact.remaining} of ${poolImpact.total} upstreams will remain enabled.`}
                </span>
              ) : null}
            </>
          )
        }
        confirmLabel={confirmEnabled ? 'Enable' : 'Disable'}
        destructive={!confirmEnabled}
        onConfirm={() => {
          const nextEnabled = confirmEnabled;
          // Warm-up follows the upstream: disabling pauses it, enabling
          // turns it back on.
          const body: UpdateUpstreamWarmupSettingsRequest = {
            enabled: nextEnabled,
          };
          if (upstream.warmup_enabled !== nextEnabled) {
            body.warmup_enabled = nextEnabled;
          }
          toggle.mutate(
            { id: upstream.id, body, spec_revision: upstream.spec_revision },
            {
              onSuccess: () =>
                toast.success(
                  nextEnabled ? 'Upstream enabled' : 'Upstream disabled',
                ),
            },
          );
        }}
      />
    </>
  );
}
