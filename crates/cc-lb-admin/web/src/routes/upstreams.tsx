import { Meter as BaseMeter } from '@base-ui/react/meter';
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
  Button,
  Card,
  CardBody,
  CardHeader,
  ConfirmDialog,
  cx,
  EmptyState,
  Hint,
  Notice,
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
import { SidebarCouponNudge } from '../components/upstreams/CouponNudge';
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
  selectSidebarQuotaWindows,
  selectVisibleGraphWindows,
} from '../components/upstreams/quotaWindowVisibility';
import { SettingsCard } from '../components/upstreams/SettingsCard';
import {
  QuotaFreshnessCaption,
  upstreamHealth,
} from '../components/upstreams/upstreamHealth';
import { WarmupCardMinimal } from '../components/upstreams/warmup/WarmupCardMinimal';
import {
  ApiError,
  type OrganizationMetadataInner,
  type QuotaSnapshot,
  type SubscriptionQuotaWindow,
  type UpstreamOAuthStatusResponse,
  WINDOW_LABELS,
} from '../lib/api';
import { getWindowColor, SERIES_FILL_OPACITY } from '../lib/colors';
import { DEFAULT_ANTHROPIC_BASE_URL } from '../lib/constants';
import { fmtChartTooltipTs, sumTokens } from '../lib/format';
import { isMessagesRequestEvent } from '../lib/logRows';
import {
  classifyOAuthReconnect,
  LONG_LIVED_EXPIRING_SOON_SECS,
  REFRESH_EXPIRING_SOON_SECS,
  useOAuthReconnectNudges,
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
  formatQuotaPercent,
  QUOTA_SEVERITY_FILL_CLASS,
  QUOTA_SEVERITY_TEXT_CLASS,
  quotaSeverity,
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
  const upstreams = useUpstreams();

  const upstreamIds = useMemo(() => {
    return upstreams.data?.upstreams.map((u) => u.id).join(',') ?? '';
  }, [upstreams.data]);

  // Single source of truth for /latest polling. The DetailView calls the same
  // hook with identical params so TanStack dedups them into one request; the
  // parent sidebar reads the 3 windows it needs, the detail reads the full 5.
  const quotaLatest = useSubscriptionQuotaLatest({
    upstreamIds,
    windows: '5h,7d,overage,7d_sonnet,7d_opus,7d_fable',
    source: 'merged',
    refetchInterval: 5_000,
  });

  const listUsage = useUsage('7d', 'hour', 'upstream', undefined, 'totals');
  const usageByUpstreamId = useMemo(() => {
    const m = new Map<string, { cost_usd: number; tokens: number }>();
    for (const series of listUsage.data?.series ?? []) {
      let cost = 0;
      let tokens = 0;
      for (const b of series.buckets) {
        cost += (b.virtual_cost_micros ?? 0) / 1_000_000;
        tokens += sumTokens(b);
      }
      m.set(series.key, { cost_usd: cost, tokens });
    }
    return m;
  }, [listUsage.data]);

  // /admin/v1/status reports per-upstream runtime state incl. OAuth binding.
  const status = useStatus();
  const quotaLatestPending =
    quotaLatest.data === undefined && quotaLatest.isPending;
  const listUsagePending = listUsage.data === undefined && listUsage.isPending;
  const statusPending = status.data === undefined && status.isPending;
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
  // One dialog serves every entry point: "New" (header, empty state, and the
  // ?action=new deep link used by the command palette) and Connect/Reconnect
  // on an OAuth upstream (card button, reconnect notice, ?action=reconnect).
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
    () => upstreams.data?.upstreams ?? [],
    [upstreams.data],
  );
  // The kind badge only helps tell rows apart when the list mixes kinds.
  const showKindBadge = new Set(visibleUpstreams.map((u) => u.kind)).size > 1;

  // Reconnect nudges derive from the same oauth/status queries the detail
  // view reads; disabled upstreams stay in the map so their rows still carry
  // the badge even though the global summary ignores them.
  const { nudges: reconnectNudges } = useOAuthReconnectNudges(visibleUpstreams);

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

  const listNowUnixSecs = Math.floor(Date.now() / 1000);

  return (
    <div className="h-[calc(100dvh-3rem)] min-h-0 flex w-full max-w-[120rem] mx-auto">
      {/* List pane */}
      <aside
        className={cx(
          'border-r border-subtle flex flex-col min-h-0 w-full md:w-[360px] shrink-0',
          selected ? 'hidden md:flex' : 'flex',
        )}
      >
        <div className="px-4 py-3 border-b border-subtle shrink-0 [&>header]:mb-0">
          <PageHeader
            title="Upstreams"
            description={
              <span className="flex h-4 items-center text-caption text-text-faint">
                {upstreams.isLoading ? (
                  <Skeleton className="h-3 w-14" />
                ) : (
                  `${visibleUpstreams.length} total`
                )}
              </span>
            }
            actions={
              <Button size="sm" iconLeft={<Plus />} onClick={openCreate}>
                New
              </Button>
            }
          />
        </div>
        <div className="flex-1 overflow-y-auto p-2 pb-8 space-y-1">
          {upstreams.isLoading ? (
            Array.from({ length: 3 }).map((_, i) => (
              <SidebarUpstreamRowSkeleton key={i} />
            ))
          ) : visibleUpstreams.length ? (
            visibleUpstreams.map((u) => {
              const latest = quotaLatest.data?.upstreams.find(
                (l) => l.upstream_id === u.id,
              );
              const reconnectNudge = reconnectNudges.get(u.id);
              const runtimeStatus = statusByUpstreamId.get(u.id);
              const health = upstreamHealth(
                u.enabled,
                runtimeStatus?.status,
                reconnectNudge,
              );
              const dotTone = health.tone;
              const dotLabel = health.label;
              const barWindows = quotaLatestPending
                ? ['5h', '7d']
                : selectSidebarQuotaWindows({
                    latestWindows: latest?.windows,
                    nowUnixSecs: listNowUnixSecs,
                  });
              return (
                <button
                  key={u.id}
                  type="button"
                  onClick={() => select(u.id)}
                  className={cx(
                    '@container w-full text-left px-3 py-2.5 rounded-sm transition-colors text-text',
                    u.id === selectedId ? 'bg-overlay-5' : 'hover:bg-overlay-2',
                  )}
                  aria-current={u.id === selectedId ? 'true' : undefined}
                  title={runtimeStatus?.last_apply_error ?? undefined}
                >
                  <div className="mb-2 flex flex-col gap-1.5">
                    <div className="flex min-w-0 items-start gap-2">
                      {statusPending ? (
                        <Skeleton className="mt-2 h-1.5 w-1.5 shrink-0 rounded-full" />
                      ) : (
                        <>
                          <span
                            aria-hidden="true"
                            className={cx('status-dot mt-2 shrink-0', dotTone)}
                          />
                          <span className="sr-only">{dotLabel}:</span>
                        </>
                      )}
                      <span className="min-w-0 break-words text-body font-medium">
                        {u.name}
                      </span>
                    </div>
                    {reconnectNudge || showKindBadge ? (
                      <div className="flex flex-wrap items-center gap-x-3 gap-y-1 pl-3.5">
                        {reconnectNudge ? (
                          <span
                            className={cx(
                              'text-caption',
                              reconnectNudge.tone === 'danger'
                                ? 'text-danger-text'
                                : 'text-warn-text',
                            )}
                          >
                            {reconnectNudge.label}
                          </span>
                        ) : null}
                        {showKindBadge ? (
                          <span className="text-caption text-text-faint">
                            {u.kind === 'anthropic_oauth'
                              ? 'OAuth'
                              : u.kind === 'anthropic_api_key'
                                ? 'API key'
                                : u.kind}
                          </span>
                        ) : null}
                      </div>
                    ) : null}
                  </div>
                  {u.kind === 'anthropic_oauth' ? (
                    <div className="flex w-full flex-col gap-1.5 pl-3.5">
                      {barWindows.map((windowName) => {
                        const snap = latest?.windows.find(
                          (w) => w.window === windowName,
                        );
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
                        const pct = formatQuotaPercent(
                          utilization == null ? null : utilization * 100,
                        );
                        const severity = quotaSeverity(
                          utilization == null ? null : utilization * 100,
                        );
                        return (
                          <div
                            key={windowName}
                            className="flex items-center gap-2 w-full text-caption"
                          >
                            <div className="w-16 shrink-0 truncate text-text-faint">
                              {windowName === 'overage' ? 'Extra' : label}
                            </div>
                            {quotaLatestPending ? (
                              <>
                                <Skeleton className="h-1.5 flex-1 rounded-xs" />
                                <Skeleton className="h-3 w-8 shrink-0" />
                              </>
                            ) : (
                              <>
                                <BaseMeter.Root
                                  value={
                                    utilization == null
                                      ? 0
                                      : Math.min(
                                          100,
                                          Math.max(0, utilization * 100),
                                        )
                                  }
                                  max={100}
                                  className="flex-1 h-1.5 bg-progress-track rounded-xs overflow-hidden"
                                >
                                  <BaseMeter.Track className="h-full">
                                    <BaseMeter.Indicator
                                      className={cx(
                                        'h-full',
                                        QUOTA_SEVERITY_FILL_CLASS[severity],
                                      )}
                                    />
                                  </BaseMeter.Track>
                                </BaseMeter.Root>
                                <div
                                  className={cx(
                                    'w-9 shrink-0 text-right tabular-nums',
                                    QUOTA_SEVERITY_TEXT_CLASS[severity],
                                  )}
                                >
                                  {pct}
                                </div>
                              </>
                            )}
                          </div>
                        );
                      })}
                      {quotaLatestPending ? null : (
                        <QuotaFreshnessCaption
                          snapshots={barWindows.flatMap((windowName) => {
                            const snap = latest?.windows.find(
                              (w) => w.window === windowName,
                            );
                            return snap ? [snap] : [];
                          })}
                        />
                      )}
                      <SidebarCouponNudge
                        upstream={u}
                        windows={
                          quotaLatest.isError ? [] : (latest?.windows ?? [])
                        }
                      />
                    </div>
                  ) : (
                    <div className="flex min-h-4 items-center gap-2 pl-3.5 text-caption text-text-faint">
                      {listUsagePending ? (
                        <Skeleton className="h-3 w-32" />
                      ) : (
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
                      )}
                    </div>
                  )}
                </button>
              );
            })
          ) : (
            <UpstreamsListEmpty onCreate={openCreate} />
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
          <DetailView
            key={selected.id}
            upstream={selected}
            onBack={() => select(undefined)}
            onConnect={() =>
              setConnectTarget({ mode: 'reconnect', upstream: selected })
            }
          />
        ) : upstreams.isLoading ? (
          <UpstreamDetailLoadingShell />
        ) : (
          <div className="flex-1 flex items-center justify-center">
            {visibleUpstreams.length ? (
              <EmptyState
                headingLevel={2}
                title="Select an upstream"
                description="Pick an upstream from the list to see its configuration, OAuth state, and recent requests."
              />
            ) : (
              <EmptyState
                headingLevel={2}
                title="Upstream details appear here"
                description="Configuration, OAuth state, quota, and recent requests show here once you add an upstream."
              />
            )}
          </div>
        )}
      </section>

      <UpstreamConnectDialog
        target={connectTarget}
        onClose={() => setConnectTarget(null)}
        onCreated={(created) => select(created.id)}
      />
    </div>
  );
}

const QUOTA_SNAPSHOT_GRID_CLASS =
  'grid min-h-[203px] gap-3 [grid-template-columns:repeat(auto-fit,minmax(240px,1fr))]';
const QUOTA_HISTORY_RANGES = TIME_PRESETS;
const QUOTA_HISTORY_RANGE_OPTIONS = TIME_PRESET_OPTIONS;
// Loading placeholder mirrors SegmentedControl (size md) so nothing shifts.
const QUOTA_HISTORY_RANGE_GROUP_CLASS =
  'inline-flex items-center gap-0.5 rounded-sm border border-subtle bg-overlay-2 p-0.5';
const QUOTA_HISTORY_RANGE_ITEM_CLASS =
  'h-9 md:h-[1.625rem] px-2.5 text-xs rounded-sm';

const METADATA_GRID_CLASS = 'flex flex-wrap gap-x-6 gap-y-1.5';

function MetadataStripSkeleton() {
  return (
    <div
      data-testid="upstream-metadata-loading"
      className={METADATA_GRID_CLASS}
    >
      {['w-28', 'w-24', 'w-32', 'w-40', 'w-36'].map((width) => (
        <span key={width} className="flex h-5 items-center">
          <Skeleton className={cx('h-3', width)} />
        </span>
      ))}
    </div>
  );
}

function SidebarUpstreamRowSkeleton() {
  return (
    <div
      data-testid="upstream-list-loading-row"
      className="@container w-full rounded-sm px-3 py-2.5"
    >
      <div className="mb-2 flex items-center gap-2">
        <Skeleton className="h-1.5 w-1.5 shrink-0 rounded-full" />
        <Skeleton className="h-4 w-32" />
      </div>
      <div className="flex w-full flex-col gap-1.5">
        {['5h', '7d'].map((windowName) => (
          <div
            key={windowName}
            className="flex w-full items-center gap-2 text-caption"
          >
            <span className="w-10 shrink-0 text-text-faint">{windowName}</span>
            <Skeleton className="h-1.5 flex-1 rounded-xs" />
            <Skeleton className="h-3 w-8 shrink-0" />
          </div>
        ))}
      </div>
      <div className="mt-1.5 min-h-4">
        <Skeleton className="h-3 w-32" />
      </div>
    </div>
  );
}

function QuotaSnapshotCardSkeleton() {
  return (
    <Card data-testid="quota-snapshot-skeleton-card">
      <CardBody className="flex min-h-[168px] flex-col gap-3">
        <div className="flex items-center justify-between">
          <Skeleton className="h-4 w-20" />
          <Skeleton className="h-3 w-24" />
        </div>
        <Skeleton className="h-8 w-20" />
        <Skeleton className="h-2 w-full rounded-xs" />
        <Skeleton className="mt-auto h-3 w-32" />
      </CardBody>
    </Card>
  );
}

function UpstreamDetailLoadingShell() {
  return (
    <div
      data-testid="upstream-detail-loading-shell"
      className="contents"
      aria-busy="true"
      aria-label="Loading upstream details"
    >
      <div className="sticky top-0 z-30 border-b border-subtle bg-bg-sub">
        <div className="flex shrink-0 flex-wrap items-start justify-between gap-3 px-4 py-3 md:px-6">
          <div className="flex items-center gap-3">
            <Skeleton className="h-7 w-48" />
            <Skeleton className="h-5 w-20" />
          </div>
          <Skeleton className="h-7 w-20" />
        </div>
        <div
          data-testid="upstream-detail-loading-metadata"
          className="min-h-9 border-t border-subtle px-4 py-2 md:px-6"
        >
          <MetadataStripSkeleton />
        </div>
      </div>

      <div className="flex-1 space-y-8 overflow-y-auto p-4 pb-8 md:p-6 md:pb-12">
        <Section>
          <Card>
            <CardHeader
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
            />
            <CardBody className="pt-3">
              <div
                data-testid="quota-history-legend-slot"
                className="mb-2 flex min-h-5 items-center justify-end gap-4"
              >
                <Skeleton className="h-3 w-20" />
                <Skeleton className="h-3 w-20" />
              </div>
              <div className="h-full min-h-[300px] w-full">
                <Skeleton className="h-[300px] w-full" />
              </div>
            </CardBody>
          </Card>
          <div
            data-testid="quota-snapshot-grid"
            className={QUOTA_SNAPSHOT_GRID_CLASS}
          >
            {Array.from({ length: 3 }).map((_, index) => (
              <QuotaSnapshotCardSkeleton key={index} />
            ))}
          </div>
        </Section>
      </div>
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
  const [showMoreMeta, setShowMoreMeta] = useState(false);
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

  const chartRows = useMemo(
    () => chartData.rows.filter((row) => row.unix >= seriesSinceUnixSecs),
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
        value: (
          <span className="font-mono text-data">
            {upstream.id.slice(0, 8)}…
          </span>
        ),
        tooltip: `Internal upstream identifier (${upstream.id})`,
      },
    ];

    if (isOauth) {
      primaryLabels = new Set([
        'ID',
        'Plan',
        'Rate',
        'Extra usage billing',
        'Account',
      ]);
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
            <span className="font-mono text-data">
              {orgMeta.rate_limit_tier}
            </span>
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
      primaryLabels = new Set(['ID', 'Base URL']);
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

    const visibleFields = isOauth
      ? fields.filter((f) => showMoreMeta || primaryLabels.has(f.label))
      : fields;
    const hiddenCount = isOauth
      ? fields.filter((f) => !primaryLabels.has(f.label)).length
      : 0;

    return (
      <div className="sticky top-0 z-30 bg-bg-sub border-b border-subtle">
        <div className="px-4 md:px-6 py-3 flex items-start justify-between gap-3 flex-wrap shrink-0">
          <div className="min-w-0">
            <button
              type="button"
              onClick={onBack}
              className="md:hidden -ml-1 mb-1 inline-flex min-h-9 items-center gap-1 rounded-sm px-1 text-body-sm text-text-muted hover:text-text"
            >
              <ChevronLeft className="size-3.5" strokeWidth={1.75} /> Back
            </button>
            <div className="flex items-center gap-3 flex-wrap">
              <InlineNameEditor upstream={upstream} />
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
                  below owns the diagnosis, the header stays quiet. */}
              {headerHealth.tone === 'danger' && !reconnectNudge ? (
                <StatusBadge tone="danger" label={headerHealth.label} />
              ) : null}
              {togglePendingLabel ? (
                <span
                  role="status"
                  aria-live="polite"
                  data-testid="upstream-enabled-pending"
                  className="inline-flex items-center gap-1.5 text-caption text-text-muted"
                >
                  <Spinner className="w-3 h-3 text-text-muted" />
                  {togglePendingLabel}
                </span>
              ) : null}
            </div>
          </div>
          <div className="flex items-center gap-2 shrink-0">
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
        {fields.length > 0 && (
          <div
            data-testid="upstream-metadata-strip"
            className="flex min-h-9 items-start gap-3 border-t border-subtle px-4 py-2 md:px-6"
          >
            <div className="flex-1 min-w-0">
              {metadataPending ? (
                <MetadataStripSkeleton />
              ) : (
                <dl className={METADATA_GRID_CLASS}>
                  {visibleFields.map((f) => (
                    <div
                      key={f.label}
                      className="flex min-h-5 min-w-0 items-baseline gap-2"
                    >
                      <dt className="shrink-0 text-label text-text-faint">
                        <Hint label={f.tooltip}>
                          <span
                            tabIndex={0}
                            className="inline-flex cursor-help items-center gap-1 rounded-sm hover:text-text-muted"
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
                      <dd className="min-w-0 break-words text-body-sm text-text">
                        {f.value}
                      </dd>
                    </div>
                  ))}
                </dl>
              )}
            </div>
            <div className="flex h-5 items-center gap-1 shrink-0">
              {!metadataPending && hiddenCount > 0 && (
                <button
                  type="button"
                  aria-expanded={showMoreMeta}
                  onClick={() => setShowMoreMeta((v) => !v)}
                  className="rounded-sm px-1 text-caption text-text-muted hover:text-text"
                >
                  {showMoreMeta ? 'Show less' : `Show ${hiddenCount} more`}
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
                    className="inline-flex size-6 items-center justify-center rounded-sm text-text-faint hover:bg-overlay-3 hover:text-text disabled:opacity-40"
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
            </div>
          </div>
        )}
      </div>
    );
  };

  return (
    <>
      {renderHeader()}

      <div className="flex-1 overflow-y-auto p-4 md:p-6 pb-8 md:pb-12 space-y-8">
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

        {isOauth && (
          <Section>
            <Card>
              <CardHeader
                title="Quota history"
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
              />
              <CardBody className="pt-3">
                <div
                  data-testid="quota-history-legend-slot"
                  className="mb-2 flex min-h-5 flex-wrap items-center justify-end gap-x-4 gap-y-1"
                >
                  {quotaHistoryPending ? (
                    <>
                      <Skeleton className="h-3 w-20" />
                      <Skeleton className="h-3 w-20" />
                    </>
                  ) : chartData.rows.length > 0 &&
                    visibleGraphWindows.length > 0 ? (
                    visibleGraphWindows.map((windowName) => {
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
                            'flex cursor-pointer items-center gap-1.5 rounded-sm text-caption transition-opacity',
                            dimmed ? 'opacity-40 hover:opacity-70' : '',
                          )}
                        >
                          <span
                            aria-hidden="true"
                            className="h-0.5 w-2.5 rounded-xs"
                            style={{
                              backgroundColor:
                                getWindowColor(windowName).stroke,
                            }}
                          />
                          <span className="text-text-muted">
                            {windowLabel(windowName)}
                          </span>
                          {current != null ? (
                            <span className="tabular-nums text-text">
                              {formatQuotaPercent(current * 100)}
                            </span>
                          ) : null}
                        </button>
                      );
                    })
                  ) : null}
                </div>
                <div className="w-full h-[300px]" style={{ minWidth: 0 }}>
                  {quotaHistoryPending ? (
                    <Skeleton className="h-full w-full" />
                  ) : !chartRows.length || !visibleGraphWindows.length ? (
                    <EmptyState title="No data in range" />
                  ) : (
                    <ResponsiveContainer width="100%" height={300}>
                      <AreaChart
                        data={chartRows}
                        margin={{ top: 20, right: 28, bottom: 0, left: 0 }}
                      >
                        <CartesianGrid
                          vertical={false}
                          stroke="var(--color-border-row)"
                        />
                        <XAxis
                          dataKey="unix"
                          type="number"
                          domain={xDomain}
                          // Four ticks inset from the edges so the first and
                          // last labels never clip.
                          ticks={[1, 3, 5, 7].map(
                            (i) =>
                              seriesSinceUnixSecs +
                              ((seriesUntilUnixSecs - seriesSinceUnixSecs) *
                                i) /
                                8,
                          )}
                          interval={0}
                          tickFormatter={(val) => {
                            const d = new Date(Number(val) * 1000);
                            return range === '7d' || range === '24h'
                              ? `${d.getMonth() + 1}/${d.getDate()} ${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`
                              : `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`;
                          }}
                          axisLine={false}
                          tickLine={false}
                        />
                        <YAxis
                          tickFormatter={(val) => `${val}%`}
                          ticks={[0, 50, 100]}
                          axisLine={false}
                          tickLine={false}
                          width={40}
                          domain={[0, 100]}
                          allowDataOverflow={false}
                        />
                        {[
                          { y: 80, tone: 'warn' },
                          { y: 95, tone: 'danger' },
                        ].map(({ y, tone }) => (
                          <ReferenceLine
                            key={y}
                            y={y}
                            stroke={`var(--color-${tone})`}
                            strokeOpacity={0.5}
                            strokeDasharray="3 3"
                            ifOverflow="extendDomain"
                          >
                            <Label
                              value={`${y}%`}
                              position="right"
                              fontSize={11}
                              fill={`var(--color-${tone}-text)`}
                            />
                          </ReferenceLine>
                        ))}
                        <Tooltip
                          cursor={{
                            stroke: 'var(--color-border-strong)',
                            strokeWidth: 1,
                          }}
                          content={({ active, payload, label }) => {
                            if (!active || !payload?.length) return null;
                            const first = selectedLatest?.windows[0];
                            return (
                              <div className="glass-strong min-w-36 rounded-md px-2.5 py-2 text-caption text-text">
                                <div className="mb-1 text-text-faint">
                                  {fmtChartTooltipTs(Number(label))}
                                </div>
                                {payload.map((p, i) => {
                                  const key = String(p.dataKey);
                                  return (
                                    <div
                                      key={i}
                                      className="flex items-center justify-between gap-3 py-px"
                                    >
                                      <span className="flex items-center gap-1.5 text-text-muted">
                                        <span
                                          aria-hidden="true"
                                          className="h-0.5 w-2.5 rounded-xs"
                                          style={{
                                            backgroundColor:
                                              getWindowColor(key).stroke,
                                          }}
                                        />
                                        {windowLabel(key)}
                                      </span>
                                      <span className="tabular-nums">
                                        {typeof p.value === 'number'
                                          ? formatQuotaPercent(p.value)
                                          : '—'}
                                      </span>
                                    </div>
                                  );
                                })}
                                {first?.source && (
                                  <div className="mt-1.5 border-t border-subtle pt-1.5 text-text-faint">
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
                          const span =
                            seriesUntilUnixSecs - seriesSinceUnixSecs;
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
                            const names = group.windows
                              .map(windowLabel)
                              .join(', ');
                            return (
                              <ReferenceLine
                                key={`marker-${group.kind}-${group.ts}`}
                                x={group.ts}
                                stroke={getWindowColor(firstWindow).stroke}
                                strokeOpacity={0.5}
                                strokeDasharray={
                                  group.kind === 'start' ? '4 6' : '2 4'
                                }
                              >
                                <Label
                                  value={
                                    group.kind === 'start'
                                      ? `${names} start`
                                      : `${names} reset`
                                  }
                                  position={raised ? 'top' : 'insideTop'}
                                  fontSize={11}
                                  fill="var(--color-text-muted)"
                                />
                              </ReferenceLine>
                            );
                          });
                        })()}
                        {visibleGraphWindows.map((windowName) => {
                          // Overlapping fills turn murky: only the isolated
                          // (or first) series gets the flat fill; the rest
                          // are lines.
                          const filled =
                            windowName ===
                            (effectiveIsolatedWindow ?? visibleGraphWindows[0]);
                          return (
                            <Area
                              key={windowName}
                              type="monotone"
                              dataKey={windowName}
                              stroke={getWindowColor(windowName).stroke}
                              strokeWidth={1.5}
                              fill={getWindowColor(windowName).fill}
                              fillOpacity={filled ? SERIES_FILL_OPACITY : 0}
                              isAnimationActive={false}
                              connectNulls={false}
                              hide={
                                effectiveIsolatedWindow !== null &&
                                effectiveIsolatedWindow !== windowName
                              }
                            />
                          );
                        })}
                      </AreaChart>
                    </ResponsiveContainer>
                  )}
                </div>
              </CardBody>
            </Card>

            {/* On phones the current window numbers lead; the chart follows. */}
            <div className="order-first md:order-none">
              {(() => {
                const latest = selectedLatest;
                if (quotaLatestPending) {
                  return (
                    <div
                      data-testid="quota-snapshot-grid"
                      className={QUOTA_SNAPSHOT_GRID_CLASS}
                    >
                      {Array.from({ length: 3 }).map((_, index) => (
                        <QuotaSnapshotCardSkeleton key={index} />
                      ))}
                    </div>
                  );
                }
                if (!latest) {
                  return (
                    <div
                      data-testid="quota-snapshot-grid"
                      className={QUOTA_SNAPSHOT_GRID_CLASS}
                    >
                      <Card className="col-span-full flex items-center justify-center">
                        <EmptyState
                          title={`No subscription quota data for ${upstream.name}`}
                        />
                      </Card>
                    </div>
                  );
                }
                return (
                  <div className="space-y-4">
                    <div
                      data-testid="quota-snapshot-grid"
                      className={QUOTA_SNAPSHOT_GRID_CLASS}
                    >
                      {selectQuotaCardSnapshots({
                        latestWindows: latest.windows,
                        nowUnixSecs,
                      }).map((snap) => {
                        const isOverage =
                          snap.window === 'overage' &&
                          (snap.extra_usage_enabled ||
                            snap.extra_usage_monthly_limit != null);
                        const usedPct = isOverage
                          ? snap.extra_usage_monthly_limit != null &&
                            snap.extra_usage_used_credits != null
                            ? (snap.extra_usage_used_credits /
                                snap.extra_usage_monthly_limit) *
                              100
                            : null
                          : snap.utilization == null
                            ? null
                            : snap.utilization * 100;
                        const severity = quotaSeverity(usedPct);
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
                        const meterPct = isOverage
                          ? usedPct
                          : snap.utilization == null
                            ? null
                            : snap.utilization * 100;
                        return (
                          <Card key={snap.window}>
                            <CardBody className="flex min-h-[168px] flex-col gap-2">
                              <div className="flex flex-col gap-0.5">
                                <h3 className="text-title-card text-text">
                                  {isOverage
                                    ? windowLabel(snap.window)
                                    : `${windowLabel(snap.window)} window`}
                                </h3>
                                <SnapshotStatusComposite snap={snap} />
                              </div>
                              {isOverage ? (
                                snap.extra_usage_monthly_limit != null &&
                                snap.extra_usage_used_credits != null ? (
                                  <>
                                    <div
                                      className={cx(
                                        'text-display tabular-nums',
                                        QUOTA_SEVERITY_TEXT_CLASS[severity],
                                      )}
                                    >
                                      {formatQuotaPercent(
                                        (snap.extra_usage_used_credits /
                                          snap.extra_usage_monthly_limit) *
                                          100,
                                      )}
                                    </div>
                                    <div className="text-body-sm tabular-nums text-text-muted">
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
                                  <span className="text-body-sm text-text-muted">
                                    Enabled — no limit set
                                  </span>
                                )
                              ) : snap.utilization == null ? (
                                snap.status ? (
                                  <div className="text-body text-text-muted">
                                    {snap.status}
                                  </div>
                                ) : (
                                  <div className="text-display text-text-faint">
                                    —
                                  </div>
                                )
                              ) : (
                                <div
                                  className={cx(
                                    'text-display tabular-nums',
                                    QUOTA_SEVERITY_TEXT_CLASS[severity],
                                  )}
                                >
                                  {formatQuotaPercent(snap.utilization * 100)}
                                </div>
                              )}
                              {meterPct != null ? (
                                <BaseMeter.Root
                                  value={Math.min(100, Math.max(0, meterPct))}
                                  max={100}
                                  className="w-full h-2 bg-progress-track rounded-xs overflow-hidden"
                                >
                                  <BaseMeter.Track className="h-full">
                                    <BaseMeter.Indicator
                                      className={cx(
                                        'h-full',
                                        QUOTA_SEVERITY_FILL_CLASS[severity],
                                      )}
                                    />
                                  </BaseMeter.Track>
                                </BaseMeter.Root>
                              ) : null}
                              {windowNotStarted ? (
                                <div className="text-caption text-text-faint">
                                  Not started — begins on first request or
                                  warm-up
                                </div>
                              ) : snap.resets_at_unix_secs ? (
                                <div className="text-caption text-text-faint">
                                  <ResetCountdown
                                    compact
                                    ts={snap.resets_at_unix_secs * 1000}
                                  />
                                </div>
                              ) : null}
                              {!isOverage && !windowNotStarted && (
                                <div className="mt-auto flex flex-col gap-1 border-t border-subtle pt-2.5">
                                  <div className="flex items-baseline justify-between gap-2">
                                    <span className="text-label text-text-faint">
                                      ETA to limit
                                    </span>
                                    {quotaAnalysisPending ? (
                                      <Skeleton className="h-5 w-28" />
                                    ) : (
                                      <span className="text-body-sm tabular-nums text-text">
                                        {eta != null && eta <= 0 ? (
                                          'Already saturated'
                                        ) : (
                                          <RelativeOffsetTime
                                            compact
                                            offsetSeconds={eta}
                                          />
                                        )}
                                      </span>
                                    )}
                                  </div>
                                  <div className="flex min-h-4 items-baseline justify-between gap-2">
                                    <span className="text-label text-text-faint">
                                      Burn
                                    </span>
                                    {quotaAnalysisPending ? (
                                      <Skeleton className="h-3 w-32" />
                                    ) : (
                                      <span className="text-caption tabular-nums text-text-muted">
                                        {actualBurn == null
                                          ? '—'
                                          : `${(actualBurn * 60 * 100).toFixed(2)}%/min`}
                                        {' · projected '}
                                        {projBurn == null
                                          ? '—'
                                          : `${((projBurn / 60) * 100).toFixed(2)}%/min`}
                                      </span>
                                    )}
                                  </div>
                                  {!quotaAnalysisPending &&
                                    waitingForGrowth && (
                                      <div className="text-caption text-warn-text">
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
                  </div>
                );
              })()}
            </div>
          </Section>
        )}

        {!isOauth && (
          <ApiUsageCard
            data={apiUsageQ.data}
            isLoading={apiUsageQ.data === undefined && apiUsageQ.isPending}
            range={apiUsageRange}
            onRangeChange={setApiUsageRange}
            metric={apiUsageMetric}
            onMetricChange={setApiUsageMetric}
          />
        )}

        {isOauth ? (
          <div className="grid gap-4 2xl:grid-cols-[3fr_2fr]">
            <WarmupCardMinimal
              upstream={upstream}
              credentialNoticeShown={reconnectNudge != null}
            />
            <Card className="w-full h-full flex flex-col">
              <CardHeader
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
                  oauthStatusPending ? (
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
                  )
                }
                align="center"
              />
              <CardBody
                data-testid="oauth-status-card-body"
                className="min-h-28 flex-1 text-body-sm"
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
              </CardBody>
            </Card>
          </div>
        ) : (
          <WarmupCardMinimal upstream={upstream} />
        )}

        {!isOauth && <SettingsCard upstream={upstream} />}

        <Card data-testid="recent-requests-card">
          <CardHeader
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
          />
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
        </Card>
        {selectedLatest && (a5h?.deficit || caveats.length > 0) && (
          <Section title="Quota analysis">
            <div className="space-y-4">
              {a5h?.deficit && (
                <Card>
                  <CardHeader title="Quota deficit" headingLevel={3} />
                  <CardBody>
                    <dl className="grid grid-cols-[auto_1fr] items-baseline gap-x-6 gap-y-2 text-body-sm">
                      <dt className="text-label text-text-faint">Shortfall</dt>
                      <dd className="text-right tabular-nums text-warn-text">
                        {Math.round(
                          a5h.deficit.shortfall_tokens,
                        ).toLocaleString()}{' '}
                        <span className="text-text-muted">tokens</span>
                      </dd>
                      <dt className="text-label text-text-faint">
                        Recommended multiplier
                      </dt>
                      <dd className="text-right tabular-nums text-warn-text">
                        {a5h.deficit.recommended_multiplier}×
                      </dd>
                      <dt className="text-label text-text-faint">Confidence</dt>
                      <dd className="text-right text-text">
                        {a5h.deficit.confidence}
                      </dd>
                    </dl>
                  </CardBody>
                </Card>
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
      </div>

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
