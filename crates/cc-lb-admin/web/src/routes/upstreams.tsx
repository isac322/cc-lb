import { useQueryClient } from '@tanstack/react-query';
import {
  createFileRoute,
  stripSearchParams,
  useNavigate,
} from '@tanstack/react-router';
import { ExternalLink, KeyRound, Plus, RefreshCw } from 'lucide-react';
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
  SeriesFillGradient,
  useChartId,
} from '../components/ui/charts';
import {
  DetailFact,
  DetailFacts,
  DetailHeader,
  DetailHeaderSkeleton,
  DetailPane,
  DetailSection,
  DetailSectionGrid,
} from '../components/ui/DetailPane';
import {
  EntityList,
  type EntityListView,
  type EntityListViewConfig,
  useEntityListView,
} from '../components/ui/EntityList';
import {
  Badge,
  Button,
  ConfirmDialog,
  cx,
  EmptyState,
  IconButton,
  Notice,
  SegmentedControl,
  Skeleton,
  StatusBadge,
} from '../components/ui/primitives';
import { RelativeTime } from '../components/ui/RelativeTime';
import { RequestEventsFeed } from '../components/ui/RequestEventsFeed';
import { PaceLegend } from '../components/ui/UsageMeter';
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
  QuotaWindowRow,
  QuotaWindowRowSkeleton,
  QuotaWindowRows,
  windowLabel,
} from '../components/upstreams/QuotaWindowRows';
import {
  selectQuotaCardSnapshots,
  selectVisibleGraphWindows,
} from '../components/upstreams/quotaWindowVisibility';
import { SettingsCard } from '../components/upstreams/SettingsCard';
import { UpstreamPlanCaption } from '../components/upstreams/UpstreamPlanCaption';
import {
  formatQuotaStamp,
  subscriptionPlanLabel,
  UPSTREAM_LATEST_WINDOWS,
  type UpstreamUsageRow,
  useUpstreamUsageData,
} from '../components/upstreams/UpstreamUsageTable';
import {
  QuotaFreshnessCaption,
  upstreamHealth,
} from '../components/upstreams/upstreamHealth';
import { WarmupCardMinimal } from '../components/upstreams/warmup/WarmupCardMinimal';
import {
  ApiError,
  type OrganizationMetadataInner,
  type SubscriptionMetadataResponse,
  type UpstreamOAuthStatusResponse,
} from '../lib/api';
import { getWindowColor } from '../lib/colors';
import { fmtChartTooltipTs, formatCount } from '../lib/format';
import { useTimezone } from '../lib/locale';
import {
  classifyOAuthReconnect,
  isTerminalOAuthReconnectReason,
  type OAuthReconnectNudge,
  REFRESH_EXPIRING_SOON_SECS,
} from '../lib/oauthReconnect';
import {
  qk,
  type UpdateUpstreamWarmupSettingsRequest,
  type Upstream,
  useDeleteUpstream,
  usePrincipalNameMap,
  useStatus,
  useSubscriptionQuotaLatest,
  useSubscriptionQuotaSeries,
  useTriggerSubscriptionMetadataRefresh,
  useUpdateUpstreamWarmupSettings,
  useUpstreamOAuthStatus,
  useUpstreamSubscriptionMetadata,
  useUpstreams,
  useUsage,
} from '../lib/queries';
import {
  formatQuotaPercent,
  QUOTA_SEVERITY_TEXT_CLASS,
  type QuotaSeverity,
  quotaSeverity,
  snapshotQuotaPacePct,
  snapshotQuotaSeverity,
  worstQuotaSeverity,
} from '../lib/quotaSeverity';
import {
  TIME_PRESET_OPTIONS,
  TIME_PRESETS,
  useSharedTimeRange,
} from '../lib/timePresets';
import { useRequestEventsFeed } from '../lib/useRequestEventsFeed';

const UPSTREAM_FILTERS = [
  'all',
  'attention',
  'disabled',
  'oauth',
  'api_key',
] as const;
type UpstreamFilter = (typeof UPSTREAM_FILTERS)[number];
const UPSTREAM_SORTS = ['attention', 'usage', 'name'] as const;
type UpstreamSort = (typeof UPSTREAM_SORTS)[number];

const UPSTREAM_VIEW_DEFAULTS = {
  q: '',
  filter: 'all',
  sort: 'attention',
} as const satisfies EntityListView<UpstreamFilter, UpstreamSort>;

const upstreamSearchSchema = z.object({
  selectedId: z.string().optional(),
  action: z.enum(['new', 'reconnect']).optional(),
  q: z.string().default('').catch(''),
  filter: z.enum(UPSTREAM_FILTERS).default('all').catch('all'),
  sort: z.enum(UPSTREAM_SORTS).default('attention').catch('attention'),
});

export const Route = createFileRoute('/upstreams')({
  validateSearch: upstreamSearchSchema,
  // Defaults stay out of the address bar.
  search: { middlewares: [stripSearchParams(UPSTREAM_VIEW_DEFAULTS)] },
  component: UpstreamsPage,
});

const UPSTREAM_FILTER_OPTIONS: readonly {
  value: UpstreamFilter;
  label: string;
}[] = [
  { value: 'all', label: 'All upstreams' },
  { value: 'attention', label: 'Needs attention' },
  { value: 'disabled', label: 'Disabled' },
  { value: 'oauth', label: 'Subscription (OAuth)' },
  { value: 'api_key', label: 'API key' },
];
const UPSTREAM_SORT_OPTIONS: readonly {
  value: UpstreamSort;
  label: string;
}[] = [
  { value: 'attention', label: 'Needs attention first' },
  { value: 'usage', label: 'Highest usage' },
  { value: 'name', label: 'Name' },
];

/** The row's window facts, in reading order, with their compact names. */
const ROW_WINDOWS = [
  ['5h', '5h'],
  ['7d', '7d'],
  ['7d_fable', 'Fable'],
] as const;
// Row quota facts read as three fixed columns (5h, 7d, Fable) so every row
// puts each window's label and figure at the same x: a constant label and a
// right-aligned figure slot wide enough for "100%". A missing window keeps
// its (invisible) cell so the columns never shift.
const ROW_FACTS_CLASS = 'grid grid-cols-[repeat(3,auto)] gap-x-2.5';
const ROW_FACT_CLASS = 'grid grid-cols-[auto_4.5ch] items-baseline gap-x-1';
/** Windows whose utilization counts toward a row's peak. */
const PEAK_WINDOWS: ReadonlySet<string> = new Set([
  '5h',
  '7d',
  '7d_sonnet',
  '7d_opus',
  '7d_fable',
]);

type RowSnapshot = UpstreamUsageRow['windows'][number];

/** The subscription window with the highest used%; null without a reading. */
function peakSnapshot(row: UpstreamUsageRow): RowSnapshot | null {
  let peak: RowSnapshot | null = null;
  for (const snap of row.windows) {
    if (!PEAK_WINDOWS.has(snap.window) || snap.utilization == null) continue;
    if (peak === null || snap.utilization > (peak.utilization ?? -1))
      peak = snap;
  }
  return peak;
}

/** Highest used% (0-100) across the subscription windows; null without one. */
function peakUsedPct(row: UpstreamUsageRow): number | null {
  const utilization = peakSnapshot(row)?.utilization;
  return utilization == null ? null : utilization * 100;
}

/**
 * The row's quota severity: the worst pace-relative severity across its
 * subscription windows at `nowUnixSecs`, the same rule each figure is
 * inked by. Disabled rows carry no quota severity.
 */
function rowQuotaSeverity(
  row: UpstreamUsageRow,
  nowUnixSecs: number,
): QuotaSeverity {
  if (!row.upstream.enabled) return 'none';
  return worstQuotaSeverity(
    row.windows
      .filter((snap) => PEAK_WINDOWS.has(snap.window))
      .map((snap) => snapshotQuotaSeverity(snap, nowUnixSecs)),
  );
}

/** A status problem worth a phrase: reconnect nudges and danger health. */
function rowProblem(
  row: UpstreamUsageRow,
): { tone: 'warn' | 'danger'; label: string } | null {
  if (row.nudge) return { tone: row.nudge.tone, label: row.nudge.label };
  if (
    row.upstream.enabled &&
    (row.health.tone === 'danger' || row.health.tone === 'warn')
  )
    return { tone: row.health.tone, label: row.health.label };
  return null;
}

function needsAttention(row: UpstreamUsageRow, nowUnixSecs: number): boolean {
  // Disabled rows stay out of quota attention, but a confirmed OAuth nudge
  // still needs the same recovery path as an enabled row.
  if (!row.upstream.enabled) return row.nudge != null;
  const quota = rowQuotaSeverity(row, nowUnixSecs);
  return rowProblem(row) !== null || quota === 'warn' || quota === 'danger';
}

function compareUpstreamNames(
  a: UpstreamUsageRow,
  b: UpstreamUsageRow,
): number {
  return a.upstream.name.localeCompare(b.upstream.name, undefined, {
    numeric: true,
    sensitivity: 'base',
  });
}

function compareByUsage(a: UpstreamUsageRow, b: UpstreamUsageRow): number {
  return (
    (peakUsedPct(b) ?? -1) - (peakUsedPct(a) ?? -1) ||
    compareUpstreamNames(a, b)
  );
}

/**
 * Most urgent first: terminal credential failures, then danger (a danger
 * status such as unreadable credentials, or a window in pace-relative
 * danger), then warn, healthy, and disabled. Ties fall back to usage, then
 * name.
 */
function attentionRank(row: UpstreamUsageRow, nowUnixSecs: number): number {
  if (!row.upstream.enabled && row.nudge == null) return 4;
  if (row.nudge != null && isTerminalOAuthReconnectReason(row.nudge.reason))
    return 0;
  const problem = rowProblem(row);
  const quota = rowQuotaSeverity(row, nowUnixSecs);
  if (problem?.tone === 'danger' || quota === 'danger') return 1;
  if (problem || quota === 'warn') return 2;
  return row.upstream.enabled ? 3 : 4;
}

function upstreamRowId(row: UpstreamUsageRow): string {
  return row.upstream.id;
}

function UpstreamsPage() {
  const search = Route.useSearch();
  const { selectedId, action } = search;
  const view: EntityListView<UpstreamFilter, UpstreamSort> = {
    q: search.q ?? '',
    filter: search.filter ?? 'all',
    sort: search.sort ?? 'attention',
  };
  const navigate = useNavigate({ from: Route.fullPath });
  const queryClient = useQueryClient();
  // Upstreams, the 5s /latest poll, runtime status, reconnect nudges and
  // 7-day spend in one place; the DetailView polls /latest with the same
  // key, so TanStack dedups it into one request.
  const upstreamUsage = useUpstreamUsageData();

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

  // One clock for every quota judgment on this render: the row figures, the
  // attention filter and the attention sort.
  const nowUnixSecs = Math.floor(Date.now() / 1000);
  const viewConfig = useMemo<
    EntityListViewConfig<UpstreamUsageRow, UpstreamFilter, UpstreamSort>
  >(
    () => ({
      searchText: ({ upstream }) => {
        const oauth = upstream.kind === 'anthropic_oauth';
        // The plan is whatever the rows already fetched; no extra requests.
        const plan = oauth
          ? subscriptionPlanLabel(
              queryClient.getQueryData<SubscriptionMetadataResponse>(
                qk.upstreamSubscriptionMetadata(upstream.id),
              )?.organization_metadata,
            )
          : null;
        return [
          upstream.name,
          upstream.id,
          oauth ? 'Subscription OAuth' : 'API key',
          plan,
        ];
      },
      defaultFilter: 'all',
      filters: {
        all: () => true,
        attention: (row) => needsAttention(row, nowUnixSecs),
        disabled: (row) => !row.upstream.enabled,
        oauth: (row) => row.upstream.kind === 'anthropic_oauth',
        api_key: (row) => row.upstream.kind !== 'anthropic_oauth',
      },
      sorts: {
        attention: (a, b) =>
          attentionRank(a, nowUnixSecs) - attentionRank(b, nowUnixSecs) ||
          compareByUsage(a, b),
        usage: compareByUsage,
        name: compareUpstreamNames,
      },
    }),
    [queryClient, nowUnixSecs],
  );
  const listView = useEntityListView(upstreamUsage.rows, view, viewConfig);

  const selected =
    upstreamUsage.rows.find((row) => row.upstream.id === selectedId)
      ?.upstream ?? null;
  // `resetScroll: false`: the router's scroll restoration would otherwise
  // snap the list pane back to its top on every selection, so arrowing to
  // a row below the fold would leave it out of view.
  const select = (id: string | undefined) =>
    navigate({ search: { selectedId: id, ...view }, resetScroll: false });
  const changeView = (
    patch: Partial<EntityListView<UpstreamFilter, UpstreamSort>>,
  ) =>
    navigate({
      replace: true,
      resetScroll: false,
      search: { selectedId, ...view, ...patch },
    });

  // From md the detail pane is never blank: pick the first row of the
  // current sort and filter. The sorts rank by quota and connection status,
  // so wait for both; picking earlier selects a row that then sorts away
  // from the top.
  const firstVisibleId = listView.visible[0]?.upstream.id;
  const sortInputsPending =
    upstreamUsage.isLoading ||
    upstreamUsage.quotaPending ||
    upstreamUsage.statusPending;
  useEffect(() => {
    if (sortInputsPending || selected || !firstVisibleId) return;
    if (window.matchMedia('(min-width: 768px)').matches) {
      navigate({
        search: (previous) => ({ ...previous, selectedId: firstVisibleId }),
        replace: true,
        resetScroll: false,
      });
    }
  }, [sortInputsPending, selected, firstVisibleId, navigate]);

  const total = upstreamUsage.rows.length;

  return (
    <div className="h-shell min-h-0 flex w-full max-w-[90rem] mx-auto">
      <EntityList
        className={cx(
          'w-full shrink-0 border-r border-subtle md:w-[360px] xl:w-[400px]',
          selected ? 'hidden md:flex' : 'flex',
        )}
        title="Upstreams"
        noun="upstreams"
        countLine={
          upstreamUsage.isLoading
            ? null
            : [
                `${formatCount(total)} ${total === 1 ? 'upstream' : 'upstreams'}`,
                upstreamUsage.reconnectCount > 0
                  ? `${formatCount(upstreamUsage.reconnectCount)} need${upstreamUsage.reconnectCount === 1 ? 's' : ''} reconnect`
                  : null,
              ]
                .filter(Boolean)
                .join(' · ')
        }
        action={
          <Button
            size="sm"
            variant="primary"
            iconLeft={<Plus />}
            onClick={openCreate}
          >
            Add upstream
          </Button>
        }
        loading={upstreamUsage.isLoading}
        totalCount={total}
        result={listView}
        toolbar={{
          view,
          filterOptions: UPSTREAM_FILTER_OPTIONS,
          sortOptions: UPSTREAM_SORT_OPTIONS,
          onViewChange: changeView,
          onClear: () => changeView({ q: '', filter: 'all' }),
        }}
        getId={upstreamRowId}
        renderRow={(row) => {
          const { upstream } = row;
          const oauth = upstream.kind === 'anthropic_oauth';
          const quotaPending = oauth && upstreamUsage.quotaPending;
          const peakSnap = oauth ? peakSnapshot(row) : null;
          const peak =
            peakSnap?.utilization == null ? null : peakSnap.utilization * 100;
          const problem = rowProblem(row);
          const facts = oauth
            ? ROW_WINDOWS.flatMap(([windowName, label]) => {
                const snap = row.windows.find((s) => s.window === windowName);
                return snap?.utilization == null
                  ? []
                  : [
                      {
                        window: windowName,
                        label,
                        used: snap.utilization * 100,
                        severity: snapshotQuotaSeverity(snap, nowUnixSecs),
                      },
                    ];
              })
            : [];
          const factsText = facts
            .map((f) => `${f.label} ${formatQuotaPercent(f.used)}`)
            .join(' · ');
          return {
            name: upstream.name,
            muted: !upstream.enabled,
            title: [
              upstream.name,
              !upstream.enabled ? 'Disabled' : null,
              problem?.label,
              factsText ? `${factsText} used` : null,
              row.runtimeError,
            ]
              .filter(Boolean)
              .join('\n'),
            trailing: quotaPending ? (
              <Skeleton as="span" className="inline-block h-4 w-8" />
            ) : peakSnap != null && peak != null ? (
              <>
                <span
                  className={
                    QUOTA_SEVERITY_TEXT_CLASS[
                      snapshotQuotaSeverity(peakSnap, nowUnixSecs)
                    ]
                  }
                >
                  {formatQuotaPercent(peak)}
                </span>
                <span className="sr-only"> used</span>
              </>
            ) : null,
            caption: problem ? (
              <>
                {!upstream.enabled ? (
                  <span className="text-text-muted">Disabled · </span>
                ) : null}
                <span
                  aria-hidden="true"
                  className={cx('status-dot shrink-0', problem.tone)}
                />
                <span
                  className={cx(
                    'truncate',
                    problem.tone === 'danger'
                      ? 'text-danger-text'
                      : 'text-warn-text',
                  )}
                >
                  {problem.label}
                </span>
              </>
            ) : !upstream.enabled ? (
              <span className="text-text-muted">Disabled</span>
            ) : (
              <UpstreamPlanCaption upstream={upstream} />
            ),
            captionTrailing: quotaPending ? (
              <Skeleton as="span" className="inline-block h-3 w-24" />
            ) : facts.length ? (
              <span
                className={ROW_FACTS_CLASS}
                data-testid="upstream-row-quota"
              >
                {ROW_WINDOWS.map(([windowName, label]) => {
                  const fact = facts.find((f) => f.window === windowName);
                  const severity = fact?.severity ?? 'none';
                  return (
                    <span
                      key={windowName}
                      aria-hidden={fact ? undefined : true}
                      className={cx(ROW_FACT_CLASS, !fact && 'invisible')}
                      data-window={windowName}
                    >
                      <span className="text-text-faint">{label}</span>
                      <span
                        className={cx(
                          'text-right',
                          severity === 'warn' || severity === 'danger'
                            ? QUOTA_SEVERITY_TEXT_CLASS[severity]
                            : 'text-text-muted',
                        )}
                      >
                        {fact ? formatQuotaPercent(fact.used) : '—'}
                      </span>
                    </span>
                  );
                })}
              </span>
            ) : null,
          };
        }}
        selectedId={selectedId}
        onSelect={select}
        empty={<UpstreamsListEmpty onCreate={openCreate} />}
        skeletonTestId="upstream-list-loading-row"
      />

      {/* Phones show the list or the selected upstream, never both; from md
          the detail sits beside the list and DetailPane owns its scroll.
          Keyed by the selection so each upstream opens at the top. */}
      <div
        key={selected?.id ?? 'none'}
        className={cx(
          'min-h-0 min-w-0 flex-1 flex-col',
          selected ? 'flex' : 'hidden md:flex',
        )}
        data-testid="upstream-detail-pane"
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
        ) : upstreamUsage.isLoading ? (
          <UpstreamDetailLoadingShell />
        ) : total ? (
          <div className="flex flex-1 items-center justify-center px-4 md:px-8">
            <EmptyState
              headingLevel={2}
              title="Select an upstream"
              description="Pick an upstream from the list to see its configuration, OAuth state, and recent requests."
            />
          </div>
        ) : (
          <div className="flex flex-1 items-center justify-center px-4 md:px-8">
            <EmptyState
              headingLevel={2}
              title="Upstream details appear here"
              description="Quota, OAuth state, settings, and recent requests show here once you add an upstream."
            />
          </div>
        )}
      </div>

      <UpstreamConnectDialog
        target={connectTarget}
        onClose={() => setConnectTarget(null)}
        onCreated={(created) => select(created.id)}
      />
    </div>
  );
}

const QUOTA_HISTORY_RANGES = TIME_PRESETS;
const QUOTA_HISTORY_RANGE_OPTIONS = TIME_PRESET_OPTIONS;
// Loading placeholder mirrors SegmentedControl (size md) so nothing shifts.
const QUOTA_HISTORY_RANGE_GROUP_CLASS =
  'inline-flex items-center gap-0.5 rounded-sm border border-subtle bg-overlay-2 p-0.5';
const QUOTA_HISTORY_RANGE_ITEM_CLASS =
  'h-9 md:h-[1.625rem] px-2.5 text-xs rounded-sm';
const QUOTA_CHART_HEIGHT = 280;

/**
 * Windows that keep a gradient fill in the quota-history chart: the three
 * live subscription windows. Legacy model windows, unified and overage draw
 * as bare strokes so five-plus overlapping windows stay readable instead of
 * stacking into a tinted wash.
 */
const QUOTA_FILL_WINDOWS: Record<string, true> = {
  '5h': true,
  '7d': true,
  '7d_fable': true,
};

function IdentitySkeleton() {
  return (
    <div
      data-testid="upstream-metadata-loading"
      className="grid grid-cols-2 gap-x-6 gap-y-4 @2xl:grid-cols-3 @4xl:grid-cols-4"
    >
      {['w-24', 'w-20', 'w-28', 'w-16'].map((width) => (
        <div key={width} className="flex flex-col gap-1.5">
          <Skeleton className="h-3 w-16" />
          <Skeleton className={cx('h-4', width)} />
        </div>
      ))}
    </div>
  );
}

function UpstreamDetailLoadingShell() {
  return (
    <DetailPane
      data-testid="upstream-detail-loading-shell"
      aria-busy="true"
      aria-label="Loading upstream details"
      header={<DetailHeaderSkeleton />}
    >
      <DetailSectionGrid>
        <DetailSection
          span="full"
          title="Quota"
          description={<Skeleton as="span" className="block h-4 w-56" />}
        >
          <QuotaWindowRows>
            {Array.from({ length: 3 }).map((_, index) => (
              <QuotaWindowRowSkeleton key={index} />
            ))}
          </QuotaWindowRows>
        </DetailSection>
        <DetailSection
          span="full"
          title="Quota history"
          description="Used per window over time"
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
        </DetailSection>
        <DetailSection
          collapsible
          title="Account metadata"
          description="Subscription details Anthropic reports for this account"
        >
          <div
            data-testid="upstream-detail-loading-metadata"
            className="min-h-9"
          >
            <IdentitySkeleton />
          </div>
        </DetailSection>
      </DetailSectionGrid>
    </DetailPane>
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

// The reconnect classifier owns terminal failures and the mode-specific login
// deadline. A usable credential can be green; an observed retry or a lapsed
// renewable access token remains yellow, never a new reconnect requirement.
function oauthBadge(
  entry: UpstreamOAuthStatusResponse | undefined,
  reconnect: OAuthReconnectNudge | null,
  lastApplyError: string | null | undefined,
  nowUnixSecs: number,
): OAuthBadge {
  if (!entry || entry.status === 'wrong_kind')
    return { tone: 'neutral', label: 'Unknown' };
  if (entry.status === 'corrupted')
    return { tone: 'danger', label: 'Reconnect required' };
  if (reconnect?.tone === 'danger')
    return {
      tone: 'danger',
      label:
        reconnect.reason === 'long_lived_expired' ||
        reconnect.reason === 'refresh_token_expired' ||
        reconnect.reason === 'access_token_expired'
          ? 'Login expired'
          : 'Reconnect required',
    };
  if (!entry.has_credentials || entry.status === 'missing')
    return { tone: 'neutral', label: 'Not connected' };
  if (lastApplyError)
    return {
      tone: 'warn',
      label: entry.mode === 'long_lived_365d' ? 'Degraded' : 'Retrying',
    };
  if (reconnect?.tone === 'warn')
    return {
      tone: 'warn',
      label:
        reconnect.reason === 'refresh_token_missing'
          ? 'Refresh missing'
          : 'Login expiring',
    };
  if (entry.mode == null || entry.expires_at_unix_secs == null)
    return { tone: 'neutral', label: 'Unknown' };
  if (entry.mode === 'long_lived_365d')
    return { tone: 'ok', label: 'Long-lived' };
  if (!entry.can_refresh) return { tone: 'warn', label: 'Renewal unavailable' };
  if (entry.expires_at_unix_secs <= nowUnixSecs)
    return { tone: 'warn', label: 'Renewing' };
  return { tone: 'ok', label: 'Connected' };
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
          className="inline-flex items-center gap-1 rounded-sm text-text underline decoration-border-strong underline-offset-2 transition-colors hover:decoration-accent focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-2"
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
  // The quota-history range is the shared, persisted preset: choosing it on
  // any non-Logs surface re-scopes this chart and vice versa.
  const { range, setRange } = useSharedTimeRange();
  const [isolatedWindow, setIsolatedWindow] = useState<string | null>(null);
  const chartId = useChartId();
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

  // Keep a separate wall clock for snapshot freshness/countdowns. Series
  // requests use stable range keys and resolve their own absolute bounds
  // when each request starts.
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
    windows: UPSTREAM_LATEST_WINDOWS,
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
  const seriesSinceUnixSecs =
    quotaSeries.data?.since_unix_secs ?? nowUnixSecs - rangeSecs;
  const seriesUntilUnixSecs = quotaSeries.data?.until_unix_secs ?? nowUnixSecs;

  // API usage follows the same shared preset as the quota history; 1h/6h
  // bucket by minute so the chart has more than one point (as on Overview).
  const [apiUsageMetric, setApiUsageMetric] = useState<'tokens' | 'cost'>(
    'tokens',
  );
  const apiUsageQ = useUsage(
    range,
    range === '7d' || range === '24h' ? 'hour' : 'minute',
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

  // The chart plots used% (as Claude reports it), clamped to the 0-100 axis.
  const chartRows = useMemo(
    () =>
      chartData.rows
        .filter((row) => row.unix >= seriesSinceUnixSecs)
        .map((row) => {
          const next: typeof row = { ...row };
          for (const [key, value] of Object.entries(row)) {
            if (key !== 'unix' && typeof value === 'number') {
              next[key] = Math.min(100, Math.max(0, value));
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

  const feed = useRequestEventsFeed({
    filters: {
      upstream_id: upstream.id,
      event_kind: 'messages',
    },
    mode: 'infinite',
    initialHistoryLimit: 500,
    pageSize: 50,
    maxRetained: 500,
  });

  const metadataPending =
    isOauth &&
    subscriptionMetadataQ.data === undefined &&
    subscriptionMetadataQ.isPending;
  const quotaLatestPending =
    quotaLatest.data === undefined && quotaLatest.isPending;
  const quotaHistoryPending =
    quotaLatestPending ||
    (quotaSeries.data === undefined && quotaSeries.isPending);
  const oauthStatusPending =
    isOauth && upstreamOAuthQ.data === undefined && upstreamOAuthQ.isPending;
  const subMeta = subscriptionMetadataQ.data?.subscription_metadata;
  const orgMeta = subscriptionMetadataQ.data?.organization_metadata;
  const principalEntry = upstreamOAuthQ.data?.has_credentials
    ? upstreamOAuthQ.data
    : null;
  const hasBoundToken = Boolean(principalEntry);
  const reconnectNudge = isOauth
    ? classifyOAuthReconnect(
        upstreamOAuthQ.data,
        upstream.status.last_apply_error,
        nowUnixSecs,
      )
    : null;
  const oauthStatusBadge: OAuthBadge =
    upstreamOAuthQ.isError && reconnectNudge?.tone !== 'danger'
      ? { tone: 'danger', label: 'Unavailable' }
      : oauthBadge(
          upstreamOAuthQ.data,
          reconnectNudge,
          upstream.status.last_apply_error,
          nowUnixSecs,
        );
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

  const planLabel = isOauth
    ? (subscriptionPlanLabel(orgMeta) ?? 'Claude subscription')
    : 'API key';
  // The header carries plan, account and organization; the collapsed
  // metadata section keeps only what the header does not say. API-key
  // endpoint and key source live in Settings, where they are edited.
  const headerMeta: React.ReactNode[] = !isOauth
    ? []
    : metadataPending
      ? [<Skeleton key="meta" as="span" className="inline-block h-4 w-48" />]
      : [
          orgMeta?.account_email || orgMeta?.account_display_name,
          orgMeta?.organization_name,
        ];

  const metadataFacts: { label: string; value: React.ReactNode }[] = [];
  if (
    orgMeta?.account_display_name &&
    orgMeta.account_email &&
    orgMeta.account_display_name !== orgMeta.account_email
  )
    metadataFacts.push({
      label: 'Account name',
      value: orgMeta.account_display_name,
    });
  if (subMeta?.organization_role)
    metadataFacts.push({ label: 'Role', value: subMeta.organization_role });
  if (subMeta?.workspace_role)
    metadataFacts.push({ label: 'Seat', value: subMeta.workspace_role });
  if (orgMeta?.billing_type)
    metadataFacts.push({
      label: 'Billing',
      // "stripe_subscription" → "Stripe subscription"
      value: `${orgMeta.billing_type.charAt(0).toUpperCase()}${orgMeta.billing_type.slice(1).replaceAll('_', ' ')}`,
    });
  if (orgMeta?.subscription_created_at_unix_secs)
    metadataFacts.push({
      label: 'Subscribed',
      value: (
        <RelativeTime
          ts={new Date(orgMeta.subscription_created_at_unix_secs * 1000)}
        />
      ),
    });

  const identity = (
    <DetailSection
      collapsible
      // Open whenever a credential is bound: the expanded facts then stand
      // beside the taller Credential section so the shared row has no hole.
      defaultOpen={hasBoundToken}
      title="Account metadata"
      description="Subscription details Anthropic reports for this account"
      action={
        <IconButton
          label="Refresh subscription metadata"
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
        >
          <RefreshCw
            strokeWidth={1.75}
            className={cx(
              triggerSubscriptionMetadataRefresh.isPending && 'animate-spin',
            )}
          />
        </IconButton>
      }
    >
      <div data-testid="upstream-metadata-strip" className="min-h-9">
        {metadataPending ? (
          <IdentitySkeleton />
        ) : metadataFacts.length ? (
          <DetailFacts>
            {metadataFacts.map((f) => (
              <DetailFact key={f.label} label={f.label}>
                {f.value}
              </DetailFact>
            ))}
          </DetailFacts>
        ) : (
          <p className="flex min-h-36 items-center text-body-sm text-text-muted">
            No subscription metadata reported yet.
          </p>
        )}
      </div>
    </DetailSection>
  );

  const quotaWindows = (
    <DetailSection
      span="full"
      title="Quota"
      description={
        <span className="flex flex-wrap items-center gap-x-2">
          Used per window and when it resets
          {selectedLatest ? (
            <>
              <QuotaFreshnessCaption snapshots={selectedLatest.windows} />
              <PaceLegend />
            </>
          ) : null}
        </span>
      }
      action={
        <LimitResetAction
          key={upstream.id}
          upstream={upstream}
          credentialsStored={upstreamOAuthQ.data?.has_credentials}
          quotaWindows={
            quotaLatest.isError ? [] : (selectedLatest?.windows ?? [])
          }
        />
      }
    >
      {quotaLatestPending ? (
        <QuotaWindowRows>
          {Array.from({ length: 3 }).map((_, index) => (
            <QuotaWindowRowSkeleton key={index} />
          ))}
        </QuotaWindowRows>
      ) : !selectedLatest ? (
        <EmptyState title={`No subscription quota data for ${upstream.name}`} />
      ) : (
        <QuotaWindowRows>
          {selectQuotaCardSnapshots({
            latestWindows: selectedLatest.windows,
          }).map((snap) => (
            <QuotaWindowRow
              key={snap.window}
              snap={snap}
              nowUnixSecs={nowUnixSecs}
            />
          ))}
        </QuotaWindowRows>
      )}
    </DetailSection>
  );

  const quotaHistory = (
    <DetailSection
      span="full"
      title="Quota history"
      description="Used per window over time"
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
          className="mb-3 flex min-h-5 flex-wrap items-center gap-x-5 gap-y-1 max-md:gap-y-0"
        >
          {quotaHistoryPending ? (
            <>
              <Skeleton className="h-3 w-20" />
              <Skeleton className="h-3 w-20" />
            </>
          ) : chartData.rows.length > 0 && visibleGraphWindows.length > 0 ? (
            visibleGraphWindows.map((windowName) => {
              const dimmed =
                effectiveIsolatedWindow !== null &&
                effectiveIsolatedWindow !== windowName;
              const currentSnap = selectedLatest?.windows.find(
                (w) => w.window === windowName,
              );
              const current = currentSnap?.utilization;
              const currentSeverity = currentSnap
                ? snapshotQuotaSeverity(currentSnap, nowUnixSecs)
                : 'none';
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
                  className="-mx-1.5 flex min-h-6 max-md:min-h-10 cursor-pointer items-center gap-1.5 rounded-sm px-1.5 text-body-sm transition-colors hover:bg-overlay-5 focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-1"
                >
                  <span
                    aria-hidden="true"
                    className={cx(
                      'size-2.5 shrink-0 rounded-xs',
                      dimmed && 'opacity-40',
                    )}
                    style={{
                      backgroundColor: getWindowColor(windowName).stroke,
                    }}
                  />
                  <span
                    className={dimmed ? 'text-text-faint' : 'text-text-muted'}
                  >
                    {windowLabel(windowName)}
                  </span>
                  {current != null ? (
                    <span
                      className={cx(
                        'tabular-nums',
                        dimmed
                          ? 'text-text-faint'
                          : QUOTA_SEVERITY_TEXT_CLASS[currentSeverity],
                      )}
                    >
                      {formatQuotaPercent(current * 100)} used
                    </span>
                  ) : null}
                </button>
              );
            })
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
                          // A sample's pace comes from its own moment inside
                          // the latest snapshot's window; samples from earlier
                          // windows have no known reset and stay neutral
                          // rather than judged by today's pace.
                          const sampleSnap = selectedLatest?.windows.find(
                            (w) => w.window === key,
                          );
                          const samplePace = sampleSnap
                            ? snapshotQuotaPacePct(sampleSnap, Number(label))
                            : null;
                          return (
                            <div
                              key={i}
                              className="flex items-center justify-between gap-4 py-px"
                            >
                              <span className="flex items-center gap-1.5 text-text-muted">
                                <span
                                  aria-hidden="true"
                                  className="size-2.5 shrink-0 rounded-xs"
                                  style={{
                                    backgroundColor: getWindowColor(key).stroke,
                                  }}
                                />
                                {windowLabel(key)}
                              </span>
                              <span
                                className={cx(
                                  'tabular-nums',
                                  typeof p.value === 'number' &&
                                    QUOTA_SEVERITY_TEXT_CLASS[
                                      quotaSeverity(p.value, samplePace)
                                    ],
                                )}
                              >
                                {typeof p.value === 'number'
                                  ? `${formatQuotaPercent(p.value)} used`
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
                <defs>
                  {visibleGraphWindows.map((windowName) =>
                    QUOTA_FILL_WINDOWS[windowName] ? (
                      <SeriesFillGradient
                        key={windowName}
                        id={`${chartId}-${windowName}`}
                        color={getWindowColor(windowName).fill}
                        // Many overlapping windows stack their tints; keep
                        // each lighter so the lower lines still read.
                        strength={
                          effectiveIsolatedWindow === null &&
                          visibleGraphWindows.length > 3
                            ? 1
                            : 2
                        }
                      />
                    ) : null,
                  )}
                </defs>
                {visibleGraphWindows.map((windowName) => (
                  <Area
                    key={windowName}
                    type="monotone"
                    dataKey={windowName}
                    stroke={getWindowColor(windowName).stroke}
                    strokeWidth={1.5}
                    // Only the live windows get a gradient fill; legacy
                    // windows, unified and overage draw as strokes so
                    // overlapping series stay individually traceable.
                    fill={
                      QUOTA_FILL_WINDOWS[windowName]
                        ? `url(#${chartId}-${windowName})`
                        : 'none'
                    }
                    fillOpacity={1}
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
          </p>
        )}
      </div>
    </DetailSection>
  );

  const titleId = 'upstream-detail-title';
  const hasBillingNotice =
    isOauth &&
    (orgMeta?.claude_code_trial_ends_at ||
      orgMeta?.payment_auth_hosted_invoice_url ||
      orgMeta?.overage_credit_granted ||
      orgMeta?.overage_credit_eligible ||
      (orgMeta?.overage_credit_amount_minor_units != null &&
        orgMeta.overage_credit_amount_minor_units > 0));
  const applyError = isOauth ? null : upstreamRuntimeStatus?.last_apply_error;
  const hasNotices =
    reconnectNudge != null || !!applyError || !!hasBillingNotice;

  return (
    <>
      <DetailPane
        aria-labelledby={titleId}
        header={
          <DetailHeader
            backLabel="All upstreams"
            onBack={onBack}
            title={<InlineNameEditor upstream={upstream} />}
            titleId={titleId}
            badge={
              metadataPending ? (
                <Skeleton as="span" className="inline-block h-5 w-24" />
              ) : (
                <Badge>{planLabel}</Badge>
              )
            }
            // One danger surface per problem: when the reconnect notice
            // below owns the diagnosis, the header stays quiet.
            status={
              headerHealth.tone === 'danger' && !reconnectNudge ? (
                <StatusBadge tone="danger" label={headerHealth.label} />
              ) : null
            }
            meta={headerMeta}
            id={upstream.id}
            idLabel="Upstream ID"
            enabled={upstream.enabled}
            onEnabledChange={(checked) =>
              setEnabledConfirm({ open: true, enabled: checked })
            }
            enabledDisabled={toggle.isPending}
            pendingLabel={togglePendingLabel}
            onDelete={() => setConfirmDeleteOpen(true)}
            deleting={del.isPending}
          />
        }
        notices={
          hasNotices ? (
            <>
              {reconnectNudge ? (
                <OAuthReconnectNotice
                  nudge={reconnectNudge}
                  onReconnect={onConnect}
                />
              ) : null}
              {applyError ? (
                <Notice tone="danger" title="Last apply failed">
                  <span className="break-words">{applyError}</span>
                </Notice>
              ) : null}
              {hasBillingNotice ? (
                <>
                  <TrialBanner orgMeta={orgMeta} />
                  <PaymentWarning orgMeta={orgMeta} />
                  <PromotionalCreditsBadge orgMeta={orgMeta} />
                </>
              ) : null}
            </>
          ) : null
        }
      >
        <DetailSectionGrid>
          {isOauth ? (
            <>
              {quotaWindows}
              {quotaHistory}
            </>
          ) : (
            <ApiUsageCard
              data={apiUsageQ.data}
              isLoading={apiUsageQ.data === undefined && apiUsageQ.isPending}
              range={range}
              onRangeChange={setRange}
              metric={apiUsageMetric}
              onMetricChange={setApiUsageMetric}
            />
          )}

          <DetailSection
            span="full"
            data-testid="recent-requests-card"
            title="Recent requests"
            // The table names an empty result and shows the count; the
            // subtitle only says what the list is.
            description="Latest requests routed here"
          >
            <RequestEventsFeed
              feed={feed}
              principalNameMap={principalNameMap}
              columns={{
                upstream: false,
                cost: true,
                tokens: true,
              }}
              tableContainerClassName="glass min-h-48 h-96 max-h-[60vh] overflow-auto rounded-md"
              emptyTitle="No recent requests for this upstream"
            />
          </DetailSection>

          {isOauth ? (
            <>
              <DetailSection
                title="Credential"
                description={
                  oauthStatusPending ? (
                    <Skeleton className="h-3 w-32" />
                  ) : upstreamOAuthQ.isError || !upstreamOAuthQ.data ? (
                    'Credential status unavailable'
                  ) : hasBoundToken ? (
                    'OAuth token bound on this upstream'
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
                      <span data-testid="oauth-current-status">
                        <StatusBadge
                          tone={oauthStatusBadge.tone}
                          label={oauthStatusBadge.label}
                        />
                      </span>
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
              >
                <div
                  data-testid="oauth-status-card-body"
                  className="text-body-sm"
                >
                  {oauthStatusPending ? (
                    <div
                      data-testid="oauth-status-loading-grid"
                      className="space-y-3"
                    >
                      <div className="grid grid-cols-1 gap-4 @md:grid-cols-2">
                        {Array.from({ length: 3 }).map((_, index) => (
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
                  ) : upstreamOAuthQ.isError || !upstreamOAuthQ.data ? (
                    <p className="text-body-sm text-danger-text">
                      Credential status could not be loaded. Refresh this page
                      to try again.
                    </p>
                  ) : !hasBoundToken ? (
                    <p className="text-body-sm text-text-muted">
                      Use Connect to authorize this upstream with a Claude
                      account.
                    </p>
                  ) : principalEntry ? (
                    <div
                      data-testid="oauth-status-loaded-grid"
                      className="space-y-3"
                    >
                      <div className="grid grid-cols-1 gap-4 @md:grid-cols-2">
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
                                      principalEntry.expires_at_unix_secs *
                                        1000,
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
                                  Auto-refreshes; reauthorize about every 30
                                  days
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
                              <span className="text-text-muted">
                                Not stored
                              </span>
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
                          <div className="text-label text-text-faint">
                            Scopes
                          </div>
                          <div className="mt-1 break-all font-mono text-data text-text-muted">
                            {principalEntry.scopes.join(', ')}
                          </div>
                        </div>
                      ) : null}
                    </div>
                  ) : (
                    <p className="text-body-sm text-text-muted">
                      Token is bound on the upstream but no credential details
                      are available right now.
                    </p>
                  )}
                </div>
              </DetailSection>
              {/* Account metadata pairs beside Credential in the row above;
                  Warm-up keeps the full row below — half-width pairing with
                  these two sections always leaves a dead column gap. */}
              {identity}
              <WarmupCardMinimal
                upstream={upstream}
                credentialNoticeShown={reconnectNudge != null}
              />
            </>
          ) : (
            <SettingsCard upstream={upstream} />
          )}
        </DetailSectionGrid>
      </DetailPane>

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
