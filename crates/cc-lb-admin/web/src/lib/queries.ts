// biome-ignore-all lint/suspicious/noExplicitAny: existing API types use any for record params
// TanStack Query hooks for every admin v1 endpoint surfaced by the dashboard.
// Source-of-truth: .omo/plans/cc-lb-dashboard-overhaul.md (API SURFACE section).

import {
  type InfiniteData,
  type QueryClient,
  queryOptions,
  experimental_streamedQuery as streamedQuery,
  useInfiniteQuery,
  useIsMutating,
  useMutation,
  useQuery,
  useQueryClient,
} from '@tanstack/react-query';
import { useMemo } from 'react';
import { toast } from 'sonner';
import {
  type AggregateResponse,
  type AnalysisResponse,
  ApiError,
  type Upstream as ApiUpstream,
  type AuditQueryResponse,
  type ConfigDraftResponse,
  type ConfigHistoryResponse,
  type ConfigSchemaResponse,
  completeOauthDraft,
  createUpstreamFromOauthDraft,
  type DashboardSummaryResponse,
  type DashboardUsageResponse,
  deleteJson,
  type EventsHistogramPayload,
  type EventsHistogramRange,
  fetchEventsHistogram,
  fetchWithAuth,
  getJson,
  type KeyListResponse,
  type LatestResponse,
  type PoolHistoryResponse,
  type PrincipalLimitsResponse,
  patchJson,
  postJson,
  putJson,
  type RecentEventsPayload,
  type RequestEvent,
  type RequestEventUpdate,
  type SeriesResponse,
  type SubscriptionMetadataResponse,
  startOauthDraft,
  streamRequestEventUpdates,
  triggerSubscriptionMetadataRefresh,
  type UpstreamOAuthStatusResponse,
} from './api';

import {
  type CacheKeepaliveSessionsFilters,
  getCacheKeepaliveSessionDetail,
  getCacheKeepaliveSessions,
} from './cacheKeepaliveApi';
import { usePolledData } from './usePolledData';
import { useVisibility } from './visibilityManager';

const PRINCIPAL_WRITE_MUTATION_KEY = ['principal-write'] as const;

export const POLLING_INTERVALS = {
  SUMMARY_MS: 5_000,
  USAGE_MS: 5_000,
  QUOTA_LATEST_MS: 5_000,
  QUOTA_AGGREGATE_MS: 30_000,
  QUOTA_ANALYSIS_MS: 120_000,
  QUOTA_SERIES_MS: 30_000,
  QUOTA_POOL_HISTORY_MS: 30_000,
  STATUS_MS: 15_000,
  UPSTREAMS_MS: 30_000,
  WARMUP_SUMMARY_MS: 30_000,
  UPSTREAM_SUB_META_MS: 30_000,
  RECENT_EVENTS_MS: 10_000,
  CACHE_KEEPALIVE_SESSIONS_MS: 5_000,
  CACHE_KEEPALIVE_SUMMARY_MS: 5_000,
  CACHE_KEEPALIVE_DETAIL_MS: 5_000,
};

export type Upstream = ApiUpstream;
export type UpstreamWarmupDialectPlugin = NonNullable<
  Upstream['warmup_dialect_plugin']
>;

interface UpstreamListResp {
  upstreams: Upstream[];
}

export type LimitKind =
  | 'requests'
  | 'input_tokens'
  | 'output_tokens'
  | 'total_tokens'
  | 'cost_usd'
  | 'concurrent';

export interface PrincipalDefaultLimit {
  kind: LimitKind;
  window_secs: number;
  cap_micros: number;
}
export interface Principal {
  id: string;
  name: string;
  kind: 'machine' | 'human' | 'admin';
  enabled: boolean;
  revision: number;
  allowed_models: string[];
  allowed_upstreams: string[];
  default_limits: PrincipalDefaultLimit[];
  cache_keepalive?: {
    enabled: boolean;
    refresh_lead_time_5m_secs: number;
    refresh_lead_time_1h_secs: number;
    max_refreshes_per_session: number;
    max_total_duration_secs: number;
    snapshot_max_bytes: number;
    classifier: {
      extra_wait_for_user_tools: string[];
      treat_end_turn_as_ambiguous: boolean;
      llm_judge?: unknown;
    };
  } | null;
}
interface PrincipalListResp {
  principals: Principal[];
}

export type PluginMetadata = {
  purpose: string;
  keeps: string;
  drops: string;
  empty_behavior: string;
  examples: string[];
};

export type HookMetadata = {
  wire_version: number;
  description: string;
  usage: string;
  mode?: 'active' | 'noop';
};

export interface PluginEntry {
  id: string;
  sha256_hex: string;
  name: string;
  original_filename: string;
  description: string;
  usage: string;
  hook_metadata: Record<string, HookMetadata>;
  label: string | null;
  size_bytes: number;
  refcount: number;
  revision: number;
  uploaded_at_unix_secs: number;
  is_builtin?: boolean;
  kind?: string;
  wire_version?: number;
  version?: string | null;
  metadata: PluginMetadata | null;
  supported_slots?: ChainSlot[];
}
interface PluginListResp {
  entries: PluginEntry[];
}

export type ChainSlot = 'router' | 'observability_hook' | 'shape';
export interface PluginChainEntry {
  id: string;
  principal_id: string;
  slot: ChainSlot;
  order: number;
  wasm_registry_id: string;
  config: unknown;
  sse_per_event: boolean;
  batched_events_per_flush: number;
  batched_flush_ms: number;
  revision: number;
}
interface PluginChainResp {
  entries: PluginChainEntry[];
}

interface StatusUpstream {
  id: string;
  name: string;
  status: string;
  last_apply_at_unix_secs: number | null;
  last_apply_error: string | null;
}
interface StatusResponse {
  version: string;
  git_sha: string;
  uptime_secs: number;
  build: { rust_version: string; profile: string; target: string };
  generation: number;
  upstreams: StatusUpstream[];
}

interface QuotaQueryIdentity {
  readonly upstreamIds?: string;
  readonly windows?: string;
  readonly source?: string;
}

function hasSameQuotaIdentity(
  previousQueryKey: readonly unknown[] | undefined,
  resource: 'latest' | 'series' | 'analysis' | 'aggregate' | 'pool-history',
  currentParams: QuotaQueryIdentity,
): boolean {
  const previousParams = previousQueryKey?.[2];
  if (
    previousQueryKey?.[0] !== 'subscription-quota' ||
    previousQueryKey[1] !== resource ||
    previousParams === null ||
    typeof previousParams !== 'object' ||
    Array.isArray(previousParams)
  ) {
    return false;
  }
  const previous = previousParams as QuotaQueryIdentity;
  return (
    previous.upstreamIds === currentParams.upstreamIds &&
    previous.windows === currentParams.windows &&
    previous.source === currentParams.source
  );
}

function effectiveFilterEntries(
  filters: Record<string, string | undefined>,
): [string, string][] {
  return Object.entries(filters)
    .filter((entry): entry is [string, string] => Boolean(entry[1]))
    .sort(([left], [right]) => left.localeCompare(right));
}

function hasSameEventFilters(
  previousFilters: unknown,
  currentFilters: Record<string, string | undefined>,
): boolean {
  if (
    previousFilters === null ||
    typeof previousFilters !== 'object' ||
    Array.isArray(previousFilters)
  ) {
    return false;
  }
  const previousEntries = effectiveFilterEntries(
    previousFilters as Record<string, string | undefined>,
  );
  const currentEntries = effectiveFilterEntries(currentFilters);
  return (
    previousEntries.length === currentEntries.length &&
    previousEntries.every(
      ([key, value], index) =>
        key === currentEntries[index]?.[0] &&
        value === currentEntries[index]?.[1],
    )
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Query keys

export const qk = {
  health: ['health'] as const,
  status: ['status'] as const,
  summary: (range: string) => ['summary', range] as const,
  usage: (
    range: string,
    step: string,
    group: string,
    upstreamId?: string,
    projection?: 'full' | 'totals',
  ) =>
    [
      'usage',
      range,
      step,
      group,
      upstreamId ?? null,
      projection ?? 'full',
    ] as const,
  upstreams: ['upstreams'] as const,
  upstream: (id: string) => ['upstream', id] as const,
  principals: ['principals'] as const,
  principal: (id: string) => ['principal', id] as const,
  routerTerminal: (id: string) => ['router-terminal', id] as const,
  principalUsage: (id: string, range: string, step: string) =>
    ['principal-usage', id, range, step] as const,
  principalLimits: (id: string) => ['principal-limits', id] as const,
  principalKeys: (id: string) => ['principal-keys', id] as const,
  pluginRegistry: ['plugins', 'registry'] as const,
  pluginChain: (pid: string, slot?: ChainSlot) =>
    ['plugin-chain', pid, slot ?? 'all'] as const,
  pluginReferences: (id: string) => ['plugin-references', id] as const,
  events: (filters: Record<string, string | undefined>) =>
    ['events', filters] as const,
  audit: (filters: Record<string, string | undefined>) =>
    ['audit', filters] as const,
  upstreamOauthStatus: (id: string) => ['upstream-oauth-status', id] as const,
  upstreamSubscriptionMetadata: (id: string) =>
    ['upstream-subscription-metadata', id] as const,
  configCurrent: ['config', 'current'] as const,
  configSchema: ['config', 'schema'] as const,
  configDraft: ['config', 'draft'] as const,
  configHistory: ['config', 'history'] as const,
  configDiff: (from: number, to: number) =>
    ['config', 'diff', from, to] as const,
  subscriptionQuotaLatest: (params: Record<string, any>) =>
    ['subscription-quota', 'latest', params] as const,
  subscriptionQuotaSeries: (params: Record<string, any>) =>
    ['subscription-quota', 'series', params] as const,
  subscriptionQuotaAnalysis: (params: Record<string, any>) =>
    ['subscription-quota', 'analysis', params] as const,
  subscriptionQuotaAggregate: (params: Record<string, any>) =>
    ['subscription-quota', 'aggregate', params] as const,
  subscriptionQuotaPoolHistory: (params: Record<string, any>) =>
    ['subscription-quota', 'pool-history', params] as const,
  cacheKeepaliveSessions: (
    principalId: string,
    filters: CacheKeepaliveSessionsFilters,
  ) => ['cache-keepalive', 'sessions', principalId, filters] as const,
  cacheKeepaliveSummary: (principalId: string) =>
    ['cache-keepalive', 'summary', principalId] as const,
  cacheKeepaliveSessionDetail: (principalId: string, sessionId: string) =>
    ['cache-keepalive', 'detail', principalId, sessionId] as const,
};

async function invalidatePrincipalRecordQueries(
  client: QueryClient,
  id: string,
) {
  await Promise.all([
    client.invalidateQueries({ queryKey: qk.principal(id) }),
    client.invalidateQueries({ queryKey: qk.principals }),
    client.invalidateQueries({ queryKey: qk.routerTerminal(id) }),
  ]);
}

// ─────────────────────────────────────────────────────────────────────────────
// Read hooks

export function useHealth() {
  return useQuery({
    queryKey: qk.health,
    queryFn: () =>
      getJson<{
        status: string;
        version: string;
        git_sha: string;
        uptime_secs: number;
      }>('/admin/health'),
    refetchInterval: 15_000,
  });
}
export function useStatus() {
  return usePolledData(
    {
      queryKey: qk.status,
      queryFn: () => getJson<StatusResponse>('/admin/v1/status'),
    },
    POLLING_INTERVALS.STATUS_MS,
  );
}
export function useSummary(range: string) {
  return usePolledData(
    {
      queryKey: qk.summary(range),
      queryFn: () =>
        getJson<DashboardSummaryResponse>(
          `/admin/dashboard/summary?range=${encodeURIComponent(range)}`,
        ),
    },
    POLLING_INTERVALS.SUMMARY_MS,
  );
}
export function useUsage(
  range: string,
  step: string,
  group: 'none' | 'model' | 'principal' | 'upstream',
  upstreamId?: string,
  projection?: 'full' | 'totals',
) {
  return usePolledData(
    {
      queryKey: qk.usage(range, step, group, upstreamId, projection),
      queryFn: ({ signal }) => {
        const params = new URLSearchParams({
          range,
          step,
          group_by: group,
        });
        if (upstreamId) params.set('upstream_id', upstreamId);
        if (projection) params.set('projection', projection);
        return getJson<DashboardUsageResponse>(
          `/admin/usage?${params.toString()}`,
          { signal },
        );
      },
      placeholderData: (previousData, previousQuery) => {
        const previousKey = previousQuery?.queryKey;
        return previousKey?.[0] === 'usage' &&
          previousKey[3] === group &&
          previousKey[4] === (upstreamId ?? null) &&
          previousKey[5] === (projection ?? 'full')
          ? previousData
          : undefined;
      },
    },
    POLLING_INTERVALS.USAGE_MS,
  );
}
export function useUpstreams() {
  return usePolledData(
    {
      queryKey: qk.upstreams,
      queryFn: () => getJson<UpstreamListResp>('/admin/v1/upstreams'),
    },
    POLLING_INTERVALS.UPSTREAMS_MS,
  );
}
export function usePrincipals() {
  return useQuery({
    queryKey: qk.principals,
    queryFn: () => getJson<PrincipalListResp>('/admin/v1/principals'),
  });
}
export function usePrincipalLimits(id: string | null) {
  return useQuery({
    queryKey: qk.principalLimits(id ?? ''),
    queryFn: () =>
      getJson<PrincipalLimitsResponse>(`/admin/v1/principals/${id}/limits`),
    enabled: !!id,
  });
}
export function usePrincipalKeys(id: string | null) {
  return useQuery({
    queryKey: qk.principalKeys(id ?? ''),
    queryFn: () => getJson<KeyListResponse>(`/admin/v1/principals/${id}/keys`),
    enabled: !!id,
  });
}
export function usePluginRegistry() {
  return useQuery({
    queryKey: qk.pluginRegistry,
    queryFn: () => getJson<PluginListResp>('/admin/v1/plugins/registry'),
  });
}
export function usePluginChain(principalId: string | null, slot?: ChainSlot) {
  return useQuery({
    queryKey: qk.pluginChain(principalId ?? '', slot),
    queryFn: () => {
      const q = slot ? `?slot=${slot}` : '';
      return getJson<PluginChainResp>(
        `/admin/v1/principals/${principalId}/plugin-chain${q}`,
      );
    },
    enabled: !!principalId,
  });
}
export function useRouterTerminalStrategy(principalId: string | null) {
  return useQuery({
    queryKey: qk.routerTerminal(principalId ?? ''),
    queryFn: () =>
      getJson<{ strategy: string; revision: number }>(
        `/admin/v1/principals/${principalId}/router-terminal`,
      ),
    enabled: !!principalId,
  });
}
export function useRecentEvents(filters: Record<string, string | undefined>) {
  const params = new URLSearchParams();
  for (const [k, v] of Object.entries(filters)) if (v) params.set(k, v);
  return usePolledData(
    {
      queryKey: qk.events(filters),
      queryFn: ({ signal }) =>
        getJson<RecentEventsPayload>(
          `/admin/events/recent?${params.toString()}`,
          { signal },
        ),
      placeholderData: (previousData, previousQuery) =>
        previousQuery?.queryKey[0] === 'events' &&
        hasSameEventFilters(previousQuery.queryKey[1], filters)
          ? previousData
          : undefined,
    },
    POLLING_INTERVALS.RECENT_EVENTS_MS,
  );
}

export function useEventsHistogram(
  filters: Record<string, string | undefined>,
  range: EventsHistogramRange | null,
  options?: { readonly poll?: boolean },
) {
  return useQuery<EventsHistogramPayload>({
    queryKey: ['events', 'histogram', filters, range],
    queryFn: () => {
      if (range === null) {
        throw new Error('Histogram range is required');
      }
      return fetchEventsHistogram(filters, range);
    },
    enabled: range !== null,
    // The bucket-snapped range keeps the key stable, so without a poll the
    // trailing bucket would only grow when a bucket boundary rolls over.
    // Matches the request list interval so both advance together. A parked
    // historical view does not poll: those buckets cannot change.
    refetchInterval:
      options?.poll === false ? false : POLLING_INTERVALS.RECENT_EVENTS_MS,
    placeholderData: (previousData, previousQuery) =>
      previousQuery?.queryKey[0] === 'events' &&
      previousQuery.queryKey[1] === 'histogram' &&
      hasSameEventFilters(previousQuery.queryKey[2], filters)
        ? previousData
        : undefined,
  });
}

export function useRequestEventDetail(eventId: string | null) {
  return useQuery({
    queryKey: ['request-event-detail', eventId ?? ''],
    queryFn: () =>
      getJson<RequestEvent>(
        `/admin/v1/events/detail/${encodeURIComponent(eventId ?? '')}`,
      ),
    enabled: !!eventId,
    staleTime: Infinity,
  });
}

const RECENT_EVENTS_PAGE_SIZE = 200;

export type RecentEventsCursor = {
  readonly ts_ms: number;
  readonly event_id: string;
};

export type RecentEventsPageParam =
  | { readonly kind: 'initial'; readonly limit: number }
  | {
      readonly kind: 'cursor';
      readonly limit: number;
      readonly ts_ms: number;
      readonly event_id: string;
    };

type RecentEventsQueryKey = readonly [
  'events',
  Record<string, string | undefined>,
  'infinite',
];

type RecentEventsPageQueryKey = readonly [
  'events',
  Record<string, string | undefined>,
  'page',
  number,
  number | null,
  string | null,
];

export function getRecentEventsCursor(event: {
  readonly ts?: number | null;
  readonly ts_ms?: number | null;
  readonly event_id?: string;
  readonly request_id: string;
}): RecentEventsCursor | undefined {
  const ts_ms = event.ts_ms ?? (event.ts != null ? event.ts * 1000 : null);
  const event_id = event.event_id ?? event.request_id;
  if (ts_ms == null || !Number.isFinite(ts_ms) || !event_id) return undefined;
  return { ts_ms, event_id };
}

export function fetchRecentEventsPage(
  filters: Record<string, string | undefined>,
  pageParam: RecentEventsPageParam,
): Promise<RecentEventsPayload> {
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(filters)) {
    if (value) params.set(key, value);
  }
  if (pageParam.kind === 'cursor') {
    params.set('until_ts_ms', String(pageParam.ts_ms));
    params.set('until_event_id', pageParam.event_id);
  }
  params.set('limit', String(pageParam.limit));
  return getJson<RecentEventsPayload>(
    `/admin/events/recent?${params.toString()}`,
  );
}

export function recentEventsPageQueryOptions(
  filters: Record<string, string | undefined>,
  pageParam: RecentEventsPageParam,
) {
  const cursorTs = pageParam.kind === 'cursor' ? pageParam.ts_ms : null;
  const cursorEventId = pageParam.kind === 'cursor' ? pageParam.event_id : null;
  return queryOptions<
    RecentEventsPayload,
    Error,
    RecentEventsPayload,
    RecentEventsPageQueryKey
  >({
    queryKey: [
      ...qk.events(filters),
      'page',
      pageParam.limit,
      cursorTs,
      cursorEventId,
    ],
    queryFn: () => fetchRecentEventsPage(filters, pageParam),
  });
}

export function useRecentEventsPage(
  filters: Record<string, string | undefined>,
  pageParam: RecentEventsPageParam,
) {
  return useQuery(recentEventsPageQueryOptions(filters, pageParam));
}

export function useRecentEventsInfinite(
  filters: Record<string, string | undefined>,
) {
  const queryKey: RecentEventsQueryKey = [...qk.events(filters), 'infinite'];
  const initialPageParam: RecentEventsPageParam = {
    kind: 'initial',
    limit: RECENT_EVENTS_PAGE_SIZE,
  };
  return useInfiniteQuery<
    RecentEventsPayload,
    Error,
    InfiniteData<RecentEventsPayload, RecentEventsPageParam>,
    RecentEventsQueryKey,
    RecentEventsPageParam
  >({
    queryKey,
    initialPageParam,
    queryFn: ({ pageParam }) => fetchRecentEventsPage(filters, pageParam),
    getNextPageParam: (last) => {
      if (!last.events.length || last.events.length < last.limit) {
        return undefined;
      }
      const cursor = getRecentEventsCursor(last.events[last.events.length - 1]);
      if (cursor === undefined) return undefined;
      return {
        kind: 'cursor',
        limit: RECENT_EVENTS_PAGE_SIZE,
        ...cursor,
      };
    },
  });
}

import type { RequestEventWithPhase } from './RequestEventTypes';

function upsertLiveRequestEvent(
  acc: RequestEventWithPhase[],
  update: RequestEventUpdate,
  cap: number,
): RequestEventWithPhase[] {
  let newEvent: RequestEventWithPhase;
  let key: string | undefined;

  if (update.phase === 'final') {
    newEvent = { ...update.payload.event, _phase: 'final' };
    key = update.payload.event.event_id ?? update.payload.event.request_id;
  } else {
    newEvent = { ...update.payload, _phase: 'partial' };
    key = update.payload.event_id ?? update.payload.request_id;
  }

  const idx = acc.findIndex((e) => (e.event_id ?? e.request_id) === key);
  if (idx >= 0) {
    const out = acc.slice();
    out[idx] = newEvent;
    return out;
  }
  return [newEvent, ...acc].slice(0, cap);
}

export function useLiveRequestEvents(enabled: boolean, cap = 500) {
  return useQuery({
    queryKey: ['request-events', 'live', cap] as const,
    enabled,
    staleTime: Infinity,
    gcTime: Infinity,
    retry: false,
    queryFn: streamedQuery<RequestEventUpdate, RequestEventWithPhase[]>({
      streamFn: ({ signal }) => streamRequestEventUpdates(signal),
      initialValue: [],
      reducer: (acc, next) => upsertLiveRequestEvent(acc, next, cap),
      refetchMode: 'reset',
    }),
  });
}

export function usePrincipalNameMap(): Map<string, string> {
  const principals = usePrincipals();
  return useMemo(() => {
    const m = new Map<string, string>();
    for (const p of principals.data?.principals ?? []) m.set(p.id, p.name);
    return m;
  }, [principals.data]);
}
export function useUpstreamNameMap(): Map<string, string> {
  const upstreams = useUpstreams();
  return useMemo(() => {
    const m = new Map<string, string>();
    for (const u of upstreams.data?.upstreams ?? []) {
      m.set(u.id, u.name);
      m.set(u.name, u.name); // identity for when value already is the name
    }
    return m;
  }, [upstreams.data]);
}
export function useAudit(filters: Record<string, string | undefined>) {
  const params = new URLSearchParams();
  for (const [k, v] of Object.entries(filters)) if (v) params.set(k, v);
  return useQuery({
    queryKey: qk.audit(filters),
    queryFn: () =>
      getJson<AuditQueryResponse>(`/admin/audit?${params.toString()}`),
  });
}
export function useUpstreamOAuthStatus(id: string | null | undefined) {
  return useQuery({
    queryKey: qk.upstreamOauthStatus(id ?? ''),
    queryFn: ({ signal }) =>
      getJson<UpstreamOAuthStatusResponse>(
        `/admin/v1/upstreams/${id}/oauth/status`,
        { signal },
      ),
    placeholderData: (previousData, previousQuery) =>
      previousQuery?.queryKey[0] === 'upstream-oauth-status' &&
      previousQuery.queryKey[1] === (id ?? '')
        ? previousData
        : undefined,
    enabled: Boolean(id),
  });
}
export function useUpstreamSubscriptionMetadata(upstreamId: string) {
  return usePolledData(
    {
      queryKey: qk.upstreamSubscriptionMetadata(upstreamId),
      queryFn: ({ signal }) =>
        getJson<SubscriptionMetadataResponse>(
          `/admin/v1/upstreams/${upstreamId}/subscription-metadata`,
          { signal },
        ),
      placeholderData: (previousData, previousQuery) =>
        previousQuery?.queryKey[0] === 'upstream-subscription-metadata' &&
        previousQuery.queryKey[1] === upstreamId
          ? previousData
          : undefined,
      enabled: !!upstreamId,
    },
    POLLING_INTERVALS.UPSTREAM_SUB_META_MS,
  );
}
export function useTriggerSubscriptionMetadataRefresh() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (upstreamId: string) =>
      triggerSubscriptionMetadataRefresh(upstreamId),
    onSuccess: (data, upstreamId) => {
      qc.setQueryData(qk.upstreamSubscriptionMetadata(upstreamId), data);
      qc.invalidateQueries({
        queryKey: qk.upstreamSubscriptionMetadata(upstreamId),
      });
    },
  });
}
export function useConfigCurrent() {
  return useQuery({
    queryKey: qk.configCurrent,
    queryFn: () => getJson<Record<string, unknown>>('/admin/config/current'),
  });
}
export function useConfigSchema() {
  return useQuery({
    queryKey: qk.configSchema,
    queryFn: () => getJson<ConfigSchemaResponse>('/admin/config/schema'),
  });
}
export function useConfigDraft() {
  return useQuery({
    queryKey: qk.configDraft,
    queryFn: () => getJson<ConfigDraftResponse>('/admin/config/draft'),
  });
}
export function useConfigHistory() {
  return useQuery({
    queryKey: qk.configHistory,
    queryFn: () =>
      getJson<ConfigHistoryResponse>('/admin/config/history?limit=20'),
  });
}

export function useSubscriptionQuotaLatest(params: {
  upstreamIds?: string;
  windows?: string;
  source?: string;
  maxStalenessSecs?: number;
  refetchInterval?: number;
}) {
  const searchParams = new URLSearchParams();
  if (params.upstreamIds) searchParams.set('upstream_ids', params.upstreamIds);
  if (params.windows) searchParams.set('windows', params.windows);
  if (params.source) searchParams.set('source', params.source);
  if (params.maxStalenessSecs !== undefined)
    searchParams.set('max_staleness_secs', String(params.maxStalenessSecs));

  return usePolledData(
    {
      queryKey: qk.subscriptionQuotaLatest(params),
      queryFn: ({ signal }) =>
        getJson<LatestResponse>(
          `/admin/v1/subscription-quotas/latest?${searchParams.toString()}`,
          { signal },
        ),
      placeholderData: (previousData, previousQuery) =>
        hasSameQuotaIdentity(previousQuery?.queryKey, 'latest', params)
          ? previousData
          : undefined,
    },
    params.refetchInterval ?? POLLING_INTERVALS.QUOTA_LATEST_MS,
  );
}

export function useSubscriptionQuotaSeries(params: {
  upstreamIds?: string;
  windows?: string;
  source?: string;
  rangeSecs: number;
  bucketSecs?: number;
  maxPointsPerSeries?: number;
}) {
  return usePolledData(
    {
      queryKey: qk.subscriptionQuotaSeries(params),
      queryFn: ({ signal }) => {
        const untilUnixSecs = Math.floor(Date.now() / 1000);
        const searchParams = new URLSearchParams();
        if (params.upstreamIds)
          searchParams.set('upstream_ids', params.upstreamIds);
        if (params.windows) searchParams.set('windows', params.windows);
        if (params.source) searchParams.set('source', params.source);
        searchParams.set(
          'since_unix_secs',
          String(untilUnixSecs - params.rangeSecs),
        );
        searchParams.set('until_unix_secs', String(untilUnixSecs));
        if (params.bucketSecs !== undefined)
          searchParams.set('bucket_secs', String(params.bucketSecs));
        if (params.maxPointsPerSeries !== undefined)
          searchParams.set(
            'max_points_per_series',
            String(params.maxPointsPerSeries),
          );
        return getJson<SeriesResponse>(
          `/admin/v1/subscription-quotas/series?${searchParams.toString()}`,
          { signal },
        );
      },
      placeholderData: (previousData, previousQuery) =>
        hasSameQuotaIdentity(previousQuery?.queryKey, 'series', params)
          ? previousData
          : undefined,
    },
    POLLING_INTERVALS.QUOTA_SERIES_MS,
  );
}

export function useSubscriptionQuotaAnalysis(params: {
  upstreamIds?: string;
  windows?: string;
  source?: string;
  rangeSecs: number;
}) {
  return usePolledData(
    {
      queryKey: qk.subscriptionQuotaAnalysis(params),
      queryFn: ({ signal }) => {
        const untilUnixSecs = Math.floor(Date.now() / 1000);
        const searchParams = new URLSearchParams();
        if (params.upstreamIds)
          searchParams.set('upstream_ids', params.upstreamIds);
        if (params.windows) searchParams.set('windows', params.windows);
        if (params.source) searchParams.set('source', params.source);
        searchParams.set(
          'since_unix_secs',
          String(untilUnixSecs - params.rangeSecs),
        );
        searchParams.set('until_unix_secs', String(untilUnixSecs));
        return getJson<AnalysisResponse>(
          `/admin/v1/subscription-quotas/analysis?${searchParams.toString()}`,
          { signal },
        );
      },
      placeholderData: (previousData, previousQuery) =>
        hasSameQuotaIdentity(previousQuery?.queryKey, 'analysis', params)
          ? previousData
          : undefined,
    },
    POLLING_INTERVALS.QUOTA_ANALYSIS_MS,
  );
}

export function useSubscriptionQuotaAggregate(params: {
  upstreamIds?: string;
  windows?: string;
  source?: string;
  maxStalenessSecs?: number;
}) {
  const searchParams = new URLSearchParams();
  if (params.upstreamIds) searchParams.set('upstream_ids', params.upstreamIds);
  if (params.windows) searchParams.set('windows', params.windows);
  if (params.source) searchParams.set('source', params.source);
  if (params.maxStalenessSecs !== undefined)
    searchParams.set('max_staleness_secs', String(params.maxStalenessSecs));

  return usePolledData(
    {
      queryKey: qk.subscriptionQuotaAggregate(params),
      queryFn: ({ signal }) =>
        getJson<AggregateResponse>(
          `/admin/v1/subscription-quotas/aggregate?${searchParams.toString()}`,
          { signal },
        ),
      placeholderData: (previousData, previousQuery) =>
        hasSameQuotaIdentity(previousQuery?.queryKey, 'aggregate', params)
          ? previousData
          : undefined,
    },
    POLLING_INTERVALS.QUOTA_AGGREGATE_MS,
  );
}

type PoolHistoryQueryData = PoolHistoryResponse & {
  readonly range_secs: number;
};

export function useSubscriptionQuotaPoolHistory(params: {
  windows?: string;
  rangeSecs: number;
  windowQuantumSecs?: number;
  maxPointsPerSeries?: number;
}) {
  return usePolledData(
    {
      queryKey: qk.subscriptionQuotaPoolHistory(params),
      queryFn: async ({ signal }) => {
        const nowUnixSecs = Math.floor(Date.now() / 1000);
        const quantumSecs = params.windowQuantumSecs ?? 0;
        const untilUnixSecs =
          quantumSecs > 0
            ? Math.ceil(nowUnixSecs / quantumSecs) * quantumSecs
            : nowUnixSecs;
        const searchParams = new URLSearchParams();
        searchParams.set('series_projection', 'chart');
        if (params.windows) searchParams.set('windows', params.windows);
        searchParams.set(
          'since_unix_secs',
          String(untilUnixSecs - params.rangeSecs - quantumSecs),
        );
        searchParams.set('until_unix_secs', String(untilUnixSecs));
        if (params.maxPointsPerSeries !== undefined)
          searchParams.set(
            'max_points_per_series',
            String(params.maxPointsPerSeries),
          );
        const response = await getJson<PoolHistoryResponse>(
          `/admin/v1/subscription-quotas/pool-history?${searchParams.toString()}`,
          { signal },
        );
        return {
          ...response,
          range_secs: params.rangeSecs,
        } satisfies PoolHistoryQueryData;
      },
      placeholderData: (previousData, previousQuery) =>
        hasSameQuotaIdentity(previousQuery?.queryKey, 'pool-history', params)
          ? previousData
          : undefined,
    },
    POLLING_INTERVALS.QUOTA_POOL_HISTORY_MS,
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Mutations

export interface CreateUpstreamRequest {
  name: string;
  kind: string;
  base_url?: string | null;
  api_key_env?: string | null;
  api_key_value?: string | null;
}
export function useCreateUpstream() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: CreateUpstreamRequest) =>
      postJson<Upstream, CreateUpstreamRequest>('/admin/v1/upstreams', body),
    onSuccess: () => qc.invalidateQueries({ queryKey: qk.upstreams }),
  });
}
export function useDeleteUpstream() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({
      id,
      spec_revision,
    }: {
      id: string;
      spec_revision: number;
    }) => deleteJson(`/admin/v1/upstreams/${id}`, { ifMatch: spec_revision }),
    onSuccess: () => qc.invalidateQueries({ queryKey: qk.upstreams }),
  });
}
export interface UpdateUpstreamRequest {
  name?: string | null;
  base_url?: string | null;
  api_key_env?: string | null;
  api_key_value?: string | null;
}

export interface UpdateUpstreamWarmupSettingsRequest {
  enabled?: boolean;
  warmup_enabled?: boolean;
  warmup_dialect_plugin?: UpstreamWarmupDialectPlugin | null;
}

export type FireNowErrorReason =
  | WarmupSkipReason
  | WarmupTransientFailureReason
  | WarmupPermanentFailureReason;

export type FireNowResponse =
  | { fired: true; cycle_key: number }
  | { fired: false; reason: 'lease_held'; held_by: string }
  | { fired: false; reason: FireNowErrorReason };

export function useUpdateUpstream() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({
      id,
      body,
      spec_revision,
    }: {
      id: string;
      body: UpdateUpstreamRequest;
      spec_revision: number;
    }) =>
      putJson<Upstream, UpdateUpstreamRequest>(
        `/admin/v1/upstreams/${id}`,
        body,
        { ifMatch: spec_revision },
      ),
    onSuccess: (_d, vars) => {
      qc.invalidateQueries({ queryKey: qk.upstreams });
      qc.invalidateQueries({ queryKey: qk.upstream(vars.id) });
    },
  });
}
export function useFireNowUpstreamWarmup() {
  const qc = useQueryClient();
  return useMutation({
    mutationKey: ['upstreams', 'fire-now'],
    mutationFn: async (id: string): Promise<FireNowResponse> => {
      try {
        const res = await fetchWithAuth(
          `/admin/v1/upstreams/${id}/warmup/fire-now`,
          { method: 'POST' },
        );
        return (await res.json()) as FireNowResponse;
      } catch (error) {
        if (
          error instanceof ApiError &&
          (error.status === 400 ||
            error.status === 502 ||
            error.status === 503) &&
          error.body &&
          typeof error.body === 'object' &&
          (error.body as { fired?: unknown }).fired === false
        ) {
          return error.body as FireNowResponse;
        }
        throw error;
      }
    },
    onSuccess: async (_response, id) => {
      await qc.invalidateQueries({ queryKey: qk.upstream(id) });
      await qc.invalidateQueries({ queryKey: qk.upstreams });
    },
  });
}
export function useClearUpstreamWarmupDialectPlugin() {
  const qc = useQueryClient();
  return useMutation({
    mutationKey: ['upstreams', 'clear-warmup-dialect-plugin'],
    mutationFn: ({
      id,
      spec_revision,
    }: {
      id: string;
      spec_revision: number;
    }) =>
      deleteJson<Upstream>(`/admin/v1/upstreams/${id}/warmup-dialect-plugin`, {
        ifMatch: spec_revision,
      }),
    onSuccess: async (serverResponse, { id }) => {
      qc.setQueryData(qk.upstream(id), serverResponse);
      await qc.invalidateQueries({ queryKey: qk.upstream(id) });
      await qc.invalidateQueries({ queryKey: qk.upstreams });
    },
  });
}
export function useUpdateUpstreamWarmupSettings() {
  const qc = useQueryClient();
  return useMutation({
    mutationKey: ['upstreams', 'update-warmup-settings'],
    mutationFn: ({
      id,
      body,
      spec_revision,
    }: {
      id: string;
      body: UpdateUpstreamWarmupSettingsRequest;
      spec_revision: number;
    }) =>
      patchJson<Upstream, UpdateUpstreamWarmupSettingsRequest>(
        `/admin/v1/upstreams/${id}`,
        body,
        { ifMatch: spec_revision },
      ),
    onSuccess: async (serverResponse, { id }) => {
      qc.setQueryData(qk.upstream(id), serverResponse);
      await qc.invalidateQueries({ queryKey: qk.upstream(id) });
      await qc.invalidateQueries({ queryKey: qk.upstreams });
    },
  });
}
export function useOAuthStart() {
  return useMutation({
    mutationFn: (id: string) =>
      postJson<
        { authorize_url: string; state_token: string; revision: number },
        Record<string, never>
      >(`/admin/v1/upstreams/${id}/oauth/start`, {}),
  });
}
export function useOAuthComplete() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({
      id,
      state_token,
      code,
    }: {
      id: string;
      state_token: string;
      code: string;
    }) =>
      postJson<
        {
          upstream_id: string;
          expires_at_unix_secs: number;
          access_token_fingerprint: string;
        },
        { state_token: string; code: string }
      >(`/admin/v1/upstreams/${id}/oauth/complete`, { state_token, code }),
    // Only complete() success means the backend's OAuth credential changed.
    // useOAuthStart() intentionally invalidates nothing: clicking "Reconnect"
    // and then bailing out of the modal must NOT flip the OAuth Status card.
    onSuccess: async (_data, { id }) => {
      await Promise.all([
        qc.invalidateQueries({ queryKey: qk.upstreams }),
        qc.invalidateQueries({ queryKey: qk.status }),
        qc.invalidateQueries({ queryKey: qk.upstreamOauthStatus(id) }),
        qc.invalidateQueries({
          queryKey: qk.upstreamSubscriptionMetadata(id),
        }),
      ]);
    },
    onError: (error) => {
      const message =
        error instanceof ApiError
          ? error.message || `Request failed (${error.status})`
          : error instanceof Error
            ? error.message
            : String(error);
      toast.error(`OAuth verification failed: ${message}`);
    },
  });
}
export function useCreatePrincipal() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: {
      name: string;
      kind: string;
      allowed_models?: string[];
      allowed_upstreams?: string[];
      default_limits?: PrincipalDefaultLimit[];
    }) => postJson<Principal, typeof body>('/admin/v1/principals', body),
    onSuccess: () => qc.invalidateQueries({ queryKey: qk.principals }),
  });
}
export function usePrincipalWritePending(id: string) {
  return useIsMutating({
    mutationKey: PRINCIPAL_WRITE_MUTATION_KEY,
    predicate: (mutation) => {
      const variables = mutation.state.variables;
      if (variables === null || typeof variables !== 'object') return false;
      if ('id' in variables && variables.id === id) return true;
      return 'principalId' in variables && variables.principalId === id;
    },
  });
}

export function useDeletePrincipal() {
  const qc = useQueryClient();
  return useMutation({
    mutationKey: PRINCIPAL_WRITE_MUTATION_KEY,
    mutationFn: ({ id, revision }: { id: string; revision: number }) =>
      deleteJson(`/admin/v1/principals/${id}`, { ifMatch: revision }),
    onSuccess: (_d, vars) => {
      qc.invalidateQueries({ queryKey: qk.principals });
      qc.invalidateQueries({ queryKey: qk.status });
      qc.invalidateQueries({ queryKey: ['plugin-chain', vars.id] });
    },
  });
}
export function useTogglePrincipal() {
  const qc = useQueryClient();
  return useMutation({
    mutationKey: PRINCIPAL_WRITE_MUTATION_KEY,
    mutationFn: ({
      id,
      enabled,
      revision,
    }: {
      id: string;
      enabled: boolean;
      revision: number;
    }) =>
      postJson<Principal, Record<string, never>>(
        `/admin/v1/principals/${id}/${enabled ? 'enable' : 'disable'}`,
        {},
        { ifMatch: revision },
      ),
    onSuccess: async (_data, vars) => {
      await invalidatePrincipalRecordQueries(qc, vars.id);
    },
  });
}
export function useSetAllowedModels() {
  const qc = useQueryClient();
  return useMutation({
    mutationKey: PRINCIPAL_WRITE_MUTATION_KEY,
    mutationFn: ({
      id,
      models,
      expected_revision,
    }: {
      id: string;
      models: string[];
      expected_revision: number;
    }) =>
      putJson<Principal, { models: string[]; expected_revision: number }>(
        `/admin/v1/principals/${id}/allowed_models`,
        { models, expected_revision },
      ),
    onSuccess: async (_data, vars) => {
      await invalidatePrincipalRecordQueries(qc, vars.id);
    },
  });
}
export function useUpdatePrincipalDefaultLimits() {
  const qc = useQueryClient();
  return useMutation({
    mutationKey: PRINCIPAL_WRITE_MUTATION_KEY,
    mutationFn: ({
      id,
      default_limits,
      expected_revision,
    }: {
      id: string;
      default_limits: PrincipalDefaultLimit[];
      expected_revision: number;
    }) =>
      patchJson<Principal, { default_limits: PrincipalDefaultLimit[] }>(
        `/admin/v1/principals/${id}`,
        { default_limits },
        { ifMatch: expected_revision },
      ),
    onSuccess: async (_data, vars) => {
      await invalidatePrincipalRecordQueries(qc, vars.id);
    },
  });
}

export function useUpdatePrincipalCacheKeepalive() {
  const qc = useQueryClient();
  return useMutation({
    mutationKey: PRINCIPAL_WRITE_MUTATION_KEY,
    mutationFn: ({
      id,
      cache_keepalive,
      expected_revision,
    }: {
      id: string;
      cache_keepalive: Principal['cache_keepalive'];
      expected_revision: number;
    }) =>
      patchJson<Principal, { cache_keepalive: Principal['cache_keepalive'] }>(
        `/admin/v1/principals/${id}`,
        { cache_keepalive },
        { ifMatch: expected_revision },
      ),
    onSuccess: async (_data, vars) => {
      await invalidatePrincipalRecordQueries(qc, vars.id);
    },
  });
}

export function useIssueKey() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ id, label }: { id: string; label?: string }) =>
      postJson<{ key_id: string; plaintext_key: string }, { label?: string }>(
        `/admin/v1/principals/${id}/keys`,
        { label },
      ),
    onSuccess: (_d, vars) =>
      qc.invalidateQueries({ queryKey: qk.principalKeys(vars.id) }),
  });
}
export function useRevokeKey() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ id, key_id }: { id: string; key_id: string }) =>
      postJson(`/admin/v1/principals/${id}/keys/${key_id}/revoke`, {}),
    onSuccess: (_d, vars) =>
      qc.invalidateQueries({ queryKey: qk.principalKeys(vars.id) }),
  });
}
export interface UploadWasmResponse {
  id: string;
  sha256_hex: string;
  size_bytes: number;
  original_filename: string;
  revision: number;
  idempotent: boolean;
  version?: string;
  action: 'created' | 'noop' | 'replaced';
}
export function useUploadWasm() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async ({
      file,
      confirmReplacement,
      replaceRegistryId,
      expectedRevision,
    }: {
      file: File;
      confirmReplacement?: boolean;
      replaceRegistryId?: string;
      expectedRevision?: number;
    }): Promise<UploadWasmResponse> => {
      const form = new FormData();
      // Do NOT set Content-Type: the browser must inject the multipart boundary.
      // Do NOT send `name`: backend uses it as a strict equality guard against the
      // embedded metadata name, which a filename stem almost never matches (400).
      form.append('original_filename', file.name);
      form.append('bytes', file);

      if (confirmReplacement) form.append('confirm_replacement', 'true');
      if (replaceRegistryId)
        form.append('replace_registry_id', replaceRegistryId);
      if (expectedRevision !== undefined)
        form.append('expected_revision', String(expectedRevision));

      const res = await fetchWithAuth('/admin/v1/plugins/wasm', {
        method: 'POST',
        body: form,
      });
      return res.json() as Promise<UploadWasmResponse>;
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: qk.pluginRegistry }),
  });
}
export interface PluginReference {
  kind: 'plugin_chain' | 'upstream_warmup_dialect';
  chain_entry_id?: string;
  principal_id?: string;
  principal_name?: string;
  slot?: ChainSlot;
  revision: number;
  upstream_id?: string;
  upstream_name?: string;
}

export interface PluginReferencesResponse {
  registry: PluginEntry;
  refcount: number;
  reference_fingerprint: string;
  references: PluginReference[];
}

export function usePluginReferences(id: string | null) {
  return useQuery({
    queryKey: qk.pluginReferences(id ?? ''),
    queryFn: () =>
      getJson<PluginReferencesResponse>(
        `/admin/v1/plugins/registry/${id}/references`,
      ),
    enabled: !!id,
    gcTime: 0,
  });
}

export function useDeletePlugin() {
  const qc = useQueryClient();
  return useMutation({
    onMutate: async (vars) => {
      await qc.cancelQueries({
        queryKey: qk.pluginReferences(vars.id),
        exact: true,
      });
    },
    mutationFn: ({
      id,
      revision,
      cascade,
      referenceFingerprint,
    }: {
      id: string;
      revision: number;
      cascade?: boolean;
      referenceFingerprint?: string;
    }) => {
      const headers: Record<string, string> = {};
      if (referenceFingerprint) {
        headers['X-Reference-Fingerprint'] = referenceFingerprint;
      }
      const qs = cascade ? '?cascade=references' : '';
      return deleteJson(`/admin/v1/plugins/registry/${id}${qs}`, {
        ifMatch: revision,
        headers,
      });
    },
    onSuccess: async (_data, vars) => {
      const invalidations = [
        qc.invalidateQueries({ queryKey: qk.pluginRegistry }),
      ];
      if (vars.cascade) {
        invalidations.push(
          qc.invalidateQueries({ queryKey: ['plugin-chain'] }),
          qc.invalidateQueries({ queryKey: qk.upstreams }),
        );
      }
      await Promise.all(invalidations);
      qc.removeQueries({
        queryKey: qk.pluginReferences(vars.id),
        exact: true,
        type: 'inactive',
      });
    },
  });
}
export function usePatchPlugin() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({
      id,
      label,
      revision,
    }: {
      id: string;
      label: string | null;
      revision: number;
    }) =>
      patchJson<PluginEntry, { label: string | null }>(
        `/admin/v1/plugins/registry/${id}`,
        { label },
        { ifMatch: revision },
      ),
    onSuccess: () => qc.invalidateQueries({ queryKey: qk.pluginRegistry }),
  });
}
export function useGcPlugins() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: () =>
      postJson<{ removed: string[]; count: number }, Record<string, never>>(
        '/admin/v1/plugins/wasm/gc',
        {},
      ),
    onSuccess: () => qc.invalidateQueries({ queryKey: qk.pluginRegistry }),
  });
}
export function useInsertChainEntry() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({
      pid,
      body,
    }: {
      pid: string;
      body: {
        slot: ChainSlot;
        wasm_registry_id: string;
        order?: number;
        config?: unknown;
        sse_per_event?: boolean;
        batched_events_per_flush?: number;
        batched_flush_ms?: number;
      };
    }) =>
      postJson<PluginChainEntry, typeof body>(
        `/admin/v1/principals/${pid}/plugin-chain`,
        body,
      ),
    onSuccess: (_d, vars) =>
      qc.invalidateQueries({ queryKey: ['plugin-chain', vars.pid] }),
  });
}
export function useReorderChain() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({
      pid,
      entries,
    }: {
      pid: string;
      entries: { id: string; order: number; expected_revision: number }[];
    }) =>
      postJson(`/admin/v1/principals/${pid}/plugin-chain/reorder`, { entries }),
    onSuccess: (_d, vars) =>
      qc.invalidateQueries({ queryKey: ['plugin-chain', vars.pid] }),
  });
}
export function useDeleteChainEntry() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ id, revision }: { id: string; revision: number }) =>
      deleteJson(`/admin/v1/plugin-chain-entries/${id}`, { ifMatch: revision }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['plugin-chain'] }),
  });
}
export function useUpdateRouterTerminalStrategy() {
  const qc = useQueryClient();
  return useMutation({
    mutationKey: PRINCIPAL_WRITE_MUTATION_KEY,
    mutationFn: ({
      id,
      strategy,
      revision,
    }: {
      id: string;
      strategy: string;
      revision: number;
    }) =>
      putJson<{ strategy: string; revision: number }, { strategy: string }>(
        `/admin/v1/principals/${id}/router-terminal`,
        { strategy },
        { ifMatch: revision },
      ),
    onSuccess: async (_data, vars) => {
      await invalidatePrincipalRecordQueries(qc, vars.id);
    },
  });
}
export function useApplyConfig() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (expected_revision: number) =>
      postJson<
        { applied_revision: number; applied_at_unix_secs: number },
        { expected_revision: number }
      >('/admin/config/apply', { expected_revision }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: qk.configDraft });
      qc.invalidateQueries({ queryKey: qk.configHistory });
      qc.invalidateQueries({ queryKey: qk.configCurrent });
    },
  });
}
export function useValidateConfig() {
  return useMutation({
    mutationFn: (expected_revision: number) =>
      postJson<
        { valid: boolean; revision: number; error?: string },
        { expected_revision: number }
      >('/admin/config/draft/validate', { expected_revision }),
  });
}
export function useSaveDraft() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({
      draft,
      expected_revision,
    }: {
      draft: Record<string, unknown>;
      expected_revision: number;
    }) =>
      putJson<
        { revision: number; saved_at_unix_secs: number },
        { draft: Record<string, unknown>; expected_revision: number }
      >('/admin/config/draft', { draft, expected_revision }),
    onSuccess: () => qc.invalidateQueries({ queryKey: qk.configDraft }),
  });
}
export function useReloadConfig() {
  return useMutation({
    mutationFn: () =>
      postJson<{ status: string; reloading: boolean }, Record<string, never>>(
        '/admin/config/reload',
        {},
      ),
  });
}

export function useStartOauthDraft() {
  return useMutation({
    mutationFn: () => startOauthDraft(),
    meta: { inlineError: true },
  });
}

export function useCompleteOauthDraft() {
  return useMutation({
    mutationFn: (body: { state_token: string; code: string }) =>
      completeOauthDraft(body),
    meta: { inlineError: true },
  });
}

export function useCreateFromOauthDraft() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: {
      state_token: string;
      name: string;
      base_url?: string | null;
    }) => createUpstreamFromOauthDraft(body),
    onSuccess: () => qc.invalidateQueries({ queryKey: qk.upstreams }),
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// Warmup attempts history + summary

export type WarmupAttemptStatus =
  | 'success'
  | 'skipped'
  | 'transient_failure'
  | 'permanent_failure';

export type WarmupSuccessReason = 'cycle_advanced' | 'window_already_active';
export type WarmupSkipReason =
  | 'seven_day_quota_exhausted'
  | 'upstream_disabled'
  | 'upstream_deleted';
export type WarmupTransientFailureReason =
  | 'rate_limited_cycle_key_missing'
  | 'upstream_5xx'
  | 'network_error'
  | 'request_timeout'
  | 'dialect_plugin_transient';
export type WarmupPermanentFailureReason =
  | 'oauth_credentials_missing'
  | 'request_build_failed'
  | 'oauth_refresh_failed'
  | 'credential_decrypt_failed'
  | 'auth_failed'
  | 'forbidden'
  | 'bad_request'
  | 'not_found'
  | 'dialect_plugin_failed';

export type WarmupAttemptOutcome =
  | { status: 'success'; reason: WarmupSuccessReason }
  | { status: 'skipped'; reason: WarmupSkipReason }
  | { status: 'transient_failure'; reason: WarmupTransientFailureReason }
  | { status: 'permanent_failure'; reason: WarmupPermanentFailureReason };

export type WarmupDispatchKind = 'not_dispatched' | 'http' | 'dialect_plugin';

export function attemptOutcome(a: WarmupAttempt): WarmupAttemptOutcome {
  return { status: a.status, reason: a.reason } as WarmupAttemptOutcome;
}

export type WarmupTrigger = 'scheduled' | 'manual';

export interface WarmupDialectPluginSnapshot {
  wasm_registry_id: string;
  wire_version?: number;
  config: Record<string, unknown>;
}

export interface WarmupAttempt {
  id: string;
  upstream_id: string;
  attempted_at_unix_secs: number;
  completed_at_unix_secs: number | null;
  scheduled_for_unix_secs: number;
  trigger: WarmupTrigger;
  status: WarmupAttemptStatus;
  reason:
    | WarmupSuccessReason
    | WarmupSkipReason
    | WarmupTransientFailureReason
    | WarmupPermanentFailureReason;
  dispatch_kind: WarmupDispatchKind | null;
  http_status: number | null;
  cycle_key: number | null;
  expected_cycle_key: number | null;
  idle_secs_since_prev_window: number | null;
  replica_id: string | null;
  lease_holder: string | null;
  upstream_spec_revision: number;
  dialect_plugin_snapshot: WarmupDialectPluginSnapshot | null;
  error_detail: string | null;
}

export interface WarmupRecentSummary {
  success: number;
  skipped: number;
  transient_failure: number;
  permanent_failure: number;
}

export interface WarmupSummary {
  upstream_id: string;
  last_attempt: WarmupAttempt | null;
  recent_attempts: WarmupAttempt[];
  next_scheduled_at_unix_secs: number | null;
  recent_summary_7d: WarmupRecentSummary;
  dialect_plugin: WarmupDialectPluginSnapshot | null;
}

export interface WarmupHistoryPage {
  attempts: WarmupAttempt[];
  next_cursor: string | null;
}

export const warmupKeys = {
  summary: (upstreamId: string) => ['warmup', 'summary', upstreamId] as const,
  attempts: (
    upstreamId: string,
    filters: { status?: WarmupAttemptStatus | null; limit?: number },
  ) => ['warmup', 'attempts', upstreamId, filters] as const,
};

export function useWarmupSummary(upstreamId: string) {
  return usePolledData(
    {
      queryKey: warmupKeys.summary(upstreamId),
      queryFn: () =>
        getJson<WarmupSummary>(`/admin/v1/upstreams/${upstreamId}/warmup`),
      enabled: Boolean(upstreamId),
      staleTime: 15_000,
    },
    POLLING_INTERVALS.WARMUP_SUMMARY_MS,
  );
}

export function useWarmupAttempts(
  upstreamId: string,
  filters: { status?: WarmupAttemptStatus | null; limit?: number } = {},
  enabled = true,
) {
  return useInfiniteQuery({
    queryKey: warmupKeys.attempts(upstreamId, filters),
    enabled: enabled && Boolean(upstreamId),
    initialPageParam: undefined as string | undefined,
    queryFn: async ({ pageParam }) => {
      const params = new URLSearchParams();
      if (filters.status) params.set('status', filters.status);
      if (filters.limit) params.set('limit', String(filters.limit));
      if (pageParam) params.set('before', pageParam);
      const qs = params.toString();
      return getJson<WarmupHistoryPage>(
        `/admin/v1/upstreams/${upstreamId}/warmup/attempts${qs ? `?${qs}` : ''}`,
      );
    },
    getNextPageParam: (last) => last.next_cursor ?? undefined,
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// Cache keepalive sessions (Principal detail card + drawers)

export function useCacheKeepaliveSessions(
  principalId: string,
  filters: CacheKeepaliveSessionsFilters = {},
  enabled = true,
) {
  const visibility = useVisibility();
  const isHidden = visibility.gracePeriodElapsed || !visibility.visible;

  return useInfiniteQuery({
    queryKey: qk.cacheKeepaliveSessions(principalId, filters),
    enabled: enabled && Boolean(principalId),
    initialPageParam: undefined as string | undefined,
    queryFn: ({ pageParam }) =>
      getCacheKeepaliveSessions(principalId, { ...filters, cursor: pageParam }),
    getNextPageParam: (last) => last.next_cursor ?? undefined,
    refetchInterval:
      enabled && !isHidden
        ? POLLING_INTERVALS.CACHE_KEEPALIVE_SESSIONS_MS
        : false,
  });
}

export function useCacheKeepaliveSummary(principalId: string) {
  return usePolledData(
    {
      queryKey: qk.cacheKeepaliveSummary(principalId),
      enabled: Boolean(principalId),
      queryFn: () => getCacheKeepaliveSessions(principalId, { limit: 0 }),
      select: (response) => response.summary,
    },
    POLLING_INTERVALS.CACHE_KEEPALIVE_SUMMARY_MS,
  );
}

export function useCacheKeepaliveSessionDetail(
  principalId: string,
  sessionId: string | null,
) {
  return usePolledData(
    {
      queryKey: qk.cacheKeepaliveSessionDetail(principalId, sessionId ?? ''),
      enabled: Boolean(principalId) && Boolean(sessionId),
      queryFn: () =>
        getCacheKeepaliveSessionDetail(principalId, sessionId ?? ''),
    },
    POLLING_INTERVALS.CACHE_KEEPALIVE_DETAIL_MS,
  );
}
