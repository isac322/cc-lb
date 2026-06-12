// biome-ignore-all lint/suspicious/noExplicitAny: existing API types use any for record params
// TanStack Query hooks for every admin v1 endpoint surfaced by the dashboard.
// Source-of-truth: .omo/plans/cc-lb-dashboard-overhaul.md (API SURFACE section).

import {
  useInfiniteQuery,
  useMutation,
  useQuery,
  useQueryClient,
} from '@tanstack/react-query';
import { useMemo } from 'react';
import { toast } from 'sonner';
import {
  type AnalysisResponse,
  ApiError,
  type AuditQueryResponse,
  type ConfigDraftResponse,
  type ConfigHistoryResponse,
  type ConfigSchemaResponse,
  type CredentialsResponse,
  completeOauthDraft,
  createUpstreamFromOauthDraft,
  type DashboardSummaryResponse,
  type DashboardUsageResponse,
  deleteJson,
  fetchWithAuth,
  getJson,
  type KeyListResponse,
  type LatestResponse,
  type OAuthStatusResponse,
  type PluginsStatusResponse,
  type PrincipalLimitsResponse,
  patchJson,
  postJson,
  putJson,
  type RecentEventsPayload,
  type SeriesResponse,
  type SubscriptionMetadataResponse,
  startOauthDraft,
  triggerSubscriptionMetadataRefresh,
  type UpstreamOAuthStatusResponse,
} from './api';

// ─────────────────────────────────────────────────────────────────────────────
// Entity shapes the new UI uses. (These mirror what mock-server.ts returns
// and what the old hooks/* declared piecemeal.)

export interface Upstream {
  id: string;
  name: string;
  kind: 'anthropic_api_key' | 'anthropic_oauth';
  enabled: boolean;
  revision: number;
  base_url?: string | null;
  api_key_env?: string | null;
}
interface UpstreamListResp {
  upstreams: Upstream[];
}

interface PrincipalDefaultLimit {
  model: string;
  rpm: number;
  tpm: number;
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

export interface PluginEntry {
  id: string;
  sha256_hex: string;
  name: string;
  original_filename: string;
  label: string | null;
  size_bytes: number;
  refcount: number;
  revision: number;
  uploaded_at_unix_secs: number;
  is_builtin?: boolean;
  kind?: string;
  wire_version?: number;
  metadata: PluginMetadata | null;
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
  principals: {
    id: string;
    name: string;
    enabled: boolean;
    last_apply_error: string | null;
  }[];
  plugin_chain_summary: {
    principal_count_with_chain: number;
    total_entries: number;
  };
  killswitch: boolean;
  last_reload_status: {
    ok: boolean;
    applied_revision: number;
    applied_at_unix_secs: number;
  } | null;
  restart_required_changes: {
    field: string;
    current: string;
    new: string;
    reason: string;
  }[];
}

// ─────────────────────────────────────────────────────────────────────────────
// Query keys

export const qk = {
  health: ['health'] as const,
  status: ['status'] as const,
  summary: (range: string) => ['summary', range] as const,
  usage: (range: string, step: string, group: string, upstreamId?: string) =>
    ['usage', range, step, group, upstreamId ?? null] as const,
  upstreams: ['upstreams'] as const,
  upstream: (id: string) => ['upstream', id] as const,
  principals: ['principals'] as const,
  principal: (id: string) => ['principal', id] as const,
  principalUsage: (id: string, range: string, step: string) =>
    ['principal-usage', id, range, step] as const,
  principalLimits: (id: string) => ['principal-limits', id] as const,
  principalKeys: (id: string) => ['principal-keys', id] as const,
  pluginRegistry: ['plugins', 'registry'] as const,
  pluginChain: (pid: string, slot?: ChainSlot) =>
    ['plugin-chain', pid, slot ?? 'all'] as const,
  events: (filters: Record<string, string | undefined>) =>
    ['events', filters] as const,
  audit: (filters: Record<string, string | undefined>) =>
    ['audit', filters] as const,
  credentials: ['credentials'] as const,
  oauthStatus: ['oauth-status'] as const,
  upstreamOauthStatus: (id: string) => ['upstream-oauth-status', id] as const,
  upstreamSubscriptionMetadata: (id: string) =>
    ['upstream-subscription-metadata', id] as const,
  pluginStatus: ['plugins', 'status'] as const,
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
};

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
  return useQuery({
    queryKey: qk.status,
    queryFn: () => getJson<StatusResponse>('/admin/v1/status'),
    refetchInterval: 15_000,
  });
}
export function useSummary(range: string) {
  return useQuery({
    queryKey: qk.summary(range),
    queryFn: () =>
      getJson<DashboardSummaryResponse>(
        `/admin/dashboard/summary?range=${encodeURIComponent(range)}`,
      ),
    refetchInterval: 30_000,
  });
}
export function useUsage(
  range: string,
  step: string,
  group: 'none' | 'model' | 'principal' | 'upstream',
  upstreamId?: string,
) {
  return useQuery({
    queryKey: qk.usage(range, step, group, upstreamId),
    queryFn: () => {
      const params = new URLSearchParams({
        range,
        step,
        group_by: group,
      });
      if (upstreamId) params.set('upstream_id', upstreamId);
      return getJson<DashboardUsageResponse>(
        `/admin/usage?${params.toString()}`,
      );
    },
    refetchInterval: 30_000,
  });
}
export function useUpstreams() {
  return useQuery({
    queryKey: qk.upstreams,
    queryFn: () => getJson<UpstreamListResp>('/admin/v1/upstreams'),
  });
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
    queryKey: ['router-terminal', principalId ?? ''],
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
  return useQuery({
    queryKey: qk.events(filters),
    queryFn: () =>
      getJson<RecentEventsPayload>(`/admin/events/recent?${params.toString()}`),
    refetchInterval: 10_000,
  });
}
export function useRecentEventsInfinite(
  filters: Record<string, string | undefined>,
) {
  return useInfiniteQuery({
    queryKey: [...qk.events(filters), 'infinite'],
    initialPageParam: undefined as number | undefined,
    queryFn: async ({ pageParam }) => {
      const params = new URLSearchParams();
      for (const [k, v] of Object.entries(filters)) if (v) params.set(k, v);
      if (pageParam != null) params.set('until_unix_secs', String(pageParam));
      params.set('limit', '200');
      return getJson<RecentEventsPayload>(
        `/admin/events/recent?${params.toString()}`,
      );
    },
    getNextPageParam: (last) => {
      const evs = last.events;
      if (!evs.length || evs.length < (last.limit ?? 200)) return undefined;
      const oldest = evs[evs.length - 1];
      const ts =
        oldest.ts ??
        (oldest.ts_ms != null ? Math.floor(oldest.ts_ms / 1000) : null);
      return ts != null ? ts - 1 : undefined;
    },
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
export function useCredentials() {
  return useQuery({
    queryKey: qk.credentials,
    queryFn: () => getJson<CredentialsResponse>('/admin/credentials'),
  });
}
export function useOAuthStatus() {
  return useQuery({
    queryKey: qk.oauthStatus,
    queryFn: () => getJson<OAuthStatusResponse>('/admin/oauth/status'),
  });
}
export function useUpstreamOAuthStatus(id: string | null | undefined) {
  return useQuery({
    queryKey: qk.upstreamOauthStatus(id ?? ''),
    queryFn: () =>
      getJson<UpstreamOAuthStatusResponse>(
        `/admin/v1/upstreams/${id}/oauth/status`,
      ),
    enabled: Boolean(id),
  });
}
export function useUpstreamSubscriptionMetadata(upstreamId: string) {
  return useQuery({
    queryKey: qk.upstreamSubscriptionMetadata(upstreamId),
    queryFn: () =>
      getJson<SubscriptionMetadataResponse>(
        `/admin/v1/upstreams/${upstreamId}/subscription-metadata`,
      ),
    enabled: !!upstreamId,
    refetchInterval: 30_000,
  });
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
export function usePluginStatus() {
  return useQuery({
    queryKey: qk.pluginStatus,
    queryFn: () => getJson<PluginsStatusResponse>('/admin/status'),
    refetchInterval: 15_000,
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
}) {
  const searchParams = new URLSearchParams();
  if (params.upstreamIds) searchParams.set('upstream_ids', params.upstreamIds);
  if (params.windows) searchParams.set('windows', params.windows);
  if (params.source) searchParams.set('source', params.source);
  if (params.maxStalenessSecs !== undefined)
    searchParams.set('max_staleness_secs', String(params.maxStalenessSecs));

  return useQuery({
    queryKey: qk.subscriptionQuotaLatest(params),
    queryFn: () =>
      getJson<LatestResponse>(
        `/admin/v1/subscription-quotas/latest?${searchParams.toString()}`,
      ),
    refetchInterval: 30_000,
  });
}

export function useSubscriptionQuotaSeries(params: {
  upstreamIds?: string;
  windows?: string;
  source?: string;
  sinceUnixSecs: number;
  untilUnixSecs: number;
  bucketSecs?: number;
  maxPointsPerSeries?: number;
}) {
  const searchParams = new URLSearchParams();
  if (params.upstreamIds) searchParams.set('upstream_ids', params.upstreamIds);
  if (params.windows) searchParams.set('windows', params.windows);
  if (params.source) searchParams.set('source', params.source);
  searchParams.set('since_unix_secs', String(params.sinceUnixSecs));
  searchParams.set('until_unix_secs', String(params.untilUnixSecs));
  if (params.bucketSecs !== undefined)
    searchParams.set('bucket_secs', String(params.bucketSecs));
  if (params.maxPointsPerSeries !== undefined)
    searchParams.set(
      'max_points_per_series',
      String(params.maxPointsPerSeries),
    );

  return useQuery({
    queryKey: qk.subscriptionQuotaSeries(params),
    queryFn: () =>
      getJson<SeriesResponse>(
        `/admin/v1/subscription-quotas/series?${searchParams.toString()}`,
      ),
    refetchInterval: 60_000,
  });
}

export function useSubscriptionQuotaAnalysis(params: {
  upstreamIds?: string;
  windows?: string;
  source?: string;
  sinceUnixSecs: number;
  untilUnixSecs: number;
}) {
  const searchParams = new URLSearchParams();
  if (params.upstreamIds) searchParams.set('upstream_ids', params.upstreamIds);
  if (params.windows) searchParams.set('windows', params.windows);
  if (params.source) searchParams.set('source', params.source);
  searchParams.set('since_unix_secs', String(params.sinceUnixSecs));
  searchParams.set('until_unix_secs', String(params.untilUnixSecs));

  return useQuery({
    queryKey: qk.subscriptionQuotaAnalysis(params),
    queryFn: () =>
      getJson<AnalysisResponse>(
        `/admin/v1/subscription-quotas/analysis?${searchParams.toString()}`,
      ),
    refetchInterval: 120_000,
  });
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
    mutationFn: ({ id, revision }: { id: string; revision: number }) =>
      deleteJson(`/admin/v1/upstreams/${id}`, { ifMatch: revision }),
    onSuccess: () => qc.invalidateQueries({ queryKey: qk.upstreams }),
  });
}
export function useToggleUpstream() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({
      id,
      enabled,
      revision,
    }: {
      id: string;
      enabled: boolean;
      revision: number;
    }) =>
      postJson<Upstream, Record<string, never>>(
        `/admin/v1/upstreams/${id}/${enabled ? 'enable' : 'disable'}`,
        {},
        { ifMatch: revision },
      ),
    onSuccess: (_d, vars) => {
      qc.invalidateQueries({ queryKey: qk.upstreams });
      qc.invalidateQueries({ queryKey: qk.upstream(vars.id) });
    },
  });
}
export interface UpdateUpstreamRequest {
  name?: string | null;
  base_url?: string | null;
  api_key_env?: string | null;
  api_key_value?: string | null;
}
export function useUpdateUpstream() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({
      id,
      body,
      revision,
    }: {
      id: string;
      body: UpdateUpstreamRequest;
      revision: number;
    }) =>
      putJson<Upstream, UpdateUpstreamRequest>(
        `/admin/v1/upstreams/${id}`,
        body,
        { ifMatch: revision },
      ),
    onSuccess: (_d, vars) => {
      qc.invalidateQueries({ queryKey: qk.upstreams });
      qc.invalidateQueries({ queryKey: qk.upstream(vars.id) });
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
    onSuccess: (_data, { id }) => {
      qc.invalidateQueries({ queryKey: qk.upstreams });
      qc.invalidateQueries({ queryKey: qk.status });
      qc.invalidateQueries({ queryKey: qk.upstreamOauthStatus(id) });
      qc.invalidateQueries({
        queryKey: qk.upstreamSubscriptionMetadata(id),
      });
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
export function useDeletePrincipal() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ id, revision }: { id: string; revision: number }) =>
      deleteJson(`/admin/v1/principals/${id}`, { ifMatch: revision }),
    onSuccess: () => qc.invalidateQueries({ queryKey: qk.principals }),
  });
}
export function useTogglePrincipal() {
  const qc = useQueryClient();
  return useMutation({
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
    onSuccess: () => qc.invalidateQueries({ queryKey: qk.principals }),
  });
}
export function useSetAllowedModels() {
  const qc = useQueryClient();
  return useMutation({
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
    onSuccess: (_d, vars) => {
      qc.invalidateQueries({ queryKey: qk.principal(vars.id) });
      qc.invalidateQueries({ queryKey: qk.principals });
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
}
export function useUploadWasm() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (file: File): Promise<UploadWasmResponse> => {
      const form = new FormData();
      // Do NOT set Content-Type: the browser must inject the multipart boundary.
      form.append('name', file.name.replace(/\.wasm$/, ''));
      form.append('original_filename', file.name);
      form.append('bytes', file);
      const res = await fetchWithAuth('/admin/v1/plugins/wasm', {
        method: 'POST',
        body: form,
      });
      return res.json() as Promise<UploadWasmResponse>;
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: qk.pluginRegistry }),
  });
}
export function useDeletePlugin() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ id, revision }: { id: string; revision: number }) =>
      deleteJson(`/admin/v1/plugins/registry/${id}`, { ifMatch: revision }),
    onSuccess: () => qc.invalidateQueries({ queryKey: qk.pluginRegistry }),
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
    onSuccess: (_d, vars) =>
      qc.invalidateQueries({ queryKey: ['router-terminal', vars.id] }),
  });
}
export function useKillswitch() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (enable: boolean) =>
      enable
        ? postJson<
            { status: string; killswitch: boolean },
            Record<string, never>
          >('/admin/killswitch', {})
        : deleteJson('/admin/killswitch'),
    onSuccess: () => qc.invalidateQueries({ queryKey: qk.status }),
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
  });
}

export function useCompleteOauthDraft() {
  return useMutation({
    mutationFn: (body: { state_token: string; code: string }) =>
      completeOauthDraft(body),
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
