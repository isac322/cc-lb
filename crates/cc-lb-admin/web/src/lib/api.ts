// biome-ignore-all lint/suspicious/noExplicitAny: existing API types use any for record params
import { createEventSource, type EventSourceClient } from 'eventsource-client';
import { clearAdminToken, getAdminToken } from './auth';

const AUTH_REQUIRED_EVENT = 'cclb:auth-required';

function notifyAuthRequired(): void {
  clearAdminToken();
  if (typeof window !== 'undefined') {
    window.dispatchEvent(new CustomEvent(AUTH_REQUIRED_EVENT));
  }
}

export class ApiError extends Error {
  status: number;
  code: string | null;
  body: unknown;

  constructor(
    status: number,
    code: string | null,
    body: unknown,
    message: string,
  ) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
    this.code = code;
    this.body = body;
  }
}

export function buildHeaders(
  base: HeadersInit | undefined,
  ifMatch: number | undefined,
): Headers {
  const h = new Headers(base);
  if (ifMatch !== undefined) {
    h.set('If-Match', `W/"${ifMatch}"`);
  }
  return h;
}

export async function fetchWithAuth(
  path: string,
  options: RequestInit = {},
): Promise<Response> {
  const token = getAdminToken();
  const headers = new Headers(options.headers);
  if (token) {
    headers.set('Authorization', `Bearer ${token}`);
  }

  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), 30000);
  const signal = options.signal || controller.signal;

  try {
    const res = await fetch(path, { ...options, headers, signal });
    if (!res.ok) {
      let body: unknown = null;
      let code: string | null = null;
      let message = res.statusText;
      try {
        body = await res.json();
        if (body && typeof body === 'object') {
          const b = body as Record<string, unknown>;
          if ('code' in b) {
            code = String(b.code);
          } else if ('error' in b) {
            code = String(b.error);
          }

          if ('message' in b) {
            message = String(b.message);
          } else if ('detail' in b) {
            message = String(b.detail);
          }
        }
      } catch {
        // ignore json parse error
      }
      if (res.status === 401) {
        code = 'unauthorized';
        notifyAuthRequired();
      }
      throw new ApiError(res.status, code, body, message);
    }
    return res;
  } finally {
    clearTimeout(timeoutId);
  }
}

export async function getJson<T>(
  path: string,
  options?: { signal?: AbortSignal; headers?: HeadersInit },
): Promise<T> {
  const res = await fetchWithAuth(path, {
    method: 'GET',
    headers: options?.headers,
    signal: options?.signal,
  });
  return res.json();
}

export async function postJson<T, B>(
  path: string,
  body: B,
  options?: { signal?: AbortSignal; headers?: HeadersInit; ifMatch?: number },
): Promise<T> {
  const headers = buildHeaders(
    { 'Content-Type': 'application/json', ...options?.headers },
    options?.ifMatch,
  );
  const res = await fetchWithAuth(path, {
    method: 'POST',
    headers,
    body: JSON.stringify(body),
    signal: options?.signal,
  });
  if (res.status === 204) {
    return {} as T;
  }
  return res.json();
}

export async function putJson<T, B>(
  path: string,
  body: B,
  options?: { signal?: AbortSignal; headers?: HeadersInit; ifMatch?: number },
): Promise<T> {
  const headers = buildHeaders(
    { 'Content-Type': 'application/json', ...options?.headers },
    options?.ifMatch,
  );
  const res = await fetchWithAuth(path, {
    method: 'PUT',
    headers,
    body: JSON.stringify(body),
    signal: options?.signal,
  });
  if (res.status === 204) {
    return {} as T;
  }
  return res.json();
}

export async function deleteJson<T>(
  path: string,
  options?: { signal?: AbortSignal; headers?: HeadersInit; ifMatch?: number },
): Promise<T> {
  const headers = buildHeaders(options?.headers, options?.ifMatch);
  const res = await fetchWithAuth(path, {
    method: 'DELETE',
    headers,
    signal: options?.signal,
  });
  if (res.status === 204) {
    return {} as T;
  }
  return res.json();
}

export async function patchJson<T, B>(
  path: string,
  body: B,
  options?: { signal?: AbortSignal; headers?: HeadersInit; ifMatch?: number },
): Promise<T> {
  const headers = buildHeaders(
    { 'Content-Type': 'application/json', ...options?.headers },
    options?.ifMatch,
  );
  const res = await fetchWithAuth(path, {
    method: 'PATCH',
    headers,
    body: JSON.stringify(body),
    signal: options?.signal,
  });
  if (res.status === 204) {
    return {} as T;
  }
  return res.json();
}

export function eventTime(e: {
  ts?: number | null;
  ts_ms?: number | null;
}): Date | null {
  const ms = e.ts_ms ?? (e.ts != null ? e.ts * 1000 : null);
  return ms != null && Number.isFinite(ms) ? new Date(ms) : null;
}

export async function downloadJson(
  path: string,
  filename: string,
): Promise<void> {
  const res = await fetchWithAuth(path, { method: 'GET' });
  const blob = await res.blob();
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}

export function streamEventsFetch(
  path: string,
  options: {
    onEvent: (data: string) => void;
    onError: (err: Error) => void;
    onConnect: () => void;
    signal?: AbortSignal;
  },
): () => void {
  let isClosed = false;
  let client: EventSourceClient | null = null;

  const token = getAdminToken();
  const headers: Record<string, string> = token
    ? { Authorization: `Bearer ${token}` }
    : {};

  const closeForAuthFailure = () => {
    notifyAuthRequired();
    isClosed = true;
    client?.close();
  };

  client = createEventSource({
    url: path,
    headers,
    fetch: async (url, init) => {
      if (isClosed) {
        throw new DOMException('SSE stream closed', 'AbortError');
      }
      const res = await fetch(url, init as RequestInit);
      if (res.status === 401) {
        const error = new ApiError(401, 'unauthorized', null, 'Unauthorized');
        closeForAuthFailure();
        options.onError(error);
        throw error;
      }
      if (!res.ok) {
        throw new Error(`HTTP ${res.status}`);
      }
      return res;
    },
    onConnect: options.onConnect,
    onScheduleReconnect: () => {
      if (!isClosed) {
        options.onError(new Error('SSE stream reconnecting'));
      }
    },
    onMessage(ev) {
      options.onEvent(ev.data);
    },
  });

  options.signal?.addEventListener(
    'abort',
    () => {
      isClosed = true;
      client?.close();
    },
    { once: true },
  );
  if (options.signal?.aborted) {
    isClosed = true;
    client.close();
  }

  return () => {
    isClosed = true;
    client?.close();
  };
}

export interface SummaryTotals {
  request_count: number;
  input_tokens: number;
  output_tokens: number;
  cache_creation_input_tokens: number;
  cache_read_input_tokens: number;
  error_count: number;
  error_rate: number;
  virtual_cost_micros: number;
  avg_latency_ms: number;
  avg_proxy_setup_ms: number;
  avg_shape_ms: number;
  avg_sign_ms: number;
  avg_upstream_ttfb_ms: number;
  avg_upstream_body_ms: number;
}

export interface UsageBucket {
  bucket_start_unix_secs: number;
  request_count: number;
  input_tokens: number;
  output_tokens: number;
  cache_creation_input_tokens: number;
  cache_read_input_tokens: number;
  error_count: number;
  virtual_cost_micros: number;
  latency_ms_sum: number;
  latency_count: number;
  latency_ms_min?: number;
  latency_ms_max?: number;
  proxy_setup_ms_sum: number;
  proxy_setup_ms_count: number;
  shape_ms_sum: number;
  shape_ms_count: number;
  sign_ms_sum: number;
  sign_ms_count: number;
  upstream_ttfb_ms_sum: number;
  upstream_ttfb_ms_count: number;
  upstream_body_ms_sum: number;
  upstream_body_ms_count: number;
}

export interface Sparkline {
  buckets: UsageBucket[];
}

export interface DashboardSummaryResponse {
  range: string;
  step: string;
  window_start_unix_secs: number;
  window_end_unix_secs: number;
  totals: SummaryTotals;
  sparkline: Sparkline;
  observed: boolean;
}

export interface UsageSeries {
  key: string;
  buckets: UsageBucket[];
}

export interface DashboardUsageResponse {
  range: string;
  step: string;
  group_by: string;
  window_start_unix_secs: number;
  window_end_unix_secs: number;
  series: UsageSeries[];
  truncated_series_count?: number;
  observed: boolean;
}

export interface RequestEvent {
  ts: number | null;
  ts_ms?: number | null;
  request_id: string;
  principal_id?: string;
  key_id?: string;
  principal_kind?: string;
  upstream?: string;
  upstream_name?: string;
  model?: string;
  status: number;
  input_tokens?: number;
  output_tokens?: number;
  cache_creation_input_tokens?: number;
  cache_creation_input_tokens_5m?: number;
  cache_creation_input_tokens_1h?: number;
  cache_read_input_tokens?: number;
  cost_usd_micros?: number;
  cost_input_micros?: number;
  cost_output_micros?: number;
  cost_cache_creation_5m_micros?: number;
  cost_cache_creation_1h_micros?: number;
  cost_cache_read_micros?: number;
  duration_ms: number;
  proxy_setup_ms?: number;
  shape_ms?: number;
  sign_ms?: number;
  upstream_ttfb_ms?: number;
  upstream_body_ms?: number;
  auth_ms?: number;
  route_ms?: number;
  limit_reserve_ms?: number;
  bulkhead_wait_ms?: number;
  dns_ms?: number;
  connect_ms?: number;
  connection_reused?: boolean;
  limit_reconcile_ms?: number;
  observability_post_ms?: number;
  first_body_chunk_ms?: number;
  body_chunk_count?: number;
  body_bytes?: number;
  stream_message_start_ms?: number;
  stream_content_block_start_ms?: number;
  stream_first_content_delta_ms?: number;
  stream_last_content_delta_ms?: number;
  stream_message_stop_ms?: number;
  stream_last_chunk_ms?: number;
  stream_total_ms?: number;
  sse_event_count?: number;
  content_delta_count?: number;
  ping_count?: number;
  inter_token_avg_ms?: number;
  error_code?: string;
}

export interface RecentEventsPayload {
  events: RequestEvent[];
  observed: boolean;
  count: number;
  limit: number;
}

interface PrincipalLimitSnapshot {
  kind: 'requests' | 'tokens' | 'input_tokens' | 'output_tokens';
  limit: number | null;
  remaining: number | null;
  reset: string | null;
  observed_at_unix_secs: number;
  stored_at_unix_secs: number;
  observed: boolean;
}

interface PrincipalLimitWindow {
  window: string;
  snapshots: PrincipalLimitSnapshot[];
}

interface PrincipalLimitIdentity {
  identity_kind: 'account' | 'credential' | 'unobserved';
  identity_value: string | null;
  account_observed: boolean;
  windows: PrincipalLimitWindow[];
}

export interface PrincipalLimitsResponse {
  principal_id: string;
  observed: boolean;
  identities: PrincipalLimitIdentity[];
}

interface ApiKeyRecord {
  key_id: string;
  label: string | null;
  issued_at_unix_secs: number;
  revoked_at_unix_secs: number | null;
  last_4: string;
  last_used_at_unix_secs?: number | null;
}

export interface KeyListResponse {
  keys: ApiKeyRecord[];
}

interface CredentialEntry {
  principal_id: string;
  provider: string;
  kind: string;
  identity: string;
  associated_principals: string[];
  has_credentials: boolean;
  expires_at_unix_secs: number | null;
  status: string;
}

export interface CredentialsResponse {
  credentials: CredentialEntry[];
  observed: boolean;
}

export interface ConfigSchemaResponse {
  schema: Record<string, unknown>;
  coverage_checklist: string[];
}

export interface ConfigDraftResponse {
  draft: Record<string, unknown> | null;
  revision: number;
  last_validated_revision: number | null;
  last_validation_error: string | null;
  saved_at_unix_secs: number | null;
}

interface HistorySummary {
  upstreams: number;
  principals: number;
  plugin_count: number;
  tls_enabled: boolean;
}

interface ConfigHistoryItem {
  revision: number;
  applied_at_unix_secs: number;
  config_summary: HistorySummary;
}

export interface ConfigHistoryResponse {
  history: ConfigHistoryItem[];
}

interface PluginStatusEntry {
  slot: string;
  name: string;
  wasm_path: string;
  loaded: boolean;
  disabled: boolean;
  failure_count: number;
  last_error: string | null;
  sse_per_event: boolean | null;
  batched_events_per_flush: number | null;
  batched_flush_ms: number | null;
}

export interface PluginsStatusResponse {
  plugins: PluginStatusEntry[];
}

interface OAuthCredentialStatus {
  principal_id: string;
  provider: string;
  has_credentials: boolean;
  expires_at_unix_secs: number | null;
  refresh_token_present: boolean;
  last_updated_unix_secs: number | null;
  status: string;
  scopes: string[];
}

export interface OAuthStatusResponse {
  credentials: OAuthCredentialStatus[];
  observed: boolean;
}

export interface UpstreamOAuthStatusResponse {
  upstream_id: string;
  kind: string;
  has_credentials: boolean;
  status: string;
  expires_at_unix_secs: number | null;
  refresh_token_present: boolean;
  scopes: string[];
}

export interface SubscriptionMetadataInner {
  upstream_id: string;
  organization_uuid: string | null;
  organization_role: string | null;
  workspace_role: string | null;
  observed_at_unix_millis: number;
  last_error: string | null;
  raw_roles: string | null;
  raw_bootstrap: string | null;
}

export interface OrganizationMetadataInner {
  organization_uuid: string;
  organization_name: string | null;
  organization_type: string | null;
  rate_limit_tier: string | null;
  has_extra_usage_enabled: boolean | null;
  billing_type: string | null;
  subscription_created_at_unix_secs: number | null;
  account_email: string | null;
  account_display_name: string | null;
  account_uuid: string | null;
  overage_credit_amount_minor_units: number | null;
  overage_credit_currency: string | null;
  overage_credit_granted: boolean | null;
  overage_credit_eligible: boolean | null;
  claude_code_trial_ends_at?: number | null;
  payment_auth_hosted_invoice_url?: string | null;
  observed_at_unix_millis: number;
  last_error: string | null;
  raw_profile: string | null;
  raw_overage_grant: string | null;
}

export interface SubscriptionMetadataResponse {
  upstream_id: string;
  subscription_metadata: SubscriptionMetadataInner | null;
  organization_metadata: OrganizationMetadataInner | null;
}

export function triggerSubscriptionMetadataRefresh(
  upstreamId: string,
): Promise<SubscriptionMetadataResponse> {
  return postJson<SubscriptionMetadataResponse, Record<string, never>>(
    `/admin/v1/upstreams/${upstreamId}/subscription-metadata/refresh`,
    {},
  );
}

interface AuditEntry {
  ts: number | null;
  ts_ms?: number | null;
  request_id: string;
  principal_id: string;
  route: string;
  upstream: string;
  model: string | null;
  status: number;
  input_tokens: number;
  output_tokens: number;
  duration_ms: number;
  agent_label: string | null;
  actor: string | null;
  admin_action: string | null;
  kind?: string;
  payload?: Record<string, unknown>;
}

export interface AuditQueryResponse {
  entries: AuditEntry[];
}

export type SubscriptionQuotaWindow =
  | '5h'
  | '7d'
  | '7d_sonnet'
  | '7d_opus'
  | 'overage'
  | 'unified';

export const WINDOW_LABELS: Record<SubscriptionQuotaWindow, string> = {
  '5h': '5h',
  '7d': '7d',
  '7d_sonnet': '7d (Sonnet)',
  '7d_opus': '7d (Opus)',
  overage: 'Extra Usage',
  unified: 'Unified',
};

export type SubscriptionQuotaDataState = 'fresh' | 'stale' | 'missing';
export type SubscriptionQuotaSourceMerge = 'header' | 'api' | 'merged';

export interface QuotaSnapshot {
  window: SubscriptionQuotaWindow;
  state: SubscriptionQuotaDataState;
  source: string | null;
  utilization: number | null;
  status: string | null;
  resets_at_unix_secs: number | null;
  surpassed_threshold: boolean | null;
  representative_claim: string | null;
  disabled_reason: string | null;
  extra_usage_enabled: boolean | null;
  extra_usage_monthly_limit: number | null;
  extra_usage_used_credits: number | null;
  observed_at_unix_millis: number | null;
  age_secs: number | null;
}

export interface UpstreamLatestSnapshots {
  upstream_id: string;
  upstream_name: string;
  windows: QuotaSnapshot[];
}

export interface LatestResponse {
  now_unix_secs: number;
  max_staleness_secs: number;
  upstreams: UpstreamLatestSnapshots[];
}

export interface SeriesBucketResponse {
  bucket_start_unix_secs: number;
  observed: boolean;
  sample_count: number;
  utilization_min: number | null;
  utilization_avg: number | null;
  utilization_max: number | null;
  utilization_last: number | null;
  status_last: string | null;
  resets_at_unix_secs_last: number | null;
  observed_at_unix_millis_last: number | null;
  sources_seen: string[];
}

export interface SeriesMarkerResponse {
  kind: string;
  at_unix_secs?: number;
  from_unix_secs?: number;
  to_unix_secs?: number;
}

export interface SeriesResponseItem {
  upstream_id: string;
  upstream_name: string;
  window: string;
  buckets: SeriesBucketResponse[];
  markers: SeriesMarkerResponse[];
}

export interface SeriesResponse {
  since_unix_secs: number;
  until_unix_secs: number;
  bucket_secs: number;
  source: string;
  series: SeriesResponseItem[];
}

export interface BurnResponse {
  utilization_per_second: number | null;
  utilization_per_hour: number | null;
  eta_to_limit_secs: number | null;
  resets_before_limit: boolean | null;
  confidence: string;
  sample_count: number;
  reason: string | null;
}

export interface ProxyBurnResponse {
  proxy_tokens_per_second: number | null;
  proxy_tokens_per_hour: number | null;
  effective_limit_tokens_estimate: number | null;
  utilization_per_hour: number | null;
  eta_to_limit_secs: number | null;
  resets_before_limit: boolean | null;
  confidence: string;
  sample_count: number;
  reason: string | null;
}

export interface DeficitResponse {
  projected_proxy_tokens_window: number;
  effective_limit_tokens_estimate: number;
  shortfall_tokens: number;
  recommended_multiplier: number;
  confidence: string;
}

export interface AnalysisWindowResponse {
  window: string;
  current_utilization: number | null;
  resets_at_unix_secs: number | null;
  data_state: string;
  actual_account_burn: BurnResponse;
  proxy_projected_burn: ProxyBurnResponse;
  deficit: DeficitResponse | null;
  caveats: string[];
}

export interface AnalysisUpstreamResponse {
  upstream_id: string;
  upstream_name: string;
  windows: AnalysisWindowResponse[];
}

export interface AnalysisResponse {
  since_unix_secs: number;
  until_unix_secs: number;
  now_unix_secs: number;
  max_staleness_secs: number;
  upstreams: AnalysisUpstreamResponse[];
}

export interface AggregateProviderLotResponse {
  upstream_id: string;
  upstream_name: string;
  window: string;
  source: string | null;
  state: string;
  provider_start_unix_secs: number | null;
  provider_reset_unix_secs: number | null;
  observed_at_unix_millis: number | null;
  utilization: number | null;
  capacity_estimate_tokens: number | null;
  used_before_cc_window_tokens: number;
  capacity_to_now_tokens_estimate: number | null;
  projected_capacity_tokens_estimate: number | null;
  confidence: string;
}

export interface AggregateWindowResponse {
  window: string;
  cc_window_start_unix_secs: number;
  cc_window_reset_unix_secs: number;
  used_tokens: number;
  utilization: number | null;
  utilization_percent: number | null;
  capacity_to_now_tokens_estimate: number | null;
  projected_capacity_tokens_estimate: number | null;
  remaining_to_now_tokens_estimate: number | null;
  confidence: string;
  contributing_upstreams: number;
  stale_upstreams: number;
  missing_capacity_upstreams: number;
  provider_lots: AggregateProviderLotResponse[];
  caveats: string[];
}

export interface AggregateResponse {
  now_unix_secs: number;
  window_anchor_unix_secs: number;
  max_staleness_secs: number;
  upstream_count: number;
  windows: AggregateWindowResponse[];
  caveats: string[];
}

export interface PoolHistoryPoint {
  snapshot_at_unix_secs: number;
  utilization: number | null;
  utilization_percent: number | null;
  contributing_upstreams: number;
  eligible_upstreams: number;
  stale_upstreams: number;
  max_observed_at_unix_millis: number | null;
}

export interface PoolHistoryWindowResponse {
  window: string;
  latest: PoolHistoryPoint | null;
  series: PoolHistoryPoint[];
}

export interface PoolHistoryResponse {
  now_unix_secs: number;
  windows: PoolHistoryWindowResponse[];
}
export interface DraftCompleteResponse {
  state_token: string;
  suggested_name: string;
  subscription_metadata: SubscriptionMetadataInner | null;
  organization_metadata: OrganizationMetadataInner | null;
}

export function startOauthDraft(): Promise<{
  authorize_url: string;
  state_token: string;
}> {
  return postJson('/admin/v1/oauth/draft/start', {});
}

export function completeOauthDraft(body: {
  state_token: string;
  code: string;
}): Promise<DraftCompleteResponse> {
  return postJson('/admin/v1/oauth/draft/complete', body);
}

export function createUpstreamFromOauthDraft(body: {
  state_token: string;
  name: string;
  base_url?: string | null;
}): Promise<any> {
  return postJson('/admin/v1/upstreams/from-oauth-draft', body);
}
