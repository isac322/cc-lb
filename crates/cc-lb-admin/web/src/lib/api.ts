import { useEffect, useState } from 'react';
import { getAdminToken } from './auth';

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

async function fetchWithAuth(
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
  options?: { signal?: AbortSignal; headers?: HeadersInit },
): Promise<T> {
  const res = await fetchWithAuth(path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...options?.headers },
    body: JSON.stringify(body),
    signal: options?.signal,
  });
  return res.json();
}

export async function putJson<T, B>(
  path: string,
  body: B,
  options?: { signal?: AbortSignal; headers?: HeadersInit },
): Promise<T> {
  const res = await fetchWithAuth(path, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json', ...options?.headers },
    body: JSON.stringify(body),
    signal: options?.signal,
  });
  return res.json();
}

export async function deleteJson<T>(
  path: string,
  options?: { signal?: AbortSignal; headers?: HeadersInit },
): Promise<T> {
  const res = await fetchWithAuth(path, {
    method: 'DELETE',
    headers: options?.headers,
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
  options?: { signal?: AbortSignal; headers?: HeadersInit },
): Promise<T> {
  const res = await fetchWithAuth(path, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json', ...options?.headers },
    body: JSON.stringify(body),
    signal: options?.signal,
  });
  return res.json();
}

export function streamEventsFetch(
  path: string,
  options: {
    onEvent: (ev: MessageEvent) => void;
    onError: (err: Error) => void;
    onConnect: () => void;
    signal?: AbortSignal;
  },
): () => void {
  const controller = new AbortController();
  const signal = options.signal
    ? AbortSignal.any([controller.signal, options.signal])
    : controller.signal;

  let isClosed = false;

  async function connect() {
    try {
      const token = getAdminToken();
      const headers = new Headers();
      if (token) {
        headers.set('Authorization', `Bearer ${token}`);
      }
      headers.set('Accept', 'text/event-stream');

      const res = await fetch(path, { headers, signal });
      if (!res.ok) {
        if (res.status === 401) {
          throw new ApiError(401, 'unauthorized', null, 'Unauthorized');
        }
        throw new Error(`HTTP ${res.status}`);
      }

      options.onConnect();

      if (!res.body) throw new Error('No response body');

      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';

      while (!isClosed) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n\n');
        buffer = lines.pop() || '';

        for (const block of lines) {
          if (!block.trim()) continue;
          const lines = block.split('\n');
          let eventType = 'message';
          let data = '';
          let id = '';

          for (const line of lines) {
            if (line.startsWith('event:')) {
              eventType = line.slice(6).trim();
            } else if (line.startsWith('data:')) {
              data += `${line.slice(5).trim()}\n`;
            } else if (line.startsWith('id:')) {
              id = line.slice(3).trim();
            }
          }

          if (data) {
            options.onEvent(
              new MessageEvent(eventType, {
                data: data.trim(),
                lastEventId: id,
              }),
            );
          }
        }
      }
    } catch (err) {
      if (err instanceof Error && err.name === 'AbortError') return;
      options.onError(err instanceof Error ? err : new Error(String(err)));
    }
  }

  connect();

  return () => {
    isClosed = true;
    controller.abort();
  };
}

export type ConnectionState =
  | 'live'
  | 'reconnecting'
  | 'auth_required'
  | 'disconnected';

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
  ts: number;
  ts_ms?: number;
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
  cache_read_input_tokens?: number;
  cost_usd_micros?: number;
  duration_ms: number;
  proxy_setup_ms?: number;
  shape_ms?: number;
  sign_ms?: number;
  upstream_ttfb_ms?: number;
  upstream_body_ms?: number;
  error_code?: string;
}

export interface RecentEventsPayload {
  events: RequestEvent[];
  observed: boolean;
  count: number;
  limit: number;
}

export function useDashboardConnection() {
  const [state, setState] = useState<ConnectionState>('reconnecting');

  useEffect(() => {
    let closeStream: (() => void) | null = null;
    let retryTimeout: ReturnType<typeof setTimeout>;

    function connect() {
      closeStream = streamEventsFetch('/admin/events/stream', {
        onConnect: () => setState('live'),
        onEvent: () => {},
        onError: (err) => {
          if (err instanceof ApiError && err.status === 401) {
            setState('auth_required');
          } else {
            setState('reconnecting');
            retryTimeout = setTimeout(connect, 5000);
          }
        },
      });
    }

    // Initial health check
    getJson('/admin/health')
      .then(() => {
        connect();
      })
      .catch((err) => {
        if (err instanceof ApiError && err.status === 401) {
          setState('auth_required');
        } else {
          setState('disconnected');
          retryTimeout = setTimeout(connect, 5000);
        }
      });

    return () => {
      if (closeStream) closeStream();
      clearTimeout(retryTimeout);
    };
  }, []);

  return state;
}

export interface PrincipalListResponse {
  principals: { id: string }[];
}

export interface PrincipalLimitSnapshot {
  kind: 'requests' | 'tokens' | 'input_tokens' | 'output_tokens';
  limit: number | null;
  remaining: number | null;
  reset: string | null;
  observed_at_unix_secs: number;
  stored_at_unix_secs: number;
  observed: boolean;
}

export interface PrincipalLimitWindow {
  window: string;
  snapshots: PrincipalLimitSnapshot[];
}

export interface PrincipalLimitIdentity {
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

export interface QuotasConfig {
  default_window_secs: number;
  default_requests_per_window: number;
  default_input_tokens: number;
  default_output_tokens: number;
}

export interface PrincipalRuntimeSpec {
  quotas?: QuotasConfig;
  disabled?: boolean;
  allowed_models: string[];
  credentials_ref?: string;
}

export interface CreatePrincipalRequest {
  id: string;
  spec: PrincipalRuntimeSpec;
}

export interface UpdatePrincipalRequest {
  spec: PrincipalRuntimeSpec;
}

export interface AllowedModelsRequest {
  allowed_models: string[];
}

export interface IssueKeyRequest {
  label?: string;
}

export interface PrincipalMutationResponse {
  revision: number;
  principal_id: string;
}

export interface PrincipalAllowedModelsResponse {
  revision: number;
  principal_id: string;
  allowed_models: string[];
}

export interface IssueKeyResponse {
  principal_id: string;
  key_id: string;
  plaintext_key: string;
  issued_at_unix_secs: number;
}

export interface ApiKeyRecord {
  key_id: string;
  label: string | null;
  issued_at_unix_secs: number;
  revoked_at_unix_secs: number | null;
}

export interface KeyListResponse {
  keys: ApiKeyRecord[];
}

export interface RevokeKeyResponse {
  key_id: string;
  revoked_at_unix_secs: number;
}

export interface CredentialEntry {
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

export interface RotateCredentialResponse {
  principal_id: string;
  provider: string;
  kind: string;
  new_key_id: string;
  revoked_key_id: string | null;
  plaintext_key: string;
  issued_at_unix_secs: number;
}

export interface RevokeCredentialResponse {
  principal_id: string;
  provider: string;
  kind: string;
  revoked_keys: string[];
}

export interface QuotaOverrideRequest {
  requests_per_window?: number;
  input_tokens_per_window?: number;
  output_tokens_per_window?: number;
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

export interface PutConfigDraftRequest {
  draft: Record<string, unknown>;
  expected_revision: number;
}

export interface PutConfigDraftResponse {
  revision: number;
  saved_at_unix_secs: number;
}

export interface ValidateConfigDraftRequest {
  expected_revision: number;
}

export interface ValidateConfigDraftResponse {
  valid: boolean;
  revision: number;
  error?: string;
}

export interface ApplyConfigRequest {
  expected_revision: number;
}

export interface ApplyConfigResponse {
  applied_revision: number;
  applied_at_unix_secs: number;
}

export interface RestartRequiredField {
  field: string;
  current: string;
  new: string;
  reason: string;
}

export interface HistorySummary {
  upstreams: number;
  principals: number;
  plugin_count: number;
  tls_enabled: boolean;
}

export interface ConfigHistoryItem {
  revision: number;
  applied_at_unix_secs: number;
  config_summary: HistorySummary;
}

export interface ConfigHistoryResponse {
  history: ConfigHistoryItem[];
}

export interface ConfigDiffItem {
  path: string;
  from: unknown;
  to: unknown;
}

export interface ConfigDiffResponse {
  from: number;
  to: number;
  diff: ConfigDiffItem[];
  truncated_changes_count?: number;
}

export interface BreakerHealth {
  state: string;
  failure_count: number;
  half_open_in_flight: number;
  observed: boolean;
}

export interface BulkheadHealth {
  max_conns: number;
  available_permits: number;
  observed: boolean;
}

export interface DrainHealth {
  draining: boolean;
  in_flight: number;
}

export interface UpstreamHealthResponse {
  name: string;
  kind: string;
  breaker: BreakerHealth;
  bulkhead: BulkheadHealth;
  drain: DrainHealth;
  killswitch: boolean;
  last_probe_unix_secs: number | null;
  error_count_recent: number;
}

export interface PluginStatusEntry {
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

export interface OAuthCredentialStatus {
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

export interface AuditEntry {
  ts: number;
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
  kind?: string;
  payload?: Record<string, unknown>;
}

export interface AuditQueryResponse {
  entries: AuditEntry[];
}

export interface UpstreamListResponse {
  upstreams: { name: string; kind: string }[];
}
