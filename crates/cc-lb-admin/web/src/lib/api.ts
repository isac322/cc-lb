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
  cache_read_input_tokens?: number;
  cost_usd_micros?: number;
  duration_ms: number;
  proxy_setup_ms?: number;
  shape_ms?: number;
  sign_ms?: number;
  upstream_ttfb_ms?: number;
  upstream_body_ms?: number;
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
  kind?: string;
  payload?: Record<string, unknown>;
}

export interface AuditQueryResponse {
  entries: AuditEntry[];
}
