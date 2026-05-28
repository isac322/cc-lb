export type UpstreamKind = 'anthropic_api_key' | 'anthropic_oauth' | 'custom';

export interface UpstreamCreateBody {
  name: string;
  kind: UpstreamKind;
  base_url?: string;
  api_key_env?: string;
}

export interface UpstreamUpdateBody {
  name?: string;
  base_url?: string;
  api_key_env?: string;
}

export interface UpstreamResponse {
  id: string;
  name: string;
  kind: UpstreamKind;
  enabled: boolean;
  revision: number;
}

export interface UpstreamListResponse {
  upstreams: UpstreamResponse[];
}

export type PrincipalKind = 'machine' | 'human' | 'admin';

export interface Limit {
  kind: 'requests' | 'tokens' | 'input_tokens' | 'output_tokens';
  window: string;
  limit: number;
}

export interface CreatePrincipalBody {
  name: string;
  kind: PrincipalKind;
  allowed_models?: string[];
  default_limits?: Limit[];
}

export interface UpdatePrincipalBody {
  name?: string;
  allowed_models?: string[];
  default_limits?: Limit[];
}

export interface AllowedModelsBody {
  models: string[];
  expected_revision: number;
}

export interface PrincipalResponse {
  id: string;
  name: string;
  kind: PrincipalKind;
  enabled: boolean;
  revision: number;
  allowed_models: string[];
  default_limits: Limit[];
}

export interface PrincipalSummaryResponse {
  id: string;
  name: string;
  kind: PrincipalKind;
  enabled: boolean;
  revision: number;
}

export interface PrincipalListResponse {
  principals: PrincipalResponse[];
}

export type PluginSlot = 'router' | 'observability_hook';

export interface PatchRegistryBody {
  label?: string;
}

export type Position = string | { before: string } | { after: string };

export interface InsertChainBody {
  slot: PluginSlot;
  wasm_registry_id: string;
  config?: unknown;
  sse_per_event?: boolean;
  batched_events_per_flush?: number;
  batched_flush_ms?: number;
  position?: Position;
}

export interface ReorderEntry {
  id: string;
  order: number;
  expected_revision: number;
}

export interface ReorderBody {
  entries: ReorderEntry[];
}

export interface RegistryEntryResponse {
  id: string;
  sha256_hex: string;
  name: string;
  original_filename: string;
  label?: string;
  size_bytes: number;
  refcount: number;
  revision: number;
  uploaded_at_unix_secs: number;
}

export interface RegistryListResponse {
  entries: RegistryEntryResponse[];
}

export interface PluginChainEntry {
  id: string;
  principal_id: string;
  slot: PluginSlot;
  order: number;
  wasm_registry_id: string;
  config: unknown;
  sse_per_event: boolean;
  batched_events_per_flush: number;
  batched_flush_ms: number;
  revision: number;
}

export interface ChainListResponse {
  entries: PluginChainEntry[];
}

export interface UploadResponse {
  sha256_hex: string;
  id: string;
  size_bytes: number;
  original_filename: string;
  revision: number;
  idempotent: boolean;
}

export interface StatusUpstream {
  id: string;
  name: string;
  status: string;
  last_apply_at_unix_secs: number;
  last_apply_error?: string;
}

export interface StatusPrincipal {
  id: string;
  name: string;
  enabled: boolean;
  last_apply_error?: string;
}

export interface PluginChainSummary {
  principal_count_with_chain: number;
  total_entries: number;
}

export interface BuildInfo {
  rust_version: string;
  profile: string;
  target: string;
}

export interface StatusResponse {
  version: string;
  git_sha: string;
  uptime_secs: number;
  build: BuildInfo;
  replica_id?: string;
  generation: number;
  upstreams: StatusUpstream[];
  principals: StatusPrincipal[];
  plugin_chain_summary: PluginChainSummary;
  killswitch: boolean;
  last_reload_status?: unknown;
}

export interface OAuthStartResponse {
  authorize_url: string;
  state_token: string;
}

export interface OAuthCompleteRequest {
  state_token: string;
  code: string;
}

export interface OAuthCompleteResponse {
  upstream_id: string;
  expires_at_unix_secs: number;
  access_token_fingerprint: string;
}

export class ConflictError extends Error {
  latest: unknown;

  constructor(message: string, latest: unknown) {
    super(message);
    this.name = 'ConflictError';
    this.latest = latest;
  }
}
