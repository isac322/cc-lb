// cc-lb mock admin API server.
// Mirrors crates/cc-lb-admin Rust handlers; every endpoint returns plausible
// data so the dashboard is fully clickable without the real backend.

import { serve } from "bun";

type Json = Record<string, unknown> | unknown[];
type ChainSlot = "router" | "observability_hook" | "shape";

const NOW = () => Math.floor(Date.now() / 1000);
const startedAt = NOW();
const BUILTIN_CACHE_AFFINITY_ID = "00000000-0000-0000-0000-000000000001";

// ──────────────────────────────────────────────────────────────────────────────
// Seed data

const upstreams: any[] = [
  {
    id: "us-anthropic-primary",
    name: "anthropic-primary",
    kind: "anthropic_api_key",
    enabled: true,
    spec_revision: 7,
    base_url: "https://api.anthropic.com",
    api_key_env: "ANTHROPIC_API_KEY",
    warmup_enabled: false,
    warmup_dialect_plugin: null,
    status: { last_apply_error: null, last_apply_at_unix_secs: null, last_warmup_at_unix_secs: null },
  },
  {
    id: "us-anthropic-secondary",
    name: "anthropic-secondary",
    kind: "anthropic_api_key",
    enabled: false,
    spec_revision: 3,
    base_url: "https://api.anthropic.com",
    api_key_env: "ANTHROPIC_API_KEY_2",
    warmup_enabled: false,
    warmup_dialect_plugin: null,
    status: { last_apply_error: null, last_apply_at_unix_secs: null, last_warmup_at_unix_secs: null },
  },
  {
    id: "us-oauth-healthy",
    name: "oauth-healthy",
    kind: "anthropic_oauth",
    enabled: true,
    spec_revision: 4,
    base_url: "https://api.anthropic.com",
    warmup_enabled: true,
    warmup_dialect_plugin: {
      wasm_registry_id: "pl-shape-llama",
      wire_version: 3,
      config: {
        mode: "compact",
        max_tokens: 1,
        model: "claude-haiku-4-5-20251015",
        anthropic_beta: ["oauth-2025-04-20"],
        retry_on_429: true,
      },
    },
    status: { last_apply_error: null, last_apply_at_unix_secs: null, last_warmup_at_unix_secs: 1718380800 },
  },
  {
    id: "us-oauth-disabled",
    name: "oauth-disabled",
    kind: "anthropic_oauth",
    enabled: false,
    spec_revision: 2,
    base_url: "https://api.anthropic.com",
    warmup_enabled: false,
    warmup_dialect_plugin: null,
    status: { last_apply_error: null, last_apply_at_unix_secs: null, last_warmup_at_unix_secs: null },
  },
];

const principals: any[] = [
  {
    id: "pr-admin",
    name: "admin",
    kind: "admin",
    enabled: true,
    revision: 5,
    allowed_models: ["claude-opus-4-5-20251104", "claude-sonnet-4-5-20251002", "claude-haiku-4-5-20251015"],
    allowed_upstreams: ["us-anthropic-primary", "us-oauth-provider"],
    default_limits: [
      { kind: "requests", window_secs: 60, cap_micros: 1000 },
      { kind: "total_tokens", window_secs: 60, cap_micros: 400000 },
    ],
  },
  {
    id: "pr-dev-team",
    name: "engineering-shared",
    kind: "human",
    enabled: true,
    revision: 9,
    allowed_models: ["claude-sonnet-4-5-20251002", "claude-haiku-4-5-20251015"],
    allowed_upstreams: ["us-anthropic-primary"],
    default_limits: [
      { kind: "requests", window_secs: 60, cap_micros: 3000 },
      { kind: "total_tokens", window_secs: 60, cap_micros: 1000000 },
    ],
  },
  {
    id: "pr-ci",
    name: "ci-runner",
    kind: "machine",
    enabled: true,
    revision: 12,
    allowed_models: ["claude-haiku-4-5-20251015"],
    allowed_upstreams: ["us-anthropic-primary"],
    default_limits: [
      { kind: "requests", window_secs: 60, cap_micros: 600 },
      { kind: "total_tokens", window_secs: 60, cap_micros: 200000 },
      { kind: "cost_usd", window_secs: 86400, cap_micros: 50_000_000 },
    ],
  },
  {
    id: "pr-prod-app",
    name: "production-claude-code",
    kind: "machine",
    enabled: true,
    revision: 22,
    allowed_models: ["claude-sonnet-4-5-20251002", "claude-opus-4-5-20251104"],
    allowed_upstreams: ["us-oauth-provider"],
    default_limits: [
      { kind: "requests", window_secs: 60, cap_micros: 5000 },
      { kind: "total_tokens", window_secs: 60, cap_micros: 2000000 },
      { kind: "concurrent", window_secs: 1, cap_micros: 32 },
    ],
  },
  {
    id: "pr-disabled-old",
    name: "legacy-internal-tool",
    kind: "machine",
    enabled: false,
    revision: 2,
    allowed_models: [],
    allowed_upstreams: [],
    default_limits: [],
  },
];

const plugins: any[] = [
  {
    id: BUILTIN_CACHE_AFFINITY_ID,
    sha256_hex: "0000000000000000000000000000000000000000000000000000000000000000",
    name: "cache-affinity",
    original_filename: "builtin://cache-affinity",
    label: "Built-in cache affinity filter",
    size_bytes: 0,
    refcount: 5,
    revision: 0,
    uploaded_at_unix_secs: 0,
    kind: "filter",
    wire_version: 3,
    is_builtin: true,
    slot: "router",
    metadata: {
      purpose: "Prefer upstreams whose prompt cache is already warm for this request.",
      keeps: "Candidates with a positive prefill_cache_score (the upstream has already cached the prefix).",
      drops: "Candidates with zero cache score — only when at least one candidate is a cache hit; otherwise nothing is dropped.",
      empty_behavior: "Never drops everything. Falls back to passing all candidates through when no cache hit exists.",
      examples: ["5 candidates, 2 with positive cache score → keep the 2 hits.", "5 candidates, all with zero cache score → pass all 5 through.", "Exactly 1 candidate → no change."],
    },
  },
  {
    id: "pl-rate-limiter",
    sha256_hex: "9f2c6c0e8b3f5d1e7a4b9c2e0f1d3a5b7c9e1f0a2d4b6c8e0f1d3a5b7c9e1f0a",
    name: "rate-limiter",
    original_filename: "rate-limiter-v1.2.0.wasm",
    label: "Token-bucket per-principal rate limiter",
    size_bytes: 142_336,
    refcount: 3,
    revision: 4,
    uploaded_at_unix_secs: NOW() - 86400 * 14,
    slot: "router",
    metadata: null,
  },
  {
    id: "pl-audit-logger",
    sha256_hex: "1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b",
    name: "audit-logger",
    original_filename: "audit-logger-v0.9.4.wasm",
    label: "Append-only audit hook (writes to S3)",
    size_bytes: 218_112,
    refcount: 5,
    revision: 2,
    uploaded_at_unix_secs: NOW() - 86400 * 30,
    slot: "observability_hook",
    metadata: null,
  },
  {
    id: "pl-cost-tracker",
    sha256_hex: "3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2c3d4e",
    name: "cost-tracker",
    original_filename: "cost-tracker.wasm",
    label: "Per-principal cost projection",
    size_bytes: 96_672,
    refcount: 2,
    revision: 1,
    uploaded_at_unix_secs: NOW() - 86400 * 3,
    slot: "observability_hook",
    metadata: null,
  },
  {
    id: "pl-shape-llama",
    sha256_hex: "5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2c3d4e5f6a",
    name: "anthropic-to-openai-shape",
    original_filename: "shape-llama.wasm",
    label: "Translates Anthropic Messages → OpenAI Chat Completions for self-hosted llama.cpp",
    size_bytes: 312_096,
    refcount: 1,
    revision: 3,
    uploaded_at_unix_secs: NOW() - 86400 * 7,
    slot: "shape",
    metadata: null,
  },
  {
    id: "pl-routing-canary",
    sha256_hex: "7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b",
    name: "canary-router",
    original_filename: "canary-router-v0.2.0.wasm",
    label: "10% canary traffic split based on header",
    size_bytes: 67_584,
    refcount: 1,
    revision: 1,
    uploaded_at_unix_secs: NOW() - 86400 * 2,
    slot: "router",
    metadata: null,
  },
  {
    id: "pl-anthropic-shape-v2",
    sha256_hex: "2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2c3d4e5f6a7b8c9d0e1f2a3b",
    name: "anthropic-shape-v2",
    original_filename: "anthropic-shape-v2.wasm",
    label: "Anthropic shape translator v2 with enhanced caching",
    size_bytes: 285_440,
    refcount: 0,
    revision: 2,
    uploaded_at_unix_secs: NOW() - 86400 * 5,
    slot: "shape",
    metadata: null,
  },
  {
    id: "pl-claude-edge-shape",
    sha256_hex: "4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d",
    name: "claude-edge-shape",
    original_filename: "claude-edge-shape.wasm",
    label: "Claude edge-optimized shape converter",
    size_bytes: 201_728,
    refcount: 0,
    revision: 1,
    uploaded_at_unix_secs: NOW() - 86400 * 1,
    slot: "shape",
    metadata: null,
  },
];

let chainEntryAutoId = 1000;
const chainEntries: any[] = [
  { id: `pce-${++chainEntryAutoId}`, principal_id: "pr-admin", slot: "router", order: 0, wasm_registry_id: BUILTIN_CACHE_AFFINITY_ID, config: {}, sse_per_event: false, batched_events_per_flush: 1, batched_flush_ms: 100, revision: 0, wire_version: 3 },
  // pr-admin
  { id: `pce-${++chainEntryAutoId}`, principal_id: "pr-admin", slot: "router", order: 1, wasm_registry_id: "pl-rate-limiter", config: { tier: "admin" }, sse_per_event: false, batched_events_per_flush: 0, batched_flush_ms: 0, revision: 2 },
  { id: `pce-${++chainEntryAutoId}`, principal_id: "pr-admin", slot: "observability_hook", order: 1, wasm_registry_id: "pl-audit-logger", config: {}, sse_per_event: true, batched_events_per_flush: 0, batched_flush_ms: 0, revision: 1 },
  { id: `pce-${++chainEntryAutoId}`, principal_id: "pr-admin", slot: "observability_hook", order: 2, wasm_registry_id: "pl-cost-tracker", config: {}, sse_per_event: false, batched_events_per_flush: 50, batched_flush_ms: 1000, revision: 1 },
  { id: `pce-${++chainEntryAutoId}`, principal_id: "pr-dev-team", slot: "router", order: 0, wasm_registry_id: BUILTIN_CACHE_AFFINITY_ID, config: {}, sse_per_event: false, batched_events_per_flush: 1, batched_flush_ms: 100, revision: 0, wire_version: 3 },
  // pr-dev-team
  { id: `pce-${++chainEntryAutoId}`, principal_id: "pr-dev-team", slot: "router", order: 1, wasm_registry_id: "pl-rate-limiter", config: { tier: "dev" }, sse_per_event: false, batched_events_per_flush: 0, batched_flush_ms: 0, revision: 1 },
  { id: `pce-${++chainEntryAutoId}`, principal_id: "pr-dev-team", slot: "observability_hook", order: 1, wasm_registry_id: "pl-cost-tracker", config: {}, sse_per_event: false, batched_events_per_flush: 50, batched_flush_ms: 1000, revision: 1 },
  { id: `pce-${++chainEntryAutoId}`, principal_id: "pr-prod-app", slot: "router", order: 0, wasm_registry_id: BUILTIN_CACHE_AFFINITY_ID, config: {}, sse_per_event: false, batched_events_per_flush: 1, batched_flush_ms: 100, revision: 0, wire_version: 3 },
  // pr-prod-app
  { id: `pce-${++chainEntryAutoId}`, principal_id: "pr-prod-app", slot: "router", order: 1, wasm_registry_id: "pl-routing-canary", config: { canary_percent: 10 }, sse_per_event: false, batched_events_per_flush: 0, batched_flush_ms: 0, revision: 1 },
  { id: `pce-${++chainEntryAutoId}`, principal_id: "pr-prod-app", slot: "observability_hook", order: 1, wasm_registry_id: "pl-audit-logger", config: {}, sse_per_event: true, batched_events_per_flush: 0, batched_flush_ms: 0, revision: 1 },
  { id: `pce-${++chainEntryAutoId}`, principal_id: "pr-ci", slot: "router", order: 0, wasm_registry_id: BUILTIN_CACHE_AFFINITY_ID, config: {}, sse_per_event: false, batched_events_per_flush: 1, batched_flush_ms: 100, revision: 0, wire_version: 3 },
  // pr-ci
  { id: `pce-${++chainEntryAutoId}`, principal_id: "pr-ci", slot: "shape", order: 1, wasm_registry_id: "pl-shape-llama", config: {}, sse_per_event: false, batched_events_per_flush: 0, batched_flush_ms: 0, revision: 1 },
];
for (const principal of principals) {
  if (!chainEntries.some((entry) => entry.principal_id === principal.id && entry.slot === "router" && entry.wasm_registry_id === BUILTIN_CACHE_AFFINITY_ID)) {
    chainEntries.push({ id: `pce-${++chainEntryAutoId}`, principal_id: principal.id, slot: "router", order: 0, wasm_registry_id: BUILTIN_CACHE_AFFINITY_ID, config: {}, sse_per_event: false, batched_events_per_flush: 1, batched_flush_ms: 100, revision: 0, wire_version: 3 });
  }
}

// API keys per principal (separate from principal CRUD)
let keyAutoId = 100;
const apiKeys: any[] = [
  { key_id: `key-${++keyAutoId}`, principal_id: "pr-admin", label: "Sisyphus laptop", issued_at_unix_secs: NOW() - 86400 * 30, revoked_at_unix_secs: null, status: "active", last_4: "9af2", expires_at_unix_secs: null },
  { key_id: `key-${++keyAutoId}`, principal_id: "pr-admin", label: "Spike — debugging incident 2026-05-21", issued_at_unix_secs: NOW() - 86400 * 9, revoked_at_unix_secs: NOW() - 86400 * 2, status: "revoked", last_4: "1c3e", expires_at_unix_secs: null },
  { key_id: `key-${++keyAutoId}`, principal_id: "pr-dev-team", label: "team-shared", issued_at_unix_secs: NOW() - 86400 * 22, revoked_at_unix_secs: null, status: "active", last_4: "7d11", expires_at_unix_secs: NOW() + 86400 * 60 },
  { key_id: `key-${++keyAutoId}`, principal_id: "pr-ci", label: "github-actions", issued_at_unix_secs: NOW() - 86400 * 11, revoked_at_unix_secs: null, status: "active", last_4: "ab48", expires_at_unix_secs: null },
  { key_id: `key-${++keyAutoId}`, principal_id: "pr-prod-app", label: "prod-claude-code", issued_at_unix_secs: NOW() - 86400 * 60, revoked_at_unix_secs: null, status: "active", last_4: "ff09", expires_at_unix_secs: null },
];

const MODEL_LIST = [
  "claude-opus-4-5-20251104",
  "claude-sonnet-4-5-20251002",
  "claude-haiku-4-5-20251015",
  "claude-3-5-sonnet-20241022",
];

const upstreamModels: Record<string, string[]> = {
  "us-anthropic-primary": ["claude-3-5-sonnet-20241022", "claude-3-5-haiku-20241022"],
  "us-anthropic-secondary": ["claude-3-5-sonnet-20241022", "claude-3-5-haiku-20241022"],
  "us-oauth-provider": ["claude-3-5-sonnet-20241022", "claude-3-5-haiku-20241022"],
};

let killswitch = false;
let configRevision = 12;
let configDraft: { draft: unknown | null; revision: number; last_validated_revision: number | null; last_validation_error: string | null; saved_at_unix_secs: number | null } = {
  draft: null,
  revision: configRevision,
  last_validated_revision: null,
  last_validation_error: null,
  saved_at_unix_secs: null,
};

const terminalStrategies: Record<string, { strategy: string; revision: number }> = {};

const configHistory = Array.from({ length: 8 }).map((_, i) => ({
  revision: configRevision - i,
  applied_at_unix_secs: NOW() - 3600 * (i * 18 + 4),
  config_summary: {
    upstreams: 3 + (i % 2),
    principals: 4 + (i % 3),
    plugin_count: 4 + (i % 2),
    tls_enabled: true,
  },
}));

// ──────────────────────────────────────────────────────────────────────────────
// Helpers

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, PUT, PATCH, DELETE, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Authorization, If-Match",
  "Access-Control-Expose-Headers": "etag, x-total-count, location, link",
};

function ok(body: Json | null = null, extra: HeadersInit = {}): Response {
  if (body === null) {
    return new Response(null, { status: 204, headers: { ...CORS, ...(extra as Record<string, string>) } });
  }
  return new Response(JSON.stringify(body), { status: 200, headers: { "Content-Type": "application/json", ...CORS, ...(extra as Record<string, string>) } });
}
function created(body: Json, extra: HeadersInit = {}): Response {
  return new Response(JSON.stringify(body), { status: 201, headers: { "Content-Type": "application/json", ...CORS, ...(extra as Record<string, string>) } });
}
function err(status: number, code: string, message?: string): Response {
  return new Response(JSON.stringify({ error: code, message: message ?? code }), { status, headers: { "Content-Type": "application/json", ...CORS } });
}
function notFound(code = "not_found") {
  return err(404, code);
}
function paginate<T>(items: T[], url: URL, getId: (item: T) => string | number): { slice: T[]; total: number } {
  const limit = Math.min(Math.max(parseInt(url.searchParams.get("limit") ?? "100", 10) || 100, 1), 1000);
  const after = url.searchParams.get("after");
  let start = 0;
  if (after) {
    const ix = items.findIndex((it) => String(getId(it)) === after);
    if (ix >= 0) start = ix + 1;
  }
  return { slice: items.slice(start, start + limit), total: items.length };
}
function listResponse<T>(items: T[], url: URL, getId: (item: T) => string | number, key: string): Response {
  const { slice, total } = paginate(items, url, getId);
  return ok({ [key]: slice } as Json, { "x-total-count": String(total), "X-Total-Count": String(total) });
}
function readJson<T = any>(req: Request): Promise<T> { return req.json() as Promise<T>; }
function withRouterChain(principal: any) {
  return {
    ...principal,
    router_chain: chainEntries
      .filter((entry) => entry.principal_id === principal.id && entry.slot === "router")
      .sort((a, b) => a.order - b.order),
  };
}

function randomSeed(seed: number): () => number {
  let s = seed >>> 0 || 1;
  return () => {
    s = (s * 1664525 + 1013904223) >>> 0;
    return s / 0xffffffff;
  };
}
function buildBuckets(range: string, step: string, groups: string[], requestEnd = NOW(), groupBy: string = "none", upstreamId?: string) {
  const stepSecs = step === "minute" ? 60 : step === "hour" ? 3600 : 60;
  const rangeSecs: Record<string, number> = {
    "1h": 3600, "6h": 6 * 3600, "24h": 24 * 3600, "7d": 7 * 24 * 3600, "30d": 30 * 24 * 3600,
  };
  const totalRangeSecs = rangeSecs[range] ?? rangeSecs["1h"];
  const points = Math.min(Math.floor(totalRangeSecs / stepSecs), 720);
  const windowEnd = requestEnd;
  const windowStart = windowEnd - points * stepSecs;
  const series = groups.map((g, gi) => {
    const seedBase = upstreamId ? upstreamId.charCodeAt(0) * 1000 + upstreamId.length : 0xabc;
    const rng = randomSeed(seedBase + gi * 31 + g.length);
    const upstreamName = groupBy === "upstream" ? upstreams.find((u) => u.name === g)?.name : undefined;
    return {
      key: g,
      ...(upstreamName && { upstream_name: upstreamName }),
      buckets: Array.from({ length: points }).map((_, i) => {
        const ts = windowStart + i * stepSecs;
        const base = 60 + Math.floor(rng() * 220);
        const requests = Math.floor(base * (0.6 + 0.6 * Math.sin(i / 8)));
        const errors = i % 17 === 0 ? Math.floor(requests * 0.05) : 0;
        const input = requests * (300 + Math.floor(rng() * 200));
        const output = requests * (700 + Math.floor(rng() * 600));
        const cacheCreation = Math.floor(input * 0.1);
        const cacheRead = Math.floor(input * 0.05);
        const latencySum = requests * (40 + Math.floor(rng() * 200));
        const proxySetupMs = Math.floor(requests * (2 + rng() * 8));
        const shapeMs = Math.floor(requests * (1 + rng() * 4));
        const signMs = Math.floor(requests * (1 + rng() * 3));
        const upstreamTtfbMs = Math.floor(requests * (20 + rng() * 100));
        const upstreamBodyMs = Math.floor(requests * (10 + rng() * 50));
        return {
          bucket_start_unix_secs: ts,
          request_count: requests,
          input_tokens: input,
          output_tokens: output,
          cache_creation_input_tokens: cacheCreation,
          cache_read_input_tokens: cacheRead,
          error_count: errors,
          virtual_cost_micros: input * 3 + output * 15,
          latency_ms_sum: latencySum,
          latency_count: requests,
          latency_ms_min: requests > 0 ? 20 : undefined,
          latency_ms_max: requests > 0 ? 300 : undefined,
          proxy_setup_ms_sum: proxySetupMs,
          proxy_setup_ms_count: requests,
          shape_ms_sum: shapeMs,
          shape_ms_count: requests,
          sign_ms_sum: signMs,
          sign_ms_count: requests,
          upstream_ttfb_ms_sum: upstreamTtfbMs,
          upstream_ttfb_ms_count: requests,
          upstream_body_ms_sum: upstreamBodyMs,
          upstream_body_ms_count: requests,
        };
      }),
    };
  });
  return { range, step, group_by: groupBy, window_start_unix_secs: windowStart, window_end_unix_secs: windowEnd, series, observed: true };
}

function generateEvent(seq: number, forceUpstreamId?: string) {
  const rng = randomSeed(seq);
  const upstream = forceUpstreamId ? upstreams.find((u) => u.id === forceUpstreamId) : upstreams[Math.floor(rng() * upstreams.length)];
  if (!upstream) return null;
  const principal = principals[Math.floor(rng() * principals.length)];
  const upstreamModelList = upstreamModels[upstream.id] ?? MODEL_LIST;
  const model = upstreamModelList[Math.floor(rng() * upstreamModelList.length)];
  const status = rng() < 0.92 ? 200 : rng() < 0.5 ? 429 : 500;
  const latency = Math.floor(40 + rng() * 800);
  const inputTokens = Math.floor(120 + rng() * 1800);
  const outputTokens = Math.floor(220 + rng() * 4200);
  // Cache scenarios to exercise the redesigned Token + Cost cells:
  //   0: no cache, 1: 5m-only, 2: 5m + 1h, 3: 1h-only + read, 4: read-only
  const cacheScenario = Math.floor(rng() * 5);
  let cc5m = 0;
  let cc1h = 0;
  let cr = 0;
  if (cacheScenario === 1) cc5m = Math.floor(800 + rng() * 4000);
  else if (cacheScenario === 2) {
    cc5m = Math.floor(400 + rng() * 1500);
    cc1h = Math.floor(2000 + rng() * 12000);
  } else if (cacheScenario === 3) {
    cc1h = Math.floor(2000 + rng() * 8000);
    cr = Math.floor(500 + rng() * 2400);
  } else if (cacheScenario === 4) {
    cr = Math.floor(800 + rng() * 6000);
  }
  // Anthropic Sonnet-like rates: input $3, output $15, cc_5m $3.75, cc_1h $6, cr $0.30
  const costInput = inputTokens * 3;
  const costOutput = outputTokens * 15;
  const costCc5m = Math.floor(cc5m * 3.75);
  const costCc1h = cc1h * 6;
  const costCr = Math.floor(cr * 0.3);
  const costMicros = costInput + costOutput + costCc5m + costCc1h + costCr;

  const hasTrace = rng() < 0.33;
  const routing_trace = hasTrace ? {
    stages: [
      { plugin_id: "pl-rate-limiter", plugin_name: "rate-limiter", kept_upstream_ids: [upstream.id], reason: "Allowed", per_candidate_reasons: [], duration_us: 150, passthrough: null }
    ],
    terminal: { strategy: "random", chosen: upstream.id }
  } : undefined;
  const internal_errors = hasTrace && rng() < 0.5 ? [
    { stage: "RouterFilter", index: 0, kind: "Other", message: "Mock internal error" }
  ] : undefined;

  return {
    ts: NOW() - Math.floor(rng() * 600),
    request_id: `req_${seq.toString(36)}${Math.floor(rng() * 0xffff).toString(36)}`,
    principal_id: principal.id,
    principal_name: principal.name,
    key_id: apiKeys.find((k) => k.principal_id === principal.id)?.key_id ?? null,
    route: "/v1/messages",
    upstream: upstream.name,
    upstream_id: upstream.id,
    model,
    status,
    input_tokens: inputTokens,
    output_tokens: outputTokens,
    cache_creation_input_tokens: cc5m + cc1h,
    cache_creation_input_tokens_5m: cc5m,
    cache_creation_input_tokens_1h: cc1h,
    cache_read_input_tokens: cr,
    cost_usd_micros: costMicros,
    cost_input_micros: costInput,
    cost_output_micros: costOutput,
    cost_cache_creation_5m_micros: costCc5m,
    cost_cache_creation_1h_micros: costCc1h,
    cost_cache_read_micros: costCr,
    duration_ms: latency,
    queue_ms: Math.floor(rng() * 30),
    auth_ms: Math.floor(rng() * 12),
    upstream_ms: latency - 12 - Math.floor(rng() * 30),
    agent_label: rng() < 0.4 ? "claude-code" : rng() < 0.7 ? "opencode" : "manual",
    kind: status >= 500 ? "error" : status === 429 ? "rate_limit" : "request",
    payload: {},
    routing_trace,
    internal_errors,
  };
}
const RECENT_EVENTS = Array.from({ length: 220 }).map((_, i) => generateEvent(i + 1)).filter((e): e is NonNullable<typeof e> => e !== null).sort((a, b) => b.ts - a.ts);

// ──────────────────────────────────────────────────────────────────────────────
// Warmup attempts fixture

const WARMUP_PLUGIN_SNAPSHOT_FIXTURE = {
  wasm_registry_id: "pl-shape-llama",
  wire_version: 3,
  config: {
    mode: "compact",
    max_tokens: 1,
    model: "claude-haiku-4-5-20251015",
    anthropic_beta: ["oauth-2025-04-20"],
    retry_on_429: true,
  },
};

const WARMUP_FIVE_HOUR = 5 * 3600;

function pluginSnapshotFor(u: any) {
  if (!u.warmup_dialect_plugin) return null;
  return {
    ...WARMUP_PLUGIN_SNAPSHOT_FIXTURE,
    wasm_registry_id: u.warmup_dialect_plugin.wasm_registry_id,
    config: u.warmup_dialect_plugin.config && Object.keys(u.warmup_dialect_plugin.config).length
      ? u.warmup_dialect_plugin.config
      : WARMUP_PLUGIN_SNAPSHOT_FIXTURE.config,
  };
}

function generateWarmupAttempts(upstreamId: string): any[] {
  const upstream = upstreams.find((u) => u.id === upstreamId);
  const seedBase = [...upstreamId].reduce((acc, ch) => acc + ch.charCodeAt(0), 0);
  const rng = randomSeed(seedBase + 7);
  const out: any[] = [];
  let prevFreshAt = NOW() - 30 * 86400 - WARMUP_FIVE_HOUR;

  for (let day = 29; day >= 0; day--) {
    const attemptsToday = 3 + Math.floor(rng() * 4);
    for (let i = 0; i < attemptsToday; i++) {
      const dayBase = NOW() - day * 86400;
      const ts = dayBase - Math.floor(rng() * 86400);
      const trigger = rng() < 0.15 ? "manual" : "scheduled";
      const completedDelta = 0.2 + rng() * 1.2;

      let outcome: string;
      let reason: string | null = null;
      let httpStatus: number | null = null;
      let cycleKey: number | null = null;
      let expectedCycleKey: number | null = null;
      let idleSecs: number | null = null;
      let errorDetail: string | null = null;
      let leaseHolder: string | null = null;

      const roll = rng();
      if (roll < 0.62) {
        outcome = "success_fresh";
        httpStatus = 200;
        cycleKey = Math.floor(ts / WARMUP_FIVE_HOUR);
        const idleRaw = ts - prevFreshAt - WARMUP_FIVE_HOUR;
        idleSecs = idleRaw > 0 ? idleRaw : 0;
        prevFreshAt = ts;
      } else if (roll < 0.78) {
        outcome = "success_redundant";
        reason = "window_already_active";
        httpStatus = 200;
        cycleKey = Math.floor(prevFreshAt / WARMUP_FIVE_HOUR);
        expectedCycleKey = cycleKey + 1;
      } else if (roll < 0.88) {
        outcome = "transient_failure";
        const opts = ["upstream_5xx", "network_error", "request_timeout", "http_429_missing_cycle_key", "dialect_plugin_transient"];
        reason = opts[Math.floor(rng() * opts.length)]!;
        httpStatus = reason === "upstream_5xx" ? (rng() < 0.5 ? 502 : 503) : reason === "http_429_missing_cycle_key" ? 429 : null;
        errorDetail = reason === "network_error"
          ? "tcp connect: connection refused (3 retries exhausted)"
          : reason === "request_timeout"
            ? "upstream did not respond within 12000ms"
            : reason === "http_429_missing_cycle_key"
              ? "429 returned without anthropic-ratelimit-* headers — cannot tell which cycle"
              : reason === "dialect_plugin_transient"
                ? "shape plugin transient error: deadline_exceeded after 250ms"
                : "upstream returned 5xx";
      } else if (roll < 0.95) {
        outcome = "permanent_failure";
        const opts = ["auth_failed", "forbidden", "bad_request", "not_found", "dialect_plugin_failed", "oauth_refresh_failed", "request_build_failed", "credential_decrypt_failed"];
        reason = opts[Math.floor(rng() * opts.length)]!;
        httpStatus = reason === "auth_failed" ? 401 : reason === "forbidden" ? 403 : reason === "not_found" ? 404 : reason === "bad_request" ? 400 : null;
        errorDetail = reason === "dialect_plugin_failed"
          ? "shape plugin returned error: schema_mismatch (field 'model' expected string, got null)"
          : reason === "auth_failed"
            ? "OAuth token rejected: invalid_token"
            : reason === "forbidden"
              ? "403: scope 'user:inference' missing on credential"
              : reason === "oauth_refresh_failed"
                ? "refresh_token revoked by upstream"
                : reason === "request_build_failed"
                  ? "could not build /v1/messages request body (serde_json::Error)"
                  : reason === "credential_decrypt_failed"
                    ? "AEAD decrypt failed: wrong nonce"
                    : `Upstream rejected: ${reason}`;
      } else {
        outcome = "skipped";
        const opts = ["lease_held", "upstream_disabled", "oauth_credentials_missing", "upstream_deleted"];
        reason = opts[Math.floor(rng() * opts.length)]!;
        if (reason === "lease_held") {
          leaseHolder = `replica-${1 + Math.floor(rng() * 3)}`;
        }
      }

      out.push({
        id: `wa-${upstreamId}-${day}-${i}`,
        upstream_id: upstreamId,
        attempted_at_unix_secs: ts,
        completed_at_unix_secs: outcome === "skipped" ? ts : ts + completedDelta,
        scheduled_for_unix_secs: trigger === "manual" ? ts : Math.floor(ts / 3600) * 3600,
        trigger,
        outcome,
        reason,
        http_status: httpStatus,
        cycle_key: cycleKey,
        expected_cycle_key: expectedCycleKey,
        idle_secs_since_prev_window: idleSecs,
        replica_id: `replica-${1 + Math.floor(rng() * 3)}`,
        lease_holder: leaseHolder,
        upstream_spec_revision: upstream?.spec_revision ?? 1,
        dialect_plugin_snapshot: pluginSnapshotFor(upstream ?? {}),
        error_detail: errorDetail,
      });
    }
  }

  out.sort((a, b) => b.attempted_at_unix_secs - a.attempted_at_unix_secs);

  if (upstreamId === "us-oauth-healthy") {
    for (let i = 0; i < 3; i++) {
      if (out[i]) {
        out[i].outcome = "permanent_failure";
        out[i].reason = "oauth_refresh_failed";
        out[i].http_status = null;
        out[i].error_detail = "refresh_token revoked by upstream";
      }
    }
  }

  return out;
}

const warmupAttemptsByUpstream: Record<string, any[]> = {};
function getWarmupAttempts(upstreamId: string): any[] {
  if (!warmupAttemptsByUpstream[upstreamId]) {
    warmupAttemptsByUpstream[upstreamId] = generateWarmupAttempts(upstreamId);
  }
  return warmupAttemptsByUpstream[upstreamId]!;
}
function prependWarmupAttempt(upstreamId: string, attempt: any) {
  getWarmupAttempts(upstreamId).unshift(attempt);
}

function summarizeWarmupAttempts(attempts: any[], windowSecs: number) {
  const cutoff = NOW() - windowSecs;
  const summary = { success_fresh: 0, success_redundant: 0, transient_failure: 0, permanent_failure: 0, skipped: 0 };
  for (const a of attempts) {
    if (a.attempted_at_unix_secs < cutoff) break;
    if (a.outcome in summary) summary[a.outcome as keyof typeof summary]++;
  }
  return summary;
}

function encodeWarmupCursor(attempt: any): string {
  return Buffer.from(`${attempt.attempted_at_unix_secs}|${attempt.id}`).toString("base64");
}
function decodeWarmupCursor(cursor: string): { ts: number; id: string } | null {
  try {
    const raw = Buffer.from(cursor, "base64").toString("utf-8");
    const [ts, id] = raw.split("|");
    return { ts: Number(ts), id: id ?? "" };
  } catch {
    return null;
  }
}

// ──────────────────────────────────────────────────────────────────────────────
// Route handlers (path-based)

async function handle(req: Request, url: URL): Promise<Response> {
  const path = url.pathname;
  const m = req.method;

  // Health/connection
  if (path === "/admin/health" && m === "GET") {
    return ok({ status: "ok", version: "1.4.0-mock", git_sha: "deadbeef", uptime_secs: NOW() - startedAt });
  }

  // ── Dashboard summary + usage
  if (path === "/admin/dashboard/summary" && m === "GET") {
    const range = url.searchParams.get("range") ?? "1h";
    const sparkline = buildBuckets(range, range === "7d" ? "hour" : "minute", ["all"], NOW(), "none");
    const totals = {
      request_count: 12_345,
      input_tokens: 4_222_111,
      output_tokens: 18_900_232,
      error_count: 142,
      virtual_cost_micros: 312_400_000,
      avg_latency_ms: 218,
    };
    return ok({ range, step: sparkline.step, window_start_unix_secs: sparkline.window_start_unix_secs, window_end_unix_secs: sparkline.window_end_unix_secs, totals, sparkline: { buckets: sparkline.series[0]!.buckets }, observed: true });
  }
  if (path === "/admin/usage" && m === "GET") {
    const range = url.searchParams.get("range") ?? "1h";
    const step = url.searchParams.get("step") ?? (range === "7d" ? "hour" : "minute");
    const groupBy = url.searchParams.get("group_by") ?? "none";
    const upstreamId = url.searchParams.get("upstream_id");
    let groups: string[] = ["all"];
    if (groupBy === "model") {
      groups = upstreamId ? (upstreamModels[upstreamId] ?? []) : MODEL_LIST;
    } else if (groupBy === "principal") {
      groups = principals.map((p) => p.name);
    } else if (groupBy === "upstream") {
      groups = upstreamId ? [upstreams.find((u) => u.id === upstreamId)?.name ?? "unknown"].filter(Boolean) : upstreams.map((u) => u.name);
    }
    return ok(buildBuckets(range, step, groups, NOW(), groupBy, upstreamId || undefined));
  }

  // ── Events recent + SSE stream
  if (path === "/admin/events/recent" && m === "GET") {
    const limit = Math.min(parseInt(url.searchParams.get("limit") ?? "100", 10) || 100, 1000);
    const filter = (e: any) => {
      const pid = url.searchParams.get("principal_id");
      const kid = url.searchParams.get("key_id");
      const ups = url.searchParams.get("upstream");
      const upsId = url.searchParams.get("upstream_id");
      const model = url.searchParams.get("model");
      const status = url.searchParams.get("status");
      if (pid && e.principal_id !== pid) return false;
      if (kid && e.key_id !== kid) return false;
      if (ups && e.upstream !== ups) return false;
      if (upsId && e.upstream_id !== upsId) return false;
      if (model && e.model !== model) return false;
      if (status && String(e.status) !== status) return false;
      return true;
    };
    const filtered = RECENT_EVENTS.filter(filter).slice(0, limit);
    return ok({ events: filtered, observed: true, count: filtered.length, limit });
  }
  if (path === "/admin/events/stream" && m === "GET") {
    let interval: ReturnType<typeof setInterval> | null = null;
    let seq = RECENT_EVENTS.length;
    const stream = new ReadableStream({
      start(controller) {
        const send = () => {
          const ev = generateEvent(++seq);
          if (!ev) return;
          ev.ts = NOW();
          controller.enqueue(new TextEncoder().encode(`data: ${JSON.stringify(ev)}\n\n`));
        };
        send();
        interval = setInterval(send, 1200);
      },
      cancel() { if (interval) clearInterval(interval); },
    });
    return new Response(stream, { headers: { ...CORS, "Content-Type": "text/event-stream", "Cache-Control": "no-cache", "Connection": "keep-alive" } });
  }

  // ── Upstreams CRUD
  if (path === "/admin/v1/upstreams" && m === "GET") return listResponse(upstreams, url, (u) => u.id, "upstreams");
  if (path === "/admin/v1/upstreams" && m === "POST") {
    const body = await readJson<any>(req);
    if (!body.name) return err(400, "invalid_input", "name required");
    if (upstreams.find((u) => u.name === body.name)) return err(409, "conflict", "name already exists");
    const newU = { id: `us-${Date.now().toString(36)}`, name: body.name, kind: body.kind ?? "anthropic_api_key", enabled: true, spec_revision: 1, base_url: body.base_url ?? "https://api.anthropic.com", api_key_env: body.api_key_env ?? null, warmup_enabled: false, warmup_dialect_plugin: null, status: { last_apply_error: null, last_apply_at_unix_secs: null, last_warmup_at_unix_secs: null } };
    upstreams.push(newU);
    return created(newU, { etag: `"${newU.spec_revision}"`, location: `/admin/v1/upstreams/${newU.id}` });
  }
  {
    const mm = path.match(/^\/admin\/v1\/upstreams\/([^/]+)$/);
    if (mm) {
      const id = mm[1]!;
      const u = upstreams.find((x) => x.id === id);
      if (!u) return notFound("upstream_not_found");
      if (m === "GET") return ok(u, { etag: `"${u.spec_revision}"` });
      if (m === "PUT") {
        const body = await readJson<any>(req);
        Object.assign(u, body);
        u.spec_revision++;
        return ok(u, { etag: `"${u.spec_revision}"` });
      }
      if (m === "PATCH") {
        const ifMatch = req.headers.get("If-Match");
        if (!ifMatch) return err(428, "precondition_required", "If-Match header required");
        const expectedEtag = `W/"${u.spec_revision}"`;
        if (ifMatch !== expectedEtag && ifMatch !== `"${u.spec_revision}"`) return err(412, "precondition_failed");
        const body = await readJson<any>(req);
        if (body.enabled !== undefined) u.enabled = body.enabled;
        if (body.warmup_enabled !== undefined) u.warmup_enabled = body.warmup_enabled;
        if (body.warmup_dialect_plugin !== undefined) u.warmup_dialect_plugin = body.warmup_dialect_plugin;
        u.spec_revision++;
        return ok(u, { etag: `"${u.spec_revision}"` });
      }
      if (m === "DELETE") {
        const ix = upstreams.indexOf(u);
        upstreams.splice(ix, 1);
        return ok(null);
      }
    }
  }
  {
    const mm = path.match(/^\/admin\/v1\/upstreams\/([^/]+)\/(enable|disable)$/);
    if (mm && m === "POST") {
      const u = upstreams.find((x) => x.id === mm[1]);
      if (!u) return notFound("upstream_not_found");
      u.enabled = mm[2] === "enable";
      u.spec_revision++;
      return ok(u, { etag: `"${u.spec_revision}"` });
    }
  }
  {
    const mm = path.match(/^\/admin\/v1\/upstreams\/([^/]+)\/warmup\/fire-now$/);
    if (mm && m === "POST") {
      const u = upstreams.find((x) => x.id === mm[1]);
      if (!u) return notFound("upstream_not_found");
      const outcome = url.searchParams.get("mock_outcome") ?? "success";
      const ts = NOW();
      const cycleKeyNow = Math.floor(ts / WARMUP_FIVE_HOUR);
      const makeAttempt = (overrides: Record<string, unknown>) => ({
        id: `wa-${u.id}-fire-${ts}-${Math.floor(Math.random() * 1000)}`,
        upstream_id: u.id,
        attempted_at_unix_secs: ts,
        completed_at_unix_secs: ts + 0.4,
        scheduled_for_unix_secs: ts,
        trigger: "manual",
        outcome: "success_fresh",
        reason: null,
        http_status: null,
        cycle_key: null,
        expected_cycle_key: null,
        idle_secs_since_prev_window: null,
        replica_id: "replica-1",
        lease_holder: null,
        upstream_spec_revision: u.spec_revision,
        dialect_plugin_snapshot: pluginSnapshotFor(u),
        error_detail: null,
        ...overrides,
      });
      if (outcome === "success") {
        prependWarmupAttempt(u.id, makeAttempt({ outcome: "success_fresh", http_status: 200, cycle_key: cycleKeyNow, idle_secs_since_prev_window: 600 }));
        return ok({ fired: true, cycle_key: cycleKeyNow });
      }
      if (outcome === "lease_held") {
        prependWarmupAttempt(u.id, makeAttempt({ outcome: "skipped", reason: "lease_held", lease_holder: "replica-2" }));
        return new Response(JSON.stringify({ fired: false, reason: "lease_held", held_by: "replica-2" }), { status: 202, headers: { "Content-Type": "application/json", ...CORS } });
      }
      if (["auth_failed", "forbidden", "bad_request", "not_found", "dialect_plugin_failed"].includes(outcome)) {
        const httpStatus = outcome === "auth_failed" ? 401 : outcome === "forbidden" ? 403 : outcome === "not_found" ? 404 : outcome === "bad_request" ? 400 : null;
        prependWarmupAttempt(u.id, makeAttempt({ outcome: "permanent_failure", reason: outcome, http_status: httpStatus, error_detail: `Permanent failure: ${outcome}` }));
        return new Response(JSON.stringify({ fired: false, reason: outcome }), { status: 502, headers: { "Content-Type": "application/json", ...CORS } });
      }
      if (outcome === "transient") {
        prependWarmupAttempt(u.id, makeAttempt({ outcome: "transient_failure", reason: "upstream_5xx", http_status: 503, error_detail: "Transient: upstream 5xx" }));
        return new Response(JSON.stringify({ fired: false, reason: "transient" }), { status: 503, headers: { "Content-Type": "application/json", ...CORS } });
      }
      prependWarmupAttempt(u.id, makeAttempt({ outcome: "success_fresh", http_status: 200, cycle_key: cycleKeyNow }));
      return ok({ fired: true, cycle_key: cycleKeyNow });
    }
  }
  {
    const mm = path.match(/^\/admin\/v1\/upstreams\/([^/]+)\/warmup$/);
    if (mm && m === "GET") {
      const u = upstreams.find((x) => x.id === mm[1]);
      if (!u) return notFound("upstream_not_found");
      const attempts = getWarmupAttempts(u.id);
      const last = attempts[0] ?? null;
      const nextScheduled = u.enabled && u.warmup_enabled
        ? NOW() + 60 * 12 + ((u.id.length * 47) % 600)
        : null;
      return ok({
        upstream_id: u.id,
        last_attempt: last,
        recent_attempts: attempts.slice(0, 10),
        next_scheduled_at_unix_secs: nextScheduled,
        recent_summary_7d: summarizeWarmupAttempts(attempts, 7 * 86400),
        dialect_plugin: pluginSnapshotFor(u),
      });
    }
  }
  {
    const mm = path.match(/^\/admin\/v1\/upstreams\/([^/]+)\/warmup\/attempts$/);
    if (mm && m === "GET") {
      const u = upstreams.find((x) => x.id === mm[1]);
      if (!u) return notFound("upstream_not_found");
      const all = getWarmupAttempts(u.id);
      const limit = Math.min(parseInt(url.searchParams.get("limit") ?? "50", 10) || 50, 200);
      const outcomeFilter = url.searchParams.get("outcome");
      const before = url.searchParams.get("before");
      let startIdx = 0;
      if (before) {
        const decoded = decodeWarmupCursor(before);
        if (decoded) {
          const ix = all.findIndex((a) => a.id === decoded.id);
          if (ix >= 0) startIdx = ix + 1;
        }
      }
      const filtered = (outcomeFilter ? all.filter((a) => a.outcome === outcomeFilter) : all).slice(startIdx);
      const slice = filtered.slice(0, limit);
      const next = slice.length === limit ? slice[slice.length - 1] : null;
      return ok({
        attempts: slice,
        next_cursor: next ? encodeWarmupCursor(next) : null,
      });
    }
  }
  {
    const mm = path.match(/^\/admin\/v1\/upstreams\/([^/]+)\/warmup-dialect-plugin$/);
    if (mm && m === "DELETE") {
      const u = upstreams.find((x) => x.id === mm[1]);
      if (!u) return notFound("upstream_not_found");
      const ifMatch = req.headers.get("If-Match");
      if (!ifMatch) return err(428, "precondition_required", "If-Match header required");
      const expectedEtag = `W/"${u.spec_revision}"`;
      if (ifMatch !== expectedEtag && ifMatch !== `"${u.spec_revision}"`) return err(412, "precondition_failed");
      u.warmup_dialect_plugin = null;
      u.spec_revision++;
      return ok(u, { etag: `"${u.spec_revision}"` });
    }
  }
  {
    const mm = path.match(/^\/admin\/v1\/upstreams\/([^/]+)\/oauth\/(start|complete)$/);
    if (mm && m === "POST") {
      const id = mm[1]!;
      const u = upstreams.find((x) => x.id === id);
      if (!u) return notFound("upstream_not_found");
      if (u.kind !== "anthropic_oauth") return err(400, "wrong_kind");
      if (mm[2] === "start") {
        const state = `state_${Math.random().toString(36).slice(2)}`;
        return ok({ authorize_url: `https://api.anthropic.com/oauth/authorize?client_id=mock&state=${state}`, state_token: state });
      }
      return ok({ upstream_id: id, expires_at_unix_secs: NOW() + 3600 * 8, access_token_fingerprint: Math.random().toString(36).slice(2, 10) });
    }
  }

  // ── Principals CRUD
  if (path === "/admin/v1/principals" && m === "GET") {
    const { slice, total } = paginate(principals, url, (p) => p.id);
    return ok({ principals: slice.map(withRouterChain) }, { "x-total-count": String(total), "X-Total-Count": String(total) });
  }
  if (path === "/admin/v1/principals" && m === "POST") {
    const body = await readJson<any>(req);
    if (!body.name) return err(400, "invalid_input", "name required");
    if (principals.find((p) => p.name === body.name)) return err(409, "conflict");
    const np = { id: `pr-${Date.now().toString(36)}`, name: body.name, kind: body.kind ?? "human", enabled: true, revision: 1, allowed_models: body.allowed_models ?? [], allowed_upstreams: body.allowed_upstreams ?? [], default_limits: body.default_limits ?? [] };
    principals.push(np);
    chainEntries.push({ id: `pce-${++chainEntryAutoId}`, principal_id: np.id, slot: "router", order: 0, wasm_registry_id: BUILTIN_CACHE_AFFINITY_ID, config: {}, sse_per_event: false, batched_events_per_flush: 1, batched_flush_ms: 100, revision: 0, wire_version: 3 });
    return created(withRouterChain(np), { etag: `"${np.revision}"`, location: `/admin/v1/principals/${np.id}` });
  }
  {
    const mm = path.match(/^\/admin\/v1\/principals\/([^/]+)$/);
    if (mm) {
      const id = mm[1]!;
      const p = principals.find((x) => x.id === id);
      if (!p) return notFound("unknown_principal");
      if (m === "GET") return ok(withRouterChain(p), { etag: `"${p.revision}"` });
      if (m === "PUT" || m === "PATCH") {
        const body = await readJson<any>(req);
        Object.assign(p, body);
        p.revision++;
        return ok(withRouterChain(p), { etag: `"${p.revision}"` });
      }
      if (m === "DELETE") {
        const ix = principals.indexOf(p);
        principals.splice(ix, 1);
        return ok(null);
      }
    }
  }
  {
    const mm = path.match(/^\/admin\/v1\/principals\/([^/]+)\/(enable|disable)$/);
    if (mm && m === "POST") {
      const p = principals.find((x) => x.id === mm[1]);
      if (!p) return notFound("unknown_principal");
      p.enabled = mm[2] === "enable";
      p.revision++;
      return ok(withRouterChain(p), { etag: `"${p.revision}"` });
    }
  }
  {
    const mm = path.match(/^\/admin\/v1\/principals\/([^/]+)\/allowed_models$/);
    if (mm && m === "PUT") {
      const p = principals.find((x) => x.id === mm[1]);
      if (!p) return notFound("unknown_principal");
      const body = await readJson<any>(req);
      p.allowed_models = body.models ?? [];
      p.revision++;
      return ok(p, { etag: `"${p.revision}"` });
    }
  }
  {
    const mm = path.match(/^\/admin\/v1\/principals\/([^/]+)\/limits$/);
    if (mm && m === "GET") {
      const p = principals.find((x) => x.id === mm[1]);
      if (!p) return notFound("unknown_principal");
      return ok({
        principal_id: p.id, observed: true,
        identities: [
          { identity_kind: "principal", identity_value: p.id, account_observed: true,
            windows: p.default_limits.map((l: any) => ({
              window: `${l.window_secs}s`,
              snapshots: [{
                kind: l.kind,
                limit: l.cap_micros,
                remaining: Math.floor(l.cap_micros * 0.62),
                reset: NOW() + 45,
                observed_at_unix_secs: NOW(),
                stored_at_unix_secs: NOW() - 5,
                observed: true,
              }],
            })),
          },
        ],
      });
    }
  }

  // Principal keys
  {
    const mm = path.match(/^\/admin\/v1\/principals\/([^/]+)\/keys$/);
    if (mm) {
      const pid = mm[1]!;
      if (m === "GET") return ok({ keys: apiKeys.filter((k) => k.principal_id === pid) });
      if (m === "POST") {
        const body = await readJson<any>(req).catch(() => ({}));
        const k = { key_id: `key-${++keyAutoId}`, principal_id: pid, label: body.label ?? null, issued_at_unix_secs: NOW(), revoked_at_unix_secs: null, status: "active", last_4: Math.random().toString(36).slice(2, 6), expires_at_unix_secs: null };
        apiKeys.push(k);
        return created({ principal_id: pid, key_id: k.key_id, plaintext_key: `cclb_${Math.random().toString(36).slice(2)}${Math.random().toString(36).slice(2)}`, issued_at_unix_secs: k.issued_at_unix_secs });
      }
    }
  }
  {
    const mm = path.match(/^\/admin\/v1\/principals\/([^/]+)\/keys\/([^/]+)\/revoke$/);
    if (mm && m === "POST") {
      const k = apiKeys.find((kk) => kk.principal_id === mm[1] && kk.key_id === mm[2]);
      if (!k) return notFound("key_not_found");
      k.revoked_at_unix_secs = NOW();
      k.status = "revoked";
      return ok({ key_id: k.key_id, revoked_at_unix_secs: k.revoked_at_unix_secs });
    }
  }

  // ── Plugins registry
  if (path === "/admin/v1/plugins/registry" && m === "GET") {
    const slot = url.searchParams.get("slot");
    const noPlugins = (globalThis as any).Bun?.env?.MOCK_NO_PLUGINS === "1" || (globalThis as any).process?.env?.MOCK_NO_PLUGINS === "1";
    let filtered = plugins;
    if (slot) {
      filtered = plugins.filter((p) => p.slot === slot);
    }
    if (noPlugins && slot === "shape") {
      filtered = [];
    }
    return listResponse(filtered, url, (p) => p.id, "entries");
  }
  if (path === "/admin/v1/plugins/wasm" && m === "POST") {
    // multipart, but we don't strictly parse — just return mock success
    const id = `pl-${Date.now().toString(36)}`;
    const e = { id, sha256_hex: Math.random().toString(16).slice(2).padEnd(64, "0"), name: `uploaded-${id}`, original_filename: `${id}.wasm`, label: null, size_bytes: 100_000, refcount: 0, revision: 1, uploaded_at_unix_secs: NOW() };
    plugins.push(e);
    return created({ sha256_hex: e.sha256_hex, id: e.id, size_bytes: e.size_bytes, original_filename: e.original_filename, revision: e.revision, idempotent: false }, { location: `/admin/v1/plugins/registry/${id}` });
  }
  if (path === "/admin/v1/plugins/wasm/gc" && m === "POST") {
    const orphans = plugins.filter((p) => p.refcount === 0);
    orphans.forEach((o) => { plugins.splice(plugins.indexOf(o), 1); });
    return ok({ removed: orphans.map((o) => o.id), count: orphans.length });
  }
  {
    const mm = path.match(/^\/admin\/v1\/plugins\/registry\/([^/]+)$/);
    if (mm) {
      const id = mm[1]!;
      const p = plugins.find((x) => x.id === id);
      if (!p) return notFound("unknown_registry_entry");
      if (m === "GET") return ok(p, { etag: `"${p.revision}"` });
      if (m === "PATCH") {
        if (p.is_builtin) return err(409, "builtin_plugin_immutable");
        const body = await readJson<any>(req);
        if (body.label !== undefined) p.label = body.label;
        p.revision++;
        return ok(p, { etag: `"${p.revision}"` });
      }
      if (m === "DELETE") {
        if (p.is_builtin) return err(409, "builtin_plugin_immutable");
        if (p.refcount > 0) return err(409, "referenced_by", `${p.refcount} chain entries reference this plugin`);
        plugins.splice(plugins.indexOf(p), 1);
        return ok(null);
      }
    }
  }

  // ── Plugin chain
  {
    const mm = path.match(/^\/admin\/v1\/principals\/([^/]+)\/router-terminal$/);
    if (mm) {
      const pid = mm[1]!;
      if (m === "GET") {
        return ok(terminalStrategies[pid] ?? { strategy: "first-pick", revision: 1 });
      }
      if (m === "PUT") {
        const body = await readJson<any>(req);
        const next = { strategy: body.strategy, revision: (body.revision ?? 0) + 1 };
        terminalStrategies[pid] = next;
        return ok(next);
      }
    }
  }
  {
    const mm = path.match(/^\/admin\/v1\/principals\/([^/]+)\/plugin-chain$/);
    if (mm) {
      const pid = mm[1]!;
      const slot = url.searchParams.get("slot") as ChainSlot | null;
      if (m === "GET") {
        const entries = chainEntries
          .filter((e) => e.principal_id === pid && (!slot || e.slot === slot))
          .sort((a, b) => a.order - b.order);
        return ok({ entries });
      }
      if (m === "POST") {
        const body = await readJson<any>(req);
        const slotKey: ChainSlot = body.slot;
        const sameSlot = chainEntries.filter((e) => e.principal_id === pid && e.slot === slotKey);
        const order = sameSlot.length + 1;
        const newE = { id: `pce-${++chainEntryAutoId}`, principal_id: pid, slot: slotKey, order, wasm_registry_id: body.wasm_registry_id, config: body.config ?? {}, sse_per_event: body.sse_per_event ?? false, batched_events_per_flush: body.batched_events_per_flush ?? 0, batched_flush_ms: body.batched_flush_ms ?? 0, revision: 1 };
        chainEntries.push(newE);
        const reg = plugins.find((p) => p.id === body.wasm_registry_id);
        if (reg) reg.refcount = (reg.refcount ?? 0) + 1;
        return created(newE, { etag: `"${newE.revision}"`, location: `/admin/v1/plugin-chain-entries/${newE.id}` });
      }
    }
  }
  {
    const mm = path.match(/^\/admin\/v1\/principals\/([^/]+)\/plugin-chain\/reorder$/);
    if (mm && m === "POST") {
      const body = await readJson<any>(req);
      for (const upd of body.entries ?? []) {
        const e = chainEntries.find((x) => x.id === upd.id);
        if (e) { e.order = upd.order; e.revision++; }
      }
      return ok({ entries: chainEntries.filter((e) => e.principal_id === mm[1]) });
    }
  }
  {
    const mm = path.match(/^\/admin\/v1\/plugin-chain-entries\/([^/]+)$/);
    if (mm) {
      const e = chainEntries.find((x) => x.id === mm[1]);
      if (!e) return notFound("unknown_plugin_chain_entry");
      if (m === "PUT") {
        const body = await readJson<any>(req);
        if (body.config !== undefined) e.config = body.config;
        if (body.sse_per_event !== undefined) e.sse_per_event = body.sse_per_event;
        if (body.batched_events_per_flush !== undefined) e.batched_events_per_flush = body.batched_events_per_flush;
        if (body.batched_flush_ms !== undefined) e.batched_flush_ms = body.batched_flush_ms;
        e.revision++;
        return ok(e, { etag: `"${e.revision}"` });
      }
      if (m === "DELETE") {
        const reg = plugins.find((p) => p.id === e.wasm_registry_id);
        if (reg && reg.refcount > 0) reg.refcount--;
        chainEntries.splice(chainEntries.indexOf(e), 1);
        return ok(null);
      }
    }
  }

  // ── Status (full)
  if (path === "/admin/v1/status" && m === "GET") {
    return ok({
      version: "1.4.0-mock", git_sha: "deadbeef", uptime_secs: NOW() - startedAt,
      build: { rust_version: "1.85.0", profile: "release", target: "x86_64-unknown-linux-musl" },
      generation: 14,
      upstreams: upstreams.map((u) => ({ id: u.id, name: u.name, status: u.enabled ? "healthy" : "disabled", last_apply_at_unix_secs: NOW() - 30, last_apply_error: null })),
      principals: principals.map((p) => ({ id: p.id, name: p.name, enabled: p.enabled, last_apply_error: null })),
      plugin_chain_summary: { principal_count_with_chain: new Set(chainEntries.map((e) => e.principal_id)).size, total_entries: chainEntries.length },
      killswitch,
      last_reload_status: { ok: true, applied_revision: configRevision, applied_at_unix_secs: NOW() - 600 },
      restart_required_changes: [],
    });
  }
  if (path === "/admin/v1/export" && m === "GET") {
    return ok({
      exported_at_unix_secs: NOW(), schema_version: 2,
      upstreams: upstreams.map((u) => ({ name: u.name, kind: u.kind, enabled: u.enabled, oauth_credentials_present: u.kind === "anthropic_oauth", expires_at_unix_secs: u.kind === "anthropic_oauth" ? NOW() + 3600 * 8 : null, spec_revision: u.spec_revision })),
      principals: principals.map((p) => ({ name: p.name, kind: p.kind, allowed_models: p.allowed_models, default_limits: p.default_limits, enabled: p.enabled, revision: p.revision })),
      plugins: {
        registry: plugins.map((p) => ({ sha256_hex: p.sha256_hex, name: p.name, original_filename: p.original_filename, label: p.label, size_bytes: p.size_bytes, refcount: p.refcount })),
        chains: principals.reduce((acc, p) => {
          acc[p.name] = {
            Router: chainEntries.filter((e) => e.principal_id === p.id && e.slot === "router").map((e) => ({ wasm_registry_id: e.wasm_registry_id, config: e.config, sse_per_event: e.sse_per_event })),
            ObservabilityHook: chainEntries.filter((e) => e.principal_id === p.id && e.slot === "observability_hook").map((e) => ({ wasm_registry_id: e.wasm_registry_id, config: e.config, sse_per_event: e.sse_per_event })),
            Shape: chainEntries.filter((e) => e.principal_id === p.id && e.slot === "shape").map((e) => ({ wasm_registry_id: e.wasm_registry_id, config: e.config, sse_per_event: e.sse_per_event })),
          };
          return acc;
        }, {} as Record<string, any>),
      },
    });
  }

  // ── Audit log
  if (path === "/admin/audit" && m === "GET") {
    const limit = Math.min(parseInt(url.searchParams.get("limit") ?? "100", 10) || 100, 1000);
    const pid = url.searchParams.get("principal_id");
    const since = parseInt(url.searchParams.get("since") ?? "0", 10);
    const until = parseInt(url.searchParams.get("until") ?? "0", 10) || NOW();
    const filtered = RECENT_EVENTS.filter((e) => (!pid || e.principal_id === pid) && e.ts >= since && e.ts <= until).slice(0, limit);
    return ok({ entries: filtered.map((e) => ({ ts: e.ts, request_id: e.request_id, principal_id: e.principal_id, route: e.route, upstream: e.upstream, model: e.model, status: e.status, input_tokens: e.input_tokens, output_tokens: e.output_tokens, duration_ms: e.duration_ms, agent_label: e.agent_label, kind: e.kind, payload: e.payload })) });
  }

  // ── Plugin status (live slot info)
  if (path === "/admin/status" && m === "GET") {
    const principalsMap: Record<string, any> = {};
    for (const p of principals) {
      const routerEntries = chainEntries.filter((e) => e.principal_id === p.id && e.slot === "router");
      principalsMap[p.id] = {
        router_plugin: routerEntries[0]?.wasm_registry_id ?? null,
        observability_hooks: chainEntries.filter((e) => e.principal_id === p.id && e.slot === "observability_hook").map((e) => ({ wasm_registry_id: e.wasm_registry_id, sse_per_event: e.sse_per_event })),
      };
    }
    return ok({
      plugins: plugins.map((p) => ({ slot: chainEntries.find((e) => e.wasm_registry_id === p.id)?.slot ?? "router", name: p.name, wasm_path: `registry://${p.id}`, loaded: true, disabled: false, failure_count: 0, last_error: null, sse_per_event: false, batched_events_per_flush: 0, batched_flush_ms: 0 })),
      principals: principalsMap,
      last_reload_status: { ok: true, applied_revision: configRevision, applied_at_unix_secs: NOW() - 600 },
    });
  }

  // ── Killswitch
  if (path === "/admin/killswitch" && m === "POST") { killswitch = true; return ok({ status: "ok", killswitch: true }); }
  if (path === "/admin/killswitch" && m === "DELETE") { killswitch = false; return ok({ status: "ok", killswitch: false }); }

  // ── Config draft/apply pipeline
  if (path === "/admin/config/current" && m === "GET") {
    return ok({ listener: { proxy_addr: "0.0.0.0:52251", admin_addr: "0.0.0.0:52252", metrics_addr: "0.0.0.0:52253" }, storage: { kind: "postgres", url: "postgres://***@db.internal:5432/cclb" }, aead: { key_env: "CC_LB_AEAD_KEY" }, oauth: { anthropic: { client_id: "***" } } });
  }
  if (path === "/admin/config/schema" && m === "GET") {
    return ok({ schema: { type: "object", required: ["listener", "storage"] }, coverage_checklist: ["listener.proxy_addr", "listener.admin_addr", "storage.url", "aead.key_env", "oauth.anthropic.client_id"] });
  }
  if (path === "/admin/config/draft" && m === "GET") return ok(configDraft);
  if (path === "/admin/config/draft" && m === "PUT") {
    const body = await readJson<any>(req);
    if (body.expected_revision !== configDraft.revision) return err(409, "stale_draft_revision");
    configDraft = { draft: body.draft ?? null, revision: configDraft.revision + 1, last_validated_revision: null, last_validation_error: null, saved_at_unix_secs: NOW() };
    return ok({ revision: configDraft.revision, saved_at_unix_secs: configDraft.saved_at_unix_secs });
  }
  if (path === "/admin/config/draft/validate" && m === "POST") {
    const body = await readJson<any>(req);
    if (!configDraft.draft) return err(409, "draft_missing");
    if (body.expected_revision !== configDraft.revision) return err(409, "stale_draft_revision");
    configDraft.last_validated_revision = configDraft.revision;
    configDraft.last_validation_error = null;
    return ok({ valid: true, revision: configDraft.revision });
  }
  if (path === "/admin/config/apply" && m === "POST") {
    const body = await readJson<any>(req);
    if (body.expected_revision !== configDraft.last_validated_revision) return err(409, "unvalidated_revision");
    configRevision++;
    const applied = { revision: configRevision, applied_at_unix_secs: NOW(), config_summary: { upstreams: upstreams.length, principals: principals.length, plugin_count: plugins.length, tls_enabled: true } };
    configHistory.unshift(applied as any);
    configDraft = { draft: null, revision: configDraft.revision + 1, last_validated_revision: null, last_validation_error: null, saved_at_unix_secs: null };
    return ok({ applied_revision: configRevision, applied_at_unix_secs: applied.applied_at_unix_secs });
  }
  if (path === "/admin/config/history" && m === "GET") {
    const limit = Math.min(parseInt(url.searchParams.get("limit") ?? "20", 10) || 20, 200);
    return ok({ history: configHistory.slice(0, limit) });
  }
  if (path === "/admin/config/diff" && m === "GET") {
    const from = parseInt(url.searchParams.get("from_revision") ?? "0", 10);
    const to = parseInt(url.searchParams.get("to_revision") ?? "0", 10);
    return ok({ from, to, diff: [{ path: "listener.proxy_addr", from: "0.0.0.0:52250", to: "0.0.0.0:52251" }, { path: "storage.url", from: "sqlite:///var/lib/cclb.sqlite", to: "postgres://...@db" }] });
  }
  if (path === "/admin/config/reload" && m === "POST") return ok({ status: "ok", reloading: true });

  // ── Credentials / OAuth status
  if (path === "/admin/credentials" && m === "GET") {
    return ok({
      credentials: upstreams.map((u) => ({ principal_id: null, provider: "anthropic", kind: u.kind === "anthropic_oauth" ? "oauth" : "api_key", identity: u.kind === "anthropic_oauth" ? "claude-code-token" : u.api_key_env, associated_principals: principals.filter((p) => p.allowed_upstreams?.includes(u.id)).map((p) => p.id), has_credentials: true, expires_at_unix_secs: u.kind === "anthropic_oauth" ? NOW() + 3600 * 8 : null, status: "active" })),
      observed: true,
    });
  }
  if (path === "/admin/oauth/status" && m === "GET") {
    return ok({
      credentials: upstreams.filter((u) => u.kind === "anthropic_oauth").map(() => ({ principal_id: null, provider: "anthropic", has_credentials: true, expires_at_unix_secs: NOW() + 3600 * 8, refresh_token_present: true, last_updated_unix_secs: NOW() - 1800, status: "active", scopes: ["org:create_api_key", "user:profile", "user:inference"] })),
      observed: true,
    });
  }
  {
    const mm = path.match(/^\/admin\/credentials\/([^/]+)\/([^/]+)\/(rotate|revoke)$/);
    if (mm && m === "POST") {
      const provider = mm[1];
      const cred_id = mm[2];
      if (mm[3] === "rotate") return err(501, "rotate_unsupported");
      return ok({ provider, cred_id, kind: "oauth", revoked_keys: [`key-revoked-${Date.now().toString(36)}`] });
    }
  }

  if (path === "/admin/v1/subscription-quotas/latest" && m === "GET") {
    const now = NOW();
    return ok({
      now_unix_secs: now,
      max_staleness_secs: 300,
      upstreams: upstreams.filter((u) => u.kind === "anthropic_oauth" && u.enabled).map((u) => ({
        upstream_id: u.id,
        upstream_name: u.name,
        windows: ["5h", "7d", "7d_sonnet"].map((win, idx) => ({
          window: win,
          state: "fresh",
          source: "merged",
          utilization: 0.25 + idx * 0.15,
          status: "ok",
          resets_at_unix_secs: now + (win === "5h" ? 3600 : 7 * 24 * 3600),
          surpassed_threshold: false,
          representative_claim: null,
          disabled_reason: null,
          extra_usage_enabled: false,
          extra_usage_monthly_limit: null,
          extra_usage_used_credits: null,
          observed_at_unix_millis: now * 1000,
          age_secs: 10,
        })),
      })),
    });
  }
  if (path === "/admin/v1/subscription-quotas/series" && m === "GET") {
    const now = NOW();
    const range = url.searchParams.get("range") || "7d";
    const rangeSecs = range === "24h" ? 24 * 3600 : range === "6h" ? 6 * 3600 : range === "1h" ? 3600 : 7 * 24 * 3600;
    const bucketSecs = range === "1h" ? 60 : range === "6h" ? 300 : range === "24h" ? 1800 : 3600;
    const nBuckets = Math.min(Math.floor(rangeSecs / bucketSecs), 200);
    const targets = upstreams.filter((u) => u.kind === "anthropic_oauth" && u.enabled);
    return ok({
      since_unix_secs: now - rangeSecs,
      until_unix_secs: now,
      bucket_secs: bucketSecs,
      source: "merged",
      series: targets.flatMap((u) => ["5h", "7d", "7d_sonnet"].map((win, wi) => ({
        upstream_id: u.id,
        upstream_name: u.name,
        window: win,
        buckets: Array.from({ length: nBuckets }, (_, i) => {
          const ts = now - (nBuckets - i) * bucketSecs;
          const base = 0.2 + wi * 0.12;
          const wave = Math.sin((i / nBuckets) * Math.PI * 2) * 0.18;
          const util = Math.max(0.02, Math.min(0.96, base + wave + (i / nBuckets) * 0.15));
          return {
            bucket_start_unix_secs: ts,
            observed: true,
            sample_count: 1,
            utilization_min: util,
            utilization_avg: util,
            utilization_max: util,
            utilization_last: util,
            status_last: "ok",
            resets_at_unix_secs_last: now + (win === "5h" ? 3600 : 7 * 24 * 3600),
            observed_at_unix_millis_last: ts * 1000,
            sources_seen: ["merged"],
          };
        }),
        markers: [],
      }))),
    });
  }
  if (path === "/admin/v1/subscription-quotas/aggregate" && m === "GET") {
    const now = NOW();
    const enabledOAuth = upstreams.filter((u) => u.kind === "anthropic_oauth" && u.enabled);
    const upstreamCount = 3; // Mocking 3 upstreams
    
    const providerLots5h = [
      {
        upstream_id: "us-bear-max",
        upstream_name: "bear-max",
        window: "5h",
        source: "header",
        state: "fresh",
        provider_start_unix_secs: now - 5 * 3600,
        provider_reset_unix_secs: now + 4.3 * 3600,
        observed_at_unix_millis: (now - 30) * 1000,
        utilization: 0.10,
        capacity_estimate_tokens: Math.floor(1500000 * (20 / 45)),
        used_before_cc_window_tokens: 0,
        capacity_to_now_tokens_estimate: Math.floor(1500000 * (20 / 45)),
        projected_capacity_tokens_estimate: Math.floor(1500000 * (20 / 45)),
        confidence: "high",
        capacity_ratio: 20.0,
      },
      {
        upstream_id: "us-bh322yoo-max",
        upstream_name: "bh322yoo-max",
        window: "5h",
        source: "header",
        state: "fresh",
        provider_start_unix_secs: now - 5 * 3600,
        provider_reset_unix_secs: now + 0.6 * 3600,
        observed_at_unix_millis: (now - 25) * 1000,
        utilization: 0.16,
        capacity_estimate_tokens: Math.floor(1500000 * (5 / 45)),
        used_before_cc_window_tokens: 0,
        capacity_to_now_tokens_estimate: Math.floor(1500000 * (5 / 45)),
        projected_capacity_tokens_estimate: Math.floor(1500000 * (5 / 45)),
        confidence: "high",
        capacity_ratio: 5.0,
      },
      {
        upstream_id: "us-isac-personal",
        upstream_name: "isac-personal",
        window: "5h",
        source: "header",
        state: "fresh",
        provider_start_unix_secs: now - 5 * 3600,
        provider_reset_unix_secs: now + 2.1 * 3600,
        observed_at_unix_millis: (now - 28) * 1000,
        utilization: 0.0,
        capacity_estimate_tokens: Math.floor(1500000 * (20 / 45)),
        used_before_cc_window_tokens: 0,
        capacity_to_now_tokens_estimate: Math.floor(1500000 * (20 / 45)),
        projected_capacity_tokens_estimate: Math.floor(1500000 * (20 / 45)),
        confidence: "high",
        capacity_ratio: 20.0,
      }
    ];

    const providerLots7d = [
      {
        upstream_id: "us-bear-max",
        upstream_name: "bear-max",
        window: "7d",
        source: "header",
        state: "fresh",
        provider_start_unix_secs: now - 7 * 24 * 3600,
        provider_reset_unix_secs: now + 1.6 * 24 * 3600,
        observed_at_unix_millis: (now - 30) * 1000,
        utilization: 0.78,
        capacity_estimate_tokens: Math.floor(14000000 * (20 / 45)),
        used_before_cc_window_tokens: 0,
        capacity_to_now_tokens_estimate: Math.floor(14000000 * (20 / 45)),
        projected_capacity_tokens_estimate: Math.floor(14000000 * (20 / 45)),
        confidence: "high",
        capacity_ratio: 20.0,
      },
      {
        upstream_id: "us-bh322yoo-max",
        upstream_name: "bh322yoo-max",
        window: "7d",
        source: "header",
        state: "fresh",
        provider_start_unix_secs: now - 7 * 24 * 3600,
        provider_reset_unix_secs: now + 4 * 24 * 3600,
        observed_at_unix_millis: (now - 25) * 1000,
        utilization: 0.05,
        capacity_estimate_tokens: Math.floor(14000000 * (5 / 45)),
        used_before_cc_window_tokens: 0,
        capacity_to_now_tokens_estimate: Math.floor(14000000 * (5 / 45)),
        projected_capacity_tokens_estimate: Math.floor(14000000 * (5 / 45)),
        confidence: "high",
        capacity_ratio: 5.0,
      },
      {
        upstream_id: "us-isac-personal",
        upstream_name: "isac-personal",
        window: "7d",
        source: "header",
        state: "fresh",
        provider_start_unix_secs: now - 7 * 24 * 3600,
        provider_reset_unix_secs: now + 1.5 * 24 * 3600,
        observed_at_unix_millis: (now - 28) * 1000,
        utilization: 0.69,
        capacity_estimate_tokens: Math.floor(14000000 * (20 / 45)),
        used_before_cc_window_tokens: 0,
        capacity_to_now_tokens_estimate: Math.floor(14000000 * (20 / 45)),
        projected_capacity_tokens_estimate: Math.floor(14000000 * (20 / 45)),
        confidence: "high",
        capacity_ratio: 20.0,
      }
    ];

    const util5h = (0.10 * 20 + 0.16 * 5 + 0.0 * 20) / 45;
    const util7d = (0.78 * 20 + 0.05 * 5 + 0.69 * 20) / 45;

    const windowSpec = [
      { window: "5h", utilization: util5h, capacity: 1_500_000, secs: 5 * 3600, lots: providerLots5h },
      { window: "7d", utilization: util7d, capacity: 14_000_000, secs: 7 * 24 * 3600, lots: providerLots7d },
    ];
    return ok({
      now_unix_secs: now,
      window_anchor_unix_secs: now,
      max_staleness_secs: 300,
      upstream_count: upstreamCount,
      windows: windowSpec.map((w) => ({
        window: w.window,
        cc_window_start_unix_secs: now - w.secs,
        cc_window_reset_unix_secs: now + w.secs,
        used_tokens: Math.floor(w.capacity * w.utilization),
        utilization: w.utilization,
        utilization_percent: w.utilization * 100,
        capacity_to_now_tokens_estimate: w.capacity,
        projected_capacity_tokens_estimate: w.capacity,
        remaining_to_now_tokens_estimate: Math.floor(w.capacity * (1 - w.utilization)),
        confidence: "high",
        contributing_upstreams: upstreamCount,
        stale_upstreams: 0,
        missing_capacity_upstreams: 0,
        provider_lots: w.lots,
        caveats: [],
      })),
      caveats: [],
    });
  }
  if (path === "/admin/v1/subscription-quotas/pool-history" && m === "GET") {
    const now = NOW();
    const windowsParam = url.searchParams.get("windows") || "5h,7d";
    const windows = windowsParam.split(",");
    const since = parseInt(url.searchParams.get("since_unix_secs") || String(now - 3600), 10);
    const until = parseInt(url.searchParams.get("until_unix_secs") || String(now), 10);
    
    const seriesData = windows.map(win => {
      const is7d = win === "7d";
      const baseUtil = is7d ? 0.60 : 0.15;
      const points = [];
      for (let t = since; t <= until; t += 60) {
        // Random walk around baseUtil
        const noise = (Math.sin(t / 300) + Math.cos(t / 700)) * 0.05;
        const util = Math.max(0, Math.min(1, baseUtil + noise));
        points.push({
          snapshot_at_unix_secs: t,
          utilization: util,
          utilization_percent: util * 100,
          contributing_upstreams: 3,
          eligible_upstreams: 3,
          stale_upstreams: 0,
          max_observed_at_unix_millis: (t - 30) * 1000,
        });
      }
      return {
        window: win,
        latest: points[points.length - 1] || null,
        series: points,
      };
    });

    return ok({
      now_unix_secs: now,
      windows: seriesData,
    });
  }
  if (path === "/admin/v1/subscription-quotas/analysis" && m === "GET") {
    const now = NOW();
    const range = url.searchParams.get("range") || "7d";
    const rangeSecs = range === "24h" ? 24 * 3600 : range === "6h" ? 6 * 3600 : range === "1h" ? 3600 : 7 * 24 * 3600;
    return ok({
      since_unix_secs: now - rangeSecs,
      until_unix_secs: now,
      now_unix_secs: now,
      max_staleness_secs: 300,
      upstreams: [],
    });
  }

  return notFound(`unhandled:${m}:${path}`);
}

// ──────────────────────────────────────────────────────────────────────────────
const server = serve({
  port: 8001,
  hostname: "0.0.0.0",
  idleTimeout: 0,
  async fetch(req) {
    if (req.method === "OPTIONS") return new Response(null, { headers: CORS });
    const url = new URL(req.url);
    try {
      return await handle(req, url);
    } catch (e: any) {
      console.error("[mock]", req.method, url.pathname, e);
      return err(500, "internal_error", String(e?.message ?? e));
    }
  },
});

console.log(`Mock server running at http://0.0.0.0:${server.port}`);
