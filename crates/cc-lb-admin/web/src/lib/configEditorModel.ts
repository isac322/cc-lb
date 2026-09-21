import type {
  ConfigEditorResponse,
  ConfigOverrideInfo,
  ConfigValidationIssue,
} from './api';

export type ConfigPathSegment = string | number;
export type ConfigPath = readonly ConfigPathSegment[];
export type ConfigSchema = Record<string, unknown>;

export const OPAQUE_STORAGE_URL_SENTINEL =
  '__CC_LB_STORAGE_URL_UNCHANGED__' as const;

export type ConfigEditorLeafKind =
  | 'string'
  | 'integer'
  | 'number'
  | 'boolean'
  | 'enum'
  | 'array'
  | 'unknown';

export interface ConfigEditorCategoryMetadata {
  id: string;
  label: string;
  description: string;
  roots: readonly string[];
}

export const CONFIG_EDITOR_CATEGORIES = [
  {
    id: 'network',
    label: 'Network & requests',
    description: 'Listeners, request bodies, and request deadlines.',
    roots: ['listener', 'body', 'timeouts'],
  },
  {
    id: 'data',
    label: 'Storage & data',
    description: 'Primary storage, encryption, event transport, and retention.',
    roots: ['storage', 'aead', 'event_bus', 'request_event_retention_days'],
  },
  {
    id: 'scheduling',
    label: 'Scheduling',
    description: 'Worker pools, recurring jobs, and scheduler concurrency.',
    roots: ['scheduler'],
  },
  {
    id: 'routing',
    label: 'Routing & resilience',
    description:
      'Affinity, circuit breaking, bulkheads, caching, and reservations.',
    roots: [
      'upstream_affinity',
      'circuit_breaker',
      'bulkhead',
      'prompt_cache_shadow',
      'limit_reservation_ttl',
    ],
  },
  {
    id: 'identity',
    label: 'Identity & access',
    description: 'Admin authentication, OAuth, and cluster identity.',
    roots: ['admin', 'oauth', 'cluster'],
  },
  {
    id: 'quota',
    label: 'Pricing & quotas',
    description: 'Price catalog and subscription quota collection.',
    roots: ['price_catalog', 'subscription_quota'],
  },
  {
    id: 'runtime',
    label: 'Runtime & observability',
    description: 'Process runtime and observability behavior.',
    roots: ['runtime', 'observability'],
  },
] as const satisfies readonly ConfigEditorCategoryMetadata[];
/**
 * Glob-style path matcher. `*` matches exactly one path segment (including
 * array indexes and map keys), `**` as a whole segment matches zero or more
 * segments, and `*` inside a segment matches any characters (`*_secs`).
 */
export type ConfigPathMatcher = string;

export interface ConfigEditorDanger {
  path: ConfigPathMatcher;
  impact: string;
}

export interface ConfigEditorSectionMetadata {
  id: string;
  categoryId: string;
  label: string;
  description: string;
  paths: readonly ConfigPathMatcher[];
  advancedPaths: readonly ConfigPathMatcher[];
  danger: readonly ConfigEditorDanger[];
}

export const CONFIG_EDITOR_UNASSIGNED_SECTION_ID = 'other' as const;
export const CONFIG_EDITOR_UNASSIGNED_LABEL = 'Other settings' as const;

export const CONFIG_EDITOR_SECTIONS = [
  {
    id: 'listener-endpoints',
    categoryId: 'network',
    label: 'Listener endpoints',
    description: 'Bind addresses for the proxy, admin, and metrics listeners.',
    paths: ['listener.*'],
    advancedPaths: [],
    danger: [
      {
        path: 'listener.admin_addr',
        impact:
          'Changing the admin bind address can cut off access to this console.',
      },
      {
        path: 'listener.proxy_addr',
        impact:
          'Changing the proxy bind address moves every client-facing endpoint.',
      },
    ],
  },
  {
    id: 'tls',
    categoryId: 'network',
    label: 'TLS',
    description: 'Certificate, key, and SIGHUP reload behavior.',
    paths: ['listener.tls.**'],
    advancedPaths: [],
    danger: [
      {
        path: 'listener.tls.*_path',
        impact:
          'A wrong certificate or key path prevents the proxy listener from starting.',
      },
    ],
  },
  {
    id: 'request-body-limits',
    categoryId: 'network',
    label: 'Request body limits',
    description: 'Caps on inbound request payload sizes.',
    paths: ['body.*'],
    advancedPaths: [],
    danger: [],
  },
  {
    id: 'timeouts',
    categoryId: 'network',
    label: 'Timeouts',
    description: 'Upstream request and shutdown drain deadlines.',
    paths: ['timeouts.*'],
    advancedPaths: [],
    danger: [],
  },
  {
    id: 'primary-storage',
    categoryId: 'data',
    label: 'Primary storage',
    description: 'Storage backend, connection URL, and database file.',
    paths: ['storage.*'],
    advancedPaths: [],
    danger: [
      {
        path: 'storage.kind',
        impact:
          'Switching the storage backend does not migrate data; existing state stays in the old backend.',
      },
      {
        path: 'storage.url',
        impact:
          'A wrong storage URL prevents the proxy from reaching its state store.',
      },
      {
        path: 'storage.path',
        impact:
          'A wrong database path prevents the proxy from finding its state store.',
      },
    ],
  },
  {
    id: 'database-pool',
    categoryId: 'data',
    label: 'Database pool',
    description: 'PostgreSQL connection pool sizing and lifecycle.',
    paths: ['storage.pool.max_connections', 'storage.pool.min_connections'],
    advancedPaths: ['storage.pool.**'],
    danger: [],
  },
  {
    id: 'encryption',
    categoryId: 'data',
    label: 'Encryption',
    description: 'Key used to encrypt stored secrets.',
    paths: ['aead.*'],
    advancedPaths: [],
    danger: [
      {
        path: 'aead.key_env',
        impact:
          'A wrong key variable makes previously stored secrets undecryptable.',
      },
    ],
  },
  {
    id: 'event-transport',
    categoryId: 'data',
    label: 'Event transport',
    description: 'How storage change events reach running instances.',
    paths: [
      'event_bus.broadcast_capacity',
      'event_bus.pg_notify_channel',
      'event_bus.storage_tail_poll_interval_ms',
    ],
    advancedPaths: [],
    danger: [],
  },
  {
    id: 'retention',
    categoryId: 'data',
    label: 'Retention',
    description: 'How long request events and partial records are kept.',
    paths: [
      'request_event_retention_days',
      'event_bus.partial_retention_ttl_secs',
      'event_bus.partial_retention_max_entries',
    ],
    advancedPaths: [],
    danger: [],
  },
  {
    id: 'recurring-jobs',
    categoryId: 'scheduling',
    label: 'Recurring jobs',
    description: 'Per-job enablement and cadence.',
    paths: ['scheduler.recurring_jobs.**'],
    advancedPaths: [],
    danger: [],
  },
  {
    id: 'concurrency',
    categoryId: 'scheduling',
    label: 'Concurrency',
    description: 'Scheduler parallelism limits.',
    paths: ['scheduler.*_concurrency'],
    advancedPaths: [],
    danger: [],
  },
  {
    id: 'dead-letter-retention',
    categoryId: 'scheduling',
    label: 'Dead-letter retention',
    description: 'How long dead-lettered work is kept.',
    paths: ['scheduler.dlq_retention_days'],
    advancedPaths: [],
    danger: [],
  },
  {
    id: 'separate-scheduler-pool',
    categoryId: 'scheduling',
    label: 'Separate scheduler pool',
    description: 'Dedicated PostgreSQL pool for scheduled work.',
    paths: [
      'scheduler.separate_pool.max_connections',
      'scheduler.separate_pool.min_connections',
    ],
    advancedPaths: ['scheduler.separate_pool.**'],
    danger: [],
  },
  {
    id: 'affinity',
    categoryId: 'routing',
    label: 'Affinity',
    description: 'How long upstream affinity is remembered.',
    paths: ['upstream_affinity.*'],
    advancedPaths: [],
    danger: [],
  },
  {
    id: 'circuit-breaker',
    categoryId: 'routing',
    label: 'Circuit breaker',
    description: 'Failure thresholds that stop traffic to an upstream.',
    paths: ['circuit_breaker.*'],
    advancedPaths: [],
    danger: [],
  },
  {
    id: 'bulkhead',
    categoryId: 'routing',
    label: 'Bulkhead',
    description: 'Per-upstream concurrency isolation.',
    paths: ['bulkhead.*'],
    advancedPaths: [],
    danger: [],
  },
  {
    id: 'prompt-cache-shadow',
    categoryId: 'routing',
    label: 'Prompt cache shadow',
    description: 'Shared observation store used for cache-aware routing.',
    paths: ['prompt_cache_shadow.*'],
    advancedPaths: [],
    danger: [],
  },
  {
    id: 'limit-reservations',
    categoryId: 'routing',
    label: 'Limit reservations',
    description: 'Sweeper that refunds stale limit reservations.',
    paths: ['limit_reservation_ttl.*'],
    advancedPaths: [],
    danger: [],
  },
  {
    id: 'admin-auth-providers',
    categoryId: 'identity',
    label: 'Admin auth providers',
    description: 'How administrators authenticate to this console.',
    paths: ['admin.**'],
    advancedPaths: [],
    danger: [
      {
        path: 'admin.**',
        impact:
          'A broken provider list can lock every administrator out of this console.',
      },
    ],
  },
  {
    id: 'anthropic-oauth',
    categoryId: 'identity',
    label: 'Anthropic OAuth',
    description: 'OAuth client used for Anthropic sign-in.',
    paths: ['oauth.**'],
    advancedPaths: [],
    danger: [],
  },
  {
    id: 'cluster-identity',
    categoryId: 'identity',
    label: 'Cluster identity',
    description: 'Instance URL and shared token for cluster membership.',
    paths: ['cluster.*'],
    advancedPaths: [],
    danger: [
      {
        path: 'cluster.token_env',
        impact:
          'A wrong token variable breaks authentication between cluster instances.',
      },
    ],
  },
  {
    id: 'price-catalog',
    categoryId: 'quota',
    label: 'Price catalog',
    description: 'Model price catalog source and cache location.',
    paths: ['price_catalog.*'],
    advancedPaths: [],
    danger: [],
  },
  {
    id: 'subscription-quota-freshness',
    categoryId: 'quota',
    label: 'Subscription quota freshness',
    description: 'How fresh subscription quota data must be for routing.',
    paths: ['subscription_quota.routing_max_staleness_secs'],
    advancedPaths: [],
    danger: [],
  },
  {
    id: 'writer-pipeline',
    categoryId: 'quota',
    label: 'Writer pipeline',
    description: 'Batching for the subscription quota writer.',
    paths: ['subscription_quota.writer_*'],
    advancedPaths: [],
    danger: [],
  },
  {
    id: 'observability',
    categoryId: 'runtime',
    label: 'Observability',
    description: 'Tracing level, telemetry endpoint, and log redaction.',
    paths: ['observability.*'],
    advancedPaths: [],
    danger: [
      {
        path: 'observability.log_redaction',
        impact:
          'Disabling log redaction can write credentials and tokens to logs.',
      },
      {
        path: 'observability.user_prompt_redaction',
        impact:
          'Disabling prompt redaction stores user prompt text in plain form.',
      },
    ],
  },
  {
    id: 'runtime-paths',
    categoryId: 'runtime',
    label: 'Runtime paths',
    description: 'Filesystem locations the runtime uses.',
    paths: ['runtime.*'],
    advancedPaths: [],
    danger: [],
  },
  {
    id: 'wasmtime-behavior',
    categoryId: 'runtime',
    label: 'Wasmtime behavior',
    description: 'Plugin allocation strategy and origin policy.',
    paths: [
      'runtime.wasmtime.allocation_strategy',
      'runtime.wasmtime.shape_origin_policy',
      'runtime.wasmtime.cookie_redaction',
    ],
    advancedPaths: [],
    danger: [],
  },
  {
    id: 'wasmtime-memory',
    categoryId: 'runtime',
    label: 'Wasmtime memory',
    description: 'Memory limits and pooling for plugin instances.',
    paths: ['runtime.wasmtime.memory_*', 'runtime.wasmtime.pool_total_*'],
    advancedPaths: [],
    danger: [],
  },
  {
    id: 'wire-bounds',
    categoryId: 'runtime',
    label: 'Wire bounds',
    description: 'Upper bounds on plugin wire I/O.',
    paths: ['runtime.wasmtime.wire_bounds.*'],
    advancedPaths: [],
    danger: [],
  },
] as const satisfies readonly ConfigEditorSectionMetadata[];

const CONFIG_EDITOR_SECTION_LIST: readonly ConfigEditorSectionMetadata[] =
  CONFIG_EDITOR_SECTIONS;

function matchSegmentGlob(pattern: string, segment: string): boolean {
  if (pattern === '*') return true;
  if (!pattern.includes('*')) return pattern === segment;
  const source = pattern
    .split('*')
    .map((part) => part.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'))
    .join('[^.]*');
  return new RegExp(`^${source}$`).test(segment);
}

function matchPathSegments(
  pattern: readonly string[],
  segments: readonly ConfigPathSegment[],
): boolean {
  if (pattern.length === 0) return segments.length === 0;
  const [head, ...rest] = pattern;
  if (head === '**') {
    for (let skip = 0; skip <= segments.length; skip += 1) {
      if (matchPathSegments(rest, segments.slice(skip))) return true;
    }
    return false;
  }
  const [segment, ...remaining] = segments;
  if (segment === undefined) return false;
  if (!matchSegmentGlob(head ?? '', String(segment))) return false;
  return matchPathSegments(rest, remaining);
}

export function matchConfigPath(
  pattern: ConfigPathMatcher,
  path: string | ConfigPath,
): boolean {
  return matchPathSegments(pattern.split('.'), parseConfigPath(path));
}

export type ConfigLeafPresentation =
  | 'boolean'
  | 'enum'
  | 'duration'
  | 'bytes'
  | 'count'
  | 'address'
  | 'url'
  | 'path'
  | 'env'
  | 'string'
  | 'array'
  | 'unknown';

export type ConfigLeafUnit = 'ms' | 'secs' | 'days' | 'bytes';

export interface ConfigLeafClassification {
  categoryId: string;
  sectionId: string | null;
  advanced: boolean;
  dangerous: boolean;
  dangerImpact: string | null;
  presentation: ConfigLeafPresentation;
  unit: ConfigLeafUnit | null;
}

const PRESENTATION_RULES: readonly {
  pattern: ConfigPathMatcher;
  kinds: readonly ConfigEditorLeafKind[];
  presentation: ConfigLeafPresentation;
  unit: ConfigLeafUnit | null;
}[] = [
  {
    pattern: '**.*_addr',
    kinds: ['string'],
    presentation: 'address',
    unit: null,
  },
  {
    pattern: '**.*_env',
    kinds: ['string'],
    presentation: 'env',
    unit: null,
  },
  {
    pattern: '**.*_url',
    kinds: ['string'],
    presentation: 'url',
    unit: null,
  },
  { pattern: '**.url', kinds: ['string'], presentation: 'url', unit: null },
  {
    pattern: '**.*_uri',
    kinds: ['string'],
    presentation: 'url',
    unit: null,
  },
  {
    pattern: '**.*_endpoint',
    kinds: ['string'],
    presentation: 'url',
    unit: null,
  },
  {
    pattern: '**.*_path',
    kinds: ['string'],
    presentation: 'path',
    unit: null,
  },
  { pattern: '**.path', kinds: ['string'], presentation: 'path', unit: null },
  {
    pattern: '**.*_dir',
    kinds: ['string'],
    presentation: 'path',
    unit: null,
  },
  {
    pattern: '**.*_bytes',
    kinds: ['integer', 'number'],
    presentation: 'bytes',
    unit: 'bytes',
  },
  {
    pattern: '**.*_secs',
    kinds: ['integer', 'number'],
    presentation: 'duration',
    unit: 'secs',
  },
  {
    pattern: '**.*_ms',
    kinds: ['integer', 'number'],
    presentation: 'duration',
    unit: 'ms',
  },
  {
    pattern: '**.*_days',
    kinds: ['integer', 'number'],
    presentation: 'duration',
    unit: 'days',
  },
];

const KIND_PRESENTATION: Record<ConfigEditorLeafKind, ConfigLeafPresentation> =
  {
    boolean: 'boolean',
    enum: 'enum',
    integer: 'count',
    number: 'count',
    string: 'string',
    array: 'array',
    unknown: 'unknown',
  };

export function classifyConfigLeaf(
  path: string | ConfigPath,
  kind?: ConfigEditorLeafKind,
): ConfigLeafClassification {
  const segments = parseConfigPath(path);
  let sectionId: string | null = null;
  let advanced = false;
  for (const section of CONFIG_EDITOR_SECTION_LIST) {
    if (
      section.paths.some((pattern) =>
        matchPathSegments(pattern.split('.'), segments),
      )
    ) {
      sectionId = section.id;
      break;
    }
    if (
      section.advancedPaths.some((pattern) =>
        matchPathSegments(pattern.split('.'), segments),
      )
    ) {
      sectionId = section.id;
      advanced = true;
      break;
    }
  }
  const section = sectionId
    ? CONFIG_EDITOR_SECTION_LIST.find((entry) => entry.id === sectionId)
    : undefined;
  const root = segments[0];
  const categoryId =
    section?.categoryId ??
    (typeof root === 'string'
      ? (CONFIG_EDITOR_CATEGORIES.find((category) =>
          (category.roots as readonly string[]).includes(root),
        )?.id ??
        CONFIG_EDITOR_CATEGORIES.at(-1)?.id ??
        'runtime')
      : (CONFIG_EDITOR_CATEGORIES.at(-1)?.id ?? 'runtime'));
  let dangerImpact: string | null = null;
  for (const entry of CONFIG_EDITOR_SECTION_LIST) {
    for (const danger of entry.danger) {
      if (matchPathSegments(danger.path.split('.'), segments)) {
        dangerImpact = danger.impact;
        break;
      }
    }
    if (dangerImpact !== null) break;
  }
  let presentation: ConfigLeafPresentation | null = null;
  let unit: ConfigLeafUnit | null = null;
  for (const rule of PRESENTATION_RULES) {
    if (kind !== undefined && !rule.kinds.includes(kind)) continue;
    if (matchPathSegments(rule.pattern.split('.'), segments)) {
      presentation = rule.presentation;
      unit = rule.unit;
      break;
    }
  }
  if (presentation === null) {
    presentation = KIND_PRESENTATION[kind ?? 'unknown'];
  }
  return {
    categoryId,
    sectionId,
    advanced,
    dangerous: dangerImpact !== null,
    dangerImpact,
    presentation,
    unit,
  };
}

export type ConfigImpactDimension =
  | 'availability'
  | 'cost'
  | 'cpu'
  | 'durability'
  | 'latency'
  | 'memory'
  | 'network'
  | 'observability'
  | 'security'
  | 'storage'
  | 'throughput';

/**
 * Operational guidance for one config field or recurring job: a short purpose
 * sentence plus the trade-off of moving the value in either direction (or
 * toggling it). Produced by {@link resolveConfigFieldGuidance}.
 */
export interface ConfigFieldGuidance {
  /** What the field controls, in one sentence. */
  description: string;
  /** Effect of setting a numeric/duration/bytes value lower. */
  lower?: string;
  /** Effect of setting a numeric/duration/bytes value higher. */
  higher?: string;
  /** Effect of enabling a boolean field. */
  enabled?: string;
  /** Effect of disabling a boolean field. */
  disabled?: string;
  /** Operational recommendation or enum-choice guidance. */
  recommendation?: string;
  /** Operational dimensions the field affects. */
  impactDimensions?: readonly ConfigImpactDimension[];
}

/** Table entries may omit any field; the resolver backfills from fallbacks. */
type ConfigFieldGuidanceOverride = Partial<ConfigFieldGuidance>;

type ConfigFieldGuidanceFactory = (
  segments: readonly ConfigPathSegment[],
) => ConfigFieldGuidanceOverride;

export interface ConfigRecurringJobMetadata {
  key: string;
  label: string;
  purpose: string;
}

/**
 * Built-in recurring jobs registered by the scheduler
 * (`default_scheduler_recurring_jobs` in cc-lb-config). Unknown job keys in
 * the config file fail validation but are still rendered so they can be
 * removed.
 */
export const CONFIG_RECURRING_JOBS = [
  {
    key: 'anthropic_compat_refresh',
    label: 'Anthropic compat refresh',
    purpose:
      'Refreshes Anthropic compatibility metadata such as supported client versions.',
  },
  {
    key: 'apalis_housekeeping',
    label: 'Apalis housekeeping',
    purpose:
      'Reclaims finished and stale job-queue rows so the scheduler queue stays small.',
  },
  {
    key: 'oauth_refresh_watchdog',
    label: 'OAuth refresh watchdog',
    purpose:
      'Detects OAuth tokens nearing expiry and enqueues refresh jobs for them.',
  },
  {
    key: 'oauth_usage_poll',
    label: 'OAuth usage poll',
    purpose:
      'Polls upstream OAuth usage endpoints to keep per-upstream quota state current.',
  },
  {
    key: 'pool_quota_snapshot',
    label: 'Pool quota snapshot',
    purpose:
      'Snapshots per-pool quota state so routing decisions use fresh limits.',
  },
  {
    key: 'price_catalog_refresh',
    label: 'Price catalog refresh',
    purpose:
      'Fetches the latest model price catalog and stores a new version when it changes.',
  },
  {
    key: 'prompt_cache_purge',
    label: 'Prompt cache purge',
    purpose:
      'Removes expired prompt-cache observations so shadow-cache state stays bounded.',
  },
  {
    key: 'upstream_affinity_purge',
    label: 'Upstream affinity purge',
    purpose: 'Deletes upstream-affinity records that are past their TTL.',
  },
  {
    key: 'usage_prune',
    label: 'Usage prune',
    purpose:
      'Deletes request events, rollups, and audit records older than their retention windows.',
  },
  {
    key: 'usage_rollup',
    label: 'Usage rollup',
    purpose:
      'Aggregates raw request events into usage buckets for metering and quota accounting.',
  },
  {
    key: 'warmup_watchdog',
    label: 'Warmup watchdog',
    purpose:
      'Checks that upstream warmup cycles are being enqueued and re-enqueues missing ones.',
  },
] as const satisfies readonly ConfigRecurringJobMetadata[];

export function recurringJobMetadata(
  key: string,
): ConfigRecurringJobMetadata | null {
  return CONFIG_RECURRING_JOBS.find((job) => job.key === key) ?? null;
}

function recurringJobFieldGuidance(
  field: 'enabled' | 'interval_secs' | 'jitter_secs',
): ConfigFieldGuidanceFactory {
  return (segments) => {
    const job = recurringJobMetadata(String(segments[2] ?? ''));
    const label = job?.label ?? 'this recurring job';
    switch (field) {
      case 'enabled':
        return {
          description: `Whether ${label} runs on its schedule.`,
          impactDimensions: ['availability'],
        };
      case 'interval_secs':
        return {
          description: `How often ${label} runs.`,
          lower:
            'Runs more often — fresher results but more database and upstream load.',
          higher: 'Runs less often — lighter load but staler results.',
          impactDimensions: ['throughput', 'latency'],
        };
      case 'jitter_secs':
        return {
          description: `Random extra delay added to each ${label} run.`,
          lower:
            'Runs closer to the exact interval; jobs may fire in synchronized bursts.',
          higher:
            'Spreads runs over a wider window, smoothing load but delaying each run.',
          impactDimensions: ['throughput'],
        };
    }
  };
}

const CONFIG_FIELD_GUIDANCE: readonly {
  pattern: ConfigPathMatcher;
  guidance: ConfigFieldGuidanceOverride | ConfigFieldGuidanceFactory;
}[] = [
  {
    pattern: 'admin.auth.providers.*.audiences',
    guidance: {
      description:
        'Audience tags this provider accepts when validating admin requests.',
      recommendation:
        'Cloudflare Access providers need the application audience tag; static token providers ignore this.',
      impactDimensions: ['security'],
    },
  },
  {
    pattern: 'admin.auth.providers.*.header',
    guidance: {
      description:
        'HTTP header that carries the Cloudflare Access JWT assertion.',
      recommendation:
        'Must match the header Cloudflare sends; the default works for standard Access deployments.',
      impactDimensions: ['security', 'availability'],
    },
  },
  {
    pattern: 'admin.auth.providers.*.id',
    guidance: {
      description:
        'Unique identifier for this provider, used in logs and session records.',
      impactDimensions: ['observability'],
    },
  },
  {
    pattern: 'admin.auth.providers.*.kind',
    guidance: {
      description: 'Authentication mechanism for this provider.',
      recommendation:
        "'static_token' checks a shared bearer token; 'cloudflare' validates Cloudflare Access JWTs against the team domain.",
      impactDimensions: ['security'],
    },
  },
  {
    pattern: 'admin.auth.providers.*.team_domain',
    guidance: {
      description:
        'Cloudflare Access team domain, used as the JWT issuer and JWKS base URL.',
      recommendation:
        'Format is `<team>.cloudflareaccess.com`; a wrong domain makes every admin login fail.',
      impactDimensions: ['security', 'availability'],
    },
  },
  {
    pattern: 'admin.auth.providers.*.token_env',
    guidance: {
      description:
        'Environment variable holding the static admin bearer token.',
      recommendation:
        'The variable must be set before startup; a missing value locks out static-token admin access.',
      impactDimensions: ['security', 'availability'],
    },
  },
  {
    pattern: 'aead.key_env',
    guidance: {
      description:
        'Environment variable holding the master key that encrypts stored secrets.',
      recommendation:
        'Losing or rotating this key without re-encrypting makes stored credentials unreadable.',
      impactDimensions: ['security', 'durability'],
    },
  },
  {
    pattern: 'body.files_cap_bytes',
    guidance: {
      description:
        'Maximum total size of file attachments accepted in one request body.',
      lower: 'Rejects large file uploads sooner and bounds memory per request.',
      higher:
        'Accepts bigger uploads but raises peak memory and parsing cost per request.',
      impactDimensions: ['memory', 'availability'],
    },
  },
  {
    pattern: 'body.messages_cap_bytes',
    guidance: {
      description:
        'Maximum size of the messages payload accepted in one request body.',
      lower: 'Rejects oversized prompts sooner and bounds per-request memory.',
      higher:
        'Accepts larger prompts at the cost of higher peak memory per request.',
      impactDimensions: ['memory', 'availability'],
    },
  },
  {
    pattern: 'bulkhead.max_conns_per_upstream',
    guidance: {
      description: 'Maximum simultaneous connections opened to one upstream.',
      lower:
        'Protects upstreams from connection floods but queues or rejects excess traffic.',
      higher:
        'Allows more parallel upstream traffic but can overwhelm a weak upstream.',
      impactDimensions: ['throughput', 'availability'],
    },
  },
  {
    pattern: 'bulkhead.semaphore_per_upstream',
    guidance: {
      description:
        'Maximum in-flight requests permitted per upstream before new ones are shed.',
      lower:
        'Sheds load earlier, protecting upstreams at the cost of more rejected requests.',
      higher:
        'Admits more concurrent work but weakens isolation when an upstream degrades.',
      impactDimensions: ['throughput', 'availability'],
    },
  },
  {
    pattern: 'circuit_breaker.failures_to_open',
    guidance: {
      description:
        'Failures inside the rolling window required to open the breaker.',
      lower:
        'Trips sooner, failing fast on flaky upstreams but risking false positives.',
      higher:
        'Tolerates more errors before tripping, keeping traffic flowing to a degraded upstream longer.',
      impactDimensions: ['availability', 'latency'],
    },
  },
  {
    pattern: 'circuit_breaker.half_open_after_secs',
    guidance: {
      description:
        'How long the breaker stays open before a probe request is allowed through.',
      lower:
        'Retries recovery sooner but may probe a still-down upstream more often.',
      higher:
        'Gives upstreams longer to recover at the cost of extended fail-fast periods.',
      impactDimensions: ['availability', 'latency'],
    },
  },
  {
    pattern: 'circuit_breaker.window_secs',
    guidance: {
      description: 'Rolling window over which upstream failures are counted.',
      lower: 'Reacts to recent failures quickly but forgets older context.',
      higher:
        'Smooths the failure count over a longer period, slowing reaction to new outages.',
      impactDimensions: ['availability'],
    },
  },
  {
    pattern: 'cluster.instance_url',
    guidance: {
      description: 'Base URL other cluster members use to reach this instance.',
      recommendation:
        'Must be routable from peers; a wrong value breaks cluster coordination for this node.',
      impactDimensions: ['availability', 'network'],
    },
  },
  {
    pattern: 'cluster.token_env',
    guidance: {
      description:
        'Environment variable holding the shared token for cluster-internal requests.',
      recommendation:
        'Must match on every member; a mismatch rejects inter-node calls.',
      impactDimensions: ['security', 'availability'],
    },
  },
  {
    pattern: 'event_bus.broadcast_capacity',
    guidance: {
      description:
        'Event slots buffered per broadcast subscriber before events are dropped.',
      lower: 'Slow consumers drop events sooner, keeping memory bounded.',
      higher:
        'Tolerates slower consumers at the cost of more buffered events in memory.',
      impactDimensions: ['memory', 'availability'],
    },
  },
  {
    pattern: 'event_bus.partial_retention_max_entries',
    guidance: {
      description:
        'Maximum partial events retained for subscribers that reconnect.',
      lower:
        'Bounds retention memory but evicts events before slow subscribers catch up.',
      higher:
        'Keeps more history for reconnecting subscribers at higher memory cost.',
      impactDimensions: ['memory', 'durability'],
    },
  },
  {
    pattern: 'event_bus.partial_retention_ttl_secs',
    guidance: {
      description:
        'How long a retained partial event stays available for replay.',
      lower:
        'Expires events sooner, bounding memory but shrinking the replay window.',
      higher:
        'Extends the replay window for reconnecting subscribers at higher memory cost.',
      impactDimensions: ['memory', 'durability'],
    },
  },
  {
    pattern: 'event_bus.pg_notify_channel',
    guidance: {
      description:
        'Postgres NOTIFY channel used to fan out partial events between instances.',
      recommendation:
        'Must be identical on every instance sharing the database; a typo silently splits the event bus.',
      impactDimensions: ['availability', 'network'],
    },
  },
  {
    pattern: 'event_bus.storage_tail_poll_interval_ms',
    guidance: {
      description:
        'How often the event bus polls storage for events it may have missed.',
      lower: 'Catches missed events faster but queries storage more often.',
      higher: 'Reduces polling load but delays recovery of missed events.',
      impactDimensions: ['latency', 'storage'],
    },
  },
  {
    pattern: 'limit_reservation_ttl.tick_secs',
    guidance: {
      description: 'How often the sweeper releases expired limit reservations.',
      lower: 'Frees stale reservations sooner but runs the sweep more often.',
      higher: 'Sweeps less often, leaving stale reservations held longer.',
      impactDimensions: ['availability', 'storage'],
    },
  },
  {
    pattern: 'limit_reservation_ttl.ttl_secs',
    guidance: {
      description:
        'How long a quota reservation is held while waiting for confirmation.',
      lower:
        'Releases quota faster after abandoned requests but may expire slow confirmations.',
      higher:
        'Gives slow requests more time but ties up quota longer when requests die.',
      impactDimensions: ['availability', 'throughput'],
    },
  },
  {
    pattern: 'listener.admin_addr',
    guidance: {
      description: 'Bind address for the admin API and dashboard.',
      recommendation:
        'Keep it on loopback or a private interface; binding publicly exposes the admin surface.',
      impactDimensions: ['security', 'availability'],
    },
  },
  {
    pattern: 'listener.metrics_addr',
    guidance: {
      description: 'Bind address for the Prometheus metrics endpoint.',
      recommendation:
        'Expose only where the scraper can reach it; metrics can reveal internal topology.',
      impactDimensions: ['security', 'observability'],
    },
  },
  {
    pattern: 'listener.proxy_addr',
    guidance: {
      description: 'Bind address for the public proxy listener.',
      recommendation:
        'Use a wildcard address to serve external traffic; loopback restricts the proxy to local clients.',
      impactDimensions: ['availability', 'network'],
    },
  },
  {
    pattern: 'listener.tls.cert_path',
    guidance: {
      description:
        'Filesystem path to the PEM certificate chain served to TLS clients.',
      recommendation:
        'Must be readable by the process; a wrong path disables TLS or fails startup.',
      impactDimensions: ['security', 'availability'],
    },
  },
  {
    pattern: 'listener.tls.key_path',
    guidance: {
      description:
        'Filesystem path to the PEM private key for the TLS certificate.',
      recommendation:
        'Must match the certificate and be readable only by the service account.',
      impactDimensions: ['security', 'availability'],
    },
  },
  {
    pattern: 'listener.tls.reload_on_sighup',
    guidance: {
      description:
        'Reload the TLS certificate and key when the process receives SIGHUP.',
      enabled: 'Certificate rotation applies on SIGHUP without a restart.',
      disabled: 'Certificate changes require a full process restart.',
      impactDimensions: ['availability', 'security'],
    },
  },
  {
    pattern: 'oauth.anthropic.auth_url',
    guidance: {
      description:
        'Anthropic OAuth authorization endpoint users are redirected to.',
      recommendation:
        'Only change for a compatible OAuth deployment; a wrong URL breaks sign-in.',
      impactDimensions: ['availability', 'security'],
    },
  },
  {
    pattern: 'oauth.anthropic.client_id',
    guidance: {
      description:
        'OAuth client ID registered with Anthropic for this deployment.',
      recommendation:
        'Must match the registered application; a wrong ID fails authorization.',
      impactDimensions: ['security', 'availability'],
    },
  },
  {
    pattern: 'oauth.anthropic.redirect_uri',
    guidance: {
      description: 'Redirect URI Anthropic returns authorization codes to.',
      recommendation:
        'Must exactly match the URI registered with Anthropic or authorization is rejected.',
      impactDimensions: ['security', 'availability'],
    },
  },
  {
    pattern: 'oauth.anthropic.long_lived_scopes',
    guidance: {
      description:
        'Scopes requested when connecting an upstream in long-lived 365-day mode.',
      recommendation:
        'Keep to inference-only scopes; Anthropic refuses a 365-day expiry for Remote Control, connectors, or API-key scopes and the connect flow then falls back to a refreshing credential.',
      impactDimensions: ['security', 'availability'],
    },
  },
  {
    pattern: 'oauth.anthropic.scopes',
    guidance: {
      description:
        'OAuth scopes requested when authorizing Anthropic accounts.',
      recommendation:
        'Request only the scopes upstream features need; extra scopes widen token access.',
      impactDimensions: ['security'],
    },
  },
  {
    pattern: 'oauth.anthropic.token_url',
    guidance: {
      description:
        'Anthropic OAuth endpoint used to exchange codes and refresh tokens.',
      recommendation:
        'Only change for a compatible OAuth deployment; a wrong URL breaks token refresh.',
      impactDimensions: ['availability', 'security'],
    },
  },
  {
    pattern: 'observability.log_redaction',
    guidance: {
      description: 'Redact secrets and credential-like values from log output.',
      enabled:
        'Logs mask sensitive values, reducing leak risk at slight context cost.',
      disabled:
        'Logs include raw values — useful for debugging but may expose secrets.',
      impactDimensions: ['security', 'observability'],
    },
  },
  {
    pattern: 'observability.otlp_endpoint',
    guidance: {
      description:
        'OTLP collector endpoint that receives exported traces and metrics.',
      recommendation:
        'Leave unset to disable export; a wrong endpoint drops telemetry silently.',
      impactDimensions: ['observability', 'network'],
    },
  },
  {
    pattern: 'observability.tracing_level',
    guidance: {
      description:
        'Minimum severity for emitted traces and logs (error, warn, info, debug, trace).',
      recommendation:
        "'debug' and 'trace' increase volume sharply; keep 'info' in production.",
      impactDimensions: ['observability', 'cost'],
    },
  },
  {
    pattern: 'observability.user_prompt_redaction',
    guidance: {
      description: 'Redact user prompt content from logs and traces.',
      enabled:
        'Prompt text is masked, protecting user data but limiting prompt debugging.',
      disabled:
        'Prompt text appears in telemetry — helpful for debugging, risky for privacy.',
      impactDimensions: ['security', 'observability'],
    },
  },
  {
    pattern: 'price_catalog.cache_path',
    guidance: {
      description:
        'Local file where the downloaded price catalog is cached between refreshes.',
      recommendation:
        'Must be writable; a bad path forces every read to fall back to the last fetch.',
      impactDimensions: ['cost', 'durability'],
    },
  },
  {
    pattern: 'price_catalog.url',
    guidance: {
      description:
        'URL of the LiteLLM-compatible price catalog fetched by the refresh job.',
      recommendation:
        'Point at a trusted mirror to control update cadence; a wrong URL stops price updates.',
      impactDimensions: ['cost', 'availability'],
    },
  },
  {
    pattern: 'prompt_cache_shadow.grace_margin_secs',
    guidance: {
      description:
        "Margin subtracted from the observed TTL when a cache observation's expiry is recorded.",
      lower: 'Entries live closer to their true TTL, risking stale warm hits.',
      higher:
        'Entries expire earlier, absorbing provider-side TTL jitter at the cost of fewer warm hits.',
      impactDimensions: ['latency'],
    },
  },
  {
    pattern: 'request_event_retention_days',
    guidance: {
      description:
        'How long request event records are kept before the prune job deletes them.',
      lower:
        'Deletes history sooner, saving storage but shortening the audit window.',
      higher: 'Keeps a longer audit trail at steadily growing storage cost.',
      impactDimensions: ['storage', 'durability'],
    },
  },
  {
    pattern: 'runtime.data_dir',
    guidance: {
      description:
        'Directory for runtime state, caches, and plugin working data.',
      recommendation:
        'Must be writable and persistent; an ephemeral or read-only path loses state on restart.',
      impactDimensions: ['durability', 'availability'],
    },
  },
  {
    pattern: 'runtime.wasmtime.allocation_strategy',
    guidance: {
      description: 'How Wasm instances and memories are allocated for plugins.',
      recommendation:
        "'ondemand' creates instances per request with lower idle memory; 'pooling' pre-allocates warm instances for lower latency at higher baseline memory.",
      impactDimensions: ['memory', 'latency'],
    },
  },
  {
    pattern: 'runtime.wasmtime.cookie_redaction',
    guidance: {
      description:
        'Strip cookie headers from request data visible to Wasm plugins.',
      enabled:
        'Plugins never see cookies, reducing the credential-leak surface.',
      disabled:
        'Plugins receive cookies — needed only by plugins that inspect them.',
      impactDimensions: ['security'],
    },
  },
  {
    pattern: 'runtime.wasmtime.memory_guard_bytes',
    guidance: {
      description: 'Guard region reserved around each Wasm linear memory.',
      lower: 'Saves address space but narrows the out-of-bounds safety margin.',
      higher:
        'Widens the safety margin at the cost of more reserved address space.',
      impactDimensions: ['memory', 'security'],
    },
  },
  {
    pattern: 'runtime.wasmtime.memory_max_pages',
    guidance: {
      description:
        'Maximum Wasm memory pages a single plugin may allocate (64 KiB each).',
      lower: 'Caps plugin memory tightly but can fail memory-hungry plugins.',
      higher:
        'Allows larger plugin memories at a higher per-plugin memory ceiling.',
      impactDimensions: ['memory', 'availability'],
    },
  },
  {
    pattern: 'runtime.wasmtime.memory_reservation_bytes',
    guidance: {
      description:
        'Virtual address space reserved for each Wasm linear memory.',
      lower:
        'Reserves less address space but limits how far a memory can grow.',
      higher: 'Allows more in-place growth at the cost of larger reservations.',
      impactDimensions: ['memory'],
    },
  },
  {
    pattern: 'runtime.wasmtime.pool_total_core_instances',
    guidance: {
      description: 'Core instances kept warm in the pooling allocator.',
      lower:
        'Lower warm-pool memory but slower cold-start allocation under burst.',
      higher: 'More instances ready instantly at higher idle memory.',
      impactDimensions: ['memory', 'latency'],
    },
  },
  {
    pattern: 'runtime.wasmtime.pool_total_memories',
    guidance: {
      description: 'Memories kept warm in the pooling allocator.',
      lower: 'Lower warm-pool footprint but more allocation on demand.',
      higher: 'More pre-allocated memories at higher baseline memory use.',
      impactDimensions: ['memory', 'latency'],
    },
  },
  {
    pattern: 'runtime.wasmtime.shape_origin_policy',
    guidance: {
      recommendation:
        "'unrestricted' preserves historical behavior; 'selected_upstream_origin' rejects plugin URLs whose origin differs from the chosen upstream.",
      impactDimensions: ['security'],
    },
  },
  {
    pattern: 'runtime.wasmtime.wire_bounds.max_header_value_bytes',
    guidance: {
      description:
        'Maximum size of a single header value crossing the plugin boundary.',
      lower:
        'Rejects oversized headers sooner but may break plugins needing large values.',
      higher: 'Allows larger header values at higher per-message memory.',
      impactDimensions: ['memory', 'availability'],
    },
  },
  {
    pattern: 'runtime.wasmtime.wire_bounds.max_headers',
    guidance: {
      description:
        'Maximum number of headers allowed on a plugin wire message.',
      lower: 'Bounds parsing work but may truncate header-heavy messages.',
      higher: 'Accepts header-heavy messages at slightly higher parsing cost.',
      impactDimensions: ['memory', 'availability'],
    },
  },
  {
    pattern: 'runtime.wasmtime.wire_bounds.output_body_bytes',
    guidance: {
      description: 'Maximum body size a plugin may return on the wire.',
      lower: 'Bounds response memory but rejects large plugin outputs.',
      higher: 'Allows larger plugin outputs at higher memory per response.',
      impactDimensions: ['memory', 'availability'],
    },
  },
  {
    pattern: 'runtime.wasmtime.wire_bounds.reason_bytes',
    guidance: {
      description:
        'Maximum size of a reason or diagnostic string on the plugin wire.',
      lower: 'Bounds message size but truncates long diagnostic strings.',
      higher: 'Preserves longer diagnostics at marginally higher memory.',
      impactDimensions: ['memory', 'observability'],
    },
  },
  {
    pattern: 'scheduler.dlq_retention_days',
    guidance: {
      description: 'How long dead-lettered jobs are kept before deletion.',
      lower:
        'Dead letters disappear sooner, saving storage but shortening the debugging window.',
      higher: 'Keeps failed jobs longer for inspection at higher storage cost.',
      impactDimensions: ['storage', 'observability'],
    },
  },
  {
    pattern: 'scheduler.entity_concurrency',
    guidance: {
      description:
        'Worker slots for per-entity jobs such as per-upstream refreshes.',
      lower:
        'Runs fewer entity jobs in parallel, smoothing load but slowing completion.',
      higher:
        'Processes more entities at once at higher CPU and database load.',
      impactDimensions: ['throughput', 'cpu'],
    },
  },
  {
    pattern: 'scheduler.keepalive_concurrency',
    guidance: {
      description:
        'Worker slots for keepalive jobs that hold pooled resources warm.',
      lower:
        'Fewer parallel keepalives, reducing load but risking stale pools.',
      higher: 'More parallel keepalives at higher background load.',
      impactDimensions: ['throughput', 'cpu'],
    },
  },
  {
    pattern: 'scheduler.recurring_jobs',
    guidance: {
      description:
        'Per-job enablement and cadence for the built-in maintenance jobs.',
    },
  },
  {
    pattern: 'scheduler.recurring_jobs.*',
    guidance: (segments) => {
      const job = recurringJobMetadata(String(segments[2] ?? ''));
      if (job) {
        return {
          description: job.purpose,
          impactDimensions: ['availability'],
        };
      }
      return {
        description:
          'Recurring job declared in the config file. This key is not a built-in job and fails validation.',
        recommendation: 'Remove the entry or rename it to a built-in job key.',
        impactDimensions: ['availability'],
      };
    },
  },
  {
    pattern: 'scheduler.recurring_jobs.*.enabled',
    guidance: recurringJobFieldGuidance('enabled'),
  },
  {
    pattern: 'scheduler.recurring_jobs.*.interval_secs',
    guidance: recurringJobFieldGuidance('interval_secs'),
  },
  {
    pattern: 'scheduler.recurring_jobs.*.jitter_secs',
    guidance: recurringJobFieldGuidance('jitter_secs'),
  },
  {
    pattern: 'scheduler.separate_pool.acquire_timeout_secs',
    guidance: {
      description:
        'How long the scheduler waits for a pool connection before failing.',
      lower:
        'Fails faster under pool exhaustion but may abort under brief contention.',
      higher:
        'Waits longer for a free connection, masking exhaustion as latency.',
      impactDimensions: ['availability', 'latency'],
    },
  },
  {
    pattern: 'scheduler.separate_pool.idle_timeout_secs',
    guidance: {
      description:
        'How long an idle scheduler connection is kept before being closed.',
      lower: 'Releases idle connections sooner, freeing database slots.',
      higher:
        'Keeps connections warm longer, avoiding reconnect cost but holding slots.',
      impactDimensions: ['availability'],
    },
  },
  {
    pattern: 'scheduler.separate_pool.max_connections',
    guidance: {
      description:
        'Maximum connections the scheduler pool may open to the database.',
      lower:
        'Bounds database load but can starve scheduler work under concurrency.',
      higher:
        'Allows more parallel scheduler work at higher database connection usage.',
      impactDimensions: ['throughput', 'availability'],
    },
  },
  {
    pattern: 'scheduler.separate_pool.min_connections',
    guidance: {
      description: 'Connections the scheduler pool keeps open even when idle.',
      lower: 'Frees database slots when idle but pays reconnect cost on wake.',
      higher:
        'Keeps connections warm for instant scheduling at constant slot usage.',
      impactDimensions: ['latency', 'availability'],
    },
  },
  {
    pattern: 'scheduler.separate_pool.sslmode',
    guidance: {
      description:
        'TLS mode for scheduler database connections (disable, allow, prefer, require, verify-ca, verify-full).',
      recommendation:
        "'require' or stronger protects credentials in transit; use 'disable' only on trusted networks.",
      impactDimensions: ['security', 'network'],
    },
  },
  {
    pattern: 'scheduler.separate_pool.statement_timeout_secs',
    guidance: {
      description:
        'Maximum runtime of a single scheduler query before it is cancelled.',
      lower:
        'Cancels runaway queries sooner but may abort legitimate long work.',
      higher:
        'Allows longer queries at the risk of holding connections on slow statements.',
      impactDimensions: ['availability', 'latency'],
    },
  },
  {
    pattern: 'scheduler.singleton_concurrency',
    guidance: {
      description:
        'Worker slots for singleton jobs that run once cluster-wide.',
      lower:
        'Runs fewer singleton jobs in parallel, serializing maintenance work.',
      higher: 'Overlaps more singleton jobs at higher database and CPU load.',
      impactDimensions: ['throughput', 'cpu'],
    },
  },
  {
    pattern: 'storage.kind',
    guidance: {
      description: 'Storage backend for primary data.',
      recommendation:
        "'sqlite' runs embedded against a local file; 'postgres' uses an external database and unlocks pooling and multi-instance deployments.",
      impactDimensions: ['durability', 'availability'],
    },
  },
  {
    pattern: 'storage.path',
    guidance: {
      description: 'Filesystem path of the SQLite database file.',
      recommendation:
        'Must be writable and persistent; a wrong path creates an empty database elsewhere.',
      impactDimensions: ['durability', 'availability'],
    },
  },
  {
    pattern: 'storage.pool.acquire_timeout_secs',
    guidance: {
      description:
        'How long a request waits for a pooled connection before failing.',
      lower:
        'Fails faster under exhaustion but may abort during brief contention.',
      higher:
        'Waits longer for a connection, surfacing exhaustion as latency instead of errors.',
      impactDimensions: ['availability', 'latency'],
    },
  },
  {
    pattern: 'storage.pool.idle_timeout_secs',
    guidance: {
      description:
        'How long an idle pooled connection is kept before being closed.',
      lower: 'Releases idle connections sooner, freeing database slots.',
      higher:
        'Keeps connections warm longer at the cost of held database slots.',
      impactDimensions: ['availability'],
    },
  },
  {
    pattern: 'storage.pool.max_connections',
    guidance: {
      description: 'Maximum connections the pool may open to the database.',
      lower: 'Bounds database load but queues requests when traffic spikes.',
      higher:
        'Serves more parallel queries at higher database connection usage.',
      impactDimensions: ['throughput', 'availability'],
    },
  },
  {
    pattern: 'storage.pool.max_lifetime_secs',
    guidance: {
      description: 'Maximum age of a pooled connection before it is recycled.',
      lower:
        'Recycles connections more often, picking up server-side changes sooner.',
      higher:
        'Reuses connections longer, saving reconnect cost but holding stale sessions.',
      impactDimensions: ['availability'],
    },
  },
  {
    pattern: 'storage.pool.min_connections',
    guidance: {
      description: 'Connections kept open even when the pool is idle.',
      lower:
        'Frees database slots when idle but pays reconnect cost on the next request.',
      higher:
        'Keeps connections warm for instant queries at constant slot usage.',
      impactDimensions: ['latency', 'availability'],
    },
  },
  {
    pattern: 'storage.pool.sslmode',
    guidance: {
      description:
        'TLS mode for database connections (disable, allow, prefer, require, verify-ca, verify-full).',
      recommendation:
        "'require' or stronger protects credentials in transit; use 'disable' only on trusted networks.",
      impactDimensions: ['security', 'network'],
    },
  },
  {
    pattern: 'storage.pool.statement_timeout_secs',
    guidance: {
      description:
        'Maximum runtime of a single query before the database cancels it.',
      lower:
        'Cancels runaway queries sooner but may abort legitimate slow reads.',
      higher:
        'Allows longer queries at the risk of holding connections on slow statements.',
      impactDimensions: ['availability', 'latency'],
    },
  },
  {
    pattern: 'storage.pool.test_before_acquire',
    guidance: {
      description: 'Health-check each pooled connection before handing it out.',
      enabled:
        'Stale connections are detected before use, adding a small checkout latency.',
      disabled:
        'Checkout is faster but a dead connection can surface as a request error.',
      impactDimensions: ['availability', 'latency'],
    },
  },
  {
    pattern: 'storage.url',
    guidance: {
      description: 'Postgres connection URL for primary storage.',
      recommendation:
        'Contains credentials — treat it as a secret; a wrong URL prevents startup or silently targets another database.',
      impactDimensions: ['durability', 'security', 'availability'],
    },
  },
  {
    pattern: 'subscription_quota.routing_max_staleness_secs',
    guidance: {
      description:
        'How old cached subscription-quota state may be before routing treats it as stale.',
      lower: 'Routing uses fresher quota data but re-fetches more often.',
      higher:
        'Tolerates staler quota data, reducing reads but risking outdated limit decisions.',
      impactDimensions: ['availability', 'cost'],
    },
  },
  {
    pattern: 'subscription_quota.writer_batch_max_records',
    guidance: {
      description: 'Maximum quota records flushed per writer batch.',
      lower:
        'Smaller batches flush sooner with less memory but more write round-trips.',
      higher:
        'Bigger batches amortize writes at higher memory and longer flush latency.',
      impactDimensions: ['throughput', 'storage'],
    },
  },
  {
    pattern: 'subscription_quota.writer_channel_capacity',
    guidance: {
      description:
        'Buffered quota records the writer channel holds before producers block or drop.',
      lower: 'Bounds memory but back-pressures or drops records under burst.',
      higher: 'Absorbs bigger bursts at higher memory cost.',
      impactDimensions: ['memory', 'throughput'],
    },
  },
  {
    pattern: 'subscription_quota.writer_flush_ms',
    guidance: {
      description:
        'Maximum time quota records wait in the buffer before a flush.',
      lower:
        'Flushes sooner, keeping quota state fresh at higher write frequency.',
      higher: 'Batches longer, reducing writes but delaying quota visibility.',
      impactDimensions: ['latency', 'storage'],
    },
  },
  {
    pattern: 'timeouts.drain_secs',
    guidance: {
      description:
        'Grace period for in-flight requests to finish during shutdown.',
      lower: 'Shuts down faster but cuts off requests still in flight.',
      higher:
        'Gives requests more time to complete at the cost of slower restarts.',
      impactDimensions: ['availability', 'latency'],
    },
  },
  {
    pattern: 'timeouts.upstream_total_secs',
    guidance: {
      description:
        'Total deadline for a single upstream request, including retries.',
      lower:
        'Fails slow upstreams sooner, freeing capacity but aborting long requests.',
      higher:
        'Allows slower upstream responses at the cost of longer-held connections.',
      impactDimensions: ['latency', 'availability'],
    },
  },
  {
    pattern: 'upstream_affinity.ttl_days',
    guidance: {
      description:
        'How long an upstream affinity record is kept before the purge job removes it.',
      lower:
        'Affinity expires sooner, rebalancing traffic but losing stickiness.',
      higher:
        'Keeps clients pinned to upstreams longer at higher storage and imbalance risk.',
      impactDimensions: ['storage', 'latency'],
    },
  },
];

const GENERIC_LEAF_GUIDANCE: Record<
  ConfigLeafPresentation,
  ConfigFieldGuidanceOverride & { description: string }
> = {
  boolean: {
    description: 'Boolean flag.',
  },
  enum: {
    description: 'Selects one of the supported modes.',
  },
  duration: {
    description: 'Duration that controls how quickly the system reacts.',
    lower: 'Shorter values react faster but add scheduling and I/O overhead.',
    higher: 'Longer values reduce overhead but react more slowly to change.',
  },
  bytes: {
    description: 'Size limit in bytes.',
    lower:
      'Smaller values bound memory and disk use but reject larger payloads.',
    higher:
      'Larger values accept bigger payloads at higher memory and disk cost.',
  },
  count: {
    description: 'Numeric tuning value.',
    lower: 'Lower values conserve resources but limit throughput.',
    higher: 'Higher values raise throughput at greater resource cost.',
  },
  address: {
    description:
      'Network address in host:port form; a wrong value can make the service unreachable or expose it broadly.',
  },
  url: {
    description:
      'URL of an external endpoint; a wrong value breaks the integration that depends on it.',
  },
  path: {
    description:
      'Filesystem path; a wrong value causes file-access failures at runtime.',
  },
  env: {
    description:
      'Name of an environment variable read at startup; a missing or wrong variable keeps the service from loading the value.',
  },
  string: {
    description: 'Free-form text value.',
  },
  array: {
    description: 'List of values.',
  },
  unknown: {
    description: 'Setting preserved from the config file.',
  },
};

/**
 * Resolve operational guidance for a config path. Explicit per-path metadata
 * wins first; the schema description is used when the table entry omits one;
 * presentation-based generic text backfills anything still missing, so every
 * leaf — including schema fields added later — resolves to a non-empty
 * description, and numeric leaves always carry trade-off text. Boolean
 * leaves carry enabled/disabled effects only when the table spells out
 * asymmetric operational consequences.
 *
 * Recurring job paths (`scheduler.recurring_jobs.<name>` and its fields) are
 * synthesized from {@link CONFIG_RECURRING_JOBS} plus wildcard field guidance.
 */
export function resolveConfigFieldGuidance(
  path: string | ConfigPath,
  schemaDescription?: string,
  kind?: ConfigEditorLeafKind,
): ConfigFieldGuidance {
  const segments = parseConfigPath(path);
  const generic =
    GENERIC_LEAF_GUIDANCE[classifyConfigLeaf(segments, kind).presentation];
  const schemaText =
    typeof schemaDescription === 'string' && schemaDescription.trim()
      ? schemaDescription.trim()
      : null;
  let override: ConfigFieldGuidanceOverride | null = null;
  for (const entry of CONFIG_FIELD_GUIDANCE) {
    if (!matchPathSegments(entry.pattern.split('.'), segments)) continue;
    override =
      typeof entry.guidance === 'function'
        ? entry.guidance(segments)
        : entry.guidance;
    break;
  }
  return {
    description: override?.description ?? schemaText ?? generic.description,
    lower: override?.lower ?? generic.lower,
    higher: override?.higher ?? generic.higher,
    enabled: override?.enabled ?? generic.enabled,
    disabled: override?.disabled ?? generic.disabled,
    recommendation: override?.recommendation,
    impactDimensions: override?.impactDimensions,
  };
}

const DURATION_UNITS: readonly { label: string; ms: number }[] = [
  { label: 'day', ms: 86_400_000 },
  { label: 'hour', ms: 3_600_000 },
  { label: 'minute', ms: 60_000 },
  { label: 'second', ms: 1_000 },
];

const BYTE_UNITS = ['B', 'KiB', 'MiB', 'GiB', 'TiB'] as const;

function humanizeDuration(totalMs: number): string | null {
  if (!Number.isFinite(totalMs) || totalMs < 0) return null;
  if (totalMs < 1_000) return `${Math.round(totalMs)} ms`;
  const parts: string[] = [];
  let remaining = Math.round(totalMs);
  for (const { label, ms } of DURATION_UNITS) {
    if (parts.length === 2) break;
    const amount = Math.floor(remaining / ms);
    if (amount === 0) continue;
    remaining -= amount * ms;
    parts.push(`${amount} ${label}${amount === 1 ? '' : 's'}`);
  }
  if (parts.length === 0) return `${Math.round(totalMs)} ms`;
  return parts.join(' ');
}

function humanizeBytes(bytes: number): string | null {
  if (!Number.isFinite(bytes) || bytes < 0) return null;
  let value = bytes;
  let unitIndex = 0;
  while (value >= 1024 && unitIndex < BYTE_UNITS.length - 1) {
    value /= 1024;
    unitIndex += 1;
  }
  const rounded =
    unitIndex === 0 ? Math.round(value) : Math.round(value * 10) / 10;
  return `${rounded} ${BYTE_UNITS[unitIndex]}`;
}

export function humanizeConfigValue(
  value: unknown,
  unit: ConfigLeafUnit | null,
): string | null {
  if (typeof value !== 'number' || !Number.isFinite(value) || unit === null) {
    return null;
  }
  switch (unit) {
    case 'bytes':
      return humanizeBytes(value);
    case 'ms':
      return humanizeDuration(value);
    case 'secs':
      return humanizeDuration(value * 1_000);
    case 'days':
      return humanizeDuration(value * 86_400_000);
  }
}

export function isConfigLeafActive(
  leaf: Pick<ConfigSchemaLeaf, 'variantAlternatives'>,
  source: unknown,
): boolean {
  return leaf.variantAlternatives.some((alternative) =>
    alternative.every((constraint) => {
      const current = getConfigValue(source, constraint.path);
      return current === undefined || current === constraint.value;
    }),
  );
}

export function configLeafLabel(
  leaf: Pick<ConfigSchemaLeaf, 'path' | 'schema'>,
): string {
  const title = leaf.schema.title;
  if (typeof title === 'string' && title) return title;
  const segment = leaf.path.at(-1);
  const text = String(segment ?? 'value').replaceAll('_', ' ');
  return text.charAt(0).toUpperCase() + text.slice(1);
}

export interface ConfigSearchResult {
  leaf: ConfigEditorLeaf;
  path: string;
  label: string;
  categoryId: string;
  sectionId: string | null;
  breadcrumb: string;
  matched: 'label' | 'path' | 'section' | 'description';
}

export function searchConfigLeaves(
  model: ConfigEditorModel,
  query: string,
): ConfigSearchResult[] {
  const needle = query.trim().toLowerCase();
  if (!needle) return [];
  const results: { result: ConfigSearchResult; rank: number }[] = [];
  for (const leaf of model.leaves) {
    if (!leaf.active) continue;
    const label = configLeafLabel(leaf);
    const section = leaf.sectionId
      ? CONFIG_EDITOR_SECTION_LIST.find((entry) => entry.id === leaf.sectionId)
      : undefined;
    const category = CONFIG_EDITOR_CATEGORIES.find(
      (entry) => entry.id === leaf.categoryId,
    );
    const description =
      typeof leaf.schema.description === 'string'
        ? leaf.schema.description
        : '';
    const breadcrumb = section
      ? `${category?.label ?? leaf.categoryId} › ${section.label}`
      : `${category?.label ?? leaf.categoryId} › ${CONFIG_EDITOR_UNASSIGNED_LABEL}`;
    const base = {
      leaf,
      path: leaf.pathString,
      label,
      categoryId: leaf.categoryId,
      sectionId: leaf.sectionId,
      breadcrumb,
    };
    if (label.toLowerCase().includes(needle)) {
      results.push({ result: { ...base, matched: 'label' }, rank: 0 });
    } else if (leaf.pathString.toLowerCase().includes(needle)) {
      results.push({ result: { ...base, matched: 'path' }, rank: 1 });
    } else if (section?.label.toLowerCase().includes(needle)) {
      results.push({ result: { ...base, matched: 'section' }, rank: 2 });
    } else if (description.toLowerCase().includes(needle)) {
      results.push({ result: { ...base, matched: 'description' }, rank: 3 });
    }
  }
  return results
    .sort(
      (left, right) =>
        left.rank - right.rank ||
        left.result.path.localeCompare(right.result.path),
    )
    .map((entry) => entry.result);
}

export interface ConfigSchemaVariant {
  schema: ConfigSchema;
  label: string;
  tag: { property: string; value: string } | null;
}

export interface ConfigVariantConstraint {
  path: ConfigPath;
  value: string;
}

export interface ConfigSchemaLeaf {
  path: ConfigPath;
  pathString: string;
  schema: ConfigSchema;
  kind: ConfigEditorLeafKind;
  required: boolean;
  nullable: boolean;
  unknown: boolean;
  /**
   * Discriminator constraints of the tagged-union variants this leaf was
   * reached through. Each entry is one alternative (AND of constraints); the
   * leaf is active when every constraint of at least one alternative matches
   * the current editor source. An empty list means the leaf is unconditional.
   */
  variantAlternatives: readonly (readonly ConfigVariantConstraint[])[];
}

export interface ConfigValueResolution {
  editorValue: unknown;
  fileValue: unknown;
  defaultValue: unknown;
  effectiveValue: unknown;
  resolvedFileValue: unknown;
  origin: 'draft' | 'file' | 'default' | 'unset';
  override: ConfigOverrideInfo | null;
}

export interface ConfigEditorLeaf
  extends ConfigSchemaLeaf,
    ConfigValueResolution {
  modified: boolean;
  overridden: boolean;
  issues: ConfigValidationIssue[];
  /** Whether the leaf exists in the union variants active in the draft. */
  active: boolean;
  categoryId: string;
  /** Owning section, or null when the leaf falls back to Other settings. */
  sectionId: string | null;
  advanced: boolean;
  dangerous: boolean;
  dangerImpact: string | null;
  presentation: ConfigLeafPresentation;
  unit: ConfigLeafUnit | null;
}

export interface ConfigEditorCounts {
  modified: number;
  overrides: number;
  errors: number;
  warnings: number;
}

export interface ConfigEditorSection extends ConfigEditorSectionMetadata {
  /** Active primary leaves rendered directly in the section card. */
  leaves: ConfigEditorLeaf[];
  /** Active advanced leaves rendered under the section's disclosure. */
  advancedLeaves: ConfigEditorLeaf[];
  /** Every leaf matched by this section, including inactive variants. */
  matchedLeaves: ConfigEditorLeaf[];
  counts: ConfigEditorCounts;
}

export interface ConfigEditorCategory extends ConfigEditorCategoryMetadata {
  leaves: ConfigEditorLeaf[];
  sections: ConfigEditorSection[];
  counts: ConfigEditorCounts;
}

export interface ConfigEditorUnassigned {
  id: typeof CONFIG_EDITOR_UNASSIGNED_SECTION_ID;
  label: typeof CONFIG_EDITOR_UNASSIGNED_LABEL;
  leaves: ConfigEditorLeaf[];
  counts: ConfigEditorCounts;
}

export interface ConfigEditorModel {
  source: Record<string, unknown>;
  leaves: ConfigEditorLeaf[];
  categories: ConfigEditorCategory[];
  sections: ConfigEditorSection[];
  unassigned: ConfigEditorUnassigned;
  counts: ConfigEditorCounts;
}

type SchemaWalkContext = {
  root: ConfigSchema;
  sources: readonly unknown[];
  leaves: Map<string, ConfigSchemaLeaf>;
  resolving: ReadonlySet<string>;
  variantConstraints: readonly ConfigVariantConstraint[];
};

function objectValue(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null;
}

function schemaRecord(value: unknown): ConfigSchema | null {
  return objectValue(value);
}

function schemaList(value: unknown): ConfigSchema[] {
  if (!Array.isArray(value)) return [];
  const schemas: ConfigSchema[] = [];
  for (const item of value) {
    const schema = schemaRecord(item);
    if (schema) schemas.push(schema);
  }
  return schemas;
}

function schemaTypes(schema: ConfigSchema): string[] {
  const type = schema.type;
  if (typeof type === 'string') return [type];
  return Array.isArray(type)
    ? type.filter((item): item is string => typeof item === 'string')
    : [];
}

function localRefTarget(root: ConfigSchema, ref: string): ConfigSchema | null {
  if (!ref.startsWith('#/')) return null;
  let value: unknown = root;
  for (const encodedSegment of ref.slice(2).split('/')) {
    const object = objectValue(value);
    if (!object) return null;
    const segment = encodedSegment.replaceAll('~1', '/').replaceAll('~0', '~');
    value = object[segment];
  }
  return schemaRecord(value);
}

export function resolveConfigSchema(
  root: ConfigSchema,
  schema: ConfigSchema,
  resolving: ReadonlySet<string> = new Set(),
): ConfigSchema {
  const ref = typeof schema.$ref === 'string' ? schema.$ref : null;
  if (!ref || resolving.has(ref)) return schema;
  const target = localRefTarget(root, ref);
  if (!target) return schema;

  const nextResolving = new Set(resolving);
  nextResolving.add(ref);
  const resolved = resolveConfigSchema(root, target, nextResolving);
  const siblings = Object.fromEntries(
    Object.entries(schema).filter(([key]) => key !== '$ref'),
  );
  return { ...resolved, ...siblings };
}

export function isNullableConfigSchema(
  root: ConfigSchema,
  schema: ConfigSchema,
): boolean {
  const resolved = resolveConfigSchema(root, schema);
  if (schemaTypes(resolved).includes('null')) return true;
  return [...schemaList(resolved.oneOf), ...schemaList(resolved.anyOf)].some(
    (branch) => schemaTypes(resolveConfigSchema(root, branch)).includes('null'),
  );
}

function nonNullVariants(
  root: ConfigSchema,
  schema: ConfigSchema,
): ConfigSchema[] {
  const resolved = resolveConfigSchema(root, schema);
  const variants = [
    ...schemaList(resolved.oneOf),
    ...schemaList(resolved.anyOf),
  ].filter(
    (branch) =>
      !schemaTypes(resolveConfigSchema(root, branch)).includes('null'),
  );
  return variants.length > 0 ? variants : [resolved];
}

function variantTag(root: ConfigSchema, schema: ConfigSchema) {
  const resolved = resolveConfigSchema(root, schema);
  const properties = schemaRecord(resolved.properties);
  if (!properties) return null;
  for (const [property, propertyValue] of Object.entries(properties)) {
    const propertySchema = schemaRecord(propertyValue);
    if (!propertySchema) continue;
    const concrete = resolveConfigSchema(root, propertySchema);
    const value =
      typeof concrete.const === 'string'
        ? concrete.const
        : Array.isArray(concrete.enum) &&
            concrete.enum.length === 1 &&
            typeof concrete.enum[0] === 'string'
          ? concrete.enum[0]
          : null;
    if (value !== null) return { property, value };
  }
  return null;
}

export function getConfigSchemaVariants(
  root: ConfigSchema,
  schema: ConfigSchema,
): ConfigSchemaVariant[] {
  return nonNullVariants(root, schema).map((branch, index) => {
    const resolved = resolveConfigSchema(root, branch);
    const tag = variantTag(root, resolved);
    return {
      schema: resolved,
      label:
        (typeof resolved.title === 'string' && resolved.title) ||
        tag?.value ||
        `Option ${index + 1}`,
      tag,
    };
  });
}

export function configPathToString(path: ConfigPath): string {
  return path.reduce<string>((result, segment) => {
    if (typeof segment === 'number') return `${result}[${segment}]`;
    if (segment === '*') return result ? `${result}.*` : '*';
    return result ? `${result}.${segment}` : segment;
  }, '');
}

export function parseConfigPath(
  path: string | ConfigPath,
): ConfigPathSegment[] {
  if (typeof path !== 'string') return [...path];
  if (!path) return [];
  const segments: ConfigPathSegment[] = [];
  for (const part of path.split('.')) {
    const property = part.match(/^[^[]+/)?.[0];
    if (property) segments.push(property);
    for (const match of part.matchAll(/\[(\d+)]/g)) {
      segments.push(Number(match[1]));
    }
  }
  return segments;
}

export function getConfigValue(
  value: unknown,
  path: string | ConfigPath,
): unknown {
  let current = value;
  for (const segment of parseConfigPath(path)) {
    if (segment === '*') return undefined;
    if (typeof segment === 'number') {
      if (!Array.isArray(current)) return undefined;
      current = current[segment];
    } else {
      const object = objectValue(current);
      if (!object) return undefined;
      current = object[segment];
    }
  }
  return current;
}

export function setConfigValue(
  value: unknown,
  path: string | ConfigPath,
  nextValue: unknown,
): unknown {
  const segments = parseConfigPath(path);
  if (segments.length === 0) return nextValue;

  const update = (current: unknown, index: number): unknown => {
    const segment = segments[index];
    if (segment === undefined || segment === '*') return current;
    const last = index === segments.length - 1;
    const object = objectValue(current);
    const clone = Array.isArray(current)
      ? [...current]
      : object
        ? { ...object }
        : typeof segment === 'number'
          ? []
          : {};
    if (typeof segment === 'number') {
      if (!Array.isArray(clone)) return current;
      clone[segment] = last ? nextValue : update(clone[segment], index + 1);
    } else {
      if (Array.isArray(clone)) return current;
      clone[segment] = last ? nextValue : update(clone[segment], index + 1);
    }
    return clone;
  };

  return update(value, 0);
}

export function unsetConfigValue(
  value: unknown,
  path: string | ConfigPath,
): unknown {
  const segments = parseConfigPath(path);
  if (segments.length === 0) return {};

  const update = (current: unknown, index: number): unknown => {
    const segment = segments[index];
    if (segment === undefined || segment === '*') return current;
    const last = index === segments.length - 1;
    if (typeof segment === 'number') {
      if (!Array.isArray(current)) return current;
      const clone = [...current];
      if (last) clone.splice(segment, 1);
      else clone[segment] = update(clone[segment], index + 1);
      return clone;
    }
    const object = objectValue(current);
    if (!object || !(segment in object)) return current;
    const clone = { ...object };
    if (last) delete clone[segment];
    else clone[segment] = update(clone[segment], index + 1);
    return clone;
  };

  return update(value, 0);
}

function schemaForProperty(
  root: ConfigSchema,
  schema: ConfigSchema,
  property: string,
): ConfigSchema | null {
  for (const variant of nonNullVariants(root, schema)) {
    const resolved = resolveConfigSchema(root, variant);
    const properties = schemaRecord(resolved.properties);
    const propertySchema = properties && schemaRecord(properties[property]);
    if (propertySchema) return propertySchema;
    const additional = schemaRecord(resolved.additionalProperties);
    if (additional) return additional;
  }
  return null;
}

function schemaForArrayItem(
  root: ConfigSchema,
  schema: ConfigSchema,
): ConfigSchema | null {
  for (const variant of nonNullVariants(root, schema)) {
    const resolved = resolveConfigSchema(root, variant);
    const items = schemaRecord(resolved.items);
    if (items) return items;
  }
  return null;
}

function normalizeValue(
  root: ConfigSchema,
  schema: ConfigSchema,
  value: unknown,
): unknown {
  if (value === null || value === undefined) return undefined;
  if (Array.isArray(value)) {
    const itemSchema = schemaForArrayItem(root, schema) ?? {};
    const normalized = value
      .map((item) => normalizeValue(root, itemSchema, item))
      .filter((item) => item !== undefined);
    return normalized;
  }
  const object = objectValue(value);
  if (!object) return value;

  const normalized: Record<string, unknown> = {};
  for (const [key, child] of Object.entries(object)) {
    const childSchema = schemaForProperty(root, schema, key) ?? {};
    const normalizedChild = normalizeValue(root, childSchema, child);
    if (normalizedChild !== undefined) normalized[key] = normalizedChild;
  }
  return normalized;
}

export function normalizeConfigDraft(
  schema: ConfigSchema,
  draft: Record<string, unknown>,
): Record<string, unknown> {
  return normalizeValue(schema, schema, draft) as Record<string, unknown>;
}

function inferredSchema(value: unknown): ConfigSchema {
  if (Array.isArray(value)) return { type: 'array' };
  if (objectValue(value)) return { type: 'object' };
  if (value === null) return { type: ['null', 'string'] };
  return { type: typeof value };
}

function valuesAt(sources: readonly unknown[], path: ConfigPath): unknown[] {
  return sources
    .map((source) => getConfigValue(source, path))
    .filter((value) => value !== undefined && value !== null);
}

function leafKind(
  root: ConfigSchema,
  schema: ConfigSchema,
): ConfigEditorLeafKind {
  const candidates = nonNullVariants(root, schema).map((candidate) =>
    resolveConfigSchema(root, candidate),
  );
  for (const candidate of candidates) {
    if ('const' in candidate || Array.isArray(candidate.enum)) return 'enum';
  }
  for (const candidate of candidates) {
    const type = schemaTypes(candidate).find((item) => item !== 'null');
    if (
      type === 'string' ||
      type === 'integer' ||
      type === 'number' ||
      type === 'boolean' ||
      type === 'array'
    ) {
      return type;
    }
  }
  return 'unknown';
}

function addLeaf(
  context: SchemaWalkContext,
  path: ConfigPath,
  schema: ConfigSchema,
  required: boolean,
  unknown: boolean,
): void {
  const pathString = configPathToString(path);
  const existing = context.leaves.get(pathString);
  if (existing) {
    const schemas = schemaList(existing.schema.oneOf);
    const alternatives = [...existing.variantAlternatives];
    if (
      !alternatives.some(
        (alternative) =>
          alternative.length === context.variantConstraints.length &&
          alternative.every((constraint, index) => {
            const current = context.variantConstraints[index];
            return (
              current !== undefined &&
              current.value === constraint.value &&
              current.path.length === constraint.path.length &&
              current.path.every(
                (segment, segmentIndex) =>
                  segment === constraint.path[segmentIndex],
              )
            );
          }),
      )
    ) {
      alternatives.push(context.variantConstraints);
    }
    context.leaves.set(pathString, {
      ...existing,
      schema: {
        oneOf: [...(schemas.length > 0 ? schemas : [existing.schema]), schema],
      },
      required: existing.required || required,
      nullable:
        existing.nullable || isNullableConfigSchema(context.root, schema),
      unknown: existing.unknown && unknown,
      variantAlternatives: alternatives,
    });
    return;
  }
  context.leaves.set(pathString, {
    path: [...path],
    pathString,
    schema,
    kind: leafKind(context.root, schema),
    required,
    nullable: isNullableConfigSchema(context.root, schema),
    unknown,
    variantAlternatives: [context.variantConstraints],
  });
}

function walkSchema(
  context: SchemaWalkContext,
  schema: ConfigSchema,
  path: ConfigPath,
  required: boolean,
  unknown: boolean,
): void {
  const ref = typeof schema.$ref === 'string' ? schema.$ref : null;
  if (ref && context.resolving.has(ref)) {
    addLeaf(context, path, schema, required, unknown);
    return;
  }
  const activeContext =
    ref === null
      ? context
      : {
          ...context,
          resolving: new Set(context.resolving).add(ref),
        };
  const resolved = resolveConfigSchema(activeContext.root, schema);
  const declaredVariants = [
    ...schemaList(resolved.oneOf),
    ...schemaList(resolved.anyOf),
  ].filter(
    (branch) =>
      !schemaTypes(resolveConfigSchema(activeContext.root, branch)).includes(
        'null',
      ),
  );
  if (declaredVariants.length > 0) {
    const containsObject = declaredVariants.some((variant) => {
      const concrete = resolveConfigSchema(activeContext.root, variant);
      if (
        schemaTypes(concrete).includes('object') ||
        schemaRecord(concrete.properties) !== null
      ) {
        return true;
      }
      const itemSchema = schemaRecord(concrete.items);
      return (
        schemaTypes(concrete).includes('array') &&
        itemSchema !== null &&
        nonNullVariants(activeContext.root, itemSchema).some((item) => {
          const resolvedItem = resolveConfigSchema(activeContext.root, item);
          return (
            schemaTypes(resolvedItem).includes('object') ||
            schemaRecord(resolvedItem.properties) !== null
          );
        })
      );
    });
    if (!containsObject) {
      addLeaf(activeContext, path, resolved, required, unknown);
      return;
    }
    for (const variant of declaredVariants) {
      const tag = variantTag(activeContext.root, variant);
      const variantContext: SchemaWalkContext = tag
        ? {
            ...activeContext,
            variantConstraints: [
              ...activeContext.variantConstraints,
              { path: [...path, tag.property], value: tag.value },
            ],
          }
        : activeContext;
      walkSchema(variantContext, variant, path, required, unknown);
    }
    return;
  }

  const concrete = resolved;
  const types = schemaTypes(concrete).filter((type) => type !== 'null');
  const properties = schemaRecord(concrete.properties);
  const isObject = types.includes('object') || properties !== null;
  if (isObject) {
    const requiredProperties = new Set(
      Array.isArray(concrete.required)
        ? concrete.required.filter(
            (item): item is string => typeof item === 'string',
          )
        : [],
    );
    const knownProperties = properties ?? {};
    for (const [property, propertyValue] of Object.entries(knownProperties)) {
      const propertySchema = schemaRecord(propertyValue);
      if (!propertySchema) continue;
      walkSchema(
        activeContext,
        propertySchema,
        [...path, property],
        requiredProperties.has(property),
        unknown,
      );
    }

    const sourceKeys = new Set<string>();
    for (const value of valuesAt(activeContext.sources, path)) {
      const object = objectValue(value);
      if (!object) continue;
      for (const key of Object.keys(object)) sourceKeys.add(key);
    }
    const additionalSchema = schemaRecord(concrete.additionalProperties);
    for (const key of sourceKeys) {
      if (key in knownProperties) continue;
      const samples = valuesAt(activeContext.sources, [...path, key]);
      walkSchema(
        activeContext,
        additionalSchema ?? inferredSchema(samples[0]),
        [...path, key],
        false,
        additionalSchema === null,
      );
    }
    if (additionalSchema && sourceKeys.size === 0) {
      walkSchema(activeContext, additionalSchema, [...path, '*'], false, false);
    }
    if (
      Object.keys(knownProperties).length === 0 &&
      !additionalSchema &&
      sourceKeys.size === 0
    ) {
      addLeaf(activeContext, path, concrete, required, unknown);
    }
    return;
  }

  if (types.includes('array')) {
    const itemSchema = schemaRecord(concrete.items);
    if (!itemSchema) {
      addLeaf(activeContext, path, concrete, required, unknown);
      return;
    }
    const itemTypes = nonNullVariants(activeContext.root, itemSchema).flatMap(
      (item) => schemaTypes(resolveConfigSchema(activeContext.root, item)),
    );
    const itemHasProperties = nonNullVariants(
      activeContext.root,
      itemSchema,
    ).some(
      (item) =>
        schemaRecord(
          resolveConfigSchema(activeContext.root, item).properties,
        ) !== null,
    );
    if (!itemTypes.includes('object') && !itemHasProperties) {
      addLeaf(activeContext, path, concrete, required, unknown);
      return;
    }

    const indexes = new Set<number>();
    for (const value of valuesAt(activeContext.sources, path)) {
      if (!Array.isArray(value)) continue;
      for (let index = 0; index < value.length; index += 1) indexes.add(index);
    }
    if (indexes.size === 0) {
      walkSchema(activeContext, itemSchema, [...path, '*'], false, unknown);
    } else {
      for (const index of indexes) {
        walkSchema(activeContext, itemSchema, [...path, index], false, unknown);
      }
    }
    return;
  }

  addLeaf(activeContext, path, concrete, required, unknown);
}

export function expandConfigSchema(
  schema: ConfigSchema,
  ...sources: readonly unknown[]
): ConfigSchemaLeaf[] {
  const leaves = new Map<string, ConfigSchemaLeaf>();
  walkSchema(
    {
      root: schema,
      sources,
      leaves,
      resolving: new Set(),
      variantConstraints: [],
    },
    schema,
    [],
    true,
    false,
  );
  return [...leaves.values()].sort((left, right) =>
    left.pathString.localeCompare(right.pathString),
  );
}

function valuesEqual(left: unknown, right: unknown): boolean {
  if (Object.is(left, right)) return true;
  if (Array.isArray(left) && Array.isArray(right)) {
    return (
      left.length === right.length &&
      left.every((item, index) => valuesEqual(item, right[index]))
    );
  }
  const leftObject = objectValue(left);
  const rightObject = objectValue(right);
  if (leftObject && rightObject) {
    const leftKeys = Object.keys(leftObject);
    const rightKeys = Object.keys(rightObject);
    return (
      leftKeys.length === rightKeys.length &&
      leftKeys.every(
        (key) =>
          key in rightObject && valuesEqual(leftObject[key], rightObject[key]),
      )
    );
  }
  return false;
}

function overrideForPath(
  overrides: readonly ConfigOverrideInfo[],
  pathString: string,
): ConfigOverrideInfo | null {
  return (
    overrides.find(
      (override) =>
        override.path === pathString ||
        override.path.startsWith(`${pathString}.`) ||
        override.path.startsWith(`${pathString}[`) ||
        pathString.startsWith(`${override.path}.`) ||
        pathString.startsWith(`${override.path}[`),
    ) ?? null
  );
}

export function resolveConfigValue(
  path: string | ConfigPath,
  values: {
    draft: Record<string, unknown> | null;
    editor?: Record<string, unknown>;
    file: Record<string, unknown>;
    defaults: Record<string, unknown>;
    effective: Record<string, unknown>;
    overrides?: readonly ConfigOverrideInfo[];
  },
): ConfigValueResolution {
  const parsedPath = parseConfigPath(path);
  const fileValue = getConfigValue(values.file, parsedPath);
  const defaultValue = getConfigValue(values.defaults, parsedPath);
  const effectiveValue = getConfigValue(values.effective, parsedPath);
  const editorSource = values.editor ?? values.draft ?? values.file;
  const editorValue = getConfigValue(editorSource, parsedPath);
  const editorSet = editorValue !== undefined && editorValue !== null;
  const edited =
    (values.draft !== null || values.editor !== undefined) &&
    !valuesEqual(editorValue, fileValue);
  return {
    editorValue,
    fileValue,
    defaultValue,
    effectiveValue,
    resolvedFileValue: editorSet ? editorValue : defaultValue,
    origin: editorSet
      ? edited
        ? 'draft'
        : 'file'
      : defaultValue !== undefined
        ? 'default'
        : 'unset',
    override: overrideForPath(
      values.overrides ?? [],
      configPathToString(parsedPath),
    ),
  };
}

function allValidationIssues(
  response: ConfigEditorResponse,
): ConfigValidationIssue[] {
  const report = response.last_validation;
  return report
    ? [
        ...report.file.issues,
        ...report.effective.issues,
        ...report.filesystem,
        ...report.overrides,
      ]
    : [];
}

function issuesForPath(
  issues: readonly ConfigValidationIssue[],
  pathString: string,
): ConfigValidationIssue[] {
  return issues.filter(
    (issue) =>
      issue.path === pathString ||
      issue.path.startsWith(`${pathString}.`) ||
      pathString.startsWith(`${issue.path}.`) ||
      issue.path.startsWith(`${pathString}[`) ||
      pathString.startsWith(`${issue.path}[`),
  );
}

export function countConfigValidationIssues(
  issues: readonly ConfigValidationIssue[],
): Pick<ConfigEditorCounts, 'errors' | 'warnings'> {
  const issueKeys = new Set<string>();
  let errors = 0;
  let warnings = 0;
  for (const issue of issues) {
    const key = `${issue.severity}\u0000${issue.path}\u0000${issue.code}\u0000${issue.message}`;
    if (issueKeys.has(key)) continue;
    issueKeys.add(key);
    if (issue.severity === 'error') errors += 1;
    else warnings += 1;
  }
  return { errors, warnings };
}

export function countConfigEditorLeaves(
  leaves: readonly ConfigEditorLeaf[],
): ConfigEditorCounts {
  const issues = leaves.flatMap((leaf) => leaf.issues);
  const validationCounts = countConfigValidationIssues(issues);
  return {
    modified: leaves.filter((leaf) => leaf.modified).length,
    overrides: new Set(
      leaves
        .map((leaf) => leaf.override?.path)
        .filter((path): path is string => path !== undefined),
    ).size,
    ...validationCounts,
  };
}

export function buildConfigEditorModel(
  response: ConfigEditorResponse,
  draft: Record<string, unknown> = response.draft ?? response.file_config,
): ConfigEditorModel {
  const source = draft;
  const issues = allValidationIssues(response);
  const leaves = expandConfigSchema(
    response.schema,
    response.default_config,
    response.file_config,
    response.effective_config,
    source,
  ).map<ConfigEditorLeaf>((leaf) => {
    const resolution = resolveConfigValue(leaf.path, {
      draft: response.draft,
      editor: source,
      file: response.file_config,
      defaults: response.default_config,
      effective: response.effective_config,
      overrides: response.overrides,
    });
    const classification = classifyConfigLeaf(leaf.pathString, leaf.kind);
    return {
      ...leaf,
      ...resolution,
      modified: !valuesEqual(resolution.editorValue, resolution.fileValue),
      overridden: resolution.override !== null,
      issues: issuesForPath(issues, leaf.pathString),
      active: isConfigLeafActive(leaf, source),
      ...classification,
    };
  });
  const sections = CONFIG_EDITOR_SECTION_LIST.map<ConfigEditorSection>(
    (metadata) => {
      const matchedLeaves = leaves.filter(
        (leaf) => leaf.sectionId === metadata.id,
      );
      const activeLeaves = matchedLeaves.filter((leaf) => leaf.active);
      return {
        ...metadata,
        leaves: activeLeaves.filter((leaf) => !leaf.advanced),
        advancedLeaves: activeLeaves.filter((leaf) => leaf.advanced),
        matchedLeaves,
        counts: countConfigEditorLeaves(matchedLeaves),
      };
    },
  );
  const unassignedLeaves = leaves.filter((leaf) => leaf.sectionId === null);
  const unassigned: ConfigEditorUnassigned = {
    id: CONFIG_EDITOR_UNASSIGNED_SECTION_ID,
    label: CONFIG_EDITOR_UNASSIGNED_LABEL,
    leaves: unassignedLeaves,
    counts: countConfigEditorLeaves(unassignedLeaves),
  };
  const categories = CONFIG_EDITOR_CATEGORIES.map((metadata) => {
    const categorySections = sections.filter(
      (section) => section.categoryId === metadata.id,
    );
    const categoryLeaves = categorySections.flatMap(
      (section) => section.matchedLeaves,
    );
    return {
      ...metadata,
      leaves: categoryLeaves,
      sections: categorySections,
      counts: countConfigEditorLeaves(categoryLeaves),
    };
  });
  const counts = countConfigEditorLeaves(leaves);
  const validationCounts = countConfigValidationIssues(issues);
  return {
    source,
    leaves,
    categories,
    sections,
    unassigned,
    counts: {
      ...counts,
      overrides: new Set(response.overrides.map((override) => override.path))
        .size,
      ...validationCounts,
    },
  };
}
