// @vitest-environment jsdom

import { readFileSync } from 'node:fs';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { ConfigEditorResponse } from './api';
import { downloadConfigDraft } from './api';
import {
  buildConfigEditorModel,
  CONFIG_EDITOR_CATEGORIES,
  CONFIG_EDITOR_SECTIONS,
  CONFIG_EDITOR_UNASSIGNED_SECTION_ID,
  CONFIG_RECURRING_JOBS,
  type ConfigSchema,
  classifyConfigLeaf,
  expandConfigSchema,
  getConfigSchemaVariants,
  getConfigValue,
  humanizeConfigValue,
  isNullableConfigSchema,
  matchConfigPath,
  normalizeConfigDraft,
  OPAQUE_STORAGE_URL_SENTINEL,
  parseConfigPath,
  recurringJobMetadata,
  resolveConfigFieldGuidance,
  resolveConfigValue,
  searchConfigLeaves,
  setConfigValue,
  unsetConfigValue,
} from './configEditorModel';

vi.mock('./auth', () => ({
  clearAdminToken: vi.fn(),
  getAdminToken: vi.fn(() => 'admin-token'),
}));

const schema = {
  type: 'object',
  required: ['listener', 'storage', 'scheduler', 'admin'],
  properties: {
    listener: { $ref: '#/$defs/Listener' },
    storage: { $ref: '#/$defs/Storage' },
    scheduler: { $ref: '#/$defs/Scheduler' },
    admin: { $ref: '#/$defs/Admin' },
    oauth: {
      anyOf: [{ $ref: '#/$defs/OAuth' }, { type: 'null' }],
    },
  },
  $defs: {
    Listener: {
      type: 'object',
      required: ['port'],
      properties: {
        port: { type: 'integer', default: 8080 },
        tls: {
          anyOf: [{ $ref: '#/$defs/Tls' }, { type: 'null' }],
        },
      },
    },
    Tls: {
      type: 'object',
      properties: {
        cert_path: { type: ['string', 'null'] },
        key_path: { anyOf: [{ type: 'string' }, { type: 'null' }] },
      },
    },
    Storage: {
      oneOf: [
        {
          title: 'SQLite',
          type: 'object',
          required: ['kind', 'path'],
          properties: {
            kind: { const: 'sqlite' },
            path: { type: 'string' },
          },
        },
        {
          title: 'PostgreSQL',
          type: 'object',
          required: ['kind', 'url'],
          properties: {
            kind: { const: 'postgres' },
            url: { type: 'string' },
            pool: {
              type: 'object',
              properties: {
                max_connections: { type: 'integer', default: 20 },
                acquire_timeout_secs: { type: 'integer', default: 5 },
              },
            },
          },
        },
      ],
    },
    Scheduler: {
      type: 'object',
      properties: {
        recurring_jobs: {
          type: 'object',
          additionalProperties: { $ref: '#/$defs/RecurringJob' },
        },
      },
    },
    RecurringJob: {
      type: 'object',
      properties: {
        enabled: { type: 'boolean', default: true },
        interval_secs: { type: 'integer', default: 3600 },
        jitter_secs: { type: 'integer', default: 30 },
      },
    },
    Admin: {
      type: 'object',
      properties: {
        auth: {
          type: 'object',
          properties: {
            providers: {
              type: 'array',
              items: { $ref: '#/$defs/AdminProvider' },
            },
          },
        },
      },
    },
    AdminProvider: {
      oneOf: [
        {
          title: 'Static token',
          type: 'object',
          required: ['kind', 'id', 'token_env'],
          properties: {
            kind: { const: 'static_token' },
            id: { type: 'string' },
            token_env: { type: 'string' },
          },
        },
        {
          title: 'Cloudflare Access',
          type: 'object',
          required: ['kind', 'id', 'team_domain', 'audiences'],
          properties: {
            kind: { const: 'cloudflare_access' },
            id: { type: 'string' },
            team_domain: { type: 'string' },
            audiences: { type: 'array', items: { type: 'string' } },
          },
        },
      ],
    },
    OAuth: {
      type: 'object',
      properties: {
        anthropic: {
          anyOf: [
            {
              type: 'object',
              properties: {
                scopes: { type: 'array', items: { type: 'string' } },
              },
            },
            { type: 'null' },
          ],
        },
      },
    },
  },
} satisfies ConfigSchema;

const defaultConfig = {
  listener: { port: 8080, tls: null },
  storage: { kind: 'sqlite', path: '/var/lib/cc-lb/storage.sqlite' },
  scheduler: {
    recurring_jobs: {
      usage_rollup: { enabled: true, interval_secs: 3600, jitter_secs: 30 },
    },
  },
  admin: { auth: { providers: [] } },
  oauth: {},
};

const fileConfig = {
  listener: { port: 9000 },
  storage: { kind: 'postgres', url: OPAQUE_STORAGE_URL_SENTINEL },
  scheduler: {
    recurring_jobs: {
      custom_job: { enabled: false, interval_secs: 45, jitter_secs: 4 },
    },
  },
  admin: {
    auth: {
      providers: [
        { kind: 'static_token', id: 'local', token_env: 'CC_LB_ADMIN_TOKEN' },
      ],
    },
  },
  unknown_file_key: { preserved: 'yes' },
};

function editorResponse(): ConfigEditorResponse {
  return {
    schema,
    default_config: defaultConfig,
    file_config: fileConfig,
    effective_config: {
      ...defaultConfig,
      ...fileConfig,
      listener: { port: 9100, tls: null },
    },
    draft: {
      ...fileConfig,
      listener: { port: 9200, tls: { cert_path: null } },
    },
    revision: 4,
    last_validated_revision: 4,
    last_validation: {
      revision: 4,
      file: {
        valid: false,
        issues: [
          {
            path: 'scheduler.recurring_jobs.custom_job',
            code: 'unknown_recurring_job',
            message: 'Unknown recurring job',
            severity: 'error',
          },
        ],
      },
      effective: { valid: true, issues: [] },
      filesystem: [],
      overrides: [
        {
          path: 'listener.port',
          code: 'env_supplied',
          message: 'Supplied by the environment',
          severity: 'warning',
        },
      ],
    },
    saved_at_unix_secs: 1_700_000_000,
    file: {
      path: '/etc/cc-lb/cc-lb.toml',
      exists: true,
      mode: 'writable',
      reason: null,
      fingerprint: 'sha256:current',
    },
    overrides: [
      {
        path: 'listener.port',
        source: 'env',
        name: 'CC_LB_LISTENER__PORT',
        sensitive: false,
        effective_value: 9100,
      },
    ],
    restart_required: true,
  };
}

describe('schema traversal', () => {
  it('enumerates every schema leaf across refs, nullable objects, unions, arrays, maps, and unknown keys', () => {
    const leaves = expandConfigSchema(schema, defaultConfig, fileConfig);
    const paths = leaves.map((leaf) => leaf.pathString);
    const expectedPaths = [
      'admin.auth.providers[0].audiences',
      'admin.auth.providers[0].id',
      'admin.auth.providers[0].kind',
      'admin.auth.providers[0].team_domain',
      'admin.auth.providers[0].token_env',
      'listener.port',
      'listener.tls.cert_path',
      'listener.tls.key_path',
      'oauth.anthropic.scopes',
      'scheduler.recurring_jobs.custom_job.enabled',
      'scheduler.recurring_jobs.custom_job.interval_secs',
      'scheduler.recurring_jobs.custom_job.jitter_secs',
      'scheduler.recurring_jobs.usage_rollup.enabled',
      'scheduler.recurring_jobs.usage_rollup.interval_secs',
      'scheduler.recurring_jobs.usage_rollup.jitter_secs',
      'storage.kind',
      'storage.path',
      'storage.pool.acquire_timeout_secs',
      'storage.pool.max_connections',
      'storage.url',
      'unknown_file_key.preserved',
    ];

    expect(paths).toHaveLength(expectedPaths.length);
    expect(paths).toEqual(expect.arrayContaining(expectedPaths));
    expect(
      leaves.find((leaf) => leaf.pathString === 'unknown_file_key.preserved'),
    ).toMatchObject({
      kind: 'string',
      required: false,
      unknown: true,
    });
    expect(
      expandConfigSchema(schema).find(
        (leaf) => leaf.pathString === 'listener.tls.key_path',
      )?.nullable,
    ).toBe(true);
  });

  it('describes nullable schemas and every tagged-union option', () => {
    const oauthSchema = schema.properties.oauth;
    const variants = getConfigSchemaVariants(schema, schema.$defs.Storage);

    expect(isNullableConfigSchema(schema, oauthSchema)).toBe(true);
    expect(variants.map((variant) => variant.tag)).toEqual([
      { property: 'kind', value: 'sqlite' },
      { property: 'kind', value: 'postgres' },
    ]);
  });

  it('uses wildcard paths when object arrays and maps have no instances', () => {
    const paths = expandConfigSchema(schema, {
      admin: { auth: { providers: [] } },
      scheduler: { recurring_jobs: {} },
    }).map((leaf) => leaf.pathString);

    expect(paths).toContain('admin.auth.providers.*.token_env');
    expect(paths).toContain('scheduler.recurring_jobs.*.enabled');
  });

  it('stops recursive refs without losing the recursive field', () => {
    const recursiveSchema = {
      type: 'object',
      properties: { node: { $ref: '#/$defs/Node' } },
      $defs: {
        Node: {
          type: 'object',
          properties: {
            value: { type: 'string' },
            child: {
              anyOf: [{ $ref: '#/$defs/Node' }, { type: 'null' }],
            },
          },
        },
      },
    } satisfies ConfigSchema;

    expect(
      expandConfigSchema(recursiveSchema).map((leaf) => leaf.pathString),
    ).toEqual(['node.child', 'node.value']);
  });
});

describe('immutable paths and normalization', () => {
  it('gets, sets, and unsets nested object and array paths without mutation', () => {
    const original = {
      admin: { providers: [{ id: 'first' }, { id: 'second' }] },
      untouched: { value: true },
    };
    const changed = setConfigValue(
      original,
      'admin.providers[1].id',
      'updated',
    ) as typeof original;
    const removed = unsetConfigValue(changed, [
      'admin',
      'providers',
      0,
    ]) as typeof original;

    expect(
      getConfigValue(changed, parseConfigPath('admin.providers[1].id')),
    ).toBe('updated');
    expect(original.admin.providers[1]?.id).toBe('second');
    expect(changed.untouched).toBe(original.untouched);
    expect(removed.admin.providers).toEqual([{ id: 'updated' }]);
  });

  it('removes null unset markers while preserving unknown file keys', () => {
    expect(
      normalizeConfigDraft(schema, {
        listener: { port: 9000, tls: null },
        oauth: { anthropic: { scopes: ['one', null, 'two'] } },
        unknown_file_key: { preserved: 'yes', removed: null },
      }),
    ).toEqual({
      listener: { port: 9000 },
      oauth: { anthropic: { scopes: ['one', 'two'] } },
      unknown_file_key: { preserved: 'yes' },
    });
  });
});

describe('resolved editor model', () => {
  it('separates file, default, effective, and draft values', () => {
    const response = editorResponse();
    const resolution = resolveConfigValue('listener.port', {
      draft: response.draft,
      file: response.file_config,
      defaults: response.default_config,
      effective: response.effective_config,
      overrides: response.overrides,
    });

    expect(resolution).toMatchObject({
      editorValue: 9200,
      fileValue: 9000,
      defaultValue: 8080,
      effectiveValue: 9100,
      resolvedFileValue: 9200,
      origin: 'draft',
      override: { name: 'CC_LB_LISTENER__PORT' },
    });
  });
  it('matches a parent override to indexed array child fields', () => {
    const response = editorResponse();
    const resolution = resolveConfigValue('admin.auth.providers[0].token_env', {
      draft: response.draft,
      file: response.file_config,
      defaults: response.default_config,
      effective: response.effective_config,
      overrides: [
        {
          path: 'admin.auth.providers',
          source: 'special_env',
          name: 'CC_LB_ADMIN_AUTH_PROVIDERS_JSON',
          sensitive: true,
          effective_value: null,
        },
      ],
    });

    expect(resolution.override).toMatchObject({
      path: 'admin.auth.providers',
      name: 'CC_LB_ADMIN_AUTH_PROVIDERS_JSON',
    });
  });

  it('falls back to defaults when null marks a file value as unset', () => {
    const resolution = resolveConfigValue('listener.port', {
      draft: { listener: { port: null } },
      file: fileConfig,
      defaults: defaultConfig,
      effective: defaultConfig,
    });

    expect(resolution.editorValue).toBeNull();
    expect(resolution.resolvedFileValue).toBe(8080);
    expect(resolution.origin).toBe('default');
  });

  it('keeps unchanged draft leaves attributed to the file', () => {
    const resolution = resolveConfigValue('listener.port', {
      draft: fileConfig,
      file: fileConfig,
      defaults: defaultConfig,
      effective: defaultConfig,
    });

    expect(resolution.origin).toBe('file');
  });

  it('counts modifications, overrides, and unique validation issues', () => {
    const model = buildConfigEditorModel(editorResponse());
    const network = model.categories.find(
      (category) => category.id === 'network',
    );
    const scheduling = model.categories.find(
      (category) => category.id === 'scheduling',
    );

    expect(
      model.leaves.find((leaf) => leaf.pathString === 'listener.port'),
    ).toMatchObject({
      modified: true,
      overridden: true,
    });
    expect(
      model.leaves.some(
        (leaf) => leaf.pathString === 'unknown_file_key.preserved',
      ),
    ).toBe(true);
    expect(model.counts).toMatchObject({
      errors: 1,
      warnings: 1,
      overrides: 1,
    });
    expect(network?.counts).toMatchObject({ warnings: 1, overrides: 1 });
    expect(scheduling?.counts.errors).toBe(1);
  });

  it('publishes seven categories that cover each config root exactly once', () => {
    const roots = CONFIG_EDITOR_CATEGORIES.flatMap(
      (category) => category.roots,
    );

    expect(CONFIG_EDITOR_CATEGORIES).toHaveLength(7);
    expect(new Set(roots).size).toBe(20);
    expect(roots).toEqual(
      expect.arrayContaining([
        'listener',
        'body',
        'timeouts',
        'request_event_retention_days',
        'price_catalog',
        'storage',
        'scheduler',
        'upstream_affinity',
        'aead',
        'observability',
        'admin',
        'event_bus',
        'cluster',
        'oauth',
        'subscription_quota',
        'runtime',
        'circuit_breaker',
        'bulkhead',
        'prompt_cache_shadow',
        'limit_reservation_ttl',
      ]),
    );
  });
});
describe('config editor information architecture', () => {
  it('matches glob path patterns against segments, indexes, and map keys', () => {
    expect(matchConfigPath('listener.*', 'listener.port')).toBe(true);
    expect(matchConfigPath('listener.*', 'listener.tls.cert_path')).toBe(false);
    expect(matchConfigPath('listener.tls.**', 'listener.tls')).toBe(true);
    expect(matchConfigPath('listener.tls.**', 'listener.tls.cert_path')).toBe(
      true,
    );
    expect(
      matchConfigPath('admin.**', 'admin.auth.providers[0].token_env'),
    ).toBe(true);
    expect(
      matchConfigPath(
        'scheduler.recurring_jobs.**',
        'scheduler.recurring_jobs.custom_job.enabled',
      ),
    ).toBe(true);
    expect(
      matchConfigPath(
        'scheduler.*_concurrency',
        'scheduler.entity_concurrency',
      ),
    ).toBe(true);
    expect(
      matchConfigPath(
        'scheduler.*_concurrency',
        'scheduler.dlq_retention_days',
      ),
    ).toBe(false);
    expect(matchConfigPath('**.*_secs', 'timeouts.drain_secs')).toBe(true);
    expect(matchConfigPath('**.*_secs', 'timeouts')).toBe(false);
  });

  it('classifies leaves into sections with presentation and danger metadata', () => {
    expect(classifyConfigLeaf('listener.proxy_addr', 'string')).toMatchObject({
      categoryId: 'network',
      sectionId: 'listener-endpoints',
      advanced: false,
      dangerous: true,
      presentation: 'address',
      unit: null,
    });
    expect(
      classifyConfigLeaf('storage.pool.acquire_timeout_secs', 'integer'),
    ).toMatchObject({
      categoryId: 'data',
      sectionId: 'database-pool',
      advanced: true,
      presentation: 'duration',
      unit: 'secs',
    });
    expect(
      classifyConfigLeaf('admin.auth.providers[0].token_env', 'string'),
    ).toMatchObject({
      categoryId: 'identity',
      sectionId: 'admin-auth-providers',
      dangerous: true,
      presentation: 'env',
    });
    expect(
      classifyConfigLeaf('body.messages_cap_bytes', 'integer'),
    ).toMatchObject({
      sectionId: 'request-body-limits',
      presentation: 'bytes',
      unit: 'bytes',
    });
    expect(classifyConfigLeaf('aead.key_env', 'string')).toMatchObject({
      sectionId: 'encryption',
      dangerous: true,
      presentation: 'env',
    });
  });

  it('sends unknown roots and unmatched paths to the unassigned fallback', () => {
    expect(classifyConfigLeaf('unknown_file_key.preserved')).toMatchObject({
      sectionId: null,
      presentation: 'unknown',
    });
    expect(classifyConfigLeaf('storage')).toMatchObject({
      categoryId: 'data',
      sectionId: null,
    });
  });

  it('humanizes durations and byte counts for display', () => {
    expect(humanizeConfigValue(600, 'secs')).toBe('10 minutes');
    expect(humanizeConfigValue(90, 'secs')).toBe('1 minute 30 seconds');
    expect(humanizeConfigValue(250, 'ms')).toBe('250 ms');
    expect(humanizeConfigValue(90, 'days')).toBe('90 days');
    expect(humanizeConfigValue(33_554_432, 'bytes')).toBe('32 MiB');
    expect(humanizeConfigValue(104_857_600, 'bytes')).toBe('100 MiB');
    expect(humanizeConfigValue(512, 'bytes')).toBe('512 B');
    expect(humanizeConfigValue('nope', 'secs')).toBeNull();
    expect(humanizeConfigValue(10, null)).toBeNull();
  });

  it('groups model leaves into sections and counts advanced leaves from the active variant only', () => {
    const model = buildConfigEditorModel(editorResponse());
    const pool = model.sections.find(
      (section) => section.id === 'database-pool',
    );
    const providers = model.sections.find(
      (section) => section.id === 'admin-auth-providers',
    );
    const tls = model.sections.find((section) => section.id === 'tls');
    // Draft selects the postgres storage variant: pool leaves are active.
    expect(pool?.leaves.map((leaf) => leaf.pathString)).toEqual([
      'storage.pool.max_connections',
    ]);
    expect(pool?.advancedLeaves.map((leaf) => leaf.pathString)).toEqual([
      'storage.pool.acquire_timeout_secs',
    ]);
    // The draft's provider is static_token: cloudflare-only fields are inactive.
    expect(providers?.leaves.map((leaf) => leaf.pathString)).toEqual([
      'admin.auth.providers[0].id',
      'admin.auth.providers[0].kind',
      'admin.auth.providers[0].token_env',
    ]);
    expect(tls?.leaves.map((leaf) => leaf.pathString)).toEqual([
      'listener.tls.cert_path',
      'listener.tls.key_path',
    ]);
    expect(model.unassigned.id).toBe(CONFIG_EDITOR_UNASSIGNED_SECTION_ID);
    expect(model.unassigned.leaves.map((leaf) => leaf.pathString)).toEqual([
      'unknown_file_key.preserved',
    ]);
  });

  it('drops variant leaves from sections when the draft selects another variant', () => {
    const response = editorResponse();
    const model = buildConfigEditorModel(response, {
      ...response.file_config,
      storage: { kind: 'sqlite', path: '/var/lib/cc-lb/storage.sqlite' },
    });
    const pool = model.sections.find(
      (section) => section.id === 'database-pool',
    );
    const storage = model.sections.find(
      (section) => section.id === 'primary-storage',
    );

    expect(pool?.leaves).toHaveLength(0);
    expect(pool?.advancedLeaves).toHaveLength(0);
    expect(pool?.matchedLeaves.length).toBeGreaterThan(0);
    expect(storage?.leaves.map((leaf) => leaf.pathString)).toEqual(
      expect.arrayContaining(['storage.kind', 'storage.path']),
    );
  });

  it('searches active leaves by label, path, section, and description', () => {
    const model = buildConfigEditorModel(editorResponse());

    const byPath = searchConfigLeaves(model, 'cert_path');
    expect(byPath.map((result) => result.path)).toEqual([
      'listener.tls.cert_path',
    ]);
    expect(byPath[0]).toMatchObject({
      sectionId: 'tls',
      categoryId: 'network',
      matched: 'path',
    });
    expect(byPath[0]?.breadcrumb).toContain('TLS');

    const bySection = searchConfigLeaves(model, 'database pool');
    expect(bySection.length).toBeGreaterThan(0);
    expect(
      bySection.every((result) => result.sectionId === 'database-pool'),
    ).toBe(true);

    // Inactive union-variant leaves never appear in results.
    expect(
      searchConfigLeaves(model, 'team_domain').map((result) => result.path),
    ).toEqual([]);
    expect(searchConfigLeaves(model, '   ')).toEqual([]);
  });

  it('assigns every real schema leaf to exactly one section or the unassigned fallback', () => {
    const realSchema = JSON.parse(
      readFileSync(
        `${import.meta.dirname}/../../../../../config-schema.json`,
        'utf-8',
      ),
    ) as ConfigSchema;
    const leaves = expandConfigSchema(realSchema);
    const sectionIds = new Set(
      CONFIG_EDITOR_SECTIONS.map((section) => section.id),
    );

    expect(leaves.length).toBeGreaterThan(0);
    for (const leaf of leaves) {
      const memberships = CONFIG_EDITOR_SECTIONS.filter(
        (section) =>
          section.paths.some((pattern) =>
            matchConfigPath(pattern, leaf.pathString),
          ) ||
          section.advancedPaths.some((pattern) =>
            matchConfigPath(pattern, leaf.pathString),
          ),
      );
      expect(
        memberships.length,
        `${leaf.pathString} must match at most one section`,
      ).toBeLessThanOrEqual(1);
      if (leaf.unknown) continue;
      expect(
        memberships.length,
        `${leaf.pathString} is a schema leaf hidden from every section`,
      ).toBe(1);
      expect(sectionIds.has(memberships[0]?.id ?? '')).toBe(true);
    }
  });
});

describe('config field guidance', () => {
  const realSchema = JSON.parse(
    readFileSync(
      `${import.meta.dirname}/../../../../../config-schema.json`,
      'utf-8',
    ),
  ) as ConfigSchema;

  it('resolves explicit guidance with numeric trade-offs', () => {
    const guidance = resolveConfigFieldGuidance(
      'storage.pool.max_connections',
      undefined,
      'integer',
    );
    expect(guidance.description).toContain('pool');
    expect(guidance.lower).toBeTruthy();
    expect(guidance.higher).toBeTruthy();
    expect(guidance.impactDimensions?.length).toBeGreaterThan(0);
  });

  it('resolves boolean guidance with enabled and disabled effects', () => {
    const guidance = resolveConfigFieldGuidance(
      'observability.log_redaction',
      undefined,
      'boolean',
    );
    expect(guidance.enabled).toBeTruthy();
    expect(guidance.disabled).toBeTruthy();
  });

  it('falls back to the schema description when the table omits one', () => {
    const guidance = resolveConfigFieldGuidance(
      'runtime.wasmtime.shape_origin_policy',
      'Schema-provided description.',
      'enum',
    );
    expect(guidance.description).toBe('Schema-provided description.');
    expect(guidance.recommendation).toContain('unrestricted');
  });

  it('falls back to presentation guidance for schema leaves without metadata', () => {
    const numeric = resolveConfigFieldGuidance(
      'future.new_timeout_secs',
      undefined,
      'integer',
    );
    expect(numeric.description).toBeTruthy();
    expect(numeric.lower).toBeTruthy();
    expect(numeric.higher).toBeTruthy();

    const toggle = resolveConfigFieldGuidance(
      'future.new_flag',
      undefined,
      'boolean',
    );
    expect(toggle.description).toBeTruthy();
    expect(toggle.enabled).toBeUndefined();
    expect(toggle.disabled).toBeUndefined();
  });

  it('synthesizes recurring job guidance from the catalog and wildcard fields', () => {
    const job = resolveConfigFieldGuidance(
      'scheduler.recurring_jobs.usage_rollup',
    );
    expect(job.description).toBe(recurringJobMetadata('usage_rollup')?.purpose);

    const interval = resolveConfigFieldGuidance(
      'scheduler.recurring_jobs.usage_rollup.interval_secs',
      undefined,
      'integer',
    );
    expect(interval.description).toContain('Usage rollup');
    expect(interval.lower).toBeTruthy();
    expect(interval.higher).toBeTruthy();

    const enabled = resolveConfigFieldGuidance(
      'scheduler.recurring_jobs.usage_rollup.enabled',
      undefined,
      'boolean',
    );
    expect(enabled.description).toContain('Usage rollup');
    expect(enabled.enabled).toBeUndefined();
    expect(enabled.disabled).toBeUndefined();
  });

  it('describes unknown recurring job keys without built-in purposes', () => {
    const job = resolveConfigFieldGuidance(
      'scheduler.recurring_jobs.not_a_job',
    );
    expect(job.description).toContain('not a built-in job');

    const field = resolveConfigFieldGuidance(
      'scheduler.recurring_jobs.not_a_job.enabled',
      undefined,
      'boolean',
    );
    expect(field.description).toContain('this recurring job');
    expect(field.enabled).toBeUndefined();
  });

  it('catalogs every built-in recurring job', () => {
    expect(CONFIG_RECURRING_JOBS.map((job) => job.key)).toEqual([
      'anthropic_compat_refresh',
      'apalis_housekeeping',
      'oauth_refresh_watchdog',
      'oauth_usage_poll',
      'pool_quota_snapshot',
      'price_catalog_refresh',
      'prompt_cache_purge',
      'upstream_affinity_purge',
      'usage_prune',
      'usage_rollup',
      'warmup_watchdog',
    ]);
    for (const job of CONFIG_RECURRING_JOBS) {
      expect(job.label).toBeTruthy();
      expect(job.purpose).toBeTruthy();
    }
  });

  it('describes every canonical schema leaf with the right trade-off shape', () => {
    const leaves = expandConfigSchema(realSchema);
    expect(leaves.length).toBeGreaterThan(0);
    for (const leaf of leaves) {
      const schemaDescription =
        typeof leaf.schema.description === 'string'
          ? leaf.schema.description
          : undefined;
      const guidance = resolveConfigFieldGuidance(
        leaf.pathString,
        schemaDescription,
        leaf.kind,
      );
      expect(
        guidance.description.trim().length,
        `${leaf.pathString} must resolve a non-empty description`,
      ).toBeGreaterThan(0);
      if (leaf.kind === 'integer' || leaf.kind === 'number') {
        expect(
          guidance.lower,
          `${leaf.pathString} must describe lowering the value`,
        ).toBeTruthy();
        expect(
          guidance.higher,
          `${leaf.pathString} must describe raising the value`,
        ).toBeTruthy();
      }
    }
  });
});

describe('downloadConfigDraft', () => {
  beforeEach(() => {
    vi.stubGlobal('URL', {
      createObjectURL: vi.fn(() => 'blob:config'),
      revokeObjectURL: vi.fn(),
    });
  });

  afterEach(() => {
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
  });

  it('authenticates one attachment request and one click while pending', async () => {
    const pending = Promise.withResolvers<Response>();
    const fetchMock = vi.fn(
      (_input: RequestInfo | URL, _init?: RequestInit) => pending.promise,
    );
    vi.stubGlobal('fetch', fetchMock);
    const click = vi
      .spyOn(HTMLAnchorElement.prototype, 'click')
      .mockImplementation(() => undefined);

    const first = downloadConfigDraft(12, 'postgres://replacement');
    const duplicate = downloadConfigDraft(12, 'postgres://replacement');

    expect(duplicate).toBe(first);
    expect(fetchMock).toHaveBeenCalledTimes(1);
    pending.resolve(
      new Response('[storage]\nkind = "postgres"', {
        headers: {
          'Content-Type': 'application/toml',
          'Content-Disposition': 'attachment; filename="validated-config.toml"',
        },
      }),
    );
    await first;

    expect(fetchMock).toHaveBeenCalledWith(
      '/admin/v1/config/draft/download',
      expect.objectContaining({
        method: 'POST',
        body: JSON.stringify({
          expected_revision: 12,
          storage_url_replacement: 'postgres://replacement',
        }),
      }),
    );
    const requestInit = fetchMock.mock.calls[0]?.[1];
    expect(new Headers(requestInit?.headers).get('Authorization')).toBe(
      'Bearer admin-token',
    );
    expect(click).toHaveBeenCalledTimes(1);
    expect(URL.revokeObjectURL).toHaveBeenCalledWith('blob:config');
  });
});
