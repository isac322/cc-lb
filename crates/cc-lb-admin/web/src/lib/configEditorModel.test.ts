// @vitest-environment jsdom

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { type ConfigEditorResponse, downloadConfigDraft } from './api';
import {
  buildConfigEditorModel,
  CONFIG_EDITOR_CATEGORIES,
  type ConfigSchema,
  expandConfigSchema,
  getConfigSchemaVariants,
  getConfigValue,
  isNullableConfigSchema,
  normalizeConfigDraft,
  OPAQUE_STORAGE_URL_SENTINEL,
  parseConfigPath,
  resolveConfigValue,
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
        providers: {
          type: 'array',
          items: { $ref: '#/$defs/AdminProvider' },
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
        scopes: { type: 'array', items: { type: 'string' } },
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
  admin: { providers: [] },
  oauth: null,
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
    providers: [
      { kind: 'static_token', id: 'local', token_env: 'CC_LB_ADMIN_TOKEN' },
    ],
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
      'admin.providers[0].audiences',
      'admin.providers[0].id',
      'admin.providers[0].kind',
      'admin.providers[0].team_domain',
      'admin.providers[0].token_env',
      'listener.port',
      'listener.tls.cert_path',
      'listener.tls.key_path',
      'oauth.scopes',
      'scheduler.recurring_jobs.custom_job.enabled',
      'scheduler.recurring_jobs.custom_job.interval_secs',
      'scheduler.recurring_jobs.custom_job.jitter_secs',
      'scheduler.recurring_jobs.usage_rollup.enabled',
      'scheduler.recurring_jobs.usage_rollup.interval_secs',
      'scheduler.recurring_jobs.usage_rollup.jitter_secs',
      'storage.kind',
      'storage.path',
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
      admin: { providers: [] },
      scheduler: { recurring_jobs: {} },
    }).map((leaf) => leaf.pathString);

    expect(paths).toContain('admin.providers.*.token_env');
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
        oauth: { scopes: ['one', null, 'two'] },
        unknown_file_key: { preserved: 'yes', removed: null },
      }),
    ).toEqual({
      listener: { port: 9000 },
      oauth: { scopes: ['one', 'two'] },
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
    const resolution = resolveConfigValue('admin.providers[0].token_env', {
      draft: response.draft,
      file: response.file_config,
      defaults: response.default_config,
      effective: response.effective_config,
      overrides: [
        {
          path: 'admin.providers',
          source: 'special_env',
          name: 'CC_LB_ADMIN_AUTH_PROVIDERS_JSON',
          sensitive: true,
          effective_value: null,
        },
      ],
    });

    expect(resolution.override).toMatchObject({
      path: 'admin.providers',
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
