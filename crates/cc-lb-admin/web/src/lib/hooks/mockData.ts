import type {
  ConfigDiffResponse,
  ConfigDraftResponse,
  ConfigHistoryResponse,
  ConfigSchemaResponse,
  DashboardUsageResponse,
  PrincipalLimitsResponse,
  PrincipalListResponse,
} from '../api';

export const MOCK_DIRECTORY: PrincipalListResponse = {
  principals: [{ id: 'principal-a' }, { id: 'principal-b' }],
};

export const MOCK_USAGE: DashboardUsageResponse = {
  range: '1h',
  step: '1m',
  group_by: 'model',
  window_start_unix_secs: 1764000000,
  window_end_unix_secs: 1764003600,
  observed: true,
  series: [
    {
      key: 'claude-3-opus-20240229',
      buckets: [
        {
          bucket_start_unix_secs: 1764000000,
          request_count: 10,
          input_tokens: 1000,
          output_tokens: 500,
          error_count: 0,
          virtual_cost_micros: 15000,
          latency_ms_sum: 5000,
          latency_count: 10,
        },
        {
          bucket_start_unix_secs: 1764000060,
          request_count: 15,
          input_tokens: 1500,
          output_tokens: 750,
          error_count: 1,
          virtual_cost_micros: 22500,
          latency_ms_sum: 7500,
          latency_count: 15,
        },
      ],
    },
  ],
};

export const MOCK_LIMITS: PrincipalLimitsResponse = {
  principal_id: 'principal-a',
  observed: true,
  identities: [
    {
      identity_kind: 'account',
      identity_value: 'acc-12345',
      account_observed: true,
      windows: [
        {
          window: '5h',
          snapshots: [
            {
              kind: 'requests',
              limit: 200,
              remaining: 100,
              reset: new Date(Date.now() + 3600000).toISOString(),
              observed_at_unix_secs: 1764000000,
              stored_at_unix_secs: 1764000000,
              observed: true,
            },
            {
              kind: 'input_tokens',
              limit: 100000,
              remaining: 30000,
              reset: new Date(Date.now() + 3600000).toISOString(),
              observed_at_unix_secs: 1764000000,
              stored_at_unix_secs: 1764000000,
              observed: true,
            },
            {
              kind: 'output_tokens',
              limit: 100000,
              remaining: 20000,
              reset: new Date(Date.now() + 3600000).toISOString(),
              observed_at_unix_secs: 1764000000,
              stored_at_unix_secs: 1764000000,
              observed: true,
            },
          ],
        },
        {
          window: 'weekly',
          snapshots: [
            {
              kind: 'requests',
              limit: 10000,
              remaining: 5000,
              reset: new Date(Date.now() + 86400000 * 3).toISOString(),
              observed_at_unix_secs: 1764000000,
              stored_at_unix_secs: 1764000000,
              observed: true,
            },
            {
              kind: 'input_tokens',
              limit: 5000000,
              remaining: 2000000,
              reset: new Date(Date.now() + 86400000 * 3).toISOString(),
              observed_at_unix_secs: 1764000000,
              stored_at_unix_secs: 1764000000,
              observed: true,
            },
          ],
        },
      ],
    },
    {
      identity_kind: 'unobserved',
      identity_value: null,
      account_observed: false,
      windows: [
        {
          window: '5h',
          snapshots: [
            {
              kind: 'requests',
              limit: null,
              remaining: null,
              reset: null,
              observed_at_unix_secs: 0,
              stored_at_unix_secs: 0,
              observed: false,
            },
            {
              kind: 'tokens',
              limit: null,
              remaining: null,
              reset: null,
              observed_at_unix_secs: 0,
              stored_at_unix_secs: 0,
              observed: false,
            },
          ],
        },
      ],
    },
  ],
};

export const MOCK_SCHEMA: ConfigSchemaResponse = {
  schema: {
    type: 'object',
    properties: {
      listener: {
        type: 'object',
        title: 'Listener',
        description: 'Server listener configuration',
        properties: {
          address: {
            type: 'string',
            title: 'Address',
            description: 'Bind address',
          },
          port: {
            type: 'integer',
            title: 'Port',
            description: 'Bind port',
            minimum: 1,
            maximum: 65535,
          },
        },
      },
      tls: {
        type: 'object',
        title: 'TLS',
        description: 'Global TLS configuration',
        properties: {
          cert_path: { type: 'string', title: 'Certificate Path' },
          key_path: { type: 'string', title: 'Key Path' },
        },
      },
      body: {
        type: 'object',
        title: 'Body',
        description: 'Request body limits',
        properties: {
          max_size_bytes: { type: 'integer', title: 'Max Size (Bytes)' },
        },
      },
      timeouts: {
        type: 'object',
        title: 'Timeouts',
        description: 'Global timeouts',
        properties: {
          connect_timeout_ms: {
            type: 'integer',
            title: 'Connect Timeout (ms)',
          },
          read_timeout_ms: { type: 'integer', title: 'Read Timeout (ms)' },
        },
      },
      upstreams: {
        type: 'array',
        title: 'Upstreams',
        description: 'Backend upstreams',
        items: {
          type: 'object',
          properties: {
            id: { type: 'string', title: 'ID' },
            url: { type: 'string', title: 'URL' },
            weight: { type: 'integer', title: 'Weight' },
            auth_token: { type: 'string', title: 'Auth Token' },
          },
        },
      },
      principals: {
        type: 'object',
        title: 'Principals',
        description: 'Client principals',
        additionalProperties: {
          type: 'object',
          properties: {
            allowed_models: {
              type: 'array',
              items: { type: 'string' },
              title: 'Allowed Models',
            },
            disabled: { type: 'boolean', title: 'Disabled' },
          },
        },
      },
      plugins: {
        type: 'object',
        title: 'Plugins',
        description: 'Extism plugins',
        properties: {
          authn_plugin: { type: 'string', title: 'Authn Plugin Path' },
        },
      },
      storage: {
        type: 'object',
        title: 'Storage',
        description: 'Storage configuration',
        properties: {
          path: { type: 'string', title: 'Path' },
        },
      },
      signers: {
        type: 'object',
        title: 'Signers',
        description: 'JWT signers',
        properties: {
          keys: { type: 'array', items: { type: 'string' }, title: 'Keys' },
        },
      },
      observability: {
        type: 'object',
        title: 'Observability',
        description: 'Metrics and tracing',
        properties: {
          metrics_enabled: { type: 'boolean', title: 'Metrics Enabled' },
        },
      },
      quotas: {
        type: 'object',
        title: 'Quotas',
        description: 'Global quotas',
        properties: {
          default_window_secs: { type: 'integer', title: 'Default Window (s)' },
        },
      },
      admin: {
        type: 'object',
        title: 'Admin',
        description: 'Admin API configuration',
        properties: {
          admin_token: { type: 'string', title: 'Admin Token' },
        },
      },
      circuit_breaker: {
        type: 'object',
        title: 'Circuit Breaker',
        description: 'Circuit breaker settings',
        properties: {
          enabled: { type: 'boolean', title: 'Enabled' },
        },
      },
      bulkhead: {
        type: 'object',
        title: 'Bulkhead',
        description: 'Concurrency limits',
        properties: {
          max_concurrent_requests: {
            type: 'integer',
            title: 'Max Concurrent Requests',
          },
        },
      },
      dns: {
        type: 'object',
        title: 'DNS',
        description: 'DNS resolver settings',
        properties: {
          ttl_secs: { type: 'integer', title: 'TTL (s)' },
        },
      },
      egress: {
        type: 'object',
        title: 'Egress',
        description: 'Egress proxy settings',
        properties: {
          proxy_url: { type: 'string', title: 'Proxy URL' },
        },
      },
    },
  },
  coverage_checklist: [
    'listener',
    'tls',
    'body',
    'timeouts',
    'upstreams',
    'principals',
    'plugins',
    'storage',
    'signers',
    'observability',
    'quotas',
    'admin',
    'circuit_breaker',
    'bulkhead',
    'dns',
    'egress',
  ],
};

export const MOCK_DRAFT: ConfigDraftResponse = {
  draft: {
    listener: { address: '0.0.0.0', port: 8080 },
    tls: { cert_path: '/etc/certs/cert.pem', key_path: '/etc/certs/key.pem' },
    body: { max_size_bytes: 10485760 },
    timeouts: { connect_timeout_ms: 5000, read_timeout_ms: 30000 },
    upstreams: [
      {
        id: 'anthropic',
        url: 'https://api.anthropic.com',
        weight: 1,
        auth_token: '${ANTHROPIC_API_KEY}',
      },
      {
        id: 'openai',
        url: 'https://api.openai.com',
        weight: 1,
        auth_token: '${OPENAI_API_KEY}',
      },
    ],
    principals: {
      'user-1': { allowed_models: ['claude-3-opus-20240229'], disabled: false },
      'user-2': { allowed_models: ['gpt-4-turbo'], disabled: true },
    },
    plugins: { authn_plugin: null },
    storage: { path: '/var/lib/cc-lb/data.redb' },
    signers: { keys: [] },
    observability: { metrics_enabled: true },
    quotas: { default_window_secs: 60 },
    admin: { admin_token: '${ADMIN_TOKEN}' },
    circuit_breaker: { enabled: true },
    bulkhead: { max_concurrent_requests: 100 },
    dns: { ttl_secs: 300 },
    egress: { proxy_url: null },
  },
  revision: 42,
  last_validated_revision: 41,
  last_validation_error: null,
  saved_at_unix_secs: Math.floor(Date.now() / 1000) - 120,
};

export const MOCK_HISTORY: ConfigHistoryResponse = {
  history: [
    {
      revision: 41,
      applied_at_unix_secs: Math.floor(Date.now() / 1000) - 3600,
      config_summary: {
        upstreams: 2,
        principals: 2,
        plugin_count: 0,
        tls_enabled: true,
      },
    },
    {
      revision: 40,
      applied_at_unix_secs: Math.floor(Date.now() / 1000) - 86400,
      config_summary: {
        upstreams: 1,
        principals: 1,
        plugin_count: 0,
        tls_enabled: false,
      },
    },
    {
      revision: 39,
      applied_at_unix_secs: Math.floor(Date.now() / 1000) - 172800,
      config_summary: {
        upstreams: 1,
        principals: 0,
        plugin_count: 0,
        tls_enabled: false,
      },
    },
  ],
};

export const MOCK_DIFF: ConfigDiffResponse = {
  from: 41,
  to: 42,
  diff: [
    { path: 'listener.port', from: 80, to: 8080 },
    {
      path: 'upstreams[1]',
      from: null,
      to: {
        id: 'openai',
        url: 'https://api.openai.com',
        weight: 1,
        auth_token: '<redacted>',
      },
    },
    {
      path: 'principals.user-2',
      from: null,
      to: { allowed_models: ['gpt-4-turbo'], disabled: true },
    },
    { path: 'observability.metrics_enabled', from: false, to: true },
    { path: 'admin.admin_token', from: '<redacted>', to: '<redacted>' },
  ],
};
