// @vitest-environment jsdom
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  within,
} from '@testing-library/react';
import type React from 'react';
import { afterEach, beforeEach, expect, test, vi } from 'vitest';
import { Route as SettingsRoute } from './settings';

const queryMocks = vi.hoisted(() => ({
  useConfigDraft: vi.fn(),
  useConfigEditor: vi.fn(),
  useConfigHistory: vi.fn(),
  useSaveConfigFile: vi.fn(),
  useSaveDraft: vi.fn(),
  useStatus: vi.fn(),
  useValidateConfig: vi.fn(),
}));

const apiMocks = vi.hoisted(() => ({
  downloadConfigDraft: vi.fn(),
  downloadJson: vi.fn(),
}));

const localeMocks = vi.hoisted(() => ({
  setLocale: vi.fn(),
  setTimezone: vi.fn(),
}));

vi.mock('../lib/queries', () => queryMocks);

vi.mock('../lib/api', async () => {
  const actual = await vi.importActual<Record<string, unknown>>('../lib/api');
  return {
    ...actual,
    downloadConfigDraft: apiMocks.downloadConfigDraft,
    downloadJson: apiMocks.downloadJson,
  };
});

vi.mock('../lib/locale', () => ({
  formatAbsolute: () => 'Sep 15, 2026, 12:00:00 PM',
  useLocale: () => ({
    locale: 'en-US',
    effective: 'en-US',
    setLocale: localeMocks.setLocale,
  }),
  useTimezone: () => ({
    timezone: 'UTC',
    effective: 'UTC',
    setTimezone: localeMocks.setTimezone,
  }),
}));

const SettingsComponent = SettingsRoute.options
  .component as React.ComponentType;

const loadedStatus = {
  version: '1.2.3',
  git_sha: 'abc1234',
  uptime_secs: 120,
  build: {
    rust_version: '1.90.0',
    profile: 'release',
    target: 'aarch64-unknown-linux-gnu',
  },
  generation: 7,
};

const configSchema = {
  type: 'object',
  properties: {
    listener: { $ref: '#/$defs/Listener' },
    body: {
      type: 'object',
      properties: { messages_cap_bytes: { type: 'integer', minimum: 1 } },
    },
    timeouts: {
      type: 'object',
      properties: { upstream_total_secs: { type: 'integer', minimum: 1 } },
    },
    storage: {
      oneOf: [
        {
          title: 'SQLite',
          type: 'object',
          properties: {
            kind: { const: 'sqlite' },
            path: { type: 'string' },
          },
        },
        {
          title: 'PostgreSQL',
          type: 'object',
          properties: {
            kind: { const: 'postgres' },
            url: { type: 'string' },
            pool: {
              type: 'object',
              properties: { max_connections: { type: 'integer' } },
            },
          },
        },
      ],
    },
    aead: {
      type: 'object',
      properties: { key_env: { type: 'string' } },
    },
    event_bus: {
      type: 'object',
      properties: { broadcast_capacity: { type: 'integer' } },
    },
    request_event_retention_days: { type: 'integer' },
    scheduler: {
      type: 'object',
      properties: {
        separate_pool: {
          type: 'object',
          properties: { max_connections: { type: 'integer' } },
        },
        recurring_jobs: {
          type: 'object',
          additionalProperties: { $ref: '#/$defs/RecurringJob' },
        },
        dlq_retention_days: { type: 'integer' },
      },
    },
    upstream_affinity: {
      type: 'object',
      properties: { ttl_days: { type: 'integer' } },
    },
    circuit_breaker: {
      type: 'object',
      properties: { failures_to_open: { type: 'integer' } },
    },
    bulkhead: {
      type: 'object',
      properties: { max_conns_per_upstream: { type: 'integer' } },
    },
    prompt_cache_shadow: {
      type: 'object',
      properties: { grace_margin_secs: { type: 'integer' } },
    },
    limit_reservation_ttl: {
      type: 'object',
      properties: { ttl_secs: { type: 'integer' } },
    },
    admin: {
      type: 'object',
      properties: {
        auth: {
          type: 'object',
          properties: {
            providers: {
              type: 'array',
              items: {
                oneOf: [
                  {
                    title: 'Static token',
                    type: 'object',
                    properties: {
                      kind: { const: 'static_token' },
                      id: { type: 'string' },
                      token_env: { type: 'string' },
                    },
                  },
                  {
                    title: 'Cloudflare Access',
                    type: 'object',
                    properties: {
                      kind: { const: 'cloudflare_access' },
                      id: { type: 'string' },
                      team_domain: { type: 'string' },
                      audiences: { type: 'array', items: { type: 'string' } },
                      header: { type: 'string' },
                    },
                  },
                ],
              },
            },
          },
        },
      },
    },
    oauth: {
      type: 'object',
      properties: {
        anthropic: {
          anyOf: [{ $ref: '#/$defs/AnthropicOAuth' }, { type: 'null' }],
        },
      },
    },
    cluster: {
      type: 'object',
      properties: {
        instance_url: { type: ['string', 'null'] },
        token_env: { type: 'string' },
      },
    },
    price_catalog: {
      type: 'object',
      properties: { url: { type: 'string' }, cache_path: { type: 'string' } },
    },
    subscription_quota: {
      type: 'object',
      properties: { writer_batch_max_records: { type: 'integer' } },
    },
    runtime: {
      type: 'object',
      properties: {
        data_dir: { type: ['string', 'null'] },
        wasmtime: {
          type: 'object',
          properties: {
            allocation_strategy: { enum: ['ondemand', 'pooling'] },
            cookie_redaction: { type: 'boolean' },
            memory_max_pages: { type: ['integer', 'null'], minimum: 0 },
          },
        },
      },
    },
    observability: {
      type: 'object',
      properties: {
        tracing_level: { type: 'string' },
        otlp_endpoint: { type: ['string', 'null'] },
        log_redaction: { type: 'boolean' },
      },
    },
  },
  $defs: {
    Listener: {
      type: 'object',
      properties: {
        proxy_addr: { type: 'string' },
        admin_addr: { type: 'string' },
        metrics_addr: { type: 'string' },
        tls: { anyOf: [{ $ref: '#/$defs/Tls' }, { type: 'null' }] },
      },
    },
    Tls: {
      type: 'object',
      properties: {
        cert_path: { type: ['string', 'null'] },
        key_path: { type: ['string', 'null'] },
        reload_on_sighup: { type: 'boolean' },
      },
    },
    RecurringJob: {
      type: 'object',
      properties: {
        enabled: { type: 'boolean' },
        interval_secs: { type: 'integer' },
        jitter_secs: { type: 'integer' },
      },
    },
    AnthropicOAuth: {
      type: 'object',
      properties: {
        client_id: { type: 'string' },
        scopes: { type: 'array', items: { type: 'string' } },
      },
    },
  },
};

const fileConfig = {
  listener: {
    proxy_addr: '0.0.0.0:8080',
    admin_addr: '127.0.0.1:9090',
    metrics_addr: '127.0.0.1:9091',
    tls: null,
  },
  body: { messages_cap_bytes: 1024 },
  timeouts: { upstream_total_secs: 600 },
  storage: {
    kind: 'postgres',
    url: '__CC_LB_STORAGE_URL_UNCHANGED__',
    pool: { max_connections: 10 },
  },
  aead: { key_env: 'CC_LB_MASTER_KEY' },
  event_bus: { broadcast_capacity: 4096 },
  request_event_retention_days: 90,
  scheduler: {
    separate_pool: { max_connections: 5 },
    recurring_jobs: {
      quota_refresh: { enabled: true, interval_secs: 3600, jitter_secs: 30 },
      custom_job: { enabled: false, interval_secs: 600, jitter_secs: 10 },
    },
    dlq_retention_days: 30,
  },
  upstream_affinity: { ttl_days: 90 },
  circuit_breaker: { failures_to_open: 5 },
  bulkhead: { max_conns_per_upstream: 50 },
  prompt_cache_shadow: { grace_margin_secs: 30 },
  limit_reservation_ttl: { ttl_secs: 300 },
  admin: {
    auth: {
      providers: [
        { kind: 'static_token', id: 'primary', token_env: 'CC_LB_ADMIN_TOKEN' },
      ],
    },
  },
  oauth: { anthropic: null },
  cluster: { instance_url: null, token_env: 'CC_LB_CLUSTER_TOKEN' },
  price_catalog: {
    url: 'https://example.com/prices.json',
    cache_path: '/tmp/prices.json',
  },
  subscription_quota: { writer_batch_max_records: 256 },
  runtime: {
    data_dir: null,
    wasmtime: {
      allocation_strategy: 'ondemand',
      memory_max_pages: null,
      cookie_redaction: false,
    },
  },
  observability: {
    tracing_level: 'info',
    otlp_endpoint: null,
    log_redaction: true,
  },
};

const defaultConfig = {
  ...structuredClone(fileConfig),
  listener: {
    ...structuredClone(fileConfig.listener),
    proxy_addr: '[::]:8080',
  },
  storage: {
    kind: 'sqlite',
    path: '/var/lib/cc-lb/storage.sqlite',
  },
  scheduler: {
    ...structuredClone(fileConfig.scheduler),
    recurring_jobs: {
      quota_refresh: structuredClone(
        fileConfig.scheduler.recurring_jobs.quota_refresh,
      ),
    },
  },
  admin: { auth: { providers: [] } },
};

const validReport = {
  revision: 7,
  file: { valid: true, issues: [] },
  effective: { valid: true, issues: [] },
  filesystem: [],
  overrides: [],
};

function editorResponse(overrides: Record<string, unknown> = {}) {
  return {
    schema: configSchema,
    default_config: structuredClone(defaultConfig),
    file_config: structuredClone(fileConfig),
    effective_config: structuredClone(fileConfig),
    draft: structuredClone(fileConfig),
    revision: 7,
    last_validated_revision: 7,
    last_validation: structuredClone(validReport),
    saved_at_unix_secs: 1_789_473_600,
    file: {
      path: '/etc/cc-lb/cc-lb.toml',
      exists: true,
      mode: 'writable',
      reason: null,
      fingerprint: 'sha256:current',
    },
    overrides: [],
    restart_required: false,
    ...overrides,
  };
}

function draftResponse(overrides: Record<string, unknown> = {}) {
  return {
    draft: structuredClone(fileConfig),
    revision: 7,
    last_validated_revision: 7,
    last_validation: structuredClone(validReport),
    saved_at_unix_secs: 1_789_473_600,
    ...overrides,
  };
}

function loadingResult() {
  return {
    data: undefined,
    isFetching: true,
    isLoading: true,
    isPending: true,
    isError: false,
    refetch: vi.fn(),
  };
}

function loadedResult<T>(data: T) {
  return {
    data,
    isFetching: false,
    isLoading: false,
    isPending: false,
    isError: false,
    refetch: vi.fn(),
  };
}
function errorResult(error = new Error('request failed')) {
  return {
    data: undefined,
    isFetching: false,
    isLoading: false,
    isPending: false,
    isError: true,
    error,
    refetch: vi.fn(),
  };
}

function mutationResult(mutate = vi.fn()) {
  return {
    mutate,
    isPending: false,
    isError: false,
    error: null,
    variables: undefined,
  };
}

function setSettingsLoaded(editor = editorResponse(), draft = draftResponse()) {
  queryMocks.useStatus.mockReturnValue(loadedResult(loadedStatus));
  queryMocks.useConfigEditor.mockReturnValue(loadedResult(editor));
  queryMocks.useConfigDraft.mockReturnValue(loadedResult(draft));
  queryMocks.useConfigHistory.mockReturnValue(loadedResult({ entries: [] }));
}

function openCategory(name: string) {
  const button = screen.getByRole('button', { name: new RegExp(`^${name}`) });
  if (button.getAttribute('aria-expanded') !== 'true') fireEvent.click(button);
}

beforeEach(() => {
  vi.useFakeTimers({
    toFake: ['Date'],
    now: new Date('2026-09-15T12:00:01.000Z'),
  });
  vi.stubGlobal('requestAnimationFrame', (callback: FrameRequestCallback) => {
    queueMicrotask(() => callback(0));
    return 1;
  });
  HTMLElement.prototype.scrollIntoView = vi.fn();
  vi.clearAllMocks();
  apiMocks.downloadConfigDraft.mockResolvedValue(undefined);
  apiMocks.downloadJson.mockResolvedValue(undefined);
  queryMocks.useStatus.mockReturnValue(loadingResult());
  queryMocks.useConfigEditor.mockReturnValue(loadingResult());
  queryMocks.useConfigDraft.mockReturnValue(loadingResult());
  queryMocks.useConfigHistory.mockReturnValue(loadingResult());
  queryMocks.useSaveConfigFile.mockReturnValue(mutationResult());
  queryMocks.useSaveDraft.mockReturnValue(mutationResult());
  queryMocks.useValidateConfig.mockReturnValue(mutationResult());
});

afterEach(() => {
  cleanup();
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

test('settings cold load reserves version, editor, and history heights without a wide table', () => {
  render(<SettingsComponent />);

  const versionCard = screen.getByTestId('version-card');
  expect(versionCard.querySelectorAll('.skeleton')).toHaveLength(5);
  expect(screen.getByTestId('config-editor-metadata').className).toContain(
    'min-h-12',
  );

  const editorSkeleton = screen.getByTestId('config-editor-skeleton');
  expect(editorSkeleton.className).toContain('min-h-[560px]');
  expect(editorSkeleton.children).toHaveLength(7);

  const historySlot = screen.getByTestId('config-history-slot');
  expect(historySlot.className).toContain('min-h-[173px]');
  expect(historySlot.className).toContain('sm:min-h-[163px]');
  expect(historySlot.querySelector('table')).toBeNull();
  expect(historySlot.querySelectorAll('.skeleton')).toHaveLength(8);
});

test('settings preserves version, localization, metadata history, and database backups', () => {
  setSettingsLoaded();
  queryMocks.useConfigHistory.mockReturnValue(
    loadedResult({
      entries: [{ revision: 7, saved_at_unix_secs: 1_789_473_600 }],
    }),
  );

  render(<SettingsComponent />);

  expect(screen.getByText('Version')).toBeDefined();
  expect(screen.getByText('Localization')).toBeDefined();
  expect(screen.getByText('Saved Config History')).toBeDefined();
  expect(screen.getByText('Data & Backups')).toBeDefined();
  expect(screen.getByText('Database resources snapshot')).toBeDefined();
  expect(
    within(screen.getByTestId('config-history-slot')).getByText('7'),
  ).toBeDefined();
  expect(screen.queryByText('Admin Token')).toBeNull();
  expect(screen.queryByRole('button', { name: 'Apply' })).toBeNull();
  expect(screen.queryByRole('button', { name: 'Reload' })).toBeNull();
});
test('history load failures stay distinct from empty history and offer retry', () => {
  const beforeProcessStart = 1_789_473_000;
  setSettingsLoaded(
    editorResponse({ saved_at_unix_secs: beforeProcessStart }),
    draftResponse({ saved_at_unix_secs: beforeProcessStart }),
  );
  const history = errorResult(new Error('history offline'));
  queryMocks.useConfigHistory.mockReturnValue(history);

  render(<SettingsComponent />);

  expect(screen.queryByText('No saved config history available.')).toBeNull();
  expect(screen.getByText('Restart status may be incomplete')).toBeDefined();
  expect(
    screen.getByText(
      /cannot confirm whether a saved revision is still waiting/,
    ),
  ).toBeDefined();
  const retry = screen.getByRole('button', { name: 'Retry history' });
  fireEvent.click(retry);
  expect(history.refetch).toHaveBeenCalledTimes(1);
});

test('structured editor renders seven category shells and initial controls', () => {
  setSettingsLoaded();
  render(<SettingsComponent />);

  expect(document.querySelectorAll('[data-config-category]')).toHaveLength(7);
  expect(screen.getByRole('textbox', { name: 'Proxy Addr' })).toBeDefined();
  expect(screen.getByRole('checkbox', { name: /Tls/ })).toBeDefined();
});

test('an environment-overridden field keeps its file value editable and shows effective provenance', () => {
  const editor = editorResponse({
    effective_config: {
      ...structuredClone(fileConfig),
      listener: {
        ...fileConfig.listener,
        proxy_addr: '127.0.0.1:8181',
      },
    },
    overrides: [
      {
        path: 'listener.proxy_addr',
        source: 'env',
        name: 'CC_LB_PROXY_ADDR',
        sensitive: false,
        effective_value: '127.0.0.1:8181',
      },
    ],
  });
  setSettingsLoaded(editor);
  render(<SettingsComponent />);

  const input = screen.getByLabelText('Proxy Addr') as HTMLInputElement;
  expect(input.value).toBe('0.0.0.0:8080');
  expect(screen.getByText('Environment · CC_LB_PROXY_ADDR')).toBeDefined();
  expect(screen.getByText('Effective: 127.0.0.1:8181')).toBeDefined();

  fireEvent.change(input, { target: { value: '0.0.0.0:8181' } });
  expect(input.value).toBe('0.0.0.0:8181');
  expect(screen.getByText('Effective: 127.0.0.1:8181')).toBeDefined();
  expect(
    screen.getByRole('button', {
      name: /^Network & requests.*1 modified/,
    }),
  ).toBeDefined();
});
test('admin provider fields inherit provenance from the array override', () => {
  setSettingsLoaded(
    editorResponse({
      overrides: [
        {
          path: 'admin.auth.providers',
          source: 'special_env',
          name: 'CC_LB_ADMIN_AUTH_PROVIDERS_JSON',
          sensitive: true,
          effective_value: null,
        },
      ],
    }),
  );
  render(<SettingsComponent />);
  openCategory('Identity & access');

  const providerId = document.querySelector<HTMLElement>(
    '[data-config-path="admin.auth.providers[0].id"]',
  );
  expect(providerId).not.toBeNull();
  expect(
    within(providerId as HTMLElement).getByText(
      'Environment · CC_LB_ADMIN_AUTH_PROVIDERS_JSON',
    ),
  ).toBeDefined();
});

test('dirty edits require save draft and validation before file save or download', () => {
  setSettingsLoaded();
  render(<SettingsComponent />);

  const saveDraftButton = screen.getByRole('button', { name: 'Save draft' });
  const validateButton = screen.getByRole('button', { name: 'Validate' });
  const saveFileButton = screen.getByRole('button', {
    name: 'Save to config file',
  });
  const downloadButton = screen.getByRole('button', { name: 'Download TOML' });
  expect(saveDraftButton.hasAttribute('disabled')).toBe(true);
  expect(validateButton.hasAttribute('disabled')).toBe(false);
  expect(saveFileButton.hasAttribute('disabled')).toBe(false);
  expect(downloadButton.hasAttribute('disabled')).toBe(false);
  expect(screen.getByText('Configuration validated')).toBeDefined();

  fireEvent.change(screen.getByLabelText('Proxy Addr'), {
    target: { value: '0.0.0.0:8181' },
  });

  expect(saveDraftButton.hasAttribute('disabled')).toBe(false);
  expect(validateButton.hasAttribute('disabled')).toBe(true);
  expect(saveFileButton.hasAttribute('disabled')).toBe(true);
  expect(downloadButton.hasAttribute('disabled')).toBe(true);
  expect(screen.getByText('Draft has unsaved changes')).toBeDefined();
});
test('filesystem validation errors block file save but not download', async () => {
  const filesystemReport = {
    ...validReport,
    filesystem: [
      {
        path: 'listener.tls.cert_path',
        code: 'missing_required_file',
        message: 'TLS certificate file does not exist.',
        severity: 'error' as const,
      },
    ],
  };
  setSettingsLoaded(
    editorResponse({ last_validation: filesystemReport }),
    draftResponse({ last_validation: filesystemReport }),
  );
  render(<SettingsComponent />);

  expect(screen.queryByText('Configuration validated')).toBeNull();
  expect(
    screen.getByRole('button', {
      name: /listener\.tls\.cert_path · missing_required_file/,
    }),
  ).toBeDefined();
  expect(
    screen
      .getByRole('button', { name: 'Save to config file' })
      .hasAttribute('disabled'),
  ).toBe(true);
  const downloadButton = screen.getByRole('button', {
    name: 'Download TOML',
  });
  expect(downloadButton.hasAttribute('disabled')).toBe(false);
  fireEvent.click(downloadButton);
  expect(apiMocks.downloadConfigDraft).toHaveBeenCalledWith(
    expect.any(Number),
    undefined,
  );
});

test('nullable integer controls keep numeric draft values', () => {
  const saveDraftMutate = vi.fn();
  queryMocks.useSaveDraft.mockReturnValue(mutationResult(saveDraftMutate));
  setSettingsLoaded();
  render(<SettingsComponent />);
  openCategory('Runtime & observability');

  const field = document.querySelector<HTMLElement>(
    '[data-config-path="runtime.wasmtime.memory_max_pages"]',
  );
  expect(field).not.toBeNull();
  fireEvent.click(
    within(field as HTMLElement).getByRole('button', { name: 'Set value' }),
  );
  const input = screen.getByLabelText('Memory Max Pages') as HTMLInputElement;
  expect(input.type).toBe('number');
  fireEvent.change(input, { target: { value: '128' } });
  fireEvent.click(screen.getByRole('button', { name: 'Save draft' }));

  const request = saveDraftMutate.mock.calls[0]?.[0] as {
    draft: {
      runtime: { wasmtime: { memory_max_pages: unknown } };
    };
  };
  expect(request.draft.runtime.wasmtime.memory_max_pages).toBe(128);
});
test('clearing a numeric input unsets the draft path', () => {
  const saveDraftMutate = vi.fn();
  queryMocks.useSaveDraft.mockReturnValue(mutationResult(saveDraftMutate));
  setSettingsLoaded();
  render(<SettingsComponent />);
  openCategory('Scheduling');

  const field = document.querySelector<HTMLElement>(
    '[data-config-path="scheduler.dlq_retention_days"]',
  );
  expect(field).not.toBeNull();
  fireEvent.change(within(field as HTMLElement).getByRole('spinbutton'), {
    target: { value: '' },
  });
  fireEvent.click(screen.getByRole('button', { name: 'Save draft' }));

  const request = saveDraftMutate.mock.calls[0]?.[0] as {
    draft: { scheduler: Record<string, unknown> };
  };
  expect('dlq_retention_days' in request.draft.scheduler).toBe(false);
});

test('clearing a nullable numeric input preserves editing until blur', () => {
  setSettingsLoaded();
  render(<SettingsComponent />);
  openCategory('Runtime & observability');

  const field = document.querySelector<HTMLElement>(
    '[data-config-path="runtime.wasmtime.memory_max_pages"]',
  );
  expect(field).not.toBeNull();
  fireEvent.click(
    within(field as HTMLElement).getByRole('button', { name: 'Set value' }),
  );
  const input = within(field as HTMLElement).getByRole(
    'spinbutton',
  ) as HTMLInputElement;
  input.focus();
  fireEvent.change(input, { target: { value: '' } });

  expect(document.activeElement).toBe(input);
  expect(within(field as HTMLElement).getByRole('spinbutton')).toBe(input);

  fireEvent.change(input, { target: { value: '123' } });
  expect(input.value).toBe('123');
  fireEvent.change(input, { target: { value: '' } });
  fireEvent.blur(input);
  expect(
    within(field as HTMLElement).getByRole('button', { name: 'Set value' }),
  ).toBeDefined();
});

test('storage URL stays opaque and replacement is attached only to validate', () => {
  const validateMutate = vi.fn();
  queryMocks.useValidateConfig.mockReturnValue(mutationResult(validateMutate));
  setSettingsLoaded();
  render(<SettingsComponent />);
  openCategory('Storage & data');

  expect(
    screen.queryByDisplayValue('__CC_LB_STORAGE_URL_UNCHANGED__'),
  ).toBeNull();
  const urlField = document.querySelector<HTMLElement>(
    '[data-config-path="storage.url"]',
  );
  expect(urlField).not.toBeNull();
  expect(
    within(urlField as HTMLElement).queryByRole('button', { name: 'Reset' }),
  ).toBeNull();
  expect(
    within(urlField as HTMLElement).getByRole('button', { name: 'Unset' }),
  ).toBeDefined();

  fireEvent.click(screen.getByRole('button', { name: 'Replace URL' }));
  const urlInput = screen.getByLabelText('Url') as HTMLInputElement;
  expect(urlInput.type).toBe('password');
  fireEvent.change(urlInput, {
    target: { value: 'postgres://new-secret@db/cc_lb' },
  });
  fireEvent.click(screen.getByRole('button', { name: 'Validate' }));
  expect(validateMutate).toHaveBeenLastCalledWith(
    {
      expected_revision: 7,
      storage_url_replacement: 'postgres://new-secret@db/cc_lb',
    },
    expect.objectContaining({ onSuccess: expect.any(Function) }),
  );

  fireEvent.change(urlInput, { target: { value: '' } });
  expect(screen.getByRole('button', { name: 'Replace URL' })).toBeDefined();
  fireEvent.click(screen.getByRole('button', { name: 'Validate' }));
  expect(validateMutate).toHaveBeenLastCalledWith(
    {
      expected_revision: 7,
      storage_url_replacement: undefined,
    },
    expect.objectContaining({ onSuccess: expect.any(Function) }),
  );
});
test('opaque storage URL can be unset without exposing it or dropping pool settings', () => {
  const saveDraftMutate = vi.fn();
  queryMocks.useSaveDraft.mockReturnValue(mutationResult(saveDraftMutate));
  setSettingsLoaded();
  render(<SettingsComponent />);
  openCategory('Storage & data');

  const urlField = document.querySelector<HTMLElement>(
    '[data-config-path="storage.url"]',
  );
  expect(urlField).not.toBeNull();
  expect(
    within(urlField as HTMLElement).queryByText(
      '__CC_LB_STORAGE_URL_UNCHANGED__',
    ),
  ).toBeNull();
  fireEvent.click(
    within(urlField as HTMLElement).getByRole('button', { name: 'Unset' }),
  );
  expect(
    within(urlField as HTMLElement).getByText('No stored URL'),
  ).toBeDefined();
  fireEvent.click(screen.getByRole('button', { name: 'Save draft' }));

  const request = saveDraftMutate.mock.calls[0]?.[0] as {
    draft: {
      storage: { url?: unknown; pool: { max_connections: number } };
    };
  };
  expect('url' in request.draft.storage).toBe(false);
  expect(request.draft.storage.pool).toEqual({ max_connections: 10 });
});

test('structured validation issues open their category and focus the field', async () => {
  const invalidReport = {
    ...validReport,
    file: {
      valid: false,
      issues: [
        {
          path: 'observability.tracing_level',
          code: 'invalid_value',
          message: 'Use a supported tracing level.',
          severity: 'error' as const,
        },
      ],
    },
    effective: {
      valid: false,
      issues: [
        {
          path: 'observability.tracing_level',
          code: 'invalid_value',
          message: 'Use a supported tracing level.',
          severity: 'error' as const,
        },
      ],
    },
  };
  setSettingsLoaded(
    editorResponse({
      last_validated_revision: null,
      last_validation: invalidReport,
    }),
    draftResponse({
      last_validated_revision: null,
      last_validation: invalidReport,
    }),
  );
  render(<SettingsComponent />);
  expect(
    screen.getAllByRole('button', {
      name: /observability\.tracing_level · invalid_value/,
    }),
  ).toHaveLength(1);
  expect(screen.queryByText('Stale')).toBeNull();
  expect(
    screen
      .getByRole('button', { name: 'Save to config file' })
      .hasAttribute('disabled'),
  ).toBe(true);
  expect(
    screen
      .getByRole('button', { name: 'Download TOML' })
      .hasAttribute('disabled'),
  ).toBe(true);

  fireEvent.click(
    screen.getByRole('button', {
      name: /observability\.tracing_level · invalid_value/,
    }),
  );
  await act(async () => Promise.resolve());

  const runtimeCategory = screen.getByRole('button', {
    name: /^Runtime & observability/,
  });
  expect(runtimeCategory.getAttribute('aria-expanded')).toBe('true');
  expect(document.activeElement).toBe(screen.getByLabelText('Tracing Level'));
});

test('array-level validation issues focus the matching admin provider', async () => {
  const providerIssue = {
    path: 'admin.auth.providers[0]',
    code: 'invalid_provider',
    message: 'Provider configuration is incomplete.',
    severity: 'error' as const,
  };
  const invalidReport = {
    ...validReport,
    file: { valid: false, issues: [providerIssue] },
  };
  setSettingsLoaded(
    editorResponse({
      last_validated_revision: null,
      last_validation: invalidReport,
    }),
    draftResponse({
      last_validated_revision: null,
      last_validation: invalidReport,
    }),
  );
  render(<SettingsComponent />);

  fireEvent.click(
    screen.getByRole('button', {
      name: /admin\.auth\.providers\[0\] · invalid_provider/,
    }),
  );
  await act(async () => Promise.resolve());

  expect(
    screen
      .getByRole('button', { name: /^Identity & access/ })
      .getAttribute('aria-expanded'),
  ).toBe('true');
  expect(document.activeElement?.getAttribute('data-config-path')).toBe(
    'admin.auth.providers[0].kind',
  );
});

test('clean validated read-only config can download on initial load', () => {
  setSettingsLoaded(
    editorResponse({
      file: {
        path: '/etc/cc-lb/cc-lb.toml',
        exists: true,
        mode: 'read_only',
        reason: 'Bind mount is read-only.',
        fingerprint: 'sha256:current',
      },
    }),
    draftResponse(),
  );
  render(<SettingsComponent />);

  expect(screen.getByText('Config file is read-only')).toBeDefined();
  expect(screen.getByText('Configuration validated')).toBeDefined();
  expect(
    screen
      .getByRole('button', { name: 'Save to config file' })
      .hasAttribute('disabled'),
  ).toBe(true);
  const downloadButton = screen.getByRole('button', {
    name: 'Download TOML',
  });
  expect(downloadButton.hasAttribute('disabled')).toBe(false);

  fireEvent.click(downloadButton);
  expect(apiMocks.downloadConfigDraft).toHaveBeenCalledWith(7, undefined);
});

test('writable missing config can be created directly on initial load', () => {
  const saveFileMutate = vi.fn();
  queryMocks.useSaveConfigFile.mockReturnValue(mutationResult(saveFileMutate));
  setSettingsLoaded(
    editorResponse({
      file: {
        path: '/etc/cc-lb/cc-lb.toml',
        exists: false,
        mode: 'writable',
        reason: null,
        fingerprint: null,
      },
    }),
    draftResponse(),
  );
  render(<SettingsComponent />);

  expect(screen.getByText('Config file is missing')).toBeDefined();
  const saveFileButton = screen.getByRole('button', {
    name: 'Save to config file',
  });
  expect(saveFileButton.hasAttribute('disabled')).toBe(false);
  fireEvent.click(saveFileButton);

  expect(saveFileMutate).toHaveBeenCalledWith(
    {
      expected_revision: 7,
      expected_fingerprint: null,
      storage_url_replacement: undefined,
      confirm_self_lockout: false,
    },
    expect.objectContaining({ onSuccess: expect.any(Function) }),
  );
});

test('read-only missing config can download but cannot be created', () => {
  setSettingsLoaded(
    editorResponse({
      file: {
        path: '/etc/cc-lb/cc-lb.toml',
        exists: false,
        mode: 'read_only',
        reason: 'Parent directory is not writable.',
        fingerprint: null,
      },
    }),
    draftResponse(),
  );
  render(<SettingsComponent />);

  expect(
    screen.getByText('Config file is missing and cannot be created'),
  ).toBeDefined();
  expect(
    screen
      .getByRole('button', { name: 'Save to config file' })
      .hasAttribute('disabled'),
  ).toBe(true);
  expect(
    screen
      .getByRole('button', { name: 'Download TOML' })
      .hasAttribute('disabled'),
  ).toBe(false);
});

test('existing-file overwrite and admin-provider changes require explicit confirmation', () => {
  const changedDraft = {
    ...structuredClone(fileConfig),
    admin: {
      auth: {
        providers: [
          {
            kind: 'cloudflare_access',
            id: 'access',
            team_domain: 'example.cloudflareaccess.com',
            audiences: ['aud'],
            header: 'cf-access-jwt-assertion',
          },
        ],
      },
    },
  };
  const changedEditor = editorResponse({
    draft: changedDraft,
    effective_config: changedDraft,
  });
  const changedDraftResponse = draftResponse({ draft: changedDraft });
  const saveFileMutate = vi.fn();
  queryMocks.useSaveConfigFile.mockReturnValue(mutationResult(saveFileMutate));
  setSettingsLoaded(changedEditor, changedDraftResponse);
  render(<SettingsComponent />);

  fireEvent.click(screen.getByRole('button', { name: 'Save to config file' }));
  expect(
    screen.getByText('Confirm config overwrite and admin access changes'),
  ).toBeDefined();
  expect(screen.getByText(/can lock you out after restart/)).toBeDefined();
  fireEvent.click(
    screen.getByRole('button', { name: 'Save and accept lockout risk' }),
  );

  expect(saveFileMutate).toHaveBeenCalledWith(
    expect.objectContaining({
      expected_revision: 7,
      expected_fingerprint: 'sha256:current',
      confirm_self_lockout: true,
    }),
    expect.objectContaining({ onError: expect.any(Function) }),
  );
});

test('config file save exposes pending state and keeps failures in the editor', () => {
  const saveFileMutate = vi.fn();
  queryMocks.useSaveConfigFile.mockReturnValue(mutationResult(saveFileMutate));
  setSettingsLoaded();
  const view = render(<SettingsComponent />);

  fireEvent.click(screen.getByRole('button', { name: 'Save to config file' }));
  fireEvent.click(screen.getByRole('button', { name: 'Overwrite file' }));
  const options = saveFileMutate.mock.calls[0]?.[1] as {
    onError: (error: unknown) => void;
  };
  act(() => options.onError(new Error('disk full')));
  expect(screen.getByText('Configuration action failed')).toBeDefined();
  expect(screen.getByText('disk full')).toBeDefined();

  queryMocks.useSaveConfigFile.mockReturnValue({
    ...mutationResult(saveFileMutate),
    isPending: true,
  });
  view.rerender(<SettingsComponent />);
  const pendingButton = screen.getByRole('button', {
    name: 'Save to config file',
  });
  expect(pendingButton.getAttribute('aria-busy')).toBe('true');
  expect(pendingButton.hasAttribute('disabled')).toBe(true);
});

test('restart drift, pending download, and failed download fallback stay visible', async () => {
  const pending = Promise.withResolvers<void>();
  apiMocks.downloadConfigDraft.mockReturnValue(pending.promise);
  setSettingsLoaded(editorResponse({ restart_required: true }));
  render(<SettingsComponent />);

  expect(screen.getByText('Restart required after saving')).toBeDefined();
  const downloadButton = screen.getByRole('button', { name: 'Download TOML' });
  fireEvent.click(downloadButton);
  expect(downloadButton.getAttribute('aria-busy')).toBe('true');
  expect(screen.getByTestId('restart-drift-banner')).toBeDefined();
  expect(downloadButton.hasAttribute('disabled')).toBe(true);

  await act(async () => pending.reject(new Error('network offline')));
  expect(screen.getByText('Download failed')).toBeDefined();
  expect(screen.getByText(/network offline/)).toBeDefined();
  expect(screen.getByRole('button', { name: 'Copy draft JSON' })).toBeDefined();
  expect(screen.getByRole('button', { name: 'Retry' })).toBeDefined();
});

test('restart drift survives config save draft clearing through latest history metadata', () => {
  const saveFileMutate = vi.fn();
  queryMocks.useSaveConfigFile.mockReturnValue(mutationResult(saveFileMutate));
  const beforeProcessStart = 1_789_473_000;
  setSettingsLoaded(
    editorResponse({ saved_at_unix_secs: beforeProcessStart }),
    draftResponse({ saved_at_unix_secs: beforeProcessStart }),
  );
  queryMocks.useConfigHistory.mockReturnValue(
    loadedResult({
      entries: [{ revision: 6, saved_at_unix_secs: beforeProcessStart }],
    }),
  );
  const view = render(<SettingsComponent />);

  expect(screen.queryByTestId('restart-drift-banner')).toBeNull();
  expect(queryMocks.useConfigHistory).toHaveBeenCalledTimes(1);

  fireEvent.click(screen.getByRole('button', { name: 'Save to config file' }));
  fireEvent.click(screen.getByRole('button', { name: 'Overwrite file' }));
  const options = saveFileMutate.mock.calls[0]?.[1] as {
    onSuccess: (response: {
      revision: number;
      saved_at_unix_secs: number;
      restart_required: boolean;
      fingerprint: string;
    }) => void;
  };
  act(() =>
    options.onSuccess({
      revision: 7,
      saved_at_unix_secs: 1_789_473_600,
      restart_required: true,
      fingerprint: 'sha256:saved',
    }),
  );

  setSettingsLoaded(
    editorResponse({
      draft: null,
      saved_at_unix_secs: null,
      last_validated_revision: null,
      last_validation: null,
    }),
    draftResponse({
      draft: null,
      saved_at_unix_secs: null,
      last_validated_revision: null,
      last_validation: null,
    }),
  );
  queryMocks.useConfigHistory.mockReturnValue(
    loadedResult({
      entries: [
        { revision: 7, saved_at_unix_secs: 1_789_473_600 },
        { revision: 6, saved_at_unix_secs: beforeProcessStart },
      ],
    }),
  );
  view.rerender(<SettingsComponent />);

  expect(screen.getByTestId('restart-drift-banner').textContent).toContain(
    'not applied yet — restart cc-lb',
  );
  expect(queryMocks.useConfigHistory).toHaveBeenCalledTimes(2);
});

test('database snapshot exposes progress and blocks duplicate clicks', () => {
  setSettingsLoaded();
  apiMocks.downloadJson.mockReturnValue(Promise.withResolvers<void>().promise);
  render(<SettingsComponent />);

  fireEvent.click(
    screen.getByRole('button', { name: 'Download database snapshot' }),
  );
  const downloading = screen.getByRole('button', { name: 'Downloading...' });
  expect(downloading.hasAttribute('disabled')).toBe(true);
  expect(downloading.getAttribute('aria-busy')).toBe('true');
  fireEvent.click(downloading);
  expect(apiMocks.downloadJson).toHaveBeenCalledTimes(1);
  expect(apiMocks.downloadJson).toHaveBeenCalledWith(
    '/admin/v1/export',
    'cc-lb-database-resources-2026-09-15T12:00.json',
  );
});
