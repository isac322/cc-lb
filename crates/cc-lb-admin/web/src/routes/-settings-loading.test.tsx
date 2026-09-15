// @vitest-environment jsdom
import {
  act,
  cleanup,
  fireEvent,
  type RenderResult,
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

const routerMocks = vi.hoisted(() => ({
  navigate: vi.fn(),
  search: {} as Record<string, unknown>,
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

vi.mock('@tanstack/react-router', async () => {
  const actual = await vi.importActual<Record<string, unknown>>(
    '@tanstack/react-router',
  );
  return {
    ...actual,
    useNavigate: () => routerMocks.navigate,
  };
});

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
              properties: {
                max_connections: { type: 'integer', minimum: 1, maximum: 64 },
                min_connections: { type: 'integer' },
                idle_timeout_secs: { type: 'integer' },
                max_lifetime_secs: { type: 'integer' },
              },
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
          properties: {
            max_connections: { type: 'integer' },
            keepalive_secs: { type: 'integer' },
          },
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
    pool: {
      max_connections: 10,
      min_connections: 2,
      idle_timeout_secs: 600,
      max_lifetime_secs: 1800,
    },
  },
  aead: { key_env: 'CC_LB_MASTER_KEY' },
  event_bus: { broadcast_capacity: 4096 },
  request_event_retention_days: 90,
  scheduler: {
    separate_pool: { max_connections: 5, keepalive_secs: 120 },
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

function applySearchUpdate(
  update:
    | Record<string, unknown>
    | ((previous: Record<string, unknown>) => Record<string, unknown>),
) {
  routerMocks.search =
    typeof update === 'function' ? update(routerMocks.search) : update;
}

function flushEffects() {
  return act(async () => Promise.resolve());
}

function showCategory(view: RenderResult, name: string) {
  const rail = screen.getByTestId('config-rail');
  fireEvent.click(
    within(rail).getByRole('button', { name: new RegExp(`^${name}`) }),
  );
  view.rerender(<SettingsComponent />);
}

function revealField(path: string) {
  const field = document.querySelector<HTMLElement>(
    `[data-config-path="${path}"]`,
  );
  expect(field, `field ${path} to exist`).not.toBeNull();
  const details = field?.closest('details');
  if (details && !details.hasAttribute('open')) {
    const summary = details.querySelector('summary');
    if (summary) fireEvent.click(summary);
  }
  return field as HTMLElement;
}

// Resolves the declared minimum height of an element to pixels. jsdom has no
// layout, so this reads the min-height declaration — inline style, arbitrary
// Tailwind value (min-h-[44px]), or spacing scale (min-h-11 = 44px).
function minHeightPx(element: HTMLElement): number | null {
  const inline = element.style.minHeight;
  if (inline.endsWith('px')) return Number.parseFloat(inline);
  const arbitrary = element.className.match(/min-h-\[(\d+(?:\.\d+)?)px\]/);
  if (arbitrary) return Number.parseFloat(arbitrary[1] ?? '');
  const scale = element.className.match(
    /(?:^|\s)min-h-(\d+(?:\.\d+)?)(?:\s|$)/,
  );
  if (scale) return Number.parseFloat(scale[1] ?? '') * 4;
  return null;
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
  routerMocks.search = {};
  routerMocks.navigate.mockImplementation(
    (args: {
      search?:
        | Record<string, unknown>
        | ((previous: Record<string, unknown>) => Record<string, unknown>);
    }) => {
      if (args?.search) applySearchUpdate(args.search);
    },
  );
  Object.assign(SettingsRoute, { useSearch: () => routerMocks.search });
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
  expect(editorSkeleton.className).toMatch(/min-h-/);
  expect(editorSkeleton.children.length).toBeGreaterThan(0);

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
  expect(screen.queryByRole('link', { name: 'Apply' })).toBeNull();
  expect(screen.queryByText(/hot.?reload/i)).toBeNull();
  expect(screen.queryByText(/takes effect immediately/i)).toBeNull();
});

test('operator rail navigates seven categories and renders only the selected panel', () => {
  setSettingsLoaded();
  const view = render(<SettingsComponent />);

  const rail = screen.getByTestId('config-rail');
  expect(rail.tagName).toBe('NAV');
  const items = within(rail).getAllByRole('button');
  expect(items).toHaveLength(7);
  const labels = [
    'Network & requests',
    'Storage & data',
    'Scheduling',
    'Routing & resilience',
    'Identity & access',
    'Pricing & quotas',
    'Runtime & observability',
  ];
  items.forEach((item, index) => {
    expect(item.textContent).toContain(labels[index]);
  });

  const network = within(rail).getByRole('button', {
    name: /^Network & requests/,
  });
  expect(network.getAttribute('aria-current')).toBe('page');
  expect(screen.getByRole('textbox', { name: 'Proxy Addr' })).toBeDefined();
  expect(screen.queryByLabelText('Tracing Level')).toBeNull();

  fireEvent.click(
    within(rail).getByRole('button', { name: /^Runtime & observability/ }),
  );
  expect(routerMocks.navigate).toHaveBeenCalled();
  expect(routerMocks.search).toMatchObject({ category: 'runtime' });
  view.rerender(<SettingsComponent />);

  expect(
    within(screen.getByTestId('config-rail'))
      .getByRole('button', { name: /^Runtime & observability/ })
      .getAttribute('aria-current'),
  ).toBe('page');
  expect(
    within(screen.getByTestId('config-rail'))
      .getByRole('button', { name: /^Network & requests/ })
      .getAttribute('aria-current'),
  ).toBeNull();
  expect(screen.queryByLabelText('Proxy Addr')).toBeNull();
  expect(screen.getByLabelText('Tracing Level')).toBeDefined();
});

test('mobile category select offers the same seven categories and drives the panel', () => {
  setSettingsLoaded();
  const view = render(<SettingsComponent />);

  const select = screen.getByRole('combobox', {
    name: 'Configuration category',
  }) as HTMLSelectElement;
  const options = within(select).getAllByRole('option');
  expect(options).toHaveLength(7);
  expect(select.selectedOptions[0]?.textContent).toContain(
    'Network & requests',
  );

  const scheduling = options.find((option) =>
    option.textContent?.includes('Scheduling'),
  ) as HTMLOptionElement;
  fireEvent.change(select, { target: { value: scheduling.value } });
  expect(routerMocks.search).toMatchObject({ category: 'scheduling' });
  view.rerender(<SettingsComponent />);

  expect(select.selectedOptions[0]?.textContent).toContain('Scheduling');
  expect(screen.queryByLabelText('Proxy Addr')).toBeNull();
  expect(screen.getByText('custom_job')).toBeDefined();
});

test('section cards group controls and keep advanced fields behind one disclosure', () => {
  setSettingsLoaded();
  const view = render(<SettingsComponent />);

  for (const heading of [
    'Listener endpoints',
    'TLS',
    'Request body limits',
    'Timeouts',
  ]) {
    expect(screen.getByRole('heading', { name: heading })).toBeDefined();
  }
  showCategory(view, 'Scheduling');
  const cards = document.querySelectorAll(
    '[data-testid="config-section-card"]',
  );
  expect(cards.length).toBeGreaterThan(1);
  for (const card of cards) {
    expect(
      card.querySelectorAll('[data-testid="config-advanced"]').length,
    ).toBeLessThanOrEqual(1);
  }

  const keepaliveField = document.querySelector<HTMLElement>(
    '[data-config-path="scheduler.separate_pool.keepalive_secs"]',
  );
  expect(keepaliveField).not.toBeNull();
  const advanced = keepaliveField?.closest('details');
  expect(advanced?.getAttribute('data-testid')).toBe('config-advanced');
  expect(advanced?.hasAttribute('open')).toBe(false);
});

test('a deep-linked advanced field opens its disclosure and receives focus', async () => {
  setSettingsLoaded();
  routerMocks.search = {
    category: 'scheduling',
    field: 'scheduler.separate_pool.keepalive_secs',
  };
  render(<SettingsComponent />);
  await flushEffects();

  const field = document.querySelector<HTMLElement>(
    '[data-config-path="scheduler.separate_pool.keepalive_secs"]',
  );
  expect(field).not.toBeNull();
  expect(field?.closest('details')?.hasAttribute('open')).toBe(true);
  expect(field?.contains(document.activeElement)).toBe(true);
  expect(document.activeElement?.hasAttribute('data-field-control')).toBe(true);
});

test('settings search stays local until a result jumps to its field', async () => {
  setSettingsLoaded();
  const view = render(<SettingsComponent />);

  const search = screen.getByRole('searchbox', { name: /search/i });
  fireEvent.change(search, { target: { value: 'tracing' } });
  expect(routerMocks.navigate).not.toHaveBeenCalled();

  const results = screen.getByTestId('config-search-results');
  const option = within(results).getByRole('button', { name: /tracing/i });
  fireEvent.click(option);
  expect(routerMocks.search).toMatchObject({
    category: 'runtime',
    field: 'observability.tracing_level',
  });
  view.rerender(<SettingsComponent />);
  await flushEffects();

  const field = document.querySelector<HTMLElement>(
    '[data-config-path="observability.tracing_level"]',
  );
  expect(field?.contains(document.activeElement)).toBe(true);
});

test('duration and byte controls pair raw storage values with humanized units', () => {
  setSettingsLoaded();
  render(<SettingsComponent />);

  const timeout = revealField('timeouts.upstream_total_secs');
  expect(within(timeout).getByRole('spinbutton')).toBeDefined();
  expect(timeout.textContent).toMatch(/10 minutes/);
  expect(timeout.textContent).toMatch(/600/);

  const cap = revealField('body.messages_cap_bytes');
  expect(cap.textContent).toMatch(/1 KiB/);
  expect(cap.textContent).toMatch(/1024/);
});

test('unknown file keys land in the Other settings fallback area', () => {
  setSettingsLoaded(
    editorResponse({
      file_config: {
        ...structuredClone(fileConfig),
        mystery_key: 'preserved',
      },
    }),
  );
  const view = render(<SettingsComponent />);

  showCategory(view, 'Runtime & observability');
  expect(screen.getByText(/Other settings/i)).toBeDefined();
  expect(
    document.querySelector('[data-config-path="mystery_key"]'),
  ).not.toBeNull();
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

test('running summary exposes effective listener and storage facts with restart-only framing', () => {
  setSettingsLoaded();
  render(<SettingsComponent />);

  const summary = screen.getByTestId('config-running-summary');
  expect(summary.textContent).toContain('127.0.0.1:9090');
  expect(summary.textContent).toMatch(/postgres/i);
  expect(
    screen.getAllByText(/requires a (cc-lb )?restart/i).length,
  ).toBeGreaterThan(0);
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

  const field = revealField('listener.proxy_addr');
  const input = screen.getByLabelText('Proxy Addr') as HTMLInputElement;
  expect(input.value).toBe('0.0.0.0:8080');
  expect(input.hasAttribute('disabled')).toBe(false);
  expect(field.textContent).toMatch(/Effective[: ]*127\.0\.0\.1:8181/);

  fireEvent.click(within(field).getByText('Value details'));
  expect(field.textContent).toMatch(/File[: ]*0\.0\.0\.0:8080/);
  expect(field.textContent).toMatch(/Default[: ]*\[::\]:8080/);
  expect(field.textContent).toMatch(/Environment · CC_LB_PROXY_ADDR/);

  fireEvent.change(input, { target: { value: '0.0.0.0:8181' } });
  expect(input.value).toBe('0.0.0.0:8181');
  expect(field.textContent).toMatch(/Effective[: ]*127\.0\.0\.1:8181/);
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
  const view = render(<SettingsComponent />);
  showCategory(view, 'Identity & access');

  const providerId = document.querySelector<HTMLElement>(
    '[data-config-path="admin.auth.providers[0].id"]',
  );
  expect(providerId).not.toBeNull();
  expect(
    within(providerId as HTMLElement).getAllByText(
      /Environment · CC_LB_ADMIN_AUTH_PROVIDERS_JSON/,
    ).length,
  ).toBeGreaterThan(0);
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
      name: /listener\.tls\.cert_path[\s\S]*missing_required_file/,
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
  const view = render(<SettingsComponent />);
  showCategory(view, 'Runtime & observability');

  const field = revealField('runtime.wasmtime.memory_max_pages');
  fireEvent.click(within(field).getByRole('button', { name: 'Set value' }));
  const input = within(field).getByRole('spinbutton') as HTMLInputElement;
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
  const view = render(<SettingsComponent />);
  showCategory(view, 'Scheduling');
  expect(screen.getByText('custom_job')).toBeDefined();
  expect(
    document.querySelector(
      '[data-config-path="scheduler.recurring_jobs.custom_job.enabled"]',
    ),
  ).not.toBeNull();

  const field = revealField('scheduler.dlq_retention_days');
  fireEvent.change(within(field).getByRole('spinbutton'), {
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
  const view = render(<SettingsComponent />);
  showCategory(view, 'Runtime & observability');

  const field = revealField('runtime.wasmtime.memory_max_pages');
  fireEvent.click(within(field).getByRole('button', { name: 'Set value' }));
  const input = within(field).getByRole('spinbutton') as HTMLInputElement;
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
  const view = render(<SettingsComponent />);
  showCategory(view, 'Storage & data');

  expect(
    screen.queryByDisplayValue('__CC_LB_STORAGE_URL_UNCHANGED__'),
  ).toBeNull();
  const urlField = revealField('storage.url');
  expect(within(urlField).queryByRole('button', { name: 'Reset' })).toBeNull();
  expect(within(urlField).getByRole('button', { name: 'Unset' })).toBeDefined();

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
  const view = render(<SettingsComponent />);
  showCategory(view, 'Storage & data');

  const urlField = revealField('storage.url');
  expect(
    within(urlField).queryByText('__CC_LB_STORAGE_URL_UNCHANGED__'),
  ).toBeNull();
  fireEvent.click(within(urlField).getByRole('button', { name: 'Unset' }));
  expect(within(urlField).getByText('No stored URL')).toBeDefined();
  fireEvent.click(screen.getByRole('button', { name: 'Save draft' }));

  const request = saveDraftMutate.mock.calls[0]?.[0] as {
    draft: {
      storage: { url?: unknown; pool: Record<string, number> };
    };
  };
  expect('url' in request.draft.storage).toBe(false);
  expect(request.draft.storage.pool).toEqual({
    max_connections: 10,
    min_connections: 2,
    idle_timeout_secs: 600,
    max_lifetime_secs: 1800,
  });
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
      name: /observability\.tracing_level[\s\S]*invalid_value/,
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
      name: /observability\.tracing_level[\s\S]*invalid_value/,
    }),
  );
  await flushEffects();

  const field = document.querySelector<HTMLElement>(
    '[data-config-path="observability.tracing_level"]',
  );
  expect(field?.contains(document.activeElement)).toBe(true);
});

test('a failed validation focuses the error summary before field navigation', async () => {
  const validateMutate = vi.fn();
  queryMocks.useValidateConfig.mockReturnValue(mutationResult(validateMutate));
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
  };
  setSettingsLoaded();
  render(<SettingsComponent />);

  fireEvent.click(screen.getByRole('button', { name: 'Validate' }));
  const options = validateMutate.mock.calls[0]?.[1] as {
    onSuccess: (report: unknown) => void;
  };
  act(() => options.onSuccess(invalidReport));
  await flushEffects();

  const summary = screen.getByTestId('config-validation-summary');
  // Focus moves to the summary, but the summary must not re-announce itself
  // as an alert on top of the focus change.
  expect(summary.getAttribute('role')).not.toBe('alert');
  expect(document.activeElement).toBe(summary);
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
      name: /admin\.auth\.providers\[0\][\s\S]*invalid_provider/,
    }),
  );
  await flushEffects();

  const activePath = document.activeElement
    ?.closest('[data-config-path]')
    ?.getAttribute('data-config-path');
  expect(activePath).toMatch(/^admin\.auth\.providers\[0\]/);
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
    timeouts: { upstream_total_secs: 300 },
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

  const reviewList = screen.getByTestId('config-review-list');
  expect(reviewList.textContent).toContain('admin.auth.providers');
  expect(reviewList.textContent).toContain('timeouts.upstream_total_secs');
  expect(reviewList.textContent).toContain('600');
  expect(reviewList.textContent).toContain('300');
  expect(reviewList.textContent).toContain('Dangerous changes');
  expect(reviewList.textContent).toContain('Network & requests');
  expect(
    reviewList.textContent?.indexOf('admin.auth.providers') ?? -1,
  ).toBeLessThan(
    reviewList.textContent?.indexOf('timeouts.upstream_total_secs') ?? -1,
  );
  // The provider item leaves already describe the diff, so the bare
  // container row must not be repeated.
  expect(reviewList.textContent).not.toMatch(/admin\.auth\.providers:/);

  const confirmButton = screen.getByRole('button', {
    name: 'Save and accept lockout risk',
  });
  expect(confirmButton.hasAttribute('disabled')).toBe(true);
  fireEvent.click(screen.getByTestId('self-lockout-ack'));
  expect(confirmButton.hasAttribute('disabled')).toBe(false);
  fireEvent.click(confirmButton);
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

test('database pool renders each field once with advanced controls behind the disclosure', () => {
  setSettingsLoaded();
  const view = render(<SettingsComponent />);
  showCategory(view, 'Storage & data');

  const poolCard = document.querySelector<HTMLElement>(
    '[data-config-section="database-pool"]',
  );
  expect(poolCard).not.toBeNull();
  // The storage union must not collapse into this section: its container and
  // the pool object render elsewhere or not at all.
  expect(poolCard?.querySelector('[data-config-path="storage"]')).toBeNull();
  expect(
    poolCard?.querySelector('[data-config-path="storage.pool"]'),
  ).toBeNull();

  for (const path of [
    'storage.pool.max_connections',
    'storage.pool.min_connections',
  ]) {
    const fields = poolCard?.querySelectorAll(`[data-config-path="${path}"]`);
    expect(fields, path).toHaveLength(1);
    expect(fields?.[0]?.closest('details'), path).toBeNull();
  }

  const disclosure = poolCard?.querySelector<HTMLElement>(
    '[data-testid="config-advanced"]',
  );
  expect(disclosure).not.toBeNull();
  expect(disclosure?.hasAttribute('open')).toBe(false);
  for (const path of [
    'storage.pool.idle_timeout_secs',
    'storage.pool.max_lifetime_secs',
  ]) {
    const fields = poolCard?.querySelectorAll(`[data-config-path="${path}"]`);
    expect(fields, path).toHaveLength(1);
    expect(fields?.[0]?.closest('details'), path).toBe(disclosure);
  }

  // The union editor itself stays in the primary storage section.
  expect(
    document.querySelectorAll('[data-config-path="storage"]'),
  ).toHaveLength(1);
});

test('numeric fields surface schema bounds as a range hint', () => {
  setSettingsLoaded();
  const view = render(<SettingsComponent />);

  const cap = revealField('body.messages_cap_bytes');
  expect(cap.textContent).toMatch(/range/i);
  expect(cap.textContent).toMatch(/range[^\d]*1\D*no max/i);

  const address = revealField('listener.proxy_addr');
  expect(address.textContent).not.toMatch(/range/i);

  showCategory(view, 'Storage & data');
  const maxConnections = revealField('storage.pool.max_connections');
  expect(maxConnections.textContent).toMatch(/range[^\d]*1\D*64/i);

  const capacity = revealField('event_bus.broadcast_capacity');
  expect(capacity.textContent).not.toMatch(/range/i);
});

test('unassigned leaves land in their own category Other settings and stay searchable', async () => {
  const schema = {
    ...configSchema,
    properties: {
      ...configSchema.properties,
      event_bus: {
        type: 'object',
        properties: {
          broadcast_capacity: { type: 'integer' },
          retry_attempts: { type: 'integer' },
        },
      },
    },
  };
  const file = {
    ...structuredClone(fileConfig),
    event_bus: {
      broadcast_capacity: 4096,
      retry_attempts: 3,
      mystery_flag: 'preserved',
    },
    legacy_mode: true,
  };
  const draft = structuredClone(file);
  draft.event_bus.retry_attempts = 5;
  setSettingsLoaded(
    editorResponse({
      schema,
      file_config: file,
      effective_config: file,
      draft,
    }),
    draftResponse({ draft }),
  );
  const view = render(<SettingsComponent />);

  showCategory(view, 'Storage & data');
  const dataPanel = document.querySelector<HTMLElement>(
    '[data-config-category-panel="data"]',
  );
  expect(dataPanel).not.toBeNull();
  const dataOther = dataPanel?.querySelector<HTMLElement>(
    '[data-config-section="other"]',
  );
  expect(dataOther).not.toBeNull();
  expect(
    dataOther?.querySelector('[data-config-path="event_bus.retry_attempts"]'),
  ).not.toBeNull();
  expect(
    dataOther?.querySelector('[data-config-path="event_bus.mystery_flag"]'),
  ).not.toBeNull();
  // Schema-known and unknown unassigned leaves get distinct explanations.
  expect(dataOther?.textContent).toMatch(/not covered by a settings section/i);
  expect(dataOther?.textContent).toMatch(/not recognized by the schema/i);
  // An unknown root key belongs to the fallback category, not this one.
  expect(
    dataOther?.querySelector('[data-config-path="legacy_mode"]'),
  ).toBeNull();
  // The modified unassigned leaf counts toward its own category.
  expect(
    screen.getByRole('button', { name: /^Storage & data.*1 modified/ }),
  ).toBeDefined();

  const search = screen.getByRole('searchbox', { name: /search/i });
  fireEvent.change(search, { target: { value: 'retry_attempts' } });
  const results = screen.getByTestId('config-search-results');
  fireEvent.click(
    within(results).getByRole('button', { name: /retry attempts/i }),
  );
  expect(routerMocks.search).toMatchObject({
    category: 'data',
    field: 'event_bus.retry_attempts',
  });
  view.rerender(<SettingsComponent />);
  await flushEffects();

  const field = document.querySelector<HTMLElement>(
    '[data-config-path="event_bus.retry_attempts"]',
  );
  expect(field?.contains(document.activeElement)).toBe(true);

  showCategory(view, 'Runtime & observability');
  const runtimeOther = document
    .querySelector('[data-config-category-panel="runtime"]')
    ?.querySelector<HTMLElement>('[data-config-section="other"]');
  expect(runtimeOther).not.toBeNull();
  expect(
    runtimeOther?.querySelector('[data-config-path="legacy_mode"]'),
  ).not.toBeNull();
  expect(runtimeOther?.textContent).toMatch(/not recognized by the schema/i);
});

test('search results are plain buttons announced through a status region', () => {
  setSettingsLoaded();
  render(<SettingsComponent />);

  const search = screen.getByRole('searchbox', { name: /search/i });
  fireEvent.change(search, { target: { value: 'tracing' } });

  const results = screen.getByTestId('config-search-results');
  expect(results.getAttribute('role')).not.toBe('listbox');
  const buttons = within(results).getAllByRole('button');
  expect(buttons.length).toBeGreaterThan(0);
  for (const button of buttons) {
    expect(button.getAttribute('role')).toBeNull();
    expect(button.getAttribute('aria-selected')).toBeNull();
  }
  const status = screen.getByTestId('config-search-status');
  expect(status.getAttribute('role')).toBe('status');
  expect(status.className).toMatch(/sr-only/);
  expect(status.textContent).toMatch(/\d+ settings? match/i);

  fireEvent.change(search, { target: { value: 'zzz-no-such-setting' } });
  expect(screen.getByTestId('config-search-status').textContent).toMatch(
    /no settings match/i,
  );
});

test('value details disclosure exposes a 44px touch target', () => {
  setSettingsLoaded();
  render(<SettingsComponent />);

  const field = revealField('listener.proxy_addr');
  const details = within(field).getByTestId('config-value-details');
  const summary = details.querySelector('summary');
  expect(summary).not.toBeNull();
  expect(summary?.textContent).toMatch(/value details/i);
  expect(minHeightPx(summary as HTMLElement)).toBeGreaterThanOrEqual(44);
});

test('storage variant switch review lists removed leaves without a container row', () => {
  const sqliteDraft = {
    ...structuredClone(fileConfig),
    storage: {
      kind: 'sqlite',
      path: '/var/lib/cc-lb/storage.sqlite',
    },
  };
  setSettingsLoaded(
    editorResponse({ draft: sqliteDraft, effective_config: sqliteDraft }),
    draftResponse({ draft: sqliteDraft }),
  );
  render(<SettingsComponent />);
  // Counts track displayable active fields: only storage.kind and
  // storage.path are active and modified after the switch.
  expect(
    screen.getByRole('button', { name: /^Storage & data.*2 modified/ }),
  ).toBeDefined();
  const categorySelect = screen.getByRole('combobox', {
    name: 'Configuration category',
  }) as HTMLSelectElement;
  expect(
    [...categorySelect.options].find((option) => option.value === 'data')
      ?.textContent,
  ).toContain('2 modified');

  fireEvent.click(screen.getByRole('button', { name: 'Save to config file' }));
  const reviewList = screen.getByTestId('config-review-list');
  // Every path deleted by the postgres → sqlite switch is listed with its old
  // value becoming Not set; the opaque URL stays hidden.
  expect(reviewList.textContent).toMatch(
    /storage\.kind:\s*postgres\s*→\s*sqlite/,
  );
  expect(reviewList.textContent).toMatch(/storage\.path:\s*Not set\s*→/);
  expect(reviewList.textContent).toMatch(
    /storage\.url:\s*Hidden\s*→\s*Not set/,
  );
  expect(reviewList.textContent).not.toContain(
    '__CC_LB_STORAGE_URL_UNCHANGED__',
  );
  for (const path of [
    'storage.pool.max_connections',
    'storage.pool.min_connections',
    'storage.pool.idle_timeout_secs',
    'storage.pool.max_lifetime_secs',
  ]) {
    expect(reviewList.textContent, path).toMatch(
      new RegExp(`${path.replaceAll('.', '\\.')}:[^→]*→\\s*Not set`),
    );
  }
  // The leaf rows already describe the diff; no bare container rows.
  expect(within(reviewList).queryAllByText('storage')).toHaveLength(0);
  expect(within(reviewList).queryAllByText('storage.pool')).toHaveLength(0);
});
