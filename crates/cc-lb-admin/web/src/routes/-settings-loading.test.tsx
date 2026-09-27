// @vitest-environment jsdom
import {
  act,
  cleanup,
  createEvent,
  fireEvent,
  type RenderResult,
  render,
  screen,
  within,
} from '@testing-library/react';
import type React from 'react';
import { afterEach, beforeEach, expect, test, vi } from 'vitest';
import {
  CONFIG_EDITOR_CATEGORIES,
  recurringJobMetadata,
  resolveConfigFieldGuidance,
} from '../lib/configEditorModel';
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
      description: 'Primary state store backend and connection.',
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
        github: {
          type: 'object',
          properties: {
            client_id: { type: 'string' },
            scopes: { type: 'array', items: { type: 'string' } },
          },
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
      description: 'Tracing, telemetry, and log redaction.',
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
      usage_rollup: { enabled: true, interval_secs: 3600, jitter_secs: 30 },
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
  oauth: { anthropic: null, github: { client_id: 'gh-client', scopes: [] } },
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
      usage_rollup: structuredClone(
        fileConfig.scheduler.recurring_jobs.usage_rollup,
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

function showCategory(_view: RenderResult, name: string) {
  const nav = screen.getByTestId('config-category-nav');
  // The click drives selectCategory → setSelectedCategory, which re-renders
  // the editor with the new panel on its own. The navigate mock only mutates
  // routerMocks.search; re-rendering the whole page to re-read it produces
  // identical state (the category-sync effect sets the same value), so the
  // extra full-page render is pure cost.
  fireEvent.click(
    within(nav).getByRole('button', { name: new RegExp(`^${name}`) }),
  );
}

function categoryNavItems(nav: HTMLElement) {
  return Array.from(nav.querySelectorAll<HTMLElement>('button[data-value]'));
}

// Resolves whether a section element effectively renders a top separator.
// Sections carry no chrome of their own: the parent's `space-y-*` gap
// separates sibling sections. A `border-t`/`pt-*` on the section or a
// `divide-y` parent would draw a rule or padding instead. An intro line
// before the first section is not a section, so the first section stays
// flush. Returns what the reader sees.
function sectionTopSeparator(
  section: HTMLElement,
): 'border' | 'padding' | 'divide' | 'space' | 'none' {
  const isFirst = section.parentElement?.firstElementChild === section;
  let previous = section.previousElementSibling;
  while (previous && previous.tagName !== section.tagName) {
    previous = previous.previousElementSibling;
  }
  const isFirstSection = previous === null;
  const tokens = section.className.split(/\s+/);
  const hasToken = (re: RegExp) => tokens.some((token) => re.test(token));
  const firstReset = (suffix: string) =>
    tokens.some((token) => token.includes('first') && token.endsWith(suffix));
  const border = hasToken(/^border-t$/) || hasToken(/^border-t-(?!0\b)/);
  const padding = hasToken(/^pt-(?!0\b)/);
  if (border && !(isFirst && firstReset(':border-t-0'))) return 'border';
  if (padding && !(isFirst && firstReset(':pt-0'))) return 'padding';
  const parentTokens =
    section.parentElement?.className.split(/\s+/) ?? ([] as string[]);
  if (
    !isFirstSection &&
    parentTokens.some((token) => /^divide-y/.test(token))
  ) {
    return 'divide';
  }
  if (
    !isFirstSection &&
    parentTokens.some((token) => /^space-y-(?!0\b)/.test(token))
  ) {
    return 'space';
  }
  return 'none';
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

// Resolves a declared fixed height/width to pixels — arbitrary Tailwind value
// (h-[32px]) or spacing scale (h-8 = 32px). Used for touch-target checks.
function dimensionPx(element: HTMLElement, axis: 'h' | 'w'): number | null {
  const arbitrary = element.className.match(
    new RegExp(`(?:^|\\s)${axis}-\\[(\\d+(?:\\.\\d+)?)px\\]`),
  );
  if (arbitrary) return Number.parseFloat(arbitrary[1] ?? '');
  const scale = element.className.match(
    new RegExp(`(?:^|\\s)${axis}-(\\d+(?:\\.\\d+)?)(?:\\s|$)`),
  );
  if (scale) return Number.parseFloat(scale[1] ?? '') * 4;
  return null;
}

beforeEach(() => {
  vi.useFakeTimers({
    // Fake the clock and the page's timers (the 1s "now" ticker in
    // SettingsPage, react-timeago reschedules) so no stray render can fire
    // mid-test. queueMicrotask stays real: the requestAnimationFrame stub
    // below and act() flushing depend on it.
    toFake: [
      'Date',
      'setTimeout',
      'clearTimeout',
      'setInterval',
      'clearInterval',
    ],
    now: new Date('2026-09-15T12:00:01.000Z'),
  });
  // The localization card enumerates every ICU locale and timezone into
  // <option> elements (~560 nodes) and re-sorts them on each render. No test
  // here asserts those lists, so pin them to a single entry — it keeps the
  // rendered DOM small enough that per-test wall time stays well inside the
  // 5s budget even on a CPU-starved worker.
  vi.spyOn(Intl, 'supportedValuesOf').mockReturnValue(['UTC']);
  vi.spyOn(Intl.DateTimeFormat, 'supportedLocalesOf').mockReturnValue([
    'en-US',
  ]);
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
  vi.restoreAllMocks();
});

test('settings cold load shows skeletons for version, editor, and history', () => {
  render(<SettingsComponent />);

  const versionCard = screen.getByTestId('version-card');
  expect(versionCard.querySelectorAll('.skeleton')).toHaveLength(6);
  // The status zone sits above the editor card; its facts strip is a
  // borderless, low-emphasis pair of lists — no panel chrome or divider.
  const statusZone = screen.getByTestId('config-status-zone');
  const editorCard = screen.getByTestId('config-editor-card');
  expect(
    (statusZone.compareDocumentPosition(editorCard) &
      Node.DOCUMENT_POSITION_FOLLOWING) !==
      0,
  ).toBe(true);
  const metadata = within(statusZone).getByTestId('config-editor-metadata');
  const factsStrip = metadata.parentElement as HTMLElement;
  expect(factsStrip.className).not.toMatch(/(^|\s)(border|bg-|rounded|min-h-)/);
  // Until categories load, one placeholder per category stands in the tab
  // row — no real navigation that could be clicked before data arrives.
  const tabsSkeleton = within(editorCard).getByTestId(
    'config-category-nav-skeleton',
  );
  expect(tabsSkeleton.querySelectorAll('.skeleton')).toHaveLength(
    CONFIG_EDITOR_CATEGORIES.length,
  );
  expect(within(editorCard).queryByTestId('config-category-nav')).toBeNull();
  // The body skeleton mirrors the loaded layout: flat space-separated
  // sections instead of boxed cards.
  const editorSkeleton = screen.getByTestId('config-editor-skeleton');
  const skeletonSections = Array.from(editorSkeleton.children).slice(
    1,
  ) as HTMLElement[];
  expect(skeletonSections.length).toBeGreaterThan(0);
  skeletonSections.forEach((section, index) => {
    // Mirrors the loaded contract: the first section is flush, later
    // siblings are separated by space only — no rule, no padding.
    expect(sectionTopSeparator(section)).toBe(index === 0 ? 'none' : 'space');
    expect(section.className).not.toMatch(/(^|\s)rounded/);
    expect(section.className).not.toMatch(/(^|\s)bg-/);
  });

  const historySlot = screen.getByTestId('config-history-slot');
  expect(historySlot.querySelectorAll('.skeleton')).toHaveLength(8);
  expect(
    within(historySlot).queryByText('No saved config history available.'),
  ).toBeNull();
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
  expect(screen.getByText('Saved config history')).toBeDefined();
  expect(screen.getByText('Data & backups')).toBeDefined();
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

test('category navigation landmark lists seven categories and renders only the selected panel', () => {
  setSettingsLoaded();
  routerMocks.search = { q: 'tracing' };
  const view = render(<SettingsComponent />);

  // The control is a navigation landmark, not a tab widget: plain buttons
  // marked with aria-current, no tablist/tab roles or aria-selected.
  const nav = screen.getByTestId('config-category-nav');
  expect(nav.tagName).toBe('NAV');
  expect(nav.getAttribute('aria-label')).toBe('Configuration categories');
  expect(nav.getAttribute('role')).toBeNull();
  expect(nav.querySelector('[role="tablist"]')).toBeNull();
  const items = categoryNavItems(nav);
  expect(items).toHaveLength(7);
  items.forEach((item, index) => {
    const meta = CONFIG_EDITOR_CATEGORIES[index];
    expect(item.textContent).toContain(meta?.label);
    expect(item.getAttribute('role')).toBeNull();
    expect(item.getAttribute('aria-selected')).toBeNull();
  });

  const network = within(nav).getByRole('button', {
    name: /^Network & requests/,
  });
  expect(network.getAttribute('aria-current')).toBe('page');

  // A q deep-link restores the query text without opening the overlay.
  const searchInput = screen.getByRole('combobox', {
    name: /search/i,
  }) as HTMLInputElement;
  expect(searchInput.value).toBe('tracing');
  expect(screen.queryByTestId('config-search-results')).toBeNull();
  expect(searchInput.getAttribute('aria-expanded')).toBe('false');

  // The nav is the editor card's own top row: it sits inside the card,
  // after the status zone and before the panel it drives. The active
  // category's h3 names the panel (via aria-labelledby) and its description
  // is visible exactly once, above the panel rather than inside it.
  const editorCard = screen.getByTestId('config-editor-card');
  const panel = screen.getByRole('region', { name: 'Network & requests' });
  expect(editorCard.contains(nav)).toBe(true);
  expect(
    (screen.getByTestId('config-status-zone').compareDocumentPosition(nav) &
      Node.DOCUMENT_POSITION_FOLLOWING) !==
      0,
  ).toBe(true);
  expect(
    (nav.compareDocumentPosition(panel) & Node.DOCUMENT_POSITION_FOLLOWING) !==
      0,
  ).toBe(true);
  const heading = within(editorCard).getByRole('heading', {
    level: 3,
    name: 'Network & requests',
  });
  const labelTarget = document.getElementById(
    panel.getAttribute('aria-labelledby') ?? '',
  );
  expect(labelTarget).not.toBeNull();
  expect(heading.contains(labelTarget)).toBe(true);
  expect(within(panel).queryByRole('heading', { level: 3 })).toBeNull();
  expect(panel.textContent).not.toContain(
    'Listeners, request bodies, and request deadlines.',
  );
  const visibleDescriptions = screen
    .getAllByText('Listeners, request bodies, and request deadlines.')
    .filter((element) => element.closest('.sr-only') === null);
  expect(visibleDescriptions).toHaveLength(1);
  expect(editorCard.contains(visibleDescriptions[0] as Node)).toBe(true);
  expect(
    within(panel).getByRole('textbox', { name: 'Proxy address' }),
  ).toBeDefined();
  expect(screen.queryByLabelText('Tracing level')).toBeNull();

  fireEvent.click(
    within(nav).getByRole('button', { name: /^Runtime & observability/ }),
  );
  // In-place category switches keep the operator's scroll position: the route
  // opts out of scroll restoration and no field is focused or scrolled to.
  expect(routerMocks.navigate).toHaveBeenCalledWith(
    expect.objectContaining({ resetScroll: false }),
  );
  // The switch clears the field deep-link but preserves the search query.
  expect(routerMocks.search).toMatchObject({
    category: 'runtime',
    q: 'tracing',
  });
  expect(routerMocks.search.field).toBeUndefined();
  expect(HTMLElement.prototype.scrollIntoView).not.toHaveBeenCalled();
  expect(document.activeElement?.closest('[data-config-path]')).toBeNull();
  view.rerender(<SettingsComponent />);

  const updatedNav = screen.getByTestId('config-category-nav');
  expect(
    within(updatedNav)
      .getByRole('button', { name: /^Runtime & observability/ })
      .getAttribute('aria-current'),
  ).toBe('page');
  expect(
    within(updatedNav)
      .getByRole('button', { name: /^Network & requests/ })
      .getAttribute('aria-current'),
  ).toBeNull();
  expect(
    categoryNavItems(updatedNav).filter(
      (item) => item.getAttribute('aria-current') === 'page',
    ),
  ).toHaveLength(1);
  const runtimePanel = screen.getByRole('region', {
    name: 'Runtime & observability',
  });
  const runtimeHeading = within(
    screen.getByTestId('config-editor-card'),
  ).getByRole('heading', { level: 3, name: 'Runtime & observability' });
  const runtimeLabelTarget = document.getElementById(
    runtimePanel.getAttribute('aria-labelledby') ?? '',
  );
  expect(runtimeHeading.contains(runtimeLabelTarget)).toBe(true);
  expect(within(runtimePanel).queryByRole('heading', { level: 3 })).toBeNull();
  expect(screen.queryByLabelText('Proxy address')).toBeNull();
  expect(screen.getByLabelText('Tracing level')).toBeDefined();
});

test('one underline tab row serves every viewport — no drawer or select', () => {
  setSettingsLoaded();
  const view = render(<SettingsComponent />);

  // Exactly one navigation landmark exists; there is no disclosure trigger,
  // drawer, dialog, or native select standing in for it at any viewport.
  expect(
    screen.getAllByRole('navigation', { name: 'Configuration categories' }),
  ).toHaveLength(1);
  expect(screen.queryByTestId('config-category-trigger')).toBeNull();
  expect(screen.queryByTestId('config-category-drawer')).toBeNull();
  expect(screen.queryByRole('dialog')).toBeNull();
  expect(
    screen.queryByRole('combobox', { name: 'Configuration category' }),
  ).toBeNull();

  // All seven categories are always rendered — none hidden at a breakpoint;
  // narrow screens scroll the one row instead.
  const nav = screen.getByTestId('config-category-nav');
  expect(nav.className).not.toMatch(/(^|\s)hidden(\s|$)/);
  const items = categoryNavItems(nav);
  expect(items).toHaveLength(CONFIG_EDITOR_CATEGORIES.length);
  for (const item of items) {
    expect(item.className).not.toMatch(/(^|\s)hidden(\s|$)/);
    expect(item.className).not.toMatch(/(^|\s)[a-z0-9]+:hidden(\s|$)/);
  }
  expect(
    items.filter((item) => item.getAttribute('aria-current') === 'page'),
  ).toHaveLength(1);
  // Status travels with the tab name for assistive tech, not only as dots.
  expect(
    within(nav).getByRole('button', { name: /^Network & requests$/ }),
  ).toBeDefined();

  // The same nav drives the panel directly — no intermediate disclosure.
  fireEvent.click(within(nav).getByRole('button', { name: /^Scheduling/ }));
  expect(routerMocks.navigate).toHaveBeenCalledWith(
    expect.objectContaining({ resetScroll: false }),
  );
  expect(routerMocks.search).toMatchObject({ category: 'scheduling' });
  expect(routerMocks.search.field).toBeUndefined();
  expect(HTMLElement.prototype.scrollIntoView).not.toHaveBeenCalled();
  view.rerender(<SettingsComponent />);

  const schedulingPanel = screen.getByRole('region', { name: 'Scheduling' });
  const schedulingHeading = within(
    screen.getByTestId('config-editor-card'),
  ).getByRole('heading', { level: 3, name: 'Scheduling' });
  const schedulingLabelTarget = document.getElementById(
    schedulingPanel.getAttribute('aria-labelledby') ?? '',
  );
  expect(schedulingHeading.contains(schedulingLabelTarget)).toBe(true);
  expect(
    within(schedulingPanel).queryByRole('heading', { level: 3 }),
  ).toBeNull();
  expect(screen.queryByLabelText('Proxy address')).toBeNull();
  expect(screen.getByText('custom_job')).toBeDefined();
});

test('sections stay flat on the category canvas and keep advanced fields behind one disclosure', () => {
  setSettingsLoaded();
  const view = render(<SettingsComponent />);

  // The card header owns the single category h3; flat sections sit one level
  // below inside the panel, which carries no heading of its own.
  const networkPanel = screen.getByRole('region', {
    name: 'Network & requests',
  });
  const networkHeading = within(
    screen.getByTestId('config-editor-card'),
  ).getByRole('heading', { level: 3, name: 'Network & requests' });
  const networkLabelTarget = document.getElementById(
    networkPanel.getAttribute('aria-labelledby') ?? '',
  );
  expect(networkHeading.contains(networkLabelTarget)).toBe(true);
  expect(within(networkPanel).queryByRole('heading', { level: 3 })).toBeNull();
  expect(
    within(screen.getByTestId('config-editor-card')).getAllByRole('heading', {
      level: 3,
    }),
  ).toHaveLength(1);
  for (const heading of [
    'Listener endpoints',
    'TLS',
    'Request body limits',
    'Timeouts',
  ]) {
    expect(
      screen.getByRole('heading', { level: 4, name: heading }),
    ).toBeDefined();
  }
  showCategory(view, 'Scheduling');
  const cards = Array.from(
    document.querySelectorAll<HTMLElement>(
      '[data-testid="config-section-card"]',
    ),
  );
  expect(cards.length).toBeGreaterThan(1);
  cards.forEach((card, index) => {
    // The first section sits flush under the card header. Every later
    // sibling is separated by space only — no top rule or padding; the
    // wrapper itself carries no card chrome (rounded border, tinted
    // background, or inset padding).
    expect(
      sectionTopSeparator(card),
      `section ${card.getAttribute('data-config-section')} separator`,
    ).toBe(index === 0 ? 'none' : 'space');
    expect(card.className).not.toMatch(/(^|\s)rounded/);
    expect(card.className).not.toMatch(/(^|\s)bg-/);
    expect(card.className).not.toMatch(/(^|\s)p[xy]?-\d/);
    expect(
      card.querySelectorAll('[data-testid="config-advanced"]').length,
    ).toBeLessThanOrEqual(1);
  });

  const keepaliveField = document.querySelector<HTMLElement>(
    '[data-config-path="scheduler.separate_pool.keepalive_secs"]',
  );
  expect(keepaliveField).not.toBeNull();
  const advanced = keepaliveField?.closest('details');
  expect(advanced?.getAttribute('data-testid')).toBe('config-advanced');
  expect(advanced?.hasAttribute('open')).toBe(false);
});
test('single-root compound sections let the section heading own the label', () => {
  setSettingsLoaded();
  const view = render(<SettingsComponent />);

  // When a section's whole body is one compound root, the section h4 owns
  // the label and the root's own heading and schema description are
  // suppressed — controls (switches, segmented selectors, add buttons) stay
  // untitled. Multi-root sections keep per-root headings so siblings stay
  // distinguishable, and field labels are never affected.
  const expectSingleRootSection = (
    sectionId: string,
    rootPath: string,
    hiddenDescription?: string,
  ) => {
    const card = document.querySelector<HTMLElement>(
      `[data-config-section="${sectionId}"]`,
    );
    expect(card, `${sectionId} section`).not.toBeNull();
    const root = card?.querySelector<HTMLElement>(
      `:scope > div > [data-config-path="${rootPath}"]`,
    );
    expect(root, `${sectionId} root ${rootPath}`).not.toBeNull();
    // Exactly one root-level element in the primary grid.
    const roots = Array.from(
      card?.querySelectorAll<HTMLElement>(
        ':scope > div > [data-config-path]',
      ) ?? [],
    );
    expect(roots, `${sectionId} single root`).toHaveLength(1);
    expect(roots[0]).toBe(root);
    // No heading belongs to the root container — nested containers may keep
    // theirs, but this fixture's single roots have none.
    const rootHeadings = Array.from(
      root?.querySelectorAll('h1, h2, h3, h4, h5, h6') ?? [],
    ).filter((heading) => heading.closest('[data-config-path]') === root);
    expect(rootHeadings, `${sectionId} root heading`).toHaveLength(0);
    if (hiddenDescription) {
      expect(card?.textContent).not.toContain(hiddenDescription);
    }
  };

  // Network: the TLS section is just the nullable listener.tls object. Its
  // Enabled switch is hoisted into the section header row — the same
  // items-center flex row that holds the h4 and its description — and the
  // root itself renders no duplicate toggle header.
  expectSingleRootSection('tls', 'listener.tls');
  const tlsSection = document.querySelector<HTMLElement>(
    '[data-config-section="tls"]',
  ) as HTMLElement;
  const tlsSwitch = within(tlsSection).getByRole('checkbox', {
    name: 'Enabled',
  });
  // The hoisted switch carries the root's own path so focusConfigPath can
  // resolve it as the exact target for listener.tls deep-links.
  expect(tlsSwitch.getAttribute('data-config-path')).toBe('listener.tls');
  expect(tlsSwitch.hasAttribute('data-field-control')).toBe(true);
  const tlsHeaderRow = tlsSwitch.closest('div.flex') as HTMLElement;
  expect(tlsHeaderRow.className).toContain('items-center');
  expect(
    within(tlsHeaderRow).getByRole('heading', { level: 4, name: 'TLS' }),
  ).toBeDefined();
  expect(tlsHeaderRow.textContent).toContain(
    'Certificate, key, and SIGHUP reload behavior.',
  );
  expect(tlsHeaderRow.parentElement).toBe(tlsSection);
  const tlsRoot = document.querySelector<HTMLElement>(
    'div[data-config-path="listener.tls"]',
  ) as HTMLElement;
  expect(tlsRoot).not.toBeNull();
  expect(within(tlsRoot).queryByRole('checkbox')).toBeNull();
  expect(tlsRoot.querySelector('div.flex')).toBeNull();

  // Storage & data: the storage union is the whole Primary storage section —
  // the segmented backend selector stays without a "Storage" heading or the
  // schema description.
  showCategory(view, 'Storage & data');
  expectSingleRootSection(
    'primary-storage',
    'storage',
    'Primary state store backend and connection.',
  );
  const storageRoot = document.querySelector<HTMLElement>(
    '[data-config-path="storage"]',
  );
  expect(
    within(storageRoot as HTMLElement).getByRole('radiogroup', {
      name: 'Storage backend',
    }),
  ).toBeDefined();

  // Scheduling: the recurring-jobs editor is the whole section.
  showCategory(view, 'Scheduling');
  expectSingleRootSection('recurring-jobs', 'scheduler.recurring_jobs');

  // Identity & access: the providers editor keeps "Add provider" untitled.
  // Heading suppression drops only the duplicate h5 — the token-handling
  // note stays visible under the section heading.
  showCategory(view, 'Identity & access');
  expectSingleRootSection('admin-auth-providers', 'admin.auth.providers');
  const providersSection = document.querySelector<HTMLElement>(
    '[data-config-section="admin-auth-providers"]',
  );
  expect(providersSection?.textContent).toContain(
    'Environment-backed tokens are referenced by name and never displayed.',
  );
  const providersRoot = document.querySelector<HTMLElement>(
    '[data-config-path="admin.auth.providers"]',
  );
  expect(
    within(providersRoot as HTMLElement).getByRole('button', {
      name: 'Add provider',
    }),
  ).toBeDefined();
  expectSingleRootSection('cluster-identity', 'cluster');

  // Pricing & quotas.
  showCategory(view, 'Pricing & quotas');
  expectSingleRootSection('price-catalog', 'price_catalog');

  // Runtime & observability: the observability object drops its heading and
  // schema description under the section h4.
  showCategory(view, 'Runtime & observability');
  expectSingleRootSection(
    'observability',
    'observability',
    'Tracing, telemetry, and log redaction.',
  );

  // Routing & resilience: the affinity section renders its leaf directly —
  // a scalar root keeps its own field label.
  showCategory(view, 'Routing & resilience');
  expect(
    within(
      document.querySelector(
        '[data-config-path="upstream_affinity.ttl_days"]',
      ) as HTMLElement,
    ).getByRole('spinbutton', { name: 'TTL' }),
  ).toBeDefined();

  // Multi-root sections keep per-root headings: the OAuth section renders
  // the nullable anthropic object and the github object side by side, each
  // with its own heading so the siblings stay distinguishable.
  showCategory(view, 'Identity & access');
  const oauthSection = document.querySelector<HTMLElement>(
    '[data-config-section="anthropic-oauth"]',
  );
  expect(oauthSection).not.toBeNull();
  expect(
    within(oauthSection as HTMLElement).getByRole('heading', {
      name: 'Anthropic',
    }),
  ).toBeDefined();
  expect(
    within(oauthSection as HTMLElement).getByRole('heading', {
      name: 'Github',
    }),
  ).toBeDefined();

  // Field labels are always kept — scalar roots still show their labels.
  showCategory(view, 'Network & requests');
  expect(screen.getByLabelText('Proxy address')).toBeDefined();
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

test('a deep link into a disabled TLS object focuses its hoisted enable switch', async () => {
  // listener.tls is null in the fixture, so no cert_path leaf renders — the
  // deep-link falls back to the listener.tls container, whose only control
  // is the Enabled switch hoisted into the TLS section header.
  setSettingsLoaded();
  routerMocks.search = {
    category: 'network',
    field: 'listener.tls.cert_path',
  };
  render(<SettingsComponent />);
  await flushEffects();

  expect(
    document.querySelector('[data-config-path="listener.tls.cert_path"]'),
  ).toBeNull();
  const tlsSwitch = document.querySelector<HTMLElement>(
    'input[data-config-path="listener.tls"]',
  );
  expect(tlsSwitch).not.toBeNull();
  expect(document.activeElement).toBe(tlsSwitch);
});

test('settings search leads the editor toolbar and overlays results without shifting the panel', async () => {
  setSettingsLoaded();
  const view = render(<SettingsComponent />);

  // The search control leads the editor card's toolbar — before the
  // save/validate/download actions — not a field inside the panel. It spans
  // the toolbar on mobile and a 256–320px column on larger screens.
  const editorCard = screen.getByTestId('config-editor-card');
  const search = screen.getByRole('combobox', { name: /search/i });
  expect(editorCard.contains(search)).toBe(true);
  const panel = screen.getByRole('region', { name: 'Network & requests' });
  expect(panel.contains(search)).toBe(false);
  const searchWrapper = search.parentElement as HTMLElement;
  const toolbar = screen.getByTestId('config-editor-toolbar');
  const actions = screen.getByTestId('config-editor-actions');
  expect(searchWrapper.parentElement).toBe(toolbar);
  expect(actions.parentElement).toBe(toolbar);
  expect(
    searchWrapper.compareDocumentPosition(actions) &
      Node.DOCUMENT_POSITION_FOLLOWING,
  ).toBeTruthy();
  expect(searchWrapper.className).toContain('w-full');
  expect(searchWrapper.className).toMatch(
    /(?:sm|md|lg|xl|2xl):w-(?:64|72|80|\[(?:2[5-9]\d|3[0-2]\d)px\])/,
  );
  // Draft, validate, and download actions share the toolbar.
  expect(
    within(actions).getByRole('button', { name: 'Save draft' }),
  ).toBeDefined();
  expect(
    within(actions).getByRole('button', { name: 'Validate' }),
  ).toBeDefined();
  expect(
    within(actions).getByRole('button', { name: 'Download TOML' }),
  ).toBeDefined();

  // Combobox semantics: the input autocompletes against a listbox popup.
  expect(search.getAttribute('aria-autocomplete')).toBe('list');
  expect(search.getAttribute('aria-haspopup')).toBe('listbox');

  // Closed state: the input advertises a collapsed overlay with no popup
  // relationship — aria-controls and the status region only exist while the
  // overlay is mounted.
  expect(search.getAttribute('aria-expanded')).toBe('false');
  expect(search.getAttribute('aria-controls')).toBeNull();
  expect(screen.queryByTestId('config-search-results')).toBeNull();
  expect(screen.queryByTestId('config-search-status')).toBeNull();

  // Typing stays local — no navigation — and the results render as an
  // absolute overlay anchored to the search wrapper, so the panel's
  // document flow and geometry never change.
  const panelHtml = panel.innerHTML;
  fireEvent.change(search, { target: { value: 'tracing' } });
  expect(routerMocks.navigate).not.toHaveBeenCalled();
  expect(panel.innerHTML).toBe(panelHtml);

  const results = screen.getByTestId('config-search-results');
  expect(results.getAttribute('role')).toBe('listbox');
  expect(results.className).toContain('absolute');
  expect(results.className).toContain('top-full');
  expect(searchWrapper.className).toContain('relative');
  expect(results.parentElement).toBe(searchWrapper);
  expect(search.getAttribute('aria-expanded')).toBe('true');
  expect(search.getAttribute('aria-controls')).toBe(results.id);
  expect(screen.getByTestId('config-search-status')).toBeDefined();

  // Selecting a result closes the overlay and keeps the existing
  // category/field/q URL navigation plus field focus.
  const option = within(results).getByRole('option', { name: /tracing/i });
  fireEvent.click(option);

  expect(screen.queryByTestId('config-search-results')).toBeNull();
  expect(routerMocks.search).toMatchObject({
    category: 'runtime',
    field: 'observability.tracing_level',
    q: 'tracing',
  });
  // Jumping to a result still keeps the page scroll: the route opts out of
  // scroll restoration and only the target field is centered and focused.
  expect(routerMocks.navigate).toHaveBeenCalledWith(
    expect.objectContaining({ resetScroll: false }),
  );
  view.rerender(<SettingsComponent />);
  await flushEffects();

  const field = document.querySelector<HTMLElement>(
    '[data-config-path="observability.tracing_level"]',
  );
  expect(field?.contains(document.activeElement)).toBe(true);
  expect(HTMLElement.prototype.scrollIntoView).toHaveBeenCalledWith(
    expect.objectContaining({ block: 'center' }),
  );
});

test('search overlay dismiss keeps the query while clear resets both', async () => {
  setSettingsLoaded();
  render(<SettingsComponent />);

  const search = screen.getByRole('combobox', {
    name: /search/i,
  }) as HTMLInputElement;
  fireEvent.change(search, { target: { value: 'tracing' } });
  const results = screen.getByTestId('config-search-results');

  // Escape is handled once on the wrapper: pressing it on a result option
  // closes the overlay and returns focus to the input — the query stays.
  const option = within(results).getByRole('option', { name: /tracing/i });
  fireEvent.keyDown(option, { key: 'Escape' });
  expect(screen.queryByTestId('config-search-results')).toBeNull();
  expect(search.value).toBe('tracing');
  expect(search.getAttribute('aria-expanded')).toBe('false');
  expect(document.activeElement).toBe(search);

  // Escape on the input itself takes the same path.
  fireEvent.focusIn(search);
  expect(screen.getByTestId('config-search-results')).toBeDefined();
  fireEvent.keyDown(search, { key: 'Escape' });
  expect(screen.queryByTestId('config-search-results')).toBeNull();
  expect(search.value).toBe('tracing');
  expect(document.activeElement).toBe(search);

  // Focus moving inside the wrapper keeps the overlay; focus leaving the
  // wrapper closes it while keeping the query.
  fireEvent.focusIn(search);
  const reopened = screen.getByTestId('config-search-results');
  const clearButton = screen.getByRole('button', { name: 'Clear search' });
  fireEvent.focusOut(search, { relatedTarget: clearButton });
  expect(screen.getByTestId('config-search-results')).toBe(reopened);
  fireEvent.focusOut(clearButton, { relatedTarget: document.body });
  expect(screen.queryByTestId('config-search-results')).toBeNull();
  expect(search.value).toBe('tracing');

  // An outside pointerdown dismisses the overlay the same way — query kept.
  fireEvent.focusIn(search);
  expect(screen.getByTestId('config-search-results')).toBeDefined();
  fireEvent.pointerDown(document.body);
  expect(screen.queryByTestId('config-search-results')).toBeNull();
  expect(search.value).toBe('tracing');

  // The clear button is a ≥32px target that empties the query, dismisses the
  // overlay, and returns focus to the input.
  fireEvent.focusIn(search);
  const clear = screen.getByRole('button', { name: 'Clear search' });
  expect(dimensionPx(clear, 'h')).toBeGreaterThanOrEqual(32);
  expect(dimensionPx(clear, 'w')).toBeGreaterThanOrEqual(32);
  fireEvent.click(clear);
  await flushEffects();
  expect(search.value).toBe('');
  expect(screen.queryByTestId('config-search-results')).toBeNull();
  expect(screen.queryByRole('button', { name: 'Clear search' })).toBeNull();
  expect(document.activeElement).toBe(search);
});

test('search combobox moves the active option with arrow keys while the input keeps focus', async () => {
  setSettingsLoaded();
  const view = render(<SettingsComponent />);
  const search = screen.getByRole('combobox', {
    name: /search/i,
  }) as HTMLInputElement;
  act(() => search.focus());
  fireEvent.change(search, { target: { value: 'pool' } });
  const results = screen.getByTestId('config-search-results');
  const options = within(results).getAllByRole('option');
  expect(options.length).toBeGreaterThan(2);
  const activeOption = () =>
    options.find((option) => option.getAttribute('aria-selected') === 'true');

  // No option is active until the operator arrows into the list.
  expect(search.getAttribute('aria-activedescendant')).toBeNull();
  expect(activeOption()).toBeUndefined();

  // ArrowDown activates the first option and scrolls it into view without
  // stealing focus from the input.
  fireEvent.keyDown(search, { key: 'ArrowDown' });
  expect(search.getAttribute('aria-activedescendant')).toBe(options[0]?.id);
  expect(activeOption()).toBe(options[0]);
  expect(document.activeElement).toBe(search);
  expect(HTMLElement.prototype.scrollIntoView).toHaveBeenCalledTimes(1);
  expect(HTMLElement.prototype.scrollIntoView).toHaveBeenCalledWith({
    block: 'nearest',
  });

  // Pointer hover only changes the highlighted option; it must not move the
  // results list's scroll position.
  fireEvent.mouseMove(options[2]);
  expect(activeOption()).toBe(options[2]);
  expect(HTMLElement.prototype.scrollIntoView).toHaveBeenCalledTimes(1);
  fireEvent.keyDown(search, { key: 'ArrowDown' });
  expect(search.getAttribute('aria-activedescendant')).toBe(options[3]?.id);
  expect(activeOption()).toBe(options[3]);
  expect(HTMLElement.prototype.scrollIntoView).toHaveBeenCalledTimes(2);

  // ArrowUp steps back from the current highlighted option. Hovering the
  // first option remains highlight-only; ArrowUp from there wraps to last.
  fireEvent.keyDown(search, { key: 'ArrowUp' });
  expect(search.getAttribute('aria-activedescendant')).toBe(options[2]?.id);
  fireEvent.mouseMove(options[0]);
  expect(activeOption()).toBe(options[0]);
  expect(HTMLElement.prototype.scrollIntoView).toHaveBeenCalledTimes(3);
  fireEvent.keyDown(search, { key: 'ArrowUp' });
  const last = options[options.length - 1];
  expect(search.getAttribute('aria-activedescendant')).toBe(last?.id);
  expect(activeOption()).toBe(last);

  // Home/End remain native text editing keys and must not be intercepted.
  for (const key of ['Home', 'End']) {
    const event = createEvent.keyDown(search, { key });
    fireEvent(search, event);
    expect(event.defaultPrevented).toBe(false);
  }
  for (const key of ['Home', 'End']) {
    const event = createEvent.keyDown(search, {
      key,
      shiftKey: true,
    });
    fireEvent(search, event);
    expect(event.defaultPrevented).toBe(false);
  }

  // A new query resets the active option — no stale activedescendant.
  fireEvent.change(search, { target: { value: 'max_connections' } });
  expect(search.getAttribute('aria-activedescendant')).toBeNull();
  const narrowed = within(
    screen.getByTestId('config-search-results'),
  ).getAllByRole('option');
  expect(narrowed.length).toBeGreaterThan(1);
  for (const option of narrowed) {
    expect(option.getAttribute('aria-selected')).toBe('false');
  }

  // Enter with no active option is a no-op — no navigation, overlay stays.
  fireEvent.keyDown(search, { key: 'Enter' });
  expect(routerMocks.navigate).not.toHaveBeenCalled();
  expect(screen.getByTestId('config-search-results')).toBeDefined();

  // Escape closes the overlay and clears the active option; ArrowDown
  // reopens it and starts again from the first option.
  fireEvent.keyDown(search, { key: 'Escape' });
  expect(screen.queryByTestId('config-search-results')).toBeNull();
  expect(search.getAttribute('aria-activedescendant')).toBeNull();
  fireEvent.keyDown(search, { key: 'ArrowDown' });
  const reopened = within(
    screen.getByTestId('config-search-results'),
  ).getAllByRole('option');
  expect(search.getAttribute('aria-activedescendant')).toBe(reopened[0]?.id);

  // Enter activates the active option exactly like a click: the overlay
  // closes, the URL gains category/field/q, and the field receives focus.
  fireEvent.keyDown(search, { key: 'Enter' });
  expect(screen.queryByTestId('config-search-results')).toBeNull();
  expect(routerMocks.search).toMatchObject({
    category: 'scheduling',
    field: 'scheduler.separate_pool.max_connections',
    q: 'max_connections',
  });
  expect(routerMocks.navigate).toHaveBeenCalledWith(
    expect.objectContaining({ resetScroll: false }),
  );
  view.rerender(<SettingsComponent />);
  await flushEffects();

  const field = document.querySelector<HTMLElement>(
    '[data-config-path="scheduler.separate_pool.max_connections"]',
  );
  expect(field?.contains(document.activeElement)).toBe(true);
});

test('search option pointer selection survives the input blur race', () => {
  setSettingsLoaded();
  render(<SettingsComponent />);

  const search = screen.getByRole('combobox', {
    name: /search/i,
  }) as HTMLInputElement;
  act(() => search.focus());
  fireEvent.change(search, { target: { value: 'tracing' } });
  const results = screen.getByTestId('config-search-results');
  const option = within(results).getByRole('option', { name: /tracing/i });

  // The option's mousedown prevents the native focus shift, so a real
  // pointer press never blurs the input before the click lands.
  const mouseDown = createEvent.mouseDown(option);
  fireEvent(option, mouseDown);
  expect(mouseDown.defaultPrevented).toBe(true);

  // Even if a blur still slips through — a focusout whose relatedTarget is
  // null means focus went nowhere, which only pointer presses produce — the
  // option must stay mounted so the click can activate it.
  fireEvent.focusOut(search, { relatedTarget: null });
  expect(screen.getByTestId('config-search-results')).toBe(results);
  expect(option.isConnected).toBe(true);

  fireEvent.click(option);
  expect(screen.queryByTestId('config-search-results')).toBeNull();
  expect(routerMocks.search).toMatchObject({
    category: 'runtime',
    field: 'observability.tracing_level',
    q: 'tracing',
  });
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
  // The history failure is an abnormal alert inside the status zone.
  const zone = screen.getByTestId('config-status-zone');
  expect(
    within(zone).getByText('Restart status may be incomplete'),
  ).toBeDefined();
  expect(
    within(zone).getByText(
      /cannot confirm whether a saved revision is still waiting/,
    ),
  ).toBeDefined();
  const retry = screen.getByRole('button', { name: 'Retry history' });
  fireEvent.click(retry);
  expect(history.refetch).toHaveBeenCalledTimes(1);
});

test('status zone gathers metadata, running facts, and only abnormal alerts', () => {
  setSettingsLoaded();
  render(<SettingsComponent />);

  // One compact zone above the editor card carries a single borderless
  // low-emphasis facts strip — metadata and running facts as inline
  // label/value pairs — plus the alerts that actually apply.
  const zone = screen.getByTestId('config-status-zone');
  const editorCard = screen.getByTestId('config-editor-card');
  expect(
    (zone.compareDocumentPosition(editorCard) &
      Node.DOCUMENT_POSITION_FOLLOWING) !==
      0,
  ).toBe(true);
  const metadata = within(zone).getByTestId('config-editor-metadata');
  expect(metadata.textContent).toContain('/etc/cc-lb/cc-lb.toml');
  expect(metadata.textContent).toContain('Revision');
  const summary = within(zone).getByTestId('config-running-summary');
  expect(summary.textContent).toContain('127.0.0.1:9090');
  expect(summary.textContent).toMatch(/postgres/i);
  // Metadata and running facts share one strip element directly inside the
  // zone — no panel chrome, no metadata/run divider, no reserved height,
  // no uppercase label styling.
  const strip = metadata.parentElement as HTMLElement;
  expect(strip.parentElement).toBe(zone);
  expect(strip.contains(summary)).toBe(true);
  expect(strip.className).not.toMatch(/(^|\s)(border|bg-|rounded|min-h-)/);
  expect(strip.querySelector('.border-t')).toBeNull();
  expect(strip.querySelector('.uppercase')).toBeNull();
  // The fixture saved after process start, so the pending-restart drift
  // banner is a real abnormal alert and lives in the zone. It is announced
  // once by that timestamped notice — there is no separate
  // "saved changes pending restart" badge copy.
  expect(within(zone).getByTestId('restart-drift-banner')).toBeDefined();
  expect(screen.queryByText(/saved changes pending restart/i)).toBeNull();
  // Unconditional restart explainers are gone: nothing claims a restart is
  // needed when none is pending, and the old static subtitle is removed.
  expect(screen.queryByText('Structured config editor')).toBeNull();
  expect(screen.queryByText('Startup-fixed configuration')).toBeNull();
  expect(screen.queryByText('Restart required after saving')).toBeNull();
  expect(
    screen.queryByText(/Every config-file change requires a (cc-lb )?restart/i),
  ).toBeNull();
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
  const input = screen.getByLabelText('Proxy address') as HTMLInputElement;
  expect(input.value).toBe('0.0.0.0:8080');
  expect(input.hasAttribute('disabled')).toBe(false);
  expect(field.textContent).toMatch(/Effective[: ]*127\.0\.0\.1:8181/);

  fireEvent.click(within(field).getByText('Details'));
  const details = within(field).getByTestId('config-value-details');
  // The details list provenance only — File, Default, Source — and never
  // repeats the Effective value already shown beside the control.
  expect(details.textContent).toMatch(/File[: ]*0\.0\.0\.0:8080/);
  expect(details.textContent).toMatch(/Default[: ]*\[::\]:8080/);
  expect(details.textContent).toMatch(/Source[: ]*Environment/);
  expect(details.textContent).toContain('CC_LB_PROXY_ADDR');
  expect(details.textContent).not.toMatch(/Effective/);
  // Provenance is informational, not a warning: the note under the control
  // uses neutral muted text, not amber/warn styling.
  const provenance = within(field).getByText(
    /Effective value comes from Environment/,
  );
  expect(provenance.className).toContain('text-text-muted');
  expect(provenance.className).not.toMatch(/amber|warn/);

  fireEvent.change(input, { target: { value: '0.0.0.0:8181' } });
  expect(input.value).toBe('0.0.0.0:8181');
  expect(field.textContent).toMatch(/Effective[: ]*127\.0\.0\.1:8181/);
  expect(
    within(screen.getByTestId('config-category-nav')).getByRole('button', {
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

test('admin providers render as divider-separated rows without nested boxes', () => {
  setSettingsLoaded();
  const view = render(<SettingsComponent />);
  showCategory(view, 'Identity & access');

  // The outer editor container is chromeless — no border, background, or
  // padding of its own; it only spans the section grid and stacks its rows.
  const providers = document.querySelector<HTMLElement>(
    '[data-config-path="admin.auth.providers"]',
  ) as HTMLElement;
  expect(providers).not.toBeNull();
  expect(providers.className).toContain('col-span-full');
  expect(providers.className).not.toMatch(/(^|\s)border/);
  expect(providers.className).not.toMatch(/(^|\s)bg-/);
  expect(providers.className).not.toMatch(/(^|\s)p[xytrbl]?-\d/);
  // The header row (note + Add provider) sits directly on the section canvas.
  expect(
    within(providers).getByRole('button', { name: 'Add provider' }),
  ).toBeDefined();
  expect(providers.textContent).toContain(
    'Environment-backed tokens are referenced by name and never displayed.',
  );

  // Providers are divider-separated rows on the editor canvas: no provider,
  // and nothing inside one, draws a box of its own.
  const provider = document.querySelector<HTMLElement>(
    '[data-config-path="admin.auth.providers[0]"]',
  ) as HTMLElement;
  expect(provider).not.toBeNull();
  const boxed = Array.from(
    providers.querySelectorAll<HTMLElement>('div[data-config-path]'),
  ).filter(
    (element) =>
      /(^|\s)border(\s|$)/.test(element.className) ||
      /(^|\s)bg-/.test(element.className),
  );
  expect(boxed).toEqual([]);
  const nestedFieldContainers = Array.from(
    provider.querySelectorAll<HTMLElement>('div[data-config-path]'),
  );
  expect(nestedFieldContainers.length).toBeGreaterThan(0);
  for (const field of nestedFieldContainers) {
    expect(field.hasAttribute('data-field-embedded')).toBe(true);
    expect(field.className).not.toMatch(/(^|\s)border/);
    expect(field.className).not.toMatch(/(^|\s)bg-/);
    expect(field.className).not.toMatch(/(^|\s)p[xytrbl]?-\d/);
  }
  // The leaf controls themselves still render and stay editable.
  expect(within(provider).getByLabelText('ID')).toBeDefined();
  // Embedded drops only the outer chrome — the leaf keeps its own field
  // actions (suppression is a separate contract owned by row headers). The
  // saved values are configured but unchanged, so only Unset shows until the
  // draft diverges from the file.
  const idField = provider.querySelector<HTMLElement>(
    '[data-config-path="admin.auth.providers[0].id"]',
  ) as HTMLElement;
  const tokenEnvField = provider.querySelector<HTMLElement>(
    '[data-config-path="admin.auth.providers[0].token_env"]',
  ) as HTMLElement;
  for (const field of [idField, tokenEnvField]) {
    expect(field).not.toBeNull();
    expect(field.hasAttribute('data-field-embedded')).toBe(true);
    expect(within(field).queryByRole('button', { name: 'Reset' })).toBeNull();
    expect(within(field).getByRole('button', { name: 'Unset' })).toBeDefined();
  }
  fireEvent.change(within(idField).getByLabelText('ID'), {
    target: { value: 'renamed' },
  });
  expect(within(idField).getByRole('button', { name: 'Reset' })).toBeDefined();
});

test('field actions appear only when they would change something', () => {
  setSettingsLoaded();
  render(<SettingsComponent />);

  // Saved and unchanged: nothing to reset, but the explicit value can be unset.
  const field = revealField('listener.admin_addr');
  expect(within(field).queryByRole('button', { name: 'Reset' })).toBeNull();
  expect(within(field).getByRole('button', { name: 'Unset' })).toBeDefined();

  // Unset: inherited from defaults, the saved value differs — Reset restores it.
  fireEvent.click(within(field).getByRole('button', { name: 'Unset' }));
  expect(within(field).queryByRole('button', { name: 'Unset' })).toBeNull();
  fireEvent.click(within(field).getByRole('button', { name: 'Reset' }));
  const input = within(field).getByLabelText(
    'Admin address',
  ) as HTMLInputElement;
  expect(input.value).toBe('127.0.0.1:9090');
  expect(within(field).queryByRole('button', { name: 'Reset' })).toBeNull();
});

test('an invalid address shows an inline error on blur that clears once valid', () => {
  setSettingsLoaded();
  render(<SettingsComponent />);

  const field = revealField('listener.admin_addr');
  const input = within(field).getByLabelText(
    'Admin address',
  ) as HTMLInputElement;
  fireEvent.change(input, { target: { value: 'not-an-address' } });
  // No error while still typing — the check runs on blur.
  expect(within(field).queryByText(/Enter host:port/)).toBeNull();
  fireEvent.blur(input);
  const message = within(field).getByText(/Enter host:port/);
  expect(message.className).toContain('text-danger-text');
  expect(input.getAttribute('aria-invalid')).toBe('true');
  expect(input.getAttribute('aria-describedby')).toBe(message.id);

  fireEvent.change(input, { target: { value: '127.0.0.1:9191' } });
  expect(within(field).queryByText(/Enter host:port/)).toBeNull();
  expect(input.getAttribute('aria-invalid')).toBe('false');

  // Unmodified neighbours carry no Reset action.
  const proxy = revealField('listener.proxy_addr');
  expect(within(proxy).queryByRole('button', { name: 'Reset' })).toBeNull();
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

  fireEvent.change(screen.getByLabelText('Proxy address'), {
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
  const urlInput = screen.getByLabelText('URL') as HTMLInputElement;
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
test('storage backend is a required two-option segmented selector, not a combobox', () => {
  const saveDraftMutate = vi.fn();
  queryMocks.useSaveDraft.mockReturnValue(mutationResult(saveDraftMutate));
  setSettingsLoaded();
  const view = render(<SettingsComponent />);
  showCategory(view, 'Storage & data');

  // The storage.kind discriminator is a segmented radiogroup inside the
  // storage union container — no native select or combobox stands in for it.
  const storage = document.querySelector<HTMLElement>(
    '[data-config-path="storage"]',
  ) as HTMLElement;
  const group = within(storage).getByRole('radiogroup', {
    name: 'Storage backend',
  });
  expect(group.getAttribute('data-config-path')).toBe('storage.kind');
  expect(within(storage).queryByRole('combobox')).toBeNull();
  const radios = within(group).getAllByRole('radio');
  expect(radios.map((radio) => radio.textContent)).toEqual([
    'SQLite',
    'PostgreSQL',
  ]);

  // The saved postgres draft selects PostgreSQL; the selection is required
  // and single — clicking the checked option never deselects it.
  const sqlite = within(group).getByRole('radio', { name: 'SQLite' });
  const postgres = within(group).getByRole('radio', { name: 'PostgreSQL' });
  expect(postgres.getAttribute('aria-checked')).toBe('true');
  expect(sqlite.getAttribute('aria-checked')).toBe('false');
  fireEvent.click(postgres);
  expect(postgres.getAttribute('aria-checked')).toBe('true');
  expect(
    document.querySelector('[data-config-path="storage.url"]'),
  ).not.toBeNull();
  expect(
    document.querySelector('[data-config-path="storage.path"]'),
  ).toBeNull();

  // Switching to SQLite cuts the draft over to { kind: 'sqlite' } and swaps
  // the rendered variant fields.
  fireEvent.click(sqlite);
  view.rerender(<SettingsComponent />);
  const storageAfter = document.querySelector<HTMLElement>(
    '[data-config-path="storage"]',
  ) as HTMLElement;
  const groupAfter = within(storageAfter).getByRole('radiogroup', {
    name: 'Storage backend',
  });
  expect(
    within(groupAfter)
      .getByRole('radio', { name: 'SQLite' })
      .getAttribute('aria-checked'),
  ).toBe('true');
  expect(
    within(groupAfter)
      .getByRole('radio', { name: 'PostgreSQL' })
      .getAttribute('aria-checked'),
  ).toBe('false');
  expect(
    document.querySelector('[data-config-path="storage.path"]'),
  ).not.toBeNull();
  // Only the selected variant's fields render — the postgres URL and pool
  // leaves are hidden under sqlite.
  expect(document.querySelector('[data-config-path="storage.url"]')).toBeNull();
  expect(
    document.querySelector('[data-config-path="storage.pool.max_connections"]'),
  ).toBeNull();

  fireEvent.click(screen.getByRole('button', { name: 'Save draft' }));
  const request = saveDraftMutate.mock.calls[0]?.[0] as {
    draft: { storage: Record<string, unknown> };
  };
  expect(request.draft.storage).toEqual({ kind: 'sqlite' });
});

test('switching storage backend clears a pending URL replacement', () => {
  setSettingsLoaded();
  const view = render(<SettingsComponent />);
  showCategory(view, 'Storage & data');

  // Start a replacement on the postgres variant, then switch away and back:
  // the variant cutover drops the pending replacement, so the URL field
  // returns to its opaque badge state instead of the password input.
  fireEvent.click(screen.getByRole('button', { name: 'Replace URL' }));
  const urlInput = screen.getByLabelText('URL') as HTMLInputElement;
  fireEvent.change(urlInput, {
    target: { value: 'postgres://new-secret@db/cc_lb' },
  });
  expect(urlInput.value).toBe('postgres://new-secret@db/cc_lb');

  const group = screen.getByRole('radiogroup', { name: 'Storage backend' });
  fireEvent.click(within(group).getByRole('radio', { name: 'SQLite' }));
  view.rerender(<SettingsComponent />);
  const groupAfter = screen.getByRole('radiogroup', {
    name: 'Storage backend',
  });
  fireEvent.click(
    within(groupAfter).getByRole('radio', { name: 'PostgreSQL' }),
  );
  view.rerender(<SettingsComponent />);

  const urlField = document.querySelector<HTMLElement>(
    '[data-config-path="storage.url"]',
  ) as HTMLElement;
  expect(within(urlField).getByText('No stored URL')).toBeDefined();
  expect(
    within(urlField).getByRole('button', { name: 'Replace URL' }),
  ).toBeDefined();
  expect(urlField.querySelector('input')).toBeNull();
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

  // The read-only state is a status line under the config file path in the
  // Draft facts; the reason is visible text beside it — readable by keyboard
  // and touch, not hover-only.
  const zone = screen.getByTestId('config-status-zone');
  const draftFacts = within(zone).getByTestId('config-editor-metadata');
  expect(within(draftFacts).getByText('Read-only')).toBeDefined();
  expect(
    within(draftFacts).getByText('Bind mount is read-only.'),
  ).toBeDefined();
  expect(screen.queryByTitle(/Bind mount is read-only/)).toBeNull();
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

  const draftFacts = within(
    screen.getByTestId('config-status-zone'),
  ).getByTestId('config-editor-metadata');
  expect(within(draftFacts).getByText('Missing')).toBeDefined();
  expect(
    within(draftFacts).getByText('Saving will create this file.'),
  ).toBeDefined();
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

  const zone = screen.getByTestId('config-status-zone');
  const draftFacts = within(zone).getByTestId('config-editor-metadata');
  expect(within(draftFacts).getByText('Missing and read-only')).toBeDefined();
  expect(
    within(draftFacts).getByText('Parent directory is not writable.'),
  ).toBeDefined();
  expect(screen.queryByTitle(/Parent directory is not writable/)).toBeNull();
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
  // The editor is memoized, so a bare parent rerender no longer reaches it;
  // in the app the mutation hook's own subscription re-renders it. Change a
  // prop (the URL search) to make the mocked hook value observable here.
  routerMocks.search = { category: 'network' };
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

  // restart_required is always true server-side, so the old unconditional
  // notice is gone; the drift banner is the real pending-restart signal and
  // lives in the status zone.
  const zone = screen.getByTestId('config-status-zone');
  expect(screen.queryByText('Restart required after saving')).toBeNull();
  const downloadButton = screen.getByRole('button', { name: 'Download TOML' });
  fireEvent.click(downloadButton);
  expect(downloadButton.getAttribute('aria-busy')).toBe('true');
  expect(within(zone).getByTestId('restart-drift-banner')).toBeDefined();
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

test('numeric fields surface schema bounds as a hint in the details', () => {
  setSettingsLoaded();
  const view = render(<SettingsComponent />);

  const cap = revealField('body.messages_cap_bytes');
  expect(cap.textContent).toMatch(/Minimum 1\./);

  const address = revealField('listener.proxy_addr');
  expect(address.textContent).not.toMatch(/Minimum|Maximum|Allowed range/);

  showCategory(view, 'Storage & data');
  const maxConnections = revealField('storage.pool.max_connections');
  expect(maxConnections.textContent).toMatch(/Allowed range 1–64\./);

  const capacity = revealField('event_bus.broadcast_capacity');
  expect(capacity.textContent).not.toMatch(/Minimum|Maximum|Allowed range/);
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
          canary_flag: { type: 'boolean' },
        },
      },
    },
  };
  const file = {
    ...structuredClone(fileConfig),
    event_bus: {
      broadcast_capacity: 4096,
      retry_attempts: 3,
      canary_flag: true,
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
  // The unassigned section is flattened like every other section: space
  // above it, no card chrome, and no amber warning border — the warning
  // lives in a badge/text instead. As a non-first sibling it is separated by
  // space; the panel's first section is flush.
  expect(sectionTopSeparator(dataOther as HTMLElement)).toBe('space');
  const dataSections = Array.from(
    dataPanel?.querySelectorAll<HTMLElement>('[data-config-section]') ?? [],
  );
  expect(dataSections[0]).not.toBe(dataOther);
  expect(sectionTopSeparator(dataSections[0] as HTMLElement)).toBe('none');
  expect(dataOther?.className).not.toMatch(/amber/);
  expect(dataOther?.className).not.toMatch(/(^|\s)rounded/);
  expect(dataOther?.className).not.toMatch(/(^|\s)bg-/);
  expect(dataOther?.innerHTML).toMatch(/text-warn-text/);
  // Schema-known and unknown unassigned leaves get distinct explanations.
  expect(dataOther?.textContent).toMatch(/not covered by a settings section/i);
  expect(dataOther?.textContent).toMatch(/not recognized by the schema/i);
  // An unknown root key belongs to the fallback category, not this one.
  expect(
    dataOther?.querySelector('[data-config-path="legacy_mode"]'),
  ).toBeNull();
  // A schema-known boolean leaf without path-specific guidance renders a
  // switch with no tautological On/Off effect rows.
  const canary = dataOther?.querySelector<HTMLElement>(
    '[data-config-path="event_bus.canary_flag"]',
  );
  expect(canary).not.toBeNull();
  expect(within(canary as HTMLElement).getByRole('checkbox')).toBeDefined();
  expect(within(canary as HTMLElement).queryByText('On')).toBeNull();
  expect(within(canary as HTMLElement).queryByText('Off')).toBeNull();
  const canaryGuidance = resolveConfigFieldGuidance(
    'event_bus.canary_flag',
    undefined,
    'boolean',
  );
  expect(canaryGuidance.enabled).toBeFalsy();
  expect(canaryGuidance.disabled).toBeFalsy();
  // The modified unassigned leaf counts toward its own category.
  expect(
    within(screen.getByTestId('config-category-nav')).getByRole('button', {
      name: /^Storage & data.*1 modified/,
    }),
  ).toBeDefined();

  const search = screen.getByRole('combobox', { name: /search/i });
  fireEvent.change(search, { target: { value: 'retry_attempts' } });
  const results = screen.getByTestId('config-search-results');
  fireEvent.click(
    within(results).getByRole('option', { name: /retry attempts/i }),
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

test('search results are listbox options announced through a status region', () => {
  setSettingsLoaded();
  render(<SettingsComponent />);

  const search = screen.getByRole('combobox', { name: /search/i });
  fireEvent.change(search, { target: { value: 'tracing' } });

  // The overlay is a real listbox: each result is an option, and the empty
  // state is a disabled option rather than a dead-end message.
  const results = screen.getByTestId('config-search-results');
  expect(results.getAttribute('role')).toBe('listbox');
  const options = within(results).getAllByRole('option');
  expect(options.length).toBeGreaterThan(0);
  for (const option of options) {
    expect(option.getAttribute('role')).toBe('option');
    expect(option.getAttribute('aria-selected')).toBe('false');
    // Options are reached through the combobox's activedescendant, never
    // through Tab — they stay out of the tab order.
    expect(option.tabIndex).toBe(-1);
  }
  const status = screen.getByTestId('config-search-status');
  expect(status.getAttribute('role')).toBe('status');
  expect(status.className).toMatch(/sr-only/);
  expect(status.textContent).toMatch(/\d+ settings? match/i);

  fireEvent.change(search, { target: { value: 'zzz-no-such-setting' } });
  expect(screen.getByTestId('config-search-status').textContent).toMatch(
    /no settings match/i,
  );
  const emptyOption = within(
    screen.getByTestId('config-search-results'),
  ).getByRole('option');
  expect(emptyOption.getAttribute('aria-disabled')).toBe('true');
});

test('field details is a quiet chevron disclosure without the effective value', () => {
  setSettingsLoaded();
  render(<SettingsComponent />);

  const field = revealField('listener.proxy_addr');
  const details = within(field).getByTestId('config-value-details');
  const summary = details.querySelector('summary');
  expect(summary).not.toBeNull();
  expect(summary?.textContent).toMatch(/^details$/i);
  expect(minHeightPx(summary as HTMLElement)).toBeGreaterThanOrEqual(44);
  // Quiet affordance: the summary hugs its label instead of spanning the
  // field, and carries no button chrome (border, background, or padding).
  expect(summary?.className).toContain('w-fit');
  expect(summary?.className).not.toMatch(/(^|\s)(border|bg-|p[xy]?-\d)/);
  // A chevron marks the disclosure and carries the open-state rotation class.
  const chevron = summary?.querySelector('svg');
  expect(chevron).not.toBeNull();
  fireEvent.click(summary as HTMLElement);
  expect(chevron?.getAttribute('class')).toContain('rotate-90');
  // Provenance (file, default, source) and the raw key live inside; the
  // Effective value already visible beside the control is not repeated.
  for (const label of ['File', 'Default', 'Source', 'Key']) {
    expect(within(details).getByText(label).tagName).toBe('DT');
  }
  expect(within(details).getByText('listener.proxy_addr')).toBeDefined();
  expect(details.textContent).not.toMatch(/Effective/);
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
    within(screen.getByTestId('config-category-nav')).getByRole('button', {
      name: /^Storage & data.*2 modified/,
    }),
  ).toBeDefined();

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

test('recurring jobs render one flat card per key with the enabled switch in the header', () => {
  setSettingsLoaded();
  const view = render(<SettingsComponent />);
  showCategory(view, 'Scheduling');

  const section = document.querySelector<HTMLElement>(
    '[data-config-section="recurring-jobs"]',
  );
  expect(section).not.toBeNull();
  // The section card already owns the "Recurring jobs" heading and
  // description; the editor must not repeat them inside a nested card.
  expect(
    within(section as HTMLElement).getAllByRole('heading', {
      name: /recurring jobs/i,
    }),
  ).toHaveLength(1);
  expect(section?.textContent).not.toMatch(/unknown file keys/i);

  const editor = document.querySelector<HTMLElement>(
    '[data-config-path="scheduler.recurring_jobs"]',
  );
  expect(editor).not.toBeNull();
  // Flattened: the editor sits directly in the section grid and spans it,
  // separating jobs with dividers rather than a surface of its own.
  expect(editor?.parentElement?.className).toContain('grid');
  expect(editor?.className).toContain('col-span-full');
  expect(editor?.className).not.toMatch(/rounded|bg-/);

  // Exactly one job card per configured key — no nested job containers.
  const jobCards = [...(editor?.querySelectorAll('[data-config-path]') ?? [])]
    .map((element) => element.getAttribute('data-config-path') ?? '')
    .filter((path) => /^scheduler\.recurring_jobs\.[^.[\]]+$/.test(path));
  expect(jobCards.sort()).toEqual([
    'scheduler.recurring_jobs.custom_job',
    'scheduler.recurring_jobs.usage_rollup',
  ]);

  const job = document.querySelector<HTMLElement>(
    '[data-config-path="scheduler.recurring_jobs.usage_rollup"]',
  ) as HTMLElement;
  // Built-in jobs get the catalog label and purpose next to the raw key.
  expect(job.textContent).toContain('usage_rollup');
  expect(job.textContent).toContain(
    recurringJobMetadata('usage_rollup')?.label ?? 'missing label',
  );
  expect(job.textContent).toContain(
    recurringJobMetadata('usage_rollup')?.purpose ?? 'missing purpose',
  );

  // Enabled is a compact switch in the job header — before the body fields —
  // not a standalone field card in the body grid.
  const enabled = job.querySelector<HTMLElement>(
    '[data-config-path="scheduler.recurring_jobs.usage_rollup.enabled"]',
  );
  expect(enabled).not.toBeNull();
  expect(enabled?.tagName).toBe('INPUT');
  expect(enabled?.hasAttribute('data-field-control')).toBe(true);
  expect(enabled?.closest('[data-field-embedded]')).toBeNull();
  // WAI switch label stability: the accessible name stays "Enabled" in both
  // states. The header switch carries no description sentence, so the compact
  // header cluster keeps a small tap target and cannot overflow on mobile.
  const enabledLabel = enabled?.closest('label');
  const enabledGuidance = resolveConfigFieldGuidance(
    'scheduler.recurring_jobs.usage_rollup.enabled',
    undefined,
    'boolean',
  );
  expect(enabledLabel?.textContent).toBe('Enabled');
  expect(enabledLabel?.textContent).not.toContain(enabledGuidance.description);
  const actionsCluster = enabledLabel?.parentElement;
  const headerRow = actionsCluster?.parentElement;
  expect(actionsCluster?.className).toContain('flex-wrap');
  expect(headerRow?.className).toContain('flex-wrap');
  expect(headerRow?.className).toContain('min-w-0');
  expect(headerRow?.firstElementChild?.className).toContain('min-w-0');
  expect(within(job).getByRole('checkbox', { name: 'Enabled' })).toBe(enabled);
  fireEvent.click(enabled as HTMLElement);
  const toggled = job.querySelector<HTMLInputElement>(
    '[data-config-path="scheduler.recurring_jobs.usage_rollup.enabled"]',
  );
  expect(toggled?.checked).toBe(false);
  expect(within(job).getByRole('checkbox', { name: 'Enabled' })).toBe(toggled);
  expect(within(job).queryByRole('checkbox', { name: 'Disabled' })).toBeNull();

  const interval = job.querySelector<HTMLElement>(
    '[data-config-path="scheduler.recurring_jobs.usage_rollup.interval_secs"]',
  );
  const jitter = job.querySelector<HTMLElement>(
    '[data-config-path="scheduler.recurring_jobs.usage_rollup.jitter_secs"]',
  );
  expect(interval).not.toBeNull();
  expect(jitter).not.toBeNull();
  // Interval and jitter are embedded siblings in one compact two-column body.
  expect(interval?.hasAttribute('data-field-embedded')).toBe(true);
  expect(jitter?.hasAttribute('data-field-embedded')).toBe(true);
  const body = interval?.parentElement;
  expect(body).toBe(jitter?.parentElement);
  expect(body?.className).toContain('sm:grid-cols-2');
  expect(body?.contains(enabled as Node)).toBe(false);
  expect(
    (enabled?.compareDocumentPosition(interval as Node) ?? 0) &
      Node.DOCUMENT_POSITION_FOLLOWING,
  ).toBeTruthy();
  // Embedded fields keep their guidance: purpose plus Lower/Higher trade-offs.
  expect(within(interval as HTMLElement).getByText('Lower')).toBeDefined();
  expect(within(interval as HTMLElement).getByText('Higher')).toBeDefined();
  // The job header owns Reset/Unset for the whole job — embedded leaves
  // suppress their own copies so the actions never duplicate. The toggle above
  // diverged the draft, so Reset shows alongside Unset.
  expect(within(job).getAllByRole('button', { name: 'Reset' })).toHaveLength(1);
  expect(within(job).getAllByRole('button', { name: 'Unset' })).toHaveLength(1);
  for (const field of [interval, jitter]) {
    expect(
      within(field as HTMLElement).queryByRole('button', { name: 'Reset' }),
    ).toBeNull();
    expect(
      within(field as HTMLElement).queryByRole('button', { name: 'Unset' }),
    ).toBeNull();
  }
  // Reset restores the saved job; with nothing left to reset only Unset stays.
  fireEvent.click(within(job).getByRole('button', { name: 'Reset' }));
  expect(within(job).queryByRole('button', { name: 'Reset' })).toBeNull();
  expect(within(job).getByRole('button', { name: 'Unset' })).toBeDefined();
  expect(
    job.querySelector<HTMLInputElement>(
      '[data-config-path="scheduler.recurring_jobs.usage_rollup.enabled"]',
    )?.checked,
  ).toBe(true);
  // Unset leaves nothing explicit to unset.
  fireEvent.click(within(job).getByRole('button', { name: 'Unset' }));
  expect(within(job).queryByRole('button', { name: 'Unset' })).toBeNull();
  expect(within(job).getByRole('button', { name: 'Reset' })).toBeDefined();
  // The enabled switch is self-explanatory: no On/Off effect rows repeat the
  // obvious enqueue/stop outcome anywhere in the job card.
  expect(enabledGuidance.enabled).toBeFalsy();
  expect(enabledGuidance.disabled).toBeFalsy();
  expect(within(job).queryByText('On')).toBeNull();
  expect(within(job).queryByText('Off')).toBeNull();

  // Unknown keys stay visible and marked, with their fields still editable.
  const unknown = document.querySelector<HTMLElement>(
    '[data-config-path="scheduler.recurring_jobs.custom_job"]',
  ) as HTMLElement;
  expect(unknown).not.toBeNull();
  expect(unknown.textContent).toContain('custom_job');
  expect(unknown.textContent).toContain('Unknown key');
  // The job-level guidance explains why the key is flagged and recommends the
  // fix in the row body.
  const unknownGuidance = resolveConfigFieldGuidance(
    'scheduler.recurring_jobs.custom_job',
  );
  expect(unknown.textContent).toContain(unknownGuidance.description);
  expect(unknownGuidance.recommendation).toBeTruthy();
  expect(within(unknown).getByText('Recommendation')).toBeDefined();
  expect(unknown.textContent).toContain(
    unknownGuidance.recommendation as string,
  );
  expect(
    unknown.querySelector(
      '[data-config-path="scheduler.recurring_jobs.custom_job.enabled"]',
    ),
  ).not.toBeNull();
  expect(
    unknown
      .querySelector(
        '[data-config-path="scheduler.recurring_jobs.custom_job.interval_secs"]',
      )
      ?.hasAttribute('data-field-embedded'),
  ).toBe(true);
});

test('recurring job unknown badge follows defaults, not the static catalog', () => {
  // A job present in default_config but missing from the static metadata
  // catalog is still a known job: the catalog only enriches label and
  // purpose. Only keys with no default (file-only entries like custom_job)
  // are flagged unknown.
  const defaults = structuredClone(defaultConfig);
  const defaultJobs = defaults.scheduler.recurring_jobs as Record<
    string,
    unknown
  >;
  defaultJobs.legacy_sweep = {
    enabled: true,
    interval_secs: 120,
    jitter_secs: 10,
  };
  expect(recurringJobMetadata('legacy_sweep')).toBeNull();
  setSettingsLoaded(editorResponse({ default_config: defaults }));
  const view = render(<SettingsComponent />);
  showCategory(view, 'Scheduling');

  const legacy = document.querySelector<HTMLElement>(
    '[data-config-path="scheduler.recurring_jobs.legacy_sweep"]',
  );
  expect(legacy).not.toBeNull();
  expect(legacy?.textContent).toContain('legacy_sweep');
  expect(legacy?.textContent).not.toContain('Unknown key');

  const custom = document.querySelector<HTMLElement>(
    '[data-config-path="scheduler.recurring_jobs.custom_job"]',
  );
  expect(custom?.textContent).toContain('Unknown key');
});

test('scalar fields show purpose and trade-off guidance across every category', () => {
  setSettingsLoaded();
  const view = render(<SettingsComponent />);

  const expectGuidance = (
    path: string,
    kind: 'integer' | 'boolean' | 'string',
    labels: readonly string[],
    guidanceKeys: readonly (
      | 'description'
      | 'lower'
      | 'higher'
      | 'enabled'
      | 'disabled'
      | 'recommendation'
    )[],
  ) => {
    const field = document.querySelector<HTMLElement>(
      `[data-config-path="${path}"]`,
    );
    expect(field, `${path} to render`).not.toBeNull();
    const guidance = resolveConfigFieldGuidance(path, undefined, kind);
    // The purpose is visible text, not hidden behind a title tooltip.
    expect(
      within(field as HTMLElement).getByText(guidance.description),
      `${path} purpose`,
    ).toBeDefined();
    for (const label of labels) {
      expect(
        within(field as HTMLElement).getByText(label),
        `${path} shows ${label}`,
      ).toBeDefined();
    }
    for (const key of guidanceKeys) {
      const text = guidance[key];
      expect(text, `${path} guidance.${key}`).toBeTruthy();
      expect(field?.textContent, `${path} renders guidance.${key}`).toContain(
        text,
      );
    }
  };

  // Network & requests — numeric bytes field.
  expectGuidance(
    'body.messages_cap_bytes',
    'integer',
    ['Lower', 'Higher'],
    ['lower', 'higher'],
  );

  // Storage & data — numeric pool field.
  showCategory(view, 'Storage & data');
  expectGuidance(
    'storage.pool.max_connections',
    'integer',
    ['Lower', 'Higher'],
    ['lower', 'higher'],
  );

  // Scheduling — numeric retention field.
  showCategory(view, 'Scheduling');
  expectGuidance(
    'scheduler.dlq_retention_days',
    'integer',
    ['Lower', 'Higher'],
    ['lower', 'higher'],
  );

  // Routing & resilience — numeric threshold field.
  showCategory(view, 'Routing & resilience');
  expectGuidance(
    'circuit_breaker.failures_to_open',
    'integer',
    ['Lower', 'Higher'],
    ['lower', 'higher'],
  );

  // Identity & access — env-var string field with a recommendation.
  showCategory(view, 'Identity & access');
  expectGuidance(
    'cluster.token_env',
    'string',
    ['Recommendation'],
    ['recommendation'],
  );

  // Pricing & quotas — numeric batch field.
  showCategory(view, 'Pricing & quotas');
  expectGuidance(
    'subscription_quota.writer_batch_max_records',
    'integer',
    ['Lower', 'Higher'],
    ['lower', 'higher'],
  );

  // Runtime & observability — booleans with asymmetric operational risk keep
  // meaningful On/Off effects; generic booleans no longer carry them.
  showCategory(view, 'Runtime & observability');
  expectGuidance(
    'observability.log_redaction',
    'boolean',
    ['On', 'Off'],
    ['enabled', 'disabled'],
  );
  expectGuidance(
    'runtime.wasmtime.cookie_redaction',
    'boolean',
    ['On', 'Off'],
    ['enabled', 'disabled'],
  );
});

test('section grids use the 1/2/3-column contract with full-span composite editors', () => {
  setSettingsLoaded();
  const view = render(<SettingsComponent />);

  const expectGridContract = (
    grid: Element | null | undefined,
    name: string,
  ) => {
    expect(grid, `${name} grid`).not.toBeNull();
    expect(grid?.className, name).toContain('grid-cols-1');
    expect(grid?.className, name).toContain('md:grid-cols-2');
    expect(grid?.className, name).toContain('2xl:grid-cols-3');
    expect(grid?.className, name).not.toContain('lg:grid-cols-2');
  };

  // Network: scalar fields sit in the responsive grid; the optional TLS
  // object toggle is a composite and spans every column.
  const proxyAddr = document.querySelector<HTMLElement>(
    '[data-config-path="listener.proxy_addr"]',
  );
  const networkGrid = proxyAddr?.parentElement;
  expectGridContract(networkGrid, 'listener-endpoints');
  expect(proxyAddr?.className).not.toContain('col-span-full');
  // The optional TLS object is a composite: its root container spans the
  // section grid even while the Enabled switch is hoisted into the section
  // header (the switch input carries the same data-config-path, so the
  // container is matched by tag).
  const tlsSection = document.querySelector<HTMLElement>(
    '[data-config-section="tls"]',
  );
  const tlsToggle = tlsSection?.querySelector<HTMLElement>(
    'div[data-config-path="listener.tls"]',
  );
  expect(tlsToggle).not.toBeNull();
  expect(tlsToggle?.className).toContain('col-span-full');
  expect(tlsToggle?.parentElement?.className).toContain('grid');

  // The category nav is inline at every viewport and never becomes a side
  // column, so sections keep their full width — including the 1024px tablet
  // case where md:grid-cols-2 leaves controls comfortably reachable.
  const nav = screen.getByTestId('config-category-nav');
  const navLayout = nav.parentElement;
  expect(navLayout?.className).not.toContain('xl:flex');
  expect(navLayout?.className).not.toContain('lg:flex');
  expect(navLayout?.className).not.toContain('md:flex');

  // Storage & data: the tagged-union storage editor spans the grid.
  showCategory(view, 'Storage & data');
  const storage = document.querySelector<HTMLElement>(
    '[data-config-path="storage"]',
  );
  expect(storage?.className).toContain('col-span-full');
  expect(storage?.parentElement?.className).toContain('grid');
  const maxConnections = document.querySelector<HTMLElement>(
    '[data-config-path="storage.pool.max_connections"]',
  );
  expectGridContract(maxConnections?.parentElement, 'database-pool');

  // Scheduling: the recurring jobs editor is a full-span composite.
  showCategory(view, 'Scheduling');
  const jobs = document.querySelector<HTMLElement>(
    '[data-config-path="scheduler.recurring_jobs"]',
  );
  expect(jobs?.className).toContain('col-span-full');
  expect(jobs?.parentElement?.className).toContain('grid');

  // Identity & access: admin providers and the nested cluster object span
  // the grid; the nested object's inner grid follows the same contract.
  showCategory(view, 'Identity & access');
  const providers = document.querySelector<HTMLElement>(
    '[data-config-path="admin.auth.providers"]',
  );
  expect(providers?.className).toContain('col-span-full');
  const cluster = document.querySelector<HTMLElement>(
    '[data-config-path="cluster"]',
  );
  expect(cluster?.className).toContain('col-span-full');
  const clusterChild = document.querySelector<HTMLElement>(
    '[data-config-path="cluster.token_env"]',
  );
  expectGridContract(clusterChild?.parentElement, 'cluster');
});

test('nullable objects keep one compact switch in both enabled and disabled states', () => {
  setSettingsLoaded();
  const view = render(<SettingsComponent />);

  // Disabled: listener.tls is null — the object control is a switch, not an
  // enable-only affordance. The single-root section hoists it into the
  // section header row, so it lives on the section, not inside the root.
  const tlsSection = document.querySelector<HTMLElement>(
    '[data-config-section="tls"]',
  ) as HTMLElement;
  const tls = document.querySelector<HTMLElement>(
    '[data-config-path="listener.tls"]',
  );
  expect(tls).not.toBeNull();
  const tlsSwitch = within(tlsSection).getByRole('checkbox', {
    name: 'Enabled',
  });
  expect(tlsSwitch.hasAttribute('data-field-control')).toBe(true);
  expect((tlsSwitch as HTMLInputElement).checked).toBe(false);
  expect(
    within(tlsSection).queryByRole('button', { name: /disable/i }),
  ).toBeNull();
  expect(
    document.querySelector('[data-config-path="listener.tls.cert_path"]'),
  ).toBeNull();

  // The compact switch is a bare inline track — no card chrome and no top
  // offset — so it centers on the header row instead of aligning to a card.
  const tlsTrack = tlsSwitch
    .closest('label')
    ?.querySelector<HTMLElement>('span.relative');
  expect(tlsTrack?.className).toContain('h-5');
  expect(tlsTrack?.className).toContain('w-9');
  expect(tlsTrack?.className).not.toMatch(/(^|\s)(mt|pt|top)-/);
  const tlsLabel = tlsSwitch.closest('label');
  expect(tlsLabel?.className).not.toMatch(/(^|\s)border/);
  expect(tlsLabel?.className).not.toMatch(/(^|\s)bg-/);

  // Switching on creates the object in place — the same switch stays put and
  // now reads checked; no Disable button appears.
  fireEvent.click(tlsSwitch);
  view.rerender(<SettingsComponent />);
  const tlsSectionOn = document.querySelector<HTMLElement>(
    '[data-config-section="tls"]',
  ) as HTMLElement;
  const tlsSwitchOn = within(tlsSectionOn).getByRole('checkbox', {
    name: 'Enabled',
  }) as HTMLInputElement;
  expect(tlsSwitchOn.checked).toBe(true);
  expect(
    within(tlsSectionOn).queryByRole('button', { name: /disable/i }),
  ).toBeNull();
  expect(
    document.querySelector('[data-config-path="listener.tls.cert_path"]'),
  ).not.toBeNull();

  // Switching back off unsets the object — the same control, same position.
  fireEvent.click(tlsSwitchOn);
  view.rerender(<SettingsComponent />);
  const tlsSectionOff = document.querySelector<HTMLElement>(
    '[data-config-section="tls"]',
  ) as HTMLElement;
  expect(
    (
      within(tlsSectionOff).getByRole('checkbox', {
        name: 'Enabled',
      }) as HTMLInputElement
    ).checked,
  ).toBe(false);
  expect(
    document.querySelector('[data-config-path="listener.tls.cert_path"]'),
  ).toBeNull();

  // oauth.anthropic follows the same contract in Identity & access. As a
  // multi-root sibling it keeps its own header: the h5/description block and
  // the switch share one items-center row inside the root.
  showCategory(view, 'Identity & access');
  const oauth = document.querySelector<HTMLElement>(
    '[data-config-path="oauth.anthropic"]',
  );
  expect(oauth).not.toBeNull();
  const oauthSwitch = within(oauth as HTMLElement).getByRole('checkbox', {
    name: 'Enabled',
  });
  expect((oauthSwitch as HTMLInputElement).checked).toBe(false);
  const oauthHeaderRow = oauthSwitch.closest('div.flex') as HTMLElement;
  expect(oauthHeaderRow.className).toContain('items-center');
  expect(
    within(oauthHeaderRow).getByRole('heading', { name: 'Anthropic' }),
  ).toBeDefined();
  expect(oauthHeaderRow.parentElement).toBe(oauth);
  expect(
    within(oauth as HTMLElement).queryByRole('button', { name: /disable/i }),
  ).toBeNull();
  fireEvent.click(oauthSwitch);
  view.rerender(<SettingsComponent />);
  const oauthOn = document.querySelector<HTMLElement>(
    '[data-config-path="oauth.anthropic"]',
  );
  expect(oauthOn).not.toBeNull();
  expect(
    (
      within(oauthOn as HTMLElement).getByRole('checkbox', {
        name: 'Enabled',
      }) as HTMLInputElement
    ).checked,
  ).toBe(true);
  expect(
    within(oauthOn as HTMLElement).queryByRole('button', { name: /disable/i }),
  ).toBeNull();
});

test('field errors mark only the failing control and risk is flagged by a badge', () => {
  const issueReport = {
    ...validReport,
    file: {
      valid: false,
      issues: [
        {
          path: 'body.messages_cap_bytes',
          code: 'too_small',
          message: 'Cap is below the minimum.',
          severity: 'error' as const,
        },
      ],
    },
  };
  setSettingsLoaded(
    editorResponse({ last_validation: issueReport }),
    draftResponse({ last_validation: issueReport }),
  );
  render(<SettingsComponent />);

  // Only the field with a validation error marks its control invalid and
  // shows the issue text.
  const cap = document.querySelector<HTMLElement>(
    '[data-config-path="body.messages_cap_bytes"]',
  );
  expect(cap?.querySelector('input')?.getAttribute('aria-invalid')).toBe(
    'true',
  );
  expect(
    within(cap as HTMLElement).getByText('Cap is below the minimum.'),
  ).toBeDefined();

  // Dangerous fields carry a risk badge and impact text; they are not
  // marked invalid.
  const proxy = document.querySelector<HTMLElement>(
    '[data-config-path="listener.proxy_addr"]',
  );
  expect(proxy?.querySelector('input')?.getAttribute('aria-invalid')).toBe(
    'false',
  );
  expect(
    within(proxy as HTMLElement).getByText(/operational risk/i),
  ).toBeDefined();
  expect(
    within(proxy as HTMLElement).getByText(
      /moves every client-facing endpoint/,
    ),
  ).toBeDefined();

  // A normal field: no risk badge.
  const metrics = document.querySelector<HTMLElement>(
    '[data-config-path="listener.metrics_addr"]',
  );
  expect(
    within(metrics as HTMLElement).queryByText(/operational risk/i),
  ).toBeNull();

  const summary = screen.getByTestId('config-validation-summary');
  expect(within(summary).getByText('1 error')).toBeDefined();
  // Only exceptions are listed: a valid effective config adds no status.
  expect(within(summary).queryByText(/Effective/)).toBeNull();

  // A dirty field marks itself with a Modified badge.
  fireEvent.change(screen.getByLabelText('Proxy address'), {
    target: { value: '0.0.0.0:8181' },
  });
  expect(within(proxy as HTMLElement).getByText('Modified')).toBeDefined();
});
