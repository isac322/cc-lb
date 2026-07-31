// @vitest-environment jsdom
import { cleanup, render, screen } from '@testing-library/react';
import type React from 'react';
import { afterEach, beforeEach, expect, test, vi } from 'vitest';
import { Route as SettingsRoute } from './settings';
import { Route as StatusRoute } from './status';

const queryMocks = vi.hoisted(() => ({
  useApplyConfig: vi.fn(),
  useConfigCurrent: vi.fn(),
  useConfigDraft: vi.fn(),
  useConfigHistory: vi.fn(),
  useConfigSchema: vi.fn(),
  useCredentials: vi.fn(),
  useKillswitch: vi.fn(),
  useOAuthStatus: vi.fn(),
  usePrincipalNameMap: vi.fn(),
  useReloadConfig: vi.fn(),
  useSaveDraft: vi.fn(),
  useStatus: vi.fn(),
  useValidateConfig: vi.fn(),
}));

const localeMocks = vi.hoisted(() => ({
  setLocale: vi.fn(),
  setTimezone: vi.fn(),
}));

vi.mock('../lib/queries', () => queryMocks);

vi.mock('../lib/locale', () => ({
  formatAbsolute: () => 'Jul 30, 2026, 12:00:00 PM',
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

const StatusComponent = StatusRoute.options.component as React.ComponentType;
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
  upstreams: [],
  principals: [],
  plugin_chain_summary: {
    principal_count_with_chain: 2,
    total_entries: 4,
  },
  killswitch: false,
  last_reload_status: {
    ok: true,
    applied_revision: 7,
    applied_at_unix_secs: 1_722_340_800,
  },
  restart_required_changes: [],
};

const loadedDraft = {
  draft: {},
  revision: 7,
  last_validated_revision: 7,
  last_validation_error: null,
  saved_at_unix_secs: 1_722_340_800,
};

function loadingResult() {
  return { data: undefined, isLoading: true };
}

function loadedResult<T>(data: T) {
  return { data, isLoading: false };
}

function mutationResult() {
  return { mutate: vi.fn(), isPending: false };
}

function setSettingsLoaded() {
  queryMocks.useStatus.mockReturnValue(loadedResult(loadedStatus));
  queryMocks.useConfigCurrent.mockReturnValue(loadedResult({}));
  queryMocks.useConfigDraft.mockReturnValue(loadedResult(loadedDraft));
  queryMocks.useConfigSchema.mockReturnValue(
    loadedResult({ schema: {}, coverage_checklist: [] }),
  );
  queryMocks.useConfigHistory.mockReturnValue(loadedResult({ history: [] }));
}

beforeEach(() => {
  vi.clearAllMocks();

  queryMocks.useStatus.mockReturnValue(loadingResult());
  queryMocks.useCredentials.mockReturnValue(loadingResult());
  queryMocks.useOAuthStatus.mockReturnValue(loadingResult());
  queryMocks.useConfigCurrent.mockReturnValue(loadingResult());
  queryMocks.useConfigDraft.mockReturnValue(loadingResult());
  queryMocks.useConfigSchema.mockReturnValue(loadingResult());
  queryMocks.useConfigHistory.mockReturnValue(loadingResult());
  queryMocks.usePrincipalNameMap.mockReturnValue(
    new Map([['principal-1', 'Primary principal']]),
  );

  queryMocks.useKillswitch.mockReturnValue(mutationResult());
  queryMocks.useApplyConfig.mockReturnValue(mutationResult());
  queryMocks.useReloadConfig.mockReturnValue(mutationResult());
  queryMocks.useSaveDraft.mockReturnValue(mutationResult());
  queryMocks.useValidateConfig.mockReturnValue(mutationResult());
});

afterEach(cleanup);

test('status cold load reserves restart, system, credentials, and OAuth geometry', () => {
  render(<StatusComponent />);

  const restartBody = screen.getByTestId('restart-required-body');
  expect(restartBody.className).toContain('min-h-[72px]');
  expect(restartBody.querySelectorAll('.skeleton')).toHaveLength(2);

  const systemCards = screen.getAllByTestId('system-card');
  expect(systemCards).toHaveLength(3);
  for (const card of systemCards) {
    expect(card.className).toContain('min-h-[144px]');
  }
  expect(
    screen.getByTestId('system-last-reload-slot').querySelector('.skeleton'),
  ).not.toBeNull();
  expect(screen.getByTestId('system-last-reload-slot').className).toContain(
    'min-w-20',
  );
  expect(
    screen.getByTestId('system-principal-chain-count-slot').className,
  ).toContain('min-w-8');
  expect(
    screen.getByTestId('system-total-chain-count-slot').className,
  ).toContain('min-w-8');
  expect(
    screen
      .getByTestId('system-killswitch-status-slot')
      .querySelector('.skeleton'),
  ).not.toBeNull();
  expect(
    screen.getByTestId('system-killswitch-status-slot').className,
  ).toContain('min-w-14');
  const killswitchControl = screen.getByTestId(
    'killswitch-control',
  ) as HTMLButtonElement;
  expect(killswitchControl.disabled).toBe(true);
  expect(killswitchControl.querySelector('.skeleton')).not.toBeNull();

  const credentialsSlot = screen.getByTestId('credentials-table-slot');
  expect(credentialsSlot.className).toContain('min-h-[191px]');
  const credentialRows = credentialsSlot.querySelectorAll('tbody tr');
  expect(credentialRows).toHaveLength(3);
  for (const row of credentialRows) {
    const cells = row.querySelectorAll('td');
    expect(cells).toHaveLength(5);
    for (const cell of cells) {
      expect(cell.className).toContain('px-4');
      expect(cell.className).toContain('py-2');
    }
  }

  const oauthGrid = screen.getByTestId('oauth-grid');
  expect(oauthGrid.className).toContain('min-h-[230px]');
  expect(oauthGrid.className).toContain('sm:min-h-[205px]');
  const oauthCard = screen.getByTestId('oauth-card');
  expect(screen.getAllByTestId('oauth-card')).toHaveLength(1);
  expect(oauthCard.className).toContain('min-h-[230px]');
  expect(oauthCard.className).toContain('sm:min-h-[205px]');
  expect(oauthCard.className).toContain('md:col-span-2');
  expect(oauthCard.querySelectorAll('.skeleton').length).toBeGreaterThan(0);
});

test('status resolved empty and loaded OAuth states retain the reserved card grid', () => {
  queryMocks.useStatus.mockReturnValue(loadedResult(loadedStatus));
  queryMocks.useCredentials.mockReturnValue(
    loadedResult({ credentials: [], observed: true }),
  );
  queryMocks.useOAuthStatus.mockReturnValue(
    loadedResult({ credentials: [], observed: true }),
  );

  const { rerender } = render(<StatusComponent />);

  expect(screen.getByTestId('restart-required-body').className).toContain(
    'min-h-[72px]',
  );
  expect(screen.getByTestId('credentials-table-slot').className).toContain(
    'min-h-[191px]',
  );
  expect(screen.getByText('No credentials observed')).toBeDefined();
  expect(screen.getByTestId('oauth-grid').className).toContain('min-h-[230px]');
  expect(screen.getByTestId('oauth-card').className).toContain('min-h-[230px]');
  expect(screen.getByTestId('oauth-card').className).toContain(
    'sm:min-h-[205px]',
  );
  expect(screen.getByTestId('oauth-card').className).toContain('md:col-span-2');
  expect(screen.getByText('No OAuth tokens')).toBeDefined();

  queryMocks.useOAuthStatus.mockReturnValue(
    loadedResult({
      observed: true,
      credentials: [
        {
          principal_id: 'principal-1',
          provider: 'anthropic',
          has_credentials: true,
          expires_at_unix_secs: 1_900_000_000,
          refresh_token_present: true,
          last_updated_unix_secs: 1_722_340_800,
          status: 'active',
          scopes: ['user:inference'],
        },
      ],
    }),
  );
  rerender(<StatusComponent />);

  expect(screen.getByTestId('oauth-grid').className).toContain('min-h-[230px]');
  expect(screen.getByTestId('oauth-card').className).toContain('min-h-[230px]');
  expect(screen.getByTestId('oauth-card').className).toContain(
    'sm:min-h-[205px]',
  );
  expect(screen.getByTestId('oauth-card').className).toContain('md:col-span-2');
  expect(screen.getByText('anthropic')).toBeDefined();
});

test('settings cold load skeletonizes fixed metadata, checklist, and history slots', () => {
  render(<SettingsComponent />);

  const versionCard = screen.getByTestId('version-card');
  const versionSkeletons = versionCard.querySelectorAll('.skeleton');
  const versionSubtitle = versionCard.querySelector(
    '[data-slot="card-subtitle"]',
  );
  const versionSubtitleSkeleton =
    versionSubtitle?.querySelector<HTMLSpanElement>('span.skeleton');
  expect(versionSkeletons).toHaveLength(5);
  expect(versionSubtitleSkeleton?.tagName).toBe('SPAN');
  expect(versionSubtitleSkeleton?.getAttribute('aria-hidden')).toBe('true');
  expect(versionSubtitleSkeleton?.className).toContain('block');
  expect(versionSubtitleSkeleton?.className).toContain('h-8');
  expect(versionSubtitleSkeleton?.className).toContain('w-72');
  expect(versionSubtitleSkeleton?.className).toContain('max-w-full');
  expect(versionSubtitleSkeleton?.className).toContain('sm:h-4');
  expect(versionSubtitle?.querySelector('div.skeleton')).toBeNull();
  expect(versionCard.textContent).not.toContain('—');

  const draftMetadata = screen.getByTestId('draft-metadata');
  expect(draftMetadata.className).toContain('min-h-4');
  expect(draftMetadata.querySelectorAll('.skeleton')).toHaveLength(3);
  expect(draftMetadata.textContent).not.toContain('—');

  const checklistSlot = screen.getByTestId('config-checklist-slot');
  expect(checklistSlot.className).toContain('min-h-[20px]');
  expect(checklistSlot.querySelector('.skeleton')).not.toBeNull();

  const historySlot = screen.getByTestId('config-history-slot');
  expect(historySlot.className).toContain('min-h-[173px]');
  expect(historySlot.className).toContain('sm:min-h-[163px]');
  expect(historySlot.querySelectorAll('thead th')).toHaveLength(6);
  expect(historySlot.querySelector('table')?.className).toContain(
    'min-w-[640px]',
  );
  const historyRows = historySlot.querySelectorAll('tbody tr');
  expect(historyRows).toHaveLength(4);
  for (const row of historyRows) {
    const cells = row.querySelectorAll('td');
    expect(cells).toHaveLength(6);
    for (const cell of cells) {
      expect(cell.className).toContain('px-4');
      expect(cell.className).toContain('py-2');
    }
  }
});

test('settings empty and loaded checklist/history states retain their slots', () => {
  setSettingsLoaded();

  const { rerender } = render(<SettingsComponent />);

  expect(screen.getByTestId('config-checklist-slot').className).toContain(
    'min-h-[20px]',
  );
  expect(screen.getByText('Coverage checklist (0 fields)')).toBeDefined();
  expect(screen.getByTestId('config-history-slot').className).toContain(
    'min-h-[173px]',
  );
  expect(screen.getByTestId('config-history-slot').className).toContain(
    'sm:min-h-[163px]',
  );
  expect(screen.getByText('No history available.')).toBeDefined();

  queryMocks.useConfigSchema.mockReturnValue(
    loadedResult({
      schema: {},
      coverage_checklist: ['listener.proxy_addr', 'storage.url'],
    }),
  );
  queryMocks.useConfigHistory.mockReturnValue(
    loadedResult({
      history: [
        {
          revision: 7,
          applied_at_unix_secs: 1_722_340_800,
          config_summary: {
            upstreams: 2,
            principals: 3,
            plugin_count: 4,
            tls_enabled: true,
          },
        },
      ],
    }),
  );
  rerender(<SettingsComponent />);

  expect(screen.getByTestId('config-checklist-slot').className).toContain(
    'min-h-[20px]',
  );
  expect(screen.getByText('Coverage checklist (2 fields)')).toBeDefined();
  expect(screen.getByTestId('config-history-slot').className).toContain(
    'min-h-[173px]',
  );
  expect(screen.getByText('on')).toBeDefined();
});
