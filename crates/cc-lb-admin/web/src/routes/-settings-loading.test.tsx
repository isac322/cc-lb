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
  useConfigCurrent: vi.fn(),
  useConfigDraft: vi.fn(),
  useConfigHistory: vi.fn(),
  useSaveDraft: vi.fn(),
  useStatus: vi.fn(),
  useValidateConfig: vi.fn(),
}));

const apiMocks = vi.hoisted(() => ({
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
    downloadJson: apiMocks.downloadJson,
  };
});

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

const loadedDraft = {
  draft: {},
  revision: 7,
  last_validated_revision: 7,
  last_validation_error: null,
  saved_at_unix_secs: 1_722_340_800,
};

function loadingResult() {
  return {
    data: undefined,
    isFetching: true,
    isLoading: true,
    isPending: true,
  };
}

function loadedResult<T>(data: T) {
  return {
    data,
    isFetching: false,
    isLoading: false,
    isPending: false,
  };
}

function mutationResult() {
  return { mutate: vi.fn(), isPending: false, variables: undefined };
}

function setSettingsLoaded() {
  queryMocks.useStatus.mockReturnValue(loadedResult(loadedStatus));
  queryMocks.useConfigCurrent.mockReturnValue(loadedResult({}));
  queryMocks.useConfigDraft.mockReturnValue(loadedResult(loadedDraft));
  queryMocks.useConfigHistory.mockReturnValue(loadedResult({ history: [] }));
}

beforeEach(() => {
  vi.useFakeTimers({
    toFake: ['Date'],
    now: new Date('2026-06-18T00:00:01.000Z'),
  });
  vi.clearAllMocks();
  apiMocks.downloadJson.mockResolvedValue(undefined);

  queryMocks.useStatus.mockReturnValue(loadingResult());
  queryMocks.useConfigCurrent.mockReturnValue(loadingResult());
  queryMocks.useConfigDraft.mockReturnValue(loadingResult());
  queryMocks.useConfigHistory.mockReturnValue(loadingResult());

  queryMocks.useSaveDraft.mockReturnValue(mutationResult());
  queryMocks.useValidateConfig.mockReturnValue(mutationResult());
});

afterEach(() => {
  cleanup();
  vi.useRealTimers();
});

test('settings cold load reserves fixed metadata and history heights', () => {
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

  const historySlot = screen.getByTestId('config-history-slot');
  expect(historySlot.className).toContain('min-h-[173px]');
  expect(historySlot.className).toContain('sm:min-h-[163px]');
  expect(historySlot.querySelectorAll('thead th')).toHaveLength(2);
  expect(historySlot.querySelector('table')?.className).toContain(
    'min-w-[400px]',
  );
  const historyRows = historySlot.querySelectorAll('tbody tr');
  expect(historyRows).toHaveLength(4);
  for (const row of historyRows) {
    const cells = row.querySelectorAll('td');
    expect(cells).toHaveLength(2);
    for (const cell of cells) {
      expect(cell.className).toContain('px-4');
      expect(cell.className).toContain('py-2');
    }
  }
});

test('settings separates saved config history from database backups', () => {
  setSettingsLoaded();

  const { rerender } = render(<SettingsComponent />);

  const historySlot = screen.getByTestId('config-history-slot');
  expect(historySlot.className).toContain('min-h-[173px]');
  expect(historySlot.className).toContain('sm:min-h-[163px]');
  expect(screen.getByText('Saved Config History')).toBeDefined();
  expect(screen.getByText('Last 20 saved revisions')).toBeDefined();
  expect(screen.getByText('No saved config history available.')).toBeDefined();
  expect(
    within(historySlot)
      .getAllByRole('columnheader')
      .map((header) => header.textContent),
  ).toEqual(['Rev', 'Saved']);

  expect(screen.getByText('Data & Backups')).toBeDefined();
  expect(screen.getByText('Database resources snapshot')).toBeDefined();
  expect(
    screen.getByText(
      'Download a JSON snapshot of upstreams, principals, plugins, and chains stored in the database.',
    ),
  ).toBeDefined();
  expect(screen.queryByText('Configuration Export')).toBeNull();
  expect(screen.queryByText('Admin Token')).toBeNull();
  expect(screen.queryByText('Coverage checklist')).toBeNull();
  expect(screen.queryByText('Restart-Required Fields')).toBeNull();
  expect(screen.queryByRole('button', { name: 'Apply' })).toBeNull();
  expect(screen.queryByRole('button', { name: 'Reload' })).toBeNull();

  queryMocks.useConfigHistory.mockReturnValue(
    loadedResult({
      history: [
        {
          revision: 7,
          applied_at_unix_secs: 1_722_340_800,
        },
      ],
    }),
  );
  rerender(<SettingsComponent />);

  const loadedHistorySlot = screen.getByTestId('config-history-slot');
  expect(loadedHistorySlot.className).toContain('min-h-[173px]');
  expect(loadedHistorySlot.querySelectorAll('tbody td')).toHaveLength(2);
  expect(
    within(loadedHistorySlot).getByTitle('Jul 30, 2026, 12:00:00 PM'),
  ).toBeDefined();
  expect(within(loadedHistorySlot).getByText('7')).toBeDefined();
});

test('settings editor loads the saved draft without deprecated settings views', () => {
  setSettingsLoaded();
  const savedDraft = {
    routing: { strategy: 'saved-draft' },
  };
  queryMocks.useConfigCurrent.mockReturnValue(
    loadedResult({ routing: { strategy: 'current-config' } }),
  );
  queryMocks.useConfigDraft.mockReturnValue(
    loadedResult({ ...loadedDraft, draft: savedDraft }),
  );

  render(<SettingsComponent />);
  const editor = screen.getByTestId(
    'config-draft-editor',
  ) as HTMLTextAreaElement;
  expect(editor.value).toBe(JSON.stringify(savedDraft, null, 2));
  expect(
    screen.getByRole('button', { name: 'Save' }).hasAttribute('disabled'),
  ).toBe(true);
  expect(screen.getByTestId('draft-revision-slot').textContent).toBe('7');
  expect(screen.getByTestId('draft-last-validated-slot').textContent).toBe(
    'rev 7',
  );
  expect(screen.getByTestId('draft-saved-at-slot').textContent).not.toBe('—');

  expect(screen.queryByText('Admin Token')).toBeNull();
  expect(screen.queryByText('Coverage checklist')).toBeNull();
  expect(screen.queryByText('Restart-Required Fields')).toBeNull();
  expect(screen.queryByRole('button', { name: 'Apply' })).toBeNull();
  expect(screen.queryByRole('button', { name: 'Reload' })).toBeNull();
});

test('settings editor initializes from current config when no saved draft exists', () => {
  setSettingsLoaded();
  const currentConfig = {
    routing: { strategy: 'current-config' },
    listener: { proxy_addr: '127.0.0.1:8080' },
  };
  queryMocks.useConfigCurrent.mockReturnValue(loadedResult(currentConfig));
  queryMocks.useConfigDraft.mockReturnValue(
    loadedResult({
      draft: null,
      revision: 0,
      last_validated_revision: null,
      last_validation_error: null,
      saved_at_unix_secs: null,
    }),
  );
  const saveMutate = vi.fn();
  queryMocks.useSaveDraft.mockReturnValue({
    mutate: saveMutate,
    isPending: false,
    variables: undefined,
  });

  render(<SettingsComponent />);
  const editor = screen.getByTestId(
    'config-draft-editor',
  ) as HTMLTextAreaElement;
  expect(editor.value).toBe(JSON.stringify(currentConfig, null, 2));
  expect(editor.placeholder).toBe('JSON config draft…');
  expect(screen.getByTestId('draft-revision-slot').textContent).toBe('0');
  expect(screen.getByTestId('draft-last-validated-slot').textContent).toBe('—');
  expect(screen.getByTestId('draft-saved-at-slot').textContent).toBe('—');
  const saveButton = screen.getByRole('button', { name: 'Save' });
  expect(saveButton.hasAttribute('disabled')).toBe(false);
  fireEvent.click(saveButton);
  expect(saveMutate).toHaveBeenCalledWith(
    { draft: currentConfig, expected_revision: 0 },
    expect.objectContaining({ onSuccess: expect.any(Function) }),
  );
  expect(
    screen.getByRole('button', { name: 'Validate' }).hasAttribute('disabled'),
  ).toBe(true);
  expect(screen.getByTestId('config-pipeline-status').textContent).toBe(
    'Save this configuration as a server draft before validating.',
  );
});

test('settings editor recovers from an initial draft failure without losing its revision context', () => {
  setSettingsLoaded();
  const currentConfig = {
    routing: { strategy: 'current-config' },
  };
  const recoveredDraft = {
    routing: { strategy: 'recovered-draft' },
  };
  const currentRefetch = vi.fn();
  const draftRefetch = vi.fn();
  const saveMutate = vi.fn();
  queryMocks.useConfigCurrent.mockReturnValue({
    ...loadedResult(currentConfig),
    refetch: currentRefetch,
  });
  queryMocks.useConfigDraft.mockReturnValue({
    data: undefined,
    isFetching: false,
    isLoading: false,
    isPending: false,
    isError: true,
    error: { status: 500 },
    refetch: draftRefetch,
  });
  queryMocks.useSaveDraft.mockReturnValue({
    mutate: saveMutate,
    isPending: false,
    variables: undefined,
  });

  const view = render(<SettingsComponent />);
  const editor = screen.getByTestId(
    'config-draft-editor',
  ) as HTMLTextAreaElement;

  expect(screen.getByTestId('draft-revision-slot').textContent).toBe('—');
  expect(screen.getByTestId('draft-last-validated-slot').textContent).toBe('—');
  expect(screen.getByTestId('draft-saved-at-slot').textContent).toBe('—');
  expect(screen.getByRole('alert').textContent).toContain(
    'Failed to load the configuration draft.',
  );
  expect(editor.hasAttribute('disabled')).toBe(true);
  expect(
    screen.getByRole('button', { name: 'Save' }).hasAttribute('disabled'),
  ).toBe(true);

  fireEvent.click(screen.getByRole('button', { name: 'Retry' }));
  expect(draftRefetch).toHaveBeenCalledTimes(1);
  expect(currentRefetch).not.toHaveBeenCalled();

  queryMocks.useConfigDraft.mockReturnValue({
    ...loadedResult({
      ...loadedDraft,
      draft: recoveredDraft,
      revision: 7,
    }),
    isError: false,
    refetch: draftRefetch,
  });
  view.rerender(<SettingsComponent />);

  expect(screen.queryByRole('alert')).toBeNull();
  expect(editor.hasAttribute('disabled')).toBe(false);
  expect(editor.value).toBe(JSON.stringify(recoveredDraft, null, 2));
  expect(screen.getByTestId('draft-revision-slot').textContent).toBe('7');

  const editedText = JSON.stringify(
    { routing: { strategy: 'edited-after-retry' } },
    null,
    2,
  );
  fireEvent.change(editor, { target: { value: editedText } });
  queryMocks.useConfigDraft.mockReturnValue({
    ...loadedResult({
      ...loadedDraft,
      draft: { routing: { strategy: 'refresh-result' } },
      revision: 9,
      last_validated_revision: 9,
    }),
    isError: true,
    error: { status: 500 },
    refetch: draftRefetch,
  });
  view.rerender(<SettingsComponent />);

  expect(screen.queryByRole('alert')).toBeNull();
  expect(editor.value).toBe(editedText);
  expect(screen.getByTestId('draft-revision-slot').textContent).toBe('7');
  fireEvent.click(screen.getByRole('button', { name: 'Save' }));
  expect(saveMutate.mock.calls[0]?.[0]).toEqual({
    draft: { routing: { strategy: 'edited-after-retry' } },
    expected_revision: 7,
  });
});

test('pristine editor follows an expired server draft at the new revision', () => {
  setSettingsLoaded();
  const currentConfig = {
    routing: { strategy: 'current-config' },
  };
  const expiringDraft = {
    routing: { strategy: 'invalid-saved-draft' },
  };
  queryMocks.useConfigCurrent.mockReturnValue(loadedResult(currentConfig));
  queryMocks.useConfigDraft.mockReturnValue(
    loadedResult({
      ...loadedDraft,
      draft: expiringDraft,
      last_validation_error: 'invalid draft',
    }),
  );

  const view = render(<SettingsComponent />);
  const editor = screen.getByTestId(
    'config-draft-editor',
  ) as HTMLTextAreaElement;
  expect(editor.value).toBe(JSON.stringify(expiringDraft, null, 2));

  queryMocks.useConfigDraft.mockReturnValue(
    loadedResult({
      draft: null,
      revision: 8,
      last_validated_revision: null,
      last_validation_error: null,
      saved_at_unix_secs: null,
    }),
  );
  view.rerender(<SettingsComponent />);

  expect(editor.value).toBe(JSON.stringify(currentConfig, null, 2));
  expect(screen.getByTestId('draft-revision-slot').textContent).toBe('8');
  expect(screen.getByTestId('draft-last-validated-slot').textContent).toBe('—');
  expect(screen.getByTestId('draft-saved-at-slot').textContent).toBe('—');
  expect(screen.getByTestId('config-pipeline-status').textContent).toBe(
    'Save this configuration as a server draft before validating.',
  );
});

test('dirty editor survives background revisions and conflict mutation states while saving its starting revision', () => {
  setSettingsLoaded();
  const saveMutate = vi.fn();
  queryMocks.useSaveDraft.mockReturnValue({
    mutate: saveMutate,
    isPending: false,
    variables: undefined,
  });
  queryMocks.useConfigDraft.mockReturnValue(
    loadedResult({
      ...loadedDraft,
      draft: { routing: { strategy: 'original' } },
      revision: 7,
    }),
  );

  const view = render(<SettingsComponent />);
  const editor = screen.getByTestId(
    'config-draft-editor',
  ) as HTMLTextAreaElement;
  const editedText = JSON.stringify(
    { routing: { strategy: 'edited-locally' } },
    null,
    2,
  );
  fireEvent.change(editor, { target: { value: editedText } });

  expect(
    screen.getByRole('button', { name: 'Validate' }).hasAttribute('disabled'),
  ).toBe(true);
  expect(screen.getByTestId('config-pipeline-status').textContent).toBe(
    'Save your editor changes before validating. Validate uses the saved server draft.',
  );

  queryMocks.useConfigDraft.mockReturnValue(
    loadedResult({
      ...loadedDraft,
      draft: { routing: { strategy: 'changed-on-server' } },
      revision: 9,
      last_validated_revision: 9,
    }),
  );
  view.rerender(<SettingsComponent />);

  expect(editor.value).toBe(editedText);
  expect(screen.getByTestId('draft-revision-slot').textContent).toBe('7');
  expect(screen.getByTestId('config-pipeline-status').textContent).toContain(
    'The server draft is revision 9, while this editor started from revision 7.',
  );

  fireEvent.click(screen.getByRole('button', { name: 'Save' }));
  expect(saveMutate).toHaveBeenCalledTimes(1);
  expect(saveMutate.mock.calls[0]?.[0]).toEqual({
    draft: { routing: { strategy: 'edited-locally' } },
    expected_revision: 7,
  });

  for (const status of [409, 412]) {
    queryMocks.useSaveDraft.mockReturnValue({
      mutate: saveMutate,
      isPending: false,
      isError: true,
      error: { status },
      variables: undefined,
    });
    view.rerender(<SettingsComponent />);
    expect(editor.value).toBe(editedText);
    expect(screen.getByTestId('draft-revision-slot').textContent).toBe('7');
  }
});

test('save response advances the editor revision used by validate', () => {
  setSettingsLoaded();
  const saveMutate = vi.fn();
  const validateMutate = vi.fn();
  queryMocks.useSaveDraft.mockReturnValue({
    mutate: saveMutate,
    isPending: false,
    variables: undefined,
  });
  queryMocks.useValidateConfig.mockReturnValue({
    mutate: validateMutate,
    isPending: false,
    variables: undefined,
  });
  queryMocks.useConfigDraft.mockReturnValue(
    loadedResult({
      ...loadedDraft,
      draft: { routing: { strategy: 'old' } },
      revision: 7,
    }),
  );

  const view = render(<SettingsComponent />);
  const editor = screen.getByTestId(
    'config-draft-editor',
  ) as HTMLTextAreaElement;
  const savedText = JSON.stringify({ routing: { strategy: 'new' } }, null, 2);
  fireEvent.change(editor, { target: { value: savedText } });
  fireEvent.click(screen.getByRole('button', { name: 'Save' }));
  queryMocks.useConfigDraft.mockReturnValue(
    loadedResult({
      ...loadedDraft,
      draft: { routing: { strategy: 'new' } },
      revision: 8,
      last_validated_revision: 7,
    }),
  );
  view.rerender(<SettingsComponent />);

  const saveOptions = saveMutate.mock.calls[0]?.[1] as {
    onSuccess: (response: {
      revision: number;
      saved_at_unix_secs: number;
    }) => void;
  };
  act(() => {
    saveOptions.onSuccess({
      revision: 8,
      saved_at_unix_secs: 1_722_340_900,
    });
  });

  expect(editor.value).toBe(savedText);
  expect(screen.getByTestId('draft-revision-slot').textContent).toBe('8');
  expect(
    screen.getByRole('button', { name: 'Save' }).hasAttribute('disabled'),
  ).toBe(true);
  expect(
    screen.getByRole('button', { name: 'Validate' }).hasAttribute('disabled'),
  ).toBe(false);

  fireEvent.click(screen.getByRole('button', { name: 'Validate' }));
  expect(validateMutate.mock.calls[0]?.[0]).toBe(8);
  const validateOptions = validateMutate.mock.calls[0]?.[1] as {
    onSuccess: (response: {
      valid: boolean;
      revision: number;
      error?: string;
    }) => void;
  };
  act(() => {
    validateOptions.onSuccess({ valid: true, revision: 8 });
  });

  expect(screen.getByTestId('draft-last-validated-slot').textContent).toBe(
    'rev 8',
  );
});

test('save completion advances revision while preserving edits made in flight', () => {
  setSettingsLoaded();
  const saveMutate = vi.fn();
  queryMocks.useSaveDraft.mockReturnValue({
    mutate: saveMutate,
    isPending: false,
    variables: undefined,
  });
  queryMocks.useConfigDraft.mockReturnValue(
    loadedResult({
      ...loadedDraft,
      draft: { routing: { strategy: 'old' } },
      revision: 7,
    }),
  );

  render(<SettingsComponent />);
  const editor = screen.getByTestId(
    'config-draft-editor',
  ) as HTMLTextAreaElement;
  const firstText = JSON.stringify(
    { routing: { strategy: 'first-edit' } },
    null,
    2,
  );
  const secondText = JSON.stringify(
    { routing: { strategy: 'second-edit' } },
    null,
    2,
  );

  fireEvent.change(editor, { target: { value: firstText } });
  fireEvent.click(screen.getByRole('button', { name: 'Save' }));
  fireEvent.change(editor, { target: { value: secondText } });

  const saveOptions = saveMutate.mock.calls[0]?.[1] as {
    onSuccess: (response: {
      revision: number;
      saved_at_unix_secs: number;
    }) => void;
  };
  act(() => {
    saveOptions.onSuccess({
      revision: 8,
      saved_at_unix_secs: 1_722_340_900,
    });
  });

  expect(editor.value).toBe(secondText);
  expect(screen.getByTestId('draft-revision-slot').textContent).toBe('8');
  fireEvent.click(screen.getByRole('button', { name: 'Save' }));
  expect(saveMutate.mock.calls[1]?.[0]).toEqual({
    draft: { routing: { strategy: 'second-edit' } },
    expected_revision: 8,
  });
});

test('database snapshot exposes download progress and blocks duplicate clicks', () => {
  setSettingsLoaded();
  apiMocks.downloadJson.mockReturnValue(Promise.withResolvers<void>().promise);
  render(<SettingsComponent />);

  fireEvent.click(
    screen.getByRole('button', { name: 'Download database snapshot' }),
  );

  const downloading = screen.getByRole('button', { name: 'Downloading...' });
  expect(downloading.hasAttribute('disabled')).toBe(true);
  expect(downloading.getAttribute('aria-busy')).toBe('true');
  expect(downloading.querySelector('svg.animate-spin')).not.toBeNull();

  fireEvent.click(downloading);
  expect(apiMocks.downloadJson).toHaveBeenCalledTimes(1);
  expect(apiMocks.downloadJson).toHaveBeenCalledWith(
    '/admin/v1/export',
    'cc-lb-database-resources-2026-06-18T00:00.json',
  );
});
