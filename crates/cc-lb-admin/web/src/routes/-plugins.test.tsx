import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type * as ReactRouterModule from '@tanstack/react-router';
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from '@testing-library/react';
import type React from 'react';
import { useState } from 'react';
import { afterEach, beforeEach, expect, test, vi } from 'vitest';

vi.mock('sonner', () => ({
  toast: {
    error: vi.fn(),
    success: vi.fn(),
  },
}));

const fetchWithAuthMock = vi.hoisted(() => vi.fn());

vi.mock('../lib/api', async () => {
  const actual = await vi.importActual<typeof ApiModule>('../lib/api');
  return {
    ...actual,
    fetchWithAuth: (...args: unknown[]) => fetchWithAuthMock(...args),
  };
});

import { toast } from 'sonner';
import type * as ApiModule from '../lib/api';
import { ApiError } from '../lib/api';
import * as queries from '../lib/queries';
import { Route } from './plugins';

type SearchState = {
  readonly plugin?: string;
  readonly action?: 'upload';
};

type NavigateArgs = {
  readonly replace?: boolean;
  readonly search?: SearchState | ((previous: SearchState) => SearchState);
};

const navigateMock = vi.fn();
let initialSearch: SearchState = {};
let currentSearch: SearchState = {};
let setMockSearch: React.Dispatch<React.SetStateAction<SearchState>> | null =
  null;

vi.mock('@tanstack/react-router', async () => {
  const actual = await vi.importActual<typeof ReactRouterModule>(
    '@tanstack/react-router',
  );
  return {
    ...actual,
    useNavigate: () => (args: NavigateArgs) => {
      navigateMock(args);
      const search = args.search;
      if (!setMockSearch || !search) return;
      setMockSearch((previous) =>
        typeof search === 'function' ? search(previous) : search,
      );
    },
  };
});

vi.mock('../lib/queries', async () => {
  const actual = await vi.importActual<typeof queries>('../lib/queries');
  return {
    ...actual,
    usePluginRegistry: vi.fn(),
    useUploadWasm: vi.fn(),
    useDeletePlugin: vi.fn(),
    useGcPlugins: vi.fn(),
    usePrincipals: vi.fn(),
    usePluginChain: vi.fn(),
    usePluginReferences: vi.fn(),
  };
});

const queryClient = new QueryClient({
  defaultOptions: {
    queries: { retry: false },
    mutations: { retry: false },
  },
});

const Component = Route.options.component as React.ComponentType;

const pluginEntry: queries.PluginEntry = {
  id: 'plugin-1',
  name: 'My Plugin',
  original_filename: 'my-plugin.wasm',
  label: null,
  metadata: null,
  description: 'Mock description',
  usage: 'Mock usage',
  hook_metadata: {},
  sha256_hex: 'abcd',
  size_bytes: 100,
  refcount: 2,
  revision: 1,
  uploaded_at_unix_secs: 0,
  supported_slots: ['router'],
};

function TestWrapper() {
  const [search, setSearch] = useState<SearchState>(initialSearch);
  currentSearch = search;
  setMockSearch = setSearch;
  Object.assign(Route, { useSearch: () => search });
  return <Component />;
}

function renderWithProviders() {
  return render(
    <QueryClientProvider client={queryClient}>
      <TestWrapper />
    </QueryClientProvider>,
  );
}

beforeEach(() => {
  vi.clearAllMocks();
  initialSearch = {};
  currentSearch = {};
  setMockSearch = null;
});

afterEach(() => {
  cleanup();
  queryClient.clear();
  vi.restoreAllMocks();
});

function mockPluginPageDependencies() {
  vi.mocked(queries.usePluginRegistry).mockReturnValue({
    data: { entries: [] },
    isLoading: false,
  } as never);
  vi.mocked(queries.useDeletePlugin).mockReturnValue({
    mutateAsync: vi.fn(),
  } as never);
  vi.mocked(queries.useGcPlugins).mockReturnValue({
    mutate: vi.fn(),
  } as never);
  vi.mocked(queries.usePluginReferences).mockReturnValue({
    data: null,
    isLoading: false,
  } as never);
}

function mockPluginPage(uploadPending = false) {
  const mutate = vi.fn();
  mockPluginPageDependencies();
  vi.mocked(queries.useUploadWasm).mockReturnValue({
    mutate,
    isPending: uploadPending,
  } as never);
  return mutate;
}

async function mockPluginPageWithRealUpload() {
  mockPluginPageDependencies();
  const actual = await vi.importActual<typeof queries>('../lib/queries');
  vi.mocked(queries.useUploadWasm).mockImplementation(actual.useUploadWasm);
}

function openUploadAction() {
  const setSearch = setMockSearch;
  if (!setSearch) throw new Error('Plugin route is not mounted');
  act(() => {
    setSearch((previous) => ({ ...previous, action: 'upload' }));
  });
}

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((promiseResolve, promiseReject) => {
    resolve = promiseResolve;
    reject = promiseReject;
  });
  return { promise, reject, resolve };
}

test('consumes the upload action once while preserving plugin selection', async () => {
  initialSearch = { plugin: 'preserved-plugin', action: 'upload' };
  const mutate = mockPluginPage();
  const fileInputClick = vi.spyOn(HTMLInputElement.prototype, 'click');

  renderWithProviders();

  expect(
    await screen.findByRole('dialog', { name: 'Upload plugin' }),
  ).toBeDefined();
  const browse = screen.getByRole('button', { name: 'Choose .wasm file' });
  await waitFor(() => {
    expect(document.activeElement).toBe(browse);
  });
  expect(mutate).not.toHaveBeenCalled();
  expect(fileInputClick).not.toHaveBeenCalled();
  await waitFor(() => {
    expect(currentSearch.plugin).toBe('preserved-plugin');
    expect(currentSearch.action).toBeUndefined();
  });
  expect(navigateMock).toHaveBeenCalledTimes(1);
  expect(navigateMock.mock.calls[0]?.[0]).toMatchObject({ replace: true });

  fireEvent.click(screen.getByRole('button', { name: 'Close dialog' }));
  await waitFor(() => {
    expect(screen.queryByRole('dialog', { name: 'Upload plugin' })).toBeNull();
  });
  expect(navigateMock).toHaveBeenCalledTimes(1);
});

test('keeps action-opened upload dialog locked while an upload is pending', async () => {
  initialSearch = { action: 'upload' };
  mockPluginPage(true);

  renderWithProviders();

  const dialog = await screen.findByRole('dialog', { name: 'Upload plugin' });
  const uploadTarget = screen.getByRole('button', {
    name: 'Uploading plugin',
  });
  expect(uploadTarget.getAttribute('aria-disabled')).toBe('true');
  expect(uploadTarget.getAttribute('tabindex')).toBe('-1');

  const close = screen.getByRole('button', { name: 'Close dialog' });
  await waitFor(() => {
    expect(close.hasAttribute('disabled')).toBe(true);
  });
  expect(currentSearch.action).toBe('upload');
  expect(navigateMock).not.toHaveBeenCalled();
  fireEvent.click(close);
  expect(dialog.isConnected).toBe(true);
});

test('keeps an inline upload observed and blocks a duplicate after the upload action opens', async () => {
  await mockPluginPageWithRealUpload();
  const request = deferred<{
    json: () => Promise<queries.UploadWasmResponse>;
  }>();
  fetchWithAuthMock.mockReturnValue(request.promise);

  renderWithProviders();

  const file = new File(['first upload'], 'first.wasm', {
    type: 'application/wasm',
  });
  fireEvent.change(screen.getByLabelText('Plugin file'), {
    target: { files: [file] },
  });
  await waitFor(() => {
    expect(fetchWithAuthMock).toHaveBeenCalledTimes(1);
  });

  openUploadAction();
  const dialog = await screen.findByRole('dialog', { name: 'Upload plugin' });
  const modalFileInput = within(dialog).getByLabelText(
    'Plugin file',
  ) as HTMLInputElement;
  expect(modalFileInput.disabled).toBe(true);

  fireEvent.change(modalFileInput, {
    target: {
      files: [
        new File(['duplicate upload'], 'duplicate.wasm', {
          type: 'application/wasm',
        }),
      ],
    },
  });
  expect(fetchWithAuthMock).toHaveBeenCalledTimes(1);

  await act(async () => {
    request.resolve({
      json: async () => ({
        id: 'uploaded-plugin',
        sha256_hex: 'abcd',
        size_bytes: file.size,
        original_filename: file.name,
        revision: 0,
        idempotent: false,
        action: 'created',
      }),
    });
  });

  await waitFor(() => {
    expect(toast.success).toHaveBeenCalledWith('Uploaded first.wasm');
    expect(currentSearch.plugin).toBe('uploaded-plugin');
    expect(currentSearch.action).toBeUndefined();
  });
});

test('reports a deferred inline upload failure after the upload action opens', async () => {
  await mockPluginPageWithRealUpload();
  const request = deferred<{
    json: () => Promise<queries.UploadWasmResponse>;
  }>();
  fetchWithAuthMock.mockReturnValue(request.promise);

  renderWithProviders();

  fireEvent.change(screen.getByLabelText('Plugin file'), {
    target: {
      files: [
        new File(['failed upload'], 'failed.wasm', {
          type: 'application/wasm',
        }),
      ],
    },
  });
  await waitFor(() => {
    expect(fetchWithAuthMock).toHaveBeenCalledTimes(1);
  });

  openUploadAction();
  await screen.findByRole('dialog', { name: 'Upload plugin' });
  await act(async () => {
    request.reject(new Error('upload failed'));
  });

  await waitFor(() => {
    expect(toast.error).toHaveBeenCalledWith('upload failed');
    expect(currentSearch.action).toBeUndefined();
  });
});

test('keeps deferred inline replacement confirmation actionable after the upload action opens', async () => {
  await mockPluginPageWithRealUpload();
  const initialRequest = deferred<{
    json: () => Promise<queries.UploadWasmResponse>;
  }>();
  const replacementRequest = deferred<{
    json: () => Promise<queries.UploadWasmResponse>;
  }>();
  fetchWithAuthMock
    .mockReturnValueOnce(initialRequest.promise)
    .mockReturnValueOnce(replacementRequest.promise);

  renderWithProviders();

  const file = new File(['replacement upload'], 'replacement.wasm', {
    type: 'application/wasm',
  });
  fireEvent.change(screen.getByLabelText('Plugin file'), {
    target: { files: [file] },
  });
  await waitFor(() => {
    expect(fetchWithAuthMock).toHaveBeenCalledTimes(1);
  });

  openUploadAction();
  await screen.findByRole('dialog', { name: 'Upload plugin' });
  await act(async () => {
    initialRequest.reject(
      new ApiError(
        409,
        'replacement_confirmation_required',
        {
          name: 'replacement',
          error: 'replacement_confirmation_required',
          replace_registry_id: 'reg-1',
          expected_revision: 7,
          current_version: '1.0.0',
          incoming_version: '1.1.0',
          current_sha256_hex: 'aaaa',
          incoming_sha256_hex: 'bbbb',
        },
        'replacement_confirmation_required',
      ),
    );
  });

  expect(await screen.findByText('Confirm Plugin Replacement')).toBeDefined();
  fireEvent.click(screen.getByRole('button', { name: 'Replace' }));

  await waitFor(() => {
    expect(fetchWithAuthMock).toHaveBeenCalledTimes(2);
  });
  const replacementInit = fetchWithAuthMock.mock.calls[1]?.[1] as {
    body: FormData;
  };
  expect(replacementInit.body.get('bytes')).toBe(file);
  expect(replacementInit.body.get('confirm_replacement')).toBe('true');
  expect(replacementInit.body.get('replace_registry_id')).toBe('reg-1');
  expect(replacementInit.body.get('expected_revision')).toBe('7');

  await act(async () => {
    replacementRequest.resolve({
      json: async () => ({
        id: 'replaced-plugin',
        sha256_hex: 'bbbb',
        size_bytes: file.size,
        original_filename: file.name,
        revision: 8,
        idempotent: false,
        action: 'replaced',
      }),
    });
  });

  await waitFor(() => {
    expect(toast.success).toHaveBeenCalledWith('Replaced replacement.wasm');
    expect(currentSearch.plugin).toBe('replaced-plugin');
  });
});

test('shows references, cascade deletes, and returns a deleted detail to catalog', async () => {
  initialSearch = { plugin: pluginEntry.id };
  const deleteMutateMock = vi.fn();
  deleteMutateMock.mockResolvedValue(undefined);
  vi.mocked(queries.usePluginRegistry).mockReturnValue({
    data: {
      entries: [
        {
          ...pluginEntry,
        },
      ],
    },
    isLoading: false,
  } as unknown as ReturnType<typeof queries.usePluginRegistry>);
  vi.mocked(queries.useUploadWasm).mockReturnValue({
    mutate: vi.fn(),
    isPending: false,
  } as unknown as ReturnType<typeof queries.useUploadWasm>);
  vi.mocked(queries.useDeletePlugin).mockReturnValue({
    mutateAsync: deleteMutateMock,
    isPending: false,
  } as never);
  vi.mocked(queries.useGcPlugins).mockReturnValue({
    mutate: vi.fn(),
  } as unknown as ReturnType<typeof queries.useGcPlugins>);
  vi.mocked(queries.usePluginReferences).mockReturnValue({
    data: {
      registry: pluginEntry,
      refcount: 2,
      reference_fingerprint: 'fingerprint-123',
      references: [
        {
          kind: 'plugin_chain',
          principal_id: 'p-1',
          principal_name: 'principal-1',
          revision: 1,
          slot: 'router',
        },
        {
          kind: 'upstream_warmup_dialect',
          revision: 2,
          upstream_id: 'u-1',
          upstream_name: 'upstream-1',
        },
      ],
    },
    isLoading: false,
  } as unknown as ReturnType<typeof queries.usePluginReferences>);

  renderWithProviders();

  const deleteBtn = screen.getByRole('button', { name: 'Delete Plugin' });
  fireEvent.click(deleteBtn);

  expect(
    await screen.findByText('Delete Plugin and Remove References?'),
  ).toBeDefined();
  expect(screen.getByText('Principal principal-1 (router)')).toBeDefined();
  expect(screen.getByText('Upstream upstream-1 warmup')).toBeDefined();

  fireEvent.click(
    screen.getByRole('button', { name: 'Delete and remove uses' }),
  );

  await waitFor(() => {
    expect(deleteMutateMock).toHaveBeenCalledWith({
      id: 'plugin-1',
      revision: 1,
      cascade: true,
      referenceFingerprint: 'fingerprint-123',
    });
  });
  await waitFor(() => {
    expect(currentSearch.plugin).toBeUndefined();
    expect(toast.success).toHaveBeenCalledWith('Plugin deleted');
    expect(screen.getByText('Plugin library')).toBeDefined();
  });
});

test('shows catalog and opens detail view', async () => {
  vi.mocked(queries.usePluginRegistry).mockReturnValue({
    data: {
      entries: [
        {
          ...pluginEntry,
          is_builtin: true,
          description: 'Test description',
          usage: 'Test usage',
          hook_metadata: {
            test_hook: {
              wire_version: 1,
              description: 'Test hook description',
              usage: 'Test hook usage',
            },
          },
        },
      ],
    },
    isLoading: false,
  } as unknown as ReturnType<typeof queries.usePluginRegistry>);
  vi.mocked(queries.useUploadWasm).mockReturnValue({
    mutate: vi.fn(),
    isPending: false,
  } as unknown as ReturnType<typeof queries.useUploadWasm>);
  vi.mocked(queries.useDeletePlugin).mockReturnValue({
    mutateAsync: vi.fn(),
  } as never);
  vi.mocked(queries.useGcPlugins).mockReturnValue({
    mutate: vi.fn(),
  } as unknown as ReturnType<typeof queries.useGcPlugins>);
  vi.mocked(queries.usePluginReferences).mockReturnValue({
    data: null,
    isLoading: false,
  } as unknown as ReturnType<typeof queries.usePluginReferences>);

  renderWithProviders();

  expect(screen.getByText('My Plugin')).toBeDefined();
  expect(screen.getByText('Built-in')).toBeDefined();
  expect(screen.getByText('Router')).toBeDefined();
  expect(screen.getByText('Test description')).toBeDefined();

  fireEvent.click(screen.getByRole('button', { name: 'Inspect' }));

  expect(await screen.findByText('Back to Catalog')).toBeDefined();
  expect(screen.getByText('What this plugin does')).toBeDefined();
  expect(screen.getByText('Test usage')).toBeDefined();
  expect(screen.getByText('test_hook')).toBeDefined();
  expect(screen.getByText('Test hook description')).toBeDefined();
  expect(screen.getByText('File details')).toBeDefined();
  expect(screen.getByText('Used by')).toBeDefined();
  expect(screen.getByText('Use this plugin')).toBeDefined();
  expect(screen.getByText('Manage this plugin')).toBeDefined();
  expect(screen.getByText(/Updating this plugin/)).toBeDefined();
  expect(screen.getByText(/cc_lb_plugin_call_duration_seconds/)).toBeDefined();
});
