import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from '@testing-library/react';
import type React from 'react';
import { useState } from 'react';
import { afterEach, beforeEach, expect, test, vi } from 'vitest';
import { ApiError } from '../lib/api';
import * as queries from '../lib/queries';
import { Route } from './plugins';

type SearchState = {
  readonly plugin?: string;
};

type NavigateArgs = {
  readonly search?: SearchState;
};

let setMockSearch: React.Dispatch<React.SetStateAction<SearchState>> | null =
  null;

vi.mock('@tanstack/react-router', async () => {
  const actual = await vi.importActual<typeof import('@tanstack/react-router')>(
    '@tanstack/react-router',
  );
  return {
    ...actual,
    useNavigate:
      () =>
      ({ search }: NavigateArgs) => {
        if (setMockSearch && search) setMockSearch(search);
      },
  };
});

vi.mock('../lib/queries', async () => {
  const actual =
    await vi.importActual<typeof import('../lib/queries')>('../lib/queries');
  return {
    ...actual,
    usePluginRegistry: vi.fn(),
    useUploadWasm: vi.fn(),
    useDeletePlugin: vi.fn(),
    useGcPlugins: vi.fn(),
    usePluginStatus: vi.fn(),
    usePrincipals: vi.fn(),
    usePluginChain: vi.fn(),
    usePluginReferences: vi.fn(),
  };
});

const queryClient = new QueryClient({
  defaultOptions: { queries: { retry: false } },
});

type UploadVariables = Parameters<
  ReturnType<typeof queries.useUploadWasm>['mutate']
>[0];
type UploadOptions = Parameters<
  ReturnType<typeof queries.useUploadWasm>['mutate']
>[1];
type UploadOnError = Extract<
  NonNullable<UploadOptions>['onError'],
  (...args: never[]) => unknown
>;
type UploadErrorContext = Parameters<UploadOnError>[3];

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
  const [search, setSearch] = useState<SearchState>({});
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
});

afterEach(() => {
  cleanup();
});

test('uploads plugin', async () => {
  const mutateMock = vi.fn();
  vi.mocked(queries.usePluginRegistry).mockReturnValue({
    data: { entries: [] },
    isLoading: false,
  } as unknown as ReturnType<typeof queries.usePluginRegistry>);
  vi.mocked(queries.useUploadWasm).mockReturnValue({
    mutate: mutateMock,
    isPending: false,
  } as unknown as ReturnType<typeof queries.useUploadWasm>);
  vi.mocked(queries.useDeletePlugin).mockReturnValue({
    mutate: vi.fn(),
  } as unknown as ReturnType<typeof queries.useDeletePlugin>);
  vi.mocked(queries.useGcPlugins).mockReturnValue({
    mutate: vi.fn(),
  } as unknown as ReturnType<typeof queries.useGcPlugins>);
  vi.mocked(queries.usePluginReferences).mockReturnValue({
    data: null,
    isLoading: false,
  } as unknown as ReturnType<typeof queries.usePluginReferences>);

  renderWithProviders();

  const fileInput = document.getElementById(
    'btn-upload-wasm',
  ) as HTMLInputElement;
  const file = new File(['dummy content'], 'test.wasm', {
    type: 'application/wasm',
  });

  fireEvent.change(fileInput, { target: { files: [file] } });

  await waitFor(() => {
    expect(mutateMock).toHaveBeenCalledWith({ file }, expect.anything());
  });
});

test('handles replacement confirmation retry', async () => {
  let onErrorCallback: NonNullable<UploadOptions>['onError'] | undefined;
  const mutateMock = vi
    .fn()
    .mockImplementation((vars: UploadVariables, options: UploadOptions) => {
      if (!vars.confirmReplacement) {
        onErrorCallback = options?.onError;
      }
    });

  vi.mocked(queries.usePluginRegistry).mockReturnValue({
    data: { entries: [] },
    isLoading: false,
  } as unknown as ReturnType<typeof queries.usePluginRegistry>);
  vi.mocked(queries.useUploadWasm).mockReturnValue({
    mutate: mutateMock,
    isPending: false,
  } as unknown as ReturnType<typeof queries.useUploadWasm>);
  vi.mocked(queries.useDeletePlugin).mockReturnValue({
    mutate: vi.fn(),
  } as unknown as ReturnType<typeof queries.useDeletePlugin>);
  vi.mocked(queries.useGcPlugins).mockReturnValue({
    mutate: vi.fn(),
  } as unknown as ReturnType<typeof queries.useGcPlugins>);
  vi.mocked(queries.usePluginReferences).mockReturnValue({
    data: null,
    isLoading: false,
  } as unknown as ReturnType<typeof queries.usePluginReferences>);

  renderWithProviders();

  const fileInput = document.getElementById(
    'btn-upload-wasm',
  ) as HTMLInputElement;
  const file = new File(['dummy content'], 'test.wasm', {
    type: 'application/wasm',
  });

  fireEvent.change(fileInput, { target: { files: [file] } });

  await waitFor(() => {
    expect(mutateMock).toHaveBeenCalled();
  });

  onErrorCallback?.(
    new ApiError(
      409,
      'replacement_confirmation_required',
      {
        name: 'test',
        error: 'replacement_confirmation_required',
        replace_registry_id: 'reg-1',
        expected_revision: 1,
        current_version: '1.0.0',
        incoming_version: '1.0.1',
        current_sha256_hex: 'aaaa',
        incoming_sha256_hex: 'bbbb',
      },
      'replacement_confirmation_required',
    ),
    { file },
    undefined,
    {} as UploadErrorContext,
  );

  expect(await screen.findByText('Confirm Plugin Replacement')).toBeDefined();
  expect(screen.getByText(/1\.0\.0/)).toBeDefined();
  expect(screen.getByText(/1\.0\.1/)).toBeDefined();

  fireEvent.click(screen.getByRole('button', { name: 'Replace' }));

  await waitFor(() => {
    expect(mutateMock).toHaveBeenCalledWith(
      {
        file,
        confirmReplacement: true,
        replaceRegistryId: 'reg-1',
        expectedRevision: 1,
      },
      expect.anything(),
    );
  });
});

test('shows references preview and cascade deletes', async () => {
  const deleteMutateMock = vi.fn();
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
    mutate: deleteMutateMock,
  } as unknown as ReturnType<typeof queries.useDeletePlugin>);
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

  const deleteBtn = screen.getByRole('button', { name: 'Delete plugin' });
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
    expect(deleteMutateMock).toHaveBeenCalledWith(
      {
        id: 'plugin-1',
        revision: 1,
        cascade: true,
        referenceFingerprint: 'fingerprint-123',
      },
      expect.anything(),
    );
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
    mutate: vi.fn(),
  } as unknown as ReturnType<typeof queries.useDeletePlugin>);
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
