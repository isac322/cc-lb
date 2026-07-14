import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from '@testing-library/react';
import { afterEach, beforeEach, expect, test, vi } from 'vitest';
import { ApiError } from '../lib/api';
import * as queries from '../lib/queries';
import { Route } from './plugins';

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
  sha256_hex: 'abcd',
  size_bytes: 100,
  refcount: 2,
  revision: 1,
  uploaded_at_unix_secs: 0,
};

function renderWithProviders(ui: React.ReactElement) {
  return render(
    <QueryClientProvider client={queryClient}>{ui}</QueryClientProvider>,
  );
}

beforeEach(() => {
  vi.clearAllMocks();
});

afterEach(() => {
  cleanup();
});

test('uploads plugin with selected slot kind', async () => {
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

  renderWithProviders(<Component />);

  const select = screen.getByRole('combobox', { name: 'Upload slot kind' });
  fireEvent.change(select, { target: { value: 'shape' } });

  const fileInput = document.getElementById(
    'btn-upload-wasm',
  ) as HTMLInputElement;
  const file = new File(['dummy content'], 'test.wasm', {
    type: 'application/wasm',
  });

  fireEvent.change(fileInput, { target: { files: [file] } });

  await waitFor(() => {
    expect(mutateMock).toHaveBeenCalledWith(
      { file, slotKind: 'shape' },
      expect.anything(),
    );
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

  renderWithProviders(<Component />);

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
    { file, slotKind: 'router' },
    undefined,
    {} as UploadErrorContext,
  );

  // Dialog should appear
  expect(await screen.findByText('Confirm Plugin Replacement')).toBeDefined();
  expect(screen.getByText(/1\.0\.0/)).toBeDefined();
  expect(screen.getByText(/1\.0\.1/)).toBeDefined();

  // Click replace
  fireEvent.click(screen.getByRole('button', { name: 'Replace' }));

  await waitFor(() => {
    expect(mutateMock).toHaveBeenCalledWith(
      {
        file,
        slotKind: 'router',
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

  renderWithProviders(<Component />);

  // Click delete button
  const deleteBtn = screen.getByRole('button', { name: 'Delete plugin' });
  fireEvent.click(deleteBtn);

  // Dialog should appear
  expect(await screen.findByText('Cascade Delete Plugin?')).toBeDefined();
  expect(
    screen.getByText('Chain entry for principal-1 (slot: router)'),
  ).toBeDefined();
  expect(screen.getByText('Warmup dialect for upstream-1')).toBeDefined();

  // Click cascade delete
  fireEvent.click(screen.getByRole('button', { name: 'Cascade Delete' }));

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
