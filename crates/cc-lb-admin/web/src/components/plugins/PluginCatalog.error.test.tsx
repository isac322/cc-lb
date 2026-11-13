import {
  onlineManager,
  QueryClient,
  QueryClientProvider,
} from '@tanstack/react-query';
import {
  act,
  cleanup,
  fireEvent,
  render,
  renderHook,
  screen,
  waitFor,
  within,
} from '@testing-library/react';
import type { ReactNode } from 'react';
import { toast } from 'sonner';
import { afterEach, beforeEach, expect, test, vi } from 'vitest';
import {
  type PluginEntry,
  qk,
  usePluginReferences,
  usePluginRegistry,
} from '../../lib/queries';
import { queryClient } from '../../lib/queryClient';
import { PluginCatalog } from './PluginCatalog';
import { PluginsPage } from './PluginsPage';

let selectedPluginId: string | undefined;
const clients: QueryClient[] = [];
const navigate = vi.fn();

vi.mock('@tanstack/react-router', () => ({
  useNavigate: () => navigate,
}));

vi.mock('../../routes/plugins', () => ({
  Route: {
    id: '/plugins',
    useSearch: () => ({ plugin: selectedPluginId }),
  },
}));

const pluginEntry = (id: string, name: string): PluginEntry => ({
  id,
  sha256_hex: 'a'.repeat(64),
  name,
  original_filename: `${id}.wasm`,
  description: `${name} description`,
  usage: `${name} usage`,
  hook_metadata: {},
  label: null,
  size_bytes: 1024,
  refcount: 1,
  revision: 1,
  uploaded_at_unix_secs: 1_700_000_000,
  is_builtin: false,
  version: '1.0.0',
  metadata: null,
  supported_slots: ['router'],
});

const entries = [
  pluginEntry('plugin-1', 'First plugin'),
  pluginEntry('plugin-2', 'Second plugin'),
];

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json' },
  });
}

function unavailable(): Response {
  return json({ message: 'Plugin service unavailable' }, 503);
}

function stubApi(registry: () => Promise<Response>, referencesFail = false) {
  vi.stubGlobal(
    'fetch',
    vi.fn(async (input: RequestInfo | URL) => {
      const path = typeof input === 'string' ? input : input.toString();
      if (path === '/admin/v1/upstreams') return json({ upstreams: [] });
      if (path === '/admin/v1/plugins/registry') return registry();
      if (path === '/admin/v1/plugins/registry/plugin-1/references') {
        return referencesFail
          ? unavailable()
          : json({
              registry: entries[0],
              refcount: 0,
              reference_fingerprint: 'empty',
              references: [],
            });
      }
      throw new Error(`Unexpected request: ${path}`);
    }),
  );
}

function createClient() {
  const client = new QueryClient({
    defaultOptions: {
      queries: { retry: false, gcTime: Infinity },
      mutations: { retry: false },
    },
  });
  clients.push(client);
  return client;
}

function renderCatalog(client = createClient()) {
  return render(
    <QueryClientProvider client={client}>
      <PluginCatalog onSelectPlugin={vi.fn()} />
    </QueryClientProvider>,
  );
}

function expectLoadedRows() {
  expect(screen.getByTestId('plugin-count-slot').textContent).toBe(
    '2 available · 0 not used anywhere',
  );
  expect(screen.getAllByText('First plugin')).toHaveLength(2);
  expect(screen.getAllByText('Second plugin')).toHaveLength(2);
}

beforeEach(() => {
  selectedPluginId = undefined;
  navigate.mockReset();
});

afterEach(() => {
  cleanup();
  for (const client of clients) client.clear();
  clients.length = 0;
  queryClient.clear();
  onlineManager.setOnline(true);
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

test('QA-05: initial registry failure is unavailable, not an empty library, and cleanup remains usable', async () => {
  stubApi(async () => unavailable());
  renderCatalog();

  const alert = await screen.findByRole('alert');
  expect(alert.textContent).toContain('Could not load plugin library');
  expect(alert.textContent).toContain(
    'Check the admin connection and try again.',
  );
  const count = screen.getByTestId('plugin-count-slot');
  expect(count.textContent).toBe('Unavailable');
  expect(count.textContent).not.toContain('0 available');
  expect(screen.queryAllByText('No plugins uploaded.')).toHaveLength(0);
  expect(screen.getAllByRole('alert')).toHaveLength(1);
  expect(screen.queryByRole('table')).toBeNull();
  expect(screen.queryByRole('list')).toBeNull();
  const retry = screen.getByRole('button', { name: 'Retry' });
  expect(retry.hasAttribute('disabled')).toBe(false);
  const clean = screen.getByRole('button', { name: 'Clean orphaned uploads' });
  expect(clean.hasAttribute('disabled')).toBe(false);

  fireEvent.click(clean);
  const dialog = await screen.findByRole('alertdialog', {
    name: 'Clean orphaned uploads?',
  });
  expect(
    within(dialog).getByRole('button', { name: 'Delete orphaned uploads' }),
  ).toBeDefined();
});

test('QA-06: initial Retry shows skeletons, restores an error on 503, and recovers on 200', async () => {
  const failedRetry = Promise.withResolvers<Response>();
  const successfulRetry = Promise.withResolvers<Response>();
  const registry = vi
    .fn<() => Promise<Response>>()
    .mockResolvedValueOnce(unavailable())
    .mockReturnValueOnce(failedRetry.promise)
    .mockReturnValueOnce(successfulRetry.promise);
  stubApi(registry);
  const view = renderCatalog();
  await screen.findByRole('alert');

  fireEvent.click(screen.getByRole('button', { name: 'Retry' }));
  await waitFor(() => {
    expect(screen.queryAllByRole('alert')).toHaveLength(0);
    expect(screen.queryByText('Unavailable')).toBeNull();
    expect(
      screen.getByTestId('plugin-count-slot').querySelector('.skeleton'),
    ).not.toBeNull();
  });
  expect(view.container.querySelectorAll('.skeleton').length).toBeGreaterThan(
    1,
  );
  expect(screen.queryAllByText('No plugins uploaded.')).toHaveLength(0);

  await act(async () => failedRetry.resolve(unavailable()));
  expect((await screen.findByRole('alert')).textContent).toContain(
    'Could not load plugin library',
  );
  expect(screen.getByTestId('plugin-count-slot').textContent).toBe(
    'Unavailable',
  );

  fireEvent.click(screen.getByRole('button', { name: 'Retry' }));
  await waitFor(() => expect(screen.queryAllByRole('alert')).toHaveLength(0));
  await act(async () => successfulRetry.resolve(json({ entries })));
  await waitFor(expectLoadedRows);
  expect(screen.queryAllByRole('alert')).toHaveLength(0);
  expect(screen.queryByRole('button', { name: 'Retry' })).toBeNull();
  expect(registry).toHaveBeenCalledTimes(3);
});

test('QA-06: an offline initial Retry keeps count and row skeletons until reconnecting', async () => {
  const registry = vi
    .fn<() => Promise<Response>>()
    .mockResolvedValueOnce(unavailable())
    .mockResolvedValueOnce(json({ entries }));
  stubApi(registry);
  const client = createClient();
  const view = renderCatalog(client);
  await screen.findByRole('alert');

  act(() => onlineManager.setOnline(false));
  fireEvent.click(screen.getByRole('button', { name: 'Retry' }));

  await waitFor(() => {
    expect(client.getQueryState(qk.pluginRegistry)).toMatchObject({
      status: 'pending',
      fetchStatus: 'paused',
      data: undefined,
      error: null,
    });
    const count = screen.getByTestId('plugin-count-slot');
    expect(count.querySelector('.skeleton')).not.toBeNull();
    expect(count.textContent).not.toContain('0 available');
    expect(count.textContent).not.toContain('Unavailable');
    expect(screen.queryAllByText('No plugins uploaded.')).toHaveLength(0);
    expect(screen.queryAllByRole('alert')).toHaveLength(0);
    expect(
      view.container.querySelectorAll('ul > li[aria-hidden="true"]'),
    ).toHaveLength(3);
    expect(
      view.container.querySelectorAll('tbody .skeleton').length,
    ).toBeGreaterThan(0);
  });
  expect(registry).toHaveBeenCalledTimes(1);

  act(() => onlineManager.setOnline(true));
  await waitFor(expectLoadedRows);
  expect(screen.queryAllByRole('alert')).toHaveLength(0);
  expect(screen.queryAllByText('No plugins uploaded.')).toHaveLength(0);
  expect(view.container.querySelector('.skeleton')).toBeNull();
  expect(registry).toHaveBeenCalledTimes(2);
});

test('QA-07: stale registry errors retain rows and count and keep Retry busy until recovery', async () => {
  const recovery = Promise.withResolvers<Response>();
  const registry = vi
    .fn<() => Promise<Response>>()
    .mockResolvedValueOnce(json({ entries }))
    .mockResolvedValueOnce(unavailable())
    .mockReturnValueOnce(recovery.promise);
  stubApi(registry);
  const client = createClient();
  renderCatalog(client);
  await waitFor(expectLoadedRows);

  await act(async () => {
    await client.refetchQueries({ queryKey: qk.pluginRegistry });
  });
  const warning = await screen.findByRole('status');
  expect(warning.textContent).toContain('Could not refresh plugin library');
  expect(warning.textContent).toContain('Showing the last loaded plugin list.');
  expect(warning.getAttribute('data-tone')).toBe('warning');
  expectLoadedRows();
  expect(screen.queryAllByRole('alert')).toHaveLength(0);

  fireEvent.click(within(warning).getByRole('button', { name: 'Retry' }));
  await waitFor(() => {
    const retry = screen.getByRole('button', { name: 'Retry' });
    expect(retry.getAttribute('aria-busy')).toBe('true');
    expect(retry.hasAttribute('disabled')).toBe(true);
  });
  expectLoadedRows();
  await act(async () => recovery.resolve(json({ entries })));
  await waitFor(() => {
    expect(screen.queryByText('Could not refresh plugin library')).toBeNull();
    expect(screen.queryByRole('button', { name: 'Retry' })).toBeNull();
  });
  expectLoadedRows();
  expect(registry).toHaveBeenCalledTimes(3);
});

test('QA-07: an offline background Retry preserves cached rows and count until recovery', async () => {
  const recovery = Promise.withResolvers<Response>();
  const registry = vi
    .fn<() => Promise<Response>>()
    .mockResolvedValueOnce(json({ entries }))
    .mockResolvedValueOnce(unavailable())
    .mockReturnValueOnce(recovery.promise);
  stubApi(registry);
  const client = createClient();
  const view = renderCatalog(client);
  await waitFor(expectLoadedRows);

  await act(async () => {
    await client.refetchQueries({ queryKey: qk.pluginRegistry });
  });
  const warning = await screen.findByRole('status');
  expect(warning.textContent).toContain('Could not refresh plugin library');

  act(() => onlineManager.setOnline(false));
  fireEvent.click(within(warning).getByRole('button', { name: 'Retry' }));
  await waitFor(() => {
    expect(client.getQueryState(qk.pluginRegistry)?.fetchStatus).toBe('paused');
    expectLoadedRows();
    expect(view.container.querySelector('.skeleton')).toBeNull();
    expect(screen.queryAllByText('No plugins uploaded.')).toHaveLength(0);
    expect(screen.queryAllByRole('alert')).toHaveLength(0);
  });
  expect(screen.getByText('Could not refresh plugin library')).toBeDefined();
  expect(registry).toHaveBeenCalledTimes(2);

  act(() => onlineManager.setOnline(true));
  await waitFor(() => expect(registry).toHaveBeenCalledTimes(3));
  expectLoadedRows();
  expect(view.container.querySelector('.skeleton')).toBeNull();
  await act(async () => recovery.resolve(json({ entries })));
  await waitFor(() => {
    expect(screen.queryByText('Could not refresh plugin library')).toBeNull();
    expect(screen.queryByRole('button', { name: 'Retry' })).toBeNull();
  });
  expectLoadedRows();
  expect(screen.queryAllByText('No plugins uploaded.')).toHaveLength(0);
  expect(view.container.querySelector('.skeleton')).toBeNull();
});

test('QA-08: a healthy empty registry still renders its genuine empty state', async () => {
  stubApi(async () => json({ entries: [] }));
  renderCatalog();

  await waitFor(() => {
    expect(screen.getByTestId('plugin-count-slot').textContent).toBe(
      '0 available · 0 not used anywhere',
    );
  });
  expect(screen.getAllByText('No plugins uploaded.')).toHaveLength(2);
  expect(screen.queryAllByRole('alert')).toHaveLength(0);
  expect(screen.queryAllByRole('button', { name: 'Retry' })).toHaveLength(0);
});

test('QA-09: a failed deep link retries through the detail skeleton and opens the recovered plugin', async () => {
  selectedPluginId = 'plugin-1';
  const failedRetry = Promise.withResolvers<Response>();
  const successfulRetry = Promise.withResolvers<Response>();
  const registry = vi
    .fn<() => Promise<Response>>()
    .mockResolvedValueOnce(unavailable())
    .mockReturnValueOnce(failedRetry.promise)
    .mockReturnValueOnce(successfulRetry.promise);
  stubApi(registry);
  const client = createClient();
  render(
    <QueryClientProvider client={client}>
      <PluginsPage />
    </QueryClientProvider>,
  );

  expect((await screen.findByRole('alert')).textContent).toContain(
    'Could not load plugin library',
  );
  fireEvent.click(screen.getByRole('button', { name: 'Retry' }));
  const skeleton = await screen.findByRole('status', {
    name: 'Loading plugin details',
  });
  expect(skeleton.querySelectorAll('.skeleton').length).toBeGreaterThan(10);
  expect(screen.queryByText('Plugin library')).toBeNull();
  expect(screen.queryAllByRole('alert')).toHaveLength(0);

  await act(async () => failedRetry.resolve(unavailable()));
  expect((await screen.findByRole('alert')).textContent).toContain(
    'Could not load plugin library',
  );
  fireEvent.click(screen.getByRole('button', { name: 'Retry' }));
  await screen.findByRole('status', { name: 'Loading plugin details' });
  await act(async () => successfulRetry.resolve(json({ entries })));

  expect(
    await screen.findByRole('heading', { name: 'First plugin' }),
  ).toBeDefined();
  expect(screen.getByRole('heading', { name: 'Used by' })).toBeDefined();
  expect(screen.queryByText('Plugin library')).toBeNull();
  expect(screen.queryAllByRole('alert')).toHaveLength(0);
  expect(
    screen.queryByRole('status', { name: 'Loading plugin details' }),
  ).toBeNull();
  expect(registry).toHaveBeenCalledTimes(3);
});

test('QA-09: an offline deep-link Retry stays in the detail skeleton until reconnecting', async () => {
  selectedPluginId = 'plugin-1';
  const registry = vi
    .fn<() => Promise<Response>>()
    .mockResolvedValueOnce(unavailable())
    .mockResolvedValueOnce(json({ entries }));
  stubApi(registry);
  const client = createClient();
  render(
    <QueryClientProvider client={client}>
      <PluginsPage />
    </QueryClientProvider>,
  );
  await screen.findByRole('alert');

  act(() => onlineManager.setOnline(false));
  fireEvent.click(screen.getByRole('button', { name: 'Retry' }));
  await waitFor(() => {
    expect(client.getQueryState(qk.pluginRegistry)).toMatchObject({
      status: 'pending',
      fetchStatus: 'paused',
      data: undefined,
      error: null,
    });
    const skeleton = screen.getByRole('status', {
      name: 'Loading plugin details',
    });
    expect(skeleton.querySelectorAll('.skeleton').length).toBeGreaterThan(10);
    expect(
      screen.getByRole('button', { name: 'Back to plugins' }),
    ).toBeDefined();
    expect(screen.queryByText('Plugin library')).toBeNull();
    expect(screen.queryByTestId('plugin-count-slot')).toBeNull();
    expect(screen.queryAllByText('No plugins uploaded.')).toHaveLength(0);
    expect(screen.queryAllByRole('alert')).toHaveLength(0);
  });
  expect(registry).toHaveBeenCalledTimes(1);

  act(() => onlineManager.setOnline(true));
  expect(
    await screen.findByRole('heading', { name: 'First plugin' }),
  ).toBeDefined();
  expect(screen.getByRole('heading', { name: 'Used by' })).toBeDefined();
  expect(screen.getByRole('button', { name: 'Back to plugins' })).toBeDefined();
  expect(screen.queryByText('Plugin library')).toBeNull();
  expect(
    screen.queryByRole('status', { name: 'Loading plugin details' }),
  ).toBeNull();
  expect(screen.queryAllByRole('alert')).toHaveLength(0);
  expect(registry).toHaveBeenCalledTimes(2);
});

test('QA-11: registry and reference failures preserve global error toasts for other callers', async () => {
  stubApi(async () => unavailable(), true);
  const toastError = vi.spyOn(toast, 'error').mockImplementation(() => 'toast');
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  );
  const { result } = renderHook(
    () => ({
      registry: usePluginRegistry(),
      references: usePluginReferences('plugin-1'),
    }),
    { wrapper },
  );

  await waitFor(
    () => {
      expect(result.current.registry.isError).toBe(true);
      expect(result.current.references.isError).toBe(true);
      expect(toastError).toHaveBeenCalledTimes(2);
    },
    { timeout: 3_000 },
  );
  const registry = queryClient
    .getQueryCache()
    .find({ queryKey: qk.pluginRegistry });
  const references = queryClient
    .getQueryCache()
    .find({ queryKey: qk.pluginReferences('plugin-1') });
  expect(registry).toBeDefined();
  expect(references).toBeDefined();
  expect(registry?.meta?.inlineError).toBeUndefined();
  expect(references?.meta?.inlineError).toBeUndefined();
  expect(toastError).toHaveBeenCalledWith('Plugin service unavailable');
});
