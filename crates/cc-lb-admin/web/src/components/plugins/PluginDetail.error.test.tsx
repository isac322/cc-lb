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
  screen,
  waitFor,
  within,
} from '@testing-library/react';
import { afterEach, expect, test, vi } from 'vitest';
import { type PluginEntry, type PluginReference, qk } from '../../lib/queries';
import { PluginDetail } from './PluginDetail';

const clients: QueryClient[] = [];
const emptyMessage = 'This plugin is uploaded but not used anywhere yet.';
const plugin: PluginEntry = {
  id: 'plugin-1',
  name: 'First plugin',
  original_filename: 'plugin-1.wasm',
  description: 'First plugin description',
  usage: 'First plugin usage',
  hook_metadata: {},
  sha256_hex: 'a'.repeat(64),
  label: null,
  metadata: null,
  size_bytes: 1024,
  refcount: 1,
  revision: 1,
  uploaded_at_unix_secs: 1_700_000_000,
  supported_slots: ['router'],
};
const references: PluginReference[] = [
  {
    kind: 'plugin_chain',
    chain_entry_id: 'chain-1',
    principal_id: 'principal-1',
    principal_name: 'Production principal',
    slot: 'router',
    revision: 1,
  },
];

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json' },
  });
}

function usageResponse(rows = references): Response {
  return json({
    registry: plugin,
    refcount: rows.length,
    reference_fingerprint: 'references-v1',
    references: rows,
  });
}

function stubApi(referenceRequest: () => Promise<Response>) {
  vi.stubGlobal(
    'fetch',
    vi.fn(async (input: RequestInfo | URL) => {
      const path = typeof input === 'string' ? input : input.toString();
      if (path === '/admin/v1/upstreams') return json({ upstreams: [] });
      if (path === '/admin/v1/plugins/registry/plugin-1/references') {
        return referenceRequest();
      }
      throw new Error(`Unexpected request: ${path}`);
    }),
  );
}

function renderDetail() {
  const client = new QueryClient({
    defaultOptions: {
      queries: { retry: false, gcTime: Infinity },
      mutations: { retry: false },
    },
  });
  clients.push(client);
  render(
    <QueryClientProvider client={client}>
      <PluginDetail plugin={plugin} onBack={vi.fn()} />
    </QueryClientProvider>,
  );
  return client;
}

afterEach(() => {
  cleanup();
  for (const client of clients) client.clear();
  clients.length = 0;
  onlineManager.setOnline(true);
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

test('QA-10: an initial usage failure is not unused and Retry loads references through skeletons', async () => {
  const recovery = Promise.withResolvers<Response>();
  const referenceRequest = vi
    .fn<() => Promise<Response>>()
    .mockResolvedValueOnce(json({ message: 'Usage unavailable' }, 503))
    .mockReturnValueOnce(recovery.promise);
  stubApi(referenceRequest);
  renderDetail();

  const alert = await screen.findByRole('alert');
  expect(alert.textContent).toContain('Could not load plugin usage');
  expect(alert.textContent).toContain(
    'Reference details could not be fetched.',
  );
  expect(alert.getAttribute('data-tone')).toBe('danger');
  expect(screen.getAllByRole('alert')).toHaveLength(1);
  expect(screen.queryByText(emptyMessage)).toBeNull();
  const retry = within(alert).getByRole('button', { name: 'Retry' });
  expect(retry.hasAttribute('disabled')).toBe(false);

  fireEvent.click(retry);
  await waitFor(() => {
    expect(screen.queryAllByRole('alert')).toHaveLength(0);
    expect(
      screen.getByTestId('plugin-used-by-slot').querySelectorAll('.skeleton'),
    ).toHaveLength(2);
  });
  expect(screen.queryByText(emptyMessage)).toBeNull();
  expect(screen.queryByRole('button', { name: 'Retry' })).toBeNull();

  await act(async () => recovery.resolve(usageResponse()));
  const reference = await screen.findByRole('link', {
    name: 'Production principal',
  });
  expect(reference.getAttribute('href')).toBe(
    '/principals?selectedId=principal-1',
  );
  expect(screen.queryAllByRole('alert')).toHaveLength(0);
  expect(screen.queryByRole('button', { name: 'Retry' })).toBeNull();
  expect(screen.queryByText(emptyMessage)).toBeNull();
  expect(referenceRequest).toHaveBeenCalledTimes(2);
});

test('QA-10: an offline initial usage Retry keeps skeletons until references recover on reconnect', async () => {
  const referenceRequest = vi
    .fn<() => Promise<Response>>()
    .mockResolvedValueOnce(json({ message: 'Usage unavailable' }, 503))
    .mockResolvedValueOnce(usageResponse());
  stubApi(referenceRequest);
  const client = renderDetail();
  const alert = await screen.findByRole('alert');

  act(() => onlineManager.setOnline(false));
  fireEvent.click(within(alert).getByRole('button', { name: 'Retry' }));
  await waitFor(() => {
    expect(client.getQueryState(qk.pluginReferences(plugin.id))).toMatchObject({
      status: 'pending',
      fetchStatus: 'paused',
      data: undefined,
      error: null,
    });
    expect(
      screen.getByTestId('plugin-used-by-slot').querySelectorAll('.skeleton'),
    ).toHaveLength(2);
    expect(screen.queryByText(emptyMessage)).toBeNull();
    expect(screen.queryAllByRole('alert')).toHaveLength(0);
  });
  expect(referenceRequest).toHaveBeenCalledTimes(1);

  act(() => onlineManager.setOnline(true));
  const reference = await screen.findByRole('link', {
    name: 'Production principal',
  });
  expect(reference.getAttribute('href')).toBe(
    '/principals?selectedId=principal-1',
  );
  expect(
    screen.getByTestId('plugin-used-by-slot').querySelector('.skeleton'),
  ).toBeNull();
  expect(screen.queryByText(emptyMessage)).toBeNull();
  expect(screen.queryAllByRole('alert')).toHaveLength(0);
  expect(referenceRequest).toHaveBeenCalledTimes(2);
});

test('QA-10: a stale usage error retains references with a busy Retry until refresh recovers', async () => {
  const recovery = Promise.withResolvers<Response>();
  const referenceRequest = vi
    .fn<() => Promise<Response>>()
    .mockResolvedValueOnce(usageResponse())
    .mockResolvedValueOnce(json({ message: 'Usage unavailable' }, 503))
    .mockReturnValueOnce(recovery.promise);
  stubApi(referenceRequest);
  const client = renderDetail();
  await screen.findByRole('link', { name: 'Production principal' });

  await act(async () => {
    await client.refetchQueries({ queryKey: qk.pluginReferences(plugin.id) });
  });
  const warning = await screen.findByRole('status');
  expect(warning.textContent).toContain('Could not refresh plugin usage');
  expect(warning.textContent).toContain('Showing the last loaded references.');
  expect(warning.getAttribute('data-tone')).toBe('warning');
  expect(
    screen.getByRole('link', { name: 'Production principal' }),
  ).toBeDefined();
  expect(screen.queryAllByRole('alert')).toHaveLength(0);
  expect(screen.queryByText(emptyMessage)).toBeNull();

  fireEvent.click(within(warning).getByRole('button', { name: 'Retry' }));
  await waitFor(() => {
    const retry = screen.getByRole('button', { name: 'Retry' });
    expect(retry.getAttribute('aria-busy')).toBe('true');
    expect(retry.hasAttribute('disabled')).toBe(true);
  });
  expect(
    screen.getByRole('link', { name: 'Production principal' }),
  ).toBeDefined();
  expect(
    screen.getByTestId('plugin-used-by-slot').querySelector('.skeleton'),
  ).toBeNull();

  await act(async () => recovery.resolve(usageResponse()));
  await waitFor(() => {
    expect(screen.queryByText('Could not refresh plugin usage')).toBeNull();
    expect(screen.queryByRole('button', { name: 'Retry' })).toBeNull();
  });
  expect(
    screen.getByRole('link', { name: 'Production principal' }),
  ).toBeDefined();
  expect(screen.queryAllByRole('alert')).toHaveLength(0);
  expect(referenceRequest).toHaveBeenCalledTimes(3);
});

test('a healthy empty usage response still explains that the plugin is unused', async () => {
  stubApi(async () => usageResponse([]));
  renderDetail();

  expect(await screen.findByText(emptyMessage)).toBeDefined();
  expect(screen.queryAllByRole('alert')).toHaveLength(0);
  expect(screen.queryAllByRole('button', { name: 'Retry' })).toHaveLength(0);
  expect(
    screen.getByTestId('plugin-used-by-slot').querySelector('.skeleton'),
  ).toBeNull();
});
