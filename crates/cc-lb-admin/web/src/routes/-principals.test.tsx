import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import type React from 'react';
import { toast } from 'sonner';
import { afterEach, beforeEach, expect, test, vi } from 'vitest';
import * as queries from '../lib/queries';
import {
  ApiKeysCard,
  RecentRequestsCard,
  Route,
  RouterSlotEditor,
} from './principals';

const navigateMock = vi.fn();

vi.mock('@tanstack/react-router', async () => {
  const actual = await vi.importActual('@tanstack/react-router');
  return {
    ...actual,
    useNavigate: () => navigateMock,
  };
});

vi.mock('../lib/queries', async () => {
  const actual = await vi.importActual<typeof queries>('../lib/queries');
  return {
    ...actual,
    usePluginChain: vi.fn(),
    usePluginRegistry: vi.fn(),
    useReorderChain: vi.fn(),
    useInsertChainEntry: vi.fn(),
    useDeleteChainEntry: vi.fn(),
    useRouterTerminalStrategy: vi.fn(),
    useUpdateRouterTerminalStrategy: vi.fn(),
    usePrincipals: vi.fn(),
    usePrincipalWritePending: vi.fn(),
    useCreatePrincipal: vi.fn(),
    useDeletePrincipal: vi.fn(),
    useTogglePrincipal: vi.fn(),
    useSetAllowedModels: vi.fn(),
    useUpdatePrincipalDefaultLimits: vi.fn(),
    useUpdatePrincipalCacheKeepalive: vi.fn(),
    useCacheKeepaliveSummary: vi.fn(),
    useRecentEvents: vi.fn(),
    usePrincipalNameMap: vi.fn(),
    useUpstreamNameMap: vi.fn(),
    usePrincipalKeys: vi.fn(),
    useIssueKey: vi.fn(),
    useRevokeKey: vi.fn(),
  };
});

const queryClient = new QueryClient({
  defaultOptions: { queries: { retry: false } },
});

const Component = Route.options.component as React.ComponentType;

const principal: queries.Principal = {
  id: 'p-1',
  name: 'Ada',
  kind: 'human',
  enabled: true,
  revision: 1,
  allowed_models: [],
  allowed_upstreams: [],
  default_limits: [],
  cache_keepalive: null,
};

function withProviders(ui: React.ReactElement) {
  return <QueryClientProvider client={queryClient}>{ui}</QueryClientProvider>;
}

function renderWithProviders(ui: React.ReactElement) {
  return render(withProviders(ui));
}

function principalFixture(
  overrides: Partial<queries.Principal> = {},
): queries.Principal {
  return {
    id: 'p-1',
    name: 'p-1',
    kind: 'machine',
    enabled: true,
    revision: 7,
    allowed_models: [],
    allowed_upstreams: [],
    default_limits: [],
    cache_keepalive: null,
    ...overrides,
  };
}

function renderPrincipalDetail(
  selectedPrincipal: queries.Principal = principalFixture(),
) {
  Object.assign(Route, {
    useSearch: () => ({ selectedId: selectedPrincipal.id }),
  });
  vi.mocked(queries.usePrincipals).mockReturnValue({
    data: { principals: [selectedPrincipal] },
    isLoading: false,
    isPending: false,
  } as never);
  return renderWithProviders(<Component />);
}

// Access rows are headed regions inside one card; every other heading names a
// card. Either way this scopes queries to the controls that heading owns.
function cardNamed(title: string) {
  const card = screen
    .getByRole('heading', { name: title })
    .closest('section[aria-labelledby], .glass');
  if (!card) throw new Error(`Card not found: ${title}`);
  return within(card as HTMLElement);
}

beforeEach(() => {
  vi.clearAllMocks();
  navigateMock.mockReset();
  queryClient.clear();
  Object.assign(Route, { useSearch: () => ({}) });
  vi.mocked(queries.usePrincipals).mockReturnValue({
    data: { principals: [] },
    isLoading: false,
    isPending: false,
  } as never);
  vi.mocked(queries.usePrincipalWritePending).mockReturnValue(0);
  vi.mocked(queries.usePluginChain).mockReturnValue({
    data: { entries: [] },
    isLoading: false,
    isPending: false,
  } as never);
  vi.mocked(queries.usePluginRegistry).mockReturnValue({
    data: { entries: [] },
    isLoading: false,
    isPending: false,
  } as never);
  vi.mocked(queries.useRouterTerminalStrategy).mockReturnValue({
    data: { strategy: 'first-pick', revision: 1 },
    isLoading: false,
    isPending: false,
  } as never);
  vi.mocked(queries.useRecentEvents).mockReturnValue({
    data: { events: [] },
    isLoading: false,
    isPending: false,
    isPlaceholderData: false,
  } as never);
  vi.mocked(queries.usePrincipalNameMap).mockReturnValue(new Map());
  vi.mocked(queries.useUpstreamNameMap).mockReturnValue(new Map());
  vi.mocked(queries.usePrincipalKeys).mockReturnValue({
    data: { keys: [] },
    isLoading: false,
    isPending: false,
  } as never);
  vi.mocked(queries.useCacheKeepaliveSummary).mockReturnValue({
    data: undefined,
    isLoading: false,
    isPending: false,
  } as never);

  vi.mocked(queries.useCreatePrincipal).mockReturnValue({
    mutate: vi.fn(),
    isPending: false,
  } as never);
  vi.mocked(queries.useDeletePrincipal).mockReturnValue({
    mutate: vi.fn(),
    isPending: false,
  } as never);
  vi.mocked(queries.useTogglePrincipal).mockReturnValue({
    mutate: vi.fn(),
    isPending: false,
  } as never);
  vi.mocked(queries.useSetAllowedModels).mockReturnValue({
    mutate: vi.fn(),
    isPending: false,
  } as never);
  vi.mocked(queries.useUpdatePrincipalDefaultLimits).mockReturnValue({
    mutate: vi.fn(),
    isPending: false,
  } as never);
  vi.mocked(queries.useUpdatePrincipalCacheKeepalive).mockReturnValue({
    mutate: vi.fn(),
    isPending: false,
  } as never);
  vi.mocked(queries.useReorderChain).mockReturnValue({
    mutate: vi.fn(),
    isPending: false,
  } as never);
  vi.mocked(queries.useInsertChainEntry).mockReturnValue({
    mutate: vi.fn(),
    isPending: false,
  } as never);
  vi.mocked(queries.useDeleteChainEntry).mockReturnValue({
    mutate: vi.fn(),
    isPending: false,
  } as never);
  vi.mocked(queries.useUpdateRouterTerminalStrategy).mockReturnValue({
    mutate: vi.fn(),
    isPending: false,
  } as never);
  vi.mocked(queries.useIssueKey).mockReturnValue({
    mutate: vi.fn(),
    isPending: false,
  } as never);
  vi.mocked(queries.useRevokeKey).mockReturnValue({
    mutate: vi.fn(),
    isPending: false,
  } as never);

  Object.defineProperty(window, 'matchMedia', {
    writable: true,
    value: vi.fn().mockReturnValue({
      matches: false,
      media: '',
      onchange: null,
      addListener: vi.fn(),
      removeListener: vi.fn(),
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
      dispatchEvent: vi.fn(),
    }),
  });
});

afterEach(() => {
  cleanup();
});

test('pending principals keep the mobile list, desktop detail shell, and shared row geometry', () => {
  vi.mocked(queries.usePrincipals).mockReturnValue({
    data: undefined,
    isLoading: true,
  } as never);

  const loadingView = renderWithProviders(<Component />);

  expect(screen.queryByText('0 total')).toBeNull();
  expect(screen.queryByText('Select a principal')).toBeNull();
  expect(screen.getByTestId('principal-count-skeleton')).toBeDefined();

  const rowGeometryClasses = [
    'w-full',
    'min-h-[72px]',
    'text-left',
    'p-3',
    'rounded-sm',
    'border',
  ];
  const loadingCards = screen.getAllByTestId('principal-list-skeleton');
  expect(loadingCards).toHaveLength(4);
  for (const card of loadingCards) {
    for (const className of rowGeometryClasses) {
      expect(card.className).toContain(className);
    }
    expect(card.querySelectorAll('.skeleton')).toHaveLength(4);
  }

  const listPane = screen.getByText('Principals').closest('aside');
  expect(listPane?.className).toContain('flex');
  expect(listPane?.className).not.toContain('hidden');

  const detailShell = screen.getByRole('status', {
    name: 'Loading principal details',
  });
  const detailPane = detailShell.closest('section');
  expect(detailPane?.className).toContain('hidden');
  expect(detailPane?.className).toContain('md:flex');
  expect(detailShell.querySelectorAll('.glass')).toHaveLength(7);
  expect(detailShell.querySelectorAll('.skeleton').length).toBeGreaterThan(40);

  loadingView.unmount();
  vi.mocked(queries.usePrincipals).mockReturnValue({
    data: { principals: [principal] },
    isLoading: false,
  } as never);

  renderWithProviders(<Component />);

  expect(screen.getByText('1 total')).toBeDefined();
  const loadedRow = screen.getByRole('button', { name: /Ada/ });
  for (const className of rowGeometryClasses) {
    expect(loadedRow.className).toContain(className);
  }
});

test('recent requests delegates pending geometry to the structured table', () => {
  vi.mocked(queries.usePrincipalNameMap).mockReturnValue(
    new Map([[principal.id, principal.name]]),
  );
  vi.mocked(queries.useUpstreamNameMap).mockReturnValue(new Map());
  vi.mocked(queries.useRecentEvents).mockReturnValue({
    data: undefined,
    isLoading: true,
    isPending: true,
    isPlaceholderData: false,
  } as never);

  const { container } = renderWithProviders(
    <RecentRequestsCard principal={principal} />,
  );

  expect(screen.queryByText(/Loading recent requests/)).toBeNull();
  expect(
    screen.queryByText('No recent requests for this principal'),
  ).toBeNull();
  expect(screen.getByTestId('recent-requests-subtitle-skeleton')).toBeDefined();
  const slot = screen.getByTestId('recent-requests-table-slot');
  expect(slot.className).toContain('min-h-48');
  // Column visibility belongs to RequestEventsTable; the card only needs every
  // skeleton row to fill the same columns as its header.
  const columnCount = slot.querySelectorAll('thead th').length;
  expect(columnCount).toBeGreaterThan(0);
  const rows = slot.querySelectorAll('tbody tr');
  expect(rows).toHaveLength(5);
  for (const row of rows) {
    expect(row.className).toContain('border-b');
    expect(row.querySelectorAll('td')).toHaveLength(columnCount);
  }
  expect(container.textContent).not.toContain('—');
});

test('recent requests asks the backend for messages only and hides other categories', () => {
  vi.mocked(queries.useRecentEvents).mockReturnValue({
    data: {
      events: [
        {
          duration_ms: 10,
          event_kind: 'messages',
          request_id: 'request-messages',
          status: 200,
          ts: 3,
        },
        {
          duration_ms: 10,
          event_kind: 'messages',
          request_id: 'request-renewal',
          source_kind: 'renewal',
          status: 200,
          ts: 2,
        },
        {
          duration_ms: 10,
          request_id: 'request-unclassified',
          status: 200,
          ts: 1,
        },
      ],
    },
    isLoading: false,
    isPending: false,
    isPlaceholderData: false,
  } as never);

  renderWithProviders(<RecentRequestsCard principal={principal} />);

  expect(queries.useRecentEvents).toHaveBeenCalledWith({
    principal_id: principal.id,
    limit: '5',
    event_kind: 'messages',
  });
  expect(screen.getByLabelText('View request request-messages')).toBeDefined();
  expect(screen.queryByLabelText('View request request-renewal')).toBeNull();
  expect(
    screen.queryByLabelText('View request request-unclassified'),
  ).toBeNull();
});

test('API key loading keeps the table header and per-column skeleton rows', () => {
  vi.mocked(queries.usePrincipalKeys).mockReturnValue({
    data: undefined,
    isLoading: true,
  } as never);
  vi.mocked(queries.useIssueKey).mockReturnValue({
    mutate: vi.fn(),
    isPending: false,
  } as never);
  vi.mocked(queries.useRevokeKey).mockReturnValue({
    mutate: vi.fn(),
    isPending: false,
  } as never);

  const { container } = renderWithProviders(
    <ApiKeysCard principal={principal} />,
  );

  expect(screen.queryByText('No API keys issued.')).toBeNull();
  const slot = screen.getByTestId('api-keys-table-slot');
  expect(slot.className).toContain('min-h-32');
  expect(slot.querySelectorAll('thead th')).toHaveLength(7);
  const rows = slot.querySelectorAll('tbody tr');
  expect(rows).toHaveLength(3);
  for (const row of rows) {
    expect(row.getAttribute('aria-hidden')).toBe('true');
    expect(row.className).toContain('border-row');
    expect(row.querySelectorAll('td')).toHaveLength(7);
    expect(row.querySelectorAll('.skeleton')).toHaveLength(7);
  }
  expect(container.textContent).not.toContain('—');
});

test('renders ordered list with locked terminal row', () => {
  vi.mocked(queries.usePluginChain).mockReturnValue({
    data: {
      entries: [
        {
          id: 'entry-1',
          order: 100,
          wasm_registry_id: 'plugin-1',
          revision: 1,
        },
      ],
    },
  } as unknown as ReturnType<typeof queries.usePluginChain>);
  vi.mocked(queries.usePluginRegistry).mockReturnValue({
    data: {
      entries: [
        {
          id: 'plugin-1',
          name: 'My Plugin',
          metadata: {
            purpose: 'Purpose',
            keeps: 'Keeps',
            drops: 'Drops',
            empty_behavior: 'Empty behavior',
            examples: [],
          },
          sha256_hex: '',
        },
        {
          id: 'subscription-preference-id',
          name: 'subscription-preference',
          metadata: null,
          sha256_hex: '',
        },
      ],
    },
  } as unknown as ReturnType<typeof queries.usePluginRegistry>);
  vi.mocked(queries.useRouterTerminalStrategy).mockReturnValue({
    data: { strategy: 'first-pick', revision: 1 },
    isLoading: false,
  } as unknown as ReturnType<typeof queries.useRouterTerminalStrategy>);
  vi.mocked(queries.useUpdateRouterTerminalStrategy).mockReturnValue({
    mutate: vi.fn(),
    isPending: false,
  } as unknown as ReturnType<typeof queries.useUpdateRouterTerminalStrategy>);
  vi.mocked(queries.useReorderChain).mockReturnValue({
    mutate: vi.fn(),
    isPending: false,
  } as unknown as ReturnType<typeof queries.useReorderChain>);
  vi.mocked(queries.useInsertChainEntry).mockReturnValue({
    mutate: vi.fn(),
    isPending: false,
  } as unknown as ReturnType<typeof queries.useInsertChainEntry>);
  vi.mocked(queries.useDeleteChainEntry).mockReturnValue({
    mutate: vi.fn(),
    isPending: false,
  } as unknown as ReturnType<typeof queries.useDeleteChainEntry>);

  renderWithProviders(<RouterSlotEditor principal={principalFixture()} />);

  expect(screen.getByText('My Plugin')).toBeDefined();
  expect(screen.getByText('Terminal step')).toBeDefined();
});

test('loading router metadata keeps Basic selected and disables mutations', () => {
  vi.mocked(queries.usePluginChain).mockReturnValue({
    data: undefined,
    isLoading: true,
  } as never);
  vi.mocked(queries.usePluginRegistry).mockReturnValue({
    data: undefined,
    isLoading: true,
  } as never);
  vi.mocked(queries.useRouterTerminalStrategy).mockReturnValue({
    data: undefined,
    isLoading: true,
  } as never);
  vi.mocked(queries.useUpdateRouterTerminalStrategy).mockReturnValue({
    mutate: vi.fn(),
    isPending: false,
  } as never);
  vi.mocked(queries.useReorderChain).mockReturnValue({
    mutate: vi.fn(),
    isPending: false,
  } as never);
  vi.mocked(queries.useInsertChainEntry).mockReturnValue({
    mutate: vi.fn(),
    isPending: false,
  } as never);
  vi.mocked(queries.useDeleteChainEntry).mockReturnValue({
    mutate: vi.fn(),
    isPending: false,
  } as never);

  renderWithProviders(<RouterSlotEditor principal={principalFixture()} />);

  const basicTab = screen.getByRole('tab', { name: 'Basic' });
  expect(basicTab.getAttribute('aria-disabled')).toBe('false');
  expect(basicTab.getAttribute('aria-selected')).toBe('true');
  expect(screen.getByRole('switch').hasAttribute('disabled')).toBe(true);
});

test('empty router chain keeps Basic available and enables subscription-preference', () => {
  const insertMock = vi.fn();
  vi.mocked(queries.usePluginChain).mockReturnValue({
    data: { entries: [] },
  } as unknown as ReturnType<typeof queries.usePluginChain>);
  vi.mocked(queries.usePluginRegistry).mockReturnValue({
    data: {
      entries: [
        {
          id: 'subscription-preference-id',
          name: 'subscription-preference',
          metadata: null,
          sha256_hex: '',
        },
      ],
    },
  } as unknown as ReturnType<typeof queries.usePluginRegistry>);
  vi.mocked(queries.useRouterTerminalStrategy).mockReturnValue({
    data: { strategy: 'first-pick', revision: 1 },
    isLoading: false,
  } as unknown as ReturnType<typeof queries.useRouterTerminalStrategy>);
  vi.mocked(queries.useUpdateRouterTerminalStrategy).mockReturnValue({
    mutate: vi.fn(),
    isPending: false,
  } as unknown as ReturnType<typeof queries.useUpdateRouterTerminalStrategy>);
  vi.mocked(queries.useReorderChain).mockReturnValue({
    mutate: vi.fn(),
    isPending: false,
  } as unknown as ReturnType<typeof queries.useReorderChain>);
  vi.mocked(queries.useInsertChainEntry).mockReturnValue({
    mutate: insertMock,
    isPending: false,
  } as unknown as ReturnType<typeof queries.useInsertChainEntry>);
  vi.mocked(queries.useDeleteChainEntry).mockReturnValue({
    mutate: vi.fn(),
    isPending: false,
  } as unknown as ReturnType<typeof queries.useDeleteChainEntry>);

  renderWithProviders(<RouterSlotEditor principal={principalFixture()} />);

  expect(
    screen.getByRole('tab', { name: 'Basic' }).getAttribute('aria-disabled'),
  ).toBe('false');
  const toggle = screen.getByRole('switch') as HTMLInputElement;
  expect(toggle.checked).toBe(false);
  fireEvent.click(toggle);
  expect(insertMock).toHaveBeenCalledWith(
    {
      pid: 'p-1',
      body: {
        slot: 'router',
        wasm_registry_id: 'subscription-preference-id',
        order: 0,
      },
    },
    expect.anything(),
  );
});

test('toggles terminal strategy', async () => {
  vi.mocked(queries.usePluginChain).mockReturnValue({
    data: {
      entries: [
        {
          id: 'entry-subscription-preference',
          order: 0,
          wasm_registry_id: 'subscription-preference-id',
          revision: 1,
        },
      ],
    },
  } as unknown as ReturnType<typeof queries.usePluginChain>);
  vi.mocked(queries.usePluginRegistry).mockReturnValue({
    data: {
      entries: [
        {
          id: 'subscription-preference-id',
          name: 'subscription-preference',
          metadata: null,
          sha256_hex: '',
        },
      ],
    },
  } as unknown as ReturnType<typeof queries.usePluginRegistry>);
  const mutateMock = vi.fn();
  vi.mocked(queries.useRouterTerminalStrategy).mockReturnValue({
    data: { strategy: 'first-pick', revision: 1 },
    isLoading: false,
  } as unknown as ReturnType<typeof queries.useRouterTerminalStrategy>);
  vi.mocked(queries.useUpdateRouterTerminalStrategy).mockReturnValue({
    mutate: mutateMock,
    isPending: false,
  } as unknown as ReturnType<typeof queries.useUpdateRouterTerminalStrategy>);
  vi.mocked(queries.useReorderChain).mockReturnValue({
    mutate: vi.fn(),
    isPending: false,
  } as unknown as ReturnType<typeof queries.useReorderChain>);
  vi.mocked(queries.useInsertChainEntry).mockReturnValue({
    mutate: vi.fn(),
    isPending: false,
  } as unknown as ReturnType<typeof queries.useInsertChainEntry>);
  vi.mocked(queries.useDeleteChainEntry).mockReturnValue({
    mutate: vi.fn(),
    isPending: false,
  } as unknown as ReturnType<typeof queries.useDeleteChainEntry>);

  renderWithProviders(<RouterSlotEditor principal={principalFixture()} />);
  fireEvent.click(screen.getByRole('tab', { name: 'Advanced' }));
  expect(
    screen.queryByText('Keep prompt cache warm by reusing upstreams'),
  ).toBeNull();
  fireEvent.click(screen.getByRole('tab', { name: 'Basic' }));
  expect(
    screen.getByText('Keep prompt cache warm by reusing upstreams'),
  ).toBeDefined();

  // In Basic tab
  fireEvent.click(screen.getByRole('radio', { name: /Random/ }));

  await waitFor(() => {
    expect(mutateMock).toHaveBeenCalledWith(
      { id: 'p-1', strategy: 'random', revision: 1 },
      expect.anything(),
    );
  });

  // Undo re-applies the previous strategy against the revision the forward
  // write returned.
  const successToast = vi.spyOn(toast, 'success');
  mutateMock.mock.calls[0][1].onSuccess({ strategy: 'random', revision: 2 });
  const action = successToast.mock.calls[0][1]?.action as unknown as {
    onClick: () => void;
  };
  action.onClick();
  expect(mutateMock).toHaveBeenLastCalledWith(
    { id: 'p-1', strategy: 'first-pick', revision: 2 },
    expect.anything(),
  );
  successToast.mockRestore();
});

test('complex chain forces Advanced and disables Basic with tooltip', () => {
  vi.mocked(queries.usePluginChain).mockReturnValue({
    data: {
      entries: [
        {
          id: 'entry-1',
          order: 100,
          wasm_registry_id: 'plugin-1',
          revision: 1,
        },
      ],
    },
  } as unknown as ReturnType<typeof queries.usePluginChain>);
  vi.mocked(queries.usePluginRegistry).mockReturnValue({
    data: {
      entries: [
        {
          id: 'plugin-1',
          name: 'My Plugin',
          metadata: null,
          sha256_hex: '',
        },
        {
          id: 'subscription-preference-id',
          name: 'subscription-preference',
          metadata: null,
          sha256_hex: '',
        },
      ],
    },
  } as unknown as ReturnType<typeof queries.usePluginRegistry>);
  vi.mocked(queries.useRouterTerminalStrategy).mockReturnValue({
    data: { strategy: 'first-pick', revision: 1 },
    isLoading: false,
  } as unknown as ReturnType<typeof queries.useRouterTerminalStrategy>);
  vi.mocked(queries.useUpdateRouterTerminalStrategy).mockReturnValue({
    mutate: vi.fn(),
    isPending: false,
  } as unknown as ReturnType<typeof queries.useUpdateRouterTerminalStrategy>);
  vi.mocked(queries.useReorderChain).mockReturnValue({
    mutate: vi.fn(),
    isPending: false,
  } as unknown as ReturnType<typeof queries.useReorderChain>);
  vi.mocked(queries.useInsertChainEntry).mockReturnValue({
    mutate: vi.fn(),
    isPending: false,
  } as unknown as ReturnType<typeof queries.useInsertChainEntry>);
  vi.mocked(queries.useDeleteChainEntry).mockReturnValue({
    mutate: vi.fn(),
    isPending: false,
  } as unknown as ReturnType<typeof queries.useDeleteChainEntry>);

  renderWithProviders(<RouterSlotEditor principal={principalFixture()} />);

  const basicTab = screen.getByRole('tab', { name: 'Basic' });
  expect(basicTab.getAttribute('aria-disabled')).toBe('true');

  fireEvent.click(basicTab);
  expect(screen.getByRole('button', { name: 'Dismiss' })).toBeDefined();
  expect(
    screen.getByRole('tab', { name: 'Advanced' }).getAttribute('aria-selected'),
  ).toBe('true');
});

test('Subscription preference toggle removes the built-in chain entry', async () => {
  const deleteMock = vi.fn();
  const subscriptionPreferencePlugin = {
    id: 'subscription-preference-id',
    name: 'subscription-preference',
    metadata: null,
    sha256_hex: '',
  };

  vi.mocked(queries.usePluginChain).mockReturnValue({
    data: {
      entries: [
        {
          id: 'entry-subscription-preference',
          wasm_registry_id: 'subscription-preference-id',
          order: 0,
          revision: 2,
        },
      ],
    },
  } as unknown as ReturnType<typeof queries.usePluginChain>);
  vi.mocked(queries.usePluginRegistry).mockReturnValue({
    data: { entries: [subscriptionPreferencePlugin] },
  } as unknown as ReturnType<typeof queries.usePluginRegistry>);
  vi.mocked(queries.useRouterTerminalStrategy).mockReturnValue({
    data: { strategy: 'first-pick', revision: 1 },
    isLoading: false,
  } as unknown as ReturnType<typeof queries.useRouterTerminalStrategy>);
  vi.mocked(queries.useUpdateRouterTerminalStrategy).mockReturnValue({
    mutate: vi.fn(),
    isPending: false,
  } as unknown as ReturnType<typeof queries.useUpdateRouterTerminalStrategy>);
  vi.mocked(queries.useReorderChain).mockReturnValue({
    mutate: vi.fn(),
    isPending: false,
  } as unknown as ReturnType<typeof queries.useReorderChain>);
  vi.mocked(queries.useInsertChainEntry).mockReturnValue({
    mutate: vi.fn(),
    isPending: false,
  } as unknown as ReturnType<typeof queries.useInsertChainEntry>);
  vi.mocked(queries.useDeleteChainEntry).mockReturnValue({
    mutate: deleteMock,
    isPending: false,
  } as unknown as ReturnType<typeof queries.useDeleteChainEntry>);

  renderWithProviders(<RouterSlotEditor principal={principalFixture()} />);

  const toggle = screen.getByRole('switch') as HTMLInputElement;
  expect(toggle.checked).toBe(true);
  fireEvent.click(toggle);
  expect(deleteMock).toHaveBeenCalledWith(
    { id: 'entry-subscription-preference', revision: 2 },
    expect.anything(),
  );
});

test('picker disables already-in-chain entries', () => {
  vi.mocked(queries.usePluginChain).mockReturnValue({
    data: {
      entries: [
        {
          id: 'entry-cache',
          order: 0,
          wasm_registry_id: 'subscription-preference-id',
          revision: 1,
        },
      ],
    },
  } as unknown as ReturnType<typeof queries.usePluginChain>);
  vi.mocked(queries.usePluginRegistry).mockReturnValue({
    data: {
      entries: [
        {
          id: 'subscription-preference-id',
          name: 'subscription-preference',
          metadata: null,
          sha256_hex: '',
          supported_slots: ['router'],
        },
        {
          id: 'other-id',
          name: 'other-plugin',
          metadata: null,
          sha256_hex: '',
          supported_slots: ['router'],
        },
      ],
    },
  } as unknown as ReturnType<typeof queries.usePluginRegistry>);
  vi.mocked(queries.useRouterTerminalStrategy).mockReturnValue({
    data: { strategy: 'first-pick', revision: 1 },
    isLoading: false,
  } as unknown as ReturnType<typeof queries.useRouterTerminalStrategy>);
  vi.mocked(queries.useUpdateRouterTerminalStrategy).mockReturnValue({
    mutate: vi.fn(),
    isPending: false,
  } as unknown as ReturnType<typeof queries.useUpdateRouterTerminalStrategy>);
  vi.mocked(queries.useReorderChain).mockReturnValue({
    mutate: vi.fn(),
    isPending: false,
  } as unknown as ReturnType<typeof queries.useReorderChain>);
  vi.mocked(queries.useInsertChainEntry).mockReturnValue({
    mutate: vi.fn(),
    isPending: false,
  } as unknown as ReturnType<typeof queries.useInsertChainEntry>);
  vi.mocked(queries.useDeleteChainEntry).mockReturnValue({
    mutate: vi.fn(),
    isPending: false,
  } as unknown as ReturnType<typeof queries.useDeleteChainEntry>);

  renderWithProviders(<RouterSlotEditor principal={principalFixture()} />);

  // Switch to Advanced tab
  fireEvent.click(screen.getByRole('tab', { name: 'Advanced' }));

  // Open picker
  fireEvent.click(screen.getByText('Add filter'));

  const preferenceBtn = screen.getAllByRole('button', {
    name: /subscription-preference/,
  })[1];
  expect(preferenceBtn.hasAttribute('disabled')).toBe(true);
  expect(screen.getByText('Already in chain')).toBeDefined();

  const otherBtn = screen.getByRole('button', { name: /other-plugin/ });
  expect(otherBtn.hasAttribute('disabled')).toBe(false);
});

test('addFilter no-ops when entry is disabled', () => {
  const insertMock = vi.fn();
  vi.mocked(queries.usePluginChain).mockReturnValue({
    data: {
      entries: [
        {
          id: 'entry-cache',
          order: 0,
          wasm_registry_id: 'subscription-preference-id',
          revision: 1,
        },
      ],
    },
  } as unknown as ReturnType<typeof queries.usePluginChain>);
  vi.mocked(queries.usePluginRegistry).mockReturnValue({
    data: {
      entries: [
        {
          id: 'subscription-preference-id',
          name: 'subscription-preference',
          metadata: null,
          sha256_hex: '',
          supported_slots: ['router'],
        },
      ],
    },
  } as unknown as ReturnType<typeof queries.usePluginRegistry>);
  vi.mocked(queries.useRouterTerminalStrategy).mockReturnValue({
    data: { strategy: 'first-pick', revision: 1 },
    isLoading: false,
  } as unknown as ReturnType<typeof queries.useRouterTerminalStrategy>);
  vi.mocked(queries.useUpdateRouterTerminalStrategy).mockReturnValue({
    mutate: vi.fn(),
    isPending: false,
  } as unknown as ReturnType<typeof queries.useUpdateRouterTerminalStrategy>);
  vi.mocked(queries.useReorderChain).mockReturnValue({
    mutate: vi.fn(),
    isPending: false,
  } as unknown as ReturnType<typeof queries.useReorderChain>);
  vi.mocked(queries.useInsertChainEntry).mockReturnValue({
    mutate: insertMock,
    isPending: false,
  } as unknown as ReturnType<typeof queries.useInsertChainEntry>);
  vi.mocked(queries.useDeleteChainEntry).mockReturnValue({
    mutate: vi.fn(),
    isPending: false,
  } as unknown as ReturnType<typeof queries.useDeleteChainEntry>);

  renderWithProviders(<RouterSlotEditor principal={principalFixture()} />);

  // Switch to Advanced tab
  fireEvent.click(screen.getByRole('tab', { name: 'Advanced' }));

  // Open picker
  fireEvent.click(screen.getByText('Add filter'));

  const preferenceBtn = screen.getAllByRole('button', {
    name: /subscription-preference/,
  })[1];
  fireEvent.click(preferenceBtn);

  expect(insertMock).not.toHaveBeenCalled();
});

test('opens the new-principal modal from URL intent and consumes only the action', async () => {
  const search = { selectedId: principal.id, action: 'new' as const };
  Object.assign(Route, { useSearch: () => search });
  vi.mocked(queries.usePrincipals).mockReturnValue({
    data: undefined,
    isLoading: true,
    isPending: true,
  } as never);

  renderWithProviders(<Component />);

  expect(
    await screen.findByRole('dialog', { name: 'New principal' }),
  ).toBeDefined();
  await waitFor(() => expect(navigateMock).toHaveBeenCalledTimes(1));

  const navigation = navigateMock.mock.calls[0]?.[0] as {
    replace: boolean;
    search: (previous: typeof search) => {
      selectedId?: string;
      action?: 'new';
    };
  };
  expect(navigation.replace).toBe(true);
  expect(navigation.search(search)).toEqual({
    selectedId: principal.id,
    action: undefined,
  });
});

test('isolates allowed-model and default-limit drafts when principal identity changes', () => {
  const setAllowed = vi.fn();
  const updateLimits = vi.fn();
  const principalA = principalFixture({
    id: 'principal-a',
    name: 'Ada',
    revision: 3,
    allowed_models: ['a-server'],
    default_limits: [],
  });
  const principalB = principalFixture({
    id: 'principal-b',
    name: 'Grace',
    revision: 8,
    allowed_models: ['b-server'],
    default_limits: [{ kind: 'requests', window_secs: 60, cap_micros: 250 }],
  });
  let selectedId = principalA.id;
  Object.assign(Route, { useSearch: () => ({ selectedId }) });
  vi.mocked(queries.usePrincipals).mockImplementation(
    () =>
      ({
        data: { principals: [principalA, principalB] },
        isLoading: false,
        isPending: false,
      }) as never,
  );
  vi.mocked(queries.useSetAllowedModels).mockReturnValue({
    mutate: setAllowed,
    isPending: false,
  } as never);
  vi.mocked(queries.useUpdatePrincipalDefaultLimits).mockReturnValue({
    mutate: updateLimits,
    isPending: false,
  } as never);
  const view = renderWithProviders(<Component />);

  fireEvent.click(
    cardNamed('Allowed models').getByRole('button', { name: 'Edit' }),
  );
  fireEvent.change(cardNamed('Allowed models').getByRole('textbox'), {
    target: { value: 'a-draft' },
  });
  fireEvent.click(
    cardNamed('Default limits').getByRole('button', { name: 'Edit' }),
  );
  fireEvent.click(
    cardNamed('Default limits').getByRole('button', { name: 'Add limit' }),
  );

  selectedId = principalB.id;
  view.rerender(withProviders(<Component />));

  expect(screen.getByRole('heading', { name: principalB.name })).toBeDefined();
  const allowedModels = cardNamed('Allowed models');
  expect(allowedModels.queryByRole('textbox')).toBeNull();
  fireEvent.click(allowedModels.getByRole('button', { name: 'Edit' }));
  expect(
    (allowedModels.getByRole('textbox') as HTMLTextAreaElement).value,
  ).toBe('b-server');
  fireEvent.click(allowedModels.getByRole('button', { name: 'Save' }));

  const defaultLimits = cardNamed('Default limits');
  fireEvent.click(defaultLimits.getByRole('button', { name: 'Edit' }));
  expect(defaultLimits.getAllByRole('button', { name: 'Remove' })).toHaveLength(
    1,
  );
  fireEvent.click(defaultLimits.getByRole('button', { name: 'Save' }));

  expect(setAllowed).toHaveBeenCalledWith(
    {
      id: principalB.id,
      models: ['b-server'],
      expected_revision: principalB.revision,
    },
    expect.anything(),
  );
  expect(updateLimits).toHaveBeenCalledWith(
    {
      id: principalB.id,
      default_limits: principalB.default_limits,
      expected_revision: principalB.revision,
    },
    expect.anything(),
  );
});

test('keeps dirty drafts and their starting revision until cancel, then re-enters from latest server state', () => {
  const setAllowed = vi.fn();
  const updateLimits = vi.fn();
  let current = principalFixture({
    id: 'principal-a',
    name: 'Ada',
    revision: 7,
    allowed_models: ['server-old'],
    default_limits: [{ kind: 'requests', window_secs: 60, cap_micros: 100 }],
  });
  Object.assign(Route, { useSearch: () => ({ selectedId: current.id }) });
  vi.mocked(queries.usePrincipals).mockImplementation(
    () =>
      ({
        data: { principals: [current] },
        isLoading: false,
        isPending: false,
      }) as never,
  );
  vi.mocked(queries.useSetAllowedModels).mockReturnValue({
    mutate: setAllowed,
    isPending: false,
  } as never);
  vi.mocked(queries.useUpdatePrincipalDefaultLimits).mockReturnValue({
    mutate: updateLimits,
    isPending: false,
  } as never);
  const view = renderWithProviders(<Component />);

  fireEvent.click(
    cardNamed('Allowed models').getByRole('button', { name: 'Edit' }),
  );
  fireEvent.change(cardNamed('Allowed models').getByRole('textbox'), {
    target: { value: 'operator-draft' },
  });
  fireEvent.click(
    cardNamed('Default limits').getByRole('button', { name: 'Edit' }),
  );
  fireEvent.click(
    cardNamed('Default limits').getByRole('button', { name: 'Add limit' }),
  );

  current = {
    ...current,
    revision: 8,
    allowed_models: ['server-new'],
    default_limits: [{ kind: 'requests', window_secs: 60, cap_micros: 900 }],
  };
  view.rerender(withProviders(<Component />));

  const allowedModels = cardNamed('Allowed models');
  expect(
    (allowedModels.getByRole('textbox') as HTMLTextAreaElement).value,
  ).toBe('operator-draft');
  const defaultLimits = cardNamed('Default limits');
  expect(defaultLimits.getAllByRole('button', { name: 'Remove' })).toHaveLength(
    2,
  );

  fireEvent.click(allowedModels.getByRole('button', { name: 'Save' }));
  fireEvent.click(defaultLimits.getByRole('button', { name: 'Save' }));
  expect(setAllowed).toHaveBeenLastCalledWith(
    {
      id: current.id,
      models: ['operator-draft'],
      expected_revision: 7,
    },
    expect.anything(),
  );
  expect(updateLimits).toHaveBeenLastCalledWith(
    {
      id: current.id,
      default_limits: [
        { kind: 'requests', window_secs: 60, cap_micros: 100 },
        { kind: 'requests', window_secs: 60, cap_micros: 0 },
      ],
      expected_revision: 7,
    },
    expect.anything(),
  );

  fireEvent.click(allowedModels.getByRole('button', { name: 'Cancel' }));
  fireEvent.click(defaultLimits.getByRole('button', { name: 'Cancel' }));
  fireEvent.click(
    cardNamed('Allowed models').getByRole('button', { name: 'Edit' }),
  );
  fireEvent.click(
    cardNamed('Default limits').getByRole('button', { name: 'Edit' }),
  );

  expect(
    (cardNamed('Allowed models').getByRole('textbox') as HTMLTextAreaElement)
      .value,
  ).toBe('server-new');
  expect(
    cardNamed('Default limits').getAllByRole('button', { name: 'Remove' }),
  ).toHaveLength(1);

  setAllowed.mockClear();
  updateLimits.mockClear();
  fireEvent.click(
    cardNamed('Allowed models').getByRole('button', { name: 'Save' }),
  );
  fireEvent.click(
    cardNamed('Default limits').getByRole('button', { name: 'Save' }),
  );
  expect(setAllowed).toHaveBeenCalledWith(
    {
      id: current.id,
      models: ['server-new'],
      expected_revision: 8,
    },
    expect.anything(),
  );
  expect(updateLimits).toHaveBeenCalledWith(
    {
      id: current.id,
      default_limits: current.default_limits,
      expected_revision: 8,
    },
    expect.anything(),
  );
});

test('create pending locks the modal draft and dismissal controls', () => {
  const mutate = vi.fn();
  vi.mocked(queries.useCreatePrincipal).mockReturnValue({
    mutate,
    isPending: false,
  } as never);

  const view = renderWithProviders(<Component />);
  fireEvent.click(screen.getByRole('button', { name: 'New' }));

  let dialog = screen.getByRole('dialog', { name: 'New principal' });
  fireEvent.change(within(dialog).getByPlaceholderText('engineering-shared'), {
    target: { value: 'engineering' },
  });
  fireEvent.click(within(dialog).getByRole('button', { name: 'Add limit' }));
  const createButton = within(dialog).getByRole('button', { name: 'Create' });
  fireEvent.click(createButton);
  fireEvent.click(createButton);
  expect(mutate).toHaveBeenCalledTimes(1);

  vi.mocked(queries.useCreatePrincipal).mockReturnValue({
    mutate,
    isPending: true,
  } as never);
  view.rerender(withProviders(<Component />));

  dialog = screen.getByRole('dialog', { name: 'New principal' });
  const creating = within(dialog).getByRole('button', { name: 'Creating...' });
  expect(creating.hasAttribute('disabled')).toBe(true);
  expect(creating.getAttribute('aria-busy')).toBe('true');
  expect(creating.querySelector('svg.animate-spin')).not.toBeNull();
  expect(
    within(dialog)
      .getByRole('button', { name: 'Cancel' })
      .hasAttribute('disabled'),
  ).toBe(true);
  expect(
    within(dialog)
      .getByRole('button', { name: 'Close dialog' })
      .hasAttribute('disabled'),
  ).toBe(true);
  for (const control of [
    ...within(dialog).getAllByRole('textbox'),
    ...within(dialog).getAllByRole('combobox'),
    within(dialog).getByRole('button', { name: 'Remove' }),
    within(dialog).getByRole('button', { name: 'Add limit' }),
  ]) {
    expect(control.hasAttribute('disabled')).toBe(true);
  }

  fireEvent.click(within(dialog).getByRole('button', { name: 'Cancel' }));
  fireEvent.keyDown(dialog, { key: 'Escape', code: 'Escape' });
  expect(screen.getByRole('dialog', { name: 'New principal' })).toBeDefined();
});

test('toggle pending locks both principal header mutations', () => {
  vi.mocked(queries.useTogglePrincipal).mockReturnValue({
    mutate: vi.fn(),
    isPending: true,
  } as never);

  renderPrincipalDetail();

  const toggle = screen.getByRole('switch', { name: 'Enabled' });
  expect(toggle.hasAttribute('disabled')).toBe(true);
  expect(screen.getByRole('status').textContent).toContain('Disabling...');
  expect(
    screen.getByRole('button', { name: 'Delete' }).hasAttribute('disabled'),
  ).toBe(true);
});

test('principal disable waits for confirmation before mutating', () => {
  const mutate = vi.fn();
  vi.mocked(queries.useTogglePrincipal).mockReturnValue({
    mutate,
    isPending: false,
  } as never);
  renderPrincipalDetail(principalFixture({ name: 'isac-max' }));

  fireEvent.click(screen.getByRole('switch', { name: 'Enabled' }));
  expect(mutate).not.toHaveBeenCalled();
  let dialog = screen.getByRole('alertdialog', { name: 'Disable principal?' });
  expect(dialog.textContent).toContain('isac-max');
  fireEvent.click(within(dialog).getByRole('button', { name: 'Cancel' }));
  expect(mutate).not.toHaveBeenCalled();

  fireEvent.click(screen.getByRole('switch', { name: 'Enabled' }));
  dialog = screen.getByRole('alertdialog', { name: 'Disable principal?' });
  fireEvent.click(within(dialog).getByRole('button', { name: 'Disable' }));
  expect(mutate).toHaveBeenCalledWith(
    { id: 'p-1', enabled: false, revision: 7 },
    expect.anything(),
  );
});

test('delete pending keeps principal confirmation context and locks actions', () => {
  const mutate = vi.fn();
  vi.mocked(queries.useDeletePrincipal).mockReturnValue({
    mutate,
    isPending: false,
  } as never);
  const view = renderPrincipalDetail();
  const principalToggle = screen.getByRole('switch', { name: 'Enabled' });
  const principalDelete = screen.getByRole('button', { name: 'Delete' });

  fireEvent.click(principalDelete);
  let dialog = screen.getByRole('alertdialog', { name: 'Delete principal?' });
  fireEvent.click(within(dialog).getByRole('button', { name: 'Delete' }));
  expect(mutate).toHaveBeenCalledTimes(1);

  vi.mocked(queries.useDeletePrincipal).mockReturnValue({
    mutate,
    isPending: true,
  } as never);
  view.rerender(withProviders(<Component />));

  dialog = screen.getByRole('alertdialog', { name: 'Delete principal?' });
  expect(
    within(dialog)
      .getByRole('button', { name: 'Cancel' })
      .hasAttribute('disabled'),
  ).toBe(true);
  const confirmation = within(dialog).getByRole('button', { name: 'Delete' });
  expect(confirmation.hasAttribute('disabled')).toBe(true);
  expect(confirmation.getAttribute('aria-busy')).toBe('true');
  expect(principalToggle.hasAttribute('disabled')).toBe(true);
  expect(principalDelete.hasAttribute('disabled')).toBe(true);

  fireEvent.keyDown(dialog, { key: 'Escape', code: 'Escape' });
  expect(
    screen.getByRole('alertdialog', { name: 'Delete principal?' }),
  ).toBeDefined();
});

test('allowed-model pending locks its draft and shows saving progress', () => {
  const mutate = vi.fn();
  vi.mocked(queries.useSetAllowedModels).mockReturnValue({
    mutate,
    isPending: false,
  } as never);
  const selectedPrincipal = principalFixture({
    allowed_models: ['gpt-5'],
  });
  const view = renderPrincipalDetail(selectedPrincipal);

  fireEvent.click(
    cardNamed('Allowed models').getByRole('button', { name: 'Edit' }),
  );
  vi.mocked(queries.useSetAllowedModels).mockReturnValue({
    mutate,
    isPending: true,
  } as never);
  view.rerender(withProviders(<Component />));

  const card = cardNamed('Allowed models');
  expect(card.getByRole('textbox').hasAttribute('disabled')).toBe(true);
  expect(
    card.getByRole('button', { name: 'Cancel' }).hasAttribute('disabled'),
  ).toBe(true);
  const saving = card.getByRole('button', { name: 'Saving...' });
  expect(saving.hasAttribute('disabled')).toBe(true);
  expect(saving.getAttribute('aria-busy')).toBe('true');
  expect(saving.querySelector('svg.animate-spin')).not.toBeNull();

  fireEvent.click(card.getByRole('button', { name: 'Cancel' }));
  expect(
    cardNamed('Allowed models').getByRole('button', { name: 'Saving...' }),
  ).toBeDefined();
});

test('default-limit pending locks every draft control and shows saving progress', () => {
  const mutate = vi.fn();
  vi.mocked(queries.useUpdatePrincipalDefaultLimits).mockReturnValue({
    mutate,
    isPending: false,
  } as never);
  const selectedPrincipal = principalFixture({
    default_limits: [{ kind: 'requests', window_secs: 60, cap_micros: 100 }],
  });
  const view = renderPrincipalDetail(selectedPrincipal);

  fireEvent.click(
    cardNamed('Default limits').getByRole('button', { name: 'Edit' }),
  );
  vi.mocked(queries.useUpdatePrincipalDefaultLimits).mockReturnValue({
    mutate,
    isPending: true,
  } as never);
  view.rerender(withProviders(<Component />));

  const card = cardNamed('Default limits');
  for (const control of [
    card.getByRole('combobox'),
    ...card.getAllByRole('textbox'),
    card.getByRole('button', { name: 'Remove' }),
    card.getByRole('button', { name: 'Add limit' }),
    card.getByRole('button', { name: 'Cancel' }),
  ]) {
    expect(control.hasAttribute('disabled')).toBe(true);
  }
  const saving = card.getByRole('button', { name: 'Saving...' });
  expect(saving.hasAttribute('disabled')).toBe(true);
  expect(saving.getAttribute('aria-busy')).toBe('true');
  expect(saving.querySelector('svg.animate-spin')).not.toBeNull();
});

test('principal-wide pending writes lock every principal record control without borrowing progress labels', () => {
  const allowedMutate = vi.fn();
  const selectedPrincipal = principalFixture({
    allowed_models: ['gpt-5'],
    default_limits: [{ kind: 'requests', window_secs: 60, cap_micros: 100 }],
  });
  vi.mocked(queries.usePrincipalWritePending).mockReturnValue(1);
  vi.mocked(queries.useSetAllowedModels).mockReturnValue({
    mutate: allowedMutate,
    isPending: false,
  } as never);
  const view = renderPrincipalDetail(selectedPrincipal);

  const principalToggle = screen.getByRole('switch', { name: 'Enabled' });
  expect(principalToggle.hasAttribute('disabled')).toBe(true);
  expect(screen.queryByText(/Enabling\.\.\.|Disabling\.\.\./)).toBeNull();
  expect(
    screen.getByRole('button', { name: 'Delete' }).hasAttribute('disabled'),
  ).toBe(true);
  expect(
    cardNamed('Allowed models')
      .getByRole('button', { name: 'Edit' })
      .hasAttribute('disabled'),
  ).toBe(true);
  expect(
    cardNamed('Default limits')
      .getByRole('button', { name: 'Edit' })
      .hasAttribute('disabled'),
  ).toBe(true);
  expect(
    cardNamed('Router')
      .getAllByRole('radio', { hidden: true })
      .filter((radio) => radio.hasAttribute('disabled')),
  ).toHaveLength(2);
  const addFilter = cardNamed('Router')
    .getByText('Add filter')
    .closest('[role="button"]');
  expect(addFilter?.getAttribute('aria-disabled')).toBe('true');
  expect(
    cardNamed('Shape')
      .getAllByRole('radio', { hidden: true })
      .filter((radio) => radio.hasAttribute('disabled')),
  ).toHaveLength(1);
  expect(screen.queryByText('Updating strategy...')).toBeNull();

  vi.mocked(queries.usePrincipalWritePending).mockReturnValue(0);
  view.rerender(withProviders(<Component />));
  fireEvent.click(
    cardNamed('Allowed models').getByRole('button', { name: 'Edit' }),
  );
  fireEvent.click(
    cardNamed('Default limits').getByRole('button', { name: 'Edit' }),
  );

  vi.mocked(queries.usePrincipalWritePending).mockReturnValue(1);
  vi.mocked(queries.useSetAllowedModels).mockReturnValue({
    mutate: allowedMutate,
    isPending: true,
  } as never);
  view.rerender(withProviders(<Component />));

  const allowedModels = cardNamed('Allowed models');
  expect(allowedModels.getByRole('textbox').hasAttribute('disabled')).toBe(
    true,
  );
  expect(
    allowedModels
      .getByRole('button', { name: 'Cancel' })
      .hasAttribute('disabled'),
  ).toBe(true);
  const allowedSaving = allowedModels.getByRole('button', {
    name: 'Saving...',
  });
  expect(allowedSaving.hasAttribute('disabled')).toBe(true);
  expect(allowedSaving.getAttribute('aria-busy')).toBe('true');

  const defaultLimits = cardNamed('Default limits');
  for (const control of [
    defaultLimits.getByRole('combobox'),
    ...defaultLimits.getAllByRole('textbox'),
    defaultLimits.getByRole('button', { name: 'Remove' }),
    defaultLimits.getByRole('button', { name: 'Add limit' }),
    defaultLimits.getByRole('button', { name: 'Cancel' }),
    defaultLimits.getByRole('button', { name: 'Save' }),
  ]) {
    expect(control.hasAttribute('disabled')).toBe(true);
  }
  expect(defaultLimits.queryByRole('button', { name: 'Saving...' })).toBeNull();
  expect(screen.queryByText('Updating strategy...')).toBeNull();
});

test('router pending locks terminal, reorder, insert, and delete controls', () => {
  const entries = [
    {
      id: 'router-entry-1',
      order: 100,
      wasm_registry_id: 'router-plugin-1',
      revision: 1,
    },
    {
      id: 'router-entry-2',
      order: 200,
      wasm_registry_id: 'router-plugin-2',
      revision: 2,
    },
  ];
  vi.mocked(queries.usePluginChain).mockReturnValue({
    data: { entries },
    isLoading: false,
  } as never);
  vi.mocked(queries.usePluginRegistry).mockReturnValue({
    data: {
      entries: [
        {
          id: 'router-plugin-1',
          name: 'Router One',
          metadata: null,
          sha256_hex: '',
          supported_slots: ['router'],
        },
        {
          id: 'router-plugin-2',
          name: 'Router Two',
          metadata: null,
          sha256_hex: '',
          supported_slots: ['router'],
        },
      ],
    },
    isLoading: false,
  } as never);
  const view = renderWithProviders(
    <RouterSlotEditor principal={principalFixture()} />,
  );
  fireEvent.click(screen.getByRole('tab', { name: 'Advanced' }));

  vi.mocked(queries.useUpdateRouterTerminalStrategy).mockReturnValue({
    mutate: vi.fn(),
    isPending: true,
  } as never);
  vi.mocked(queries.useReorderChain).mockReturnValue({
    mutate: vi.fn(),
    isPending: true,
  } as never);
  vi.mocked(queries.useInsertChainEntry).mockReturnValue({
    mutate: vi.fn(),
    isPending: true,
  } as never);
  vi.mocked(queries.useDeleteChainEntry).mockReturnValue({
    mutate: vi.fn(),
    isPending: true,
  } as never);
  view.rerender(
    withProviders(<RouterSlotEditor principal={principalFixture()} />),
  );

  const terminalGroup = screen.getByRole('radiogroup');
  expect(terminalGroup.getAttribute('aria-busy')).toBe('true');
  expect(screen.getByText('Updating strategy...')).toBeDefined();
  expect(screen.getByText('Adding filter...')).toBeDefined();
  expect(
    within(terminalGroup)
      .getAllByRole('radio', { hidden: true })
      .filter((radio) => radio.hasAttribute('disabled')),
  ).toHaveLength(2);
  for (const label of ['Move filter up', 'Move filter down', 'Remove filter']) {
    for (const button of screen.getAllByRole('button', { name: label })) {
      expect(button.hasAttribute('disabled')).toBe(true);
    }
  }
  const addFilter = screen.getByText('Adding...').closest('[role="button"]');
  expect(addFilter?.getAttribute('aria-disabled')).toBe('true');
});

test('router reorder locks subscription preference while its own write shows progress', () => {
  const insert = vi.fn();
  vi.mocked(queries.usePluginChain).mockReturnValue({
    data: { entries: [] },
    isLoading: false,
  } as never);
  vi.mocked(queries.usePluginRegistry).mockReturnValue({
    data: {
      entries: [
        {
          id: 'subscription-preference-id',
          name: 'subscription-preference',
          metadata: null,
          sha256_hex: '',
          supported_slots: ['router'],
        },
      ],
    },
    isLoading: false,
  } as never);
  vi.mocked(queries.useReorderChain).mockReturnValue({
    mutate: vi.fn(),
    isPending: true,
  } as never);
  vi.mocked(queries.useInsertChainEntry).mockReturnValue({
    mutate: insert,
    isPending: false,
  } as never);

  const view = renderWithProviders(
    <RouterSlotEditor principal={principalFixture()} />,
  );

  let subscriptionPreference = screen.getByRole('switch');
  expect(subscriptionPreference.hasAttribute('disabled')).toBe(true);
  expect(subscriptionPreference.getAttribute('aria-busy')).toBeNull();
  expect(screen.queryByText('Turning on...')).toBeNull();
  fireEvent.click(subscriptionPreference);
  expect(insert).not.toHaveBeenCalled();

  vi.mocked(queries.useReorderChain).mockReturnValue({
    mutate: vi.fn(),
    isPending: false,
  } as never);
  vi.mocked(queries.useInsertChainEntry).mockReturnValue({
    mutate: insert,
    isPending: true,
    variables: {
      pid: 'p-1',
      body: {
        slot: 'router',
        wasm_registry_id: 'subscription-preference-id',
        order: 0,
      },
    },
  } as never);
  view.rerender(
    withProviders(<RouterSlotEditor principal={principalFixture()} />),
  );

  subscriptionPreference = screen.getByRole('switch');
  expect(subscriptionPreference.hasAttribute('disabled')).toBe(true);
  expect(subscriptionPreference.getAttribute('aria-busy')).toBe('true');
  const progress = screen.getByRole('status');
  expect(progress.textContent).toContain('Turning on...');
  expect(progress.querySelector('svg.animate-spin')).not.toBeNull();
});

test('observability add pending locks its modal and shows adding progress', async () => {
  const mutate = vi.fn();
  vi.mocked(queries.usePluginRegistry).mockReturnValue({
    data: {
      entries: [
        {
          id: 'observability-plugin',
          name: 'Audit Hook',
          metadata: null,
          sha256_hex: '',
          supported_slots: ['observability_hook'],
        },
      ],
    },
    isLoading: false,
  } as never);
  vi.mocked(queries.useInsertChainEntry).mockReturnValue({
    mutate,
    isPending: false,
  } as never);
  const view = renderPrincipalDetail();

  const observabilityCard = cardNamed('Observability');
  fireEvent.click(observabilityCard.getByRole('button', { name: 'Add' }));
  let dialog = screen.getByRole('dialog', {
    name: 'Add plugin to Observability',
  });
  await userEvent.click(within(dialog).getByRole('combobox'));
  await userEvent.click(
    await screen.findByRole('option', { name: 'Audit Hook' }),
  );
  await userEvent.click(within(dialog).getByRole('button', { name: 'Add' }));
  expect(mutate).toHaveBeenCalledTimes(1);

  vi.mocked(queries.useInsertChainEntry).mockReturnValue({
    mutate,
    isPending: true,
  } as never);
  view.rerender(withProviders(<Component />));

  dialog = screen.getByRole('dialog', {
    name: 'Add plugin to Observability',
  });
  expect(within(dialog).getByRole('combobox').hasAttribute('disabled')).toBe(
    true,
  );
  expect(
    within(dialog)
      .getByRole('button', { name: 'Cancel' })
      .hasAttribute('disabled'),
  ).toBe(true);
  expect(
    within(dialog)
      .getByRole('button', { name: 'Close dialog' })
      .hasAttribute('disabled'),
  ).toBe(true);
  const adding = within(dialog).getByRole('button', { name: 'Adding...' });
  expect(adding.hasAttribute('disabled')).toBe(true);
  expect(adding.getAttribute('aria-busy')).toBe('true');

  fireEvent.keyDown(dialog, { key: 'Escape', code: 'Escape' });
  expect(
    screen.getByRole('dialog', { name: 'Add plugin to Observability' }),
  ).toBeDefined();
});

test('observability remove pending preserves confirmation and locks row actions', () => {
  const mutate = vi.fn();
  vi.mocked(queries.usePluginChain).mockImplementation(
    (_principalId, slot) =>
      ({
        data: {
          entries:
            slot === 'observability_hook'
              ? [
                  {
                    id: 'observability-entry',
                    order: 100,
                    wasm_registry_id: 'observability-plugin',
                    revision: 4,
                  },
                ]
              : [],
        },
        isLoading: false,
      }) as never,
  );
  vi.mocked(queries.usePluginRegistry).mockReturnValue({
    data: {
      entries: [
        {
          id: 'observability-plugin',
          name: 'Audit Hook',
          metadata: null,
          sha256_hex: '',
          supported_slots: ['observability_hook'],
        },
      ],
    },
    isLoading: false,
  } as never);
  vi.mocked(queries.useDeleteChainEntry).mockReturnValue({
    mutate,
    isPending: false,
  } as never);
  const view = renderPrincipalDetail();

  const observabilityCard = cardNamed('Observability');
  const addButton = observabilityCard.getByRole('button', { name: 'Add' });
  const dragButton = observabilityCard.getByRole('button', {
    name: 'Drag to reorder',
  });
  const removeButton = observabilityCard.getByRole('button', {
    name: 'Remove Audit Hook',
  });
  fireEvent.click(removeButton);
  let dialog = screen.getByRole('alertdialog', {
    name: 'Remove plugin from chain?',
  });
  fireEvent.click(within(dialog).getByRole('button', { name: 'Remove' }));
  expect(mutate).toHaveBeenCalledTimes(1);

  vi.mocked(queries.useDeleteChainEntry).mockReturnValue({
    mutate,
    isPending: true,
  } as never);
  view.rerender(withProviders(<Component />));

  for (const button of [addButton, dragButton, removeButton]) {
    expect(button.hasAttribute('disabled')).toBe(true);
  }
  expect(screen.queryByText('Saving order...')).toBeNull();
  dialog = screen.getByRole('alertdialog', {
    name: 'Remove plugin from chain?',
  });
  expect(
    within(dialog)
      .getByRole('button', { name: 'Cancel' })
      .hasAttribute('disabled'),
  ).toBe(true);
  const removal = within(dialog).getByRole('button', {
    name: 'Removing...',
  });
  expect(removal.hasAttribute('disabled')).toBe(true);
  expect(removal.getAttribute('aria-busy')).toBe('true');
  fireEvent.keyDown(dialog, { key: 'Escape', code: 'Escape' });
  expect(
    screen.getByRole('alertdialog', {
      name: 'Remove plugin from chain?',
    }),
  ).toBeDefined();
});

test('API-key issue pending locks actions, ignores dismissal, and submits once', () => {
  const mutate = vi.fn();
  vi.mocked(queries.useIssueKey).mockReturnValue({
    mutate,
    isPending: false,
  } as never);
  const view = renderPrincipalDetail(principal);

  fireEvent.click(
    cardNamed('API keys').getByRole('button', { name: 'Issue key' }),
  );
  let dialog = screen.getByRole('dialog', { name: 'Issue API key' });
  fireEvent.change(within(dialog).getByRole('textbox'), {
    target: { value: 'ci' },
  });
  const issueButton = within(dialog).getByRole('button', { name: 'Issue' });
  fireEvent.click(issueButton);
  fireEvent.click(issueButton);
  expect(mutate).toHaveBeenCalledTimes(1);

  vi.mocked(queries.useIssueKey).mockReturnValue({
    mutate,
    isPending: true,
  } as never);
  view.rerender(withProviders(<Component />));

  dialog = screen.getByRole('dialog', { name: 'Issue API key' });
  const issuing = within(dialog).getByRole('button', { name: 'Issuing...' });
  expect(issuing.hasAttribute('disabled')).toBe(true);
  expect(issuing.getAttribute('aria-busy')).toBe('true');
  expect(issuing.querySelector('svg.animate-spin')).not.toBeNull();
  expect(
    within(dialog)
      .getByRole('button', { name: 'Cancel' })
      .hasAttribute('disabled'),
  ).toBe(true);
  expect(
    within(dialog)
      .getByRole('button', { name: 'Close dialog' })
      .hasAttribute('disabled'),
  ).toBe(true);
  expect(within(dialog).getByRole('textbox').hasAttribute('disabled')).toBe(
    true,
  );

  fireEvent.click(issuing);
  fireEvent.click(within(dialog).getByRole('button', { name: 'Cancel' }));
  fireEvent.click(within(dialog).getByRole('button', { name: 'Close dialog' }));
  fireEvent.keyDown(dialog, { key: 'Escape', code: 'Escape' });

  expect(mutate).toHaveBeenCalledTimes(1);
  expect(screen.getByRole('dialog', { name: 'Issue API key' })).toBeDefined();
});

test('issued API-key plaintext cannot be dismissed until Done', () => {
  const mutate = vi.fn(
    (
      _variables: unknown,
      options?: {
        onSuccess?: (result: { plaintext_key: string; key_id: string }) => void;
      },
    ) => {
      options?.onSuccess?.({
        plaintext_key: 'cc-secret-once',
        key_id: 'key-new',
      });
    },
  );
  vi.mocked(queries.useIssueKey).mockReturnValue({
    mutate,
    isPending: false,
  } as never);
  renderPrincipalDetail(principal);

  fireEvent.click(
    cardNamed('API keys').getByRole('button', { name: 'Issue key' }),
  );
  const issueDialog = screen.getByRole('dialog', { name: 'Issue API key' });
  fireEvent.click(within(issueDialog).getByRole('button', { name: 'Issue' }));

  const dialog = screen.getByRole('dialog', { name: 'API key issued' });
  expect(within(dialog).getByText('cc-secret-once')).toBeDefined();
  const close = within(dialog).getByRole('button', { name: 'Close dialog' });
  expect(close.hasAttribute('disabled')).toBe(true);
  expect(close.getAttribute('aria-disabled')).toBe('true');

  fireEvent.click(close);
  expect(screen.getByRole('dialog', { name: 'API key issued' })).toBeDefined();
  fireEvent.keyDown(dialog, { key: 'Escape', code: 'Escape' });
  expect(screen.getByRole('dialog', { name: 'API key issued' })).toBeDefined();
  const backdrop = document.querySelector('.bg-modal-backdrop');
  expect(backdrop).toBeInstanceOf(HTMLElement);
  fireEvent.click(backdrop as HTMLElement);
  expect(screen.getByRole('dialog', { name: 'API key issued' })).toBeDefined();
  expect(screen.getByText('cc-secret-once')).toBeDefined();

  fireEvent.click(within(dialog).getByRole('button', { name: 'Done' }));
  expect(screen.queryByRole('dialog', { name: 'API key issued' })).toBeNull();
  expect(screen.queryByText('cc-secret-once')).toBeNull();
});

test('API-key revoke pending preserves confirmation and locks row action', () => {
  const mutate = vi.fn();
  vi.mocked(queries.usePrincipalKeys).mockReturnValue({
    data: {
      keys: [
        {
          key_id: 'key-1',
          label: 'CI',
          last_4: '1234',
          issued_at_unix_secs: 1,
          last_used_at_unix_secs: null,
          revoked_at_unix_secs: null,
        },
      ],
    },
    isLoading: false,
  } as never);
  vi.mocked(queries.useRevokeKey).mockReturnValue({
    mutate,
    isPending: false,
  } as never);
  const view = renderPrincipalDetail(principal);
  const apiKeysCard = cardNamed('API keys');
  const revokeButton = apiKeysCard.getByRole('button', {
    name: 'Revoke key CI (key-1)',
  });

  fireEvent.click(revokeButton);
  let dialog = screen.getByRole('alertdialog', { name: 'Revoke API key?' });
  fireEvent.click(within(dialog).getByRole('button', { name: 'Revoke' }));
  expect(mutate).toHaveBeenCalledTimes(1);

  vi.mocked(queries.useRevokeKey).mockReturnValue({
    mutate,
    isPending: true,
  } as never);
  view.rerender(withProviders(<Component />));

  expect(revokeButton.hasAttribute('disabled')).toBe(true);
  dialog = screen.getByRole('alertdialog', { name: 'Revoke API key?' });
  expect(
    within(dialog)
      .getByRole('button', { name: 'Cancel' })
      .hasAttribute('disabled'),
  ).toBe(true);
  const confirmation = within(dialog).getByRole('button', { name: 'Revoke' });
  expect(confirmation.hasAttribute('disabled')).toBe(true);
  expect(confirmation.getAttribute('aria-busy')).toBe('true');

  fireEvent.keyDown(dialog, { key: 'Escape', code: 'Escape' });
  expect(
    screen.getByRole('alertdialog', { name: 'Revoke API key?' }),
  ).toBeDefined();
});
