import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from '@testing-library/react';
import type React from 'react';
import { afterEach, beforeEach, expect, test, vi } from 'vitest';
import * as queries from '../lib/queries';
import {
  ApiKeysCard,
  RecentRequestsCard,
  Route,
  RouterSlotEditor,
} from './principals';

vi.mock('@tanstack/react-router', async () => {
  const actual = await vi.importActual('@tanstack/react-router');
  return {
    ...actual,
    useNavigate: () => vi.fn(),
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

function renderWithProviders(ui: React.ReactElement) {
  return render(
    <QueryClientProvider client={queryClient}>{ui}</QueryClientProvider>,
  );
}

beforeEach(() => {
  vi.clearAllMocks();
  Object.assign(Route, { useSearch: () => ({}) });
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
    expect(card.querySelectorAll('.skeleton')).toHaveLength(5);
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
  expect(detailShell.querySelectorAll('.glass')).toHaveLength(8);
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
  expect(slot.querySelectorAll('thead th')).toHaveLength(8);
  const rows = slot.querySelectorAll('tbody tr');
  expect(rows).toHaveLength(5);
  for (const row of rows) {
    expect(row.className).toContain('border-b');
    expect(row.querySelectorAll('td')).toHaveLength(8);
  }
  expect(container.textContent).not.toContain('—');
});

test('API key loading keeps the table header and per-column skeleton rows', () => {
  vi.mocked(queries.usePrincipalKeys).mockReturnValue({
    data: undefined,
    isLoading: true,
  } as never);
  vi.mocked(queries.useIssueKey).mockReturnValue({
    mutate: vi.fn(),
  } as never);
  vi.mocked(queries.useRevokeKey).mockReturnValue({
    mutate: vi.fn(),
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
  } as unknown as ReturnType<typeof queries.useReorderChain>);
  vi.mocked(queries.useInsertChainEntry).mockReturnValue({
    mutate: vi.fn(),
  } as unknown as ReturnType<typeof queries.useInsertChainEntry>);
  vi.mocked(queries.useDeleteChainEntry).mockReturnValue({
    mutate: vi.fn(),
  } as unknown as ReturnType<typeof queries.useDeleteChainEntry>);

  renderWithProviders(<RouterSlotEditor principalId="p-1" />);

  expect(screen.getByText('My Plugin')).toBeDefined();
  expect(screen.getByText('Terminal step')).toBeDefined();
});

test('renders empty state when no entries', () => {
  vi.mocked(queries.usePluginChain).mockReturnValue({
    data: { entries: [] },
  } as unknown as ReturnType<typeof queries.usePluginChain>);
  vi.mocked(queries.usePluginRegistry).mockReturnValue({
    data: { entries: [] },
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
  } as unknown as ReturnType<typeof queries.useReorderChain>);
  vi.mocked(queries.useInsertChainEntry).mockReturnValue({
    mutate: vi.fn(),
  } as unknown as ReturnType<typeof queries.useInsertChainEntry>);
  vi.mocked(queries.useDeleteChainEntry).mockReturnValue({
    mutate: vi.fn(),
  } as unknown as ReturnType<typeof queries.useDeleteChainEntry>);

  renderWithProviders(<RouterSlotEditor principalId="p-1" />);

  // It should default to Basic tab because it's not complex
  expect(
    screen.getByText('Keep prompt cache warm by reusing upstreams'),
  ).toBeDefined();

  // Switch to Advanced tab
  fireEvent.click(screen.getByRole('tab', { name: 'Advanced' }));
  expect(screen.getByText('No filters yet.')).toBeDefined();
});

test('toggles terminal strategy', async () => {
  vi.mocked(queries.usePluginChain).mockReturnValue({
    data: { entries: [] },
  } as unknown as ReturnType<typeof queries.usePluginChain>);
  vi.mocked(queries.usePluginRegistry).mockReturnValue({
    data: { entries: [] },
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
  } as unknown as ReturnType<typeof queries.useReorderChain>);
  vi.mocked(queries.useInsertChainEntry).mockReturnValue({
    mutate: vi.fn(),
  } as unknown as ReturnType<typeof queries.useInsertChainEntry>);
  vi.mocked(queries.useDeleteChainEntry).mockReturnValue({
    mutate: vi.fn(),
  } as unknown as ReturnType<typeof queries.useDeleteChainEntry>);

  renderWithProviders(<RouterSlotEditor principalId="p-1" />);

  // In Basic tab
  const randomRadio = screen.getAllByRole('radio', { name: /Random/ })[0];
  fireEvent.click(randomRadio.querySelector('input')!);

  await waitFor(() => {
    expect(mutateMock).toHaveBeenCalledWith(
      { id: 'p-1', strategy: 'random', revision: 1 },
      expect.anything(),
    );
  });
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
  } as unknown as ReturnType<typeof queries.useReorderChain>);
  vi.mocked(queries.useInsertChainEntry).mockReturnValue({
    mutate: vi.fn(),
  } as unknown as ReturnType<typeof queries.useInsertChainEntry>);
  vi.mocked(queries.useDeleteChainEntry).mockReturnValue({
    mutate: vi.fn(),
  } as unknown as ReturnType<typeof queries.useDeleteChainEntry>);

  renderWithProviders(<RouterSlotEditor principalId="p-1" />);

  const basicTab = screen.getByRole('tab', { name: 'Basic' });
  expect(basicTab.getAttribute('aria-disabled')).toBe('true');

  // Click basic tab should show notice
  fireEvent.click(basicTab);
  expect(
    screen.getAllByText(
      /Basic can't show this chain without losing the extra filters/,
    )[0],
  ).toBeDefined();
});

test('Smart routing toggle inserts subscription-preference at order 0 then removes it', async () => {
  const insertMock = vi.fn();
  const deleteMock = vi.fn();

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
  } as unknown as ReturnType<typeof queries.useReorderChain>);
  vi.mocked(queries.useInsertChainEntry).mockReturnValue({
    mutate: insertMock,
    isPending: false,
  } as unknown as ReturnType<typeof queries.useInsertChainEntry>);
  vi.mocked(queries.useDeleteChainEntry).mockReturnValue({
    mutate: deleteMock,
    isPending: false,
  } as unknown as ReturnType<typeof queries.useDeleteChainEntry>);

  const { rerender } = renderWithProviders(
    <RouterSlotEditor principalId="p-1" />,
  );

  const toggle = screen.getByRole('switch');
  expect(toggle.getAttribute('aria-checked')).toBe('false');

  fireEvent.click(toggle);

  await waitFor(() => {
    expect(insertMock).toHaveBeenCalledWith({
      pid: 'p-1',
      body: {
        slot: 'router',
        wasm_registry_id: 'subscription-preference-id',
        order: 0,
      },
    });
  });

  // Mock that it was added
  vi.mocked(queries.usePluginChain).mockReturnValue({
    data: {
      entries: [
        {
          id: 'entry-cache',
          order: 0,
          wasm_registry_id: 'subscription-preference-id',
          revision: 2,
        },
      ],
    },
  } as unknown as ReturnType<typeof queries.usePluginChain>);

  rerender(
    <QueryClientProvider client={queryClient}>
      <RouterSlotEditor principalId="p-1" />
    </QueryClientProvider>,
  );

  const toggleOn = screen.getByRole('switch');
  expect(toggleOn.getAttribute('aria-checked')).toBe('true');

  fireEvent.click(toggleOn);

  await waitFor(() => {
    expect(deleteMock).toHaveBeenCalledWith({
      id: 'entry-cache',
      revision: 2,
    });
  });
});

test('picker disables already-in-chain entries and Basic-managed subscription-preference', () => {
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
  } as unknown as ReturnType<typeof queries.useReorderChain>);
  vi.mocked(queries.useInsertChainEntry).mockReturnValue({
    mutate: vi.fn(),
  } as unknown as ReturnType<typeof queries.useInsertChainEntry>);
  vi.mocked(queries.useDeleteChainEntry).mockReturnValue({
    mutate: vi.fn(),
  } as unknown as ReturnType<typeof queries.useDeleteChainEntry>);

  renderWithProviders(<RouterSlotEditor principalId="p-1" />);

  // Switch to Advanced tab
  fireEvent.click(screen.getByRole('tab', { name: 'Advanced' }));

  // Open picker
  fireEvent.click(screen.getByText('Add filter'));

  const preferenceBtn = screen.getAllByRole('button', {
    name: /subscription-preference/,
  })[1];
  expect(preferenceBtn.hasAttribute('disabled')).toBe(true);
  expect(screen.getByText('Managed by Basic')).toBeDefined();

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
  } as unknown as ReturnType<typeof queries.useReorderChain>);
  vi.mocked(queries.useInsertChainEntry).mockReturnValue({
    mutate: insertMock,
    isPending: false,
  } as unknown as ReturnType<typeof queries.useInsertChainEntry>);
  vi.mocked(queries.useDeleteChainEntry).mockReturnValue({
    mutate: vi.fn(),
  } as unknown as ReturnType<typeof queries.useDeleteChainEntry>);

  renderWithProviders(<RouterSlotEditor principalId="p-1" />);

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
