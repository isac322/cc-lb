import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from '@testing-library/react';
import { afterEach, beforeEach, expect, test, vi } from 'vitest';
import * as queries from '../lib/queries';
import { RouterSlotEditor } from './principals';

vi.mock('../lib/queries', async () => {
  const actual =
    await vi.importActual<typeof import('../lib/queries')>('../lib/queries');
  return {
    ...actual,
    usePluginChain: vi.fn(),
    usePluginRegistry: vi.fn(),
    useReorderChain: vi.fn(),
    useInsertChainEntry: vi.fn(),
    useDeleteChainEntry: vi.fn(),
    useRouterTerminalStrategy: vi.fn(),
    useUpdateRouterTerminalStrategy: vi.fn(),
  };
});

const queryClient = new QueryClient({
  defaultOptions: { queries: { retry: false } },
});

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
  } as any);
  vi.mocked(queries.usePluginRegistry).mockReturnValue({
    data: {
      entries: [{ id: 'plugin-1', name: 'My Plugin' }],
    },
  } as any);
  vi.mocked(queries.useRouterTerminalStrategy).mockReturnValue({
    data: { strategy: 'first-pick', revision: 1 },
    isLoading: false,
  } as any);
  vi.mocked(queries.useUpdateRouterTerminalStrategy).mockReturnValue({
    mutate: vi.fn(),
    isPending: false,
  } as any);
  vi.mocked(queries.useReorderChain).mockReturnValue({
    mutate: vi.fn(),
  } as any);
  vi.mocked(queries.useInsertChainEntry).mockReturnValue({
    mutate: vi.fn(),
  } as any);
  vi.mocked(queries.useDeleteChainEntry).mockReturnValue({
    mutate: vi.fn(),
  } as any);

  renderWithProviders(<RouterSlotEditor principalId="p-1" />);

  expect(screen.getByText('My Plugin')).toBeDefined();
  expect(screen.getByText('Terminal')).toBeDefined();
  expect(screen.getByText('Final upstream selection')).toBeDefined();

  const select = screen.getByRole('combobox', {
    name: 'Terminal strategy',
  }) as HTMLSelectElement;
  expect(select.value).toBe('first-pick');
});

test('toggles terminal strategy', async () => {
  vi.mocked(queries.usePluginChain).mockReturnValue({
    data: { entries: [] },
  } as any);
  vi.mocked(queries.usePluginRegistry).mockReturnValue({
    data: { entries: [] },
  } as any);
  const mutateMock = vi.fn();
  vi.mocked(queries.useRouterTerminalStrategy).mockReturnValue({
    data: { strategy: 'first-pick', revision: 1 },
    isLoading: false,
  } as any);
  vi.mocked(queries.useUpdateRouterTerminalStrategy).mockReturnValue({
    mutate: mutateMock,
    isPending: false,
  } as any);
  vi.mocked(queries.useReorderChain).mockReturnValue({
    mutate: vi.fn(),
  } as any);
  vi.mocked(queries.useInsertChainEntry).mockReturnValue({
    mutate: vi.fn(),
  } as any);
  vi.mocked(queries.useDeleteChainEntry).mockReturnValue({
    mutate: vi.fn(),
  } as any);

  renderWithProviders(<RouterSlotEditor principalId="p-1" />);

  const select = screen.getByRole('combobox', { name: 'Terminal strategy' });
  fireEvent.change(select, { target: { value: 'random' } });

  await waitFor(() => {
    expect(mutateMock).toHaveBeenCalledWith(
      { id: 'p-1', strategy: 'random', revision: 1 },
      expect.anything(),
    );
  });
});
