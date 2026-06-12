import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from '@testing-library/react';
import { afterEach, beforeEach, expect, test, vi } from 'vitest';
import { z } from 'zod';
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
          id: 'cache-affinity-id',
          name: 'cache-affinity',
          metadata: null,
          sha256_hex: '',
        }
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

  // It should default to Advanced tab because it's a complex chain (no cache-affinity)
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
  expect(screen.getByText('Keep prompt cache warm by reusing upstreams')).toBeDefined();
  
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
          id: 'cache-affinity-id',
          name: 'cache-affinity',
          metadata: null,
          sha256_hex: '',
        }
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
  expect(screen.getAllByText(/Basic can't show this chain without losing the extra filters/)[0]).toBeDefined();
});

test('Sticky toggle inserts cache-affinity at order 0 then removes it', async () => {
  const insertMock = vi.fn();
  const deleteMock = vi.fn();
  
  vi.mocked(queries.usePluginChain).mockReturnValue({
    data: { entries: [] },
  } as unknown as ReturnType<typeof queries.usePluginChain>);
  vi.mocked(queries.usePluginRegistry).mockReturnValue({
    data: {
      entries: [
        {
          id: 'cache-affinity-id',
          name: 'cache-affinity',
          metadata: null,
          sha256_hex: '',
        }
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

  const { rerender } = renderWithProviders(<RouterSlotEditor principalId="p-1" />);

  const toggle = screen.getByRole('switch');
  expect(toggle.getAttribute('aria-checked')).toBe('false');
  
  fireEvent.click(toggle);
  
  await waitFor(() => {
    expect(insertMock).toHaveBeenCalledWith({
      pid: 'p-1',
      body: {
        slot: 'router',
        wasm_registry_id: 'cache-affinity-id',
        order: 0,
      }
    });
  });

  // Mock that it was added
  vi.mocked(queries.usePluginChain).mockReturnValue({
    data: {
      entries: [
        {
          id: 'entry-cache',
          order: 0,
          wasm_registry_id: 'cache-affinity-id',
          revision: 2,
        },
      ],
    },
  } as unknown as ReturnType<typeof queries.usePluginChain>);

  rerender(<QueryClientProvider client={queryClient}><RouterSlotEditor principalId="p-1" /></QueryClientProvider>);

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

test('picker disables already-in-chain entries and pinned cache-affinity', () => {
  vi.mocked(queries.usePluginChain).mockReturnValue({
    data: {
      entries: [
        {
          id: 'entry-cache',
          order: 0,
          wasm_registry_id: 'cache-affinity-id',
          revision: 1,
        },
      ],
    },
  } as unknown as ReturnType<typeof queries.usePluginChain>);
  vi.mocked(queries.usePluginRegistry).mockReturnValue({
    data: {
      entries: [
        {
          id: 'cache-affinity-id',
          name: 'cache-affinity',
          metadata: null,
          sha256_hex: '',
        },
        {
          id: 'other-id',
          name: 'other-plugin',
          metadata: null,
          sha256_hex: '',
        }
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
  
  const cacheBtn = screen.getAllByRole('button', { name: /cache-affinity/ })[1]; // The one in the picker
  expect(cacheBtn.hasAttribute('disabled')).toBe(true);
  expect(screen.getByText('Pinned by Sticky')).toBeDefined();
  
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
          wasm_registry_id: 'cache-affinity-id',
          revision: 1,
        },
      ],
    },
  } as unknown as ReturnType<typeof queries.usePluginChain>);
  vi.mocked(queries.usePluginRegistry).mockReturnValue({
    data: {
      entries: [
        {
          id: 'cache-affinity-id',
          name: 'cache-affinity',
          metadata: null,
          sha256_hex: '',
        }
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
  
  const cacheBtn = screen.getAllByRole('button', { name: /cache-affinity/ })[1];
  fireEvent.click(cacheBtn);
  
  expect(insertMock).not.toHaveBeenCalled();
});
