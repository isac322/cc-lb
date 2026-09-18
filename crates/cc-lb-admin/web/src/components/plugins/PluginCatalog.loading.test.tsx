import { cleanup, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest';
import type { PluginEntry } from '../../lib/queries';
import { PluginCatalog } from './PluginCatalog';
import { PluginsPage } from './PluginsPage';

type RegistryState = {
  data: { entries: PluginEntry[] } | undefined;
  isLoading: boolean;
};

let selectedPluginId: string | undefined;
let registryState: RegistryState;
let gcIsPending: boolean;
const navigate = vi.fn();
const gcMutate = vi.fn();
const uploadMutate = vi.fn();

const pluginEntry = (id: string, name: string): PluginEntry => ({
  id,
  sha256_hex: id.repeat(16).slice(0, 64),
  name,
  original_filename: `${name}.wasm`,
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

const PLUGIN_ENTRIES = [
  pluginEntry('plugin-1', 'First plugin'),
  pluginEntry('plugin-2', 'Second plugin'),
];

vi.mock('@tanstack/react-router', () => ({
  useNavigate: () => navigate,
}));

vi.mock('../../routes/plugins', () => ({
  Route: {
    id: '/plugins',
    useSearch: () => ({ plugin: selectedPluginId }),
  },
}));

vi.mock('../../lib/queries', () => ({
  usePluginRegistry: () => registryState,
  useGcPlugins: () => ({
    isPending: gcIsPending,
    mutate: gcMutate,
  }),
  useUploadWasm: () => ({
    isPending: false,
    mutate: uploadMutate,
  }),
}));

vi.mock('../../lib/useCopyButton', () => ({
  useCopyButton: () => ({ copy: vi.fn() }),
}));

vi.mock('./PluginDeleteDialog', () => ({
  PluginDeleteDialog: () => null,
}));

vi.mock('./PluginDetail', () => ({
  PluginDetail: () => <div data-testid="loaded-plugin-detail" />,
}));

vi.mock('./PluginUploadCard', () => ({
  PluginUploadCard: () => <div data-testid="plugin-upload-card" />,
}));

beforeEach(() => {
  selectedPluginId = undefined;
  registryState = { data: { entries: [] }, isLoading: false };
  gcIsPending = false;
  navigate.mockReset();
  gcMutate.mockReset();
  uploadMutate.mockReset();
});

afterEach(cleanup);

describe('plugin loading geometry', () => {
  test('keeps a deep-linked cold load in a PluginDetail-shaped shell', () => {
    selectedPluginId = 'plugin-1';
    registryState = { data: undefined, isLoading: true };

    render(<PluginsPage />);

    const detailShell = screen.getByRole('status', {
      name: 'Loading plugin details',
    });
    expect(detailShell.className).toContain('space-y-6');
    expect(detailShell.className).toContain('mt-6');

    const detailGrid = detailShell.querySelector('.grid');
    expect(detailGrid?.className).toContain('grid-cols-1');
    expect(detailGrid?.className).toContain('lg:grid-cols-3');
    expect(detailShell.querySelectorAll('section')).toHaveLength(5);
    expect(detailShell.querySelectorAll('.skeleton').length).toBeGreaterThan(
      10,
    );

    expect(screen.queryByText('Plugin library')).toBeNull();
    expect(screen.queryByTestId('plugin-upload-card')).toBeNull();
    expect(screen.queryByTestId('loaded-plugin-detail')).toBeNull();
  });

  test('shows orphan cleanup progress and locks only the cleanup action while pending', () => {
    registryState = {
      data: {
        entries: [
          { ...pluginEntry('unused-plugin', 'Unused plugin'), refcount: 0 },
        ],
      },
      isLoading: false,
    };
    gcIsPending = true;

    render(<PluginCatalog onSelectPlugin={vi.fn()} />);

    const cleaning = screen.getByRole('button', { name: 'Cleaning...' });
    expect(cleaning.hasAttribute('disabled')).toBe(true);
    expect(cleaning.getAttribute('aria-busy')).toBe('true');
    expect(cleaning.querySelector('svg.animate-spin')).not.toBeNull();

    const progress = screen.getByTestId('plugin-gc-progress');
    expect(progress.textContent).toBe(
      'Cleaning orphaned uploads — waiting for the server.',
    );
    expect(progress.getAttribute('aria-live')).toBe('polite');
    expect(screen.queryByText('Clean orphaned uploads')).toBeNull();
    expect(
      screen
        .getByRole('button', { name: 'Delete plugin' })
        .hasAttribute('disabled'),
    ).toBe(false);
  });

  test('offers orphan cleanup without treating registered usage as its target count', () => {
    registryState = {
      data: { entries: [pluginEntry('used-plugin', 'Used plugin')] },
      isLoading: false,
    };

    render(<PluginCatalog onSelectPlugin={vi.fn()} />);

    const cleanupAction = screen.getByRole('button', {
      name: 'Clean orphaned uploads',
    });
    expect(cleanupAction.hasAttribute('disabled')).toBe(false);
    expect(cleanupAction.getAttribute('title')).toContain(
      'Registered plugins remain available',
    );
    expect(screen.getByTestId('plugin-count-slot').textContent).toContain(
      '0 not used anywhere',
    );
  });

  test('keeps catalog geometry stable while counts and rows load', () => {
    registryState = { data: undefined, isLoading: true };

    const { rerender } = render(<PluginCatalog onSelectPlugin={vi.fn()} />);

    const countSlot = screen.getByTestId('plugin-count-slot');
    expect(countSlot.className).toContain('min-h-5');
    expect(countSlot.className).toContain('min-w-56');
    const countSkeleton =
      countSlot.querySelector<HTMLSpanElement>('span.skeleton');
    expect(countSkeleton?.tagName).toBe('SPAN');
    expect(countSkeleton?.getAttribute('aria-hidden')).toBe('true');
    expect(countSkeleton?.className).toContain('block');
    expect(countSkeleton?.className).toContain('h-4');
    expect(countSkeleton?.className).toContain('w-48');
    expect(
      countSlot
        .closest('[data-slot="card-subtitle"]')
        ?.querySelector('div.skeleton'),
    ).toBeNull();
    expect(countSlot.textContent).not.toContain('0 available');

    const table = screen.getByRole('table') as HTMLTableElement;
    expect(table.className).toContain('min-w-[960px]');
    expect(table.className).toContain('table-fixed');
    const catalogViewport = table.parentElement;
    expect(catalogViewport?.classList.contains('min-h-72')).toBe(true);
    expect(catalogViewport?.classList.contains('h-72')).toBe(false);
    expect(catalogViewport?.classList.contains('overflow-y-auto')).toBe(false);
    expect(screen.getAllByRole('columnheader')).toHaveLength(7);

    expect(
      Array.from(table.querySelectorAll('col')).map((col) => col.className),
    ).toEqual([
      'w-1/4',
      'w-1/6',
      'w-1/6',
      'w-1/12',
      'w-1/12',
      'w-1/8',
      'w-1/8',
    ]);

    const loadingRows = Array.from(table.tBodies[0]?.rows ?? []);
    expect(loadingRows).toHaveLength(3);
    for (const row of loadingRows) {
      expect(row.className).toContain('border-b');
      expect(row.className).toContain('h-20');
      expect(row.cells).toHaveLength(7);
      for (const cell of Array.from(row.cells)) {
        expect(cell.hasAttribute('colspan')).toBe(false);
        expect(cell.querySelector('.skeleton')).not.toBeNull();
      }
      const skeletonClassNames = Array.from(
        row.cells,
        (cell) => cell.querySelector('.skeleton')?.className,
      );
      expect(skeletonClassNames).toEqual([
        expect.stringContaining('h-16 w-4/5'),
        expect.stringContaining('h-5 w-20'),
        expect.stringContaining('h-4 w-28'),
        expect.stringContaining('ml-auto h-4 w-16'),
        expect.stringContaining('mx-auto h-5 w-10'),
        expect.stringContaining('h-4 w-20'),
        expect.stringContaining('ml-auto h-4 w-24'),
      ]);
    }

    registryState = {
      data: { entries: PLUGIN_ENTRIES },
      isLoading: false,
    };
    rerender(<PluginCatalog onSelectPlugin={vi.fn()} />);

    const loadedTable = screen.getByRole('table') as HTMLTableElement;
    const loadedRows = Array.from(loadedTable.tBodies[0]?.rows ?? []);
    expect(loadedRows).toHaveLength(PLUGIN_ENTRIES.length);
    for (const row of loadedRows) {
      expect(row.className).toContain('h-20');
    }
    expect(loadedTable.parentElement).toBe(catalogViewport);
    expect(
      screen.getByTestId('plugin-count-slot').classList.contains('min-w-56'),
    ).toBe(true);
    expect(screen.getByTestId('plugin-count-slot').textContent).toContain(
      '2 available · 0 not used anywhere',
    );

    registryState = { data: { entries: [] }, isLoading: false };
    rerender(<PluginCatalog onSelectPlugin={vi.fn()} />);

    const emptyTable = screen.getByRole('table') as HTMLTableElement;
    expect(emptyTable.parentElement).toBe(catalogViewport);
    expect(emptyTable.parentElement?.classList.contains('min-h-72')).toBe(true);
    expect(emptyTable.tBodies[0]?.rows).toHaveLength(1);
    expect(screen.getByTestId('plugin-count-slot').textContent).toContain(
      '0 available · 0 not used anywhere',
    );
    expect(screen.getByText('No plugins uploaded.')).toBeDefined();
  });
});
