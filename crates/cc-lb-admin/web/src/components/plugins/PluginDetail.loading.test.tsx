import { cleanup, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest';
import type { PluginEntry, PluginReference } from '../../lib/queries';
import * as queries from '../../lib/queries';
import { PluginDeleteDialog } from './PluginDeleteDialog';
import { PluginDetail } from './PluginDetail';
import { PluginDetailOperate } from './PluginDetailOperate';

vi.mock('../../lib/queries', async () => {
  const actual =
    await vi.importActual<Record<string, unknown>>('../../lib/queries');
  return {
    ...actual,
    useDeletePlugin: vi.fn(),
    usePatchPlugin: vi.fn(),
    usePluginReferences: vi.fn(),
    useStatus: vi.fn(),
  };
});

const plugin: PluginEntry = {
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
  refcount: 1,
  revision: 1,
  uploaded_at_unix_secs: 0,
  supported_slots: ['router'],
};

type ReferenceQueryMock = {
  data:
    | {
        registry: PluginEntry;
        refcount: number;
        reference_fingerprint: string;
        references: PluginReference[];
      }
    | undefined;
  isLoading: boolean;
};

function referencesQuery(
  references: PluginReference[],
  isLoading = false,
): ReferenceQueryMock {
  return {
    data: isLoading
      ? undefined
      : {
          registry: plugin,
          refcount: references.length,
          reference_fingerprint: 'fingerprint-123',
          references,
        },
    isLoading,
  };
}

function setReferencesQuery(query: ReferenceQueryMock) {
  vi.mocked(queries.usePluginReferences).mockReturnValue(query as never);
}

const usedByStates: [string, ReferenceQueryMock][] = [
  ['pending', referencesQuery([], true)],
  ['zero references', referencesQuery([])],
  [
    'one reference',
    referencesQuery([
      {
        kind: 'plugin_chain',
        principal_id: 'principal-1',
        principal_name: 'Principal One',
        revision: 1,
        slot: 'router',
      },
    ]),
  ],
];

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(queries.useDeletePlugin).mockReturnValue({
    mutate: vi.fn(),
  } as never);
  vi.mocked(queries.usePatchPlugin).mockReturnValue({
    mutate: vi.fn(),
  } as never);
  vi.mocked(queries.useStatus).mockReturnValue({
    data: {
      killswitch: false,
      plugin_chain_summary: {
        principal_count_with_chain: 1,
        total_entries: 1,
      },
    },
    isLoading: false,
  } as never);
  setReferencesQuery(referencesQuery([]));
});

afterEach(cleanup);

describe('PluginDetail loading geometry', () => {
  test.each(usedByStates)(
    'keeps the Used by content slot height in the %s state',
    (_, query) => {
      setReferencesQuery(query);

      render(<PluginDetail plugin={plugin} onBack={() => {}} />);

      expect(screen.getByTestId('plugin-used-by-slot').className).toContain(
        'min-h-20',
      );
    },
  );

  test('renders two structured Used by skeleton lines while pending', () => {
    setReferencesQuery(referencesQuery([], true));

    render(<PluginDetail plugin={plugin} onBack={() => {}} />);

    const slot = screen.getByTestId('plugin-used-by-slot');
    expect(slot.querySelectorAll('.skeleton')).toHaveLength(2);
  });

  test('reserves the Global Usage line while status resolves', () => {
    vi.mocked(queries.useStatus).mockReturnValue({
      data: undefined,
      isLoading: true,
    } as never);

    const view = render(
      <PluginDetailOperate plugin={plugin} onDelete={() => {}} />,
    );

    let slot = screen.getByTestId('plugin-global-usage-slot');
    expect(slot.className).toContain('min-h-4');
    expect(slot.querySelectorAll('.skeleton')).toHaveLength(1);

    vi.mocked(queries.useStatus).mockReturnValue({
      data: {
        killswitch: false,
        plugin_chain_summary: {
          principal_count_with_chain: 1,
          total_entries: 1,
        },
      },
      isLoading: false,
    } as never);
    view.rerender(<PluginDetailOperate plugin={plugin} onDelete={() => {}} />);

    slot = screen.getByTestId('plugin-global-usage-slot');
    expect(slot.className).toContain('min-h-4');
    expect(slot.querySelectorAll('.skeleton')).toHaveLength(0);
    expect(slot.textContent).toContain('1 entries across 1 principals.');
  });

  test('matches pending delete skeleton rows to the known refcount', () => {
    const pendingDelete = {
      id: plugin.id,
      revision: plugin.revision,
      name: plugin.name,
      refcount: 3,
    };
    setReferencesQuery(referencesQuery([], true));

    const view = render(
      <PluginDeleteDialog pendingDelete={pendingDelete} onClose={() => {}} />,
    );

    let list = screen.getByTestId('plugin-delete-reference-list');
    const skeletons = list.querySelectorAll('.skeleton');
    expect(skeletons).toHaveLength(pendingDelete.refcount);
    skeletons.forEach((skeleton) => {
      expect(skeleton.className).toContain('w-full');
    });

    setReferencesQuery(
      referencesQuery([
        {
          kind: 'plugin_chain',
          principal_id: 'principal-1',
          revision: 1,
          slot: 'router',
        },
        {
          kind: 'plugin_chain',
          principal_id: 'principal-2',
          revision: 1,
          slot: 'shape',
        },
        {
          kind: 'upstream_warmup_dialect',
          upstream_id: 'upstream-1',
          revision: 1,
        },
      ]),
    );
    view.rerender(
      <PluginDeleteDialog pendingDelete={pendingDelete} onClose={() => {}} />,
    );

    list = screen.getByTestId('plugin-delete-reference-list');
    expect(list.querySelectorAll('.skeleton')).toHaveLength(0);
    expect(list.querySelectorAll('li')).toHaveLength(pendingDelete.refcount);
  });
});
