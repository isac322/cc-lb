import { cleanup, fireEvent, render, screen } from '@testing-library/react';
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
  };
});

const deleteMutate = vi.fn();
const patchMutate = vi.fn();
let deleteIsPending: boolean;
let patchIsPending: boolean;

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
  deleteMutate.mockReset();
  patchMutate.mockReset();
  deleteIsPending = false;
  patchIsPending = false;
  vi.mocked(queries.useDeletePlugin).mockImplementation(
    () =>
      ({
        mutate: deleteMutate,
        isPending: deleteIsPending,
      }) as never,
  );
  vi.mocked(queries.usePatchPlugin).mockImplementation(
    () =>
      ({
        mutate: patchMutate,
        isPending: patchIsPending,
      }) as never,
  );
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

  test('locks the label draft and shows save progress while patching', () => {
    const view = render(
      <PluginDetailOperate plugin={plugin} onDelete={() => {}} />,
    );

    fireEvent.click(screen.getByRole('button', { name: 'Edit' }));
    fireEvent.change(screen.getByPlaceholderText('Optional label'), {
      target: { value: 'release candidate' },
    });

    patchIsPending = true;
    view.rerender(<PluginDetailOperate plugin={plugin} onDelete={() => {}} />);

    const form = screen.getByTestId('plugin-label-edit-form');
    expect(form.getAttribute('aria-busy')).toBe('true');

    const input = screen.getByPlaceholderText(
      'Optional label',
    ) as HTMLInputElement;
    expect(input.value).toBe('release candidate');
    expect(input.hasAttribute('disabled')).toBe(true);
    expect(
      screen.getByRole('button', { name: 'Cancel' }).hasAttribute('disabled'),
    ).toBe(true);

    const saving = screen.getByRole('button', { name: 'Saving...' });
    expect(saving.hasAttribute('disabled')).toBe(true);
    expect(saving.getAttribute('aria-busy')).toBe('true');
    expect(saving.querySelector('svg.animate-spin')).not.toBeNull();

    fireEvent.click(screen.getByRole('button', { name: 'Cancel' }));
    expect(screen.getByPlaceholderText('Optional label')).toBeDefined();
    expect(
      (screen.getByPlaceholderText('Optional label') as HTMLInputElement).value,
    ).toBe('release candidate');
  });

  test('disables destructive confirmation until references finish loading', () => {
    const pendingDelete = {
      id: plugin.id,
      revision: plugin.revision,
      name: plugin.name,
      refcount: 1,
    };
    setReferencesQuery(referencesQuery([], true));

    render(
      <PluginDeleteDialog pendingDelete={pendingDelete} onClose={() => {}} />,
    );

    const confirm = screen.getByRole('button', {
      name: 'Delete and remove uses',
    });
    expect(confirm.hasAttribute('disabled')).toBe(true);
    fireEvent.click(confirm);
    expect(deleteMutate).not.toHaveBeenCalled();
  });

  test('keeps the delete dialog open until the mutation succeeds', () => {
    const onClose = vi.fn();
    let completeDelete: (() => void) | undefined;
    deleteMutate.mockImplementation(
      (_variables: unknown, options: { onSuccess: () => void }) => {
        completeDelete = options.onSuccess;
      },
    );
    setReferencesQuery(
      referencesQuery([
        {
          kind: 'plugin_chain',
          principal_id: 'principal-1',
          revision: 1,
          slot: 'router',
        },
      ]),
    );

    render(
      <PluginDeleteDialog
        pendingDelete={{
          id: plugin.id,
          revision: plugin.revision,
          name: plugin.name,
          refcount: 1,
        }}
        onClose={onClose}
      />,
    );

    fireEvent.click(
      screen.getByRole('button', { name: 'Delete and remove uses' }),
    );

    expect(deleteMutate).toHaveBeenCalledTimes(1);
    expect(onClose).not.toHaveBeenCalled();
    expect(
      screen.getByText('Delete Plugin and Remove References?'),
    ).toBeDefined();

    completeDelete?.();
    expect(onClose).toHaveBeenCalledTimes(1);
  });

  test('retains and locks the delete dialog while deletion is pending', () => {
    const onClose = vi.fn();
    deleteIsPending = true;
    setReferencesQuery(
      referencesQuery([
        {
          kind: 'plugin_chain',
          principal_id: 'principal-1',
          revision: 1,
          slot: 'router',
        },
      ]),
    );

    render(
      <PluginDeleteDialog
        pendingDelete={{
          id: plugin.id,
          revision: plugin.revision,
          name: plugin.name,
          refcount: 1,
        }}
        onClose={onClose}
      />,
    );

    const dialog = screen.getByRole('alertdialog');
    const cancel = screen.getByRole('button', { name: 'Cancel' });
    const deleting = screen.getByRole('button', { name: 'Deleting...' });
    expect(cancel.hasAttribute('disabled')).toBe(true);
    expect(deleting.hasAttribute('disabled')).toBe(true);
    expect(deleting.getAttribute('aria-busy')).toBe('true');
    expect(deleting.querySelector('svg.animate-spin')).not.toBeNull();

    fireEvent.click(cancel);
    fireEvent.keyDown(dialog, { key: 'Escape', code: 'Escape' });

    expect(onClose).not.toHaveBeenCalled();
    expect(
      screen.getByText('Delete Plugin and Remove References?'),
    ).toBeDefined();
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
    expect(list.querySelectorAll('[role="listitem"]')).toHaveLength(
      pendingDelete.refcount,
    );
    const skeletons = list.querySelectorAll('.skeleton');
    expect(skeletons).toHaveLength(pendingDelete.refcount);
    skeletons.forEach((skeleton) => {
      expect(skeleton.tagName).toBe('SPAN');
      expect(skeleton.className).toContain('block');
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
    expect(list.querySelectorAll('[role="listitem"]')).toHaveLength(
      pendingDelete.refcount,
    );
    expect(list.querySelector('ul, ol, li, div')).toBeNull();
  });
});
