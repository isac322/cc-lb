import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, test, vi } from 'vitest';
import {
  applyEntityListView,
  EntityList,
  type EntityListView,
  type EntityListViewConfig,
} from './EntityList';

type Item = { id: string; name: string; on: boolean };
type F = 'all' | 'on';
type S = 'name' | 'reverse';

const CONFIG: EntityListViewConfig<Item, F, S> = {
  searchText: (item) => [item.name, item.id],
  defaultFilter: 'all',
  filters: { all: () => true, on: (item) => item.on },
  sorts: {
    name: (a, b) => a.name.localeCompare(b.name),
    reverse: (a, b) => b.name.localeCompare(a.name),
  },
};

function items(count: number): Item[] {
  return Array.from({ length: count }, (_, i) => ({
    id: `id-${i}`,
    name: `Item ${String.fromCharCode(97 + i)}`,
    on: i % 2 === 0,
  }));
}

function setViewport(desktop: boolean) {
  Object.defineProperty(window, 'matchMedia', {
    writable: true,
    value: vi.fn().mockReturnValue({ matches: desktop }),
  });
}

function renderList(
  list: Item[],
  view: EntityListView<F, S>,
  selectedId?: string,
) {
  const onSelect = vi.fn();
  const onViewChange = vi.fn();
  const onClear = vi.fn();
  render(
    <EntityList<Item, F, S>
      title="Things"
      noun="things"
      countLine={`${list.length} things`}
      action={null}
      loading={false}
      totalCount={list.length}
      result={applyEntityListView(list, view, CONFIG)}
      toolbar={{
        view,
        filterOptions: [
          { value: 'all', label: 'All' },
          { value: 'on', label: 'On' },
        ],
        sortOptions: [
          { value: 'name', label: 'Name' },
          { value: 'reverse', label: 'Reverse' },
        ],
        onViewChange,
        onClear,
      }}
      getId={(item) => item.id}
      renderRow={(item) => ({ name: item.name, caption: item.id })}
      selectedId={selectedId}
      onSelect={onSelect}
      empty={<p>Nothing yet</p>}
      skeletonTestId="thing-skeleton"
    />,
  );
  return { onSelect, onViewChange, onClear };
}

afterEach(() => cleanup());

describe('applyEntityListView', () => {
  test('a short list ignores search and filter but still sorts', () => {
    const result = applyEntityListView(
      items(3),
      { q: 'zzz', filter: 'on', sort: 'reverse' },
      CONFIG,
    );
    expect(result.toolbar).toBe(false);
    expect(result.narrowed).toBe(false);
    expect(result.visible.map((i) => i.id)).toEqual(['id-2', 'id-1', 'id-0']);
  });

  test('from eight items search and filter combine, case-insensitively', () => {
    const result = applyEntityListView(
      items(8),
      { q: 'ITEM', filter: 'on', sort: 'name' },
      CONFIG,
    );
    expect(result.toolbar).toBe(true);
    expect(result.narrowed).toBe(true);
    expect(result.visible.map((i) => i.id)).toEqual([
      'id-0',
      'id-2',
      'id-4',
      'id-6',
    ]);
    expect(
      applyEntityListView(
        items(8),
        { q: 'id-7', filter: 'all', sort: 'name' },
        CONFIG,
      ).visible.map((i) => i.id),
    ).toEqual(['id-7']);
  });
});

describe('EntityList', () => {
  test('hides the toolbar below eight items', () => {
    setViewport(true);
    renderList(items(3), { q: '', filter: 'all', sort: 'name' });
    expect(screen.queryByTestId('entity-list-search')).toBeNull();
    expect(screen.queryByTestId('entity-list-result-line')).toBeNull();
  });

  test('shows the result line and a distinct no-match state when narrowed', () => {
    setViewport(true);
    const { onClear } = renderList(items(9), {
      q: 'nope',
      filter: 'all',
      sort: 'name',
    });
    expect(screen.getByTestId('entity-list-result-line').textContent).toContain(
      'Showing 0 of 9',
    );
    expect(screen.getByText('No things match "nope"')).toBeDefined();
    expect(screen.queryByText('Nothing yet')).toBeNull();
    fireEvent.click(screen.getByRole('button', { name: 'Clear filters' }));
    expect(onClear).toHaveBeenCalledTimes(1);
  });

  test('the list is one tab stop and marks the selected row', () => {
    setViewport(true);
    renderList(items(3), { q: '', filter: 'all', sort: 'name' }, 'id-1');
    const list = screen.getByRole('list', { name: 'Things' });
    expect(list.tabIndex).toBe(0);
    const rows = screen.getAllByRole('button');
    for (const row of rows) expect(row.tabIndex).toBe(-1);
    const selected = rows.find((row) => row.getAttribute('aria-current'));
    expect(selected?.textContent).toContain('Item b');
    expect(selected?.getAttribute('aria-current')).toBe('true');
  });

  test('arrow keys move the selection from md, clamped at the ends', () => {
    setViewport(true);
    const { onSelect } = renderList(
      items(3),
      { q: '', filter: 'all', sort: 'name' },
      'id-1',
    );
    const list = screen.getByRole('list', { name: 'Things' });
    fireEvent.keyDown(list, { key: 'ArrowDown' });
    expect(onSelect).toHaveBeenLastCalledWith('id-2');
    fireEvent.keyDown(list, { key: 'Home' });
    expect(onSelect).toHaveBeenLastCalledWith('id-0');
    fireEvent.keyDown(list, { key: 'End' });
    expect(onSelect).toHaveBeenLastCalledWith('id-2');
  });

  test('on phones arrows only highlight and Enter opens the detail', () => {
    setViewport(false);
    const { onSelect } = renderList(items(3), {
      q: '',
      filter: 'all',
      sort: 'name',
    });
    const list = screen.getByRole('list', { name: 'Things' });
    fireEvent.keyDown(list, { key: 'ArrowDown' });
    fireEvent.keyDown(list, { key: 'ArrowDown' });
    expect(onSelect).not.toHaveBeenCalled();
    fireEvent.keyDown(list, { key: 'Enter' });
    expect(onSelect).toHaveBeenCalledWith('id-1');
  });
});
