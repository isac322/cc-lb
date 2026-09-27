// The one list pane behind Upstreams and Principals: pane header, an
// optional toolbar (search, one filter, one sort) once the list is long
// enough to need it, a result line while filtered, and a single-tab-stop
// list of compact two-line rows. Selection is a neutral fill plus the name
// in full ink (DESIGN.md selection language); never an edge or a stripe.
import { Search } from 'lucide-react';
import {
  type KeyboardEvent,
  type ReactNode,
  useCallback,
  useMemo,
  useRef,
  useState,
} from 'react';
import { Button, cx, INPUT_SM_CLASS, PageHeader, Skeleton } from './primitives';
import { Select, type SelectOption } from './Select';

/** Lists shorter than this get no toolbar: with a handful of rows it is noise. */
export const ENTITY_LIST_TOOLBAR_MIN_ITEMS = 8;

const MD_QUERY = '(min-width: 768px)';

export interface EntityListView<F extends string, S extends string> {
  q: string;
  filter: F;
  sort: S;
}

export interface EntityListViewConfig<T, F extends string, S extends string> {
  /** Lower-cased strings the search box matches against (name, id, plan…). */
  searchText: (item: T) => readonly (string | null | undefined)[];
  filters: Record<F, (item: T) => boolean>;
  sorts: Record<S, (a: T, b: T) => number>;
  defaultFilter: F;
}

export interface EntityListViewResult<T> {
  visible: readonly T[];
  /** A search or a non-default filter is narrowing the list. */
  narrowed: boolean;
  /** The toolbar shows (the list is long enough to need it). */
  toolbar: boolean;
}

/**
 * Applies search, filter and sort. Search and filter only apply while the
 * toolbar shows, so a short list never hides rows behind controls the
 * operator cannot see; sort always applies (auto-select follows it).
 */
export function applyEntityListView<T, F extends string, S extends string>(
  items: readonly T[],
  view: EntityListView<F, S>,
  config: EntityListViewConfig<T, F, S>,
): EntityListViewResult<T> {
  const toolbar = items.length >= ENTITY_LIST_TOOLBAR_MIN_ITEMS;
  const needle = toolbar ? view.q.trim().toLowerCase() : '';
  const filter = toolbar ? view.filter : config.defaultFilter;
  const keep = config.filters[filter] ?? config.filters[config.defaultFilter];
  const compare = config.sorts[view.sort];
  const visible = items.filter(
    (item) =>
      keep(item) &&
      (needle === '' ||
        config
          .searchText(item)
          .some((text) => text?.toLowerCase().includes(needle))),
  );
  if (compare) visible.sort(compare);
  return {
    visible,
    narrowed: needle !== '' || filter !== config.defaultFilter,
    toolbar,
  };
}

/** Memoized `applyEntityListView` for route components. */
export function useEntityListView<T, F extends string, S extends string>(
  items: readonly T[],
  view: EntityListView<F, S>,
  config: EntityListViewConfig<T, F, S>,
): EntityListViewResult<T> {
  const { q, filter, sort } = view;
  return useMemo(
    () => applyEntityListView(items, { q, filter, sort }, config),
    [items, q, filter, sort, config],
  );
}

export interface EntityListRowContent {
  name: string;
  /** Disabled items read muted until selected. */
  muted?: boolean;
  /** Right of the name: one tabular figure. */
  trailing?: ReactNode;
  caption: ReactNode;
  /** Right of the caption: compact tabular facts. */
  captionTrailing?: ReactNode;
  /** Full value for the truncated line(s); defaults to the name. */
  title?: string;
}

export interface EntityListToolbarProps<F extends string, S extends string> {
  view: EntityListView<F, S>;
  filterOptions: readonly { value: F; label: string }[];
  sortOptions: readonly { value: S; label: string }[];
  onViewChange: (next: Partial<EntityListView<F, S>>) => void;
  /** Resets search and filter; sort is a preference and stays. */
  onClear: () => void;
}

export interface EntityListProps<T, F extends string, S extends string> {
  title: string;
  /** Plural noun for copy ("upstreams", "principals"). */
  noun: string;
  /** The count line under the title; `null` while loading. */
  countLine: ReactNode | null;
  action: ReactNode;
  loading: boolean;
  /** Every item, before search and filter. */
  totalCount: number;
  result: EntityListViewResult<T>;
  toolbar: EntityListToolbarProps<F, S>;
  getId: (item: T) => string;
  renderRow: (item: T) => EntityListRowContent;
  selectedId: string | undefined;
  onSelect: (id: string) => void;
  /** First-run state when there is nothing at all. */
  empty: ReactNode;
  skeletonTestId: string;
  countSkeletonTestId?: string;
  className?: string;
}

// `relative` keeps absolutely positioned descendants (sr-only text) inside
// the scrolling pane; without it they escape to the page and scroll it.
export const ENTITY_LIST_ROW_CLASS =
  'relative flex w-full min-h-[52px] flex-col justify-center gap-0.5 rounded-sm px-3 py-2 text-left transition-colors';

function isDesktop(): boolean {
  return (
    typeof window !== 'undefined' &&
    typeof window.matchMedia === 'function' &&
    window.matchMedia(MD_QUERY).matches
  );
}

function RowSkeleton({ testId }: { testId: string }) {
  return (
    <div
      className={ENTITY_LIST_ROW_CLASS}
      data-testid={testId}
      aria-hidden="true"
    >
      <div className="flex items-center justify-between gap-3">
        <Skeleton className="h-4 w-32" />
        <Skeleton className="h-4 w-8" />
      </div>
      <div className="flex items-center justify-between gap-3">
        <Skeleton className="h-3 w-40" />
        <Skeleton className="h-3 w-16" />
      </div>
    </div>
  );
}

function Toolbar<F extends string, S extends string>({
  noun,
  toolbar,
}: {
  noun: string;
  toolbar: EntityListToolbarProps<F, S>;
}) {
  const { view, filterOptions, sortOptions, onViewChange } = toolbar;
  return (
    <div className="flex flex-col gap-2">
      <label className="relative block">
        <span className="sr-only">Search {noun}</span>
        <Search
          aria-hidden="true"
          strokeWidth={1.75}
          className="pointer-events-none absolute left-2.5 top-1/2 size-3.5 -translate-y-1/2 text-text-faint"
        />
        <input
          type="search"
          value={view.q}
          placeholder={`Search ${noun}`}
          onChange={(e) => onViewChange({ q: e.target.value })}
          onKeyDown={(e) => {
            if (e.key === 'Escape' && view.q !== '') {
              e.preventDefault();
              onViewChange({ q: '' });
            }
          }}
          className={cx(INPUT_SM_CLASS, 'w-full pl-8 max-md:h-11')}
          data-testid="entity-list-search"
        />
      </label>
      <div className="flex gap-2">
        <Select
          size="sm"
          aria-label={`Filter ${noun}`}
          className="min-w-0 flex-1 max-md:h-11"
          value={view.filter}
          options={filterOptions as readonly SelectOption[]}
          onChange={(value) => {
            if (value) onViewChange({ filter: value as F });
          }}
          data-testid="entity-list-filter"
        />
        <Select
          size="sm"
          aria-label={`Sort ${noun}`}
          className="min-w-0 flex-1 max-md:h-11"
          value={view.sort}
          options={sortOptions as readonly SelectOption[]}
          onChange={(value) => {
            if (value) onViewChange({ sort: value as S });
          }}
          data-testid="entity-list-sort"
        />
      </div>
    </div>
  );
}

export function EntityList<T, F extends string, S extends string>({
  title,
  noun,
  countLine,
  action,
  loading,
  totalCount,
  result,
  toolbar,
  getId,
  renderRow,
  selectedId,
  onSelect,
  empty,
  skeletonTestId,
  countSkeletonTestId,
  className,
}: EntityListProps<T, F, S>) {
  const rowRefs = useRef(new Map<string, HTMLButtonElement>());
  // Phones keep the list on screen while arrowing: the arrow keys move a
  // highlight and Enter opens the detail. From md they move the selection.
  const [highlightId, setHighlightId] = useState<string | null>(null);
  const { visible, narrowed } = result;
  const ids = useMemo(() => visible.map(getId), [visible, getId]);

  const scrollToRow = useCallback((id: string) => {
    rowRefs.current.get(id)?.scrollIntoView?.({ block: 'nearest' });
  }, []);

  const onListKeyDown = (event: KeyboardEvent<HTMLUListElement>) => {
    if (ids.length === 0) return;
    const desktop = isDesktop();
    const currentId = desktop ? selectedId : (highlightId ?? selectedId);
    const index = currentId ? ids.indexOf(currentId) : -1;
    let next: number;
    switch (event.key) {
      case 'ArrowDown':
        next = index < 0 ? 0 : Math.min(ids.length - 1, index + 1);
        break;
      case 'ArrowUp':
        next = index < 0 ? 0 : Math.max(0, index - 1);
        break;
      case 'Home':
        next = 0;
        break;
      case 'End':
        next = ids.length - 1;
        break;
      case 'Enter':
      case ' ':
        if (!desktop && highlightId && ids.includes(highlightId)) {
          event.preventDefault();
          onSelect(highlightId);
        }
        return;
      default:
        return;
    }
    event.preventDefault();
    const id = ids[next];
    if (desktop) {
      if (id !== selectedId) onSelect(id);
    } else {
      setHighlightId(id);
    }
    scrollToRow(id);
  };

  const showToolbar = result.toolbar && !loading;
  const clear = toolbar.onClear;
  const query = toolbar.view.q.trim();

  return (
    <aside className={cx('flex min-h-0 flex-col', className)}>
      <div className="flex shrink-0 flex-col gap-3 border-b border-subtle px-4 py-3 [&>header]:mb-0">
        <PageHeader
          title={title}
          description={
            <span className="flex h-4 items-center text-caption text-text-faint">
              {countLine === null ? (
                <span
                  className="skeleton inline-block h-3 w-24"
                  data-testid={countSkeletonTestId}
                  aria-hidden="true"
                />
              ) : (
                countLine
              )}
            </span>
          }
          actions={action}
        />
        {showToolbar ? <Toolbar noun={noun} toolbar={toolbar} /> : null}
      </div>
      {showToolbar && narrowed ? (
        <div
          className="flex shrink-0 items-center justify-between gap-2 px-4 pt-2 text-caption text-text-muted"
          data-testid="entity-list-result-line"
        >
          <span role="status" className="tabular-nums">
            Showing {visible.length} of {totalCount}
          </span>
          <Button
            size="sm"
            variant="ghost"
            className="max-md:h-11"
            onClick={clear}
          >
            Clear
          </Button>
        </div>
      ) : null}
      <div className="min-h-0 flex-1 overflow-y-auto p-2 pb-8">
        {loading ? (
          <div className="flex flex-col gap-0.5">
            {Array.from({ length: 4 }).map((_, i) => (
              <RowSkeleton key={i} testId={skeletonTestId} />
            ))}
          </div>
        ) : totalCount === 0 ? (
          empty
        ) : visible.length === 0 ? (
          <div
            className="flex flex-col items-center gap-3 px-4 py-8 text-center"
            data-testid="entity-list-no-match"
          >
            <p className="text-body-sm text-text-muted">
              {query
                ? `No ${noun} match "${query}"`
                : `No ${noun} match this filter`}
            </p>
            <Button size="sm" className="max-md:h-11" onClick={clear}>
              Clear filters
            </Button>
          </div>
        ) : (
          <ul
            aria-label={title}
            // One tab stop: the rows are reached with the arrow keys.
            tabIndex={0}
            onKeyDown={onListKeyDown}
            onBlur={(e) => {
              if (!e.currentTarget.contains(e.relatedTarget as Node | null))
                setHighlightId(null);
            }}
            className="flex flex-col gap-0.5 rounded-sm outline-none focus-visible:outline-2 focus-visible:outline-accent focus-visible:-outline-offset-2"
          >
            {visible.map((item) => {
              const id = getId(item);
              const row = renderRow(item);
              const selected = id === selectedId;
              const highlighted = id === highlightId && !selected;
              return (
                <li key={id}>
                  <button
                    ref={(node) => {
                      if (node) rowRefs.current.set(id, node);
                      else rowRefs.current.delete(id);
                    }}
                    type="button"
                    tabIndex={-1}
                    onClick={() => {
                      setHighlightId(null);
                      onSelect(id);
                    }}
                    aria-current={selected ? 'true' : undefined}
                    title={row.title ?? row.name}
                    data-entity-id={id}
                    className={cx(
                      ENTITY_LIST_ROW_CLASS,
                      selected ? 'bg-selected' : 'hover:bg-overlay-2',
                      highlighted &&
                        'bg-overlay-2 outline-2 outline-accent -outline-offset-2',
                    )}
                  >
                    <span className="flex min-w-0 items-baseline justify-between gap-3">
                      <span
                        className={cx(
                          'min-w-0 truncate text-body',
                          selected
                            ? 'font-medium text-text'
                            : row.muted
                              ? 'text-text-muted'
                              : 'text-text',
                        )}
                      >
                        {row.name}
                      </span>
                      {row.trailing != null ? (
                        <span className="shrink-0 text-body tabular-nums">
                          {row.trailing}
                        </span>
                      ) : null}
                    </span>
                    <span className="flex min-w-0 items-center justify-between gap-3 text-caption">
                      <span className="flex min-w-0 items-center gap-1.5 truncate text-text-faint">
                        {row.caption}
                      </span>
                      {row.captionTrailing != null ? (
                        <span className="shrink-0 tabular-nums">
                          {row.captionTrailing}
                        </span>
                      ) : null}
                    </span>
                  </button>
                </li>
              );
            })}
          </ul>
        )}
      </div>
    </aside>
  );
}
