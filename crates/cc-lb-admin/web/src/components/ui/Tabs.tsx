import {
  type KeyboardEvent,
  type ReactNode,
  useCallback,
  useEffect,
  useRef,
  useState,
} from 'react';
import { cx } from './primitives';

export interface TabItem<T extends string> {
  value: T;
  label: ReactNode;
  /** Trailing adornment after the label: a count or status dots. */
  trailing?: ReactNode;
  /** Extra accessible text (e.g. "2 modified"), read after the label. */
  srDescription?: string;
  disabled?: boolean;
}

/**
 * Underline tabs: 14/500 labels, 36px row, a square 2px accent rule under
 * the active item, one horizontally scrolling line with an edge fade —
 * never a grid of bordered cells and never wrapping.
 *
 * - `mode="tabs"` (default): an ARIA tablist with roving focus (arrows,
 *   Home/End). With `idPrefix`, tab `i` is `${idPrefix}-tab-${value}` and
 *   controls `${idPrefix}-panel-${value}` — see `tabPanelProps`.
 * - `mode="nav"`: navigation between views; items are plain buttons with
 *   `aria-current="page"` inside a labelled `<nav>`.
 */
export function Tabs<T extends string>({
  items,
  value,
  onChange,
  ariaLabel,
  mode = 'tabs',
  idPrefix,
  className,
  'data-testid': testId,
}: {
  items: readonly TabItem<T>[];
  value: T;
  onChange: (value: T) => void;
  ariaLabel: string;
  mode?: 'tabs' | 'nav';
  idPrefix?: string;
  className?: string;
  'data-testid'?: string;
}) {
  const scrollerRef = useRef<HTMLDivElement | null>(null);
  const itemRefs = useRef<(HTMLButtonElement | null)[]>([]);
  const [fade, setFade] = useState({ start: false, end: false });

  const updateFade = useCallback(() => {
    const el = scrollerRef.current;
    if (!el) return;
    const start = el.scrollLeft > 1;
    const end = el.scrollLeft + el.clientWidth < el.scrollWidth - 1;
    setFade((prev) =>
      prev.start === start && prev.end === end ? prev : { start, end },
    );
  }, []);

  useEffect(() => {
    const el = scrollerRef.current;
    if (!el) return;
    updateFade();
    if (typeof ResizeObserver === 'undefined') return;
    const observer = new ResizeObserver(updateFade);
    observer.observe(el);
    return () => observer.disconnect();
  }, [updateFade]);

  const activeIndex = items.findIndex((item) => item.value === value);

  // Keep the active item in view horizontally when it changes (e.g. deep links).
  // Only the tab scroller moves; the page scroll position is never touched.
  useEffect(() => {
    const el = scrollerRef.current;
    const node = itemRefs.current[activeIndex];
    if (!el || !node) return;
    const left = node.offsetLeft;
    const right = left + node.offsetWidth;
    if (left < el.scrollLeft) {
      el.scrollLeft = Math.max(0, left - 24);
    } else if (right > el.scrollLeft + el.clientWidth) {
      el.scrollLeft = right - el.clientWidth + 24;
    }
  }, [activeIndex]);

  const isTabs = mode === 'tabs';
  const tabbableIndex = activeIndex === -1 ? 0 : activeIndex;

  const handleKeyDown = (event: KeyboardEvent<HTMLButtonElement>) => {
    if (!isTabs) return;
    const enabled = items
      .map((item, index) => (item.disabled ? -1 : index))
      .filter((index) => index !== -1);
    if (enabled.length === 0) return;
    const position = enabled.indexOf(tabbableIndex);
    let next: number | undefined;
    switch (event.key) {
      case 'ArrowRight':
        next = enabled[(position + 1) % enabled.length];
        break;
      case 'ArrowLeft':
        next = enabled[(position - 1 + enabled.length) % enabled.length];
        break;
      case 'Home':
        next = enabled[0];
        break;
      case 'End':
        next = enabled[enabled.length - 1];
        break;
      default:
        return;
    }
    event.preventDefault();
    if (next === undefined) return;
    const item = items[next];
    if (!item) return;
    itemRefs.current[next]?.focus();
    if (item.value !== value) onChange(item.value);
  };

  const mask =
    fade.start || fade.end
      ? `linear-gradient(to right, ${fade.start ? 'transparent 0, black 24px' : 'black 0'}, ${fade.end ? 'black calc(100% - 24px), transparent 100%' : 'black 100%'})`
      : undefined;

  const list = (
    <div
      ref={scrollerRef}
      onScroll={updateFade}
      role={isTabs ? 'tablist' : undefined}
      aria-label={isTabs ? ariaLabel : undefined}
      aria-orientation={isTabs ? 'horizontal' : undefined}
      className="flex h-9 items-stretch gap-4 overflow-x-auto overflow-y-hidden [scrollbar-width:none] [&::-webkit-scrollbar]:hidden"
      style={mask ? { maskImage: mask, WebkitMaskImage: mask } : undefined}
    >
      {items.map((item, index) => {
        const active = index === activeIndex;
        return (
          <button
            key={item.value}
            ref={(node) => {
              itemRefs.current[index] = node;
            }}
            type="button"
            id={
              isTabs && idPrefix ? `${idPrefix}-tab-${item.value}` : undefined
            }
            role={isTabs ? 'tab' : undefined}
            aria-selected={isTabs ? active : undefined}
            aria-controls={
              isTabs && idPrefix ? `${idPrefix}-panel-${item.value}` : undefined
            }
            aria-current={!isTabs && active ? 'page' : undefined}
            tabIndex={isTabs ? (index === tabbableIndex ? 0 : -1) : undefined}
            disabled={item.disabled}
            data-value={item.value}
            onClick={() => {
              if (!active) onChange(item.value);
            }}
            onKeyDown={handleKeyDown}
            className={cx(
              'relative inline-flex shrink-0 items-center gap-1.5 whitespace-nowrap text-sm font-medium transition-colors',
              'focus-visible:outline-2 focus-visible:-outline-offset-2 focus-visible:outline-accent',
              'disabled:cursor-not-allowed disabled:opacity-40',
              "after:absolute after:inset-x-0 after:bottom-0 after:h-0.5 after:content-['']",
              active
                ? 'text-text after:bg-accent'
                : 'text-text-muted hover:text-text after:bg-transparent',
            )}
          >
            <span>{item.label}</span>
            {item.srDescription ? (
              <span className="sr-only">{`, ${item.srDescription}`}</span>
            ) : null}
            {item.trailing != null ? (
              <span className="inline-flex items-center gap-1 text-caption text-text-faint tabular-nums">
                {item.trailing}
              </span>
            ) : null}
          </button>
        );
      })}
    </div>
  );

  return isTabs ? (
    <div
      className={cx('border-b border-subtle', className)}
      data-testid={testId}
    >
      {list}
    </div>
  ) : (
    <nav
      aria-label={ariaLabel}
      className={cx('border-b border-subtle', className)}
      data-testid={testId}
    >
      {list}
    </nav>
  );
}

/** Props for the panel a `Tabs` item controls (tabs mode with `idPrefix`). */
export function tabPanelProps(idPrefix: string, value: string) {
  return {
    id: `${idPrefix}-panel-${value}`,
    role: 'tabpanel' as const,
    'aria-labelledby': `${idPrefix}-tab-${value}`,
    tabIndex: 0,
  };
}
