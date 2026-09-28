import { Check, ChevronLeft, ChevronRight, Copy, Trash2 } from 'lucide-react';
import {
  createContext,
  type HTMLAttributes,
  type ReactNode,
  useContext,
  useId,
  useState,
} from 'react';
import { useCopyButton } from '../../lib/useCopyButton';
import {
  Button,
  cx,
  IconButton,
  Skeleton,
  Spinner,
  ToggleSwitch,
} from './primitives';

/**
 * The selected-item detail pane shared by Upstreams and Principals.
 *
 * `DetailPane` is the pane's scroll container. From `md` its `header` (a
 * `DetailHeader`) stays pinned at the top and gains a 1px bottom line once
 * the body scrolls under it; on phones it scrolls away with the body. The
 * body is the `detail` size container; it stacks optional `notices` and then
 * `DetailSection`s, each opened by a full-width rule. Wrap settings-like
 * sections in a `DetailSectionGrid` so they pair up in two columns when the
 * pane is wide.
 */

const DetailScrolledContext = createContext(false);

/**
 * Horizontal inset shared by the header and the body. From `lg` the right
 * inset is PageContainer's 40px gutter, so the pane's content ends on the
 * same edge as every other page's content.
 */
const PANE_INSET_CLASS = 'px-4 md:px-8 lg:pr-10';
/**
 * Space between one section's content and the next section's rule. The body
 * stack and `DetailSectionGrid` rows share it so the rhythm never changes.
 */
const SECTION_GAP_CLASS = 'gap-y-6 md:gap-y-8';

export function DetailPane({
  header,
  notices,
  children,
  className,
  ...rest
}: {
  header: ReactNode;
  /** Incident notices and page-level actions, above the first section. */
  notices?: ReactNode;
  children: ReactNode;
} & Omit<HTMLAttributes<HTMLElement>, 'children'>) {
  const [scrolled, setScrolled] = useState(false);
  return (
    <section
      {...rest}
      data-detail-pane=""
      onScroll={(event) => setScrolled(event.currentTarget.scrollTop > 0)}
      className={cx(
        '@container flex min-h-0 min-w-0 flex-1 flex-col overflow-y-auto',
        className,
      )}
    >
      <DetailScrolledContext.Provider value={scrolled}>
        {header}
      </DetailScrolledContext.Provider>
      <div
        className={cx(
          '@container/detail flex flex-col pb-10 md:pb-16',
          SECTION_GAP_CLASS,
          PANE_INSET_CLASS,
        )}
      >
        {notices ? (
          <div data-detail-notices="" className="flex flex-col gap-2">
            {notices}
          </div>
        ) : null}
        {children}
      </div>
    </section>
  );
}

/**
 * Pane header, identical for every entity. Narrow panes (phones, and
 * split panes under 42rem): the back button (phones only) shares the first
 * row with the enabled switch and Delete, then the name (the one `h2`) with
 * its kind/plan badge gets a full-width line. Wide panes: name and badge on
 * the left, switch and Delete on the right. Then one muted meta line ending
 * in the mono ID with a copy button. Sticky from `md` only: on phones a
 * pinned header would hold a quarter of the screen.
 */
export function DetailHeader({
  backLabel,
  onBack,
  title,
  titleId,
  badge,
  status,
  meta,
  id,
  idLabel,
  enabled,
  onEnabledChange,
  enabledDisabled,
  pendingLabel,
  onDelete,
  deleteDisabled,
  deleting,
}: {
  /** `All upstreams` / `All principals`. */
  backLabel: string;
  onBack: () => void;
  title: ReactNode;
  titleId: string;
  badge?: ReactNode;
  /** At most one exception status, beside the badge. */
  status?: ReactNode;
  /** Muted facts before the ID, joined with ` · `. */
  meta?: ReactNode[];
  id: string;
  /** Noun for the copy toast, e.g. `Upstream ID`. */
  idLabel: string;
  enabled: boolean;
  onEnabledChange: (next: boolean) => void;
  enabledDisabled?: boolean;
  /** `Enabling...` / `Disabling...` while the toggle is in flight. */
  pendingLabel?: string | null;
  onDelete: () => void;
  deleteDisabled?: boolean;
  deleting?: boolean;
}) {
  const scrolled = useContext(DetailScrolledContext);
  const { copied, copy } = useCopyButton();
  const facts = (meta ?? []).filter(
    (fact) => fact != null && fact !== false && fact !== '',
  );
  return (
    <header
      data-detail-header=""
      data-scrolled={scrolled ? 'true' : undefined}
      className={cx(
        'z-20 grid shrink-0 grid-cols-[minmax(0,1fr)_auto] items-center gap-x-6 border-b bg-bg pt-2 pb-4 transition-colors md:sticky md:top-0 md:pt-8',
        scrolled ? 'border-subtle' : 'border-transparent',
        PANE_INSET_CLASS,
      )}
    >
      <button
        type="button"
        onClick={onBack}
        className="col-start-1 row-start-1 -ml-1 inline-flex min-h-11 w-fit items-center gap-1 rounded-sm px-1 text-body text-text-muted hover:text-text focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-1 md:hidden"
      >
        <ChevronLeft className="size-4" strokeWidth={1.75} /> {backLabel}
      </button>
      <div className="col-start-2 row-start-1 flex shrink-0 items-center justify-self-end gap-3">
        {pendingLabel ? (
          <span
            role="status"
            aria-live="polite"
            data-testid="detail-enabled-pending"
            className="inline-flex items-center gap-1.5 text-body-sm text-text-muted"
          >
            <Spinner className="size-3 text-text-muted" />
            {pendingLabel}
          </span>
        ) : null}
        <ToggleSwitch
          variant="compact"
          role="switch"
          aria-label="Enabled"
          label={
            <span
              className={
                enabledDisabled ? 'text-text-faint' : 'text-text-muted'
              }
            >
              {enabled ? 'Enabled' : 'Disabled'}
            </span>
          }
          checked={enabled}
          disabled={enabledDisabled}
          onChange={(event) => onEnabledChange(event.target.checked)}
          className="flex-row-reverse"
        />
        <Button
          size="sm"
          variant="danger"
          iconLeft={<Trash2 />}
          loading={deleting}
          disabled={deleteDisabled}
          onClick={onDelete}
        >
          Delete
        </Button>
      </div>
      <div className="col-span-2 row-start-2 mt-2 flex min-w-0 flex-wrap items-center gap-x-3 gap-y-1 @2xl:col-span-1 @2xl:col-start-1 @2xl:row-start-1 @2xl:mt-0">
        <h2
          id={titleId}
          className="min-w-0 break-words text-title-page text-text"
        >
          {title}
        </h2>
        {badge}
        {status}
      </div>
      <p className="col-span-2 mt-1 flex min-h-5 min-w-0 flex-wrap items-center gap-x-1.5 text-body-sm text-text-muted">
        {facts.map((fact, index) => (
          <span key={index} className="inline-flex min-w-0 items-center">
            {fact}
            <span aria-hidden="true" className="ml-1.5 text-text-faint">
              ·
            </span>
          </span>
        ))}
        <span className="inline-flex min-w-0 items-center gap-0.5">
          <span className="break-all font-mono text-data">{id}</span>
          <IconButton
            label={`Copy ${idLabel.toLowerCase()}`}
            className="-my-3 md:-my-1.5 md:h-7 md:w-7 [&_svg]:size-3.5"
            onClick={() => void copy(id, idLabel)}
          >
            {copied ? (
              <Check strokeWidth={1.75} />
            ) : (
              <Copy strokeWidth={1.75} />
            )}
          </IconButton>
        </span>
      </p>
    </header>
  );
}

/** Loading placeholder with the `DetailHeader` geometry. */
export function DetailHeaderSkeleton() {
  return (
    <header
      data-detail-header=""
      className={cx(
        'z-20 shrink-0 border-b border-transparent bg-bg pt-2 pb-4 md:sticky md:top-0 md:pt-8',
        PANE_INSET_CLASS,
      )}
    >
      <div className="flex min-h-11 items-center justify-between gap-3 @2xl:hidden">
        <Skeleton className="h-4 w-28 md:invisible" />
        <div className="flex items-center gap-3">
          <Skeleton className="h-5 w-24" />
          <Skeleton className="h-7 w-20" />
        </div>
      </div>
      <div className="mt-2 flex flex-wrap items-center justify-between gap-x-6 gap-y-2 @2xl:mt-0">
        <div className="flex min-h-8 items-center gap-3">
          <Skeleton className="h-7 w-48" />
          <Skeleton className="h-5 w-20" />
        </div>
        <div className="hidden min-h-11 items-center gap-3 @2xl:flex">
          <Skeleton className="h-5 w-24" />
          <Skeleton className="h-7 w-20" />
        </div>
      </div>
      <div className="mt-1 flex min-h-5 items-center">
        <Skeleton className="h-4 w-72 max-w-full" />
      </div>
    </header>
  );
}

/**
 * Lays `DetailSection`s out in reading order: one column, then two equal
 * columns once the pane body (`@container/detail`) is 56rem wide. Sections in
 * one row share the row's top, so their rules line up; `span="full"` sections
 * take the whole row.
 */
export function DetailSectionGrid({ children }: { children: ReactNode }) {
  return (
    <div
      data-detail-section-grid=""
      className={cx(
        'grid min-w-0 grid-cols-1 items-start @[56rem]/detail:grid-cols-2 @[56rem]/detail:gap-x-12',
        SECTION_GAP_CLASS,
      )}
    >
      {children}
    </div>
  );
}

/**
 * One titled region of a detail pane: a full-width 1px rule, then a
 * `text-title-section` h3 with a one-line muted description and an optional
 * action, then the content. `collapsible` sections are a disclosure, closed
 * unless `defaultOpen`. Each section is its own size container, so rows and
 * fact grids inside respond to the section's width, not the pane's.
 */
export function DetailSection({
  title,
  description,
  action,
  children,
  className,
  titleId,
  span,
  collapsible = false,
  defaultOpen = false,
  ...rest
}: {
  title: ReactNode;
  description?: ReactNode;
  action?: ReactNode;
  children: ReactNode;
  className?: string;
  titleId?: string;
  /** In a `DetailSectionGrid`, span both columns. */
  span?: 'full';
  collapsible?: boolean;
  defaultOpen?: boolean;
} & Omit<HTMLAttributes<HTMLElement>, 'title' | 'children'>) {
  const generatedId = useId();
  const headingId = titleId ?? generatedId;
  const sectionClass = cx(
    '@container flex min-w-0 flex-col border-t border-subtle pt-5 md:pt-6',
    span === 'full' && '@[56rem]/detail:col-span-2',
    className,
  );
  const heading = (
    <div className="min-w-0">
      <h3 id={headingId} className="text-title-section text-text">
        {title}
      </h3>
      {description ? (
        <div
          className="mt-0.5 min-h-5 text-body-sm text-text-muted"
          data-slot="section-subtitle"
        >
          {description}
        </div>
      ) : null}
    </div>
  );

  if (collapsible) {
    return (
      <section
        aria-labelledby={headingId}
        data-detail-section=""
        className={sectionClass}
        {...rest}
      >
        <details open={defaultOpen || undefined} className="group/detail">
          <summary className="group/summary -mx-1 flex cursor-pointer list-none items-start justify-between gap-4 rounded-sm px-1 focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-1 [&::-webkit-details-marker]:hidden">
            {heading}
            <span
              aria-hidden="true"
              className="inline-flex min-h-7 shrink-0 items-center gap-1 text-body-sm text-text-muted transition-colors group-hover/summary:text-text"
            >
              <span className="group-open/detail:hidden">Show</span>
              <span className="hidden group-open/detail:inline">Hide</span>
              <ChevronRight
                aria-hidden="true"
                strokeWidth={1.75}
                className="size-3 shrink-0 transition-transform duration-150 group-open/detail:rotate-90 motion-reduce:transition-none"
              />
            </span>
          </summary>
          <div className="mt-3 flex flex-col gap-3">
            {action ? <div className="flex justify-end">{action}</div> : null}
            {children}
          </div>
        </details>
      </section>
    );
  }

  return (
    <section
      aria-labelledby={headingId}
      data-detail-section=""
      className={cx(sectionClass, 'gap-3')}
      {...rest}
    >
      {/* The action shares the heading's line while both fit the section
          and wraps under it otherwise, so a half-width section never
          overflows. */}
      <header className="flex flex-wrap items-start justify-between gap-x-4 gap-y-3">
        {heading}
        {action ? (
          <div className="flex min-w-0 flex-wrap items-center gap-2">
            {action}
          </div>
        ) : null}
      </header>
      {children}
    </section>
  );
}

/** Label / value / action rows with `border-row` dividers. */
export function DetailRows({
  children,
  className,
  ...rest
}: HTMLAttributes<HTMLDivElement>) {
  return (
    <div
      className={cx(
        'flex flex-col divide-y divide-border-row border-y border-border-row',
        className,
      )}
      {...rest}
    >
      {children}
    </div>
  );
}

export function DetailRow({
  label,
  description,
  children,
  action,
  className,
  ...rest
}: {
  label: ReactNode;
  description?: ReactNode;
  children?: ReactNode;
  action?: ReactNode;
  className?: string;
} & Omit<HTMLAttributes<HTMLDivElement>, 'children'>) {
  return (
    <div
      className={cx(
        'grid min-w-0 gap-x-6 gap-y-1.5 py-3 @lg:grid-cols-[minmax(0,12rem)_minmax(0,1fr)_auto] @lg:items-baseline',
        className,
      )}
      {...rest}
    >
      <div className="min-w-0">
        <div className="text-label text-text-muted">{label}</div>
        {description ? (
          <div className="mt-0.5 text-caption text-text-faint">
            {description}
          </div>
        ) : null}
      </div>
      <div className="min-w-0 break-words text-body text-text">{children}</div>
      {action ? (
        <div className="flex items-center gap-2 @lg:justify-end">{action}</div>
      ) : null}
    </div>
  );
}

/** A 2–4 column grid of small related facts. */
export function DetailFacts({
  children,
  className,
  ...rest
}: HTMLAttributes<HTMLDListElement>) {
  return (
    <dl
      className={cx(
        'grid grid-cols-2 gap-x-6 gap-y-4 @2xl:grid-cols-3 @4xl:grid-cols-4',
        className,
      )}
      {...rest}
    >
      {children}
    </dl>
  );
}

export function DetailFact({
  label,
  children,
  hint,
}: {
  label: ReactNode;
  children: ReactNode;
  /** Faint caption under the value. */
  hint?: ReactNode;
}) {
  return (
    <div className="flex min-w-0 flex-col gap-0.5">
      <dt className="text-label text-text-muted">{label}</dt>
      <dd className="min-w-0 break-words text-body text-text">
        {children}
        {hint ? (
          <span className="mt-0.5 block text-caption text-text-faint">
            {hint}
          </span>
        ) : null}
      </dd>
    </div>
  );
}
