import { AlertDialog as BaseAlertDialog } from '@base-ui/react/alert-dialog';
import { Button as BaseButton } from '@base-ui/react/button';
import { Dialog as BaseDialog } from '@base-ui/react/dialog';
import { Popover as BasePopover } from '@base-ui/react/popover';
import { CircleAlert, CircleCheck, Info, TriangleAlert, X } from 'lucide-react';
import {
  type ButtonHTMLAttributes,
  cloneElement,
  createContext,
  type FocusEventHandler,
  type HTMLAttributes,
  type InputHTMLAttributes,
  isValidElement,
  type KeyboardEvent,
  type MouseEventHandler,
  type PointerEventHandler,
  type ReactNode,
  useContext,
  useEffect,
  useId,
  useRef,
  useState,
} from 'react';

// ─── classnames helper ───────────────────────────────────────────────────────
export function cx(...parts: Array<string | false | null | undefined>): string {
  return parts.filter(Boolean).join(' ');
}

// ─── Card ────────────────────────────────────────────────────────────────────
/** Self-contained object: flat panel on the ground, 1px line, 6px radius. */
export function Card({ className, ...rest }: HTMLAttributes<HTMLDivElement>) {
  return <div className={cx('glass rounded-md', className)} {...rest} />;
}
export function CardHeader({
  title,
  titleId,
  action,
  subtitle,
  className,
  align = 'start',
  headingLevel = 2,
}: {
  readonly title: ReactNode;
  readonly titleId?: string;
  readonly subtitle?: ReactNode;
  readonly action?: ReactNode;
  readonly className?: string;
  readonly align?: 'start' | 'center';
  /** Card titles sit under the page `h1`; use 3/4 for cards nested in a titled section. */
  readonly headingLevel?: 2 | 3 | 4;
}) {
  const Heading = `h${headingLevel}` as const;
  return (
    <div
      className={cx(
        'flex flex-col sm:flex-row sm:justify-between gap-3 sm:gap-4 px-4 py-3 border-b border-subtle',
        align === 'center' ? 'sm:items-center' : 'sm:items-start',
        className,
      )}
    >
      <div className="min-w-0">
        <Heading id={titleId} className="text-title-card text-text">
          {title}
        </Heading>
        {subtitle ? (
          <div
            className="mt-0.5 min-h-4 text-caption text-text-faint"
            data-slot="card-subtitle"
          >
            {subtitle}
          </div>
        ) : null}
      </div>
      {action ? (
        <div className="flex flex-wrap items-center gap-2 min-w-0 w-full sm:w-auto">
          {action}
        </div>
      ) : null}
    </div>
  );
}
export function CardBody({
  className,
  ...rest
}: HTMLAttributes<HTMLDivElement>) {
  return <div className={cx('p-4', className)} {...rest} />;
}

// ─── Button ──────────────────────────────────────────────────────────────────
/**
 * - `primary`: one per view, the commit action (Save, Create, Continue):
 *   solid accent with accent-ink text.
 * - `secondary`: the default; 1px outline on transparent. Toolbars, pairs.
 * - `ghost`: tertiary, in-row links, icon-adjacent actions.
 * - `danger`: outline; page-level Delete / Revoke.
 * - `danger-solid`: only the confirm button of a destructive `ConfirmDialog`.
 */
export type ButtonVariant =
  | 'primary'
  | 'secondary'
  | 'ghost'
  | 'danger'
  | 'danger-solid';
export type ButtonSize = 'sm' | 'md' | 'lg';
interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: ButtonVariant;
  size?: ButtonSize;
  fullWidth?: boolean;
  iconLeft?: ReactNode;
  iconRight?: ReactNode;
  loading?: boolean;
}
const BTN_VARIANTS: Record<ButtonVariant, string> = {
  primary:
    'bg-accent border border-accent text-accent-ink hover:brightness-110',
  secondary:
    'border border-subtle-strong text-text hover:bg-panel-strong disabled:hover:bg-transparent',
  ghost: 'text-text-muted hover:bg-hover-bg hover:text-text',
  danger:
    'border border-danger text-danger-text hover:bg-danger/10 disabled:hover:bg-transparent',
  'danger-solid':
    'bg-danger-solid border border-danger-solid text-white hover:bg-danger-solid-hover',
};
const BTN_SIZES: Record<ButtonSize, string> = {
  sm: 'h-7 px-2.5 text-xs gap-1.5 [&_svg]:size-3.5',
  md: 'h-8 px-3 text-[0.8125rem] gap-1.5 [&_svg]:size-3.5',
  lg: 'h-9 px-3.5 text-sm gap-2 [&_svg]:size-4',
};
/**
 * The Button chrome as a class string, for elements that must stay a real
 * anchor (never popup-blocked, keeps link semantics) yet look like a Button.
 */
export function buttonClassName(
  variant: ButtonVariant = 'secondary',
  size: ButtonSize = 'md',
): string {
  return cx(
    'inline-flex items-center justify-center rounded-sm font-medium whitespace-nowrap transition-colors select-none [&_svg]:shrink-0',
    'disabled:cursor-not-allowed disabled:opacity-40',
    'focus-visible:outline focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-2',
    BTN_VARIANTS[variant],
    BTN_SIZES[size],
  );
}
export function Button({
  variant = 'secondary',
  size = 'md',
  fullWidth,
  iconLeft,
  iconRight,
  loading = false,
  className,
  children,
  type,
  disabled,
  'aria-busy': ariaBusy,
  ...rest
}: ButtonProps) {
  // A disabled primary is not the commit action yet: it renders as a
  // disabled secondary so a grey slab never reads as the primary.
  const look =
    variant === 'primary' && disabled && !loading ? 'secondary' : variant;
  // `disabled` and `aria-busy` are applied after the prop spread so a loading
  // Button can never be re-enabled or stripped of its busy state by callers.
  return (
    <BaseButton
      type={type ?? 'button'}
      className={cx(
        buttonClassName(look, size),
        fullWidth ? 'w-full' : '',
        className,
      )}
      {...rest}
      disabled={loading || disabled}
      aria-busy={loading ? true : ariaBusy}
    >
      {loading ? <Spinner className="w-3 h-3" /> : iconLeft}
      {children}
      {iconRight}
    </BaseButton>
  );
}

/**
 * Icon-only button. `label` is required and becomes the accessible name.
 * Below `md` the hit area is 44×44 for touch; from `md` up it is 32×32. The
 * glyph is 16px regardless of the icon's own size classes.
 */
export function IconButton({
  className,
  children,
  label,
  type,
  ...rest
}: Omit<ButtonProps, 'loading'> & { label: string }) {
  return (
    <BaseButton
      type={type ?? 'button'}
      aria-label={label}
      className={cx(
        'inline-flex shrink-0 items-center justify-center rounded-sm text-text-muted transition-colors [&_svg]:size-4 [&_svg]:shrink-0',
        'h-11 w-11 md:h-8 md:w-8 hover:bg-overlay-5 hover:text-text',
        'disabled:cursor-not-allowed disabled:opacity-40 disabled:hover:bg-transparent disabled:hover:text-text-muted',
        'focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-1',
        className,
      )}
      {...rest}
    >
      {children}
    </BaseButton>
  );
}

// ─── Badge ───────────────────────────────────────────────────────────────────
/**
 * 20px sentence-case tag. `accent` is an outlined chip (the mockup's
 * "binds pool"); tones are tone/12 fills. `mono` is for machine strings
 * (model IDs, key IDs) only. At most one status badge per object header;
 * healthy states are shown by omission.
 */
export type BadgeTone =
  | 'neutral'
  | 'accent'
  | 'ok'
  | 'warn'
  | 'danger'
  | 'mono';
const BADGE_TONES: Record<BadgeTone, string> = {
  neutral: 'bg-overlay-5 text-text-muted text-caption font-medium',
  accent: 'border border-accent text-accent-text text-caption font-medium',
  ok: 'bg-overlay-5 text-success-text text-caption font-medium',
  warn: 'bg-warn/12 text-warn-text text-caption font-medium',
  danger: 'bg-danger/12 text-danger-text text-caption font-medium',
  mono: 'bg-overlay-4 text-text font-mono text-data',
};
export function Badge({
  tone = 'neutral',
  children,
  className,
}: {
  tone?: BadgeTone;
  children: ReactNode;
  className?: string;
}) {
  return (
    <span
      className={cx(
        'inline-flex min-h-5 items-center gap-1 rounded-sm px-1.5 tabular-nums',
        BADGE_TONES[tone],
        className,
      )}
    >
      {children}
    </span>
  );
}

// ─── StatusBadge (dot + label) ───────────────────────────────────────────────
const STATUS_TEXT: Record<
  'ok' | 'warn' | 'danger' | 'neutral' | 'live',
  string
> = {
  ok: 'text-text-muted',
  neutral: 'text-text-muted',
  live: 'text-text-muted',
  warn: 'text-warn-text',
  danger: 'text-danger-text',
};
/** Dot + sentence-case phrase. Exceptions carry their tone; healthy stays muted. */
export function StatusBadge({
  tone,
  label,
}: {
  tone: 'ok' | 'warn' | 'danger' | 'neutral' | 'live';
  label: ReactNode;
}) {
  return (
    <span
      className={cx(
        'inline-flex h-5 shrink-0 items-center gap-1.5 whitespace-nowrap text-label',
        STATUS_TEXT[tone],
      )}
    >
      <span className={cx('status-dot', tone)} />
      <span>{label}</span>
    </span>
  );
}

// ─── Skeleton ────────────────────────────────────────────────────────────────
export function Skeleton({
  as: Component = 'div',
  className,
  style,
}: {
  as?: 'div' | 'span';
  className?: string;
  style?: React.CSSProperties;
}) {
  const classNames = className?.split(/\s+/) ?? [];
  const hasHeight = classNames.some(
    (name) => name.startsWith('h-') || name.startsWith('size-'),
  );
  const hasWidth = classNames.some(
    (name) => name.startsWith('w-') || name.startsWith('size-'),
  );

  return (
    <Component
      aria-hidden="true"
      className={cx(
        'skeleton',
        !hasHeight && 'h-4',
        !hasWidth && 'w-full',
        className,
      )}
      style={style}
    />
  );
}
export function SkeletonRow({
  cols = 5,
  className,
  style,
  cellClassNames,
  skeletonClassNames,
}: {
  cols?: number;
  className?: string;
  style?: React.CSSProperties;
  cellClassNames?: readonly (string | undefined)[];
  skeletonClassNames?: readonly (string | undefined)[];
}) {
  return (
    <tr
      className={cx('border-b border-row', className)}
      style={style}
      aria-hidden="true"
    >
      {Array.from({ length: cols }).map((_, i) => (
        <td key={i} className={cx('px-3 py-2', cellClassNames?.[i])}>
          <Skeleton className={skeletonClassNames?.[i]} />
        </td>
      ))}
    </tr>
  );
}

// ─── EmptyState ──────────────────────────────────────────────────────────────
/**
 * Quiet, centred empty state with no frame of its own: it sits inside the
 * card or section that owns the data, and that container shrinks to it.
 * In-card empties usually need only `title`.
 */
export function EmptyState({
  icon,
  title,
  description,
  action,
  headingLevel = 3,
}: {
  icon?: ReactNode;
  title: ReactNode;
  description?: ReactNode;
  action?: ReactNode;
  /**
   * Outline level of the title. Default 3 suits a state inside a titled
   * section or card; use 2 directly under a page's h1.
   */
  headingLevel?: 2 | 3 | 4;
}) {
  const Heading = `h${headingLevel}` as const;
  return (
    <div className="flex flex-col items-center justify-center text-center py-8 px-4">
      {icon ? (
        <div className="mb-2 text-text-faint [&_svg]:size-5">{icon}</div>
      ) : null}
      <Heading
        className={cx(
          description || action
            ? 'text-title-card text-text'
            : 'text-body-sm text-text-muted',
        )}
      >
        {title}
      </Heading>
      {description ? (
        <p className="mt-1 text-body-sm text-text-muted max-w-md">
          {description}
        </p>
      ) : null}
      {action ? <div className="mt-4">{action}</div> : null}
    </div>
  );
}

// ─── Modal (Radix Dialog) ────────────────────────────────────────────────────
export function Modal({
  open,
  onOpenChange,
  title,
  description,
  children,
  footer,
  size = 'md',
  preventDismiss = false,
}: {
  open: boolean;
  onOpenChange: (
    open: boolean,
    eventDetails: BaseDialog.Root.ChangeEventDetails,
  ) => void;
  title: ReactNode;
  description?: ReactNode;
  children: ReactNode;
  footer?: ReactNode;
  size?: 'sm' | 'md' | 'lg';
  preventDismiss?: boolean;
}) {
  const sizeClass =
    size === 'sm' ? 'max-w-sm' : size === 'lg' ? 'max-w-2xl' : 'max-w-md';
  return (
    <BaseDialog.Root
      open={open}
      onOpenChange={(nextOpen, eventDetails) => {
        // Swallow close requests while dismissal is locked: backdrop clicks,
        // Escape and the X control must not interrupt in-flight work.
        if (preventDismiss && !nextOpen) return;
        onOpenChange(nextOpen, eventDetails);
      }}
    >
      <BaseDialog.Portal>
        <BaseDialog.Backdrop className="fixed inset-0 z-50 bg-modal-backdrop transition-opacity duration-150 ease-out data-[ending-style]:opacity-0 data-[starting-style]:opacity-0" />
        <BaseDialog.Popup
          className={cx(
            'fixed top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 z-50 w-[calc(100%-2rem)] outline-none',
            'bg-bg-sub border border-subtle-strong rounded-md shadow-overlay flex flex-col max-h-[85vh]',
            'transition-[opacity,scale] duration-150 ease-out data-[ending-style]:opacity-0 data-[ending-style]:scale-95 data-[starting-style]:opacity-0 data-[starting-style]:scale-95',
            sizeClass,
          )}
        >
          <div className="flex items-start justify-between gap-3 px-4 py-3 border-b border-subtle">
            <div className="min-w-0">
              <BaseDialog.Title className="text-title-section text-text">
                {title}
              </BaseDialog.Title>
              {description ? (
                <BaseDialog.Description className="mt-0.5 text-body-sm text-text-muted">
                  {description}
                </BaseDialog.Description>
              ) : null}
            </div>
            <BaseDialog.Close
              aria-label="Close dialog"
              disabled={preventDismiss}
              aria-disabled={preventDismiss || undefined}
              className={cx(
                'inline-flex shrink-0 items-center justify-center text-text-muted hover:text-text rounded-sm',
                'h-11 w-11 -mr-2 md:mr-0 md:h-8 md:w-8 hover:bg-overlay-5',
                'focus-visible:outline focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-1',
                'disabled:cursor-not-allowed disabled:opacity-40 disabled:hover:bg-transparent disabled:hover:text-text-muted',
              )}
            >
              <X className="w-4 h-4" />
            </BaseDialog.Close>
          </div>
          <div className="flex-1 p-4 overflow-y-auto">{children}</div>
          {footer ? (
            <div className="px-4 py-3 border-t border-subtle flex flex-wrap items-center justify-end gap-2">
              {footer}
            </div>
          ) : null}
        </BaseDialog.Popup>
      </BaseDialog.Portal>
    </BaseDialog.Root>
  );
}

export function ConfirmDialog({
  open,
  onOpenChange,
  title,
  description,
  confirmLabel = 'Confirm',
  cancelLabel = 'Cancel',
  destructive = false,
  onConfirm,
  confirmDisabled = false,
  pending = false,
  closeOnConfirm = true,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  title: ReactNode;
  description?: ReactNode;
  confirmLabel?: string;
  cancelLabel?: string;
  destructive?: boolean;
  onConfirm: () => void;
  confirmDisabled?: boolean;
  pending?: boolean;
  closeOnConfirm?: boolean;
}) {
  // While the confirmed action is in flight the dialog owns the interaction:
  // no backdrop click, Escape or Cancel may tear it down mid-request.
  const requestClose = () => {
    if (pending) return;
    onOpenChange(false);
  };
  return (
    <BaseAlertDialog.Root
      open={open}
      onOpenChange={(nextOpen: boolean) => {
        if (pending && !nextOpen) return;
        onOpenChange(nextOpen);
      }}
    >
      <BaseAlertDialog.Portal>
        <BaseAlertDialog.Backdrop className="fixed inset-0 z-50 bg-modal-backdrop transition-opacity duration-150 ease-out data-[ending-style]:opacity-0 data-[starting-style]:opacity-0" />
        <BaseAlertDialog.Popup
          className={cx(
            'fixed top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 z-50 w-[calc(100%-2rem)] outline-none',
            'bg-bg-sub border border-subtle-strong rounded-md shadow-overlay flex flex-col max-h-[85vh] max-w-sm',
            'transition-[opacity,scale] duration-150 ease-out data-[ending-style]:opacity-0 data-[ending-style]:scale-95 data-[starting-style]:opacity-0 data-[starting-style]:scale-95',
          )}
        >
          <div className="px-4 pt-4">
            <BaseAlertDialog.Title className="text-title-card text-text">
              {title}
            </BaseAlertDialog.Title>
          </div>
          <div className="flex-1 px-4 pt-2 pb-4 overflow-y-auto">
            {description ? (
              <BaseAlertDialog.Description className="text-body text-text-muted">
                {description}
              </BaseAlertDialog.Description>
            ) : null}
          </div>
          <div className="px-4 py-3 border-t border-subtle flex flex-wrap items-center justify-end gap-2">
            {/* A destructive confirm must not be one stray Enter away: focus
                lands on Cancel, and on the confirm button otherwise. */}
            <Button
              autoFocus={destructive}
              variant="secondary"
              disabled={pending}
              onClick={requestClose}
            >
              {cancelLabel}
            </Button>
            <Button
              autoFocus={!destructive}
              disabled={confirmDisabled}
              loading={pending}
              onClick={() => {
                onConfirm();
                if (closeOnConfirm) onOpenChange(false);
              }}
              variant={destructive ? 'danger-solid' : 'primary'}
            >
              {confirmLabel}
            </Button>
          </div>
        </BaseAlertDialog.Popup>
      </BaseAlertDialog.Portal>
    </BaseAlertDialog.Root>
  );
}

type HintChildProps = {
  onPointerEnter?: PointerEventHandler;
  onPointerLeave?: PointerEventHandler;
  onFocus?: FocusEventHandler;
  onClick?: MouseEventHandler;
};

export function Hint({
  label,
  children,
  side = 'top',
  stopClickPropagation = true,
  openOnFocus = true,
}: {
  label: ReactNode;
  children: ReactNode;
  side?: 'top' | 'right' | 'bottom' | 'left';
  stopClickPropagation?: boolean;
  /** When false, keyboard focus does not open the hint; click/Enter/Space does. */
  openOnFocus?: boolean;
}) {
  const [engaged, setEngaged] = useState(false);
  const [open, setOpen] = useState(false);
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    return () => {
      if (timerRef.current) clearTimeout(timerRef.current);
    };
  }, []);
  if (!label) return children;

  if (!engaged) {
    if (isValidElement<HintChildProps>(children)) {
      const childElement = children;
      return cloneElement(childElement, {
        onPointerEnter: (e) => {
          childElement.props.onPointerEnter?.(e);
          timerRef.current = setTimeout(() => {
            setEngaged(true);
            setOpen(true);
          }, 200);
        },
        onPointerLeave: (e) => {
          childElement.props.onPointerLeave?.(e);
          if (timerRef.current) clearTimeout(timerRef.current);
        },
        onFocus: (e) => {
          childElement.props.onFocus?.(e);
          if (!openOnFocus) return;
          setEngaged(true);
          setOpen(true);
        },
        onClick: (e) => {
          if (stopClickPropagation) e.stopPropagation();
          childElement.props.onClick?.(e);
          if (!openOnFocus) {
            setEngaged(true);
            setOpen(true);
          }
        },
      });
    }
    return (
      <span
        onPointerEnter={() => {
          timerRef.current = setTimeout(() => {
            setEngaged(true);
            setOpen(true);
          }, 200);
        }}
        onPointerLeave={() => {
          if (timerRef.current) clearTimeout(timerRef.current);
        }}
        onFocus={() => {
          if (!openOnFocus) return;
          setEngaged(true);
          setOpen(true);
        }}
        onClick={(e) => {
          if (stopClickPropagation) e.stopPropagation();
          if (!openOnFocus) {
            setEngaged(true);
            setOpen(true);
          }
        }}
      >
        {children}
      </span>
    );
  }

  return (
    <BasePopover.Root open={open} onOpenChange={setOpen}>
      <BasePopover.Trigger
        delay={200}
        onClick={(event: React.MouseEvent) => {
          if (stopClickPropagation) event.stopPropagation();
        }}
        openOnHover
        render={isValidElement(children) ? children : <span />}
      >
        {!isValidElement(children) ? children : null}
      </BasePopover.Trigger>
      <BasePopover.Portal>
        <BasePopover.Positioner className="z-50" side={side} sideOffset={4}>
          <BasePopover.Popup className="max-w-xs px-2 py-1 text-caption rounded-sm bg-bg-sub border border-subtle-strong text-text shadow-overlay">
            {label}
          </BasePopover.Popup>
        </BasePopover.Positioner>
      </BasePopover.Portal>
    </BasePopover.Root>
  );
}

// ─── PageHeader (the page's single h1) ───────────────────────────────────────
/**
 * The shell's top bar shows the current page's name. `AppShell` publishes
 * it here so a `PageHeader` whose string title matches keeps its h1 for
 * assistive tech but does not print the same words twice.
 */
const ShellPageTitleContext = createContext<string | null>(null);
export const ShellPageTitleProvider = ShellPageTitleContext.Provider;

/**
 * `text-title-page` h1 with an optional one-line description and actions.
 * When the title equals the top bar's page name the h1 is visually hidden
 * and only the description and actions show.
 */
export function PageHeader({
  title,
  description,
  actions,
}: {
  title: ReactNode;
  description?: ReactNode;
  actions?: ReactNode;
}) {
  const shellTitle = useContext(ShellPageTitleContext);
  const titleInShell = typeof title === 'string' && title === shellTitle;
  if (titleInShell && !description && !actions) {
    return (
      <header className="sr-only">
        <h1>{title}</h1>
      </header>
    );
  }
  return (
    <header className="mb-8 flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
      <div className="min-w-0">
        <h1 className={titleInShell ? 'sr-only' : 'text-title-page text-text'}>
          {title}
        </h1>
        {description ? (
          <div
            className={cx(
              'max-w-[70ch] text-body text-text-muted',
              titleInShell ? undefined : 'mt-1',
            )}
          >
            {description}
          </div>
        ) : null}
      </div>
      {actions ? (
        <div className="flex flex-wrap items-center gap-2">{actions}</div>
      ) : null}
    </header>
  );
}

// ─── Section (used in pages) ─────────────────────────────────────────────────
/**
 * A flat, titled page region: sentence-case `text-title-section` h2 on the
 * ground, 16px to its content, no box. Sections are separated by space,
 * not rules. The page title comes from `PageHeader`.
 */
export function Section({
  title,
  subtitle,
  action,
  children,
  className,
}: {
  title?: ReactNode;
  subtitle?: ReactNode;
  action?: ReactNode;
  children: ReactNode;
  className?: string;
}) {
  return (
    <section className={cx('flex flex-col gap-4', className)}>
      {title || action ? (
        <header className="flex flex-wrap items-baseline justify-between gap-x-4 gap-y-2">
          <div className="min-w-0 flex-1">
            {title ? (
              <h2 className="text-title-section text-text">{title}</h2>
            ) : null}
            {subtitle ? (
              <div
                className="mt-0.5 min-h-4 text-body-sm text-text-muted"
                data-slot="section-subtitle"
              >
                {subtitle}
              </div>
            ) : null}
          </div>
          {action ? <div className="flex-shrink-0">{action}</div> : null}
        </header>
      ) : null}
      {children}
    </section>
  );
}

// ─── PageContainer ───────────────────────────────────────────────────────────
export function PageContainer({
  children,
  className,
}: {
  children: ReactNode;
  className?: string;
}) {
  return (
    <div
      className={cx(
        'px-4 pt-5 pb-10 md:px-8 md:pt-8 lg:px-10 lg:pt-10 lg:pb-16 w-full max-w-[90rem] mx-auto space-y-12 lg:space-y-16',
        className,
      )}
    >
      {children}
    </div>
  );
}

export function FullPage({
  children,
  className,
}: {
  children: ReactNode;
  className?: string;
}) {
  return (
    <div
      className={cx(
        // Viewport-fit console with an inner scroller from `md` (`h-shell`:
        // viewport minus top bar and, below `lg`, the tab bar); below `md`
        // the page scrolls normally so open filters never squeeze the rows away.
        'flex flex-col md:h-shell min-h-0 px-4 md:px-8 pt-4 md:pt-6 pb-4 w-full max-w-[120rem] mx-auto',
        className,
      )}
    >
      {children}
    </div>
  );
}

// ─── Spinner ─────────────────────────────────────────────────────────────────
export function Spinner({ className }: { className?: string }) {
  return (
    <svg
      className={cx('animate-spin', className ?? 'w-4 h-4 text-text-muted')}
      viewBox="0 0 24 24"
      fill="none"
    >
      <circle
        cx="12"
        cy="12"
        r="10"
        stroke="currentColor"
        strokeOpacity="0.2"
        strokeWidth="2"
      />
      <path
        d="M4 12a8 8 0 018-8"
        stroke="currentColor"
        strokeWidth="2"
        strokeLinecap="round"
      />
    </svg>
  );
}

// ─── Field (label + control container) ───────────────────────────────────────
const NATIVE_FORM_CONTROLS: Record<string, true> = {
  input: true,
  select: true,
  textarea: true,
};
type NativeControlProps = {
  required?: boolean;
  'aria-required'?: boolean;
  'aria-invalid'?: boolean;
  'aria-describedby'?: string;
};

/**
 * Label + control. When the only child is a native `input`/`select`/`textarea`
 * it receives `required`/`aria-required`, `aria-invalid`, and
 * `aria-describedby` pointing at the hint or error, which render outside the
 * `<label>` so they describe the control instead of joining its name. Custom
 * controls wire `errorId`/`hintId` themselves.
 */
export function Field({
  label,
  hint,
  error,
  errorId,
  hintId,
  children,
  required,
}: {
  label: string;
  hint?: string;
  error?: string;
  errorId?: string;
  hintId?: string;
  children: ReactNode;
  required?: boolean;
}) {
  const generatedId = useId();
  const resolvedErrorId = errorId ?? `${generatedId}-error`;
  const resolvedHintId = hintId ?? `${generatedId}-hint`;
  const describedBy = error ? resolvedErrorId : hint ? resolvedHintId : null;

  let control = children;
  if (
    isValidElement<NativeControlProps>(children) &&
    typeof children.type === 'string' &&
    NATIVE_FORM_CONTROLS[children.type]
  ) {
    const only = children;
    const own = only.props['aria-describedby'];
    const merged = [own, describedBy].filter(Boolean).join(' ');
    control = cloneElement(only, {
      ...(required ? { required: true, 'aria-required': true } : {}),
      ...(error ? { 'aria-invalid': true } : {}),
      ...(merged ? { 'aria-describedby': merged } : {}),
    });
  }

  return (
    <div className="flex flex-col gap-1">
      <label className="flex flex-col gap-1.5">
        <span className="text-label text-text-muted">
          {label}
          {required ? (
            <span aria-hidden="true" className="text-danger-text ml-1">
              *
            </span>
          ) : null}
        </span>
        {control}
      </label>
      {hint && !error ? (
        <span id={resolvedHintId} className="text-caption text-text-faint">
          {hint}
        </span>
      ) : null}
      {error ? (
        <span id={resolvedErrorId} className="text-caption text-danger-text">
          {error}
        </span>
      ) : null}
    </div>
  );
}

// ─── ToggleSwitch ────────────────────────────────────────────────────────────
type ToggleSwitchName =
  | { label: ReactNode; 'aria-label'?: string }
  | { label?: undefined; 'aria-label': string };

/**
 * 36×20 accent switch over a native checkbox. Needs an accessible name: a
 * visible `label`, or `aria-label` when the surrounding row already shows one.
 */
export function ToggleSwitch({
  label,
  description,
  className,
  variant = 'card',
  ...rest
}: Omit<InputHTMLAttributes<HTMLInputElement>, 'type' | 'aria-label'> &
  ToggleSwitchName & {
    description?: ReactNode;
    /**
     * 'card' renders the toggle as an in-card well (fill, no border) for
     * standalone settings rows. 'compact' drops the well so the switch can
     * sit inline inside a denser header row.
     */
    variant?: 'card' | 'compact';
  }) {
  const compact = variant === 'compact';
  const hasText = label != null || description != null;
  return (
    <label
      className={cx(
        compact
          ? 'flex min-h-[44px] min-w-0 cursor-pointer items-center gap-2'
          : 'well flex min-w-0 cursor-pointer items-start justify-between gap-4 px-3 py-2.5',
        rest.disabled ? 'cursor-not-allowed opacity-40' : undefined,
        className,
      )}
    >
      {hasText ? (
        <span className="min-w-0">
          {label != null ? (
            <span className="block text-body-sm text-text">{label}</span>
          ) : null}
          {description ? (
            <span className="mt-0.5 block text-caption text-text-faint">
              {description}
            </span>
          ) : null}
        </span>
      ) : null}
      <span
        className={cx(
          'relative inline-flex h-5 w-9 shrink-0',
          compact ? undefined : 'mt-0.5',
        )}
      >
        <input {...rest} type="checkbox" className="peer sr-only" />
        <span className="absolute inset-0 rounded-full border border-subtle-strong bg-progress-track transition-colors peer-checked:border-accent peer-checked:bg-accent peer-focus-visible:outline-2 peer-focus-visible:outline-accent peer-focus-visible:outline-offset-2" />
        <span className="pointer-events-none absolute left-0.5 top-0.5 h-4 w-4 rounded-full bg-text-muted transition-transform motion-reduce:transition-none peer-checked:translate-x-4 peer-checked:bg-accent-ink" />
      </span>
    </label>
  );
}

// ─── SegmentedControl ────────────────────────────────────────────────────────
export interface SegmentedOption<T extends string | number> {
  value: T;
  label: ReactNode;
}

/** Desktop outer height 32 (md) / 28 (sm); taller segments below `md` for touch. */
const SEGMENT_SIZES = {
  sm: 'h-8 md:h-[1.375rem] px-2 text-xs',
  md: 'h-9 md:h-[1.625rem] px-2.5 text-[0.8125rem]',
} as const;

/**
 * Single-choice segmented control (ARIA radiogroup). Arrow keys move and
 * select, Home/End jump to the ends; only the selected segment is tabbable.
 */
export function SegmentedControl<T extends string | number>({
  value,
  onChange,
  options,
  ariaLabel,
  size = 'md',
  className,
}: {
  value: T;
  onChange: (value: T) => void;
  options: readonly SegmentedOption<T>[];
  ariaLabel: string;
  size?: 'sm' | 'md';
  className?: string;
}) {
  const buttonsRef = useRef<(HTMLButtonElement | null)[]>([]);
  const selectedIndex = options.findIndex((option) => option.value === value);
  const tabbableIndex = selectedIndex === -1 ? 0 : selectedIndex;

  const handleKeyDown = (event: KeyboardEvent<HTMLButtonElement>) => {
    const last = options.length - 1;
    const from = selectedIndex === -1 ? 0 : selectedIndex;
    let next: number;
    switch (event.key) {
      case 'ArrowRight':
      case 'ArrowDown':
        next = from >= last ? 0 : from + 1;
        break;
      case 'ArrowLeft':
      case 'ArrowUp':
        next = from <= 0 ? last : from - 1;
        break;
      case 'Home':
        next = 0;
        break;
      case 'End':
        next = last;
        break;
      default:
        return;
    }
    event.preventDefault();
    const option = options[next];
    if (!option) return;
    buttonsRef.current[next]?.focus();
    if (option.value !== value) onChange(option.value);
  };

  return (
    <div
      role="radiogroup"
      aria-label={ariaLabel}
      className={cx(
        'inline-flex items-center gap-0.5 rounded-sm border border-subtle-strong p-0.5',
        className,
      )}
    >
      {options.map((option, index) => {
        const selected = index === selectedIndex;
        return (
          <button
            key={String(option.value)}
            ref={(node) => {
              buttonsRef.current[index] = node;
            }}
            type="button"
            role="radio"
            aria-checked={selected}
            tabIndex={index === tabbableIndex ? 0 : -1}
            onClick={() => {
              if (!selected) onChange(option.value);
            }}
            onKeyDown={handleKeyDown}
            className={cx(
              'inline-flex items-center justify-center whitespace-nowrap rounded-sm font-medium transition-colors',
              'focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-1',
              SEGMENT_SIZES[size],
              selected
                ? 'bg-panel-strong text-text shadow-[inset_0_0_0_1px_var(--color-border)]'
                : 'text-text-muted hover:bg-hover-bg hover:text-text',
            )}
          >
            {option.label}
          </button>
        );
      })}
    </div>
  );
}

// ─── Drawer (right-hand sheet) ───────────────────────────────────────────────
const DRAWER_WIDTHS = {
  md: 'sm:max-w-md',
  lg: 'sm:max-w-lg',
  xl: 'sm:max-w-[60rem]',
} as const;

/**
 * Right-hand sheet on Base UI Dialog: full width on mobile, `width` from `sm`.
 * Slides in; with reduced motion it fades instead.
 */
export function Drawer({
  open,
  onOpenChange,
  title,
  description,
  width,
  children,
  footer,
}: {
  open: boolean;
  onOpenChange: (
    open: boolean,
    eventDetails: BaseDialog.Root.ChangeEventDetails,
  ) => void;
  title: ReactNode;
  description?: ReactNode;
  width: 'md' | 'lg' | 'xl';
  children: ReactNode;
  footer?: ReactNode;
}) {
  return (
    <BaseDialog.Root open={open} onOpenChange={onOpenChange}>
      <BaseDialog.Portal>
        <BaseDialog.Backdrop className="fixed inset-0 z-40 bg-drawer-backdrop transition-opacity duration-200 ease-out data-[ending-style]:opacity-0 data-[starting-style]:opacity-0" />
        <BaseDialog.Popup
          className={cx(
            'fixed inset-y-0 right-0 z-50 flex w-full flex-col overflow-x-hidden bg-bg-sub border-l border-subtle-strong shadow-overlay outline-none',
            'duration-200 ease-out',
            'motion-safe:transition-transform motion-safe:data-[ending-style]:translate-x-full motion-safe:data-[starting-style]:translate-x-full',
            'motion-reduce:transition-opacity motion-reduce:data-[ending-style]:opacity-0 motion-reduce:data-[starting-style]:opacity-0',
            DRAWER_WIDTHS[width],
          )}
        >
          <div className="flex items-start justify-between gap-3 px-4 py-3 border-b border-subtle">
            <div className="min-w-0">
              <BaseDialog.Title className="text-title-section text-text">
                {title}
              </BaseDialog.Title>
              {description ? (
                <BaseDialog.Description className="mt-0.5 text-body-sm text-text-muted">
                  {description}
                </BaseDialog.Description>
              ) : null}
            </div>
            <BaseDialog.Close
              aria-label="Close"
              className={cx(
                'inline-flex shrink-0 items-center justify-center rounded-sm text-text-muted transition-colors',
                'h-11 w-11 md:h-8 md:w-8 -mr-2 md:mr-0 hover:bg-overlay-5 hover:text-text',
                'focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-1',
              )}
            >
              <X className="w-4 h-4" aria-hidden="true" />
            </BaseDialog.Close>
          </div>
          <div className="flex-1 min-h-0 overflow-y-auto">{children}</div>
          {footer ? (
            <div className="px-4 py-3 border-t border-subtle flex flex-wrap items-center justify-end gap-2">
              {footer}
            </div>
          ) : null}
        </BaseDialog.Popup>
      </BaseDialog.Portal>
    </BaseDialog.Root>
  );
}

// ─── Notice ──────────────────────────────────────────────────────────────────
export type NoticeTone = 'info' | 'success' | 'warning' | 'danger';
/**
 * - `inline` (default): a well inside a card — tone/8 fill, no border, 14px
 *   icon. Use it inside the object that owns the problem.
 * - `banner`: the page-level incident line (at most one per page) — tone/8
 *   fill, a uniform 1px tone/45 line, 16px icon, title + short summary and
 *   at most one action. Collapse lists
 *   into a sentence.
 */
export type NoticeVariant = 'inline' | 'banner';

const NOTICE_FILL: Record<NoticeTone, string> = {
  info: 'bg-overlay-3',
  success: 'bg-overlay-3',
  warning: 'bg-warn/8',
  danger: 'bg-danger/8',
};
const NOTICE_BORDER: Record<NoticeTone, string> = {
  info: 'border-subtle',
  success: 'border-subtle',
  warning: 'border-warn/45',
  danger: 'border-danger/45',
};
// `info` is neutral: there is no blue in the system.
const NOTICE_ICON: Record<
  NoticeTone,
  { Icon: typeof Info; className: string }
> = {
  info: { Icon: Info, className: 'text-text-muted' },
  success: { Icon: CircleCheck, className: 'text-success-text' },
  warning: { Icon: TriangleAlert, className: 'text-warn-text' },
  danger: { Icon: CircleAlert, className: 'text-danger-text' },
};

export function Notice({
  tone = 'info',
  variant = 'inline',
  title,
  children,
  action,
  className,
  role,
}: {
  tone?: NoticeTone;
  variant?: NoticeVariant;
  title?: ReactNode;
  children?: ReactNode;
  action?: ReactNode;
  className?: string;
  role?: 'alert' | 'status';
}) {
  const banner = variant === 'banner';
  const { Icon, className: iconClass } = NOTICE_ICON[tone];
  return (
    <div
      className={cx(
        'flex gap-2.5 text-body text-text-muted',
        banner
          ? cx(
              'flex-wrap items-center rounded-md border px-4 py-3 sm:flex-nowrap',
              NOTICE_BORDER[tone],
            )
          : 'flex-col rounded-sm p-3 sm:flex-row sm:items-start',
        NOTICE_FILL[tone],
        className,
      )}
      data-tone={tone}
      role={role ?? (tone === 'danger' ? 'alert' : 'status')}
    >
      <div
        className={cx(
          'flex min-w-0 flex-1 gap-2.5',
          banner ? 'items-center' : 'items-start',
        )}
      >
        <Icon
          aria-hidden="true"
          strokeWidth={1.75}
          className={cx(
            'shrink-0',
            banner ? 'size-4' : 'mt-0.5 size-3.5',
            iconClass,
          )}
        />
        <div className={cx('min-w-0', banner && 'sm:truncate')}>
          {title ? (
            <span
              className={cx(
                'font-semibold text-text',
                banner ? 'mr-1.5' : 'block mb-0.5',
              )}
            >
              {title}
            </span>
          ) : null}
          {children ? (
            banner ? (
              <span>{children}</span>
            ) : (
              <div>{children}</div>
            )
          ) : null}
        </div>
      </div>
      {action ? <div className="shrink-0">{action}</div> : null}
    </div>
  );
}

// ─── Input + Select base classes ─────────────────────────────────────────────
const INPUT_BASE_CLASS =
  'w-full px-2.5 text-sm bg-input-bg border border-subtle-strong rounded-sm text-text placeholder:text-text-faint transition-colors ' +
  'focus:border-accent focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-1 disabled:cursor-not-allowed disabled:opacity-40';
/** 36px (md) text input / select trigger. */
export const INPUT_CLASS = `${INPUT_BASE_CLASS} h-9`;
/** 32px (sm) text input / select trigger for toolbars and dense forms. */
export const INPUT_SM_CLASS = `${INPUT_BASE_CLASS} h-8`;
