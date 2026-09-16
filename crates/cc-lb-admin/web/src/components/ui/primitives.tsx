import { AlertDialog as BaseAlertDialog } from '@base-ui/react/alert-dialog';
import { Button as BaseButton } from '@base-ui/react/button';
import { Dialog as BaseDialog } from '@base-ui/react/dialog';
import { Popover as BasePopover } from '@base-ui/react/popover';
import { X } from 'lucide-react';
import {
  type ButtonHTMLAttributes,
  cloneElement,
  type FocusEventHandler,
  type HTMLAttributes,
  type InputHTMLAttributes,
  isValidElement,
  type MouseEventHandler,
  type PointerEventHandler,
  type ReactNode,
  useEffect,
  useRef,
  useState,
} from 'react';
import { Area, AreaChart, ResponsiveContainer } from 'recharts';

import { getWindowColor } from '../../lib/colors';

// ─── classnames helper ───────────────────────────────────────────────────────
export function cx(...parts: Array<string | false | null | undefined>): string {
  return parts.filter(Boolean).join(' ');
}

// ─── Card ────────────────────────────────────────────────────────────────────
export function Card({ className, ...rest }: HTMLAttributes<HTMLDivElement>) {
  return <div className={cx('glass rounded-sm', className)} {...rest} />;
}
export function CardHeader({
  title,
  action,
  subtitle,
  className,
  align = 'start',
}: {
  readonly title: ReactNode;
  readonly subtitle?: ReactNode;
  readonly action?: ReactNode;
  readonly className?: string;
  readonly align?: 'start' | 'center';
}) {
  return (
    <div
      className={cx(
        'flex flex-col sm:flex-row sm:justify-between gap-3 sm:gap-4 px-4 py-3 border-b border-subtle',
        align === 'center' ? 'sm:items-center' : 'sm:items-start',
        className,
      )}
    >
      <div className="min-w-0">
        <h3 className="text-sm font-medium text-text">{title}</h3>
        {subtitle ? (
          <div
            className="mt-0.5 min-h-4 text-xs text-text-faint"
            data-slot="card-subtitle"
          >
            {subtitle}
          </div>
        ) : null}
      </div>
      {action ? (
        <div className="flex flex-wrap items-center gap-2 sm:shrink-0 min-w-0 w-full sm:w-auto">
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
type ButtonVariant = 'primary' | 'secondary' | 'ghost' | 'danger' | 'accent';
type ButtonSize = 'sm' | 'md';
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
    'bg-[color:var(--color-text)] text-[color:var(--color-bg)] hover:opacity-90 disabled:opacity-50',
  secondary:
    'bg-[color:var(--color-panel-strong)] border border-[color:var(--color-border)] text-[color:var(--color-text)] hover:bg-[color:var(--color-hover-bg)]',
  ghost:
    'text-[color:var(--color-text-muted)] hover:bg-[color:var(--color-hover-bg)] hover:text-[color:var(--color-text)]',
  danger:
    'bg-red-500/25 text-[color:var(--color-text)] border border-red-500/50 hover:bg-red-500/40 shadow-[0_0_0_1px_rgba(239,68,68,0.15)]',
  accent:
    'bg-[color:var(--color-accent-dim)] text-[color:var(--color-accent)] border border-[color:var(--color-accent)] hover:opacity-90',
};
const BTN_SIZES: Record<ButtonSize, string> = {
  sm: 'h-7 px-2.5 text-xs gap-1.5',
  md: 'h-9 px-3 text-sm gap-2',
};
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
  // `disabled` and `aria-busy` are applied after the prop spread so a loading
  // Button can never be re-enabled or stripped of its busy state by callers.
  return (
    <BaseButton
      type={type ?? 'button'}
      className={cx(
        'inline-flex items-center justify-center rounded-sm font-medium transition-colors select-none',
        'disabled:cursor-not-allowed disabled:opacity-50',
        'focus-visible:outline focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-2',
        BTN_VARIANTS[variant],
        BTN_SIZES[size],
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
        'inline-flex items-center justify-center text-text-muted hover:text-text rounded-sm',
        'h-9 w-9 md:h-8 md:w-8 hover:bg-overlay-5',
        'focus-visible:outline focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-1',
        className,
      )}
      {...rest}
    >
      {children}
    </BaseButton>
  );
}

// ─── Badge ───────────────────────────────────────────────────────────────────
type BadgeTone = 'neutral' | 'accent' | 'ok' | 'warn' | 'danger' | 'mono';
const BADGE_TONES: Record<BadgeTone, string> = {
  neutral: 'bg-overlay-4 text-text border-subtle',
  accent: 'bg-accent/15 text-accent border-accent/40',
  ok: 'bg-[color:var(--color-ok)]/15 text-[color:var(--color-ok)] border-[color:var(--color-ok)]/40',
  warn: 'bg-[color:var(--color-warn)]/15 text-[color:var(--color-warn)] border-[color:var(--color-warn)]/40',
  danger:
    'bg-[color:var(--color-danger)]/15 text-[color:var(--color-danger)] border-[color:var(--color-danger)]/40',
  mono: 'bg-overlay-5 text-text border-subtle font-mono',
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
        'inline-flex items-center px-2 py-0.5 text-[11px] rounded-sm border',
        BADGE_TONES[tone],
        className,
      )}
    >
      {children}
    </span>
  );
}

// ─── StatusBadge (dot + label) ───────────────────────────────────────────────
export function StatusBadge({
  tone,
  label,
}: {
  tone: 'ok' | 'warn' | 'danger' | 'neutral' | 'live';
  label: ReactNode;
}) {
  return (
    <span className="inline-flex items-center gap-2 px-2 py-0.5 text-[11px] rounded-sm bg-overlay-3 border border-subtle text-text-muted">
      <span className={cx('status-dot', tone)} />
      <span className="font-mono uppercase tracking-wider">{label}</span>
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
export function EmptyState({
  icon,
  title,
  description,
  action,
}: {
  icon?: ReactNode;
  title: ReactNode;
  description?: ReactNode;
  action?: ReactNode;
}) {
  return (
    <div className="flex flex-col items-center justify-center text-center py-12 px-6 border border-dashed border-subtle rounded-sm bg-overlay-1">
      {icon ? <div className="mb-3 text-text-faint">{icon}</div> : null}
      <h3 className="text-sm font-medium text-text">{title}</h3>
      {description ? (
        <p className="mt-1 text-xs text-text-faint max-w-md">{description}</p>
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
        <BaseDialog.Backdrop className="fixed inset-0 z-50 bg-modal-backdrop backdrop-blur-sm transition-opacity duration-150 ease-out data-[ending-style]:opacity-0 data-[starting-style]:opacity-0" />
        <BaseDialog.Popup
          className={cx(
            'fixed top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 z-50 w-[calc(100%-2rem)] outline-none',
            'bg-bg-sub border border-subtle-strong rounded-sm shadow-2xl flex flex-col max-h-[85vh]',
            'transition-[opacity,scale] duration-150 ease-out data-[ending-style]:opacity-0 data-[ending-style]:scale-95 data-[starting-style]:opacity-0 data-[starting-style]:scale-95',
            sizeClass,
          )}
        >
          <div className="flex items-start justify-between px-4 py-3 border-b border-subtle">
            <div className="min-w-0">
              <BaseDialog.Title className="text-sm font-medium text-text">
                {title}
              </BaseDialog.Title>
              {description ? (
                <BaseDialog.Description className="mt-0.5 text-xs text-text-faint">
                  {description}
                </BaseDialog.Description>
              ) : null}
            </div>
            <BaseDialog.Close
              aria-label="Close dialog"
              disabled={preventDismiss}
              aria-disabled={preventDismiss || undefined}
              className={cx(
                'inline-flex items-center justify-center text-text-muted hover:text-text rounded-sm',
                'h-9 w-9 md:h-8 md:w-8 hover:bg-overlay-5',
                'focus-visible:outline focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-1',
                'disabled:cursor-not-allowed disabled:opacity-50 disabled:hover:bg-transparent disabled:hover:text-text-muted',
              )}
            >
              <X className="w-4 h-4" />
            </BaseDialog.Close>
          </div>
          <div className="flex-1 p-4 overflow-y-auto">{children}</div>
          {footer ? (
            <div className="px-4 py-3 border-t border-subtle flex items-center justify-end gap-2">
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
        <BaseAlertDialog.Backdrop className="fixed inset-0 z-50 bg-modal-backdrop backdrop-blur-sm transition-opacity duration-150 ease-out data-[ending-style]:opacity-0 data-[starting-style]:opacity-0" />
        <BaseAlertDialog.Popup
          className={cx(
            'fixed top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 z-50 w-[calc(100%-2rem)] outline-none',
            'bg-bg-sub border border-subtle-strong rounded-sm shadow-2xl flex flex-col max-h-[85vh] max-w-sm',
            'transition-[opacity,scale] duration-150 ease-out data-[ending-style]:opacity-0 data-[ending-style]:scale-95 data-[starting-style]:opacity-0 data-[starting-style]:scale-95',
          )}
        >
          <div className="flex items-start justify-between px-4 py-3 border-b border-subtle">
            <div className="min-w-0">
              <BaseAlertDialog.Title className="text-sm font-medium text-text">
                {title}
              </BaseAlertDialog.Title>
            </div>
          </div>
          <div className="flex-1 p-4 overflow-y-auto">
            {description ? (
              <BaseAlertDialog.Description className="text-sm leading-relaxed text-text-muted">
                {description}
              </BaseAlertDialog.Description>
            ) : null}
          </div>
          <div className="px-4 py-3 border-t border-subtle flex items-center justify-end gap-2">
            <Button variant="ghost" disabled={pending} onClick={requestClose}>
              {cancelLabel}
            </Button>
            <Button
              autoFocus
              disabled={confirmDisabled}
              loading={pending}
              onClick={() => {
                onConfirm();
                if (closeOnConfirm) onOpenChange(false);
              }}
              variant={destructive ? 'danger' : 'primary'}
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
}: {
  label: ReactNode;
  children: ReactNode;
  side?: 'top' | 'right' | 'bottom' | 'left';
  stopClickPropagation?: boolean;
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
          setEngaged(true);
          setOpen(true);
        },
        onClick: (e) => {
          if (stopClickPropagation) e.stopPropagation();
          childElement.props.onClick?.(e);
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
          setEngaged(true);
          setOpen(true);
        }}
        onClick={(e) => {
          if (stopClickPropagation) e.stopPropagation();
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
        <BasePopover.Positioner side={side} sideOffset={4}>
          <BasePopover.Popup className="z-50 px-2 py-1 text-[11px] rounded-sm bg-bg-sub border border-subtle-strong text-text shadow-lg">
            {label}
          </BasePopover.Popup>
        </BasePopover.Positioner>
      </BasePopover.Portal>
    </BasePopover.Root>
  );
}

// ─── KpiTile ─────────────────────────────────────────────────────────────────
interface KpiTileProps {
  label: string;
  value: ReactNode;
  delta?: {
    value: ReactNode;
    direction: 'up' | 'down' | 'flat';
    isPositive: boolean | null;
  };
  hint?: string;
}
export function KpiTile({ label, value, delta, hint }: KpiTileProps) {
  const deltaColor =
    !delta || delta.isPositive === null
      ? 'text-text-faint'
      : delta.isPositive
        ? 'text-green-400'
        : 'text-red-400';
  const arrow =
    delta?.direction === 'up' ? '↑' : delta?.direction === 'down' ? '↓' : '·';
  return (
    <div className="glass rounded-sm p-3 flex flex-col gap-1.5 min-h-[80px]">
      <div className="flex items-center justify-between gap-2">
        <span className="text-[11px] uppercase tracking-wider text-text-faint truncate">
          {label}
        </span>
        {hint ? (
          <Hint label={hint}>
            <span className="text-text-faint text-[10px] cursor-help shrink-0">
              i
            </span>
          </Hint>
        ) : null}
      </div>
      <div className="flex flex-wrap items-baseline gap-x-2 gap-y-0.5">
        <span className="text-2xl font-medium tabular-nums leading-none">
          {value}
        </span>
        {delta ? (
          <span
            className={cx(
              'text-[11px] font-mono tabular-nums whitespace-nowrap',
              deltaColor,
            )}
          >
            {arrow} {delta.value}
          </span>
        ) : null}
      </div>
    </div>
  );
}

// ─── Sparkline ───────────────────────────────────────────────────────────────
export function Sparkline({
  data,
  color = 'var(--color-accent)',
}: {
  data: number[];
  color?: string;
}) {
  const chartData = data.map((value, i) => ({ i, value }));
  const gid = `spark-${color.replace(/[^a-z0-9]/gi, '')}`;
  return (
    <div className="w-full" style={{ minWidth: 60, height: 28 }}>
      <ResponsiveContainer
        width="100%"
        height="100%"
        minWidth={60}
        minHeight={28}
      >
        <AreaChart
          data={chartData}
          margin={{ top: 1, right: 0, bottom: 1, left: 0 }}
        >
          <defs>
            <linearGradient id={gid} x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor={color} stopOpacity={0.45} />
              <stop offset="100%" stopColor={color} stopOpacity={0} />
            </linearGradient>
          </defs>
          <Area
            type="monotone"
            dataKey="value"
            stroke={color}
            strokeWidth={1.4}
            fill={`url(#${gid})`}
            isAnimationActive={false}
          />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}

// ─── QuotaMiniChart ──────────────────────────────────────────────────────────
export function QuotaMiniChart({
  data,
}: {
  data: { i: number; val5h: number | null; val7d: number | null }[];
}) {
  const c5h = getWindowColor('5h');
  const c7d = getWindowColor('7d');

  return (
    <div className="w-full" style={{ minWidth: 60, height: 28 }}>
      <ResponsiveContainer
        width="100%"
        height="100%"
        minWidth={60}
        minHeight={28}
      >
        <AreaChart
          data={data}
          margin={{ top: 1, right: 0, bottom: 1, left: 0 }}
        >
          <Area
            type="stepAfter"
            dataKey="val7d"
            stroke={c7d.stroke}
            strokeWidth={1.4}
            fill="none"
            isAnimationActive={false}
            connectNulls={false}
          />
          <Area
            type="stepAfter"
            dataKey="val5h"
            stroke={c5h.stroke}
            strokeWidth={1.4}
            fill="none"
            isAnimationActive={false}
            connectNulls={false}
          />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}

// ─── Section (used in pages) ─────────────────────────────────────────────────
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
    <section className={cx('flex flex-col gap-3', className)}>
      {title || action ? (
        <header className="flex flex-wrap items-start justify-between gap-3">
          <div className="min-w-0">
            {title ? (
              <h2 className="text-sm font-medium text-text">{title}</h2>
            ) : null}
            {subtitle ? (
              <div
                className="text-xs text-text-faint mt-0.5 min-h-4"
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
        'p-4 md:p-6 pb-8 md:pb-12 w-full max-w-[90rem] mx-auto space-y-6',
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
        'flex flex-col h-[calc(100dvh-3rem)] min-h-0 px-4 md:px-6 pt-4 md:pt-6 pb-4 w-full max-w-[120rem] mx-auto',
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
export function Field({
  label,
  hint,
  error,
  errorId,
  children,
  required,
}: {
  label: string;
  hint?: string;
  error?: string;
  errorId?: string;
  children: ReactNode;
  required?: boolean;
}) {
  return (
    <label className="flex flex-col gap-1.5">
      <span className="text-[11px] uppercase tracking-wider text-text-faint">
        {label}
        {required ? <span className="text-red-400 ml-1">*</span> : null}
      </span>
      {children}
      {hint && !error ? (
        <span className="text-[11px] text-text-faint">{hint}</span>
      ) : null}
      {error ? (
        <span id={errorId} className="text-[11px] text-red-400">
          {error}
        </span>
      ) : null}
    </label>
  );
}

export function ToggleSwitch({
  label,
  description,
  className,
  variant = 'card',
  ...rest
}: Omit<InputHTMLAttributes<HTMLInputElement>, 'type'> & {
  label: ReactNode;
  description?: ReactNode;
  /**
   * 'card' renders the bordered panel used for standalone toggles.
   * 'compact' drops the border/background/padding so the switch can sit
   * inline inside a denser header row.
   */
  variant?: 'card' | 'compact';
}) {
  const compact = variant === 'compact';
  return (
    <label
      className={cx(
        compact
          ? 'flex min-h-[44px] min-w-0 cursor-pointer items-center gap-2'
          : 'flex min-w-0 cursor-pointer items-start justify-between gap-4 rounded-sm border border-subtle bg-panel-strong/35 px-3 py-2.5',
        rest.disabled ? 'cursor-not-allowed opacity-60' : undefined,
        className,
      )}
    >
      <span className="min-w-0">
        <span
          className={cx('block text-text', compact ? 'text-xs' : 'text-sm')}
        >
          {label}
        </span>
        {description ? (
          <span className="mt-0.5 block text-xs leading-relaxed text-text-faint">
            {description}
          </span>
        ) : null}
      </span>
      <span className="relative mt-0.5 inline-flex h-5 w-9 shrink-0">
        <input {...rest} type="checkbox" className="peer sr-only" />
        <span className="absolute inset-0 rounded-full border border-subtle-strong bg-overlay-10 transition-colors peer-checked:border-accent peer-checked:bg-accent/35 peer-focus-visible:outline peer-focus-visible:outline-2 peer-focus-visible:outline-accent peer-focus-visible:outline-offset-2" />
        <span className="pointer-events-none absolute left-0.5 top-0.5 h-4 w-4 rounded-full bg-text-muted shadow-sm transition-transform peer-checked:translate-x-4 peer-checked:bg-accent" />
      </span>
    </label>
  );
}

type NoticeTone = 'info' | 'success' | 'warning' | 'danger';

const NOTICE_TONE_CLASS: Record<NoticeTone, string> = {
  info: 'border-blue-500/30 bg-blue-500/10 text-blue-100',
  success: 'border-emerald-500/30 bg-emerald-500/10 text-emerald-100',
  warning: 'border-amber-500/35 bg-amber-500/10 text-amber-100',
  danger: 'border-red-500/35 bg-red-500/10 text-red-100',
};

export function Notice({
  tone = 'info',
  title,
  children,
  action,
  className,
  role,
}: {
  tone?: NoticeTone;
  title?: ReactNode;
  children: ReactNode;
  action?: ReactNode;
  className?: string;
  role?: 'alert' | 'status';
}) {
  return (
    <div
      className={cx(
        'flex flex-col gap-3 rounded-sm border px-3 py-2.5 text-xs sm:flex-row sm:items-start sm:justify-between',
        NOTICE_TONE_CLASS[tone],
        className,
      )}
      role={role ?? (tone === 'danger' ? 'alert' : 'status')}
    >
      <div className="min-w-0 leading-relaxed">
        {title ? (
          <div className="mb-0.5 font-medium text-text">{title}</div>
        ) : null}
        <div>{children}</div>
      </div>
      {action ? <div className="shrink-0">{action}</div> : null}
    </div>
  );
}

// ─── Input + Select base classes ─────────────────────────────────────────────
export const INPUT_CLASS =
  'w-full h-9 px-2.5 text-sm bg-bg border border-subtle rounded-sm text-text placeholder:text-text-faint ' +
  'focus:border-accent focus:outline-none transition-colors';
