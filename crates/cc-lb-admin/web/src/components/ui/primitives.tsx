import * as Dialog from '@radix-ui/react-dialog';
import * as Tooltip from '@radix-ui/react-tooltip';
import { X } from 'lucide-react';
import type { ButtonHTMLAttributes, HTMLAttributes, ReactNode } from 'react';
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
}: {
  title: ReactNode;
  subtitle?: ReactNode;
  action?: ReactNode;
  className?: string;
}) {
  return (
    <div
      className={cx(
        'flex flex-col sm:flex-row sm:items-start sm:justify-between gap-3 sm:gap-4 px-4 py-3 border-b border-subtle',
        className,
      )}
    >
      <div className="min-w-0">
        <h3 className="text-sm font-medium text-text">{title}</h3>
        {subtitle ? (
          <p className="mt-0.5 text-xs text-text-faint">{subtitle}</p>
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
  className,
  children,
  ...rest
}: ButtonProps) {
  return (
    <button
      type="button"
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
    >
      {iconLeft}
      {children}
      {iconRight}
    </button>
  );
}

// ─── IconButton (44px touch target on mobile) ────────────────────────────────
export function IconButton({
  className,
  children,
  label,
  ...rest
}: ButtonProps & { label: string }) {
  return (
    <button
      type="button"
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
    </button>
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
  className,
  style,
}: {
  className?: string;
  style?: React.CSSProperties;
}) {
  return <div className={cx('skeleton h-4 w-full', className)} style={style} />;
}
export function SkeletonRow({ cols = 5 }: { cols?: number }) {
  return (
    <tr>
      {Array.from({ length: cols }).map((_, i) => (
        <td key={i} className="px-3 py-2">
          <Skeleton />
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
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  title: ReactNode;
  description?: ReactNode;
  children: ReactNode;
  footer?: ReactNode;
  size?: 'sm' | 'md' | 'lg';
}) {
  const sizeClass =
    size === 'sm' ? 'max-w-sm' : size === 'lg' ? 'max-w-2xl' : 'max-w-md';
  return (
    <Dialog.Root open={open} onOpenChange={onOpenChange}>
      <Dialog.Portal>
        <Dialog.Overlay className="fixed inset-0 z-50 bg-modal-backdrop backdrop-blur-sm data-[state=open]:animate-in data-[state=open]:fade-in" />
        <Dialog.Content
          className={cx(
            'fixed top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 z-50 w-[calc(100%-2rem)] outline-none',
            'bg-bg-sub border border-subtle-strong rounded-sm shadow-2xl flex flex-col max-h-[85vh]',
            sizeClass,
          )}
        >
          <div className="flex items-start justify-between px-4 py-3 border-b border-subtle">
            <div className="min-w-0">
              <Dialog.Title className="text-sm font-medium text-text">
                {title}
              </Dialog.Title>
              {description ? (
                <Dialog.Description className="mt-0.5 text-xs text-text-faint">
                  {description}
                </Dialog.Description>
              ) : null}
            </div>
            <Dialog.Close asChild>
              <IconButton label="Close dialog">
                <X className="w-4 h-4" />
              </IconButton>
            </Dialog.Close>
          </div>
          <div className="flex-1 p-4 overflow-y-auto">{children}</div>
          {footer ? (
            <div className="px-4 py-3 border-t border-subtle flex items-center justify-end gap-2">
              {footer}
            </div>
          ) : null}
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
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
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  title: ReactNode;
  description?: ReactNode;
  confirmLabel?: string;
  cancelLabel?: string;
  destructive?: boolean;
  onConfirm: () => void;
}) {
  return (
    <Modal
      open={open}
      onOpenChange={onOpenChange}
      title={title}
      description={description}
      size="sm"
      footer={
        <>
          <Button variant="ghost" onClick={() => onOpenChange(false)}>
            {cancelLabel}
          </Button>
          <Button
            variant={destructive ? 'danger' : 'primary'}
            onClick={() => {
              onConfirm();
              onOpenChange(false);
            }}
          >
            {confirmLabel}
          </Button>
        </>
      }
    >
      {null}
    </Modal>
  );
}

// ─── Tooltip wrapper ─────────────────────────────────────────────────────────
export function Hint({
  label,
  children,
  side = 'top',
}: {
  label: ReactNode;
  children: ReactNode;
  side?: 'top' | 'right' | 'bottom' | 'left';
}) {
  return (
    <Tooltip.Provider delayDuration={200}>
      <Tooltip.Root>
        <Tooltip.Trigger asChild>{children}</Tooltip.Trigger>
        <Tooltip.Portal>
          <Tooltip.Content
            side={side}
            sideOffset={4}
            className="z-50 px-2 py-1 text-[11px] rounded-sm bg-bg-sub border border-subtle-strong text-text shadow-lg"
          >
            {label}
            <Tooltip.Arrow className="fill-[color:var(--color-bg-sub)]" />
          </Tooltip.Content>
        </Tooltip.Portal>
      </Tooltip.Root>
    </Tooltip.Provider>
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
              <p className="text-xs text-text-faint mt-0.5">{subtitle}</p>
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
  children,
  required,
}: {
  label: string;
  hint?: string;
  error?: string;
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
      {error ? <span className="text-[11px] text-red-400">{error}</span> : null}
    </label>
  );
}

// ─── Input + Select base classes ─────────────────────────────────────────────
export const INPUT_CLASS =
  'w-full h-9 px-2.5 text-sm bg-bg border border-subtle rounded-sm text-text placeholder:text-text-faint ' +
  'focus:border-accent focus:outline-none transition-colors';
