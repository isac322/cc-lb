export type StatusVariant =
  | 'ok'
  | 'warn'
  | 'danger'
  | 'info'
  | 'neutral'
  | 'live';

const VARIANTS: Record<StatusVariant, string> = {
  ok: 'bg-green-500/10 text-green-400 border-green-500/20',
  warn: 'bg-amber-500/10 text-amber-400 border-amber-500/20',
  danger: 'bg-red-500/10 text-red-400 border-red-500/20',
  info: 'bg-cyan-500/10 text-cyan-400 border-cyan-500/20',
  neutral: 'bg-graphite-800 text-graphite-300 border-graphite-700',
  live: 'bg-cyan-500/10 text-cyan-400 border-cyan-500/20 animate-pulse-live',
};

export function StatusChip({
  variant,
  children,
}: {
  variant: StatusVariant;
  children: React.ReactNode;
}) {
  return (
    <span
      className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium border ${VARIANTS[variant]}`}
    >
      {variant === 'live' && (
        <span className="w-1.5 h-1.5 rounded-full bg-cyan-400 mr-1.5" />
      )}
      {children}
    </span>
  );
}
