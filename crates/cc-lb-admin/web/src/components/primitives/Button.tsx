export type ButtonVariant = 'primary' | 'secondary' | 'ghost' | 'danger';

const VARIANTS: Record<ButtonVariant, string> = {
  primary:
    'bg-cyan-500 hover:bg-cyan-400 text-graphite-950 font-medium border-transparent',
  secondary:
    'bg-graphite-800 hover:bg-graphite-700 text-graphite-100 border border-graphite-700',
  ghost:
    'bg-transparent hover:bg-graphite-800 text-graphite-300 hover:text-graphite-50 border-transparent',
  danger:
    'bg-red-500/10 hover:bg-red-500/20 text-red-400 hover:text-red-300 border-transparent',
};

export function Button({
  variant = 'secondary',
  className = '',
  children,
  ...props
}: React.ButtonHTMLAttributes<HTMLButtonElement> & {
  variant?: ButtonVariant;
}) {
  return (
    <button
      className={`inline-flex items-center justify-center px-3 py-2 text-sm rounded-md transition-colors focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-cyan-500 disabled:opacity-50 disabled:cursor-not-allowed ${VARIANTS[variant]} ${className}`}
      {...props}
    >
      {children}
    </button>
  );
}
