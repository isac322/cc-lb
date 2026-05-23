export type ButtonVariant = 'primary' | 'secondary' | 'ghost' | 'danger';

const VARIANTS: Record<ButtonVariant, string> = {
  primary: 'bg-cyan-600 hover:bg-cyan-500 text-white border-transparent',
  secondary: 'bg-graphite-800 hover:bg-graphite-700 text-graphite-100 border-graphite-700',
  ghost: 'bg-transparent hover:bg-graphite-800 text-graphite-300 hover:text-graphite-100 border-transparent',
  danger: 'bg-red-600/10 hover:bg-red-600/20 text-red-400 border-red-500/20',
};

export function Button({
  variant = 'secondary',
  className = '',
  children,
  ...props
}: React.ButtonHTMLAttributes<HTMLButtonElement> & { variant?: ButtonVariant }) {
  return (
    <button
      className={`inline-flex items-center justify-center px-4 py-2 text-sm font-medium rounded border transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-cyan-500 disabled:opacity-50 disabled:cursor-not-allowed ${VARIANTS[variant]} ${className}`}
      {...props}
    >
      {children}
    </button>
  );
}
