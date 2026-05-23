export function Card({
  children,
  className = '',
}: {
  children: React.ReactNode;
  className?: string;
}) {
  return (
    <div
      className={`bg-graphite-850 border border-graphite-800/70 rounded-lg shadow-sm ${className}`}
    >
      {children}
    </div>
  );
}
