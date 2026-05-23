export function Tooltip({ content, children }: { content: string; children: React.ReactNode }) {
  return (
    <div className="group relative inline-block">
      {children}
      <div className="absolute bottom-full left-1/2 -translate-x-1/2 mb-2 px-2 py-1 bg-graphite-800 text-graphite-100 text-xs rounded opacity-0 group-hover:opacity-100 transition-opacity pointer-events-none whitespace-nowrap z-50 border border-graphite-700 shadow-sm">
        {content}
        <div className="absolute top-full left-1/2 -translate-x-1/2 border-4 border-transparent border-t-graphite-800" />
      </div>
    </div>
  );
}
