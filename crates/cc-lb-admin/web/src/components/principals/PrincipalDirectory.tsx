import { PrincipalListResponse } from '../../lib/api';

interface PrincipalDirectoryProps {
  data: PrincipalListResponse | null;
  isLoading: boolean;
  selectedId: string | null;
  onSelect: (id: string) => void;
}

export function PrincipalDirectory({ data, isLoading, selectedId, onSelect }: PrincipalDirectoryProps) {
  if (isLoading) {
    return (
      <div className="w-full lg:w-48 flex-shrink-0 space-y-2">
        {[1, 2, 3].map((i) => (
          <div key={i} className="h-8 bg-graphite-800 rounded animate-pulse" />
        ))}
      </div>
    );
  }

  if (!data || data.principals.length === 0) {
    return (
      <div className="w-full lg:w-48 flex-shrink-0 text-sm text-graphite-400">
        No principals found.
      </div>
    );
  }

  return (
    <div className="w-full lg:w-48 flex-shrink-0 flex lg:flex-col overflow-x-auto lg:overflow-x-visible space-x-2 lg:space-x-0 lg:space-y-1 pb-2 lg:pb-0">
      {data.principals.map((p) => {
        const isSelected = p.id === selectedId;
        return (
          <button
            key={p.id}
            onClick={() => onSelect(p.id)}
            className={`px-3 py-2 text-sm font-mono text-left whitespace-nowrap rounded transition-colors ${
              isSelected
                ? 'bg-graphite-800 text-cyan-400 border-l-2 border-cyan-400'
                : 'text-graphite-300 hover:bg-graphite-800/50 hover:text-graphite-100 border-l-2 border-transparent'
            }`}
          >
            {p.id}
          </button>
        );
      })}
    </div>
  );
}
