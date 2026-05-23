import { useSearchParams } from 'react-router';

const RANGES = [
  { value: '15m', label: '15m' },
  { value: '1h', label: '1h' },
  { value: '6h', label: '6h' },
  { value: '24h', label: '24h' },
  { value: '7d', label: '7d' },
];

export function RangeSelector() {
  const [searchParams, setSearchParams] = useSearchParams();
  const currentRange = searchParams.get('range') || '1h';

  const handleRangeChange = (range: string) => {
    const newParams = new URLSearchParams(searchParams);
    newParams.set('range', range);
    setSearchParams(newParams);
  };

  return (
    <div className="flex items-center space-x-1 bg-graphite-900 p-1 rounded-md border border-graphite-800">
      {RANGES.map(({ value, label }) => (
        <button
          key={value}
          onClick={() => handleRangeChange(value)}
          className={`px-3 py-1 text-sm font-medium transition-colors ${
            currentRange === value
              ? 'bg-cyan-500/15 text-cyan-300 border border-cyan-500/30 rounded-md'
              : 'text-graphite-400 hover:text-graphite-100 hover:bg-graphite-800 rounded-md border border-transparent'
          }`}
        >
          {label}
        </button>
      ))}
    </div>
  );
}
