interface BulkheadGaugeProps {
  available: number;
  max: number;
  observed: boolean;
}

export function BulkheadGauge({
  available,
  max,
  observed,
}: BulkheadGaugeProps) {
  if (!observed) {
    return <span className="text-xs text-graphite-400">unobserved</span>;
  }

  const used = max - available;
  const percent = max > 0 ? (used / max) * 100 : 0;

  let colorClass = 'bg-green-500';
  if (percent > 80) colorClass = 'bg-red-500';
  else if (percent > 60) colorClass = 'bg-amber-500';

  return (
    <div className="flex items-center gap-2">
      <div className="w-24 h-2 bg-gray-200 rounded-full overflow-hidden">
        <div
          className={`h-full ${colorClass} transition-all duration-300`}
          style={{ width: `${Math.min(100, Math.max(0, percent))}%` }}
        />
      </div>
      <span className="text-xs text-gray-600 font-mono">
        {available}/{max}
      </span>
    </div>
  );
}
