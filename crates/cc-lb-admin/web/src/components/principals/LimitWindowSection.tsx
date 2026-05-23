import type { PrincipalLimitWindow } from '../../lib/api';
import { LimitDonutChart } from './LimitDonutChart';

interface LimitWindowSectionProps {
  window: PrincipalLimitWindow;
}

const KIND_ORDER = ['requests', 'input_tokens', 'output_tokens', 'tokens'];

export function LimitWindowSection({ window }: LimitWindowSectionProps) {
  // Sort snapshots according to KIND_ORDER
  const sortedSnapshots = [...window.snapshots].sort((a, b) => {
    const idxA = KIND_ORDER.indexOf(a.kind);
    const idxB = KIND_ORDER.indexOf(b.kind);
    if (idxA === -1 && idxB === -1) return a.kind.localeCompare(b.kind);
    if (idxA === -1) return 1;
    if (idxB === -1) return -1;
    return idxA - idxB;
  });

  return (
    <div className="mb-6 last:mb-0">
      <h4 className="text-sm font-medium text-graphite-300 mb-3 flex items-center">
        <span className="bg-graphite-800 px-2 py-1 rounded text-xs font-mono mr-2">
          {window.window}
        </span>
        window
      </h4>
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        {sortedSnapshots.map((snapshot) => (
          <LimitDonutChart key={snapshot.kind} snapshot={snapshot} />
        ))}
      </div>
    </div>
  );
}
