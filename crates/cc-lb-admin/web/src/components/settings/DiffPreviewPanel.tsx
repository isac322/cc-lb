import { useConfigDiff } from '../../lib/hooks/useConfigDiff';
import { LoadingState } from '../primitives/LoadingState';
import { ErrorState } from '../primitives/ErrorState';

interface DiffPreviewPanelProps {
  fromRevision: number | null;
  toRevision: number | null;
}

export function DiffPreviewPanel({ fromRevision, toRevision }: DiffPreviewPanelProps) {
  const { diff, error, loading } = useConfigDiff(fromRevision, toRevision);

  if (loading) return <LoadingState message="Loading diff..." />;
  if (error) return <ErrorState title="Failed to load diff" message={error.message} />;
  if (!diff) return null;

  if (diff.diff.length === 0) {
    return (
      <div className="p-4 text-center text-graphite-400 text-sm">
        No changes between revision {diff.from} and {diff.to}.
      </div>
    );
  }

  return (
    <div className="space-y-2">
      <div className="flex justify-between items-center mb-4">
        <h3 className="text-sm font-medium text-graphite-200">
          Changes (Rev {diff.from} → Rev {diff.to})
        </h3>
        {diff.truncated_changes_count && (
          <span className="text-xs text-yellow-400 bg-yellow-400/10 px-2 py-1 rounded">
            +{diff.truncated_changes_count} more changes truncated
          </span>
        )}
      </div>
      <div className="border border-graphite-800 rounded-md overflow-hidden">
        <table className="min-w-full divide-y divide-graphite-800">
          <thead className="bg-graphite-900">
            <tr>
              <th className="px-4 py-2 text-left text-xs font-medium text-graphite-400 uppercase tracking-wider">Path</th>
              <th className="px-4 py-2 text-left text-xs font-medium text-graphite-400 uppercase tracking-wider">From</th>
              <th className="px-4 py-2 text-left text-xs font-medium text-graphite-400 uppercase tracking-wider">To</th>
            </tr>
          </thead>
          <tbody className="bg-graphite-900/50 divide-y divide-graphite-800">
            {diff.diff.map((item, idx) => (
              <tr key={idx}>
                <td className="px-4 py-2 text-sm text-graphite-300 font-mono">{item.path}</td>
                <td className="px-4 py-2 text-sm text-red-400 font-mono bg-red-400/5">
                  {JSON.stringify(item.from)}
                </td>
                <td className="px-4 py-2 text-sm text-green-400 font-mono bg-green-400/5">
                  {JSON.stringify(item.to)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
