import { useConfigHistory } from '../../lib/hooks/useConfigHistory';
import { LoadingState } from '../primitives/LoadingState';
import { ErrorState } from '../primitives/ErrorState';

interface HistoryDrawerProps {
  onSelectRevision: (revision: number) => void;
}

function formatRelativeTime(unixSecs: number) {
  const rtf = new Intl.RelativeTimeFormat('en', { numeric: 'auto' });
  const diffSecs = unixSecs - Math.floor(Date.now() / 1000);
  
  if (Math.abs(diffSecs) < 60) return rtf.format(diffSecs, 'second');
  if (Math.abs(diffSecs) < 3600) return rtf.format(Math.floor(diffSecs / 60), 'minute');
  if (Math.abs(diffSecs) < 86400) return rtf.format(Math.floor(diffSecs / 3600), 'hour');
  return rtf.format(Math.floor(diffSecs / 86400), 'day');
}

export function HistoryDrawer({ onSelectRevision }: HistoryDrawerProps) {
  const { history, error, loading } = useConfigHistory(20);

  if (loading) return <div className="p-4"><LoadingState message="Loading history..." /></div>;
  if (error) return <div className="p-4"><ErrorState title="Failed to load history" message={error.message} /></div>;
  if (!history) return null;

  return (
    <div className="p-4">
      <h3 className="text-sm font-semibold text-graphite-50 mb-4 uppercase tracking-wider">Recent Revisions</h3>
      <div className="space-y-3">
        {history.history.map((item) => (
          <button
            key={item.revision}
            onClick={() => onSelectRevision(item.revision)}
            className="w-full text-left p-3 rounded-md border border-graphite-800 bg-graphite-900/50 hover:bg-graphite-800 transition-colors"
          >
            <div className="flex justify-between items-center mb-2">
              <span className="font-mono font-medium text-graphite-200">Rev {item.revision}</span>
              <span className="text-xs text-graphite-400">{formatRelativeTime(item.applied_at_unix_secs)}</span>
            </div>
            <div className="flex flex-wrap gap-1">
              <span className="inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-medium bg-graphite-800 text-graphite-300">
                {item.config_summary.upstreams} upstreams
              </span>
              <span className="inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-medium bg-graphite-800 text-graphite-300">
                {item.config_summary.principals} principals
              </span>
              {item.config_summary.tls_enabled && (
                <span className="inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-medium bg-blue-900/30 text-blue-300">
                  TLS
                </span>
              )}
            </div>
          </button>
        ))}
      </div>
    </div>
  );
}
