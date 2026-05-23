import { Link } from 'react-router';
import { formatDuration } from '../../lib/format';
import { useLiveEvents } from '../../lib/hooks/useLiveEvents';
import { Card } from '../primitives/Card';

export function EmbeddedLiveLog() {
  const { events, status } = useLiveEvents();

  return (
    <Card className="flex flex-col overflow-hidden">
      <div className="p-4 border-b border-graphite-800 flex items-center justify-between bg-graphite-900/50">
        <div className="flex items-center space-x-3">
          <h3 className="text-sm font-medium text-graphite-50">
            Live Requests
          </h3>
          <div className="flex items-center space-x-1.5">
            <div
              className={`w-2 h-2 rounded-full ${
                status === 'live'
                  ? 'bg-green-500 animate-pulse'
                  : status === 'connecting' || status === 'reconnecting'
                    ? 'bg-yellow-500'
                    : 'bg-red-500'
              }`}
            />
            <span className="text-xs text-graphite-400 capitalize">
              {status}
            </span>
          </div>
        </div>
      </div>

      <div className="overflow-x-auto">
        <table className="w-full text-sm text-left">
          <thead className="text-xs text-graphite-400 bg-graphite-900/30 border-b border-graphite-800">
            <tr>
              <th className="px-4 py-2 font-medium">Time</th>
              <th className="px-4 py-2 font-medium">Principal</th>
              <th className="px-4 py-2 font-medium">Model</th>
              <th className="px-4 py-2 font-medium">Status</th>
              <th className="px-4 py-2 font-medium text-right">Duration</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-graphite-800/50">
            {events.length === 0 ? (
              <tr>
                <td
                  colSpan={5}
                  className="px-4 py-8 text-center text-graphite-500"
                >
                  {status === 'live'
                    ? 'Waiting for requests...'
                    : 'Connecting...'}
                </td>
              </tr>
            ) : (
              events.map((ev) => {
                const date = new Date(ev.ts);
                const timeStr = date.toLocaleTimeString([], {
                  hour12: false,
                  hour: '2-digit',
                  minute: '2-digit',
                  second: '2-digit',
                });
                const isError = ev.status >= 400;

                return (
                  <tr
                    key={ev.request_id}
                    className="hover:bg-graphite-800/30 transition-colors"
                  >
                    <td
                      className="px-4 py-2 text-graphite-400 whitespace-nowrap"
                      title={date.toISOString()}
                    >
                      {timeStr}
                    </td>
                    <td
                      className="px-4 py-2 font-mono text-xs text-graphite-300 truncate max-w-[150px]"
                      title={ev.principal_id}
                    >
                      {ev.principal_id || '-'}
                    </td>
                    <td
                      className="px-4 py-2 text-graphite-300 truncate max-w-[150px]"
                      title={ev.model}
                    >
                      {ev.model}
                    </td>
                    <td className="px-4 py-2">
                      <span
                        className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${
                          isError
                            ? 'bg-red-500/10 text-red-400'
                            : 'bg-green-500/10 text-green-400'
                        }`}
                      >
                        {ev.status}
                      </span>
                    </td>
                    <td className="px-4 py-2 text-right text-graphite-400 whitespace-nowrap">
                      {formatDuration(ev.duration_ms)}
                    </td>
                  </tr>
                );
              })
            )}
          </tbody>
        </table>
      </div>

      <div className="p-3 border-t border-graphite-800 bg-graphite-900/30 text-center">
        <Link
          to="/log"
          className="text-sm text-blue-400 hover:text-blue-300 transition-colors"
        >
          View full log &rarr;
        </Link>
      </div>
    </Card>
  );
}
