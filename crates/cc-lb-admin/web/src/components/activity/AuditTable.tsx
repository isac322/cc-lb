import type { AuditEntry } from '../../lib/api';
import { AuditRow } from './AuditRow';

interface AuditTableProps {
  events: AuditEntry[];
}

export function AuditTable({ events }: AuditTableProps) {
  return (
    <div className="w-full overflow-x-auto border border-graphite-800 rounded-lg bg-graphite-850">
      <table className="w-full text-sm text-left">
        <thead className="text-xs text-graphite-400 uppercase bg-graphite-900 border-b border-graphite-800 sticky top-0">
          <tr>
            <th className="px-4 py-3 font-medium w-48">Time</th>
            <th className="px-4 py-3 font-medium w-48">Action</th>
            <th className="px-4 py-3 font-medium w-32">Actor</th>
            <th className="px-4 py-3 font-medium">Payload</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-graphite-800">
          {events.map((event) => (
            <AuditRow key={`${event.ts}-${event.request_id}`} event={event} />
          ))}
          {events.length === 0 && (
            <tr>
              <td
                colSpan={4}
                className="px-4 py-8 text-center text-graphite-500"
              >
                No data available
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
}
