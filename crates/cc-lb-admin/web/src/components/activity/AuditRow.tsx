import { AuditEntry } from '../../lib/api';
import { StatusChip } from '../primitives/StatusChip';
import { Tooltip } from '../primitives/Tooltip';
import { formatRelativeTime } from '../../lib/time';
import { AuditPayloadView } from './AuditPayloadView';

interface AuditRowProps {
  event: AuditEntry;
}

function getKindColor(kind: string | undefined): 'ok' | 'warn' | 'danger' | 'neutral' {
  if (!kind) return 'neutral';
  if (kind.includes('create') || kind.includes('issue') || kind.includes('complete')) return 'ok';
  if (kind.includes('revoke') || kind.includes('disable') || kind.includes('killswitch')) return 'danger';
  if (kind.includes('update') || kind.includes('rotate') || kind.includes('override')) return 'warn';
  return 'neutral';
}

export function AuditRow({ event }: AuditRowProps) {
  const date = new Date(event.ts * 1000);
  const absoluteTime = date.toLocaleString();
  const relativeTime = formatRelativeTime(event.ts);

  return (
    <tr>
      <td className="whitespace-nowrap">
        <Tooltip content={absoluteTime}>
          <span className="text-sm text-graphite-400 cursor-help border-b border-dotted border-graphite-700">
            {relativeTime}
          </span>
        </Tooltip>
      </td>
      <td className="whitespace-nowrap">
        <StatusChip variant={getKindColor(event.kind)}>
          {event.kind || 'unknown'}
        </StatusChip>
      </td>
      <td className="whitespace-nowrap">
        <span className="text-sm font-mono text-graphite-50">
          {event.principal_id}
        </span>
      </td>
      <td className="w-full">
        {event.payload ? (
          <AuditPayloadView payload={event.payload} />
        ) : (
          <span className="text-sm text-gray-400 italic">none</span>
        )}
      </td>
    </tr>
  );
}
