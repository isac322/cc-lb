import { MergedCredentialRow } from '../../lib/hooks/useCredentialStatus';
import { StatusChip } from '../primitives/StatusChip';
import { Tooltip } from '../primitives/Tooltip';
import { formatRelativeTime } from '../../lib/time';
import { CredentialStatusBadge } from './CredentialStatusBadge';
import { ActionNeededAlert } from './ActionNeededAlert';

interface CredentialRowProps {
  row: MergedCredentialRow;
}

export function CredentialRow({ row }: CredentialRowProps) {
  const expiresAbsolute = row.expires_at_unix_secs 
    ? new Date(row.expires_at_unix_secs * 1000).toLocaleString() 
    : null;

  return (
    <tr>
      <td className="whitespace-nowrap">
        <div className="flex flex-col">
          <span className="text-sm font-mono text-graphite-50 truncate max-w-[200px]" title={row.identity}>
            {row.identity}
          </span>
          <div className="flex items-center gap-2 mt-1">
            <StatusChip variant="neutral">{row.kind}</StatusChip>
            <span className="text-xs text-graphite-400">
              {row.associated_principals.length} principal{row.associated_principals.length !== 1 ? 's' : ''}
            </span>
          </div>
        </div>
      </td>
      <td className="whitespace-nowrap">
        <div className="flex flex-col items-start gap-1">
          <CredentialStatusBadge status={row.status} />
          <ActionNeededAlert status={row.status} />
        </div>
      </td>
      <td className="whitespace-nowrap">
        {row.expires_at_unix_secs ? (
          <Tooltip content={expiresAbsolute || ''}>
            <span className="text-sm text-graphite-50 cursor-help border-b border-dotted border-graphite-700">
              {formatRelativeTime(row.expires_at_unix_secs)}
            </span>
          </Tooltip>
        ) : (
          <span className="text-sm text-gray-400 italic">never</span>
        )}
      </td>
      <td className="whitespace-nowrap">
        {row.kind === 'oauth' ? (
          <StatusChip variant={row.refresh_token_present ? 'ok' : 'neutral'}>
            {row.refresh_token_present ? 'yes' : 'no'}
          </StatusChip>
        ) : (
          <span className="text-sm text-gray-400 italic">n/a</span>
        )}
      </td>
      <td className="whitespace-nowrap">
        {row.last_updated_unix_secs ? (
          <span className="text-sm text-graphite-50">
            {formatRelativeTime(row.last_updated_unix_secs)}
          </span>
        ) : (
          <span className="text-sm text-gray-400 italic">unknown</span>
        )}
      </td>
    </tr>
  );
}
