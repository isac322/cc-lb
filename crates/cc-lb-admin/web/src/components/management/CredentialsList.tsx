import { useState } from 'react';
import { useCredentials } from '../../lib/hooks/useCredentials';
import { formatRelativeTime } from '../../lib/time';
import { Button } from '../primitives/Button';
import { StatusChip } from '../primitives/StatusChip';
import { Table } from '../primitives/Table';
import { CredentialRevokeConfirm } from './CredentialRevokeConfirm';
import { CredentialRotateDialog } from './CredentialRotateDialog';

export function CredentialsList({ mock }: { mock?: boolean }) {
  const { credentials, refresh, isLoading, error } = useCredentials(mock);
  const [rotating, setRotating] = useState<{
    principalId: string;
    provider: string;
  } | null>(null);
  const [revoking, setRevoking] = useState<{
    principalId: string;
    provider: string;
  } | null>(null);

  if (isLoading) {
    return <div className="text-graphite-400">Loading credentials...</div>;
  }

  if (error) {
    return <div className="text-red-400">Error: {error.message}</div>;
  }

  return (
    <div className="space-y-4">
      <Table
        data={credentials}
        keyExtractor={(c) => `${c.principal_id}-${c.provider}`}
        columns={[
          {
            header: 'Principal',
            render: (c) => (
              <span className="font-mono text-graphite-100">
                {c.principal_id}
              </span>
            ),
          },
          {
            header: 'Provider',
            render: (c) => (
              <span className="text-graphite-300">{c.provider}</span>
            ),
          },
          {
            header: 'Kind',
            render: (c) => <span className="text-graphite-300">{c.kind}</span>,
          },
          {
            header: 'Identity',
            render: (c) => (
              <span className="text-graphite-300">{c.identity}</span>
            ),
          },
          {
            header: 'Associated',
            render: (c) => (
              <div className="flex flex-wrap gap-1">
                {c.associated_principals.map((p) => (
                  <span
                    key={p}
                    className="px-1.5 py-0.5 bg-graphite-800 rounded text-xs text-graphite-300"
                  >
                    {p}
                  </span>
                ))}
              </div>
            ),
          },
          {
            header: 'Status',
            render: (c) => {
              let variant: 'ok' | 'warn' | 'danger' | 'neutral' = 'neutral';
              if (c.status === 'valid') variant = 'ok';
              else if (c.status === 'expiring_soon') variant = 'warn';
              else if (c.status === 'expired' || c.status === 'revoked')
                variant = 'danger';

              return <StatusChip variant={variant}>{c.status}</StatusChip>;
            },
          },
          {
            header: 'Expires',
            render: (c) => (
              <span className="text-graphite-300 text-sm">
                {c.expires_at_unix_secs
                  ? formatRelativeTime(c.expires_at_unix_secs)
                  : '—'}
              </span>
            ),
          },
          {
            header: 'Actions',
            className: 'text-right',
            render: (c) => (
              <div className="flex justify-end gap-2">
                <Button
                  variant="secondary"
                  onClick={() =>
                    setRotating({
                      principalId: c.principal_id,
                      provider: c.provider,
                    })
                  }
                >
                  Rotate
                </Button>
                <Button
                  variant="secondary"
                  onClick={() =>
                    setRevoking({
                      principalId: c.principal_id,
                      provider: c.provider,
                    })
                  }
                >
                  Revoke
                </Button>
              </div>
            ),
          },
        ]}
      />

      {rotating && (
        <CredentialRotateDialog
          principalId={rotating.principalId}
          provider={rotating.provider}
          onClose={() => setRotating(null)}
          onSuccess={() => {
            refresh();
          }}
          mock={mock}
        />
      )}

      {revoking && (
        <CredentialRevokeConfirm
          principalId={revoking.principalId}
          provider={revoking.provider}
          onClose={() => setRevoking(null)}
          onSuccess={() => {
            setRevoking(null);
            refresh();
          }}
          mock={mock}
        />
      )}
    </div>
  );
}
