import { useState } from 'react';
import { usePrincipalKeys } from '../../lib/hooks/usePrincipalKeys';
import { formatRelativeTime } from '../../lib/time';
import { Button } from '../primitives/Button';
import { Modal } from '../primitives/Modal';
import { Table } from '../primitives/Table';
import { IssueKeyDialog } from './IssueKeyDialog';
import { RevokeKeyConfirm } from './RevokeKeyConfirm';

export function KeyManagementPanel({
  principalId,
  onClose,
  mock,
}: {
  principalId: string;
  onClose: () => void;
  mock?: boolean;
}) {
  const { keys, refresh, error } = usePrincipalKeys(principalId, mock);
  const [isIssuing, setIsIssuing] = useState(false);
  const [revokingKey, setRevokingKey] = useState<string | null>(null);

  return (
    <Modal isOpen onClose={onClose} title={`Keys for ${principalId}`}>
      <div className="space-y-4">
        {error && <div className="text-red-400 text-sm">{error.message}</div>}

        <div className="flex justify-end">
          <Button variant="primary" onClick={() => setIsIssuing(true)}>
            Issue New Key
          </Button>
        </div>

        <Table
          data={keys}
          keyExtractor={(k) => k.key_id}
          columns={[
            {
              header: 'Label',
              render: (k) => (
                <span className="text-graphite-100">{k.label || '—'}</span>
              ),
            },
            {
              header: 'Issued',
              render: (k) => (
                <span className="text-graphite-300 text-sm">
                  {formatRelativeTime(k.issued_at_unix_secs)}
                </span>
              ),
            },
            {
              header: 'Revoked',
              render: (k) => (
                <span className="text-graphite-300 text-sm">
                  {k.revoked_at_unix_secs
                    ? formatRelativeTime(k.revoked_at_unix_secs)
                    : '—'}
                </span>
              ),
            },
            {
              header: 'Actions',
              className: 'text-right',
              render: (k) => (
                <div className="flex justify-end">
                  {!k.revoked_at_unix_secs && (
                    <Button
                      variant="secondary"
                      onClick={() => setRevokingKey(k.key_id)}
                    >
                      Revoke
                    </Button>
                  )}
                </div>
              ),
            },
          ]}
        />

        {isIssuing && (
          <IssueKeyDialog
            principalId={principalId}
            onClose={() => setIsIssuing(false)}
            onSuccess={() => {
              refresh();
            }}
            mock={mock}
          />
        )}

        {revokingKey && (
          <RevokeKeyConfirm
            principalId={principalId}
            keyId={revokingKey}
            onClose={() => setRevokingKey(null)}
            onSuccess={() => {
              setRevokingKey(null);
              refresh();
            }}
            mock={mock}
          />
        )}
      </div>
    </Modal>
  );
}
