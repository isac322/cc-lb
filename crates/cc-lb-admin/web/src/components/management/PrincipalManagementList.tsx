import { useState } from 'react';
import type { PrincipalWithId } from '../../lib/hooks/usePrincipalsManagement';
import { Button } from '../primitives/Button';
import { StatusChip } from '../primitives/StatusChip';
import { Table } from '../primitives/Table';
import { EnableDisableSwitch } from './EnableDisableSwitch';
import { KeyManagementPanel } from './KeyManagementPanel';
import { PrincipalEditForm } from './PrincipalEditForm';

export function PrincipalManagementList({
  principals,
  onRefresh,
  mock,
}: {
  principals: PrincipalWithId[];
  onRefresh: () => void;
  mock?: boolean;
}) {
  const [editingPrincipal, setEditingPrincipal] =
    useState<PrincipalWithId | null>(null);
  const [managingKeysFor, setManagingKeysFor] = useState<string | null>(null);

  return (
    <div className="space-y-4">
      <Table
        data={principals}
        keyExtractor={(p) => p.id}
        columns={[
          {
            header: 'ID',
            render: (p) => (
              <span className="font-mono text-graphite-100">{p.id}</span>
            ),
          },
          {
            header: 'Status',
            render: (p) => (
              <StatusChip variant={p.disabled ? 'danger' : 'ok'}>
                {p.disabled ? 'Disabled' : 'Enabled'}
              </StatusChip>
            ),
          },
          {
            header: 'Quota',
            render: (p) => {
              if (!p.quotas)
                return <span className="text-graphite-500">Default</span>;
              return (
                <span className="text-graphite-300 text-xs">
                  {p.quotas.default_requests_per_window} req /{' '}
                  {p.quotas.default_window_secs}s
                </span>
              );
            },
          },
          {
            header: 'Allowed Models',
            render: (p) => (
              <span className="text-graphite-300">
                {p.allowed_models?.length || 0} models
              </span>
            ),
          },
          {
            header: 'Actions',
            className: 'text-right',
            render: (p) => (
              <div className="flex items-center justify-end gap-2">
                <EnableDisableSwitch
                  principal={p}
                  onRefresh={onRefresh}
                  mock={mock}
                />
                <Button
                  variant="secondary"
                  onClick={() => setEditingPrincipal(p)}
                >
                  Edit
                </Button>
                <Button
                  variant="secondary"
                  onClick={() => setManagingKeysFor(p.id)}
                >
                  Keys
                </Button>
              </div>
            ),
          },
        ]}
      />

      {editingPrincipal && (
        <PrincipalEditForm
          principal={editingPrincipal}
          onClose={() => setEditingPrincipal(null)}
          onSuccess={() => {
            setEditingPrincipal(null);
            onRefresh();
          }}
          mock={mock}
        />
      )}

      {managingKeysFor && (
        <KeyManagementPanel
          principalId={managingKeysFor}
          onClose={() => setManagingKeysFor(null)}
          mock={mock}
        />
      )}
    </div>
  );
}
