import { useCallback, useEffect, useState } from 'react';
import { Link, useSearchParams } from 'react-router';
import { CredentialsList } from '../components/management/CredentialsList';
import { PrincipalCreateDialog } from '../components/PrincipalCreateDialog';
import { Button } from '../components/primitives/Button';
import { Card } from '../components/primitives/Card';
import { Table } from '../components/primitives/Table';
import { ApiError } from '../lib/api';
import {
  useAllowedModels,
  useDelete,
  useDisable,
  useEnable,
  useList,
} from '../lib/hooks/usePrincipals';
import { ConflictError, type PrincipalResponse } from '../lib/types/v1';

function PrincipalStatusBadge({ enabled }: { enabled: boolean }) {
  return (
    <span
      className={`px-2 py-1 rounded text-xs font-medium ${enabled ? 'bg-green-900 text-green-300' : 'bg-red-900 text-red-300'}`}
    >
      {enabled ? 'Enabled' : 'Disabled'}
    </span>
  );
}

function AllowedModelsInlineEditor({
  principal,
  onSuccess,
}: {
  principal: PrincipalResponse;
  onSuccess: () => void;
}) {
  const [isEditing, setIsEditing] = useState(false);
  const [models, setModels] = useState(principal.allowed_models.join(', '));
  const [error, setError] = useState<string | null>(null);
  const updateAllowedModels = useAllowedModels();

  const handleSave = async () => {
    try {
      const parsed = models
        .split(',')
        .map((s) => s.trim())
        .filter(Boolean);
      await updateAllowedModels(principal.id, principal.revision, parsed);
      setIsEditing(false);
      onSuccess();
    } catch (err) {
      if (err instanceof ConflictError) {
        setError('Conflict: please refresh');
      } else {
        setError(err instanceof Error ? err.message : String(err));
      }
    }
  };

  if (isEditing) {
    return (
      <div className="flex items-center gap-2">
        <input
          type="text"
          value={models}
          onChange={(e) => setModels(e.target.value)}
          className="bg-graphite-900 border border-graphite-800 rounded px-2 py-1 text-sm text-graphite-50 w-48"
          autoFocus
          onKeyDown={(e) => {
            if (e.key === 'Enter') handleSave();
            if (e.key === 'Escape') setIsEditing(false);
          }}
        />
        <button
          onClick={handleSave}
          className="text-green-400 hover:text-green-300"
        >
          ✓
        </button>
        <button
          onClick={() => setIsEditing(false)}
          className="text-graphite-400 hover:text-graphite-300"
        >
          ✕
        </button>
        {error && <span className="text-red-400 text-xs">{error}</span>}
      </div>
    );
  }

  return (
    <div
      className="group cursor-pointer flex items-center gap-2"
      onClick={() => setIsEditing(true)}
      title={principal.allowed_models.join(', ')}
    >
      <span className="text-graphite-300">
        {principal.allowed_models.length} models
      </span>
      <span className="opacity-0 group-hover:opacity-100 text-cyan-400 text-xs">
        ✎
      </span>
    </div>
  );
}

export default function PrincipalManagement() {
  const [searchParams, setSearchParams] = useSearchParams();
  const mock = searchParams.get('mock') === '1';
  const urlTab = searchParams.get('tab') as 'principals' | 'credentials' | null;
  const [tab, setTab] = useState<'principals' | 'credentials'>(
    urlTab || 'principals',
  );

  const [isCreating, setIsCreating] = useState(false);
  const [editingPrincipal, setEditingPrincipal] =
    useState<PrincipalResponse | null>(null);
  const [deleteError, setDeleteError] = useState<{
    id: string;
    references: { kind: string; id: string }[];
  } | null>(null);

  const listPrincipals = useList();
  const deletePrincipal = useDelete();
  const enablePrincipal = useEnable();
  const disablePrincipal = useDisable();

  const [principals, setPrincipals] = useState<PrincipalResponse[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    setIsLoading(true);
    try {
      const res = await listPrincipals();
      setPrincipals(res.principals);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setIsLoading(false);
    }
  }, [listPrincipals]);

  useEffect(() => {
    load();
  }, [load]);

  useEffect(() => {
    if (urlTab && urlTab !== tab) {
      setTab(urlTab);
    }
  }, [urlTab, tab]);

  const handleTabChange = (newTab: 'principals' | 'credentials') => {
    setTab(newTab);
    const newParams = new URLSearchParams(searchParams);
    newParams.set('tab', newTab);
    setSearchParams(newParams);
  };

  const handleDelete = async (p: PrincipalResponse) => {
    if (!confirm(`Delete principal ${p.name}?`)) return;
    try {
      await deletePrincipal(p.id, p.revision);
      load();
    } catch (err: unknown) {
      if (
        err instanceof ApiError &&
        err.status === 409 &&
        err.body &&
        typeof err.body === 'object' &&
        'error' in err.body &&
        err.body.error === 'referenced_by'
      ) {
        setDeleteError({
          id: p.id,
          references: (
            err.body as unknown as {
              references: { kind: string; id: string }[];
            }
          ).references,
        });
      } else {
        alert(err instanceof Error ? err.message : String(err));
      }
    }
  };

  const handleToggleEnable = async (p: PrincipalResponse) => {
    try {
      if (p.enabled) {
        await disablePrincipal(p.id, p.revision);
      } else {
        await enablePrincipal(p.id, p.revision);
      }
      load();
    } catch (err) {
      alert(err instanceof Error ? err.message : String(err));
    }
  };

  return (
    <div className="space-y-6 pb-16">
      <div className="flex items-center justify-between">
        <div className="flex gap-4 border-b border-graphite-800 w-full">
          <button
            className={`pb-2 px-1 text-sm font-medium transition-colors ${
              tab === 'principals'
                ? 'text-cyan-400 border-b-2 border-cyan-400'
                : 'text-graphite-400 hover:text-graphite-200'
            }`}
            onClick={() => handleTabChange('principals')}
          >
            Principals
          </button>
          <button
            className={`pb-2 px-1 text-sm font-medium transition-colors ${
              tab === 'credentials'
                ? 'text-cyan-400 border-b-2 border-cyan-400'
                : 'text-graphite-400 hover:text-graphite-200'
            }`}
            onClick={() => handleTabChange('credentials')}
          >
            Credentials
          </button>
        </div>
      </div>

      {tab === 'principals' && (
        <Card className="p-6">
          <div className="flex items-center justify-between mb-6">
            <h2 className="text-base font-semibold text-graphite-50">Roster</h2>
            <Button variant="primary" onClick={() => setIsCreating(true)}>
              + New principal
            </Button>
          </div>

          {deleteError && (
            <div className="mb-4 p-4 bg-red-900/50 border border-red-800 rounded">
              <h3 className="text-red-200 font-medium mb-2">
                Cannot delete principal (referenced by plugin chains)
              </h3>
              <ul className="list-disc list-inside text-red-300 text-sm">
                {deleteError.references.map((ref, i) => (
                  <li key={i}>
                    {ref.kind}:{' '}
                    <Link
                      to={`/admin/registry?highlight=${ref.id}`}
                      className="underline"
                    >
                      {ref.id}
                    </Link>
                  </li>
                ))}
              </ul>
              <Button
                variant="secondary"
                className="mt-3"
                onClick={() => setDeleteError(null)}
              >
                Dismiss
              </Button>
            </div>
          )}

          {isLoading ? (
            <div className="text-graphite-400">Loading principals...</div>
          ) : error ? (
            <div className="text-red-400">Error: {error}</div>
          ) : (
            <Table
              data={principals}
              keyExtractor={(p) => p.id}
              columns={[
                {
                  header: 'ID',
                  render: (p) => (
                    <span className="font-mono text-graphite-100 text-xs">
                      {p.id}
                    </span>
                  ),
                },
                {
                  header: 'Name',
                  render: (p) => (
                    <span className="font-medium text-graphite-100">
                      {p.name}
                    </span>
                  ),
                },
                {
                  header: 'Kind',
                  render: (p) => (
                    <span className="text-graphite-300 capitalize">
                      {p.kind}
                    </span>
                  ),
                },
                {
                  header: 'Allowed Models',
                  render: (p) => (
                    <AllowedModelsInlineEditor principal={p} onSuccess={load} />
                  ),
                },
                {
                  header: 'Default Limits',
                  render: (p) => (
                    <span className="text-graphite-300 text-sm">
                      {p.default_limits.length > 0
                        ? p.default_limits
                            .map((l) => `${l.limit} ${l.kind}/${l.window}`)
                            .join(', ')
                        : 'None'}
                    </span>
                  ),
                },
                {
                  header: 'Status',
                  render: (p) => <PrincipalStatusBadge enabled={p.enabled} />,
                },
                {
                  header: 'Revision',
                  render: (p) => (
                    <span className="text-graphite-400 font-mono text-xs">
                      {p.revision}
                    </span>
                  ),
                },
                {
                  header: 'Actions',
                  className: 'text-right',
                  render: (p) => (
                    <div className="flex items-center justify-end gap-2">
                      <Button
                        variant="secondary"
                        onClick={() => handleToggleEnable(p)}
                      >
                        {p.enabled ? 'Disable' : 'Enable'}
                      </Button>
                      <Button
                        variant="secondary"
                        onClick={() => setEditingPrincipal(p)}
                      >
                        Edit
                      </Button>
                      <Button
                        variant="secondary"
                        onClick={() => handleDelete(p)}
                      >
                        Delete
                      </Button>
                    </div>
                  ),
                },
              ]}
            />
          )}
        </Card>
      )}

      {tab === 'credentials' && (
        <Card className="p-6">
          <div className="mb-6">
            <h2 className="text-base font-semibold text-graphite-50">
              Active credentials
            </h2>
          </div>
          <CredentialsList mock={mock} />
        </Card>
      )}

      {(isCreating || editingPrincipal) && (
        <PrincipalCreateDialog
          principal={editingPrincipal || undefined}
          onClose={() => {
            setIsCreating(false);
            setEditingPrincipal(null);
          }}
          onSuccess={() => {
            setIsCreating(false);
            setEditingPrincipal(null);
            load();
          }}
        />
      )}
    </div>
  );
}
