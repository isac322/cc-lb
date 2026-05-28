import { Edit2, Power, PowerOff, RefreshCw, Trash2 } from 'lucide-react';
import { useCallback, useEffect, useState } from 'react';
import { Button } from '../components/primitives/Button';
import { EmptyState } from '../components/primitives/EmptyState';
import { ErrorState } from '../components/primitives/ErrorState';
import { LoadingState } from '../components/primitives/LoadingState';
import { Modal } from '../components/primitives/Modal';
import { StatusBadge } from '../components/StatusBadge';
import { UpstreamCreateDialog } from '../components/UpstreamCreateDialog';
import { useStatus } from '../lib/hooks/useStatusOverview';
import {
  useDelete,
  useDisable,
  useEnable,
  useList,
  useUpdate,
} from '../lib/hooks/useUpstreams';
import { ConflictError, type UpstreamResponse } from '../lib/types/v1';

export default function Upstreams() {
  const [isCreating, setIsCreating] = useState(false);
  const [editingUpstream, setEditingUpstream] =
    useState<UpstreamResponse | null>(null);
  const [deletingUpstream, setDeletingUpstream] =
    useState<UpstreamResponse | null>(null);
  const [conflictError, setConflictError] = useState<{
    message: string;
    latest: UpstreamResponse;
  } | null>(null);

  const [upstreams, setUpstreams] = useState<UpstreamResponse[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  const listUpstreams = useList();
  const { upstreams: statusUpstreams } = useStatus();
  const enableUpstream = useEnable();
  const disableUpstream = useDisable();
  const deleteUpstream = useDelete();
  const updateUpstream = useUpdate();

  const fetchUpstreams = useCallback(async () => {
    setIsLoading(true);
    try {
      const res = await listUpstreams();
      setUpstreams(res.upstreams);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err : new Error(String(err)));
    } finally {
      setIsLoading(false);
    }
  }, [listUpstreams]);

  useEffect(() => {
    fetchUpstreams();
  }, [fetchUpstreams]);

  const handleEnable = async (upstream: UpstreamResponse) => {
    try {
      await enableUpstream(upstream.id, upstream.revision);
      fetchUpstreams();
    } catch (err) {
      if (err instanceof ConflictError) {
        setConflictError({
          message: err.message,
          latest: err.latest as UpstreamResponse,
        });
      } else {
        alert(err instanceof Error ? err.message : String(err));
      }
    }
  };

  const handleDisable = async (upstream: UpstreamResponse) => {
    try {
      await disableUpstream(upstream.id, upstream.revision);
      fetchUpstreams();
    } catch (err) {
      if (err instanceof ConflictError) {
        setConflictError({
          message: err.message,
          latest: err.latest as UpstreamResponse,
        });
      } else {
        alert(err instanceof Error ? err.message : String(err));
      }
    }
  };

  const handleDelete = async () => {
    if (!deletingUpstream) return;
    try {
      await deleteUpstream(deletingUpstream.id, deletingUpstream.revision);
      setDeletingUpstream(null);
      fetchUpstreams();
    } catch (err) {
      if (err instanceof ConflictError) {
        setDeletingUpstream(null);
        setConflictError({
          message: err.message,
          latest: err.latest as UpstreamResponse,
        });
      } else if (
        err instanceof Error &&
        err.message.includes('referenced_by')
      ) {
        try {
          alert(err.message);
        } catch (_e) {
          alert(err.message);
        }
      } else {
        alert(err instanceof Error ? err.message : String(err));
      }
    }
  };

  const handleEditSubmit = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    if (!editingUpstream) return;

    const formData = new FormData(e.currentTarget);
    const name = formData.get('name') as string;
    const baseUrl = formData.get('base_url') as string;
    const apiKeyEnv = formData.get('api_key_env') as string;

    try {
      await updateUpstream(editingUpstream.id, editingUpstream.revision, {
        name,
        ...(baseUrl ? { base_url: baseUrl } : {}),
        ...(apiKeyEnv ? { api_key_env: apiKeyEnv } : {}),
      });
      setEditingUpstream(null);
      fetchUpstreams();
    } catch (err) {
      if (err instanceof ConflictError) {
        setEditingUpstream(null);
        setConflictError({
          message: err.message,
          latest: err.latest as UpstreamResponse,
        });
      } else {
        alert(err instanceof Error ? err.message : String(err));
      }
    }
  };

  if (isLoading && upstreams.length === 0) {
    return <LoadingState message="Loading upstreams..." />;
  }

  if (error && upstreams.length === 0) {
    return <ErrorState message={error.message} onRetry={fetchUpstreams} />;
  }

  return (
    <div className="space-y-6">
      <div className="flex justify-between items-center">
        <div>
          <p className="text-sm text-graphite-400">
            Manage upstream providers and their credentials.
          </p>
        </div>
        <div className="flex gap-2">
          <Button
            variant="secondary"
            onClick={fetchUpstreams}
            disabled={isLoading}
          >
            <RefreshCw
              className={`w-4 h-4 mr-2 ${isLoading ? 'animate-spin' : ''}`}
            />
            Refresh
          </Button>
          <Button variant="primary" onClick={() => setIsCreating(true)}>
            + New Upstream
          </Button>
        </div>
      </div>

      {upstreams.length === 0 ? (
        <EmptyState
          title="No upstreams configured"
          message="Create an upstream to get started."
        />
      ) : (
        <div className="bg-graphite-900 border border-graphite-800 rounded-lg overflow-hidden">
          <table className="w-full text-left text-sm">
            <thead className="bg-graphite-800/50 text-graphite-300">
              <tr>
                <th className="px-4 py-3 font-medium">Name</th>
                <th className="px-4 py-3 font-medium">Kind</th>
                <th className="px-4 py-3 font-medium">Status</th>
                <th className="px-4 py-3 font-medium">OAuth Expires</th>
                <th className="px-4 py-3 font-medium">Created</th>
                <th className="px-4 py-3 font-medium">Revision</th>
                <th className="px-4 py-3 font-medium text-right">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-graphite-800">
              {upstreams.map((upstream) => {
                const status = statusUpstreams.find(
                  (s) => s.id === upstream.id,
                );
                const statusLabel =
                  status?.status || (upstream.enabled ? 'active' : 'disabled');

                return (
                  <tr
                    key={upstream.id}
                    className="hover:bg-graphite-800/30 transition-colors"
                  >
                    <td className="px-4 py-3 font-medium text-graphite-100">
                      {upstream.name}
                    </td>
                    <td className="px-4 py-3 text-graphite-300">
                      {upstream.kind}
                    </td>
                    <td className="px-4 py-3">
                      <StatusBadge
                        status={statusLabel as 'active' | 'disabled' | 'error'}
                        lastApplyError={status?.last_apply_error}
                        lastApplyAt={
                          status?.last_apply_at_unix_secs
                            ? new Date(
                                status.last_apply_at_unix_secs * 1000,
                              ).toISOString()
                            : null
                        }
                      />
                    </td>
                    <td className="px-4 py-3 text-graphite-400">-</td>
                    <td className="px-4 py-3 text-graphite-400">-</td>
                    <td className="px-4 py-3 text-graphite-400">
                      {upstream.revision}
                    </td>
                    <td className="px-4 py-3 text-right">
                      <div className="flex items-center justify-end gap-2">
                        {upstream.enabled ? (
                          <Button
                            variant="secondary"
                            onClick={() => handleDisable(upstream)}
                            title="Disable"
                          >
                            <PowerOff className="w-4 h-4" />
                          </Button>
                        ) : (
                          <Button
                            variant="secondary"
                            onClick={() => handleEnable(upstream)}
                            title="Enable"
                          >
                            <Power className="w-4 h-4" />
                          </Button>
                        )}
                        <Button
                          variant="secondary"
                          onClick={() => setEditingUpstream(upstream)}
                          title="Edit"
                        >
                          <Edit2 className="w-4 h-4" />
                        </Button>
                        <Button
                          variant="secondary"
                          onClick={() => setDeletingUpstream(upstream)}
                          title="Delete"
                          className="text-red-400 hover:text-red-300"
                        >
                          <Trash2 className="w-4 h-4" />
                        </Button>
                      </div>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}

      {isCreating && (
        <UpstreamCreateDialog
          onClose={() => setIsCreating(false)}
          onSuccess={() => {
            setIsCreating(false);
            fetchUpstreams();
          }}
        />
      )}

      {editingUpstream && (
        <Modal
          isOpen={true}
          onClose={() => setEditingUpstream(null)}
          title="Edit Upstream"
        >
          <p className="text-sm text-graphite-300 mb-4">
            Update configuration for {editingUpstream.name}.
          </p>
          <form onSubmit={handleEditSubmit}>
            <div className="space-y-4 py-4">
              <div className="space-y-2">
                <label htmlFor="name" className="text-sm font-medium text-graphite-200">
                  Name
                </label>
                <input
                  id="name"
                  className="w-full bg-graphite-900 border border-graphite-700 rounded px-3 py-2 text-sm text-graphite-100 focus:outline-none focus:border-cyan-500"
                  name="name"
                  defaultValue={editingUpstream.name}
                  required
                />
              </div>
              <div className="space-y-2">
                <label htmlFor="base_url" className="text-sm font-medium text-graphite-200">
                  Base URL
                </label>
                <input
                  id="base_url"
                  className="w-full bg-graphite-900 border border-graphite-700 rounded px-3 py-2 text-sm text-graphite-100 focus:outline-none focus:border-cyan-500"
                  name="base_url"
                  placeholder="Optional"
                />
              </div>
              {editingUpstream.kind === 'anthropic_api_key' && (
                <div className="space-y-2">
                  <label className="text-sm font-medium text-graphite-200">
                    API Key Env Var
                  </label>
                  <input
                    className="w-full bg-graphite-900 border border-graphite-700 rounded px-3 py-2 text-sm text-graphite-100 focus:outline-none focus:border-cyan-500"
                    name="api_key_env"
                    placeholder="Optional"
                  />
                </div>
              )}
            </div>
            <div className="flex justify-end gap-2 mt-4">
              <Button
                type="button"
                variant="secondary"
                onClick={() => setEditingUpstream(null)}
              >
                Cancel
              </Button>
              <Button type="submit" variant="primary">
                Save Changes
              </Button>
            </div>
          </form>
        </Modal>
      )}

      {deletingUpstream && (
        <Modal
          isOpen={true}
          onClose={() => setDeletingUpstream(null)}
          title="Delete Upstream"
        >
          <p className="text-sm text-graphite-300 mb-4">
            Are you sure you want to delete {deletingUpstream.name}? This action
            cannot be undone.
          </p>
          <div className="flex justify-end gap-2 mt-4">
            <Button
              variant="secondary"
              onClick={() => setDeletingUpstream(null)}
            >
              Cancel
            </Button>
            <Button
              variant="primary"
              onClick={handleDelete}
              className="bg-red-500 hover:bg-red-600 text-white border-transparent"
            >
              Delete
            </Button>
          </div>
        </Modal>
      )}

      {conflictError && (
        <Modal
          isOpen={true}
          onClose={() => setConflictError(null)}
          title="Conflict Detected"
        >
          <p className="text-sm text-graphite-300 mb-4">
            Someone else just changed this upstream.
          </p>
          <div className="py-4">
            <p className="text-sm text-graphite-300 mb-4">
              The current revision is {conflictError.latest.revision}. Would you
              like to load the latest data?
            </p>
          </div>
          <div className="flex justify-end gap-2 mt-4">
            <Button variant="secondary" onClick={() => setConflictError(null)}>
              Cancel
            </Button>
            <Button
              variant="primary"
              onClick={() => {
                setConflictError(null);
                fetchUpstreams();
              }}
            >
              Load Latest
            </Button>
          </div>
        </Modal>
      )}
    </div>
  );
}
