import { toast } from 'sonner';
import { useDeletePlugin, usePluginReferences } from '../../lib/queries';
import { ConfirmDialog, Skeleton } from '../ui/primitives';

export function PluginDeleteDialog({
  pendingDelete,
  onClose,
}: {
  pendingDelete: {
    id: string;
    revision: number;
    name: string;
    refcount: number;
  } | null;
  onClose: () => void;
}) {
  const del = useDeletePlugin();
  const refs = usePluginReferences(
    pendingDelete?.refcount ? pendingDelete.id : null,
  );

  return (
    <ConfirmDialog
      open={pendingDelete !== null}
      onOpenChange={(o) => {
        if (!o) onClose();
      }}
      title={
        <div className="flex flex-col">
          <span>
            {pendingDelete?.refcount
              ? 'Delete Plugin and Remove References?'
              : 'Delete Plugin?'}
          </span>
        </div>
      }
      description={
        pendingDelete ? (
          <span className="space-y-2 block">
            <span className="flex flex-col space-y-1">
              <span>
                <span className="font-mono">{pendingDelete.name}</span> will be
                permanently deleted.
              </span>
            </span>
            {pendingDelete.refcount > 0 && (
              <span className="text-xs bg-overlay-1 p-2 rounded-sm border border-subtle block mt-2">
                <span className="font-medium text-red-400 mb-1 block">
                  Used in {pendingDelete.refcount} location
                  {pendingDelete.refcount === 1 ? '' : 's'}.
                </span>
                {refs.isLoading ? (
                  <ul
                    data-testid="plugin-delete-reference-list"
                    className="space-y-1"
                    aria-hidden="true"
                  >
                    {Array.from({ length: pendingDelete.refcount }).map(
                      (_, i) => (
                        <li key={i}>
                          <Skeleton className="h-4 w-full" />
                        </li>
                      ),
                    )}
                  </ul>
                ) : refs.data ? (
                  <ul
                    data-testid="plugin-delete-reference-list"
                    className="list-disc pl-4 space-y-1"
                  >
                    {refs.data.references.map((r, i) => (
                      <li key={i}>
                        {r.kind === 'plugin_chain'
                          ? `Principal ${r.principal_name ?? r.principal_id} (${r.slot})`
                          : `Upstream ${r.upstream_name ?? r.upstream_id} warmup`}
                      </li>
                    ))}
                  </ul>
                ) : null}
                <span className="mt-2 flex flex-col space-y-1">
                  <span>
                    Deleting this plugin will also remove it from these
                    locations.
                  </span>
                </span>
              </span>
            )}
          </span>
        ) : null
      }
      confirmLabel={
        pendingDelete?.refcount ? 'Delete and remove uses' : 'Delete plugin'
      }
      destructive
      onConfirm={() => {
        if (!pendingDelete) return;
        if (pendingDelete.refcount > 0 && !refs.data?.reference_fingerprint) {
          toast.error('Reference preview is still loading');
          return;
        }
        del.mutate(
          {
            id: pendingDelete.id,
            revision: pendingDelete.revision,
            cascade: pendingDelete.refcount > 0,
            referenceFingerprint: refs.data?.reference_fingerprint,
          },
          {
            onSuccess: () => toast.success('Plugin deleted'),
            onError: (e) => toast.error(String(e)),
          },
        );
        onClose();
      }}
    />
  );
}
