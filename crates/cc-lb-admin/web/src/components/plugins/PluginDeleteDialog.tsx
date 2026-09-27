import { toast } from 'sonner';
import { useDeletePlugin, usePluginReferences } from '../../lib/queries';
import { ConfirmDialog, Skeleton } from '../ui/primitives';

export function PluginDeleteDialog({
  pendingDelete,
  onClose,
  onDeleted,
}: {
  pendingDelete: {
    id: string;
    revision: number;
    name: string;
    refcount: number;
  } | null;
  onClose: () => void;
  onDeleted?: () => void;
}) {
  const del = useDeletePlugin();
  const refs = usePluginReferences(
    pendingDelete?.refcount ? pendingDelete.id : null,
  );
  const cascade = (pendingDelete?.refcount ?? 0) > 0;
  // A cascading delete is only accepted together with the fingerprint the
  // server uses to reject the request when the reference set moved underneath
  // us, so the destructive confirm stays inert until that fingerprint is in
  // hand.
  const referenceFingerprint = refs.data?.reference_fingerprint;
  const fingerprintPending = cascade && !referenceFingerprint;

  return (
    <ConfirmDialog
      open={pendingDelete !== null}
      onOpenChange={(o) => {
        if (!o) onClose();
      }}
      title={
        pendingDelete?.refcount
          ? 'Delete plugin and remove references?'
          : 'Delete plugin?'
      }
      description={
        pendingDelete ? (
          <span className="block space-y-3">
            <span className="block">
              <span className="font-medium text-text">
                {pendingDelete.name}
              </span>{' '}
              will be permanently deleted.
            </span>
            {pendingDelete.refcount > 0 && (
              <span className="well block p-3 text-body">
                <span className="mb-1 block font-medium text-danger-text">
                  Used in {pendingDelete.refcount} location
                  {pendingDelete.refcount === 1 ? '' : 's'}.
                </span>
                {refs.data ? (
                  <span
                    role="list"
                    data-testid="plugin-delete-reference-list"
                    className="list-disc pl-4 space-y-1 block"
                  >
                    {refs.data.references.map((r, i) => (
                      <span key={i} role="listitem" className="list-item">
                        {r.kind === 'plugin_chain'
                          ? `Principal ${r.principal_name ?? r.principal_id} (${r.slot})`
                          : `Upstream ${r.upstream_name ?? r.upstream_id} warmup`}
                      </span>
                    ))}
                  </span>
                ) : refs.isError ? (
                  <span className="block text-text-muted">
                    Reference details could not be loaded. Close and reopen to
                    retry — the delete needs them to detect concurrent edits.
                  </span>
                ) : (
                  <span
                    role="list"
                    data-testid="plugin-delete-reference-list"
                    className="space-y-1 block"
                    aria-hidden="true"
                  >
                    {Array.from({ length: pendingDelete.refcount }).map(
                      (_, i) => (
                        <span key={i} role="listitem" className="block">
                          <Skeleton as="span" className="block h-4 w-full" />
                        </span>
                      ),
                    )}
                  </span>
                )}
                <span className="mt-2 block">
                  Deleting this plugin will also remove it from these locations.
                </span>
              </span>
            )}
          </span>
        ) : null
      }
      confirmLabel={
        del.isPending
          ? 'Deleting...'
          : cascade
            ? 'Delete and remove uses'
            : 'Delete plugin'
      }
      destructive
      confirmDisabled={fingerprintPending}
      pending={del.isPending}
      closeOnConfirm={false}
      onConfirm={async () => {
        if (!pendingDelete || del.isPending || fingerprintPending) return;
        try {
          await del.mutateAsync({
            id: pendingDelete.id,
            revision: pendingDelete.revision,
            cascade,
            referenceFingerprint,
          });
        } catch {
          // The shared MutationCache owns the error toast.
          return;
        }
        toast.success('Plugin deleted');
        if (onDeleted) {
          onDeleted();
        } else {
          onClose();
        }
      }}
    />
  );
}
