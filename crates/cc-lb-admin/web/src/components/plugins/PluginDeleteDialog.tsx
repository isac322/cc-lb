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
      onConfirm={() => {
        if (!pendingDelete || del.isPending || fingerprintPending) return;
        del.mutate(
          {
            id: pendingDelete.id,
            revision: pendingDelete.revision,
            cascade,
            referenceFingerprint,
          },
          {
            onSuccess: () => {
              toast.success('Plugin deleted');
              onClose();
            },
          },
        );
      }}
    />
  );
}
