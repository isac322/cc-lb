import { useState } from 'react';
import { Modal } from '../primitives/Modal';
import { Button } from '../primitives/Button';
import { FormField } from '../primitives/FormField';
import { PrincipalWithId } from '../../lib/hooks/usePrincipalsManagement';
import { useDraftPrincipals } from '../../lib/hooks/useDraftPrincipals';
import { AllowedModelsEditor } from './AllowedModelsEditor';
import { QuotaOverrideForm } from './QuotaOverrideForm';

export function PrincipalEditForm({
  principal,
  onClose,
  onSuccess,
  mock,
}: {
  principal?: PrincipalWithId;
  onClose: () => void;
  onSuccess: () => void;
  mock?: boolean;
}) {
  const isEdit = !!principal;
  const { createPrincipal, updatePrincipal } = useDraftPrincipals(mock);
  
  const [id, setId] = useState(principal?.id || '');
  const [allowedModels, setAllowedModels] = useState<string[]>(principal?.allowed_models || []);
  const [isPending, setIsPending] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setIsPending(true);
    setError(null);
    try {
      const spec = {
        allowed_models: allowedModels,
        ...(principal?.quotas ? { quotas: principal.quotas } : {}),
        ...(principal?.disabled !== undefined ? { disabled: principal.disabled } : {}),
        ...(principal?.credentials_ref ? { credentials_ref: principal.credentials_ref } : {}),
      };

      if (isEdit) {
        await updatePrincipal(id, spec);
      } else {
        await createPrincipal(id, spec);
      }
      onSuccess();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setIsPending(false);
    }
  };

  return (
    <Modal isOpen onClose={onClose} title={isEdit ? 'Edit Principal' : 'New Principal'}>
      <form onSubmit={handleSubmit} className="space-y-4">
        <FormField label="ID" error={undefined}>
          <input
            type="text"
            value={id}
            onChange={(e) => setId(e.target.value)}
            disabled={isEdit}
            className="w-full bg-graphite-900 border border-graphite-800 rounded px-3 py-2 text-graphite-50 focus:outline-none focus:border-cyan-500 disabled:opacity-50"
            required
          />
        </FormField>

        <div className="space-y-2">
          <label className="block text-sm font-medium text-graphite-300">Allowed Models</label>
          <AllowedModelsEditor models={allowedModels} onChange={setAllowedModels} />
        </div>

        {isEdit && (
          <div className="pt-4 border-t border-graphite-800">
            <h3 className="text-sm font-medium text-graphite-300 mb-2">Live Quota Override</h3>
            <QuotaOverrideForm principalId={id} mock={mock} />
          </div>
        )}

        {error && <div className="text-red-400 text-sm">{error}</div>}

        <div className="pt-4 flex justify-end gap-2">
          <Button type="button" variant="secondary" onClick={onClose}>
            Cancel
          </Button>
          <Button type="submit" variant="primary" disabled={isPending || !id}>
            {isPending ? 'Saving...' : 'Save to Draft'}
          </Button>
        </div>
      </form>
    </Modal>
  );
}
