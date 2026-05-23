import { useState } from 'react';
import { useCredentials } from '../../lib/hooks/useCredentials';
import { Button } from '../primitives/Button';
import { Modal } from '../primitives/Modal';

export function CredentialRevokeConfirm({
  principalId,
  provider,
  onClose,
  onSuccess,
  mock,
}: {
  principalId: string;
  provider: string;
  onClose: () => void;
  onSuccess: () => void;
  mock?: boolean;
}) {
  const { revokeCredential } = useCredentials(mock);
  const [isPending, setIsPending] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleRevoke = async () => {
    setIsPending(true);
    setError(null);
    try {
      await revokeCredential(principalId, provider);
      onSuccess();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setIsPending(false);
    }
  };

  return (
    <Modal isOpen onClose={onClose} title="Revoke Credential">
      <div className="space-y-4">
        <p className="text-graphite-300 text-sm">
          Are you sure you want to revoke the credential for{' '}
          <span className="font-mono text-graphite-100">{principalId}</span> (
          {provider})? This action cannot be undone.
        </p>

        {error && <div className="text-red-400 text-sm">{error}</div>}

        <div className="pt-4 flex justify-end gap-2">
          <Button type="button" variant="secondary" onClick={onClose}>
            Cancel
          </Button>
          <Button
            type="button"
            variant="primary"
            onClick={handleRevoke}
            disabled={isPending}
            className="bg-red-600 hover:bg-red-500 text-white border-red-500"
          >
            {isPending ? 'Revoking...' : 'Revoke Credential'}
          </Button>
        </div>
      </div>
    </Modal>
  );
}
