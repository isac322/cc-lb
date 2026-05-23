import { useState } from 'react';
import { usePrincipalKeys } from '../../lib/hooks/usePrincipalKeys';
import { Button } from '../primitives/Button';
import { Modal } from '../primitives/Modal';

export function RevokeKeyConfirm({
  principalId,
  keyId,
  onClose,
  onSuccess,
  mock,
}: {
  principalId: string;
  keyId: string;
  onClose: () => void;
  onSuccess: () => void;
  mock?: boolean;
}) {
  const { revokeKey } = usePrincipalKeys(principalId, mock);
  const [isPending, setIsPending] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleRevoke = async () => {
    setIsPending(true);
    setError(null);
    try {
      await revokeKey(keyId);
      onSuccess();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setIsPending(false);
    }
  };

  return (
    <Modal isOpen onClose={onClose} title="Revoke Key">
      <div className="space-y-4">
        <p className="text-graphite-300 text-sm">
          Are you sure you want to revoke the key{' '}
          <span className="font-mono text-graphite-100">{keyId}</span>? This
          action cannot be undone.
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
            {isPending ? 'Revoking...' : 'Revoke Key'}
          </Button>
        </div>
      </div>
    </Modal>
  );
}
