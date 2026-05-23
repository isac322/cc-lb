import { useState } from 'react';
import { useCredentials } from '../../lib/hooks/useCredentials';
import { Button } from '../primitives/Button';
import { Modal } from '../primitives/Modal';

export function CredentialRotateDialog({
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
  const { rotateCredential } = useCredentials(mock);
  const [isPending, setIsPending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [plaintextKey, setPlaintextKey] = useState<string | null>(null);
  const [copied, setCopied] = useState(false);

  const handleRotate = async () => {
    setIsPending(true);
    setError(null);
    try {
      const res = await rotateCredential(principalId, provider);
      setPlaintextKey(res.plaintext_key);
      onSuccess();
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      if (msg.includes('rotate_unsupported')) {
        setError(
          'Rotation requires running the OAuth flow. Please visit the Credentials page to re-authenticate.',
        );
      } else {
        setError(msg);
      }
    } finally {
      setIsPending(false);
    }
  };

  const handleCopy = async () => {
    if (plaintextKey) {
      await navigator.clipboard.writeText(plaintextKey);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    }
  };

  const handleDismiss = () => {
    setPlaintextKey(null);
    onClose();
  };

  if (plaintextKey) {
    return (
      <Modal isOpen onClose={handleDismiss} title="Credential Rotated">
        <div className="space-y-4">
          <div className="bg-yellow-900/30 border border-yellow-700/50 rounded p-3 text-yellow-200 text-sm">
            <strong>Warning:</strong> This is the only time the new key will be
            displayed. Copy it now.
          </div>
          <div className="flex items-center gap-2">
            <input
              type="text"
              readOnly
              value={plaintextKey}
              className="flex-1 bg-graphite-950 border border-graphite-800 rounded px-3 py-2 text-graphite-50 font-mono select-all focus:outline-none"
            />
            <Button variant="secondary" onClick={handleCopy}>
              {copied ? 'Copied!' : 'Copy'}
            </Button>
          </div>
          <div className="flex justify-end pt-4">
            <Button variant="primary" onClick={handleDismiss}>
              Dismiss
            </Button>
          </div>
        </div>
      </Modal>
    );
  }

  return (
    <Modal isOpen onClose={onClose} title="Rotate Credential">
      <div className="space-y-4">
        <p className="text-graphite-300 text-sm">
          Are you sure you want to rotate the credential for{' '}
          <span className="font-mono text-graphite-100">{principalId}</span> (
          {provider})? The old credential will be revoked immediately.
        </p>

        {error && (
          <div className="text-red-400 text-sm">
            {error}
            {error.includes('OAuth') && (
              <div className="mt-2">
                <a
                  href="/credentials"
                  className="text-cyan-400 hover:underline"
                >
                  Go to Credentials →
                </a>
              </div>
            )}
          </div>
        )}

        <div className="pt-4 flex justify-end gap-2">
          <Button type="button" variant="secondary" onClick={onClose}>
            Cancel
          </Button>
          <Button
            type="button"
            variant="primary"
            onClick={handleRotate}
            disabled={isPending}
          >
            {isPending ? 'Rotating...' : 'Rotate Credential'}
          </Button>
        </div>
      </div>
    </Modal>
  );
}
