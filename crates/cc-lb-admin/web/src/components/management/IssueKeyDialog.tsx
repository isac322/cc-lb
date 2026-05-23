import { useEffect, useState } from 'react';
import { usePrincipalKeys } from '../../lib/hooks/usePrincipalKeys';
import { Button } from '../primitives/Button';
import { FormField } from '../primitives/FormField';
import { Modal } from '../primitives/Modal';

export function IssueKeyDialog({
  principalId,
  onClose,
  onSuccess,
  mock,
  autoReveal,
}: {
  principalId: string;
  onClose: () => void;
  onSuccess: () => void;
  mock?: boolean;
  autoReveal?: boolean;
}) {
  const { issueKey } = usePrincipalKeys(principalId, mock);
  const [label, setLabel] = useState('');
  const [isPending, setIsPending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [plaintextKey, setPlaintextKey] = useState<string | null>(null);
  const [copied, setCopied] = useState(false);

  useEffect(() => {
    if (autoReveal && mock) {
      setPlaintextKey(
        `mock-issued-key-${Math.random().toString(36).substring(2, 15)}`,
      );
    }
  }, [autoReveal, mock]);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setIsPending(true);
    setError(null);
    try {
      const res = await issueKey(label || undefined);
      setPlaintextKey(res.plaintext_key);
      onSuccess();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
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
      <Modal isOpen onClose={handleDismiss} title="Key Issued">
        <div className="space-y-4">
          <div className="bg-yellow-900/30 border border-yellow-700/50 rounded p-3 text-yellow-200 text-sm">
            <strong>Warning:</strong> This is the only time the key will be
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
    <Modal isOpen onClose={onClose} title="Issue New Key">
      <form onSubmit={handleSubmit} className="space-y-4">
        <FormField label="Label (Optional)" error={undefined}>
          <input
            type="text"
            value={label}
            onChange={(e) => setLabel(e.target.value)}
            placeholder="e.g. Production API Key"
            className="w-full bg-graphite-900 border border-graphite-800 rounded px-3 py-2 text-graphite-50 focus:outline-none focus:border-cyan-500"
          />
        </FormField>

        {error && <div className="text-red-400 text-sm">{error}</div>}

        <div className="pt-4 flex justify-end gap-2">
          <Button type="button" variant="secondary" onClick={onClose}>
            Cancel
          </Button>
          <Button type="submit" variant="primary" disabled={isPending}>
            {isPending ? 'Issuing...' : 'Issue Key'}
          </Button>
        </div>
      </form>
    </Modal>
  );
}
