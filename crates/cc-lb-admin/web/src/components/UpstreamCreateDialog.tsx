import { useState } from 'react';
import { useCreate } from '../lib/hooks/useUpstreams';
import type { UpstreamKind, UpstreamResponse } from '../lib/types/v1';
import { OAuthConnectButton } from './OAuthConnectButton';
import { Button } from './primitives/Button';
import { Modal } from './primitives/Modal';

interface UpstreamCreateDialogProps {
  onClose: () => void;
  onSuccess: () => void;
}

export function UpstreamCreateDialog({
  onClose,
  onSuccess,
}: UpstreamCreateDialogProps) {
  const [name, setName] = useState('');
  const [kind, setKind] = useState<UpstreamKind>('anthropic_api_key');
  const [apiKeyEnv, setApiKeyEnv] = useState('');
  const [baseUrl, setBaseUrl] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [createdUpstream, setCreatedUpstream] =
    useState<UpstreamResponse | null>(null);

  const createUpstream = useCreate();

  const handleSubmit = async () => {
    if (!name) {
      setError('Name is required');
      return;
    }
    if (kind === 'anthropic_api_key' && !apiKeyEnv) {
      setError('API Key Env is required for API Key upstreams');
      return;
    }

    setIsSubmitting(true);
    setError(null);

    try {
      const res = await createUpstream({
        name,
        kind,
        ...(baseUrl ? { base_url: baseUrl } : {}),
        ...(kind === 'anthropic_api_key' ? { api_key_env: apiKeyEnv } : {}),
      });

      if (kind === 'anthropic_oauth') {
        setCreatedUpstream(res);
      } else {
        onSuccess();
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setIsSubmitting(false);
    }
  };

  if (createdUpstream) {
    return (
      <Modal isOpen={true} onClose={onClose} title="Connect OAuth">
        <p className="text-sm text-graphite-300 mb-4">
          Upstream created successfully. Please connect Claude OAuth to
          continue.
        </p>
        <div className="py-6 flex justify-center">
          <OAuthConnectButton
            upstreamId={createdUpstream.id}
            initialRevision={createdUpstream.revision}
            onSuccess={onSuccess}
          />
        </div>
      </Modal>
    );
  }

  return (
    <Modal isOpen={true} onClose={onClose} title="Create Upstream">
      <p className="text-sm text-graphite-300 mb-4">
        Add a new upstream provider.
      </p>
      <div className="space-y-4 py-4">
        <div className="space-y-2">
          <label
            htmlFor="name"
            className="text-sm font-medium text-graphite-200"
          >
            Name
          </label>
          <input
            id="name"
            className="w-full bg-graphite-900 border border-graphite-700 rounded px-3 py-2 text-sm text-graphite-100 focus:outline-none focus:border-cyan-500"
            value={name}
            onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
              setName(e.target.value)
            }
            placeholder="e.g., production-anthropic"
          />
        </div>

        <div className="space-y-2">
          <label className="text-sm font-medium text-graphite-200">Kind</label>
          <div className="flex flex-col gap-2">
            <label className="flex items-center gap-2 text-sm text-graphite-300">
              <input
                type="radio"
                name="kind"
                value="anthropic_api_key"
                checked={kind === 'anthropic_api_key'}
                onChange={() => setKind('anthropic_api_key')}
                className="text-cyan-400 focus:ring-cyan-400 bg-graphite-900 border-graphite-700"
              />
              Anthropic API Key
            </label>
            <label className="flex items-center gap-2 text-sm text-graphite-300">
              <input
                type="radio"
                name="kind"
                value="anthropic_oauth"
                checked={kind === 'anthropic_oauth'}
                onChange={() => setKind('anthropic_oauth')}
                className="text-cyan-400 focus:ring-cyan-400 bg-graphite-900 border-graphite-700"
              />
              Anthropic OAuth
            </label>
            <label className="flex items-center gap-2 text-sm text-graphite-300">
              <input
                type="radio"
                name="kind"
                value="custom"
                checked={kind === 'custom'}
                onChange={() => setKind('custom')}
                className="text-cyan-400 focus:ring-cyan-400 bg-graphite-900 border-graphite-700"
              />
              Custom
            </label>
          </div>
        </div>

        {kind === 'anthropic_api_key' && (
          <div className="space-y-2">
            <label
              htmlFor="api_key_env"
              className="text-sm font-medium text-graphite-200"
            >
              API Key Env Var
            </label>
            <input
              id="api_key_env"
              className="w-full bg-graphite-900 border border-graphite-700 rounded px-3 py-2 text-sm text-graphite-100 focus:outline-none focus:border-cyan-500"
              value={apiKeyEnv}
              onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                setApiKeyEnv(e.target.value)
              }
              placeholder="e.g., ANTHROPIC_API_KEY"
            />
          </div>
        )}

        <div className="space-y-2">
          <label className="text-sm font-medium text-graphite-200">
            Base URL (Optional)
          </label>
          <input
            className="w-full bg-graphite-900 border border-graphite-700 rounded px-3 py-2 text-sm text-graphite-100 focus:outline-none focus:border-cyan-500"
            value={baseUrl}
            onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
              setBaseUrl(e.target.value)
            }
            placeholder="e.g., https://api.anthropic.com"
          />
        </div>

        {error && <div className="text-red-400 text-sm">{error}</div>}
      </div>
      <div className="flex justify-end gap-2 mt-4">
        <Button variant="secondary" onClick={onClose}>
          Cancel
        </Button>
        <Button
          variant="primary"
          onClick={handleSubmit}
          disabled={isSubmitting}
        >
          {isSubmitting ? 'Creating...' : 'Create'}
        </Button>
      </div>
    </Modal>
  );
}
