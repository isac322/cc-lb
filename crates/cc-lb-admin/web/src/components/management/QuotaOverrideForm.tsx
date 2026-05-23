import { useState } from 'react';
import { Button } from '../primitives/Button';
import { useDraftPrincipals } from '../../lib/hooks/useDraftPrincipals';

export function QuotaOverrideForm({
  principalId,
  mock,
}: {
  principalId: string;
  mock?: boolean;
}) {
  const { overrideQuota } = useDraftPrincipals(mock);
  const [requests, setRequests] = useState('');
  const [inputTokens, setInputTokens] = useState('');
  const [outputTokens, setOutputTokens] = useState('');
  const [isPending, setIsPending] = useState(false);
  const [message, setMessage] = useState<{ type: 'success' | 'error'; text: string } | null>(null);

  const handleOverride = async () => {
    setIsPending(true);
    setMessage(null);
    try {
      await overrideQuota(principalId, {
        requests_per_window: requests ? parseInt(requests, 10) : undefined,
        input_tokens_per_window: inputTokens ? parseInt(inputTokens, 10) : undefined,
        output_tokens_per_window: outputTokens ? parseInt(outputTokens, 10) : undefined,
      });
      setMessage({ type: 'success', text: 'Live override applied' });
    } catch (err) {
      setMessage({ type: 'error', text: err instanceof Error ? err.message : String(err) });
    } finally {
      setIsPending(false);
    }
  };

  return (
    <div className="space-y-3">
      <div className="grid grid-cols-3 gap-2">
        <div>
          <label className="block text-xs text-graphite-400 mb-1">Requests</label>
          <input
            type="number"
            value={requests}
            onChange={(e) => setRequests(e.target.value)}
            placeholder="Default"
            className="w-full bg-graphite-900 border border-graphite-800 rounded px-2 py-1 text-sm text-graphite-50 focus:outline-none focus:border-cyan-500"
          />
        </div>
        <div>
          <label className="block text-xs text-graphite-400 mb-1">Input Tokens</label>
          <input
            type="number"
            value={inputTokens}
            onChange={(e) => setInputTokens(e.target.value)}
            placeholder="Default"
            className="w-full bg-graphite-900 border border-graphite-800 rounded px-2 py-1 text-sm text-graphite-50 focus:outline-none focus:border-cyan-500"
          />
        </div>
        <div>
          <label className="block text-xs text-graphite-400 mb-1">Output Tokens</label>
          <input
            type="number"
            value={outputTokens}
            onChange={(e) => setOutputTokens(e.target.value)}
            placeholder="Default"
            className="w-full bg-graphite-900 border border-graphite-800 rounded px-2 py-1 text-sm text-graphite-50 focus:outline-none focus:border-cyan-500"
          />
        </div>
      </div>
      <div className="flex items-center justify-between">
        <div className="text-xs">
          {message && (
            <span className={message.type === 'success' ? 'text-green-400' : 'text-red-400'}>
              {message.text}
            </span>
          )}
        </div>
        <Button type="button" variant="secondary"  onClick={handleOverride} disabled={isPending}>
          Apply Override
        </Button>
      </div>
    </div>
  );
}
