import { useState } from 'react';
import { useOAuthComplete, useOAuthStart } from '../lib/hooks/useUpstreams';
import { Button } from './primitives/Button';

interface OAuthConnectButtonProps {
  upstreamId: string;
  initialRevision: number;
  onSuccess: () => void;
}

export function OAuthConnectButton({
  upstreamId,
  onSuccess,
}: OAuthConnectButtonProps) {
  const [stateToken, setStateToken] = useState<string | null>(null);
  const [manualCode, setManualCode] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [isStarting, setIsStarting] = useState(false);
  const [isCompleting, setIsCompleting] = useState(false);

  const startOAuth = useOAuthStart();
  const completeOAuth = useOAuthComplete();

  const handleStart = async () => {
    setError(null);
    setIsStarting(true);
    const popup = window.open('about:blank', '_blank');
    try {
      const res = await startOAuth(upstreamId);
      setStateToken(res.state_token);
      if (popup && !popup.closed) {
        popup.location.href = res.authorize_url;
      } else {
        window.location.assign(res.authorize_url);
      }
    } catch (err) {
      if (popup && !popup.closed) popup.close();
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setIsStarting(false);
    }
  };

  const handleComplete = async () => {
    if (!stateToken || !manualCode) return;
    setIsCompleting(true);
    setError(null);
    try {
      // Anthropic shows the code as `<code>#<state>`; keep only the code part.
      const code = manualCode.trim().split('#')[0].split('&')[0].trim();
      await completeOAuth(upstreamId, {
        state_token: stateToken,
        code,
      });
      onSuccess();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setIsCompleting(false);
    }
  };

  if (!stateToken) {
    return (
      <div className="flex flex-col items-center gap-3 w-full">
        <Button
          variant="primary"
          onClick={handleStart}
          disabled={isStarting}
          className="w-full"
        >
          {isStarting ? 'Opening Claude...' : 'Connect Claude OAuth'}
        </Button>
        {error && (
          <div className="text-red-400 text-sm text-center w-full">{error}</div>
        )}
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-3 w-full">
      <ol className="text-sm text-graphite-300 space-y-1 list-decimal list-inside">
        <li>Authorize cc-lb in the new tab that just opened.</li>
        <li>
          On the resulting page, copy the authorization{' '}
          <span className="font-mono text-graphite-200">code</span>.
        </li>
        <li>Paste it below and click Complete.</li>
      </ol>
      <div className="space-y-2">
        <label className="text-sm font-medium text-graphite-200">
          Authorization Code
        </label>
        <input
          type="text"
          autoComplete="off"
          spellCheck={false}
          className="w-full bg-graphite-900 border border-graphite-700 rounded px-3 py-2 text-sm text-graphite-100 focus:outline-none focus:border-cyan-500"
          value={manualCode}
          onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
            setManualCode(e.target.value)
          }
          placeholder="Paste code here..."
        />
      </div>
      {error && <div className="text-red-400 text-sm">{error}</div>}
      <div className="flex justify-end gap-2">
        <Button
          variant="secondary"
          onClick={() => {
            setStateToken(null);
            setManualCode('');
            setError(null);
          }}
        >
          Restart
        </Button>
        <Button
          variant="primary"
          onClick={handleComplete}
          disabled={!manualCode.trim() || isCompleting}
        >
          {isCompleting ? 'Completing...' : 'Complete'}
        </Button>
      </div>
    </div>
  );
}
