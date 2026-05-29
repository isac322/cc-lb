import { useEffect, useRef, useState } from 'react';
import {
  useGet,
  useOAuthComplete,
  useOAuthStart,
} from '../lib/hooks/useUpstreams';
import { Button } from './primitives/Button';
import { Modal } from './primitives/Modal';

interface OAuthConnectButtonProps {
  upstreamId: string;
  initialRevision: number;
  onSuccess: () => void;
}

export function OAuthConnectButton({
  upstreamId,
  initialRevision,
  onSuccess,
}: OAuthConnectButtonProps) {
  const [isPolling, setIsPolling] = useState(false);
  const [showFallback, setShowFallback] = useState(false);
  const [stateToken, setStateToken] = useState<string | null>(null);
  const [manualCode, setManualCode] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [isCompleting, setIsCompleting] = useState(false);

  const startOAuth = useOAuthStart();
  const getUpstream = useGet();
  const completeOAuth = useOAuthComplete();
  const abortControllerRef = useRef<AbortController | null>(null);

  const handleStart = async () => {
    try {
      setError(null);
      const res = await startOAuth(upstreamId);
      setStateToken(res.state_token);
      window.open(res.authorize_url, '_blank');
      setIsPolling(true);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  };

  useEffect(() => {
    if (!isPolling) return;

    const controller = new AbortController();
    abortControllerRef.current = controller;
    const startTime = Date.now();
    const timeout = 5 * 60 * 1000; // 5 minutes

    const poll = async () => {
      if (controller.signal.aborted) return;

      if (Date.now() - startTime > timeout) {
        setIsPolling(false);
        setShowFallback(true);
        return;
      }

      try {
        const upstream = await getUpstream(upstreamId);
        // We check if revision has increased, which indicates the OAuth flow completed
        // and updated the upstream record.
        if (upstream.revision > initialRevision) {
          setIsPolling(false);
          onSuccess();
          return;
        }
      } catch (err) {
        console.error('Polling error:', err);
      }

      setTimeout(poll, 2000);
    };

    poll();

    return () => {
      controller.abort();
    };
  }, [isPolling, upstreamId, initialRevision, getUpstream, onSuccess]);

  const handleManualComplete = async () => {
    if (!stateToken || !manualCode) return;
    setIsCompleting(true);
    setError(null);
    try {
      await completeOAuth(upstreamId, {
        state_token: stateToken,
        code: manualCode,
      });
      setShowFallback(false);
      onSuccess();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setIsCompleting(false);
    }
  };

  return (
    <>
      <Button variant="primary" onClick={handleStart} disabled={isPolling}>
        {isPolling ? 'Waiting for authorization...' : 'Connect Claude OAuth'}
      </Button>

      {isPolling && (
        <Button
          variant="secondary"
          className="ml-2"
          onClick={() => {
            setIsPolling(false);
            abortControllerRef.current?.abort();
            setShowFallback(true);
          }}
        >
          I just authorized
        </Button>
      )}

      <Modal
        isOpen={showFallback}
        onClose={() => setShowFallback(false)}
        title="Manual OAuth Completion"
      >
        <div className="space-y-4 py-4">
          <p className="text-sm text-graphite-300">
            If the automatic redirect didn't work, please paste the
            authorization code here.
          </p>
          <div className="space-y-2">
            <label className="text-sm font-medium text-graphite-200">
              Authorization Code
            </label>
            <input
              className="w-full bg-graphite-900 border border-graphite-700 rounded px-3 py-2 text-sm text-graphite-100 focus:outline-none focus:border-cyan-500"
              value={manualCode}
              onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                setManualCode(e.target.value)
              }
              placeholder="Paste code here..."
            />
          </div>
          {error && <div className="text-red-400 text-sm">{error}</div>}
        </div>
        <div className="flex justify-end gap-2 mt-4">
          <Button variant="secondary" onClick={() => setShowFallback(false)}>
            Cancel
          </Button>
          <Button
            variant="primary"
            onClick={handleManualComplete}
            disabled={!manualCode || isCompleting}
          >
            {isCompleting ? 'Completing...' : 'Complete'}
          </Button>
        </div>
      </Modal>
    </>
  );
}
