import { AlertTriangle, X } from 'lucide-react';
import { useEffect, useState } from 'react';
import { Button, IconButton } from './ui/primitives';

export function LiveTailFailureBanner({
  permanentFailure,
  permanentFailureSince,
  reconnectAttempts,
  onRetry,
}: {
  permanentFailure: boolean;
  permanentFailureSince: number | null;
  reconnectAttempts: number;
  onRetry: () => void;
}) {
  const [dismissed, setDismissed] = useState(false);

  // Reset dismissed state if permanentFailure transitions from false to true
  useEffect(() => {
    if (permanentFailure) {
      setDismissed(false);
    }
  }, [permanentFailure]);

  if (!permanentFailure || dismissed) {
    return null;
  }

  const formatMinutesSince = (timestamp: number | null) => {
    if (!timestamp) return '0';
    const minutes = Math.floor((Date.now() - timestamp) / 60000);
    return minutes.toString();
  };

  return (
    <div className="flex items-start gap-3 p-3 mb-4 rounded-sm border border-[color:var(--color-danger)]/30 bg-red-500/10 text-[color:var(--color-danger)]">
      <AlertTriangle className="w-5 h-5 shrink-0 mt-0.5" />
      <div className="flex-1 min-w-0">
        <h3 className="text-sm font-bold">Live tail disconnected</h3>
        <p className="text-xs mt-1 opacity-90">
          Unable to reach the admin event stream after {reconnectAttempts}{' '}
          attempts over the last {formatMinutesSince(permanentFailureSince)}{' '}
          minutes.
        </p>
        <div className="mt-3">
          <Button variant="danger" size="sm" onClick={onRetry}>
            Retry now
          </Button>
        </div>
      </div>
      <IconButton
        label="Dismiss"
        className="shrink-0 -mt-1 -mr-1 text-[color:var(--color-danger)] hover:bg-red-500/20"
        onClick={() => setDismissed(true)}
      >
        <X className="w-4 h-4" />
      </IconButton>
    </div>
  );
}
