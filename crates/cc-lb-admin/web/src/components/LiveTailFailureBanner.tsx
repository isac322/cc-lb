import { X } from 'lucide-react';
import { useEffect, useState } from 'react';
import { Button, IconButton, Notice } from './ui/primitives';

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
    <Notice
      tone="danger"
      variant="banner"
      title="Live tail disconnected"
      action={
        <div className="flex items-center gap-1">
          <Button size="sm" onClick={onRetry}>
            Retry now
          </Button>
          <IconButton
            label="Dismiss"
            className="-mr-1.5"
            onClick={() => setDismissed(true)}
          >
            <X aria-hidden="true" />
          </IconButton>
        </div>
      }
    >
      Unable to reach the admin event stream after {reconnectAttempts} attempts
      over the last {formatMinutesSince(permanentFailureSince)} minutes.
    </Notice>
  );
}
