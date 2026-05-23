import { RefreshCw } from 'lucide-react';
import { useState } from 'react';
import { AuditFilterBar } from '../components/activity/AuditFilterBar';
import { AuditTable } from '../components/activity/AuditTable';
import { Button } from '../components/primitives/Button';
import { EmptyState } from '../components/primitives/EmptyState';
import { ErrorState } from '../components/primitives/ErrorState';
import { LoadingState } from '../components/primitives/LoadingState';
import { useAuditEvents } from '../lib/hooks/useAuditEvents';

export default function AdminActivity() {
  const [selectedKind, setSelectedKind] = useState<string>('');
  const { events, isLoading, error, refresh } = useAuditEvents({
    limit: 100,
    kind: selectedKind || undefined,
  });

  return (
    <div className="space-y-6">
      <div className="flex justify-between items-center">
        <div>
          <p className="text-sm text-graphite-400">
            Audit log of administrative actions and configuration changes.
          </p>
        </div>
        <Button variant="secondary" onClick={refresh} disabled={isLoading}>
          <RefreshCw
            className={`w-4 h-4 mr-2 ${isLoading ? 'animate-spin' : ''}`}
          />
          Refresh
        </Button>
      </div>

      <AuditFilterBar
        selectedKind={selectedKind}
        onKindChange={setSelectedKind}
      />

      {isLoading && events.length === 0 ? (
        <LoadingState message="Loading audit events..." />
      ) : error && events.length === 0 ? (
        <ErrorState message={error.message} onRetry={refresh} />
      ) : events.length === 0 ? (
        <EmptyState
          title="No activity found"
          message={
            selectedKind
              ? `No events matching kind '${selectedKind}'.`
              : 'No administrative actions have been recorded yet.'
          }
        />
      ) : (
        <div className="bg-graphite-900 rounded-lg border border-graphite-800 shadow-sm overflow-hidden">
          <AuditTable events={events} />
        </div>
      )}
    </div>
  );
}
