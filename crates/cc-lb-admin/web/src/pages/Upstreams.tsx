import { RefreshCw } from 'lucide-react';
import { Button } from '../components/primitives/Button';
import { EmptyState } from '../components/primitives/EmptyState';
import { ErrorState } from '../components/primitives/ErrorState';
import { LoadingState } from '../components/primitives/LoadingState';
import { UpstreamCard } from '../components/upstreams/UpstreamCard';
import { useUpstreamHealth } from '../lib/hooks/useUpstreamHealth';

export default function Upstreams() {
  const { healthByName, isLoading, error, refresh } = useUpstreamHealth();

  if (isLoading && Object.keys(healthByName).length === 0) {
    return <LoadingState message="Loading upstreams..." />;
  }

  if (error && Object.keys(healthByName).length === 0) {
    return <ErrorState message={error.message} onRetry={refresh} />;
  }

  const upstreams = Object.values(healthByName);

  return (
    <div className="space-y-6">
      <div className="flex justify-between items-center">
        <div>
          <p className="text-sm text-graphite-400">
            Real-time health and circuit breaker status for configured
            upstreams.
          </p>
        </div>
        <Button variant="secondary" onClick={refresh} disabled={isLoading}>
          <RefreshCw
            className={`w-4 h-4 mr-2 ${isLoading ? 'animate-spin' : ''}`}
          />
          Refresh
        </Button>
      </div>

      {upstreams.length === 0 ? (
        <EmptyState
          title="No upstreams configured"
          message="Edit settings to add one."
        />
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {upstreams.map((health) => (
            <UpstreamCard key={health.name} health={health} />
          ))}
        </div>
      )}
    </div>
  );
}
