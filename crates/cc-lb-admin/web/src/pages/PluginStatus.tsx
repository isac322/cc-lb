import { RefreshCw } from 'lucide-react';
import { PluginSection } from '../components/plugins/PluginSection';
import { Button } from '../components/primitives/Button';
import { EmptyState } from '../components/primitives/EmptyState';
import { ErrorState } from '../components/primitives/ErrorState';
import { LoadingState } from '../components/primitives/LoadingState';
import { usePluginStatus } from '../lib/hooks/usePluginStatus';

export default function PluginStatus() {
  const { plugins, isLoading, error, refresh } = usePluginStatus();

  if (isLoading && plugins.length === 0) {
    return <LoadingState message="Loading plugins..." />;
  }

  if (error && plugins.length === 0) {
    return <ErrorState message={error.message} onRetry={refresh} />;
  }

  const slots = ['authn', 'router', 'observability', 'dialect', 'signer'];
  const groupedPlugins = slots.reduce(
    (acc, slot) => {
      acc[slot] = plugins.filter((p) => p.slot === slot);
      return acc;
    },
    {} as Record<string, typeof plugins>,
  );

  return (
    <div className="space-y-6">
      <div className="flex justify-between items-center">
        <div>
          <p className="text-sm text-graphite-400">
            Runtime status and health of loaded Extism WASM plugins.
          </p>
        </div>
        <Button variant="secondary" onClick={refresh} disabled={isLoading}>
          <RefreshCw
            className={`w-4 h-4 mr-2 ${isLoading ? 'animate-spin' : ''}`}
          />
          Refresh
        </Button>
      </div>

      {plugins.length === 0 ? (
        <EmptyState
          title="No plugins configured"
          message="Edit settings to add plugins."
        />
      ) : (
        <div>
          {slots.map((slot) => (
            <PluginSection
              key={slot}
              slot={slot}
              plugins={groupedPlugins[slot]}
            />
          ))}
        </div>
      )}
    </div>
  );
}
