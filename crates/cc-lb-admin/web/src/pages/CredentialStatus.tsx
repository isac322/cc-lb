import { RefreshCw } from 'lucide-react';
import { CredentialRow } from '../components/credentials/CredentialRow';
import { Button } from '../components/primitives/Button';
import { EmptyState } from '../components/primitives/EmptyState';
import { ErrorState } from '../components/primitives/ErrorState';
import { LoadingState } from '../components/primitives/LoadingState';
import { useCredentialStatus } from '../lib/hooks/useCredentialStatus';

export default function CredentialStatus() {
  const { rows, isLoading, error, refresh } = useCredentialStatus();

  return (
    <div className="space-y-6">
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-2xl font-semibold text-graphite-50">
            Credential Status
          </h1>
          <p className="text-sm text-graphite-400 mt-1">
            Status of API keys and OAuth credentials across all principals.
          </p>
        </div>
        <Button variant="secondary" onClick={refresh} disabled={isLoading}>
          <RefreshCw
            className={`w-4 h-4 mr-2 ${isLoading ? 'animate-spin' : ''}`}
          />
          Refresh
        </Button>
      </div>

      {isLoading && rows.length === 0 ? (
        <LoadingState message="Loading credentials..." />
      ) : error && rows.length === 0 ? (
        <ErrorState message={error.message} onRetry={refresh} />
      ) : rows.length === 0 ? (
        <EmptyState
          title="No credentials found"
          message="No credentials have been configured yet."
        />
      ) : (
        <div className="w-full overflow-x-auto border border-graphite-800 rounded-lg bg-graphite-850">
          <table className="w-full text-sm text-left">
            <thead className="text-xs text-graphite-400 uppercase bg-graphite-900 border-b border-graphite-800 sticky top-0">
              <tr>
                <th className="px-4 py-3 font-medium">Identity</th>
                <th className="px-4 py-3 font-medium">Status</th>
                <th className="px-4 py-3 font-medium">Expires</th>
                <th className="px-4 py-3 font-medium">Refresh Token</th>
                <th className="px-4 py-3 font-medium">Last Updated</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-graphite-800">
              {rows.map((row) => (
                <CredentialRow
                  key={`${row.principal_id}-${row.provider}`}
                  row={row}
                />
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
