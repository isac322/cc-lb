import { useState, useEffect } from 'react';
import { useSearchParams } from 'react-router';
import { PrincipalDirectory } from '../components/principals/PrincipalDirectory';
import { PrincipalUsageSummary } from '../components/principals/PrincipalUsageSummary';
import { AccountGrouping } from '../components/principals/AccountGrouping';
import { RangeSelector } from '../components/overview/RangeSelector';
import { EmptyState } from '../components/primitives/EmptyState';
import { ErrorState } from '../components/primitives/ErrorState';
import { usePrincipalDirectory } from '../lib/hooks/usePrincipalDirectory';
import { usePrincipalUsage } from '../lib/hooks/usePrincipalUsage';
import { usePrincipalLimits } from '../lib/hooks/usePrincipalLimits';

export default function PrincipalLimits() {
  const [searchParams, setSearchParams] = useSearchParams();
  const isMock = searchParams.get('mock') === '1';
  const range = searchParams.get('range') || '1h';
  
  const urlPrincipal = searchParams.get('principal');
  const [selectedPrincipal, setSelectedPrincipal] = useState<string | null>(urlPrincipal);

  const { data: dirData, isLoading: dirLoading, error: dirError } = usePrincipalDirectory(isMock);
  const { data: usageData, isLoading: usageLoading, error: usageError } = usePrincipalUsage(selectedPrincipal, range, isMock);
  const { data: limitsData, isLoading: limitsLoading, error: limitsError } = usePrincipalLimits(selectedPrincipal, isMock);

  // Sync URL principal to state on mount or URL change
  useEffect(() => {
    if (urlPrincipal && urlPrincipal !== selectedPrincipal) {
      setSelectedPrincipal(urlPrincipal);
    }
  }, [urlPrincipal, selectedPrincipal]);

  // Auto-select first principal if none selected
  useEffect(() => {
    if (!selectedPrincipal && dirData?.principals.length) {
      const id = dirData.principals[0].id;
      setSelectedPrincipal(id);
      const newParams = new URLSearchParams(searchParams);
      newParams.set('principal', id);
      setSearchParams(newParams);
    }
  }, [dirData, selectedPrincipal, searchParams, setSearchParams]);

  const handleSelectPrincipal = (id: string) => {
    setSelectedPrincipal(id);
    const newParams = new URLSearchParams(searchParams);
    newParams.set('principal', id);
    setSearchParams(newParams);
  };

  if (dirError) return <ErrorState title="Failed to load principals" message={dirError.message} />;

  return (
    <div className="flex flex-col lg:flex-row gap-6">
      <PrincipalDirectory 
        data={dirData} 
        isLoading={dirLoading} 
        selectedId={selectedPrincipal} 
        onSelect={handleSelectPrincipal} 
      />
      
      <div className="flex-1 space-y-6 min-w-0">
        <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
          <div className="flex items-center space-x-3">
            <h2 className="text-lg font-semibold text-graphite-50">Principal Limits</h2>
            {selectedPrincipal && (
              <span className="px-2 py-1 bg-graphite-800 text-graphite-300 rounded text-sm font-mono">
                {selectedPrincipal}
              </span>
            )}
          </div>
          <RangeSelector />
        </div>

        {selectedPrincipal ? (
          <>
            {usageError ? (
              <ErrorState title="Failed to load usage" message={usageError.message} />
            ) : (
              <PrincipalUsageSummary usage={usageData} isLoading={usageLoading} />
            )}

            {limitsError ? (
              <ErrorState title="Failed to load limits" message={limitsError.message} />
            ) : limitsLoading ? (
              <div className="space-y-6">
                {[1, 2].map(i => (
                  <div key={i} className="h-64 bg-graphite-800 rounded-lg animate-pulse" />
                ))}
              </div>
            ) : !limitsData?.observed || limitsData.identities.length === 0 ? (
              <EmptyState 
                title="No rate-limit snapshots observed" 
                message="No rate-limit snapshots observed for this principal yet — Anthropic limit headers populate this view as traffic flows." 
              />
            ) : (
              <div className="space-y-6">
                {limitsData.identities.map((identity, idx) => (
                  <AccountGrouping key={`${identity.identity_kind}-${identity.identity_value || idx}`} identity={identity} />
                ))}
              </div>
            )}
          </>
        ) : (
          <EmptyState title="Select a principal" message="Choose a principal from the directory to view limits." />
        )}
      </div>
    </div>
  );
}
