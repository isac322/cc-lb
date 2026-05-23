import { useState, useEffect } from 'react';
import { useSearchParams } from 'react-router';
import { Card } from '../components/primitives/Card';
import { Button } from '../components/primitives/Button';
import { PrincipalManagementList } from '../components/management/PrincipalManagementList';
import { CredentialsList } from '../components/management/CredentialsList';
import { PrincipalEditForm } from '../components/management/PrincipalEditForm';
import { usePrincipalsManagement } from '../lib/hooks/usePrincipalsManagement';
import { useDraftPrincipals } from '../lib/hooks/useDraftPrincipals';

import { IssueKeyDialog } from '../components/management/IssueKeyDialog';
import { CredentialRotateDialog } from '../components/management/CredentialRotateDialog';

export default function PrincipalManagement() {
  const [searchParams, setSearchParams] = useSearchParams();
  const mock = searchParams.get('mock') === '1';
  const urlTab = searchParams.get('tab') as 'principals' | 'credentials' | null;
  const autoOpenDialog = searchParams.get('dialog');

  const [tab, setTab] = useState<'principals' | 'credentials'>(urlTab || 'principals');
  const [isCreating, setIsCreating] = useState(false);

  useEffect(() => {
    if (urlTab && urlTab !== tab) {
      setTab(urlTab);
    }
  }, [urlTab, tab]);

  const handleTabChange = (newTab: 'principals' | 'credentials') => {
    setTab(newTab);
    const newParams = new URLSearchParams(searchParams);
    newParams.set('tab', newTab);
    setSearchParams(newParams);
  };

  const { principals, refresh, isLoading, error } = usePrincipalsManagement(mock);
  const { lastDraftRevision } = useDraftPrincipals(mock);

  return (
    <div className="space-y-6 pb-16">
      <div className="flex items-center justify-between">
        <div className="flex gap-4 border-b border-graphite-800 w-full">
          <button
            className={`pb-2 px-1 text-sm font-medium transition-colors ${
              tab === 'principals'
                ? 'text-cyan-400 border-b-2 border-cyan-400'
                : 'text-graphite-400 hover:text-graphite-200'
            }`}
            onClick={() => handleTabChange('principals')}
          >
            Principals
          </button>
          <button
            className={`pb-2 px-1 text-sm font-medium transition-colors ${
              tab === 'credentials'
                ? 'text-cyan-400 border-b-2 border-cyan-400'
                : 'text-graphite-400 hover:text-graphite-200'
            }`}
            onClick={() => handleTabChange('credentials')}
          >
            Credentials
          </button>
        </div>
      </div>

      {tab === 'principals' && (
        <Card className="p-6">
          <div className="flex items-center justify-between mb-6">
            <h2 className="text-lg font-semibold text-graphite-50">Principals</h2>
            <Button variant="primary"  onClick={() => setIsCreating(true)}>
              + New principal
            </Button>
          </div>
          
          {isLoading ? (
            <div className="text-graphite-400">Loading principals...</div>
          ) : error ? (
            <div className="text-red-400">Error: {error.message}</div>
          ) : (
            <PrincipalManagementList principals={principals} onRefresh={refresh} mock={mock} />
          )}
        </Card>
      )}

      {tab === 'credentials' && (
        <Card className="p-6">
          <div className="mb-6">
            <h2 className="text-lg font-semibold text-graphite-50">Credentials</h2>
          </div>
          <CredentialsList mock={mock} />
        </Card>
      )}

      {isCreating && (
        <PrincipalEditForm
          onClose={() => setIsCreating(false)}
          onSuccess={() => {
            setIsCreating(false);
            refresh();
          }}
          mock={mock}
        />
      )}

      {autoOpenDialog === 'issue' && (
        <IssueKeyDialog
          principalId="alice"
          onClose={() => {
            const newParams = new URLSearchParams(searchParams);
            newParams.delete('dialog');
            setSearchParams(newParams);
          }}
          onSuccess={() => {}}
          mock={mock}
          autoReveal={true}
        />
      )}

      {autoOpenDialog === 'rotate' && (
        <CredentialRotateDialog
          principalId="bob"
          provider="api_key"
          onClose={() => {
            const newParams = new URLSearchParams(searchParams);
            newParams.delete('dialog');
            setSearchParams(newParams);
          }}
          onSuccess={() => {}}
          mock={mock}
        />
      )}

      {lastDraftRevision !== null && (
        <div className="fixed bottom-0 left-0 right-0 bg-graphite-900 border-t border-graphite-800 p-4 flex items-center justify-center z-40">
          <div className="text-sm text-graphite-300">
            Draft revision {lastDraftRevision} saved.{' '}
            <a href="/settings" className="text-cyan-400 hover:underline font-medium">
              Review & apply →
            </a>
          </div>
        </div>
      )}
    </div>
  );
}
