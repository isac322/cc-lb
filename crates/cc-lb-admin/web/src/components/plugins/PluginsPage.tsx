import { useNavigate } from '@tanstack/react-router';
import { usePluginRegistry } from '../../lib/queries';
import { Route } from '../../routes/plugins';
import { PageContainer } from '../ui/primitives';
import { PluginCatalog } from './PluginCatalog';
import { PluginDetail } from './PluginDetail';
import { PluginUploadCard } from './PluginUploadCard';

export function PluginsPage() {
  const { plugin: selectedPluginId } = Route.useSearch();
  const navigate = useNavigate({ from: Route.id });
  const reg = usePluginRegistry();

  const setSelectedPluginId = (id: string | null) => {
    navigate({ search: { plugin: id || undefined } });
  };

  const selectedPlugin = reg.data?.entries.find(
    (p) => p.id === selectedPluginId,
  );

  return (
    <PageContainer>
      <div className="mb-6">
        <h1 className="text-xl font-medium font-sans text-text">Plugins</h1>
        <div className="mt-1 space-y-1">
          <p className="text-sm text-text-faint">
            Upload and manage WebAssembly plugins. Apply them to Principals or
            Upstreams to customize behavior.
          </p>
        </div>
      </div>
      {selectedPlugin ? (
        <PluginDetail
          plugin={selectedPlugin}
          onBack={() => setSelectedPluginId(null)}
        />
      ) : (
        <div className="space-y-6">
          <PluginUploadCard onUploaded={setSelectedPluginId} />
          <PluginCatalog onSelectPlugin={setSelectedPluginId} />
        </div>
      )}
    </PageContainer>
  );
}
