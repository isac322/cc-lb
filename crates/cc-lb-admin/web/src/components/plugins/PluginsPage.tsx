import { useNavigate } from '@tanstack/react-router';
import { ArrowLeft } from 'lucide-react';
import { usePluginRegistry } from '../../lib/queries';
import { Route } from '../../routes/plugins';
import {
  Button,
  Card,
  CardBody,
  PageContainer,
  Section,
  Skeleton,
} from '../ui/primitives';
import { PluginCatalog } from './PluginCatalog';
import { PluginDetail } from './PluginDetail';
import { PluginUploadCard } from './PluginUploadCard';

function PluginDetailSkeleton({ onBack }: { onBack: () => void }) {
  return (
    <div
      aria-busy="true"
      aria-label="Loading plugin details"
      className="space-y-6 mt-6"
      role="status"
    >
      <div className="flex items-center gap-4">
        <Button variant="ghost" size="sm" onClick={onBack}>
          <ArrowLeft className="w-4 h-4 mr-2" />
          Back to Catalog
        </Button>
        <Skeleton className="h-7 w-52 max-w-full" />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <div className="lg:col-span-2 space-y-6">
          <Section
            title={
              <span className="text-lg font-medium">What this plugin does</span>
            }
          >
            <Card>
              <CardBody className="space-y-4 min-h-64">
                <div className="space-y-2">
                  <Skeleton className="h-4 w-24" />
                  <Skeleton className="h-16" />
                </div>
                <div className="space-y-2">
                  <Skeleton className="h-4 w-16" />
                  <Skeleton className="h-16" />
                </div>
                <div className="space-y-2">
                  <Skeleton className="h-4 w-32" />
                  <Skeleton className="h-7 w-40" />
                </div>
              </CardBody>
            </Card>
          </Section>

          <Section title="Used by">
            <Card>
              <CardBody className="min-h-28">
                <Skeleton className="h-20" />
              </CardBody>
            </Card>
          </Section>

          <Section title="Use this plugin">
            <Card>
              <CardBody className="space-y-4 min-h-32">
                <Skeleton className="h-10" />
                <div className="flex flex-wrap gap-3">
                  <Skeleton className="h-9 w-48" />
                  <Skeleton className="h-9 w-44" />
                </div>
              </CardBody>
            </Card>
          </Section>
        </div>

        <div className="space-y-6">
          <Section
            title={
              <span className="text-lg font-medium font-sans">
                File details
              </span>
            }
          >
            <Card>
              <CardBody className="space-y-3 text-sm min-h-64">
                <div className="flex justify-between">
                  <Skeleton className="h-4 w-20" />
                  <Skeleton className="h-4 w-16" />
                </div>
                <div className="flex justify-between">
                  <Skeleton className="h-4 w-12" />
                  <Skeleton className="h-4 w-20" />
                </div>
                <div className="flex justify-between">
                  <Skeleton className="h-4 w-16" />
                  <Skeleton className="h-4 w-32" />
                </div>
                <div className="flex justify-between">
                  <Skeleton className="h-4 w-20" />
                  <Skeleton className="h-4 w-24" />
                </div>
                <div className="flex justify-between">
                  <Skeleton className="h-4 w-16" />
                  <Skeleton className="h-4 w-32" />
                </div>
              </CardBody>
            </Card>
          </Section>

          <Section
            title={
              <span className="text-lg font-medium">Manage this plugin</span>
            }
          >
            <Card>
              <CardBody className="space-y-4 min-h-72">
                <Skeleton className="h-16" />
                <Skeleton className="h-20" />
                <Skeleton className="h-16" />
              </CardBody>
            </Card>
          </Section>
        </div>
      </div>
    </div>
  );
}

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
      {selectedPluginId && reg.isLoading ? (
        <PluginDetailSkeleton onBack={() => setSelectedPluginId(null)} />
      ) : selectedPlugin ? (
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
