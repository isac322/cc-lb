import { useNavigate } from '@tanstack/react-router';
import { useCallback, useEffect, useRef, useState } from 'react';
import { usePluginRegistry, useUploadWasm } from '../../lib/queries';
import { Route } from '../../routes/plugins';
import {
  Badge,
  Card,
  CardBody,
  Modal,
  PageContainer,
  PageHeader,
  Section,
  Skeleton,
} from '../ui/primitives';
import { BackToCatalogLink } from './BackToCatalogLink';
import { PluginCatalog } from './PluginCatalog';
import { PluginDetail } from './PluginDetail';
import { PluginUploadCard } from './PluginUploadCard';

function PluginDetailSkeleton() {
  return (
    <div
      aria-busy="true"
      aria-label="Loading plugin details"
      className="space-y-8"
      role="status"
    >
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-x-6 gap-y-8">
        <div className="lg:col-span-2 space-y-8">
          <Section title="What this plugin does">
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

        <div className="space-y-8">
          <Section title="File details">
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

          <Section title="Manage this plugin">
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
  const { plugin: selectedPluginId, action } = Route.useSearch();
  const navigate = useNavigate({ from: Route.id });
  const reg = usePluginRegistry();
  const upload = useUploadWasm();
  const handledUploadAction = useRef(false);
  const [uploadOpen, setUploadOpen] = useState(false);

  const setSelectedPluginId = (id: string | null) => {
    navigate({
      search: (previous) => ({
        ...previous,
        plugin: id || undefined,
      }),
    });
  };

  useEffect(() => {
    if (action !== 'upload') {
      handledUploadAction.current = false;
      return;
    }
    setUploadOpen(true);
  }, [action]);

  const consumeUploadAction = useCallback(() => {
    if (action !== 'upload' || handledUploadAction.current) return;
    handledUploadAction.current = true;
    navigate({
      replace: true,
      search: (previous) => ({ ...previous, action: undefined }),
    });
  }, [action, navigate]);

  const selectedPlugin = reg.data?.entries.find(
    (p) => p.id === selectedPluginId,
  );

  const handleUploaded = (id: string) => {
    setUploadOpen(false);
    setSelectedPluginId(id);
  };

  const detailLoading = Boolean(selectedPluginId) && reg.isLoading;
  const backToCatalog = () => setSelectedPluginId(null);

  return (
    <PageContainer>
      {detailLoading || selectedPlugin ? (
        <div>
          <BackToCatalogLink onBack={backToCatalog} />
          <PageHeader
            title={
              selectedPlugin ? (
                <span className="flex flex-wrap items-center gap-x-3 gap-y-1">
                  <span className="break-all">{selectedPlugin.name}</span>
                  {selectedPlugin.is_builtin && (
                    <Badge tone="neutral">Built-in</Badge>
                  )}
                </span>
              ) : (
                <>
                  <span className="sr-only">Loading plugin</span>
                  <Skeleton as="span" className="block h-7 w-52 max-w-full" />
                </>
              )
            }
          />
        </div>
      ) : (
        <PageHeader
          title="Plugins"
          description="Upload and manage WebAssembly plugins. Apply them to Principals or Upstreams to customize behavior."
        />
      )}
      {detailLoading ? (
        <PluginDetailSkeleton />
      ) : selectedPlugin ? (
        <PluginDetail plugin={selectedPlugin} onBack={backToCatalog} />
      ) : (
        <div className="space-y-8">
          <div hidden={uploadOpen}>
            <PluginUploadCard
              upload={upload}
              onUploaded={setSelectedPluginId}
            />
          </div>
          <PluginCatalog onSelectPlugin={setSelectedPluginId} />
        </div>
      )}
      <Modal
        open={uploadOpen}
        onOpenChange={setUploadOpen}
        preventDismiss={upload.isPending}
        size="lg"
        title="Upload plugin"
        description="Choose a WebAssembly plugin file, then review its capabilities and usage."
      >
        <PluginUploadCard
          upload={upload}
          bare
          autoFocus
          onAutoFocus={consumeUploadAction}
          onUploaded={handleUploaded}
        />
      </Modal>
    </PageContainer>
  );
}
