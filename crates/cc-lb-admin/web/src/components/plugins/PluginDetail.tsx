import { ArrowLeft } from 'lucide-react';
import { useState } from 'react';
import { type PluginEntry, usePluginReferences } from '../../lib/queries';
import {
  Badge,
  Button,
  Card,
  CardBody,
  Section,
  Skeleton,
} from '../ui/primitives';
import { PluginDeleteDialog } from './PluginDeleteDialog';
import { PluginDetailApply } from './PluginDetailApply';
import { PluginDetailIntegrity } from './PluginDetailIntegrity';
import { PluginDetailOperate } from './PluginDetailOperate';
import { PluginDetailUnderstand } from './PluginDetailUnderstand';

export function PluginDetail({
  plugin,
  onBack,
}: {
  plugin: PluginEntry;
  onBack: () => void;
}) {
  const refs = usePluginReferences(plugin.id);
  const [pendingDelete, setPendingDelete] = useState<{
    id: string;
    revision: number;
    name: string;
    refcount: number;
  } | null>(null);

  return (
    <div className="space-y-6 mt-6">
      <div className="flex items-center gap-4">
        <Button variant="ghost" size="sm" onClick={onBack}>
          <ArrowLeft className="w-4 h-4 mr-2" />
          Back to Catalog
        </Button>
        <h2 className="text-xl font-medium font-sans">{plugin.name}</h2>
        {plugin.is_builtin && <Badge tone="neutral">Built-in</Badge>}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <div className="lg:col-span-2 space-y-6">
          <PluginDetailUnderstand plugin={plugin} />

          <Section title="Used by">
            <Card>
              <CardBody>
                <div data-testid="plugin-used-by-slot" className="min-h-20">
                  {refs.isLoading ? (
                    <div className="space-y-2" aria-hidden="true">
                      <div className="flex items-center justify-between gap-4 p-2 border border-subtle rounded-sm bg-overlay-1">
                        <Skeleton className="h-4 w-2/3" />
                        <Skeleton className="h-5 w-16 shrink-0" />
                      </div>
                    </div>
                  ) : refs.data?.references.length ? (
                    <ul className="space-y-2">
                      {refs.data.references.map((r, i) => (
                        <li
                          key={i}
                          className="flex items-center justify-between text-sm p-2 border border-subtle rounded-sm bg-overlay-1"
                        >
                          {r.kind === 'plugin_chain' ? (
                            <>
                              <span className="flex flex-col gap-0.5">
                                <span className="text-sm">
                                  Used by principal{' '}
                                  <a
                                    href={`/principals?selectedId=${r.principal_id}`}
                                    className="text-accent hover:underline font-medium"
                                  >
                                    {r.principal_name ?? r.principal_id}
                                  </a>
                                </span>
                              </span>
                              <Badge tone="neutral">Slot: {r.slot}</Badge>
                            </>
                          ) : (
                            <>
                              <span className="flex flex-col gap-0.5">
                                <span className="text-sm">
                                  Used by upstream{' '}
                                  <a
                                    href={`/upstreams?selectedId=${r.upstream_id}`}
                                    className="text-accent hover:underline font-medium"
                                  >
                                    {r.upstream_name ?? r.upstream_id}
                                  </a>
                                </span>
                              </span>
                              <Badge tone="neutral">Warmup</Badge>
                            </>
                          )}
                        </li>
                      ))}
                    </ul>
                  ) : (
                    <p className="text-sm text-text-faint">
                      This plugin is uploaded but not used anywhere yet.
                    </p>
                  )}
                </div>
              </CardBody>
            </Card>
          </Section>

          <PluginDetailApply plugin={plugin} />
        </div>

        <div className="space-y-6">
          <PluginDetailIntegrity plugin={plugin} />

          <PluginDetailOperate
            plugin={plugin}
            onDelete={() => {
              setPendingDelete({
                id: plugin.id,
                revision: plugin.revision,
                name: plugin.name,
                refcount: plugin.refcount,
              });
            }}
          />
        </div>
      </div>

      <PluginDeleteDialog
        pendingDelete={pendingDelete}
        onClose={() => {
          setPendingDelete(null);
        }}
        onDeleted={onBack}
      />
    </div>
  );
}
