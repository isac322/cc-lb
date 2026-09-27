import { useState } from 'react';
import { type PluginEntry, usePluginReferences } from '../../lib/queries';
import { Section, Skeleton } from '../ui/primitives';
import { PluginDeleteDialog } from './PluginDeleteDialog';
import { PluginDetailApply } from './PluginDetailApply';
import { PluginDetailIntegrity } from './PluginDetailIntegrity';
import { PluginDetailOperate } from './PluginDetailOperate';
import { PluginDetailUnderstand } from './PluginDetailUnderstand';
import { SLOTS } from './slots/model';

const USED_BY_ROW = 'flex items-center justify-between gap-4 py-3 text-body';

const REFERENCE_LINK_CLASS =
  'font-medium text-text underline decoration-subtle-strong underline-offset-4 transition-colors hover:decoration-accent focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-2 rounded-sm';

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
    <div className="space-y-12">
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-x-10 gap-y-12">
        <div className="lg:col-span-2 space-y-12">
          <PluginDetailUnderstand plugin={plugin} />

          <Section title="Used by">
            <div data-testid="plugin-used-by-slot">
              {refs.isLoading ? (
                <div className="divide-y divide-row" aria-hidden="true">
                  <div className={USED_BY_ROW}>
                    <Skeleton className="h-4 w-2/3" />
                    <Skeleton className="h-4 w-16 shrink-0" />
                  </div>
                </div>
              ) : refs.data?.references.length ? (
                <ul className="divide-y divide-row">
                  {refs.data.references.map((r, i) => (
                    <li key={i} className={USED_BY_ROW}>
                      {r.kind === 'plugin_chain' ? (
                        <>
                          <span className="min-w-0 truncate text-text-muted">
                            Principal{' '}
                            <a
                              href={`/principals?selectedId=${r.principal_id}`}
                              className={REFERENCE_LINK_CLASS}
                            >
                              {r.principal_name ?? r.principal_id}
                            </a>
                          </span>
                          <span className="shrink-0 text-body-sm text-text-muted">
                            {SLOTS.find((s) => s.id === r.slot)?.label ??
                              r.slot}{' '}
                            slot
                          </span>
                        </>
                      ) : (
                        <>
                          <span className="min-w-0 truncate text-text-muted">
                            Upstream{' '}
                            <a
                              href={`/upstreams?selectedId=${r.upstream_id}`}
                              className={REFERENCE_LINK_CLASS}
                            >
                              {r.upstream_name ?? r.upstream_id}
                            </a>
                          </span>
                          <span className="shrink-0 text-body-sm text-text-muted">
                            Warmup
                          </span>
                        </>
                      )}
                    </li>
                  ))}
                </ul>
              ) : (
                <p className="text-body text-text-muted">
                  This plugin is uploaded but not used anywhere yet.
                </p>
              )}
            </div>
          </Section>

          <PluginDetailApply plugin={plugin} />
        </div>

        <div className="space-y-12">
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
