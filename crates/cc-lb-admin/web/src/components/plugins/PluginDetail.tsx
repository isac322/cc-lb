import { useState } from 'react';
import { type PluginEntry, usePluginReferences } from '../../lib/queries';
import { Button, cx, Section, Skeleton } from '../ui/primitives';
import { PluginDeleteDialog } from './PluginDeleteDialog';
import { PluginDetailApply } from './PluginDetailApply';
import { PluginDetailIntegrity } from './PluginDetailIntegrity';
import { PluginDetailOperate } from './PluginDetailOperate';
import { PluginDetailUnderstand } from './PluginDetailUnderstand';
import { SLOTS } from './slots/model';

// Phones stack the name over a "Principal · Router slot" caption; the name is
// a full-width 41px link that still truncates. From `sm` the kind leads the
// name and the slot sits right.
const USED_BY_ROW =
  'flex flex-col pb-3 text-body sm:flex-row sm:items-center sm:justify-between sm:gap-4 sm:py-3';

const USED_BY_NAME = 'min-w-0 truncate text-text-muted';

const USED_BY_FACT = 'shrink-0 text-body-sm text-text-muted';

const REFERENCE_LINK_CLASS =
  'font-medium text-text underline decoration-subtle-strong underline-offset-4 transition-colors hover:decoration-accent focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-2 rounded-sm max-sm:block max-sm:truncate max-sm:pt-3 max-sm:pb-2';

/** Phones list this many uses, then a "Show all N" toggle; wider screens show all. */
const PHONE_USED_BY_LIMIT = 8;

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
  const [showAllUses, setShowAllUses] = useState(false);

  return (
    <div className="space-y-12">
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-x-10 gap-y-12">
        {/* Flex rather than space-y so the Hooks section can move to the end
            of this column on phones (see PluginDetailUnderstand). */}
        <div className="lg:col-span-2 flex flex-col gap-12">
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
                <>
                  <ul className="divide-y divide-row">
                    {refs.data.references.map((r, i) => (
                      <li
                        key={i}
                        className={cx(
                          USED_BY_ROW,
                          !showAllUses && i >= PHONE_USED_BY_LIMIT
                            ? 'max-sm:hidden'
                            : undefined,
                        )}
                      >
                        {r.kind === 'plugin_chain' ? (
                          <>
                            <span className={USED_BY_NAME}>
                              <span className="max-sm:hidden">Principal </span>
                              <a
                                href={`/principals?selectedId=${r.principal_id}`}
                                className={REFERENCE_LINK_CLASS}
                              >
                                {r.principal_name ?? r.principal_id}
                              </a>
                            </span>
                            <span className={USED_BY_FACT}>
                              <span className="sm:hidden">Principal · </span>
                              {SLOTS.find((s) => s.id === r.slot)?.label ??
                                r.slot}{' '}
                              slot
                            </span>
                          </>
                        ) : (
                          <>
                            <span className={USED_BY_NAME}>
                              <span className="max-sm:hidden">Upstream </span>
                              <a
                                href={`/upstreams?selectedId=${r.upstream_id}`}
                                className={REFERENCE_LINK_CLASS}
                              >
                                {r.upstream_name ?? r.upstream_id}
                              </a>
                            </span>
                            <span className={USED_BY_FACT}>
                              <span className="sm:hidden">Upstream · </span>
                              Warmup
                            </span>
                          </>
                        )}
                      </li>
                    ))}
                  </ul>
                  {refs.data.references.length > PHONE_USED_BY_LIMIT ? (
                    <Button
                      variant="ghost"
                      size="sm"
                      className="mt-1 -ml-2.5 sm:hidden"
                      aria-expanded={showAllUses}
                      onClick={() => setShowAllUses((open) => !open)}
                    >
                      {showAllUses
                        ? 'Show fewer'
                        : `Show all ${refs.data.references.length}`}
                    </Button>
                  ) : null}
                </>
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
