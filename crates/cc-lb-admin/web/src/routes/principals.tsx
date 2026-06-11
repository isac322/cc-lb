import {
  closestCenter,
  DndContext,
  type DragEndEvent,
  KeyboardSensor,
  PointerSensor,
  useSensor,
  useSensors,
} from '@dnd-kit/core';
import {
  SortableContext,
  sortableKeyboardCoordinates,
  useSortable,
  verticalListSortingStrategy,
} from '@dnd-kit/sortable';
import { CSS } from '@dnd-kit/utilities';
import { createFileRoute, useNavigate } from '@tanstack/react-router';
import {
  CheckCircle2,
  ChevronLeft,
  Copy,
  Filter,
  GripVertical,
  Info,
  KeyRound,
  Plus,
  Trash2,
  X,
  XCircle,
} from 'lucide-react';
import React, { useEffect, useMemo, useState } from 'react';
import { toast } from 'sonner';
import { Drawer } from 'vaul';
import { z } from 'zod';
import {
  Badge,
  Button,
  Card,
  CardBody,
  CardHeader,
  ConfirmDialog,
  cx,
  EmptyState,
  Field,
  Hint,
  INPUT_CLASS,
  Modal,
  Skeleton,
  Spinner,
  StatusBadge,
} from '../components/ui/primitives';
import { RelativeTime } from '../components/ui/RelativeTime';
import { RequestEventsTable } from '../components/ui/RequestEventsTable';
import {
  type PluginEntry,
  type Principal,
  useCreatePrincipal,
  useDeleteChainEntry,
  useDeletePrincipal,
  useInsertChainEntry,
  useIssueKey,
  usePluginChain,
  usePluginRegistry,
  usePrincipalKeys,
  usePrincipalLimits,
  usePrincipalNameMap,
  usePrincipals,
  useRecentEvents,
  useReorderChain,
  useRevokeKey,
  useRouterTerminalStrategy,
  useSetAllowedModels,
  useTogglePrincipal,
  useUpdateRouterTerminalStrategy,
  useUpstreamNameMap,
} from '../lib/queries';

const principalSearchSchema = z.object({ selectedId: z.string().optional() });

export const Route = createFileRoute('/principals')({
  validateSearch: principalSearchSchema,
  component: PrincipalsPage,
});

function PrincipalsPage() {
  const { selectedId } = Route.useSearch();
  const navigate = useNavigate({ from: Route.fullPath });
  const principals = usePrincipals();
  const [createOpen, setCreateOpen] = useState(false);

  const selected =
    principals.data?.principals.find((p) => p.id === selectedId) ?? null;
  const select = (id: string | undefined) =>
    navigate({ search: id ? { selectedId: id } : {} });

  useEffect(() => {
    if (
      !principals.isLoading &&
      !selected &&
      principals.data?.principals &&
      principals.data.principals.length > 0
    ) {
      if (window.matchMedia('(min-width: 768px)').matches) {
        navigate({
          search: { selectedId: principals.data.principals[0].id },
          replace: true,
        });
      }
    }
  }, [principals.isLoading, selected, principals.data?.principals, navigate]);

  return (
    <div className="h-[calc(100dvh-3rem)] min-h-0 flex w-full max-w-[120rem] mx-auto">
      <aside
        className={cx(
          'border-r border-subtle flex flex-col min-h-0 w-full md:w-[360px] shrink-0',
          selected ? 'hidden md:flex' : 'flex',
        )}
      >
        <div className="h-12 px-4 flex items-center justify-between border-b border-subtle shrink-0">
          <div>
            <h1 className="text-sm font-medium">Principals</h1>
            <p className="text-[11px] text-text-faint">
              {principals.data?.principals.length ?? 0} total
            </p>
          </div>
          <Button
            id="btn-new-principal"
            size="sm"
            variant="primary"
            iconLeft={<Plus className="w-3 h-3" />}
            onClick={() => setCreateOpen(true)}
          >
            New
          </Button>
        </div>
        <div className="flex-1 overflow-y-auto p-2 pb-8 space-y-1">
          {principals.isLoading ? (
            Array.from({ length: 4 }).map((_, i) => (
              <Skeleton key={i} className="h-20" />
            ))
          ) : principals.data?.principals.length ? (
            principals.data.principals.map((p) => (
              <button
                key={p.id}
                type="button"
                onClick={() => select(p.id)}
                className={cx(
                  'w-full text-left p-3 rounded-sm border transition-colors',
                  p.id === selectedId
                    ? 'border-accent/40 bg-accent/5'
                    : 'border-subtle hover:bg-overlay-3',
                )}
              >
                <div className="flex items-center justify-between gap-2 mb-1">
                  <div className="flex items-center gap-2 min-w-0">
                    <span
                      className={cx('status-dot', p.enabled ? 'ok' : 'neutral')}
                    />
                    <span className="font-medium text-sm truncate">
                      {p.name}
                    </span>
                  </div>
                  <Badge tone="mono">{p.kind}</Badge>
                </div>
                <div className="flex items-center justify-between text-[11px] text-text-faint mt-2">
                  <span>
                    {p.allowed_models.length}{' '}
                    {p.allowed_models.length === 1 ? 'model' : 'models'} ·{' '}
                    {p.default_limits.length}{' '}
                    {p.default_limits.length === 1 ? 'limit' : 'limits'}
                  </span>
                  <span className="font-mono tabular-nums">
                    rev {p.revision}
                  </span>
                </div>
              </button>
            ))
          ) : (
            <EmptyState
              title="No principals"
              description="Create your first principal."
              action={
                <Button variant="primary" onClick={() => setCreateOpen(true)}>
                  New principal
                </Button>
              }
            />
          )}
        </div>
      </aside>

      <section
        className={cx(
          'flex-1 flex flex-col bg-bg min-w-0',
          selected ? 'flex' : 'hidden md:flex',
        )}
      >
        {selected ? (
          <PrincipalDetail
            principal={selected}
            onBack={() => select(undefined)}
          />
        ) : (
          <div className="flex-1 flex items-center justify-center">
            <EmptyState
              title="Select a principal"
              description="Pick a principal to see allowed models, default limits, plugin chain, and API keys."
            />
          </div>
        )}
      </section>

      <CreatePrincipalModal open={createOpen} onOpenChange={setCreateOpen} />
    </div>
  );
}

function PrincipalDetail({
  principal,
  onBack,
}: {
  principal: Principal;
  onBack: () => void;
}) {
  const toggle = useTogglePrincipal();
  const del = useDeletePrincipal();
  const [confirmDeleteOpen, setConfirmDeleteOpen] = useState(false);
  return (
    <>
      <header className="px-4 md:px-6 py-4 border-b border-subtle flex items-start justify-between gap-3 flex-wrap shrink-0">
        <div className="min-w-0">
          <button
            type="button"
            onClick={onBack}
            className="md:hidden inline-flex items-center gap-1 text-xs text-text-faint hover:text-text mb-1"
          >
            <ChevronLeft className="w-3 h-3" /> Back
          </button>
          <div className="flex items-center gap-3 flex-wrap">
            <h2 className="text-lg font-medium text-text truncate">
              {principal.name}
            </h2>
            <StatusBadge
              tone={principal.enabled ? 'ok' : 'neutral'}
              label={principal.enabled ? 'Enabled' : 'Disabled'}
            />
            <Badge tone="mono">{principal.kind}</Badge>
          </div>
          <div className="text-xs text-text-faint font-mono mt-0.5">
            ID {principal.id} · rev {principal.revision}
          </div>
        </div>
        <div className="flex items-center gap-2 shrink-0">
          <Button
            size="sm"
            onClick={() =>
              toggle.mutate(
                {
                  id: principal.id,
                  enabled: !principal.enabled,
                  revision: principal.revision,
                },
                {
                  onSuccess: () =>
                    toast.success(
                      principal.enabled
                        ? 'Principal disabled'
                        : 'Principal enabled',
                    ),
                },
              )
            }
          >
            {principal.enabled ? 'Disable' : 'Enable'}
          </Button>
          <Button
            size="sm"
            variant="danger"
            iconLeft={<Trash2 className="w-3 h-3" />}
            onClick={() => setConfirmDeleteOpen(true)}
          >
            Delete
          </Button>
        </div>
      </header>
      <ConfirmDialog
        open={confirmDeleteOpen}
        onOpenChange={setConfirmDeleteOpen}
        title="Delete principal?"
        description={
          <>
            <span className="font-mono">{principal.name}</span> and all its API
            keys / plugin chain entries will be permanently removed.
          </>
        }
        confirmLabel="Delete"
        destructive
        onConfirm={() =>
          del.mutate(
            { id: principal.id, revision: principal.revision },
            {
              onSuccess: () => {
                toast.success('Principal deleted');
                onBack();
              },
            },
          )
        }
      />

      <div className="flex-1 overflow-y-auto p-4 md:p-6 pb-8 md:pb-12 space-y-6">
        <AllowedModelsCard principal={principal} />
        <DefaultLimitsCard principal={principal} />
        <LiveLimitsCard principal={principal} />
        <RecentRequestsCard principal={principal} />
        <PluginChainCard principal={principal} />
        <ApiKeysCard principal={principal} />
      </div>
    </>
  );
}

function RecentRequestsCard({ principal }: { principal: Principal }) {
  const principalNameMap = usePrincipalNameMap();
  const upstreamNameMap = useUpstreamNameMap();
  const recent = useRecentEvents({
    principal_id: principal.id,
    limit: '5',
  });
  const events = recent.data?.events ?? [];
  return (
    <Card>
      <CardHeader
        title="Recent Requests"
        subtitle={
          events.length === 0
            ? `No recent requests from ${principal.name}`
            : `Last ${events.length} from ${principal.name}`
        }
      />
      <div className="overflow-x-auto">
        <RequestEventsTable
          events={events}
          principalNameMap={principalNameMap}
          upstreamNameMap={upstreamNameMap}
          loading={recent.isLoading}
          columns={{
            principal: false,
            cost: true,
            tokens: true,
          }}
          minWidthClass="min-w-[820px]"
          emptyTitle="No recent requests for this principal"
        />
      </div>
    </Card>
  );
}

function AllowedModelsCard({ principal }: { principal: Principal }) {
  const setAllowed = useSetAllowedModels();
  const [models, setModels] = useState(principal.allowed_models.join(', '));
  const [editing, setEditing] = useState(false);
  return (
    <Card>
      <CardHeader
        title="Allowed Models"
        action={
          editing ? (
            <div className="flex items-center gap-2">
              <Button
                size="sm"
                onClick={() => {
                  setEditing(false);
                  setModels(principal.allowed_models.join(', '));
                }}
              >
                Cancel
              </Button>
              <Button
                size="sm"
                variant="primary"
                onClick={() =>
                  setAllowed.mutate(
                    {
                      id: principal.id,
                      models: models
                        .split(',')
                        .map((m) => m.trim())
                        .filter(Boolean),
                      expected_revision: principal.revision,
                    },
                    {
                      onSuccess: () => {
                        toast.success('Allowed models updated');
                        setEditing(false);
                      },
                    },
                  )
                }
              >
                Save
              </Button>
            </div>
          ) : (
            <Button size="sm" onClick={() => setEditing(true)}>
              Edit
            </Button>
          )
        }
      />
      <CardBody>
        {editing ? (
          <textarea
            className={`${INPUT_CLASS} min-h-[80px] font-mono`}
            value={models}
            onChange={(e) => setModels(e.target.value)}
            placeholder="comma-separated model ids"
          />
        ) : principal.allowed_models.length ? (
          <div className="flex flex-wrap gap-2">
            {principal.allowed_models.map((m) => (
              <Badge key={m} tone="mono">
                {m}
              </Badge>
            ))}
          </div>
        ) : (
          <p className="text-xs text-text-faint">
            No restrictions — principal may use any model the upstream supports.
          </p>
        )}
      </CardBody>
    </Card>
  );
}

function DefaultLimitsCard({ principal }: { principal: Principal }) {
  return (
    <Card>
      <CardHeader
        title="Default Limits"
        subtitle="Per-model RPM / TPM defaults baked into spec"
      />
      {principal.default_limits.length ? (
        <div className="overflow-x-auto">
          <table className="w-full font-mono text-xs">
            <thead className="table-header sticky top-0 z-10">
              <tr className="text-[10px] uppercase tracking-wider">
                <th className="text-left px-4 py-2">Model</th>
                <th className="text-right px-4 py-2">RPM</th>
                <th className="text-right px-4 py-2">TPM</th>
              </tr>
            </thead>
            <tbody>
              {principal.default_limits.map((l) => (
                <tr key={l.model} className="border-b border-row">
                  <td className="px-4 py-2">{l.model}</td>
                  <td className="px-4 py-2 text-right tabular-nums">
                    {l.rpm.toLocaleString()}
                  </td>
                  <td className="px-4 py-2 text-right tabular-nums">
                    {l.tpm.toLocaleString()}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <CardBody>
          <p className="text-xs text-text-faint">
            No default limits configured for this principal.
          </p>
        </CardBody>
      )}
    </Card>
  );
}

function LiveLimitsCard({ principal }: { principal: Principal }) {
  const limits = usePrincipalLimits(principal.id);
  return (
    <Card>
      <CardHeader
        title="Live Limit Snapshots"
        subtitle="Observed rate-limit headers from upstream"
      />
      <CardBody>
        {limits.isLoading ? (
          <Skeleton className="h-12" />
        ) : limits.data ? (
          <div className="space-y-3">
            {limits.data.identities.flatMap((id) =>
              id.windows.flatMap((w) =>
                w.snapshots.map((s, i) => (
                  <div
                    key={`${w.window}-${i}`}
                    className="flex items-center justify-between text-xs"
                  >
                    <div className="flex items-center gap-2">
                      <Badge tone="mono">{w.window}</Badge>
                      <span className="text-text font-mono">
                        {(s as { model?: string }).model ?? s.kind ?? 'unknown'}
                      </span>
                    </div>
                    <span className="font-mono tabular-nums text-text">
                      {s.remaining ?? '—'} / {s.limit ?? '—'}
                    </span>
                  </div>
                )),
              ),
            )}
          </div>
        ) : (
          <p className="text-xs text-text-faint">
            No live limit data observed.
          </p>
        )}
      </CardBody>
    </Card>
  );
}

function PluginChainCard({ principal }: { principal: Principal }) {
  return (
    <Card>
      <CardHeader
        title="Plugin Chain"
        subtitle="Router runs DbRouter by default; Shape inherits the router's dialect when unset; Observability hooks are chained in order."
      />
      <CardBody className="space-y-5">
        <RouterSlotEditor principalId={principal.id} />
        <ObservabilityHookEditor principalId={principal.id} />
        <ShapeSlotEditor principalId={principal.id} />
      </CardBody>
    </Card>
  );
}

function SlotRadioCard({
  name,
  desc,
  isActive,
  isMutating,
  isMutatingOther,
  isDefault,
  isNone,
  badge,
  onClick,
}: {
  name: string;
  desc: string;
  isActive: boolean;
  isMutating: boolean;
  isMutatingOther: boolean;
  isDefault?: boolean;
  isNone?: boolean;
  badge?: string;
  onClick: () => void;
}) {
  return (
    <li role="radio" aria-checked={isActive}>
      <label
        className={cx(
          'flex items-start gap-3 p-3 border rounded-md cursor-pointer transition-colors',
          isActive
            ? cx(
                'border-accent bg-[color-mix(in_srgb,var(--color-accent)_8%,transparent)]',
                isNone && 'border-dashed',
              )
            : 'border-subtle hover:bg-overlay-3',
          isMutatingOther ? 'pointer-events-none opacity-50' : '',
        )}
      >
        <input
          type="radio"
          className="hidden"
          checked={isActive}
          onChange={onClick}
        />
        <div className="mt-1 flex items-center justify-center w-4 h-4 shrink-0">
          {isMutating ? (
            <Spinner className="w-3 h-3 text-accent" />
          ) : isNone ? (
            <span className="text-text-faint text-xs leading-none">⊘</span>
          ) : isDefault ? (
            <span className="w-1.5 h-1.5 rounded-full border border-text-faint" />
          ) : (
            <span className={cx('status-dot', isActive ? 'ok' : 'neutral')} />
          )}
        </div>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2">
            <span className="font-medium text-sm truncate">{name}</span>
            {badge && <Badge tone="mono">{badge}</Badge>}
          </div>
          <div className="text-xs text-text-faint mt-0.5">{desc}</div>
        </div>
      </label>
    </li>
  );
}

function FlowConnector({ caption }: { caption: string }) {
  return (
    <div className="flex flex-col items-center justify-center py-1">
      <div className="w-px h-3 bg-subtle" />
      <div className="text-[10px] text-text-faint flex items-center gap-1">
        <span className="text-[8px]">↓</span> {caption}
      </div>
      <div className="w-px h-3 bg-subtle" />
    </div>
  );
}

function PluginDetailDrawer({
  plugin,
  open,
  onOpenChange,
}: {
  plugin: PluginEntry | null;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}) {
  if (!plugin) return null;

  return (
    <Drawer.Root open={open} onOpenChange={onOpenChange} direction="right">
      <Drawer.Portal>
        <Drawer.Overlay className="fixed inset-0 z-40 bg-drawer-backdrop" />
        <Drawer.Content className="fixed right-0 top-0 bottom-0 w-full max-w-lg bg-bg-sub border-l border-subtle z-50 flex flex-col">
          <Drawer.Title className="sr-only">Plugin detail</Drawer.Title>
          <Drawer.Description className="sr-only">
            Detail view of a plugin
          </Drawer.Description>

          <div className="p-4 border-b border-subtle flex items-start justify-between gap-3">
            <div className="min-w-0">
              <div className="flex items-center gap-2 mb-1">
                <h2 className="text-lg font-medium text-text truncate">
                  {plugin.name}
                </h2>
                {plugin.kind && <Badge tone="accent">[{plugin.kind}]</Badge>}
                {plugin.is_builtin && <Badge tone="accent">Built-in</Badge>}
              </div>
            </div>
            <button
              type="button"
              aria-label="Close"
              onClick={() => onOpenChange(false)}
              className="text-text-muted hover:text-text shrink-0"
            >
              <X className="w-4 h-4" />
            </button>
          </div>

          <div className="flex-1 overflow-y-auto p-4 pb-8 space-y-6 text-sm">
            {plugin.metadata ? (
              <>
                <section data-testid="plugin-purpose">
                  <h3 className="text-xs font-medium text-text-faint uppercase tracking-wider mb-2">
                    Purpose
                  </h3>
                  <p className="text-text">{plugin.metadata.purpose}</p>
                </section>

                <section data-testid="plugin-keeps">
                  <h3 className="text-xs font-medium text-text-faint uppercase tracking-wider mb-2 flex items-center gap-1">
                    <CheckCircle2 className="w-3.5 h-3.5 text-emerald-500" />{' '}
                    Keeps
                  </h3>
                  <p className="text-text">{plugin.metadata.keeps}</p>
                </section>

                <section data-testid="plugin-drops">
                  <h3 className="text-xs font-medium text-text-faint uppercase tracking-wider mb-2 flex items-center gap-1">
                    <XCircle className="w-3.5 h-3.5 text-red-400" /> Drops
                  </h3>
                  <p className="text-text">{plugin.metadata.drops}</p>
                </section>

                <section data-testid="plugin-empty-behavior">
                  <h3 className="text-xs font-medium text-text-faint uppercase tracking-wider mb-2">
                    Empty behavior
                  </h3>
                  <p className="text-text">{plugin.metadata.empty_behavior}</p>
                </section>

                <section data-testid="plugin-examples">
                  <h3 className="text-xs font-medium text-text-faint uppercase tracking-wider mb-2">
                    Examples
                  </h3>
                  <ul className="list-disc pl-4 space-y-1 text-text">
                    {plugin.metadata.examples.map((ex, i) => (
                      <li key={i}>{ex}</li>
                    ))}
                  </ul>
                </section>
              </>
            ) : (
              <Card>
                <CardBody>
                  <p className="text-text-faint italic">
                    Built by operator. No description was supplied with this
                    plugin.
                  </p>
                </CardBody>
              </Card>
            )}

            <section className="pt-4 border-t border-subtle">
              <h3 className="text-xs font-medium text-text-faint uppercase tracking-wider mb-2">
                Technicals
              </h3>
              <div className="space-y-1.5 text-xs font-mono text-text-faint">
                {plugin.wire_version !== undefined && (
                  <div>wire_version: {plugin.wire_version}</div>
                )}
                <div>sha256: {plugin.sha256_hex.slice(0, 16)}...</div>
                <div className="flex items-center gap-2">
                  id: {plugin.id}
                  <button
                    type="button"
                    aria-label="Copy plugin id"
                    className="hover:text-text"
                    onClick={() => {
                      navigator.clipboard.writeText(plugin.id);
                      toast.success('Plugin ID copied');
                    }}
                  >
                    <Copy className="w-3 h-3" />
                  </button>
                </div>
              </div>
            </section>
          </div>
        </Drawer.Content>
      </Drawer.Portal>
    </Drawer.Root>
  );
}

function RouterChainItem({
  id,
  order,
  plugin,
  onDelete,
  onInfo,
}: {
  id: string;
  order: number;
  plugin: PluginEntry;
  onDelete: () => void;
  onInfo: () => void;
}) {
  const {
    attributes,
    listeners,
    setNodeRef,
    transform,
    transition,
    isDragging,
  } = useSortable({ id });
  const style = {
    transform: CSS.Transform.toString(transform),
    transition,
    opacity: isDragging ? 0.5 : 1,
  } as React.CSSProperties;

  return (
    <li
      ref={setNodeRef}
      style={style}
      data-testid={`chain-row-${id}`}
      className="flex items-center gap-3 p-3 border border-subtle rounded-sm bg-overlay-1"
    >
      <button
        type="button"
        {...attributes}
        {...listeners}
        aria-label="Drag to reorder"
        className="text-text-faint hover:text-text cursor-grab active:cursor-grabbing shrink-0"
      >
        <GripVertical className="w-4 h-4" />
      </button>
      <Badge tone="mono">#{Math.floor(order)}</Badge>
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2">
          {plugin.kind && <Badge tone="accent">[{plugin.kind}]</Badge>}
          <span className="text-sm font-medium truncate">{plugin.name}</span>
          {plugin.is_builtin && <Badge tone="accent">Built-in</Badge>}
        </div>
        <div className="text-[11px] text-text-faint truncate mt-0.5">
          {plugin.metadata ? (
            plugin.metadata.purpose
          ) : (
            <span className="italic">
              User-uploaded filter (no description supplied).
            </span>
          )}
        </div>
      </div>
      <div className="flex items-center gap-2 shrink-0">
        <button
          type="button"
          aria-label="Plugin details"
          className="text-text-faint hover:text-text"
          onClick={onInfo}
        >
          <Info className="w-4 h-4" />
        </button>
        <Hint
          label={
            plugin.is_builtin
              ? "Removing only affects this principal's chain. Registry entry remains."
              : 'Remove plugin'
          }
        >
          <button
            type="button"
            aria-label="Remove plugin"
            className="text-text-faint hover:text-red-400"
            onClick={onDelete}
          >
            <Trash2 className="w-4 h-4" />
          </button>
        </Hint>
      </div>
    </li>
  );
}

export function useRouterChainController(principalId: string) {
  const slot = 'router' as const;
  const label = 'Router';
  const desc = 'Filters candidates and picks the upstream. Executed in order.';
  const chain = usePluginChain(principalId, slot);
  const registry = usePluginRegistry();
  const reorder = useReorderChain();
  const insert = useInsertChainEntry();
  const del = useDeleteChainEntry();
  const terminalStrategy = useRouterTerminalStrategy(principalId);
  const updateTerminalStrategy = useUpdateRouterTerminalStrategy();

  const [addOpen, setAddOpen] = useState(false);
  const [selectedPluginId, setSelectedPluginId] = useState<string>('');
  const [pendingRemove, setPendingRemove] = useState<{
    id: string;
    revision: number;
    name: string;
  } | null>(null);
  const [detailPlugin, setDetailPlugin] = useState<PluginEntry | null>(null);

  const sensors = useSensors(
    useSensor(PointerSensor),
    useSensor(KeyboardSensor, {
      coordinateGetter: sortableKeyboardCoordinates,
    }),
  );

  const entries = useMemo(
    () => [...(chain.data?.entries ?? [])].sort((a, b) => a.order - b.order),
    [chain.data],
  );

  const onDragEnd = (e: DragEndEvent) => {
    if (!e.over || e.over.id === e.active.id) return;
    const ids = entries.map((x) => x.id);
    const oldIx = ids.indexOf(String(e.active.id));
    const newIx = ids.indexOf(String(e.over.id));
    if (oldIx < 0 || newIx < 0) return;
    const reordered = [...entries];
    const [moved] = reordered.splice(oldIx, 1);
    reordered.splice(newIx, 0, moved!);

    const movedPluginId = moved!.wasm_registry_id;
    const movedPluginName =
      registry.data?.entries.find((r) => r.id === movedPluginId)?.name ??
      movedPluginId;

    reorder.mutate(
      {
        pid: principalId,
        entries: reordered.map((x, i) => ({
          id: x.id,
          order: (i + 1) * 100,
          expected_revision: x.revision,
        })),
      },
      {
        onSuccess: () => {
          if (newIx === 0) {
            toast.success(
              `Reordered: ${movedPluginName} is now first in chain.`,
            );
          } else if (newIx === reordered.length - 1) {
            toast.success(
              `Reordered: ${movedPluginName} is now last filter (runs right before terminal).`,
            );
          } else {
            const nextPluginId = reordered[newIx + 1]!.wasm_registry_id;
            const nextPluginName =
              registry.data?.entries.find((r) => r.id === nextPluginId)?.name ??
              nextPluginId;
            toast.success(
              `Reordered: ${movedPluginName} now executes before ${nextPluginName}.`,
            );
          }
        },
      },
    );
  };

  return {
    principalId,
    slot,
    label,
    desc,
    chain,
    registry,
    reorder,
    insert,
    del,
    terminalStrategy,
    updateTerminalStrategy,
    addOpen,
    setAddOpen,
    selectedPluginId,
    setSelectedPluginId,
    pendingRemove,
    setPendingRemove,
    detailPlugin,
    setDetailPlugin,
    sensors,
    entries,
    onDragEnd,
  };
}

function RouterChainVariantA_Compact({
  ctrl,
}: {
  ctrl: ReturnType<typeof useRouterChainController>;
}) {
  return (
    <div>
      <div className="flex items-end justify-between mb-4">
        <div>
          <div className="text-sm font-medium text-text">{ctrl.label}</div>
          <div className="text-[11px] text-text-faint">{ctrl.desc}</div>
        </div>
        <Button
          size="sm"
          iconLeft={<Plus className="w-3 h-3" />}
          onClick={() => ctrl.setAddOpen(true)}
        >
          Add
        </Button>
      </div>

      <div
        data-testid="pipeline-summary"
        className="mb-4 p-3 border border-subtle rounded-sm bg-overlay-1 font-mono text-xs text-text-faint"
      >
        <div className="text-text mb-1">Pipeline</div>
        <div>
          N upstreams enter → {ctrl.entries.length} filters → Terminal selector
        </div>
      </div>

      {ctrl.entries.length === 0 ? (
        <div className="mb-4">
          <EmptyState
            icon={<Filter className="w-6 h-6 text-text-faint" />}
            title="No filters active"
            description="Requests flow directly to the terminal selector. Every upstream candidate is considered."
            action={
              <Button
                size="sm"
                variant="primary"
                onClick={() => ctrl.setAddOpen(true)}
              >
                Add
              </Button>
            }
          />
        </div>
      ) : (
        <DndContext
          sensors={ctrl.sensors}
          collisionDetection={closestCenter}
          onDragEnd={ctrl.onDragEnd}
        >
          <SortableContext
            items={ctrl.entries.map((e) => e.id)}
            strategy={verticalListSortingStrategy}
          >
            <ul className="space-y-0">
              {ctrl.entries.map((e, i) => {
                const reg = ctrl.registry.data?.entries.find(
                  (r) => r.id === e.wasm_registry_id,
                );
                return (
                  <React.Fragment key={e.id}>
                    <RouterChainItem
                      id={e.id}
                      order={e.order}
                      plugin={
                        reg ?? {
                          id: e.wasm_registry_id,
                          name: e.wasm_registry_id,
                          sha256_hex: '',
                          original_filename: '',
                          label: null,
                          size_bytes: 0,
                          refcount: 0,
                          revision: 0,
                          uploaded_at_unix_secs: 0,
                          metadata: null,
                        }
                      }
                      onDelete={() =>
                        ctrl.setPendingRemove({
                          id: e.id,
                          revision: e.revision,
                          name: reg?.name ?? e.wasm_registry_id,
                        })
                      }
                      onInfo={() => ctrl.setDetailPlugin(reg ?? null)}
                    />
                    <FlowConnector
                      caption={
                        i === ctrl.entries.length - 1
                          ? 'remaining candidates'
                          : 'passes to next filter'
                      }
                    />
                  </React.Fragment>
                );
              })}
            </ul>
          </SortableContext>
        </DndContext>
      )}

      {/* Locked Terminal Row */}
      <div className="flex flex-col gap-2 p-3 border border-subtle rounded-sm bg-overlay-2">
        <div className="flex items-center gap-2">
          <div className="w-4 h-4 flex items-center justify-center text-text-faint">
            <span className="w-1.5 h-1.5 rounded-full border border-text-faint" />
          </div>
          <Badge tone="mono">Terminal</Badge>
          <span className="flex-1 text-sm font-medium truncate text-text-faint flex items-center gap-2">
            Final upstream selection
            <Hint label="The terminal stage chooses the final upstream from whatever candidates remain after all filters.">
              <Info className="w-3.5 h-3.5" />
            </Hint>
          </span>
          <select
            aria-label="Terminal strategy"
            className={cx(INPUT_CLASS, 'w-auto py-1 text-xs')}
            value={ctrl.terminalStrategy.data?.strategy ?? 'first-pick'}
            disabled={
              ctrl.terminalStrategy.isLoading ||
              ctrl.updateTerminalStrategy.isPending
            }
            onChange={(e) => {
              if (!ctrl.terminalStrategy.data) return;
              ctrl.updateTerminalStrategy.mutate(
                {
                  id: ctrl.principalId,
                  strategy: e.target.value,
                  revision: ctrl.terminalStrategy.data.revision,
                },
                {
                  onSuccess: () => toast.success('Terminal strategy updated'),
                },
              );
            }}
          >
            <option value="first-pick">First-pick</option>
            <option value="random">Random</option>
          </select>
        </div>
        <div className="text-[11px] text-text-faint italic ml-8">
          {ctrl.terminalStrategy.data?.strategy === 'random'
            ? 'Picks one survivor uniformly at random.'
            : 'Always picks the first survivor (deterministic).'}
        </div>
      </div>
      <FlowConnector caption="1 upstream → dispatched" />
    </div>
  );
}

function RouterChainVariantB_Verbose({
  ctrl,
}: {
  ctrl: ReturnType<typeof useRouterChainController>;
}) {
  return (
    <div className="space-y-4">
      <div className="flex items-end justify-between">
        <div>
          <div className="text-sm font-medium text-text">{ctrl.label}</div>
          <div className="text-[11px] text-text-faint">{ctrl.desc}</div>
        </div>
        <Button
          size="sm"
          iconLeft={<Plus className="w-3 h-3" />}
          onClick={() => ctrl.setAddOpen(true)}
        >
          Add
        </Button>
      </div>

      {ctrl.entries.length === 0 ? (
        <EmptyState
          icon={<Filter className="w-6 h-6 text-text-faint" />}
          title="No filters active"
          description="Requests flow directly to the terminal selector. Every upstream candidate is considered."
          action={
            <Button
              size="sm"
              variant="primary"
              onClick={() => ctrl.setAddOpen(true)}
            >
              Add
            </Button>
          }
        />
      ) : (
        <DndContext
          sensors={ctrl.sensors}
          collisionDetection={closestCenter}
          onDragEnd={ctrl.onDragEnd}
        >
          <SortableContext
            items={ctrl.entries.map((e) => e.id)}
            strategy={verticalListSortingStrategy}
          >
            <ul className="space-y-4">
              {ctrl.entries.map((e) => {
                const reg = ctrl.registry.data?.entries.find(
                  (r) => r.id === e.wasm_registry_id,
                );
                const plugin = reg ?? {
                  id: e.wasm_registry_id,
                  name: e.wasm_registry_id,
                  sha256_hex: '',
                  original_filename: '',
                  label: null,
                  size_bytes: 0,
                  refcount: 0,
                  revision: 0,
                  uploaded_at_unix_secs: 0,
                  metadata: null,
                };
                return (
                  <VerboseChainItem
                    key={e.id}
                    id={e.id}
                    order={e.order}
                    plugin={plugin}
                    onDelete={() =>
                      ctrl.setPendingRemove({
                        id: e.id,
                        revision: e.revision,
                        name: plugin.name,
                      })
                    }
                  />
                );
              })}
            </ul>
          </SortableContext>
        </DndContext>
      )}

      <Card>
        <CardHeader title="Terminal" subtitle="Final upstream selection" />
        <CardBody>
          <div className="flex items-center gap-4">
            <select
              aria-label="Terminal strategy"
              className={cx(INPUT_CLASS, 'w-auto py-1 text-xs')}
              value={ctrl.terminalStrategy.data?.strategy ?? 'first-pick'}
              disabled={
                ctrl.terminalStrategy.isLoading ||
                ctrl.updateTerminalStrategy.isPending
              }
              onChange={(e) => {
                if (!ctrl.terminalStrategy.data) return;
                ctrl.updateTerminalStrategy.mutate(
                  {
                    id: ctrl.principalId,
                    strategy: e.target.value,
                    revision: ctrl.terminalStrategy.data.revision,
                  },
                  {
                    onSuccess: () => toast.success('Terminal strategy updated'),
                  },
                );
              }}
            >
              <option value="first-pick">First-pick</option>
              <option value="random">Random</option>
            </select>
            <div className="text-[11px] text-text-faint italic">
              {ctrl.terminalStrategy.data?.strategy === 'random'
                ? 'Picks one survivor uniformly at random.'
                : 'Always picks the first survivor (deterministic).'}
            </div>
          </div>
        </CardBody>
      </Card>
    </div>
  );
}

function VerboseChainItem({
  id,
  order,
  plugin,
  onDelete,
}: {
  id: string;
  order: number;
  plugin: PluginEntry;
  onDelete: () => void;
}) {
  const {
    attributes,
    listeners,
    setNodeRef,
    transform,
    transition,
    isDragging,
  } = useSortable({ id });
  const style = {
    transform: CSS.Transform.toString(transform),
    transition,
    opacity: isDragging ? 0.5 : 1,
  } as React.CSSProperties;

  return (
    <li ref={setNodeRef} style={style} data-testid={`chain-row-${id}`}>
      <Card>
        <div className="p-4 border-b border-subtle flex items-center justify-between">
          <div className="flex items-center gap-3">
            <button
              type="button"
              {...attributes}
              {...listeners}
              aria-label="Drag to reorder"
              className="text-text-faint hover:text-text cursor-grab active:cursor-grabbing"
            >
              <GripVertical className="w-4 h-4" />
            </button>
            <Badge tone="mono">#{Math.floor(order)}</Badge>
            {plugin.kind && <Badge tone="accent">[{plugin.kind}]</Badge>}
            <span className="font-medium">{plugin.name}</span>
            {plugin.is_builtin && <Badge tone="accent">Built-in</Badge>}
          </div>
          <Hint
            label={
              plugin.is_builtin
                ? "Removing only affects this principal's chain. Registry entry remains."
                : 'Remove plugin'
            }
          >
            <button
              type="button"
              aria-label="Remove plugin"
              className="text-text-faint hover:text-red-400"
              onClick={onDelete}
            >
              <Trash2 className="w-4 h-4" />
            </button>
          </Hint>
        </div>
        <CardBody className="space-y-4 text-sm">
          {plugin.metadata ? (
            <>
              <div>
                <span className="text-xs font-medium text-text-faint uppercase tracking-wider mr-2">
                  Purpose
                </span>
                <span className="text-text">{plugin.metadata.purpose}</span>
              </div>
              <div>
                <span className="text-xs font-medium text-text-faint uppercase tracking-wider mr-2">
                  Keeps
                </span>
                <span className="text-text">{plugin.metadata.keeps}</span>
              </div>
              <div>
                <span className="text-xs font-medium text-text-faint uppercase tracking-wider mr-2">
                  Drops
                </span>
                <span className="text-text">{plugin.metadata.drops}</span>
              </div>
              <div>
                <span className="text-xs font-medium text-text-faint uppercase tracking-wider mr-2">
                  Empty behavior
                </span>
                <span className="text-text">
                  {plugin.metadata.empty_behavior}
                </span>
              </div>
            </>
          ) : (
            <p className="text-text-faint italic">
              Built by operator. No description was supplied with this plugin.
            </p>
          )}
        </CardBody>
        <div className="px-4 py-2 bg-overlay-1 border-t border-subtle text-[10px] font-mono text-text-faint flex items-center gap-4">
          {plugin.wire_version !== undefined && (
            <span>wire_version: {plugin.wire_version}</span>
          )}
          <span>sha256: {plugin.sha256_hex.slice(0, 12)}</span>
        </div>
      </Card>
    </li>
  );
}

function RouterChainVariantC_Flow({
  ctrl,
}: {
  ctrl: ReturnType<typeof useRouterChainController>;
}) {
  return (
    <div className="space-y-2">
      <div className="flex items-end justify-between mb-4">
        <div>
          <div className="text-sm font-medium text-text">{ctrl.label}</div>
          <div className="text-[11px] text-text-faint">{ctrl.desc}</div>
        </div>
        <Button
          size="sm"
          iconLeft={<Plus className="w-3 h-3" />}
          onClick={() => ctrl.setAddOpen(true)}
        >
          Add
        </Button>
      </div>

      <div className="flex justify-center mb-2">
        <Badge tone="mono">[N in]</Badge>
      </div>

      {ctrl.entries.length === 0 ? (
        <div className="flex justify-center py-4">
          <span className="text-xs text-text-faint italic">No filters</span>
        </div>
      ) : (
        <DndContext
          sensors={ctrl.sensors}
          collisionDetection={closestCenter}
          onDragEnd={ctrl.onDragEnd}
        >
          <SortableContext
            items={ctrl.entries.map((e) => e.id)}
            strategy={verticalListSortingStrategy}
          >
            <ul className="space-y-0">
              {ctrl.entries.map((e) => {
                const reg = ctrl.registry.data?.entries.find(
                  (r) => r.id === e.wasm_registry_id,
                );
                const plugin = reg ?? {
                  id: e.wasm_registry_id,
                  name: e.wasm_registry_id,
                  sha256_hex: '',
                  original_filename: '',
                  label: null,
                  size_bytes: 0,
                  refcount: 0,
                  revision: 0,
                  uploaded_at_unix_secs: 0,
                  metadata: null,
                };

                // Heuristic for keep fraction
                const keepFraction =
                  plugin.name === 'cache-affinity' ? 0.5 : 1.0;

                return (
                  <React.Fragment key={e.id}>
                    <FlowChainItem
                      id={e.id}
                      plugin={plugin}
                      keepFraction={keepFraction}
                      onDelete={() =>
                        ctrl.setPendingRemove({
                          id: e.id,
                          revision: e.revision,
                          name: plugin.name,
                        })
                      }
                      onInfo={() => ctrl.setDetailPlugin(reg ?? null)}
                    />
                    <div className="flex justify-center py-1 text-text-faint text-[10px]">
                      ▼
                    </div>
                  </React.Fragment>
                );
              })}
            </ul>
          </SortableContext>
        </DndContext>
      )}

      <div className="flex flex-col items-center gap-2 p-3 border border-subtle rounded-sm bg-overlay-2">
        <div className="flex items-center gap-2 w-full">
          <Badge tone="mono">Terminal</Badge>
          <select
            aria-label="Terminal strategy"
            className={cx(INPUT_CLASS, 'flex-1 py-1 text-xs')}
            value={ctrl.terminalStrategy.data?.strategy ?? 'first-pick'}
            disabled={
              ctrl.terminalStrategy.isLoading ||
              ctrl.updateTerminalStrategy.isPending
            }
            onChange={(e) => {
              if (!ctrl.terminalStrategy.data) return;
              ctrl.updateTerminalStrategy.mutate(
                {
                  id: ctrl.principalId,
                  strategy: e.target.value,
                  revision: ctrl.terminalStrategy.data.revision,
                },
                {
                  onSuccess: () => toast.success('Terminal strategy updated'),
                },
              );
            }}
          >
            <option value="first-pick">First-pick</option>
            <option value="random">Random</option>
          </select>
        </div>
      </div>

      <div className="flex justify-center mt-2">
        <Badge tone="mono">[1 out]</Badge>
      </div>
    </div>
  );
}

function FlowChainItem({
  id,
  plugin,
  keepFraction,
  onDelete,
  onInfo,
}: {
  id: string;
  plugin: PluginEntry;
  keepFraction: number;
  onDelete: () => void;
  onInfo: () => void;
}) {
  const {
    attributes,
    listeners,
    setNodeRef,
    transform,
    transition,
    isDragging,
  } = useSortable({ id });
  const style = {
    transform: CSS.Transform.toString(transform),
    transition,
    opacity: isDragging ? 0.5 : 1,
  } as React.CSSProperties;

  return (
    <li
      ref={setNodeRef}
      style={style}
      data-testid={`chain-row-${id}`}
      className="border border-subtle rounded-sm bg-overlay-1 p-3"
    >
      <div className="flex items-center justify-between mb-2">
        <div className="flex items-center gap-2">
          <button
            type="button"
            {...attributes}
            {...listeners}
            aria-label="Drag to reorder"
            className="text-text-faint hover:text-text cursor-grab active:cursor-grabbing"
          >
            <GripVertical className="w-4 h-4" />
          </button>
          <span className="font-medium text-sm">{plugin.name}</span>
        </div>
        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={onInfo}
            className="text-text-faint hover:text-text"
          >
            <Info className="w-4 h-4" />
          </button>
          <button
            type="button"
            onClick={onDelete}
            className="text-text-faint hover:text-red-400"
          >
            <Trash2 className="w-4 h-4" />
          </button>
        </div>
      </div>
      <div className="h-2 w-full bg-overlay-3 rounded-full overflow-hidden flex">
        <div
          className="h-full bg-accent"
          style={{ width: `${keepFraction * 100}%` }}
        />
      </div>
      <div className="text-[10px] text-text-faint mt-1 text-center">
        ~{keepFraction === 1 ? 'M' : 'M/2'} kept
      </div>
    </li>
  );
}

function RouterChainVariantD_Narrative({
  ctrl,
}: {
  ctrl: ReturnType<typeof useRouterChainController>;
}) {
  const moveUp = (index: number) => {
    if (index === 0) return;
    const reordered = [...ctrl.entries];
    const temp = reordered[index - 1];
    reordered[index - 1] = reordered[index]!;
    reordered[index] = temp!;

    ctrl.reorder.mutate({
      pid: ctrl.principalId,
      entries: reordered.map((x, i) => ({
        id: x.id,
        order: (i + 1) * 100,
        expected_revision: x.revision,
      })),
    });
  };

  const moveDown = (index: number) => {
    if (index === ctrl.entries.length - 1) return;
    const reordered = [...ctrl.entries];
    const temp = reordered[index + 1];
    reordered[index + 1] = reordered[index]!;
    reordered[index] = temp!;

    ctrl.reorder.mutate({
      pid: ctrl.principalId,
      entries: reordered.map((x, i) => ({
        id: x.id,
        order: (i + 1) * 100,
        expected_revision: x.revision,
      })),
    });
  };

  return (
    <div className="space-y-4">
      <div className="flex items-end justify-between">
        <div>
          <div className="text-sm font-medium text-text">{ctrl.label}</div>
          <div className="text-[11px] text-text-faint">{ctrl.desc}</div>
        </div>
        <Button
          size="sm"
          iconLeft={<Plus className="w-3 h-3" />}
          onClick={() => ctrl.setAddOpen(true)}
        >
          Add
        </Button>
      </div>

      <Card>
        <CardBody className="text-sm leading-relaxed space-y-2">
          <p>
            When a request arrives, <strong>N upstream candidates</strong> are
            considered.
          </p>

          {ctrl.entries.length === 0 ? (
            <p>There are no filters active.</p>
          ) : (
            <ul className="space-y-2">
              {ctrl.entries.map((e, i) => {
                const reg = ctrl.registry.data?.entries.find(
                  (r) => r.id === e.wasm_registry_id,
                );
                const plugin = reg ?? {
                  id: e.wasm_registry_id,
                  name: e.wasm_registry_id,
                  sha256_hex: '',
                  original_filename: '',
                  label: null,
                  size_bytes: 0,
                  refcount: 0,
                  revision: 0,
                  uploaded_at_unix_secs: 0,
                  metadata: null,
                };

                return (
                  <li
                    key={e.id}
                    className="flex items-center gap-2 flex-wrap"
                    data-testid={`chain-row-${e.id}`}
                  >
                    <span>Step {i + 1} &middot;</span>
                    <button
                      type="button"
                      className="font-bold text-accent hover:underline"
                      onClick={() => ctrl.setDetailPlugin(reg ?? null)}
                    >
                      {plugin.name}
                    </button>
                    <span>
                      {plugin.metadata?.purpose ??
                        'User-uploaded filter (no description supplied).'}
                    </span>
                    <div className="flex items-center gap-1 ml-auto">
                      <button
                        type="button"
                        className="text-[10px] px-1.5 py-0.5 border border-subtle rounded hover:bg-overlay-3 disabled:opacity-30"
                        onClick={() => moveUp(i)}
                        disabled={i === 0}
                      >
                        ↑
                      </button>
                      <button
                        type="button"
                        className="text-[10px] px-1.5 py-0.5 border border-subtle rounded hover:bg-overlay-3 disabled:opacity-30"
                        onClick={() => moveDown(i)}
                        disabled={i === ctrl.entries.length - 1}
                      >
                        ↓
                      </button>
                      <button
                        type="button"
                        className="text-[10px] px-1.5 py-0.5 border border-red-900/30 text-red-400 rounded hover:bg-red-900/20"
                        onClick={() =>
                          ctrl.setPendingRemove({
                            id: e.id,
                            revision: e.revision,
                            name: plugin.name,
                          })
                        }
                      >
                        Remove
                      </button>
                    </div>
                  </li>
                );
              })}
            </ul>
          )}

          <div className="flex items-center gap-2 flex-wrap pt-2">
            <span>Finally, the</span>
            <select
              aria-label="Terminal strategy"
              className={cx(
                INPUT_CLASS,
                'w-auto py-0.5 px-2 text-xs font-bold',
              )}
              value={ctrl.terminalStrategy.data?.strategy ?? 'first-pick'}
              disabled={
                ctrl.terminalStrategy.isLoading ||
                ctrl.updateTerminalStrategy.isPending
              }
              onChange={(e) => {
                if (!ctrl.terminalStrategy.data) return;
                ctrl.updateTerminalStrategy.mutate(
                  {
                    id: ctrl.principalId,
                    strategy: e.target.value,
                    revision: ctrl.terminalStrategy.data.revision,
                  },
                  {
                    onSuccess: () => toast.success('Terminal strategy updated'),
                  },
                );
              }}
            >
              <option value="first-pick">first-pick</option>
              <option value="random">random</option>
            </select>
            <span>terminal picks one and dispatches.</span>
          </div>
        </CardBody>
      </Card>
    </div>
  );
}

export function RouterChainVariantSwitcher({
  variant,
  setVariant,
}: {
  variant: string;
  setVariant: (v: string) => void;
}) {
  return (
    <div className="flex items-center gap-1 p-1 bg-overlay-1 border border-subtle rounded-md w-fit mb-4">
      <Hint label="Compact: 1-line row + drawer + locked terminal row + summary header">
        <button
          type="button"
          className={cx(
            'px-3 py-1 text-xs rounded-sm transition-colors',
            variant === 'A'
              ? 'bg-overlay-3 text-text shadow-sm'
              : 'text-text-faint hover:text-text',
          )}
          onClick={() => setVariant('A')}
        >
          Compact
        </button>
      </Hint>
      <Hint label="Verbose: Each entry is a Card with inline details">
        <button
          type="button"
          className={cx(
            'px-3 py-1 text-xs rounded-sm transition-colors',
            variant === 'B'
              ? 'bg-overlay-3 text-text shadow-sm'
              : 'text-text-faint hover:text-text',
          )}
          onClick={() => setVariant('B')}
        >
          Verbose
        </button>
      </Hint>
      <Hint label="Flow: Column layout with funnel bars and connectors">
        <button
          type="button"
          className={cx(
            'px-3 py-1 text-xs rounded-sm transition-colors',
            variant === 'C'
              ? 'bg-overlay-3 text-text shadow-sm'
              : 'text-text-faint hover:text-text',
          )}
          onClick={() => setVariant('C')}
        >
          Flow
        </button>
      </Hint>
      <Hint label="Narrative: Prose paragraph(s) inside a single Card">
        <button
          type="button"
          className={cx(
            'px-3 py-1 text-xs rounded-sm transition-colors',
            variant === 'D'
              ? 'bg-overlay-3 text-text shadow-sm'
              : 'text-text-faint hover:text-text',
          )}
          onClick={() => setVariant('D')}
        >
          Narrative
        </button>
      </Hint>
    </div>
  );
}

export function RouterSlotEditor({ principalId }: { principalId: string }) {
  const [variant, setVariant] = useState(() => {
    if (typeof window !== 'undefined') {
      return localStorage.getItem('cc-lb.router-chain-variant') || 'A';
    }
    return 'A';
  });

  useEffect(() => {
    localStorage.setItem('cc-lb.router-chain-variant', variant);
  }, [variant]);

  const ctrl = useRouterChainController(principalId);

  return (
    <div>
      <RouterChainVariantSwitcher variant={variant} setVariant={setVariant} />
      {variant === 'A' && <RouterChainVariantA_Compact ctrl={ctrl} />}
      {variant === 'B' && <RouterChainVariantB_Verbose ctrl={ctrl} />}
      {variant === 'C' && <RouterChainVariantC_Flow ctrl={ctrl} />}
      {variant === 'D' && <RouterChainVariantD_Narrative ctrl={ctrl} />}

      <PluginDetailDrawer
        plugin={ctrl.detailPlugin}
        open={ctrl.detailPlugin !== null}
        onOpenChange={(open) => {
          if (!open) ctrl.setDetailPlugin(null);
        }}
      />

      <Modal
        open={ctrl.addOpen}
        onOpenChange={ctrl.setAddOpen}
        title={`Add plugin to ${ctrl.label}`}
        footer={
          <>
            <Button onClick={() => ctrl.setAddOpen(false)}>Cancel</Button>
            <Button
              variant="primary"
              disabled={!ctrl.selectedPluginId}
              onClick={() =>
                ctrl.insert.mutate(
                  {
                    pid: ctrl.principalId,
                    body: {
                      slot: ctrl.slot,
                      wasm_registry_id: ctrl.selectedPluginId,
                      order: (ctrl.entries.length + 1) * 100,
                    },
                  },
                  {
                    onSuccess: () => {
                      toast.success('Plugin added');
                      ctrl.setAddOpen(false);
                      ctrl.setSelectedPluginId('');
                    },
                  },
                )
              }
            >
              Add
            </Button>
          </>
        }
      >
        <Field label="Plugin" required>
          <select
            className={INPUT_CLASS}
            value={ctrl.selectedPluginId}
            onChange={(e) => ctrl.setSelectedPluginId(e.target.value)}
          >
            <option value="">— select —</option>
            {ctrl.registry.data?.entries.map((p) => (
              <option key={p.id} value={p.id}>
                {p.name}
              </option>
            ))}
          </select>
        </Field>
      </Modal>

      <ConfirmDialog
        open={ctrl.pendingRemove !== null}
        onOpenChange={(o) => {
          if (!o) ctrl.setPendingRemove(null);
        }}
        title="Remove plugin from chain?"
        description={
          ctrl.pendingRemove ? (
            <>
              <span className="font-mono">{ctrl.pendingRemove.name}</span> will
              be removed from the {ctrl.label} chain. You can re-add it later.
            </>
          ) : null
        }
        confirmLabel="Remove"
        destructive
        onConfirm={() => {
          if (!ctrl.pendingRemove) return;
          ctrl.del.mutate(
            {
              id: ctrl.pendingRemove.id,
              revision: ctrl.pendingRemove.revision,
            },
            { onSuccess: () => toast.success('Plugin removed from chain') },
          );
          ctrl.setPendingRemove(null);
        }}
      />
    </div>
  );
}
function ShapeSlotEditor({ principalId }: { principalId: string }) {
  const slot = 'shape';
  const label = 'Shape';
  const desc = 'Request/response transform';
  const chain = usePluginChain(principalId, slot);
  const registry = usePluginRegistry();
  const insert = useInsertChainEntry();
  const del = useDeleteChainEntry();

  const entries = useMemo(
    () => [...(chain.data?.entries ?? [])].sort((a, b) => a.order - b.order),
    [chain.data],
  );

  const activeEntry = entries[0];
  const hasMultiple = entries.length > 1;

  const [mutatingId, setMutatingId] = useState<string | null>(null);

  const handleSelect = async (pluginId: string | null) => {
    if (mutatingId) return;
    const currentPluginId = activeEntry?.wasm_registry_id ?? null;
    if (pluginId === currentPluginId) return;

    setMutatingId(pluginId ?? 'none');

    try {
      let currentEntries = entries;
      let retryCount = 0;

      while (retryCount < 2) {
        try {
          for (const entry of currentEntries) {
            await del.mutateAsync({ id: entry.id, revision: entry.revision });
          }

          if (pluginId) {
            await insert.mutateAsync({
              pid: principalId,
              body: { slot, wasm_registry_id: pluginId, order: 100 },
            });
          }

          const pluginName =
            registry.data?.entries.find((e) => e.id === pluginId)?.name ??
            pluginId;
          toast.success(
            pluginId ? `Shape set to ${pluginName}` : 'Shape disabled',
          );
          break;
        } catch (err) {
          const error = err as {
            status?: number;
            message?: string;
            error?: string;
          };
          const isConflict =
            error?.status === 409 ||
            error?.message?.includes('revision_conflict') ||
            error?.message?.includes('slot_singleton') ||
            error?.error === 'slot_singleton';
          if (isConflict && retryCount === 0) {
            retryCount++;
            const freshChain = await chain.refetch();
            currentEntries = [...(freshChain.data?.entries ?? [])].sort(
              (a, b) => a.order - b.order,
            );
            continue;
          }
          throw err;
        }
      }
    } catch (err) {
      const error = err as { message?: string };
      toast.error(
        `Failed to update shape: ${error.message || 'Unknown error'}`,
      );
    } finally {
      setMutatingId(null);
    }
  };

  const candidates = registry.data?.entries ?? [];

  return (
    <div>
      <div className="flex items-end justify-between mb-3">
        <div>
          <div className="flex items-center gap-2">
            <div className="text-sm font-medium text-text">{label}</div>
            {hasMultiple && (
              <Hint label="Database invariant violated: multiple shape entries detected. Selecting a new option will clear them.">
                <Badge tone="warn">Multiple entries detected</Badge>
              </Hint>
            )}
          </div>
          <div className="text-[11px] text-text-faint">{desc}</div>
        </div>
      </div>
      <ul
        className="space-y-2"
        role="radiogroup"
        aria-busy={mutatingId !== null}
      >
        <SlotRadioCard
          name="None"
          desc="Inherits the dialect returned by the router (typically anthropic-direct)."
          isActive={!activeEntry}
          isMutating={mutatingId === 'none'}
          isMutatingOther={mutatingId !== null && mutatingId !== 'none'}
          isNone
          badge="Off"
          onClick={() => handleSelect(null)}
        />
        {/* TODO(slot-filter): once usePluginStatus carries slot metadata reliably, filter candidates by slot. */}
        {candidates.map((p) => (
          <SlotRadioCard
            key={p.id}
            name={p.name}
            desc={p.label || 'Custom shape plugin'}
            isActive={activeEntry?.wasm_registry_id === p.id}
            isMutating={mutatingId === p.id}
            isMutatingOther={mutatingId !== null && mutatingId !== p.id}
            onClick={() => handleSelect(p.id)}
          />
        ))}
      </ul>
    </div>
  );
}

function ObservabilityHookEditor({ principalId }: { principalId: string }) {
  const slot = 'observability_hook';
  const label = 'Observability';
  const desc = 'SSE / audit hooks. Executed in order. Multiple allowed.';
  const chain = usePluginChain(principalId, slot);
  const registry = usePluginRegistry();
  const reorder = useReorderChain();
  const insert = useInsertChainEntry();
  const del = useDeleteChainEntry();
  const [addOpen, setAddOpen] = useState(false);
  const [selectedPluginId, setSelectedPluginId] = useState<string>('');
  const [pendingRemove, setPendingRemove] = useState<{
    id: string;
    revision: number;
    name: string;
  } | null>(null);

  const sensors = useSensors(
    useSensor(PointerSensor),
    useSensor(KeyboardSensor, {
      coordinateGetter: sortableKeyboardCoordinates,
    }),
  );

  const entries = useMemo(
    () => [...(chain.data?.entries ?? [])].sort((a, b) => a.order - b.order),
    [chain.data],
  );

  const onDragEnd = (e: DragEndEvent) => {
    if (!e.over || e.over.id === e.active.id) return;
    const ids = entries.map((x) => x.id);
    const oldIx = ids.indexOf(String(e.active.id));
    const newIx = ids.indexOf(String(e.over.id));
    if (oldIx < 0 || newIx < 0) return;
    const reordered = [...entries];
    const [moved] = reordered.splice(oldIx, 1);
    reordered.splice(newIx, 0, moved!);
    reorder.mutate(
      {
        pid: principalId,
        entries: reordered.map((x, i) => ({
          id: x.id,
          order: (i + 1) * 100,
          expected_revision: x.revision,
        })),
      },
      { onSuccess: () => toast.success('Chain reordered') },
    );
  };

  return (
    <div>
      <div className="flex items-end justify-between mb-2">
        <div>
          <div className="text-sm font-medium text-text">{label}</div>
          <div className="text-[11px] text-text-faint">{desc}</div>
        </div>
        <Button
          size="sm"
          iconLeft={<Plus className="w-3 h-3" />}
          onClick={() => setAddOpen(true)}
        >
          Add
        </Button>
      </div>
      {entries.length ? (
        <DndContext
          sensors={sensors}
          collisionDetection={closestCenter}
          onDragEnd={onDragEnd}
        >
          <SortableContext
            items={entries.map((e) => e.id)}
            strategy={verticalListSortingStrategy}
          >
            <ul className="space-y-1.5">
              {entries.map((e) => {
                const reg = registry.data?.entries.find(
                  (r) => r.id === e.wasm_registry_id,
                );
                return (
                  <SortableChainItem
                    key={e.id}
                    id={e.id}
                    order={e.order}
                    name={reg?.name ?? e.wasm_registry_id}
                    onDelete={() =>
                      setPendingRemove({
                        id: e.id,
                        revision: e.revision,
                        name: reg?.name ?? e.wasm_registry_id,
                      })
                    }
                  />
                );
              })}
            </ul>
          </SortableContext>
        </DndContext>
      ) : (
        <button
          type="button"
          className="w-full border border-dashed border-subtle rounded-sm py-4 text-xs text-text-faint hover:text-text hover:border-[color:var(--color-border-strong)]"
          onClick={() => setAddOpen(true)}
        >
          + Set {label.toLowerCase()} plugin
        </button>
      )}

      <Modal
        open={addOpen}
        onOpenChange={setAddOpen}
        title={`Add plugin to ${label}`}
        footer={
          <>
            <Button onClick={() => setAddOpen(false)}>Cancel</Button>
            <Button
              variant="primary"
              disabled={!selectedPluginId}
              onClick={() =>
                insert.mutate(
                  {
                    pid: principalId,
                    body: { slot, wasm_registry_id: selectedPluginId },
                  },
                  {
                    onSuccess: () => {
                      toast.success('Plugin added');
                      setAddOpen(false);
                      setSelectedPluginId('');
                    },
                  },
                )
              }
            >
              Add
            </Button>
          </>
        }
      >
        <Field label="Plugin" required>
          <select
            className={INPUT_CLASS}
            value={selectedPluginId}
            onChange={(e) => setSelectedPluginId(e.target.value)}
          >
            <option value="">— select —</option>
            {registry.data?.entries.map((p) => (
              <option key={p.id} value={p.id}>
                {p.name}
              </option>
            ))}
          </select>
        </Field>
      </Modal>

      <ConfirmDialog
        open={pendingRemove !== null}
        onOpenChange={(o) => {
          if (!o) setPendingRemove(null);
        }}
        title="Remove plugin from chain?"
        description={
          pendingRemove ? (
            <>
              <span className="font-mono">{pendingRemove.name}</span> will be
              removed from the {label} chain. You can re-add it later.
            </>
          ) : null
        }
        confirmLabel="Remove"
        destructive
        onConfirm={() => {
          if (!pendingRemove) return;
          del.mutate(
            { id: pendingRemove.id, revision: pendingRemove.revision },
            { onSuccess: () => toast.success('Plugin removed from chain') },
          );
          setPendingRemove(null);
        }}
      />
    </div>
  );
}

function SortableChainItem({
  id,
  order,
  name,
  isBuiltin,
  onDelete,
}: {
  id: string;
  order: number;
  name: string;
  isBuiltin?: boolean;
  onDelete: () => void;
}) {
  const {
    attributes,
    listeners,
    setNodeRef,
    transform,
    transition,
    isDragging,
  } = useSortable({ id });
  const style = {
    transform: CSS.Transform.toString(transform),
    transition,
    opacity: isDragging ? 0.5 : 1,
  } as React.CSSProperties;
  return (
    <li
      ref={setNodeRef}
      style={style}
      className="flex items-center gap-2 p-2 border border-subtle rounded-sm bg-overlay-1"
    >
      <button
        type="button"
        {...attributes}
        {...listeners}
        aria-label="Drag to reorder"
        className="text-text-faint hover:text-text cursor-grab active:cursor-grabbing"
      >
        <GripVertical className="w-4 h-4" />
      </button>
      <Badge tone="mono">#{Math.floor(order)}</Badge>
      <span className="flex-1 text-sm font-medium truncate flex items-center gap-2">
        {name}
        {isBuiltin && <Badge tone="accent">Built-in</Badge>}
      </span>
      <Hint
        label={
          isBuiltin
            ? "Removing only affects this principal's chain. Registry entry remains."
            : 'Remove plugin'
        }
      >
        <button
          type="button"
          aria-label="Remove plugin"
          className="text-text-faint hover:text-red-400"
          onClick={onDelete}
        >
          <Trash2 className="w-4 h-4" />
        </button>
      </Hint>
    </li>
  );
}

function ApiKeysCard({ principal }: { principal: Principal }) {
  const keys = usePrincipalKeys(principal.id);
  const issue = useIssueKey();
  const revoke = useRevokeKey();
  const [issueOpen, setIssueOpen] = useState(false);
  const [label, setLabel] = useState('');
  const [issued, setIssued] = useState<{
    plaintext_key: string;
    key_id: string;
  } | null>(null);
  const [pendingRevoke, setPendingRevoke] = useState<{
    key_id: string;
    label: string;
  } | null>(null);

  return (
    <Card>
      <CardHeader
        title="API Keys"
        subtitle="Issue, view fingerprints, revoke"
        action={
          <Button
            size="sm"
            iconLeft={<KeyRound className="w-3 h-3" />}
            onClick={() => setIssueOpen(true)}
          >
            Issue Key
          </Button>
        }
      />
      <div className="overflow-x-auto">
        {keys.isLoading ? (
          <CardBody>
            <Skeleton className="h-12" />
          </CardBody>
        ) : keys.data?.keys.length ? (
          <table className="w-full font-mono text-xs">
            <thead className="table-header sticky top-0 z-10">
              <tr className="text-[10px] uppercase tracking-wider">
                <th className="text-left px-4 py-2">Label</th>
                <th className="text-left px-4 py-2">Key ID</th>
                <th className="text-left px-4 py-2">Last 4</th>
                <th className="text-left px-4 py-2">Issued</th>
                <th className="text-left px-4 py-2">Last Used</th>
                <th className="text-left px-4 py-2">Status</th>
                <th className="px-4 py-2 w-8"></th>
              </tr>
            </thead>
            <tbody>
              {keys.data.keys.map((k) => (
                <tr key={k.key_id} className="border-b border-row">
                  <td className="px-4 py-2">{k.label ?? '—'}</td>
                  <td className="px-4 py-2">{k.key_id}</td>
                  <td className="px-4 py-2 text-text-faint">
                    {k.last_4 ? `···${k.last_4}` : '—'}
                  </td>
                  <td className="px-4 py-2">
                    <RelativeTime ts={new Date(k.issued_at_unix_secs * 1000)} />
                  </td>
                  <td className="px-4 py-2">
                    <RelativeTime
                      ts={
                        k.last_used_at_unix_secs
                          ? new Date(k.last_used_at_unix_secs * 1000)
                          : null
                      }
                    />
                  </td>
                  <td className="px-4 py-2">
                    {k.revoked_at_unix_secs ? (
                      <StatusBadge tone="danger" label="Revoked" />
                    ) : (
                      <StatusBadge tone="ok" label="Active" />
                    )}
                  </td>
                  <td className="px-4 py-2 text-right">
                    {!k.revoked_at_unix_secs ? (
                      <button
                        type="button"
                        className="text-text-faint hover:text-red-400"
                        aria-label="Revoke key"
                        onClick={() =>
                          setPendingRevoke({
                            key_id: k.key_id,
                            label: k.label ?? k.key_id,
                          })
                        }
                      >
                        <Trash2 className="w-3.5 h-3.5" />
                      </button>
                    ) : null}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : (
          <CardBody>
            <p className="text-xs text-text-faint">No API keys issued.</p>
          </CardBody>
        )}
      </div>

      <Modal
        open={issueOpen}
        onOpenChange={(o) => {
          setIssueOpen(o);
          if (!o) {
            setLabel('');
            setIssued(null);
          }
        }}
        title={issued ? 'API key issued' : 'Issue API key'}
        footer={
          issued ? (
            <Button
              variant="primary"
              onClick={() => {
                setIssueOpen(false);
                setLabel('');
                setIssued(null);
              }}
            >
              Done
            </Button>
          ) : (
            <>
              <Button onClick={() => setIssueOpen(false)}>Cancel</Button>
              <Button
                variant="primary"
                onClick={() =>
                  issue.mutate(
                    { id: principal.id, label },
                    { onSuccess: (res) => setIssued(res) },
                  )
                }
              >
                Issue
              </Button>
            </>
          )
        }
      >
        {issued ? (
          <div className="space-y-3">
            <p className="text-xs text-text-faint">
              Copy the key now. It will not be shown again.
            </p>
            <div className="flex items-center gap-2">
              <code className="flex-1 p-2 text-xs font-mono bg-overlay-2 border border-subtle rounded-sm break-all">
                {issued.plaintext_key}
              </code>
              <Button
                size="sm"
                iconLeft={<Copy className="w-3 h-3" />}
                onClick={() => {
                  navigator.clipboard.writeText(issued.plaintext_key);
                  toast.success('Copied');
                }}
              >
                Copy
              </Button>
            </div>
          </div>
        ) : (
          <Field
            label="Label"
            hint="Human-readable name, e.g. 'github-actions'"
          >
            <input
              className={INPUT_CLASS}
              value={label}
              onChange={(e) => setLabel(e.target.value)}
              placeholder="laptop, ci, prod-app"
            />
          </Field>
        )}
      </Modal>

      <ConfirmDialog
        open={pendingRevoke !== null}
        onOpenChange={(o) => {
          if (!o) setPendingRevoke(null);
        }}
        title="Revoke API key?"
        description={
          pendingRevoke ? (
            <>
              Key <span className="font-mono">{pendingRevoke.label}</span> will
              stop authenticating new requests immediately.
            </>
          ) : null
        }
        confirmLabel="Revoke"
        destructive
        onConfirm={() => {
          if (!pendingRevoke) return;
          revoke.mutate(
            { id: principal.id, key_id: pendingRevoke.key_id },
            { onSuccess: () => toast.success('Key revoked') },
          );
          setPendingRevoke(null);
        }}
      />
    </Card>
  );
}

function CreatePrincipalModal({
  open,
  onOpenChange,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}) {
  const create = useCreatePrincipal();
  const [name, setName] = useState('');
  const [kind, setKind] = useState<'machine' | 'human' | 'admin'>('human');
  const reset = () => {
    setName('');
    setKind('human');
  };
  return (
    <Modal
      open={open}
      onOpenChange={(o) => {
        onOpenChange(o);
        if (!o) reset();
      }}
      title="New principal"
      footer={
        <>
          <Button onClick={() => onOpenChange(false)}>Cancel</Button>
          <Button
            variant="primary"
            disabled={!name.trim()}
            onClick={() =>
              create.mutate(
                { name, kind },
                {
                  onSuccess: () => {
                    toast.success('Principal created');
                    onOpenChange(false);
                    reset();
                  },
                },
              )
            }
          >
            Create
          </Button>
        </>
      }
    >
      <div className="space-y-3">
        <Field label="Name" required>
          <input
            className={INPUT_CLASS}
            value={name}
            onChange={(e) => setName(e.target.value)}
            placeholder="engineering-shared"
          />
        </Field>
        <Field label="Kind" required>
          <select
            className={INPUT_CLASS}
            value={kind}
            onChange={(e) => setKind(e.target.value as typeof kind)}
          >
            <option value="human">human</option>
            <option value="machine">machine</option>
            <option value="admin">admin</option>
          </select>
        </Field>
      </div>
    </Modal>
  );
}
