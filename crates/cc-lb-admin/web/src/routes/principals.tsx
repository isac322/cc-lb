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
  ChevronLeft,
  Copy,
  GripVertical,
  KeyRound,
  Plus,
  Trash2,
} from 'lucide-react';
import { useEffect, useMemo, useState } from 'react';
import { toast } from 'sonner';
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
  INPUT_CLASS,
  Modal,
  Skeleton,
  StatusBadge,
} from '../components/ui/primitives';
import { RelativeTime } from '../components/ui/RelativeTime';
import { RequestEventsTable } from '../components/ui/RequestEventsTable';
import {
  type ChainSlot,
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
  useSetAllowedModels,
  useTogglePrincipal,
  useUpstreamNameMap,
} from '../lib/queries';

const principalSearchSchema = z.object({ selectedId: z.string().optional() });

export const Route = createFileRoute('/principals')({
  validateSearch: principalSearchSchema,
  component: PrincipalsPage,
});

const SLOTS: { id: ChainSlot; label: string; desc: string }[] = [
  { id: 'router', label: 'Router', desc: 'Picks the upstream' },
  {
    id: 'observability_hook',
    label: 'Observability',
    desc: 'SSE / audit hooks',
  },
  { id: 'shape', label: 'Shape', desc: 'Request/response transform' },
];

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
        subtitle="Three slots: router, observability hooks, shape. Drag to reorder."
      />
      <CardBody className="space-y-5">
        {SLOTS.map((slot) => (
          <SlotEditor
            key={slot.id}
            principalId={principal.id}
            slot={slot.id}
            label={slot.label}
            desc={slot.desc}
          />
        ))}
      </CardBody>
    </Card>
  );
}

function SlotEditor({
  principalId,
  slot,
  label,
  desc,
}: {
  principalId: string;
  slot: ChainSlot;
  label: string;
  desc: string;
}) {
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
  onDelete,
}: {
  id: string;
  order: number;
  name: string;
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
      <span className="flex-1 text-sm font-medium truncate">{name}</span>
      <button
        type="button"
        aria-label="Remove plugin"
        className="text-text-faint hover:text-red-400"
        onClick={onDelete}
      >
        <Trash2 className="w-4 h-4" />
      </button>
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
