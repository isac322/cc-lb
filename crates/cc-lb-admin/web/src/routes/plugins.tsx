import { Tabs as BaseTabs } from '@base-ui/react/tabs';
import { createFileRoute } from '@tanstack/react-router';
import { Copy, Trash2, UploadCloud } from 'lucide-react';
import { useRef, useState } from 'react';
import { toast } from 'sonner';
import {
  Badge,
  Button,
  Card,
  CardHeader,
  ConfirmDialog,
  cx,
  Hint,
  PageContainer,
  Section,
  Skeleton,
} from '../components/ui/primitives';
import { RelativeTime } from '../components/ui/RelativeTime';
import {
  useDeletePlugin,
  useGcPlugins,
  usePluginRegistry,
  usePluginStatus,
  usePrincipals,
  useUploadWasm,
} from '../lib/queries';

export const Route = createFileRoute('/plugins')({
  component: PluginsPage,
});

const SLOTS: {
  id: 'router' | 'observability_hook' | 'shape';
  label: string;
}[] = [
  { id: 'router', label: 'Router' },
  { id: 'observability_hook', label: 'Observability' },
  { id: 'shape', label: 'Shape' },
];

type PluginsTab = 'registry' | 'chains' | 'status';
const TABS: { id: PluginsTab; label: string }[] = [
  { id: 'registry', label: 'Registry' },
  { id: 'chains', label: 'Chains by Principal' },
  { id: 'status', label: 'Runtime status' },
];

function PluginsPage() {
  const [tab, setTab] = useState<PluginsTab>('registry');
  return (
    <PageContainer>
      <BaseTabs.Root
        onValueChange={(value) => setTab(value as PluginsTab)}
        value={tab}
      >
        <BaseTabs.List className="flex items-center gap-1 border-b border-subtle">
          {TABS.map(({ id, label }) => (
            <BaseTabs.Tab
              className="px-3 py-2 text-sm border-b-2 -mb-px transition-colors border-transparent text-text-faint hover:text-text data-[active]:border-[color:var(--color-accent)] data-[active]:text-[color:var(--color-text)]"
              key={id}
              value={id}
            >
              {label}
            </BaseTabs.Tab>
          ))}
        </BaseTabs.List>
      </BaseTabs.Root>

      {tab === 'registry' ? (
        <RegistryTab />
      ) : tab === 'chains' ? (
        <ChainsTab />
      ) : (
        <StatusTab />
      )}
    </PageContainer>
  );
}

function StatusTab() {
  const status = usePluginStatus();
  const plugins = status.data?.plugins ?? [];
  const loaded = plugins.filter((p) => p.loaded && !p.disabled).length;
  const failing = plugins.filter((p) => p.failure_count > 0).length;
  return (
    <Section
      title="Runtime status"
      subtitle="Live state of every Extism plugin bound to a slot. Polled from /admin/status every 15 s."
      className="mt-6"
    >
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <Stat label="Total" value={plugins.length} />
        <Stat label="Loaded" value={loaded} />
        <Stat
          label="Disabled"
          value={plugins.filter((p) => p.disabled).length}
        />
        <Stat
          label="Failing"
          value={failing}
          tone={failing > 0 ? 'warn' : 'neutral'}
        />
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mt-4">
        {SLOTS.map((slot) => {
          const slotEntries = plugins.filter((p) => p.slot === slot.id);
          return (
            <Card key={slot.id}>
              <CardHeader
                title={slot.label}
                subtitle={`${slotEntries.length} plugin${slotEntries.length === 1 ? '' : 's'}`}
              />
              <div className="px-4 pb-4">
                {slotEntries.length ? (
                  <ul className="space-y-2 text-xs">
                    {slotEntries.map((p) => (
                      <li
                        key={p.name}
                        className="flex items-center justify-between gap-2 border border-subtle/60 rounded-sm px-2 py-1.5 bg-overlay-1"
                      >
                        <div className="min-w-0">
                          <div className="font-sans font-medium truncate">
                            {p.name}
                          </div>
                          <div className="text-[10px] text-text-faint truncate font-mono">
                            {p.wasm_path}
                          </div>
                          {p.last_error ? (
                            <div
                              className="text-[10px] text-red-400 truncate"
                              title={p.last_error}
                            >
                              error: {p.last_error}
                            </div>
                          ) : null}
                        </div>
                        <div className="flex flex-col items-end gap-0.5 shrink-0">
                          <Badge
                            tone={
                              p.disabled
                                ? 'danger'
                                : p.loaded
                                  ? 'ok'
                                  : 'neutral'
                            }
                          >
                            {p.disabled
                              ? 'disabled'
                              : p.loaded
                                ? 'loaded'
                                : 'unloaded'}
                          </Badge>
                          {p.failure_count > 0 ? (
                            <Badge tone="warn">{p.failure_count} fail</Badge>
                          ) : null}
                        </div>
                      </li>
                    ))}
                  </ul>
                ) : (
                  <div className="text-xs text-text-faint italic">
                    No plugins bound to this slot.
                  </div>
                )}
              </div>
            </Card>
          );
        })}
      </div>
    </Section>
  );
}

function Stat({
  label,
  value,
  tone = 'neutral',
}: {
  label: string;
  value: React.ReactNode;
  tone?: 'neutral' | 'warn';
}) {
  return (
    <div
      className={cx(
        'rounded-sm border bg-overlay-1 p-3 flex flex-col gap-0.5',
        tone === 'warn'
          ? 'border-[color:var(--color-warn)]/30'
          : 'border-subtle',
      )}
    >
      <span className="text-[10px] uppercase tracking-wider text-text-faint">
        {label}
      </span>
      <span
        className={cx(
          'text-base tabular-nums',
          tone === 'warn' ? 'text-[color:var(--color-warn)]' : '',
        )}
      >
        {value}
      </span>
    </div>
  );
}

function RegistryTab() {
  const reg = usePluginRegistry();
  const upload = useUploadWasm();
  const del = useDeletePlugin();
  const gc = useGcPlugins();
  const fileRef = useRef<HTMLInputElement>(null);
  const [pendingDelete, setPendingDelete] = useState<{
    id: string;
    revision: number;
    name: string;
  } | null>(null);

  const handleFile = (file: File | null | undefined) => {
    if (!file || upload.isPending) return;
    upload.mutate(file, {
      onSuccess: (data) => {
        const suffix = data.idempotent ? ' (already in registry)' : '';
        toast.success(`Uploaded ${data.original_filename}${suffix}`);
      },
      onError: (e) => toast.error(e instanceof Error ? e.message : String(e)),
    });
  };

  const uploading = upload.isPending;

  return (
    <div className="space-y-6 mt-6">
      <Card>
        <CardHeader
          title="Upload Wasm Plugin"
          subtitle="Drag & drop or click. Max 32 MiB. SHA-256 deduped."
          action={
            <Button
              size="sm"
              onClick={() =>
                gc.mutate(undefined, {
                  onSuccess: (r) =>
                    toast.success(`GC removed ${r.count} orphans`),
                })
              }
            >
              GC orphans
            </Button>
          }
        />
        <div
          aria-busy={uploading}
          className={cx(
            'm-4 mt-0 p-8 border border-dashed border-subtle rounded-sm flex flex-col items-center justify-center text-center transition-colors',
            uploading
              ? 'cursor-wait opacity-70 border-accent/40'
              : 'cursor-pointer hover:border-accent/40',
          )}
          onClick={() => {
            if (!uploading) fileRef.current?.click();
          }}
          onDragOver={(e) => {
            e.preventDefault();
          }}
          onDrop={(e) => {
            e.preventDefault();
            if (!uploading) handleFile(e.dataTransfer.files?.[0]);
          }}
        >
          <UploadCloud
            className={cx(
              'w-8 h-8 mb-2',
              uploading ? 'text-accent animate-pulse' : 'text-text-faint',
            )}
          />
          <div className="text-sm">
            {uploading ? 'Uploading…' : 'Upload .wasm'}
          </div>
          <div className="text-[11px] text-text-faint mt-1">
            Drag and drop or click to browse. Max 32 MiB.
          </div>
          <input
            id="btn-upload-wasm"
            ref={fileRef}
            type="file"
            accept=".wasm"
            className="hidden"
            disabled={uploading}
            onChange={(e) => {
              handleFile(e.target.files?.[0]);
              e.target.value = '';
            }}
          />
        </div>
      </Card>

      <Card>
        <CardHeader
          title="Registry"
          subtitle={`${reg.data?.entries.length ?? 0} plugins`}
        />
        <div className="overflow-x-auto">
          <table className="w-full font-mono text-xs">
            <thead className="table-header sticky top-0 z-10">
              <tr className="text-[10px] uppercase tracking-wider">
                <th className="text-left px-4 py-2">Name</th>
                <th className="text-left px-4 py-2">Slots</th>
                <th className="text-left px-4 py-2">SHA256</th>
                <th className="text-right px-4 py-2 tabular-nums">Size</th>
                <th className="text-center px-4 py-2 tabular-nums">Refs</th>
                <th className="text-left px-4 py-2">Uploaded</th>
                <th className="text-right px-4 py-2">Actions</th>
              </tr>
            </thead>
            <tbody>
              {reg.isLoading ? (
                Array.from({ length: 3 }).map((_, i) => (
                  <tr key={i}>
                    <td colSpan={7} className="px-4 py-2">
                      <Skeleton />
                    </td>
                  </tr>
                ))
              ) : reg.data?.entries.length ? (
                reg.data.entries.map((p) => (
                  <tr
                    key={p.id}
                    className="border-b border-row hover:bg-overlay-1"
                  >
                    <td className="px-4 py-2 max-w-[260px]">
                      <div
                        className="text-sm font-medium font-sans truncate"
                        title={p.name}
                      >
                        {p.name}
                      </div>
                      {p.label ? (
                        <div
                          className="text-[11px] text-text-faint truncate"
                          title={p.label}
                        >
                          {p.label}
                        </div>
                      ) : null}
                    </td>
                    <td className="px-4 py-2">
                      <div className="flex flex-wrap gap-1">
                        {p.supported_slots && p.supported_slots.length > 0 ? (
                          p.supported_slots.map((slot) => (
                            <Badge key={slot} tone="accent">
                              {SLOTS.find((s) => s.id === slot)?.label ?? slot}
                            </Badge>
                          ))
                        ) : (
                          <Badge tone="warn">Unknown</Badge>
                        )}
                      </div>
                    </td>
                    <td className="px-4 py-2">
                      <div className="flex items-center gap-1">
                        <Hint label={p.sha256_hex}>
                          <code className="cursor-help">
                            {p.sha256_hex.slice(0, 12)}…
                          </code>
                        </Hint>
                        <button
                          type="button"
                          aria-label="Copy SHA256"
                          className="text-text-faint hover:text-text"
                          onClick={() => {
                            navigator.clipboard.writeText(p.sha256_hex);
                            toast.success('SHA256 copied');
                          }}
                        >
                          <Copy className="w-3 h-3" />
                        </button>
                      </div>
                    </td>
                    <td className="px-4 py-2 text-right tabular-nums">
                      {(p.size_bytes / 1024).toFixed(1)} KB
                    </td>
                    <td className="px-4 py-2 text-center">
                      <Badge tone={p.refcount > 0 ? 'accent' : 'neutral'}>
                        {p.refcount}
                      </Badge>
                    </td>
                    <td className="px-4 py-2">
                      <RelativeTime
                        compact
                        ts={new Date(p.uploaded_at_unix_secs * 1000)}
                      />
                    </td>
                    <td className="px-4 py-2 text-right">
                      <button
                        type="button"
                        aria-label={
                          p.refcount > 0
                            ? 'Cannot delete — plugin is referenced'
                            : 'Delete plugin'
                        }
                        title={
                          p.refcount > 0
                            ? `In use by ${p.refcount} chain entr${p.refcount === 1 ? 'y' : 'ies'}`
                            : 'Delete plugin'
                        }
                        disabled={p.refcount > 0}
                        className={cx(
                          'transition-colors',
                          p.refcount > 0
                            ? 'text-text-faint/40 cursor-not-allowed'
                            : 'text-text-faint hover:text-red-400',
                        )}
                        onClick={() => {
                          if (p.refcount > 0) return;
                          setPendingDelete({
                            id: p.id,
                            revision: p.revision,
                            name: p.name,
                          });
                        }}
                      >
                        <Trash2 className="w-3.5 h-3.5" />
                      </button>
                    </td>
                  </tr>
                ))
              ) : (
                <tr>
                  <td
                    colSpan={7}
                    className="px-4 py-8 text-center text-text-faint text-xs"
                  >
                    No plugins uploaded.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </Card>

      <ConfirmDialog
        open={pendingDelete !== null}
        onOpenChange={(o) => {
          if (!o) setPendingDelete(null);
        }}
        title="Delete Wasm plugin?"
        description={
          pendingDelete ? (
            <>
              <span className="font-mono">{pendingDelete.name}</span> will be
              removed from the registry. This cannot be undone.
            </>
          ) : null
        }
        confirmLabel="Delete"
        destructive
        onConfirm={() => {
          if (!pendingDelete) return;
          del.mutate(
            { id: pendingDelete.id, revision: pendingDelete.revision },
            {
              onSuccess: () => toast.success('Plugin deleted'),
              onError: (e) => toast.error(String(e)),
            },
          );
          setPendingDelete(null);
        }}
      />
    </div>
  );
}

function ChainsTab() {
  const principals = usePrincipals();
  const reg = usePluginRegistry();

  return (
    <Section
      title="Chains by Principal"
      subtitle="Matrix view: rows = principals, columns = slot. Each cell shows ordered plugin names."
      className="mt-6"
    >
      <Card>
        <div className="overflow-x-auto">
          <table className="w-full font-mono text-xs">
            <thead className="table-header sticky top-0 z-10">
              <tr className="text-[10px] uppercase tracking-wider">
                <th className="text-left px-4 py-2 sticky left-0 bg-bg-sub z-10">
                  Principal
                </th>
                {SLOTS.map((s) => (
                  <th key={s.id} className="text-left px-4 py-2 min-w-[200px]">
                    {s.label}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {principals.isLoading || reg.isLoading ? (
                <tr>
                  <td
                    colSpan={4}
                    className="px-4 py-6 text-center text-text-faint"
                  >
                    Loading…
                  </td>
                </tr>
              ) : principals.data?.principals.length ? (
                principals.data.principals.map((p) => (
                  <PrincipalRow
                    key={p.id}
                    principalId={p.id}
                    principalName={p.name}
                  />
                ))
              ) : (
                <tr>
                  <td
                    colSpan={4}
                    className="px-4 py-6 text-center text-text-faint"
                  >
                    No principals
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </Card>
    </Section>
  );
}

function PrincipalRow({
  principalId,
  principalName,
}: {
  principalId: string;
  principalName: string;
}) {
  const reg = usePluginRegistry();
  return (
    <tr className="border-b border-row">
      <td className="px-4 py-3 sticky left-0 bg-bg-sub z-10 font-sans font-medium">
        <a
          href={`/principals?selectedId=${principalId}`}
          className="hover:text-accent"
        >
          {principalName}
        </a>
      </td>
      {SLOTS.map((s) => (
        <SlotCell
          key={s.id}
          principalId={principalId}
          slot={s.id}
          registry={reg.data?.entries ?? []}
        />
      ))}
    </tr>
  );
}

function SlotCell({
  principalId,
  slot,
  registry,
}: {
  principalId: string;
  slot: 'router' | 'observability_hook' | 'shape';
  registry: { id: string; name: string }[];
}) {
  // We could fetch the chain per cell, but doing many requests at once is fine in mock; in real backend this should be a single bulk query.
  const chain = usePluginChain(principalId, slot);
  const entries = chain.data?.entries ?? [];
  return (
    <td className="px-4 py-2 align-top">
      {entries.length ? (
        <ul className="space-y-1">
          {entries.map((e) => {
            const reg = registry.find((r) => r.id === e.wasm_registry_id);
            return (
              <li key={e.id} className="text-xs">
                {reg?.name ?? e.wasm_registry_id}
              </li>
            );
          })}
        </ul>
      ) : (
        <span className="text-text-faint">—</span>
      )}
    </td>
  );
}

// avoid circular import order issue
import { usePluginChain } from '../lib/queries';
