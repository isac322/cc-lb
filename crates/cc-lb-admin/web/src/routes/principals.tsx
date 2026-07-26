import { Dialog as BaseDialog } from '@base-ui/react/dialog';
import { NumberField as BaseNumberField } from '@base-ui/react/number-field';
import { Popover as BasePopover } from '@base-ui/react/popover';
import { Radio as BaseRadio } from '@base-ui/react/radio';
import { RadioGroup as BaseRadioGroup } from '@base-ui/react/radio-group';
import { Switch as BaseSwitch } from '@base-ui/react/switch';
import { Tabs as BaseTabs } from '@base-ui/react/tabs';
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
  ArrowDown,
  ArrowUp,
  CheckCircle2,
  ChevronDown,
  ChevronLeft,
  Copy,
  GripVertical,
  KeyRound,
  Plus,
  Trash2,
  X,
  XCircle,
} from 'lucide-react';
import React, { useEffect, useMemo, useState } from 'react';
import { toast } from 'sonner';
import * as z from 'zod';
import {
  CACHE_KEEPALIVE_CARD_GEOMETRY_CLASS,
  CacheKeepaliveCard,
} from '../components/principals/cache-keepalive/CacheKeepaliveCard';
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
  SkeletonRow,
  Spinner,
  StatusBadge,
} from '../components/ui/primitives';
import { RelativeTime } from '../components/ui/RelativeTime';
import { RequestEventsTable } from '../components/ui/RequestEventsTable';
import {
  type ChainSlot,
  type LimitKind,
  type PluginEntry,
  type Principal,
  type PrincipalDefaultLimit,
  useCreatePrincipal,
  useDeleteChainEntry,
  useDeletePrincipal,
  useInsertChainEntry,
  useIssueKey,
  usePluginChain,
  usePluginRegistry,
  usePrincipalKeys,
  usePrincipalNameMap,
  usePrincipals,
  useRecentEvents,
  useReorderChain,
  useRevokeKey,
  useRouterTerminalStrategy,
  useSetAllowedModels,
  useTogglePrincipal,
  useUpdatePrincipalDefaultLimits,
  useUpdatePrincipalSmartRouting,
  useUpdateRouterTerminalStrategy,
  useUpstreamNameMap,
} from '../lib/queries';
import { useCopyButton } from '../lib/useCopyButton';

const principalSearchSchema = z.object({ selectedId: z.string().optional() });

const PRINCIPAL_LIST_ROW_CLASS =
  'w-full min-h-[72px] text-left p-3 rounded-sm border';
const PRINCIPAL_DETAIL_HEADER_CLASS =
  'px-4 md:px-6 py-4 border-b border-subtle flex items-start justify-between gap-3 flex-wrap shrink-0';
const PRINCIPAL_DETAIL_BODY_CLASS =
  'flex-1 overflow-y-auto p-4 md:p-6 pb-8 md:pb-12 space-y-6';
const PRINCIPAL_RECENT_REQUESTS_TABLE_SLOT_CLASS = 'overflow-x-auto min-h-48';
// Minimums measured against the production admin fixture at 1440×1000.
const PRINCIPAL_DETAIL_CARD_CLASS_NAMES = {
  cacheKeepalive: CACHE_KEEPALIVE_CARD_GEOMETRY_CLASS,
  allowedModels: 'min-h-[103px]',
  defaultLimits: 'min-h-[113px]',
  recentRequests: 'min-h-[308px]',
  router: 'min-h-[336px]',
  observability: 'min-h-[147px]',
  shape: 'min-h-[236px]',
  apiKeys: 'min-h-[202px]',
} as const;
const EMPTY_PRINCIPAL_DETAIL_NAME_MAP = new Map<string, string>();

function pluginSupportsSlot(plugin: PluginEntry, slot: ChainSlot): boolean {
  const slots = plugin.supported_slots;
  if (!slots || slots.length === 0) {
    return false;
  }
  return slots.includes(slot);
}

function slotLabel(slot: ChainSlot): string {
  switch (slot) {
    case 'router':
      return 'Router';
    case 'shape':
      return 'Shape';
    case 'observability_hook':
      return 'Observability';
  }
}

export const Route = createFileRoute('/principals')({
  validateSearch: principalSearchSchema,
  component: PrincipalsPage,
});

function PrincipalDetailLoadingShell() {
  return (
    <div
      aria-busy="true"
      aria-label="Loading principal details"
      className="contents"
      data-testid="principal-detail-loading-shell"
      role="status"
    >
      <header className={PRINCIPAL_DETAIL_HEADER_CLASS}>
        <div className="min-w-0">
          <div className="flex items-center gap-3 flex-wrap">
            <Skeleton className="h-7 w-48" />
            <Skeleton className="h-5 w-20" />
            <Skeleton className="h-5 w-16" />
          </div>
          <Skeleton className="mt-0.5 h-4 w-44" />
        </div>
        <div className="flex items-center gap-2 shrink-0">
          <Skeleton className="h-7 w-20" />
          <Skeleton className="h-7 w-20" />
        </div>
      </header>

      <div className={PRINCIPAL_DETAIL_BODY_CLASS}>
        <Card
          className={cx(
            PRINCIPAL_DETAIL_CARD_CLASS_NAMES.cacheKeepalive,
            'w-full flex flex-col',
          )}
          data-testid="cache-keepalive-card"
        >
          <CardHeader
            title={<Skeleton as="span" className="block h-5 w-32" />}
            action={<Skeleton className="h-5 w-9" />}
          />
          <CardBody className="space-y-4 flex-1 flex flex-col">
            <div className="grid grid-cols-2 gap-4">
              {Array.from({ length: 4 }).map((_, index) => (
                <div key={index} className="flex flex-col gap-1">
                  <Skeleton className="h-3 w-24" />
                  <div className="flex h-7 items-center">
                    <Skeleton className="h-5 w-16" />
                  </div>
                  <Skeleton className="h-3 w-32" />
                </div>
              ))}
            </div>
            <Skeleton className="h-3 w-56" />
            <div className="mt-auto grid grid-cols-2 gap-2 border-t border-subtle pt-3">
              <Skeleton className="h-7 w-full" />
              <Skeleton className="h-7 w-full" />
            </div>
          </CardBody>
        </Card>

        <Card className={PRINCIPAL_DETAIL_CARD_CLASS_NAMES.allowedModels}>
          <CardHeader
            title={<Skeleton as="span" className="block h-5 w-32" />}
            action={<Skeleton className="h-7 w-24" />}
          />
          <CardBody>
            <Skeleton className="h-4 w-3/5" />
          </CardBody>
        </Card>

        <Card className={PRINCIPAL_DETAIL_CARD_CLASS_NAMES.defaultLimits}>
          <CardHeader
            title={<Skeleton as="span" className="block h-5 w-32" />}
            subtitle={<Skeleton as="span" className="block h-4 w-64" />}
            action={<Skeleton className="h-7 w-24" />}
          />
          <CardBody>
            <Skeleton className="h-4 w-2/5" />
          </CardBody>
        </Card>

        <Card className={PRINCIPAL_DETAIL_CARD_CLASS_NAMES.recentRequests}>
          <CardHeader
            title={<Skeleton as="span" className="block h-5 w-32" />}
            subtitle={<Skeleton as="span" className="block h-4 w-48" />}
          />
          <div className={PRINCIPAL_RECENT_REQUESTS_TABLE_SLOT_CLASS}>
            <RequestEventsTable
              events={[]}
              principalNameMap={EMPTY_PRINCIPAL_DETAIL_NAME_MAP}
              upstreamNameMap={EMPTY_PRINCIPAL_DETAIL_NAME_MAP}
              loading
              columns={{
                principal: false,
                cost: true,
                tokens: true,
              }}
              minWidthClass="min-w-[920px]"
            />
          </div>
        </Card>

        <Card className={PRINCIPAL_DETAIL_CARD_CLASS_NAMES.router}>
          <CardHeader
            title={<Skeleton as="span" className="block h-5 w-24" />}
            subtitle={<Skeleton as="span" className="block h-8 w-full" />}
            action={<Skeleton className="h-7 w-24" />}
          />
          <CardBody className="space-y-3">
            <Skeleton className="h-10" />
            <Skeleton className="h-10" />
            <Skeleton className="h-28" />
          </CardBody>
        </Card>

        <Card className={PRINCIPAL_DETAIL_CARD_CLASS_NAMES.observability}>
          <CardHeader
            title={<Skeleton as="span" className="block h-5 w-32" />}
            subtitle={<Skeleton as="span" className="block h-4 w-64" />}
            action={<Skeleton className="h-7 w-24" />}
          />
          <CardBody>
            <Skeleton className="h-10" />
          </CardBody>
        </Card>

        <Card className={PRINCIPAL_DETAIL_CARD_CLASS_NAMES.shape}>
          <CardHeader
            title={<Skeleton as="span" className="block h-5 w-24" />}
            subtitle={<Skeleton as="span" className="block h-4 w-96" />}
          />
          <CardBody className="space-y-3">
            <Skeleton className="h-14" />
            <Skeleton className="h-14" />
          </CardBody>
        </Card>

        <Card className={PRINCIPAL_DETAIL_CARD_CLASS_NAMES.apiKeys}>
          <CardHeader
            title={<Skeleton as="span" className="block h-5 w-24" />}
            subtitle={<Skeleton as="span" className="block h-4 w-44" />}
            action={<Skeleton className="h-7 w-24" />}
          />
          <CardBody className="space-y-3">
            <Skeleton className="h-9" />
            <Skeleton className="h-9" />
          </CardBody>
        </Card>
      </div>
    </div>
  );
}

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
            <div className="h-4 flex items-center text-[11px] text-text-faint">
              {principals.isLoading ? (
                <span
                  className="skeleton inline-block h-3 w-12"
                  data-testid="principal-count-skeleton"
                  aria-hidden="true"
                />
              ) : (
                `${principals.data?.principals.length ?? 0} total`
              )}
            </div>
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
              <div
                key={i}
                className={cx(PRINCIPAL_LIST_ROW_CLASS, 'border-subtle')}
                data-testid="principal-list-skeleton"
                aria-hidden="true"
              >
                <div className="flex items-center justify-between gap-2 mb-1">
                  <div className="flex items-center gap-2 min-w-0">
                    <Skeleton className="h-2 w-2 rounded-full shrink-0" />
                    <Skeleton className="h-4 w-28" />
                  </div>
                  <Skeleton className="h-5 w-14" />
                </div>
                <div className="flex items-center justify-between mt-2">
                  <Skeleton className="h-3 w-32" />
                  <Skeleton className="h-3 w-10" />
                </div>
              </div>
            ))
          ) : principals.data?.principals.length ? (
            principals.data.principals.map((p) => (
              <button
                key={p.id}
                type="button"
                onClick={() => select(p.id)}
                className={cx(
                  PRINCIPAL_LIST_ROW_CLASS,
                  'transition-colors',
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
        ) : principals.isLoading ? (
          <PrincipalDetailLoadingShell />
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
  const routerChain = usePluginChain(principal.id, 'router');
  const observabilityChain = usePluginChain(principal.id, 'observability_hook');
  const shapeChain = usePluginChain(principal.id, 'shape');
  const deleteChainCount =
    (routerChain.data?.entries.length ?? 0) +
    (observabilityChain.data?.entries.length ?? 0) +
    (shapeChain.data?.entries.length ?? 0);
  const deleteChainCountLoading =
    routerChain.isLoading ||
    observabilityChain.isLoading ||
    shapeChain.isLoading;
  const deleteChainCountLabel = deleteChainCountLoading ? (
    <span
      className="skeleton inline-block h-3 w-48"
      data-testid="plugin-chain-count-skeleton"
      aria-hidden="true"
    />
  ) : (
    `${deleteChainCount} plugin chain ${deleteChainCount === 1 ? 'entry' : 'entries'} will be deleted.`
  );
  const [confirmDeleteOpen, setConfirmDeleteOpen] = useState(false);
  return (
    <>
      <header className={PRINCIPAL_DETAIL_HEADER_CLASS}>
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
            <span className="block mt-2 text-text-muted">
              {deleteChainCountLabel}
            </span>
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

      <div className={PRINCIPAL_DETAIL_BODY_CLASS}>
        <CacheKeepaliveCard principal={principal} />
        <AllowedModelsCard principal={principal} />
        <DefaultLimitsCard principal={principal} />
        <RecentRequestsCard principal={principal} />
        <RouterSlotEditor principal={principal} />
        <ObservabilityHookEditor principalId={principal.id} />
        <ShapeSlotEditor principalId={principal.id} />
        <ApiKeysCard principal={principal} />
      </div>
    </>
  );
}

export function RecentRequestsCard({ principal }: { principal: Principal }) {
  const principalNameMap = usePrincipalNameMap();
  const upstreamNameMap = useUpstreamNameMap();
  const recent = useRecentEvents({
    principal_id: principal.id,
    limit: '5',
  });
  const events = useMemo(() => {
    return (recent.data?.events ?? []).map((e) => ({
      ...e,
      _phase: 'final' as const,
    }));
  }, [recent.data]);
  const loading = recent.isPending || recent.isPlaceholderData;
  return (
    <Card className={PRINCIPAL_DETAIL_CARD_CLASS_NAMES.recentRequests}>
      <CardHeader
        title="Recent Requests"
        subtitle={
          <span className="inline-flex h-4 items-center">
            {loading ? (
              <span
                className="skeleton inline-block h-3 w-48"
                data-testid="recent-requests-subtitle-skeleton"
                aria-hidden="true"
              />
            ) : events.length === 0 ? (
              `No recent requests from ${principal.name}`
            ) : (
              `Last ${events.length} from ${principal.name}`
            )}
          </span>
        }
      />
      <div
        className={PRINCIPAL_RECENT_REQUESTS_TABLE_SLOT_CLASS}
        data-testid="recent-requests-table-slot"
      >
        <RequestEventsTable
          events={recent.isPlaceholderData ? [] : events}
          principalNameMap={principalNameMap}
          upstreamNameMap={upstreamNameMap}
          loading={loading}
          columns={{
            principal: false,
            cost: true,
            tokens: true,
          }}
          minWidthClass="min-w-[920px]"
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
    <Card className={PRINCIPAL_DETAIL_CARD_CLASS_NAMES.allowedModels}>
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

const LIMIT_KIND_OPTIONS: { value: LimitKind; label: string }[] = [
  { value: 'requests', label: 'Requests' },
  { value: 'input_tokens', label: 'Input tokens' },
  { value: 'output_tokens', label: 'Output tokens' },
  { value: 'total_tokens', label: 'Total tokens' },
  { value: 'cost_usd', label: 'Cost (USD)' },
  { value: 'concurrent', label: 'Concurrent' },
];

const LIMIT_KIND_LABEL: Record<LimitKind, string> = LIMIT_KIND_OPTIONS.reduce(
  (acc, opt) => {
    acc[opt.value] = opt.label;
    return acc;
  },
  {} as Record<LimitKind, string>,
);

function formatWindowSecs(secs: number): string {
  if (!Number.isFinite(secs) || secs <= 0) return `${secs}s`;
  if (secs % 86400 === 0) return `${secs / 86400}d`;
  if (secs % 3600 === 0) return `${secs / 3600}h`;
  if (secs % 60 === 0) return `${secs / 60}m`;
  return `${secs}s`;
}

function formatLimitCap(limit: PrincipalDefaultLimit): string {
  if (limit.kind === 'cost_usd') {
    const usd = limit.cap_micros / 1_000_000;
    return `$${usd.toLocaleString(undefined, {
      minimumFractionDigits: 2,
      maximumFractionDigits: 6,
    })}`;
  }
  return limit.cap_micros.toLocaleString();
}

function LimitsEditor({
  value,
  onChange,
}: {
  value: PrincipalDefaultLimit[];
  onChange: (next: PrincipalDefaultLimit[]) => void;
}) {
  const updateRow = (idx: number, patch: Partial<PrincipalDefaultLimit>) => {
    const next = value.map((row, i) =>
      i === idx ? { ...row, ...patch } : row,
    );
    onChange(next);
  };
  const removeRow = (idx: number) => {
    onChange(value.filter((_, i) => i !== idx));
  };
  const addRow = () => {
    onChange([...value, { kind: 'requests', window_secs: 60, cap_micros: 0 }]);
  };

  return (
    <div className="space-y-2">
      {value.length === 0 ? (
        <p className="text-xs text-text-faint">No limits. Add one below.</p>
      ) : null}
      {value.map((row, idx) => {
        const isCost = row.kind === 'cost_usd';
        const capValue = isCost ? row.cap_micros / 1_000_000 : row.cap_micros;
        return (
          <div
            key={idx}
            className="flex flex-wrap items-end gap-2 border border-subtle rounded-md p-2"
          >
            <div className="w-40">
              <Field label="Kind">
                <select
                  className={INPUT_CLASS}
                  value={row.kind}
                  onChange={(e) =>
                    updateRow(idx, { kind: e.target.value as LimitKind })
                  }
                >
                  {LIMIT_KIND_OPTIONS.map((opt) => (
                    <option key={opt.value} value={opt.value}>
                      {opt.label}
                    </option>
                  ))}
                </select>
              </Field>
            </div>
            <div className="w-32">
              <Field label="Window (sec)">
                <BaseNumberField.Root
                  min={1}
                  step={1}
                  value={row.window_secs}
                  onValueChange={(nextValue) =>
                    updateRow(idx, {
                      window_secs: Math.max(1, Math.floor(nextValue ?? 0)),
                    })
                  }
                >
                  <BaseNumberField.Input className={INPUT_CLASS} />
                </BaseNumberField.Root>
              </Field>
            </div>
            <div className="w-40">
              <Field label={isCost ? 'Cap (USD)' : 'Cap'}>
                <BaseNumberField.Root
                  min={0}
                  step={isCost ? 0.01 : 1}
                  value={capValue}
                  onValueChange={(nextValue) => {
                    const raw = nextValue ?? 0;
                    if (!Number.isFinite(raw) || raw < 0) {
                      updateRow(idx, { cap_micros: 0 });
                      return;
                    }
                    updateRow(idx, {
                      cap_micros: isCost
                        ? Math.round(raw * 1_000_000)
                        : Math.floor(raw),
                    });
                  }}
                >
                  <BaseNumberField.Input className={INPUT_CLASS} />
                </BaseNumberField.Root>
              </Field>
            </div>
            <Button
              size="sm"
              variant="danger"
              iconLeft={<Trash2 className="w-3 h-3" />}
              onClick={() => removeRow(idx)}
            >
              Remove
            </Button>
          </div>
        );
      })}
      <Button
        size="sm"
        iconLeft={<Plus className="w-3 h-3" />}
        onClick={addRow}
      >
        Add limit
      </Button>
    </div>
  );
}

function DefaultLimitsCard({ principal }: { principal: Principal }) {
  const update = useUpdatePrincipalDefaultLimits();
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState<PrincipalDefaultLimit[]>(
    principal.default_limits,
  );
  useEffect(() => {
    if (!editing) setDraft(principal.default_limits);
  }, [editing, principal.default_limits]);

  return (
    <Card className={PRINCIPAL_DETAIL_CARD_CLASS_NAMES.defaultLimits}>
      <CardHeader
        title="Default Limits"
        subtitle="Per-principal rate caps applied to every API key"
        action={
          editing ? (
            <div className="flex items-center gap-2">
              <Button
                size="sm"
                onClick={() => {
                  setEditing(false);
                  setDraft(principal.default_limits);
                }}
              >
                Cancel
              </Button>
              <Button
                size="sm"
                variant="primary"
                onClick={() =>
                  update.mutate(
                    {
                      id: principal.id,
                      default_limits: draft,
                      expected_revision: principal.revision,
                    },
                    {
                      onSuccess: () => {
                        toast.success('Default limits updated');
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
      {editing ? (
        <CardBody>
          <LimitsEditor value={draft} onChange={setDraft} />
        </CardBody>
      ) : principal.default_limits.length ? (
        <div className="overflow-x-auto">
          <table className="w-full font-mono text-xs">
            <thead className="table-header sticky top-0 z-10">
              <tr className="text-[10px] uppercase tracking-wider">
                <th className="text-left px-4 py-2">Kind</th>
                <th className="text-right px-4 py-2">Window</th>
                <th className="text-right px-4 py-2">Cap</th>
              </tr>
            </thead>
            <tbody>
              {principal.default_limits.map((l, idx) => (
                <tr
                  key={`${l.kind}-${l.window_secs}-${idx}`}
                  className="border-b border-row"
                >
                  <td className="px-4 py-2">
                    {LIMIT_KIND_LABEL[l.kind] ?? l.kind}
                  </td>
                  <td className="px-4 py-2 text-right tabular-nums">
                    {formatWindowSecs(l.window_secs)}
                  </td>
                  <td className="px-4 py-2 text-right tabular-nums">
                    {formatLimitCap(l)}
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

function SlotRadioCard({
  value,
  name,
  desc,
  isActive,
  isMutating,
  isMutatingOther,
  isDefault,
  isNone,
  badge,
}: {
  value: string;
  name: string;
  desc: string;
  isActive: boolean;
  isMutating: boolean;
  isMutatingOther: boolean;
  isDefault?: boolean;
  isNone?: boolean;
  badge?: string;
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
        <BaseRadio.Root
          className="mt-1 flex items-center justify-center w-4 h-4 shrink-0"
          disabled={isMutatingOther}
          value={value}
        >
          {isMutating ? (
            <Spinner className="w-3 h-3 text-accent" />
          ) : isNone ? (
            <>
              <BaseRadio.Indicator className="sr-only" />
              <span className="text-text-faint text-xs leading-none">⊘</span>
            </>
          ) : isDefault ? (
            <>
              <BaseRadio.Indicator className="sr-only" />
              <span className="w-1.5 h-1.5 rounded-full border border-text-faint" />
            </>
          ) : (
            <>
              <BaseRadio.Indicator className="status-dot ok" />
              <span
                className={cx('status-dot neutral', isActive && 'hidden!')}
              />
            </>
          )}
        </BaseRadio.Root>
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

function PluginDetailDrawer({
  plugin,
  open,
  onOpenChange,
}: {
  plugin: PluginEntry | null;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}) {
  const { copy } = useCopyButton();
  if (!plugin) return null;

  return (
    <BaseDialog.Root open={open} onOpenChange={onOpenChange}>
      <BaseDialog.Portal>
        <BaseDialog.Backdrop className="fixed inset-0 z-40 bg-drawer-backdrop" />
        <BaseDialog.Popup className="fixed right-0 top-0 bottom-0 w-full max-w-lg bg-bg-sub border-l border-subtle z-50 flex flex-col outline-none transition-transform duration-200 ease-out data-[ending-style]:translate-x-full data-[starting-style]:translate-x-full">
          <BaseDialog.Title className="sr-only">Plugin detail</BaseDialog.Title>
          <BaseDialog.Description className="sr-only">
            Detail view of a plugin
          </BaseDialog.Description>

          <div className="p-4 border-b border-subtle flex items-start justify-between gap-3">
            <div className="min-w-0">
              <div className="flex items-center gap-2 mb-1 flex-wrap">
                <h2 className="text-lg font-medium text-text truncate">
                  {plugin.name}
                </h2>
                {plugin.is_builtin && <Badge tone="accent">Built-in</Badge>}
                {plugin.supported_slots && plugin.supported_slots.length > 0 ? (
                  plugin.supported_slots.map((slot) => (
                    <Badge key={slot} tone="accent">
                      {slotLabel(slot)}
                    </Badge>
                  ))
                ) : (
                  <Badge tone="warn">Unknown slot</Badge>
                )}
              </div>
            </div>
            <BaseDialog.Close
              aria-label="Close"
              className="text-text-muted hover:text-text shrink-0"
            >
              <X className="w-4 h-4" />
            </BaseDialog.Close>
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
                    onClick={() => copy(plugin.id, 'Plugin ID')}
                  >
                    <Copy className="w-3 h-3" />
                  </button>
                </div>
              </div>
            </section>
          </div>
        </BaseDialog.Popup>
      </BaseDialog.Portal>
    </BaseDialog.Root>
  );
}

function useFlipReorder(
  listRef: React.RefObject<HTMLUListElement | null>,
  items: unknown[],
) {
  const oldRects = React.useRef<Record<string, DOMRect>>({});

  // biome-ignore lint/correctness/useExhaustiveDependencies: items is the trigger
  React.useLayoutEffect(() => {
    if (!listRef.current) return;
    const children = Array.from(listRef.current.children) as HTMLElement[];

    children.forEach((child) => {
      const key = child.dataset.key;
      if (!key) return;

      const oldRect = oldRects.current[key];
      const newRect = child.getBoundingClientRect();

      if (oldRect) {
        const deltaY = oldRect.top - newRect.top;
        if (deltaY !== 0) {
          child.style.transform = `translateY(${deltaY}px)`;
          child.style.transition = 'none';

          requestAnimationFrame(() => {
            child.style.transform = '';
            child.style.transition =
              'transform 220ms cubic-bezier(0.4, 0, 0.2, 1)';
          });
        }
      } else {
        // New element
        child.style.opacity = '0';
        child.style.transform = 'translateY(10px)';
        child.style.transition = 'none';
        requestAnimationFrame(() => {
          child.style.opacity = '1';
          child.style.transform = '';
          child.style.transition = 'all 220ms cubic-bezier(0.4, 0, 0.2, 1)';
        });
      }
    });

    // Update old rects for next render
    oldRects.current = {};
    children.forEach((child) => {
      const key = child.dataset.key;
      if (key) {
        oldRects.current[key] = child.getBoundingClientRect();
      }
    });
  }, [items]);
}

export function RouterSlotEditor({ principal }: { principal: Principal }) {
  const principalId = principal.id;
  const slot = 'router' as const;
  const chain = usePluginChain(principalId, slot);
  const registry = usePluginRegistry();
  const reorder = useReorderChain();
  const insert = useInsertChainEntry();
  const del = useDeleteChainEntry();
  const terminalStrategy = useRouterTerminalStrategy(principalId);
  const updateTerminalStrategy = useUpdateRouterTerminalStrategy();
  const updateSmartRouting = useUpdatePrincipalSmartRouting();

  const entries = useMemo(
    () => [...(chain.data?.entries ?? [])].sort((a, b) => a.order - b.order),
    [chain.data],
  );

  const subscriptionPreferencePlugin = registry.data?.entries.find(
    (e) => e.name === 'subscription-preference',
  );
  const subscriptionPreferenceEntry = entries.find(
    (e) => e.wasm_registry_id === subscriptionPreferencePlugin?.id,
  );

  // A leftover built-in chain entry is an advanced override now that the filter
  // is driven by principal.smart_routing_enabled, so surface it in Advanced.
  const isComplex = useMemo(() => {
    if (!subscriptionPreferencePlugin) return false;
    const hasOther = entries.some(
      (e) => e.wasm_registry_id !== subscriptionPreferencePlugin.id,
    );
    return hasOther || !!subscriptionPreferenceEntry;
  }, [entries, subscriptionPreferencePlugin, subscriptionPreferenceEntry]);

  const [detailPlugin, setDetailPlugin] = useState<PluginEntry | null>(null);
  const [activeTab, setActiveTab] = useState<'basic' | 'advanced'>(
    isComplex ? 'advanced' : 'basic',
  );
  const [showMobileNotice, setShowMobileNotice] = useState(false);

  useEffect(() => {
    if (isComplex && activeTab === 'basic') {
      setActiveTab('advanced');
    }
  }, [isComplex, activeTab]);

  const isSmartRouting = principal.smart_routing_enabled;

  const toggleSmartRouting = () => {
    updateSmartRouting.mutate({
      id: principalId,
      smart_routing_enabled: !principal.smart_routing_enabled,
      expected_revision: principal.revision,
    });
  };

  const setStrategy = (strategy: string) => {
    if (!terminalStrategy.data) return;
    updateTerminalStrategy.mutate(
      {
        id: principalId,
        strategy,
        revision: terminalStrategy.data.revision,
      },
      {
        onSuccess: () => toast.success('Terminal strategy updated'),
      },
    );
  };

  const handleTabClick = (tab: 'basic' | 'advanced') => {
    if (tab === 'basic' && isComplex) {
      setShowMobileNotice(true);
      setTimeout(() => setShowMobileNotice(false), 4000);
      return;
    }
    setActiveTab(tab);
    setShowMobileNotice(false);
  };

  const listRef = React.useRef<HTMLUListElement>(null);
  useFlipReorder(listRef, entries);

  const [pickerOpen, setPickerOpen] = useState(false);

  const moveUp = (index: number) => {
    if (index === 0) return;
    const reordered = [...entries];
    const temp = reordered[index - 1];
    reordered[index - 1] = reordered[index]!;
    reordered[index] = temp!;

    reorder.mutate({
      pid: principalId,
      entries: reordered.map((x, i) => ({
        id: x.id,
        order: (i + 1) * 100,
        expected_revision: x.revision,
      })),
    });
  };

  const moveDown = (index: number) => {
    if (index === entries.length - 1) return;
    const reordered = [...entries];
    const temp = reordered[index + 1];
    reordered[index + 1] = reordered[index]!;
    reordered[index] = temp!;

    reorder.mutate({
      pid: principalId,
      entries: reordered.map((x, i) => ({
        id: x.id,
        order: (i + 1) * 100,
        expected_revision: x.revision,
      })),
    });
  };

  const addFilter = (pluginId: string) => {
    insert.mutate({
      pid: principalId,
      body: {
        slot,
        wasm_registry_id: pluginId,
        order: (entries.length + 1) * 100,
      },
    });
    setPickerOpen(false);
  };

  const removeFilter = (id: string, revision: number) => {
    del.mutate({ id, revision });
  };

  const strategy = terminalStrategy.data?.strategy ?? 'first-pick';

  return (
    <Card className={PRINCIPAL_DETAIL_CARD_CLASS_NAMES.router}>
      <CardHeader
        title="Router"
        subtitle="Pick which upstream serves each request. Returning users stick to the upstream they hit before unless you turn that off; new users go to the first eligible upstream by default."
        className="border-b-0 pb-0"
      />
      <div className="flex flex-col px-4 border-b border-subtle relative">
        <BaseTabs.Root
          value={activeTab}
          onValueChange={(value) => {
            if (value === 'basic' || value === 'advanced') {
              handleTabClick(value);
            }
          }}
        >
          <BaseTabs.List className="flex items-center gap-1 w-fit mt-3">
            <Hint
              label={
                isComplex
                  ? "Basic can't show this chain without losing the extra filters. Open Advanced to edit the full chain."
                  : ''
              }
            >
              <BaseTabs.Tab
                aria-disabled={isComplex}
                className={cx(
                  'px-3 py-1.5 text-xs font-medium border-b-2 -mb-px transition-colors border-transparent text-text-faint hover:text-text data-[active]:border-[color:var(--color-accent)] data-[active]:text-[color:var(--color-text)]',
                  isComplex && 'opacity-50 cursor-not-allowed',
                )}
                value="basic"
              >
                Basic
              </BaseTabs.Tab>
            </Hint>
            <BaseTabs.Tab
              className={cx(
                'px-3 py-1.5 text-xs font-medium border-b-2 -mb-px transition-colors border-transparent text-text-faint hover:text-text data-[active]:border-[color:var(--color-accent)] data-[active]:text-[color:var(--color-text)]',
              )}
              value="advanced"
            >
              Advanced
            </BaseTabs.Tab>
          </BaseTabs.List>
        </BaseTabs.Root>
        {showMobileNotice && (
          <div className="absolute left-4 top-full mt-2 z-10 text-amber-300 bg-amber-500/10 border border-amber-500/30 rounded-sm px-3 py-2 text-xs shadow-lg flex items-start gap-2 max-w-xs">
            <span>
              Basic can't show this chain without losing the extra filters. Open
              Advanced to edit the full chain.
            </span>
            <button
              onClick={() => setShowMobileNotice(false)}
              className="text-amber-300 hover:text-amber-100 shrink-0"
            >
              &times;
            </button>
          </div>
        )}
      </div>
      <CardBody>
        {activeTab === 'basic' && (
          <div role="tabpanel" className="space-y-5">
            <div className="flex items-center justify-between p-3 border border-subtle rounded-sm bg-overlay-1">
              <div>
                <div className="text-sm font-medium text-text">
                  Keep prompt cache warm by reusing upstreams
                </div>
                <div className="text-xs text-text-faint">
                  Requests with similar prompts get routed to the upstream that
                  already served them, so the prompt cache hits stay high.
                </div>
              </div>
              <BaseSwitch.Root
                checked={isSmartRouting}
                className="group inline-flex items-center gap-2 h-7 px-2 rounded-sm transition-colors focus:outline-none focus-visible:ring-2 focus-visible:ring-accent/40 disabled:opacity-50 disabled:cursor-not-allowed hover:bg-overlay-3"
                nativeButton
                onCheckedChange={() => toggleSmartRouting()}
                render={<button type="button" />}
                disabled={updateSmartRouting.isPending}
              >
                <div
                  className={cx(
                    'relative inline-flex h-4 w-8 shrink-0 items-center rounded-full transition-colors duration-200 ease-in-out border',
                    isSmartRouting
                      ? 'bg-emerald-500 border-emerald-500'
                      : 'bg-overlay-5 border-subtle-strong group-hover:border-text-muted',
                  )}
                >
                  <BaseSwitch.Thumb
                    className={cx(
                      'pointer-events-none inline-block h-3 w-3 transform rounded-full bg-white shadow-sm transition-transform duration-200 ease-in-out',
                      isSmartRouting ? 'translate-x-4' : 'translate-x-0.5',
                    )}
                  />
                </div>
              </BaseSwitch.Root>
            </div>

            <div>
              <div className="text-sm font-medium text-text mb-3">
                When multiple upstreams qualify, pick
              </div>
              <BaseRadioGroup
                className="grid grid-cols-1 sm:grid-cols-2 gap-3"
                name="strategy"
                onValueChange={setStrategy}
                render={<ul />}
                value={strategy}
              >
                <li
                  role="radio"
                  aria-checked={strategy === 'first-pick'}
                  className="h-full"
                >
                  <label
                    className={cx(
                      'flex items-start gap-3 p-3 border rounded-md cursor-pointer transition-colors h-full',
                      strategy === 'first-pick'
                        ? 'border-accent bg-[color-mix(in_srgb,var(--color-accent)_8%,transparent)]'
                        : 'border-subtle hover:bg-overlay-3',
                    )}
                  >
                    <BaseRadio.Root
                      className="mt-1 flex items-center justify-center w-4 h-4 shrink-0"
                      value="first-pick"
                    >
                      <BaseRadio.Indicator className="status-dot ok" />
                      <span
                        className={cx(
                          'status-dot neutral',
                          strategy === 'first-pick' && 'hidden!',
                        )}
                      />
                    </BaseRadio.Root>
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center gap-2">
                        <span className="font-medium text-sm truncate">
                          First eligible
                        </span>
                      </div>
                      <div className="text-xs text-text-faint mt-0.5">
                        Always pick the first upstream in the candidate list.
                        Predictable, easy to reason about.
                      </div>
                    </div>
                  </label>
                </li>
                <li
                  role="radio"
                  aria-checked={strategy === 'random'}
                  className="h-full"
                >
                  <label
                    className={cx(
                      'flex items-start gap-3 p-3 border rounded-md cursor-pointer transition-colors h-full',
                      strategy === 'random'
                        ? 'border-accent bg-[color-mix(in_srgb,var(--color-accent)_8%,transparent)]'
                        : 'border-subtle hover:bg-overlay-3',
                    )}
                  >
                    <BaseRadio.Root
                      className="mt-1 flex items-center justify-center w-4 h-4 shrink-0"
                      value="random"
                    >
                      <BaseRadio.Indicator className="status-dot ok" />
                      <span
                        className={cx(
                          'status-dot neutral',
                          strategy === 'random' && 'hidden!',
                        )}
                      />
                    </BaseRadio.Root>
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center gap-2">
                        <span className="font-medium text-sm truncate">
                          Random
                        </span>
                      </div>
                      <div className="text-xs text-text-faint mt-0.5">
                        Pick a random upstream from the candidate list. Helps
                        spread load when many are equivalent.
                      </div>
                    </div>
                  </label>
                </li>
              </BaseRadioGroup>
            </div>
          </div>
        )}

        {activeTab === 'advanced' && (
          <div role="tabpanel" className="space-y-4">
            {entries.length === 0 && (
              <div className="p-4 border border-subtle rounded-sm bg-overlay-1 text-center mb-4">
                <p className="text-sm text-text mb-1">No filters yet.</p>
                <p className="text-xs text-text-faint">
                  Incoming requests will go straight to the terminal step. Add a
                  filter to narrow candidates by some property (cache prefix,
                  cost, region, ...).
                </p>
              </div>
            )}

            <BasePopover.Root open={pickerOpen} onOpenChange={setPickerOpen}>
              <ul ref={listRef} className="space-y-0">
                {entries.map((e, idx) => {
                  const reg = registry.data?.entries.find(
                    (r) => r.id === e.wasm_registry_id,
                  );
                  return (
                    <React.Fragment key={e.id}>
                      {idx > 0 && (
                        <li
                          className="flex flex-col items-center"
                          data-key={`connector-${idx}`}
                        >
                          <div className="w-px h-4 bg-subtle"></div>
                          <ChevronDown className="w-3 h-3 text-text-faint -mt-1 mb-1" />
                        </li>
                      )}
                      <li
                        className="flex items-center gap-3 p-3 border border-subtle rounded-sm bg-overlay-1"
                        data-key={e.id}
                      >
                        <div className="text-[10px] tabular-nums text-text-faint w-8 shrink-0">
                          Step {idx + 1}
                        </div>
                        <div className="flex-1 min-w-0">
                          <div className="flex items-center gap-2">
                            <button
                              type="button"
                              className="text-sm font-medium text-text truncate hover:underline"
                              onClick={() => setDetailPlugin(reg ?? null)}
                            >
                              {reg?.name ?? e.wasm_registry_id}
                            </button>
                          </div>
                          <div className="text-xs text-text-faint truncate mt-0.5">
                            {reg?.metadata?.purpose ??
                              'User-uploaded filter (no description supplied).'}
                          </div>
                        </div>
                        <div className="flex items-center gap-1 shrink-0">
                          <button
                            onClick={() => moveUp(idx)}
                            className="h-7 w-7 inline-flex items-center justify-center rounded-sm text-text-faint hover:text-text hover:bg-overlay-3 disabled:opacity-30 disabled:cursor-not-allowed"
                            disabled={idx === 0}
                          >
                            <ArrowUp className="w-4 h-4" />
                          </button>
                          <button
                            onClick={() => moveDown(idx)}
                            className="h-7 w-7 inline-flex items-center justify-center rounded-sm text-text-faint hover:text-text hover:bg-overlay-3 disabled:opacity-30 disabled:cursor-not-allowed"
                            disabled={idx === entries.length - 1}
                          >
                            <ArrowDown className="w-4 h-4" />
                          </button>
                          <button
                            onClick={() => removeFilter(e.id, e.revision)}
                            className="h-7 w-7 inline-flex items-center justify-center rounded-sm text-text-faint hover:text-red-400 hover:bg-overlay-3"
                          >
                            <Trash2 className="w-4 h-4" />
                          </button>
                        </div>
                      </li>
                    </React.Fragment>
                  );
                })}

                {entries.length > 0 && (
                  <li
                    className="flex flex-col items-center"
                    data-key="connector-end"
                  >
                    <div className="w-px h-4 bg-subtle"></div>
                    <ChevronDown className="w-3 h-3 text-text-faint -mt-1 mb-1" />
                  </li>
                )}

                <BasePopover.Trigger
                  nativeButton={false}
                  render={
                    <li
                      className="flex items-center justify-center p-3 border border-dashed border-subtle-strong rounded-sm bg-overlay-1/50 hover:bg-overlay-2 cursor-pointer transition-colors"
                      data-key="add-filter-placeholder"
                      onClick={(e) => e.stopPropagation()}
                    />
                  }
                >
                  <div className="flex items-center gap-2 text-text-muted hover:text-text">
                    <Plus className="w-4 h-4" />
                    <span className="text-sm font-medium">Add filter</span>
                  </div>
                </BasePopover.Trigger>
              </ul>

              <div className="flex flex-col gap-3 p-3 border border-subtle border-l-2 border-l-accent rounded-sm bg-overlay-1 mt-4">
                <div className="flex items-center gap-3">
                  <div className="text-[10px] tabular-nums text-text-faint w-8 shrink-0 flex items-center gap-1">
                    <ChevronDown className="w-3 h-3" />
                    Final
                  </div>
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2">
                      <span className="text-sm font-medium text-text truncate">
                        Terminal step
                      </span>
                    </div>
                    <div className="text-xs text-text-faint truncate mt-0.5">
                      Picks the upstream that will serve the request.
                    </div>
                  </div>
                </div>
                <div className="pl-11">
                  <BaseRadioGroup
                    className="grid grid-cols-1 sm:grid-cols-2 gap-3"
                    name="term-strategy"
                    onValueChange={setStrategy}
                    render={<ul />}
                    value={strategy}
                  >
                    <li
                      role="radio"
                      aria-checked={strategy === 'first-pick'}
                      className="h-full"
                    >
                      <label
                        className={cx(
                          'flex items-start gap-3 p-3 border rounded-md cursor-pointer transition-colors h-full',
                          strategy === 'first-pick'
                            ? 'border-accent bg-[color-mix(in_srgb,var(--color-accent)_8%,transparent)]'
                            : 'border-subtle hover:bg-overlay-3',
                        )}
                      >
                        <BaseRadio.Root
                          className="mt-1 flex items-center justify-center w-4 h-4 shrink-0"
                          value="first-pick"
                        >
                          <BaseRadio.Indicator className="status-dot ok" />
                          <span
                            className={cx(
                              'status-dot neutral',
                              strategy === 'first-pick' && 'hidden!',
                            )}
                          />
                        </BaseRadio.Root>
                        <div className="flex-1 min-w-0">
                          <div className="flex items-center gap-2">
                            <span className="font-medium text-sm truncate">
                              First eligible
                            </span>
                          </div>
                          <div className="text-xs text-text-faint mt-0.5">
                            Always pick the first upstream in the candidate
                            list. Predictable, easy to reason about.
                          </div>
                        </div>
                      </label>
                    </li>
                    <li
                      role="radio"
                      aria-checked={strategy === 'random'}
                      className="h-full"
                    >
                      <label
                        className={cx(
                          'flex items-start gap-3 p-3 border rounded-md cursor-pointer transition-colors h-full',
                          strategy === 'random'
                            ? 'border-accent bg-[color-mix(in_srgb,var(--color-accent)_8%,transparent)]'
                            : 'border-subtle hover:bg-overlay-3',
                        )}
                      >
                        <BaseRadio.Root
                          className="mt-1 flex items-center justify-center w-4 h-4 shrink-0"
                          value="random"
                        >
                          <BaseRadio.Indicator className="status-dot ok" />
                          <span
                            className={cx(
                              'status-dot neutral',
                              strategy === 'random' && 'hidden!',
                            )}
                          />
                        </BaseRadio.Root>
                        <div className="flex-1 min-w-0">
                          <div className="flex items-center gap-2">
                            <span className="font-medium text-sm truncate">
                              Random
                            </span>
                          </div>
                          <div className="text-xs text-text-faint mt-0.5">
                            Pick a random upstream from the candidate list.
                            Helps spread load when many are equivalent.
                          </div>
                        </div>
                      </label>
                    </li>
                  </BaseRadioGroup>
                </div>
              </div>
              <BasePopover.Portal>
                <BasePopover.Positioner
                  align="start"
                  className="z-50"
                  sideOffset={4}
                >
                  <BasePopover.Popup
                    className="w-64 bg-bg-sub border border-subtle rounded-sm shadow-lg z-50 py-1"
                    onClick={(e) => e.stopPropagation()}
                  >
                    {registry.data?.entries
                      .filter((p) => pluginSupportsSlot(p, 'router'))
                      .map((p) => {
                        const inChain = entries.some(
                          (e) => e.wasm_registry_id === p.id,
                        );
                        const disabled = inChain;

                        return (
                          <button
                            key={p.id}
                            className={cx(
                              'w-full text-left px-3 py-2 text-sm flex flex-col gap-0.5',
                              disabled
                                ? 'opacity-50 cursor-not-allowed'
                                : 'hover:bg-overlay-3',
                            )}
                            disabled={disabled}
                            onClick={() => addFilter(p.id)}
                          >
                            <div className="flex items-center justify-between">
                              <span className="font-medium text-text">
                                {p.name}
                              </span>
                              {disabled ? (
                                <span className="text-[10px] text-text-faint">
                                  Already in chain
                                </span>
                              ) : null}
                            </div>
                            <span className="text-xs text-text-faint truncate">
                              {p.metadata?.purpose ?? 'Custom filter'}
                            </span>
                          </button>
                        );
                      })}
                  </BasePopover.Popup>
                </BasePopover.Positioner>
              </BasePopover.Portal>
            </BasePopover.Root>
          </div>
        )}
      </CardBody>

      <PluginDetailDrawer
        plugin={detailPlugin}
        open={detailPlugin !== null}
        onOpenChange={(open) => {
          if (!open) setDetailPlugin(null);
        }}
      />
    </Card>
  );
}

function ShapeSlotEditor({ principalId }: { principalId: string }) {
  const slot = 'shape';
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
    <Card className={PRINCIPAL_DETAIL_CARD_CLASS_NAMES.shape}>
      <CardHeader
        title={
          <div className="flex items-center gap-2">
            Shape
            {hasMultiple && (
              <Hint label="Database invariant violated: multiple shape entries detected. Selecting a new option will clear them.">
                <Badge tone="warn">Multiple entries detected</Badge>
              </Hint>
            )}
          </div>
        }
        subtitle="Request / response transform. Inherits the dialect returned by the router when unset."
      />
      <CardBody>
        <BaseRadioGroup
          className="space-y-2"
          aria-busy={mutatingId !== null}
          name="shape-slot"
          onValueChange={(nextValue) => {
            void handleSelect(nextValue === 'none' ? null : nextValue);
          }}
          render={<ul />}
          value={activeEntry?.wasm_registry_id ?? 'none'}
        >
          <SlotRadioCard
            value="none"
            name="None"
            desc="Inherits the dialect returned by the router (typically anthropic-direct)."
            isActive={!activeEntry}
            isMutating={mutatingId === 'none'}
            isMutatingOther={mutatingId !== null && mutatingId !== 'none'}
            isNone
            badge="Off"
          />
          {candidates
            .filter((p) => pluginSupportsSlot(p, 'shape'))
            .map((p) => (
              <SlotRadioCard
                key={p.id}
                value={p.id}
                name={p.name}
                desc={p.label || 'Custom shape plugin'}
                isActive={activeEntry?.wasm_registry_id === p.id}
                isMutating={mutatingId === p.id}
                isMutatingOther={mutatingId !== null && mutatingId !== p.id}
              />
            ))}
        </BaseRadioGroup>
      </CardBody>
    </Card>
  );
}

function ObservabilityHookEditor({ principalId }: { principalId: string }) {
  const slot = 'observability_hook';
  const label = 'Observability';
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
    <Card className={PRINCIPAL_DETAIL_CARD_CLASS_NAMES.observability}>
      <CardHeader
        title="Observability"
        subtitle="SSE / audit hooks. Executed in order. Multiple allowed."
        action={
          <Button
            size="sm"
            iconLeft={<Plus className="w-3 h-3" />}
            onClick={() => setAddOpen(true)}
          >
            Add
          </Button>
        }
      />
      <CardBody>
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
              {registry.data?.entries
                .filter((p) => pluginSupportsSlot(p, 'observability_hook'))
                .map((p) => (
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
      </CardBody>
    </Card>
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

const API_KEY_SKELETON_CELL_CLASSES = [
  'px-4',
  'px-4',
  'px-4',
  'px-4',
  'px-4',
  'px-4',
  'px-4 w-8',
] as const;

const API_KEY_SKELETON_CLASSES = [
  'w-24',
  'w-48',
  'w-12',
  'w-20',
  'w-20',
  'w-16',
  'w-4',
] as const;

export function ApiKeysCard({ principal }: { principal: Principal }) {
  const keys = usePrincipalKeys(principal.id);
  const issue = useIssueKey();
  const revoke = useRevokeKey();
  const { copy } = useCopyButton();
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
    <Card className={PRINCIPAL_DETAIL_CARD_CLASS_NAMES.apiKeys}>
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
      <div
        className="overflow-x-auto min-h-32"
        data-testid="api-keys-table-slot"
      >
        <table className="w-full min-w-[840px] font-mono text-xs">
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
            {keys.isLoading ? (
              Array.from({ length: 3 }).map((_, i) => (
                <SkeletonRow
                  key={i}
                  cols={7}
                  cellClassNames={API_KEY_SKELETON_CELL_CLASSES}
                  skeletonClassNames={API_KEY_SKELETON_CLASSES}
                />
              ))
            ) : keys.data?.keys.length ? (
              keys.data.keys.map((k) => (
                <tr key={k.key_id} className="border-b border-row">
                  <td className="px-4 py-2">{k.label ?? '—'}</td>
                  <td className="px-4 py-2">{k.key_id}</td>
                  <td className="px-4 py-2 text-text-faint">
                    {k.last_4 ? `···${k.last_4}` : '—'}
                  </td>
                  <td className="px-4 py-2">
                    <RelativeTime
                      compact
                      ts={new Date(k.issued_at_unix_secs * 1000)}
                    />
                  </td>
                  <td className="px-4 py-2">
                    <RelativeTime
                      compact
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
              ))
            ) : (
              <tr className="border-b border-row">
                <td
                  colSpan={7}
                  className="h-24 px-4 py-4 align-top text-xs text-text-faint"
                >
                  No API keys issued.
                </td>
              </tr>
            )}
          </tbody>
        </table>
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
                onClick={() => copy(issued.plaintext_key)}
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
  const [defaultLimits, setDefaultLimits] = useState<PrincipalDefaultLimit[]>(
    [],
  );
  const reset = () => {
    setName('');
    setKind('human');
    setDefaultLimits([]);
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
                { name, kind, default_limits: defaultLimits },
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
        <div className="flex flex-col gap-1.5">
          <span className="text-[11px] uppercase tracking-wider text-text-faint">
            Default limits
          </span>
          <LimitsEditor value={defaultLimits} onChange={setDefaultLimits} />
          <span className="text-[11px] text-text-faint">
            Optional. Applied to every API key issued for this principal.
          </span>
        </div>
      </div>
    </Modal>
  );
}
