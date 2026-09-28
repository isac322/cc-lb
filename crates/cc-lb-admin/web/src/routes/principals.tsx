import { NumberField as BaseNumberField } from '@base-ui/react/number-field';
import { Popover as BasePopover } from '@base-ui/react/popover';
import { Radio as BaseRadio } from '@base-ui/react/radio';
import { RadioGroup as BaseRadioGroup } from '@base-ui/react/radio-group';
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
import { useQueryClient } from '@tanstack/react-query';
import {
  createFileRoute,
  stripSearchParams,
  useNavigate,
} from '@tanstack/react-router';
import {
  ArrowDown,
  ArrowUp,
  CheckCircle2,
  ChevronDown,
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
import { PrincipalsListEmpty } from '../components/onboarding/ListEmptyStates';
import { CacheKeepaliveCard } from '../components/principals/cache-keepalive/CacheKeepaliveCard';
import {
  DetailHeader,
  DetailHeaderSkeleton,
  DetailPane,
  DetailSection,
  DetailSectionGrid,
} from '../components/ui/DetailPane';
import {
  EntityList,
  type EntityListView,
  type EntityListViewConfig,
  useEntityListView,
} from '../components/ui/EntityList';
import {
  Badge,
  Button,
  ConfirmDialog,
  cx,
  Drawer,
  EmptyState,
  Field,
  Hint,
  IconButton,
  INPUT_CLASS,
  Modal,
  Skeleton,
  SkeletonRow,
  Spinner,
  StatusBadge,
  ToggleSwitch,
} from '../components/ui/primitives';
import { RelativeTime } from '../components/ui/RelativeTime';
import { RequestEventsTable } from '../components/ui/RequestEventsTable';
import { Select } from '../components/ui/Select';
import {
  EmptyValue,
  Table,
  TableCell,
  TableHead,
  TableHeadCell,
  TableRow,
} from '../components/ui/Table';
import { formatCount } from '../lib/format';
import { isMessagesRequestEvent } from '../lib/logRows';
import {
  type ChainSlot,
  type LimitKind,
  type PluginEntry,
  type Principal,
  type PrincipalDefaultLimit,
  qk,
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
  usePrincipalWritePending,
  useRecentEvents,
  useReorderChain,
  useRevokeKey,
  useRouterTerminalStrategy,
  useSetAllowedModels,
  useTogglePrincipal,
  useUpdatePrincipalDefaultLimits,
  useUpdateRouterTerminalStrategy,
  useUpstreamNameMap,
  useUsage,
} from '../lib/queries';
import {
  PRINCIPAL_LIMITS_ANCHOR,
  PRINCIPAL_ROUTER_ANCHOR,
} from '../lib/requestErrorCodes';
import { undoToast } from '../lib/undoToast';
import { useCopyButton } from '../lib/useCopyButton';

const PRINCIPAL_FILTERS = [
  'all',
  'enabled',
  'disabled',
  'human',
  'machine',
  'admin',
] as const;
type PrincipalFilter = (typeof PRINCIPAL_FILTERS)[number];
// Principal carries no update timestamp, so there is no "Recently updated".
const PRINCIPAL_SORTS = ['name', 'active'] as const;
type PrincipalSort = (typeof PRINCIPAL_SORTS)[number];

const principalSearchSchema = z.object({
  selectedId: z.string().optional(),
  action: z.literal('new').optional(),
  q: z.string().default('').catch(''),
  filter: z.enum(PRINCIPAL_FILTERS).default('all').catch('all'),
  sort: z.enum(PRINCIPAL_SORTS).default('name').catch('name'),
});

const PRINCIPAL_FILTER_OPTIONS: readonly {
  value: PrincipalFilter;
  label: string;
}[] = [
  { value: 'all', label: 'All principals' },
  { value: 'enabled', label: 'Enabled' },
  { value: 'disabled', label: 'Disabled' },
  { value: 'human', label: 'Human' },
  { value: 'machine', label: 'Machine' },
  { value: 'admin', label: 'Admin' },
];
const PRINCIPAL_SORT_OPTIONS: readonly {
  value: PrincipalSort;
  label: string;
}[] = [
  { value: 'name', label: 'Name' },
  { value: 'active', label: 'Most active (24h)' },
];
const EMPTY_PRINCIPALS: readonly Principal[] = [];

function compareNames(a: { name: string }, b: { name: string }): number {
  return a.name.localeCompare(b.name, undefined, {
    numeric: true,
    sensitivity: 'base',
  });
}

function principalId(p: Principal): string {
  return p.id;
}

const PRINCIPAL_VIEW_DEFAULTS = {
  q: '',
  filter: 'all',
  sort: 'name',
} as const satisfies EntityListView<PrincipalFilter, PrincipalSort>;

// The table keeps the one flat surface in an otherwise unboxed section.
const PRINCIPAL_RECENT_REQUESTS_TABLE_SLOT_CLASS =
  'glass rounded-md overflow-x-auto min-h-48';
const PRINCIPAL_KIND_LABEL: Record<Principal['kind'], string> = {
  machine: 'Machine',
  human: 'Human',
  admin: 'Admin',
};
// Minimums measured on the unboxed sections against an admin fixture at
// 1440×1000 so the loading shell and the loaded detail keep the same geometry.
const PRINCIPAL_DETAIL_CARD_CLASS_NAMES = {
  access: 'min-h-[143px]',
  recentRequests: 'min-h-[301px]',
  router: 'min-h-[324px]',
  apiKeys: 'min-h-[190px]',
} as const;
const EMPTY_PRINCIPAL_DETAIL_NAME_MAP = new Map<string, string>();

// INPUT_CLASS carries no disabled affordance of its own; blocking mutations
// need every locked control to read as unavailable, not merely inert.
const PENDING_INPUT_CLASS =
  'disabled:cursor-not-allowed disabled:border-subtle disabled:text-text-faint';

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
  // Defaults stay out of the address bar.
  search: { middlewares: [stripSearchParams(PRINCIPAL_VIEW_DEFAULTS)] },
  component: PrincipalsPage,
});

function PrincipalDetailLoadingShell() {
  return (
    <DetailPane
      aria-busy="true"
      aria-label="Loading principal details"
      data-testid="principal-detail-loading-shell"
      role="status"
      header={<DetailHeaderSkeleton />}
    >
      <DetailSectionGrid>
        <DetailSection
          span="full"
          data-testid="cache-keepalive-card"
          title={<Skeleton as="span" className="block h-5 w-32" />}
          description={<Skeleton as="span" className="block h-4 w-64" />}
          action={
            <div className="flex items-center gap-2">
              <Skeleton className="h-7 w-24" />
              <Skeleton className="h-7 w-24" />
              <Skeleton className="h-5 w-9" />
            </div>
          }
        >
          <div className="grid grid-cols-2 gap-y-5 md:grid-cols-4">
            {Array.from({ length: 4 }).map((_, index) => (
              <div key={index} className="flex flex-col gap-1 pr-5">
                <Skeleton className="h-3 w-24" />
                <div className="flex h-9 items-center">
                  <Skeleton className="h-7 w-20" />
                </div>
                <Skeleton className="h-3 w-32" />
              </div>
            ))}
          </div>
        </DetailSection>

        <DetailSection
          span="full"
          className={PRINCIPAL_DETAIL_CARD_CLASS_NAMES.recentRequests}
          title={<Skeleton as="span" className="block h-5 w-32" />}
          description={<Skeleton as="span" className="block h-4 w-48" />}
        >
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
            />
          </div>
        </DetailSection>

        <DetailSection
          span="full"
          className={PRINCIPAL_DETAIL_CARD_CLASS_NAMES.router}
          title={<Skeleton as="span" className="block h-5 w-24" />}
          description={<Skeleton as="span" className="block h-8 w-full" />}
          action={<Skeleton className="h-7 w-24" />}
        >
          <div className="space-y-3">
            <Skeleton className="h-10" />
            <Skeleton className="h-10" />
            <Skeleton className="h-28" />
          </div>
        </DetailSection>

        <DetailSection
          span="full"
          title={<Skeleton as="span" className="block h-5 w-24" />}
          description={<Skeleton as="span" className="block h-4 w-96" />}
        >
          <div className="space-y-2">
            <Skeleton className="h-14" />
            <Skeleton className="h-14" />
          </div>
        </DetailSection>

        <DetailSection
          span="full"
          className={PRINCIPAL_DETAIL_CARD_CLASS_NAMES.apiKeys}
          title={<Skeleton as="span" className="block h-5 w-24" />}
          description={<Skeleton as="span" className="block h-4 w-44" />}
          action={<Skeleton className="h-7 w-24" />}
        >
          <div className="space-y-3">
            <Skeleton className="h-9" />
            <Skeleton className="h-9" />
          </div>
        </DetailSection>

        <DetailSection
          className={PRINCIPAL_DETAIL_CARD_CLASS_NAMES.access}
          title={<Skeleton as="span" className="block h-5 w-20" />}
          description={<Skeleton as="span" className="block h-4 w-64" />}
        >
          <div className="space-y-5">
            {Array.from({ length: 2 }).map((_, index) => (
              <div
                key={index}
                className="flex items-center justify-between gap-4"
              >
                <div className="flex items-center gap-4 min-w-0 flex-1">
                  <Skeleton className="h-4 w-28" />
                  <Skeleton className="h-4 w-2/5" />
                </div>
                <Skeleton className="h-7 w-14" />
              </div>
            ))}
          </div>
        </DetailSection>

        <DetailSection
          title={<Skeleton as="span" className="block h-5 w-32" />}
          description={<Skeleton as="span" className="block h-4 w-64" />}
          action={<Skeleton className="h-7 w-16" />}
        >
          <Skeleton className="h-10" />
        </DetailSection>
      </DetailSectionGrid>
    </DetailPane>
  );
}

function PrincipalsPage() {
  const search = Route.useSearch();
  const { selectedId, action } = search;
  const view: EntityListView<PrincipalFilter, PrincipalSort> = {
    q: search.q ?? '',
    filter: search.filter ?? 'all',
    sort: search.sort ?? 'name',
  };
  const navigate = useNavigate({ from: Route.fullPath });
  const principals = usePrincipals();
  // Same key as Overview's Top principals at 24h: one request for the list.
  const usage24h = useUsage('24h', 'hour', 'principal', undefined, 'totals');
  const [createOpen, setCreateOpen] = useState(false);

  const all = principals.data?.principals ?? EMPTY_PRINCIPALS;
  const requests24h = useMemo(() => {
    const byId = new Map<string, number>();
    for (const series of usage24h.data?.series ?? []) {
      if (!series.key) continue;
      let requests = 0;
      for (const bucket of series.buckets)
        requests += bucket.request_count ?? 0;
      byId.set(series.key, requests);
    }
    return byId;
  }, [usage24h.data]);
  const usagePending = usage24h.data === undefined && usage24h.isPending;

  const viewConfig = useMemo<
    EntityListViewConfig<Principal, PrincipalFilter, PrincipalSort>
  >(
    () => ({
      searchText: (p) => [p.name, p.id, PRINCIPAL_KIND_LABEL[p.kind]],
      defaultFilter: 'all',
      filters: {
        all: () => true,
        enabled: (p) => p.enabled,
        disabled: (p) => !p.enabled,
        human: (p) => p.kind === 'human',
        machine: (p) => p.kind === 'machine',
        admin: (p) => p.kind === 'admin',
      },
      sorts: {
        name: compareNames,
        active: (a, b) =>
          (requests24h.get(b.id) ?? 0) - (requests24h.get(a.id) ?? 0) ||
          compareNames(a, b),
      },
    }),
    [requests24h],
  );
  const listView = useEntityListView(all, view, viewConfig);

  const selected = all.find((p) => p.id === selectedId) ?? null;
  // `resetScroll: false`: the router's scroll restoration would otherwise
  // snap the list pane back to its top on every selection, so arrowing to
  // a row below the fold would leave it out of view.
  const select = (id: string | undefined) =>
    navigate({ search: { selectedId: id, ...view }, resetScroll: false });
  const changeView = (
    patch: Partial<EntityListView<PrincipalFilter, PrincipalSort>>,
  ) =>
    navigate({
      replace: true,
      resetScroll: false,
      search: { selectedId, ...view, ...patch },
    });

  useEffect(() => {
    if (action !== 'new') return;
    setCreateOpen(true);
    navigate({
      search: (prev) => ({ ...prev, action: undefined }),
      replace: true,
    });
  }, [action, navigate]);

  // From md the detail pane is never blank: pick the first row of the
  // current sort and filter.
  const firstVisibleId = listView.visible[0]?.id;
  useEffect(() => {
    if (principals.isLoading || selected || !firstVisibleId) return;
    if (window.matchMedia('(min-width: 768px)').matches) {
      navigate({
        search: (prev) => ({ ...prev, selectedId: firstVisibleId }),
        replace: true,
        resetScroll: false,
      });
    }
  }, [principals.isLoading, selected, firstVisibleId, navigate]);

  const disabledCount = all.filter((p) => !p.enabled).length;

  return (
    <div className="h-shell min-h-0 flex w-full max-w-[90rem] mx-auto">
      <EntityList
        className={cx(
          'w-full shrink-0 border-r border-subtle md:w-[360px] xl:w-[400px]',
          selected ? 'hidden md:flex' : 'flex',
        )}
        title="Principals"
        noun="principals"
        countLine={
          principals.isLoading
            ? null
            : [
                `${formatCount(all.length)} ${all.length === 1 ? 'principal' : 'principals'}`,
                disabledCount > 0
                  ? `${formatCount(disabledCount)} disabled`
                  : null,
              ]
                .filter(Boolean)
                .join(' · ')
        }
        countSkeletonTestId="principal-count-skeleton"
        action={
          <Button
            id="btn-new-principal"
            size="sm"
            variant="primary"
            iconLeft={<Plus />}
            onClick={() => setCreateOpen(true)}
          >
            New
          </Button>
        }
        loading={principals.isLoading}
        totalCount={all.length}
        result={listView}
        toolbar={{
          view,
          filterOptions: PRINCIPAL_FILTER_OPTIONS,
          sortOptions: PRINCIPAL_SORT_OPTIONS,
          onViewChange: changeView,
          onClear: () => changeView({ q: '', filter: 'all' }),
        }}
        getId={principalId}
        renderRow={(p) => {
          const requests = requests24h.get(p.id) ?? 0;
          const facts = [
            PRINCIPAL_KIND_LABEL[p.kind],
            p.allowed_models.length === 0
              ? 'Any model'
              : `${p.allowed_models.length} ${p.allowed_models.length === 1 ? 'model' : 'models'}`,
            p.default_limits.length === 0
              ? 'No limits'
              : `${p.default_limits.length} ${p.default_limits.length === 1 ? 'limit' : 'limits'}`,
          ].join(' · ');
          return {
            name: p.name,
            muted: !p.enabled,
            title: `${p.name}\n${p.enabled ? '' : 'Disabled · '}${facts}`,
            trailing: usagePending ? (
              <Skeleton as="span" className="inline-block h-3 w-6" />
            ) : requests > 0 ? (
              <span className="text-text-muted">
                {formatCount(requests)}
                <span className="sr-only"> requests in 24h</span>
              </span>
            ) : (
              <EmptyValue label="No requests in 24h" />
            ),
            caption: (
              <span className="truncate">
                {p.enabled ? null : (
                  <span className="text-text-muted">Disabled · </span>
                )}
                {facts}
              </span>
            ),
          };
        }}
        selectedId={selectedId}
        onSelect={select}
        empty={<PrincipalsListEmpty onCreate={() => setCreateOpen(true)} />}
        skeletonTestId="principal-list-skeleton"
      />

      {/* Phones show the list or the selected principal, never both; from md
          the detail sits beside the list and DetailPane owns its scroll. */}
      <div
        className={cx(
          'min-h-0 min-w-0 flex-1 flex-col bg-bg',
          selected ? 'flex' : 'hidden md:flex',
        )}
        data-testid="principal-detail-pane"
      >
        {selected ? (
          <PrincipalDetail
            key={selected.id}
            principal={selected}
            onBack={() => select(undefined)}
          />
        ) : principals.isLoading ? (
          <PrincipalDetailLoadingShell />
        ) : (
          <div className="flex flex-1 items-center justify-center px-4 md:px-8">
            {principals.data?.principals.length ? (
              <EmptyState
                headingLevel={2}
                title="Select a principal"
                description="Pick a principal to see allowed models, default limits, plugin chain, and API keys."
              />
            ) : (
              <EmptyState
                headingLevel={2}
                title="Principal details appear here"
                description="Allowed models, limits, plugin chain, and proxy keys show here once you create a principal."
              />
            )}
          </div>
        )}
      </div>

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
  const principalWritePending = usePrincipalWritePending(principal.id) > 0;
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
  const [confirmToggleOpen, setConfirmToggleOpen] = useState(false);
  const nextEnabled = !principal.enabled;
  const titleId = `principal-detail-title-${principal.id}`;
  return (
    <DetailPane
      aria-labelledby={titleId}
      header={
        <DetailHeader
          backLabel="All principals"
          onBack={onBack}
          title={principal.name}
          titleId={titleId}
          badge={<Badge>{PRINCIPAL_KIND_LABEL[principal.kind]}</Badge>}
          meta={[
            <span key="rev" className="tabular-nums">
              Revision {principal.revision}
            </span>,
          ]}
          id={principal.id}
          idLabel="Principal ID"
          enabled={principal.enabled}
          onEnabledChange={() => setConfirmToggleOpen(true)}
          enabledDisabled={
            toggle.isPending || del.isPending || principalWritePending
          }
          pendingLabel={
            toggle.isPending
              ? nextEnabled
                ? 'Enabling...'
                : 'Disabling...'
              : null
          }
          onDelete={() => setConfirmDeleteOpen(true)}
          deleteDisabled={toggle.isPending || principalWritePending}
          deleting={del.isPending}
        />
      }
    >
      <ConfirmDialog
        open={confirmToggleOpen}
        onOpenChange={setConfirmToggleOpen}
        title={nextEnabled ? 'Enable principal?' : 'Disable principal?'}
        description={
          nextEnabled ? (
            <>
              Requests using API keys of{' '}
              <span className="font-medium text-text">{principal.name}</span>{' '}
              will be accepted again.
            </>
          ) : (
            <>
              Requests using API keys of{' '}
              <span className="font-medium text-text">{principal.name}</span>{' '}
              will be rejected with 403 until it is enabled again. Keys, limits
              and plugin chain are kept.
            </>
          )
        }
        confirmLabel={nextEnabled ? 'Enable' : 'Disable'}
        destructive={!nextEnabled}
        pending={toggle.isPending}
        confirmDisabled={del.isPending || principalWritePending}
        closeOnConfirm={false}
        onConfirm={() =>
          toggle.mutate(
            {
              id: principal.id,
              enabled: nextEnabled,
              revision: principal.revision,
            },
            {
              onSuccess: () => {
                setConfirmToggleOpen(false);
                toast.success(
                  nextEnabled ? 'Principal enabled' : 'Principal disabled',
                );
              },
            },
          )
        }
      />
      <ConfirmDialog
        open={confirmDeleteOpen}
        onOpenChange={setConfirmDeleteOpen}
        title="Delete principal?"
        description={
          <>
            <span className="font-medium text-text">{principal.name}</span> and
            all its API keys / plugin chain entries will be permanently removed.
            <span className="block mt-2 text-text-muted">
              {deleteChainCountLabel}
            </span>
          </>
        }
        confirmLabel="Delete"
        destructive
        pending={del.isPending}
        confirmDisabled={principalWritePending}
        closeOnConfirm={false}
        onConfirm={() =>
          del.mutate(
            { id: principal.id, revision: principal.revision },
            {
              onSuccess: () => {
                setConfirmDeleteOpen(false);
                toast.success('Principal deleted');
                onBack();
              },
            },
          )
        }
      />

      {/* Reading order. From 56rem only Access and Observability share a row:
          measured at half width, Router runs ~150px taller than Shape and
          API keys is a wide table, so pairing any of them leaves a hole. */}
      <DetailSectionGrid>
        {/* `contents` keeps the keepalive section itself the grid item. */}
        <fieldset className="contents" disabled={principalWritePending}>
          <CacheKeepaliveCard principal={principal} />
        </fieldset>
        <RecentRequestsCard principal={principal} />
        <RouterSlotEditor principal={principal} />
        <ShapeSlotEditor principalId={principal.id} />
        <ApiKeysCard principal={principal} />
        <AccessCard principal={principal} />
        <ObservabilityHookEditor principalId={principal.id} />
      </DetailSectionGrid>
    </DetailPane>
  );
}

export function RecentRequestsCard({ principal }: { principal: Principal }) {
  const principalNameMap = usePrincipalNameMap();
  const upstreamNameMap = useUpstreamNameMap();
  const recent = useRecentEvents({
    principal_id: principal.id,
    limit: '5',
    event_kind: 'messages',
  });
  const events = useMemo(() => {
    return (recent.data?.events ?? [])
      .filter(isMessagesRequestEvent)
      .map((e) => ({
        ...e,
        _phase: 'final' as const,
      }));
  }, [recent.data]);
  const loading = recent.data === undefined && recent.isPending;
  return (
    <DetailSection
      span="full"
      className={PRINCIPAL_DETAIL_CARD_CLASS_NAMES.recentRequests}
      title="Recent requests"
      description={
        <span className="inline-flex h-5 items-center">
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
    >
      <div
        className={PRINCIPAL_RECENT_REQUESTS_TABLE_SLOT_CLASS}
        data-testid="recent-requests-table-slot"
      >
        <RequestEventsTable
          events={events}
          principalNameMap={principalNameMap}
          upstreamNameMap={upstreamNameMap}
          loading={loading}
          columns={{
            principal: false,
            cost: true,
            tokens: true,
          }}
          emptyTitle="No recent requests for this principal"
        />
      </div>
    </DetailSection>
  );
}

function AccessCard({ principal }: { principal: Principal }) {
  return (
    <DetailSection
      className={PRINCIPAL_DETAIL_CARD_CLASS_NAMES.access}
      title="Access"
      description="Which models this principal may call, and the rate caps applied to every API key"
    >
      <div className="space-y-6">
        <AllowedModelsRow principal={principal} />
        <DefaultLimitsRow principal={principal} />
      </div>
    </DetailSection>
  );
}

/**
 * One labelled row of the Access section. Each row is its own region with
 * its own edit cycle, so the heading names the region its controls belong to.
 */
function AccessRow({
  id,
  title,
  action,
  children,
}: {
  id?: string;
  title: string;
  action: React.ReactNode;
  children: React.ReactNode;
}) {
  const headingId = React.useId();
  return (
    <section id={id} aria-labelledby={headingId} className="scroll-mt-4">
      <div className="grid grid-cols-[minmax(0,1fr)_auto] items-start gap-x-4 gap-y-2 sm:grid-cols-[8rem_minmax(0,1fr)_auto]">
        <h4
          id={headingId}
          className="col-start-1 row-start-1 self-center text-label text-text-muted sm:self-start sm:pt-1.5"
        >
          {title}
        </h4>
        <div className="col-span-2 row-start-2 min-w-0 text-body text-text sm:col-span-1 sm:col-start-2 sm:row-start-1 sm:pt-1">
          {children}
        </div>
        <div className="col-start-2 row-start-1 flex items-center justify-end gap-2 sm:col-start-3">
          {action}
        </div>
      </div>
    </section>
  );
}

function AllowedModelsRow({ principal }: { principal: Principal }) {
  const setAllowed = useSetAllowedModels();
  const principalWritePending = usePrincipalWritePending(principal.id) > 0;
  const [models, setModels] = useState(principal.allowed_models.join(', '));
  const [editing, setEditing] = useState(false);
  const [editRevision, setEditRevision] = useState<number | null>(null);
  const saving = setAllowed.isPending;

  const startEditing = () => {
    setModels(principal.allowed_models.join(', '));
    setEditRevision(principal.revision);
    setEditing(true);
  };

  const cancelEditing = () => {
    setEditing(false);
    setEditRevision(null);
    setModels(principal.allowed_models.join(', '));
  };

  return (
    <AccessRow
      title="Allowed models"
      action={
        editing ? (
          <>
            <Button
              size="sm"
              disabled={saving || principalWritePending}
              onClick={cancelEditing}
            >
              Cancel
            </Button>
            <Button
              size="sm"
              variant="primary"
              loading={saving}
              disabled={
                saving || principalWritePending || editRevision === null
              }
              onClick={() => {
                if (editRevision === null) return;
                setAllowed.mutate(
                  {
                    id: principal.id,
                    models: models
                      .split(',')
                      .map((m) => m.trim())
                      .filter(Boolean),
                    expected_revision: editRevision,
                  },
                  {
                    onSuccess: () => {
                      toast.success('Allowed models updated');
                      setEditing(false);
                      setEditRevision(null);
                    },
                  },
                );
              }}
            >
              {saving ? 'Saving...' : 'Save'}
            </Button>
          </>
        ) : (
          <Button
            size="sm"
            disabled={principalWritePending}
            onClick={startEditing}
          >
            Edit
          </Button>
        )
      }
    >
      {editing ? (
        <textarea
          className={cx(
            INPUT_CLASS,
            PENDING_INPUT_CLASS,
            'min-h-[80px] py-2 font-mono text-data',
          )}
          value={models}
          onChange={(e) => setModels(e.target.value)}
          placeholder="Comma-separated model IDs"
          disabled={saving || principalWritePending}
        />
      ) : principal.allowed_models.length ? (
        <div className="flex flex-wrap gap-1.5">
          {principal.allowed_models.map((m) => (
            <Badge key={m} tone="mono">
              {m}
            </Badge>
          ))}
        </div>
      ) : (
        <p>
          Any model{' '}
          <span className="text-text-muted">
            · no restrictions, the upstream decides what it supports
          </span>
        </p>
      )}
    </AccessRow>
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
  disabled = false,
}: {
  value: PrincipalDefaultLimit[];
  onChange: (next: PrincipalDefaultLimit[]) => void;
  disabled?: boolean;
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
    <div className="@container space-y-3">
      {value.length === 0 ? (
        <p className="text-caption text-text-faint">
          No limits. Add one below.
        </p>
      ) : null}
      {value.map((row, idx) => {
        const isCost = row.kind === 'cost_usd';
        const capValue = isCost ? row.cap_micros / 1_000_000 : row.cap_micros;
        return (
          <div
            key={idx}
            className="grid grid-cols-2 items-end gap-3 pb-3 border-b border-row @xl:grid-cols-[minmax(0,1fr)_7rem_minmax(0,1fr)_auto]"
          >
            <div className="min-w-0">
              <Field label="Kind">
                <Select
                  className="w-full"
                  disabled={disabled}
                  value={row.kind}
                  options={LIMIT_KIND_OPTIONS}
                  onChange={(next) =>
                    updateRow(idx, { kind: next as LimitKind })
                  }
                />
              </Field>
            </div>
            <div className="min-w-0">
              <Field label="Window (sec)">
                <BaseNumberField.Root
                  disabled={disabled}
                  min={1}
                  step={1}
                  value={row.window_secs}
                  onValueChange={(nextValue) =>
                    updateRow(idx, {
                      window_secs: Math.max(1, Math.floor(nextValue ?? 0)),
                    })
                  }
                >
                  <BaseNumberField.Input
                    className={cx(INPUT_CLASS, PENDING_INPUT_CLASS)}
                  />
                </BaseNumberField.Root>
              </Field>
            </div>
            <div className="min-w-0">
              <Field label={isCost ? 'Cap (USD)' : 'Cap'}>
                <BaseNumberField.Root
                  disabled={disabled}
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
                  <BaseNumberField.Input
                    className={cx(INPUT_CLASS, PENDING_INPUT_CLASS)}
                  />
                </BaseNumberField.Root>
              </Field>
            </div>
            <Button
              variant="ghost"
              size="lg"
              className="justify-self-start hover:text-danger-text"
              iconLeft={<Trash2 />}
              disabled={disabled}
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
        disabled={disabled}
        onClick={addRow}
      >
        Add limit
      </Button>
    </div>
  );
}

function DefaultLimitsRow({ principal }: { principal: Principal }) {
  const update = useUpdatePrincipalDefaultLimits();
  const principalWritePending = usePrincipalWritePending(principal.id) > 0;
  const saving = update.isPending;
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState<PrincipalDefaultLimit[]>(
    principal.default_limits,
  );
  const [editRevision, setEditRevision] = useState<number | null>(null);

  const startEditing = () => {
    setDraft(principal.default_limits.map((limit) => ({ ...limit })));
    setEditRevision(principal.revision);
    setEditing(true);
  };

  const cancelEditing = () => {
    setEditing(false);
    setEditRevision(null);
    setDraft(principal.default_limits.map((limit) => ({ ...limit })));
  };

  return (
    <AccessRow
      id={PRINCIPAL_LIMITS_ANCHOR}
      title="Default limits"
      action={
        editing ? (
          <>
            <Button
              size="sm"
              disabled={saving || principalWritePending}
              onClick={cancelEditing}
            >
              Cancel
            </Button>
            <Button
              size="sm"
              variant="primary"
              loading={saving}
              disabled={
                saving || principalWritePending || editRevision === null
              }
              onClick={() => {
                if (editRevision === null) return;
                update.mutate(
                  {
                    id: principal.id,
                    default_limits: draft,
                    expected_revision: editRevision,
                  },
                  {
                    onSuccess: () => {
                      toast.success('Default limits updated');
                      setEditing(false);
                      setEditRevision(null);
                    },
                  },
                );
              }}
            >
              {saving ? 'Saving...' : 'Save'}
            </Button>
          </>
        ) : (
          <Button
            size="sm"
            disabled={principalWritePending}
            onClick={startEditing}
          >
            Edit
          </Button>
        )
      }
    >
      {editing ? (
        <LimitsEditor
          value={draft}
          onChange={setDraft}
          disabled={saving || principalWritePending}
        />
      ) : principal.default_limits.length ? (
        <ul className="divide-y divide-row">
          {principal.default_limits.map((l, idx) => (
            <li
              key={`${l.kind}-${l.window_secs}-${idx}`}
              className="flex items-baseline justify-between gap-4 py-1 first:pt-0 last:pb-0"
            >
              <span>{LIMIT_KIND_LABEL[l.kind] ?? l.kind}</span>
              <span className="tabular-nums">
                {formatLimitCap(l)}
                <span className="text-text-muted">
                  {' '}
                  per {formatWindowSecs(l.window_secs)}
                </span>
              </span>
            </li>
          ))}
        </ul>
      ) : (
        <p>
          None{' '}
          <span className="text-text-muted">
            · every API key runs without a default rate cap
          </span>
        </p>
      )}
    </AccessRow>
  );
}

const PLUGIN_DRAWER_HEADING_CLASS =
  'mb-1.5 flex items-center gap-1.5 text-label text-text-muted';

// Matches the shared underline tabs: 14/500, 36px tall (40px on phones),
// 2px accent rule.
const ROUTER_TAB_CLASS =
  'h-9 max-md:h-10 px-3 text-body font-medium border-b-2 -mb-px transition-colors border-transparent text-text-muted hover:text-text data-[active]:border-accent data-[active]:text-text';

/** Radio ring shared by the slot and terminal-strategy option cards. */
function RadioMark({ isMutating }: { isMutating: boolean }) {
  return isMutating ? (
    <Spinner className="w-3 h-3 text-text-muted" />
  ) : (
    <span className="flex size-4 items-center justify-center rounded-full border border-subtle-strong">
      <BaseRadio.Indicator className="size-2 rounded-full bg-accent" />
    </span>
  );
}

function SlotRadioCard({
  value,
  name,
  desc,
  isActive,
  isMutating,
  disabled,
}: {
  value: string;
  name: string;
  desc: string;
  isActive: boolean;
  isMutating: boolean;
  disabled: boolean;
}) {
  return (
    <li role="radio" aria-checked={isActive}>
      <label
        className={cx(
          'relative flex items-start gap-3 p-3 border rounded-sm cursor-pointer transition-colors',
          isActive
            ? 'border-accent/60 bg-accent-dim'
            : 'border-subtle hover:bg-overlay-2',
          disabled ? 'pointer-events-none control-disabled' : '',
        )}
      >
        <BaseRadio.Root
          className="mt-0.5 flex items-center justify-center w-4 h-4 shrink-0"
          disabled={disabled}
          value={value}
        >
          <RadioMark isMutating={isMutating} />
        </BaseRadio.Root>
        <div className="flex-1 min-w-0">
          <div
            className={cx(
              'text-body font-medium truncate',
              disabled ? 'text-text-faint' : 'text-text',
            )}
          >
            {name}
          </div>
          <div
            className={cx(
              'text-body mt-0.5',
              disabled ? 'text-text-faint' : 'text-text-muted',
            )}
          >
            {desc}
          </div>
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
    <Drawer
      open={open}
      onOpenChange={onOpenChange}
      width="lg"
      title={plugin.name}
      description={
        <span className="flex items-center gap-2 flex-wrap mt-1">
          {plugin.is_builtin && <Badge tone="accent">Built-in</Badge>}
          {plugin.supported_slots && plugin.supported_slots.length > 0 ? (
            plugin.supported_slots.map((slot) => (
              <Badge key={slot}>{slotLabel(slot)}</Badge>
            ))
          ) : (
            <Badge tone="warn">Unknown slot</Badge>
          )}
        </span>
      }
    >
      <div className="p-4 pb-8 space-y-6 text-body text-text">
        {plugin.metadata ? (
          <>
            <section data-testid="plugin-purpose">
              <h3 className={PLUGIN_DRAWER_HEADING_CLASS}>Purpose</h3>
              <p>{plugin.metadata.purpose}</p>
            </section>

            <section data-testid="plugin-keeps">
              <h3 className={PLUGIN_DRAWER_HEADING_CLASS}>
                <CheckCircle2
                  className="w-3.5 h-3.5 text-success-text"
                  aria-hidden="true"
                />{' '}
                Keeps
              </h3>
              <p>{plugin.metadata.keeps}</p>
            </section>

            <section data-testid="plugin-drops">
              <h3 className={PLUGIN_DRAWER_HEADING_CLASS}>
                <XCircle
                  className="w-3.5 h-3.5 text-danger-text"
                  aria-hidden="true"
                />{' '}
                Drops
              </h3>
              <p>{plugin.metadata.drops}</p>
            </section>

            <section data-testid="plugin-empty-behavior">
              <h3 className={PLUGIN_DRAWER_HEADING_CLASS}>Empty behavior</h3>
              <p>{plugin.metadata.empty_behavior}</p>
            </section>

            <section data-testid="plugin-examples">
              <h3 className={PLUGIN_DRAWER_HEADING_CLASS}>Examples</h3>
              <ul className="list-disc pl-4 space-y-1">
                {plugin.metadata.examples.map((ex, i) => (
                  <li key={i}>{ex}</li>
                ))}
              </ul>
            </section>
          </>
        ) : (
          <p className="text-text-muted">
            Built by operator. No description was supplied with this plugin.
          </p>
        )}

        <section className="pt-4">
          <h3 className={PLUGIN_DRAWER_HEADING_CLASS}>Technical details</h3>
          <dl className="grid grid-cols-[7rem_minmax(0,1fr)] items-center gap-x-3 gap-y-1.5">
            {plugin.wire_version !== undefined && (
              <>
                <dt className="text-label text-text-muted">Wire version</dt>
                <dd className="tabular-nums">{plugin.wire_version}</dd>
              </>
            )}
            <dt className="text-label text-text-muted">SHA-256</dt>
            <dd className="font-mono text-data text-text-muted truncate">
              {plugin.sha256_hex.slice(0, 16)}…
            </dd>
            <dt className="text-label text-text-muted">Plugin ID</dt>
            <dd className="flex min-w-0 items-center gap-1">
              <span className="font-mono text-data text-text-muted truncate">
                {plugin.id}
              </span>
              <IconButton
                label="Copy plugin id"
                onClick={() => copy(plugin.id, 'Plugin ID')}
              >
                <Copy className="w-3 h-3" aria-hidden="true" />
              </IconButton>
            </dd>
          </dl>
        </section>
      </div>
    </Drawer>
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
    const nextRects: Record<string, DOMRect> = {};
    const measurements: Array<{
      child: HTMLElement;
      oldRect: DOMRect | undefined;
      deltaY: number;
    }> = [];

    for (const child of children) {
      const key = child.dataset.key;
      if (!key) continue;
      const newRect = child.getBoundingClientRect();
      const oldRect = oldRects.current[key];
      nextRects[key] = newRect;
      measurements.push({
        child,
        oldRect,
        deltaY: oldRect ? oldRect.top - newRect.top : 0,
      });
    }
    oldRects.current = nextRects;

    for (const { child, oldRect, deltaY } of measurements) {
      if (oldRect) {
        if (deltaY === 0) continue;
        child.style.transform = `translateY(${deltaY}px)`;
        child.style.transition = 'none';
        continue;
      }
      child.style.opacity = '0';
      child.style.transform = 'translateY(10px)';
      child.style.transition = 'none';
    }

    if (measurements.length === 0) return;
    requestAnimationFrame(() => {
      for (const { child, oldRect } of measurements) {
        child.style.transform = '';
        if (oldRect) {
          child.style.transition =
            'transform 220ms cubic-bezier(0.4, 0, 0.2, 1)';
          continue;
        }
        child.style.opacity = '1';
        child.style.transition = 'all 220ms cubic-bezier(0.4, 0, 0.2, 1)';
      }
    });
  }, [items]);
}

const TERMINAL_STRATEGY_OPTIONS = [
  {
    value: 'first-pick',
    name: 'First eligible',
    desc: 'Always pick the first upstream in the candidate list. Predictable, easy to reason about.',
  },
  {
    value: 'random',
    name: 'Random',
    desc: 'Pick a random upstream from the candidate list. Helps spread load when many are equivalent.',
  },
] as const;

function TerminalStrategyRadioGroup({
  name,
  value,
  isPending,
  disabled,
  pendingValue,
  onSelect,
}: {
  name: string;
  value: string;
  isPending: boolean;
  disabled: boolean;
  pendingValue: string | null;
  onSelect: (strategy: string) => void;
}) {
  return (
    <div className="space-y-2">
      <BaseRadioGroup
        aria-label="When multiple upstreams qualify, pick"
        aria-busy={isPending}
        aria-disabled={disabled}
        className="grid grid-cols-1 sm:grid-cols-2 gap-3"
        name={name}
        onValueChange={onSelect}
        value={value}
      >
        {TERMINAL_STRATEGY_OPTIONS.map((opt) => {
          const isActive = value === opt.value;
          const isMutating = isPending && pendingValue === opt.value;
          return (
            // Base UI's Radio.Root is the radio; the wrapper stays neutral so
            // assistive tech does not announce each option twice.
            <div key={opt.value} className="h-full">
              <label
                className={cx(
                  'relative flex items-start gap-3 p-3 border rounded-sm transition-colors h-full',
                  isActive
                    ? 'border-accent/60 bg-accent-dim'
                    : 'border-subtle hover:bg-overlay-2',
                  isPending
                    ? 'cursor-progress'
                    : disabled
                      ? 'cursor-not-allowed'
                      : 'cursor-pointer',
                  disabled &&
                    !isMutating &&
                    'pointer-events-none control-disabled',
                )}
              >
                <BaseRadio.Root
                  className="mt-0.5 flex items-center justify-center w-4 h-4 shrink-0"
                  disabled={disabled}
                  value={opt.value}
                >
                  <RadioMark isMutating={isMutating} />
                </BaseRadio.Root>
                <div className="flex-1 min-w-0">
                  <div
                    className={cx(
                      'text-body font-medium truncate',
                      disabled && !isMutating ? 'text-text-faint' : 'text-text',
                    )}
                  >
                    {opt.name}
                  </div>
                  <div
                    className={cx(
                      'text-body mt-0.5',
                      disabled && !isMutating
                        ? 'text-text-faint'
                        : 'text-text-muted',
                    )}
                  >
                    {opt.desc}
                  </div>
                </div>
              </label>
            </div>
          );
        })}
      </BaseRadioGroup>
      {isPending && (
        <p
          role="status"
          className="flex items-center gap-2 text-caption text-text-faint"
        >
          <Spinner className="w-3 h-3" />
          Updating strategy...
        </p>
      )}
    </div>
  );
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
  const principalWritePending = usePrincipalWritePending(principalId) > 0;
  // Every chain write bumps sibling revisions, so one in-flight chain mutation
  // invalidates the expected_revision any other chain edit would submit.
  const isChainBusy = reorder.isPending || insert.isPending || del.isPending;
  const routerWriteBlocked =
    isChainBusy || updateTerminalStrategy.isPending || principalWritePending;
  const insertingPluginId = insert.isPending
    ? (insert.variables?.body.wasm_registry_id ?? null)
    : null;
  const deletingEntryId = del.isPending ? (del.variables?.id ?? null) : null;
  const pendingStrategy = updateTerminalStrategy.isPending
    ? (updateTerminalStrategy.variables?.strategy ?? null)
    : null;

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

  const isChainMetadataLoading = chain.isLoading || registry.isLoading;
  const isBasicCompatible =
    !isChainMetadataLoading &&
    subscriptionPreferencePlugin !== undefined &&
    (entries.length === 0 ||
      (entries.length === 1 &&
        entries[0]?.wasm_registry_id === subscriptionPreferencePlugin.id));
  const isComplex = !isChainMetadataLoading && !isBasicCompatible;

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

  const hasSubscriptionPreference = subscriptionPreferenceEntry !== undefined;

  // The switch writes the chain through insert/delete, so it shares the chain
  // lock; only the write it started reports progress beside the control.
  const subscriptionPreferencePending =
    (insertingPluginId !== null &&
      insertingPluginId === subscriptionPreferencePlugin?.id) ||
    (deletingEntryId !== null &&
      deletingEntryId === subscriptionPreferenceEntry?.id);

  // Both router controls apply immediately; the toast's Undo re-applies the
  // previous value through the same mutation, against the revision the
  // forward write returned.
  const toggleSubscriptionPreference = () => {
    if (routerWriteBlocked) return;
    if (subscriptionPreferenceEntry) {
      const removed = subscriptionPreferenceEntry;
      del.mutate(
        { id: removed.id, revision: removed.revision },
        {
          onSuccess: () =>
            undoToast({
              message: 'Keep prompt cache warm turned off',
              onUndo: () =>
                insert.mutate(
                  {
                    pid: principalId,
                    body: {
                      slot,
                      wasm_registry_id: removed.wasm_registry_id,
                      order: removed.order,
                      config: removed.config,
                      sse_per_event: removed.sse_per_event,
                      batched_events_per_flush:
                        removed.batched_events_per_flush,
                      batched_flush_ms: removed.batched_flush_ms,
                    },
                  },
                  {
                    onSuccess: () =>
                      toast.success('Keep prompt cache warm restored'),
                  },
                ),
            }),
        },
      );
      return;
    }
    if (!subscriptionPreferencePlugin) return;
    insert.mutate(
      {
        pid: principalId,
        body: {
          slot,
          wasm_registry_id: subscriptionPreferencePlugin.id,
          order: 0,
        },
      },
      {
        onSuccess: (created) =>
          undoToast({
            message: 'Keep prompt cache warm turned on',
            onUndo: () =>
              del.mutate(
                { id: created.id, revision: created.revision },
                {
                  onSuccess: () =>
                    toast.success('Keep prompt cache warm turned off'),
                },
              ),
          }),
      },
    );
  };
  const setStrategy = (strategy: string) => {
    if (!terminalStrategy.data || routerWriteBlocked) return;
    const previous = terminalStrategy.data.strategy;
    const [nextName, previousName] = [strategy, previous].map(
      (value) =>
        TERMINAL_STRATEGY_OPTIONS.find((o) => o.value === value)?.name ?? value,
    );
    updateTerminalStrategy.mutate(
      {
        id: principalId,
        strategy,
        revision: terminalStrategy.data.revision,
      },
      {
        onSuccess: (updated) =>
          undoToast({
            message: `Terminal strategy set to ${nextName}`,
            onUndo: () =>
              updateTerminalStrategy.mutate(
                {
                  id: principalId,
                  strategy: previous,
                  revision: updated.revision,
                },
                {
                  onSuccess: () =>
                    toast.success(
                      `Terminal strategy restored to ${previousName}`,
                    ),
                },
              ),
          }),
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
    if (index === 0 || routerWriteBlocked) return;
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
    if (index === entries.length - 1 || routerWriteBlocked) return;
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
    if (routerWriteBlocked) return;
    insert.mutate(
      {
        pid: principalId,
        body: {
          slot,
          wasm_registry_id: pluginId,
          order: (entries.length + 1) * 100,
        },
      },
      { onSuccess: () => setPickerOpen(false) },
    );
  };

  const removeFilter = (id: string, revision: number) => {
    if (routerWriteBlocked) return;
    del.mutate({ id, revision });
  };

  const strategy = terminalStrategy.data?.strategy ?? 'first-pick';

  return (
    <DetailSection
      span="full"
      id={PRINCIPAL_ROUTER_ANCHOR}
      className={cx(PRINCIPAL_DETAIL_CARD_CLASS_NAMES.router, 'scroll-mt-4')}
      title="Router"
      description="Pick which upstream serves each request. Returning users stick to the upstream they hit before unless you turn that off; new users go to the first eligible upstream by default."
    >
      <div className="flex flex-col border-b border-subtle relative">
        <BaseTabs.Root
          value={activeTab}
          onValueChange={(value) => {
            if (value === 'basic' || value === 'advanced') {
              handleTabClick(value);
            }
          }}
        >
          <BaseTabs.List className="flex items-center w-fit">
            <Hint
              label={
                isComplex
                  ? 'Basic requires a router chain containing only subscription-preference. Open Advanced to edit the full chain.'
                  : ''
              }
              stopClickPropagation={false}
            >
              <BaseTabs.Tab
                aria-disabled={isComplex}
                className={cx(
                  ROUTER_TAB_CLASS,
                  'pl-0',
                  isComplex && 'control-disabled',
                )}
                value="basic"
              >
                Basic
              </BaseTabs.Tab>
            </Hint>
            <BaseTabs.Tab className={ROUTER_TAB_CLASS} value="advanced">
              Advanced
            </BaseTabs.Tab>
          </BaseTabs.List>
        </BaseTabs.Root>
        {showMobileNotice && (
          <div
            role="status"
            className="absolute left-0 top-full mt-2 z-10 text-warn-text bg-bg-sub border border-subtle-strong rounded-md pl-3 py-1 pr-1 text-caption shadow-overlay flex items-start gap-2 max-w-xs"
          >
            <span className="py-1">
              Basic requires a router chain containing only
              subscription-preference. Open Advanced to edit the full chain.
            </span>
            <IconButton
              label="Dismiss"
              className="shrink-0"
              onClick={() => setShowMobileNotice(false)}
            >
              <X className="w-3.5 h-3.5" aria-hidden="true" />
            </IconButton>
          </div>
        )}
      </div>
      <div>
        {activeTab === 'basic' && (
          <div role="tabpanel" className="space-y-6">
            <div className="flex items-center justify-between gap-4">
              <div>
                <div className="text-body font-medium text-text">
                  Keep prompt cache warm by reusing upstreams
                </div>
                <div className="mt-0.5 text-body text-text-muted">
                  Requests with similar prompts get routed to the upstream that
                  already served them, so the prompt cache hits stay high.
                </div>
              </div>
              <div className="flex items-center gap-2.5 shrink-0">
                {subscriptionPreferencePending ? (
                  <span
                    role="status"
                    aria-live="polite"
                    className="inline-flex items-center gap-1.5 text-caption text-text-muted"
                  >
                    <Spinner className="w-3 h-3 text-text-muted" />
                    {hasSubscriptionPreference
                      ? 'Turning off...'
                      : 'Turning on...'}
                  </span>
                ) : null}
                <ToggleSwitch
                  variant="compact"
                  role="switch"
                  aria-label="Keep prompt cache warm by reusing upstreams"
                  checked={hasSubscriptionPreference}
                  onChange={toggleSubscriptionPreference}
                  aria-busy={subscriptionPreferencePending || undefined}
                  disabled={isChainMetadataLoading || routerWriteBlocked}
                />
              </div>
            </div>

            <div>
              <div className="text-body font-medium text-text mb-2">
                When multiple upstreams qualify, pick
              </div>
              <TerminalStrategyRadioGroup
                name="strategy"
                value={strategy}
                disabled={routerWriteBlocked}
                isPending={updateTerminalStrategy.isPending}
                pendingValue={pendingStrategy}
                onSelect={setStrategy}
              />
            </div>
          </div>
        )}

        {activeTab === 'advanced' && (
          <div role="tabpanel" className="space-y-4">
            {entries.length === 0 && (
              <div>
                <p className="text-body text-text mb-1">No filters yet.</p>
                <p className="text-body text-text-muted">
                  Incoming requests will go straight to the terminal step. Add a
                  filter to narrow candidates by some property (cache prefix,
                  cost, region, ...).
                </p>
              </div>
            )}

            {isChainBusy && (
              <div
                role="status"
                className="flex items-center gap-2 text-caption text-text-faint"
              >
                <Spinner className="w-3 h-3" />
                {insert.isPending
                  ? 'Adding filter...'
                  : del.isPending
                    ? 'Removing filter...'
                    : 'Reordering steps...'}
              </div>
            )}

            <BasePopover.Root
              open={pickerOpen}
              onOpenChange={(open) => {
                if (!open && insert.isPending) return;
                setPickerOpen(open);
              }}
            >
              {/* Chain steps are flat rows on the ground, one 1px line between. */}
              <ul
                ref={listRef}
                className={cx(entries.length > 0 && 'border-t border-row')}
              >
                {entries.map((e, idx) => {
                  const reg = registry.data?.entries.find(
                    (r) => r.id === e.wasm_registry_id,
                  );
                  return (
                    // Narrow sections stack "Step N" over the name so the
                    // name and purpose get the width the step column held.
                    <li
                      key={e.id}
                      className="flex items-center gap-3 py-3 border-b border-row @max-md:grid @max-md:grid-cols-[minmax(0,1fr)_auto] @max-md:gap-x-2 @max-md:gap-y-0.5"
                      data-key={e.id}
                    >
                      <div className="text-caption tabular-nums text-text-faint w-12 shrink-0 @max-md:col-start-1 @max-md:row-start-1 @max-md:w-auto">
                        Step {idx + 1}
                      </div>
                      <div className="flex-1 min-w-0 @max-md:col-start-1 @max-md:row-start-2">
                        <div className="flex items-center gap-2">
                          <button
                            type="button"
                            className="truncate rounded-sm text-body font-medium text-text underline decoration-border-strong underline-offset-2 transition-colors hover:decoration-accent focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-2 max-md:-my-2.5 max-md:py-2.5"
                            onClick={() => setDetailPlugin(reg ?? null)}
                          >
                            {reg?.name ?? e.wasm_registry_id}
                          </button>
                        </div>
                        <div className="text-body text-text-muted truncate mt-0.5 @max-md:line-clamp-2 @max-md:whitespace-normal">
                          {reg?.metadata?.purpose ??
                            'User-uploaded filter (no description supplied).'}
                        </div>
                      </div>
                      <div className="flex items-center gap-1 shrink-0 @max-md:col-start-2 @max-md:row-span-2 @max-md:row-start-1">
                        <IconButton
                          label="Move filter up"
                          onClick={() => moveUp(idx)}
                          disabled={idx === 0 || routerWriteBlocked}
                        >
                          <ArrowUp />
                        </IconButton>
                        <IconButton
                          label="Move filter down"
                          onClick={() => moveDown(idx)}
                          disabled={
                            idx === entries.length - 1 || routerWriteBlocked
                          }
                        >
                          <ArrowDown />
                        </IconButton>
                        <IconButton
                          label="Remove filter"
                          onClick={() => removeFilter(e.id, e.revision)}
                          className="hover:text-danger-text"
                          disabled={routerWriteBlocked}
                        >
                          {deletingEntryId === e.id ? (
                            <Spinner className="text-danger-text" />
                          ) : (
                            <Trash2 />
                          )}
                        </IconButton>
                      </div>
                    </li>
                  );
                })}

                <BasePopover.Trigger
                  disabled={routerWriteBlocked}
                  nativeButton={false}
                  render={
                    <li
                      className={cx(
                        'mt-3 flex items-center justify-center p-3 border border-dashed border-subtle-strong rounded-sm transition-colors',
                        isChainBusy
                          ? 'control-disabled cursor-progress'
                          : principalWritePending
                            ? 'control-disabled'
                            : 'hover:bg-overlay-2 cursor-pointer',
                      )}
                      data-key="add-filter-placeholder"
                      onClick={(e) => e.stopPropagation()}
                    />
                  }
                >
                  {insert.isPending ? (
                    <div className="flex items-center gap-2">
                      <Spinner className="w-4 h-4" />
                      <span className="text-body font-medium">Adding...</span>
                    </div>
                  ) : (
                    <div
                      className={cx(
                        'flex items-center gap-2',
                        !routerWriteBlocked &&
                          'text-text-muted hover:text-text',
                      )}
                    >
                      <Plus className="w-4 h-4" />
                      <span className="text-body font-medium">Add filter</span>
                    </div>
                  )}
                </BasePopover.Trigger>
              </ul>

              <div className="flex flex-col gap-3 mt-8">
                <div className="flex items-center gap-3 @max-md:flex-col @max-md:items-start @max-md:gap-0.5">
                  <div className="text-caption text-text-faint w-12 shrink-0 flex items-center gap-1 @max-md:w-auto">
                    <ChevronDown className="w-3 h-3" aria-hidden="true" />
                    Final
                  </div>
                  <div className="flex-1 min-w-0">
                    <div className="text-body font-medium text-text truncate">
                      Terminal step
                    </div>
                    <div className="text-body text-text-muted truncate mt-0.5 @max-md:whitespace-normal">
                      Picks the upstream that will serve the request.
                    </div>
                  </div>
                </div>
                <div className="sm:pl-15">
                  <TerminalStrategyRadioGroup
                    name="term-strategy"
                    value={strategy}
                    disabled={routerWriteBlocked}
                    isPending={updateTerminalStrategy.isPending}
                    pendingValue={pendingStrategy}
                    onSelect={setStrategy}
                  />
                </div>
              </div>
              <BasePopover.Portal>
                <BasePopover.Positioner
                  align="start"
                  className="z-50"
                  sideOffset={4}
                >
                  <BasePopover.Popup
                    className="glass-strong w-64 rounded-md z-50 p-1"
                    onClick={(e) => e.stopPropagation()}
                  >
                    {registry.data?.entries
                      .filter((p) => pluginSupportsSlot(p, 'router'))
                      .map((p) => {
                        const inChain = entries.some(
                          (e) => e.wasm_registry_id === p.id,
                        );
                        const isInserting = insertingPluginId === p.id;
                        const disabled = inChain || routerWriteBlocked;

                        return (
                          <button
                            key={p.id}
                            className={cx(
                              'w-full text-left px-2 py-1.5 rounded-sm text-body flex flex-col gap-0.5',
                              'hover:bg-overlay-2 disabled:control-disabled',
                              isInserting && 'cursor-progress',
                            )}
                            disabled={disabled}
                            onClick={() => addFilter(p.id)}
                          >
                            <div className="flex items-center justify-between">
                              <span
                                className={cx(
                                  'font-medium',
                                  disabled ? 'text-text-faint' : 'text-text',
                                )}
                              >
                                {p.name}
                              </span>
                              {isInserting ? (
                                <Spinner className="w-3 h-3 text-text-faint" />
                              ) : inChain ? (
                                <span className="text-caption text-text-faint">
                                  Already in chain
                                </span>
                              ) : null}
                            </div>
                            <span className="text-caption text-text-faint truncate">
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
      </div>

      <PluginDetailDrawer
        plugin={detailPlugin}
        open={detailPlugin !== null}
        onOpenChange={(open) => {
          if (!open) setDetailPlugin(null);
        }}
      />
    </DetailSection>
  );
}

function ShapeSlotEditor({ principalId }: { principalId: string }) {
  const slot = 'shape';
  const chain = usePluginChain(principalId, slot);
  const registry = usePluginRegistry();
  const insert = useInsertChainEntry();
  const del = useDeleteChainEntry();
  const principalWritePending = usePrincipalWritePending(principalId) > 0;

  const entries = useMemo(
    () => [...(chain.data?.entries ?? [])].sort((a, b) => a.order - b.order),
    [chain.data],
  );

  const activeEntry = entries[0];
  const hasMultiple = entries.length > 1;

  const [mutatingId, setMutatingId] = useState<string | null>(null);
  const shapeWritePending =
    mutatingId !== null || insert.isPending || del.isPending;
  const shapeWriteBlocked = shapeWritePending || principalWritePending;

  const handleSelect = async (pluginId: string | null) => {
    if (shapeWriteBlocked) return;
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
    <DetailSection
      span="full"
      title={
        <span className="flex items-center gap-2">
          Shape
          {hasMultiple && (
            <Hint label="Database invariant violated: multiple shape entries detected. Selecting a new option will clear them.">
              <Badge tone="warn">Multiple entries detected</Badge>
            </Hint>
          )}
        </span>
      }
      description="Request / response transform. Inherits the dialect returned by the router when unset."
    >
      <div>
        <BaseRadioGroup
          className="space-y-2"
          aria-busy={shapeWritePending || undefined}
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
            disabled={shapeWriteBlocked}
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
                disabled={shapeWriteBlocked}
              />
            ))}
        </BaseRadioGroup>
      </div>
    </DetailSection>
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

  // Reorder, insert, and delete all rewrite this one chain, and each write bumps
  // sibling revisions: a second write started mid-flight either races the first
  // or 409s on revisions the server already moved. One lock therefore covers
  // drag, add, and remove; the mutation that owns the interaction still renders
  // its own progress so the operator sees which write holds the chain.
  const chainBusy = reorder.isPending || insert.isPending || del.isPending;

  const onDragEnd = (e: DragEndEvent) => {
    if (chainBusy) return;
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
    <DetailSection
      title="Observability"
      description="SSE / audit hooks. Executed in order. Multiple allowed."
      action={
        <>
          {reorder.isPending ? (
            <span
              role="status"
              aria-live="polite"
              className="inline-flex items-center gap-1.5 text-caption text-text-muted"
            >
              <Spinner className="w-3 h-3 text-text-muted" />
              Saving order...
            </span>
          ) : null}
          <Button
            size="sm"
            iconLeft={<Plus className="w-3 h-3" />}
            disabled={chainBusy}
            onClick={() => setAddOpen(true)}
          >
            Add
          </Button>
        </>
      }
    >
      <div>
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
              <ul
                className="border-t border-row"
                aria-busy={chainBusy || undefined}
              >
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
                      disabled={chainBusy}
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
          <p className="text-body text-text-muted">
            No {label.toLowerCase()} plugins. Add one to run it on every
            request.
          </p>
        )}

        <Modal
          open={addOpen}
          onOpenChange={setAddOpen}
          preventDismiss={insert.isPending}
          title={`Add plugin to ${label}`}
          footer={
            <>
              <Button
                disabled={insert.isPending}
                onClick={() => setAddOpen(false)}
              >
                Cancel
              </Button>
              <Button
                variant="primary"
                disabled={!selectedPluginId || chainBusy}
                loading={insert.isPending}
                onClick={() => {
                  if (chainBusy) return;
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
                  );
                }}
              >
                {insert.isPending ? 'Adding...' : 'Add'}
              </Button>
            </>
          }
        >
          <Field label="Plugin" required>
            <Select
              className="w-full"
              value={selectedPluginId}
              disabled={insert.isPending}
              placeholder="Select a plugin"
              onChange={setSelectedPluginId}
              options={(registry.data?.entries ?? [])
                .filter((p) => pluginSupportsSlot(p, 'observability_hook'))
                .map((p) => ({ value: p.id, label: p.name }))}
            />
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
                <span className="font-medium text-text">
                  {pendingRemove.name}
                </span>{' '}
                will be removed from the {label} chain. You can re-add it later.
              </>
            ) : null
          }
          confirmLabel={del.isPending ? 'Removing...' : 'Remove'}
          destructive
          pending={del.isPending}
          closeOnConfirm={false}
          onConfirm={() => {
            if (!pendingRemove || chainBusy) return;
            del.mutate(
              { id: pendingRemove.id, revision: pendingRemove.revision },
              {
                onSuccess: () => {
                  toast.success('Plugin removed from chain');
                  setPendingRemove(null);
                },
              },
            );
          }}
        />
      </div>
    </DetailSection>
  );
}

function SortableChainItem({
  id,
  order,
  name,
  onDelete,
  disabled = false,
}: {
  id: string;
  order: number;
  name: string;
  onDelete: () => void;
  disabled?: boolean;
}) {
  const {
    attributes,
    listeners,
    setNodeRef,
    transform,
    transition,
    isDragging,
  } = useSortable({ id, disabled });
  const style = {
    transform: CSS.Transform.toString(transform),
    transition,
    opacity: isDragging ? 0.5 : 1,
  } as React.CSSProperties;
  return (
    <li
      ref={setNodeRef}
      style={style}
      className="flex items-center gap-2 py-2 bg-bg border-b border-row"
    >
      <button
        type="button"
        {...attributes}
        {...listeners}
        aria-label="Drag to reorder"
        disabled={disabled}
        className="text-text-faint hover:text-text cursor-grab active:cursor-grabbing rounded-sm focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-1 disabled:control-disabled"
      >
        <GripVertical className="w-3.5 h-3.5" />
      </button>
      <span className="w-8 text-caption tabular-nums text-text-faint">
        #{Math.floor(order)}
      </span>
      <span className="flex-1 text-body font-medium text-text truncate">
        {name}
      </span>
      <IconButton
        label={`Remove ${name}`}
        disabled={disabled}
        className="hover:text-danger-text"
        onClick={onDelete}
      >
        <Trash2 className="w-3.5 h-3.5" aria-hidden="true" />
      </IconButton>
    </li>
  );
}

// Below a 42rem section the key table reflows into one card per key, the same
// DOM restyled: checkbox, label and revoke on the first line, the key ID under
// the label, then last 4 · issued · last used (and "Revoked") as a caption.
// `!` beats the Table's `[&_td:first-child]` / `[&_td:last-child]` insets.
const API_KEY_PHONE_HIDE = '@max-2xl:hidden';
const API_KEY_PHONE_ROW =
  '@max-2xl:flex @max-2xl:h-auto @max-2xl:flex-wrap @max-2xl:items-center @max-2xl:gap-x-3 @max-2xl:gap-y-0.5 @max-2xl:py-2 @max-2xl:pl-4 @max-2xl:pr-1';
const API_KEY_PHONE_CELL = {
  select: '@max-2xl:order-1 @max-2xl:w-4 @max-2xl:p-0!',
  label:
    '@max-2xl:order-2 @max-2xl:min-w-0 @max-2xl:flex-1 @max-2xl:truncate @max-2xl:p-0! @max-2xl:font-medium',
  action: '@max-2xl:order-3 @max-2xl:-my-2 @max-2xl:p-0!',
  keyId:
    '@max-2xl:order-4 @max-2xl:basis-full @max-2xl:truncate @max-2xl:p-0! @max-2xl:pl-7!',
  last4: '@max-2xl:order-5 @max-2xl:p-0! @max-2xl:pl-7!',
  fact: '@max-2xl:order-5 @max-2xl:p-0! @max-2xl:text-caption @max-2xl:text-text-muted',
} as const;

const API_KEY_SKELETON_CELL_CLASSES = [
  'px-3 w-10',
  'px-3',
  `px-3 ${API_KEY_PHONE_HIDE}`,
  `px-3 ${API_KEY_PHONE_HIDE}`,
  `px-3 ${API_KEY_PHONE_HIDE}`,
  `px-3 ${API_KEY_PHONE_HIDE}`,
  `px-3 ${API_KEY_PHONE_HIDE}`,
  'px-2 w-10',
] as const;

const API_KEY_SKELETON_CLASSES = [
  'w-4',
  'w-24',
  'w-48',
  'w-12',
  'w-20',
  'w-20',
  'w-16',
  'w-4',
] as const;

const API_KEY_CHECKBOX_CLASS =
  'size-4 cursor-pointer align-middle accent-[var(--color-accent)] disabled:cursor-not-allowed focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-1';
const API_KEY_CHECKBOX_HIT_CLASS =
  'inline-flex cursor-pointer @max-2xl:-m-3 @max-2xl:p-3';

/** How a key is named in bulk confirm copy: its last 4, else its ID. */
function apiKeyTail(key: { key_id: string; last_4: string }): string {
  return key.last_4 ? `···${key.last_4}` : key.key_id;
}

export function ApiKeysCard({ principal }: { principal: Principal }) {
  const keys = usePrincipalKeys(principal.id);
  const issue = useIssueKey();
  const revoke = useRevokeKey();
  // Same revoke endpoint, but failures skip the global per-request toast:
  // bulk revoke reports every failed key in one summary toast instead.
  const bulkRevoke = useRevokeKey({ inlineError: true });
  const queryClient = useQueryClient();
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
  const [selectedKeyIds, setSelectedKeyIds] = useState<ReadonlySet<string>>(
    () => new Set(),
  );
  const [bulkConfirmOpen, setBulkConfirmOpen] = useState(false);
  // Snapshot of the keys the open bulk dialog names, so its copy stays intact
  // through the close transition after the selection is cleared.
  const [bulkTargets, setBulkTargets] = useState<
    { key_id: string; last_4: string }[]
  >([]);
  const [bulkPending, setBulkPending] = useState(false);
  // Issuance mints a plaintext key the server can never show again, so a
  // duplicate request from one synchronous burst of clicks would strand a live
  // secret nobody can read; revoke is the matching destructive one-shot. React
  // Query only publishes `isPending` on the next render, which lands too late
  // for that burst, so these refs latch each request until it settles. That is
  // specific to these one-time-secret writes, not a rule for every mutation.
  const issueInFlight = React.useRef(false);
  const revokeInFlight = React.useRef(false);
  const issuePending = issue.isPending;
  const revokePending = revoke.isPending || bulkPending;

  const activeKeys = (keys.data?.keys ?? []).filter(
    (k) => !k.revoked_at_unix_secs,
  );
  // Selection only ever covers keys that are still active; a key revoked
  // elsewhere drops out of the count without a stale entry lingering.
  const selectedKeys = activeKeys.filter((k) => selectedKeyIds.has(k.key_id));
  const allActiveSelected =
    activeKeys.length > 0 && selectedKeys.length === activeKeys.length;
  const someActiveSelected = selectedKeys.length > 0 && !allActiveSelected;

  const toggleKeySelected = (keyId: string) => {
    setSelectedKeyIds((prev) => {
      const next = new Set(prev);
      if (next.has(keyId)) next.delete(keyId);
      else next.add(keyId);
      return next;
    });
  };

  const toggleAllActive = () => {
    setSelectedKeyIds(
      allActiveSelected ? new Set() : new Set(activeKeys.map((k) => k.key_id)),
    );
  };

  const submitIssue = () => {
    if (issueInFlight.current || issuePending) return;
    issueInFlight.current = true;
    issue.mutate(
      { id: principal.id, label },
      {
        onSuccess: (res) => setIssued(res),
        onSettled: () => {
          issueInFlight.current = false;
        },
      },
    );
  };

  const submitRevoke = () => {
    if (!pendingRevoke || revokeInFlight.current || revokePending) return;
    revokeInFlight.current = true;
    revoke.mutate(
      { id: principal.id, key_id: pendingRevoke.key_id },
      {
        // The dialog stays mounted until the server acknowledges. Only success
        // clears the target, so a failure keeps the key in context for a retry.
        onSuccess: () => {
          toast.success('Key revoked');
          setPendingRevoke(null);
        },
        onSettled: () => {
          revokeInFlight.current = false;
        },
      },
    );
  };

  const submitBulkRevoke = async () => {
    if (revokeInFlight.current || revokePending) return;
    const targets = bulkTargets;
    if (targets.length === 0) return;
    revokeInFlight.current = true;
    setBulkPending(true);
    try {
      const results = await Promise.allSettled(
        targets.map((k) =>
          bulkRevoke.mutateAsync({ id: principal.id, key_id: k.key_id }),
        ),
      );
      const failed = targets.filter(
        (_, idx) => results[idx]?.status === 'rejected',
      );
      const revokedCount = targets.length - failed.length;
      if (failed.length === 0) {
        toast.success(
          revokedCount === 1 ? 'Key revoked' : `${revokedCount} keys revoked`,
        );
      } else {
        toast.error(
          `Revoked ${revokedCount} of ${targets.length} keys. Failed: ${failed.map(apiKeyTail).join(', ')}`,
        );
      }
    } finally {
      revokeInFlight.current = false;
      setBulkPending(false);
      setSelectedKeyIds(new Set());
      setBulkConfirmOpen(false);
      void queryClient.invalidateQueries({
        queryKey: qk.principalKeys(principal.id),
      });
    }
  };

  const bulkCount = selectedKeys.length;

  return (
    <DetailSection
      span="full"
      className={PRINCIPAL_DETAIL_CARD_CLASS_NAMES.apiKeys}
      title="API keys"
      description="Authenticates as this DB principal; routing selects a DB upstream"
      action={
        <div className="flex flex-wrap items-center gap-2">
          {bulkCount > 0 ? (
            <Button
              size="sm"
              variant="danger"
              disabled={revokePending}
              onClick={() => {
                setBulkTargets(selectedKeys);
                setBulkConfirmOpen(true);
              }}
            >
              Revoke selected ({bulkCount})
            </Button>
          ) : null}
          <Button
            size="sm"
            iconLeft={<KeyRound className="w-3 h-3" />}
            onClick={() => setIssueOpen(true)}
          >
            Issue key
          </Button>
        </div>
      }
    >
      <div
        className="glass rounded-md overflow-x-auto min-h-32"
        data-testid="api-keys-table-slot"
      >
        <Table className="@max-2xl:block @2xl:whitespace-nowrap">
          <TableHead className="@max-2xl:block">
            <tr className="@max-2xl:flex">
              <TableHeadCell className="w-10 @max-2xl:flex @max-2xl:h-10 @max-2xl:w-full @max-2xl:items-center @max-2xl:gap-3 @max-2xl:after:content-['Select_all']">
                <label className={API_KEY_CHECKBOX_HIT_CLASS}>
                  <input
                    type="checkbox"
                    aria-label="Select all active keys"
                    className={API_KEY_CHECKBOX_CLASS}
                    checked={allActiveSelected}
                    ref={(el) => {
                      if (el) el.indeterminate = someActiveSelected;
                    }}
                    disabled={activeKeys.length === 0 || revokePending}
                    onChange={toggleAllActive}
                  />
                </label>
              </TableHeadCell>
              <TableHeadCell className={API_KEY_PHONE_HIDE}>
                Label
              </TableHeadCell>
              <TableHeadCell className={API_KEY_PHONE_HIDE}>
                Key ID
              </TableHeadCell>
              <TableHeadCell className={API_KEY_PHONE_HIDE}>
                Last 4
              </TableHeadCell>
              <TableHeadCell className={API_KEY_PHONE_HIDE}>
                Issued
              </TableHeadCell>
              <TableHeadCell className={API_KEY_PHONE_HIDE}>
                Last used
              </TableHeadCell>
              <TableHeadCell className={API_KEY_PHONE_HIDE}>
                Status
              </TableHeadCell>
              <TableHeadCell className={cx('w-10', API_KEY_PHONE_HIDE)}>
                <span className="sr-only">Actions</span>
              </TableHeadCell>
            </tr>
          </TableHead>
          <tbody className="@max-2xl:block">
            {keys.isLoading ? (
              Array.from({ length: 3 }).map((_, i) => (
                <SkeletonRow
                  key={i}
                  cols={8}
                  cellClassNames={API_KEY_SKELETON_CELL_CLASSES}
                  skeletonClassNames={API_KEY_SKELETON_CLASSES}
                />
              ))
            ) : keys.data?.keys.length ? (
              keys.data.keys.map((k) => {
                const revoked = Boolean(k.revoked_at_unix_secs);
                return (
                  <TableRow
                    key={k.key_id}
                    className={cx(
                      API_KEY_PHONE_ROW,
                      revoked && 'text-text-faint',
                    )}
                  >
                    <TableCell
                      className={cx('w-10', API_KEY_PHONE_CELL.select)}
                    >
                      {!revoked ? (
                        // Phone cards: the label pads the 16px box out to a
                        // 40px target without moving it.
                        <label className={API_KEY_CHECKBOX_HIT_CLASS}>
                          <input
                            type="checkbox"
                            aria-label={`Select key ${k.label ? `${k.label} (${k.key_id})` : k.key_id}`}
                            className={API_KEY_CHECKBOX_CLASS}
                            checked={selectedKeyIds.has(k.key_id)}
                            disabled={revokePending}
                            onChange={() => toggleKeySelected(k.key_id)}
                          />
                        </label>
                      ) : null}
                    </TableCell>
                    <TableCell className={API_KEY_PHONE_CELL.label}>
                      {k.label ?? <EmptyValue label="No label" />}
                    </TableCell>
                    <TableCell mono className={API_KEY_PHONE_CELL.keyId}>
                      {k.key_id}
                    </TableCell>
                    <TableCell
                      mono
                      className={cx(
                        'text-text-muted',
                        API_KEY_PHONE_CELL.last4,
                      )}
                    >
                      {k.last_4 ? (
                        `···${k.last_4}`
                      ) : (
                        <EmptyValue label="Unknown" />
                      )}
                    </TableCell>
                    <TableCell
                      className={cx(
                        API_KEY_PHONE_CELL.fact,
                        "@max-2xl:before:content-['Issued_']",
                      )}
                    >
                      <RelativeTime
                        compact
                        ts={new Date(k.issued_at_unix_secs * 1000)}
                      />
                    </TableCell>
                    <TableCell
                      className={cx(
                        API_KEY_PHONE_CELL.fact,
                        "@max-2xl:before:content-['Used_']",
                      )}
                    >
                      <RelativeTime
                        compact
                        ts={
                          k.last_used_at_unix_secs
                            ? new Date(k.last_used_at_unix_secs * 1000)
                            : null
                        }
                      />
                    </TableCell>
                    <TableCell
                      className={
                        revoked ? API_KEY_PHONE_CELL.fact : API_KEY_PHONE_HIDE
                      }
                    >
                      {revoked ? (
                        <StatusBadge tone="neutral" label="Revoked" />
                      ) : (
                        <EmptyValue label="Active" />
                      )}
                    </TableCell>
                    <TableCell
                      className={cx(
                        'py-1 text-right',
                        API_KEY_PHONE_CELL.action,
                      )}
                    >
                      {!revoked ? (
                        <IconButton
                          className="hover:text-danger-text"
                          label={`Revoke key ${k.label ? `${k.label} (${k.key_id})` : k.key_id}`}
                          disabled={revokePending}
                          onClick={() =>
                            setPendingRevoke({
                              key_id: k.key_id,
                              label: k.label ?? k.key_id,
                            })
                          }
                        >
                          <Trash2 className="w-3.5 h-3.5" aria-hidden="true" />
                        </IconButton>
                      ) : null}
                    </TableCell>
                  </TableRow>
                );
              })
            ) : (
              <tr className="@max-2xl:block">
                <td
                  colSpan={8}
                  className="px-4 py-8 text-center @max-2xl:block"
                >
                  <p className="text-body text-text-muted">
                    No API keys issued.
                  </p>
                  <p className="mt-1 text-caption text-text-faint whitespace-normal">
                    Issue a key so this principal can send requests through the
                    proxy. The plaintext is shown once.
                  </p>
                </td>
              </tr>
            )}
          </tbody>
        </Table>
      </div>

      {/*
        The issued plaintext cannot be recovered once this dialog closes, so it
        refuses every implicit dismissal (Escape, backdrop, X) until the
        operator acknowledges the key with Done.
      */}
      <Modal
        open={issueOpen}
        onOpenChange={(o) => {
          setIssueOpen(o);
          if (!o) {
            setLabel('');
            setIssued(null);
          }
        }}
        preventDismiss={issuePending || issued !== null}
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
              <Button
                disabled={issuePending}
                onClick={() => setIssueOpen(false)}
              >
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={issuePending}
                onClick={submitIssue}
              >
                {issuePending ? 'Issuing...' : 'Issue'}
              </Button>
            </>
          )
        }
      >
        {issued ? (
          <div className="space-y-3">
            <p className="text-body text-text-muted">
              Copy the key now. It will not be shown again, so this dialog stays
              open until you choose Done.
            </p>
            <div className="flex items-center gap-2">
              <code className="well flex-1 p-2 font-mono text-data text-text break-all">
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
              className={cx(INPUT_CLASS, PENDING_INPUT_CLASS)}
              value={label}
              onChange={(e) => setLabel(e.target.value)}
              placeholder="laptop, ci, prod-app"
              disabled={issuePending}
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
              Key{' '}
              <span className="font-medium text-text">
                {pendingRevoke.label}
              </span>{' '}
              will stop authenticating new requests immediately.
            </>
          ) : null
        }
        confirmLabel="Revoke"
        destructive
        pending={revokePending}
        closeOnConfirm={false}
        onConfirm={submitRevoke}
      />

      <ConfirmDialog
        open={bulkConfirmOpen}
        onOpenChange={setBulkConfirmOpen}
        title={
          bulkTargets.length === 1
            ? 'Revoke 1 key?'
            : `Revoke ${bulkTargets.length} keys?`
        }
        description={
          <>
            Clients using{' '}
            <span className="font-medium text-text">
              {bulkTargets.map(apiKeyTail).join(', ')}
            </span>{' '}
            will get 401 immediately.
          </>
        }
        confirmLabel="Revoke"
        destructive
        pending={bulkPending}
        closeOnConfirm={false}
        onConfirm={() => {
          void submitBulkRevoke();
        }}
      />
    </DetailSection>
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
  const createInFlight = React.useRef(false);
  const [name, setName] = useState('');
  const [kind, setKind] = useState<'machine' | 'human' | 'admin'>('human');
  const [defaultLimits, setDefaultLimits] = useState<PrincipalDefaultLimit[]>(
    [],
  );
  const creating = create.isPending;
  const reset = () => {
    setName('');
    setKind('human');
    setDefaultLimits([]);
  };
  // A close request is only honoured when nothing is in flight; the draft is
  // discarded with the dialog so a cancelled form never resurfaces half-filled.
  const requestClose = () => {
    if (createInFlight.current || creating) return;
    onOpenChange(false);
    reset();
  };
  const submitCreate = () => {
    if (createInFlight.current || creating) return;
    createInFlight.current = true;
    create.mutate(
      { name, kind, default_limits: defaultLimits },
      {
        onSuccess: () => {
          toast.success('Principal created');
          onOpenChange(false);
          reset();
        },
        onSettled: () => {
          createInFlight.current = false;
        },
      },
    );
  };
  return (
    <Modal
      open={open}
      preventDismiss={creating}
      onOpenChange={(o) => {
        if (!o) {
          requestClose();
          return;
        }
        onOpenChange(true);
      }}
      title="New principal"
      footer={
        <>
          <Button disabled={creating} onClick={requestClose}>
            Cancel
          </Button>
          <Button
            variant="primary"
            disabled={!name.trim()}
            loading={creating}
            onClick={submitCreate}
          >
            {creating ? 'Creating...' : 'Create'}
          </Button>
        </>
      }
    >
      <div className="space-y-5">
        <Field label="Name" required>
          <input
            className={cx(INPUT_CLASS, PENDING_INPUT_CLASS)}
            value={name}
            onChange={(e) => setName(e.target.value)}
            placeholder="engineering-shared"
            disabled={creating}
          />
        </Field>
        <Field label="Kind" required>
          <Select
            className="w-full"
            disabled={creating}
            value={kind}
            onChange={(next) => setKind(next as typeof kind)}
            options={[
              { value: 'human', label: 'Human' },
              { value: 'machine', label: 'Machine' },
              { value: 'admin', label: 'Admin' },
            ]}
          />
        </Field>
        <div className="flex flex-col gap-1.5">
          <span className="text-label text-text-muted">Default limits</span>
          <span className="-mt-1 text-caption text-text-faint">
            Optional. Applied to every API key issued for this principal.
          </span>
          <LimitsEditor
            value={defaultLimits}
            onChange={setDefaultLimits}
            disabled={creating}
          />
        </div>
      </div>
    </Modal>
  );
}
