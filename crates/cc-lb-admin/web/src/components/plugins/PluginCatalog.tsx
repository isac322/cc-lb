import { Copy, Trash2 } from 'lucide-react';
import { useRef, useState } from 'react';
import { toast } from 'sonner';
import { useGcPlugins, usePluginRegistry } from '../../lib/queries';
import { useCopyButton } from '../../lib/useCopyButton';
import {
  Badge,
  Button,
  Card,
  ConfirmDialog,
  Hint,
  IconButton,
  Section,
  Skeleton,
  SkeletonRow,
  Spinner,
} from '../ui/primitives';
import { RelativeTime } from '../ui/RelativeTime';
import {
  EmptyValue,
  Table,
  TableCell,
  TableEmptyRow,
  TableHead,
  TableHeadCell,
} from '../ui/Table';
import { PluginDeleteDialog } from './PluginDeleteDialog';
import { SLOTS } from './slots/model';

const PLUGIN_COLUMN_WIDTHS = [
  'w-1/4',
  'w-1/6',
  'w-1/6',
  'w-1/12',
  'w-1/12',
  'w-1/8',
  'w-1/8',
] as const;

const PLUGIN_LOADING_ROWS = [0, 1, 2] as const;
/** Two text lines (name, then version · description) at a 56px row. */
const PLUGIN_ROW_GEOMETRY_CLASS = 'h-14';

const PLUGIN_SKELETON_CLASSES = [
  'h-9 w-4/5',
  'h-5 w-16',
  'h-4 w-28',
  'ml-auto h-4 w-16',
  'ml-auto h-4 w-6',
  'h-4 w-16',
  'ml-auto h-4 w-24',
] as const;

export function PluginCatalog({
  onSelectPlugin,
}: {
  onSelectPlugin: (id: string) => void;
}) {
  const reg = usePluginRegistry();
  const gc = useGcPlugins();
  const { copy } = useCopyButton();
  const entries = reg.data?.entries ?? [];
  const unusedRegisteredCount = entries.filter(
    (p) => !p.is_builtin && p.refcount === 0,
  ).length;
  const [pendingDelete, setPendingDelete] = useState<{
    id: string;
    revision: number;
    name: string;
    refcount: number;
  } | null>(null);
  const gcPending = gc.isPending;
  // React Query publishes `isPending` on the next render, which lands after a
  // synchronous burst of clicks. This ref latches the sweep until it settles so
  // one click can never queue a second destructive server call.
  const gcInFlight = useRef(false);
  const [gcConfirmOpen, setGcConfirmOpen] = useState(false);

  const runGc = () => {
    if (gcInFlight.current || gcPending) return;
    gcInFlight.current = true;
    gc.mutate(undefined, {
      onSuccess: (r) =>
        toast.success(
          r.count === 0
            ? 'No orphaned upload blobs found'
            : `Removed ${r.count} orphaned upload blob${r.count === 1 ? '' : 's'}`,
        ),
      // No local onError: the global MutationCache toast stays the single
      // failure surface, so the latch only releases the click here.
      onSettled: () => {
        gcInFlight.current = false;
      },
    });
  };

  return (
    <>
      <Section
        title="Plugin library"
        subtitle={
          <span
            className="flex min-h-5 min-w-56 items-center"
            data-testid="plugin-count-slot"
          >
            {reg.isLoading ? (
              <Skeleton as="span" className="block h-4 w-48" />
            ) : (
              <span className="tabular-nums">
                {entries.length} available · {unusedRegisteredCount} not used
                anywhere
              </span>
            )}
          </span>
        }
        action={
          <div className="flex flex-wrap items-center gap-3">
            {gcPending ? (
              <span
                role="status"
                aria-live="polite"
                className="flex items-center gap-2 text-body-sm text-text-muted"
                data-testid="plugin-gc-progress"
              >
                <Spinner className="w-3 h-3" />
                Cleaning orphaned uploads — waiting for the server.
              </span>
            ) : null}
            <Button
              size="sm"
              variant="secondary"
              disabled={gcPending}
              loading={gcPending}
              title="Remove uploaded WASM blobs no longer referenced by any registered plugin. Registered plugins remain available, even when Used by is 0."
              onClick={() => setGcConfirmOpen(true)}
            >
              {gcPending ? 'Cleaning...' : 'Clean orphaned uploads'}
            </Button>
          </div>
        }
      >
        <Card className="overflow-x-auto">
          <Table className="min-w-[960px] table-fixed">
            <colgroup>
              {PLUGIN_COLUMN_WIDTHS.map((className, index) => (
                <col key={index} className={className} />
              ))}
            </colgroup>
            <TableHead>
              <tr>
                <TableHeadCell>Name</TableHeadCell>
                <TableHeadCell>Works in</TableHeadCell>
                <TableHeadCell>File hash</TableHeadCell>
                <TableHeadCell numeric>Size</TableHeadCell>
                <TableHeadCell numeric>Used by</TableHeadCell>
                <TableHeadCell>Added</TableHeadCell>
                <TableHeadCell className="text-right">
                  <span className="sr-only">Actions</span>
                </TableHeadCell>
              </tr>
            </TableHead>
            <tbody>
              {reg.isLoading ? (
                PLUGIN_LOADING_ROWS.map((row) => (
                  <SkeletonRow
                    key={row}
                    className={PLUGIN_ROW_GEOMETRY_CLASS}
                    cols={7}
                    skeletonClassNames={PLUGIN_SKELETON_CLASSES}
                  />
                ))
              ) : entries.length ? (
                entries.map((p) => {
                  // One secondary line under the name: "v0.5.1 · label · description".
                  const subline = [
                    p.version ? `v${p.version}` : null,
                    p.label,
                    p.description,
                  ]
                    .filter(Boolean)
                    .join(' · ');
                  return (
                    // Pointer shortcut; keyboard users reach the same view
                    // through the row's Inspect button.
                    <tr
                      key={p.id}
                      className={`${PLUGIN_ROW_GEOMETRY_CLASS} border-b border-row last:border-b-0 transition-colors cursor-pointer hover:bg-overlay-2`}
                      onClick={() => onSelectPlugin(p.id)}
                    >
                      <TableCell>
                        <div className="flex min-w-0 items-center gap-2">
                          <span
                            className="truncate font-medium text-text"
                            title={p.name}
                          >
                            {p.name}
                          </span>
                          {p.is_builtin && (
                            <Badge className="shrink-0" tone="neutral">
                              Built-in
                            </Badge>
                          )}
                        </div>
                        {subline ? (
                          <div
                            className="mt-0.5 truncate text-caption text-text-faint"
                            title={subline}
                          >
                            {subline}
                          </div>
                        ) : null}
                      </TableCell>
                      <TableCell>
                        <div className="flex flex-wrap gap-1">
                          {p.supported_slots && p.supported_slots.length > 0 ? (
                            p.supported_slots.map((slot) => (
                              <Badge key={slot} tone="neutral">
                                {SLOTS.find((s) => s.id === slot)?.label ??
                                  slot}
                              </Badge>
                            ))
                          ) : (
                            <Badge tone="warn">Unknown</Badge>
                          )}
                        </div>
                      </TableCell>
                      <TableCell>
                        <div className="flex items-center gap-1">
                          <Hint label={p.sha256_hex}>
                            <code className="cursor-help font-mono text-data text-text-muted">
                              {p.sha256_hex.slice(0, 12)}…
                            </code>
                          </Hint>
                          <IconButton
                            label="Copy SHA256"
                            onClick={(e) => {
                              e.stopPropagation();
                              copy(p.sha256_hex, 'SHA256');
                            }}
                          >
                            <Copy
                              className="w-3.5 h-3.5"
                              strokeWidth={1.75}
                              aria-hidden="true"
                            />
                          </IconButton>
                        </div>
                      </TableCell>
                      <TableCell numeric>
                        {p.is_builtin && p.size_bytes === 0 ? (
                          <EmptyValue label="Built-in" />
                        ) : (
                          <>
                            {(p.size_bytes / 1024).toFixed(1)}{' '}
                            <span className="text-text-muted">KB</span>
                          </>
                        )}
                      </TableCell>
                      {/* Plain count: the row and its Inspect action already
                          open the detail view that lists every use. */}
                      <TableCell
                        numeric
                        className={
                          p.refcount > 0 ? 'text-text' : 'text-text-faint'
                        }
                      >
                        {p.refcount}
                      </TableCell>
                      <TableCell className="text-text-muted">
                        {p.is_builtin && p.uploaded_at_unix_secs === 0 ? (
                          <EmptyValue label="Built-in" />
                        ) : (
                          <RelativeTime
                            compact
                            ts={new Date(p.uploaded_at_unix_secs * 1000)}
                          />
                        )}
                      </TableCell>
                      <TableCell className="text-right">
                        <div className="flex items-center justify-end gap-1">
                          <button
                            type="button"
                            aria-label={`Inspect ${p.name}`}
                            className="inline-flex items-center h-11 md:h-7 px-2.5 rounded-sm text-label text-text underline decoration-subtle-strong underline-offset-4 transition-colors hover:bg-hover-bg hover:decoration-accent focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-1"
                            onClick={(e) => {
                              e.stopPropagation();
                              onSelectPlugin(p.id);
                            }}
                          >
                            Inspect
                          </button>
                          {p.is_builtin ? (
                            // Keeps "Inspect" aligned with deletable rows.
                            <span aria-hidden="true" className="w-11 md:w-8" />
                          ) : (
                            <IconButton
                              label="Delete plugin"
                              title={
                                p.refcount > 0
                                  ? `In use by ${p.refcount} reference(s)`
                                  : 'Delete plugin'
                              }
                              className="hover:text-danger-text"
                              onClick={(e) => {
                                e.stopPropagation();
                                setPendingDelete({
                                  id: p.id,
                                  revision: p.revision,
                                  name: p.name,
                                  refcount: p.refcount,
                                });
                              }}
                            >
                              <Trash2
                                className="w-3.5 h-3.5"
                                strokeWidth={1.75}
                                aria-hidden="true"
                              />
                            </IconButton>
                          )}
                        </div>
                      </TableCell>
                    </tr>
                  );
                })
              ) : (
                <TableEmptyRow colSpan={7}>No plugins uploaded.</TableEmptyRow>
              )}
            </tbody>
          </Table>
        </Card>
      </Section>

      <PluginDeleteDialog
        pendingDelete={pendingDelete}
        onClose={() => setPendingDelete(null)}
      />

      <ConfirmDialog
        open={gcConfirmOpen}
        onOpenChange={setGcConfirmOpen}
        destructive
        title="Clean orphaned uploads?"
        description="This permanently deletes uploaded WASM files that no registered plugin points to — typically older versions left behind after a plugin was replaced. Registered plugins, including ones not used anywhere, are kept. A deleted file can only come back by uploading it again."
        confirmLabel="Delete orphaned uploads"
        onConfirm={runGc}
      />
    </>
  );
}
