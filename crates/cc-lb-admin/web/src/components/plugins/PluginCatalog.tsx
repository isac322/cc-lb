import { Copy, Trash2 } from 'lucide-react';
import { useRef, useState } from 'react';
import { toast } from 'sonner';
import { useGcPlugins, usePluginRegistry } from '../../lib/queries';
import { useCopyButton } from '../../lib/useCopyButton';
import {
  Badge,
  Button,
  Card,
  CardHeader,
  Hint,
  Skeleton,
  SkeletonRow,
  Spinner,
} from '../ui/primitives';
import { RelativeTime } from '../ui/RelativeTime';
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
const PLUGIN_ROW_GEOMETRY_CLASS = 'h-20';

const PLUGIN_SKELETON_CELL_CLASSES = [
  '!px-4',
  '!px-4',
  '!px-4',
  '!px-4',
  '!px-4',
  '!px-4',
  '!px-4',
] as const;

const PLUGIN_SKELETON_CLASSES = [
  'h-16 w-4/5',
  'h-5 w-20',
  'h-4 w-28',
  'ml-auto h-4 w-16',
  'mx-auto h-5 w-10',
  'h-4 w-20',
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
      <Card>
        <CardHeader
          title={
            <span className="flex items-baseline gap-2">
              <span className="text-lg font-medium">Plugin library</span>
            </span>
          }
          subtitle={
            <span
              className="flex min-h-5 min-w-56 items-center"
              data-testid="plugin-count-slot"
            >
              {reg.isLoading ? (
                <Skeleton as="span" className="block h-4 w-48" />
              ) : (
                <span className="text-sm text-text-faint">
                  {entries.length} available · {unusedRegisteredCount} not used
                  anywhere
                </span>
              )}
            </span>
          }
          action={
            <>
              {gcPending ? (
                <span
                  role="status"
                  aria-live="polite"
                  className="flex items-center gap-2 text-xs text-text-faint"
                  data-testid="plugin-gc-progress"
                >
                  <Spinner className="w-3 h-3" />
                  Cleaning orphaned uploads — waiting for the server.
                </span>
              ) : null}
              <Button
                size="sm"
                variant="ghost"
                disabled={gcPending}
                loading={gcPending}
                title="Remove uploaded WASM blobs no longer referenced by any registered plugin. Registered plugins remain available, even when Used By is 0."
                onClick={runGc}
              >
                {gcPending ? 'Cleaning...' : 'Clean orphaned uploads'}
              </Button>
            </>
          }
        />
        <div className="min-h-72 overflow-x-auto">
          <table className="w-full min-w-[960px] table-fixed font-mono text-xs">
            <colgroup>
              {PLUGIN_COLUMN_WIDTHS.map((className, index) => (
                <col key={index} className={className} />
              ))}
            </colgroup>
            <thead className="table-header sticky top-0 z-10">
              <tr className="text-[10px] uppercase tracking-wider">
                <th className="text-left px-4 py-2">Name</th>
                <th className="text-left px-4 py-2">
                  <span className="block font-medium">Works in</span>
                </th>
                <th className="text-left px-4 py-2">
                  <span className="block font-medium">File hash</span>
                </th>
                <th className="text-right px-4 py-2 tabular-nums">Size</th>
                <th className="text-center px-4 py-2 tabular-nums">Used By</th>
                <th className="text-left px-4 py-2">
                  <span className="block font-medium">Added</span>
                </th>
                <th className="text-right px-4 py-2">Actions</th>
              </tr>
            </thead>
            <tbody>
              {reg.isLoading ? (
                PLUGIN_LOADING_ROWS.map((row) => (
                  <SkeletonRow
                    key={row}
                    className={PLUGIN_ROW_GEOMETRY_CLASS}
                    cols={7}
                    cellClassNames={PLUGIN_SKELETON_CELL_CLASSES}
                    skeletonClassNames={PLUGIN_SKELETON_CLASSES}
                  />
                ))
              ) : entries.length ? (
                entries.map((p) => (
                  <tr
                    key={p.id}
                    className={`${PLUGIN_ROW_GEOMETRY_CLASS} border-b border-row hover:bg-overlay-1`}
                  >
                    <td className="px-4 py-2 max-w-[260px]">
                      <div className="flex items-center gap-2">
                        <div
                          className="text-sm font-medium font-sans truncate"
                          title={p.name}
                        >
                          {p.name}
                        </div>
                        {p.is_builtin && <Badge tone="neutral">Built-in</Badge>}
                      </div>
                      {p.version ? (
                        <div className="text-[11px] text-text-faint truncate">
                          v{p.version}
                        </div>
                      ) : null}
                      {p.label ? (
                        <div
                          className="text-[11px] text-text-faint truncate"
                          title={p.label}
                        >
                          {p.label}
                        </div>
                      ) : null}
                      {p.description ? (
                        <div
                          className="text-[11px] text-text-faint truncate mt-1"
                          title={p.description}
                        >
                          {p.description}
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
                          onClick={(e) => {
                            e.stopPropagation();
                            copy(p.sha256_hex, 'SHA256');
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
                      <div className="flex items-center justify-end gap-2">
                        <button
                          type="button"
                          className="text-accent hover:underline font-medium"
                          onClick={() => onSelectPlugin(p.id)}
                        >
                          Inspect
                        </button>
                        {!p.is_builtin && (
                          <button
                            type="button"
                            aria-label="Delete plugin"
                            title={
                              p.refcount > 0
                                ? `In use by ${p.refcount} reference(s)`
                                : 'Delete plugin'
                            }
                            className="transition-colors text-text-faint hover:text-red-400 disabled:cursor-not-allowed disabled:opacity-50 disabled:hover:text-text-faint"
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
                            <Trash2 className="w-3.5 h-3.5" />
                          </button>
                        )}
                      </div>
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

      <PluginDeleteDialog
        pendingDelete={pendingDelete}
        onClose={() => setPendingDelete(null)}
      />
    </>
  );
}
