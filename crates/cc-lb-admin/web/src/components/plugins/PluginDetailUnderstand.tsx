import type { ReactNode } from 'react';
import type { PluginEntry } from '../../lib/queries';
import { Badge, Card, Section } from '../ui/primitives';
import {
  EmptyValue,
  Table,
  TableCell,
  TableHead,
  TableHeadCell,
} from '../ui/Table';
import { SLOTS } from './slots/model';

const SLOT_DESCRIPTIONS: Record<string, string> = {
  router: 'Filters or reorders upstreams before a request is sent.',
  observability_hook:
    'Receives events for logging, metrics, or other side effects.',
  shape:
    'Modifies requests before they are sent and responses before they are returned.',
};

function getSlotForHook(hookName: string): string {
  if (hookName === 'filter') return 'Router';
  if (
    hookName === 'shape' ||
    hookName === 'transform_response' ||
    hookName === 'transform_sse_event'
  )
    return 'Shape';
  if (hookName === 'observe') return 'Observability';
  return 'Unknown';
}

/** A labelled block in a section: 13px label above 14px copy. */
function Detail({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div>
      <dt className="text-label text-text-muted">{label}</dt>
      <dd className="mt-1 text-body text-text">{children}</dd>
    </div>
  );
}

export function PluginDetailUnderstand({ plugin }: { plugin: PluginEntry }) {
  const hooks = plugin.hook_metadata
    ? Object.entries(plugin.hook_metadata)
    : [];
  return (
    <>
      <Section title="What this plugin does">
        <div>
          <dl className="space-y-5">
            <Detail label="Description">
              {plugin.description || (
                <span className="text-text-faint">
                  No description provided.
                </span>
              )}
            </Detail>
            <Detail label="Usage">
              {plugin.usage ? (
                <span className="whitespace-pre-wrap">{plugin.usage}</span>
              ) : (
                <span className="text-text-faint">
                  No usage instructions provided.
                </span>
              )}
            </Detail>
            <div>
              <dt className="text-label text-text-muted">Where it can run</dt>
              <dd className="mt-1.5 flex flex-col gap-1.5">
                {plugin.supported_slots?.map((slot) => (
                  <div key={slot} className="flex items-start gap-2">
                    <Badge tone="neutral" className="shrink-0">
                      {SLOTS.find((s) => s.id === slot)?.label ?? slot}
                    </Badge>
                    <span className="text-body text-text-muted">
                      {SLOT_DESCRIPTIONS[slot] ?? ''}
                    </span>
                  </div>
                ))}
              </dd>
            </div>
          </dl>
          {plugin.metadata && (
            <div className="mt-10">
              <h3 className="text-title-card text-text">Built-in metadata</h3>
              <dl className="mt-4 grid grid-cols-1 gap-x-6 gap-y-5 md:grid-cols-2">
                <Detail label="Purpose">{plugin.metadata.purpose}</Detail>
                <Detail label="Empty behavior">
                  {plugin.metadata.empty_behavior}
                </Detail>
                <Detail label="Keeps">{plugin.metadata.keeps}</Detail>
                <Detail label="Drops">{plugin.metadata.drops}</Detail>
              </dl>
              {plugin.metadata.examples &&
                plugin.metadata.examples.length > 0 && (
                  <div className="mt-5">
                    <div className="text-label text-text-muted">Examples</div>
                    <ul className="mt-1 list-disc space-y-1 pl-4 text-body text-text">
                      {plugin.metadata.examples.map((ex, i) => (
                        <li key={i}>{ex}</li>
                      ))}
                    </ul>
                  </div>
                )}
            </div>
          )}
        </div>
      </Section>
      {hooks.length > 0 && (
        <Section title="Hooks">
          <Card className="overflow-x-auto">
            <Table className="min-w-[640px] table-fixed">
              <colgroup>
                <col className="w-[22%]" />
                <col />
                <col />
              </colgroup>
              <TableHead sticky={false}>
                <tr>
                  <TableHeadCell>Hook</TableHeadCell>
                  <TableHeadCell>Description</TableHeadCell>
                  <TableHeadCell>Usage</TableHeadCell>
                </tr>
              </TableHead>
              <tbody>
                {hooks.map(([hookName, meta]) => (
                  <tr
                    key={hookName}
                    className="border-b border-row last:border-b-0 align-top"
                  >
                    <TableCell className="py-3 align-top">
                      <div className="font-mono text-data text-text break-all">
                        {hookName}
                      </div>
                      <div className="mt-1 text-caption text-text-faint">
                        {getSlotForHook(hookName)} · wire v{meta.wire_version}
                        {meta.mode === 'noop' ? ' · No-op' : null}
                      </div>
                    </TableCell>
                    <TableCell className="py-3 align-top text-text-muted">
                      {meta.description || <EmptyValue />}
                    </TableCell>
                    <TableCell className="py-3 align-top text-text-muted">
                      {meta.usage || <EmptyValue />}
                    </TableCell>
                  </tr>
                ))}
              </tbody>
            </Table>
          </Card>
        </Section>
      )}
    </>
  );
}
