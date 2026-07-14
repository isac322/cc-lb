import type { PluginEntry } from '../../lib/queries';
import { Badge, Card, CardBody, Section } from '../ui/primitives';
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

export function PluginDetailUnderstand({ plugin }: { plugin: PluginEntry }) {
  return (
    <Section
      title={
        <span className="flex items-baseline gap-2">
          <span className="text-lg font-medium">What this plugin does</span>
        </span>
      }
    >
      <Card>
        <CardBody className="space-y-4">
          <div>
            <h3 className="text-sm font-medium text-text mb-1">Description</h3>
            <p className="text-sm text-text-faint">
              {plugin.description || 'No description provided.'}
            </p>
          </div>
          <div>
            <h3 className="text-sm font-medium text-text mb-1">Usage</h3>
            <p className="text-sm text-text-faint whitespace-pre-wrap">
              {plugin.usage || 'No usage instructions provided.'}
            </p>
          </div>
          <div>
            <h3 className="text-sm font-medium text-text mb-2 flex items-baseline gap-2">
              <span>Where it can run</span>
            </h3>
            <div className="flex flex-col gap-2">
              {plugin.supported_slots?.map((slot) => (
                <div key={slot} className="flex items-start gap-2">
                  <Badge tone="accent">
                    {SLOTS.find((s) => s.id === slot)?.label ?? slot}
                  </Badge>
                  <div className="text-xs mt-0.5 flex flex-col gap-0.5">
                    <span className="text-text-faint">
                      {SLOT_DESCRIPTIONS[slot] ?? ''}
                    </span>
                  </div>
                </div>
              ))}
            </div>
          </div>
          {plugin.metadata && (
            <div className="space-y-3 pt-4 border-t border-subtle">
              <h3 className="text-sm font-medium text-text">
                Built-in Metadata
              </h3>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-sm">
                <div>
                  <span className="text-text-faint block mb-1">Purpose</span>
                  <span>{plugin.metadata.purpose}</span>
                </div>
                <div>
                  <span className="text-text-faint block mb-1">
                    Empty Behavior
                  </span>
                  <span>{plugin.metadata.empty_behavior}</span>
                </div>
                <div>
                  <span className="text-text-faint block mb-1">Keeps</span>
                  <span>{plugin.metadata.keeps}</span>
                </div>
                <div>
                  <span className="text-text-faint block mb-1">Drops</span>
                  <span>{plugin.metadata.drops}</span>
                </div>
              </div>
              {plugin.metadata.examples &&
                plugin.metadata.examples.length > 0 && (
                  <div className="mt-2">
                    <span className="text-text-faint block mb-1 text-sm">
                      Examples
                    </span>
                    <ul className="list-disc pl-4 text-sm space-y-1">
                      {plugin.metadata.examples.map((ex, i) => (
                        <li key={i}>{ex}</li>
                      ))}
                    </ul>
                  </div>
                )}
            </div>
          )}
          {plugin.hook_metadata &&
            Object.keys(plugin.hook_metadata).length > 0 && (
              <div className="pt-4 border-t border-subtle">
                <h3 className="text-sm font-medium text-text mb-2">Hooks</h3>
                <div className="overflow-x-auto border border-subtle rounded-sm">
                  <table className="w-full text-xs text-left">
                    <thead className="bg-bg-sub border-b border-subtle">
                      <tr>
                        <th className="px-3 py-2 font-medium">Hook</th>
                        <th className="px-3 py-2 font-medium">Slot</th>
                        <th className="px-3 py-2 font-medium">Version</th>
                        <th className="px-3 py-2 font-medium">Mode</th>
                        <th className="px-3 py-2 font-medium">Description</th>
                        <th className="px-3 py-2 font-medium">Usage</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-subtle">
                      {Object.entries(plugin.hook_metadata).map(
                        ([hookName, meta]) => (
                          <tr key={hookName}>
                            <td className="px-3 py-2 font-mono">{hookName}</td>
                            <td className="px-3 py-2">
                              {getSlotForHook(hookName)}
                            </td>
                            <td className="px-3 py-2">v{meta.wire_version}</td>
                            <td className="px-3 py-2">
                              {meta.mode ? (
                                <Badge
                                  tone={
                                    meta.mode === 'active' ? 'ok' : 'neutral'
                                  }
                                >
                                  {meta.mode}
                                </Badge>
                              ) : (
                                <span className="text-text-faint">—</span>
                              )}
                            </td>
                            <td className="px-3 py-2 text-text-faint">
                              {meta.description}
                            </td>
                            <td className="px-3 py-2 text-text-faint">
                              {meta.usage}
                            </td>
                          </tr>
                        ),
                      )}
                    </tbody>
                  </table>
                </div>
              </div>
            )}
        </CardBody>
      </Card>
    </Section>
  );
}
