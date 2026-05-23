import { Copy } from 'lucide-react';
import type { PluginStatusEntry } from '../../lib/api';
import { Card } from '../primitives/Card';
import { StatusChip } from '../primitives/StatusChip';
import { PluginFailureCount } from './PluginFailureCount';

interface PluginCardProps {
  plugin: PluginStatusEntry;
}

export function PluginCard({ plugin }: PluginCardProps) {
  const copyToClipboard = () => {
    navigator.clipboard.writeText(plugin.wasm_path);
  };

  return (
    <Card className="p-4">
      <div className="flex justify-between items-start mb-4">
        <div>
          <h3 className="text-lg font-mono font-semibold text-graphite-50">
            {plugin.name}
          </h3>
          <div className="flex items-center gap-2 mt-1">
            <StatusChip variant={plugin.loaded ? 'ok' : 'danger'}>
              {plugin.loaded ? 'loaded' : 'not loaded'}
            </StatusChip>
            {plugin.disabled && (
              <StatusChip variant="warn">disabled</StatusChip>
            )}
          </div>
        </div>
        <PluginFailureCount
          count={plugin.failure_count}
          lastError={plugin.last_error}
        />
      </div>

      <div className="space-y-3">
        <div className="flex flex-col gap-1">
          <span className="text-xs text-graphite-400 uppercase tracking-wider">
            WASM Path
          </span>
          <div className="flex items-center gap-2 bg-gray-50 p-2 rounded border border-gray-100">
            <span
              className="text-sm font-mono text-gray-700 truncate flex-1"
              title={plugin.wasm_path}
            >
              {plugin.wasm_path}
            </span>
            <button
              onClick={copyToClipboard}
              className="text-gray-400 hover:text-gray-600 transition-colors"
              title="Copy path"
            >
              <Copy className="w-4 h-4" />
            </button>
          </div>
        </div>

        {(plugin.sse_per_event !== null ||
          plugin.batched_events_per_flush !== null) && (
          <div className="flex flex-col gap-1 pt-2 border-t border-gray-100">
            <span className="text-xs text-graphite-400 uppercase tracking-wider">
              Configuration
            </span>
            <div className="flex flex-wrap gap-2 mt-1">
              {plugin.sse_per_event !== null && (
                <span className="text-xs bg-gray-100 text-gray-600 px-2 py-1 rounded">
                  sse_per_event: {plugin.sse_per_event ? 'true' : 'false'}
                </span>
              )}
              {plugin.batched_events_per_flush !== null && (
                <span className="text-xs bg-gray-100 text-gray-600 px-2 py-1 rounded">
                  batch: {plugin.batched_events_per_flush} ev /{' '}
                  {plugin.batched_flush_ms} ms
                </span>
              )}
            </div>
          </div>
        )}
      </div>
    </Card>
  );
}
