import { Copy } from 'lucide-react';
import type { PluginEntry } from '../../lib/queries';
import { useCopyButton } from '../../lib/useCopyButton';
import { Card, CardBody, Hint, Section } from '../ui/primitives';
import { RelativeTime } from '../ui/RelativeTime';

export function PluginDetailIntegrity({ plugin }: { plugin: PluginEntry }) {
  const { copy } = useCopyButton();

  return (
    <Section
      title={
        <span className="flex items-baseline gap-2">
          <span className="text-lg font-medium">File details</span>
        </span>
      }
    >
      <Card>
        <CardBody className="space-y-3 text-sm">
          <div className="flex justify-between">
            <span className="text-text-faint">Version</span>
            <span className="font-mono">{plugin.version || 'N/A'}</span>
          </div>
          <div className="flex justify-between">
            <span className="text-text-faint">Revision</span>
            <span className="font-mono">{plugin.revision}</span>
          </div>
          <div className="flex justify-between">
            <span className="text-text-faint">Size</span>
            <span className="font-mono">
              {(plugin.size_bytes / 1024).toFixed(1)} KB
            </span>
          </div>
          <div className="flex justify-between items-center">
            <span className="text-text-faint">SHA256</span>
            <div className="flex items-center gap-1">
              <Hint label={plugin.sha256_hex}>
                <code className="font-mono text-xs cursor-help">
                  {plugin.sha256_hex.slice(0, 12)}…
                </code>
              </Hint>
              <button
                type="button"
                aria-label="Copy SHA256"
                className="text-text-faint hover:text-text"
                onClick={() => copy(plugin.sha256_hex, 'SHA256')}
              >
                <Copy className="w-3 h-3" />
              </button>
            </div>
          </div>
          <div className="flex justify-between">
            <span className="text-text-faint">Uploaded</span>
            <span>
              <RelativeTime
                ts={new Date(plugin.uploaded_at_unix_secs * 1000)}
              />
            </span>
          </div>
          <div className="flex justify-between">
            <span className="text-text-faint">Filename</span>
            <span
              className="font-mono text-xs truncate max-w-[150px]"
              title={plugin.original_filename}
            >
              {plugin.original_filename}
            </span>
          </div>
          <div className="pt-4 border-t border-subtle mt-4">
            <h3 className="text-sm font-medium text-text mb-2 flex items-baseline gap-2">
              <span>Updating this plugin</span>
            </h3>
            <div className="text-xs space-y-1">
              <p className="text-text-faint">
                Upload a file with the same name to update this plugin. If the
                version is clearly newer, it replaces the current file;
                otherwise you will be asked to confirm.
              </p>
            </div>
          </div>
        </CardBody>
      </Card>
    </Section>
  );
}
