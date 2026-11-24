import { Copy } from 'lucide-react';
import type { ReactNode } from 'react';
import type { PluginEntry } from '../../lib/queries';
import { useCopyButton } from '../../lib/useCopyButton';
import { Hint, IconButton, Section } from '../ui/primitives';
import { RelativeTime } from '../ui/RelativeTime';
import { EmptyValue } from '../ui/Table';

function Row({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div className="flex min-h-8 items-center justify-between gap-4 border-b border-row py-1 last:border-b-0">
      <dt className="text-label text-text-muted">{label}</dt>
      <dd className="min-w-0 text-right text-body text-text">{children}</dd>
    </div>
  );
}

export function PluginDetailIntegrity({ plugin }: { plugin: PluginEntry }) {
  const { copy } = useCopyButton();
  const builtinFile = plugin.is_builtin && plugin.size_bytes === 0;

  return (
    <Section title="File details">
      <div>
        <dl>
          <Row label="Version">
            {plugin.version ? `v${plugin.version}` : <EmptyValue />}
          </Row>
          <Row label="Revision">
            <span className="tabular-nums">{plugin.revision}</span>
          </Row>
          <Row label="Size">
            {builtinFile ? (
              <span className="text-text-muted">Built-in</span>
            ) : (
              <span className="tabular-nums">
                {(plugin.size_bytes / 1024).toFixed(1)}{' '}
                <span className="text-text-muted">KB</span>
              </span>
            )}
          </Row>
          <Row label="SHA256">
            <span className="-my-2 inline-flex items-center gap-1">
              <Hint label={plugin.sha256_hex}>
                <code className="cursor-help font-mono text-data">
                  {plugin.sha256_hex.slice(0, 12)}…
                </code>
              </Hint>
              <IconButton
                label="Copy SHA256"
                className="-mr-2"
                onClick={() => copy(plugin.sha256_hex, 'SHA256')}
              >
                <Copy className="w-3.5 h-3.5" aria-hidden="true" />
              </IconButton>
            </span>
          </Row>
          <Row label="Uploaded">
            {plugin.is_builtin && plugin.uploaded_at_unix_secs === 0 ? (
              <span className="text-text-muted">Built-in</span>
            ) : (
              <RelativeTime
                ts={new Date(plugin.uploaded_at_unix_secs * 1000)}
              />
            )}
          </Row>
          <Row label="Filename">
            <span
              className="block max-w-[12rem] truncate font-mono text-data max-md:max-w-none max-md:whitespace-normal max-md:break-all"
              title={plugin.original_filename}
            >
              {plugin.original_filename}
            </span>
          </Row>
        </dl>
        <div className="mt-8">
          <h3 className="text-title-card text-text">Updating this plugin</h3>
          <p className="mt-1 text-body text-text-muted">
            Upload a file with the same name to update this plugin. If the
            version is clearly newer, it replaces the current file; otherwise
            you will be asked to confirm.
          </p>
        </div>
      </div>
    </Section>
  );
}
