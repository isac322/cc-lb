import { Copy } from 'lucide-react';
import { useMemo } from 'react';
import type { WarmupDialectPluginSnapshot } from '../../../../lib/queries';
import { useCopyButton } from '../../../../lib/useCopyButton';
import { Button, Modal } from '../../../ui/primitives';

interface Props {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  plugin: WarmupDialectPluginSnapshot;
}

export function WarmupConfigModal({ open, onOpenChange, plugin }: Props) {
  const { copied, copy } = useCopyButton();

  const pretty = useMemo(() => {
    try {
      return JSON.stringify(plugin.config, null, 2);
    } catch {
      return String(plugin.config);
    }
  }, [plugin.config]);

  const handleCopy = () => copy(pretty, 'Plugin config');

  return (
    <Modal
      open={open}
      onOpenChange={onOpenChange}
      title={
        <span className="font-mono">
          {plugin.wasm_registry_id}
          {plugin.wire_version != null && (
            <span className="ml-2 font-sans text-caption text-text-faint">
              wire v{plugin.wire_version}
            </span>
          )}
        </span>
      }
      description="The exact config this warm-up sent to the shape plugin."
      size="md"
      footer={
        <Button variant="secondary" iconLeft={<Copy />} onClick={handleCopy}>
          {copied ? 'Copied' : 'Copy JSON'}
        </Button>
      }
    >
      <pre className="max-h-[60vh] overflow-y-auto whitespace-pre-wrap break-all rounded-sm bg-overlay-2 p-3 font-mono text-data">
        {pretty || '{}'}
      </pre>
    </Modal>
  );
}
