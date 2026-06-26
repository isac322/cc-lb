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
            <span className="ml-2 text-xs text-text-faint">
              wire v{plugin.wire_version}
            </span>
          )}
        </span>
      }
      description="The exact config this warm-up sent to the shape plugin."
      size="md"
      footer={
        <Button
          variant="secondary"
          iconLeft={<Copy className="w-3 h-3" />}
          onClick={handleCopy}
        >
          {copied ? 'Copied' : 'Copy JSON'}
        </Button>
      }
    >
      <pre className="text-xs font-mono whitespace-pre-wrap break-all bg-bg border border-subtle rounded-sm p-3 max-h-[60vh] overflow-y-auto">
        {pretty || '{}'}
      </pre>
    </Modal>
  );
}
