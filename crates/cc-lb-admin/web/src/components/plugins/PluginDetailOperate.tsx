import { Trash2 } from 'lucide-react';
import { useState } from 'react';
import { toast } from 'sonner';
import { type PluginEntry, usePatchPlugin } from '../../lib/queries';
import { Button, Card, CardBody, Section } from '../ui/primitives';

export function PluginDetailOperate({
  plugin,
  onDelete,
}: {
  plugin: PluginEntry;
  onDelete: () => void;
}) {
  const patch = usePatchPlugin();
  const [isEditingLabel, setIsEditingLabel] = useState(false);
  const [editLabelValue, setEditLabelValue] = useState(plugin.label ?? '');

  const handleSaveLabel = () => {
    patch.mutate(
      {
        id: plugin.id,
        label: editLabelValue || null,
        revision: plugin.revision,
      },
      {
        onSuccess: () => {
          toast.success('Label updated');
          setIsEditingLabel(false);
        },
      },
    );
  };

  return (
    <Section
      title={
        <span className="flex items-baseline gap-2">
          <span className="text-lg font-medium">Manage this plugin</span>
        </span>
      }
    >
      <Card>
        <CardBody className="space-y-4">
          <div>
            <h3 className="text-sm font-medium text-text mb-2">Label</h3>
            {plugin.is_builtin ? (
              <span className="text-sm text-text-faint">
                Built-in plugins cannot be edited or deleted.
              </span>
            ) : isEditingLabel ? (
              <div
                aria-busy={patch.isPending}
                className="flex gap-2"
                data-testid="plugin-label-edit-form"
              >
                <input
                  type="text"
                  value={editLabelValue}
                  onChange={(e) => setEditLabelValue(e.target.value)}
                  disabled={patch.isPending}
                  className="flex-1 bg-bg-sub border border-subtle rounded-sm px-2 py-1 text-sm disabled:opacity-50 disabled:cursor-not-allowed"
                  placeholder="Optional label"
                />
                <Button
                  size="sm"
                  loading={patch.isPending}
                  disabled={patch.isPending}
                  onClick={handleSaveLabel}
                >
                  {patch.isPending ? 'Saving...' : 'Save'}
                </Button>
                <Button
                  size="sm"
                  variant="ghost"
                  disabled={patch.isPending}
                  onClick={() => {
                    setIsEditingLabel(false);
                    setEditLabelValue(plugin.label ?? '');
                  }}
                >
                  Cancel
                </Button>
              </div>
            ) : (
              <div className="flex items-center justify-between">
                <span className="text-sm text-text-faint">
                  {plugin.label || 'No label'}
                </span>
                <Button
                  size="sm"
                  variant="secondary"
                  onClick={() => setIsEditingLabel(true)}
                >
                  Edit
                </Button>
              </div>
            )}
          </div>
          <div className="pt-4 border-t border-subtle">
            <h3 className="text-sm font-medium text-text mb-2">Health</h3>
            <div className="text-xs mb-2 space-y-1">
              <p className="text-text-faint">
                Health metrics for individual plugins are not yet available in
                the dashboard.
              </p>
            </div>
            <div className="space-y-2 text-xs text-text-faint">
              <div className="mt-2">
                <p className="font-medium text-text mb-1">
                  Available Prometheus metrics:
                </p>
                <ul className="list-disc pl-4 space-y-1 font-mono text-[10px]">
                  <li>cc_lb_plugin_call_duration_seconds{'{plugin,hook}'}</li>
                  <li>cc_lb_plugin_trap_total{'{plugin,hook,phase}'}</li>
                </ul>
              </div>
            </div>
          </div>
          {!plugin.is_builtin && (
            <div className="pt-4 border-t border-subtle">
              <h3 className="text-sm font-medium text-text mb-2">
                Delete plugin
              </h3>
              <Button variant="danger" size="sm" onClick={onDelete}>
                <Trash2 className="w-4 h-4 mr-2" />
                Delete Plugin
              </Button>
            </div>
          )}
        </CardBody>
      </Card>
    </Section>
  );
}
