import { Trash2 } from 'lucide-react';
import { useState } from 'react';
import { toast } from 'sonner';
import { type PluginEntry, usePatchPlugin } from '../../lib/queries';
import { Button, cx, INPUT_SM_CLASS, Section } from '../ui/primitives';

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
    <Section title="Manage this plugin">
      <div className="space-y-8">
        <div>
          <h3 className="text-title-card text-text">Label</h3>
          {plugin.is_builtin ? (
            <p className="mt-1 text-body text-text-muted">
              Built-in plugins cannot be edited or deleted.
            </p>
          ) : isEditingLabel ? (
            <div
              aria-busy={patch.isPending}
              className="mt-2 flex gap-2"
              data-testid="plugin-label-edit-form"
            >
              <input
                type="text"
                aria-label="Plugin label"
                value={editLabelValue}
                onChange={(e) => setEditLabelValue(e.target.value)}
                disabled={patch.isPending}
                className={cx(INPUT_SM_CLASS, 'min-w-0 flex-1')}
                placeholder="Optional label"
              />
              <Button
                variant="primary"
                loading={patch.isPending}
                disabled={patch.isPending}
                onClick={handleSaveLabel}
              >
                {patch.isPending ? 'Saving...' : 'Save'}
              </Button>
              <Button
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
            <div className="mt-1 flex items-center justify-between gap-3">
              <span
                className={cx(
                  'min-w-0 truncate text-body',
                  plugin.label ? 'text-text' : 'text-text-faint',
                )}
              >
                {plugin.label || 'No label'}
              </span>
              <Button size="sm" onClick={() => setIsEditingLabel(true)}>
                Edit
              </Button>
            </div>
          )}
        </div>
        <div>
          <h3 className="text-title-card text-text">Health</h3>
          <p className="mt-1 text-body text-text-muted">
            Health metrics for individual plugins are not yet available in the
            dashboard. Available Prometheus metrics:
          </p>
          <ul className="mt-2 space-y-1 font-mono text-data text-text-muted break-all">
            <li>cc_lb_plugin_call_duration_seconds{'{plugin,hook}'}</li>
            <li>cc_lb_plugin_trap_total{'{plugin,hook,phase}'}</li>
          </ul>
        </div>
        {!plugin.is_builtin && (
          <div>
            <Button variant="danger" size="sm" onClick={onDelete}>
              <Trash2 aria-hidden="true" strokeWidth={1.75} />
              Delete plugin
            </Button>
          </div>
        )}
      </div>
    </Section>
  );
}
