import { Dialog as BaseDialog } from '@base-ui/react/dialog';
import { X } from 'lucide-react';
import { useEffect, useState } from 'react';
import { toast } from 'sonner';
import { ApiError } from '../../../lib/api';
import {
  type Principal,
  useUpdatePrincipalCacheKeepalive,
} from '../../../lib/queries';
import { cx } from '../../ui/primitives';

interface Props {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  principal: Principal;
}

export function CacheKeepaliveSettingsDrawer({
  open,
  onOpenChange,
  principal,
}: Props) {
  const updateMutation = useUpdatePrincipalCacheKeepalive();

  const [enabled, setEnabled] = useState(false);
  const [lead5m, setLead5m] = useState(30);
  const [lead1h, setLead1h] = useState(300);
  const [maxRenewals, setMaxRenewals] = useState(12);
  const [maxDuration, setMaxDuration] = useState(14400);
  const [snapshotBytes, setSnapshotBytes] = useState(524288);
  const [extraTools, setExtraTools] = useState<string[]>([]);
  const [treatAmbiguous, setTreatAmbiguous] = useState(false);
  const [newTool, setNewTool] = useState('');

  useEffect(() => {
    if (open) {
      const cfg = principal.cache_keepalive;
      setEnabled(cfg?.enabled ?? false);
      setLead5m(cfg?.refresh_lead_time_5m_secs ?? 30);
      setLead1h(cfg?.refresh_lead_time_1h_secs ?? 300);
      setMaxRenewals(cfg?.max_refreshes_per_session ?? 12);
      setMaxDuration(cfg?.max_total_duration_secs ?? 14400);
      setSnapshotBytes(cfg?.snapshot_max_bytes ?? 524288);
      setExtraTools(cfg?.classifier?.extra_wait_for_user_tools ?? []);
      setTreatAmbiguous(cfg?.classifier?.treat_end_turn_as_ambiguous ?? false);
      setNewTool('');
    }
  }, [open, principal]);

  const reset = () => {
    const cfg = principal.cache_keepalive;
    setEnabled(cfg?.enabled ?? false);
    setLead5m(cfg?.refresh_lead_time_5m_secs ?? 30);
    setLead1h(cfg?.refresh_lead_time_1h_secs ?? 300);
    setMaxRenewals(cfg?.max_refreshes_per_session ?? 12);
    setMaxDuration(cfg?.max_total_duration_secs ?? 14400);
    setSnapshotBytes(cfg?.snapshot_max_bytes ?? 524288);
    setExtraTools(cfg?.classifier?.extra_wait_for_user_tools ?? []);
    setTreatAmbiguous(cfg?.classifier?.treat_end_turn_as_ambiguous ?? false);
    setNewTool('');
  };

  const handleSave = () => {
    if (
      !Number.isFinite(lead5m) ||
      lead5m < 0 ||
      !Number.isFinite(lead1h) ||
      lead1h < 0 ||
      !Number.isFinite(maxRenewals) ||
      maxRenewals < 0 ||
      !Number.isFinite(maxDuration) ||
      maxDuration < 0 ||
      !Number.isFinite(snapshotBytes) ||
      snapshotBytes < 0
    ) {
      toast.error('Numeric fields must be valid non-negative numbers');
      return;
    }

    updateMutation.mutate(
      {
        id: principal.id,
        expected_revision: principal.revision,
        cache_keepalive: {
          enabled,
          refresh_lead_time_5m_secs: lead5m,
          refresh_lead_time_1h_secs: lead1h,
          max_refreshes_per_session: maxRenewals,
          max_total_duration_secs: maxDuration,
          snapshot_max_bytes: snapshotBytes,
          classifier: {
            extra_wait_for_user_tools: extraTools,
            treat_end_turn_as_ambiguous: treatAmbiguous,
          },
        },
      },
      {
        onSuccess: () => {
          toast.success('Cache keepalive settings updated');
          onOpenChange(false);
        },
        onError: (error) => {
          if (error instanceof ApiError && error.status === 412) {
            toast.error(
              'Principal was modified by another user. Please refresh and try again.',
            );
          } else {
            toast.error(
              error instanceof Error
                ? error.message
                : 'Failed to update settings',
            );
          }
        },
      },
    );
  };

  const handleAddTool = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter' && newTool.trim()) {
      e.preventDefault();
      if (!extraTools.includes(newTool.trim())) {
        setExtraTools([...extraTools, newTool.trim()]);
      }
      setNewTool('');
    }
  };

  const removeTool = (tool: string) => {
    setExtraTools(extraTools.filter((t) => t !== tool));
  };

  return (
    <BaseDialog.Root open={open} onOpenChange={onOpenChange}>
      <BaseDialog.Portal>
        <BaseDialog.Backdrop className="fixed inset-0 z-40 bg-drawer-backdrop transition-opacity duration-200 ease-out data-[ending-style]:opacity-0 data-[starting-style]:opacity-0" />
        <BaseDialog.Popup
          className="fixed right-0 top-0 bottom-0 w-full max-w-md bg-bg-sub border-l border-subtle z-50 flex flex-col outline-none transition-transform duration-200 ease-out data-[ending-style]:translate-x-full data-[starting-style]:translate-x-full"
          data-testid="cache-keepalive-settings-drawer"
        >
          <BaseDialog.Title className="sr-only">
            Cache keepalive settings
          </BaseDialog.Title>
          <BaseDialog.Description className="sr-only">
            Advanced settings for cache keepalive.
          </BaseDialog.Description>

          <header className="flex items-center justify-between gap-3 px-4 py-3 border-b border-subtle shrink-0">
            <div className="min-w-0">
              <h3 className="text-sm font-medium text-text">
                Cache keepalive settings
              </h3>
              <p className="text-[11px] text-text-faint font-mono truncate">
                {principal.name}
              </p>
            </div>
            <button
              type="button"
              onClick={() => onOpenChange(false)}
              aria-label="Close settings"
              className="inline-flex items-center justify-center w-7 h-7 rounded-sm text-text-muted hover:text-text hover:bg-overlay-5 shrink-0"
            >
              <X className="w-4 h-4" />
            </button>
          </header>

          <div className="flex-1 overflow-y-auto p-4 space-y-6">
            <div className="flex items-center justify-between">
              <span className="text-[11px] uppercase tracking-wider text-text-faint">
                Enabled
              </span>
              <button
                type="button"
                role="switch"
                aria-checked={enabled}
                aria-label="Enable cache keepalive"
                onClick={() => setEnabled(!enabled)}
                className={cx(
                  'relative inline-flex h-5 w-9 shrink-0 items-center self-center rounded-full border transition-colors duration-200 ease-in-out focus:outline-none focus:ring-2 focus:ring-accent/40',
                  enabled
                    ? 'bg-[var(--color-ok)] border-[var(--color-ok)]'
                    : 'bg-overlay-5 border-subtle-strong hover:border-text-muted',
                )}
              >
                <span
                  className={cx(
                    'pointer-events-none inline-block h-4 w-4 rounded-full bg-white shadow-sm transition-transform duration-200 ease-in-out',
                    enabled ? 'translate-x-4' : 'translate-x-0.5',
                  )}
                />
              </button>
            </div>

            <div className="grid grid-cols-2 gap-4">
              <label className="flex flex-col gap-1.5">
                <span className="text-[11px] uppercase tracking-wider text-text-faint">
                  Renewal lead time · 5m TTL
                </span>
                <input
                  type="number"
                  value={lead5m}
                  onChange={(e) => setLead5m(Number(e.target.value))}
                  className="w-full h-9 px-2.5 text-sm bg-bg border border-subtle rounded-sm text-text placeholder:text-text-faint focus:border-accent focus:outline-none transition-colors"
                />
                <span className="text-[11px] text-text-faint">
                  → renews 30s before the 5m cache expires
                </span>
              </label>

              <label className="flex flex-col gap-1.5">
                <span className="text-[11px] uppercase tracking-wider text-text-faint">
                  Renewal lead time · 1h TTL
                </span>
                <input
                  type="number"
                  value={lead1h}
                  onChange={(e) => setLead1h(Number(e.target.value))}
                  className="w-full h-9 px-2.5 text-sm bg-bg border border-subtle rounded-sm text-text placeholder:text-text-faint focus:border-accent focus:outline-none transition-colors"
                />
              </label>

              <label className="flex flex-col gap-1.5">
                <span className="text-[11px] uppercase tracking-wider text-text-faint">
                  Max renewals per session
                </span>
                <input
                  type="number"
                  value={maxRenewals}
                  onChange={(e) => setMaxRenewals(Number(e.target.value))}
                  className="w-full h-9 px-2.5 text-sm bg-bg border border-subtle rounded-sm text-text placeholder:text-text-faint focus:border-accent focus:outline-none transition-colors"
                />
              </label>

              <label className="flex flex-col gap-1.5">
                <span className="text-[11px] uppercase tracking-wider text-text-faint">
                  Max total duration
                </span>
                <input
                  type="number"
                  value={maxDuration}
                  onChange={(e) => setMaxDuration(Number(e.target.value))}
                  className="w-full h-9 px-2.5 text-sm bg-bg border border-subtle rounded-sm text-text placeholder:text-text-faint focus:border-accent focus:outline-none transition-colors"
                />
                <span className="text-[11px] text-text-faint">= 4h</span>
              </label>

              <label className="flex flex-col gap-1.5">
                <span className="text-[11px] uppercase tracking-wider text-text-faint">
                  Snapshot max bytes
                </span>
                <input
                  type="number"
                  value={snapshotBytes}
                  onChange={(e) => setSnapshotBytes(Number(e.target.value))}
                  className="w-full h-9 px-2.5 text-sm bg-bg border border-subtle rounded-sm text-text placeholder:text-text-faint focus:border-accent focus:outline-none transition-colors"
                />
                <span className="text-[11px] text-text-faint">= 512 KiB</span>
              </label>
            </div>

            <div className="space-y-4">
              <label className="flex flex-col gap-1.5">
                <span className="text-[11px] uppercase tracking-wider text-text-faint">
                  extra_wait_for_user_tools
                </span>
                <div className="flex flex-wrap gap-2 p-2 min-h-9 bg-bg border border-subtle rounded-sm">
                  {extraTools.map((tool) => (
                    <span
                      key={tool}
                      className="inline-flex items-center gap-1 px-2 py-0.5 text-xs rounded-sm bg-overlay-4 text-text border border-subtle"
                    >
                      {tool}{' '}
                      <X
                        className="w-3 h-3 cursor-pointer hover:text-text-muted"
                        onClick={() => removeTool(tool)}
                      />
                    </span>
                  ))}
                  <input
                    type="text"
                    value={newTool}
                    onChange={(e) => setNewTool(e.target.value)}
                    onKeyDown={handleAddTool}
                    className="flex-1 min-w-[100px] bg-transparent outline-none text-sm text-text"
                    placeholder="Add tool..."
                  />
                </div>
              </label>

              <div className="flex items-center justify-between">
                <div className="flex flex-col">
                  <span className="text-[11px] uppercase tracking-wider text-text-faint">
                    treat end_turn as ambiguous
                  </span>
                </div>
                <button
                  type="button"
                  role="switch"
                  aria-checked={treatAmbiguous}
                  aria-label="Treat end_turn as ambiguous"
                  onClick={() => setTreatAmbiguous(!treatAmbiguous)}
                  className={cx(
                    'relative inline-flex h-5 w-9 shrink-0 items-center self-center rounded-full border transition-colors duration-200 ease-in-out focus:outline-none focus:ring-2 focus:ring-accent/40',
                    treatAmbiguous
                      ? 'bg-[var(--color-ok)] border-[var(--color-ok)]'
                      : 'bg-overlay-5 border-subtle-strong hover:border-text-muted',
                  )}
                >
                  <span
                    className={cx(
                      'pointer-events-none inline-block h-4 w-4 rounded-full bg-white shadow-sm transition-transform duration-200 ease-in-out',
                      treatAmbiguous ? 'translate-x-4' : 'translate-x-0.5',
                    )}
                  />
                </button>
              </div>

              <div className="flex items-center justify-between opacity-50">
                <div className="flex flex-col">
                  <span className="text-[11px] uppercase tracking-wider text-text-faint">
                    LLM Judge
                  </span>
                </div>
                <span className="inline-flex items-center px-2 py-0.5 text-[11px] rounded-sm border bg-overlay-4 text-text border-subtle">
                  Reserved for a future release
                </span>
              </div>
            </div>
          </div>

          <div className="px-4 py-3 border-t border-subtle flex items-center justify-end gap-2 shrink-0">
            <button
              type="button"
              onClick={reset}
              className="inline-flex items-center justify-center rounded-sm font-medium transition-colors select-none text-[var(--color-text-muted)] hover:bg-[var(--color-hover-bg)] hover:text-[var(--color-text)] h-9 px-3 text-sm"
            >
              Reset
            </button>
            <button
              type="button"
              onClick={handleSave}
              disabled={updateMutation.isPending}
              className="inline-flex items-center justify-center rounded-sm font-medium transition-colors select-none bg-[var(--color-text)] text-[var(--color-bg)] hover:opacity-90 h-9 px-3 text-sm disabled:opacity-50"
            >
              Save changes
            </button>
          </div>
        </BaseDialog.Popup>
      </BaseDialog.Portal>
    </BaseDialog.Root>
  );
}
