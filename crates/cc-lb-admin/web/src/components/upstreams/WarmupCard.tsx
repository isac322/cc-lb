import { Zap } from 'lucide-react';
import { useEffect, useState } from 'react';
import { toast } from 'sonner';
import {
  COPY,
  FIRE_NOW_COOLDOWN_MS,
  type FireErrorReason,
  LEASE_PANEL_AUTO_DISMISS_MS,
  TOAST_DURATIONS,
} from '../../lib/copy/warmup';
import { formatRelativeUnixSeconds } from '../../lib/format';
import {
  type Upstream,
  useFireNowUpstreamWarmup,
  usePluginRegistry,
  useUpdateUpstreamWarmupSettings,
} from '../../lib/queries';
import {
  Button,
  Card,
  CardBody,
  CardHeader,
  ConfirmDialog,
  cx,
  EmptyState,
  INPUT_CLASS,
} from '../ui/primitives';
import { RelativeTime, ResetCountdown } from '../ui/RelativeTime';

function pluginSupportsSlot(
  p: { supported_slots?: string[] },
  slot: string,
): boolean {
  return p.supported_slots?.includes(slot) ?? false;
}

export function WarmupCard({ upstream }: { upstream: Upstream }) {
  const updateSettings = useUpdateUpstreamWarmupSettings();
  const fireWarmup = useFireNowUpstreamWarmup();
  const registry = usePluginRegistry();

  const [staleRevisionVisible, setStaleRevisionVisible] = useState(false);
  const [leasePanel, setLeasePanel] = useState<{ heldBy: string } | null>(null);
  const [errorPanel, setErrorPanel] = useState<{
    reason: FireErrorReason;
  } | null>(null);
  const [fireCooldown, setFireCooldown] = useState(false);

  const [confirmFireOpen, setConfirmFireOpen] = useState(false);
  const [confirmClearPluginOpen, setConfirmClearPluginOpen] = useState(false);

  // Auto-dismiss lease panel
  useEffect(() => {
    if (!leasePanel) return;
    const timer = setTimeout(
      () => setLeasePanel(null),
      LEASE_PANEL_AUTO_DISMISS_MS,
    );
    return () => clearTimeout(timer);
  }, [leasePanel]);

  if (upstream.kind !== 'anthropic_oauth') {
    return null;
  }

  const handleToggle = () => {
    updateSettings.mutate(
      {
        id: upstream.id,
        revision: upstream.revision,
        body: { warmup_enabled: !upstream.warmup_enabled },
      },
      {
        onSuccess: () => {
          setStaleRevisionVisible(false);
          toast.success(
            !upstream.warmup_enabled
              ? COPY.toggleEnabledSuccess
              : COPY.toggleDisabledSuccess,
            { duration: TOAST_DURATIONS.success },
          );
        },
        // biome-ignore lint/suspicious/noExplicitAny: error type
        onError: (err: any) => {
          if (err?.status === 412) {
            setStaleRevisionVisible(true);
          } else {
            toast.error(err?.message || 'Failed to update warmup settings');
          }
        },
      },
    );
  };

  const handlePluginChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const val = e.target.value;
    if (!val) return;

    updateSettings.mutate(
      {
        id: upstream.id,
        revision: upstream.revision,
        body: {
          warmup_dialect_plugin: {
            wasm_registry_id: val,
            config: {},
          },
        },
      },
      {
        onSuccess: () => {
          setStaleRevisionVisible(false);
          toast.success(COPY.dialectPluginSaveSuccess, {
            duration: TOAST_DURATIONS.success,
          });
        },
        // biome-ignore lint/suspicious/noExplicitAny: error type
        onError: (err: any) => {
          if (err?.status === 412) {
            setStaleRevisionVisible(true);
          } else {
            toast.error(err?.message || 'Failed to update dialect plugin');
          }
        },
      },
    );
  };

  const handleClearPlugin = () => {
    updateSettings.mutate(
      {
        id: upstream.id,
        revision: upstream.revision,
        body: { warmup_dialect_plugin: null },
      },
      {
        onSuccess: () => {
          setStaleRevisionVisible(false);
          setConfirmClearPluginOpen(false);
          toast.success(COPY.dialectPluginClearSuccess, {
            duration: TOAST_DURATIONS.success,
          });
        },
        // biome-ignore lint/suspicious/noExplicitAny: error type
        onError: (err: any) => {
          if (err?.status === 412) {
            setStaleRevisionVisible(true);
            setConfirmClearPluginOpen(false);
          } else {
            toast.error(err?.message || 'Failed to clear dialect plugin');
          }
        },
      },
    );
  };

  const handleFireNow = () => {
    setLeasePanel(null);
    setErrorPanel(null);
    fireWarmup.mutate(upstream.id, {
      // biome-ignore lint/suspicious/noExplicitAny: response type
      onSuccess: (res: any) => {
        setConfirmFireOpen(false);
        if (res.fired) {
          toast.success(COPY.fireSuccess, {
            duration: TOAST_DURATIONS.success,
          });
          setFireCooldown(true);
          setTimeout(() => setFireCooldown(false), FIRE_NOW_COOLDOWN_MS);
        } else if (res.reason === 'lease_held') {
          setLeasePanel({ heldBy: res.held_by });
        } else {
          setErrorPanel({ reason: res.reason });
        }
      },
      // biome-ignore lint/suspicious/noExplicitAny: error type
      onError: (err: any) => {
        toast.error(err?.message || 'Failed to fire warmup');
      },
    });
  };

  // biome-ignore lint/suspicious/noExplicitAny: plugin type
  const shapePlugins =
    registry.data?.entries.filter((p: any) => pluginSupportsSlot(p, 'shape')) ??
    [];

  return (
    <Card data-testid="warmup-card" className="space-y-4">
      {staleRevisionVisible && (
        <div
          role="status"
          aria-live="polite"
          data-testid="warmup-stale-hint"
          className="text-xs text-amber-200 bg-amber-500/10 border border-amber-500/30 rounded p-2 mx-4 mt-3"
        >
          {COPY.staleRevisionHint}
        </div>
      )}
      <CardHeader
        title={
          <div className="flex items-center gap-2">
            <Zap className="w-4 h-4 text-amber-400" />
            <span className="text-sm font-medium">{COPY.cardTitle}</span>
          </div>
        }
        action={
          <button
            type="button"
            role="switch"
            aria-checked={upstream.warmup_enabled}
            disabled={updateSettings.isPending}
            onClick={handleToggle}
            className="group inline-flex items-center gap-2 h-7 px-2 rounded-sm transition-colors focus:outline-none focus:ring-2 focus:ring-accent/40 disabled:opacity-50 disabled:cursor-not-allowed hover:bg-overlay-3"
          >
            <div
              className={cx(
                'relative inline-flex h-4 w-8 shrink-0 items-center rounded-full transition-colors duration-200 ease-in-out border',
                upstream.warmup_enabled
                  ? 'bg-emerald-500 border-emerald-500'
                  : 'bg-overlay-5 border-subtle-strong group-hover:border-text-muted',
              )}
            >
              <span
                className={cx(
                  'pointer-events-none inline-block h-3 w-3 transform rounded-full bg-white shadow-sm transition-transform duration-200 ease-in-out',
                  upstream.warmup_enabled ? 'translate-x-4' : 'translate-x-0.5',
                )}
              />
            </div>
            <span
              className={cx(
                'text-[11px] font-mono uppercase tracking-wider',
                upstream.warmup_enabled
                  ? 'text-emerald-400'
                  : 'text-text-muted group-hover:text-text',
              )}
            >
              {upstream.warmup_enabled ? 'Enabled' : 'Disabled'}
            </span>
          </button>
        }
      />
      <CardBody>
        {!upstream.warmup_enabled ? (
          <EmptyState
            title={COPY.disabledEmpty}
            action={
              <Button
                variant="primary"
                onClick={handleToggle}
                disabled={updateSettings.isPending}
              >
                {COPY.enableButtonLabel}
              </Button>
            }
          />
        ) : (
          <div className="space-y-6">
            <div className="grid grid-cols-2 gap-4">
              <div>
                <div className="text-xs text-text-muted mb-1">
                  {COPY.nextWarmupLabel}
                </div>
                <div className="text-sm">
                  {upstream.next_warmup_at ? (
                    <ResetCountdown ts={new Date(upstream.next_warmup_at)} />
                  ) : (
                    <span className="text-text-muted">{COPY.nextNull}</span>
                  )}
                </div>
              </div>
              <div>
                <div className="text-xs text-text-muted mb-1">
                  {COPY.lastCycleLabel}
                </div>
                <div className="text-sm">
                  {upstream.last_warmup_cycle_key ? (
                    <RelativeTime
                      ts={formatRelativeUnixSeconds(
                        upstream.last_warmup_cycle_key,
                      )}
                    />
                  ) : (
                    <span className="text-text-muted">{COPY.lastNull}</span>
                  )}
                </div>
              </div>
            </div>

            <div className="space-y-2">
              <label
                htmlFor="dialect-plugin-select"
                className="block text-xs text-text-muted"
              >
                {COPY.dialectPluginLabel}
              </label>
              <div className="flex items-center gap-2">
                <select
                  id="dialect-plugin-select"
                  className={INPUT_CLASS}
                  value={upstream.warmup_dialect_plugin?.wasm_registry_id ?? ''}
                  onChange={handlePluginChange}
                  disabled={updateSettings.isPending}
                >
                  <option value="">{COPY.defaultPluginOption}</option>
                  {/* biome-ignore lint/suspicious/noExplicitAny: plugin type */}
                  {shapePlugins.map((p: any) => (
                    <option key={p.id} value={p.id}>
                      {p.name}
                    </option>
                  ))}
                </select>
                {upstream.warmup_dialect_plugin !== null && (
                  <Button
                    variant="danger"
                    size="sm"
                    onClick={() => setConfirmClearPluginOpen(true)}
                    disabled={updateSettings.isPending}
                  >
                    {COPY.clearPluginButtonLabel}
                  </Button>
                )}
              </div>
              {shapePlugins.length === 0 && (
                <div className="text-xs text-text-muted mt-1">
                  {COPY.noShapePluginsAvailable}
                </div>
              )}
            </div>
          </div>
        )}
      </CardBody>

      {upstream.warmup_enabled && (
        <div className="px-4 pb-4 pt-2 border-t border-overlay-5 flex flex-col gap-3">
          <div>
            <Button
              variant="primary"
              onClick={() => setConfirmFireOpen(true)}
              disabled={fireWarmup.isPending || fireCooldown}
            >
              ⚡ {COPY.fireNowButtonLabel}
            </Button>
          </div>

          {leasePanel && (
            <div
              data-testid="warmup-lease-panel"
              role="status"
              aria-live="polite"
              className="text-xs text-amber-200 bg-amber-500/10 border border-amber-500/30 rounded p-2"
            >
              {COPY.leaseHeldTemplate.replace(
                '{heldBy}',
                leasePanel.heldBy.substring(0, 8),
              )}
            </div>
          )}

          {errorPanel && (
            <div
              data-testid="warmup-error-panel"
              role="alert"
              aria-live="polite"
              className="text-xs text-red-200 bg-red-500/10 border border-red-500/30 rounded p-2 flex justify-between items-start"
            >
              <span>{COPY.fireErrorReasons[errorPanel.reason]}</span>
              <button
                type="button"
                onClick={() => setErrorPanel(null)}
                className="text-red-400 hover:text-red-200 ml-2"
              >
                ×
              </button>
            </div>
          )}
        </div>
      )}

      <ConfirmDialog
        open={confirmFireOpen}
        onOpenChange={setConfirmFireOpen}
        title={COPY.confirmFireTitle}
        description={COPY.confirmFireBody.replace(
          '{upstreamName}',
          upstream.name,
        )}
        confirmLabel={COPY.confirmFireConfirmLabel}
        cancelLabel={COPY.confirmFireCancelLabel}
        onConfirm={handleFireNow}
        confirmDisabled={fireWarmup.isPending}
      />

      <ConfirmDialog
        open={confirmClearPluginOpen}
        onOpenChange={setConfirmClearPluginOpen}
        title={COPY.confirmClearPluginTitle}
        description={COPY.confirmClearPluginBody}
        confirmLabel={COPY.confirmClearPluginConfirmLabel}
        onConfirm={handleClearPlugin}
        confirmDisabled={updateSettings.isPending}
        destructive={true}
      />
    </Card>
  );
}
