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
  type FireNowResponse,
  type PluginEntry,
  type Upstream,
  useClearUpstreamWarmupDialectPlugin,
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
import { RelativeTime } from '../ui/RelativeTime';
import { NextWarmupDisplay } from './NextWarmupDisplay';

function pluginSupportsSlot(
  p: { supported_slots?: string[]; slot?: string },
  slot: string,
): boolean {
  return p.supported_slots?.includes(slot) ?? p.slot === slot;
}

function LastCycleDisplay({ cycleKeyUnixSecs }: { cycleKeyUnixSecs: number }) {
  return <RelativeTime ts={formatRelativeUnixSeconds(cycleKeyUnixSecs)} />;
}

function isStaleRevisionError(err: unknown): boolean {
  const maybe = err as {
    status?: number;
    code?: string | null;
    body?: unknown;
  };
  if (maybe.status === 412) return true;
  if (maybe.status !== 409) return false;
  if (maybe.code === 'stale_revision') return true;
  const body = maybe.body;
  return (
    typeof body === 'object' &&
    body !== null &&
    'error' in body &&
    (body as { error?: unknown }).error === 'stale_revision'
  );
}

function errorMessage(err: unknown, fallback: string): string {
  const maybe = err as {
    body?: { error?: string; detail?: string };
    message?: string;
  };
  if (maybe.body?.detail) return maybe.body.detail;
  if (maybe.body?.error) return maybe.body.error;
  if (maybe.message) return maybe.message;
  return fallback;
}

function currentRevisionFromError(err: unknown): number | null {
  const body = (err as { body?: unknown }).body;
  if (typeof body !== 'object' || body === null) return null;
  const current = Number(
    (body as { current_revision?: unknown }).current_revision,
  );
  return Number.isFinite(current) ? current : null;
}

export function WarmupCard({ upstream }: { upstream: Upstream }) {
  if (upstream.kind !== 'anthropic_oauth') {
    return null;
  }

  const stateKey = `${upstream.id}:${upstream.warmup_dialect_plugin?.wasm_registry_id ?? ''}`;
  return <WarmupCardInner key={stateKey} upstream={upstream} />;
}

function WarmupCardInner({ upstream }: { upstream: Upstream }) {
  const updateSettings = useUpdateUpstreamWarmupSettings();
  const clearPlugin = useClearUpstreamWarmupDialectPlugin();
  const fireWarmup = useFireNowUpstreamWarmup();
  const registry = usePluginRegistry();

  const [staleRevisionVisible, setStaleRevisionVisible] = useState(false);
  const [revisionOverride, setRevisionOverride] = useState<number | null>(null);
  const [pendingPluginValue, setPendingPluginValue] = useState<string | null>(
    null,
  );
  const [leasePanel, setLeasePanel] = useState<{ heldBy: string } | null>(null);
  const [errorPanel, setErrorPanel] = useState<{
    reason: FireErrorReason;
  } | null>(null);
  const [fireCooldown, setFireCooldown] = useState(false);

  const [confirmFireOpen, setConfirmFireOpen] = useState(false);
  const [confirmClearPluginOpen, setConfirmClearPluginOpen] = useState(false);

  const storedPluginId = upstream.warmup_dialect_plugin?.wasm_registry_id ?? '';
  const selectedPluginValue = pendingPluginValue ?? storedPluginId;

  // Auto-dismiss lease panel
  useEffect(() => {
    if (!leasePanel) return;
    const timer = setTimeout(
      () => setLeasePanel(null),
      LEASE_PANEL_AUTO_DISMISS_MS,
    );
    return () => clearTimeout(timer);
  }, [leasePanel]);

  // Reset revision-bound flags when the server advances past us.
  // pendingPluginValue is intentionally excluded — the mutation's onSuccess
  // owns it and clearing here would clobber in-flight optimistic state.
  // biome-ignore lint/correctness/useExhaustiveDependencies: revision is the trigger, not a read
  useEffect(() => {
    setStaleRevisionVisible(false);
    setRevisionOverride(null);
  }, [upstream.spec_revision]);

  const handleToggle = () => {
    updateSettings.mutate(
      {
        id: upstream.id,
        spec_revision: revisionOverride ?? upstream.spec_revision,
        body: { warmup_enabled: !upstream.warmup_enabled },
      },
      {
        onSuccess: () => {
          setStaleRevisionVisible(false);
          setRevisionOverride(null);
          toast.success(
            !upstream.warmup_enabled
              ? COPY.toggleEnabledSuccess
              : COPY.toggleDisabledSuccess,
            { duration: TOAST_DURATIONS.success },
          );
        },
        onError: (err: unknown) => {
          if (isStaleRevisionError(err)) {
            setStaleRevisionVisible(true);
            setRevisionOverride(currentRevisionFromError(err));
          } else {
            toast.error(errorMessage(err, 'Failed to update warmup settings'));
          }
        },
      },
    );
  };

  const handlePluginChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const val = e.target.value;
    if (!val) {
      if (upstream.warmup_dialect_plugin) {
        setConfirmClearPluginOpen(true);
      }
      return;
    }
    setPendingPluginValue(val);

    updateSettings.mutate(
      {
        id: upstream.id,
        spec_revision: revisionOverride ?? upstream.spec_revision,
        body: {
          warmup_dialect_plugin: {
            wasm_registry_id: val,
            config: {},
          },
        },
      },
      {
        onSuccess: () => {
          setPendingPluginValue(null);
          setStaleRevisionVisible(false);
          setRevisionOverride(null);
          toast.success(COPY.dialectPluginSaveSuccess, {
            duration: TOAST_DURATIONS.success,
          });
        },
        onError: (err: unknown) => {
          if (isStaleRevisionError(err)) {
            setStaleRevisionVisible(true);
            setRevisionOverride(currentRevisionFromError(err));
          } else {
            setPendingPluginValue(null);
            toast.error(errorMessage(err, 'Failed to update shape plugin'));
          }
        },
      },
    );
  };

  const handleClearPlugin = () => {
    clearPlugin.mutate(
      {
        id: upstream.id,
        spec_revision: revisionOverride ?? upstream.spec_revision,
      },
      {
        onSuccess: () => {
          setPendingPluginValue(null);
          setStaleRevisionVisible(false);
          setRevisionOverride(null);
          setConfirmClearPluginOpen(false);
          toast.success(COPY.dialectPluginClearSuccess, {
            duration: TOAST_DURATIONS.success,
          });
        },
        onError: (err: unknown) => {
          if (isStaleRevisionError(err)) {
            setStaleRevisionVisible(true);
            setRevisionOverride(currentRevisionFromError(err));
            setConfirmClearPluginOpen(false);
          } else {
            toast.error(errorMessage(err, 'Failed to clear dialect plugin'));
          }
        },
      },
    );
  };

  const handleFireNow = () => {
    setLeasePanel(null);
    setErrorPanel(null);
    fireWarmup.mutate(upstream.id, {
      onSuccess: (res: FireNowResponse) => {
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
      onError: (err: unknown) => {
        toast.error(errorMessage(err, 'Failed to fire warmup'));
      },
    });
  };

  const handleConfirmFireOpenChange = (open: boolean) => {
    setConfirmFireOpen(open);
    if (!open) {
      window.setTimeout(
        () =>
          document
            .querySelector<HTMLButtonElement>('[data-testid="warmup-fire-now"]')
            ?.focus(),
        0,
      );
    }
  };

  const shapePlugins =
    registry.data?.entries.filter((p: PluginEntry) =>
      pluginSupportsSlot(p, 'shape'),
    ) ?? [];
  const selectedPluginKnown =
    !selectedPluginValue ||
    shapePlugins.some((p) => p.id === selectedPluginValue);
  const showPluginSelect =
    shapePlugins.length > 0 || upstream.warmup_dialect_plugin !== null;
  const settingsPending = updateSettings.isPending || clearPlugin.isPending;

  return (
    <Card data-testid="warmup-card" tabIndex={-1}>
      <CardHeader
        title={COPY.cardTitle}
        subtitle={COPY.cardSubtitle}
        action={
          <div className="flex items-center gap-3">
            {upstream.enabled && upstream.warmup_enabled && (
              <Button
                variant="secondary"
                size="sm"
                data-testid="warmup-fire-now"
                onClick={() => setConfirmFireOpen(true)}
                disabled={fireWarmup.isPending || fireCooldown}
              >
                ⚡ {COPY.fireNowButtonLabel}
              </Button>
            )}
            <button
              type="button"
              role="switch"
              data-testid="warmup-switch"
              aria-checked={upstream.warmup_enabled}
              disabled={settingsPending}
              onClick={handleToggle}
              className={cx(
                'relative inline-flex h-5 w-9 shrink-0 items-center rounded-full transition-colors duration-200 ease-in-out border focus:outline-none focus:ring-2 focus:ring-accent/40 disabled:opacity-50 disabled:cursor-not-allowed',
                upstream.warmup_enabled
                  ? 'bg-emerald-500 border-emerald-500'
                  : 'bg-overlay-5 border-subtle-strong hover:border-text-muted',
              )}
            >
              <span
                className={cx(
                  'pointer-events-none inline-block h-4 w-4 transform rounded-full bg-white shadow-sm transition-transform duration-200 ease-in-out',
                  upstream.warmup_enabled ? 'translate-x-4' : 'translate-x-0.5',
                )}
              />
            </button>
          </div>
        }
      />
      <CardBody className="flex flex-col gap-4">
        {staleRevisionVisible && (
          <div
            role="status"
            aria-live="polite"
            data-testid="warmup-stale-hint"
            className="text-xs text-amber-200 bg-amber-500/10 border border-amber-500/30 rounded p-2"
          >
            {COPY.staleRevisionHint}
          </div>
        )}

        {!upstream.enabled ? (
          <EmptyState title={COPY.upstreamPausedEmpty} />
        ) : !upstream.warmup_enabled ? (
          <EmptyState
            title={COPY.disabledEmpty}
            action={
              <Button
                variant="primary"
                data-testid="warmup-enable-btn"
                onClick={handleToggle}
                disabled={settingsPending}
              >
                {COPY.enableButtonLabel}
              </Button>
            }
          />
        ) : (
          <div className="flex flex-col gap-6">
            <div className="grid grid-cols-1 gap-4 text-sm sm:grid-cols-2">
              <div>
                <div className="text-xs text-text-muted mb-1">
                  {COPY.nextWarmupLabel}
                </div>
                <div className="text-text" data-testid="warmup-next">
                  {upstream.status.next_warmup_at ? (
                    <NextWarmupDisplay value={upstream.status.next_warmup_at} />
                  ) : (
                    <span className="text-text-muted">{COPY.nextNull}</span>
                  )}
                </div>
              </div>

              <div>
                <div className="text-xs text-text-muted mb-1">
                  {COPY.lastCycleLabel}
                </div>
                <div className="text-text" data-testid="warmup-last">
                  {upstream.status.last_warmup_cycle_key ? (
                    <LastCycleDisplay
                      cycleKeyUnixSecs={upstream.status.last_warmup_cycle_key}
                    />
                  ) : (
                    <span className="text-text-muted">{COPY.lastNull}</span>
                  )}
                </div>
              </div>
            </div>

            <div className="space-y-2">
              {showPluginSelect && (
                <>
                  <label
                    htmlFor="dialect-plugin-select"
                    className="block text-xs text-text-muted"
                  >
                    {COPY.dialectPluginLabel}
                  </label>
                  <select
                    id="dialect-plugin-select"
                    data-testid="warmup-plugin-select"
                    className={INPUT_CLASS}
                    value={selectedPluginValue}
                    onChange={handlePluginChange}
                    disabled={settingsPending}
                  >
                    <option value="">{COPY.defaultPluginOption}</option>
                    {selectedPluginValue && !selectedPluginKnown && (
                      <option value={selectedPluginValue}>
                        {COPY.unknownPluginTemplate.replace(
                          '{id}',
                          selectedPluginValue,
                        )}
                      </option>
                    )}
                    {shapePlugins.map((p: PluginEntry) => (
                      <option key={p.id} value={p.id}>
                        {p.name}
                      </option>
                    ))}
                  </select>
                </>
              )}
              {shapePlugins.length === 0 && (
                <div className="text-xs text-text-muted mt-1">
                  <span>{COPY.noShapePluginsAvailable}</span>{' '}
                  <a href="/plugins" className="text-accent hover:underline">
                    Plugins
                  </a>
                </div>
              )}
            </div>

            {(leasePanel || errorPanel) && (
              <div className="flex flex-col gap-2">
                {leasePanel && (
                  <div
                    data-testid="warmup-lease-panel"
                    role="status"
                    aria-live="polite"
                    className="text-xs text-amber-200 bg-amber-500/10 border border-amber-500/30 rounded p-2"
                  >
                    {COPY.leaseHeldTemplate.replace(
                      '{heldBy}',
                      leasePanel.heldBy,
                    )}
                  </div>
                )}
                {errorPanel && (
                  <div
                    data-testid="warmup-error-panel"
                    data-reason={errorPanel.reason}
                    role="alert"
                    aria-live="polite"
                    className="text-xs text-red-200 bg-red-500/10 border border-red-500/30 rounded p-2"
                  >
                    {COPY.fireErrorReasons[errorPanel.reason]}
                  </div>
                )}
              </div>
            )}
          </div>
        )}
      </CardBody>

      <ConfirmDialog
        open={confirmFireOpen}
        onOpenChange={handleConfirmFireOpenChange}
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
        confirmDisabled={clearPlugin.isPending}
        destructive={true}
      />
    </Card>
  );
}
