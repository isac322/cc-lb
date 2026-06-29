import { Switch as BaseSwitch } from '@base-ui/react/switch';
import { HelpCircle, History, Zap } from 'lucide-react';
import { useEffect, useState } from 'react';
import { toast } from 'sonner';
import {
  COPY,
  FIRE_NOW_COOLDOWN_MS,
  type FireErrorReason,
  LEASE_PANEL_AUTO_DISMISS_MS,
  TOAST_DURATIONS,
} from '../../../lib/copy/warmup';
import {
  type FireNowResponse,
  type PluginEntry,
  type Upstream,
  useClearUpstreamWarmupDialectPlugin,
  useFireNowUpstreamWarmup,
  usePluginRegistry,
  useUpdateUpstreamWarmupSettings,
  useWarmupSummary,
} from '../../../lib/queries';
import {
  Badge,
  Button,
  Card,
  CardBody,
  CardHeader,
  ConfirmDialog,
  cx,
  Hint,
  StatusBadge,
} from '../../ui/primitives';
import { RelativeTime } from '../../ui/RelativeTime';
import { REASON_LABEL } from './parts/copy';
import { WarmupConfigModal } from './parts/WarmupConfigModal';
import { detectActiveIncident } from './parts/warmupViewModel';
import { WarmupHistoryDrawer } from './WarmupHistoryDrawer';

const LAST_OUTCOME_LABEL = {
  success_fresh: 'Success',
  success_redundant: 'Success',
  transient_failure: 'Retrying',
  permanent_failure: 'Failed',
  skipped: 'Skipped',
} as const;

const LAST_OUTCOME_TONE = {
  success_fresh: 'ok',
  success_redundant: 'ok',
  transient_failure: 'warn',
  permanent_failure: 'danger',
  skipped: 'neutral',
} as const;

function pluginSupportsSlot(
  p: { supported_slots?: string[]; slot?: string },
  slot: string,
): boolean {
  return p.supported_slots?.includes(slot) ?? p.slot === slot;
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

function HelpIcon({
  label,
  text,
}: {
  readonly label: string;
  readonly text: string;
}) {
  return (
    <Hint
      label={
        <span className="block max-w-72 whitespace-normal leading-5">
          {text}
        </span>
      }
      side="top"
    >
      <span
        aria-label={label}
        className="inline-flex h-4 w-4 cursor-help items-center justify-center text-text-faint transition-colors hover:text-text"
      >
        <HelpCircle className="h-3.5 w-3.5" />
      </span>
    </Hint>
  );
}

function WarmupHelpHover() {
  return (
    <HelpIcon
      label="Warm-up help"
      text="Warm-up sends a tiny background Anthropic request when idle time would otherwise stall the next 5h window. Returning users land in an active window instead of starting one late."
    />
  );
}

function ShapePluginHelpHover() {
  return (
    <HelpIcon
      label="Shape plugin help"
      text="Warm-up is also an Anthropic request. Shape plugin applies the same request transform used for proxied traffic to that small warm-up prompt."
    />
  );
}

export function WarmupCardMinimal({ upstream }: { upstream: Upstream }) {
  if (upstream.kind !== 'anthropic_oauth') {
    return null;
  }
  const stateKey = `${upstream.id}:${
    upstream.warmup_dialect_plugin?.wasm_registry_id ?? ''
  }`;
  return <WarmupCardMinimalInner key={stateKey} upstream={upstream} />;
}

function WarmupCardMinimalInner({ upstream }: { upstream: Upstream }) {
  const updateSettings = useUpdateUpstreamWarmupSettings();
  const clearPlugin = useClearUpstreamWarmupDialectPlugin();
  const fireWarmup = useFireNowUpstreamWarmup();
  const registry = usePluginRegistry();
  const summaryQ = useWarmupSummary(upstream.id);
  const summary = summaryQ.data;
  const incident = detectActiveIncident(summary);

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
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [configOpen, setConfigOpen] = useState(false);

  const [confirmFireOpen, setConfirmFireOpen] = useState(false);
  const [confirmClearPluginOpen, setConfirmClearPluginOpen] = useState(false);

  const storedPluginId = upstream.warmup_dialect_plugin?.wasm_registry_id ?? '';
  const selectedPluginValue = pendingPluginValue ?? storedPluginId;

  useEffect(() => {
    if (!leasePanel) return;
    const timer = setTimeout(
      () => setLeasePanel(null),
      LEASE_PANEL_AUTO_DISMISS_MS,
    );
    return () => clearTimeout(timer);
  }, [leasePanel]);

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

  const openHistory = () => {
    setDrawerOpen(true);
  };

  const shapePlugins =
    registry.data?.entries.filter((p: PluginEntry) =>
      pluginSupportsSlot(p, 'shape'),
    ) ?? [];
  const selectedPluginKnown =
    !selectedPluginValue ||
    shapePlugins.some((p) => p.id === selectedPluginValue);
  const settingsPending = updateSettings.isPending || clearPlugin.isPending;

  const pluginSnapshot =
    summary?.dialect_plugin ??
    (upstream.warmup_dialect_plugin
      ? {
          wasm_registry_id: upstream.warmup_dialect_plugin.wasm_registry_id,
          wire_version: upstream.warmup_dialect_plugin.wire_version,
          config: upstream.warmup_dialect_plugin.config ?? {},
        }
      : null);

  let statusTone: 'ok' | 'warn' | 'danger' | 'neutral' = 'neutral';
  let statusLabel = 'Paused';

  if (upstream.warmup_enabled) {
    const last = summary?.last_attempt;
    const skippedBecauseAlreadyActive =
      last?.outcome === 'skipped' && last?.reason === 'window_already_active';

    if (incident) {
      statusTone = 'danger';
      statusLabel = 'Down';
    } else if (
      last?.outcome === 'success_fresh' ||
      last?.outcome === 'success_redundant' ||
      skippedBecauseAlreadyActive
    ) {
      statusTone = 'ok';
      statusLabel = 'Healthy';
    } else if (
      last?.outcome === 'transient_failure' ||
      last?.outcome === 'permanent_failure' ||
      last?.outcome === 'skipped'
    ) {
      statusTone = 'warn';
      statusLabel = 'Degraded';
    } else {
      statusTone = 'neutral';
      statusLabel = 'Pending';
    }
  }

  const headerTitle = (
    <div className="flex items-center gap-2">
      <StatusBadge tone={statusTone} label={statusLabel} />
      <span>Warm-up</span>
      <WarmupHelpHover />
    </div>
  );

  const headerActions = (
    <div className="flex items-center">
      <BaseSwitch.Root
        aria-label="Toggle warmup"
        checked={upstream.warmup_enabled}
        className={cx(
          'relative inline-flex h-5 w-9 shrink-0 items-center self-center rounded-full border transition-colors duration-200 ease-in-out focus:outline-none focus:ring-2 focus:ring-accent/40 disabled:opacity-50 disabled:cursor-not-allowed',
          upstream.warmup_enabled
            ? 'bg-[color:var(--color-ok)] border-[color:var(--color-ok)]'
            : 'bg-overlay-5 border-subtle-strong hover:border-text-muted',
        )}
        data-testid="warmup-switch"
        disabled={settingsPending}
        nativeButton
        onCheckedChange={handleToggle}
        render={<button type="button" />}
      >
        <BaseSwitch.Thumb
          className={cx(
            'pointer-events-none inline-block h-4 w-4 rounded-full bg-white shadow-sm transition-transform duration-200 ease-in-out',
            upstream.warmup_enabled ? 'translate-x-4' : 'translate-x-0.5',
          )}
        />
      </BaseSwitch.Root>
    </div>
  );

  const lastAttempt = summary?.last_attempt;

  return (
    <>
      <Card
        data-testid="warmup-card"
        data-variant="minimal"
        tabIndex={-1}
        className="w-full h-full flex flex-col"
      >
        <CardHeader
          title={headerTitle}
          subtitle="Starts the next 5h window during idle gaps."
          action={headerActions}
          align="center"
        />
        <CardBody className="space-y-4 flex-1 flex flex-col">
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

          {!upstream.warmup_enabled && (
            <div className="flex items-center justify-between bg-overlay-2 border border-subtle rounded p-3">
              <span className="text-sm text-text-muted">
                Warmup disabled — re-enable to schedule new attempts
              </span>
              <Button
                variant="primary"
                size="sm"
                data-testid="warmup-enable-btn"
                onClick={handleToggle}
                disabled={settingsPending}
              >
                {COPY.enableButtonLabel}
              </Button>
            </div>
          )}

          <div className="flex-1 flex flex-col gap-4">
            <div className="grid grid-cols-1 gap-4 border-b border-subtle pb-3 sm:grid-cols-2">
              <div className="flex flex-col gap-1">
                <span className="text-[11px] uppercase tracking-wider text-text-faint">
                  Next run
                </span>
                <div className="text-sm font-medium text-text">
                  {summary?.next_scheduled_at_unix_secs ? (
                    <RelativeTime
                      compact
                      ts={summary.next_scheduled_at_unix_secs * 1000}
                    />
                  ) : (
                    <span className="text-text-muted">Not scheduled</span>
                  )}
                </div>
              </div>

              <div className="flex flex-col gap-1">
                <span className="inline-flex items-center gap-1 text-[11px] uppercase tracking-wider text-text-faint">
                  Last run
                </span>
                <div
                  className="text-sm font-medium text-text"
                  data-testid="warmup-last"
                >
                  {lastAttempt ? (
                    <button
                      type="button"
                      onClick={openHistory}
                      className="rounded-sm text-left transition-colors hover:bg-overlay-2 focus-visible:outline focus-visible:outline-2 focus-visible:outline-accent"
                      aria-label="Open last warm-up attempt detail"
                    >
                      <div className="flex flex-wrap items-center gap-2 text-sm font-medium text-text">
                        <Badge tone={LAST_OUTCOME_TONE[lastAttempt.outcome]}>
                          {LAST_OUTCOME_LABEL[lastAttempt.outcome]}
                        </Badge>
                        <RelativeTime
                          compact
                          ts={lastAttempt.attempted_at_unix_secs * 1000}
                        />
                      </div>
                    </button>
                  ) : upstream.status.last_warmup_at_unix_secs ? (
                    <RelativeTime
                      compact
                      ts={upstream.status.last_warmup_at_unix_secs * 1000}
                    />
                  ) : (
                    <span className="text-text-muted">Never</span>
                  )}
                </div>
              </div>
            </div>

            <div className="flex items-center justify-between gap-3">
              <span className="inline-flex items-center gap-1.5 text-sm text-text-muted">
                Shape plugin
                <ShapePluginHelpHover />
              </span>
              {shapePlugins.length === 0 && !upstream.warmup_dialect_plugin ? (
                <div className="text-xs text-text-muted">
                  <span>{COPY.noShapePluginsAvailable}</span>{' '}
                  <a href="/plugins" className="text-accent hover:underline">
                    Plugins
                  </a>
                </div>
              ) : (
                <label className="inline-flex items-center gap-2 rounded-sm border border-subtle bg-overlay-2 px-2 py-1 text-xs text-text-muted">
                  <select
                    id="dialect-plugin-select"
                    data-testid="warmup-plugin-select"
                    className="max-w-[220px] bg-transparent font-mono text-text outline-none"
                    value={selectedPluginValue}
                    onChange={handlePluginChange}
                    disabled={settingsPending}
                    aria-label={COPY.dialectPluginLabel}
                    title={selectedPluginValue || COPY.defaultPluginOption}
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
                </label>
              )}
            </div>

            {lastAttempt?.error_detail && (
              <div
                className={cx(
                  'rounded-sm border p-2 text-xs leading-relaxed',
                  lastAttempt.outcome === 'permanent_failure'
                    ? 'border-[color:var(--color-danger)]/30 bg-red-500/10 text-[color:var(--color-danger)]'
                    : 'border-[color:var(--color-warn)]/30 bg-amber-500/10 text-[color:var(--color-warn)]',
                )}
              >
                {lastAttempt.reason
                  ? REASON_LABEL[lastAttempt.reason]
                  : 'Warm-up failed'}
                : {lastAttempt.error_detail}
              </div>
            )}

            {(leasePanel || errorPanel) && (
              <div className="flex flex-col gap-2 mt-2">
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

          <div className="grid grid-cols-2 gap-2 border-t border-subtle pt-3 mt-auto">
            <Button
              variant="secondary"
              size="sm"
              fullWidth
              data-testid="warmup-fire-now"
              onClick={() => setConfirmFireOpen(true)}
              disabled={
                fireWarmup.isPending || fireCooldown || !upstream.warmup_enabled
              }
              iconLeft={<Zap className="h-3 w-3" />}
            >
              {COPY.fireNowButtonLabel}
            </Button>
            <Button
              variant="ghost"
              size="sm"
              data-testid="warmup-history-button"
              onClick={() => openHistory()}
              iconLeft={<History className="h-3 w-3" />}
            >
              History
            </Button>
          </div>
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

      {pluginSnapshot && (
        <WarmupConfigModal
          open={configOpen}
          onOpenChange={setConfigOpen}
          plugin={pluginSnapshot}
        />
      )}

      <WarmupHistoryDrawer
        open={drawerOpen}
        onOpenChange={setDrawerOpen}
        upstream={upstream}
      />
    </>
  );
}
