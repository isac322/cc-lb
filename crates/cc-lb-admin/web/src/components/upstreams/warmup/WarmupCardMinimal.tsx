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
  Button,
  Card,
  CardBody,
  CardHeader,
  ConfirmDialog,
  cx,
  INPUT_CLASS,
  StatusBadge,
} from '../../ui/primitives';
import { RelativeTime, ResetCountdown } from '../../ui/RelativeTime';
import { REASON_LABEL } from './parts/copy';
import { WarmupConfigModal } from './parts/WarmupConfigModal';
import { WarmupOutcomeBadge } from './parts/WarmupOutcomeBadge';
import { WarmupRecentStrip } from './parts/WarmupRecentStrip';
import { detectActiveIncident, formatDuration } from './parts/warmupViewModel';
import { WarmupHistoryDrawer } from './WarmupHistoryDrawer';

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
    if (incident) {
      statusTone = 'danger';
      statusLabel = 'Down';
    } else if (
      summary?.last_attempt?.outcome === 'transient_failure' ||
      summary?.last_attempt?.outcome === 'permanent_failure' ||
      summary?.last_attempt?.outcome === 'skipped'
    ) {
      statusTone = 'warn';
      statusLabel = 'Degraded';
    } else if (
      summary?.last_attempt?.outcome === 'success_fresh' ||
      summary?.last_attempt?.outcome === 'success_redundant'
    ) {
      statusTone = 'ok';
      statusLabel = 'Healthy';
    } else {
      statusTone = 'neutral';
      statusLabel = 'Pending';
    }
  }

  const headerTitle = (
    <div className="flex items-center gap-2">
      <StatusBadge tone={statusTone} label={statusLabel} />
      <span>Warm-up</span>
    </div>
  );

  const headerActions = (
    <div className="flex items-center gap-2">
      <Button
        variant="ghost"
        size="sm"
        className="bg-overlay-2"
        data-testid="warmup-history-button"
        onClick={() => setDrawerOpen(true)}
      >
        History →
      </Button>
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
        aria-label="Toggle warmup"
        aria-checked={upstream.warmup_enabled}
        disabled={settingsPending || !upstream.enabled}
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
  );

  const lastAttempt = summary?.last_attempt;
  let supplementaryLine = '';

  const renderPrimaryLine = () => {
    if (!lastAttempt) return 'No warm-up attempts recorded yet.';
    const reasonLabel = lastAttempt.reason
      ? REASON_LABEL[lastAttempt.reason]
      : 'unknown reason';
    const timeNode = (
      <RelativeTime ts={lastAttempt.attempted_at_unix_secs * 1000} />
    );
    const httpStatus = lastAttempt.http_status
      ? `HTTP ${lastAttempt.http_status}`
      : '';
    const errorDetail = lastAttempt.error_detail
      ? lastAttempt.error_detail.substring(0, 80) +
        (lastAttempt.error_detail.length > 80 ? '...' : '')
      : '';
    const errorSuffix = [httpStatus, errorDetail].filter(Boolean).join(' · ');

    switch (lastAttempt.outcome) {
      case 'success_fresh':
        if (
          lastAttempt.idle_secs_since_prev_window != null &&
          lastAttempt.idle_secs_since_prev_window > 0
        ) {
          supplementaryLine = `Upstream had been idle for ${formatDuration(lastAttempt.idle_secs_since_prev_window)} before this warm-up.`;
        } else {
          supplementaryLine =
            'Triggered immediately after the previous 5h window ended.';
        }
        return <>Last attempt succeeded {timeNode}: opened a new 5h window.</>;
      case 'success_redundant':
        supplementaryLine =
          'No new cycle was started — the upstream was already primed.';
        return (
          <>Last attempt succeeded {timeNode}: 5h window was already active.</>
        );
      case 'transient_failure':
        supplementaryLine = `Scheduler will retry.${errorSuffix ? ` ${errorSuffix}` : ''}`;
        return (
          <>
            Last attempt failed transiently {timeNode}: {reasonLabel}.
          </>
        );
      case 'permanent_failure':
        supplementaryLine = `Operator action required.${errorSuffix ? ` ${errorSuffix}` : ''}`;
        return (
          <>
            Last attempt failed {timeNode}: {reasonLabel}.
          </>
        );
      case 'skipped':
        if (lastAttempt.reason === 'lease_held') {
          supplementaryLine = `Another replica (${lastAttempt.lease_holder ?? 'unknown'}) held the lease for this cycle.`;
        }
        return (
          <>
            Last attempt skipped {timeNode}: {reasonLabel}.
          </>
        );
    }
  };

  const primaryLineContent = renderPrimaryLine();

  const recent7d = summary?.recent_summary_7d;
  const successCount =
    (recent7d?.success_fresh ?? 0) + (recent7d?.success_redundant ?? 0);
  const failedCount =
    (recent7d?.transient_failure ?? 0) + (recent7d?.permanent_failure ?? 0);
  const skippedCount = recent7d?.skipped ?? 0;

  const configObj =
    pluginSnapshot?.config &&
    typeof pluginSnapshot.config === 'object' &&
    !Array.isArray(pluginSnapshot.config)
      ? (pluginSnapshot.config as Record<string, unknown>)
      : {};
  const configKeys = Object.keys(configObj);
  const configSummary =
    configKeys.length === 0
      ? '(no config)'
      : configKeys.map((k) => `${k}=${String(configObj[k])}`).join(' · ');

  return (
    <>
      <Card data-testid="warmup-card" data-variant="minimal" tabIndex={-1}>
        <CardHeader
          title={headerTitle}
          subtitle="Keeps this upstream's 5h window primed before traffic arrives"
          action={headerActions}
          className="flex-row items-start justify-between"
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

          {!upstream.enabled && (
            <div className="flex flex-col gap-1 bg-overlay-2 border border-subtle rounded p-3">
              <span className="text-sm font-medium text-text">
                {COPY.upstreamPausedEmpty}
              </span>
              <span className="text-sm text-text-muted">
                Upstream is paused — warm-up does not apply
              </span>
            </div>
          )}

          {upstream.enabled && !upstream.warmup_enabled && (
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

          <div
            className={cx(
              'flex flex-col gap-4',
              !upstream.enabled && 'opacity-60 pointer-events-none',
            )}
          >
            {/* Narrative Row */}
            <div className="flex items-start gap-2">
              {lastAttempt && (
                <div className="mt-0.5 shrink-0">
                  <WarmupOutcomeBadge outcome={lastAttempt.outcome} />
                </div>
              )}
              <div className="flex flex-col">
                <div className="text-sm text-text leading-tight">
                  {primaryLineContent}
                </div>
                {supplementaryLine && (
                  <div className="text-xs text-text-muted mt-1">
                    {supplementaryLine}
                  </div>
                )}
              </div>
            </div>

            {/* Recent Activity Strip */}
            <div className="flex flex-col gap-1.5">
              <div className="flex items-center justify-between">
                <span className="text-[11px] uppercase tracking-wider text-text-faint">
                  Recent attempts
                </span>
                <span className="text-[11px] text-text-faint">
                  Last 7d: {successCount} success · {failedCount} failed ·{' '}
                  {skippedCount} skipped
                </span>
              </div>
              <div className="flex items-center gap-2">
                <WarmupRecentStrip
                  attempts={summary?.recent_attempts}
                  onClick={() => setDrawerOpen(true)}
                />
                <span className="text-[11px] text-text-muted ml-1">
                  (click to view all)
                </span>
              </div>
            </div>

            {/* 3-Column Grid */}
            <div className="grid grid-cols-1 sm:grid-cols-[1fr_1fr_1.5fr] gap-4 border-t border-subtle pt-3 w-full">
              {/* Cell 1: Next Run */}
              <div className="flex flex-col gap-1">
                <span className="text-[11px] uppercase tracking-wider text-text-faint">
                  Next Run
                </span>
                <div className="text-sm text-text">
                  {summary?.next_scheduled_at_unix_secs ? (
                    <ResetCountdown
                      ts={summary.next_scheduled_at_unix_secs * 1000}
                    />
                  ) : (
                    <span className="text-text-muted">Not scheduled</span>
                  )}
                </div>
                <div className="text-[11px] text-text-faint">
                  Scheduled by cc-lb
                </div>
              </div>

              {/* Cell 2: Last Attempt */}
              <div className="flex flex-col gap-1">
                <span className="text-[11px] uppercase tracking-wider text-text-faint">
                  Last Attempt
                </span>
                <div className="text-sm text-text" data-testid="warmup-last">
                  {lastAttempt ? (
                    <RelativeTime
                      ts={lastAttempt.attempted_at_unix_secs * 1000}
                    />
                  ) : upstream.status.last_warmup_at_unix_secs ? (
                    <RelativeTime
                      ts={upstream.status.last_warmup_at_unix_secs * 1000}
                    />
                  ) : (
                    <span className="text-text-muted">Never</span>
                  )}
                </div>
                {lastAttempt && (
                  <div className="text-[11px] text-text-faint">
                    {lastAttempt.outcome === 'success_fresh'
                      ? 'Fresh window'
                      : lastAttempt.outcome === 'success_redundant'
                        ? 'Redundant window'
                        : lastAttempt.outcome === 'skipped'
                          ? 'Skipped'
                          : 'Failed'}{' '}
                    · {lastAttempt.trigger}
                  </div>
                )}
              </div>

              {/* Cell 3: Shape plugin */}
              <div className="flex flex-col gap-1">
                <span className="text-[11px] uppercase tracking-wider text-text-faint">
                  Shape plugin
                </span>
                {shapePlugins.length === 0 &&
                !upstream.warmup_dialect_plugin ? (
                  <div className="text-xs text-text-muted mt-1">
                    <span>{COPY.noShapePluginsAvailable}</span>{' '}
                    <a href="/plugins" className="text-accent hover:underline">
                      Plugins
                    </a>
                  </div>
                ) : (
                  <div className="flex items-center gap-2 min-w-0 w-full">
                    <select
                      id="dialect-plugin-select"
                      data-testid="warmup-plugin-select"
                      className={cx(
                        INPUT_CLASS,
                        'flex-1 min-w-0 text-ellipsis',
                      )}
                      value={selectedPluginValue}
                      onChange={handlePluginChange}
                      disabled={settingsPending || !upstream.enabled}
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
                    {upstream.warmup_dialect_plugin && (
                      <Button
                        variant="ghost"
                        size="sm"
                        data-testid="warmup-plugin-clear"
                        onClick={() => setConfirmClearPluginOpen(true)}
                        disabled={settingsPending || !upstream.enabled}
                      >
                        {COPY.clearPluginButtonLabel}
                      </Button>
                    )}
                  </div>
                )}
                {upstream.warmup_dialect_plugin && pluginSnapshot && (
                  <div className="flex flex-col gap-1 mt-1">
                    <div
                      className="text-[11px] font-mono text-text-faint truncate"
                      title={configSummary}
                    >
                      {configSummary}
                    </div>
                    <div className="flex items-center gap-2">
                      <span className="text-[11px] text-text-faint">
                        wire v{pluginSnapshot.wire_version}
                      </span>
                      <button
                        type="button"
                        className="text-[11px] text-text hover:underline shrink-0"
                        onClick={() => setConfigOpen(true)}
                      >
                        View full config
                      </button>
                    </div>
                  </div>
                )}
              </div>
            </div>
          </div>

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
