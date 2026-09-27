import { ChevronRight, HelpCircle, History, Zap } from 'lucide-react';
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
  Hint,
  Skeleton,
  StatusBadge,
  ToggleSwitch,
} from '../../ui/primitives';
import { RelativeTime } from '../../ui/RelativeTime';
import { Select } from '../../ui/Select';
import { REASON_LABEL } from './parts/copy';
import { WarmupConfigModal } from './parts/WarmupConfigModal';
import { detectActiveIncident } from './parts/warmupViewModel';
import { WarmupHistoryDrawer } from './WarmupHistoryDrawer';

const WARN_PANEL_CLASS =
  'rounded-sm bg-warn/8 px-3 py-2 text-body-sm text-warn-text';
const DANGER_PANEL_CLASS =
  'rounded-sm bg-danger/8 px-3 py-2 text-body-sm text-danger-text';

const LAST_OUTCOME_LABEL = {
  success: 'Success',
  transient_failure: 'Retrying',
  permanent_failure: 'Failed',
  skipped: 'Skipped',
} as const;

const LAST_OUTCOME_TONE = {
  success: 'ok',
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

// Failure reasons caused by the stored OAuth credential. When the detail view
// already shows the reconnect notice, the card points there instead of
// repeating the diagnosis.
const CREDENTIAL_FAILURE_REASONS: Record<string, true> = {
  credential_decrypt_failed: true,
  oauth_credentials_missing: true,
  oauth_refresh_failed: true,
  auth_failed: true,
};

type WarmupCardProps = {
  upstream: Upstream;
  /** True when the page already shows a reconnect notice for this upstream. */
  credentialNoticeShown?: boolean;
};

export function WarmupCardMinimal({
  upstream,
  credentialNoticeShown = false,
}: WarmupCardProps) {
  if (upstream.kind !== 'anthropic_oauth') {
    return null;
  }
  const stateKey = `${upstream.id}:${
    upstream.warmup_dialect_plugin?.wasm_registry_id ?? ''
  }`;
  return (
    <WarmupCardMinimalInner
      key={stateKey}
      upstream={upstream}
      credentialNoticeShown={credentialNoticeShown}
    />
  );
}

function WarmupCardMinimalInner({
  upstream,
  credentialNoticeShown,
}: Required<WarmupCardProps>) {
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

  const handlePluginChange = (val: string) => {
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
  const summaryPending = summaryQ.isPending;
  const pluginRegistryPending = registry.isPending;

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
    } else {
      const last = summary?.last_attempt;
      if (!last) {
        statusTone = 'neutral';
        statusLabel = 'Pending';
      } else {
        switch (last.status) {
          case 'success':
            statusTone = 'ok';
            statusLabel = 'Healthy';
            break;
          case 'skipped':
            statusTone = 'neutral';
            statusLabel = 'Idle';
            break;
          case 'transient_failure':
            statusTone = 'warn';
            statusLabel = 'Degraded';
            break;
          case 'permanent_failure':
            statusTone = 'danger';
            statusLabel = 'Down';
            break;
        }
      }
    }
  }

  const headerTitle = (
    <div className="flex items-center gap-2">
      <span>Warm-up</span>
      <WarmupHelpHover />
      <span
        className="ml-1 inline-flex min-h-5 w-24 items-center"
        data-testid="warmup-status-value"
      >
        {summaryPending && upstream.warmup_enabled ? (
          <Skeleton className="h-4 w-full rounded-sm" />
        ) : (
          <StatusBadge tone={statusTone} label={statusLabel} />
        )}
      </span>
    </div>
  );

  const headerActions = (
    <div className="flex items-center gap-2">
      <Button
        size="sm"
        data-testid="warmup-fire-now"
        onClick={() => setConfirmFireOpen(true)}
        disabled={
          fireWarmup.isPending || fireCooldown || !upstream.warmup_enabled
        }
        iconLeft={<Zap />}
      >
        {COPY.fireNowButtonLabel}
      </Button>
      <Button
        size="sm"
        data-testid="warmup-history-button"
        onClick={() => openHistory()}
        iconLeft={<History />}
      >
        History
      </Button>
      <ToggleSwitch
        variant="compact"
        role="switch"
        aria-label="Toggle warmup"
        checked={upstream.warmup_enabled}
        data-testid="warmup-switch"
        disabled={settingsPending}
        onChange={handleToggle}
        className="ml-1"
      />
    </div>
  );

  const lastAttempt = summary?.last_attempt;

  return (
    <>
      <Card
        data-testid="warmup-card"
        data-variant="minimal"
        tabIndex={-1}
        className="w-full h-full"
      >
        <CardHeader
          title={headerTitle}
          subtitle="Starts the next 5h window during idle gaps."
          action={headerActions}
          align="center"
        />
        <CardBody className="space-y-4">
          {staleRevisionVisible && (
            <div
              role="status"
              aria-live="polite"
              data-testid="warmup-stale-hint"
              className={WARN_PANEL_CLASS}
            >
              {COPY.staleRevisionHint}
            </div>
          )}

          {!upstream.warmup_enabled && (
            <div className="flex flex-wrap items-center justify-between gap-3 rounded-sm bg-overlay-2 px-3 py-2">
              <span className="text-body-sm text-text-muted">
                Warmup disabled — re-enable to schedule new attempts
              </span>
              <Button
                size="sm"
                data-testid="warmup-enable-btn"
                onClick={handleToggle}
                disabled={settingsPending}
              >
                {COPY.enableButtonLabel}
              </Button>
            </div>
          )}

          <div className="flex flex-col gap-4">
            <div className="grid grid-cols-1 gap-4 border-b border-subtle pb-4 sm:grid-cols-2">
              <div className="flex flex-col gap-1">
                <span className="text-label text-text-faint">Next run</span>
                <div
                  className="flex min-h-5 items-center text-body-sm text-text"
                  data-testid="warmup-next-value"
                >
                  {summaryPending ? (
                    <Skeleton className="h-4 w-24" />
                  ) : summary?.next_scheduled_at_unix_secs ? (
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
                <span className="text-label text-text-faint">Last run</span>
                <div
                  className="flex min-h-5 items-center text-body-sm text-text"
                  data-testid="warmup-last"
                >
                  {summaryPending ? (
                    <Skeleton className="h-4 w-28" />
                  ) : lastAttempt ? (
                    <button
                      type="button"
                      onClick={openHistory}
                      className="-mx-1 rounded-sm px-1 text-left transition-colors hover:bg-overlay-2 focus-visible:outline focus-visible:outline-2 focus-visible:outline-accent"
                      aria-label="Open last warm-up attempt detail"
                    >
                      <span className="flex flex-wrap items-center gap-2">
                        <StatusBadge
                          tone={LAST_OUTCOME_TONE[lastAttempt.status]}
                          label={LAST_OUTCOME_LABEL[lastAttempt.status]}
                        />
                        <RelativeTime
                          compact
                          ts={lastAttempt.attempted_at_unix_secs * 1000}
                        />
                      </span>
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

            <div
              className="flex min-h-7 items-center justify-between gap-3"
              data-testid="warmup-plugin-row"
            >
              <span className="inline-flex shrink-0 items-center gap-1.5 text-body-sm text-text-muted">
                Shape plugin
                <ShapePluginHelpHover />
              </span>
              {pluginRegistryPending ? (
                <Skeleton className="h-8 w-48 rounded-sm" />
              ) : shapePlugins.length === 0 &&
                !upstream.warmup_dialect_plugin ? (
                <div className="text-body-sm text-text-muted">
                  <span>{COPY.noShapePluginsAvailable}</span>{' '}
                  <a
                    href="/plugins"
                    className="text-accent-text underline-offset-2 hover:underline"
                  >
                    Plugins
                  </a>
                </div>
              ) : (
                <div className="w-56 max-w-full min-w-0">
                  <Select
                    id="dialect-plugin-select"
                    data-testid="warmup-plugin-select"
                    size="sm"
                    value={selectedPluginValue}
                    onChange={handlePluginChange}
                    disabled={settingsPending}
                    aria-label={COPY.dialectPluginLabel}
                    allLabel={COPY.defaultPluginOption}
                    options={[
                      ...(selectedPluginValue && !selectedPluginKnown
                        ? [
                            {
                              value: selectedPluginValue,
                              label: COPY.unknownPluginTemplate.replace(
                                '{id}',
                                selectedPluginValue,
                              ),
                            },
                          ]
                        : []),
                      ...shapePlugins.map((p: PluginEntry) => ({
                        value: p.id,
                        label: p.name,
                      })),
                    ]}
                  />
                </div>
              )}
            </div>

            <div className="min-h-12" data-testid="warmup-failure-slot">
              {summaryPending ? (
                <Skeleton className="h-12 w-full rounded-sm" />
              ) : lastAttempt?.error_detail ? (
                <div
                  className={cx(
                    'min-h-12',
                    lastAttempt.status === 'permanent_failure'
                      ? DANGER_PANEL_CLASS
                      : WARN_PANEL_CLASS,
                  )}
                >
                  <p>
                    {lastAttempt.status === 'permanent_failure'
                      ? 'Last warm-up failed'
                      : 'Last warm-up hit an error'}
                    {lastAttempt.reason
                      ? `: ${REASON_LABEL[lastAttempt.reason]}.`
                      : '.'}
                    {credentialNoticeShown &&
                    lastAttempt.reason &&
                    CREDENTIAL_FAILURE_REASONS[lastAttempt.reason]
                      ? ' Reconnect from the notice above.'
                      : null}
                  </p>
                  <details className="group mt-1">
                    <summary className="inline-flex cursor-pointer list-none items-center gap-1 rounded-sm text-caption text-text-muted hover:text-text [&::-webkit-details-marker]:hidden">
                      <ChevronRight
                        aria-hidden="true"
                        strokeWidth={1.75}
                        className="size-3 transition-transform group-open:rotate-90"
                      />
                      Details
                    </summary>
                    <code className="mt-1 block break-words font-mono text-data text-text-muted">
                      {lastAttempt.error_detail}
                    </code>
                  </details>
                </div>
              ) : null}
            </div>

            {(leasePanel || errorPanel) && (
              <div className="flex flex-col gap-2">
                {leasePanel && (
                  <div
                    data-testid="warmup-lease-panel"
                    role="status"
                    aria-live="polite"
                    className={WARN_PANEL_CLASS}
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
                    className={DANGER_PANEL_CLASS}
                  >
                    {COPY.fireErrorReasons[errorPanel.reason]}
                  </div>
                )}
              </div>
            )}
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
