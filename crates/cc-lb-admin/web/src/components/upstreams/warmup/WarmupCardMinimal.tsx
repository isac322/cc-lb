import { ChevronRight, HelpCircle, History, Zap } from 'lucide-react';
import { useEffect, useState } from 'react';
import { toast } from 'sonner';
import { windowLabel } from '../../../lib/api';
import { getWindowColor } from '../../../lib/colors';
import {
  COPY,
  FIRE_NOW_COOLDOWN_MS,
  type FireErrorReason,
  LEASE_PANEL_AUTO_DISMISS_MS,
  TOAST_DURATIONS,
} from '../../../lib/copy/warmup';
import {
  capabilitiesOf,
  type ProviderFamily,
  providerOf,
} from '../../../lib/providers';
import {
  type FireNowResponse,
  type PluginEntry,
  type Upstream,
  useClearUpstreamWarmupDialectPlugin,
  useFireNowUpstreamWarmup,
  usePluginRegistry,
  useUpdateUpstreamWarmupSettings,
  useWarmupSummary,
  type WarmupAttempt,
  type WarmupLane,
  type WarmupLaneSummary,
} from '../../../lib/queries';
import { DetailRow, DetailRows, DetailSection } from '../../ui/DetailPane';
import {
  Button,
  ConfirmDialog,
  cx,
  Hint,
  SegmentedControl,
  Skeleton,
  StatusBadge,
  ToggleSwitch,
} from '../../ui/primitives';
import { RelativeTime } from '../../ui/RelativeTime';
import { Select } from '../../ui/Select';
import { OUTCOME_LABEL, REASON_LABEL } from './parts/copy';
import { WarmupConfigModal } from './parts/WarmupConfigModal';
import {
  laneFailureAttempt,
  OUTCOME_SEVERITY,
  worstLaneStatus,
} from './parts/warmupViewModel';
import { WarmupHistoryDrawer } from './WarmupHistoryDrawer';

const WARN_PANEL_CLASS =
  'rounded-sm bg-warn/8 px-3 py-2 text-body-sm text-warn-text';
const DANGER_PANEL_CLASS =
  'rounded-sm bg-danger/8 px-3 py-2 text-body-sm text-danger-text';

function pluginSupportsWarmup(
  p: PluginEntry,
  provider: ProviderFamily,
): boolean {
  if (!p.supported_slots?.includes('shape')) return false;
  if (provider === 'anthropic') {
    // Keep legacy Anthropic registry entries selectable.
    return (
      !Object.keys(p.hook_metadata).length ||
      'shape' in p.hook_metadata ||
      'shape_anthropic' in p.hook_metadata
    );
  }
  return `shape_${provider}` in p.hook_metadata;
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
      <button
        type="button"
        aria-label={label}
        className="relative inline-flex h-4 w-4 max-md:h-11 max-md:w-11 max-md:-my-3.5 cursor-help items-center justify-center rounded-sm text-text-faint transition-colors hover:bg-overlay-5 hover:text-text focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-2"
      >
        <HelpCircle className="h-3.5 w-3.5" />
      </button>
    </Hint>
  );
}

function WarmupHelpHover() {
  return <HelpIcon label="Warm-up help" text={COPY.helpText} />;
}

function ShapePluginHelpHover() {
  return (
    <HelpIcon
      label="Shape plugin help"
      text="The shape plugin transforms the provider's small native warm-up request before OAuth signing. Only plugins with a shape hook for this provider are offered."
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

type FireLaneChoice = 'both' | WarmupLane;

/** `undefined` is the implicit primary lane of a single-lane upstream. */
type FireTarget = WarmupLane | undefined;

function laneName(lane: FireTarget): string | null {
  return lane ? COPY.laneLabels[lane] : null;
}

/** Bold "Lane name:" prefix for panels on multi-lane upstreams. */
function LanePrefix({ lane }: { lane: FireTarget }) {
  const name = laneName(lane);
  if (!name) return null;
  return (
    <>
      <span className="font-medium">{name}:</span>{' '}
    </>
  );
}

function WindowSwatches({ windows }: { windows: readonly string[] }) {
  return (
    <span className="flex flex-wrap items-center gap-x-3 gap-y-1">
      {windows.map((window) => (
        <span
          key={window}
          className="inline-flex items-center gap-1.5"
          title={windowLabel(window, { long: true })}
        >
          <span
            aria-hidden="true"
            data-slot="window-swatch"
            className="size-2.5 shrink-0 rounded-xs"
            style={{ backgroundColor: getWindowColor(window).stroke }}
          />
          {windowLabel(window)}
        </span>
      ))}
    </span>
  );
}

function NextRunValue({
  pending,
  nextAt,
  testId,
}: {
  pending: boolean;
  nextAt: number | null | undefined;
  testId: string;
}) {
  return (
    <div className="flex flex-col gap-1">
      <span className="text-label text-text-faint">Next run</span>
      <div
        className="flex min-h-5 max-md:min-h-10 items-center text-body-sm text-text"
        data-testid={testId}
      >
        {pending ? (
          <Skeleton className="h-4 w-24" />
        ) : nextAt ? (
          <RelativeTime compact ts={nextAt * 1000} />
        ) : (
          <span className="text-text-muted">Not scheduled</span>
        )}
      </div>
    </div>
  );
}

function LastRunValue({
  pending,
  attempt,
  fallbackAt,
  onOpen,
  openLabel,
  testId,
}: {
  pending: boolean;
  attempt: WarmupAttempt | null | undefined;
  /** Shown when no attempt is on record (any-lane success time). */
  fallbackAt: number | null;
  onOpen: () => void;
  openLabel: string;
  testId: string;
}) {
  return (
    <div className="flex flex-col gap-1">
      <span className="text-label text-text-faint">Last run</span>
      <div
        className="flex min-h-5 max-md:min-h-10 items-center text-body-sm text-text"
        data-testid={testId}
      >
        {pending ? (
          <Skeleton className="h-4 w-28" />
        ) : attempt ? (
          <button
            type="button"
            onClick={onOpen}
            className="-mx-1 rounded-sm px-1 text-left transition-colors hover:bg-overlay-2 max-md:min-h-10 focus-visible:outline focus-visible:outline-2 focus-visible:outline-accent"
            aria-label={openLabel}
          >
            <span className="flex flex-wrap items-center gap-2">
              <StatusBadge
                tone={OUTCOME_SEVERITY[attempt.status]}
                label={OUTCOME_LABEL[attempt.status]}
              />
              <RelativeTime
                compact
                ts={attempt.attempted_at_unix_secs * 1000}
              />
            </span>
          </button>
        ) : fallbackAt ? (
          <RelativeTime compact ts={fallbackAt * 1000} />
        ) : (
          <span className="text-text-muted">Never</span>
        )}
      </div>
    </div>
  );
}

function FailurePanel({
  attempt,
  lane,
  credentialNoticeShown,
}: {
  attempt: WarmupAttempt;
  lane: FireTarget;
  credentialNoticeShown: boolean;
}) {
  return (
    <div
      data-testid="warmup-failure-panel"
      data-lane={lane}
      className={cx(
        'min-h-12',
        attempt.status === 'permanent_failure'
          ? DANGER_PANEL_CLASS
          : WARN_PANEL_CLASS,
      )}
    >
      <p>
        <LanePrefix lane={lane} />
        {attempt.status === 'permanent_failure'
          ? 'Last warm-up failure'
          : 'Last warm-up retry error'}
        {attempt.reason ? `: ${REASON_LABEL[attempt.reason]}.` : '.'}
        {credentialNoticeShown &&
        attempt.reason &&
        CREDENTIAL_FAILURE_REASONS[attempt.reason]
          ? ' Reconnect from the notice above.'
          : null}
      </p>
      {attempt.error_detail ? (
        <details className="group mt-1">
          <summary className="inline-flex cursor-pointer list-none items-center gap-1 rounded-sm text-caption text-text-muted transition-colors hover:text-text max-md:min-h-10 focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-1 [&::-webkit-details-marker]:hidden">
            <ChevronRight
              aria-hidden="true"
              strokeWidth={1.75}
              className="size-3 transition-transform group-open:rotate-90"
            />
            Details
          </summary>
          <code className="mt-1 block break-words font-mono text-data text-text-muted">
            {attempt.error_detail}
          </code>
        </details>
      ) : null}
    </div>
  );
}

type WarmupCardProps = {
  upstream: Upstream;
  /** True when the page already shows a reconnect notice for this upstream. */
  credentialNoticeShown?: boolean;
};

export function WarmupCardMinimal({
  upstream,
  credentialNoticeShown = false,
}: WarmupCardProps) {
  if (!capabilitiesOf(upstream.kind).warmup) {
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
  const lanes: readonly WarmupLaneSummary[] = summary?.lanes ?? [];
  const multiLane = lanes.length > 1;
  // Only multi-lane upstreams name lanes on the wire; a single-lane upstream
  // keeps the plain fire-now URL.
  const fireLaneChoices: readonly WarmupLane[] = multiLane
    ? lanes.filter((l) => l.eligible).map((l) => l.lane)
    : [];

  const [staleRevisionVisible, setStaleRevisionVisible] = useState(false);
  const [revisionOverride, setRevisionOverride] = useState<number | null>(null);
  const [pendingPluginValue, setPendingPluginValue] = useState<string | null>(
    null,
  );
  const [leasePanels, setLeasePanels] = useState<
    { lane: FireTarget; heldBy: string }[]
  >([]);
  const [errorPanels, setErrorPanels] = useState<
    { lane: FireTarget; reason: FireErrorReason }[]
  >([]);
  const [fireCooldown, setFireCooldown] = useState(false);
  const [fireLaneChoice, setFireLaneChoice] = useState<FireLaneChoice>('both');
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [historyLane, setHistoryLane] = useState<WarmupLane | 'all'>('all');
  const [configOpen, setConfigOpen] = useState(false);

  const [confirmFireOpen, setConfirmFireOpen] = useState(false);
  const [confirmClearPluginOpen, setConfirmClearPluginOpen] = useState(false);

  const storedPluginId = upstream.warmup_dialect_plugin?.wasm_registry_id ?? '';
  const selectedPluginValue = pendingPluginValue ?? storedPluginId;

  useEffect(() => {
    if (leasePanels.length === 0) return;
    const timer = setTimeout(
      () => setLeasePanels([]),
      LEASE_PANEL_AUTO_DISMISS_MS,
    );
    return () => clearTimeout(timer);
  }, [leasePanels]);

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

  const handleFireNow = async () => {
    setLeasePanels([]);
    setErrorPanels([]);
    const targets: FireTarget[] =
      fireLaneChoices.length === 0
        ? [undefined]
        : fireLaneChoice === 'both' || !fireLaneChoices.includes(fireLaneChoice)
          ? [...fireLaneChoices]
          : [fireLaneChoice];
    // One request per lane; each lane reports its own outcome.
    const results = await Promise.allSettled(
      targets.map((lane) => fireWarmup.mutateAsync({ id: upstream.id, lane })),
    );
    setConfirmFireOpen(false);
    const leases: { lane: FireTarget; heldBy: string }[] = [];
    const errors: { lane: FireTarget; reason: FireErrorReason }[] = [];
    let firedAny = false;
    results.forEach((result, index) => {
      const lane = targets[index];
      const name = laneName(lane);
      if (result.status === 'rejected') {
        const message = errorMessage(result.reason, 'Failed to fire warmup');
        toast.error(name ? `${name}: ${message}` : message);
        return;
      }
      const res: FireNowResponse = result.value;
      if (res.fired) {
        firedAny = true;
        toast.success(
          name
            ? COPY.fireSuccessLaneTemplate.replace('{lane}', name)
            : COPY.fireSuccess,
          { duration: TOAST_DURATIONS.success },
        );
      } else if (res.reason === 'lease_held') {
        leases.push({ lane, heldBy: res.held_by });
      } else {
        errors.push({ lane, reason: res.reason });
      }
    });
    setLeasePanels(leases);
    setErrorPanels(errors);
    if (firedAny) {
      setFireCooldown(true);
      setTimeout(() => setFireCooldown(false), FIRE_NOW_COOLDOWN_MS);
    }
  };

  const openFireConfirm = () => {
    setFireLaneChoice('both');
    setConfirmFireOpen(true);
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

  const openHistory = (lane: WarmupLane | 'all' = 'all') => {
    setHistoryLane(multiLane ? lane : 'all');
    setDrawerOpen(true);
  };

  const shapePlugins =
    registry.data?.entries.filter((p: PluginEntry) =>
      pluginSupportsWarmup(p, providerOf(upstream.kind)),
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

  let statusTone: 'ok' | 'warn' | 'danger' | 'neutral' | 'live' = 'neutral';
  let statusLabel = 'Paused';

  const credentialRequiresReconnect =
    upstream.status.last_apply_error === 'status_400' ||
    upstream.status.last_apply_error === 'status_401' ||
    upstream.status.last_apply_error === 'refresh_token_expired';
  if (upstream.enabled && upstream.warmup_enabled) {
    if (credentialRequiresReconnect) {
      statusTone = 'danger';
      statusLabel = 'Down';
    } else if (summaryQ.isError) {
      statusTone = 'danger';
      statusLabel = 'Unavailable';
    } else {
      // Paused and Down-for-reconnect above are card-wide; otherwise the
      // most severe eligible lane sets the badge.
      const worst = worstLaneStatus(lanes);
      statusTone = worst.tone;
      statusLabel = worst.label;
    }
  }

  const headerTitle = (
    <span className="flex items-center gap-2">
      <span>Warm-up</span>
      <WarmupHelpHover />
      <span
        className="ml-1 inline-flex min-h-5 w-24 items-center"
        data-testid="warmup-status-value"
      >
        {summaryPending &&
        upstream.enabled &&
        upstream.warmup_enabled &&
        !credentialRequiresReconnect ? (
          <Skeleton className="h-4 w-full rounded-sm" />
        ) : (
          <StatusBadge tone={statusTone} label={statusLabel} />
        )}
      </span>
    </span>
  );

  const headerActions = (
    <div className="flex items-center gap-2">
      <Button
        size="sm"
        data-testid="warmup-fire-now"
        onClick={openFireConfirm}
        // Lane choices come from the summary; firing before it loads would
        // silently drop the default "Both" on multi-lane upstreams.
        disabled={
          !summary ||
          fireWarmup.isPending ||
          fireCooldown ||
          !upstream.warmup_enabled
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

  // `lanes` may be empty (non-warm-up kinds, or a degraded response).
  const primaryLane: WarmupLaneSummary | undefined = lanes[0];
  const failures: { lane: FireTarget; attempt: WarmupAttempt }[] = [];
  for (const lane of lanes) {
    if (!lane.eligible && multiLane) continue;
    const attempt = laneFailureAttempt(lane);
    if (attempt)
      failures.push({ lane: multiLane ? lane.lane : undefined, attempt });
  }

  return (
    <>
      <DetailSection
        // Full width like the neighbouring Credential and Account metadata
        // sections: half-width pairing leaves a tall dead gap in the other
        // column whenever card heights differ.
        span="full"
        data-testid="warmup-card"
        data-variant="minimal"
        tabIndex={-1}
        className="outline-none"
        title={headerTitle}
        description={
          upstream.kind === 'gemini_oauth'
            ? COPY.geminiCardSubtitle
            : COPY.cardSubtitle
        }
        action={headerActions}
      >
        <div className="space-y-4">
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
            {multiLane ? (
              <DetailRows data-testid="warmup-lanes">
                {lanes.map((lane) => (
                  <DetailRow
                    key={lane.lane}
                    data-testid={`warmup-lane-${lane.lane}`}
                    data-eligible={lane.eligible}
                    label={COPY.laneLabels[lane.lane]}
                    description={
                      lane.eligible && lane.target_windows.length > 0 ? (
                        <WindowSwatches windows={lane.target_windows} />
                      ) : undefined
                    }
                  >
                    {lane.eligible ? (
                      <div className="grid grid-cols-2 gap-4">
                        <NextRunValue
                          pending={false}
                          nextAt={lane.next_scheduled_at_unix_secs}
                          testId={`warmup-next-value-${lane.lane}`}
                        />
                        <LastRunValue
                          pending={false}
                          attempt={lane.last_attempt}
                          fallbackAt={null}
                          onOpen={() => openHistory(lane.lane)}
                          openLabel={`Open last ${COPY.laneLabels[lane.lane]} warm-up attempt detail`}
                          testId={`warmup-last-${lane.lane}`}
                        />
                      </div>
                    ) : (
                      <span
                        className="text-body-sm text-text-muted"
                        data-testid={`warmup-lane-not-reported-${lane.lane}`}
                      >
                        {COPY.laneNotReported}
                      </span>
                    )}
                  </DetailRow>
                ))}
              </DetailRows>
            ) : (
              <div className="grid grid-cols-2 gap-4">
                <NextRunValue
                  pending={summaryPending}
                  nextAt={primaryLane?.next_scheduled_at_unix_secs}
                  testId="warmup-next-value"
                />
                <LastRunValue
                  pending={summaryPending}
                  attempt={primaryLane?.last_attempt}
                  fallbackAt={upstream.status.last_warmup_at_unix_secs}
                  onOpen={() => openHistory()}
                  openLabel="Open last warm-up attempt detail"
                  testId="warmup-last"
                />
              </div>
            )}

            {/* Phones stack the label over a full-width picker; beside it the
                picker would squeeze to a truncated sliver at 360px. */}
            {capabilitiesOf(upstream.kind).warmup && (
              <div
                className="flex min-h-7 items-center justify-between gap-3 max-md:flex-col max-md:items-stretch max-md:gap-1.5"
                data-testid="warmup-plugin-row"
              >
                <span className="inline-flex shrink-0 items-center gap-1.5 text-body-sm text-text-muted">
                  Shape plugin
                  <ShapePluginHelpHover />
                </span>
                <div className="flex flex-wrap items-center justify-end gap-2 max-md:w-full max-md:flex-col max-md:items-stretch">
                  {pluginRegistryPending ? (
                    <Skeleton className="h-8 w-48 rounded-sm" />
                  ) : shapePlugins.length === 0 &&
                    !upstream.warmup_dialect_plugin ? (
                    <div className="text-body-sm text-text-muted">
                      <span>{COPY.noShapePluginsAvailable}</span>{' '}
                      <a
                        href="/plugins"
                        className="max-md:relative max-md:inline-flex max-md:min-h-10 max-md:items-center max-md:px-2 max-md:-mx-2 rounded-sm text-text underline decoration-border-strong underline-offset-2 transition-colors hover:decoration-accent focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-1"
                      >
                        Plugins
                      </a>
                    </div>
                  ) : (
                    <div className="w-56 max-w-full min-w-0 max-md:w-full">
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
                  {pluginSnapshot && (
                    <Button
                      size="sm"
                      variant="secondary"
                      data-testid="warmup-config-button"
                      onClick={() => setConfigOpen(true)}
                    >
                      View plugin config
                    </Button>
                  )}
                </div>
              </div>
            )}

            <div className="min-h-12" data-testid="warmup-failure-slot">
              {summaryPending ? (
                <Skeleton className="h-12 w-full rounded-sm" />
              ) : summaryQ.isError ? (
                <p className="text-body-sm text-danger-text">
                  Warm-up status could not be loaded. Refresh this page to try
                  again.
                </p>
              ) : failures.length > 0 ? (
                <div className="flex flex-col gap-2">
                  {failures.map(({ lane, attempt }) => (
                    <FailurePanel
                      key={lane ?? 'primary'}
                      attempt={attempt}
                      lane={lane}
                      credentialNoticeShown={credentialNoticeShown}
                    />
                  ))}
                </div>
              ) : null}
            </div>

            {(leasePanels.length > 0 || errorPanels.length > 0) && (
              <div className="flex flex-col gap-2">
                {leasePanels.map(({ lane, heldBy }) => (
                  <div
                    key={`lease-${lane ?? 'primary'}`}
                    data-testid="warmup-lease-panel"
                    data-lane={lane}
                    role="status"
                    aria-live="polite"
                    className={WARN_PANEL_CLASS}
                  >
                    <LanePrefix lane={lane} />
                    {COPY.leaseHeldTemplate.replace('{heldBy}', heldBy)}
                  </div>
                ))}
                {errorPanels.map(({ lane, reason }) => (
                  <div
                    key={`error-${lane ?? 'primary'}`}
                    data-testid="warmup-error-panel"
                    data-reason={reason}
                    data-lane={lane}
                    role="alert"
                    aria-live="polite"
                    className={DANGER_PANEL_CLASS}
                  >
                    <LanePrefix lane={lane} />
                    {COPY.fireErrorReasons[reason]}
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>

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
          onConfirm={() => {
            void handleFireNow();
          }}
          confirmDisabled={!summary || fireWarmup.isPending}
        >
          {fireLaneChoices.length > 1 ? (
            <div
              className="flex flex-wrap items-center justify-between gap-3"
              data-testid="warmup-fire-lane-choice"
            >
              <span className="text-body-sm text-text-muted">
                {COPY.laneFilterLabel}
              </span>
              <SegmentedControl
                ariaLabel={COPY.laneFilterLabel}
                size="sm"
                value={fireLaneChoice}
                onChange={setFireLaneChoice}
                options={[
                  { value: 'both', label: COPY.laneFireBothOption },
                  ...fireLaneChoices.map((lane) => ({
                    value: lane,
                    label: COPY.laneShortLabels[lane],
                  })),
                ]}
              />
            </div>
          ) : null}
        </ConfirmDialog>

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
      </DetailSection>

      {capabilitiesOf(upstream.kind).warmup && pluginSnapshot && (
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
        lanes={lanes.map((l) => l.lane)}
        laneFilter={historyLane}
        onLaneFilterChange={setHistoryLane}
      />
    </>
  );
}
