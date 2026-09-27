import { HelpCircle, History, SlidersHorizontal } from 'lucide-react';
import { useEffect, useRef, useState } from 'react';
import { toast } from 'sonner';
import { ApiError } from '../../../lib/api';
import {
  type Principal,
  useCacheKeepaliveSummary,
  usePrincipalWritePending,
  useUpdatePrincipalCacheKeepalive,
} from '../../../lib/queries';
import { undoToast } from '../../../lib/undoToast';
import { DetailSection } from '../../ui/DetailPane';
import { Button, cx, Hint, Skeleton, ToggleSwitch } from '../../ui/primitives';
import { cacheKeepaliveAnimationContract } from './__fixtures__/cacheKeepaliveContract';
import { CacheKeepaliveSessionsDrawer } from './CacheKeepaliveSessionsDrawer';
import { CacheKeepaliveSettingsDrawer } from './CacheKeepaliveSettingsDrawer';

declare global {
  interface Window {
    PAUSE_ANIMATIONS?: boolean;
  }
}

if (typeof window !== 'undefined' && window.PAUSE_ANIMATIONS === undefined) {
  window.PAUSE_ANIMATIONS = false;
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
        className="inline-flex h-4 w-4 cursor-help items-center justify-center rounded-sm text-text-faint transition-colors hover:bg-overlay-5 hover:text-text focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-1"
      >
        <HelpCircle className="h-3.5 w-3.5" />
      </button>
    </Hint>
  );
}

function CacheKeepaliveHelpHover() {
  return (
    <HelpIcon
      label="Cache keepalive help"
      text="Keeps the Anthropic prompt cache warm by renewing its TTL — fires a tiny synthetic request just before the prompt cache expires so the next real request still hits a warm cache."
    />
  );
}

function MetricTile({
  label,
  value,
  isLoading,
  subtext,
}: {
  label: string;
  value?: string | number;
  isLoading: boolean;
  subtext: string;
}) {
  const [flash, setFlash] = useState(false);
  const prevValueRef = useRef(value);

  useEffect(() => {
    if (isLoading) return;
    if (prevValueRef.current !== value) {
      prevValueRef.current = value;
      if (!window.PAUSE_ANIMATIONS) {
        setFlash(true);
        const timer = setTimeout(() => setFlash(false), 1000);
        return () => clearTimeout(timer);
      }
    }
  }, [isLoading, value]);

  return (
    <div className="flex flex-col gap-1">
      <span className="text-label text-text-muted">{label}</span>
      <div
        data-testid="cache-keepalive-metric-value"
        aria-busy={isLoading}
        className={cx(
          'flex h-9 items-center text-display text-text tabular-nums',
          flash && 'flash-text-active',
        )}
        style={
          flash && !window.PAUSE_ANIMATIONS
            ? {
                animation: `flash-text ${cacheKeepaliveAnimationContract.metricFlash}`,
              }
            : undefined
        }
      >
        {isLoading ? <Skeleton className="h-7 w-20" /> : value}
      </div>
      <span className="text-caption text-text-faint">{subtext}</span>
    </div>
  );
}

export function CacheKeepaliveCard({ principal }: { principal: Principal }) {
  const updateSettings = useUpdatePrincipalCacheKeepalive();
  // Sibling principal writes bump the revision this toggle would submit, so the
  // switch stays locked for the whole shared write, not just its own request.
  const principalWritePending = usePrincipalWritePending(principal.id) > 0;
  const togglePending = updateSettings.isPending;
  const toggleLocked = togglePending || principalWritePending;
  const summaryQ = useCacheKeepaliveSummary(principal.id);
  const summary = summaryQ.data;

  const [settingsOpen, setSettingsOpen] = useState(false);
  const [sessionsOpen, setSessionsOpen] = useState(false);

  const enabled = principal.cache_keepalive?.enabled ?? false;

  const applyEnabled = (
    nextEnabled: boolean,
    base: Pick<Principal, 'revision' | 'cache_keepalive'>,
    offerUndo: boolean,
  ) => {
    updateSettings.mutate(
      {
        id: principal.id,
        expected_revision: base.revision,
        cache_keepalive: {
          ...(base.cache_keepalive ?? {
            refresh_lead_time_5m_secs: 30,
            refresh_lead_time_1h_secs: 300,
            max_refreshes_per_session: 12,
            max_total_duration_secs: 14400,
            snapshot_max_bytes: 524288,
            classifier: {
              extra_wait_for_user_tools: [],
              treat_end_turn_as_ambiguous: false,
            },
          }),
          enabled: nextEnabled,
        },
      },
      {
        onSuccess: (updated) => {
          const message = nextEnabled
            ? 'Cache keepalive enabled'
            : 'Cache keepalive disabled';
          if (!offerUndo) {
            toast.success(message);
            return;
          }
          // Undo re-applies the previous value against the revision this
          // write produced, so it is rejected if someone else wrote since.
          undoToast({
            message,
            onUndo: () => applyEnabled(!nextEnabled, updated, false),
          });
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

  const handleToggle = () => {
    if (toggleLocked) return;
    applyEnabled(!enabled, principal, true);
  };

  const headerTitle = (
    <span className="flex items-center gap-2">
      <span>Cache keepalive</span>
      <CacheKeepaliveHelpHover />
    </span>
  );

  const headerActions = (
    <div className="flex flex-wrap items-center gap-2">
      <Button
        variant="secondary"
        iconLeft={<History />}
        size="sm"
        onClick={() => setSessionsOpen(true)}
      >
        Sessions
      </Button>
      <Button
        variant="secondary"
        iconLeft={<SlidersHorizontal />}
        size="sm"
        onClick={() => setSettingsOpen(true)}
      >
        Settings
      </Button>
      <ToggleSwitch
        variant="compact"
        role="switch"
        aria-label="Toggle cache keepalive"
        label={
          <span className="text-text-muted">
            {enabled ? 'Enabled' : 'Disabled'}
          </span>
        }
        aria-busy={togglePending || undefined}
        checked={enabled}
        data-testid="cache-keepalive-switch"
        disabled={toggleLocked}
        onChange={handleToggle}
        className="ml-1 flex-row-reverse"
      />
    </div>
  );

  const formatMoney = (val: number) => {
    return `$${val.toFixed(2)}`;
  };

  return (
    <>
      <DetailSection
        span="full"
        data-testid="cache-keepalive-card"
        title={headerTitle}
        description="Renews the prompt-cache TTL during idle gaps."
        action={headerActions}
      >
        {!enabled ? (
          <p
            className="text-body text-text-muted"
            data-testid="cache-keepalive-off"
          >
            Off. Turn on to renew the prompt-cache TTL for idle sessions.
          </p>
        ) : (
          // Readout row: 1px rules between readouts, none before the first
          // readout of each visual row (2-up on phones, 4-up from md).
          <div className="grid grid-cols-2 gap-y-5 md:grid-cols-4 *:border-l *:border-subtle *:px-5 *:odd:border-l-0 *:odd:pl-0 md:*:nth-3:border-l md:*:nth-3:pl-5">
            <MetricTile
              label="Renewing now"
              isLoading={summaryQ.isLoading}
              value={
                summaryQ.isLoading
                  ? undefined
                  : (summary?.renewing_now ?? 0).toLocaleString('en-US')
              }
              subtext="scheduled or mid-renewal"
            />
            <MetricTile
              label="Sessions (last 5m)"
              isLoading={summaryQ.isLoading}
              value={
                summaryQ.isLoading
                  ? undefined
                  : (summary?.sessions_last_5m ?? 0).toLocaleString('en-US')
              }
              subtext="seen in last 5 min"
            />
            <MetricTile
              label="Renewals fired"
              isLoading={summaryQ.isLoading}
              value={
                summaryQ.isLoading
                  ? undefined
                  : (summary?.renewals_fired ?? 0).toLocaleString('en-US')
              }
              subtext="all-time"
            />
            <MetricTile
              label="Cost saved"
              isLoading={summaryQ.isLoading}
              value={
                summaryQ.isLoading
                  ? undefined
                  : summary
                    ? formatMoney(summary.cost_saved)
                    : '$0.00'
              }
              subtext="net, after renewal spend"
            />
          </div>
        )}
      </DetailSection>

      <CacheKeepaliveSettingsDrawer
        open={settingsOpen}
        onOpenChange={setSettingsOpen}
        principal={principal}
      />
      <CacheKeepaliveSessionsDrawer
        open={sessionsOpen}
        onOpenChange={setSessionsOpen}
        principal={principal}
      />
    </>
  );
}
