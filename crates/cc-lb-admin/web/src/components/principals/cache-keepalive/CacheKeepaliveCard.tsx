import { Switch as BaseSwitch } from '@base-ui/react/switch';
import { HelpCircle } from 'lucide-react';
import { useEffect, useRef, useState } from 'react';
import { toast } from 'sonner';
import { ApiError } from '../../../lib/api';
import {
  type Principal,
  useCacheKeepaliveSummary,
  useUpdatePrincipalCacheKeepalive,
} from '../../../lib/queries';
import {
  Button,
  Card,
  CardBody,
  CardHeader,
  cx,
  Hint,
} from '../../ui/primitives';
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
        className="inline-flex h-4 w-4 cursor-help items-center justify-center text-text-faint transition-colors hover:text-text"
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
  subtext,
}: {
  label: string;
  value: string | number;
  subtext: string;
}) {
  const [flash, setFlash] = useState(false);
  const prevValueRef = useRef(value);

  useEffect(() => {
    if (prevValueRef.current !== value) {
      prevValueRef.current = value;
      if (!window.PAUSE_ANIMATIONS) {
        setFlash(true);
        const timer = setTimeout(() => setFlash(false), 1000);
        return () => clearTimeout(timer);
      }
    }
  }, [value]);

  return (
    <div className="flex flex-col gap-1">
      <span className="text-[11px] uppercase tracking-wider text-text-faint">
        {label}
      </span>
      <div
        className={cx(
          'text-sm font-medium text-text',
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
        {value}
      </div>
      <span className="text-[11px] text-text-faint">{subtext}</span>
    </div>
  );
}

export function CacheKeepaliveCard({ principal }: { principal: Principal }) {
  const updateSettings = useUpdatePrincipalCacheKeepalive();
  const summaryQ = useCacheKeepaliveSummary(principal.id);
  const summary = summaryQ.data;

  const [settingsOpen, setSettingsOpen] = useState(false);
  const [sessionsOpen, setSessionsOpen] = useState(false);

  const enabled = principal.cache_keepalive?.enabled ?? false;

  const handleToggle = () => {
    updateSettings.mutate(
      {
        id: principal.id,
        expected_revision: principal.revision,
        cache_keepalive: {
          ...(principal.cache_keepalive ?? {
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
          enabled: !enabled,
        },
      },
      {
        onSuccess: () => {
          toast.success(
            !enabled ? 'Cache keepalive enabled' : 'Cache keepalive disabled',
          );
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

  const headerTitle = (
    <div className="flex items-center gap-2">
      <span>Cache keepalive</span>
      <CacheKeepaliveHelpHover />
    </div>
  );

  const headerActions = (
    <div className="flex items-center">
      <BaseSwitch.Root
        aria-label="Toggle cache keepalive"
        checked={enabled}
        className={cx(
          'relative inline-flex h-5 w-9 shrink-0 items-center self-center rounded-full border transition-colors duration-200 ease-in-out focus:outline-none focus:ring-2 focus:ring-accent/40 disabled:opacity-50 disabled:cursor-not-allowed',
          enabled
            ? 'bg-[color:var(--color-ok)] border-[color:var(--color-ok)]'
            : 'bg-overlay-5 border-subtle-strong hover:border-text-muted',
        )}
        data-testid="cache-keepalive-switch"
        disabled={updateSettings.isPending}
        nativeButton
        onCheckedChange={handleToggle}
        render={<button type="button" />}
      >
        <BaseSwitch.Thumb
          className={cx(
            'pointer-events-none inline-block h-4 w-4 rounded-full bg-white shadow-sm transition-transform duration-200 ease-in-out',
            enabled ? 'translate-x-4' : 'translate-x-0.5',
          )}
        />
      </BaseSwitch.Root>
    </div>
  );

  const formatMoney = (val: number) => {
    return `$${val.toFixed(2)}`;
  };

  return (
    <>
      <Card
        data-testid="cache-keepalive-card"
        className="w-full h-full flex flex-col"
      >
        <CardHeader
          title={headerTitle}
          subtitle="Renews the prompt-cache TTL during idle gaps."
          action={headerActions}
          align="center"
        />
        <CardBody className="space-y-4 flex-1 flex flex-col">
          <div className="grid grid-cols-2 gap-4">
            <MetricTile
              label="Renewing now"
              value={summary?.renewing_now ?? 0}
              subtext="scheduled or mid-renewal"
            />
            <MetricTile
              label="Sessions (last 5m)"
              value={summary?.sessions_last_5m ?? 0}
              subtext="seen in last 5 min"
            />
            <MetricTile
              label="Renewals fired"
              value={summary?.renewals_fired ?? 0}
              subtext="all-time"
            />
            <MetricTile
              label="Cost saved"
              value={summary ? formatMoney(summary.cost_saved) : '$0.00'}
              subtext="net, after renewal spend"
            />
          </div>
          <div className="flex items-center gap-2 pt-2 mt-auto">
            <Button size="sm" onClick={() => setSessionsOpen(true)}>
              Sessions
            </Button>
            <Button size="sm" onClick={() => setSettingsOpen(true)}>
              Settings
            </Button>
          </div>
        </CardBody>
      </Card>

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
