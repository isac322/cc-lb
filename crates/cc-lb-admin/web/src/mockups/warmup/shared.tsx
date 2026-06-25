import { History, Zap } from 'lucide-react';
import type { ReactNode } from 'react';
import { Badge, Button, cx, StatusBadge } from '../../components/ui/primitives';
import { RelativeTime } from '../../components/ui/RelativeTime';
import {
  OUTCOME_DESCRIPTION,
  REASON_LABEL,
} from '../../components/upstreams/warmup/parts/copy';
import type { Upstream, WarmupOutcome, WarmupSummary } from '../../lib/queries';
import type { WarmupMockFixture } from './types';

const LAST_OUTCOME_LABEL: Record<WarmupOutcome, string> = {
  success_fresh: 'Success',
  success_redundant: 'Success',
  transient_failure: 'Retrying',
  permanent_failure: 'Failed',
  skipped: 'Skipped',
};

const LAST_OUTCOME_TONE: Record<
  WarmupOutcome,
  'ok' | 'warn' | 'danger' | 'neutral'
> = {
  success_fresh: 'ok',
  success_redundant: 'ok',
  transient_failure: 'warn',
  permanent_failure: 'danger',
  skipped: 'neutral',
};

const SHAPE_PLUGIN_OPTIONS = [
  { value: '', label: 'No shape plugin' },
  { value: 'subscription-launderer', label: 'subscription-launderer' },
  { value: 'conservative-shape', label: 'conservative-shape' },
  { value: 'burst-window-shaper', label: 'burst-window-shaper' },
] as const;

export function getStatus(
  upstream: Upstream,
  summary: WarmupSummary,
): {
  readonly tone: 'ok' | 'warn' | 'danger' | 'neutral';
  readonly label: string;
} {
  if (!upstream.enabled || !upstream.warmup_enabled) {
    return { tone: 'neutral', label: 'Paused' };
  }
  switch (summary.last_attempt?.outcome) {
    case 'success_fresh':
    case 'success_redundant':
      return { tone: 'ok', label: 'Healthy' };
    case 'transient_failure':
    case 'skipped':
      return { tone: 'warn', label: 'Degraded' };
    case 'permanent_failure':
      return { tone: 'danger', label: 'Down' };
    case undefined:
      return { tone: 'neutral', label: 'Pending' };
  }
}

export function MockSwitch({ checked }: { readonly checked: boolean }) {
  return (
    <button
      type="button"
      role="switch"
      aria-checked={checked}
      aria-label="Mock warmup toggle"
      data-mock="true"
      className={cx(
        'relative inline-flex h-5 w-9 shrink-0 items-center self-center rounded-full border transition-colors duration-200 ease-in-out',
        'focus:outline-none focus:ring-2 focus:ring-accent/40',
        checked
          ? 'bg-[color:var(--color-ok)] border-[color:var(--color-ok)]'
          : 'bg-overlay-5 border-subtle-strong hover:border-text-muted',
      )}
    >
      <span
        className={cx(
          'pointer-events-none inline-block h-4 w-4 rounded-full bg-white shadow-sm transition-transform duration-200 ease-in-out',
          checked ? 'translate-x-4' : 'translate-x-0.5',
        )}
      />
    </button>
  );
}

export function HeaderTitle({
  fixture,
  titleHelp,
}: {
  readonly fixture: WarmupMockFixture;
  readonly titleHelp?: ReactNode;
}) {
  const status = getStatus(fixture.upstream, fixture.summary);
  return (
    <div className="flex items-center gap-2">
      <StatusBadge tone={status.tone} label={status.label} />
      <span>Warm-up</span>
      {titleHelp}
    </div>
  );
}

export function HistoryButton() {
  return (
    <Button
      variant="ghost"
      size="sm"
      data-mock="true"
      iconLeft={<History className="h-3 w-3" />}
    >
      History
    </Button>
  );
}

export function FireNowButton({
  fullWidth = false,
}: {
  readonly fullWidth?: boolean;
}) {
  return (
    <Button
      variant="secondary"
      size="sm"
      fullWidth={fullWidth}
      data-mock="true"
      iconLeft={<Zap className="h-3 w-3" />}
    >
      Fire now
    </Button>
  );
}

export function LastAttemptLine({
  fixture,
}: {
  readonly fixture: WarmupMockFixture;
}) {
  const attempt = fixture.summary.last_attempt;
  if (!attempt) return <span className="text-sm text-text-muted">Never</span>;
  return (
    <div className="flex flex-wrap items-center gap-2 text-sm font-medium text-text">
      <Badge tone={LAST_OUTCOME_TONE[attempt.outcome]}>
        {LAST_OUTCOME_LABEL[attempt.outcome]}
      </Badge>
      <RelativeTime compact ts={attempt.attempted_at_unix_secs * 1000} />
    </div>
  );
}

export function NextRunValue({
  fixture,
}: {
  readonly fixture: WarmupMockFixture;
}) {
  return (
    <RelativeTime
      compact
      className="text-sm font-medium text-text"
      ts={
        fixture.summary.next_scheduled_at_unix_secs == null
          ? null
          : fixture.summary.next_scheduled_at_unix_secs * 1000
      }
    />
  );
}

export function PluginBadge({
  fixture,
}: {
  readonly fixture: WarmupMockFixture;
}) {
  return <Badge tone="mono">Plugin: {fixture.pluginName ?? 'none'}</Badge>;
}

export function ShapePluginSelect({
  fixture,
}: {
  readonly fixture: WarmupMockFixture;
}) {
  return (
    <label className="inline-flex items-center gap-2 rounded-sm border border-subtle bg-overlay-2 px-2 py-1 text-xs text-text-muted">
      <select
        aria-label="Select shape plugin"
        data-mock="true"
        defaultValue={fixture.pluginName ?? ''}
        className="max-w-[220px] bg-transparent font-mono text-text outline-none"
      >
        {SHAPE_PLUGIN_OPTIONS.map((option) => (
          <option key={option.value} value={option.value}>
            {option.label}
          </option>
        ))}
      </select>
    </label>
  );
}

export function FailureNotice({
  fixture,
}: {
  readonly fixture: WarmupMockFixture;
}) {
  const attempt = fixture.summary.last_attempt;
  if (!attempt?.error_detail) return null;
  const tone = attempt.outcome === 'permanent_failure' ? 'danger' : 'warn';
  const reason = attempt.reason
    ? REASON_LABEL[attempt.reason]
    : 'Warm-up failed';
  return (
    <div
      className={cx(
        'rounded-sm border p-2 text-xs leading-relaxed',
        tone === 'danger'
          ? 'border-[color:var(--color-danger)]/30 bg-red-500/10 text-[color:var(--color-danger)]'
          : 'border-[color:var(--color-warn)]/30 bg-amber-500/10 text-[color:var(--color-warn)]',
      )}
    >
      {reason}: {attempt.error_detail}
    </div>
  );
}

export function ExecutiveNarrative({
  fixture,
}: {
  readonly fixture: WarmupMockFixture;
}) {
  if (!fixture.upstream.enabled)
    return <>Paused because the upstream is disabled.</>;
  if (!fixture.upstream.warmup_enabled)
    return <>Paused. Re-enable to schedule new attempts.</>;
  const attempt = fixture.summary.last_attempt;
  if (!attempt) return <>Pending. Waiting for the first scheduled run.</>;
  return (
    <>
      {OUTCOME_DESCRIPTION[attempt.outcome]}. Last attempt{' '}
      <RelativeTime compact ts={attempt.attempted_at_unix_secs * 1000} />.
    </>
  );
}
