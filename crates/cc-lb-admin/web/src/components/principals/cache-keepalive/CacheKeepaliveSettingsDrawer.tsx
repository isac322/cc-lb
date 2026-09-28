import { X } from 'lucide-react';
import { useEffect, useId, useRef, useState } from 'react';
import { toast } from 'sonner';
import { ApiError } from '../../../lib/api';
import {
  type Principal,
  usePrincipalWritePending,
  useUpdatePrincipalCacheKeepalive,
} from '../../../lib/queries';
import {
  Button,
  cx,
  Drawer,
  Field,
  INPUT_CLASS,
  ToggleSwitch,
} from '../../ui/primitives';

// INPUT_CLASS carries no disabled affordance of its own; a control locked by an
// in-flight write must read as unavailable, not merely inert.
const PENDING_INPUT_CLASS =
  'disabled:cursor-not-allowed disabled:border-subtle disabled:text-text-faint';

interface Props {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  principal: Principal;
}

interface CacheKeepaliveDraftSnapshot {
  enabled: boolean;
  lead5m: number;
  lead1h: number;
  maxRenewals: number;
  maxDuration: number;
  snapshotBytes: number;
  extraTools: string[];
  treatAmbiguous: boolean;
}

function getDraftSnapshot(principal: Principal): CacheKeepaliveDraftSnapshot {
  const config = principal.cache_keepalive;
  return {
    enabled: config?.enabled ?? false,
    lead5m: config?.refresh_lead_time_5m_secs ?? 30,
    lead1h: config?.refresh_lead_time_1h_secs ?? 300,
    maxRenewals: config?.max_refreshes_per_session ?? 12,
    maxDuration: config?.max_total_duration_secs ?? 14400,
    snapshotBytes: config?.snapshot_max_bytes ?? 524288,
    extraTools: [...(config?.classifier?.extra_wait_for_user_tools ?? [])],
    treatAmbiguous: config?.classifier?.treat_end_turn_as_ambiguous ?? false,
  };
}

export function CacheKeepaliveSettingsDrawer({
  open,
  onOpenChange,
  principal,
}: Props) {
  const updateMutation = useUpdatePrincipalCacheKeepalive();
  const principalWritePending = usePrincipalWritePending(principal.id) > 0;
  const saving = updateMutation.isPending;
  // Any same-principal record write bumps the revision this drawer would
  // submit, so every edit control follows the shared principal write lock: the
  // draft fieldset locks the whole region and each control repeats `disabled`
  // so its own state never depends on fieldset inheritance.
  const busy = saving || principalWritePending;

  // A save in flight owns the drawer: backdrop clicks, Escape and the X control
  // must not discard the draft or hide the outcome mid-request. The success path
  // calls `onOpenChange` directly, so it is never swallowed here.
  const handleOpenChange = (next: boolean) => {
    if (saving && !next) return;
    onOpenChange(next);
  };

  const [enabled, setEnabled] = useState(false);
  const [lead5m, setLead5m] = useState(30);
  const [lead1h, setLead1h] = useState(300);
  const [maxRenewals, setMaxRenewals] = useState(12);
  const [maxDuration, setMaxDuration] = useState(14400);
  const [snapshotBytes, setSnapshotBytes] = useState(524288);
  const [extraTools, setExtraTools] = useState<string[]>([]);
  const [treatAmbiguous, setTreatAmbiguous] = useState(false);
  const [newTool, setNewTool] = useState('');
  const toolInputId = useId();
  const toolHintId = useId();
  const initialDraftRef = useRef<CacheKeepaliveDraftSnapshot | null>(null);
  const [editingRevision, setEditingRevision] = useState(principal.revision);

  // The open principal identity defines the editing session. Same-principal
  // refetches must not replace an in-progress draft or its optimistic-lock
  // revision.
  // biome-ignore lint/correctness/useExhaustiveDependencies: open/id intentionally define the draft snapshot boundary
  useEffect(() => {
    if (!open) return;

    const snapshot = getDraftSnapshot(principal);
    initialDraftRef.current = snapshot;
    setEditingRevision(principal.revision);
    setEnabled(snapshot.enabled);
    setLead5m(snapshot.lead5m);
    setLead1h(snapshot.lead1h);
    setMaxRenewals(snapshot.maxRenewals);
    setMaxDuration(snapshot.maxDuration);
    setSnapshotBytes(snapshot.snapshotBytes);
    setExtraTools(snapshot.extraTools);
    setTreatAmbiguous(snapshot.treatAmbiguous);
    setNewTool('');
  }, [open, principal.id]);

  const reset = () => {
    const snapshot = initialDraftRef.current;
    if (!snapshot) return;

    setEnabled(snapshot.enabled);
    setLead5m(snapshot.lead5m);
    setLead1h(snapshot.lead1h);
    setMaxRenewals(snapshot.maxRenewals);
    setMaxDuration(snapshot.maxDuration);
    setSnapshotBytes(snapshot.snapshotBytes);
    setExtraTools(snapshot.extraTools);
    setTreatAmbiguous(snapshot.treatAmbiguous);
    setNewTool('');
  };

  const handleSave = () => {
    if (busy) return;
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
        expected_revision: editingRevision,
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
          if (
            error instanceof ApiError &&
            (error.status === 412 ||
              (error.status === 409 &&
                (error.code === 'stale_revision' ||
                  error.code === 'storage_conflict')))
          ) {
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

  // The tool field is a framed token box: a tap anywhere inside the frame —
  // padding, gaps, a chip's text — should put the caret in the input, so the
  // whole frame is the touch target. Buttons inside keep their own behavior.
  const focusToolInput = (e: React.PointerEvent<HTMLDivElement>) => {
    if ((e.target as HTMLElement).closest('input, button')) return;
    e.preventDefault();
    e.currentTarget.querySelector('input')?.focus();
  };

  return (
    <Drawer
      open={open}
      onOpenChange={handleOpenChange}
      title="Cache keepalive settings"
      description={principal.name}
      width="md"
      footer={
        <>
          <Button variant="ghost" onClick={reset} disabled={busy}>
            Reset
          </Button>
          <Button
            variant="primary"
            loading={saving}
            disabled={busy}
            onClick={handleSave}
          >
            {saving ? 'Saving...' : 'Save changes'}
          </Button>
        </>
      }
    >
      <fieldset
        aria-busy={saving}
        className="min-w-0 p-4 space-y-10"
        data-testid="cache-keepalive-settings-form"
        disabled={busy}
      >
        <ToggleSwitch
          role="switch"
          label="Cache keepalive enabled"
          description="Renew the prompt-cache TTL for this principal's sessions."
          aria-label="Cache keepalive enabled"
          checked={enabled}
          onChange={(e) => setEnabled(e.target.checked)}
          disabled={busy}
        />

        <div className="space-y-5">
          <h3 className="text-title-section text-text">Renewal</h3>
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-x-6 gap-y-5">
            <Field
              label="Renewal lead time · 5m TTL"
              hint={`Seconds. Renews ${formatSecs(lead5m)} before the 5m cache expires.`}
            >
              <input
                type="number"
                min={0}
                value={lead5m}
                onChange={(e) => setLead5m(Number(e.target.value))}
                disabled={busy}
                className={cx(INPUT_CLASS, PENDING_INPUT_CLASS)}
              />
            </Field>

            <Field
              label="Renewal lead time · 1h TTL"
              hint={`Seconds. Renews ${formatSecs(lead1h)} before the 1h cache expires.`}
            >
              <input
                type="number"
                min={0}
                value={lead1h}
                onChange={(e) => setLead1h(Number(e.target.value))}
                disabled={busy}
                className={cx(INPUT_CLASS, PENDING_INPUT_CLASS)}
              />
            </Field>

            <Field label="Max renewals per session">
              <input
                type="number"
                min={0}
                value={maxRenewals}
                onChange={(e) => setMaxRenewals(Number(e.target.value))}
                disabled={busy}
                className={cx(INPUT_CLASS, PENDING_INPUT_CLASS)}
              />
            </Field>

            <Field
              label="Max total duration"
              hint={`Seconds. = ${formatSecs(maxDuration)}`}
            >
              <input
                type="number"
                min={0}
                value={maxDuration}
                onChange={(e) => setMaxDuration(Number(e.target.value))}
                disabled={busy}
                className={cx(INPUT_CLASS, PENDING_INPUT_CLASS)}
              />
            </Field>

            <Field
              label="Snapshot max bytes"
              hint={`= ${formatBytes(snapshotBytes)}`}
            >
              <input
                type="number"
                min={0}
                value={snapshotBytes}
                onChange={(e) => setSnapshotBytes(Number(e.target.value))}
                disabled={busy}
                className={cx(INPUT_CLASS, PENDING_INPUT_CLASS)}
              />
            </Field>
          </div>
        </div>

        <div className="space-y-5">
          <h3 className="text-title-section text-text">Classifier</h3>
          <div className="flex flex-col gap-1.5">
            <label htmlFor={toolInputId} className="text-label text-text-muted">
              Extra wait-for-user tools
            </label>
            <div
              onPointerDown={focusToolInput}
              className="flex flex-wrap gap-1.5 p-1.5 min-h-9 max-md:min-h-10 cursor-text bg-input-bg border border-subtle-strong rounded-sm focus-within:outline-2 focus-within:outline-accent focus-within:outline-offset-1"
            >
              {extraTools.map((tool) => (
                <span
                  key={tool}
                  className="inline-flex h-6 items-center gap-0.5 pl-2 font-mono text-data rounded-xs bg-overlay-4 text-text"
                >
                  {tool}
                  <button
                    type="button"
                    onClick={() => removeTool(tool)}
                    aria-label={`Remove ${tool}`}
                    disabled={busy}
                    className={cx(
                      'relative inline-flex h-6 w-6 items-center justify-center rounded-xs text-text-muted transition-colors hover:bg-overlay-5 hover:text-text disabled:hover:bg-transparent disabled:hover:text-text-muted',
                      // Phones: a ::before pad extends the 24px glyph button to a
                      // 40px tap target without growing the chip's layout.
                      'max-md:before:absolute max-md:before:-inset-2 max-md:before:content-[""]',
                      'focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-1',
                      PENDING_INPUT_CLASS,
                    )}
                  >
                    <X className="w-3 h-3" aria-hidden="true" />
                  </button>
                </span>
              ))}
              <input
                id={toolInputId}
                type="text"
                value={newTool}
                onChange={(e) => setNewTool(e.target.value)}
                onKeyDown={handleAddTool}
                disabled={busy}
                aria-describedby={toolHintId}
                className={cx(
                  'flex-1 min-w-[100px] px-1 bg-transparent outline-none text-body text-text placeholder:text-text-faint',
                  // Phones: fill the field frame's 40px so the input's box is
                  // the tap target, matching the frame's 6px padding top/bottom.
                  'max-md:h-10 max-md:-my-1.5',
                  PENDING_INPUT_CLASS,
                )}
                placeholder="Add tool..."
              />
            </div>
            <span id={toolHintId} className="text-caption text-text-faint">
              Press Enter to add a tool name. Config key{' '}
              <code className="font-mono text-data">
                extra_wait_for_user_tools
              </code>
            </span>
          </div>

          <ToggleSwitch
            role="switch"
            label="Treat end of turn as ambiguous"
            description={
              <>
                Config key{' '}
                <code className="font-mono text-data">
                  treat_end_turn_as_ambiguous
                </code>
              </>
            }
            aria-label="Treat end of turn as ambiguous"
            checked={treatAmbiguous}
            onChange={(e) => setTreatAmbiguous(e.target.checked)}
            disabled={busy}
          />
        </div>
      </fieldset>
    </Drawer>
  );
}

function formatSecs(secs: number): string {
  if (!Number.isFinite(secs) || secs < 0) return '—';
  if (secs >= 3600 && secs % 3600 === 0) return `${secs / 3600}h`;
  if (secs >= 60 && secs % 60 === 0) return `${secs / 60}m`;
  return `${secs}s`;
}

function formatBytes(bytes: number): string {
  if (!Number.isFinite(bytes) || bytes < 0) return '—';
  if (bytes >= 1024 * 1024 && bytes % (1024 * 1024) === 0)
    return `${bytes / (1024 * 1024)} MiB`;
  if (bytes >= 1024 && bytes % 1024 === 0) return `${bytes / 1024} KiB`;
  return `${bytes.toLocaleString('en-US')} B`;
}
