import { X } from 'lucide-react';
import { useEffect, useId, useRef, useState } from 'react';
import { toast } from 'sonner';
import { ApiError } from '../../../lib/api';
import {
  type Principal,
  usePrincipals,
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
  lead5m: number | null;
  lead1h: number | null;
  maxRenewals: number | null;
  maxDuration: number | null;
  snapshotBytes: number | null;
  extraTools: string[];
  treatAmbiguous: boolean;
  openaiResponsesEnabled: boolean;
}

function getDraftSnapshot(principal: Principal): CacheKeepaliveDraftSnapshot {
  const config = principal.cache_keepalive;
  return {
    enabled: config?.enabled ?? false,
    lead5m: config?.refresh_lead_time_5m_secs ?? 30,
    lead1h: config?.refresh_lead_time_1h_secs ?? 300,
    maxRenewals: config?.max_refreshes_per_session ?? 4,
    maxDuration: config?.max_total_duration_secs ?? 14400,
    snapshotBytes: config?.snapshot_max_bytes ?? 8_388_608,
    extraTools: [...(config?.classifier?.extra_wait_for_user_tools ?? [])],
    treatAmbiguous: config?.classifier?.treat_end_turn_as_ambiguous ?? false,
    openaiResponsesEnabled:
      config?.classifier?.openai_responses_enabled ?? false,
  };
}
export function CacheKeepaliveSettingsDrawer({
  open,
  onOpenChange,
  principal,
}: Props) {
  const updateMutation = useUpdatePrincipalCacheKeepalive();
  const principalsQuery = usePrincipals(false);
  const principalWritePending = usePrincipalWritePending(principal.id) > 0;
  const saving = updateMutation.isPending;
  const [conflict, setConflict] = useState(false);
  const [conflictPrincipal, setConflictPrincipal] = useState<Principal | null>(
    null,
  );
  const editingBasePrincipalRef = useRef(principal);
  const busy = saving || principalWritePending;

  // A save in flight owns the drawer: backdrop clicks, Escape and the X control
  // must not discard the draft or hide the outcome mid-request. The success path
  // calls `onOpenChange` directly, so it is never swallowed here.
  const handleOpenChange = (next: boolean) => {
    if (saving && !next) return;
    onOpenChange(next);
  };

  const [enabled, setEnabled] = useState(false);
  const [lead5m, setLead5m] = useState<number | null>(30);
  const [lead1h, setLead1h] = useState<number | null>(300);
  const [maxRenewals, setMaxRenewals] = useState<number | null>(4);
  const [maxDuration, setMaxDuration] = useState<number | null>(14400);
  const [snapshotBytes, setSnapshotBytes] = useState<number | null>(8_388_608);
  const [extraTools, setExtraTools] = useState<string[]>([]);
  const [treatAmbiguous, setTreatAmbiguous] = useState(false);
  const [openaiResponsesEnabled, setOpenaiResponsesEnabled] = useState(false);
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
    editingBasePrincipalRef.current = principal;
    initialDraftRef.current = snapshot;
    setEditingRevision(principal.revision);
    setConflict(false);
    setConflictPrincipal(null);
    setEnabled(snapshot.enabled);
    setLead5m(snapshot.lead5m);
    setLead1h(snapshot.lead1h);
    setMaxRenewals(snapshot.maxRenewals);
    setMaxDuration(snapshot.maxDuration);
    setSnapshotBytes(snapshot.snapshotBytes);
    setExtraTools(snapshot.extraTools);
    setTreatAmbiguous(snapshot.treatAmbiguous);
    setOpenaiResponsesEnabled(snapshot.openaiResponsesEnabled);
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
    setOpenaiResponsesEnabled(snapshot.openaiResponsesEnabled);
    setNewTool('');
  };
  const latestPrincipal =
    conflictPrincipal ??
    principalsQuery.data?.principals.find((item) => item.id === principal.id);

  const applyLatestPrincipal = (latest: Principal, preserveDraft: boolean) => {
    const initial = initialDraftRef.current;
    const server = getDraftSnapshot(latest);
    const current = {
      enabled,
      lead5m,
      lead1h,
      maxRenewals,
      maxDuration,
      snapshotBytes,
      extraTools,
      treatAmbiguous,
      openaiResponsesEnabled,
    };
    const next =
      preserveDraft && initial
        ? {
            enabled:
              current.enabled !== initial.enabled
                ? current.enabled
                : server.enabled,
            lead5m:
              current.lead5m !== initial.lead5m
                ? current.lead5m
                : server.lead5m,
            lead1h:
              current.lead1h !== initial.lead1h
                ? current.lead1h
                : server.lead1h,
            maxRenewals:
              current.maxRenewals !== initial.maxRenewals
                ? current.maxRenewals
                : server.maxRenewals,
            maxDuration:
              current.maxDuration !== initial.maxDuration
                ? current.maxDuration
                : server.maxDuration,
            snapshotBytes:
              current.snapshotBytes !== initial.snapshotBytes
                ? current.snapshotBytes
                : server.snapshotBytes,
            extraTools:
              current.extraTools.join('\u0000') !==
              initial.extraTools.join('\u0000')
                ? current.extraTools
                : server.extraTools,
            treatAmbiguous:
              current.treatAmbiguous !== initial.treatAmbiguous
                ? current.treatAmbiguous
                : server.treatAmbiguous,
            openaiResponsesEnabled:
              current.openaiResponsesEnabled !== initial.openaiResponsesEnabled
                ? current.openaiResponsesEnabled
                : server.openaiResponsesEnabled,
          }
        : server;

    editingBasePrincipalRef.current = latest;
    setEditingRevision(latest.revision);
    setEnabled(next.enabled);
    setLead5m(next.lead5m);
    setLead1h(next.lead1h);
    setMaxRenewals(next.maxRenewals);
    setMaxDuration(next.maxDuration);
    setSnapshotBytes(next.snapshotBytes);
    setExtraTools(next.extraTools);
    setTreatAmbiguous(next.treatAmbiguous);
    setOpenaiResponsesEnabled(next.openaiResponsesEnabled);
    if (!preserveDraft) initialDraftRef.current = server;
    setConflict(false);
    setConflictPrincipal(null);
  };

  const refreshLatestPrincipal = async () => {
    const result = await principalsQuery.refetch();
    const latest = result.data?.principals.find(
      (item) => item.id === principal.id,
    );
    if (latest) setConflictPrincipal(latest);
  };

  const resetToLatest = () => {
    if (latestPrincipal) applyLatestPrincipal(latestPrincipal, false);
  };

  const handleSave = () => {
    if (busy || conflict) return;
    if (
      lead5m === null ||
      lead1h === null ||
      maxRenewals === null ||
      maxDuration === null ||
      snapshotBytes === null ||
      ![lead5m, lead1h, maxRenewals, maxDuration, snapshotBytes].every(
        (value) => Number.isFinite(value) && value >= 0,
      )
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
          min_cache_tokens:
            editingBasePrincipalRef.current.cache_keepalive?.min_cache_tokens ??
            10_000,
          classifier: {
            extra_wait_for_user_tools: extraTools,
            treat_end_turn_as_ambiguous: treatAmbiguous,
            openai_responses_enabled: openaiResponsesEnabled,
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
            setConflict(true);
            void refreshLatestPrincipal();
            toast.error(
              'Principal was modified by another user. Review the latest revision before saving again.',
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
            disabled={busy || conflict}
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
        {conflict ? (
          <div
            className="flex flex-col gap-2 rounded-sm bg-warn/10 p-3 text-body-sm"
            role="alert"
          >
            <p className="text-warn-text">
              This principal changed while you were editing. Your edits are
              preserved; choose how to recover before saving.
            </p>
            <div className="flex flex-wrap gap-2">
              <Button
                variant="secondary"
                size="sm"
                disabled={!latestPrincipal}
                onClick={() => {
                  if (latestPrincipal) {
                    applyLatestPrincipal(latestPrincipal, true);
                  }
                }}
              >
                Use latest revision
              </Button>
              <Button
                variant="ghost"
                size="sm"
                disabled={!latestPrincipal}
                onClick={resetToLatest}
              >
                Reset to latest config
              </Button>
            </div>
          </div>
        ) : null}
        <ToggleSwitch
          role="switch"
          label="Cache keepalive enabled"
          description="Configure supported provider sessions for this principal. OpenAI Responses 30m renewal requires GPT-5.6+; 12h retention is recorded without read re-anchoring."
          aria-label="Cache keepalive enabled"
          checked={enabled}
          onChange={(e) => setEnabled(e.target.checked)}
          disabled={busy}
        />

        <div className="space-y-5">
          <h3 className="text-title-section text-text">Renewal</h3>
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-x-6 gap-y-5">
            <Field
              label="Renewal lead time · 5m / 30m"
              hint={`Seconds. Renews ${formatSecs(lead5m)} before an Anthropic 5m or OpenAI 30m cache expires.`}
            >
              <input
                type="number"
                min={0}
                value={lead5m ?? ''}
                onChange={(e) => setLead5m(parseDraftNumber(e.target.value))}
                disabled={busy}
                className={cx(INPUT_CLASS, PENDING_INPUT_CLASS)}
              />
            </Field>

            <Field
              label="Renewal lead time · 1h / 12h"
              hint={`Seconds. Renews ${formatSecs(lead1h)} before an Anthropic 1h cache expires. OpenAI 12h retention is recorded, not renewed.`}
            >
              <input
                type="number"
                min={0}
                value={lead1h ?? ''}
                onChange={(e) => setLead1h(parseDraftNumber(e.target.value))}
                disabled={busy}
                className={cx(INPUT_CLASS, PENDING_INPUT_CLASS)}
              />
            </Field>

            <Field label="Max renewals per session">
              <input
                type="number"
                min={0}
                value={maxRenewals ?? ''}
                onChange={(e) =>
                  setMaxRenewals(parseDraftNumber(e.target.value))
                }
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
                value={maxDuration ?? ''}
                onChange={(e) =>
                  setMaxDuration(parseDraftNumber(e.target.value))
                }
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
                value={snapshotBytes ?? ''}
                onChange={(e) =>
                  setSnapshotBytes(parseDraftNumber(e.target.value))
                }
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
          <ToggleSwitch
            role="switch"
            label="OpenAI Responses GPT-5.6+ keepalive"
            description="Opt in to cost-aware 30m prompt-cache read refreshes for OpenAI Responses only. A 12h retention is recorded without read re-anchoring and is not periodically renewed. Off by default."
            aria-label="OpenAI Responses GPT-5.6+ keepalive"
            checked={openaiResponsesEnabled}
            onChange={(e) => setOpenaiResponsesEnabled(e.target.checked)}
            disabled={busy}
          />
        </div>
      </fieldset>
    </Drawer>
  );
}

function formatSecs(secs: number | null): string {
  if (secs === null || !Number.isFinite(secs) || secs < 0) return '—';
  if (secs >= 3600 && secs % 3600 === 0) return `${secs / 3600}h`;
  if (secs >= 60 && secs % 60 === 0) return `${secs / 60}m`;
  return `${secs}s`;
}

function formatBytes(bytes: number | null): string {
  if (bytes === null || !Number.isFinite(bytes) || bytes < 0) return '—';
  if (bytes >= 1024 * 1024 && bytes % (1024 * 1024) === 0)
    return `${bytes / (1024 * 1024)} MiB`;
  if (bytes >= 1024 && bytes % 1024 === 0) return `${bytes / 1024} KiB`;
  return `${bytes.toLocaleString('en-US')} B`;
}
function parseDraftNumber(value: string): number | null {
  return value.trim() === '' ? null : Number(value);
}
