// What an `anthropic_api_key` upstream's key can do at Anthropic, as of the
// last probe. The description is the credential summary (key kind · when it
// was checked); the header carries one status badge plus Re-check and
// Replace key. Blocking key states (rejected, never checked, last check
// failed) are owned by the pane's key notice, so Access hands it the matching
// action and keeps only the badge. The body is label / value rows: the
// effective Endpoint always, then — only for a usable key — Workspace and
// Usage data, whose one nudge line is the single owner of the
// "get reported usage" fix. Only org-scoped keys get a workspace Change.

import { KeyRound } from 'lucide-react';
import { type ReactNode, useState } from 'react';
import { toast } from 'sonner';
import {
  API_KEY_ACCESS_NOT_CHECKED,
  ApiError,
  type ApiKeyAccess,
  type Upstream,
} from '../../lib/api';
import {
  useRecheckApiKey,
  useUpdateUpstream,
  useUpstreamWorkspaces,
} from '../../lib/queries';
import { undoToast } from '../../lib/undoToast';
import { DetailRow, DetailRows, DetailSection } from '../ui/DetailPane';
import {
  Button,
  cx,
  INPUT_SM_CLASS,
  Notice,
  StatusBadge,
} from '../ui/primitives';
import { RelativeTime } from '../ui/RelativeTime';
import { Select } from '../ui/Select';
import { apiKeyNoticeOf, apiKeyUsable } from './ApiKeyNotice';
import {
  isAnthropicBaseUrl,
  KEY_TYPE_LABEL,
  shortWorkspaceId,
  usageFix,
} from './connect/apiKeyCopy';
import {
  ANCHOR_CLASS,
  QUIET_LINK_CLASS,
  WorkspaceIdHelp,
} from './connect/parts';
import { errorMessage } from './connect/useOAuthConnectSession';
import { EndpointRow } from './EndpointRow';
import { isStaleRevision, STALE_EDITOR_MESSAGE } from './staleRevision';

/** `Service account key`, `All workspaces key`, … or null when unknown. */
function keyKindLabel(access: ApiKeyAccess): string | null {
  if (access.key_type) return `${KEY_TYPE_LABEL[access.key_type]} key`;
  if (access.key_class === 'org_scoped') return 'All workspaces key';
  if (access.key_class === 'workspace_bound') return 'Workspace key';
  return null;
}

/** Credential summary: `Service account key · checked 3 hours ago`. */
function accessSummary(access: ApiKeyAccess): ReactNode {
  const kind = keyKindLabel(access);
  const at = access.checked_at_unix_secs;
  const verb = access.verified ? 'checked' : 'last attempt';
  const check =
    at === null ? null : (
      <>
        {kind ? verb : verb.charAt(0).toUpperCase() + verb.slice(1)}{' '}
        <RelativeTime ts={at * 1000} />
      </>
    );
  if (check === null) return kind ? `${kind} · never checked` : 'Never checked';
  return kind ? (
    <>
      {kind} · {check}
    </>
  ) : (
    check
  );
}

export function ApiKeyAccessSection({
  upstream,
  workspaceEditing,
  onWorkspaceEditingChange,
  endpointEditing,
  onEndpointEditingChange,
  onReplace,
  usageReportsStopped = false,
  usageAttribution = 'api_key',
}: {
  upstream: Upstream;
  /**
   * Controlled Workspace and Endpoint editors, so the pane's parent can keep
   * one inline editor (and one primary Save) open at a time. Uncontrolled if
   * omitted.
   */
  workspaceEditing?: boolean;
  onWorkspaceEditingChange?: (editing: boolean) => void;
  endpointEditing?: boolean;
  onEndpointEditingChange?: (editing: boolean) => void;
  /** Opens the pane's Replace key dialog (one dialog for the whole pane). */
  onReplace: () => void;
  /** Anthropic usage reports worked before and now fail. */
  usageReportsStopped?: boolean;
  /** Whose figures Anthropic reports: this key's, or the whole workspace's. */
  usageAttribution?: 'api_key' | 'workspace';
}) {
  const access = upstream.api_key_access ?? API_KEY_ACCESS_NOT_CHECKED;
  const recheck = useRecheckApiKey();
  const [ownEditing, setOwnEditing] = useState(false);
  const editing = workspaceEditing ?? ownEditing;
  const setEditing = (next: boolean) => {
    setOwnEditing(next);
    onWorkspaceEditingChange?.(next);
  };
  const [ownEndpointEditing, setOwnEndpointEditing] = useState(false);
  const editingEndpoint = endpointEditing ?? ownEndpointEditing;
  const setEndpointEditing = (next: boolean) => {
    setOwnEndpointEditing(next);
    onEndpointEditingChange?.(next);
  };
  const ws = access.workspace;
  const orgScoped = access.key_class === 'org_scoped';
  const foreignBase = !isAnthropicBaseUrl(upstream.base_url ?? '');
  /** A rejected or unchecked key has no capabilities worth listing. */
  const keyUsable = apiKeyUsable(access);
  // The pane key notice owns the action that fixes its state.
  const notice = apiKeyNoticeOf(access);
  const noticeOwnsRecheck =
    notice === 'never_checked' || notice === 'check_failed';
  const recheckNow = () => recheck.mutate(upstream.id);

  const status =
    access.inference === 'denied' ? (
      <StatusBadge tone="danger" label="Key rejected" />
    ) : access.verified && access.inference === 'ok' ? (
      <StatusBadge tone="ok" label="Key accepted" />
    ) : (
      <StatusBadge tone="warn" label="Key not verified" />
    );
  const headerActions = (
    <div className="flex flex-wrap items-center gap-2">
      <span role="status">{status}</span>
      {noticeOwnsRecheck ? null : (
        <Button
          size="sm"
          variant="ghost"
          loading={recheck.isPending}
          onClick={recheckNow}
        >
          {recheck.isPending ? 'Checking…' : 'Re-check'}
        </Button>
      )}
      {notice === 'rejected' ? null : (
        <Button size="sm" iconLeft={<KeyRound />} onClick={onReplace}>
          Replace key
        </Button>
      )}
    </div>
  );

  const shortId = ws.id ? (
    <span className="font-mono text-data text-text-muted" title={ws.id}>
      {shortWorkspaceId(ws.id)}
    </span>
  ) : null;
  const workspaceValue =
    ws.mode === 'required' ? (
      <StatusBadge tone="warn" label="Not chosen" />
    ) : ws.mode === 'bound' ? (
      <>
        {shortId}
        <span className="mt-0.5 block text-caption text-text-faint">
          Set by the key
        </span>
      </>
    ) : ws.name ? (
      <span className="flex flex-wrap items-baseline gap-x-2">
        <span>{ws.name}</span>
        {shortId}
      </span>
    ) : (
      shortId
    );
  const changeWorkspace = orgScoped ? (
    <button
      type="button"
      className={QUIET_LINK_CLASS}
      aria-label="Change workspace"
      onClick={() => setEditing(true)}
    >
      Change
    </button>
  ) : undefined;

  const inlineLink = (label: string, onClick: () => void) => (
    <button type="button" className={ANCHOR_CLASS} onClick={onClick}>
      {label}
    </button>
  );
  const fix = usageFix(access.key_class);
  const [usageBadge, usageLine]: [ReactNode, ReactNode] = usageReportsStopped
    ? [
        <StatusBadge key="b" tone="warn" label="Stopped" />,
        <span key="l" className="text-text-faint">
          Showing the last report.
        </span>,
      ]
    : access.usage_report === 'ok'
      ? [
          <StatusBadge key="b" tone="ok" label="Reported" />,
          <span key="l" className="text-text-faint">
            {usageAttribution === 'workspace'
              ? 'Full access · whole-workspace figures from Anthropic'
              : "Full access · Anthropic's own figures, every 5 min"}
          </span>,
        ]
      : [
          <StatusBadge key="b" tone="neutral" label="Estimated" />,
          <span key="l" className="text-text-muted">
            {foreignBase ? (
              <>
                {inlineLink("Use Anthropic's endpoint", () =>
                  setEndpointEditing(true),
                )}{' '}
                → get reported usage.
              </>
            ) : access.usage_report === 'error' ? (
              <>
                Anthropic didn't answer
                {recheck.isPending ? null : (
                  <>
                    {' · '}
                    {inlineLink('Re-check', recheckNow)} → get reported usage.
                  </>
                )}
              </>
            ) : fix.inApp ? (
              <>
                {inlineLink(fix.action, onReplace)} → {fix.result}
              </>
            ) : (
              `${fix.action} → ${fix.result}`
            )}
          </span>,
        ];

  const workspaceNotice =
    keyUsable && orgScoped && ws.mode === 'required' && !editing;

  return (
    <DetailSection
      span="full"
      title="Access"
      description={accessSummary(access)}
      data-testid="api-key-access"
      action={headerActions}
    >
      <div className="flex flex-col gap-3">
        {recheck.error ? (
          <p role="alert" className="text-caption text-danger-text">
            Re-check failed: {errorMessage(recheck.error)}
          </p>
        ) : null}
        {workspaceNotice ? (
          <Notice
            tone="warning"
            title="No workspace chosen"
            action={
              <Button size="sm" onClick={() => setEditing(true)}>
                Choose workspace
              </Button>
            }
          >
            Choose one → finish setting up this key.
          </Notice>
        ) : null}
        <DetailRows>
          <EndpointRow
            upstream={upstream}
            kind="api_key"
            editing={editingEndpoint}
            onEditingChange={setEndpointEditing}
          />
          {keyUsable ? (
            <>
              {editing && orgScoped ? (
                <WorkspaceEditor
                  upstream={upstream}
                  access={access}
                  onClose={() => setEditing(false)}
                />
              ) : (
                <DetailRow label="Workspace" action={changeWorkspace}>
                  {workspaceValue}
                </DetailRow>
              )}
              <DetailRow
                label="Usage data"
                data-testid="api-key-usage-mode"
                tabIndex={-1}
                className="outline-none"
              >
                <div className="flex flex-col items-start gap-1">
                  {usageBadge}
                  <span className="text-caption">{usageLine}</span>
                </div>
              </DetailRow>
            </>
          ) : null}
        </DetailRows>
        {keyUsable ? null : (
          <p className="text-body-sm text-text-muted">
            Workspace and usage reports appear after a successful check.
          </p>
        )}
      </div>
    </DetailSection>
  );
}

/** The workspace editor (org-scoped keys); mounted only while choosing. */
function WorkspaceEditor({
  upstream,
  access,
  onClose,
}: {
  upstream: Upstream;
  access: ApiKeyAccess;
  onClose: () => void;
}) {
  const ws = access.workspace;
  const [draft, setDraft] = useState(
    ws.mode === 'selected' ? (ws.id ?? '') : '',
  );
  // The revision the draft was opened against. Saving with the refreshed
  // prop's revision would let this draft overwrite another admin's change;
  // the opening revision makes the server answer stale_revision instead.
  const [draftRevision] = useState(upstream.spec_revision);
  const [error, setError] = useState<string | null>(null);
  /** Typing the id instead, after the list failed to load. */
  const [manualEntry, setManualEntry] = useState(false);
  const update = useUpdateUpstream({ inlineError: true });
  // Listing runs against the stored key, only while choosing.
  const listQ = useUpstreamWorkspaces(
    access.workspace_list === 'ok' ? upstream.id : null,
  );
  const listed = listQ.data?.status === 'ok' ? listQ.data.workspaces : null;
  /** The list endpoint's own refusal (`denied` / `not_applicable`), if any. */
  const listRefused =
    listQ.data && listQ.data.status !== 'ok' ? listQ.data.status : null;
  const manual =
    manualEntry || access.workspace_list !== 'ok' || listRefused !== null;
  const listFailed = !manual && listQ.isError;

  const apply = (id: string | null, onSaved: (updated: Upstream) => void) => {
    update.mutate(
      {
        id: upstream.id,
        body: { workspace_id: id },
        spec_revision: draftRevision,
      },
      {
        onSuccess: onSaved,
        onError: (e) => {
          // §5 codes from the server's re-probe; status alone can't tell a
          // malformed id from one the key can't see.
          const code = e instanceof ApiError ? e.code : null;
          setError(
            isStaleRevision(e)
              ? STALE_EDITOR_MESSAGE
              : code === 'workspace_id_malformed'
                ? 'Workspace IDs start with wrkspc_.'
                : code === 'workspace_not_found'
                  ? `This key can't see workspace ${id} — check the id, or the key's account lacks access.`
                  : code === 'key_invalid'
                    ? 'Anthropic rejected the stored key. Replace it → pick a workspace.'
                    : code === 'probe_unreachable'
                      ? `Couldn't reach ${isAnthropicBaseUrl(upstream.base_url ?? '') ? 'Anthropic' : 'your endpoint'}. Save again → check the workspace.`
                      : errorMessage(e),
          );
        },
      },
    );
  };

  const save = () => {
    // One re-probe per save: Enter in the id field must not repeat the PUT.
    if (update.isPending) return;
    const id = draft.trim();
    if (!id.startsWith('wrkspc_')) {
      setError('Workspace IDs start with wrkspc_.');
      return;
    }
    const previous = ws.mode === 'selected' ? (ws.id ?? null) : null;
    const label =
      listed?.find((w) => w.id === id)?.name ?? shortWorkspaceId(id);
    apply(id, (updated) => {
      onClose();
      // `workspace_id: null` would keep the new id, and an org-scoped key
      // cannot be cleared, so undo exists only back to a previous id.
      if (previous === null) {
        toast.success(`Workspace set to ${label}`);
        return;
      }
      undoToast({
        message: `Workspace set to ${label}`,
        // The server re-probes the previous id; this mutation is inline-error
        // (no global toast) and the editor is closed — unmounted, so a
        // per-call `onError` would never fire. The promise outlives it.
        onUndo: () =>
          update
            .mutateAsync({
              id: upstream.id,
              body: { workspace_id: previous },
              spec_revision: updated.spec_revision,
            })
            .catch((e: unknown) => {
              toast.error(
                `Undo failed: ${errorMessage(e)}. Change → pick the previous workspace.`,
              );
            }),
      });
    });
  };
  return (
    <DetailRow
      label="Workspace"
      action={
        <>
          <Button size="sm" disabled={update.isPending} onClick={onClose}>
            Cancel
          </Button>
          <Button
            size="sm"
            variant="primary"
            loading={update.isPending}
            disabled={draft.trim() === ''}
            onClick={save}
          >
            {update.isPending ? 'Checking…' : 'Save'}
          </Button>
        </>
      }
    >
      <div className="flex max-w-md flex-col gap-1.5">
        {manual ? (
          <>
            <input
              aria-label="Workspace ID"
              aria-invalid={error ? true : undefined}
              className={cx(
                INPUT_SM_CLASS,
                'font-mono aria-busy:cursor-progress',
              )}
              value={draft}
              autoFocus
              spellCheck={false}
              placeholder="wrkspc_…"
              // Read-only, not disabled, while the save runs: focus stays put.
              readOnly={update.isPending}
              aria-busy={update.isPending || undefined}
              onChange={(e) => {
                setDraft(e.target.value);
                setError(null);
              }}
              onKeyDown={(e) => {
                if (e.key === 'Enter') save();
              }}
            />
            <WorkspaceIdHelp list={listRefused ?? access.workspace_list} />
          </>
        ) : listFailed ? (
          <Notice
            tone="warning"
            role="alert"
            action={
              <>
                <Button
                  size="sm"
                  loading={listQ.isFetching}
                  onClick={() => void listQ.refetch()}
                >
                  Retry
                </Button>
                <Button
                  size="sm"
                  variant="ghost"
                  onClick={() => setManualEntry(true)}
                >
                  Enter ID
                </Button>
              </>
            }
          >
            Couldn't load workspaces: {errorMessage(listQ.error)}
          </Notice>
        ) : (
          <Select
            aria-label="Workspace"
            size="sm"
            className="w-full"
            value={draft}
            placeholder={listQ.isPending ? 'Loading workspaces…' : 'Select…'}
            disabled={listQ.isPending}
            aria-invalid={error ? true : undefined}
            options={(listed ?? []).map((w) => ({
              value: w.id,
              label: w.name,
              hint: (
                <span className="font-mono text-data">
                  {shortWorkspaceId(w.id)}
                </span>
              ),
            }))}
            onChange={(value) => {
              setDraft(value);
              setError(null);
            }}
          />
        )}
        {error ? (
          <span role="alert" className="text-caption text-danger-text">
            {error}
          </span>
        ) : null}
      </div>
    </DetailRow>
  );
}
