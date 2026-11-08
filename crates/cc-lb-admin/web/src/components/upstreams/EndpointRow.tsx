// Where an upstream sends requests, as one settings row shared by both kinds:
// Access row 1 on API-key panes, the Credential footer on OAuth panes. The
// effective endpoint is always shown — `Provider (default)` or the custom
// URL — with a quiet `Change` that swaps the row into its inline editor.
// Each provider's official base URL is the default and is never stored as a
// custom value.

import { useEffect, useRef, useState } from 'react';
import { toast } from 'sonner';
import { type Upstream, useUpdateUpstream } from '../../lib/queries';
import { undoToast } from '../../lib/undoToast';
import { DetailRow } from '../ui/DetailPane';
import { Button, cx, INPUT_SM_CLASS } from '../ui/primitives';
import { isAnthropicBaseUrl } from './connect/apiKeyCopy';
import { QUIET_LINK_CLASS } from './connect/parts';
import { errorMessage } from './connect/useOAuthConnectSession';
import { isStaleRevision, STALE_EDITOR_MESSAGE } from './staleRevision';

const CLEAR_UNCONFIRMED =
  'Server did not confirm the switch to the default endpoint; the custom URL may still be set.';

const DEFAULT_ENDPOINTS = {
  anthropic_api_key: {
    label: 'Anthropic',
    baseUrl: 'https://api.anthropic.com/',
  },
  anthropic_oauth: {
    label: 'Anthropic',
    baseUrl: 'https://api.anthropic.com/',
  },
  openai_api_key: {
    label: 'OpenAI',
    baseUrl: 'https://api.openai.com/',
  },
  openai_oauth: {
    label: 'OpenAI',
    baseUrl: 'https://chatgpt.com/backend-api/codex',
  },
  gemini_api_key: {
    label: 'Gemini',
    baseUrl: 'https://generativelanguage.googleapis.com/',
  },
  gemini_oauth: {
    label: 'Gemini',
    baseUrl: 'https://cloudcode-pa.googleapis.com/v1internal',
  },
} satisfies Record<Upstream['kind'], { label: string; baseUrl: string }>;

function isDefaultBaseUrl(baseUrl: string, defaultBaseUrl: string) {
  if (defaultBaseUrl === DEFAULT_ENDPOINTS.anthropic_api_key.baseUrl) {
    return isAnthropicBaseUrl(baseUrl);
  }

  const trimmed = baseUrl.trim();
  if (trimmed === '') return true;
  // URL drops empty query/hash markers, but those are not the bare provider
  // root and must remain editable custom URLs.
  if (trimmed.includes('?') || trimmed.includes('#')) return false;

  try {
    const actual = new URL(trimmed);
    const expected = new URL(defaultBaseUrl);
    return (
      actual.protocol === expected.protocol &&
      actual.hostname === expected.hostname &&
      actual.port === expected.port &&
      actual.pathname === expected.pathname &&
      actual.username === '' &&
      actual.password === ''
    );
  } catch {
    return false;
  }
}

export function EndpointRow({
  upstream,
  kind,
  editing: editingProp,
  onEditingChange,
}: {
  upstream: Upstream;
  kind: 'api_key' | 'oauth';
  /**
   * Controlled editor, so the pane's parent can keep one inline editor (and
   * one primary Save) open at a time. Uncontrolled if omitted.
   */
  editing?: boolean;
  onEditingChange?: (editing: boolean) => void;
}) {
  const [ownEditing, setOwnEditing] = useState(false);
  const editing = editingProp ?? ownEditing;
  const setEditing = (next: boolean) => {
    setOwnEditing(next);
    onEditingChange?.(next);
  };
  const endpoint = DEFAULT_ENDPOINTS[upstream.kind];
  const hint =
    kind === 'api_key'
      ? `Requests and key checks go here instead of ${endpoint.label}.`
      : `Requests go here instead of ${endpoint.label}.`;
  const custom =
    upstream.base_url != null &&
    !isDefaultBaseUrl(upstream.base_url, endpoint.baseUrl);
  const customUrl = custom ? (upstream.base_url ?? '') : '';
  const [baseUrl, setBaseUrl] = useState(customUrl);
  // Value and revision captured when editing began. Save intent is judged
  // against this baseline, not live props, so a background refetch can
  // never turn an untouched field into a change; saving against the opening
  // revision makes another admin's newer save a stale_revision refusal
  // instead of a silent overwrite.
  const baseUrlBaseline = useRef(customUrl);
  const revisionBaseline = useRef(upstream.spec_revision);
  const changeRef = useRef<HTMLButtonElement>(null);
  const refocusChange = useRef(false);
  // Failures render next to the input; Undo reports its own.
  const update = useUpdateUpstream({ inlineError: true });

  // Opening (from Change or from Access's nudge) starts from the stored URL.
  const wasEditing = useRef(editing);
  const specRevision = upstream.spec_revision;
  useEffect(() => {
    if (editing && !wasEditing.current) {
      setBaseUrl(customUrl);
      baseUrlBaseline.current = customUrl;
      revisionBaseline.current = specRevision;
      update.reset();
    }
    if (!editing && refocusChange.current) {
      refocusChange.current = false;
      changeRef.current?.focus();
    }
    wasEditing.current = editing;
  }, [editing, customUrl, specRevision, update]);

  const trimmed = baseUrl.trim();
  const unchanged = trimmed === baseUrlBaseline.current;

  const restore = (previous: string, specRevision: number) =>
    update
      .mutateAsync({
        id: upstream.id,
        body: { base_url: previous },
        spec_revision: specRevision,
      })
      .catch((e: unknown) => {
        toast.error(`Undo failed: ${errorMessage(e)}`);
      });

  /** Switch back to the provider default; undo restores the custom URL. */
  const switchToDefault = () => {
    const previous = custom ? upstream.base_url : null;
    update.mutate(
      {
        id: upstream.id,
        body: { clear_base_url: true },
        spec_revision: revisionBaseline.current,
      },
      {
        onSuccess: (updated) => {
          // Older backends ignore clear_base_url and still return the stored
          // URL; treat that as unconfirmed and keep the editor open.
          if (updated.base_url !== null) {
            toast.error(CLEAR_UNCONFIRMED);
            return;
          }
          refocusChange.current = true;
          setEditing(false);
          if (!previous) return;
          undoToast({
            message: 'Using the default endpoint',
            onUndo: () => void restore(previous, updated.spec_revision),
          });
        },
      },
    );
  };

  const save = () => {
    if (trimmed === '' || unchanged || update.isPending) return;
    // A provider's own URL is the default, never stored as a custom value.
    if (isDefaultBaseUrl(trimmed, endpoint.baseUrl)) {
      switchToDefault();
      return;
    }
    update.mutate(
      {
        id: upstream.id,
        body: { base_url: trimmed },
        spec_revision: revisionBaseline.current,
      },
      {
        onSuccess: () => {
          toast.success('Custom endpoint saved');
          refocusChange.current = true;
          setEditing(false);
        },
      },
    );
  };

  const cancel = () => {
    refocusChange.current = true;
    setEditing(false);
  };

  if (editing) {
    return (
      <DetailRow
        label="Endpoint"
        data-testid="upstream-endpoint-row"
        aria-busy={update.isPending}
        action={
          <>
            {custom ? (
              <Button
                size="sm"
                variant="ghost"
                disabled={update.isPending}
                onClick={switchToDefault}
              >
                Use default
              </Button>
            ) : null}
            <Button size="sm" disabled={update.isPending} onClick={cancel}>
              Cancel
            </Button>
            <Button
              size="sm"
              variant="primary"
              loading={update.isPending}
              disabled={update.isPending || trimmed === '' || unchanged}
              onClick={save}
            >
              {update.isPending ? 'Saving…' : 'Save'}
            </Button>
          </>
        }
      >
        <div className="flex max-w-md flex-col gap-1">
          <input
            type="text"
            aria-label="Base URL"
            aria-invalid={update.error ? true : undefined}
            className={cx(INPUT_SM_CLASS, 'font-mono')}
            value={baseUrl}
            autoFocus
            spellCheck={false}
            placeholder="https://proxy.example.com"
            disabled={update.isPending}
            onChange={(e) => setBaseUrl(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Enter') save();
            }}
          />
          <span className="text-caption text-text-faint">{hint}</span>
          {update.error ? (
            <span role="alert" className="text-caption text-danger-text">
              {isStaleRevision(update.error)
                ? STALE_EDITOR_MESSAGE
                : errorMessage(update.error)}
            </span>
          ) : null}
        </div>
      </DetailRow>
    );
  }

  return (
    <DetailRow
      label="Endpoint"
      data-testid="upstream-endpoint-row"
      action={
        <button
          ref={changeRef}
          type="button"
          className={QUIET_LINK_CLASS}
          aria-label="Change endpoint"
          onClick={() => setEditing(true)}
        >
          Change
        </button>
      }
    >
      {custom ? (
        <>
          <span className="break-all font-mono text-data text-text">
            {upstream.base_url}
          </span>
          <span className="mt-0.5 block text-caption text-text-faint">
            {hint}
          </span>
        </>
      ) : (
        <>
          {endpoint.label}
          <span className="text-text-muted"> (default)</span>
        </>
      )}
    </DetailRow>
  );
}
