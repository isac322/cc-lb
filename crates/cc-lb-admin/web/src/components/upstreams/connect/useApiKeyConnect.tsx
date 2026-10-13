// Engine and screens behind the connect dialog's API-key path. The operator's
// asset is the key, so it is asked for first and classified; only what the
// classification requires is asked next:
//
//   Key → (Workspace, org-scoped keys only) → Finish
//
// "Don't have a key?" inserts a Guide step before Key. The same machine runs
// in `replace` mode from an upstream's Access section, where the Kind step and the
// name are skipped and saving replaces the stored key (and workspace).

import { RadioGroup as BaseRadioGroup } from '@base-ui/react/radio-group';
import { KeyRound, Layers } from 'lucide-react';
import { type ClipboardEvent, type ReactNode, useState } from 'react';
import { toast } from 'sonner';
import {
  ApiError,
  type PreflightRequest,
  type PreflightResponse,
  REMEDIATION_CODES,
  type RemediationCode,
  type Upstream,
} from '../../../lib/api';
import { DEFAULT_ANTHROPIC_BASE_URL } from '../../../lib/constants';
import {
  useCreateUpstream,
  usePreflightApiKey,
  useUpdateUpstream,
} from '../../../lib/queries';
import { DetailFact, DetailFacts } from '../../ui/DetailPane';
import {
  Button,
  cx,
  Field,
  INPUT_CLASS,
  Notice,
  StatusBadge,
} from '../../ui/primitives';
import { Select } from '../../ui/Select';
import { isStaleRevision, STALE_EDITOR_MESSAGE } from '../staleRevision';
import {
  CONSOLE_KEYS_URL,
  isAnthropicBaseUrl,
  KEY_TYPE_LABEL,
  shortWorkspaceId,
  usageFixLine,
} from './apiKeyCopy';
import { parseApiKeyPaste } from './parseApiKeyPaste';
import {
  ANCHOR_CLASS,
  ConsoleLink,
  CustomEndpointDisclosure,
  KindOption,
  QUIET_LINK_CLASS,
  StepHeading,
  SuccessHeader,
  WorkspaceIdHelp,
} from './parts';
import { errorMessage } from './useOAuthConnectSession';

type ApiKeyStep = 'guide' | 'key' | 'workspace' | 'finish';
type GuideTier = 'full' | 'one';

/**
 * Why the Key step cannot move on: the §5 remediation codes the Key step
 * owns, each mapping to one row of the branch table.
 */
type KeyProblem = Extract<
  RemediationCode,
  | 'key_admin'
  | 'key_enterprise_scoped'
  | 'key_oat'
  | 'key_invalid'
  | 'key_forbidden'
  | 'probe_unreachable'
>;
const KEY_PROBLEMS: readonly KeyProblem[] = [
  'key_admin',
  'key_enterprise_scoped',
  'key_oat',
  'key_invalid',
  'key_forbidden',
  'probe_unreachable',
];

const PENDING_INPUT_CLASS =
  'disabled:cursor-not-allowed disabled:border-subtle disabled:text-text-faint';

/** Rejections whose fix is a new Console key: their notice opens the Guide. */
const GUIDE_PROBLEMS: readonly KeyProblem[] = [
  'key_admin',
  'key_enterprise_scoped',
  'key_invalid',
];

const STEP_META: Record<ApiKeyStep, { title: string; hint: string }> = {
  guide: { title: 'Create the key', hint: 'In Anthropic Console' },
  key: { title: 'Key', hint: 'Paste and check' },
  workspace: { title: 'Workspace', hint: 'Where requests bill' },
  finish: { title: 'Name & save', hint: 'Confirm what it can do' },
};

function problemOf(res: PreflightResponse): KeyProblem | null {
  if (res.verdict === 'ok' || res.verdict === 'needs_workspace') return null;
  const codes = new Set(res.remediation.map((r) => r.code));
  const known = KEY_PROBLEMS.find((code) => codes.has(code));
  if (known) return known;
  return res.verdict === 'unreachable' ? 'probe_unreachable' : 'key_invalid';
}

function isRemediationCode(code: string | null): code is RemediationCode {
  return (
    code !== null && (REMEDIATION_CODES as readonly string[]).includes(code)
  );
}

/**
 * The server's reason when it refused the key's characters or length
 * (`invalid_input` on `api_key_value`): the Key field's error, not a
 * request failure, and retrying the same value can never succeed.
 */
function keyInputReason(error: unknown): string | null {
  if (!(error instanceof ApiError) || error.code !== 'invalid_input') {
    return null;
  }
  const { body } = error;
  const field =
    body && typeof body === 'object' && 'field' in body ? body.field : null;
  return field === 'api_key_value' ? error.message : null;
}

/** A save the server refused for a reason no step owns. */
function saveErrorMessage(error: unknown): string {
  if (isStaleRevision(error)) return STALE_EDITOR_MESSAGE;
  return `Couldn't save: ${errorMessage(error)}`;
}

export interface ApiKeyConnect {
  rail: ReadonlyArray<{ title: string; hint: string }>;
  activeIndex: number;
  body: ReactNode;
  footer: ReactNode;
  busy: boolean;
}

export function useApiKeyConnect({
  replace,
  onCancel,
  onBackToKind,
  onSignInInstead,
  onDone,
}: {
  /** Replace mode: the upstream whose key is being replaced. */
  replace: Upstream | null;
  onCancel: () => void;
  /** Create mode: Back from the first API-key step. */
  onBackToKind?: () => void;
  /** Create mode: an OAuth token was pasted; switch to Claude sign-in. */
  onSignInInstead?: () => void;
  onDone: (upstream: Upstream) => void;
}): ApiKeyConnect {
  const preflight = usePreflightApiKey();
  const createUpstream = useCreateUpstream();
  // Refusals render on the step that owns their fix: never a toast too.
  const updateUpstream = useUpdateUpstream({ inlineError: true });
  // Replace mode: the upstream as the dialog opened on it. The draft is built
  // from this snapshot and saved against its revision, so a poll that brings
  // in another admin's newer save makes Save a stale_revision refusal
  // instead of overwriting that save with this older draft.
  const [opened] = useState(replace);

  const [step, setStep] = useState<ApiKeyStep>('key');
  const [guideVisited, setGuideVisited] = useState(false);
  const [guideTier, setGuideTier] = useState<GuideTier>('full');
  const [keyValue, setKeyValue] = useState('');
  const [baseUrl, setBaseUrl] = useState(
    replace?.base_url ?? DEFAULT_ANTHROPIC_BASE_URL,
  );
  /** Result of checking the key alone (no workspace header). */
  const [checked, setChecked] = useState<PreflightResponse | null>(null);
  const [requestError, setRequestError] = useState<string | null>(null);
  /** The server refused the key's characters (`invalid_input`). */
  const [keyFieldError, setKeyFieldError] = useState<string | null>(null);
  /** A save refusal no step owns (stale revision, 5xx without a code). */
  const [saveError, setSaveError] = useState<string | null>(null);
  const [workspaceId, setWorkspaceId] = useState('');
  const [workspaceCheck, setWorkspaceCheck] =
    useState<PreflightResponse | null>(null);
  const [workspaceFormatError, setWorkspaceFormatError] = useState<
    string | null
  >(null);
  /** What Finish summarizes and Save persists; null when saved unchecked. */
  const [final, setFinal] = useState<PreflightResponse | null>(null);
  const [unverified, setUnverified] = useState(false);
  const [name, setName] = useState('');
  // Base URL is a rare need: hidden behind "Use a custom endpoint" unless a
  // non-Anthropic value is already there.
  const [endpointOpen, setEndpointOpen] = useState(
    () => !isAnthropicBaseUrl(replace?.base_url ?? ''),
  );

  const parsed = parseApiKeyPaste(keyValue);
  const anthropicHost = isAnthropicBaseUrl(baseUrl);
  // Client mirror of the server's prefix gate; a proxy may accept anything.
  const gate = anthropicHost && parsed.kind === 'key' ? parsed.gate : null;
  const problem: KeyProblem | null =
    gate ?? (checked ? problemOf(checked) : null);
  const saving = createUpstream.isPending || updateUpstream.isPending;
  const busy = preflight.isPending || saving;
  const isReplace = replace !== null;
  /** The key itself is wrong; a fresh key (the Guide) is the fix. */
  const keyRejected =
    !requestError && problem !== null && GUIDE_PROBLEMS.includes(problem);
  const openGuide = () => {
    setGuideVisited(true);
    setStep('guide');
  };
  const workspaceList =
    checked?.workspace_list === 'ok' ? (checked.workspaces ?? []) : null;
  const workspaceNotFound =
    workspaceCheck?.remediation.some((r) => r.code === 'workspace_not_found') ??
    false;
  /** A key-level failure of the probe that carried the chosen workspace. */
  const workspaceProblem =
    workspaceCheck && !workspaceNotFound ? problemOf(workspaceCheck) : null;
  /** Replace mode: the workspace the upstream sends today, if any. */
  const currentWorkspace =
    opened?.api_key_access?.workspace.mode === 'selected'
      ? (opened.api_key_access.workspace.id ?? null)
      : null;
  /**
   * The Workspace step's starting choice: the upstream's current workspace
   * when the new key can use it, else none — never the list's first item.
   */
  const startingWorkspace = (res: PreflightResponse) =>
    currentWorkspace !== null &&
    (res.workspace_list !== 'ok' ||
      (res.workspaces ?? []).some((w) => w.id === currentWorkspace))
      ? currentWorkspace
      : '';

  const steps: ApiKeyStep[] = [
    ...(guideVisited ? (['guide'] as const) : []),
    'key',
    ...(checked?.verdict === 'needs_workspace' ||
    final?.workspace.mode === 'selected'
      ? (['workspace'] as const)
      : []),
    'finish',
  ];

  const keyBody = (literal?: string): PreflightRequest | null => {
    const body: PreflightRequest = {};
    const key = literal ?? (parsed.kind === 'key' ? parsed.key : null);
    if (!key) return null;
    body.api_key_value = key;
    if (!anthropicHost) body.base_url = baseUrl.trim();
    return body;
  };

  const resetCheck = () => {
    setChecked(null);
    setRequestError(null);
    setKeyFieldError(null);
    setSaveError(null);
    setFinal(null);
    setUnverified(false);
    setWorkspaceCheck(null);
  };

  /** A preflight that failed as a request: the key field or the notice. */
  const failRequest = (error: unknown) => {
    const reason = keyInputReason(error);
    if (reason !== null) {
      setKeyFieldError(reason);
      setStep('key');
    } else {
      setRequestError(errorMessage(error));
    }
  };

  const finishWith = (res: PreflightResponse | null, unchecked: boolean) => {
    setFinal(res);
    setUnverified(unchecked);
    setSaveError(null);
    if (!isReplace && name.trim() === '') {
      setName(
        res?.workspace.name
          ? `${res.workspace.name} API key`
          : 'Anthropic API key',
      );
    }
    setStep('finish');
  };

  const checkKey = async (literal?: string) => {
    const body = keyBody(literal);
    if (!body || preflight.isPending) return;
    setRequestError(null);
    setKeyFieldError(null);
    try {
      const res = await preflight.mutateAsync(body);
      setChecked(res);
      if (res.verdict === 'ok') {
        finishWith(res, false);
      } else if (res.verdict === 'needs_workspace') {
        setWorkspaceId(startingWorkspace(res));
        setWorkspaceCheck(null);
        setWorkspaceFormatError(null);
        setStep('workspace');
      }
    } catch (error) {
      failRequest(error);
    }
  };

  // Re-asks with the key alone: a raised role turns manual entry into a list.
  // A key-level failure (revoked key, outage) is dispatched like Continue's.
  const recheckWorkspaces = async () => {
    const body = keyBody();
    if (!body || preflight.isPending) return;
    setRequestError(null);
    try {
      const res = await preflight.mutateAsync(body);
      if (res.verdict === 'needs_workspace') {
        setChecked(res);
        setWorkspaceCheck(null);
        if (workspaceId.trim() === '') {
          setWorkspaceId(startingWorkspace(res));
        }
      } else if (res.verdict === 'ok') {
        setChecked(res);
        finishWith(res, false);
      } else {
        // Keep the Workspace step and the draft; the problem renders here.
        setWorkspaceCheck(res);
      }
    } catch (error) {
      failRequest(error);
    }
  };

  const continueWithWorkspace = async () => {
    const id = workspaceId.trim();
    if (!id.startsWith('wrkspc_')) {
      setWorkspaceFormatError('Workspace IDs start with wrkspc_.');
      return;
    }
    const body = keyBody();
    if (!body || preflight.isPending) return;
    setRequestError(null);
    try {
      const res = await preflight.mutateAsync({ ...body, workspace_id: id });
      if (res.remediation.some((r) => r.code === 'workspace_id_malformed')) {
        setWorkspaceFormatError('Workspace IDs start with wrkspc_.');
        setWorkspaceCheck(null);
        return;
      }
      setWorkspaceCheck(res);
      if (res.verdict === 'ok') finishWith(res, false);
    } catch (error) {
      failRequest(error);
    }
  };

  // Saving unchecked from the Workspace step keeps the chosen workspace.
  const finishUnverifiedWithWorkspace = (base: PreflightResponse) => {
    const id = workspaceId.trim();
    finishWith(
      {
        ...base,
        inference: 'unverified',
        workspace: {
          mode: 'selected',
          id,
          name: workspaceList?.find((w) => w.id === id)?.name,
        },
      },
      true,
    );
  };

  const save = async () => {
    const apiKeyValue = parsed.kind === 'key' ? parsed.key : null;
    const workspace =
      final?.workspace.mode === 'selected'
        ? (final.workspace.id ?? null)
        : null;
    setSaveError(null);
    try {
      if (opened) {
        const updated = await updateUpstream.mutateAsync({
          id: opened.id,
          spec_revision: opened.spec_revision,
          body: {
            api_key_value: apiKeyValue,
            // `workspace_id: null` keeps the stored one; a key that needs
            // none must not inherit the old key's workspace.
            ...(workspace
              ? { workspace_id: workspace }
              : { clear_workspace_id: true }),
            skip_preflight: unverified || undefined,
          },
        });
        toast.success(`Key replaced on ${updated.name}`);
        onDone(updated);
      } else {
        const created = await createUpstream.mutateAsync({
          name: name.trim(),
          kind: 'anthropic_api_key',
          // Anthropic itself is the default, never stored as a custom value.
          base_url: anthropicHost ? null : baseUrl.trim(),
          api_key_value: apiKeyValue,
          workspace_id: workspace,
          skip_preflight: unverified || undefined,
        });
        toast.success(`${created.name} added`);
        onDone(created);
      }
    } catch (error) {
      // Both save mutations are `inlineError`: exactly one surface. A §5
      // code from the server's re-probe sends the operator to the step that
      // owns its fix; anything else is one alert on this step.
      const keyReason = keyInputReason(error);
      if (keyReason !== null) {
        setKeyFieldError(keyReason);
        setFinal(null);
        setUnverified(false);
        setStep('key');
        return;
      }
      const code =
        error instanceof ApiError && isRemediationCode(error.code)
          ? error.code
          : null;
      const base = checked ?? final;
      if (!code || !base) {
        setSaveError(saveErrorMessage(error));
        return;
      }
      if (
        code === 'workspace_required' ||
        code === 'workspace_not_found' ||
        code === 'workspace_id_malformed'
      ) {
        setChecked({
          ...base,
          verdict: 'needs_workspace',
          workspace: { mode: 'required' },
        });
        setWorkspaceFormatError(
          code === 'workspace_id_malformed'
            ? 'Workspace IDs start with wrkspc_.'
            : null,
        );
        setWorkspaceCheck(
          code === 'workspace_not_found'
            ? { ...base, verdict: 'needs_workspace', remediation: [{ code }] }
            : null,
        );
        setFinal(null);
        setUnverified(false);
        setStep('workspace');
      } else if (
        final?.workspace.mode === 'selected' &&
        final.workspace.id &&
        (code === 'key_forbidden' || code === 'probe_unreachable')
      ) {
        // The key itself is fine; keep the checked workspace so Save anyway /
        // Save without checking (finishUnverifiedWithWorkspace) still sends it.
        setWorkspaceId(final.workspace.id);
        setWorkspaceFormatError(null);
        setWorkspaceCheck({
          ...final,
          verdict: code === 'key_forbidden' ? 'rejected' : 'unreachable',
          remediation: [{ code }],
        });
        setFinal(null);
        setUnverified(false);
        setStep('workspace');
      } else if ((KEY_PROBLEMS as readonly string[]).includes(code)) {
        setChecked({
          ...base,
          verdict: code === 'probe_unreachable' ? 'unreachable' : 'rejected',
          remediation: [{ code }],
        });
        setWorkspaceCheck(null);
        setFinal(null);
        setUnverified(false);
        setStep('key');
      } else {
        setSaveError(saveErrorMessage(error));
      }
    }
  };

  const cancelButton = (
    <Button disabled={busy} onClick={onCancel}>
      Cancel
    </Button>
  );
  const backButton = (onClick: () => void, variant?: 'ghost') => (
    <Button
      variant={variant}
      className="mr-auto"
      disabled={busy}
      onClick={onClick}
    >
      Back
    </Button>
  );
  const backFromKey = onBackToKind ? backButton(onBackToKind) : null;

  // ─── Footer ────────────────────────────────────────────────────────────────
  let footer: ReactNode;
  if (step === 'guide') {
    footer = (
      <>
        {cancelButton}
        <Button variant="ghost" onClick={() => setStep('key')}>
          Paste a key instead
        </Button>
        <Button
          variant="primary"
          onClick={() => {
            // A known-bad key would bring the same notice straight back.
            if (keyRejected) {
              setKeyValue('');
              resetCheck();
            }
            setStep('key');
          }}
        >
          I created it — continue
        </Button>
      </>
    );
  } else if (step === 'key') {
    if (problem === 'key_oat' && onSignInInstead) {
      footer = (
        <>
          {onBackToKind ? backButton(onBackToKind, 'ghost') : null}
          {cancelButton}
          <Button variant="primary" onClick={onSignInInstead}>
            Sign in with Claude instead
          </Button>
        </>
      );
    } else if (problem === 'probe_unreachable') {
      footer = (
        <>
          {backFromKey}
          {cancelButton}
          <Button
            variant="ghost"
            disabled={busy}
            onClick={() => finishWith(null, true)}
          >
            Save without checking
          </Button>
          <Button
            variant="primary"
            loading={preflight.isPending}
            onClick={() => void checkKey()}
          >
            {preflight.isPending ? 'Checking…' : 'Try again'}
          </Button>
        </>
      );
    } else {
      footer = (
        <>
          {backFromKey}
          {cancelButton}
          {problem === 'key_forbidden' && checked ? (
            <Button
              disabled={busy}
              onClick={() =>
                finishWith({ ...checked, inference: 'unverified' }, true)
              }
            >
              Save anyway
            </Button>
          ) : null}
          <Button
            variant="primary"
            loading={preflight.isPending}
            disabled={
              gate !== null || keyBody() === null || problem === 'key_oat'
            }
            onClick={() => void checkKey()}
          >
            {preflight.isPending ? 'Checking…' : 'Check key'}
          </Button>
        </>
      );
    }
  } else if (step === 'workspace') {
    const saveUnverifiedLabel =
      workspaceProblem === 'key_forbidden'
        ? 'Save anyway'
        : workspaceProblem === 'probe_unreachable'
          ? 'Save without checking'
          : null;
    footer = (
      <>
        {backButton(() => setStep('key'))}
        {cancelButton}
        {workspaceList === null ? (
          <Button
            variant="ghost"
            disabled={busy}
            onClick={() => void recheckWorkspaces()}
          >
            Re-check
          </Button>
        ) : null}
        {saveUnverifiedLabel && workspaceCheck && workspaceId.trim() !== '' ? (
          <Button
            variant="ghost"
            disabled={busy}
            onClick={() => finishUnverifiedWithWorkspace(workspaceCheck)}
          >
            {saveUnverifiedLabel}
          </Button>
        ) : null}
        <Button
          variant="primary"
          loading={preflight.isPending}
          disabled={workspaceId.trim() === ''}
          onClick={() => void continueWithWorkspace()}
        >
          {preflight.isPending
            ? 'Checking…'
            : workspaceProblem === 'probe_unreachable'
              ? 'Try again'
              : 'Continue'}
        </Button>
      </>
    );
  } else {
    footer = (
      <>
        {backButton(() =>
          setStep(final?.workspace.mode === 'selected' ? 'workspace' : 'key'),
        )}
        {cancelButton}
        <Button
          variant="primary"
          data-testid="upstream-connect-submit"
          loading={saving}
          disabled={!isReplace && name.trim() === ''}
          onClick={() => void save()}
        >
          {isReplace
            ? saving
              ? 'Saving…'
              : 'Save key'
            : saving
              ? 'Adding…'
              : 'Add upstream'}
        </Button>
      </>
    );
  }

  // ─── Bodies ────────────────────────────────────────────────────────────────
  let body: ReactNode;
  if (step === 'guide') {
    body = <GuideStep tier={guideTier} onTierChange={setGuideTier} />;
  } else if (step === 'key') {
    const keyError =
      problem === 'key_admin'
        ? "This is an Admin key — it manages the org and can't call models."
        : problem === 'key_enterprise_scoped'
          ? "This is an Enterprise management key — it can't call models."
          : problem === 'key_invalid'
            ? anthropicHost
              ? "Anthropic rejected this key — check for a typo, or it's expired or deleted."
              : 'Your endpoint rejected this key.'
            : problem === 'key_oat'
              ? 'That is an OAuth token, not an API key — it expires and cc-lb cannot refresh it.'
              : (keyFieldError ??
                (parsed.kind === 'invalid' ? parsed.reason : undefined));
    const onKeyPaste = (e: ClipboardEvent<HTMLInputElement>) => {
      const text = e.clipboardData.getData('text');
      if (!text) return;
      e.preventDefault();
      const next = parseApiKeyPaste(text);
      setKeyValue(next.kind === 'key' ? next.key : text.trim());
      resetCheck();
      // A clean paste checks itself; gated prefixes stop at their notice.
      if (next.kind === 'key' && (next.gate === null || !anthropicHost)) {
        void checkKey(next.key);
      }
    };
    body = (
      <div className="flex flex-col gap-3">
        <StepHeading
          title={isReplace ? 'Paste the new key' : 'Paste your API key'}
          subtitle="cc-lb checks what it can do before anything is saved."
        />
        <Field
          label="API key"
          hint="Starts with sk-ant-"
          error={keyError}
          required
        >
          {/* Read-only, not disabled, while a check runs: disabling the
              focused field would drop keyboard focus to <body>. */}
          <input
            type="password"
            className={cx(INPUT_CLASS, 'font-mono aria-busy:cursor-progress')}
            value={keyValue}
            autoFocus
            autoComplete="off"
            spellCheck={false}
            placeholder="sk-ant-…"
            readOnly={busy}
            aria-busy={busy || undefined}
            onPaste={(e) => {
              if (busy) e.preventDefault();
              else onKeyPaste(e);
            }}
            onChange={(e) => {
              setKeyValue(e.target.value);
              resetCheck();
            }}
            onKeyDown={(e) => {
              if (e.key === 'Enter' && gate === null && !busy) void checkKey();
            }}
          />
        </Field>
        {/* Key-help slot: one help affordance. A rejection notice carries
            the Guide action itself, so the quiet link steps aside. */}
        {keyRejected ? null : (
          <button
            type="button"
            className={cx(QUIET_LINK_CLASS, '-mt-1')}
            disabled={busy}
            onClick={openGuide}
          >
            Don't have a key?
          </button>
        )}
        <KeyProblemNotice
          problem={problem}
          customEndpoint={!anthropicHost}
          requestError={requestError}
          onRetry={() => void checkKey()}
          onOpenGuide={openGuide}
          busy={busy}
        />
        {isReplace ? null : (
          <CustomEndpointDisclosure
            open={endpointOpen}
            value={baseUrl}
            busy={busy}
            onOpen={() => {
              setBaseUrl('');
              resetCheck();
              setEndpointOpen(true);
            }}
            onChange={(next) => {
              setBaseUrl(next);
              resetCheck();
            }}
            onUseDefault={() => {
              setBaseUrl(DEFAULT_ANTHROPIC_BASE_URL);
              resetCheck();
              setEndpointOpen(false);
            }}
          />
        )}
      </div>
    );
  } else if (step === 'workspace') {
    const notFound = workspaceNotFound
      ? `This key can't see workspace ${workspaceId.trim()} — check the id, or the key's account lacks access.`
      : undefined;
    body = (
      <div className="flex flex-col gap-3">
        <StepHeading
          title="Pick a workspace"
          subtitle={
            workspaceList
              ? 'This key can act in several workspaces — choose the one it bills.'
              : 'This key can act in several workspaces — paste the one it bills.'
          }
        />
        {workspaceList ? (
          <div className="flex flex-col gap-1">
            <span className="text-label text-text-muted">Workspace</span>
            <Select
              aria-label="Workspace"
              className="w-full"
              value={workspaceId}
              aria-invalid={workspaceNotFound}
              disabled={busy}
              options={workspaceList.map((w) => ({
                value: w.id,
                label: w.name,
                hint: (
                  <span className="font-mono text-data">
                    {shortWorkspaceId(w.id)}
                  </span>
                ),
              }))}
              onChange={(value) => {
                setWorkspaceId(value);
                setWorkspaceCheck(null);
              }}
            />
            <span className="text-caption text-text-faint">
              You can change this later in Access.
            </span>
          </div>
        ) : (
          <>
            <Field
              label="Workspace ID"
              error={workspaceFormatError ?? undefined}
              required
            >
              <input
                className={cx(INPUT_CLASS, PENDING_INPUT_CLASS, 'font-mono')}
                value={workspaceId}
                autoFocus
                spellCheck={false}
                placeholder="wrkspc_…"
                // A 404 is announced once, by the warning below.
                aria-invalid={workspaceNotFound || undefined}
                disabled={busy}
                onChange={(e) => {
                  setWorkspaceId(e.target.value);
                  setWorkspaceFormatError(null);
                  setWorkspaceCheck(null);
                }}
                onKeyDown={(e) => {
                  if (e.key === 'Enter') void continueWithWorkspace();
                }}
              />
            </Field>
            <WorkspaceIdHelp list={checked?.workspace_list ?? 'denied'} />
          </>
        )}
        {notFound ? (
          <Notice tone="warning" role="alert">
            {notFound}
          </Notice>
        ) : null}
        {workspaceProblem === 'key_invalid' ? (
          // The key itself failed since the Key step: say so, then the fix.
          <Notice tone="danger" role="alert">
            {anthropicHost
              ? "Anthropic rejected this key — check for a typo, or it's expired or deleted."
              : 'Your endpoint rejected this key.'}{' '}
            Back → paste a fresh key.
          </Notice>
        ) : null}
        <KeyProblemNotice
          problem={workspaceProblem === 'key_invalid' ? null : workspaceProblem}
          customEndpoint={!anthropicHost}
          checks="workspace"
          requestError={requestError}
          onRetry={() => void continueWithWorkspace()}
        />
      </div>
    );
  } else {
    body = (
      <FinishStep
        final={final}
        unverified={unverified}
        inferenceDenied={
          problem === 'key_forbidden' || workspaceProblem === 'key_forbidden'
        }
        name={isReplace ? null : name}
        onNameChange={setName}
        disabled={saving}
        saveError={saveError}
        onSubmit={() => void save()}
      />
    );
  }

  return {
    rail: steps.map((s) =>
      s === 'finish' && isReplace
        ? { title: 'Save', hint: 'Confirm what it can do' }
        : STEP_META[s],
    ),
    activeIndex: Math.max(0, steps.indexOf(step)),
    body,
    footer,
    busy,
  };
}

/**
 * The one announced surface of a check that didn't pass (D-UX2): every
 * branch is a `role="alert"` Notice, while the Key field error stays
 * `aria-invalid` + `aria-describedby` so nothing is announced twice.
 */
function KeyProblemNotice({
  problem,
  customEndpoint = false,
  checks = 'key',
  requestError,
  onRetry,
  onOpenGuide,
  busy = false,
}: {
  problem: KeyProblem | null;
  /** The key went to a proxy: Console's create-key guide doesn't apply. */
  customEndpoint?: boolean;
  /** What `Try again` re-checks: the key alone, or it with the workspace. */
  checks?: 'key' | 'workspace';
  requestError: string | null;
  onRetry: () => void;
  /** Key step only: rejections send the operator to the in-dialog Guide. */
  onOpenGuide?: () => void;
  busy?: boolean;
}) {
  const guideAction = onOpenGuide ? (
    <>
      {' '}
      <button
        type="button"
        className={ANCHOR_CLASS}
        disabled={busy}
        onClick={onOpenGuide}
      >
        How to create a key
      </button>
    </>
  ) : null;
  if (requestError) {
    return (
      <Notice
        tone="danger"
        role="alert"
        action={
          <Button size="sm" onClick={onRetry}>
            Try again
          </Button>
        }
      >
        cc-lb couldn't check the key: {requestError}
      </Notice>
    );
  }
  switch (problem) {
    case 'key_admin':
      return (
        <Notice role="alert">
          Use a standard API key → get inference.
          {guideAction}
        </Notice>
      );
    case 'key_enterprise_scoped':
      return (
        <Notice role="alert">
          Use a Console API key → paste it here.
          {guideAction}
        </Notice>
      );
    case 'key_invalid':
      return customEndpoint ? (
        <Notice role="alert">Check the key and Base URL → Check key.</Notice>
      ) : (
        <Notice role="alert">
          Paste it again or create a fresh key.
          {guideAction}
        </Notice>
      );
    case 'key_oat':
      return (
        <Notice role="alert">
          Sign in with Claude → get a managed session.
          <span className="mt-0.5 block text-caption text-text-faint">
            Workload identity federation tokens won't work either.
          </span>
        </Notice>
      );
    case 'key_forbidden':
      return (
        <Notice tone="warning" role="alert">
          Valid key, but Anthropic won't let it count tokens. Save anyway → add
          it unverified.
        </Notice>
      );
    case 'probe_unreachable':
      return (
        <Notice tone="warning" role="alert">
          {customEndpoint
            ? "Couldn't reach your endpoint."
            : "Couldn't reach Anthropic."}{' '}
          Try again → check the {checks}.
        </Notice>
      );
    default:
      return null;
  }
}

function GuideStep({
  tier,
  onTierChange,
}: {
  tier: GuideTier;
  onTierChange: (tier: GuideTier) => void;
}) {
  return (
    <div className="flex flex-col gap-4">
      <StepHeading
        title="Create the key"
        subtitle="A few picks in Anthropic Console → paste it back here."
      />
      <BaseRadioGroup
        name="key-coverage"
        value={tier}
        onValueChange={(value) => onTierChange(value as GuideTier)}
        className="flex flex-col gap-2"
      >
        <KindOption
          value="full"
          selected={tier === 'full'}
          icon={<Layers className="w-4 h-4" strokeWidth={1.75} />}
          title="Full access"
          badge="Recommended"
          description="All workspaces + an admin-linked account → reported usage, workspace switching."
          onActivate={() => onTierChange('full')}
        />
        <KindOption
          value="one"
          selected={tier === 'one'}
          icon={<KeyRound className="w-4 h-4" strokeWidth={1.75} />}
          title="One workspace"
          description="Bound at creation → inference only, usage stays an estimate."
          onActivate={() => onTierChange('one')}
        />
      </BaseRadioGroup>
      <ol className="flex list-decimal flex-col gap-2 pl-5 text-body-sm text-text marker:text-text-faint">
        <li>
          <ConsoleLink href={CONSOLE_KEYS_URL}>
            Open the API keys page
          </ConsoleLink>{' '}
          → Create key
        </li>
        {tier === 'full' ? (
          <>
            <li>
              Linked account → a service account with the{' '}
              <span className="font-semibold">Admin</span> role
              <span className="block text-caption text-text-faint">
                The safe pick — the minimum role isn't documented.
              </span>
            </li>
            <li>
              Workspace → <span className="font-semibold">All workspaces</span>
              <span className="block text-caption text-text-faint">
                This unlocks usage reports and workspace switching.
              </span>
            </li>
            <li>
              Expiration → your org's policy
              <span className="block text-caption text-text-faint">
                Some orgs cap key lifetime.
              </span>
            </li>
          </>
        ) : (
          <>
            <li>
              Workspace → the one requests should bill
              <span className="block text-caption text-text-faint">
                Any single workspace; usage stays an estimate for this key.
              </span>
            </li>
            <li>Linked account → you or a service account</li>
          </>
        )}
        <li>Copy the key → paste it on the next step</li>
      </ol>
      <Notice>
        Org policies can hide options — if a pick is missing, the key still
        pastes fine; cc-lb tells you what it can do.
      </Notice>
    </div>
  );
}

function FinishStep({
  final,
  unverified,
  inferenceDenied,
  name,
  onNameChange,
  disabled,
  saveError,
  onSubmit,
}: {
  final: PreflightResponse | null;
  unverified: boolean;
  inferenceDenied: boolean;
  /** `null` in replace mode: the upstream keeps its name. */
  name: string | null;
  onNameChange: (name: string) => void;
  disabled: boolean;
  /** The server refused the save for a reason no step owns. */
  saveError: string | null;
  onSubmit: () => void;
}) {
  const ws = final?.workspace;
  const usageOk = final?.usage_report === 'ok';
  const replacing = name === null;
  return (
    <div className="flex flex-col gap-4">
      {unverified ? (
        <StepHeading
          title="Key not verified"
          subtitle={
            inferenceDenied
              ? "Anthropic accepted the key but wouldn't let it count tokens. The upstream shows Key not verified until a re-check passes."
              : 'The upstream shows Key not verified until a Re-check passes.'
          }
        />
      ) : (
        <SuccessHeader
          title="Key verified"
          subtitle={
            replacing
              ? 'Check what it can do, then save the key.'
              : 'Check what it can do, then name the upstream.'
          }
        />
      )}
      <div className="@container rounded-sm border border-subtle p-3">
        <DetailFacts>
          <DetailFact label="Inference">
            {final?.inference === 'ok' ? (
              <StatusBadge tone="ok" label="Key accepted" />
            ) : (
              <StatusBadge tone="warn" label="Key not verified" />
            )}
          </DetailFact>
          <DetailFact
            label="Workspace"
            hint={
              ws?.mode === 'bound' && ws.id ? (
                <span className="font-mono text-data">
                  {shortWorkspaceId(ws.id)}
                </span>
              ) : ws?.mode === 'selected' ? (
                'Change it later in Access'
              ) : undefined
            }
          >
            {ws?.mode === 'bound' ? (
              'Fixed by the key'
            ) : ws?.mode === 'selected' ? (
              (ws.name ?? (
                <span className="font-mono text-data" title={ws.id}>
                  {ws.id ? shortWorkspaceId(ws.id) : null}
                </span>
              ))
            ) : (
              <span className="text-text-muted">Not checked</span>
            )}
          </DetailFact>
          <DetailFact label="Usage">
            {final === null || unverified ? (
              <span className="text-text-muted">Not checked</span>
            ) : usageOk ? (
              'Reported by Anthropic'
            ) : final.usage_report === 'error' ? (
              'Estimates for now'
            ) : (
              'Estimates only'
            )}
          </DetailFact>
          <DetailFact label="Key type">
            {final?.key_type ? KEY_TYPE_LABEL[final.key_type] : 'API key'}
          </DetailFact>
        </DetailFacts>
      </div>
      {final &&
      !unverified &&
      final.remediation.some((r) => r.code === 'non_anthropic_base_url') ? (
        <Notice>
          Org checks (workspaces, usage reporting) need api.anthropic.com —
          usage stays estimated.
        </Notice>
      ) : final && !unverified && final.usage_report === 'error' ? (
        <Notice>
          Anthropic didn't answer the usage check. Re-check in Access → get
          reported usage.
        </Notice>
      ) : final &&
        !unverified &&
        !usageOk &&
        // Replace already is the fix for a bound key: no nudge to do it.
        !(replacing && final.key_class === 'workspace_bound') ? (
        // "Estimates only" is already in the facts above; only the fix here.
        <Notice>{usageFixLine(final.key_class)}</Notice>
      ) : null}
      {name === null ? null : (
        <Field
          label="Upstream name"
          hint="Only used inside cc-lb — pick something recognizable."
        >
          <input
            className={cx(INPUT_CLASS, PENDING_INPUT_CLASS)}
            value={name}
            autoFocus
            disabled={disabled}
            onChange={(e) => onNameChange(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Enter' && name.trim()) onSubmit();
            }}
          />
        </Field>
      )}
      {saveError ? (
        <Notice tone="danger" role="alert">
          {saveError}
        </Notice>
      ) : null}
    </div>
  );
}
