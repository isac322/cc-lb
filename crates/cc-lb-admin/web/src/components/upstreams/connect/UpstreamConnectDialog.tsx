// One dialog for every way an operator attaches an account to cc-lb. "New
// upstream" starts with the connection type (Claude subscription or API key)
// and continues in the same dialog; Connect and Reconnect on an existing
// upstream open the same dialog at the sign-in step, and Replace key on an
// API-key upstream opens it at the key step. A left-hand step rail keeps the
// journey legible, and the sign-in step shows a miniature of Claude's
// post-approval page so the code to copy is recognizable before the operator
// ever leaves cc-lb.

import { RadioGroup as BaseRadioGroup } from '@base-ui/react/radio-group';
import {
  Check,
  CornerUpLeft,
  ExternalLink,
  KeyRound,
  UserRound,
} from 'lucide-react';
import { type ReactNode, useState } from 'react';
import { toast } from 'sonner';
import type { Upstream } from '../../../lib/api';
import {
  useCreateFromOauthDraft,
  useUpstreamOAuthStatus,
  useUpstreamSubscriptionMetadata,
} from '../../../lib/queries';
import {
  Button,
  ConfirmDialog,
  cx,
  Field,
  INPUT_CLASS,
  Modal,
  Notice,
} from '../../ui/primitives';
import { apiKeyUsable } from '../ApiKeyNotice';
import { isAnthropicBaseUrl } from './apiKeyCopy';
import {
  AccountCard,
  CodePasteField,
  CopyLinkButton,
  CustomEndpointDisclosure,
  IdentityVerdictNotice,
  KindOption,
  SessionStartError,
  SessionTimer,
  SignInLink,
  StepHeading,
  SuccessHeader,
  TokenLifetimeNote,
} from './parts';
import { useApiKeyConnect } from './useApiKeyConnect';
import {
  formatRemaining,
  identityOf,
  useOAuthConnectSession,
} from './useOAuthConnectSession';

/** `apikey`: inside the API-key flow, which owns its own sub-steps. */
type StepId = 'kind' | 'signin' | 'paste' | 'finish' | 'apikey';
type UpstreamKind = 'anthropic_oauth' | 'anthropic_api_key';

const PENDING_INPUT_CLASS =
  'disabled:cursor-not-allowed disabled:border-subtle disabled:text-text-faint';

/** What the dialog is opened for; `null` keeps it closed. */
export type UpstreamConnectTarget =
  | { mode: 'create' }
  | { mode: 'reconnect'; upstream: Upstream }
  | { mode: 'replace'; upstream: Upstream };

export function UpstreamConnectDialog({
  target,
  onClose,
  onCreated,
}: {
  target: UpstreamConnectTarget | null;
  onClose: () => void;
  /** Called with the new upstream so the caller can select it. */
  onCreated: (upstream: Upstream) => void;
}) {
  // `null` is the closed state: nothing mounts, so each opening starts from a
  // clean slate (and a fresh sign-in session) in the keyed child.
  if (!target) return null;
  if (target.mode === 'replace') {
    return (
      <ReplaceKeyDialogBody
        key={target.upstream.id}
        upstream={target.upstream}
        onClose={onClose}
      />
    );
  }
  return (
    <ConnectDialogBody
      key={target.mode === 'create' ? 'create' : target.upstream.id}
      target={target}
      onClose={onClose}
      onCreated={onCreated}
    />
  );
}

/** Replace key: the API-key flow alone, saving onto the existing upstream. */
function ReplaceKeyDialogBody({
  upstream,
  onClose,
}: {
  upstream: Upstream;
  onClose: () => void;
}) {
  const apiKey = useApiKeyConnect({
    replace: upstream,
    onCancel: onClose,
    onDone: onClose,
  });
  return (
    <Modal
      open
      onOpenChange={(next) => {
        if (!next && !apiKey.busy) onClose();
      }}
      title={`Replace key for ${upstream.name}`}
      description={
        apiKeyUsable(upstream.api_key_access ?? null)
          ? 'The current key keeps working until you save.'
          : 'Requests use the new key once you save.'
      }
      size="lg"
      preventDismiss={apiKey.busy}
      footer={apiKey.footer}
    >
      <div className="flex gap-5 min-h-[20rem]">
        <StepRail steps={apiKey.rail} activeIndex={apiKey.activeIndex} />
        <div className="min-w-0 flex-1">
          <MobileStepLine
            steps={apiKey.rail}
            activeIndex={apiKey.activeIndex}
          />
          {/* Keyed: remounts per step so the settle plays where the eye is
              and the step's autoFocus lands again. */}
          <div
            key={apiKey.rail[apiKey.activeIndex]?.title ?? apiKey.activeIndex}
            className="flash-text-active min-w-0"
          >
            {apiKey.body}
          </div>
        </div>
      </div>
    </Modal>
  );
}

function ConnectDialogBody({
  target,
  onClose,
  onCreated,
}: {
  target: Exclude<UpstreamConnectTarget, { mode: 'replace' }>;
  onClose: () => void;
  onCreated: (upstream: Upstream) => void;
}) {
  const upstream = target.mode === 'reconnect' ? target.upstream : null;
  const isCreate = upstream === null;

  // Reconnect's #1 risk is signing in with the wrong Claude account because
  // the browser is logged into another one, so the previous account is shown
  // up front and handed to the session for the identity verdict.
  const meta = useUpstreamSubscriptionMetadata(upstream?.id ?? '');
  const previousAccount = identityOf(meta.data?.organization_metadata);
  const oauthStatus = useUpstreamOAuthStatus(upstream?.id ?? null);
  // An upstream with no stored credential is being connected for the first
  // time — say "Connect", not "Reconnect", exactly like its nudge does.
  const firstConnect =
    upstream !== null && oauthStatus.data?.has_credentials === false;

  const [kind, setKind] = useState<UpstreamKind>('anthropic_oauth');
  const [step, setStep] = useState<StepId>(isCreate ? 'kind' : 'signin');
  // The sign-in session (and its 15-minute clock) starts only once the
  // operator has actually chosen the Claude path.
  const [sessionWanted, setSessionWanted] = useState(!isCreate);

  const session = useOAuthConnectSession(
    upstream
      ? { mode: 'reconnect', upstreamId: upstream.id }
      : { mode: 'create' },
    { previousAccount, enabled: sessionWanted },
  );
  const createFromDraft = useCreateFromOauthDraft();

  const [oauthName, setOauthName] = useState('');
  // Base URL is a rare need: hidden behind "Use a custom endpoint", as in the
  // API-key flow. Anthropic itself (or an empty field) is the default.
  const [oauthEndpointOpen, setOauthEndpointOpen] = useState(false);
  const [oauthBaseUrl, setOauthBaseUrl] = useState('');
  const [confirmDiscard, setConfirmDiscard] = useState(false);

  const apiKey = useApiKeyConnect({
    replace: null,
    onCancel: () => requestClose(),
    onBackToKind: () => setStep('kind'),
    onSignInInstead: () => {
      setKind('anthropic_oauth');
      setSessionWanted(true);
      setStep('signin');
    },
    onDone: (created) => {
      onCreated(created);
      onClose();
    },
  });
  const inApiKeyFlow = isCreate && kind === 'anthropic_api_key';

  const outcome = session.outcome;
  const saving = createFromDraft.isPending;
  const busy = session.completing || saving || apiKey.busy;
  // The exchanged draft lives only as long as its 15-minute sign-in window;
  // once that lapses "Add upstream" can never succeed, so offer a new sign-in.
  const draftExpired =
    isCreate && step === 'finish' && session.phase === 'expired';
  // Create mode holds an un-persisted draft on the last step; closing then
  // would silently lose an already-authorized account, so it must ask first.
  // (Reconnect has nothing left to lose: the credential is already stored.)
  const needsDiscardConfirm =
    isCreate && step === 'finish' && outcome !== null && !draftExpired;

  const steps: Exclude<StepId, 'apikey'>[] = isCreate
    ? ['kind', 'signin', 'paste', 'finish']
    : ['signin', 'paste', 'finish'];
  const rail = inApiKeyFlow
    ? [stepMeta('kind', true), ...apiKey.rail]
    : steps.map((id) => stepMeta(id, isCreate));
  const stepIndex = inApiKeyFlow
    ? step === 'kind'
      ? 0
      : 1 + apiKey.activeIndex
    : step === 'apikey'
      ? 0
      : Math.max(0, steps.indexOf(step));

  const requestClose = () => {
    if (busy) return;
    if (needsDiscardConfirm) {
      setConfirmDiscard(true);
    } else {
      onClose();
    }
  };

  const continueFromKind = () => {
    if (kind === 'anthropic_oauth') {
      setSessionWanted(true);
      setStep('signin');
    } else {
      setStep('apikey');
    }
  };

  const submitCode = async (code: string) => {
    const result = await session.complete(code);
    if (!result) return;
    if (isCreate) setOauthName(result.draft?.suggested_name ?? '');
    setStep('finish');
  };

  const saveOauth = async () => {
    const draft = outcome?.draft;
    if (!draft) return;
    try {
      const customBase =
        oauthEndpointOpen && !isAnthropicBaseUrl(oauthBaseUrl)
          ? oauthBaseUrl.trim()
          : null;
      const created = await createFromDraft.mutateAsync({
        state_token: draft.state_token,
        name: oauthName.trim() || draft.suggested_name,
        // Anthropic itself is the default, never stored as a custom value.
        ...(customBase ? { base_url: customBase } : {}),
      });
      toast.success(`${created.name} added`);
      onCreated(created);
      onClose();
    } catch {
      // The shared MutationCache owns the error toast; stay on the step.
    }
  };

  const title = isCreate
    ? 'New upstream'
    : `${firstConnect ? 'Connect' : 'Reconnect'} ${upstream.name}`;
  const description =
    step === 'kind'
      ? 'Choose how cc-lb reaches Anthropic.'
      : step === 'apikey'
        ? 'Nothing is saved until you add the upstream.'
        : isCreate
          ? 'Nothing is saved until you confirm the account.'
          : firstConnect
            ? 'Sign in with the Claude account this upstream should use.'
            : 'The current connection keeps working until you paste the new code.';

  // Back and Cancel are sibling actions: same variant and size.
  const backButton = (to: StepId) => (
    <Button className="mr-auto" disabled={busy} onClick={() => setStep(to)}>
      Back
    </Button>
  );
  const cancelButton = (
    <Button disabled={busy} onClick={requestClose}>
      Cancel
    </Button>
  );

  let footer: ReactNode;
  if (step === 'kind') {
    footer = (
      <>
        {cancelButton}
        <Button variant="primary" onClick={continueFromKind}>
          Continue
        </Button>
      </>
    );
  } else if (step === 'signin') {
    footer = (
      <>
        {isCreate ? backButton('kind') : null}
        {cancelButton}
      </>
    );
  } else if (step === 'paste') {
    footer = (
      <>
        {backButton('signin')}
        {cancelButton}
      </>
    );
  } else if (step === 'apikey') {
    footer = apiKey.footer;
  } else if (isCreate) {
    footer = draftExpired ? (
      <Button
        variant="primary"
        onClick={() => {
          void session.restart();
          setStep('signin');
        }}
      >
        Sign in again
      </Button>
    ) : (
      <Button
        variant="primary"
        data-testid="upstream-connect-submit"
        loading={createFromDraft.isPending}
        disabled={!oauthName.trim()}
        onClick={() => void saveOauth()}
      >
        {createFromDraft.isPending ? 'Adding…' : 'Add upstream'}
      </Button>
    );
  } else {
    footer = (
      <Button variant="primary" autoFocus onClick={onClose}>
        Done
      </Button>
    );
  }

  return (
    <>
      <Modal
        open
        onOpenChange={(next) => {
          if (!next) requestClose();
        }}
        title={title}
        description={description}
        size="lg"
        preventDismiss={busy}
        footer={footer}
      >
        <div className="flex gap-5 min-h-[20rem]">
          <StepRail steps={rail} activeIndex={stepIndex} />
          <div className="min-w-0 flex-1">
            <MobileStepLine steps={rail} activeIndex={stepIndex} />
            <div
              key={rail[stepIndex]?.title ?? stepIndex}
              className="flash-text-active min-w-0"
            >
              {step === 'kind' ? (
                <KindStep
                  kind={kind}
                  onKindChange={setKind}
                  onContinue={continueFromKind}
                />
              ) : null}

              {step === 'apikey' ? apiKey.body : null}

              {step === 'signin' ? (
                <div className="flex flex-col gap-4">
                  <StepHeading title="Sign in with Claude" />
                  {upstream && !firstConnect && previousAccount ? (
                    <div className="flex flex-col gap-1.5">
                      <AccountCard
                        account={previousAccount}
                        label="Sign in as"
                      />
                      <p className="text-body-sm text-text-muted">
                        If Claude shows a different account, switch accounts
                        there first.
                      </p>
                    </div>
                  ) : null}
                  <p className="text-body-sm text-text-muted">
                    A new Claude tab opens and asks you to approve access. When
                    it finishes it shows a code — that code is how cc-lb proves
                    the sign-in.
                  </p>
                  <div className="flex flex-col items-start gap-2.5">
                    <SignInLink
                      session={session}
                      size="lg"
                      className="w-full sm:w-auto"
                      onOpened={() => setStep('paste')}
                    />
                    <button
                      type="button"
                      onClick={() => setStep('paste')}
                      className="text-caption text-text-faint underline underline-offset-2 hover:text-text max-md:min-h-10"
                    >
                      I already have a code
                    </button>
                  </div>
                  <WhatYouWillSee />
                </div>
              ) : null}

              {step === 'paste' ? (
                <div className="flex flex-col gap-4">
                  <StepHeading
                    title="Paste the code"
                    subtitle="Paste what you copied — the code, or the whole page address both work."
                  />
                  <CodePasteField
                    session={session}
                    autoFocus
                    onSubmit={(code) => void submitCode(code)}
                  />
                  {session.authorizeUrl ? (
                    <a
                      href={session.authorizeUrl}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="inline-flex w-fit items-center gap-1 text-caption text-text-faint underline underline-offset-2 hover:text-text"
                    >
                      <ExternalLink className="w-3 h-3" strokeWidth={1.75} />
                      Open the sign-in page again
                    </a>
                  ) : null}
                </div>
              ) : null}

              {step === 'finish' && outcome ? (
                isCreate ? (
                  <div className="flex flex-col gap-4">
                    <SuccessHeader
                      title="Signed in"
                      subtitle="Check that this is the right account, then name the upstream."
                    />
                    <AccountCard
                      account={outcome.account}
                      tone="ok"
                      label="Connected account"
                    />
                    <TokenLifetimeNote outcome={outcome} />
                    <Field
                      label="Upstream name"
                      hint="Only used inside cc-lb — pick something recognizable."
                    >
                      <input
                        className={cx(INPUT_CLASS, PENDING_INPUT_CLASS)}
                        value={oauthName}
                        autoFocus
                        disabled={saving}
                        onChange={(e) => setOauthName(e.target.value)}
                        onKeyDown={(e) => {
                          if (e.key === 'Enter' && oauthName.trim())
                            void saveOauth();
                        }}
                      />
                    </Field>
                    <CustomEndpointDisclosure
                      open={oauthEndpointOpen}
                      value={oauthBaseUrl}
                      busy={saving}
                      hint="Requests go here instead of Anthropic."
                      onOpen={() => {
                        setOauthBaseUrl('');
                        setOauthEndpointOpen(true);
                      }}
                      onChange={setOauthBaseUrl}
                      onUseDefault={() => {
                        setOauthBaseUrl('');
                        setOauthEndpointOpen(false);
                      }}
                    />
                    {draftExpired ? (
                      <Notice tone="warning" role="alert">
                        This sign-in expired before the upstream was added. Sign
                        in again to continue — it only takes a moment.
                      </Notice>
                    ) : session.remainingMs < 300_000 ? (
                      <p className="text-body-sm text-warn-text tabular-nums">
                        Add the upstream within{' '}
                        {formatRemaining(session.remainingMs)} — the sign-in
                        expires after that.
                      </p>
                    ) : null}
                  </div>
                ) : (
                  <div className="flex flex-col gap-4">
                    <SuccessHeader
                      title={firstConnect ? 'Connected' : 'Reconnected'}
                      subtitle={`${upstream.name} is signed in with a fresh credential.`}
                    />
                    {outcome.verdict !== 'different' && outcome.account ? (
                      <AccountCard
                        account={outcome.account}
                        tone="ok"
                        label="Now"
                      />
                    ) : null}
                    <IdentityVerdictNotice outcome={outcome} />
                    <TokenLifetimeNote outcome={outcome} />
                  </div>
                )
              ) : null}

              {step === 'signin' || step === 'paste' ? (
                <div className="mt-4 flex flex-col gap-2">
                  <div className="flex items-center gap-4">
                    <SessionTimer session={session} />
                    {step === 'signin' ? (
                      <CopyLinkButton authorizeUrl={session.authorizeUrl} />
                    ) : null}
                  </div>
                  <SessionStartError session={session} />
                </div>
              ) : null}
            </div>
          </div>
        </div>
      </Modal>

      <ConfirmDialog
        open={confirmDiscard}
        onOpenChange={setConfirmDiscard}
        title="Discard this connection?"
        description="You already authorized a Claude account, but it has not been saved as an upstream yet. Closing now discards it."
        confirmLabel="Discard"
        destructive
        onConfirm={onClose}
      />
    </>
  );
}

function stepMeta(
  id: Exclude<StepId, 'apikey'>,
  isCreate: boolean,
): { title: string; hint: string } {
  switch (id) {
    case 'kind':
      return { title: 'Connection', hint: 'Subscription or API key' };
    case 'signin':
      return { title: 'Sign in', hint: 'Approve access in a Claude tab' };
    case 'paste':
      return { title: 'Paste code', hint: 'Bring the code back here' };
    case 'finish':
      return isCreate
        ? { title: 'Name & save', hint: 'Confirm the account' }
        : { title: 'Done', hint: 'Confirm the account' };
  }
}

function KindStep({
  kind,
  onKindChange,
  onContinue,
}: {
  kind: UpstreamKind;
  onKindChange: (kind: UpstreamKind) => void;
  onContinue: () => void;
}) {
  return (
    <div className="flex flex-col gap-4">
      <StepHeading title="How should cc-lb connect?" />
      <BaseRadioGroup
        name="kind"
        value={kind}
        onValueChange={(value) => onKindChange(value)}
        className="flex flex-col gap-2"
      >
        <KindOption
          value="anthropic_oauth"
          selected={kind === 'anthropic_oauth'}
          icon={<UserRound className="w-4 h-4" strokeWidth={1.75} />}
          title="Claude subscription"
          badge="Recommended"
          description="Sign in with a Claude Pro, Max, Team or Enterprise account. cc-lb detects the plan and tracks its quota automatically."
          onActivate={onContinue}
        />
        <KindOption
          value="anthropic_api_key"
          selected={kind === 'anthropic_api_key'}
          icon={<KeyRound className="w-4 h-4" strokeWidth={1.75} />}
          title="Anthropic API key"
          description="Paste a key from Anthropic Console (sk-ant-…). Billed per token."
          onActivate={onContinue}
        />
      </BaseRadioGroup>
    </div>
  );
}

function StepRail({
  steps,
  activeIndex,
}: {
  steps: ReadonlyArray<{ title: string; hint: string }>;
  activeIndex: number;
}) {
  return (
    <ol
      aria-label="Steps"
      className="hidden sm:flex sm:flex-col sm:w-44 shrink-0 border-r border-subtle pr-4"
    >
      {steps.map((s, index) => {
        const done = index < activeIndex;
        const active = index === activeIndex;
        return (
          <li
            key={s.title}
            className="relative mt-4 first:mt-0"
            aria-current={active ? 'step' : undefined}
          >
            {index < steps.length - 1 ? (
              <span
                aria-hidden="true"
                className={cx(
                  'absolute left-[9px] top-5 -bottom-4 w-px',
                  done ? 'bg-ok/50' : 'bg-overlay-4',
                )}
              />
            ) : null}
            {/* Current place, like nav's aria-current fill: no hover response. */}
            <div
              className={cx(
                'relative flex items-start gap-3 -ml-1.5 rounded-sm px-1.5 py-1.5 transition-colors duration-150',
                active && 'bg-selected',
              )}
            >
              {active ? (
                <span
                  aria-hidden="true"
                  className="flash-in pointer-events-none absolute inset-0 rounded-sm"
                />
              ) : null}
              <span
                className={cx(
                  'relative flex h-5 w-5 shrink-0 items-center justify-center rounded-full border bg-bg text-caption font-medium tabular-nums',
                  done
                    ? 'border-ok/50 bg-ok/15 text-traffic-success-text'
                    : active
                      ? 'border-text bg-text text-bg'
                      : 'border-subtle-strong text-text-faint',
                )}
              >
                {done ? (
                  <Check className="w-3 h-3" strokeWidth={1.75} />
                ) : (
                  index + 1
                )}
              </span>
              <span className="relative min-w-0">
                <span
                  className={cx(
                    'block text-label leading-5',
                    active
                      ? 'text-text'
                      : done
                        ? 'text-text-muted'
                        : 'text-text-faint',
                  )}
                >
                  {s.title}
                  <span className="sr-only">
                    {done ? ' (done)' : active ? ' (current)' : ' (upcoming)'}
                  </span>
                </span>
                <span className="block text-caption text-text-faint">
                  {s.hint}
                </span>
              </span>
            </div>
          </li>
        );
      })}
    </ol>
  );
}

/** Narrow screens hide the rail: name the step, announced on change. */
function MobileStepLine({
  steps,
  activeIndex,
}: {
  steps: ReadonlyArray<{ title: string; hint: string }>;
  activeIndex: number;
}) {
  return (
    <div
      className="sm:hidden text-caption mb-3"
      aria-live="polite"
      aria-atomic="true"
    >
      <span className="text-text-muted">
        Step {activeIndex + 1} of {steps.length}
      </span>{' '}
      ·{' '}
      <span
        key={steps[activeIndex]?.title ?? activeIndex}
        className="flash-text-active text-text"
      >
        {steps[activeIndex]?.title}
      </span>
    </div>
  );
}

// A mock of Claude's post-approval page, drawn in HTML/CSS so it stays crisp
// and theme-matched. It is an illustration, not a control: aria-hidden keeps
// the fake "Copy Code" button out of the accessibility tree.
function WhatYouWillSee() {
  return (
    <div>
      <div className="mb-1.5 text-label text-text-muted">What you'll see</div>
      <div
        aria-hidden="true"
        className="overflow-hidden rounded-sm border border-subtle bg-overlay-1"
      >
        {/* Miniature browser chrome */}
        <div className="flex items-center gap-2 border-b border-subtle px-2.5 py-1.5">
          <span className="flex gap-1">
            <span className="h-1.5 w-1.5 rounded-full bg-overlay-4" />
            <span className="h-1.5 w-1.5 rounded-full bg-overlay-4" />
            <span className="h-1.5 w-1.5 rounded-full bg-overlay-4" />
          </span>
          <span className="flex-1 truncate rounded-sm bg-overlay-2 px-2 py-0.5 text-caption text-text-faint">
            claude.ai — authentication
          </span>
        </div>
        {/* Miniature callback page */}
        <div className="flex flex-col gap-1.5 p-3">
          <div className="text-label text-text">Authentication code</div>
          <div className="text-caption text-text-faint">
            Paste this into Claude Code
          </div>
          <div className="rounded-sm border border-subtle bg-bg px-2 py-1.5 font-mono text-data text-text-muted">
            aB3x…#eyJ1…
          </div>
          <div className="mt-0.5">
            <span className="inline-flex items-center rounded-sm bg-text px-2 py-1 text-caption font-medium text-bg">
              Copy Code
            </span>
          </div>
        </div>
      </div>
      <p className="mt-1.5 flex items-center gap-1 text-caption text-text-muted">
        <CornerUpLeft className="w-3 h-3" strokeWidth={1.75} />
        Click Copy Code, then come back here
      </p>
    </div>
  );
}
