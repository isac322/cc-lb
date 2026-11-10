// One dialog for every way an operator attaches an account to cc-lb.

import { RadioGroup as BaseRadioGroup } from '@base-ui/react/radio-group';
import {
  Check,
  CornerUpLeft,
  ExternalLink,
  KeyRound,
  UserRound,
} from 'lucide-react';
import { type ReactNode, useEffect, useState } from 'react';
import { toast } from 'sonner';
import type { Upstream, UpstreamKind } from '../../../lib/api';
import {
  PROVIDER_FAMILIES,
  PROVIDER_LABELS,
  type ProviderFamily,
  providerOf,
} from '../../../lib/providers';
import {
  useCreateFromOauthDraft,
  useUpstreamOAuthStatus,
  useUpstreamSubscriptionMetadata,
} from '../../../lib/queries';
import { ProviderLogo } from '../../ui/ProviderLogo';
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
  BrowserSignInPanel,
  CodePasteField,
  CopyLinkButton,
  CustomEndpointDisclosure,
  DeviceCodePanel,
  IdentityVerdictNotice,
  KindOption,
  oauthNameSuggestion,
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
  identityOfResult,
  useOAuthConnectSession,
} from './useOAuthConnectSession';

/** `apikey`: inside the API-key flow, which owns its own sub-steps. */
type StepId =
  | 'provider'
  | 'kind'
  | 'signin'
  | 'paste'
  | 'finish'
  | 'apikey'
  | 'generic';

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
  const previousAccount = identityOfResult(meta.data);
  const oauthStatus = useUpstreamOAuthStatus(upstream?.id ?? null);
  // An upstream with no stored credential is being connected for the first
  // time — say "Connect", not "Reconnect", exactly like its nudge does.
  const firstConnect =
    upstream !== null && oauthStatus.data?.has_credentials === false;

  const [provider, setProvider] = useState<ProviderFamily>(() =>
    upstream ? providerOf(upstream.kind) : 'anthropic',
  );
  const [kind, setKind] = useState<UpstreamKind>(() =>
    upstream ? upstream.kind : 'anthropic_oauth',
  );
  const [step, setStep] = useState<StepId>(isCreate ? 'provider' : 'signin');
  const [sessionWanted, setSessionWanted] = useState(!isCreate);

  const session = useOAuthConnectSession(
    upstream
      ? {
          mode: 'reconnect',
          upstreamId: upstream.id,
          provider,
        }
      : { mode: 'create', provider },
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
    provider,
    onCancel: () => requestClose(),
    onBackToKind: () => setStep('kind'),
    onSignInInstead: () => {
      setProvider('anthropic');
      setKind('anthropic_oauth');
      setSessionWanted(true);
      setStep('signin');
    },
    onDone: (created) => {
      onCreated(created);
      onClose();
    },
  });
  const inApiKeyFlow = isCreate && kind.endsWith('_api_key');

  const outcome = session.outcome;
  useEffect(() => {
    if (!outcome) return;
    const draft = isCreate ? outcome.draft : null;
    if (draft) {
      setOauthName((current) =>
        current.trim() ? current : oauthNameSuggestion(provider, draft),
      );
    }
    if (step === 'signin') setStep('finish');
  }, [isCreate, outcome, provider, step]);
  const saving = createFromDraft.isPending;
  const busy = session.completing || saving || apiKey.busy;
  const draftExpired =
    isCreate && step === 'finish' && session.phase === 'expired';
  const needsDiscardConfirm =
    isCreate && step === 'finish' && outcome !== null && !draftExpired;

  const steps: Exclude<StepId, 'apikey'>[] = isCreate
    ? provider === 'openai'
      ? session.flow === 'paste_code'
        ? ['provider', 'kind', 'signin', 'paste', 'finish']
        : ['provider', 'kind', 'signin', 'finish']
      : provider === 'gemini'
        ? ['provider', 'kind', 'signin', 'paste', 'finish']
        : ['provider', 'kind', 'signin', 'paste', 'finish']
    : provider === 'openai'
      ? session.flow === 'paste_code'
        ? ['signin', 'paste', 'finish']
        : ['signin', 'finish']
      : provider === 'gemini'
        ? ['signin', 'paste', 'finish']
        : ['signin', 'paste', 'finish'];
  const rail = inApiKeyFlow
    ? [
        stepMeta('provider', true, provider),
        stepMeta('kind', true, provider),
        ...apiKey.rail,
      ]
    : steps.map((id) => stepMeta(id, isCreate, provider));
  const stepIndex = inApiKeyFlow
    ? step === 'provider'
      ? 0
      : step === 'kind'
        ? 1
        : 2 + apiKey.activeIndex
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

  const continueFromProvider = () => {
    setStep('kind');
  };

  const continueFromKind = () => {
    if (kind.endsWith('_oauth')) {
      setSessionWanted(true);
      setStep('signin');
    } else {
      setStep('apikey');
    }
  };

  const submitCode = async (code: string) => {
    const result = await session.complete(code);
    if (!result) return;
    const draft = isCreate ? result.draft : null;
    if (draft) {
      setOauthName((current) =>
        current.trim() ? current : oauthNameSuggestion(provider, draft),
      );
    }
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
    step === 'provider'
      ? 'Choose which provider to add.'
      : step === 'kind'
        ? 'Choose how to connect this provider.'
        : step === 'apikey'
          ? 'Nothing is saved until you add the upstream.'
          : isCreate
            ? 'Nothing is saved until you confirm the account.'
            : firstConnect
              ? `Sign in with the ${PROVIDER_LABELS[provider]} account this upstream should use.`
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
  if (step === 'provider') {
    footer = (
      <>
        {cancelButton}
        <Button variant="primary" onClick={continueFromProvider}>
          Continue
        </Button>
      </>
    );
  } else if (step === 'kind') {
    footer = (
      <>
        {backButton('provider')}
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
              {step === 'provider' ? (
                <ProviderStep
                  provider={provider}
                  onProviderChange={(next) => {
                    setProvider(next);
                    if (next !== provider) {
                      setKind(`${next}_oauth` as UpstreamKind);
                    }
                  }}
                />
              ) : null}

              {step === 'kind' ? (
                <KindStep
                  provider={provider}
                  kind={kind}
                  onKindChange={setKind}
                  onContinue={continueFromKind}
                />
              ) : null}

              {step === 'apikey' ? apiKey.body : null}

              {step === 'signin' ? (
                <div className="flex flex-col gap-4">
                  <StepHeading
                    title={
                      provider === 'openai'
                        ? 'Sign in with ChatGPT'
                        : provider === 'gemini'
                          ? 'Sign in with Google'
                          : 'Sign in with Claude'
                    }
                    subtitle={
                      provider === 'openai'
                        ? 'Use a ChatGPT subscription to connect Codex.'
                        : provider === 'gemini'
                          ? 'Connect your Antigravity account.'
                          : undefined
                    }
                  />
                  {upstream && !firstConnect && previousAccount ? (
                    <div className="flex flex-col gap-1.5">
                      <AccountCard
                        account={previousAccount}
                        provider={provider}
                        label="Sign in as"
                      />
                      <p className="text-body-sm text-text-muted">
                        Check that the browser account matches this upstream.
                      </p>
                    </div>
                  ) : null}
                  {provider === 'openai' ? (
                    session.flow === 'paste_code' ? (
                      <>
                        <p className="text-body-sm text-text-muted">
                          Sign in with a ChatGPT Plus, Pro, or Team account.
                        </p>
                        <BrowserSignInPanel
                          session={session}
                          onOpened={() => setStep('paste')}
                        />
                      </>
                    ) : (
                      <>
                        <p className="text-body-sm text-text-muted">
                          Sign in with a ChatGPT Plus, Pro, or Team account.
                          Open the verification page and enter the one-time code
                          below.
                        </p>
                        <DeviceCodePanel session={session} />
                      </>
                    )
                  ) : provider === 'gemini' ? (
                    <>
                      <p className="text-body-sm text-text-muted">
                        Sign in with your Google account to connect Antigravity.
                      </p>
                      <BrowserSignInPanel
                        session={session}
                        onOpened={() => setStep('paste')}
                      />
                    </>
                  ) : (
                    <>
                      <p className="text-body-sm text-text-muted">
                        A new Claude tab opens and asks you to approve access.
                        When it finishes it shows a code — that code is how
                        cc-lb proves the sign-in.
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
                    </>
                  )}
                </div>
              ) : null}

              {step === 'paste' ? (
                <div className="flex flex-col gap-4">
                  <StepHeading
                    title="Paste the code"
                    subtitle={
                      provider === 'openai' && session.flow === 'paste_code'
                        ? "After you approve, the tab lands on a page that won't load (localhost:1455). Copy the full address from that tab's address bar and paste it here."
                        : provider === 'gemini'
                          ? "After you approve, the tab lands on a page that won't load (localhost:51121). Copy the full address from that tab's address bar and paste it here. A bare authorization code also works."
                          : 'Paste what you copied — the code, or the whole page address both work.'
                    }
                  />
                  <CodePasteField
                    session={session}
                    autoFocus
                    placeholder={
                      provider === 'openai' && session.flow === 'paste_code'
                        ? 'Paste the localhost:1455 address'
                        : provider === 'gemini'
                          ? 'Paste the localhost:51121 address or code'
                          : undefined
                    }
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
                      provider={provider}
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
                      hint={`Requests go here instead of ${PROVIDER_LABELS[provider]}.`}
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
                        provider={outcome.provider}
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
                  {session.startNotice ? (
                    <Notice tone="info" role="status">
                      {session.startNotice}
                    </Notice>
                  ) : null}
                  <div className="flex items-center gap-4">
                    <SessionTimer session={session} />
                    {step === 'signin' && provider === 'anthropic' ? (
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
        description={`You already authorized a ${PROVIDER_LABELS[provider]} account, but it has not been saved as an upstream yet. Closing now discards it.`}
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
  provider: ProviderFamily,
): { title: string; hint: string } {
  switch (id) {
    case 'provider':
      return { title: 'Provider', hint: 'Choose a provider' };
    case 'kind':
      return { title: 'Connect', hint: 'Choose a credential type' };
    case 'generic':
      return { title: 'Credential', hint: 'Paste and save' };
    case 'signin':
      return {
        title: 'Sign in',
        hint: `Approve access in a ${
          provider === 'openai'
            ? 'ChatGPT'
            : provider === 'gemini'
              ? 'Google'
              : 'Claude'
        } tab`,
      };
    case 'paste':
      return { title: 'Paste code', hint: 'Bring the code back here' };
    case 'finish':
      return isCreate
        ? { title: 'Name & save', hint: 'Confirm the account' }
        : { title: 'Done', hint: 'Confirm the account' };
  }
}

function ProviderStep({
  provider,
  onProviderChange,
}: {
  provider: ProviderFamily;
  onProviderChange: (provider: ProviderFamily) => void;
}) {
  return (
    <div className="flex flex-col gap-4">
      <StepHeading
        title="Choose provider"
        subtitle="Add an upstream from any supported provider."
      />
      <fieldset className="flex flex-col gap-2">
        <legend className="sr-only">Provider</legend>
        {PROVIDER_FAMILIES.map((option) => (
          <button
            key={option}
            type="button"
            aria-pressed={provider === option}
            className={cx(
              'flex min-h-10 w-full items-center gap-3 rounded-sm border px-3 py-2 text-left text-body',
              provider === option
                ? 'border-text bg-selected text-text'
                : 'border-subtle text-text-muted hover:bg-overlay-2',
            )}
            onClick={() => onProviderChange(option)}
          >
            <ProviderLogo
              provider={option}
              size={24}
              className="shrink-0 text-text-muted"
            />
            <span className="text-label">
              {option === 'gemini' ? 'Google Gemini' : PROVIDER_LABELS[option]}
            </span>
          </button>
        ))}
      </fieldset>
    </div>
  );
}

function KindStep({
  provider,
  kind,
  onKindChange,
  onContinue,
}: {
  provider: ProviderFamily;
  kind: UpstreamKind;
  onKindChange: (kind: UpstreamKind) => void;
  onContinue: () => void;
}) {
  const descriptions: Record<
    ProviderFamily,
    { subscription: string; apiKey: string }
  > = {
    anthropic: {
      subscription: 'Claude Pro/Max',
      apiKey: 'Anthropic Console',
    },
    openai: {
      subscription: 'ChatGPT Plus/Pro/Team via Codex',
      apiKey: 'OpenAI Platform',
    },
    gemini: {
      subscription: 'Google AI Pro/Ultra via Gemini/Antigravity',
      apiKey: 'Google AI Studio',
    },
  };
  return (
    <div className="flex flex-col gap-4">
      <StepHeading title="How to connect" />
      <BaseRadioGroup
        name="kind"
        value={kind}
        onValueChange={(value) => onKindChange(value as UpstreamKind)}
        className="flex flex-col gap-2"
      >
        <KindOption
          value={`${provider}_oauth`}
          selected={kind.endsWith('_oauth')}
          icon={<UserRound className="w-4 h-4" strokeWidth={1.75} />}
          title="Subscription"
          description={descriptions[provider].subscription}
          onActivate={onContinue}
        />
        <KindOption
          value={`${provider}_api_key`}
          selected={kind.endsWith('_api_key')}
          icon={<KeyRound className="w-4 h-4" strokeWidth={1.75} />}
          title="API key"
          description={descriptions[provider].apiKey}
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
