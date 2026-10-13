// Presentational building blocks shared by every connect path (Claude sign-in
// and API key), so the paths differ in choreography, not in how a single
// concept is rendered.

import { Radio as BaseRadio } from '@base-ui/react/radio';
import {
  Check,
  CheckCircle2,
  Clock,
  Copy,
  ExternalLink,
  Loader2,
  RotateCw,
  ShieldCheck,
} from 'lucide-react';
import {
  type ClipboardEvent,
  type KeyboardEvent,
  type ReactNode,
  useEffect,
  useId,
  useRef,
  useState,
} from 'react';
import { toast } from 'sonner';
import type { ApiKeyOrgCapability } from '../../../lib/api';
import {
  Badge,
  Button,
  buttonClassName,
  cx,
  Field,
  INPUT_CLASS,
  INPUT_SM_CLASS,
  Notice,
} from '../../ui/primitives';
import { CONSOLE_WORKSPACES_URL } from './apiKeyCopy';
import {
  matchPasteToSession,
  type ParsedOAuthPaste,
  parseOAuthPaste,
} from './parseOAuthPaste';
import {
  type AccountIdentity,
  type ConnectOutcome,
  formatRemaining,
  type OAuthConnectSession,
} from './useOAuthConnectSession';

// ─── Sign-in link ────────────────────────────────────────────────────────────
// A real anchor: the browser treats the click as a user gesture, so it is never
// popup-blocked. Disabled (and visibly so) until the session URL exists.
export function SignInLink({
  session,
  label = 'Sign in with Claude',
  size = 'md',
  onOpened,
  className,
}: {
  session: OAuthConnectSession;
  label?: string;
  size?: 'sm' | 'md' | 'lg';
  onOpened?: () => void;
  className?: string;
}) {
  const ready = session.phase === 'ready' && session.authorizeUrl;
  // The Button primitive's `primary` chrome on a real anchor: this is the
  // step's commit action, rendered as a link so it is never popup-blocked.
  // Until the URL exists it reads as a disabled Button (secondary look).
  if (!ready) {
    return (
      <span
        aria-disabled="true"
        className={cx(
          buttonClassName('secondary', size),
          'control-disabled',
          className,
        )}
      >
        {session.phase === 'starting' ? (
          <Loader2 className="animate-spin" />
        ) : (
          <ExternalLink strokeWidth={1.75} />
        )}
        {session.phase === 'starting' ? 'Preparing sign-in…' : label}
      </span>
    );
  }
  return (
    <a
      href={session.authorizeUrl ?? undefined}
      target="_blank"
      rel="noopener noreferrer"
      onClick={onOpened}
      className={cx(buttonClassName('primary', size), className)}
    >
      <ExternalLink strokeWidth={1.75} />
      {label}
    </a>
  );
}

// ─── Copy link ───────────────────────────────────────────────────────────────
// For operators signed into Claude in another browser profile. The admin web
// is often served over plain http, where navigator.clipboard is undefined, so
// fall back to the legacy selection-based copy.
export function copyText(text: string): Promise<boolean> {
  if (navigator.clipboard?.writeText) {
    return navigator.clipboard.writeText(text).then(
      () => true,
      () => copyViaSelection(text),
    );
  }
  return Promise.resolve(copyViaSelection(text));
}

function copyViaSelection(text: string): boolean {
  const area = document.createElement('textarea');
  area.value = text;
  area.setAttribute('readonly', '');
  area.style.position = 'fixed';
  area.style.opacity = '0';
  document.body.appendChild(area);
  area.select();
  let ok = false;
  try {
    ok = document.execCommand('copy');
  } catch {
    ok = false;
  }
  area.remove();
  return ok;
}

export function CopyLinkButton({
  authorizeUrl,
  label = 'Copy link',
}: {
  authorizeUrl: string | null;
  label?: string;
}) {
  const [copied, setCopied] = useState(false);
  useEffect(() => {
    if (!copied) return;
    const id = window.setTimeout(() => setCopied(false), 2000);
    return () => window.clearTimeout(id);
  }, [copied]);
  if (!authorizeUrl) return null;
  return (
    <button
      type="button"
      title="Open it in the browser profile where the right Claude account is signed in"
      className="inline-flex items-center gap-1 text-caption text-text-faint underline underline-offset-2 hover:text-text max-md:min-h-10"
      onClick={() => {
        void copyText(authorizeUrl).then((ok) => {
          if (ok) setCopied(true);
          else
            toast.error(
              'Could not copy — right-click “Sign in with Claude” and copy the link.',
            );
        });
      }}
    >
      {copied ? <Check className="w-3 h-3" /> : <Copy className="w-3 h-3" />}
      {copied ? 'Link copied' : label}
    </button>
  );
}

// ─── Session timer ───────────────────────────────────────────────────────────
export function SessionTimer({
  session,
  className,
}: {
  session: OAuthConnectSession;
  className?: string;
}) {
  if (session.phase === 'expired') {
    return (
      <span
        className={cx(
          'inline-flex items-center gap-1.5 text-caption text-warn-text',
          className,
        )}
      >
        <Clock className="w-3.5 h-3.5" />
        Sign-in link expired
        <button
          type="button"
          onClick={() => void session.restart()}
          className="inline-flex items-center gap-1 underline underline-offset-2 hover:text-text"
        >
          <RotateCw className="w-3 h-3" />
          Get a new link
        </button>
      </span>
    );
  }
  if (session.phase !== 'ready') return null;
  const low = session.remainingMs < 120_000;
  return (
    <span
      title="The sign-in link is valid for 15 minutes."
      className={cx(
        'inline-flex items-center gap-1.5 text-caption tabular-nums',
        low ? 'text-warn-text' : 'text-text-faint',
        className,
      )}
    >
      <Clock className="w-3.5 h-3.5" />
      Link valid for {formatRemaining(session.remainingMs)}
    </span>
  );
}

// ─── Start failure ───────────────────────────────────────────────────────────
export function SessionStartError({
  session,
}: {
  session: OAuthConnectSession;
}) {
  if (session.phase !== 'start_failed') return null;
  return (
    <Notice
      tone="danger"
      action={
        <Button size="sm" onClick={() => void session.restart()}>
          Try again
        </Button>
      }
    >
      Could not prepare a sign-in link: {session.startError}
    </Notice>
  );
}

// ─── Paste-anything field ────────────────────────────────────────────────────
// Accepts the code, `code#state`, or the full callback URL. A paste that parses
// cleanly submits immediately; typing requires Enter/Connect.
export function CodePasteField({
  session,
  onSubmit,
  autoFocus = true,
  compact = false,
  placeholder = 'Paste the code from the Claude page',
}: {
  session: OAuthConnectSession;
  onSubmit: (code: string) => void;
  autoFocus?: boolean;
  compact?: boolean;
  placeholder?: string;
}) {
  const [value, setValue] = useState('');
  const inputRef = useRef<HTMLInputElement>(null);
  const feedbackId = useId();
  const parsed: ParsedOAuthPaste = parseOAuthPaste(value);
  const match = matchPasteToSession(parsed, session.oauthState);
  const disabled = session.phase !== 'ready' || session.completing;
  const invalid =
    session.completeError !== null ||
    parsed.kind === 'invalid' ||
    match === 'other_session';

  useEffect(() => {
    if (autoFocus && session.phase === 'ready') inputRef.current?.focus();
  }, [autoFocus, session.phase]);

  const trySubmit = (raw: string) => {
    const p = parseOAuthPaste(raw);
    if (p.kind !== 'code') return;
    if (matchPasteToSession(p, session.oauthState) === 'other_session') return;
    onSubmit(p.code);
  };

  const onPaste = (e: ClipboardEvent<HTMLInputElement>) => {
    const text = e.clipboardData.getData('text');
    if (!text) return;
    e.preventDefault();
    setValue(text.trim());
    session.clearCompleteError();
    trySubmit(text);
  };
  const onKeyDown = (e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      trySubmit(value);
    }
  };

  let feedback: ReactNode = null;
  if (session.completeError) {
    feedback = (
      <span className="text-danger-text">
        {friendlyCompleteError(session.completeError)}
      </span>
    );
  } else if (parsed.kind === 'invalid') {
    feedback = <span className="text-warn-text">{parsed.reason}</span>;
  } else if (match === 'other_session') {
    feedback = (
      <span className="text-warn-text">
        This code is from a different or older sign-in. Use the link above again
        and copy the new code.
      </span>
    );
  } else if (parsed.kind === 'code') {
    feedback = (
      <span className="text-traffic-success-text inline-flex items-center gap-1">
        <CheckCircle2 className="w-3.5 h-3.5" />
        {match === 'match'
          ? 'Code recognized for this sign-in'
          : 'Code recognized'}
      </span>
    );
  }

  return (
    <div className="flex flex-col gap-1.5">
      <div className="flex items-center gap-2">
        <input
          ref={inputRef}
          aria-label="Authorization code"
          aria-describedby={feedbackId}
          aria-invalid={invalid || undefined}
          className={cx(compact ? INPUT_SM_CLASS : INPUT_CLASS, 'font-mono')}
          value={value}
          disabled={disabled}
          spellCheck={false}
          autoComplete="off"
          placeholder={placeholder}
          onPaste={onPaste}
          onKeyDown={onKeyDown}
          onChange={(e) => {
            setValue(e.target.value);
            session.clearCompleteError();
          }}
        />
        <Button
          variant="primary"
          size={compact ? 'md' : 'lg'}
          loading={session.completing}
          disabled={
            disabled || parsed.kind !== 'code' || match === 'other_session'
          }
          onClick={() => trySubmit(value)}
        >
          {session.completing ? 'Connecting…' : 'Connect'}
        </Button>
      </div>
      {/* Live region: validation and connection failures are announced, not
          just painted, since a rejected paste leaves focus where it was. */}
      <div
        id={feedbackId}
        role="status"
        aria-live="polite"
        className="min-h-4 text-caption"
      >
        {feedback}
      </div>
    </div>
  );
}

export function friendlyCompleteError(message: string): string {
  if (/expired|already used|restart/i.test(message)) {
    return 'This sign-in expired or was already used. Get a new link and try again.';
  }
  if (/invalid_grant|invalid code|authorization code/i.test(message)) {
    return 'Claude did not accept this code. It may be mistyped or already used — sign in again for a fresh one.';
  }
  return `Connection failed: ${message}`;
}

// ─── Account card ────────────────────────────────────────────────────────────
export function AccountCard({
  account,
  label,
  tone = 'neutral',
  className,
}: {
  account: AccountIdentity | null;
  label?: string;
  tone?: 'neutral' | 'ok' | 'danger';
  className?: string;
}) {
  const labelTone =
    tone === 'ok'
      ? 'text-traffic-success-text'
      : tone === 'danger'
        ? 'text-danger-text'
        : 'text-text-muted';
  return (
    <div className={cx('well p-3', className)}>
      {label ? (
        <div className={cx('mb-1.5 text-label', labelTone)}>{label}</div>
      ) : null}
      {account && (account.email || account.displayName) ? (
        <div className="flex items-center gap-3 min-w-0">
          <div className="h-8 w-8 shrink-0 rounded-full bg-overlay-4 text-text flex items-center justify-center text-body-sm font-medium">
            {(account.displayName ?? account.email ?? '?')
              .slice(0, 1)
              .toUpperCase()}
          </div>
          <div className="min-w-0">
            <div className="text-body-sm text-text truncate">
              {account.email ?? account.displayName}
            </div>
            <div className="text-caption text-text-faint truncate">
              {[account.organizationName, planLabel(account)]
                .filter(Boolean)
                .join(' · ') || 'Claude account'}
            </div>
          </div>
        </div>
      ) : (
        <div className="text-body-sm text-text-faint">
          No account connected yet
        </div>
      )}
    </div>
  );
}

export function planLabel(account: AccountIdentity): string | null {
  const type = account.organizationType;
  if (!type) return null;
  if (/max/i.test(type)) return 'Claude Max';
  if (/pro/i.test(type)) return 'Claude Pro';
  if (/team/i.test(type)) return 'Claude Team';
  if (/enterprise/i.test(type)) return 'Claude Enterprise';
  return type.replace(/_/g, ' ');
}

// ─── Identity verdict ────────────────────────────────────────────────────────
export function IdentityVerdictNotice({
  outcome,
}: {
  outcome: ConnectOutcome;
}) {
  if (outcome.verdict === 'same') {
    return (
      <div className="flex items-center gap-2 text-body-sm text-traffic-success-text">
        <ShieldCheck className="size-4" strokeWidth={1.75} />
        Same account as before — nothing else changed.
      </div>
    );
  }
  if (outcome.verdict === 'different') {
    return (
      <Notice tone="danger" title="A different Claude account was connected">
        <div className="mt-2 grid grid-cols-1 gap-2 sm:grid-cols-2">
          <AccountCard account={outcome.previousAccount} label="Before" />
          <AccountCard account={outcome.account} label="Now" tone="danger" />
        </div>
        <p className="mt-2">
          Traffic for this upstream now bills the new account. If this was a
          mistake, sign out of Claude, sign in as the original account, and
          reconnect again.
        </p>
      </Notice>
    );
  }
  if (outcome.verdict === 'unknown') {
    return (
      <div className="text-body-sm text-text-muted">
        Connected. Account details are still loading — check the Subscription
        card to confirm which account this is.
      </div>
    );
  }
  return null;
}

// ─── Token lifetime explainer ────────────────────────────────────────────────
// Calm, outcome-first copy: a fallback is still a working connection.
export function TokenLifetimeNote({ outcome }: { outcome: ConnectOutcome }) {
  if (!outcome.longLivedFallback) {
    const days =
      outcome.grantedExpiresInSecs === null
        ? null
        : Math.round(outcome.grantedExpiresInSecs / 86_400);
    return (
      <div className="text-caption text-text-faint flex items-center gap-1.5">
        <CheckCircle2 className="size-3.5 text-ok" strokeWidth={1.75} />
        {days === null
          ? 'Long-lived connection — it needs no renewal until it expires.'
          : `Long-lived connection — no sign-in needed for about ${days} days.`}
      </div>
    );
  }
  return (
    <div
      data-testid="oauth-long-lived-fallback-notice"
      data-reason={outcome.fallbackReason ?? 'unknown'}
    >
      <Notice tone="warning" title="Connected — renews automatically">
        {outcome.fallbackReason === 'scope_rejected'
          ? 'Claude will not grant a year-long connection for the scopes configured in oauth.anthropic.scopes, '
          : 'Claude did not grant a year-long connection for this account, '}
        so cc-lb keeps it alive by renewing it in the background. You may be
        asked to sign in again in about 30 days. When Claude reports when
        renewal ends, cc-lb reminds you beforehand; otherwise a reconnect notice
        appears if renewal stops working.
      </Notice>
    </div>
  );
}

/** Quiet text action: "Don't have a key?", "Use a custom endpoint". */
export const QUIET_LINK_CLASS =
  'w-fit text-caption text-text-faint underline underline-offset-2 hover:text-text disabled:control-disabled max-md:min-h-10';
/** Inline action inside notice or caption copy (link or button). */
export const ANCHOR_CLASS =
  'inline-flex w-fit items-center gap-1 text-accent-text underline underline-offset-2 hover:text-text';

/** An Anthropic Console page, opened beside cc-lb. */
export function ConsoleLink({
  href,
  children,
}: {
  href: string;
  children: ReactNode;
}) {
  return (
    <a
      href={href}
      target="_blank"
      rel="noopener noreferrer"
      className={ANCHOR_CLASS}
    >
      {children}
      <ExternalLink className="size-3 shrink-0" strokeWidth={1.75} />
    </a>
  );
}

const WORKSPACE_LIST_WHY: Partial<Record<ApiKeyOrgCapability, string>> = {
  denied: "This key's account lacks the role to list workspaces.",
  error: "Anthropic didn't return the list — Re-check, or paste the ID.",
  not_applicable: 'Workspace lists need api.anthropic.com — paste the ID.',
};

/**
 * Manual workspace-ID help, shared by the connect dialog and the pane's
 * Access → Workspace: why there is no list, then where the ID lives. A
 * steady-state limit, so two quiet caption lines rather than a notice.
 */
export function WorkspaceIdHelp({ list }: { list: ApiKeyOrgCapability }) {
  const why = WORKSPACE_LIST_WHY[list];
  return (
    <div className="flex flex-col gap-0.5 text-caption text-text-faint">
      {why ? <span>{why}</span> : null}
      <span>
        <ConsoleLink href={CONSOLE_WORKSPACES_URL}>
          Open Workspaces in Console
        </ConsoleLink>{' '}
        → copy the <span className="font-mono text-data">wrkspc_…</span> in the
        URL.
      </span>
    </div>
  );
}

/**
 * The last row of a create step: a custom Base URL is a rare need, so the
 * field stays behind a quiet "Use a custom endpoint" link. It opens empty —
 * the Anthropic URL is never shown as a value — and "Use the default
 * endpoint" folds it away again. Focus follows the toggle.
 */
export function CustomEndpointDisclosure({
  open,
  value,
  busy,
  hint = 'Requests and key checks go here instead of Anthropic.',
  onOpen,
  onChange,
  onUseDefault,
}: {
  open: boolean;
  value: string;
  busy: boolean;
  hint?: string;
  /** Opening starts from an empty value. */
  onOpen: () => void;
  onChange: (value: string) => void;
  /** Back to Anthropic; the caller resets its value and closes. */
  onUseDefault: () => void;
}) {
  const fieldId = useId();
  /** Where focus goes after the toggle; consumed on mount. */
  const focusNext = useRef<'field' | 'trigger' | null>(null);

  if (!open) {
    return (
      <button
        type="button"
        ref={(el) => {
          if (el && focusNext.current === 'trigger') {
            focusNext.current = null;
            el.focus();
          }
        }}
        className={cx(QUIET_LINK_CLASS, 'mt-1')}
        aria-expanded={false}
        aria-controls={fieldId}
        disabled={busy}
        onClick={() => {
          focusNext.current = 'field';
          onOpen();
        }}
      >
        Use a custom endpoint
      </button>
    );
  }
  return (
    <div className="mt-1 flex flex-col gap-1.5">
      <Field label="Base URL" hint={hint}>
        <input
          id={fieldId}
          ref={(el) => {
            if (el && focusNext.current === 'field') {
              focusNext.current = null;
              el.focus();
            }
          }}
          className={cx(
            INPUT_CLASS,
            'disabled:cursor-not-allowed disabled:border-subtle disabled:text-text-faint',
          )}
          value={value}
          placeholder="https://proxy.example.com"
          spellCheck={false}
          disabled={busy}
          onChange={(e) => onChange(e.target.value)}
        />
      </Field>
      <button
        type="button"
        className={QUIET_LINK_CLASS}
        // The fold side of the same disclosure: it controls the open field.
        aria-expanded={true}
        aria-controls={fieldId}
        disabled={busy}
        onClick={() => {
          focusNext.current = 'trigger';
          onUseDefault();
        }}
      >
        Use the default endpoint
      </button>
    </div>
  );
}

// ─── Step chrome ─────────────────────────────────────────────────────────────
// Radio cards, step headings and the success header every connect path uses.
export function SuccessHeader({
  title,
  subtitle,
}: {
  title: string;
  subtitle: string;
}) {
  return (
    <div className="flex items-center gap-2.5">
      <CheckCircle2 className="w-5 h-5 shrink-0 text-ok" strokeWidth={1.75} />
      <div>
        <h3 className="text-title-card text-text">{title}</h3>
        <p className="text-body-sm text-text-muted">{subtitle}</p>
      </div>
    </div>
  );
}

/** Radio card; render inside a Base UI `RadioGroup`. */
export function KindOption({
  value,
  selected,
  icon,
  title,
  badge,
  description,
  onActivate,
}: {
  value: string;
  selected: boolean;
  icon: ReactNode;
  title: string;
  badge?: string;
  description: string;
  onActivate: () => void;
}) {
  return (
    <label
      onDoubleClick={onActivate}
      className={cx(
        'flex items-start gap-3 p-3 rounded-sm border cursor-pointer transition-colors',
        selected
          ? 'border-accent bg-overlay-2'
          : 'border-subtle hover:bg-overlay-2',
      )}
    >
      <span
        className={cx(
          'flex h-8 w-8 shrink-0 items-center justify-center rounded-full transition-colors',
          selected
            ? 'bg-accent-dim text-accent-text'
            : 'bg-overlay-3 text-text-muted',
        )}
      >
        {icon}
      </span>
      <span className="min-w-0 flex-1">
        <span className="flex flex-wrap items-center gap-x-2 gap-y-1">
          <span className="text-body font-medium text-text">{title}</span>
          {badge ? <Badge tone="accent">{badge}</Badge> : null}
        </span>
        <span className="mt-0.5 block text-body-sm text-text-muted">
          {description}
        </span>
      </span>
      <BaseRadio.Root
        value={value}
        className="mt-1 flex h-4 w-4 shrink-0 items-center justify-center rounded-full border border-subtle-strong bg-bg transition-colors data-[checked]:border-accent focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent"
      >
        <BaseRadio.Indicator className="h-2 w-2 rounded-full bg-accent" />
      </BaseRadio.Root>
    </label>
  );
}

export function StepHeading({
  title,
  subtitle,
}: {
  title: string;
  subtitle?: string;
}) {
  return (
    <div>
      <h3 className="text-title-card text-text">{title}</h3>
      {subtitle ? (
        <p className="mt-1 text-body-sm text-text-muted">{subtitle}</p>
      ) : null}
    </div>
  );
}
