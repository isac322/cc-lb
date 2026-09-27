// Presentational building blocks shared by every OAuth connect variant, so the
// variants differ in choreography, not in how a single concept is rendered.

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
import {
  Button,
  cx,
  INPUT_CLASS,
  INPUT_SM_CLASS,
  Notice,
} from '../../ui/primitives';
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
  // Mirrors Button's `primary` look and sizes: this anchor is the step's
  // commit action, rendered as a real link so it is never popup-blocked.
  const sizeClass =
    size === 'lg'
      ? 'h-9 px-4 text-sm gap-2 [&_svg]:size-4'
      : size === 'sm'
        ? 'h-7 px-2.5 text-xs gap-1.5 [&_svg]:size-3.5'
        : 'h-8 px-3 text-[0.8125rem] gap-1.5 [&_svg]:size-3.5';
  const base = cx(
    'inline-flex items-center justify-center rounded-sm font-medium whitespace-nowrap transition-colors select-none [&_svg]:shrink-0',
    sizeClass,
    className,
  );
  if (!ready) {
    return (
      <span
        aria-disabled="true"
        className={cx(
          base,
          'bg-panel-strong border border-subtle text-text opacity-40 cursor-not-allowed',
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
      className={cx(
        base,
        'border border-accent bg-accent text-accent-ink hover:brightness-110 focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent',
      )}
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
      className="inline-flex items-center gap-1 text-caption text-text-faint underline underline-offset-2 hover:text-text"
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
      <span className="text-success-text inline-flex items-center gap-1">
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
      ? 'text-success-text'
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
      <div className="flex items-center gap-2 text-body-sm text-success-text">
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
