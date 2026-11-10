// The API usage card's month strip: this calendar month (UTC) at Anthropic —
// tokens used, credits used at list price, and the balance. No Anthropic API
// returns a credit balance, so Balance is "—" with a Console Billing link,
// unless the latest logged request for this upstream ran into a billing stop;
// then the stop and its one fix are on screen. Provider-native billing stops
// use the same danger surface with provider-specific guidance. Provenance
// (reported or estimated) is the card's header badge; key status and
// permission fixes belong to the pane key notice and Access.

import type { ReactNode } from 'react';
import type {
  KeyBillingStopKind,
  KeyUsageMonth,
  KeyUsageProvenance,
} from '../../lib/api';
import { fmtUsdCents, splitNum } from '../../lib/format';
import { cx, Skeleton, StatusBadge } from '../ui/primitives';
import { RelativeTime } from '../ui/RelativeTime';
import { CONSOLE_BILLING_URL } from './connect/apiKeyCopy';
import { ConsoleLink } from './connect/parts';

/** `Nov 1`: month boundaries are UTC, so the day is read in UTC. */
function utcDay(unixSecs: number): string {
  return new Date(unixSecs * 1000).toLocaleDateString('en-US', {
    month: 'short',
    day: 'numeric',
    timeZone: 'UTC',
  });
}

const STOP_LABEL: Record<KeyBillingStopKind, string> = {
  credits_exhausted: 'Out of credits',
  tier_spend_cap: 'Spend limit reached',
  spend_limit: 'Spend limit reached',
  billing_error: 'Billing issue',
  openai_credit_balance_exhausted: 'Credit balance exhausted',
  openai_organization_spend_limit_exceeded: 'Organization spend limit exceeded',
  openai_project_spend_limit_exceeded: 'Project spend limit exceeded',
  openai_organization_usage_limit_exceeded: 'Organization usage limit exceeded',
  gemini_payment_required: 'Payment required',
  gemini_failed_precondition: 'Billing prerequisite failed',
};

type StopGuidance = {
  action: string;
  href?: string;
  linkLabel?: string;
};

const OPENAI_ERROR_CODES_URL =
  'https://developers.openai.com/api/docs/guides/error-codes';
const GEMINI_API_ERRORS_URL =
  'https://ai.google.dev/gemini-api/docs/api-errors';

function stopLabel(kind: string): string {
  return STOP_LABEL[kind as KeyBillingStopKind] ?? 'Billing stop';
}

function stopGuidance(
  kind: string,
  provider: KeyUsageProvenance['provider'],
  resumes: string,
): StopGuidance {
  switch (kind) {
    case 'credits_exhausted':
      return {
        action:
          'Buy credits or turn on auto-reload in Console → Billing → requests resume.',
      };
    case 'tier_spend_cap':
      return {
        action: `Request a tier increase in Console → resume before ${resumes}.`,
      };
    case 'spend_limit':
      return {
        action: 'Raise the limit in Console → Billing → requests resume.',
      };
    case 'billing_error':
      return {
        action:
          'Fix the payment method in Console → Billing → requests resume.',
      };
    case 'openai_credit_balance_exhausted':
      return {
        action: 'Add credits to resume OpenAI requests.',
        href: OPENAI_ERROR_CODES_URL,
        linkLabel: 'OpenAI error codes',
      };
    case 'openai_organization_spend_limit_exceeded':
      return {
        action: 'Raise the organization spend limit to resume requests.',
        href: OPENAI_ERROR_CODES_URL,
        linkLabel: 'OpenAI error codes',
      };
    case 'openai_project_spend_limit_exceeded':
      return {
        action: 'Raise the project spend limit to resume requests.',
        href: OPENAI_ERROR_CODES_URL,
        linkLabel: 'OpenAI error codes',
      };
    case 'openai_organization_usage_limit_exceeded':
      return {
        action: 'Request a higher organization usage limit to resume requests.',
        href: OPENAI_ERROR_CODES_URL,
        linkLabel: 'OpenAI error codes',
      };
    case 'gemini_payment_required':
      return {
        action: 'Add credits or enable auto-reload to resume requests.',
        href: GEMINI_API_ERRORS_URL,
        linkLabel: 'Gemini API errors',
      };
    case 'gemini_failed_precondition':
      return {
        action:
          'Verify project billing and account prerequisites to resume requests.',
        href: GEMINI_API_ERRORS_URL,
        linkLabel: 'Gemini API errors',
      };
    default:
      return {
        action:
          provider === 'openai'
            ? 'Review OpenAI billing settings to resume requests.'
            : provider === 'gemini'
              ? 'Review Gemini billing settings to resume requests.'
              : 'Review the provider billing settings to resume requests.',
      };
  }
}

function isAnthropicStop(kind: string): boolean {
  return (
    kind === 'credits_exhausted' ||
    kind === 'tier_spend_cap' ||
    kind === 'spend_limit' ||
    kind === 'billing_error'
  );
}

const CELL_CLASS = 'flex min-w-0 flex-col gap-0.5';
/** Cells after the first carry a left rule; Balance wraps under on narrow. */
const RULED_CELL_CLASS = 'border-l border-subtle pl-4';

function Cell({
  label,
  value,
  sub,
  className,
}: {
  label: string;
  value: ReactNode;
  sub: ReactNode;
  className?: string;
}) {
  return (
    <div className={cx(CELL_CLASS, className)} data-cell={label}>
      <dt className="text-caption text-text-faint">{label}</dt>
      <dd className="text-body font-medium tabular-nums text-text">{value}</dd>
      <dd className="text-caption text-text-muted">{sub}</dd>
    </div>
  );
}

const NO_READING = (
  <span className="text-text-faint">
    <span aria-hidden="true">—</span>
    <span className="sr-only">No reading</span>
  </span>
);

export function CreditStrip({
  month,
  provenance,
  isLoading,
  loadError = null,
}: {
  month: KeyUsageMonth | undefined;
  provenance: KeyUsageProvenance | undefined;
  isLoading: boolean;
  /**
   * Why the latest usage request failed. Without figures the strip says so
   * instead of showing 0; with retained figures it says they were not
   * refreshed. The card's one announced refresh surface.
   */
  loadError?: string | null;
}) {
  const loading = month === undefined && isLoading;
  const reported = provenance?.source === 'reported';
  const resets = month ? utcDay(month.end_unix_secs) : null;
  // Request-log retention can start the estimated month after the 1st.
  const fromFirst =
    month === undefined ||
    new Date(month.start_unix_secs * 1000).getUTCDate() === 1;
  const period = month
    ? `${fromFirst ? 'This month (UTC)' : `Since ${utcDay(month.start_unix_secs)} (UTC)`} · resets ${resets}`
    : 'This month (UTC)';

  const billing = <ConsoleLink href={CONSOLE_BILLING_URL}>Billing</ConsoleLink>;
  const stop = month?.billing_stop ?? null;
  const stopKind = stop?.kind as string | undefined;
  const guidance = stopKind
    ? stopGuidance(stopKind, provenance?.provider, resets ?? '')
    : null;
  const anthropicStop = stopKind ? isAnthropicStop(stopKind) : false;
  const tokens = month ? splitNum(month.tokens) : null;
  const skeletonValue = (
    <Skeleton as="span" className="inline-block h-5 w-16" />
  );
  const skeletonSub = <Skeleton as="span" className="inline-block h-3 w-20" />;

  return (
    <div data-testid="api-key-credits" className="flex flex-col gap-2">
      {loading ? (
        <Skeleton className="h-3 w-40" />
      ) : (
        <p className="text-caption text-text-faint">{period}</p>
      )}
      <dl className="grid grid-cols-2 gap-y-3 @md:grid-cols-3">
        <Cell
          label="Tokens used"
          value={
            loading ? (
              skeletonValue
            ) : tokens ? (
              <>
                {tokens.value}
                {tokens.unit ? (
                  <span className="text-caption text-text-muted">
                    {tokens.unit}
                  </span>
                ) : null}
              </>
            ) : (
              NO_READING
            )
          }
          sub={
            loading
              ? skeletonSub
              : month
                ? !reported
                  ? 'cc-lb traffic only'
                  : provenance?.attribution === 'workspace'
                    ? 'Whole workspace'
                    : 'Reported'
                : null
          }
        />
        <Cell
          label="Credits used"
          className={RULED_CELL_CLASS}
          value={
            loading
              ? skeletonValue
              : month
                ? fmtUsdCents(month.cost_usd_micros)
                : NO_READING
          }
          sub={loading ? skeletonSub : month ? 'At list price' : null}
        />
        <Cell
          label="Balance"
          className={cx(
            'col-span-2 border-t border-border-row pt-3',
            '@md:col-span-1 @md:border-t-0 @md:pt-0',
            '@md:border-l @md:border-subtle @md:pl-4',
          )}
          value={
            loading ? (
              skeletonValue
            ) : stop ? (
              <StatusBadge tone="danger" label={stopLabel(stopKind ?? '')} />
            ) : (
              NO_READING
            )
          }
          sub={
            loading ? (
              skeletonSub
            ) : stop ? (
              <>
                {stopKind === 'tier_spend_cap' ? (
                  <>Resumes {resets}</>
                ) : (
                  <>
                    Seen{' '}
                    <RelativeTime
                      compact
                      ts={stop.observed_at_unix_secs * 1000}
                    />
                  </>
                )}
                {anthropicStop ? (
                  <>
                    {' · '}
                    {billing}
                  </>
                ) : null}
              </>
            ) : (
              <>Not in API · {billing}</>
            )
          }
        />
      </dl>
      {stop ? (
        <p className="text-caption text-text-muted">
          {guidance?.action ??
            'Review provider billing guidance to resume requests.'}{' '}
          {guidance?.href && guidance.linkLabel ? (
            <ConsoleLink href={guidance.href}>{guidance.linkLabel}</ConsoleLink>
          ) : anthropicStop ? (
            billing
          ) : null}
        </p>
      ) : null}
      {!loading && loadError ? (
        <p role="alert" className="text-caption text-danger-text">
          {month === undefined
            ? `Couldn't load this month: ${loadError}`
            : `Couldn't refresh usage: ${loadError} — showing the last loaded figures, which may be out of date.`}
        </p>
      ) : null}
    </div>
  );
}
