import { Link, useRouter } from '@tanstack/react-router';
import { Copy } from 'lucide-react';
import type { ReactNode } from 'react';
import { eventTime } from '../../lib/api';
import {
  getRequestOutcome,
  type RequestOutcome,
  requestOutcomeTone,
} from '../../lib/format';
import { formatAbsolute, useLocale, useTimezone } from '../../lib/locale';
import type { RequestEventWithPhase } from '../../lib/RequestEventTypes';
import { reasoningBadgeText } from '../../lib/reasoningTier';
import {
  type ErrorCodeTarget,
  explainErrorCode,
  PRINCIPAL_LIMITS_ANCHOR,
  PRINCIPAL_ROUTER_ANCHOR,
} from '../../lib/requestErrorCodes';
import { useCopyButton } from '../../lib/useCopyButton';
import { Badge, cx, Hint, Skeleton } from './primitives';
import { RelativeTime } from './RelativeTime';
import { SessionChip } from './SessionChip';

const DASH = '—';

const PRINCIPAL_KIND_LABEL: Record<string, string> = {
  human: 'Human',
  machine: 'Machine',
  admin: 'Admin',
  api_key: 'API key',
  o_auth_subject: 'OAuth subject',
  oauth_subject: 'OAuth subject',
  internal_key: 'Internal key',
  workload_identity: 'Workload identity',
  subscription_bearer: 'Subscription bearer',
};

/** Sentence-case label for a server principal kind; unknown kinds keep their words. */
function principalKindLabel(kind: string): string {
  const known = PRINCIPAL_KIND_LABEL[kind];
  if (known) return known;
  const words = kind.replaceAll('_', ' ');
  return words.charAt(0).toUpperCase() + words.slice(1);
}

export function RequestOutcomeBadge({ outcome }: { outcome: RequestOutcome }) {
  if (outcome.type === 'partial') {
    return (
      <span className="inline-flex items-center gap-1.5 text-text-faint min-w-0 whitespace-normal text-right break-words">
        <span className="w-3 h-3 border-2 border-text-faint border-t-transparent rounded-full animate-spin shrink-0" />
        <span className="min-w-0 break-words">In progress</span>
      </span>
    );
  }
  if (outcome.type === 'client_disconnected') {
    return (
      <Badge
        tone="warn"
        className="min-w-0 whitespace-normal text-right break-words"
      >
        Client disconnected
      </Badge>
    );
  }
  if (outcome.type === 'semantic_error') {
    return (
      <Badge
        tone="danger"
        className="min-w-0 whitespace-normal text-right break-words"
      >
        {outcome.label}
      </Badge>
    );
  }
  return (
    <Badge
      tone={requestOutcomeTone(outcome)}
      className="min-w-0 whitespace-normal text-right break-words"
    >
      {outcome.status}
    </Badge>
  );
}

/** Absolute time in the configured timezone, with the relative age under it. */
function AbsoluteTime({ ts }: { ts: Date | null }) {
  const { effective: locale } = useLocale();
  const { effective: timezone } = useTimezone();
  if (ts == null) return <span className="text-text-faint">{DASH}</span>;
  return (
    <span className="flex flex-col items-end min-w-0">
      <span className="tabular-nums break-all">
        {formatAbsolute(ts, locale, timezone)}
      </span>
      <RelativeTime ts={ts} className="text-caption text-text-faint" />
    </span>
  );
}

export function RequestOutcomeTableCell({
  outcome,
}: {
  outcome: RequestOutcome;
}) {
  if (outcome.type === 'partial') {
    return (
      <span className="inline-flex items-center gap-1.5">
        <span className="w-3 h-3 border-2 border-text-faint border-t-transparent rounded-full animate-spin" />
        In progress
      </span>
    );
  }
  if (outcome.type === 'client_disconnected') {
    return (
      <span className="inline-flex items-center gap-1.5">
        Client disconnected
      </span>
    );
  }
  if (outcome.type === 'semantic_error') {
    return <>{outcome.label}</>;
  }
  return <>{outcome.status}</>;
}

export function RequestEventIdentity({
  event,
  principalLabel,
  isPartial,
  isDetailPending = false,
}: {
  event: RequestEventWithPhase;
  principalLabel: string;
  isPartial: boolean;
  isDetailPending?: boolean;
}) {
  const { copy } = useCopyButton();
  const outcome = getRequestOutcome(
    isPartial,
    event._phase === 'final' ? event.status : 0,
    event._phase === 'final' ? event.error_code : undefined,
    event._phase === 'final' ? event.upstream_error_type : undefined,
  );
  const reasoningText = reasoningBadgeText(
    event.reasoning_effort,
    event.thinking_budget_tokens,
    event.thinking_tokens,
  );
  const requestKind = event.request_kind?.trim() || null;

  return (
    <DetailSection title="Identity">
      <div className="space-y-1.5">
        <KvRow
          label="Timestamp"
          value={<AbsoluteTime ts={eventTime(event)} />}
        />
        <KvRow
          label="Principal"
          value={
            <div className="flex min-h-5 items-center gap-2 justify-end flex-wrap min-w-0">
              <span
                className="break-all min-w-0"
                title={event.principal_id ?? ''}
              >
                {principalLabel}
              </span>
              {event.principal_kind ? (
                <Badge className="shrink-0">
                  {principalKindLabel(event.principal_kind)}
                </Badge>
              ) : isDetailPending ? (
                <Skeleton className="h-5 w-14 shrink-0" />
              ) : null}
            </div>
          }
        />
        <KvRow
          label="Key ID"
          value={
            event.key_id ? (
              <span className="flex items-center gap-1 justify-end flex-wrap min-w-0">
                <Hint label={event.key_id}>
                  <span className="font-mono text-data break-all cursor-help min-w-0">
                    {truncateMid(event.key_id, 16)}
                  </span>
                </Hint>
                <button
                  type="button"
                  aria-label="Copy key id"
                  className="shrink-0 -m-1.5 p-1.5 rounded-sm text-text-faint hover:text-text focus-visible:outline-2 focus-visible:outline-accent"
                  onClick={() => copy(event.key_id ?? '', 'Key ID')}
                >
                  <Copy className="w-3 h-3" />
                </button>
              </span>
            ) : isDetailPending ? (
              <Skeleton className="ml-auto h-3 w-24" />
            ) : (
              DASH
            )
          }
        />
        <KvRow
          label="Upstream"
          value={
            <span className="break-all min-w-0">
              {event.upstream_name ?? DASH}
              {event.upstream && event.upstream !== event.upstream_name ? (
                <span className="font-mono text-data text-text-faint ml-2">
                  {event.upstream}
                </span>
              ) : null}
            </span>
          }
        />
        <KvRow
          label="Session"
          value={
            event.thread_id ? (
              <span className="flex items-center gap-1 justify-end flex-wrap min-w-0">
                <SessionChip sessionId={event.thread_id} />
                <button
                  type="button"
                  aria-label="Copy session id"
                  className="shrink-0 -m-1.5 p-1.5 rounded-sm text-text-faint hover:text-text focus-visible:outline-2 focus-visible:outline-accent"
                  onClick={() => copy(event.thread_id ?? '', 'Session ID')}
                >
                  <Copy className="w-3 h-3" />
                </button>
              </span>
            ) : (
              DASH
            )
          }
        />
        <KvRow
          label="Request kind"
          value={requestKind ? <Badge>{requestKind}</Badge> : DASH}
        />
        <KvRow
          label="Observed session"
          value={
            event.observed_session_id ? (
              <span className="flex items-center gap-1 justify-end flex-wrap min-w-0">
                <SessionChip sessionId={event.observed_session_id} />
                <button
                  type="button"
                  aria-label="Copy observed session id"
                  className="shrink-0 -m-1.5 p-1.5 rounded-sm text-text-faint hover:text-text focus-visible:outline-2 focus-visible:outline-accent"
                  onClick={() =>
                    copy(event.observed_session_id ?? '', 'Observed session ID')
                  }
                >
                  <Copy className="w-3 h-3" />
                </button>
              </span>
            ) : (
              DASH
            )
          }
        />
        {event.session_id_source != null && (
          <KvRow
            label="Session source"
            value={
              <span className="font-mono text-data break-all min-w-0">
                {event.session_id_source}
              </span>
            }
          />
        )}
        {event.parent_session_id != null && (
          <KvRow
            label="Parent session"
            value={
              <span className="flex items-center gap-1 justify-end flex-wrap min-w-0">
                <SessionChip sessionId={event.parent_session_id} />
                <button
                  type="button"
                  aria-label="Copy parent session id"
                  className="shrink-0 -m-1.5 p-1.5 rounded-sm text-text-faint hover:text-text focus-visible:outline-2 focus-visible:outline-accent"
                  onClick={() =>
                    copy(event.parent_session_id ?? '', 'Parent session ID')
                  }
                >
                  <Copy className="w-3 h-3" />
                </button>
              </span>
            }
          />
        )}
        {event.claude_agent_id != null && (
          <KvRow
            label="Agent"
            value={
              <span className="flex items-center gap-1 justify-end flex-wrap min-w-0">
                <span className="font-mono text-data break-all min-w-0">
                  {event.claude_agent_id}
                </span>
                <button
                  type="button"
                  aria-label="Copy agent id"
                  className="shrink-0 -m-1.5 p-1.5 rounded-sm text-text-faint hover:text-text focus-visible:outline-2 focus-visible:outline-accent"
                  onClick={() => copy(event.claude_agent_id ?? '', 'Agent ID')}
                >
                  <Copy className="w-3 h-3" />
                </button>
              </span>
            }
          />
        )}
        {event.claude_parent_agent_id != null && (
          <KvRow
            label="Parent agent"
            value={
              <span className="flex items-center gap-1 justify-end flex-wrap min-w-0">
                <span className="font-mono text-data break-all min-w-0">
                  {event.claude_parent_agent_id}
                </span>
                <button
                  type="button"
                  aria-label="Copy parent agent id"
                  className="shrink-0 -m-1.5 p-1.5 rounded-sm text-text-faint hover:text-text focus-visible:outline-2 focus-visible:outline-accent"
                  onClick={() =>
                    copy(event.claude_parent_agent_id ?? '', 'Parent agent ID')
                  }
                >
                  <Copy className="w-3 h-3" />
                </button>
              </span>
            }
          />
        )}
        {event.client_app != null && (
          <KvRow
            label="Client app"
            value={<Badge tone="mono">{event.client_app}</Badge>}
          />
        )}
        <KvRow
          label="Model"
          value={
            <span className="font-mono text-data break-all min-w-0">
              {event.model ?? DASH}
            </span>
          }
        />
        {reasoningText != null && (
          <KvRow
            label="Reasoning"
            value={<span className="break-all min-w-0">{reasoningText}</span>}
          />
        )}
        {event.service_tier != null && event.service_tier !== '' && (
          <KvRow
            label="Service tier"
            value={
              <span className="break-all min-w-0">{event.service_tier}</span>
            }
          />
        )}
        <KvRow
          label="Status"
          value={<RequestOutcomeBadge outcome={outcome} />}
        />
        {outcome.type === 'client_disconnected' ? (
          <KvRow
            label="Status code"
            value={
              <span className="tabular-nums break-all min-w-0">
                {outcome.status}
              </span>
            }
          />
        ) : null}
        {outcome.type === 'semantic_error' ? (
          <KvRow
            label="HTTP status"
            value={
              <span className="tabular-nums break-all min-w-0">
                {outcome.status}
              </span>
            }
          />
        ) : null}
        {event._phase === 'final' && event.error_code ? (
          // Prose reads left-aligned on its own full-width row under the key.
          <div className="flex min-w-0 flex-col gap-1">
            <span className="text-text-muted">Error</span>
            <ErrorExplanation
              code={event.error_code}
              warn={outcome.type === 'client_disconnected'}
              principalId={event.principal_id ?? null}
              upstreamId={event.upstream_id ?? null}
            />
          </div>
        ) : null}
      </div>
    </DetailSection>
  );
}

const TARGET_LABEL: Record<ErrorCodeTarget, string> = {
  'principal-router': 'Open principal router',
  'principal-limits': 'Open principal limits',
  upstream: 'Open upstream',
};

function ErrorExplanation({
  code,
  warn,
  principalId,
  upstreamId,
}: {
  code: string;
  warn: boolean;
  principalId: string | null;
  upstreamId: string | null;
}) {
  const explanation = explainErrorCode(code);
  const target = explanation.target;
  const link =
    target === 'upstream'
      ? upstreamId
        ? { to: '/upstreams' as const, search: { selectedId: upstreamId } }
        : null
      : target && principalId
        ? {
            to: '/principals' as const,
            search: { selectedId: principalId },
            hash:
              target === 'principal-router'
                ? PRINCIPAL_ROUTER_ANCHOR
                : PRINCIPAL_LIMITS_ANCHOR,
          }
        : null;
  return (
    <span className="flex flex-col items-start gap-0.5 min-w-0 text-left">
      <span
        className={cx(
          'break-words min-w-0',
          warn ? 'text-warn-text' : 'text-danger-text',
        )}
      >
        {explanation.summary}
      </span>
      {explanation.nextStep ? (
        <span className="text-text-muted break-words min-w-0">
          {explanation.nextStep}
        </span>
      ) : null}
      {link && target ? (
        <ConfigLink {...link}>{TARGET_LABEL[target]}</ConfigLink>
      ) : null}
      <span
        className="font-mono text-data text-text-faint break-all min-w-0"
        data-testid="request-error-code"
      >
        {code}
      </span>
    </span>
  );
}

/**
 * In-app link that also renders outside a router (component tests mount the
 * drawer bare): falls back to a plain anchor with the same href.
 */
function ConfigLink({
  to,
  search,
  hash,
  children,
}: {
  to: '/upstreams' | '/principals';
  search: { selectedId: string };
  hash?: string;
  children: ReactNode;
}) {
  const router = useRouter({ warn: false });
  const className =
    'text-text underline decoration-border-strong underline-offset-2 hover:decoration-accent focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-2 rounded-sm';
  if (!router) {
    const href = `${to}?selectedId=${encodeURIComponent(search.selectedId)}${hash ? `#${hash}` : ''}`;
    return (
      <a href={href} className={className}>
        {children}
      </a>
    );
  }
  return (
    <Link to={to} search={search} hash={hash} className={className}>
      {children}
    </Link>
  );
}

export function DetailSection({
  title,
  children,
}: {
  title: string;
  children: ReactNode;
}) {
  return (
    <div className="min-w-0">
      <h3 className="text-title-card text-text mb-3 truncate">{title}</h3>
      {children}
    </div>
  );
}

export function KvRow({ label, value }: { label: string; value: ReactNode }) {
  return (
    <div className="flex items-start justify-between gap-3 min-h-5 min-w-0">
      <span className="text-text-muted shrink-0">{label}</span>
      <div className="text-right flex-1 min-w-0 break-words">{value}</div>
    </div>
  );
}

function truncateMid(s: string, max: number): string {
  if (s.length <= max) return s;
  const half = Math.floor((max - 1) / 2);
  return `${s.slice(0, half)}…${s.slice(-half)}`;
}
