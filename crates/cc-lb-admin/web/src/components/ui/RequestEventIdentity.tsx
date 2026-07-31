import { Copy } from 'lucide-react';
import type { ReactNode } from 'react';
import { eventTime } from '../../lib/api';
import {
  getRequestOutcome,
  type RequestOutcome,
  requestOutcomeTone,
} from '../../lib/format';
import type { RequestEventWithPhase } from '../../lib/RequestEventTypes';
import { reasoningBadgeText } from '../../lib/reasoningTier';
import { requestKindBadgeText } from '../../lib/requestKind';
import { useCopyButton } from '../../lib/useCopyButton';
import { Badge, cx, Hint, Skeleton } from './primitives';
import { RelativeTime } from './RelativeTime';
import { SessionChip } from './SessionChip';

const DASH = '—';

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
  const requestKindBadge = requestKindBadgeText(event.request_kind);

  return (
    <DetailSection title="Identity">
      <div className="space-y-1.5">
        <KvRow
          label="Timestamp"
          value={<RelativeTime ts={eventTime(event)} />}
        />
        <KvRow
          label="Principal"
          value={
            <div className="flex min-h-5 items-center gap-2 justify-end flex-wrap min-w-0">
              <span
                className="font-mono break-all min-w-0"
                title={event.principal_id ?? ''}
              >
                {principalLabel}
              </span>
              {event.principal_kind ? (
                <Badge tone="mono" className="shrink-0">
                  {event.principal_kind}
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
                  <span className="font-mono break-all cursor-help min-w-0">
                    {truncateMid(event.key_id, 16)}
                  </span>
                </Hint>
                <button
                  type="button"
                  aria-label="Copy key id"
                  className="text-text-faint hover:text-text shrink-0"
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
            <span className="font-mono break-all min-w-0">
              {event.upstream_name ?? DASH}
              {event.upstream && event.upstream !== event.upstream_name ? (
                <span className="text-text-faint ml-2">({event.upstream})</span>
              ) : null}
            </span>
          }
        />
        <KvRow
          label="Session"
          value={
            event.thread_id || requestKindBadge ? (
              <span className="flex items-center gap-1 justify-end flex-wrap min-w-0">
                <SessionChip sessionId={event.thread_id ?? null} />
                {requestKindBadge && (
                  <Badge tone="mono" className="font-mono">
                    {requestKindBadge}
                  </Badge>
                )}
                {event.thread_id && (
                  <button
                    type="button"
                    aria-label="Copy session id"
                    className="text-text-faint hover:text-text shrink-0"
                    onClick={() => copy(event.thread_id ?? '', 'Session ID')}
                  >
                    <Copy className="w-3 h-3" />
                  </button>
                )}
              </span>
            ) : (
              DASH
            )
          }
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
                  className="text-text-faint hover:text-text shrink-0"
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
              <span className="font-mono break-all min-w-0">
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
                  className="text-text-faint hover:text-text shrink-0"
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
                <span className="font-mono break-all min-w-0">
                  {event.claude_agent_id}
                </span>
                <button
                  type="button"
                  aria-label="Copy agent id"
                  className="text-text-faint hover:text-text shrink-0"
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
                <span className="font-mono break-all min-w-0">
                  {event.claude_parent_agent_id}
                </span>
                <button
                  type="button"
                  aria-label="Copy parent agent id"
                  className="text-text-faint hover:text-text shrink-0"
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
            value={
              <Badge tone="mono" className="font-mono">
                {event.client_app}
              </Badge>
            }
          />
        )}
        <KvRow
          label="Model"
          value={
            <span className="font-mono break-all min-w-0">
              {event.model ?? DASH}
            </span>
          }
        />
        {reasoningText != null && (
          <KvRow
            label="Reasoning"
            value={
              <span className="font-mono break-all min-w-0">
                {reasoningText}
              </span>
            }
          />
        )}
        {event.service_tier != null && event.service_tier !== '' && (
          <KvRow
            label="Service tier"
            value={
              <span className="font-mono break-all min-w-0">
                {event.service_tier}
              </span>
            }
          />
        )}
        <KvRow
          label="Status"
          value={<RequestOutcomeBadge outcome={outcome} />}
        />
        {outcome.type === 'client_disconnected' ? (
          <KvRow
            label="Status Code"
            value={
              <span className="font-mono break-all min-w-0">
                {outcome.status}
              </span>
            }
          />
        ) : null}
        {outcome.type === 'semantic_error' ? (
          <KvRow
            label="HTTP Status"
            value={
              <span className="font-mono break-all min-w-0">
                {outcome.status}
              </span>
            }
          />
        ) : null}
        {event._phase === 'final' && event.error_code ? (
          <KvRow
            label="Error"
            value={
              <span
                className={cx(
                  'font-mono break-all min-w-0',
                  outcome.type === 'client_disconnected'
                    ? 'text-[color:var(--color-warn)]'
                    : 'text-danger',
                )}
              >
                {event.error_code}
              </span>
            }
          />
        ) : null}
      </div>
    </DetailSection>
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
      <h3 className="text-[10px] uppercase tracking-wider text-text-faint mb-2 border-b border-subtle pb-1 truncate">
        {title}
      </h3>
      {children}
    </div>
  );
}

export function KvRow({ label, value }: { label: string; value: ReactNode }) {
  return (
    <div className="flex items-start justify-between gap-3 min-h-[18px] min-w-0">
      <span className="text-text-faint shrink-0 mt-[1px]">{label}</span>
      <div className="text-right flex-1 min-w-0 break-words">{value}</div>
    </div>
  );
}

function truncateMid(s: string, max: number): string {
  if (s.length <= max) return s;
  const half = Math.floor((max - 1) / 2);
  return `${s.slice(0, half)}…${s.slice(-half)}`;
}
