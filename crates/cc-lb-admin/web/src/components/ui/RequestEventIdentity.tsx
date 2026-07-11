import { Copy } from 'lucide-react';
import type { ReactNode } from 'react';
import { eventTime } from '../../lib/api';
import {
  getRequestOutcome,
  type RequestOutcome,
  statusTone,
} from '../../lib/format';
import type { RequestEventWithPhase } from '../../lib/RequestEventTypes';
import { useCopyButton } from '../../lib/useCopyButton';
import { Badge, cx, Hint } from './primitives';
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
  return (
    <Badge
      tone={statusTone(outcome.status)}
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
  return <>{outcome.status}</>;
}

export function RequestEventIdentity({
  event,
  principalLabel,
  isPartial,
}: {
  event: RequestEventWithPhase;
  principalLabel: string;
  isPartial: boolean;
}) {
  const { copy } = useCopyButton();
  const outcome = getRequestOutcome(
    isPartial,
    event._phase === 'final' ? event.status : 0,
    event._phase === 'final' ? event.error_code : undefined,
  );

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
            <span className="flex items-center gap-2 justify-end flex-wrap min-w-0">
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
              ) : null}
            </span>
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
            event.thread_id ? (
              <span className="flex items-center gap-1 justify-end flex-wrap min-w-0">
                <SessionChip sessionId={event.thread_id} />
                <button
                  type="button"
                  aria-label="Copy session id"
                  className="text-text-faint hover:text-text shrink-0"
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
          label="Model"
          value={
            <span className="font-mono break-all min-w-0">
              {event.model ?? DASH}
            </span>
          }
        />
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
      <span className="text-right flex-1 min-w-0 break-words">{value}</span>
    </div>
  );
}

function truncateMid(s: string, max: number): string {
  if (s.length <= max) return s;
  const half = Math.floor((max - 1) / 2);
  return `${s.slice(0, half)}…${s.slice(-half)}`;
}
