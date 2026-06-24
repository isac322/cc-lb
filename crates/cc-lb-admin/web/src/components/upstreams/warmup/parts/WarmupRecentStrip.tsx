import type { WarmupAttempt, WarmupOutcome } from '../../../../lib/queries';
import { cx, Hint } from '../../../ui/primitives';
import { OUTCOME_LABEL, REASON_LABEL } from './copy';

interface Props {
  attempts: WarmupAttempt[] | undefined;
  /** Number of dots to render. Older attempts are truncated; missing slots render dimly. */
  count?: number;
  onClick?: (attempt: WarmupAttempt) => void;
}

const OUTCOME_BG: Record<WarmupOutcome, string> = {
  success_fresh: 'bg-[color:var(--color-ok)]',
  success_redundant: 'bg-[color:var(--color-warn)]',
  transient_failure:
    'bg-[color:color-mix(in_oklab,var(--color-warn)_55%,var(--color-danger))]',
  permanent_failure: 'bg-[color:var(--color-danger)]',
  skipped: 'bg-[color:var(--color-neutral)]',
};

function formatHover(attempt: WarmupAttempt): string {
  const stamp = new Date(
    attempt.attempted_at_unix_secs * 1000,
  ).toLocaleString();
  const reason = attempt.reason ? ` · ${REASON_LABEL[attempt.reason]}` : '';
  return `${OUTCOME_LABEL[attempt.outcome]}${reason} · ${stamp}`;
}

export function WarmupRecentStrip({ attempts, count = 10, onClick }: Props) {
  const slots = Array.from({ length: count });
  const newestFirst = attempts ?? [];

  return (
    <div className="flex flex-col gap-1.5">
      <div className="flex items-center justify-between">
        <span className="text-[11px] uppercase tracking-wider text-text-faint">
          Last {count} attempts
        </span>
        <span className="text-[10px] text-text-faint">Newest →</span>
      </div>
      <div className="flex items-center gap-1.5">
        {slots.map((_, idx) => {
          const attempt = newestFirst[count - 1 - idx];
          if (!attempt) {
            return (
              <span
                key={idx}
                className="w-3 h-3 rounded-full bg-overlay-3 border border-subtle"
                aria-hidden
              />
            );
          }
          const Tag = onClick ? 'button' : 'span';
          return (
            <Hint key={attempt.id} label={formatHover(attempt)}>
              <Tag
                type={onClick ? 'button' : undefined}
                onClick={onClick ? () => onClick(attempt) : undefined}
                className={cx(
                  'w-3 h-3 rounded-full ring-1 ring-black/30 cursor-help',
                  OUTCOME_BG[attempt.outcome],
                )}
                aria-label={formatHover(attempt)}
              />
            </Hint>
          );
        })}
      </div>
    </div>
  );
}
