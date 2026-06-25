import type { WarmupOutcome } from '../../../../lib/queries';
import { Badge } from '../../../ui/primitives';
import { OUTCOME_LABEL, OUTCOME_TONE } from './copy';

export function WarmupOutcomeBadge({
  outcome,
  className,
}: {
  outcome: WarmupOutcome;
  className?: string;
}) {
  return (
    <Badge tone={OUTCOME_TONE[outcome]} className={className}>
      {OUTCOME_LABEL[outcome]}
    </Badge>
  );
}
