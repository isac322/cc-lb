import type { QuotaSnapshot } from '../../lib/api';
import { cx } from '../ui/primitives';
import { RelativeTime } from '../ui/RelativeTime';

type QuotaObservation = Pick<QuotaSnapshot, 'observed_at_unix_millis'>;

export function QuotaObservedAt({
  snapshot,
  className,
}: {
  readonly snapshot: QuotaObservation;
  readonly className?: string;
}) {
  if (snapshot.observed_at_unix_millis == null) {
    return <span className={className}>—</span>;
  }

  return (
    <RelativeTime
      className={cx('normal-case', className)}
      ts={snapshot.observed_at_unix_millis}
    />
  );
}
