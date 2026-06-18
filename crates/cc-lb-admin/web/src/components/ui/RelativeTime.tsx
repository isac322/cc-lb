import { useMemo } from 'react';
import TimeAgo, { type Formatter } from 'react-timeago';
import { makeIntlFormatter } from 'react-timeago/defaultFormatter';
import { formatAbsolute, useLocale, useTimezone } from '../../lib/locale';
import { cx, Hint } from './primitives';

function formatCompact(value: number, unit: string): string {
  if (unit === 'year') return `${value}y`;
  if (unit === 'month') return `${value}mo`;
  if (unit === 'week') return `${value}w`;
  if (unit === 'day') return `${value}d`;
  if (unit === 'hour') return `${value}h`;
  if (unit === 'minute') return `${value}m`;
  return `${value}s`;
}

function makeResetFormatter(futureVerb: string, pastVerb: string): Formatter {
  return (value, unit, suffix) => {
    const duration = formatCompact(value, unit);
    if (suffix === 'from now') return `${futureVerb} in ${duration}`;
    if (unit === 'day' && value > 1) return `${pastVerb} >1d ago (stale)`;
    return `${pastVerb} ${duration} ago`;
  };
}

export function RelativeTime({
  ts,
  className,
}: {
  ts: Date | number | null | undefined;
  className?: string;
}) {
  const { effective: locale } = useLocale();
  const { effective: timezone } = useTimezone();
  const date = useMemo(() => {
    if (ts == null) return null;
    if (typeof ts === 'number') return new Date(ts);
    return ts;
  }, [ts]);
  const formatter = useMemo(
    () => makeIntlFormatter({ locale, numeric: 'auto', style: 'long' }),
    [locale],
  );
  if (!date) return <span className={cx('text-text-faint', className)}>—</span>;
  const abs = formatAbsolute(date, locale, timezone);
  return (
    <Hint label={abs}>
      <span className={cx('cursor-help', className)}>
        <TimeAgo
          component="span"
          date={date}
          formatter={formatter}
          title={abs}
        />
      </span>
    </Hint>
  );
}

export function ResetCountdown({
  ts,
  className,
  futureVerb = 'Resets',
  pastVerb = 'Reset',
}: {
  ts: Date | number | null | undefined;
  className?: string;
  futureVerb?: string;
  pastVerb?: string;
}) {
  const { effective: locale } = useLocale();
  const { effective: timezone } = useTimezone();
  const date = useMemo(() => {
    if (ts == null) return null;
    if (typeof ts === 'number') return new Date(ts);
    return ts;
  }, [ts]);
  const formatter = useMemo(
    () => makeResetFormatter(futureVerb, pastVerb),
    [futureVerb, pastVerb],
  );

  if (!date) return <span className={cx('text-text-faint', className)}>—</span>;

  const abs = formatAbsolute(date, locale, timezone);

  return (
    <Hint label={abs} side="top">
      <span className={cx('cursor-help', className)}>
        <TimeAgo
          component="span"
          date={date}
          formatter={formatter}
          title={abs}
        />
      </span>
    </Hint>
  );
}
