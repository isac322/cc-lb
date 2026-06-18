import { useMemo } from 'react';
import TimeAgo, { type Formatter } from 'react-timeago';
import { formatAbsolute, useLocale, useTimezone } from '../../lib/locale';
import { cx, Hint } from '../ui/primitives';

const OVERDUE_TOLERANCE_MS = 5_000;

function formatRelativeDelta(diffMs: number): string {
  const absMs = Math.abs(diffMs);
  const totalMin = Math.floor(absMs / 60_000);
  const d = Math.floor(totalMin / 1440);
  const h = Math.floor((totalMin % 1440) / 60);
  const m = totalMin % 60;
  if (d > 0) return `${d}d ${h}h`;
  if (h > 0) return `${h}h ${m}m`;
  return `${Math.max(1, m)}m`;
}

function makeWarmupFormatter(date: Date): Formatter {
  return (_value, _unit, _suffix, _epochMilliseconds, _nextFormatter, now) => {
    const diffMs = date.getTime() - now();
    if (diffMs < -OVERDUE_TOLERANCE_MS) {
      return (
        <span className="text-amber-300">
          Overdue by {formatRelativeDelta(diffMs)}
        </span>
      );
    }
    if (diffMs < OVERDUE_TOLERANCE_MS) return 'any moment';
    return `in ${formatRelativeDelta(diffMs)}`;
  };
}

export function NextWarmupDisplay({ value }: { value: string }) {
  const { effective: locale } = useLocale();
  const { effective: timezone } = useTimezone();
  const date = useMemo(() => new Date(value), [value]);
  const formatter = useMemo(() => makeWarmupFormatter(date), [date]);
  const abs = formatAbsolute(date, locale, timezone);
  return (
    <Hint label={abs} side="top">
      <span className={cx('cursor-help')}>
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
