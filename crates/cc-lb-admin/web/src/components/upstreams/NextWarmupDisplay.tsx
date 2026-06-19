import { useMemo } from 'react';
import TimeAgo, { type Formatter } from 'react-timeago';
import { formatAbsolute, useLocale, useTimezone } from '../../lib/locale';
import { cx, Hint } from '../ui/primitives';

const OVERDUE_TOLERANCE_MS = 5_000;

function isKoreanLocale(locale: string): boolean {
  return locale.toLowerCase().startsWith('ko');
}

function formatRelativeDelta(diffMs: number, locale: string): string {
  const absMs = Math.abs(diffMs);
  const totalMin = Math.floor(absMs / 60_000);
  const d = Math.floor(totalMin / 1440);
  const h = Math.floor((totalMin % 1440) / 60);
  const m = totalMin % 60;
  if (isKoreanLocale(locale)) {
    if (d > 0) return `${d}일 ${h}시간`;
    if (h > 0) return `${h}시간 ${m}분`;
    return `${Math.max(1, m)}분`;
  }
  if (d > 0) return `${d}d ${h}h`;
  if (h > 0) return `${h}h ${m}m`;
  return `${Math.max(1, m)}m`;
}

function makeWarmupFormatter(date: Date, locale: string): Formatter {
  return (_value, _unit, _suffix, _epochMilliseconds, _nextFormatter, now) => {
    const diffMs = date.getTime() - now();
    const korean = isKoreanLocale(locale);
    if (diffMs < -OVERDUE_TOLERANCE_MS) {
      const delta = formatRelativeDelta(diffMs, locale);
      return (
        <span className="text-amber-300">
          {korean ? `${delta} 지연됨` : `Overdue by ${delta}`}
        </span>
      );
    }
    if (diffMs < OVERDUE_TOLERANCE_MS) {
      return korean ? '곧 실행됨' : 'any moment';
    }
    const delta = formatRelativeDelta(diffMs, locale);
    return korean ? `${delta} 후` : `in ${delta}`;
  };
}

export function NextWarmupDisplay({ value }: { value: string }) {
  const { effective: locale } = useLocale();
  const { effective: timezone } = useTimezone();
  const date = useMemo(() => new Date(value), [value]);
  const formatter = useMemo(
    () => makeWarmupFormatter(date, locale),
    [date, locale],
  );
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
