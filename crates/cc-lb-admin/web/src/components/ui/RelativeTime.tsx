import { useEffect, useMemo, useReducer } from 'react';
import { formatAbsolute, useLocale, useTimezone } from '../../lib/locale';
import { cx, Hint } from './primitives';

const UNITS: { unit: Intl.RelativeTimeFormatUnit; ms: number }[] = [
  { unit: 'year', ms: 365 * 24 * 60 * 60 * 1000 },
  { unit: 'month', ms: 30 * 24 * 60 * 60 * 1000 },
  { unit: 'day', ms: 24 * 60 * 60 * 1000 },
  { unit: 'hour', ms: 60 * 60 * 1000 },
  { unit: 'minute', ms: 60 * 1000 },
  { unit: 'second', ms: 1000 },
];

export function formatRelative(
  d: Date,
  locale: string,
  now: Date = new Date(),
): string {
  const diffMs = d.getTime() - now.getTime();
  const absMs = Math.abs(diffMs);
  const rtf = new Intl.RelativeTimeFormat(locale, { numeric: 'auto' });

  // <5s → locale-natural "now"
  if (absMs < 5_000) {
    return rtf.format(0, 'second');
  }

  for (const { unit, ms } of UNITS) {
    if (absMs >= ms) {
      const value = Math.round(diffMs / ms);
      return rtf.format(value, unit);
    }
  }
  return rtf.format(0, 'second');
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
  const [, force] = useReducer((x) => x + 1, 0);
  useEffect(() => {
    const id = setInterval(force, 30_000);
    return () => clearInterval(id);
  }, []);
  if (!date) return <span className={cx('text-text-faint', className)}>—</span>;
  const rel = formatRelative(date, locale);
  const abs = formatAbsolute(date, locale, timezone);
  return (
    <Hint label={abs}>
      <span className={cx('cursor-help', className)}>{rel}</span>
    </Hint>
  );
}
