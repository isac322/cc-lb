import { type HTMLAttributes, type ReactNode, useMemo } from 'react';
import TimeAgo, { type Formatter } from 'react-timeago';
import { makeIntlFormatter } from 'react-timeago/defaultFormatter';
import { formatAbsolute, useLocale, useTimezone } from '../../lib/locale';
import { cx, Hint } from './primitives';

function isKoreanLocale(locale: string): boolean {
  return locale.toLowerCase().startsWith('ko');
}

const KO_UNIT: Record<string, string> = {
  year: '년',
  month: '개월',
  week: '주',
  day: '일',
  hour: '시간',
  minute: '분',
  second: '초',
};

const EN_COMPACT_UNIT: Record<string, string> = {
  year: 'y',
  month: 'mo',
  week: 'w',
  day: 'd',
  hour: 'h',
  minute: 'm',
  second: 's',
};

function compactDuration(value: number, unit: string, locale: string): string {
  if (isKoreanLocale(locale)) {
    return `${value}${KO_UNIT[unit] ?? unit}`;
  }
  return `${value}${EN_COMPACT_UNIT[unit] ?? unit}`;
}

function makeCompactRelativeFormatter(locale: string): Formatter {
  return (value, unit, suffix) => {
    const duration = compactDuration(value, unit, locale);
    if (isKoreanLocale(locale)) {
      return suffix === 'from now' ? `${duration} 후` : `${duration} 전`;
    }
    return suffix === 'from now' ? `in ${duration}` : `${duration} ago`;
  };
}

export type ResetTimeParts = {
  prefix: string;
  value: string;
  suffix: string;
};

/**
 * Formats the reset countdown into alignment-friendly parts without changing
 * the locale-specific wording used by the whole-string formatter.
 */
export function formatResetParts(
  value: number,
  unit: string,
  suffix: string,
  locale: string,
  compact = false,
): ResetTimeParts {
  const korean = isKoreanLocale(locale);
  const duration =
    compact || korean
      ? compactDuration(value, unit, locale)
      : `${value} ${unit}${value === 1 ? '' : 's'}`;

  if (korean) {
    return suffix === 'from now'
      ? { prefix: '', value: duration, suffix: '후 초기화' }
      : { prefix: '', value: duration, suffix: '전 초기화됨' };
  }

  if (suffix === 'from now') {
    return { prefix: 'Resets in', value: duration, suffix: '' };
  }
  if (compact && unit === 'day' && value > 1) {
    return { prefix: 'Reset', value: '>1d', suffix: 'ago (stale)' };
  }
  return { prefix: 'Reset', value: duration, suffix: 'ago' };
}

function resetPartsText(parts: ResetTimeParts): string {
  return [parts.prefix, parts.value, parts.suffix]
    .filter((part) => part.length > 0)
    .join(' ');
}

function makeResetFormatter(locale: string, compact: boolean): Formatter {
  return (value, unit, suffix) =>
    resetPartsText(formatResetParts(value, unit, suffix, locale, compact));
}

function validDate(ts: Date | number | null | undefined): Date | null {
  if (ts == null) return null;
  const date = typeof ts === 'number' ? new Date(ts) : ts;
  return Number.isFinite(date.getTime()) ? date : null;
}

function ResetPartsContainer({
  children,
  ...props
}: HTMLAttributes<HTMLSpanElement>) {
  return (
    <span {...props} className={cx('contents cursor-help', props.className)}>
      {children}
    </span>
  );
}

/** Live reset parts for layouts that share their columns across several rows. */
export function ResetCountdownParts({
  ts,
  className,
  compact = false,
  renderParts,
}: {
  ts: Date | number | null | undefined;
  className?: string;
  compact?: boolean;
  renderParts: (parts: ResetTimeParts) => ReactNode;
}) {
  const { effective: locale } = useLocale();
  const { effective: timezone } = useTimezone();
  const date = useMemo(() => validDate(ts), [ts]);
  const formatter = useMemo<Formatter>(
    () => (value, unit, suffix) =>
      renderParts(formatResetParts(value, unit, suffix, locale, compact)),
    [compact, locale, renderParts],
  );
  const abs = useMemo(
    () => (date ? formatAbsolute(date, locale, timezone) : ''),
    [date, locale, timezone],
  );

  if (!date) {
    return <span className={cx('text-text-faint', className)}>—</span>;
  }

  return (
    <Hint label={abs} side="top">
      <TimeAgo
        component={ResetPartsContainer}
        date={date}
        formatter={formatter}
        title={abs}
      />
    </Hint>
  );
}

export function RelativeTime({
  ts,
  className,
  compact = false,
}: {
  ts: Date | number | null | undefined;
  className?: string;
  compact?: boolean;
}) {
  const { effective: locale } = useLocale();
  const { effective: timezone } = useTimezone();
  const date = useMemo(() => validDate(ts), [ts]);
  const formatter = useMemo(
    () =>
      compact
        ? makeCompactRelativeFormatter(locale)
        : makeIntlFormatter({ locale, numeric: 'auto', style: 'long' }),
    [locale, compact],
  );
  const abs = useMemo(
    () => (date ? formatAbsolute(date, locale, timezone) : ''),
    [date, locale, timezone],
  );
  if (!date) return <span className={cx('text-text-faint', className)}>—</span>;
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
  compact = false,
}: {
  ts: Date | number | null | undefined;
  className?: string;
  compact?: boolean;
}) {
  const { effective: locale } = useLocale();
  const { effective: timezone } = useTimezone();
  const date = useMemo(() => validDate(ts), [ts]);
  const formatter = useMemo(
    () => makeResetFormatter(locale, compact),
    [locale, compact],
  );
  const abs = useMemo(
    () => (date ? formatAbsolute(date, locale, timezone) : ''),
    [date, locale, timezone],
  );

  if (!date) return <span className={cx('text-text-faint', className)}>—</span>;

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
