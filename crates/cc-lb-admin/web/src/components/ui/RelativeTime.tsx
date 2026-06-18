import { useMemo, useSyncExternalStore } from 'react';
import { formatAbsolute, useLocale, useTimezone } from '../../lib/locale';
import { cx, Hint } from './primitives';

const RELATIVE_TIME_TICK_MS = 1_000;

let currentNowMs = Date.now();
let clockIntervalId: number | undefined;
const clockSubscribers = new Set<() => void>();

function emitClockTick() {
  currentNowMs = Date.now();
  for (const subscriber of clockSubscribers) {
    subscriber();
  }
}

function subscribeToClock(subscriber: () => void): () => void {
  clockSubscribers.add(subscriber);
  if (clockSubscribers.size === 1 && typeof window !== 'undefined') {
    currentNowMs = Date.now();
    clockIntervalId = window.setInterval(emitClockTick, RELATIVE_TIME_TICK_MS);
  }

  return () => {
    clockSubscribers.delete(subscriber);
    if (clockSubscribers.size === 0 && clockIntervalId !== undefined) {
      window.clearInterval(clockIntervalId);
      clockIntervalId = undefined;
    }
  };
}

function getClockSnapshot(): number {
  return currentNowMs;
}

function useClockNow(): Date {
  const nowMs = useSyncExternalStore(
    subscribeToClock,
    getClockSnapshot,
    getClockSnapshot,
  );
  return useMemo(() => new Date(nowMs), [nowMs]);
}

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
  const now = useClockNow();
  if (!date) return <span className={cx('text-text-faint', className)}>—</span>;
  const rel = formatRelative(date, locale, now);
  const abs = formatAbsolute(date, locale, timezone);
  return (
    <Hint label={abs}>
      <span className={cx('cursor-help', className)}>{rel}</span>
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
  const now = useClockNow();

  if (!date) return <span className={cx('text-text-faint', className)}>—</span>;

  const diffMs = date.getTime() - now.getTime();
  const absMs = Math.abs(diffMs);
  const abs = formatAbsolute(date, locale, timezone);

  let text = '';
  if (diffMs > 0) {
    const totalMin = Math.floor(absMs / 60000);
    const d = Math.floor(totalMin / 1440);
    const h = Math.floor((totalMin % 1440) / 60);
    const m = totalMin % 60;
    if (d > 0) text = `${futureVerb} in ${d}d ${h}h`;
    else if (h > 0) text = `${futureVerb} in ${h}h ${m}m`;
    else text = `${futureVerb} in ${m}m`;
  } else {
    if (absMs > 24 * 3600000) {
      text = `${pastVerb} >1d ago (stale)`;
    } else {
      const h = Math.floor(absMs / 3600000);
      const m = Math.floor((absMs % 3600000) / 60000);
      if (h > 0) {
        text = `${pastVerb} ${h}h ${m}m ago`;
      } else {
        text = `${pastVerb} ${m}m ago`;
      }
    }
  }

  return (
    <Hint label={abs} side="top">
      <span className={cx('cursor-help', className)}>{text}</span>
    </Hint>
  );
}
