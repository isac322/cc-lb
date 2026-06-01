import { useCallback, useEffect, useState } from 'react';

export type Locale = 'auto' | string;
export type Timezone = 'auto' | string;

const STORAGE_KEY = 'cclb.locale';
const TZ_STORAGE_KEY = 'cclb.timezone';

function readStoredLocale(): Locale {
  if (typeof window === 'undefined') return 'auto';
  const stored = window.localStorage.getItem(STORAGE_KEY);
  if (stored) return stored;
  return 'auto';
}

function readStoredTimezone(): Timezone {
  if (typeof window === 'undefined') return 'auto';
  const stored = window.localStorage.getItem(TZ_STORAGE_KEY);
  if (stored) return stored;
  return 'auto';
}

function resolveEffectiveLocale(locale: Locale): string {
  if (locale === 'auto') {
    if (typeof window === 'undefined') return 'en-US';
    return window.navigator.language || 'en-US';
  }
  return locale;
}

function resolveEffectiveTimezone(tz: Timezone): string {
  if (tz === 'auto') {
    if (typeof window === 'undefined') return 'UTC';
    return Intl.DateTimeFormat().resolvedOptions().timeZone;
  }
  return tz;
}

export function initLocale() {
  readStoredLocale();
}

export function initTimezone() {
  readStoredTimezone();
}

export function useLocale() {
  const [locale, setLocaleState] = useState<Locale>(() => readStoredLocale());
  const [effective, setEffective] = useState<string>(() =>
    resolveEffectiveLocale(readStoredLocale()),
  );

  const setLocale = useCallback((next: Locale) => {
    setLocaleState(next);
    window.localStorage.setItem(STORAGE_KEY, next);
    setEffective(resolveEffectiveLocale(next));
  }, []);

  useEffect(() => {
    setEffective(resolveEffectiveLocale(locale));
  }, [locale]);

  return { locale, effective, setLocale };
}

export function useTimezone() {
  const [timezone, setTimezoneState] = useState<Timezone>(() =>
    readStoredTimezone(),
  );
  const [effective, setEffective] = useState<string>(() =>
    resolveEffectiveTimezone(readStoredTimezone()),
  );

  const setTimezone = useCallback((next: Timezone) => {
    setTimezoneState(next);
    window.localStorage.setItem(TZ_STORAGE_KEY, next);
    setEffective(resolveEffectiveTimezone(next));
  }, []);

  useEffect(() => {
    setEffective(resolveEffectiveTimezone(timezone));
  }, [timezone]);

  return { timezone, effective, setTimezone };
}

export function formatAbsolute(
  date: Date,
  locale: string,
  timezone?: string,
): string {
  return date.toLocaleString(locale, {
    dateStyle: 'medium',
    timeStyle: 'medium',
    timeZone: timezone,
  });
}
