import { useCallback, useSyncExternalStore } from 'react';

export type Locale = 'auto' | string;
export type Timezone = 'auto' | string;

const STORAGE_KEY = 'cclb.locale';
const TZ_STORAGE_KEY = 'cclb.timezone';

type PreferenceSnapshot<T extends string> = {
  value: T;
  effective: string;
};

type StoredPreferenceStore<T extends string> = {
  getSnapshot: () => PreferenceSnapshot<T>;
  getServerSnapshot: () => PreferenceSnapshot<T>;
  initialize: () => void;
  set: (next: T) => void;
  subscribe: (listener: () => void) => () => void;
};

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

function createStoredPreferenceStore<T extends string>({
  key,
  read,
  resolve,
  serverSnapshot,
}: {
  key: string;
  read: () => T;
  resolve: (value: T) => string;
  serverSnapshot: PreferenceSnapshot<T>;
}): StoredPreferenceStore<T> {
  let snapshot: PreferenceSnapshot<T> | undefined;
  const listeners = new Set<() => void>();

  const readSnapshot = (): PreferenceSnapshot<T> => {
    const value = read();
    const effective = resolve(value);
    if (snapshot?.value === value && snapshot.effective === effective) {
      return snapshot;
    }
    snapshot = { value, effective };
    return snapshot;
  };

  const notify = () => {
    for (const listener of listeners) listener();
  };

  const refresh = () => {
    const previous = snapshot;
    readSnapshot();
    if (snapshot !== previous) notify();
  };

  const onStorage = (event: StorageEvent) => {
    if (event.key === key || event.key === null) refresh();
  };

  return {
    getSnapshot: () =>
      typeof window === 'undefined'
        ? serverSnapshot
        : (snapshot ?? readSnapshot()),
    getServerSnapshot: () => serverSnapshot,
    initialize: refresh,
    set: (next) => {
      window.localStorage.setItem(key, next);
      const previous = snapshot;
      const effective = resolve(next);
      if (snapshot?.value !== next || snapshot.effective !== effective) {
        snapshot = { value: next, effective };
      }
      if (snapshot !== previous) notify();
    },
    subscribe: (listener) => {
      listeners.add(listener);
      if (listeners.size === 1 && typeof window !== 'undefined') {
        window.addEventListener('storage', onStorage);
        refresh();
      }
      return () => {
        listeners.delete(listener);
        if (listeners.size === 0) {
          if (typeof window !== 'undefined') {
            window.removeEventListener('storage', onStorage);
          }
          snapshot = undefined;
        }
      };
    },
  };
}

const localeStore = createStoredPreferenceStore<Locale>({
  key: STORAGE_KEY,
  read: readStoredLocale,
  resolve: resolveEffectiveLocale,
  serverSnapshot: { value: 'auto', effective: 'en-US' },
});

const timezoneStore = createStoredPreferenceStore<Timezone>({
  key: TZ_STORAGE_KEY,
  read: readStoredTimezone,
  resolve: resolveEffectiveTimezone,
  serverSnapshot: { value: 'auto', effective: 'UTC' },
});

export function initLocale() {
  localeStore.initialize();
}

export function initTimezone() {
  timezoneStore.initialize();
}

export function useLocale() {
  const snapshot = useSyncExternalStore(
    localeStore.subscribe,
    localeStore.getSnapshot,
    localeStore.getServerSnapshot,
  );
  const setLocale = useCallback((next: Locale) => localeStore.set(next), []);

  return {
    locale: snapshot.value,
    effective: snapshot.effective,
    setLocale,
  };
}

export function useTimezone() {
  const snapshot = useSyncExternalStore(
    timezoneStore.subscribe,
    timezoneStore.getSnapshot,
    timezoneStore.getServerSnapshot,
  );
  const setTimezone = useCallback(
    (next: Timezone) => timezoneStore.set(next),
    [],
  );

  return {
    timezone: snapshot.value,
    effective: snapshot.effective,
    setTimezone,
  };
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
