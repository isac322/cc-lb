import { useCallback, useSyncExternalStore } from 'react';

export type Theme = 'dark' | 'light' | 'system';

type ThemeSnapshot = {
  theme: Theme;
  effective: 'dark' | 'light';
};

const STORAGE_KEY = 'cclb.theme';
const SYSTEM_THEME_QUERY = '(prefers-color-scheme: light)';
const SERVER_SNAPSHOT: ThemeSnapshot = {
  theme: 'dark',
  effective: 'dark',
};

let snapshot: ThemeSnapshot | undefined;
let systemThemeQuery: MediaQueryList | undefined;
const listeners = new Set<() => void>();

function readStoredTheme(): Theme {
  if (typeof window === 'undefined') return 'dark';
  const stored = window.localStorage.getItem(STORAGE_KEY);
  if (stored === 'dark' || stored === 'light' || stored === 'system')
    return stored;
  return 'system';
}

function applyTheme(theme: Theme) {
  if (typeof document === 'undefined') return;
  document.documentElement.setAttribute('data-theme', theme);
}

function resolveEffectiveTheme(theme: Theme): 'dark' | 'light' {
  if (theme === 'system') {
    if (typeof window === 'undefined') return 'dark';
    const prefersLight =
      systemThemeQuery?.matches ??
      window.matchMedia(SYSTEM_THEME_QUERY).matches;
    return prefersLight ? 'light' : 'dark';
  }
  return theme;
}

function readSnapshot(): ThemeSnapshot {
  const theme = readStoredTheme();
  const effective = resolveEffectiveTheme(theme);
  if (snapshot?.theme === theme && snapshot.effective === effective) {
    return snapshot;
  }
  snapshot = { theme, effective };
  return snapshot;
}

function notify() {
  for (const listener of listeners) listener();
}

function stopSystemThemeListener() {
  if (!systemThemeQuery) return;
  systemThemeQuery.removeEventListener('change', onSystemThemeChange);
  systemThemeQuery = undefined;
}

function syncSystemThemeListener(theme: Theme) {
  if (
    listeners.size === 0 ||
    theme !== 'system' ||
    typeof window === 'undefined'
  ) {
    stopSystemThemeListener();
    return;
  }
  if (systemThemeQuery) return;
  systemThemeQuery = window.matchMedia(SYSTEM_THEME_QUERY);
  systemThemeQuery.addEventListener('change', onSystemThemeChange);
}

function updateTheme(theme: Theme) {
  syncSystemThemeListener(theme);
  const previous = snapshot;
  const effective = resolveEffectiveTheme(theme);
  if (snapshot?.theme !== theme || snapshot.effective !== effective) {
    snapshot = { theme, effective };
  }
  applyTheme(theme);
  if (snapshot !== previous) notify();
}

function onSystemThemeChange(event: MediaQueryListEvent) {
  const theme = snapshot?.theme ?? readStoredTheme();
  if (theme !== 'system') return;
  const previous = snapshot;
  const effective = event.matches ? 'light' : 'dark';
  if (snapshot?.theme !== theme || snapshot.effective !== effective) {
    snapshot = { theme, effective };
  }
  if (snapshot !== previous) notify();
}

function onStorage(event: StorageEvent) {
  if (event.key === STORAGE_KEY || event.key === null) {
    updateTheme(readStoredTheme());
  }
}

function subscribe(listener: () => void) {
  const isFirstListener = listeners.size === 0;
  listeners.add(listener);
  if (isFirstListener && typeof window !== 'undefined') {
    window.addEventListener('storage', onStorage);
    updateTheme(readStoredTheme());
  }
  return () => {
    listeners.delete(listener);
    if (listeners.size === 0) {
      if (typeof window !== 'undefined') {
        window.removeEventListener('storage', onStorage);
      }
      stopSystemThemeListener();
      snapshot = undefined;
    }
  };
}

function getSnapshot(): ThemeSnapshot {
  return typeof window === 'undefined'
    ? SERVER_SNAPSHOT
    : (snapshot ?? readSnapshot());
}

function getServerSnapshot(): ThemeSnapshot {
  return SERVER_SNAPSHOT;
}

export function initTheme() {
  updateTheme(readStoredTheme());
}

export function useTheme() {
  const current = useSyncExternalStore(
    subscribe,
    getSnapshot,
    getServerSnapshot,
  );
  const setTheme = useCallback((next: Theme) => {
    window.localStorage.setItem(STORAGE_KEY, next);
    updateTheme(next);
  }, []);

  return {
    theme: current.theme,
    effective: current.effective,
    setTheme,
  };
}
