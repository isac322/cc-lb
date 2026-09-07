// @vitest-environment jsdom

import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
} from '@testing-library/react';
import { renderToString } from 'react-dom/server';
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest';
import {
  RelativeTime,
  ResetCountdown,
} from '../../../components/ui/RelativeTime';
import { useLocale, useTimezone } from '../../locale';
import { initTheme, useTheme } from '../../theme';

const SYSTEM_THEME_QUERY = '(prefers-color-scheme: light)';

let prefersLight = false;
let mediaListeners: Set<(event: MediaQueryListEvent) => void>;
let addMediaListener: (
  type: string,
  listener: EventListenerOrEventListenerObject | null,
) => void;
let removeMediaListener: (
  type: string,
  listener: EventListenerOrEventListenerObject | null,
) => void;

function LocaleProbe({ id }: { id: string }) {
  const { locale, effective: effectiveLocale, setLocale } = useLocale();
  const { timezone, effective: effectiveTimezone, setTimezone } = useTimezone();

  return (
    <div>
      <span data-testid={`${id}-locale`}>
        {locale}|{effectiveLocale}
      </span>
      <span data-testid={`${id}-timezone`}>
        {timezone}|{effectiveTimezone}
      </span>
      <button type="button" onClick={() => setLocale('ko-KR')}>
        {id} Korean
      </button>
      <button type="button" onClick={() => setTimezone('Asia/Seoul')}>
        {id} Seoul
      </button>
    </div>
  );
}

function ThemeProbe({ id }: { id: string }) {
  const { theme, effective, setTheme } = useTheme();

  return (
    <div>
      <span data-testid={`${id}-theme`}>{theme}</span>
      <span data-testid={`${id}-toaster-theme`}>{effective}</span>
      <button type="button" onClick={() => setTheme('light')}>
        {id} Light
      </button>
      <button type="button" onClick={() => setTheme('dark')}>
        {id} Dark
      </button>
      <button type="button" onClick={() => setTheme('system')}>
        {id} System
      </button>
    </div>
  );
}

function AllPreferencesProbe() {
  useLocale();
  useTimezone();
  useTheme();
  return null;
}

function ServerProbe() {
  const locale = useLocale();
  const timezone = useTimezone();
  const theme = useTheme();

  const value = [
    locale.locale,
    locale.effective,
    timezone.timezone,
    timezone.effective,
    theme.theme,
    theme.effective,
  ].join('|');
  return <span>{value}</span>;
}

beforeEach(() => {
  window.localStorage.clear();
  document.documentElement.removeAttribute('data-theme');
  prefersLight = false;
  mediaListeners = new Set();
  addMediaListener = vi.fn(
    (type: string, listener: EventListenerOrEventListenerObject | null) => {
      if (type === 'change' && typeof listener === 'function') {
        mediaListeners.add(
          listener as unknown as (event: MediaQueryListEvent) => void,
        );
      }
    },
  );
  removeMediaListener = vi.fn(
    (type: string, listener: EventListenerOrEventListenerObject | null) => {
      if (type === 'change' && typeof listener === 'function') {
        mediaListeners.delete(
          listener as unknown as (event: MediaQueryListEvent) => void,
        );
      }
    },
  );
  vi.stubGlobal(
    'matchMedia',
    vi.fn(
      (query: string) =>
        ({
          get matches() {
            return prefersLight;
          },
          media: query,
          onchange: null,
          addEventListener: addMediaListener,
          removeEventListener: removeMediaListener,
          addListener: vi.fn(),
          removeListener: vi.fn(),
          dispatchEvent: vi.fn(() => true),
        }) as unknown as MediaQueryList,
    ),
  );
});

afterEach(() => {
  cleanup();
  vi.useRealTimers();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  window.localStorage.clear();
  document.documentElement.removeAttribute('data-theme');
});

describe('locale and timezone preferences', () => {
  test('shares setters across mounted consumers and follows external storage changes', () => {
    const systemLocale = window.navigator.language || 'en-US';
    const systemTimezone = Intl.DateTimeFormat().resolvedOptions().timeZone;

    render(
      <>
        <LocaleProbe id="first" />
        <LocaleProbe id="second" />
      </>,
    );

    expect(screen.getByTestId('first-locale').textContent).toBe(
      `auto|${systemLocale}`,
    );
    expect(screen.getByTestId('second-timezone').textContent).toBe(
      `auto|${systemTimezone}`,
    );

    fireEvent.click(screen.getByRole('button', { name: 'first Korean' }));
    expect(screen.getByTestId('first-locale').textContent).toBe('ko-KR|ko-KR');
    expect(screen.getByTestId('second-locale').textContent).toBe('ko-KR|ko-KR');

    fireEvent.click(screen.getByRole('button', { name: 'second Seoul' }));
    expect(screen.getByTestId('first-timezone').textContent).toBe(
      'Asia/Seoul|Asia/Seoul',
    );
    expect(screen.getByTestId('second-timezone').textContent).toBe(
      'Asia/Seoul|Asia/Seoul',
    );

    act(() => {
      window.localStorage.setItem('cclb.locale', 'en-US');
      window.dispatchEvent(
        new StorageEvent('storage', {
          key: 'cclb.locale',
          newValue: 'en-US',
        }),
      );
      window.localStorage.setItem('cclb.timezone', 'UTC');
      window.dispatchEvent(
        new StorageEvent('storage', {
          key: 'cclb.timezone',
          newValue: 'UTC',
        }),
      );
    });

    expect(screen.getByTestId('first-locale').textContent).toBe('en-US|en-US');
    expect(screen.getByTestId('second-locale').textContent).toBe('en-US|en-US');
    expect(screen.getByTestId('first-timezone').textContent).toBe('UTC|UTC');
    expect(screen.getByTestId('second-timezone').textContent).toBe('UTC|UTC');
  });
});

describe('theme preference', () => {
  test('keeps initTheme applying the stored selection before React mounts', () => {
    window.localStorage.setItem('cclb.theme', 'light');

    initTheme();

    expect(document.documentElement.getAttribute('data-theme')).toBe('light');
  });

  test('keeps mounted consumers, document theme, and effective theme synchronized', () => {
    render(
      <>
        <ThemeProbe id="first" />
        <ThemeProbe id="second" />
      </>,
    );

    expect(screen.getByTestId('first-theme').textContent).toBe('system');
    expect(screen.getByTestId('first-toaster-theme').textContent).toBe('dark');
    expect(document.documentElement.getAttribute('data-theme')).toBe('system');

    fireEvent.click(screen.getByRole('button', { name: 'first Light' }));
    expect(screen.getByTestId('first-theme').textContent).toBe('light');
    expect(screen.getByTestId('second-theme').textContent).toBe('light');
    expect(screen.getByTestId('second-toaster-theme').textContent).toBe(
      'light',
    );
    expect(document.documentElement.getAttribute('data-theme')).toBe('light');

    fireEvent.click(screen.getByRole('button', { name: 'second System' }));
    expect(screen.getByTestId('first-theme').textContent).toBe('system');
    expect(screen.getByTestId('first-toaster-theme').textContent).toBe('dark');
    expect(document.documentElement.getAttribute('data-theme')).toBe('system');

    act(() => {
      prefersLight = true;
      const event = {
        matches: true,
        media: SYSTEM_THEME_QUERY,
      } as MediaQueryListEvent;
      for (const listener of [...mediaListeners]) listener(event);
    });

    expect(screen.getByTestId('first-toaster-theme').textContent).toBe('light');
    expect(screen.getByTestId('second-toaster-theme').textContent).toBe(
      'light',
    );
    expect(document.documentElement.getAttribute('data-theme')).toBe('system');

    fireEvent.click(screen.getByRole('button', { name: 'first Dark' }));
    expect(screen.getByTestId('first-theme').textContent).toBe('dark');
    expect(screen.getByTestId('second-toaster-theme').textContent).toBe('dark');
    expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
    expect(mediaListeners.size).toBe(0);

    act(() => {
      window.localStorage.setItem('cclb.theme', 'light');
      window.dispatchEvent(
        new StorageEvent('storage', {
          key: 'cclb.theme',
          newValue: 'light',
        }),
      );
    });

    expect(screen.getByTestId('first-theme').textContent).toBe('light');
    expect(screen.getByTestId('second-toaster-theme').textContent).toBe(
      'light',
    );
    expect(document.documentElement.getAttribute('data-theme')).toBe('light');
  });
});

test('does not publish a new snapshot when stored values are unchanged', () => {
  let localeRenders = 0;
  let themeRenders = 0;

  function LocaleRenderCounter() {
    useLocale();
    localeRenders += 1;
    return null;
  }

  function ThemeRenderCounter() {
    useTheme();
    themeRenders += 1;
    return null;
  }

  render(
    <>
      <LocaleRenderCounter />
      <ThemeRenderCounter />
    </>,
  );
  const initialLocaleRenders = localeRenders;
  const initialThemeRenders = themeRenders;

  act(() => {
    window.dispatchEvent(
      new StorageEvent('storage', {
        key: 'cclb.locale',
        newValue: null,
      }),
    );
    window.dispatchEvent(
      new StorageEvent('storage', {
        key: 'cclb.theme',
        newValue: null,
      }),
    );
  });

  expect(localeRenders).toBe(initialLocaleRenders);
  expect(themeRenders).toBe(initialThemeRenders);
});

test('reuses cached client snapshots until a preference source changes', () => {
  const getItem = vi.spyOn(Storage.prototype, 'getItem');
  const resolvedOptions = vi.spyOn(
    Intl.DateTimeFormat.prototype,
    'resolvedOptions',
  );
  const view = render(
    <>
      <LocaleProbe id="cache" />
      <ThemeProbe id="cache" />
    </>,
  );
  const readCount = (key: string) =>
    getItem.mock.calls.filter(([storedKey]) => storedKey === key).length;
  const initialReads = {
    locale: readCount('cclb.locale'),
    timezone: readCount('cclb.timezone'),
    theme: readCount('cclb.theme'),
  };
  const initialTimezoneResolutions = resolvedOptions.mock.calls.length;

  expect(initialReads.locale).toBeGreaterThan(0);
  expect(initialReads.timezone).toBeGreaterThan(0);
  expect(initialReads.theme).toBeGreaterThan(0);
  expect(initialTimezoneResolutions).toBeGreaterThan(0);

  view.rerender(
    <>
      <LocaleProbe id="cache" />
      <ThemeProbe id="cache" />
    </>,
  );
  expect(readCount('cclb.locale')).toBe(initialReads.locale);
  expect(readCount('cclb.timezone')).toBe(initialReads.timezone);
  expect(readCount('cclb.theme')).toBe(initialReads.theme);
  expect(resolvedOptions).toHaveBeenCalledTimes(initialTimezoneResolutions);

  fireEvent.click(screen.getByRole('button', { name: 'cache Korean' }));
  fireEvent.click(screen.getByRole('button', { name: 'cache Light' }));
  expect(screen.getByTestId('cache-locale').textContent).toBe('ko-KR|ko-KR');
  expect(screen.getByTestId('cache-theme').textContent).toBe('light');
  expect(readCount('cclb.locale')).toBe(initialReads.locale);
  expect(readCount('cclb.timezone')).toBe(initialReads.timezone);
  expect(readCount('cclb.theme')).toBe(initialReads.theme);
  expect(resolvedOptions).toHaveBeenCalledTimes(initialTimezoneResolutions);

  act(() => {
    window.localStorage.setItem('cclb.timezone', 'UTC');
    window.dispatchEvent(
      new StorageEvent('storage', {
        key: 'cclb.timezone',
        newValue: 'UTC',
      }),
    );
  });
  expect(screen.getByTestId('cache-timezone').textContent).toBe('UTC|UTC');
  expect(readCount('cclb.locale')).toBe(initialReads.locale);
  expect(readCount('cclb.timezone')).toBe(initialReads.timezone + 1);
  expect(readCount('cclb.theme')).toBe(initialReads.theme);
  expect(resolvedOptions).toHaveBeenCalledTimes(initialTimezoneResolutions);

  const readsAfterStorage = getItem.mock.calls.length;
  view.rerender(
    <>
      <LocaleProbe id="cache" />
      <ThemeProbe id="cache" />
    </>,
  );
  expect(getItem).toHaveBeenCalledTimes(readsAfterStorage);
  expect(resolvedOptions).toHaveBeenCalledTimes(initialTimezoneResolutions);
});

test('removes shared storage and system theme listeners after the last unmount', () => {
  const addWindowListener = vi.spyOn(window, 'addEventListener');
  const removeWindowListener = vi.spyOn(window, 'removeEventListener');
  const { unmount } = render(<AllPreferencesProbe />);
  const storageListeners = addWindowListener.mock.calls
    .filter(([type]) => type === 'storage')
    .map(([, listener]) => listener);

  expect(storageListeners.length).toBeGreaterThan(0);
  expect(mediaListeners.size).toBe(1);

  unmount();

  for (const listener of storageListeners) {
    expect(removeWindowListener).toHaveBeenCalledWith('storage', listener);
  }
  expect(removeMediaListener).toHaveBeenCalled();
  expect(mediaListeners.size).toBe(0);
});

test('uses stable server defaults instead of browser preference state', () => {
  window.localStorage.setItem('cclb.locale', 'ko-KR');
  window.localStorage.setItem('cclb.timezone', 'Asia/Seoul');
  window.localStorage.setItem('cclb.theme', 'light');

  const first = renderToString(<ServerProbe />);
  const second = renderToString(<ServerProbe />);

  expect(first).toBe('<span>auto|en-US|auto|UTC|dark|dark</span>');
  expect(second).toBe(first);
});

test('does not recompute absolute timestamps for unrelated renders', () => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date('2026-06-18T00:00:00.000Z'));
  window.localStorage.setItem('cclb.locale', 'en-US');
  window.localStorage.setItem('cclb.timezone', 'UTC');
  const toLocaleStringSpy = vi
    .spyOn(Date.prototype, 'toLocaleString')
    .mockReturnValue('absolute timestamp');
  const timestamp = Date.UTC(2026, 5, 17, 23, 59, 59);
  const resetTimestamp = Date.UTC(2026, 5, 18, 0, 1, 0);

  const { rerender } = render(
    <div data-version="first">
      <RelativeTime ts={timestamp} />
      <ResetCountdown ts={resetTimestamp} />
    </div>,
  );

  expect(toLocaleStringSpy).toHaveBeenCalledTimes(2);

  rerender(
    <div data-version="second">
      <RelativeTime ts={timestamp} />
      <ResetCountdown ts={resetTimestamp} />
    </div>,
  );

  expect(toLocaleStringSpy).toHaveBeenCalledTimes(2);
});
