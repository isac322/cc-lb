import type { Page } from '@playwright/test';

export const ADMIN_TOKEN_LOCALSTORAGE_KEY = 'cc-lb-admin-token';

/**
 * Inject the admin token into localStorage BEFORE any page script runs.
 * Use this in `test.beforeEach` so the auth gate is bypassed and the
 * dashboard mounts straight into the requested route.
 */
export async function signIn(page: Page, token: string): Promise<void> {
  await page.addInitScript(
    ([key, value]) => {
      try {
        globalThis.localStorage.setItem(key as string, value as string);
      } catch {
        // localStorage may throw in incognito / restricted modes.
      }
    },
    [ADMIN_TOKEN_LOCALSTORAGE_KEY, token],
  );
}

/**
 * Remove the admin token from localStorage. Use to exercise the auth-gate
 * scenarios in §4.1.
 */
export async function signOut(page: Page): Promise<void> {
  await page.evaluate((key) => {
    try {
      globalThis.localStorage.removeItem(key);
    } catch {
      // ignore
    }
  }, ADMIN_TOKEN_LOCALSTORAGE_KEY);
}

/**
 * Navigate to a route on the worker's own dashboard. Always prefer this over
 * raw `page.goto(...)` — the dashboard binds to the worker's admin port, not
 * a global one.
 */
export async function gotoDashboard(
  page: Page,
  adminUrl: string,
  routePath: string,
): Promise<void> {
  const url = new URL(routePath.startsWith('/') ? routePath : `/${routePath}`, adminUrl);
  await page.goto(url.toString());
}
