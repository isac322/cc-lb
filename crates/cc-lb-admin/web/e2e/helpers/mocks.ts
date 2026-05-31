import type { BrowserContext, Page, Request } from '@playwright/test';

/**
 * Network-mocking primitives keyed to the BDD contract's failure scenarios.
 *
 * Conventions:
 *   - URL patterns are glob-style as accepted by `page.route()` /
 *     `context.route()`. Always anchor with `**` for the host part so the
 *     pattern survives the per-worker admin port.
 *   - Each helper returns an `unroute` function that removes the route.
 *     Tests should call it in `afterEach` if the mock should not leak.
 */

export type Unroute = () => Promise<void>;

/**
 * Make every matching request fail with the given status + JSON body.
 * Default body shape mirrors what ApiError parses on the dashboard side.
 */
export async function stubStatus(
  scope: Page | BrowserContext,
  urlPattern: string,
  status: number,
  body: unknown = { error: { message: `mocked ${status}` } },
): Promise<Unroute> {
  const handler = async (route: import('@playwright/test').Route) => {
    await route.fulfill({
      status,
      contentType: 'application/json',
      body: typeof body === 'string' ? body : JSON.stringify(body),
    });
  };
  await scope.route(urlPattern, handler);
  return () => scope.unroute(urlPattern, handler);
}

/** Stub a 500 with an arbitrary detail message. Wraps `stubStatus(500, ...)`. */
export function stub500(scope: Page | BrowserContext, urlPattern: string, detail = 'mocked server failure'): Promise<Unroute> {
  return stubStatus(scope, urlPattern, 500, { error: { message: detail } });
}

/**
 * Stub a 409 stale-revision conflict on a versioned resource. The body shape
 * matches `useConfigDraft` / `useUpstreams` ConflictError handling.
 */
export function stub409Conflict(
  scope: Page | BrowserContext,
  urlPattern: string,
  currentRevision: number,
  code = 'stale_revision',
): Promise<Unroute> {
  return stubStatus(scope, urlPattern, 409, {
    error: code,
    current_revision: currentRevision,
  });
}

/** Pretend any matching admin endpoint requires authentication. */
export function stub401(scope: Page | BrowserContext, urlPattern: string): Promise<Unroute> {
  return stubStatus(scope, urlPattern, 401, { error: 'unauthorized' });
}

/**
 * Delay matching responses by `delayMs` and then forward to the real backend.
 * Use this for loading-state scenarios (e.g. `Given the X endpoint is
 * intentionally slow`).
 */
export async function stubSlow(
  scope: Page | BrowserContext,
  urlPattern: string,
  delayMs: number,
): Promise<Unroute> {
  const handler = async (route: import('@playwright/test').Route) => {
    await new Promise((r) => setTimeout(r, delayMs));
    await route.continue();
  };
  await scope.route(urlPattern, handler);
  return () => scope.unroute(urlPattern, handler);
}

/**
 * Capture every matching request and run a callback per request. Returns a
 * list that's appended to live. Useful for asserting `If-Match` headers, exact
 * URLs, polling counts, etc.
 */
export function captureRequests(
  scope: Page | BrowserContext,
  urlPattern: RegExp | string,
): { requests: Request[]; stop: () => void } {
  const requests: Request[] = [];
  const handler = (req: Request) => {
    const url = req.url();
    const matches =
      typeof urlPattern === 'string'
        ? globMatch(urlPattern, url)
        : urlPattern.test(url);
    if (matches) requests.push(req);
  };
  scope.on('request', handler);
  return {
    requests,
    stop: () => scope.off('request', handler),
  };
}

/**
 * Install a `window.alert` capture. The Upstreams page raises native alerts
 * on mutation failure (Enable/Disable/Edit/Delete) — this returns the
 * captured messages.
 *
 * Playwright's native `page.on('dialog', d => { ...; d.accept(); })` is
 * usually enough; this helper exists for symmetry with the BDD scenario step
 * and ensures the dialog is always dismissed.
 */
export function captureAlerts(page: Page): { messages: string[]; stop: () => void } {
  const messages: string[] = [];
  const handler = async (dialog: import('@playwright/test').Dialog) => {
    if (dialog.type() === 'alert') {
      messages.push(dialog.message());
    }
    await dialog.dismiss().catch(() => undefined);
  };
  page.on('dialog', handler);
  return {
    messages,
    stop: () => page.off('dialog', handler),
  };
}

// ---------- internals ----------

function globMatch(pattern: string, url: string): boolean {
  const regex = new RegExp(
    '^' +
      pattern
        .replace(/[.+?^${}()|[\]\\]/g, '\\$&')
        .replace(/\*\*/g, '__GLOBSTAR__')
        .replace(/\*/g, '[^/]*')
        .replace(/__GLOBSTAR__/g, '.*') +
      '$',
  );
  return regex.test(url);
}
