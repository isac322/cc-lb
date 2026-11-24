import { expect, type Page, type TestInfo, test } from '@playwright/test';

const ADMIN_TOKEN = process.env.CC_LB_CSP_ADMIN_TOKEN;
if (!ADMIN_TOKEN) {
  throw new Error('CC_LB_CSP_ADMIN_TOKEN is required for the packaged CSP suite');
}

const ENTRY_MODULE = '**/assets/index-*.js';
const LIGHT_BACKGROUND = 'rgb(227, 229, 231)';
type Theme = 'dark' | 'light' | 'system';
type CspEvent = {
  directive: string;
  blockedURI: string;
  sourceFile: string;
  lineNumber: number;
  columnNumber: number;
  sample: string;
};
type CspWindow = Window & { __ccLbCspEvents?: CspEvent[]; __ccLbInlineRan?: boolean };

function recordCspEvents() {
  const events: CspEvent[] = [];
  (window as CspWindow).__ccLbCspEvents = events;
  document.addEventListener('securitypolicyviolation', (event) => {
    events.push({
      directive: event.violatedDirective,
      blockedURI: event.blockedURI,
      sourceFile: event.sourceFile,
      lineNumber: event.lineNumber,
      columnNumber: event.columnNumber,
      sample: event.sample,
    });
  });
}

async function assertNoBootstrapOrFontCspEvents(page: Page, testInfo: TestInfo, label: string) {
  const events = await page.evaluate(() => (window as CspWindow).__ccLbCspEvents ?? []);
  await testInfo.attach(`csp-${label}`, {
    body: JSON.stringify(events, null, 2),
    contentType: 'application/json',
  });
  // Eval capability probes (including Zod's) are outside this bootstrap/font claim.
  expect(
    events.filter(({ directive, blockedURI, sourceFile }) =>
      directive.startsWith('font-src') ||
      (directive.startsWith('script-src') &&
        (blockedURI === 'inline' ||
          [blockedURI, sourceFile].some((value) =>
            /(?:^|\/)theme-init\.js(?:[?#]|$)/.test(value),
          ))),
    ),
    `${label} bootstrap/font CSP events`,
  ).toEqual([]);
}

test('stored theme is applied before the entry module runs', async ({ browser, baseURL }) => {
  for (const theme of ['dark', 'light', 'system'] as const) {
    const page = await browser.newPage({ baseURL, colorScheme: 'dark' });
    await page.addInitScript((storedTheme: Theme) => {
      localStorage.setItem('cclb.theme', storedTheme);
    }, theme);
    await page.route(ENTRY_MODULE, (route) => route.abort());
    await page.goto('/', { waitUntil: 'load' });
    await expect(page.locator('html')).toHaveAttribute('data-theme', theme);
    if (theme === 'light') {
      await expect
        .poll(() => page.evaluate(() => getComputedStyle(document.documentElement).backgroundColor))
        .toBe(LIGHT_BACKGROUND);
    }
    await page.close();
  }
});

test('missing, invalid, and throwing storage leave the bootstrap on system', async ({
  browser,
  baseURL,
}) => {
  for (const storageMode of ['missing', 'invalid', 'throwing'] as const) {
    const page = await browser.newPage({ baseURL });
    const pageErrors: string[] = [];
    page.on('pageerror', (error) => pageErrors.push(error.message));
    await page.addInitScript((mode: typeof storageMode) => {
      if (mode === 'throwing') {
        Object.defineProperty(Storage.prototype, 'getItem', {
          configurable: true,
          value() {
            throw new Error('storage unavailable');
          },
        });
      } else if (mode === 'invalid') {
        localStorage.setItem('cclb.theme', 'invalid');
      } else {
        localStorage.removeItem('cclb.theme');
      }
    }, storageMode);
    await page.route(ENTRY_MODULE, (route) => route.abort());
    await page.goto('/', { waitUntil: 'load' });
    await expect(page.locator('html')).toHaveAttribute('data-theme', 'system');
    expect(pageErrors).toEqual([]);
    await page.close();
  }
});

test('packaged bootstrap and fonts load alongside authenticated routes', async ({
  page,
  baseURL,
}, testInfo) => {
  const fontResponses: Array<{ url: string; ok: boolean }> = [];
  page.on('response', (response) => {
    if (new URL(response.url()).pathname.endsWith('.woff2')) {
      fontResponses.push({ url: response.url(), ok: response.ok() });
    }
  });
  await page.addInitScript(recordCspEvents);
  await page.addInitScript((token: string) => {
    localStorage.setItem('cc-lb-admin-token', token);
  }, ADMIN_TOKEN);

  await page.goto('/', { waitUntil: 'load' });
  await expect(page.getByRole('heading', { name: 'Overview', exact: true })).toBeVisible();
  const loadedFonts = await page.evaluate(async () => {
    const [nonLatin, latin] = await Promise.all([
      document.fonts.load('16px "Hanken Grotesk Variable"', '\u0460'),
      document.fonts.load('16px "Hanken Grotesk Variable"', 'The quick brown fox'),
    ]);
    return { nonLatin: nonLatin.length, latin: latin.length };
  });
  expect(loadedFonts.nonLatin).toBeGreaterThan(0);
  expect(loadedFonts.latin).toBeGreaterThan(0);
  expect(fontResponses.length).toBeGreaterThan(0);
  const origin = new URL(baseURL ?? '').origin;
  expect(fontResponses.every(({ url, ok }) => ok && new URL(url).origin === origin)).toBe(true);
  await assertNoBootstrapOrFontCspEvents(page, testInfo, 'Overview');

  for (const [path, heading] of [
    ['/principals', 'Principals'],
    ['/logs', 'Logs'],
  ] as const) {
    await page.goto(path, { waitUntil: 'load' });
    await expect(page.getByRole('heading', { name: heading, exact: true })).toBeVisible();
    await assertNoBootstrapOrFontCspEvents(page, testInfo, heading);
  }
});

test('strict CSP blocks injected inline JavaScript', async ({ page }, testInfo) => {
  await page.addInitScript(recordCspEvents);
  await page.route(ENTRY_MODULE, (route) => route.abort());
  await page.goto('/', { waitUntil: 'load' });

  const beforeInlineEvents = await page.evaluate(
    () => (window as CspWindow).__ccLbCspEvents?.length ?? 0,
  );
  await page.evaluate(() => {
    (window as CspWindow).__ccLbInlineRan = false;
    const script = document.createElement('script');
    script.textContent = 'window.__ccLbInlineRan = true;';
    document.head.append(script);
  });
  await expect.poll(() => page.evaluate((before) =>
    (window as CspWindow).__ccLbCspEvents?.slice(before).some(({ directive, blockedURI }) =>
      directive.startsWith('script-src') && blockedURI === 'inline',
    ) ?? false,
  beforeInlineEvents)).toBe(true);
  const inlineResult = await page.evaluate(() => ({
    ran: (window as CspWindow).__ccLbInlineRan,
    events: (window as CspWindow).__ccLbCspEvents ?? [],
  }));
  await testInfo.attach('csp-inline-injection', {
    body: JSON.stringify(inlineResult.events, null, 2),
    contentType: 'application/json',
  });
  expect(inlineResult.ran).toBe(false);
  expect(inlineResult.events.slice(beforeInlineEvents).some(({ directive, blockedURI }) =>
    directive.startsWith('script-src') && blockedURI === 'inline',
  )).toBe(true);
});
