import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { chromium, expect } from '@playwright/test';

// Seed file: [{ provider: 'openai', principal: { id: '<principal UUID>' } }, ...].
// Use isolated synthetic admin data and CC_LB_ADMIN_TOKEN, as in the API-key E2E suite.
const [label, base, seedFile, outputDir] = process.argv.slice(2);
const token = process.env.CC_LB_ADMIN_TOKEN;
if (!label || !base || !seedFile || !outputDir || !token) {
  throw new Error('Usage: CC_LB_ADMIN_TOKEN=... bun qa/capture-subscription-preference.mjs <before|after> <base URL> <seed JSON> <output directory>');
}
const seeds = JSON.parse(await readFile(seedFile, 'utf8'));
await mkdir(outputDir, { recursive: true });
const browser = await chromium.launch({ executablePath: process.env.CHROME_EXE });
const results = [];
try {
  const context = await browser.newContext({ viewport: { width: 390, height: 900 }, deviceScaleFactor: 1 });
  await context.addInitScript((value) => localStorage.setItem('cc-lb-admin-token', value), token);
  const page = await context.newPage();
  const errors = [];
  page.on('pageerror', (error) => errors.push(error.message));
  for (const seed of seeds) {
    const url = new URL('/principals', base);
    url.searchParams.set('provider', seed.provider);
    url.searchParams.set('selectedId', seed.principal.id);
    await page.goto(url.toString(), { waitUntil: 'domcontentloaded' });
    const basic = page.getByRole('tab', { name: 'Basic', exact: true });
    await expect(basic).toBeVisible();
    if ((await basic.getAttribute('aria-selected')) !== 'true') await basic.click();
    await expect(page.getByRole('heading', { name: 'Router', exact: true })).toBeVisible();
    await page.evaluate(() => document.fonts.ready);
    if (label !== 'before' || seed.provider === 'anthropic') {
      await expect(page.getByRole('switch', {
        name: label === 'before' ? 'Keep prompt cache warm by reusing upstreams' : 'Prefer subscription upstreams',
        exact: true,
      })).toBeEnabled();
    }
    for (const width of [390, 768, 1440]) {
      await page.setViewportSize({ width, height: 900 });
      // Reframe below both the shell and principal's sticky header.
      const geometry = await page.evaluate(() => {
        const tab = [...document.querySelectorAll('[role=tab]')].find((node) => node.textContent.trim() === 'Basic');
        const section = tab.closest('section');
        const headers = [...document.querySelectorAll('*')].filter((node) => {
          const position = getComputedStyle(node).position;
          if (position !== 'sticky' && position !== 'fixed') return false;
          if (node.contains(section) || section.contains(node)) return false;
          const rect = node.getBoundingClientRect();
          return rect.width > 200 && rect.height > 0 && rect.top >= 0 && rect.top < 240 && rect.bottom < 300;
        });
        const headerBottom = Math.max(0, ...headers.map((node) => node.getBoundingClientRect().bottom));
        let scroller = section.parentElement;
        while (scroller && scroller !== document.documentElement) {
          if (/(auto|scroll)/.test(getComputedStyle(scroller).overflowY) && scroller.scrollHeight > scroller.clientHeight) break;
          scroller = scroller.parentElement;
        }
        const delta = section.getBoundingClientRect().top - headerBottom - 16;
        if (scroller && scroller !== document.documentElement) scroller.scrollTop += delta;
        else window.scrollBy(0, delta);
        const textVisible = (prefix) => {
          const node = [...section.querySelectorAll('*')].find((element) => element.children.length === 0 && element.textContent.trim().startsWith(prefix));
          if (!node) return false;
          const rect = node.getBoundingClientRect();
          const hit = document.elementFromPoint(rect.left + Math.min(rect.width / 2, 40), rect.top + rect.height / 2);
          return rect.top >= headerBottom && rect.bottom <= innerHeight && hit !== null && !headers.some((header) => header.contains(hit));
        };
        return { heading: textVisible('Router'), description: textVisible('Pick which upstream serves'), overflow: document.documentElement.scrollWidth - document.documentElement.clientWidth };
      });
      expect(geometry).toEqual({ heading: true, description: true, overflow: 0 });
      const file = `${label}-${seed.provider}-${width}.png`;
      await page.screenshot({ path: `${outputDir}/${file}` });
      results.push({ provider: seed.provider, width, file, ...geometry });
    }
  }
  expect(errors).toEqual([]);
  await writeFile(`${outputDir}/${label}-captures.json`, JSON.stringify(results, null, 2));
} finally {
  await browser.close();
}
