import { expect, test } from '@playwright/test';

// Sanitized admin event shapes captured during dashboard QA (2026-11-15).
// Provider/model/status values are independent of the table's layout helpers.
const rows = [
  { event_id: 'captured-openai', principal_id: 'principal-openai', upstream_id: 'upstream-openai', upstream_name: 'dashboard-openai', event_kind: 'openai_responses', model: 'gpt-6.1', status: 200 },
  { event_id: 'captured-gemini', principal_id: 'principal-gemini', upstream_id: 'upstream-gemini', upstream_name: 'dashboard-gemini', event_kind: 'gemini_generate_content', model: 'gemini-3.5-flash', status: 403 },
  { event_id: 'anthropic-control', principal_id: 'principal-anthropic', upstream_id: 'upstream-anthropic', upstream_name: 'anthropic-control', event_kind: 'messages', model: 'claude-opus-4-7', status: 200 },
].map((row) => ({ ...row, request_id: row.event_id, ts: 1794683225, ts_ms: 1794683225000, source_kind: 'proxy', request_kind: 'unknown', duration_ms: 254 }));

for (const theme of ['light', 'dark']) {
  for (const width of [390, 768, 1440]) {
    test(`request identifiers remain distinguishable at ${width}px ${theme}`, async ({ page }, testInfo) => {
      await page.setViewportSize({ width, height: 900 });
      await page.addInitScript((theme) => {
        localStorage.setItem('cc-lb-admin-token', 'synthetic-qa');
        localStorage.setItem('cclb.theme', theme);
        localStorage.setItem('cclb.locale', 'en-US');
      }, theme);
      await page.route('**/admin/**', async (route) => {
        const path = new URL(route.request().url()).pathname;
        const json = (body: unknown) => route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(body) });
        if (path === '/admin/v1/auth/session') return json({ authority: 'e2e', subject: 'operator', kind: 'human', provider_id: 'static', email: null, display_name: 'QA', expires_at_unix_secs: null, auth_mode: 'static_token' });
        if (path === '/admin/v1/events/recent') return json({ events: rows, observed: true, count: rows.length, limit: 200 });
        if (path === '/admin/v1/events/detail') return json(rows.find((row) => row.event_id === new URL(route.request().url()).searchParams.get('id')));
        if (path === '/admin/v1/events/histogram') return json({ buckets: [], bucket_ms: 60000, bucket_count: 0 });
        if (path === '/admin/v1/principals') return json({ principals: rows.map((row) => ({ id: row.principal_id, name: row.upstream_name })) });
        if (path === '/admin/v1/upstreams') return json({ upstreams: rows.map((row, index) => ({ id: row.upstream_id, name: row.upstream_name, kind: ['openai_oauth', 'gemini_oauth', 'anthropic_oauth'][index], enabled: true, spec_revision: 1, base_url: null, warmup_enabled: false, warmup_dialect_plugin: null, status: { last_apply_error: null, last_apply_at_unix_secs: null, last_warmup_at_unix_secs: null } })) });
        if (path === '/admin/health') return json({ status: 'ok', version: 'fixture', git_sha: 'fixture', uptime_secs: 1 });
        if (path === '/admin/v1/upstreams/oauth/status') return json({ upstreams: [] });
        return json({});
      });
      await page.goto('/logs?event_kind=all&since_unix_secs=1794683200&until_unix_secs=1794683300');
      await expect(page.getByRole('row', { name: 'View request captured-gemini', exact: true })).toBeVisible();
      await page.evaluate(() => document.fonts.ready);
      expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
      for (const row of rows) {
        const rendered = page.getByRole('row', { name: `View request ${row.event_id}`, exact: true });
        if (width === 390) {
          // Phones retain their existing card arrangement and full facts in
          // the drawer; the readable-column regression targets tablet tables.
          await expect(rendered.locator(`[title="${row.model}"]`)).toBeVisible();
          await rendered.click();
          const drawer = page.getByRole('dialog', { name: 'Request detail' });
          await expect(drawer).toBeVisible();
          await expect(drawer.getByText(row.model, { exact: true })).toBeVisible();
          await expect(drawer.getByText(row.upstream_name, { exact: true }).first()).toBeVisible();
          await page.keyboard.press('Escape');
          await expect(drawer).toHaveCount(0);
          continue;
        }
        // DOM text and title alone do not prove that the operator can read it.
        // Measure the text range through the distinguishing provider/version.
        for (const [title, prefix] of [
          [row.upstream_name, row.upstream_name.startsWith('dashboard-') ? row.upstream_name.slice(0, 11) : 'anthropic'],
          [row.model, row.model.replace(/^claude-/, '').slice(0, 11)],
        ]) {
          const cells = rendered.locator(`[title="${title}"]`);
          for (const cell of await cells.all()) {
            const visible = await cell.evaluate((element, prefix) => {
              const walker = document.createTreeWalker(element, NodeFilter.SHOW_TEXT);
              let node = walker.nextNode();
              while (node && !node.textContent?.includes(prefix)) node = walker.nextNode();
              if (!node) return false;
              const range = document.createRange();
              const start = node.textContent!.indexOf(prefix);
              range.setStart(node, start);
              range.setEnd(node, start + prefix.length);
              const text = range.getBoundingClientRect();
              for (let ancestor: Element | null = element; ancestor && ancestor.tagName !== 'TR'; ancestor = ancestor.parentElement) {
                const style = getComputedStyle(ancestor);
                if (style.overflowX === 'hidden' || style.overflowX === 'clip') {
                  const bounds = ancestor.getBoundingClientRect();
                  const right = bounds.right - parseFloat(style.paddingRight);
                  if (text.right > right + 1) return false;
                }
              }
              return true;
            }, prefix);
            expect(visible, `${width}px ${row.event_id}: ${prefix} must be painted`).toBe(true);
          }
          expect(await cells.count()).toBeGreaterThan(0);
        }
      }
      await page.screenshot({ path: testInfo.outputPath(`identifiers-${width}-${theme}.png`), fullPage: true });
    });
  }
}
