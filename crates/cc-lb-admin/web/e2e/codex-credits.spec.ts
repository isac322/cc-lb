import { expect, type Page, test } from '@playwright/test';
import { ID, installFixtures, scenarios } from './codexCreditsFixture';

async function expectNoOverflow(page: Page) {
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
  const facts = page.getByTestId('codex-credit-facts');
  expect(await facts.evaluate((element) => element.scrollWidth <= element.clientWidth)).toBe(true);
}

for (const width of [390, 768, 1440]) {
  for (const scenario of scenarios) {
    test(`Codex credits: ${scenario.name} at ${width}px`, async ({ page }, testInfo) => {
      await page.setViewportSize({ width, height: 900 });
      const latestRequests = await installFixtures(page, scenario.observations);
      await page.goto('/');
      const overview = page.getByTestId('upstream-usage-row');
      await expect(overview).toHaveCount(1);
      const facts = page.getByTestId('codex-credit-facts');
      await expect(facts).toHaveCount(1);
      await expect(facts.getByText(scenario.status, { exact: true })).toBeVisible();
      await expect(overview.getByRole('meter')).toHaveCount(2);
      await expect(overview.getByRole('meter', { name: '5h window used', exact: true })).toHaveAttribute('aria-valuenow', '25');
      await expect(overview.getByRole('meter', { name: '7d window used', exact: true })).toHaveAttribute('aria-valuenow', '50');
      await expect(facts.getByRole('meter')).toHaveCount(0);
      await expectNoOverflow(page);
      const overviewFacts = await facts.innerText();
      await page.screenshot({ path: testInfo.outputPath('overview.png'), fullPage: true });
      expect(latestRequests.some((url) => {
        const requested = url.searchParams.get('windows')?.split(',') ?? [];
        return requested.includes('overage') && requested.includes('unified');
      })).toBe(true);

      await overview.click();
      await expect(page).toHaveURL(
        (url) => url.pathname === '/upstreams' && url.searchParams.get('selectedId') === ID,
      );
      const detail = page.getByTestId('quota-snapshot-grid');
      await expect(detail).toBeVisible();
      await expect(detail.getByRole('listitem')).toHaveCount(3);
      await expect(facts).toHaveCount(1);
      await expect.poll(() => facts.innerText()).toBe(overviewFacts);
      await expect(detail.getByRole('meter')).toHaveCount(2);
      await expect(detail.getByRole('meter', { name: '5h', exact: true })).toHaveAttribute('aria-valuenow', '25');
      await expect(detail.getByRole('meter', { name: '7d', exact: true })).toHaveAttribute('aria-valuenow', '50');
      await expectNoOverflow(page);
      await page.screenshot({ path: testInfo.outputPath('detail.png'), fullPage: true });
    });
  }
}
