import { test, expect } from '@playwright/test';

test.describe.serial('Upstreams Management', () => {
  test.beforeEach(async ({ page }) => {
    await page.addInitScript(() => {
      localStorage.setItem('cc-lb-admin-token', 'test-admin-token');
    });
    await page.goto('/upstreams');
    await page.waitForLoadState('networkidle');
  });

  test('should list upstreams', async ({ page }) => {
    await expect(page.getByRole('cell', { name: 'dummy' })).toBeVisible({ timeout: 10000 });
  });

  test('should create an API key upstream', async ({ page }) => {
    await page.getByRole('button', { name: '+ New Upstream' }).click();
    await expect(page.getByRole('dialog')).toBeVisible();

    await page.getByLabel('Name').fill('test-api-key-upstream');
    await page.getByLabel('Anthropic API Key').check();
    await page.getByLabel('API Key Env Var').fill('TEST_API_KEY');
    await page.getByRole('button', { name: 'Create' }).click();

    await expect(page.getByRole('dialog')).not.toBeVisible();
    const row = page.getByRole('row', { name: /test-api-key-upstream/ });
    await expect(row).toBeVisible();
    await expect(row.getByRole('cell', { name: 'anthropic_api_key' })).toBeVisible();
  });

  test('should edit an upstream', async ({ page }) => {
    await page.waitForSelector('table');
    
    const row = page.getByRole('row', { name: /test-api-key-upstream/ });
    await row.getByRole('button', { name: 'Edit' }).click();

    await expect(page.getByRole('dialog')).toBeVisible();
    await page.getByLabel('Base URL').fill('http://localhost:8081');
    await page.getByRole('button', { name: 'Save Changes' }).click();

    await expect(page.getByRole('dialog')).not.toBeVisible();
    await expect(page.getByRole('cell', { name: 'test-api-key-upstream' })).toBeVisible();
  });

  test('should enable/disable an upstream', async ({ page }) => {
    await page.waitForSelector('table');
    const row = page.getByRole('row', { name: /test-api-key-upstream/ });
    
    await row.getByRole('button', { name: 'Disable' }).click();
    await expect(row.getByText('Disabled')).toBeVisible({ timeout: 10000 });

    await row.getByRole('button', { name: 'Enable' }).click();
    await expect(row.getByText('Active')).toBeVisible({ timeout: 10000 });
  });

  test('should delete an upstream', async ({ page }) => {
    await page.waitForSelector('table');
    const row = page.getByRole('row', { name: /test-api-key-upstream/ });
    
    await row.getByRole('button', { name: 'Delete' }).click();
    await expect(page.getByRole('dialog')).toBeVisible();
    await page.getByRole('dialog').getByRole('button', { name: 'Delete', exact: true }).click();

    await expect(page.getByRole('dialog')).not.toBeVisible();
    await expect(page.getByRole('cell', { name: 'test-api-key-upstream' })).not.toBeVisible();
  });

  test('should create an OAuth upstream and complete PKCE flow', async ({ page, context }) => {
    await page.getByRole('button', { name: '+ New Upstream' }).click();
    await expect(page.getByRole('dialog')).toBeVisible();

    await page.getByLabel('Name').fill('test-oauth-upstream');
    await page.getByLabel('Anthropic OAuth').check();
    await page.getByRole('button', { name: 'Create' }).click();

    await expect(page.getByText('Connect OAuth')).toBeVisible({ timeout: 10000 });

    const pagePromise = context.waitForEvent('page');
    await page.getByRole('button', { name: 'Connect Claude OAuth' }).click();
    const newPage = await pagePromise;

    await newPage.waitForLoadState();
    console.log("NEW PAGE URL:", newPage.url());
    console.log("NEW PAGE CONTENT:", await newPage.content());
    
    // If there's an authorize button on the fake-anthropic page, click it
    const authorizeBtn = newPage.getByRole('button', { name: /authorize/i });
    if (await authorizeBtn.isVisible()) {
      await authorizeBtn.click();
    }
    
    await expect(page.getByRole('dialog')).not.toBeVisible({ timeout: 10000 });
    
    await expect(page.getByRole('cell', { name: 'test-oauth-upstream' })).toBeVisible();
    await expect(page.getByRole('cell', { name: 'anthropic_oauth' })).toBeVisible();
  });
});
