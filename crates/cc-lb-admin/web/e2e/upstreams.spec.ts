import { test, expect } from '@playwright/test';

test.describe('Upstreams Management', () => {
  test.beforeEach(async ({ page }) => {
    // Set the admin token in localStorage before navigating
    await page.addInitScript(() => {
      localStorage.setItem('cc-lb-admin-token', 'test-admin-token');
    });
    await page.goto('/admin/upstreams');
  });

  test('should list upstreams (empty initially)', async ({ page }) => {
    await expect(page.getByText('No upstreams configured')).toBeVisible();
  });

  test('should create an API key upstream', async ({ page }) => {
    await page.getByRole('button', { name: '+ New Upstream' }).click();
    await expect(page.getByRole('dialog')).toBeVisible();

    await page.getByLabel('Name').fill('test-api-key-upstream');
    await page.getByLabel('Anthropic API Key').check();
    await page.getByLabel('API Key Env Var').fill('TEST_API_KEY');
    await page.getByRole('button', { name: 'Create' }).click();

    await expect(page.getByRole('dialog')).not.toBeVisible();
    await expect(page.getByRole('cell', { name: 'test-api-key-upstream' })).toBeVisible();
    await expect(page.getByRole('cell', { name: 'anthropic_api_key' })).toBeVisible();
  });

  test('should edit an upstream', async ({ page }) => {
    // Assuming the upstream from previous test exists, or we create one
    // Since tests might run in order, let's just edit the one we created
    // Wait for the table to load
    await page.waitForSelector('table');
    
    const row = page.getByRole('row', { name: /test-api-key-upstream/ });
    await row.getByRole('button', { name: 'Edit' }).click();

    await expect(page.getByRole('dialog')).toBeVisible();
    await page.getByLabel('Base URL').fill('https://api.example.com');
    await page.getByRole('button', { name: 'Save Changes' }).click();

    await expect(page.getByRole('dialog')).not.toBeVisible();
    // Verify it's still there
    await expect(page.getByRole('cell', { name: 'test-api-key-upstream' })).toBeVisible();
  });

  test('should enable/disable an upstream', async ({ page }) => {
    await page.waitForSelector('table');
    const row = page.getByRole('row', { name: /test-api-key-upstream/ });
    
    // Disable
    await row.getByRole('button', { name: 'Disable' }).click();
    await expect(row.getByText('Disabled')).toBeVisible();

    // Enable
    await row.getByRole('button', { name: 'Enable' }).click();
    await expect(row.getByText('Active')).toBeVisible();
  });

  test('should delete an upstream', async ({ page }) => {
    await page.waitForSelector('table');
    const row = page.getByRole('row', { name: /test-api-key-upstream/ });
    
    await row.getByRole('button', { name: 'Delete' }).click();
    await expect(page.getByRole('dialog')).toBeVisible();
    await page.getByRole('button', { name: 'Delete', exact: true }).click();

    await expect(page.getByRole('dialog')).not.toBeVisible();
    await expect(page.getByRole('cell', { name: 'test-api-key-upstream' })).not.toBeVisible();
  });

  test('should create an OAuth upstream and complete PKCE flow', async ({ page, context }) => {
    await page.getByRole('button', { name: '+ New Upstream' }).click();
    await expect(page.getByRole('dialog')).toBeVisible();

    await page.getByLabel('Name').fill('test-oauth-upstream');
    await page.getByLabel('Anthropic OAuth').check();
    await page.getByRole('button', { name: 'Create' }).click();

    // Should show the Connect OAuth dialog
    await expect(page.getByText('Connect OAuth')).toBeVisible();

    // Click Connect Claude OAuth and handle the new tab
    const pagePromise = context.waitForEvent('page');
    await page.getByRole('button', { name: 'Connect Claude OAuth' }).click();
    const newPage = await pagePromise;

    // The fake-anthropic server should redirect back to our app
    // Wait for the new page to load and close it or let it redirect
    await newPage.waitForLoadState();
    
    // The polling in the original page should detect the change
    await expect(page.getByRole('dialog')).not.toBeVisible({ timeout: 10000 });
    
    // Verify it's in the list
    await expect(page.getByRole('cell', { name: 'test-oauth-upstream' })).toBeVisible();
    await expect(page.getByRole('cell', { name: 'anthropic_oauth' })).toBeVisible();
  });
});
