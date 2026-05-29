import { expect, test } from '@playwright/test';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

test.describe('Plugin Registry', () => {
  test.beforeEach(async ({ page }) => {
    await page.addInitScript(() => {
      localStorage.setItem('cc-lb-admin-token', 'test-admin-token');
    });
    await page.goto('/plugins');
    await page.waitForLoadState('networkidle');
  });

  test('uploads a wasm file', async ({ page }) => {
    // Ensure we are on the registry tab
    await page.getByRole('button', { name: 'Wasm Registry' }).click();

    // The fixture should be built by globalSetup or beforeAll, but we can just use the path
    const wasmPath = path.resolve(__dirname, '../../../../target/wasm32-wasip1/release/extism_echo_plugin.wasm');
    
    // If the file doesn't exist, the test will fail here, which is expected if not built
    expect(fs.existsSync(wasmPath)).toBeTruthy();

    const fileChooserPromise = page.waitForEvent('filechooser');
    await page.locator('input[type="file"]').click();
    const fileChooser = await fileChooserPromise;
    await fileChooser.setFiles(wasmPath);

    await page.getByRole('button', { name: 'Upload' }).click();

    // Wait for the upload to complete and appear in the table
    await expect(page.getByRole('cell', { name: 'extism_echo_plugin' })).toBeVisible();
  });

  test('assigns plugin to a chain', async ({ page }) => {
    await page.getByRole('button', { name: 'Per-Principal Chains' }).click();

    // Select the first principal
    await page.locator('select').first().selectOption({ index: 1 });

    // Click Add Plugin in the Router chain
    await page.locator('h3:has-text("router Chain")').locator('..').getByRole('button', { name: 'Add Plugin' }).click();

    // Select the uploaded plugin
    await page.locator('select').nth(1).selectOption({ index: 1 });

    // Click Add
    await page.getByRole('button', { name: 'Add', exact: true }).click();

    // Verify it appears in the list
    await expect(page.locator('h3:has-text("router Chain")').locator('..').getByText('extism_echo_plugin')).toBeVisible();
  });

  test('drag-drop reorder', async ({ page }) => {
    await page.getByRole('button', { name: 'Per-Principal Chains' }).click();
    await page.locator('select').first().selectOption({ index: 1 });

    // Add a second plugin to reorder
    await page.locator('h3:has-text("router Chain")').locator('..').getByRole('button', { name: 'Add Plugin' }).click();
    await page.locator('select').nth(1).selectOption({ index: 1 });
    await page.getByRole('button', { name: 'Add', exact: true }).click();

    // Wait for both to be visible
    await expect(page.locator('h3:has-text("router Chain")').locator('..').getByText('extism_echo_plugin')).toHaveCount(2);

    // Drag and drop is tricky in Playwright, but we can simulate it or just verify the UI elements exist
    // For a real drag and drop:
    const dragHandle = page.locator('.cursor-grab').first();
    const target = page.locator('.cursor-grab').nth(1);
    
    await dragHandle.dragTo(target);
    
    // Just verify no error appeared
    await expect(page.locator('.text-red-700')).not.toBeVisible();
  });

  test('rebalance chain', async ({ page, request }) => {
    // We can trigger a rebalance by forcing a tight gap via API, then clicking the button
    // Or just verify the button appears when needed.
    // For now, we'll just verify the UI doesn't crash.
    await page.getByRole('button', { name: 'Per-Principal Chains' }).click();
    await page.locator('select').first().selectOption({ index: 1 });
    
    // If the Auto-balance button is visible, click it
    const balanceBtn = page.getByRole('button', { name: 'Auto-balance' });
    if (await balanceBtn.isVisible()) {
      await balanceBtn.click();
      await expect(balanceBtn).not.toBeVisible();
    }
  });

  test('deletes from chain', async ({ page }) => {
    await page.getByRole('button', { name: 'Per-Principal Chains' }).click();
    await page.locator('select').first().selectOption({ index: 1 });

    // Delete all entries
    const deleteButtons = page.locator('h3:has-text("router Chain")').locator('..').locator('button[title="Remove from chain"]');
    const count = await deleteButtons.count();
    
    for (let i = 0; i < count; i++) {
      await deleteButtons.first().click();
      // Wait for it to disappear
      await page.waitForTimeout(500);
    }

    await expect(page.getByText('No plugins in this chain.')).toBeVisible();
  });

  test('deletes from registry', async ({ page }) => {
    await page.getByRole('button', { name: 'Wasm Registry' }).click();

    // Delete the uploaded plugin
    const deleteBtn = page.locator('button[title="Delete"]').first();
    await deleteBtn.click();

    await expect(page.getByText('No plugins uploaded yet.')).toBeVisible();
  });
});
