import { test, expect } from '@playwright/test';
import fs from 'fs';
import path from 'path';

test.describe('Router Pipeline', () => {
  test('drag reorder, terminal toggle, and hover panels', async ({ page }) => {
    // Ensure evidence directory exists
    const evidenceDir = path.join(process.cwd(), '../../../.omo/evidence');
    if (!fs.existsSync(evidenceDir)) {
      fs.mkdirSync(evidenceDir, { recursive: true });
    }

    // Go to principals page
    await page.goto('/principals');
    
    // Wait for either the auth gate or the principals page
    const isAuthRequired = await Promise.race([
      page.waitForSelector('text=Admin token required').then(() => true),
      page.waitForSelector('text=Principals').then(() => false)
    ]);

    // Fill auth token if required
    if (isAuthRequired) {
      await page.locator('input[type="password"]').fill('mock-token');
      await page.locator('button:has-text("Sign in")').click();
      await expect(page.locator('h1', { hasText: 'Principals' })).toBeVisible();
    }

    // Step 1: principal load
    await page.locator('button:has-text("admin")').first().click();
    await expect(page.locator('text=Plugin Chain')).toBeVisible();
    await page.screenshot({ path: path.join(evidenceDir, 'task-31-step1.png') });

    // Step 2: router slot list rendered
    const pluginList = page.locator('ul').first();
    await expect(pluginList).toBeVisible();
    await page.screenshot({ path: path.join(evidenceDir, 'task-31-step2.png') });

    // Step 3: terminal row locked - no drag handle
    const terminalRow = page.locator('div.bg-overlay-1').filter({ hasText: 'Terminal step' }).first();
    await expect(terminalRow).toBeVisible();
    await expect(terminalRow.locator('button[aria-label="Drag to reorder"]')).toHaveCount(0);
    await page.screenshot({ path: path.join(evidenceDir, 'task-31-step3.png') });

    // Step 4: add second router plugin
    await page.locator('button[role="tab"]', { hasText: 'Advanced' }).click();
    const initialCount = await pluginList.locator('li[data-key]:not([data-key^="connector-"])').filter({ hasNotText: 'Add filter' }).count();
    await page.locator('text=Add filter').click();
    await page.locator('.absolute.w-64 button', { hasText: 'canary-router' }).first().click();
    await expect(pluginList.locator('li[data-key]:not([data-key^="connector-"])').filter({ hasNotText: 'Add filter' })).toHaveCount(initialCount + 1);
    await page.screenshot({ path: path.join(evidenceDir, 'task-31-step4.png') });

    // Step 5: reorder via down button
    const firstItemText = await pluginList.locator('li[data-key]:not([data-key^="connector-"])').nth(0).locator('button.hover\\:underline').textContent();
    
    // Click the down button on the first item
    const downButton = pluginList.locator('li[data-key]:not([data-key^="connector-"])').nth(0).locator('button').nth(2); // 0 is name, 1 is up, 2 is down
    await downButton.click();
    
    // Verify order changed
    await expect.poll(async () => {
      const newSecondItemText = await pluginList.locator('li[data-key]:not([data-key^="connector-"])').nth(1).locator('button.hover\\:underline').textContent();
      return newSecondItemText === firstItemText;
    }).toBeTruthy();
    

    
    await page.screenshot({ path: path.join(evidenceDir, 'task-31-step5.png') });

    const infoButton = pluginList.locator('li').filter({ hasText: 'cache-affinity' }).first().locator('button.hover\\:underline');
    await infoButton.click();
    
    const drawer = page.locator('[role="dialog"]');
    await expect(drawer).toBeVisible();
    await expect(drawer.locator('[data-testid="plugin-purpose"]')).toBeVisible();
    await expect(drawer.locator('[data-testid="plugin-keeps"]')).toBeVisible();
    await expect(drawer.locator('[data-testid="plugin-drops"]')).toBeVisible();
    await expect(drawer.locator('[data-testid="plugin-empty-behavior"]')).toBeVisible();
    await expect(drawer.locator('[data-testid="plugin-examples"]')).toBeVisible();
    
    await drawer.locator('button[aria-label="Close"]').click();
    await expect(drawer).not.toBeVisible();

    // Step 6: terminal strategy change to Random + reload + persistence
    const termRandomLabel = page.locator('label', { hasText: 'Random' }).last();
    await expect(termRandomLabel).toBeVisible();
    
    const responsePromise = page.waitForResponse(response => 
      response.url().includes('/router-terminal') && response.request().method() === 'PUT'
    );
    await termRandomLabel.click();
    await responsePromise;
    
    await page.reload();
    await expect(page.locator('text=Plugin Chain')).toBeVisible();
    await page.locator('button[role="tab"]', { hasText: 'Advanced' }).click();
    await expect(page.locator('input[name="term-strategy"][value="random"]')).toBeChecked();
    await page.screenshot({ path: path.join(evidenceDir, 'task-31-step6.png') });
  });

  test('complex chain auto-opens Advanced with disabled Basic', async ({ page }) => {
    await page.goto('/principals');
    
    const isAuthRequired = await Promise.race([
      page.waitForSelector('text=Admin token required').then(() => true),
      page.waitForSelector('text=Principals').then(() => false)
    ]);

    if (isAuthRequired) {
      await page.locator('input[type="password"]').fill('mock-token');
      await page.locator('button:has-text("Sign in")').click();
      await expect(page.locator('h1', { hasText: 'Principals' })).toBeVisible();
    }

    // Use engineering-shared which has a complex chain in mock server
    await page.locator('button:has-text("engineering-shared")').first().click();
    await expect(page.locator('text=Plugin Chain')).toBeVisible();

    // Basic tab should be disabled
    const basicTab = page.locator('button[role="tab"]', { hasText: 'Basic' });
    await expect(basicTab).toHaveAttribute('aria-disabled', 'true');

    // Advanced tab should be selected
    const advancedTab = page.locator('button[role="tab"]', { hasText: 'Advanced' });
    await expect(advancedTab).toHaveAttribute('aria-selected', 'true');

    // Click basic tab should show notice
    await basicTab.click({ force: true });
    await expect(page.locator('text=Basic can\'t show this chain without losing the extra filters').first()).toBeVisible();
  });

  test('dashed placeholder opens picker and adds filter', async ({ page }) => {
    await page.goto('/principals');
    
    const isAuthRequired = await Promise.race([
      page.waitForSelector('text=Admin token required').then(() => true),
      page.waitForSelector('text=Principals').then(() => false)
    ]);

    if (isAuthRequired) {
      await page.locator('input[type="password"]').fill('mock-token');
      await page.locator('button:has-text("Sign in")').click();
      await expect(page.locator('h1', { hasText: 'Principals' })).toBeVisible();
    }

    await page.locator('button:has-text("admin")').first().click();
    await expect(page.locator('text=Plugin Chain')).toBeVisible();

    // Switch to Advanced tab
    await page.locator('button[role="tab"]', { hasText: 'Advanced' }).click();

    // Click placeholder
    await page.locator('text=Add filter').click();

    // Picker should open
    const picker = page.locator('.absolute.w-64 button:not([disabled])').first();
    await expect(picker).toBeVisible();
    const pluginName = await picker.locator('span.font-medium').textContent();

    // Click an enabled entry
    await picker.click();

    // Pipeline gains the new entry
    await expect(page.locator('li', { hasText: pluginName! }).first()).toBeVisible();

    // Click placeholder again
    await page.locator('text=Add filter').click();

    // Entry should be disabled
    const disabledPicker = page.locator('.absolute.w-64 button', { hasText: pluginName! }).first();
    await expect(disabledPicker).toBeDisabled();
    await expect(disabledPicker).toContainText('Already in chain');
  });

  test('terminal radio cards reflect strategy change', async ({ page }) => {
    await page.goto('/principals');
    
    const isAuthRequired = await Promise.race([
      page.waitForSelector('text=Admin token required').then(() => true),
      page.waitForSelector('text=Principals').then(() => false)
    ]);

    if (isAuthRequired) {
      await page.locator('input[type="password"]').fill('mock-token');
      await page.locator('button:has-text("Sign in")').click();
      await expect(page.locator('h1', { hasText: 'Principals' })).toBeVisible();
    }

    await page.locator('button:has-text("admin")').first().click();
    await expect(page.locator('text=Plugin Chain')).toBeVisible();

    // Switch to Advanced tab
    await page.locator('button[role="tab"]', { hasText: 'Advanced' }).click();

    // In Advanced tab
    const termRandomLabel = page.locator('label', { hasText: 'Random' }).last();
    await termRandomLabel.click();

    // Verify it's checked
    const termRandomRadio = page.locator('input[name="term-strategy"][value="random"]');
    await expect(termRandomRadio).toBeChecked();
  });
});
