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
      await page.waitForSelector('text=Principals');
    }

    // Click on the first principal
    await page.locator('button:has-text("admin")').first().click();

    // Wait for Plugin Chain card
    await expect(page.locator('text=Plugin Chain')).toBeVisible();

    // Take screenshot 1
    await page.screenshot({ path: path.join(evidenceDir, 'task-31-step1.png') });

    // Terminal toggle
    const terminalSelect = page.locator('select[aria-label="Terminal strategy"]');
    await expect(terminalSelect).toBeVisible();
    
    const responsePromise = page.waitForResponse(response => 
      response.url().includes('/router-terminal') && response.request().method() === 'PUT'
    );
    await terminalSelect.selectOption('random');
    await responsePromise;
    
    // Verify persistence by reloading
    await page.reload();
    await expect(page.locator('text=Plugin Chain')).toBeVisible();
    await expect(page.locator('select[aria-label="Terminal strategy"]')).toHaveValue('random');
    await page.screenshot({ path: path.join(evidenceDir, 'task-31-step2.png') });

    // Drag reorder
    const dragHandles = page.locator('button[aria-label="Drag to reorder"]');
    // We need at least 2 items to reorder
    if (await dragHandles.count() >= 2) {
      const first = dragHandles.nth(0);
      const second = dragHandles.nth(1);
      
      // Get initial text of the first item
      const firstItemText = await page.locator('li:has(button[aria-label="Drag to reorder"])').nth(0).locator('span.truncate').textContent();
      
      const firstBox = await first.boundingBox();
      const secondBox = await second.boundingBox();
      if (firstBox && secondBox) {
        await page.mouse.move(firstBox.x + firstBox.width / 2, firstBox.y + firstBox.height / 2);
        await page.mouse.down();
        await page.mouse.move(secondBox.x + secondBox.width / 2, secondBox.y + secondBox.height / 2 + 20, { steps: 10 });
        await page.mouse.up();
      }
      
      // Verify order changed
      await expect.poll(async () => {
        const newSecondItemText = await page.locator('li:has(button[aria-label="Drag to reorder"])').nth(1).locator('span.truncate').textContent();
        return newSecondItemText === firstItemText;
      }).toBeTruthy();
      
      await page.screenshot({ path: path.join(evidenceDir, 'task-31-step4.png') });
    }

    // Hover panels in Logs page
    await page.goto('/logs');
    await expect(page.locator('text=Request Logs')).toBeVisible();
    
    // Find an upstream cell with a routing trace
    const upstreamCell = page.locator('td .cursor-help').first();
    await expect(upstreamCell).toBeVisible();
    await upstreamCell.hover();
    
    // Verify hover panel content
    const tooltip = page.locator('[role="tooltip"]');
    await expect(tooltip).toBeVisible();
    await expect(tooltip).toContainText('Routing Trace');
    await expect(tooltip).toContainText('Terminal');
    await page.screenshot({ path: path.join(evidenceDir, 'task-31-step3.png') });
  });
});
