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
    const pluginList = page.locator('ul').filter({ hasText: 'Terminal' });
    await expect(pluginList).toBeVisible();
    await page.screenshot({ path: path.join(evidenceDir, 'task-31-step2.png') });

    // Step 3: terminal row locked - no drag handle
    const terminalRow = pluginList.locator('li').filter({ hasText: 'Terminal' });
    await expect(terminalRow).toBeVisible();
    await expect(terminalRow.locator('button[aria-label="Drag to reorder"]')).toHaveCount(0);
    await page.screenshot({ path: path.join(evidenceDir, 'task-31-step3.png') });

    // Step 4: add second router plugin
    const addButton = page.locator('button:has-text("Add")').first();
    await addButton.click();
    const modal = page.locator('[role="dialog"]');
    await expect(modal).toBeVisible();
    const select = modal.locator('select');
    await select.selectOption({ index: 1 });
    await modal.locator('button:has-text("Add")').click();
    await expect(modal).not.toBeVisible();
    await expect(pluginList.locator('li:has(button[aria-label="Drag to reorder"])')).toHaveCount(3);
    await page.screenshot({ path: path.join(evidenceDir, 'task-31-step4.png') });

    // Step 5: drag reorder
    const dragHandles = pluginList.locator('button[aria-label="Drag to reorder"]');
    const first = dragHandles.nth(0);
    const second = dragHandles.nth(1);
    
    const firstItemText = await pluginList.locator('li:has(button[aria-label="Drag to reorder"])').nth(0).locator('span.truncate').textContent();
    
    const firstBox = await first.boundingBox();
    const secondBox = await second.boundingBox();
    if (firstBox && secondBox) {
      const startX = firstBox.x + firstBox.width / 2;
      const startY = firstBox.y + firstBox.height / 2;
      await page.mouse.move(startX, startY);
      await page.mouse.down();
      // dnd-kit PointerSensor activates after an 8px drag; nudge past the threshold first
      await page.mouse.move(startX, startY + 12, { steps: 4 });
      await page.mouse.move(secondBox.x + secondBox.width / 2, secondBox.y + secondBox.height / 2 + 10, { steps: 10 });
      await page.mouse.up();
    }
    
    // Verify order changed
    await expect.poll(async () => {
      const newSecondItemText = await pluginList.locator('li:has(button[aria-label="Drag to reorder"])').nth(1).locator('span.truncate').textContent();
      return newSecondItemText === firstItemText;
    }).toBeTruthy();
    await page.screenshot({ path: path.join(evidenceDir, 'task-31-step5.png') });

    // Step 6: terminal strategy change to Random + reload + persistence
    const terminalSelect = page.locator('select[aria-label="Terminal strategy"]');
    await expect(terminalSelect).toBeVisible();
    
    const responsePromise = page.waitForResponse(response => 
      response.url().includes('/router-terminal') && response.request().method() === 'PUT'
    );
    await terminalSelect.selectOption('random');
    await responsePromise;
    
    await page.reload();
    await expect(page.locator('text=Plugin Chain')).toBeVisible();
    await expect(page.locator('select[aria-label="Terminal strategy"]')).toHaveValue('random');
    await page.screenshot({ path: path.join(evidenceDir, 'task-31-step6.png') });

    // Step 7: logs page navigate
    await page.goto('/logs');
    await expect(page.locator('text=Live Logs')).toBeVisible();
    await page.screenshot({ path: path.join(evidenceDir, 'task-31-step7.png') });
    
    // Step 8: hover upstream cell → routing_trace panel
    const upstreamCell = page.locator('td').filter({ hasText: /anthropic|oauth/ }).locator('.cursor-help').first();
    await expect(upstreamCell).toBeVisible();
    await upstreamCell.hover();
    
    const tooltip = page.locator('[role="tooltip"]');
    await expect(tooltip).toBeVisible();
    await expect(tooltip).toContainText('Routing Trace');
    await expect(tooltip).toContainText('Terminal');
    await page.screenshot({ path: path.join(evidenceDir, 'task-31-step8.png') });

    // Step 9: hover status cell → internal_errors panel
    // We need to find a status cell with internal errors.
    // The mock server generates internal errors for some events.
    const statusCell = page.locator('td').filter({ hasText: /^(200|429|500)$/ }).locator('.cursor-help').first();
    await expect(statusCell).toBeVisible();
    await statusCell.hover();
    
    await expect(tooltip).toBeVisible();
    await expect(tooltip).toContainText('Internal Errors');
    await page.screenshot({ path: path.join(evidenceDir, 'task-31-step9.png') });
  });
});
