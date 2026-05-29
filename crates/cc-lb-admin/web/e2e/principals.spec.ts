import { test, expect } from '@playwright/test';
import { exec } from 'child_process';
import { promisify } from 'util';
import fs from 'fs';

const execAsync = promisify(exec);

test.describe.serial('Principal Management', () => {
  test.beforeEach(async ({ page }) => {
    await page.addInitScript(() => {
      localStorage.setItem('cc-lb-admin-token', 'test-admin-token');
    });
    await page.goto('/management');
    await page.waitForLoadState('networkidle');
  });

  test('should list principals', async ({ page }) => {
    await expect(page.getByRole('heading', { name: 'Roster' })).toBeVisible();
    await expect(page.getByRole('button', { name: '+ New principal' })).toBeVisible();
  });

  test('should create a new principal', async ({ page }) => {
    await page.getByRole('button', { name: '+ New principal' }).click();
    await page.locator('input[type="text"]').first().fill('alpha');
    await page.getByPlaceholder('e.g. claude-3-5-sonnet').fill('claude-3-*');
    await page.getByRole('button', { name: 'Add' }).click();
    await page.getByRole('button', { name: 'Save to Draft' }).click();

    await page.getByRole('link', { name: /Review & apply/ }).click();
    await page.getByRole('button', { name: 'Validate' }).click();
    await page.getByRole('button', { name: 'Apply' }).click();
    await page.getByRole('button', { name: 'Confirm Apply' }).click();

    await page.goto('/management');
    await expect(page.getByText('alpha', { exact: true })).toBeVisible();
  });

  test('should edit allowed models inline', async ({ page }) => {
    await expect(page.getByText('alpha', { exact: true })).toBeVisible();
    const row = page.locator('tr', { hasText: 'alpha' });
    await row.getByRole('button', { name: 'Edit' }).click();

    await page.getByPlaceholder('e.g. claude-3-5-sonnet').fill('gpt-4');
    await page.getByRole('button', { name: 'Add' }).click();
    await page.getByRole('button', { name: 'Save to Draft' }).click();

    await page.getByRole('link', { name: /Review & apply/ }).click();
    await page.getByRole('button', { name: 'Validate' }).click();
    await page.getByRole('button', { name: 'Apply' }).click();
    await page.getByRole('button', { name: 'Confirm Apply' }).click();

    await page.goto('/management');
    const updatedRow = page.locator('tr', { hasText: 'alpha' });
    await expect(updatedRow.getByText('2 models')).toBeVisible();
  });

  test('should edit principal via dialog', async ({ page }) => {
    const row = page.locator('tr', { hasText: 'alpha' });
    await row.getByRole('button', { name: 'Edit' }).click();

    await page.locator('input[placeholder="Default"]').first().fill('500');
    await page.getByRole('button', { name: 'Apply Override' }).click();

    await expect(page.getByText('Live override applied')).toBeVisible();
    await page.getByRole('button', { name: 'Cancel' }).click();
  });

  test('QA scenario: proxy routing with allowed models', async ({ page }) => {
    const responsePromise = page.waitForResponse(response =>
      response.url().includes('/admin/principals') && response.request().method() === 'POST'
    );

    await page.getByRole('button', { name: '+ New principal' }).click();
    await page.locator('input[type="text"]').first().fill('qa-principal');
    await page.getByPlaceholder('e.g. claude-3-5-sonnet').fill('claude-3-*');
    await page.getByRole('button', { name: 'Add' }).click();
    await page.getByRole('button', { name: 'Save to Draft' }).click();

    const response = await responsePromise;
    const body = await response.json();
    const principalId = body.principal_id;

    expect(principalId).toBeTruthy();

    await page.getByRole('link', { name: /Review & apply/ }).click();
    await page.getByRole('button', { name: 'Validate' }).click();
    await page.getByRole('button', { name: 'Apply' }).click();
    await page.getByRole('button', { name: 'Confirm Apply' }).click();

    await page.goto('/management');
    await expect(page.getByText('qa-principal', { exact: true })).toBeVisible();

    const row = page.locator('tr', { hasText: 'qa-principal' });
    await row.getByRole('button', { name: 'Keys' }).click();
    await page.getByRole('button', { name: 'Issue New Key' }).click();
    await page.getByRole('button', { name: 'Issue Key' }).click();

    const keyInput = page.locator('input[readonly]');
    await expect(keyInput).toBeVisible();
    const apiKey = await keyInput.inputValue();
    console.log('API KEY:', apiKey);

    await page.getByRole('button', { name: 'Dismiss' }).click();
    await page.keyboard.press('Escape'); // Close the Keys modal

    // Hit the proxy
    try {
      const { stdout, stderr } = await execAsync(`curl -s -w "\\n%{http_code}" -X POST http://localhost:8080/v1/messages \\
        -H "x-api-key: ${apiKey}" \\
        -H "Content-Type: application/json" \\
        -d '{"model": "claude-3-haiku-20240307", "messages": [{"role": "user", "content": "hello"}]}'`);

      fs.mkdirSync('../../.omo/evidence', { recursive: true });
      fs.writeFileSync('../../.omo/evidence/task-29-route.txt', stdout + '\n' + stderr);

      const lines = stdout.trim().split('\n');
      const statusCode = lines[lines.length - 1];

      console.log('Curl output:', stdout);
      // We expect 200 if the proxy is running and upstream is configured.
      // If not, we at least verify the request was made.
      // The task says "assert 200".
      expect(statusCode).toBe('200');
    } catch (e) {
      console.error('Curl failed', e);
      // If curl fails completely (e.g. connection refused), we still want to capture it.
      fs.mkdirSync('../../.omo/evidence', { recursive: true });
      fs.writeFileSync('../../.omo/evidence/task-29-route.txt', String(e));
      throw e;
    }
  });
});
