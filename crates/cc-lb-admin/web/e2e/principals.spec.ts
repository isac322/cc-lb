import { test, expect } from '@playwright/test';
import { exec } from 'child_process';
import { promisify } from 'util';
import fs from 'fs';

const execAsync = promisify(exec);

test.describe('Principal Management', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/admin?tab=principals');
  });

  test('should list principals', async ({ page }) => {
    await expect(page.getByRole('heading', { name: 'Roster' })).toBeVisible();
    await expect(page.getByRole('button', { name: '+ New principal' })).toBeVisible();
  });

  test('should create a new principal', async ({ page }) => {
    await page.getByRole('button', { name: '+ New principal' }).click();
    await page.getByLabel('Name').fill('alpha');
    await page.getByLabel('Kind').selectOption('machine');
    await page.getByLabel('Allowed Models (comma separated)').fill('claude-3-*');
    await page.getByRole('button', { name: 'Save' }).click();

    await expect(page.getByText('alpha', { exact: true })).toBeVisible();
  });

  test('should edit allowed models inline', async ({ page }) => {
    const row = page.locator('tr', { hasText: 'alpha' });
    const modelsCell = row.locator('td').nth(3); // ID, Name, Kind, Allowed Models
    await modelsCell.hover();
    await modelsCell.click();
    
    const input = page.getByRole('textbox');
    await input.fill('claude-3-*, gpt-4');
    await input.press('Enter');

    await expect(row.getByText('2 models')).toBeVisible();
  });

  test('should edit principal via dialog', async ({ page }) => {
    const row = page.locator('tr', { hasText: 'alpha' });
    await row.getByRole('button', { name: 'Edit' }).click();
    
    await page.getByLabel('Name').fill('alpha-updated');
    await page.getByRole('button', { name: 'Save' }).click();

    await expect(page.getByText('alpha-updated', { exact: true })).toBeVisible();
  });

  test('QA scenario: proxy routing with allowed models', async ({ page }) => {
    // Intercept the create response to get the ID
    const responsePromise = page.waitForResponse(response => 
      response.url().includes('/admin/v1/principals') && response.request().method() === 'POST'
    );

    await page.getByRole('button', { name: '+ New principal' }).click();
    await page.getByLabel('Name').fill('qa-principal');
    await page.getByLabel('Kind').selectOption('machine');
    await page.getByLabel('Allowed Models (comma separated)').fill('claude-3-*');
    await page.getByRole('button', { name: 'Save' }).click();

    const response = await responsePromise;
    const body = await response.json();
    const principalId = body.id;

    expect(principalId).toBeTruthy();

    // Wait for the principal to be visible in the list
    await expect(page.getByText('qa-principal', { exact: true })).toBeVisible();

    // Hit the proxy
    try {
      const { stdout, stderr } = await execAsync(`curl -s -w "\\n%{http_code}" -X POST http://localhost:8080/v1/messages \\
        -H "Authorization: Bearer ${principalId}" \\
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
