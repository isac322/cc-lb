import { chromium } from 'playwright';

async function run() {
  const browser = await chromium.launch({ executablePath: '/usr/bin/chromium' });
  const page = await browser.newPage();
  
  await page.goto('http://localhost:5173/admin/principals?mock=1');
  await page.waitForTimeout(2000);
  
  const text = await page.evaluate(() => document.body.innerText.toLowerCase());
  
  const forbidden = ['messages', 'system', 'tools', 'tool_use', 'content'];
  const found = forbidden.filter(word => text.includes(word));
  
  if (found.length > 0) {
    console.error('Forbidden words found:', found);
    process.exit(1);
  } else {
    console.log('No forbidden words found.');
  }

  await browser.close();
}

run().catch(console.error);
