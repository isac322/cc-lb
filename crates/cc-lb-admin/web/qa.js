import { chromium } from "playwright";
import fs from "fs";

const routes = [
  "/",
  "/principals",
  "/log",
  "/management",
  "/settings",
  "/upstreams",
  "/activity",
  "/credentials",
  "/plugins"
];

async function run() {
  const browser = await chromium.launch({ executablePath: "/usr/bin/chromium", headless: true });
  const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 } });
  
  const results = [];

  for (const route of routes) {
    const page = await ctx.newPage();
    const errors = [];
    page.on("console", msg => {
      if (msg.type() === "error") {
        const text = msg.text();
        if (!text.includes("React DevTools") && !text.includes("Vite HMR") && !text.includes("Download the React DevTools")) {
          errors.push(text);
        }
      }
    });

    await page.addInitScript(() => localStorage.setItem("cc-lb-admin-token", "dev"));
    
    const url = `http://127.0.0.1:5173${route}?mock=1`;
    console.log(`Navigating to ${url}`);
    await page.goto(url);
    await page.waitForLoadState("networkidle");
    
    const name = route === "/" ? "overview" : route.substring(1);
    const path = `../../../.omo/evidence/dashboard-final-qa/route-${name}.png`;
    await page.screenshot({ path, fullPage: true });
    
    const bodyText = await page.evaluate(() => document.body.innerText);
    const forbiddenKeys = ['"messages":', '"system":', '"tools":', '"tool_use":', '"content":'];
    const foundForbidden = forbiddenKeys.filter(k => bodyText.includes(k));
    
    const stats = fs.statSync(path);
    const sizeKB = Math.round(stats.size / 1024);
    
    results.push({
      route,
      name,
      path,
      sizeKB,
      errors,
      foundForbidden
    });
    
    await page.close();
  }
  
  await browser.close();
  
  console.log(JSON.stringify(results, null, 2));
}

run().catch(console.error);
