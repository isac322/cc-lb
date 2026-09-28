#!/usr/bin/env node
// Render an SVG to a pixel-exact PNG via headless Chromium.
//
//   node render_png.mjs <in.svg> <out.png> <w> <h> [--scheme=dark|light]
//
// - Transparent background (omitBackground).
// - deviceScaleFactor 1: the PNG is exactly <w>x<h> pixels.
// - Playwright is resolved from crates/cc-lb-admin/web/node_modules.
// - --scheme emulates prefers-color-scheme (for assets like favicon.svg
//   that adapt via @media queries). Default: light.

import { createRequire } from 'node:module';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const here = path.dirname(fileURLToPath(import.meta.url));
const require = createRequire(
  path.join(here, '../../../crates/cc-lb-admin/web/package.json'),
);
const { chromium } = require('playwright');

const args = process.argv.slice(2);
const flags = args.filter((a) => a.startsWith('--'));
const pos = args.filter((a) => !a.startsWith('--'));
if (pos.length !== 4) {
  console.error('usage: node render_png.mjs <in.svg> <out.png> <w> <h> [--scheme=dark|light]');
  process.exit(2);
}
const [inSvg, outPng, wStr, hStr] = pos;
const w = Number(wStr);
const h = Number(hStr);
if (!Number.isInteger(w) || !Number.isInteger(h) || w <= 0 || h <= 0) {
  console.error(`invalid size ${wStr}x${hStr}`);
  process.exit(2);
}
const schemeFlag = flags.find((f) => f.startsWith('--scheme='));
const colorScheme = schemeFlag ? schemeFlag.split('=')[1] : 'light';
if (!['dark', 'light'].includes(colorScheme)) {
  console.error(`invalid --scheme=${colorScheme}`);
  process.exit(2);
}

// The SVG is inlined into a tiny page so it rasterises at exactly w×h
// (a bare file:// URL lets Chromium auto-scale the document SVG instead).
let markup = readFileSync(inSvg, 'utf8');
markup = markup.replace(
  /<svg\b/,
  `<svg style="width:${w}px;height:${h}px;display:block"`,
);

const browser = await chromium.launch({
  args: ['--force-color-profile=srgb'],
});
try {
  const page = await browser.newPage({
    viewport: { width: w, height: h },
    deviceScaleFactor: 1,
    colorScheme,
  });
  await page.setContent(
    `<!doctype html><html><body style="margin:0;padding:0">${markup}</body></html>`,
  );
  await page.screenshot({ path: outPng, omitBackground: true });
} finally {
  await browser.close();
}
console.log(`${outPng}  ${w}x${h}`);
