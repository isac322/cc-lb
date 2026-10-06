import { mkdir } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';

const root = resolve(import.meta.dir, '..', '..');
const siteRoot = resolve(import.meta.dir, '..');

const assets = [
  ['assets/brand/lockup/cc-lb-lockup-horizontal-on-dark.svg', 'public/brand/lockup/cc-lb-lockup-horizontal-on-dark.svg'],
  ['assets/brand/lockup/cc-lb-lockup-horizontal-on-light.svg', 'public/brand/lockup/cc-lb-lockup-horizontal-on-light.svg'],
  ['assets/brand/mark/cc-lb-mark-on-dark.svg', 'public/brand/mark/cc-lb-mark-on-dark.svg'],
  ['assets/brand/mark/cc-lb-mark-on-light.svg', 'public/brand/mark/cc-lb-mark-on-light.svg'],
  ['assets/brand/favicon/favicon.svg', 'public/brand/favicon/favicon.svg'],
  ['assets/brand/favicon/favicon.ico', 'public/brand/favicon/favicon.ico'],
  ['assets/brand/favicon/apple-touch-icon.png', 'public/brand/favicon/apple-touch-icon.png'],
  ['assets/brand/social/cc-lb-social-preview.png', 'public/brand/social/cc-lb-social-preview.png'],
  ['assets/brand/app-icon/png/cc-lb-app-icon-192.png', 'public/brand/app-icon/cc-lb-app-icon-192.png'],
  ['assets/brand/app-icon/png/cc-lb-app-icon-512.png', 'public/brand/app-icon/cc-lb-app-icon-512.png'],
  ['assets/brand/lockup/cc-lb-lockup-horizontal-on-dark.svg', 'src/assets/cc-lb-lockup-horizontal-on-dark.svg'],
  ['assets/brand/lockup/cc-lb-lockup-horizontal-on-light.svg', 'src/assets/cc-lb-lockup-horizontal-on-light.svg'],
] as const;

for (const [source, target] of assets) {
  const targetPath = resolve(siteRoot, target);
  await mkdir(dirname(targetPath), { recursive: true });
  const bytes = await Bun.file(resolve(root, source)).arrayBuffer();
  await Bun.write(targetPath, bytes);
}
