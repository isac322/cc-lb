import { mkdir, rm } from 'node:fs/promises';
import { resolve } from 'node:path';

const root = resolve(import.meta.dir, '..', '..');
const sourceDir = resolve(root, 'assets/media');
const targetDir = resolve(import.meta.dir, '..', 'public/media');

// Fixed allowlist: only these product screenshots are published with the site.
const stills = ['upstreams.png', 'usage.png', 'access.png', 'principal-detail.png', 'logs.png'] as const;

await rm(targetDir, { recursive: true, force: true });
await mkdir(targetDir, { recursive: true });

for (const name of stills) {
  const source = Bun.file(resolve(sourceDir, name));
  if (!(await source.exists())) {
    throw new Error(`missing media source: assets/media/${name}`);
  }
  await Bun.write(resolve(targetDir, name), source);
}
