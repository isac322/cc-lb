#!/usr/bin/env node
// Computes a deterministic SHA-256 cache key over third-party dependencies and
// build toolchain inputs so workspace version bumps do not bust release deps.
import { createHash } from 'node:crypto';
import { existsSync, readdirSync, readFileSync } from 'node:fs';
import path from 'node:path';

function stripInlinePathVersion(line) {
  if (!/\bpath\s*=\s*"/.test(line) || !/\bversion\s*=\s*"/.test(line)) {
    return line;
  }
  return line
    .replace(/,\s*version\s*=\s*"[^"]*"/g, '')
    .replace(/\bversion\s*=\s*"[^"]*"\s*,\s*/g, '')
    .replace(/,\s*\}/g, ' }');
}

export function normalizeRootCargoToml(content) {
  const lines = content.split(/\r?\n/);
  const out = [];
  let section = '';
  for (const line of lines) {
    const secMatch = line.match(/^\s*\[([^\]]+)\]\s*(?:#.*)?$/);
    if (secMatch) {
      section = secMatch[1].trim();
      out.push(line);
      continue;
    }
    if (section === 'workspace.package' && /^\s*version\s*=\s*"[^"]*"\s*(?:#.*)?$/.test(line)) {
      continue;
    }
    out.push(stripInlinePathVersion(line));
  }
  return out.join('\n');
}

export function normalizeCrateCargoToml(content) {
  const lines = content.split(/\r?\n/);
  const out = [];
  let section = '';
  for (const line of lines) {
    const secMatch = line.match(/^\s*\[([^\]]+)\]\s*(?:#.*)?$/);
    if (secMatch) {
      section = secMatch[1].trim();
      out.push(line);
      continue;
    }
    if (section === 'package' && /^\s*version\s*=\s*"[^"]*"\s*(?:#.*)?$/.test(line)) {
      continue;
    }
    out.push(stripInlinePathVersion(line));
  }
  return out.join('\n');
}

export function extractThirdPartyLockPackages(lockContent) {
  const blocks = lockContent.split(/(?=^\[\[package\]\]$)/m);
  const packages = [];
  for (const block of blocks) {
    if (!block.startsWith('[[package]]')) continue;
    const sourceMatch = block.match(/^source\s*=\s*"([^"]*)"/m);
    if (!sourceMatch) continue;
    const nameMatch = block.match(/^name\s*=\s*"([^"]*)"/m);
    const versionMatch = block.match(/^version\s*=\s*"([^"]*)"/m);
    const checksumMatch = block.match(/^checksum\s*=\s*"([^"]*)"/m);
    if (!nameMatch || !versionMatch) continue;
    packages.push({
      name: nameMatch[1],
      version: versionMatch[1],
      source: sourceMatch[1],
      checksum: checksumMatch ? checksumMatch[1] : '',
    });
  }
  packages.sort((a, b) =>
    a.name.localeCompare(b.name) ||
    a.version.localeCompare(b.version) ||
    a.source.localeCompare(b.source) ||
    a.checksum.localeCompare(b.checksum),
  );
  return packages;
}

export function extractDockerfileBaseImages(dockerfileContent) {
  return dockerfileContent
    .split(/\r?\n/)
    .filter((line) => /^\s*FROM\s+.*(?:tonistiigi\/xx:|rust:)/.test(line))
    .join('\n');
}

export function computeCdDepsHash(repoRoot) {
  const h = createHash('sha256');
  const feed = (label, data) => {
    h.update(label);
    h.update('\0');
    h.update(data);
    h.update('\0');
  };

  const lockContent = readFileSync(path.join(repoRoot, 'Cargo.lock'), 'utf8');
  const thirdParty = extractThirdPartyLockPackages(lockContent);
  feed(
    'Cargo.lock#third-party',
    thirdParty.map((p) => `${p.name}=${p.version}@${p.source}#${p.checksum}`).join('\n'),
  );

  const rootToml = readFileSync(path.join(repoRoot, 'Cargo.toml'), 'utf8');
  feed('Cargo.toml#normalized', normalizeRootCargoToml(rootToml));

  const cratesDir = path.join(repoRoot, 'crates');
  const crateNames = readdirSync(cratesDir, { withFileTypes: true })
    .filter((d) => d.isDirectory())
    .map((d) => d.name)
    .sort();
  for (const crateName of crateNames) {
    const manifestPath = path.join(cratesDir, crateName, 'Cargo.toml');
    if (!existsSync(manifestPath)) continue;
    feed(
      `crates/${crateName}/Cargo.toml#normalized`,
      normalizeCrateCargoToml(readFileSync(manifestPath, 'utf8')),
    );
  }

  for (const rel of ['rust-toolchain.toml', '.cargo/config.toml', 'scripts/docker-build-musl.sh']) {
    feed(rel, readFileSync(path.join(repoRoot, rel), 'utf8'));
  }

  const dockerfile = readFileSync(path.join(repoRoot, 'Dockerfile'), 'utf8');
  feed('Dockerfile#base-images', extractDockerfileBaseImages(dockerfile));

  return h.digest('hex');
}

function main() {
  const args = process.argv.slice(2);
  let root = process.cwd();
  for (let i = 0; i < args.length; i += 1) {
    if (args[i] === '--root' && args[i + 1]) {
      root = path.resolve(args[++i]);
    } else {
      console.error('Usage: node scripts/cd-deps-hash.mjs [--root <dir>]');
      process.exit(2);
    }
  }
  process.stdout.write(`${computeCdDepsHash(root)}\n`);
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}
