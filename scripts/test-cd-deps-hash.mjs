import assert from 'node:assert/strict';
import { cpSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import {
  computeCdDepsHash,
  normalizeCrateCargoToml,
  normalizeRootCargoToml,
} from './cd-deps-hash.mjs';

function makeFixtureRepo() {
  const root = mkdtempSync(path.join(os.tmpdir(), 'cd-deps-hash-'));
  mkdirSync(path.join(root, 'crates', 'cc-lb-domain'), { recursive: true });
  mkdirSync(path.join(root, 'crates', 'cc-lb-server'), { recursive: true });
  mkdirSync(path.join(root, '.cargo'), { recursive: true });
  mkdirSync(path.join(root, 'scripts'), { recursive: true });

  writeFileSync(
    path.join(root, 'Cargo.toml'),
    [
      '[workspace]',
      'members = ["crates/cc-lb-*"]',
      '',
      '[workspace.package]',
      'version = "2.2.1"',
      'edition = "2024"',
      '',
      '[workspace.dependencies]',
      'cc-lb-domain = { path = "crates/cc-lb-domain", version = "2.2.1" }',
      'cc-lb-plugin-wire = { version = "0.9", path = "crates/cc-lb-plugin-wire" }',
      'anyhow = "1"',
      'wasmtime = { version = "50.0.2", default-features = false, features = ["cranelift"] }',
      '',
      '[profile.release]',
      'lto = "fat"',
      'codegen-units = 1',
      '',
    ].join('\n'),
  );

  writeFileSync(
    path.join(root, 'crates', 'cc-lb-domain', 'Cargo.toml'),
    ['[package]', 'name = "cc-lb-domain"', 'version.workspace = true', ''].join('\n'),
  );

  writeFileSync(
    path.join(root, 'crates', 'cc-lb-server', 'Cargo.toml'),
    [
      '[package]',
      'name = "cc-lb-server"',
      'version = "2.2.1"',
      '',
      '[dependencies]',
      'cc-lb-domain = { path = "../cc-lb-domain", version = "2.2.1" }',
      'anyhow = { workspace = true }',
      '',
    ].join('\n'),
  );

  writeFileSync(
    path.join(root, 'Cargo.lock'),
    [
      'version = 4',
      '',
      '[[package]]',
      'name = "anyhow"',
      'version = "1.0.104"',
      'source = "registry+https://github.com/rust-lang/crates.io-index"',
      'checksum = "aaaa"',
      '',
      '[[package]]',
      'name = "cc-lb-domain"',
      'version = "2.2.1"',
      '',
      '[[package]]',
      'name = "cc-lb-server"',
      'version = "2.2.1"',
      '',
    ].join('\n'),
  );

  writeFileSync(path.join(root, 'rust-toolchain.toml'), '[toolchain]\nchannel = "1.99.1"\n');
  writeFileSync(path.join(root, '.cargo', 'config.toml'), '[target.x86_64-unknown-linux-musl]\nlinker = "rust-lld"\n');
  writeFileSync(path.join(root, 'scripts', 'docker-build-musl.sh'), '#!/usr/bin/env bash\necho build\n');
  writeFileSync(
    path.join(root, 'Dockerfile'),
    [
      'FROM --platform=$BUILDPLATFORM tonistiigi/xx:1.10.0 AS xx',
      'FROM --platform=$BUILDPLATFORM oven/bun:1.4.4-alpine AS admin-spa',
      'FROM --platform=$BUILDPLATFORM rust:1.99.1-alpine AS builder',
      '',
    ].join('\n'),
  );
  return root;
}

test('computeCdDepsHash is invariant to workspace version bumps', () => {
  const root = makeFixtureRepo();
  try {
    const before = computeCdDepsHash(root);
    assert.match(before, /^[0-9a-f]{64}$/);

    // Bump workspace version 2.2.1 -> 2.2.2 across Cargo.toml, crate manifests, and Cargo.lock.
    for (const rel of ['Cargo.toml', 'crates/cc-lb-server/Cargo.toml', 'Cargo.lock']) {
      const p = path.join(root, rel);
      writeFileSync(p, readFileSync(p, 'utf8').replaceAll('2.2.1', '2.2.2'));
    }
    assert.equal(computeCdDepsHash(root), before);

    // Non-base lines in Dockerfile (like comments or distroless labels) also stay invariant.
    writeFileSync(
      path.join(root, 'Dockerfile'),
      `${readFileSync(path.join(root, 'Dockerfile'), 'utf8')}# harmless comment\n`,
    );
    assert.equal(computeCdDepsHash(root), before);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test('computeCdDepsHash changes on third-party dep, feature, profile, toolchain, or build script changes', () => {
  const root = makeFixtureRepo();
  try {
    const base = computeCdDepsHash(root);

    const mutate = (fn) => {
      const copy = `${root}-mut`;
      cpSync(root, copy, { recursive: true });
      try {
        fn(copy);
        assert.notEqual(computeCdDepsHash(copy), base);
      } finally {
        rmSync(copy, { recursive: true, force: true });
      }
    };

    mutate((dir) => {
      const p = path.join(dir, 'Cargo.lock');
      writeFileSync(p, readFileSync(p, 'utf8').replace('1.0.104', '1.0.105'));
    });
    mutate((dir) => {
      const p = path.join(dir, 'Cargo.toml');
      writeFileSync(p, readFileSync(p, 'utf8').replace('["cranelift"]', '["cranelift", "wat"]'));
    });
    mutate((dir) => {
      const p = path.join(dir, 'Cargo.toml');
      writeFileSync(p, readFileSync(p, 'utf8').replace('codegen-units = 1', 'codegen-units = 16'));
    });
    mutate((dir) => {
      const p = path.join(dir, 'crates', 'cc-lb-server', 'Cargo.toml');
      writeFileSync(p, readFileSync(p, 'utf8').replace('anyhow = { workspace = true }', 'anyhow = { workspace = true }\nextra = "1"'));
    });
    mutate((dir) => {
      writeFileSync(path.join(dir, 'rust-toolchain.toml'), '[toolchain]\nchannel = "1.99.2"\n');
    });
    mutate((dir) => {
      writeFileSync(path.join(dir, 'scripts', 'docker-build-musl.sh'), '#!/usr/bin/env bash\necho changed\n');
    });
    mutate((dir) => {
      const p = path.join(dir, 'Dockerfile');
      writeFileSync(p, readFileSync(p, 'utf8').replace('1.99.1-alpine', '1.99.2-alpine'));
    });
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test('normalizers preserve external crate version constraints and target-specific sections', () => {
  const rootNorm = normalizeRootCargoToml(
    '[workspace.package]\nversion = "2.2.1"\n[workspace.dependencies]\nfoo = { path = "crates/foo", version = "2.2.1" }\nbar = { version = "1.2.3" }\n',
  );
  assert.doesNotMatch(rootNorm, /2\.2\.1/);
  assert.match(rootNorm, /bar = \{ version = "1\.2\.3" \}/);

  const crateNorm = normalizeCrateCargoToml(
    '[package]\nname = "x"\nversion = "0.3.0"\n[dependencies]\nanyhow = { version = "1.0" }\ncc-lb-aead = { path = "../cc-lb-aead", version = "2.2.1" }\n',
  );
  assert.doesNotMatch(crateNorm, /0\.3\.0/);
  assert.doesNotMatch(crateNorm, /2\.2\.1/);
  assert.match(crateNorm, /anyhow = \{ version = "1\.0" \}/);
});
