import assert from 'node:assert/strict';
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { checkFeatureParity } from './check-cov-feature-parity.mjs';

const TARGET = 'x86_64-unknown-linux-gnu';

function writeCrate(rootDir, relDir, cargoToml, extraFiles = {}) {
  const crateDir = path.join(rootDir, relDir);
  mkdirSync(path.join(crateDir, 'src'), { recursive: true });
  writeFileSync(path.join(crateDir, 'Cargo.toml'), cargoToml, 'utf8');
  writeFileSync(path.join(crateDir, 'src/lib.rs'), '', 'utf8');
  for (const [relFile, content] of Object.entries(extraFiles)) {
    const abs = path.join(crateDir, relFile);
    mkdirSync(path.dirname(abs), { recursive: true });
    writeFileSync(abs, content, 'utf8');
  }
}

function createTempWorkspace(setupFn) {
  const rootDir = mkdtempSync(path.join(os.tmpdir(), 'cc-lb-cov-parity-'));
  setupFn(rootDir);
  return rootDir;
}

test('checkFeatureParity passes when all target and host compilation units match seed', async () => {
  const rootDir = createTempWorkspace((dir) => {
    writeFileSync(
      path.join(dir, 'Cargo.toml'),
      '[workspace]\nresolver = "2"\nmembers = ["app", "helper", "other"]\n',
      'utf8',
    );
    writeCrate(
      dir,
      'helper',
      '[package]\nname = "helper"\nversion = "0.1.0"\nedition = "2021"\n\n[features]\ntrace = []\n',
    );
    writeCrate(
      dir,
      'other',
      '[package]\nname = "other"\nversion = "0.1.0"\nedition = "2021"\n\n[features]\nextra = []\n\n[dependencies]\nhelper = { path = "../helper", features = ["trace"] }\n',
    );
    writeCrate(
      dir,
      'app',
      '[package]\nname = "app"\nversion = "0.1.0"\nedition = "2021"\n\n[dependencies]\nhelper = { path = "../helper" }\n',
    );
  });

  try {
    const config = {
      excluded: [],
      partitions: [
        { name: 'p-app', runs: [{ packages: ['app'], extra_features: ['helper/trace'] }] },
        { name: 'p-rest', runs: [{ packages: ['helper', 'other'], extra_features: [] }] },
      ],
    };
    const res = await checkFeatureParity({ rootDir, config, target: TARGET, locked: false });
    assert.deepEqual(res.errors, []);
  } finally {
    rmSync(rootDir, { recursive: true, force: true });
  }
});

test('checkFeatureParity detects Resolver 2 platform-specific cfg edge mismatch vs seed', async () => {
  // Reproduces haechibot fixture #1: Linux shard has normal dep `helper` (no features)
  // plus `[target.'cfg(windows)'.dependencies]` enabling `helper/trace`. On Linux,
  // `helper` lacks `trace`, whereas `other` in seed enables `helper/trace` on Linux.
  const rootDir = createTempWorkspace((dir) => {
    writeFileSync(
      path.join(dir, 'Cargo.toml'),
      '[workspace]\nresolver = "2"\nmembers = ["app", "helper", "other"]\n',
      'utf8',
    );
    writeCrate(
      dir,
      'helper',
      '[package]\nname = "helper"\nversion = "0.1.0"\nedition = "2021"\n\n[features]\ntrace = []\n',
    );
    writeCrate(
      dir,
      'other',
      '[package]\nname = "other"\nversion = "0.1.0"\nedition = "2021"\n\n[dependencies]\nhelper = { path = "../helper", features = ["trace"] }\n',
    );
    writeCrate(
      dir,
      'app',
      `[package]\nname = "app"\nversion = "0.1.0"\nedition = "2021"\n\n[dependencies]\nhelper = { path = "../helper" }\n\n[target.'cfg(windows)'.dependencies]\nhelper = { path = "../helper", features = ["trace"] }\n`,
    );
  });

  try {
    const config = {
      excluded: [],
      partitions: [
        { name: 'p-linux-app', runs: [{ packages: ['app'], extra_features: [] }] },
        { name: 'p-rest', runs: [{ packages: ['helper', 'other'], extra_features: [] }] },
      ],
    };
    const res = await checkFeatureParity({ rootDir, config, target: TARGET, locked: false });
    assert.ok(
      res.errors.some(
        (e) =>
          e.includes("partition 'p-linux-app'") &&
          e.includes('[target]') &&
          e.includes("'helper'"),
      ),
      `Expected target unit mismatch for helper, got: ${JSON.stringify(res.errors)}`,
    );
  } finally {
    rmSync(rootDir, { recursive: true, force: true });
  }
});

test('checkFeatureParity keeps host build-dependencies isolated so they do not mask target drift', async () => {
  // Reproduces haechibot fixture #2: `app` enables `helper/trace` only in
  // `[build-dependencies]`, while its normal `[dependencies]` leaves `helper` without `trace`.
  const rootDir = createTempWorkspace((dir) => {
    writeFileSync(
      path.join(dir, 'Cargo.toml'),
      '[workspace]\nresolver = "2"\nmembers = ["app", "helper", "other"]\n',
      'utf8',
    );
    writeCrate(
      dir,
      'helper',
      '[package]\nname = "helper"\nversion = "0.1.0"\nedition = "2021"\n\n[features]\ntrace = []\n',
    );
    writeCrate(
      dir,
      'other',
      '[package]\nname = "other"\nversion = "0.1.0"\nedition = "2021"\n\n[dependencies]\nhelper = { path = "../helper", features = ["trace"] }\n',
    );
    writeCrate(
      dir,
      'app',
      '[package]\nname = "app"\nversion = "0.1.0"\nedition = "2021"\n\n[dependencies]\nhelper = { path = "../helper" }\n\n[build-dependencies]\nhelper = { path = "../helper", features = ["trace"] }\n',
      { 'build.rs': 'fn main() {}\n' },
    );
  });

  try {
    const config = {
      excluded: [],
      partitions: [
        { name: 'p-app', runs: [{ packages: ['app'], extra_features: [] }] },
        { name: 'p-rest', runs: [{ packages: ['helper', 'other'], extra_features: [] }] },
      ],
    };
    const res = await checkFeatureParity({ rootDir, config, target: TARGET, locked: false });
    assert.ok(
      res.errors.some(
        (e) =>
          e.includes("partition 'p-app'") &&
          e.includes('[target]') &&
          e.includes("'helper'"),
      ),
      `Expected target unit mismatch unmasked by build-deps, got: ${JSON.stringify(res.errors)}`,
    );
  } finally {
    rmSync(rootDir, { recursive: true, force: true });
  }
});

test('checkFeatureParity rejects invalid extra_features and handles comma-combined selectors', async () => {
  const rootDir = createTempWorkspace((dir) => {
    writeFileSync(
      path.join(dir, 'Cargo.toml'),
      '[workspace]\nresolver = "2"\nmembers = ["app", "helper", "other"]\n',
      'utf8',
    );
    writeCrate(
      dir,
      'helper',
      '[package]\nname = "helper"\nversion = "0.1.0"\nedition = "2021"\n\n[features]\ntrace = []\n',
    );
    writeCrate(
      dir,
      'other',
      '[package]\nname = "other"\nversion = "0.1.0"\nedition = "2021"\n\n[features]\nextra = []\n\n[dependencies]\nhelper = { path = "../helper", features = ["trace"] }\n',
    );
    writeCrate(
      dir,
      'app',
      '[package]\nname = "app"\nversion = "0.1.0"\nedition = "2021"\n\n[dependencies]\nhelper = { path = "../helper" }\nother = { path = "../other" }\n',
    );
  });

  try {
    // 1. Invalid feature in extra_features must fail
    const badCfg = {
      excluded: [],
      partitions: [
        {
          name: 'p-bad-extra',
          runs: [{ packages: ['app'], extra_features: ['helper/trace', 'helper/no-such-feature'] }],
        },
        { name: 'p-rest', runs: [{ packages: ['helper', 'other'], extra_features: [] }] },
      ],
    };
    const badRes = await checkFeatureParity({ rootDir, config: badCfg, target: TARGET, locked: false });
    assert.ok(
      badRes.errors.some((e) => e.includes("partition 'p-bad-extra'") && e.includes('cargo tree failed')),
      `Expected cargo tree failure on nonexistent feature, got: ${JSON.stringify(badRes.errors)}`,
    );

    // 2. Comma/whitespace-combined valid extra_features must pass identically to Cargo
    const combinedCfg = {
      excluded: [],
      partitions: [
        {
          name: 'p-combined-extra',
          runs: [{ packages: ['app'], extra_features: ['helper/trace, other/extra'] }],
        },
        { name: 'p-rest', runs: [{ packages: ['helper', 'other'], extra_features: [] }] },
      ],
    };
    const combinedRes = await checkFeatureParity({
      rootDir,
      config: combinedCfg,
      target: TARGET,
      locked: false,
    });
    assert.deepEqual(combinedRes.errors, []);
  } finally {
    rmSync(rootDir, { recursive: true, force: true });
  }
});
