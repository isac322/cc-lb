import assert from 'node:assert/strict';
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

import { checkHotpathFirewall } from './check-hotpath-firewall.mjs';

function makeTempRepo(files) {
  const dir = mkdtempSync(path.join(os.tmpdir(), 'cc-lb-hotpath-firewall-test-'));
  for (const [relPath, content] of Object.entries(files)) {
    const abs = path.join(dir, relPath);
    mkdirSync(path.dirname(abs), { recursive: true });
    writeFileSync(abs, content, 'utf8');
  }
  return dir;
}

test('live workspace passes hotpath firewall check', () => {
  const result = checkHotpathFirewall();
  assert.equal(result.ok, true, `Unexpected violations:\n${result.violations.join('\n')}`);
});

test('allows sqlx under cc-lb-engine dev-dependencies', () => {
  const dir = makeTempRepo({
    'crates/cc-lb-engine/Cargo.toml':
      '[package]\nname = "cc-lb-engine"\n[dependencies]\nhttp.workspace = true\n[target.\'cfg(not(trybuild))\'.dev-dependencies]\nsqlx.workspace = true\ncc-lb-event-pipeline.workspace = true\n',
  });
  try {
    const result = checkHotpathFirewall(dir);
    assert.equal(result.ok, true, `Unexpected violations:\n${result.violations.join('\n')}`);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('rejects forbidden normal deps across all 4 firewall crates', () => {
  const cases = [
    [
      'crates/cc-lb-engine/Cargo.toml',
      '[package]\nname = "cc-lb-engine"\n[target.\'cfg(not(loom))\'.dependencies]\nsqlx.workspace = true\n',
      /sqlx/,
    ],
    [
      'crates/cc-lb-routing-eval/Cargo.toml',
      '[package]\nname = "cc-lb-routing-eval"\n[dependencies]\ncc-lb-storage-api = { path = "../cc-lb-storage-api" }\n',
      /cc-lb-storage-api/,
    ],
    [
      'crates/cc-lb-proxy-telemetry/Cargo.toml',
      '[package]\nname = "cc-lb-proxy-telemetry"\n[dependencies]\ncc-lb-routing-eval.workspace = true\n',
      /cc-lb-routing-eval/,
    ],
    [
      'crates/cc-lb-event-pipeline/Cargo.toml',
      '[package]\nname = "cc-lb-event-pipeline"\n[dependencies]\naxum.workspace = true\n',
      /axum/,
    ],
  ];

  for (const [file, manifest, expected] of cases) {
    const dir = makeTempRepo({ [file]: manifest });
    try {
      const result = checkHotpathFirewall(dir);
      assert.equal(result.ok, false, `Expected failure for ${file}`);
      assert.match(result.violations.join('\n'), expected);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  }
});

test('rejects postgres feature reintroduction in cc-lb-engine', () => {
  const dir = makeTempRepo({
    'crates/cc-lb-engine/Cargo.toml':
      '[package]\nname = "cc-lb-engine"\n[features]\npostgres = []\n',
  });
  try {
    const result = checkHotpathFirewall(dir);
    assert.equal(result.ok, false);
    assert.match(result.violations.join('\n'), /forbidden feature `postgres`/);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('rejects Mutex<Option<mpsc::Sender<LifecycleEvent>>> regression in event_bus.rs', () => {
  const dir = makeTempRepo({
    'crates/cc-lb-control/src/event_bus.rs':
      'struct Bad { writer_tx: Mutex<Option<mpsc::Sender<LifecycleEvent>>> }\n',
  });
  try {
    const result = checkHotpathFirewall(dir);
    assert.equal(result.ok, false);
    assert.match(result.violations.join('\n'), /event_bus\.rs:1/);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('enforces Phase-3 provider and domain isolation rules', () => {
  const cases = [
    [
      'crates/cc-lb-domain/Cargo.toml',
      '[package]\nname = "cc-lb-domain"\n[dependencies]\ncc-lb-routing-trace.workspace = true\n',
      /cc-lb-routing-trace/,
    ],
    [
      'crates/cc-lb-domain/Cargo.toml',
      '[package]\nname = "cc-lb-domain"\n[dependencies]\ncc-lb-provider-spi.workspace = true\n',
      /cc-lb-provider-spi/,
    ],
    [
      'crates/cc-lb-provider-spi/Cargo.toml',
      '[package]\nname = "cc-lb-provider-spi"\n[dependencies]\ncc-lb-provider-anthropic.workspace = true\n',
      /cc-lb-provider-anthropic/,
    ],
    [
      'crates/cc-lb-provider-openai/Cargo.toml',
      '[package]\nname = "cc-lb-provider-openai"\n[dependencies]\ncc-lb-provider-anthropic.workspace = true\ncc-lb-quota.workspace = true\n',
      /cc-lb-quota/,
    ],
    [
      'crates/cc-lb-providers/Cargo.toml',
      '[package]\nname = "cc-lb-providers"\n[dependencies]\ncc-lb-storage-api = { path = "../cc-lb-storage-api" }\n',
      /cc-lb-storage-api/,
    ],
  ];

  for (const [file, manifest, expected] of cases) {
    const dir = makeTempRepo({ [file]: manifest });
    try {
      const result = checkHotpathFirewall(dir);
      assert.equal(result.ok, false, `Expected failure for ${file}`);
      assert.match(result.violations.join('\n'), expected);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  }
});

test('rejects indirect normal dependency into storage-api and quota via cc-lb-control', () => {
  const dir = makeTempRepo({
    'crates/cc-lb-control/Cargo.toml':
      '[package]\nname = "cc-lb-control"\n[dependencies]\ncc-lb-quota.workspace = true\ncc-lb-storage-api = { path = "../cc-lb-storage-api" }\n',
    'crates/cc-lb-provider-openai/Cargo.toml':
      '[package]\nname = "cc-lb-provider-openai"\n[dependencies]\ncc-lb-control = { path = "../cc-lb-control" }\n',
    'crates/cc-lb-providers/Cargo.toml':
      '[package]\nname = "cc-lb-providers"\n[dependencies]\ncc-lb-provider-openai = { path = "../cc-lb-provider-openai" }\n',
    'crates/cc-lb-routing-eval/Cargo.toml':
      '[package]\nname = "cc-lb-routing-eval"\n[dependencies]\ncc-lb-providers = { path = "../cc-lb-providers" }\n',
  });
  try {
    const result = checkHotpathFirewall(dir);
    assert.equal(result.ok, false, 'Expected indirect control -> quota/storage-api violation');
    const joined = result.violations.join('\n');
    assert.match(joined, /cc-lb-provider-openai\/Cargo\.toml.*cc-lb-quota.*cc-lb-control -> cc-lb-quota/);
    assert.match(joined, /cc-lb-provider-openai\/Cargo\.toml.*cc-lb-storage-api.*cc-lb-control -> cc-lb-storage-api/);
    assert.match(joined, /cc-lb-routing-eval\/Cargo\.toml.*cc-lb-control/);
    assert.match(joined, /cc-lb-routing-eval\/Cargo\.toml.*cc-lb-storage-api.*cc-lb-control -> cc-lb-storage-api/);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('resolves Cargo workspace aliases for direct and transitive exclusions', () => {
  const dottedAliasDir = makeTempRepo({
    'Cargo.toml':
      '[workspace.dependencies]\ncontrol-ports = { package = "cc-lb-control", path = "crates/cc-lb-control" }\nsqlx-alias = { package = "sqlx", version = "0.9" }\n',
    'crates/cc-lb-control/Cargo.toml':
      '[package]\nname = "cc-lb-control"\n[dependencies]\ncc-lb-quota.workspace = true\ncc-lb-storage-api = { path = "../cc-lb-storage-api" }\n',
    'crates/cc-lb-provider-openai/Cargo.toml':
      '[package]\nname = "cc-lb-provider-openai"\n[dependencies]\ncontrol-ports.workspace = true\n',
    'crates/cc-lb-providers/Cargo.toml':
      '[package]\nname = "cc-lb-providers"\n[dependencies]\ncc-lb-provider-openai = { path = "../cc-lb-provider-openai" }\n',
    'crates/cc-lb-routing-eval/Cargo.toml':
      '[package]\nname = "cc-lb-routing-eval"\n[dependencies]\ncc-lb-providers = { path = "../cc-lb-providers" }\n',
    'crates/cc-lb-engine/Cargo.toml':
      '[package]\nname = "cc-lb-engine"\n[dependencies]\nsqlx-alias.workspace = true\n',
  });
  try {
    const result = checkHotpathFirewall(dottedAliasDir);
    assert.equal(result.ok, false, 'Expected workspace alias violations');
    const joined = result.violations.join('\n');
    assert.match(joined, /cc-lb-engine\/Cargo\.toml:.*forbidden normal dependency `sqlx` \(aliased as `sqlx-alias`\)/);
    assert.match(joined, /cc-lb-provider-openai\/Cargo\.toml:.*cc-lb-quota.*cc-lb-control -> cc-lb-quota/);
    assert.match(joined, /cc-lb-provider-openai\/Cargo\.toml:.*cc-lb-storage-api.*cc-lb-control -> cc-lb-storage-api/);
    assert.match(joined, /cc-lb-routing-eval\/Cargo\.toml:.*cc-lb-control/);
    assert.match(joined, /cc-lb-routing-eval\/Cargo\.toml:.*cc-lb-storage-api.*cc-lb-control -> cc-lb-storage-api/);
  } finally {
    rmSync(dottedAliasDir, { recursive: true, force: true });
  }

  const subtableAndMultilineDir = makeTempRepo({
    'Cargo.toml':
      '[workspace.dependencies.control-ports]\npackage = "cc-lb-control"\npath = "crates/cc-lb-control"\n',
    'crates/cc-lb-control/Cargo.toml':
      '[package]\nname = "cc-lb-control"\n[dependencies]\ncc-lb-quota.workspace = true\ncc-lb-storage-api = { path = "../cc-lb-storage-api" }\n',
    'crates/cc-lb-provider-openai/Cargo.toml':
      '[package]\nname = "cc-lb-provider-openai"\n[dependencies]\ncontrol-ports = {\n  workspace = true,\n}\n',
    'crates/cc-lb-provider-gemini/Cargo.toml':
      '[package]\nname = "cc-lb-provider-gemini"\n[dependencies.control-ports]\nworkspace = true\n',
  });
  try {
    const result = checkHotpathFirewall(subtableAndMultilineDir);
    assert.equal(result.ok, false, 'Expected subtable and multiline workspace alias violations');
    const joined = result.violations.join('\n');
    assert.match(joined, /cc-lb-provider-openai\/Cargo\.toml:4:.*cc-lb-quota.*cc-lb-control -> cc-lb-quota/);
    assert.match(joined, /cc-lb-provider-gemini\/Cargo\.toml:3:.*cc-lb-storage-api.*cc-lb-control -> cc-lb-storage-api/);
  } finally {
    rmSync(subtableAndMultilineDir, { recursive: true, force: true });
  }
});
