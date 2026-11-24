import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
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

test('rejects quoted dependency keys and transitive edges through non-crates workspace members', () => {
  const dir = makeTempRepo({
    'Cargo.toml':
      '[workspace]\nmembers = ["crates/*", "tools/workspace-hack", "packages/custom-helper"]\n[workspace.dependencies]\ncc-lb-workspace-hack = { path = "tools/workspace-hack" }\n',
    'tools/workspace-hack/Cargo.toml':
      '[package]\nname = "cc-lb-workspace-hack"\nversion = "0.1.0"\nedition = "2021"\n[dependencies]\nsqlx = { path = "../../crates/stub-sqlx" }\n',
    'tools/workspace-hack/src/lib.rs': '',
    'packages/custom-helper/Cargo.toml':
      '[package]\nname = "custom-helper"\nversion = "0.1.0"\nedition = "2021"\n[dependencies]\naxum = { path = "../../crates/stub-axum" }\n',
    'packages/custom-helper/src/lib.rs': '',
    'crates/stub-sqlx/Cargo.toml':
      '[package]\nname = "sqlx"\nversion = "0.1.0"\nedition = "2021"\n',
    'crates/stub-sqlx/src/lib.rs': '',
    'crates/stub-axum/Cargo.toml':
      '[package]\nname = "axum"\nversion = "0.1.0"\nedition = "2021"\n',
    'crates/stub-axum/src/lib.rs': '',
    'crates/cc-lb-provider-gemini/Cargo.toml':
      '[package]\nname = "cc-lb-provider-gemini"\nversion = "0.1.0"\nedition = "2021"\n',
    'crates/cc-lb-provider-gemini/src/lib.rs': '',
    'crates/cc-lb-provider-openai/Cargo.toml':
      '[package]\nname = "cc-lb-provider-openai"\nversion = "0.1.0"\nedition = "2021"\n[dependencies]\n\'cc-lb-provider-gemini\' = { path = "../cc-lb-provider-gemini" }\ncc-lb-workspace-hack.workspace = true\ncustom-helper = { path = "../../packages/custom-helper" }\n',
    'crates/cc-lb-provider-openai/src/lib.rs': '',
  });
  try {
    // Exercise the offline Cargo metadata enrichment path when Cargo.lock exists.
    const lockRes = spawnSync('cargo', ['generate-lockfile', '--offline', '--manifest-path', path.join(dir, 'Cargo.toml')], {
      encoding: 'utf8',
    });
    assert.equal(lockRes.status, 0, `cargo generate-lockfile failed: ${lockRes.stderr}`);

    const result = checkHotpathFirewall(dir);
    assert.equal(result.ok, false, 'Expected quoted direct and non-crates transitive violations');
    const joined = result.violations.join('\n');
    const geminiDirect = result.violations.filter((v) =>
      /cc-lb-provider-openai\/Cargo\.toml:.*forbidden normal dependency `cc-lb-provider-gemini`/.test(v),
    );
    assert.equal(geminiDirect.length, 1, `Expected deduplicated direct report, got:\n${joined}`);
    assert.match(joined, /cc-lb-provider-openai\/Cargo\.toml:.*forbidden transitive normal dependency `sqlx` via `cc-lb-provider-openai -> cc-lb-workspace-hack -> sqlx`/);
    assert.match(joined, /cc-lb-provider-openai\/Cargo\.toml:.*forbidden transitive normal dependency `axum` via `cc-lb-provider-openai -> custom-helper -> axum`/);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('enforces Phase-4 optional rust-embed and proxy rust-embed exclusion rules', () => {
  const badCases = [
    [
      'crates/cc-lb-engine/Cargo.toml',
      '[package]\nname = "cc-lb-engine"\n[dependencies]\nrust-embed.workspace = true\n',
      /rust-embed/,
    ],
    [
      'crates/cc-lb-control/Cargo.toml',
      '[package]\nname = "cc-lb-control"\n[dependencies]\nrust-embed.workspace = true\n',
      /rust-embed/,
    ],
    [
      'crates/cc-lb-admin/Cargo.toml',
      '[package]\nname = "cc-lb-admin"\n[features]\ndefault = ["sqlite"]\n[dependencies]\nrust-embed.workspace = true\n',
      /optional = true/,
    ],
  ];

  for (const [file, manifest, expected] of badCases) {
    const dir = makeTempRepo({ [file]: manifest });
    try {
      const result = checkHotpathFirewall(dir);
      assert.equal(result.ok, false, `Expected failure for ${file}`);
      assert.match(result.violations.join('\n'), expected);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  }

  const aliasedRustEmbedDir = makeTempRepo({
    'Cargo.toml': '[workspace.dependencies]\nspa-embed = { package = "rust-embed", version = "8" }\n',
    'crates/cc-lb-engine/Cargo.toml':
      '[package]\nname = "cc-lb-engine"\n[dependencies]\nspa-embed.workspace = true\n',
  });
  try {
    const result = checkHotpathFirewall(aliasedRustEmbedDir);
    assert.equal(result.ok, false);
    assert.match(result.violations.join('\n'), /rust-embed.*aliased as `spa-embed`/);
  } finally {
    rmSync(aliasedRustEmbedDir, { recursive: true, force: true });
  }

  const goodDir = makeTempRepo({
    'crates/cc-lb-admin/Cargo.toml':
      '[package]\nname = "cc-lb-admin"\n[features]\nembed-spa = ["dep:rust-embed"]\n[dependencies]\nrust-embed = { workspace = true, optional = true }\n',
  });
  try {
    const result = checkHotpathFirewall(goodDir);
    assert.equal(result.ok, true, `Unexpected violations:\n${result.violations.join('\n')}`);
  } finally {
    rmSync(goodDir, { recursive: true, force: true });
  }
});

test('enforces Phase-5 three-binary split isolation and feature gating', () => {
  const badCases = [
    [
      'crates/cc-lb-runtime-sync/Cargo.toml',
      '[package]\nname = "cc-lb-runtime-sync"\n[dependencies]\ncc-lb-scheduler = { path = "../cc-lb-scheduler" }\n',
      /cc-lb-runtime-sync\/Cargo\.toml.*forbidden normal dependency `cc-lb-scheduler`/,
    ],
    [
      'crates/cc-lb-proxy/Cargo.toml',
      '[package]\nname = "cc-lb-proxy"\n[dependencies]\ncc-lb-admin = { path = "../cc-lb-admin" }\n',
      /cc-lb-proxy\/Cargo\.toml.*forbidden normal dependency `cc-lb-admin`/,
    ],
    [
      'crates/cc-lb-admin-server/Cargo.toml',
      '[package]\nname = "cc-lb-admin-server"\n[features]\nembed-spa = ["cc-lb-admin/embed-spa"]\n[dependencies]\ncc-lb-admin = { path = "../cc-lb-admin", default-features = false }\ncc-lb-engine = { path = "../cc-lb-engine" }\n',
      /cc-lb-admin-server\/Cargo\.toml.*forbidden normal dependency `cc-lb-engine`/,
    ],
    [
      'crates/cc-lb-server/Cargo.toml',
      '[package]\nname = "cc-lb-server"\n[features]\nstorage-sqlite = ["admin"]\n[dependencies]\ncc-lb-admin = { path = "../cc-lb-admin" }\n',
      /must set `optional = true`/,
    ],
    [
      'crates/cc-lb-proxy/Cargo.toml',
      '[package]\nname = "cc-lb-proxy"\n[features]\nsqlite = ["cc-lb-server/sqlite"]\n[dependencies]\ncc-lb-server = { path = "../cc-lb-server" }\n',
      /must set `default-features = false`/,
    ],
    [
      'crates/cc-lb-proxy/Cargo.toml',
      '[package]\nname = "cc-lb-proxy"\n[features]\nsqlite = ["cc-lb-server/sqlite"]\n[dependencies]\ncc-lb-server = { path = "../cc-lb-server", default-features = false }\n',
      /feature `sqlite` must not enable `cc-lb-server\/sqlite`/,
    ],
    [
      'crates/cc-lb-admin-server/Cargo.toml',
      '[package]\nname = "cc-lb-admin-server"\n[features]\nsqlite = ["cc-lb-admin/embed-spa"]\n[dependencies]\ncc-lb-admin = { path = "../cc-lb-admin" }\n',
      /must set `default-features = false`/,
    ],
  ];

  for (const [file, manifest, expected] of badCases) {
    const dir = makeTempRepo({ [file]: manifest });
    try {
      const result = checkHotpathFirewall(dir);
      assert.equal(result.ok, false, `Expected failure for ${file}`);
      assert.match(result.violations.join('\n'), expected);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  }

  const transitiveDir = makeTempRepo({
    'crates/helper/Cargo.toml':
      '[package]\nname = "helper"\n[dependencies]\ncc-lb-engine = { path = "../cc-lb-engine" }\n',
    'crates/cc-lb-admin-server/Cargo.toml':
      '[package]\nname = "cc-lb-admin-server"\n[features]\nembed-spa = ["cc-lb-admin/embed-spa"]\n[dependencies]\ncc-lb-admin = { path = "../cc-lb-admin", default-features = false }\nhelper = { path = "../helper" }\n',
  });
  try {
    const result = checkHotpathFirewall(transitiveDir);
    assert.equal(result.ok, false, 'Expected transitive engine violation in cc-lb-admin-server');
    assert.match(
      result.violations.join('\n'),
      /cc-lb-admin-server\/Cargo\.toml.*forbidden transitive normal dependency `cc-lb-engine` via `cc-lb-admin-server -> helper -> cc-lb-engine`/,
    );
  } finally {
    rmSync(transitiveDir, { recursive: true, force: true });
  }
});
