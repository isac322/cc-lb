import assert from 'node:assert/strict';
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

import { checkBoundary } from './check-proxy-admin-boundary.mjs';

function makeTempRepo(files) {
  const dir = mkdtempSync(path.join(os.tmpdir(), 'cc-lb-boundary-test-'));
  for (const [relPath, content] of Object.entries(files)) {
    const abs = path.join(dir, relPath);
    mkdirSync(path.dirname(abs), { recursive: true });
    writeFileSync(abs, content, 'utf8');
  }
  return dir;
}

test('live workspace passes proxy/admin boundary check', () => {
  const result = checkBoundary();
  assert.equal(result.ok, true, `Unexpected violations:\n${result.violations.join('\n')}`);
});

test('rejects forbidden cc-lb-admin dependency in cc-lb-control Cargo.toml', () => {
  const dir = makeTempRepo({
    'crates/cc-lb-control/Cargo.toml': '[package]\nname = "cc-lb-control"\n[dependencies]\ncc-lb-admin = { path = "../cc-lb-admin" }\n',
    'crates/cc-lb-server/src/app.rs': 'use cc_lb_admin::{AdminState, router};\n',
  });
  try {
    const result = checkBoundary(dir);
    assert.equal(result.ok, false);
    assert.match(result.violations[0], /crates\/cc-lb-control\/Cargo\.toml/);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('rejects cc_lb_admin::subscription_quotas usage inside app.rs', () => {
  const dir = makeTempRepo({
    'crates/cc-lb-server/src/app.rs': 'let x = cc_lb_admin::subscription_quotas::build_cc_lb_oauth_usage_response();\n',
  });
  try {
    const result = checkBoundary(dir);
    assert.equal(result.ok, false);
    assert.match(result.violations.join('\n'), /subscription_quotas/);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('rejects cc_lb_admin reference inside non-allowlisted server file cron.rs', () => {
  const dir = makeTempRepo({
    'crates/cc-lb-server/src/scheduler_dispatch/cron.rs': 'use cc_lb_admin::subscription_quotas::record_provider_pool_quota_snapshots_now;\n',
  });
  try {
    const result = checkBoundary(dir);
    assert.equal(result.ok, false);
    assert.match(result.violations[0], /scheduler_dispatch\/cron\.rs:1/);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
