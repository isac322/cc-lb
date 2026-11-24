#!/usr/bin/env node
// Enforces the Phase-1 workspace & proxy-vs-admin boundary:
// 1. Core/domain/control/scheduler/storage crates must never depend on `cc-lb-admin`.
// 2. `crates/cc-lb-server/src/` may reference `cc_lb_admin` ONLY in explicit admin
//    composition/wiring files (or test-only wiring), never on proxy or scheduler paths.
import { readdirSync, readFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, '..');

const FORBIDDEN_CARGO_CRATES = [
  'cc-lb-engine',
  'cc-lb-event-pipeline',
  'cc-lb-proxy-telemetry',
  'cc-lb-routing-eval',
  'cc-lb-control',
  'cc-lb-scheduler',
  'cc-lb-quota',
  'cc-lb-routing',
  'cc-lb-storage-api',
  'cc-lb-storage-sqlite',
  'cc-lb-storage-postgres',
];

const SERVER_ADMIN_COMPOSITION_FILES = new Set([
  'crates/cc-lb-server/src/admin_ports.rs',
  'crates/cc-lb-server/src/build_meta.rs',
  'crates/cc-lb-server/src/app.rs',
  'crates/cc-lb-server/src/scheduler_dispatch_tests.rs',
  'crates/cc-lb-server/src/scheduler_dispatch/oauth/tests/limit_resets.rs',
]);

const APP_ALLOWED_ROOT_ITEMS = new Set([
  'AdminBuildInfo',
  'AdminPorts',
  'AdminState',
  'DynamicViewRebinder',
  'StartupConfigOverrides',
  'WarmupDialectDispatchError',
  'WarmupDialectDispatchErrorKind',
  'WarmupDialectDispatchOutcome',
  'WarmupDialectDispatcher',
  'WarmupNativeDispatchOutcome',
  'auth',
  'events',
  'router',
  'routes',
  'ports',
]);

const APP_ALLOWED_QUALIFIED_PATHS = [
  /^cc_lb_admin::auth::/,
  /^cc_lb_admin::events::DEFAULT_STORAGE_TAIL_CAPACITY\b/,
  /^cc_lb_admin::router\b/,
  /^cc_lb_admin::routes::with_request_tracing\b/,
  /^cc_lb_admin::ports::/,
];

const FORBIDDEN_SERVER_ADMIN_SYMBOLS = [
  /\bcc_lb_admin::subscription_quotas\b/,
  /\bcc_lb_admin::LimitResetUsageRefresh\b/,
  /\bcc_lb_admin::LimitResetUsageRefreshPort\b/,
  /\bcc_lb_admin::WarmupWindowCheck\b/,
  /\bcc_lb_admin::WarmupWindowVerifier\b/,
];

function stripLineComment(line) {
  const idx = line.indexOf('//');
  return idx === -1 ? line : line.slice(0, idx);
}

function stripTomlComment(line) {
  const idx = line.indexOf('#');
  return idx === -1 ? line : line.slice(0, idx);
}

function walkRustFiles(dir) {
  const results = [];
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      results.push(...walkRustFiles(full));
    } else if (entry.isFile() && entry.name.endsWith('.rs')) {
      results.push(full);
    }
  }
  return results.sort();
}

function validateAppFile(relPath, content, violations) {
  const cleaned = content.split(/\r?\n/).map(stripLineComment).join('\n');
  for (const pattern of FORBIDDEN_SERVER_ADMIN_SYMBOLS) {
    if (pattern.test(cleaned)) {
      violations.push(`${relPath}: forbidden admin reference matching ${pattern}`);
    }
  }

  for (const match of cleaned.matchAll(/\buse\s+cc_lb_admin::([^;]+);/g)) {
    const clause = match[1].trim();
    if (clause.startsWith('{') && clause.endsWith('}')) {
      for (const rawItem of clause.slice(1, -1).split(',')) {
        const item = rawItem.trim().split(/\s+as\s+/)[0].trim();
        if (!item) continue;
        const rootSeg = item.split('::')[0].trim();
        if (!APP_ALLOWED_ROOT_ITEMS.has(rootSeg)) {
          violations.push(`${relPath}: disallowed \`use cc_lb_admin::{${item}}\` outside admin composition`);
        }
      }
      continue;
    }
    const fullPath = `cc_lb_admin::${clause.split(/\s+as\s+/)[0].trim()}`;
    const allowed = APP_ALLOWED_QUALIFIED_PATHS.some((re) => re.test(fullPath)) ||
      APP_ALLOWED_ROOT_ITEMS.has(clause.split('::')[0].trim());
    if (!allowed) {
      violations.push(`${relPath}: disallowed \`use ${fullPath}\` outside admin composition`);
    }
  }

  for (const match of cleaned.matchAll(/\bcc_lb_admin::([A-Za-z0-9_:]+)/g)) {
    const fullPath = `cc_lb_admin::${match[1]}`;
    const rootSeg = match[1].split('::')[0];
    const allowed =
      APP_ALLOWED_QUALIFIED_PATHS.some((re) => re.test(fullPath)) ||
      APP_ALLOWED_ROOT_ITEMS.has(rootSeg);
    if (!allowed) {
      violations.push(`${relPath}: disallowed reference \`${fullPath}\` in app.rs`);
    }
  }
}

export function checkBoundary(rootDir = ROOT) {
  const violations = [];

  for (const crate of FORBIDDEN_CARGO_CRATES) {
    const manifestRel = `crates/${crate}/Cargo.toml`;
    const manifestAbs = path.join(rootDir, manifestRel);
    let raw;
    try {
      raw = readFileSync(manifestAbs, 'utf8');
    } catch {
      continue;
    }
    const lines = raw.split(/\r?\n/);
    for (let i = 0; i < lines.length; i += 1) {
      const code = stripTomlComment(lines[i]);
      if (/(^|[^A-Za-z0-9_-])cc-lb-admin([^A-Za-z0-9_-]|$)/.test(code)) {
        violations.push(`${manifestRel}:${i + 1}: forbidden dependency on cc-lb-admin`);
      }
    }
  }

  const serverSrcAbs = path.join(rootDir, 'crates/cc-lb-server/src');
  for (const absFile of walkRustFiles(serverSrcAbs)) {
    const relPath = path.relative(rootDir, absFile).split(path.sep).join('/');
    const content = readFileSync(absFile, 'utf8');
    if (relPath === 'crates/cc-lb-server/src/app.rs') {
      validateAppFile(relPath, content, violations);
      continue;
    }
    if (SERVER_ADMIN_COMPOSITION_FILES.has(relPath)) {
      const cleaned = content.split(/\r?\n/).map(stripLineComment).join('\n');
      for (const pattern of FORBIDDEN_SERVER_ADMIN_SYMBOLS) {
        if (pattern.test(cleaned)) {
          violations.push(`${relPath}: forbidden admin symbol ${pattern} (use cc_lb_control)`);
        }
      }
      continue;
    }
    const lines = content.split(/\r?\n/);
    for (let i = 0; i < lines.length; i += 1) {
      const code = stripLineComment(lines[i]);
      if (/\bcc_lb_admin\b/.test(code)) {
        violations.push(`${relPath}:${i + 1}: forbidden reference to cc_lb_admin outside admin composition files`);
      }
    }
  }

  return { ok: violations.length === 0, violations };
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const result = checkBoundary(ROOT);
  if (!result.ok) {
    for (const violation of result.violations) {
      console.error(`BOUNDARY VIOLATION: ${violation}`);
    }
    process.exit(1);
  }
  console.log('Proxy/admin boundary check passed (0 violations).');
}
