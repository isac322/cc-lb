#!/usr/bin/env node
// Enforces the Phase-2 hot-path dependency & lock-free event-bus firewall:
// 1. Hot-path & pure evaluation crates must never pull heavy/off-path crates
//    into normal `[dependencies]` (dev-dependencies remain allowed).
// 2. `cc-lb-engine` must never re-introduce a `postgres` feature.
// 3. `InMemoryBus` (`crates/cc-lb-control/src/event_bus.rs`) must never
//    re-introduce `Mutex<Option<mpsc::Sender<LifecycleEvent>>>`.
import { spawnSync } from 'node:child_process';
import { existsSync, readdirSync, readFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, '..');

export const FORBIDDEN_NORMAL_DEPS = new Map([
  [
    'crates/cc-lb-engine/Cargo.toml',
    new Set([
      'sqlx',
      'opentelemetry',
      'tracing-opentelemetry',
      'tiktoken-rs',
      'rust-embed',
      'cc-lb-admin',
      'cc-lb-event-pipeline',
      'cc-lb-storage-sqlite',
      'cc-lb-storage-postgres',
      'cc-lb-scheduler',
    ]),
  ],
  [
    'crates/cc-lb-routing-eval/Cargo.toml',
    new Set([
      'sqlx',
      'axum',
      'hyper',
      'hyper-rustls',
      'hyper-util',
      'hickory-resolver',
      'rustls',
      'rust-embed',
      'cc-lb-storage-api',
      'cc-lb-storage-sqlite',
      'cc-lb-storage-postgres',
      'cc-lb-admin',
      'cc-lb-engine',
      'cc-lb-event-pipeline',
      'cc-lb-control',
      'cc-lb-upstream',
      'cc-lb-scheduler',
      'opentelemetry',
    ]),
  ],
  [
    'crates/cc-lb-proxy-telemetry/Cargo.toml',
    new Set([
      'sqlx',
      'axum',
      'tiktoken-rs',
      'rust-embed',
      'cc-lb-storage-api',
      'cc-lb-storage-sqlite',
      'cc-lb-storage-postgres',
      'cc-lb-admin',
      'cc-lb-engine',
      'cc-lb-event-pipeline',
      'cc-lb-control',
      'cc-lb-routing',
      'cc-lb-routing-eval',
      'cc-lb-scheduler',
    ]),
  ],
  [
    'crates/cc-lb-control/Cargo.toml',
    new Set([
      'rust-embed',
      'cc-lb-admin',
      'cc-lb-server',
    ]),
  ],
  [
    'crates/cc-lb-event-pipeline/Cargo.toml',
    new Set([
      'cc-lb-admin',
      'cc-lb-server',
      'cc-lb-scheduler',
      'axum',
      'hyper',
      'rustls',
      'rust-embed',
      'tiktoken-rs',
      'opentelemetry',
    ]),
  ],
  [
    'crates/cc-lb-domain/Cargo.toml',
    new Set([
      'cc-lb-routing-trace',
      'cc-lb-provider-spi',
      'cc-lb-providers',
      'cc-lb-provider-anthropic',
      'cc-lb-provider-openai',
      'cc-lb-provider-gemini',
      'cc-lb-provider-vertex',
      'cc-lb-storage-api',
      'sqlx',
      'axum',
      'hyper',
      'rust-embed',
      'tiktoken-rs',
      'opentelemetry',
    ]),
  ],
  [
    'crates/cc-lb-routing-trace/Cargo.toml',
    new Set([
      'cc-lb-storage-api',
      'cc-lb-engine',
      'cc-lb-admin',
      'cc-lb-server',
      'cc-lb-provider-spi',
      'cc-lb-providers',
      'sqlx',
      'axum',
      'hyper',
      'rust-embed',
      'tiktoken-rs',
      'opentelemetry',
    ]),
  ],
  [
    'crates/cc-lb-provider-spi/Cargo.toml',
    new Set([
      'cc-lb-provider-anthropic',
      'cc-lb-provider-openai',
      'cc-lb-provider-gemini',
      'cc-lb-provider-vertex',
      'cc-lb-providers',
      'cc-lb-quota',
      'cc-lb-storage-api',
      'cc-lb-engine',
      'cc-lb-admin',
      'cc-lb-server',
      'sqlx',
      'axum',
      'hyper',
      'rust-embed',
      'tiktoken-rs',
      'opentelemetry',
    ]),
  ],
  [
    'crates/cc-lb-provider-anthropic/Cargo.toml',
    new Set([
      'cc-lb-provider-openai',
      'cc-lb-provider-gemini',
      'cc-lb-provider-vertex',
      'cc-lb-providers',
      'cc-lb-quota',
      'cc-lb-storage-api',
      'cc-lb-engine',
      'cc-lb-admin',
      'cc-lb-server',
      'sqlx',
      'axum',
      'hyper',
      'rust-embed',
      'tiktoken-rs',
      'opentelemetry',
    ]),
  ],
  [
    'crates/cc-lb-provider-openai/Cargo.toml',
    new Set([
      'cc-lb-provider-anthropic',
      'cc-lb-provider-gemini',
      'cc-lb-provider-vertex',
      'cc-lb-providers',
      'cc-lb-quota',
      'cc-lb-storage-api',
      'cc-lb-engine',
      'cc-lb-admin',
      'cc-lb-server',
      'sqlx',
      'axum',
      'hyper',
      'rust-embed',
      'tiktoken-rs',
      'opentelemetry',
    ]),
  ],
  [
    'crates/cc-lb-provider-gemini/Cargo.toml',
    new Set([
      'cc-lb-provider-anthropic',
      'cc-lb-provider-openai',
      'cc-lb-provider-vertex',
      'cc-lb-providers',
      'cc-lb-quota',
      'cc-lb-storage-api',
      'cc-lb-engine',
      'cc-lb-admin',
      'cc-lb-server',
      'sqlx',
      'axum',
      'hyper',
      'rust-embed',
      'tiktoken-rs',
      'opentelemetry',
    ]),
  ],
  [
    'crates/cc-lb-provider-vertex/Cargo.toml',
    new Set([
      'cc-lb-provider-anthropic',
      'cc-lb-provider-openai',
      'cc-lb-provider-gemini',
      'cc-lb-providers',
      'cc-lb-quota',
      'cc-lb-storage-api',
      'cc-lb-engine',
      'cc-lb-admin',
      'cc-lb-server',
      'sqlx',
      'axum',
      'hyper',
      'rust-embed',
      'tiktoken-rs',
      'opentelemetry',
    ]),
  ],
  [
    'crates/cc-lb-providers/Cargo.toml',
    new Set([
      'cc-lb-quota',
      'cc-lb-storage-api',
      'cc-lb-engine',
      'cc-lb-admin',
      'cc-lb-server',
      'sqlx',
      'axum',
      'hyper',
      'rust-embed',
      'tiktoken-rs',
      'opentelemetry',
    ]),
  ],
  [
    'crates/cc-lb-runtime-sync/Cargo.toml',
    new Set([
      'cc-lb-scheduler',
      'cc-lb-admin',
      'cc-lb-admin-server',
      'cc-lb-engine',
      'cc-lb-server',
      'cc-lb-proxy',
      'rust-embed',
    ]),
  ],
  [
    'crates/cc-lb-proxy/Cargo.toml',
    new Set(['cc-lb-admin', 'cc-lb-admin-server', 'rust-embed']),
  ],
  [
    'crates/cc-lb-admin-server/Cargo.toml',
    new Set([
      'cc-lb-engine',
      'cc-lb-server',
      'cc-lb-proxy',
      'cc-lb-routing',
      'cc-lb-routing-eval',
      'cc-lb-event-pipeline',
      'rust-embed',
    ]),
  ],
]);

export const FORBIDDEN_TRANSITIVE_NORMAL_DEPS = new Map([
  [
    'crates/cc-lb-engine/Cargo.toml',
    new Set([
      'sqlx',
      'rust-embed',
      'cc-lb-admin',
      'cc-lb-event-pipeline',
      'cc-lb-storage-sqlite',
      'cc-lb-storage-postgres',
      'cc-lb-scheduler',
    ]),
  ],
  [
    'crates/cc-lb-routing-eval/Cargo.toml',
    new Set([
      'sqlx',
      'axum',
      'hyper',
      'hyper-rustls',
      'hyper-util',
      'hickory-resolver',
      'rustls',
      'rust-embed',
      'cc-lb-storage-api',
      'cc-lb-storage-sqlite',
      'cc-lb-storage-postgres',
      'cc-lb-quota',
      'cc-lb-admin',
      'cc-lb-engine',
      'cc-lb-event-pipeline',
      'cc-lb-control',
      'cc-lb-scheduler',
      'opentelemetry',
    ]),
  ],
  ['crates/cc-lb-proxy-telemetry/Cargo.toml', FORBIDDEN_NORMAL_DEPS.get('crates/cc-lb-proxy-telemetry/Cargo.toml')],
  [
    'crates/cc-lb-control/Cargo.toml',
    FORBIDDEN_NORMAL_DEPS.get('crates/cc-lb-control/Cargo.toml'),
  ],
  [
    'crates/cc-lb-event-pipeline/Cargo.toml',
    new Set(['rust-embed', 'cc-lb-admin', 'cc-lb-server', 'cc-lb-scheduler']),
  ],
  ['crates/cc-lb-domain/Cargo.toml', FORBIDDEN_NORMAL_DEPS.get('crates/cc-lb-domain/Cargo.toml')],
  [
    'crates/cc-lb-routing-trace/Cargo.toml',
    FORBIDDEN_NORMAL_DEPS.get('crates/cc-lb-routing-trace/Cargo.toml'),
  ],
  [
    'crates/cc-lb-provider-spi/Cargo.toml',
    FORBIDDEN_NORMAL_DEPS.get('crates/cc-lb-provider-spi/Cargo.toml'),
  ],
  [
    'crates/cc-lb-provider-anthropic/Cargo.toml',
    FORBIDDEN_NORMAL_DEPS.get('crates/cc-lb-provider-anthropic/Cargo.toml'),
  ],
  [
    'crates/cc-lb-provider-openai/Cargo.toml',
    FORBIDDEN_NORMAL_DEPS.get('crates/cc-lb-provider-openai/Cargo.toml'),
  ],
  [
    'crates/cc-lb-provider-gemini/Cargo.toml',
    FORBIDDEN_NORMAL_DEPS.get('crates/cc-lb-provider-gemini/Cargo.toml'),
  ],
  [
    'crates/cc-lb-provider-vertex/Cargo.toml',
    FORBIDDEN_NORMAL_DEPS.get('crates/cc-lb-provider-vertex/Cargo.toml'),
  ],
  [
    'crates/cc-lb-providers/Cargo.toml',
    FORBIDDEN_NORMAL_DEPS.get('crates/cc-lb-providers/Cargo.toml'),
  ],
  [
    'crates/cc-lb-runtime-sync/Cargo.toml',
    FORBIDDEN_NORMAL_DEPS.get('crates/cc-lb-runtime-sync/Cargo.toml'),
  ],
  [
    'crates/cc-lb-admin-server/Cargo.toml',
    new Set([
      'cc-lb-engine',
      'cc-lb-server',
      'cc-lb-proxy',
      'cc-lb-routing-eval',
      'cc-lb-event-pipeline',
    ]),
  ],
]);

function stripTomlComment(line) {
  let inSingle = false;
  let inDouble = false;
  let escaped = false;
  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];
    if (escaped) {
      escaped = false;
      continue;
    }
    if (inDouble) {
      if (ch === '\\') escaped = true;
      else if (ch === '"') inDouble = false;
      continue;
    }
    if (inSingle) {
      if (ch === '\'') inSingle = false;
      continue;
    }
    if (ch === '\'') inSingle = true;
    else if (ch === '"') inDouble = true;
    else if (ch === '#') return line.slice(0, i);
  }
  return line;
}

function stripRustLineComment(line) {
  const idx = line.indexOf('//');
  return idx === -1 ? line : line.slice(0, idx);
}

function countDelimiterBalance(text) {
  let braceDepth = 0;
  let bracketDepth = 0;
  let inSingle = false;
  let inDouble = false;
  let escaped = false;
  for (let i = 0; i < text.length; i += 1) {
    const ch = text[i];
    if (escaped) {
      escaped = false;
      continue;
    }
    if (inDouble) {
      if (ch === '\\') escaped = true;
      else if (ch === '"') inDouble = false;
      continue;
    }
    if (inSingle) {
      if (ch === '\'') inSingle = false;
      continue;
    }
    if (ch === '\'') inSingle = true;
    else if (ch === '"') inDouble = true;
    else if (ch === '{') braceDepth += 1;
    else if (ch === '}') braceDepth = Math.max(0, braceDepth - 1);
    else if (ch === '[') bracketDepth += 1;
    else if (ch === ']') bracketDepth = Math.max(0, bracketDepth - 1);
  }
  return { braceDepth, bracketDepth };
}

function collectTomlStatements(raw) {
  const lines = raw.split(/\r?\n/);
  const statements = [];
  let sectionHeader = '';
  let pending = null;
  let braceDepth = 0;
  let bracketDepth = 0;

  for (let i = 0; i < lines.length; i += 1) {
    const code = stripTomlComment(lines[i]).trim();
    if (!code) continue;

    if (!pending) {
      if (code.startsWith('[') && !code.startsWith('[[')) {
        const end = code.indexOf(']');
        if (end !== -1) {
          sectionHeader = code.slice(1, end).trim();
          statements.push({ kind: 'header', header: sectionHeader, text: code, line: i + 1 });
          continue;
        }
      }
      pending = { header: sectionHeader, text: code, line: i + 1 };
    } else {
      pending.text = `${pending.text} ${code}`;
    }

    const balance = countDelimiterBalance(pending.text);
    braceDepth = balance.braceDepth;
    bracketDepth = balance.bracketDepth;
    if (braceDepth === 0 && bracketDepth === 0) {
      statements.push({
        kind: 'entry',
        header: pending.header,
        text: pending.text,
        line: pending.line,
      });
      pending = null;
    }
  }

  if (pending) {
    statements.push({
      kind: 'entry',
      header: pending.header,
      text: pending.text,
      line: pending.line,
    });
  }

  return statements;
}

function stripTomlQuotes(raw) {
  const s = raw.trim();
  if (
    s.length >= 2 &&
    ((s.startsWith('"') && s.endsWith('"')) || (s.startsWith('\'') && s.endsWith('\'')))
  ) {
    return s.slice(1, -1);
  }
  return s;
}

function splitTomlPath(pathStr) {
  const parts = [];
  let cur = '';
  let quote = null;
  let escaped = false;
  for (let i = 0; i < pathStr.length; i += 1) {
    const ch = pathStr[i];
    if (escaped) {
      cur += ch;
      escaped = false;
      continue;
    }
    if (quote === '"') {
      cur += ch;
      if (ch === '\\') escaped = true;
      else if (ch === '"') quote = null;
      continue;
    }
    if (quote === '\'') {
      cur += ch;
      if (ch === '\'') quote = null;
      continue;
    }
    if (ch === '"' || ch === '\'') {
      quote = ch;
      cur += ch;
    } else if (ch === '.') {
      parts.push(cur.trim());
      cur = '';
    } else {
      cur += ch;
    }
  }
  if (cur.trim()) {
    parts.push(cur.trim());
  }
  return parts.map(stripTomlQuotes).filter(Boolean);
}

// Classify a TOML `[header]` as:
// - `{ kind: 'normal-dep', subpkg?: string }`
// - `{ kind: 'features' }`
// - `{ kind: 'other' }`
function classifyHeader(header) {
  const segments = splitTomlPath(header);
  if (segments.length === 1 && segments[0] === 'features') {
    return { kind: 'features' };
  }
  if (segments[0] === 'dependencies') {
    if (segments.length === 1) {
      return { kind: 'normal-dep', subpkg: null };
    }
    if (segments.length === 2) {
      return { kind: 'normal-dep', subpkg: segments[1] };
    }
    return { kind: 'other' };
  }
  // Matches `[target.<cfg>.dependencies]` or `[target.<cfg>.dependencies.<pkg>]`
  // while excluding `dev-dependencies` and `build-dependencies`.
  if (segments[0] === 'target' && segments.length >= 3) {
    if (segments[segments.length - 1] === 'dependencies') {
      return { kind: 'normal-dep', subpkg: null };
    }
    if (segments.length >= 4 && segments[segments.length - 2] === 'dependencies') {
      return { kind: 'normal-dep', subpkg: segments[segments.length - 1] };
    }
  }
  return { kind: 'other' };
}

function classifyWorkspaceHeader(header) {
  const segments = splitTomlPath(header);
  if (segments.length === 2 && segments[0] === 'workspace' && segments[1] === 'dependencies') {
    return { kind: 'workspace-dep', subpkg: null };
  }
  if (segments.length === 3 && segments[0] === 'workspace' && segments[1] === 'dependencies') {
    return { kind: 'workspace-dep', subpkg: segments[2] };
  }
  return { kind: 'other' };
}

function updateDepRecordFromStatement(record, rhsOrSubkey, rhsValue) {
  if (rhsOrSubkey === 'package') {
    const strMatch = /^(['"])([A-Za-z0-9_-]+)\1$/.exec(rhsValue.trim());
    if (strMatch) record.explicitPackage = strMatch[2];
  } else if (rhsOrSubkey === 'path') {
    const strMatch = /^(['"])([^'"]+)\1$/.exec(rhsValue.trim());
    if (strMatch) record.path = strMatch[2];
  } else if (rhsOrSubkey === 'workspace') {
    if (/^true\b/.test(rhsValue.trim())) record.usesWorkspace = true;
  } else if (rhsOrSubkey === 'optional') {
    if (/^true\b/.test(rhsValue.trim())) record.optional = true;
  } else if (rhsOrSubkey === 'default-features' || rhsOrSubkey === 'default_features') {
    if (/^false\b/.test(rhsValue.trim())) record.defaultFeatures = false;
  } else if (rhsOrSubkey === null) {
    const pkgMatch = /\bpackage\s*=\s*(['"])([A-Za-z0-9_-]+)\1/.exec(rhsValue);
    if (pkgMatch) record.explicitPackage = pkgMatch[2];
    const pathMatch = /\bpath\s*=\s*(['"])([^'"]+)\1/.exec(rhsValue);
    if (pathMatch) record.path = pathMatch[2];
    if (/\bworkspace\s*=\s*true\b/.test(rhsValue)) record.usesWorkspace = true;
    if (/\boptional\s*=\s*true\b/.test(rhsValue)) record.optional = true;
    if (/\bdefault[-_]features\s*=\s*false\b/.test(rhsValue)) record.defaultFeatures = false;
  }
}

function parseDottedAssignment(text) {
  let eqIdx = -1;
  let quote = null;
  let escaped = false;
  for (let i = 0; i < text.length; i += 1) {
    const ch = text[i];
    if (escaped) {
      escaped = false;
      continue;
    }
    if (quote === '"') {
      if (ch === '\\') escaped = true;
      else if (ch === '"') quote = null;
      continue;
    }
    if (quote === '\'') {
      if (ch === '\'') quote = null;
      continue;
    }
    if (ch === '"' || ch === '\'') {
      quote = ch;
      continue;
    }
    if (ch === '=') {
      eqIdx = i;
      break;
    }
  }
  if (eqIdx === -1) return null;
  const lhs = text.slice(0, eqIdx).trim();
  const rhs = text.slice(eqIdx + 1).trim();
  const segments = splitTomlPath(lhs);
  if (segments.length !== 1 && segments.length !== 2) return null;
  for (const seg of segments) {
    if (!/^[A-Za-z0-9_-]+$/.test(seg)) return null;
  }
  return { key: segments[0], subkey: segments[1] ?? null, rhs };
}

function extractWorkspaceDepAliases(rootDir) {
  const rootManifestPath = path.join(rootDir, 'Cargo.toml');
  if (!existsSync(rootManifestPath)) return new Map();

  const raw = readFileSync(rootManifestPath, 'utf8');
  const statements = collectTomlStatements(raw);
  const byAlias = new Map();

  const ensureRecord = (alias, line) => {
    if (!byAlias.has(alias)) {
      byAlias.set(alias, {
        alias,
        line,
        explicitPackage: null,
        path: null,
      });
    }
    return byAlias.get(alias);
  };

  for (const stmt of statements) {
    const ws = classifyWorkspaceHeader(stmt.header);
    if (ws.kind !== 'workspace-dep') continue;
    if (stmt.kind === 'header') {
      if (ws.subpkg) ensureRecord(ws.subpkg, stmt.line);
      continue;
    }
    const parsed = parseDottedAssignment(stmt.text);
    if (!parsed) continue;
    if (ws.subpkg) {
      const record = ensureRecord(ws.subpkg, stmt.line);
      if (parsed.subkey === null) {
        updateDepRecordFromStatement(record, parsed.key, parsed.rhs);
      }
    } else {
      const record = ensureRecord(parsed.key, stmt.line);
      updateDepRecordFromStatement(record, parsed.subkey, parsed.rhs);
    }
  }

  const aliases = new Map();
  for (const [alias, record] of byAlias.entries()) {
    let resolved = record.explicitPackage;
    if (!resolved && record.path) {
      const targetManifest = path.join(rootDir, record.path, 'Cargo.toml');
      if (existsSync(targetManifest)) {
        resolved = extractPackageName(readFileSync(targetManifest, 'utf8'), alias);
      }
    }
    aliases.set(alias, resolved ?? alias);
  }
  return aliases;
}

function checkManifest(
  relPath,
  raw,
  forbiddenSet,
  workspaceAliases,
  metadataNormalDeps,
  violations,
) {
  const isEngine = relPath === 'crates/cc-lb-engine/Cargo.toml';
  if (isEngine) {
    for (const stmt of collectTomlStatements(raw)) {
      if (stmt.kind === 'entry' && classifyHeader(stmt.header).kind === 'features') {
        const keyMatch = /^([A-Za-z0-9_-]+)\s*=/.exec(stmt.text);
        if (keyMatch && keyMatch[1] === 'postgres') {
          violations.push(`${relPath}:${stmt.line}: forbidden feature \`postgres\` in cc-lb-engine`);
        }
      }
    }
  }

  const reportedDirect = new Set();
  for (const dep of extractNormalDeps(raw, workspaceAliases)) {
    if (forbiddenSet.has(dep.name)) {
      reportedDirect.add(dep.name);
      const aliasNote = dep.rawKey !== dep.name ? ` (aliased as \`${dep.rawKey}\`)` : '';
      violations.push(
        `${relPath}:${dep.line}: forbidden normal dependency \`${dep.name}\`${aliasNote}`,
      );
    }
  }

  const pkgName = extractPackageName(raw, path.basename(path.dirname(relPath)));
  for (const depName of metadataNormalDeps?.get(pkgName) ?? []) {
    if (forbiddenSet.has(depName) && !reportedDirect.has(depName)) {
      reportedDirect.add(depName);
      violations.push(
        `${relPath}:1: forbidden normal dependency \`${depName}\` (discovered via cargo metadata)`,
      );
    }
  }

  return reportedDirect;
}

function extractPackageName(raw, fallbackName) {
  const lines = raw.split(/\r?\n/);
  let section = '';
  for (const line of lines) {
    const code = stripTomlComment(line).trim();
    if (!code) continue;
    if (code.startsWith('[') && !code.startsWith('[[')) {
      const end = code.indexOf(']');
      if (end !== -1) section = code.slice(1, end).trim();
      continue;
    }
    if (section === 'package') {
      const m = /^name\s*=\s*"([A-Za-z0-9_-]+)"/.exec(code);
      if (m) return m[1];
    }
  }
  return fallbackName;
}

function extractNormalDeps(raw, workspaceAliases = new Map()) {
  const statements = collectTomlStatements(raw);
  const byKey = new Map();
  const order = [];

  const ensureRecord = (rawKey, line) => {
    if (!byKey.has(rawKey)) {
      const record = {
        rawKey,
        line,
        explicitPackage: null,
        usesWorkspace: false,
        optional: false,
        defaultFeatures: true,
      };
      byKey.set(rawKey, record);
      order.push(record);
    }
    return byKey.get(rawKey);
  };

  for (const stmt of statements) {
    const section = classifyHeader(stmt.header);
    if (section.kind !== 'normal-dep') continue;

    if (stmt.kind === 'header') {
      if (section.subpkg) {
        ensureRecord(section.subpkg, stmt.line);
      }
      continue;
    }

    const parsed = parseDottedAssignment(stmt.text);
    if (!parsed) continue;
    if (section.subpkg) {
      const record = ensureRecord(section.subpkg, stmt.line);
      if (parsed.subkey === null) {
        updateDepRecordFromStatement(record, parsed.key, parsed.rhs);
      }
    } else {
      const record = ensureRecord(parsed.key, stmt.line);
      updateDepRecordFromStatement(record, parsed.subkey, parsed.rhs);
    }
  }

  return order.map((record) => {
    let resolvedName = record.rawKey;
    if (record.explicitPackage) {
      resolvedName = record.explicitPackage;
    } else if (record.usesWorkspace && workspaceAliases.has(record.rawKey)) {
      resolvedName = workspaceAliases.get(record.rawKey);
    }
    return {
      name: resolvedName,
      rawKey: record.rawKey,
      line: record.line,
      optional: record.optional,
      usesWorkspace: record.usesWorkspace,
      defaultFeatures: record.defaultFeatures,
    };
  });
}

function loadCargoMetadataNormalDeps(rootDir) {
  const manifestPath = path.join(rootDir, 'Cargo.toml');
  const lockPath = path.join(rootDir, 'Cargo.lock');
  if (!existsSync(manifestPath) || !existsSync(lockPath)) {
    return null;
  }
  const result = spawnSync(
    'cargo',
    ['metadata', '--no-deps', '--offline', '--format-version', '1', '--manifest-path', manifestPath],
    { encoding: 'utf8', maxBuffer: 64 * 1024 * 1024 },
  );
  if (result.error || result.status !== 0 || !result.stdout) {
    return null;
  }
  let parsed;
  try {
    parsed = JSON.parse(result.stdout);
  } catch {
    return null;
  }
  const map = new Map();
  for (const pkg of parsed.packages ?? []) {
    const normal = [];
    for (const dep of pkg.dependencies ?? []) {
      if (dep.kind === null || dep.kind === undefined) {
        normal.push(dep.name);
      }
    }
    map.set(pkg.name, normal);
  }
  return map;
}

function discoverWorkspaceManifestRelPaths(rootDir) {
  const manifests = new Set();
  for (const parentRel of ['crates', 'tools', 'plugins']) {
    const parentDir = path.join(rootDir, parentRel);
    if (!existsSync(parentDir)) continue;
    for (const entry of readdirSync(parentDir, { withFileTypes: true })) {
      if (!entry.isDirectory()) continue;
      const candidate = path.posix.join(parentRel, entry.name, 'Cargo.toml');
      if (existsSync(path.join(rootDir, candidate))) {
        manifests.add(candidate);
      }
    }
  }

  const rootManifestPath = path.join(rootDir, 'Cargo.toml');
  if (existsSync(rootManifestPath)) {
    const rootRaw = readFileSync(rootManifestPath, 'utf8');
    for (const stmt of collectTomlStatements(rootRaw)) {
      if (stmt.kind === 'entry' && stmt.header.trim() === 'workspace') {
        const membersMatch = /^members\s*=\s*\[([\s\S]*)\]$/.exec(stmt.text);
        if (membersMatch) {
          for (const itemMatch of membersMatch[1].matchAll(/(['"])([^'"]+)\1/g)) {
            const pattern = itemMatch[2].replace(/\/+$/, '');
            if (pattern.endsWith('/*')) {
              const baseRel = pattern.slice(0, -2);
              const baseDir = path.join(rootDir, baseRel);
              if (!existsSync(baseDir)) continue;
              for (const entry of readdirSync(baseDir, { withFileTypes: true })) {
                if (!entry.isDirectory()) continue;
                const candidate = path.posix.join(baseRel, entry.name, 'Cargo.toml');
                if (existsSync(path.join(rootDir, candidate))) {
                  manifests.add(candidate);
                }
              }
            } else {
              const candidate = path.posix.join(pattern, 'Cargo.toml');
              if (existsSync(path.join(rootDir, candidate))) {
                manifests.add(candidate);
              }
            }
          }
        }
      }
    }
  }

  for (const relPath of FORBIDDEN_NORMAL_DEPS.keys()) {
    if (existsSync(path.join(rootDir, relPath))) manifests.add(relPath);
  }
  for (const relPath of FORBIDDEN_TRANSITIVE_NORMAL_DEPS.keys()) {
    if (existsSync(path.join(rootDir, relPath))) manifests.add(relPath);
  }

  return Array.from(manifests);
}

function checkTransitiveDeps(
  rootDir,
  workspaceAliases,
  metadataNormalDeps,
  reportedDirectByRel,
  violations,
) {
  const relPaths = discoverWorkspaceManifestRelPaths(rootDir);

  const depsByCrate = new Map();
  const relByCrate = new Map();
  const crateByRel = new Map();

  for (const relPath of relPaths) {
    const absPath = path.join(rootDir, relPath);
    const raw = readFileSync(absPath, 'utf8');
    const pkgName = extractPackageName(raw, path.basename(path.dirname(relPath)));
    depsByCrate.set(pkgName, extractNormalDeps(raw, workspaceAliases));
    relByCrate.set(pkgName, relPath);
    crateByRel.set(relPath, pkgName);
  }

  // Enrich with `cargo metadata` for every workspace package returned by Cargo
  // (including non-`crates/` packages such as `tools/workspace-hack`), while
  // keeping starting points scoped to FORBIDDEN_TRANSITIVE_NORMAL_DEPS.
  if (metadataNormalDeps) {
    for (const [pkgName, metaDeps] of metadataNormalDeps.entries()) {
      let existing = depsByCrate.get(pkgName);
      if (!existing) {
        existing = [];
        depsByCrate.set(pkgName, existing);
      }
      const seen = new Set(existing.map((d) => d.name));
      for (const depName of metaDeps) {
        if (!seen.has(depName)) {
          seen.add(depName);
          existing.push({ name: depName, rawKey: depName, line: 1, optional: false, usesWorkspace: false });
        }
      }
    }
  }

  for (const [relPath, forbiddenSet] of FORBIDDEN_TRANSITIVE_NORMAL_DEPS.entries()) {
    const startCrate = crateByRel.get(relPath);
    if (!startCrate) continue;
    const directDeps = depsByCrate.get(startCrate) ?? [];
    const reportedDirect = reportedDirectByRel.get(relPath) ?? new Set();
    const visited = new Set([startCrate]);
    const reported = new Set();
    const queue = [];

    for (const edge of directDeps) {
      queue.push({ crate: edge.name, directLine: edge.line, chain: [startCrate, edge.name] });
    }

    while (queue.length > 0) {
      const { crate, directLine, chain } = queue.shift();
      if (chain.length > 2 && forbiddenSet.has(crate) && !reported.has(crate)) {
        reported.add(crate);
        violations.push(
          `${relPath}:${directLine}: forbidden transitive normal dependency \`${crate}\` via \`${chain.join(' -> ')}\``,
        );
      } else if (
        chain.length === 2 &&
        !reportedDirect.has(crate) &&
        forbiddenSet.has(crate) &&
        !reported.has(crate)
      ) {
        reported.add(crate);
        // Caught if forbiddenSet is stricter than direct FORBIDDEN_NORMAL_DEPS or discovered via metadata.
        violations.push(
          `${relPath}:${directLine}: forbidden normal dependency \`${crate}\` via \`${chain.join(' -> ')}\``,
        );
      }
      if (visited.has(crate)) continue;
      visited.add(crate);
      for (const next of depsByCrate.get(crate) ?? []) {
        queue.push({ crate: next.name, directLine, chain: [...chain, next.name] });
      }
    }
  }
}

function extractFeatureEntries(raw) {
  const features = new Map();
  for (const stmt of collectTomlStatements(raw)) {
    if (stmt.kind === 'entry' && classifyHeader(stmt.header).kind === 'features') {
      const parsed = parseDottedAssignment(stmt.text);
      if (!parsed || parsed.subkey !== null) continue;
      features.set(parsed.key, {
        line: stmt.line,
        rhs: parsed.rhs,
        tokens: Array.from(parsed.rhs.matchAll(/(['"])([^'"]+)\1/g), (m) => m[2]),
      });
    }
  }
  return features;
}

function checkSplitBinaryIsolation(rootDir, workspaceAliases, violations) {
  const tryRead = (relPath) => {
    try {
      return readFileSync(path.join(rootDir, relPath), 'utf8');
    } catch {
      return null;
    }
  };

  const serverRel = 'crates/cc-lb-server/Cargo.toml';
  const serverRaw = tryRead(serverRel);
  if (serverRaw !== null) {
    const serverDeps = extractNormalDeps(serverRaw, workspaceAliases);
    const adminDep = serverDeps.find((dep) => dep.name === 'cc-lb-admin');
    if (adminDep && !adminDep.optional) {
      violations.push(
        `${serverRel}:${adminDep.line}: \`cc-lb-admin\` (key \`${adminDep.rawKey}\`) must set \`optional = true\` so \`cc-lb-proxy\` stays isolated`,
      );
    }
    const adminKey = adminDep?.rawKey ?? 'cc-lb-admin';
    const features = extractFeatureEntries(serverRaw);
    for (const featureName of ['storage-sqlite', 'storage-postgres']) {
      const entry = features.get(featureName);
      if (!entry) continue;
      for (const token of entry.tokens) {
        if (
          token === 'admin' ||
          token === 'embed-spa' ||
          token === `dep:${adminKey}` ||
          token.startsWith(`${adminKey}/`) ||
          token.startsWith(`${adminKey}?/`)
        ) {
          violations.push(
            `${serverRel}:${entry.line}: split proxy storage feature \`${featureName}\` must not enable \`${token}\``,
          );
        }
      }
    }
  }

  const proxyRel = 'crates/cc-lb-proxy/Cargo.toml';
  const proxyRaw = tryRead(proxyRel);
  if (proxyRaw !== null) {
    const proxyDeps = extractNormalDeps(proxyRaw, workspaceAliases);
    const serverDep = proxyDeps.find((dep) => dep.name === 'cc-lb-server');
    if (serverDep && serverDep.defaultFeatures !== false) {
      violations.push(
        `${proxyRel}:${serverDep.line}: \`cc-lb-server\` (key \`${serverDep.rawKey}\`) must set \`default-features = false\``,
      );
    }
    const serverKey = serverDep?.rawKey ?? 'cc-lb-server';
    const forbiddenProxyTokens = new Set([
      'admin',
      'embed-spa',
      `${serverKey}/admin`,
      `${serverKey}/embed-spa`,
      `${serverKey}/sqlite`,
      `${serverKey}/postgres`,
      'dep:cc-lb-admin',
    ]);
    for (const [featureName, entry] of extractFeatureEntries(proxyRaw).entries()) {
      for (const token of entry.tokens) {
        if (forbiddenProxyTokens.has(token)) {
          violations.push(
            `${proxyRel}:${entry.line}: feature \`${featureName}\` must not enable \`${token}\``,
          );
        }
      }
    }
  }

  const adminServerRel = 'crates/cc-lb-admin-server/Cargo.toml';
  const adminServerRaw = tryRead(adminServerRel);
  if (adminServerRaw !== null) {
    const deps = extractNormalDeps(adminServerRaw, workspaceAliases);
    const adminDep = deps.find((dep) => dep.name === 'cc-lb-admin');
    if (adminDep && adminDep.defaultFeatures !== false) {
      violations.push(
        `${adminServerRel}:${adminDep.line}: \`cc-lb-admin\` (key \`${adminDep.rawKey}\`) must set \`default-features = false\` so \`embed-spa\` stays optional`,
      );
    }
    const adminKey = adminDep?.rawKey ?? 'cc-lb-admin';
    const features = extractFeatureEntries(adminServerRaw);
    const embedSpa = features.get('embed-spa');
    const expectedToken = `${adminKey}/embed-spa`;
    if (!embedSpa || (!embedSpa.tokens.includes(expectedToken) && !embedSpa.tokens.includes(`${adminKey}?/embed-spa`))) {
      violations.push(
        `${adminServerRel}: missing \`embed-spa\` feature gating \`${expectedToken}\``,
      );
    }
    for (const featureName of ['sqlite', 'postgres']) {
      const entry = features.get(featureName);
      if (!entry) continue;
      for (const token of entry.tokens) {
        if (token === 'embed-spa' || token === expectedToken || token === `${adminKey}?/embed-spa`) {
          violations.push(
            `${adminServerRel}:${entry.line}: backend feature \`${featureName}\` must not force \`${token}\``,
          );
        }
      }
    }
  }
}

function checkAdminEmbedSpaOptional(relPath, raw, workspaceAliases, violations) {
  let embedSpaRhs = null;
  for (const stmt of collectTomlStatements(raw)) {
    if (stmt.kind === 'entry' && classifyHeader(stmt.header).kind === 'features') {
      const m = /^embed-spa\s*=\s*(.+)$/.exec(stmt.text);
      if (m) embedSpaRhs = m[1];
    }
  }

  const rustEmbedDeps = extractNormalDeps(raw, workspaceAliases).filter(
    (dep) => dep.name === 'rust-embed',
  );
  for (const dep of rustEmbedDeps) {
    if (!dep.optional) {
      violations.push(
        `${relPath}:${dep.line}: \`rust-embed\` (key \`${dep.rawKey}\`) must set \`optional = true\` behind \`embed-spa\``,
      );
    }
    if (!embedSpaRhs || !embedSpaRhs.includes(`dep:${dep.rawKey}`)) {
      violations.push(
        `${relPath}:${dep.line}: \`embed-spa\` feature must gate \`dep:${dep.rawKey}\``,
      );
    }
  }

  if (rustEmbedDeps.length === 0 && (!embedSpaRhs || !embedSpaRhs.includes('dep:rust-embed'))) {
    violations.push(
      `${relPath}: missing \`embed-spa\` feature gating \`dep:rust-embed\``,
    );
  }
}

export function checkHotpathFirewall(rootDir = ROOT) {
  const violations = [];
  const workspaceAliases = extractWorkspaceDepAliases(rootDir);
  const metadataNormalDeps = loadCargoMetadataNormalDeps(rootDir);
  const reportedDirectByRel = new Map();

  for (const [relPath, forbiddenSet] of FORBIDDEN_NORMAL_DEPS.entries()) {
    let raw;
    try {
      raw = readFileSync(path.join(rootDir, relPath), 'utf8');
    } catch {
      continue;
    }
    const reportedDirect = checkManifest(
      relPath,
      raw,
      forbiddenSet,
      workspaceAliases,
      metadataNormalDeps,
      violations,
    );
    reportedDirectByRel.set(relPath, reportedDirect);
  }

  checkTransitiveDeps(rootDir, workspaceAliases, metadataNormalDeps, reportedDirectByRel, violations);

  const adminRel = 'crates/cc-lb-admin/Cargo.toml';
  try {
    const raw = readFileSync(path.join(rootDir, adminRel), 'utf8');
    checkAdminEmbedSpaOptional(adminRel, raw, workspaceAliases, violations);
  } catch {
    // Missing only inside synthetic test fixtures that omit cc-lb-admin.
  }

  checkSplitBinaryIsolation(rootDir, workspaceAliases, violations);

  const eventBusRel = 'crates/cc-lb-control/src/event_bus.rs';
  try {
    const raw = readFileSync(path.join(rootDir, eventBusRel), 'utf8');
    const lines = raw.split(/\r?\n/);
    for (let i = 0; i < lines.length; i += 1) {
      const code = stripRustLineComment(lines[i]);
      if (/Mutex\s*<\s*Option\s*<\s*mpsc\s*::\s*Sender\s*<\s*LifecycleEvent\s*>\s*>\s*>/.test(code)) {
        violations.push(
          `${eventBusRel}:${i + 1}: forbidden subscriber Mutex<Option<mpsc::Sender<LifecycleEvent>>> (keep InMemoryBus lock-free via ArcSwap)`,
        );
      }
    }
  } catch {
    // Missing only inside synthetic test fixtures that omit event_bus.rs.
  }

  return { ok: violations.length === 0, violations };
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const result = checkHotpathFirewall(ROOT);
  if (!result.ok) {
    for (const violation of result.violations) {
      console.error(`HOTPATH FIREWALL VIOLATION: ${violation}`);
    }
    process.exit(1);
  }
  console.log('Hotpath dependency firewall check passed (0 violations).');
}
