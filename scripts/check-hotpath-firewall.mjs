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
    'crates/cc-lb-event-pipeline/Cargo.toml',
    new Set([
      'cc-lb-admin',
      'cc-lb-server',
      'cc-lb-scheduler',
      'axum',
      'hyper',
      'rustls',
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
      'tiktoken-rs',
      'opentelemetry',
    ]),
  ],
]);

export const FORBIDDEN_TRANSITIVE_NORMAL_DEPS = new Map([
  [
    'crates/cc-lb-engine/Cargo.toml',
    new Set([
      'sqlx',
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
    'crates/cc-lb-event-pipeline/Cargo.toml',
    new Set(['cc-lb-admin', 'cc-lb-server', 'cc-lb-scheduler']),
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

// Classify a TOML `[header]` as:
// - `{ kind: 'normal-dep', subpkg?: string }`
// - `{ kind: 'features' }`
// - `{ kind: 'other' }`
function classifyHeader(header) {
  const name = header.trim();
  if (name === 'features') {
    return { kind: 'features' };
  }
  if (name === 'dependencies') {
    return { kind: 'normal-dep', subpkg: null };
  }
  if (name.startsWith('dependencies.')) {
    return { kind: 'normal-dep', subpkg: name.slice('dependencies.'.length).trim() };
  }
  // Matches `[target.<cfg>.dependencies]` or `[target.<cfg>.dependencies.<pkg>]`
  // while excluding `dev-dependencies` and `build-dependencies`.
  const targetDeps = /^target\.(?:.+?)\.(dependencies(?:\.([A-Za-z0-9_-]+))?)$/.exec(name);
  if (targetDeps) {
    return { kind: 'normal-dep', subpkg: targetDeps[2] ?? null };
  }
  return { kind: 'other' };
}

function classifyWorkspaceHeader(header) {
  const name = header.trim();
  if (name === 'workspace.dependencies') {
    return { kind: 'workspace-dep', subpkg: null };
  }
  if (name.startsWith('workspace.dependencies.')) {
    return {
      kind: 'workspace-dep',
      subpkg: name.slice('workspace.dependencies.'.length).trim(),
    };
  }
  return { kind: 'other' };
}

function updateDepRecordFromStatement(record, rhsOrSubkey, rhsValue) {
  if (rhsOrSubkey === 'package') {
    const strMatch = /^"([A-Za-z0-9_-]+)"$/.exec(rhsValue.trim());
    if (strMatch) record.explicitPackage = strMatch[1];
  } else if (rhsOrSubkey === 'path') {
    const strMatch = /^"([^"]+)"$/.exec(rhsValue.trim());
    if (strMatch) record.path = strMatch[1];
  } else if (rhsOrSubkey === 'workspace') {
    if (/^true\b/.test(rhsValue.trim())) record.usesWorkspace = true;
  } else if (rhsOrSubkey === 'optional') {
    if (/^true\b/.test(rhsValue.trim())) record.optional = true;
  } else if (rhsOrSubkey === null) {
    const pkgMatch = /\bpackage\s*=\s*"([A-Za-z0-9_-]+)"/.exec(rhsValue);
    if (pkgMatch) record.explicitPackage = pkgMatch[1];
    const pathMatch = /\bpath\s*=\s*"([^"]+)"/.exec(rhsValue);
    if (pathMatch) record.path = pathMatch[1];
    if (/\bworkspace\s*=\s*true\b/.test(rhsValue)) record.usesWorkspace = true;
    if (/\boptional\s*=\s*true\b/.test(rhsValue)) record.optional = true;
  }
}

function parseDottedAssignment(text) {
  const eqIdx = text.indexOf('=');
  if (eqIdx === -1) return null;
  const lhs = text.slice(0, eqIdx).trim();
  const rhs = text.slice(eqIdx + 1).trim();
  const match = /^([A-Za-z0-9_-]+)(?:\.([A-Za-z0-9_-]+))?$/.exec(lhs);
  if (!match) return null;
  return { key: match[1], subkey: match[2] ?? null, rhs };
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

function checkManifest(relPath, raw, forbiddenSet, workspaceAliases, violations) {
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

  for (const dep of extractNormalDeps(raw, workspaceAliases)) {
    if (forbiddenSet.has(dep.name)) {
      const aliasNote = dep.rawKey !== dep.name ? ` (aliased as \`${dep.rawKey}\`)` : '';
      violations.push(
        `${relPath}:${dep.line}: forbidden normal dependency \`${dep.name}\`${aliasNote}`,
      );
    }
  }
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

function checkTransitiveDeps(rootDir, workspaceAliases, violations) {
  const cratesDir = path.join(rootDir, 'crates');
  if (!existsSync(cratesDir)) return;

  const depsByCrate = new Map();
  const relByCrate = new Map();
  const crateByRel = new Map();

  for (const entry of readdirSync(cratesDir, { withFileTypes: true })) {
    if (!entry.isDirectory()) continue;
    const relPath = `crates/${entry.name}/Cargo.toml`;
    const absPath = path.join(rootDir, relPath);
    if (!existsSync(absPath)) continue;
    const raw = readFileSync(absPath, 'utf8');
    const pkgName = extractPackageName(raw, entry.name);
    depsByCrate.set(pkgName, extractNormalDeps(raw, workspaceAliases));
    relByCrate.set(pkgName, relPath);
    crateByRel.set(relPath, pkgName);
  }

  // Enrich with `cargo metadata` when a full locked workspace and `cargo` are available.
  const metadataNormalDeps = loadCargoMetadataNormalDeps(rootDir);
  if (metadataNormalDeps) {
    for (const [pkgName, metaDeps] of metadataNormalDeps.entries()) {
      const existing = depsByCrate.get(pkgName);
      if (!existing) continue;
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
    const directForbidden = FORBIDDEN_NORMAL_DEPS.get(relPath) ?? new Set();
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
      } else if (chain.length === 2 && !directForbidden.has(crate) && forbiddenSet.has(crate)) {
        // Caught if forbiddenSet is stricter than direct FORBIDDEN_NORMAL_DEPS.
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

export function checkHotpathFirewall(rootDir = ROOT) {
  const violations = [];
  const workspaceAliases = extractWorkspaceDepAliases(rootDir);

  for (const [relPath, forbiddenSet] of FORBIDDEN_NORMAL_DEPS.entries()) {
    let raw;
    try {
      raw = readFileSync(path.join(rootDir, relPath), 'utf8');
    } catch {
      continue;
    }
    checkManifest(relPath, raw, forbiddenSet, workspaceAliases, violations);
  }

  checkTransitiveDeps(rootDir, workspaceAliases, violations);

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
