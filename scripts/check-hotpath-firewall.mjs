#!/usr/bin/env node
// Enforces the Phase-2 hot-path dependency & lock-free event-bus firewall:
// 1. Hot-path & pure evaluation crates must never pull heavy/off-path crates
//    into normal `[dependencies]` (dev-dependencies remain allowed).
// 2. `cc-lb-engine` must never re-introduce a `postgres` feature.
// 3. `InMemoryBus` (`crates/cc-lb-control/src/event_bus.rs`) must never
//    re-introduce `Mutex<Option<mpsc::Sender<LifecycleEvent>>>`.
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

function checkManifest(relPath, raw, forbiddenSet, violations) {
  const lines = raw.split(/\r?\n/);
  let section = { kind: 'other' };
  let isEngine = relPath === 'crates/cc-lb-engine/Cargo.toml';

  for (let i = 0; i < lines.length; i += 1) {
    const code = stripTomlComment(lines[i]).trim();
    if (!code) continue;

    if (code.startsWith('[') && !code.startsWith('[[')) {
      const end = code.indexOf(']');
      if (end !== -1) {
        section = classifyHeader(code.slice(1, end));
        if (section.kind === 'normal-dep' && section.subpkg && forbiddenSet.has(section.subpkg)) {
          violations.push(
            `${relPath}:${i + 1}: forbidden normal dependency table [..dependencies.${section.subpkg}]`,
          );
        }
        continue;
      }
    }

    if (section.kind === 'features' && isEngine) {
      const keyMatch = /^([A-Za-z0-9_-]+)\s*=/.exec(code);
      if (keyMatch && keyMatch[1] === 'postgres') {
        violations.push(`${relPath}:${i + 1}: forbidden feature \`postgres\` in cc-lb-engine`);
      }
      continue;
    }

    if (section.kind === 'normal-dep') {
      if (section.subpkg) {
        const pkgMatch = /\bpackage\s*=\s*"([A-Za-z0-9_-]+)"/.exec(code);
        if (pkgMatch && forbiddenSet.has(pkgMatch[1])) {
          violations.push(
            `${relPath}:${i + 1}: forbidden normal dependency package \`${pkgMatch[1]}\``,
          );
        }
        continue;
      }
      const depMatch = /^([A-Za-z0-9_-]+)(?:\.[A-Za-z0-9_.-]+)?\s*=/.exec(code);
      if (depMatch && forbiddenSet.has(depMatch[1])) {
        violations.push(
          `${relPath}:${i + 1}: forbidden normal dependency \`${depMatch[1]}\``,
        );
      }
      const renamedMatch = /\bpackage\s*=\s*"([A-Za-z0-9_-]+)"/.exec(code);
      if (renamedMatch && forbiddenSet.has(renamedMatch[1])) {
        violations.push(
          `${relPath}:${i + 1}: forbidden renamed normal dependency \`${renamedMatch[1]}\``,
        );
      }
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

function extractNormalDeps(raw) {
  const lines = raw.split(/\r?\n/);
  let section = { kind: 'other' };
  const deps = [];

  for (let i = 0; i < lines.length; i += 1) {
    const code = stripTomlComment(lines[i]).trim();
    if (!code) continue;

    if (code.startsWith('[') && !code.startsWith('[[')) {
      const end = code.indexOf(']');
      if (end !== -1) {
        section = classifyHeader(code.slice(1, end));
        if (section.kind === 'normal-dep' && section.subpkg) {
          deps.push({ name: section.subpkg, line: i + 1, subtable: true });
        }
        continue;
      }
    }

    if (section.kind !== 'normal-dep') continue;
    if (section.subpkg) {
      const pkgMatch = /\bpackage\s*=\s*"([A-Za-z0-9_-]+)"/.exec(code);
      if (pkgMatch) {
        const last = deps.at(-1);
        if (last && last.subtable) last.name = pkgMatch[1];
      }
      continue;
    }

    const depMatch = /^([A-Za-z0-9_-]+)(?:\.[A-Za-z0-9_.-]+)?\s*=/.exec(code);
    if (depMatch) {
      const renamedMatch = /\bpackage\s*=\s*"([A-Za-z0-9_-]+)"/.exec(code);
      deps.push({ name: renamedMatch ? renamedMatch[1] : depMatch[1], line: i + 1 });
    }
  }
  return deps;
}

function checkTransitiveDeps(rootDir, violations) {
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
    depsByCrate.set(pkgName, extractNormalDeps(raw));
    relByCrate.set(pkgName, relPath);
    crateByRel.set(relPath, pkgName);
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

  for (const [relPath, forbiddenSet] of FORBIDDEN_NORMAL_DEPS.entries()) {
    let raw;
    try {
      raw = readFileSync(path.join(rootDir, relPath), 'utf8');
    } catch {
      continue;
    }
    checkManifest(relPath, raw, forbiddenSet, violations);
  }

  checkTransitiveDeps(rootDir, violations);

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
