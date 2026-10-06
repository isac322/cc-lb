#!/usr/bin/env bun
// Sync and verify declared public-metadata fields against positioning.yml.
//
// positioning.yml is the single source of truth for public identity (name,
// tagline, About/registry descriptions), homepage, license, and keywords.
// This tool compares only the DECLARED fields below — literal body copy,
// H1 wording beyond the product name, per-page SEO descriptions, and per-crate
// technical descriptions are intentionally out of scope (see README: surfaces
// may carry their own prose).
//
//   bun scripts/public-metadata.mjs          # rewrite drifted syncable fields
//   bun scripts/public-metadata.mjs --check  # verify only, zero writes, exit 1 on drift
//
// Requires Bun (Bun.YAML parses the SSOT canonically). Surface field values
// are still extracted and rewritten with anchored patterns, so surrounding
// file content is never restyled.
// CC_LB_REPO_ROOT overrides the repository root (used for scratch probes).

import { existsSync, readdirSync, readFileSync, writeFileSync } from 'node:fs';
import path from 'node:path';

const checkOnly = process.argv.includes('--check');
const verbose = process.argv.includes('--verbose');
const repoRoot = process.env.CC_LB_REPO_ROOT
  ? path.resolve(process.env.CC_LB_REPO_ROOT)
  : path.resolve(import.meta.dir, '..');

function readText(rel) {
  const p = path.join(repoRoot, rel);
  if (!existsSync(p)) return null;
  try {
    return readFileSync(p, 'utf8');
  } catch {
    return ''; // directories aggregate over their own roots in check()
  }
}

const failures = [];
const fatal = (msg) => {
  failures.push({ fatal: msg });
};

function requireText(rel) {
  const t = readText(rel);
  if (t === null) fatal(`${rel}: file not found`);
  return t ?? '';
}

// ---------- positioning.yml (canonical parse; fails closed) ----------

const P = Bun.YAML.parse(requireText('positioning.yml')) ?? {};

function req(where, value) {
  if (value === undefined || value === null || value === '') {
    fatal(`positioning.yml: missing '${where}'`);
  }
  return value;
}

function reqList(where, value) {
  req(where, value);
  if (!Array.isArray(value)) {
    fatal(`positioning.yml: '${where}' is not a list`);
    return [];
  }
  return value.map(String);
}

// Per-crate registry descriptions/keywords: only keys declared in
// positioning.yml are compared; crates without an override keep their own
// technical description (never flattened to the shared one-liner).
function nonDefault(map, where) {
  if (map === undefined || map === null) return new Map();
  if (typeof map !== 'object' || Array.isArray(map)) {
    fatal(`positioning.yml: '${where}' is not a map`);
    return new Map();
  }
  return new Map(Object.entries(map).filter(([k]) => k !== 'default'));
}

const counts = req('evidence.counts', P.evidence?.counts);
const publishedCountEntry = Array.isArray(counts)
  ? counts.find((c) => c?.key === 'published_contract_crates')
  : undefined;

const V = {
  name: req('identity.name', P.identity?.name),
  qualifier: req('identity.qualifier', P.identity?.qualifier),
  repo: req('identity.repo', P.identity?.repo),
  homepage: req('identity.homepage', P.identity?.homepage),
  license: req('identity.license', P.identity?.license),
  tagline: req('category.tagline', P.category?.tagline),
  premise: req('category.premise', P.category?.premise),
  about: req('category.about_description', P.category?.about_description),
  metaDesc: req('category.meta_description', P.category?.meta_description),
  registryDescDefault: req(
    'category.registry_description.default',
    P.category?.registry_description?.default,
  ),
  metaSchema: String(req('meta.schema', P.meta?.schema)),
  metaVersion: String(req('meta.version', P.meta?.version)),
  pinnedCommit: String(req('meta.pinned_commit', P.meta?.pinned_commit)),
  githubTopics: reqList('keywords.github_topics', P.keywords?.github_topics),
  registryKeywordsDefault: reqList(
    'keywords.registry_keywords.default',
    P.keywords?.registry_keywords?.default,
  ),
  registryDescByCrate: nonDefault(
    P.category?.registry_description,
    'category.registry_description',
  ),
  registryKeywordsByCrate: nonDefault(
    P.keywords?.registry_keywords,
    'keywords.registry_keywords',
  ),
  publishedCrateCount: Number(
    req('evidence.counts.published_contract_crates', publishedCountEntry?.value),
  ),
  brandGuide: req('brand.guide', P.brand?.guide),
  brandAssets: new Map(Object.entries(req('brand.assets', P.brand?.assets) ?? {})),
};

if (!V.repo || !V.repo.includes('/')) fatal(`identity.repo '${V.repo}' is not owner/name`);

// crates.io/site structured data uses license URLs, not SPDX ids.
const LICENSE_URLS = new Map([
  ['Apache-2.0', 'https://www.apache.org/licenses/LICENSE-2.0'],
]);
if (V.license && !LICENSE_URLS.has(V.license)) {
  fatal(`no license URL mapping declared for '${V.license}' (LICENSE_URLS in this script)`);
}

// Broken SSOT: report every missing required key and stop before any file
// is read for rewriting — a failed sync run must write zero bytes.
if (failures.length) {
  for (const f of failures) console.log(`FAIL ${f.fatal}`);
  process.exit(1);
}

const [repoOwner] = V.repo.split('/');
V.repoUrl = `https://github.com/${V.repo}`;
V.ownerUrl = `https://github.com/${repoOwner}`;
V.homepageBase = V.homepage.replace(/\/$/, '');
V.docsUrl = `${V.homepageBase}/docs/`;
V.pluginsDocsUrl = `${V.homepageBase}/docs/plugins/`;
V.chartIconUrl = `${V.homepageBase}/brand/favicon/favicon.svg`;
V.socialPreviewPath = '/brand/social/cc-lb-social-preview.png';
V.licenseUrl = LICENSE_URLS.get(V.license) ?? '';

// Strip quotes from YAML scalars read by the surface-field patterns below.
function unquote(v) {
  v = v.trim();
  if ((v.startsWith('"') && v.endsWith('"')) || (v.startsWith("'") && v.endsWith("'"))) {
    return v.slice(1, -1);
  }
  return v;
}

// ---------- field evaluation ----------

function yamlScalarRender(v) {
  return /^[A-Za-z0-9][^#:;\n]*$/.test(v) && !/\s$/.test(v) ? v : JSON.stringify(v);
}

function single(re, decode = (s) => s) {
  const d = re.flags.includes('d') ? re : new RegExp(re.source, re.flags + 'd');
  return (text) => {
    const m = d.exec(text);
    if (!m) return { found: false };
    return { found: true, actual: decode(m[1]), span: m.indices[1] };
  };
}

function every(re, decode = (s) => s) {
  return (text) => {
    const g = new RegExp(re.source, re.flags + 'g');
    const hits = [...text.matchAll(g)];
    if (!hits.length) return { found: false };
    const values = hits.map((m) => decode(m[1]));
    return { found: true, actual: [...new Set(values)].join(' | '), values };
  };
}

const jsonDecode = (s) => {
  try {
    return JSON.parse(`"${s}"`);
  } catch {
    return s;
  }
};
const jsonEncode = (s) => JSON.stringify(s).slice(1, -1);

const fields = [];

function field(spec) {
  fields.push({
    mode: 'check',
    decode: (s) => s,
    render: (v) => v,
    ...spec,
    expectOf: spec.expectOf ?? ((V) => spec.expect),
  });
}

// ---- README.md (syncable: declared metadata lines only) ----
field({
  file: 'README.md', name: 'readme.h1', label: 'README H1 product name',
  mode: 'sync', extract: single(/^# (\S[^\n]*)$/m), expectOf: (V) => V.name,
});
field({
  file: 'README.md', name: 'readme.about', label: 'README one-liner under the H1 (About description)',
  mode: 'sync', extract: single(/^# \S[^\n]*\n(?:[ \t]*\n)*(\S[^\n]*)$/m), expectOf: (V) => V.about,
});
field({
  file: 'README.md', name: 'readme.website-link', label: 'README [Website] link target (homepage)',
  mode: 'sync', extract: single(/\[Website\]\((https?:\/\/[^)\s]+)\)/), expectOf: (V) => V.homepage,
});
field({
  file: 'README.md', name: 'readme.license-id', label: 'README license line SPDX id',
  mode: 'sync', extract: single(/licensed under \[([^\]]+)\]\(\.\/LICENSE\)/), expectOf: (V) => V.license,
});

// ---- PRODUCT.md (check-only: prose mirror, never rewritten) ----
field({
  file: 'PRODUCT.md', name: 'product.pointer', label: 'PRODUCT.md source-of-truth pointer (schema/version/pinned commit)',
  check(text) {
    const m = /Source of truth: `positioning\.yml` \(schema (\d+), version ([0-9A-Za-z.\-]+)\)\. Repository evidence is pinned to `([0-9a-f]{7,40})`/.exec(text);
    if (!m) return { found: false };
    const actual = `schema ${m[1]}, version ${m[2]}, commit ${m[3]}`;
    const expected = `schema ${V.metaSchema}, version ${V.metaVersion}, commit ${V.pinnedCommit}`;
    return { found: true, actual, overrideExpected: expected, ok: actual === expected };
  },
});
field({
  file: 'PRODUCT.md', name: 'product.purpose', label: 'PRODUCT.md Purpose line (category premise)',
  check(text) {
    const m = /## Purpose[ \t]*\n(?:[ \t]*\n)*(\S[^\n]*)/.exec(text);
    if (!m) return { found: false };
    return { found: true, actual: m[1].trim(), ok: m[1].trim() === V.premise };
  },
});

// ---- deploy/helm/cc-lb/Chart.yaml (chart metadata fields; version/appVersion
// are owned by the release stamping contract and intentionally not compared) ----
field({
  file: 'deploy/helm/cc-lb/Chart.yaml', name: 'chart.name', label: 'Chart.yaml name',
  extract: single(/^name:[ \t]*(.+?)[ \t]*$/m, unquote), expectOf: (V) => V.name,
});
field({
  file: 'deploy/helm/cc-lb/Chart.yaml', name: 'chart.description', label: 'Chart.yaml description (About description)',
  mode: 'sync', extract: single(/^description:[ \t]*(.+?)[ \t]*$/m, unquote),
  render: yamlScalarRender, expectOf: (V) => V.about,
});
field({
  file: 'deploy/helm/cc-lb/Chart.yaml', name: 'chart.home', label: 'Chart.yaml home (homepage)',
  mode: 'sync', extract: single(/^home:[ \t]*(.+?)[ \t]*$/m, unquote), expectOf: (V) => V.homepage,
});
field({
  file: 'deploy/helm/cc-lb/Chart.yaml', name: 'chart.icon', label: 'Chart.yaml icon URL (homepage brand favicon)',
  mode: 'sync', extract: single(/^icon:[ \t]*(.+?)[ \t]*$/m, unquote), expectOf: (V) => V.chartIconUrl,
});
field({
  file: 'deploy/helm/cc-lb/Chart.yaml', name: 'chart.sources', label: 'Chart.yaml sources list (repository URL)',
  mode: 'sync',
  extract: (text) => {
    const m = /^sources:[ \t]*$\n((?:[ \t]+-[ \t]*[^\n]*\n?)+)/md.exec(text);
    if (!m) return { found: false };
    const list = m[1].split('\n').map((l) => /^[ \t]+-[ \t]*(.*)$/.exec(l)).filter(Boolean).map((mm) => unquote(mm[1]));
    return { found: true, actual: list.join(', '), list, span: m.indices[1] };
  },
  compare: (r, expected) => JSON.stringify(r.list) === JSON.stringify(expected),
  render: (expected) => expected.map((u) => `  - ${u}\n`).join(''),
  expectOf: (V) => [V.repoUrl],
});
field({
  file: 'deploy/helm/cc-lb/Chart.yaml', name: 'chart.keywords', label: 'Chart.yaml keywords (registry keywords)',
  mode: 'sync',
  extract: (text) => {
    const m = /^keywords:[ \t]*$\n((?:[ \t]+-[ \t]*[^\n]*\n?)+)/md.exec(text);
    if (!m) return { found: false };
    const list = m[1].split('\n').map((l) => /^[ \t]+-[ \t]*(.*)$/.exec(l)).filter(Boolean).map((mm) => unquote(mm[1]));
    return { found: true, actual: list.join(', '), list, span: m.indices[1] };
  },
  compare: (r, expected) => JSON.stringify(r.list) === JSON.stringify(expected),
  render: (expected) => expected.map((u) => `  - ${u}\n`).join(''),
  expectOf: (V) => V.registryKeywordsDefault,
});
field({
  file: 'deploy/helm/cc-lb/Chart.yaml', name: 'chart.docs-annotation', label: 'Chart.yaml artifacthub Documentation link (docs URL)',
  extract: single(/^[ \t]+- name: Documentation\n[ \t]+url:[ \t]*(\S+)[ \t]*$/m), expectOf: (V) => V.docsUrl,
});
field({
  file: 'deploy/helm/cc-lb/Chart.yaml', name: 'chart.source-annotation', label: 'Chart.yaml artifacthub Source link (repository URL)',
  extract: single(/^[ \t]+- name: Source\n[ \t]+url:[ \t]*(\S+)[ \t]*$/m), expectOf: (V) => V.repoUrl,
});
field({
  file: 'deploy/helm/cc-lb/Chart.yaml', name: 'chart.maintainer', label: 'Chart.yaml maintainer (repository owner)',
  check(text) {
    const block = /^maintainers:[ \t]*$\n((?:[ \t]+[^\n]*\n?)+)/m.exec(text);
    if (!block) return { found: false };
    const name = /^[ \t]+-[ \t]*name:[ \t]*(.+?)[ \t]*$/m.exec(block[1]);
    const url = /^[ \t]+url:[ \t]*(\S+)[ \t]*$/m.exec(block[1]);
    if (!name) return { found: false };
    const actual = `name: ${unquote(name[1])}${url ? `, url: ${url[1]}` : ''}`;
    const expected = `name: ${repoOwner}, url: ${V.ownerUrl}`;
    return { found: true, actual, overrideExpected: expected, ok: unquote(name[1]) === repoOwner && url?.[1] === V.ownerUrl };
  },
});

// ---- site/public/manifest.webmanifest (syncable JSON scalar fields) ----
for (const [key, expectOf] of [
  ['name', (V) => V.name],
  ['short_name', (V) => V.name],
  ['description', (V) => V.about],
]) {
  field({
    file: 'site/public/manifest.webmanifest', name: `manifest.${key}`,
    label: `webmanifest "${key}"${key === 'description' ? ' (About description)' : ' (product name)'}`,
    mode: 'sync',
    extract: single(new RegExp(`"${key}":\\s*"((?:[^"\\\\]|\\\\.)*)"`, 'd'), jsonDecode),
    render: jsonEncode, expectOf,
  });
}

// ---- site/public/llms.txt (published machine-readable metadata) ----
field({
  file: 'site/public/llms.txt', name: 'llms.about', label: 'llms.txt blockquote (About description)',
  mode: 'sync', extract: single(/^> (\S[^\n]*)$/m), expectOf: (V) => V.about,
});
field({
  file: 'site/public/llms.txt', name: 'llms.self-links', label: 'llms.txt absolute links stay on homepage/repository',
  check(text) {
    const urls = [...text.matchAll(/\((https?:\/\/[^)\s]+)\)/g)].map((m) => m[1]);
    if (!urls.length) return { found: false };
    const bad = urls.filter((u) => !u.startsWith(`${V.homepageBase}/`) && u !== V.homepage && !u.startsWith(`${V.repoUrl}/`) && u !== V.repoUrl);
    return { found: true, actual: bad.length ? `off-canonical: ${bad.join(', ')}` : `${urls.length} links`, ok: bad.length === 0 };
  },
});

// ---- site landing + astro config (check-only: site code outside this gate's
// edit scope; drift requires a manual, owner-visible copy decision) ----
field({
  file: 'site/src/pages/index.astro', name: 'landing.site-url', label: 'landing siteUrl (homepage origin)',
  extract: single(/const siteUrl = '([^']+)'/), expectOf: (V) => V.homepageBase,
});
field({
  file: 'site/src/pages/index.astro', name: 'landing.title', label: 'landing <title> (name + tagline)',
  extract: single(/const title = '([^']+)'/), expectOf: (V) => `${V.name} | ${V.tagline}`,
});
field({
  file: 'site/src/pages/index.astro', name: 'landing.description', label: 'landing meta description (meta_description or About)',
  extract: single(/const description =\s*\n?\s*'([^']+)'/), expectOf: (V) => [V.metaDesc, V.about],
});
field({
  file: 'site/src/pages/index.astro', name: 'landing.code-repository', label: 'landing JSON-LD codeRepository (repository URL)',
  extract: every(/codeRepository: '([^']+)'/), expectOf: (V) => V.repoUrl,
});
field({
  file: 'site/src/pages/index.astro', name: 'landing.license-url', label: 'landing JSON-LD license URL',
  extract: every(/license: '([^']+)'/), expectOf: (V) => V.licenseUrl,
});
field({
  file: 'site/astro.config.mjs', name: 'astro.site-url', label: 'astro config site URL (homepage origin)',
  extract: single(/const siteUrl = '([^']+)'/), expectOf: (V) => V.homepageBase,
});
field({
  file: 'site/astro.config.mjs', name: 'astro.software-description', label: 'SoftwareApplication JSON-LD description (About description)',
  extract: single(/operatingSystem:\s*'[^']*',\s*description:\s*'([^']+)'/), expectOf: (V) => V.about,
});
field({
  file: 'site/astro.config.mjs', name: 'astro.code-repository', label: 'astro JSON-LD codeRepository (repository URL)',
  extract: every(/codeRepository: '([^']+)'/), expectOf: (V) => V.repoUrl,
});
field({
  file: 'site/astro.config.mjs', name: 'astro.license-url', label: 'astro JSON-LD license URLs',
  extract: every(/license: '([^']+)'/), expectOf: (V) => V.licenseUrl,
});
field({
  file: 'site/astro.config.mjs', name: 'astro.docs-title', label: 'Starlight site title (product name)',
  extract: single(/title: '([^']+)'/), expectOf: (V) => `${V.name} documentation`,
});

// ---- brand references (positioning brand.assets + every /brand/ URL used on
// the public site must resolve to a shipped file) ----
field({
  file: 'positioning.yml', name: 'brand.asset-paths', label: 'positioning brand.guide/brand.assets paths exist and are non-empty',
  check() {
    const missing = [];
    const entries = [['brand.guide', V.brandGuide], ...[...V.brandAssets.entries()].map(([k, v]) => [`brand.assets.${k}`, v])];
    for (const [label, rel] of entries) {
      const p = path.join(repoRoot, rel);
      if (!existsSync(p)) {
        missing.push(`${label}=${rel}`);
      } else {
        try {
          if (readdirSync(p).length === 0) missing.push(`${label}=${rel} (empty dir)`);
        } catch { /* file, fine */ }
      }
    }
    return { found: true, actual: missing.length ? `missing: ${missing.join('; ')}` : `${entries.length} paths`, ok: missing.length === 0 };
  },
});
field({
  file: 'site', name: 'brand.site-refs', label: 'every /brand/… reference on the public site resolves in site/public',
  check() {
    const roots = ['site/src/pages/index.astro', 'site/astro.config.mjs', 'site/public/manifest.webmanifest'];
    const missing = [];
    for (const rel of roots) {
      const t = readText(rel);
      if (t === null) { missing.push(`${rel} (file missing)`); continue; }
      for (const m of t.matchAll(/\/brand\/[\w./-]+/g)) {
        const ref = m[0].replace(/[?#].*$/, '');
        if (!existsSync(path.join(repoRoot, 'site/public', ref))) missing.push(`${rel}: ${ref}`);
      }
      for (const m of t.matchAll(/'\.\/(src\/assets\/[\w./-]+)'/g)) {
        if (!existsSync(path.join(repoRoot, 'site', m[1]))) missing.push(`${rel}: ./${m[1]}`);
      }
    }
    return { found: true, actual: missing.length ? `missing: ${missing.join('; ')}` : 'all refs resolve', ok: missing.length === 0 };
  },
});
field({
  file: 'README.md', name: 'brand.readme-hero', label: 'README hero image paths exist',
  check(text) {
    const refs = [...text.matchAll(/(?:srcset|src)="(assets\/[^"]+)"/g)].map((m) => m[1]);
    if (!refs.length) return { found: false };
    const missing = refs.filter((r) => !existsSync(path.join(repoRoot, r)));
    return { found: true, actual: missing.length ? `missing: ${missing.join(', ')}` : `${refs.length} refs`, ok: missing.length === 0 };
  },
});

// ---- GitHub-side metadata kept in-repo (check-only) ----
field({
  file: '.github/ISSUE_TEMPLATE/config.yml', name: 'issue-template.links', label: 'issue template contact links stay on homepage/repository',
  check(text) {
    const urls = [...text.matchAll(/url:[ \t]*(\S+)/g)].map((m) => m[1]);
    if (!urls.length) return { found: false };
    const bad = urls.filter((u) => !u.startsWith(`${V.homepageBase}/`) && !u.startsWith(`${V.repoUrl}/`));
    return { found: true, actual: bad.length ? `off-canonical: ${bad.join(', ')}` : `${urls.length} links`, ok: bad.length === 0 };
  },
});
field({
  file: '.github/workflows/runner-image.yml', name: 'oci.source-label', label: 'runner image OCI source label (repository URL)',
  extract: every(/org\.opencontainers\.image\.source=(\S+)/), expectOf: (V) => V.repoUrl,
});
field({
  file: '.github/runner-image/Dockerfile', name: 'oci.source-label-dockerfile', label: 'runner image Dockerfile OCI source label (repository URL)',
  extract: every(/org\.opencontainers\.image\.source="([^"]+)"/), expectOf: (V) => V.repoUrl,
});

// ---- workspace + published crate manifests (check-only; crates keep their
// own technical descriptions — only declared registry overrides are compared) ----
field({
  file: 'Cargo.toml', name: 'workspace.license', label: '[workspace.package] license (identity.license; crates inherit via license.workspace)',
  check(text) {
    const b = /\[workspace\.package\]\s*\n((?!\[)[\s\S]*?)(?=\n\[|$)/.exec(text);
    if (!b) return { found: false };
    const m = /license[ \t]*=[ \t]*"([^"]+)"/.exec(b[1]);
    if (!m) return { found: false };
    return { found: true, actual: m[1], ok: m[1] === V.license };
  },
});

const releasePlz = Bun.TOML.parse(requireText('release-plz.toml'));
const publishedCrates = releasePlz.package
  .filter((pkg) => pkg.release === true)
  .map((pkg) => pkg.name);

field({
  file: 'release-plz.toml', name: 'release-plz.published-set', label: 'published crate set (release=true, manifests exist, count matches positioning evidence)',
  check() {
    const problems = [];
    for (const name of publishedCrates) {
      const manifest = path.join(repoRoot, 'crates', name, 'Cargo.toml');
      if (!existsSync(manifest)) { problems.push(`${name}: no crates/${name}/Cargo.toml`); continue; }
      const t = readFileSync(manifest, 'utf8');
      if (!new RegExp(`^name[ \\t]*=[ \\t]*"${name}"`, 'm').test(t)) problems.push(`${name}: manifest name mismatch`);
      if (!/^version[ \t]*=/m.test(t)) problems.push(`${name}: no version field`);
    }
    if (publishedCrates.length !== V.publishedCrateCount) {
      problems.push(`release=true count ${publishedCrates.length} != positioning evidence.counts.published_contract_crates ${V.publishedCrateCount}`);
    }
    return { found: true, actual: problems.length ? problems.join('; ') : `${publishedCrates.length} crates`, ok: problems.length === 0 };
  },
});

for (const crateName of publishedCrates) {
  const file = `crates/${crateName}/Cargo.toml`;
  field({
    file, name: `crate.${crateName}.repository`, label: `${crateName} repository (repository URL)`,
    extract: single(/^repository[ \t]*=[ \t]*"([^"]+)"/m), expectOf: (V) => V.repoUrl,
  });
  field({
    file, name: `crate.${crateName}.homepage`, label: `${crateName} homepage`,
    extract: single(/^homepage[ \t]*=[ \t]*"([^"]+)"/m), expectOf: (V) => V.homepage,
  });
  field({
    file, name: `crate.${crateName}.documentation`, label: `${crateName} documentation link (plugins docs)`,
    extract: single(/^documentation[ \t]*=[ \t]*"([^"]+)"/m), expectOf: (V) => V.pluginsDocsUrl,
  });
  field({
    file, name: `crate.${crateName}.license`, label: `${crateName} license (workspace inheritance or SPDX id)`,
    check(text) {
      if (/^license\.workspace[ \t]*=[ \t]*true[ \t]*$/m.test(text)) {
        return { found: true, actual: 'license.workspace = true', ok: true };
      }
      const m = /^license[ \t]*=[ \t]*"([^"]+)"/m.exec(text);
      if (!m) return { found: false };
      return { found: true, actual: m[1], ok: m[1] === V.license };
    },
  });
  field({
    file, name: `crate.${crateName}.description`, label: `${crateName} description (declared registry override or non-empty technical text)`,
    check(text) {
      const m = /^description[ \t]*=[ \t]*"([^"]+)"/m.exec(text);
      if (!m) return { found: false };
      const override = V.registryDescByCrate.get(crateName);
      if (override !== undefined) {
        return { found: true, actual: m[1], ok: m[1] === override, overrideExpected: override };
      }
      return { found: true, actual: m[1], ok: m[1].trim().length > 0 };
    },
  });
  for (const key of ['keywords', 'categories']) {
    field({
      file, name: `crate.${crateName}.${key}`, label: `${crateName} ${key} (non-empty publish metadata)`,
      check(text) {
        const m = new RegExp(`^${key}[ \\t]*=[ \\t]*\\[([^\\]]*)\\]`, 'm').exec(text);
        if (!m) return { found: false };
        const items = m[1].split(',').map((s) => s.trim()).filter(Boolean);
        const override = key === 'keywords' ? V.registryKeywordsByCrate.get(crateName) : undefined;
        if (override !== undefined) {
          return { found: true, actual: items.join(', '), overrideExpected: override.join(', '), ok: JSON.stringify(items.map(unquote)) === JSON.stringify(override) };
        }
        return { found: true, actual: items.join(', '), ok: items.length > 0 };
      },
    });
  }
}

// ---------- run ----------

const byFile = new Map();
for (const f of fields) {
  if (!byFile.has(f.file)) byFile.set(f.file, []);
  byFile.get(f.file).push(f);
}

const results = [];
let applied = 0;

for (const [file, list] of byFile) {
  const text = readText(file);
  if (text === null) {
    for (const f of list) {
      results.push({ ...f, status: 'fail', actual: '<file not found>', expected: '(file must exist)' });
    }
    continue;
  }
  let current = text;
  let dirty = false;
  for (const f of list) {
    const expected = f.expectOf ? f.expectOf(V) : '(see rule)';
    const r = f.check ? f.check(current) : f.extract(current);
    if (!r.found) {
      results.push({ ...f, status: 'fail', actual: '<pattern not found>', expected: expected ?? '(see rule)' });
      continue;
    }
    const expectedShown = r.overrideExpected ?? (expected === undefined ? '(see rule)' : Array.isArray(expected) ? expected.join('  |  ') : expected);
    let ok;
    if (r.ok !== undefined) {
      ok = r.ok;
    } else if (f.compare) {
      ok = f.compare(r, expected);
    } else if (r.values) {
      ok = r.values.every((v) => (Array.isArray(expected) ? expected.includes(v) : v === expected));
    } else {
      ok = Array.isArray(expected) ? expected.includes(r.actual) : r.actual === expected;
    }
    if (ok) {
      results.push({ ...f, status: 'ok', actual: r.actual, expected: expectedShown });
      continue;
    }
    if (f.mode === 'sync' && !checkOnly && r.span) {
      const rendered = f.render(expected);
      current = current.slice(0, r.span[0]) + rendered + current.slice(r.span[1]);
      dirty = true;
      applied += 1;
      results.push({ ...f, status: 'updated', actual: r.actual, expected: expectedShown });
      continue;
    }
    results.push({ ...f, status: 'fail', actual: r.actual, expected: expectedShown, syncable: f.mode === 'sync' });
  }
  if (dirty) {
    writeFileSync(path.join(repoRoot, file), current);
  }
}

for (const r of results) {
  const tag = { ok: 'ok  ', updated: 'upd ', fail: 'FAIL' }[r.status];
  if (r.status === 'fail' || verbose) {
    console.log(`${tag} ${r.file} :: ${r.label}`);
    if (r.status !== 'ok' || verbose) {
      console.log(`      expected: ${r.expected}`);
      console.log(`      actual:   ${r.actual}`);
      if (r.status === 'fail') {
        console.log(`      fix:      ${r.syncable ? 'run `bun scripts/public-metadata.mjs` (syncable field)' : 'manual edit required (check-only surface)'}`);
      }
    }
  }
}

const failed = results.filter((r) => r.status === 'fail').length + failures.length;
for (const f of failures) console.log(`FAIL ${f.fatal}`);
console.log(
  failed
    ? `public-metadata: ${failed} drifted field(s)${applied ? ` (${applied} synced)` : ''}`
    : `public-metadata: ${results.length} fields across ${byFile.size} surfaces match positioning.yml${applied ? ` (${applied} synced)` : ''}`,
);
console.log('info github repository settings (apply manually; this tool never calls APIs):');
console.log(`  About description: ${V.about}`);
console.log(`  Website:           ${V.homepage}`);
console.log(`  Topics:            ${V.githubTopics.join(', ')}`);
process.exit(failed ? 1 : 0);
