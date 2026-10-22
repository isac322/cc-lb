import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { dirname, normalize, posix, resolve } from 'node:path';

const root = resolve(import.meta.dir, '..', '..');
const siteRoot = resolve(import.meta.dir, '..');
const blockedTerms = /Bedrock|Vertex|official|endorsed|\/Users\/|\/data\/|\.omo\/|~\/\.config\/|production-grade|enterprise-grade|battle-tested|highly available|99\.999%|\bSLA\b|\bD-(?:arch|cut|ido|meta)-\d+\b|\buniversal multi-provider(?: LLM)? gateway\b|\b(?:full|complete) (?:(?:OpenAI|Gemini|provider) )?API (?:support|compatibility)\b/i;
const providerTerms = /OpenAI|Gemini/i;

export function findBlockedTerm(content: string, sourcePath: string): string | undefined {
  const blocked = content.match(blockedTerms)?.[0];
  if (blocked) return blocked;
  // The runtime reference documents implemented provider operations, not site-wide positioning.
  if (sourcePath !== 'docs/runtime-management.md') return content.match(providerTerms)?.[0];
  return undefined;
}

const sources = [
  {
    source: 'docs/runtime-management.md',
    target: 'src/content/docs/reference/source/runtime-management.md',
    title: 'Runtime management source notes',
    description: 'Curated sections from the database-backed runtime management reference.',
    sections: ['Overview', 'Architecture', 'Admin v1 REST API', 'Status and Export API'],
  },
  {
    source: 'docs/upstream-warmup.md',
    target: 'src/content/docs/reference/source/upstream-warmup.md',
    title: 'Upstream warm-up source notes',
    description: 'Curated operator notes for OAuth upstream warm-up and quota-aware scheduling.',
    sections: ['What it does', 'Re-enabling per upstream', 'Disabling per upstream', 'Emergency stop', "What you'll see", 'Quota-aware scheduling', 'Multi-replica notes'],
  },
  {
    source: 'docs/scheduler.md',
    target: 'src/content/docs/reference/source/scheduler.md',
    title: 'Scheduler source notes',
    description: 'Curated scheduler topology, retry classes, metrics, and runbook sections.',
    sections: ['Topology Overview', 'Retry Classes', 'Scheduler Failures and Admin Endpoint', 'Metrics List', 'Local vs Durable Rule', 'Operator Runbook'],
  },
  {
    source: 'docs/plugin-author-guide.md',
    target: 'src/content/docs/reference/source/plugin-author-guide.md',
    title: 'Plugin author source notes',
    description: 'Curated source sections for the Wasmtime plugin contract.',
    sections: ['Runtime model', 'Quick Start', 'Authoring a Plugin', 'Hook Contracts', 'Metadata Contract', 'Wire Versioning'],
  },
  {
    source: 'docs/consistency-contract.md',
    target: 'src/content/docs/reference/source/consistency-contract.md',
    title: 'Admin consistency contract source notes',
    description: 'What each admin response guarantees about persistence, audit, and convergence.',
    sections: [
      'Guarantee classes',
      'Summary',
      'Admin mutations and audit records',
      'Dynamic routing view',
      'Configuration draft and save',
      'Scheduler jobs',
      'OAuth token refresh',
      'Price catalog',
      'Request events and dashboards',
      'Subscription quota observations',
      'Writing tests against this contract',
    ],
  },
];

function sectionName(line: string): string | undefined {
  const match = line.match(/^##+\s+(.+?)\s*$/);
  return match?.[1].replace(/^\d+\.\s*/, '').replace(/"/g, '');
}

function takeSections(source: string, wanted: string[]): string {
  const lines = source.split(/\r?\n/);
  const sections: string[] = [];
  let active = false;
  let depth = 0;
  for (const line of lines) {
    const heading = line.match(/^(##+)\s+/);
    if (heading) {
      const name = sectionName(line);
      const level = heading[1].length;
      if (name && wanted.includes(name)) {
        if (!active || level <= depth) depth = level;
        active = true;
        sections.push(line);
        continue;
      }
      if (active && level <= depth) active = false;
    }
    if (active) sections.push(line);
  }
  return sections.join('\n').replace(/\n{3,}/g, '\n\n').trim();
}

function rewriteLinks(body: string, sourcePath: string): string {
  return body.replace(/\]\(([^)]+)\)/g, (match, target: string) => {
    if (/^(?:https?:|mailto:|#|\/)/.test(target)) return match;
    const [path, fragment] = target.split('#', 2);
    const repoPath = normalize(posix.join(posix.dirname(sourcePath), path)).replace(/^\.\//, '');
    const suffix = fragment ? `#${fragment}` : '';
    return `](https://github.com/isac322/cc-lb/blob/master/${repoPath}${suffix})`;
  });
}

function sanitizeSource(source: string, entry: (typeof sources)[number]): string {
  const sanitized = source.replace(
    /`~\/\.config\/cc-lb\/proxy-key`/g,
    'a local file',
  );
  if (entry.source !== 'docs/scheduler.md') return sanitized;
  return sanitized
    .replace(/decision IDs D-arch-3 and D-arch-1/g, 'the local-vs-durable architecture')
    .replace(
      /This list is defined in decision IDs D-arch-7, D-ido-1, D-ido-2, D-ido-3, D-ido-5, and D-meta-1\./g,
      'This list is defined by the scheduler job registry.',
    )
    .replace(/defined in D-arch-1/g, 'defined in the at-least-once execution model')
    .replace(
      /decision ID D-arch-6 \(which specified advisory-lock leader election\)/g,
      'an earlier advisory-lock leader-election design',
    )
    .replace(/decision ID D-arch-7/g, 'the scheduler retry policy')
    .replace(/promise defined in D-cut-3/g, 'operator promise')
    .replace(/\(defined in D-arch-3 and D-arch-1\)/g, '(defined by the local-vs-durable architecture)')
    .replace(/SLA <= 60s per D-cut-2/g, 'a bounded latency budget')
    .replace(/accepted under D-arch-1/g, 'accepted by the at-least-once execution model')
    .replace(
      /Phase 1 \(PR A\) is fully deployed before running Phase 2 \(PR B\) column drops per D-cut-6/g,
      'the first migration phase is fully deployed before later column drops',
    )
    .replace(
      /The connection budget is governed by the following formula \(first recorded in a\nWave 0\.3 evidence note, `\.omo\/evidence\/task-0-3-connection-budget\.md`, a local\nworking artifact that is not tracked in this repository\):/g,
      'The connection budget is governed by the following formula:',
    );
}

export function renderSourceNotes(source: string, sourcePath: string): string {
  const entry = sources.find((candidate) => candidate.source === sourcePath);
  if (!entry) throw new Error(`Unknown documentation source: ${sourcePath}`);
  const selected = takeSections(sanitizeSource(source, entry), entry.sections);
  const blockedMatch = findBlockedTerm(selected, entry.source);
  if (!selected || blockedMatch) {
    throw new Error(
      `Sync filter rejected selected content from ${entry.source}${blockedMatch ? `: ${blockedMatch}` : ''}`,
    );
  }
  const body = rewriteLinks(selected, entry.source);
  if (!body || findBlockedTerm(body, entry.source)) {
    throw new Error(`Sync filter rejected ${entry.source}`);
  }
  const slug = `docs/${entry.target.replace(/^src\/content\/docs\//, '').replace(/\.md$/, '')}`;
  return `---\ntitle: ${JSON.stringify(entry.title)}\ndescription: ${JSON.stringify(entry.description)}\nslug: ${slug}\n---\n\n${body}\n`;
}

if (import.meta.main) {
  for (const entry of sources) {
    const sourcePath = resolve(root, entry.source);
    const targetPath = resolve(siteRoot, entry.target);
    const output = renderSourceNotes(await readFile(sourcePath, 'utf8'), entry.source);
    await mkdir(dirname(targetPath), { recursive: true });
    await writeFile(targetPath, output, 'utf8');
  }
}
