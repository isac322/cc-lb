#!/usr/bin/env node
import { readFile } from 'node:fs/promises';
import { pathToFileURL } from 'node:url';
import { DEFAULT_FIXTURE_MANIFEST, importCoverage, serializePortableCoverage, validateCoverage } from './coverage.mjs';

const HELP = {
  command: 'node tests/real-client/provider-e2e/client-harness/import-coverage.mjs',
  import: '--qa PATH --codex PATH --agy PATH --issue-map PATH [--slug-crosswalk PATH] [--fixtures MANIFEST_PATH]',
  validate: '--validate [LEDGER_PATH] [--fixtures MANIFEST_PATH] [--results MANIFEST_PATH]',
  negativeControl: '--validate [LEDGER_PATH] --mutation omission|duplicate|synthetic-live',
  output: 'Machine JSON only. Import prints portable ledger; validation prints summary. No input paths or error details are emitted.',
  note: 'Inputs must match the approved inventory content commitments. Byte SHA-256 records exact supplied bytes; content SHA-256 is invariant to root inventory row reordering. No provider calls, credentials, HOME access, or client subprocesses.',
};

/** Controlled corruptions use the same validator as the intact control, never source-text checks. */
export function mutateCoverage(ledger, mutation) {
  const altered = structuredClone(ledger);
  const fixtureManifest = [...DEFAULT_FIXTURE_MANIFEST];
  const resultManifest = [];
  if (mutation === 'omission') {
    const removed = altered.surfaces.pop();
    for (const action of altered.actions) {
      action.actualSurfaceIds = action.actualSurfaceIds.filter((id) => id !== removed.id);
      action.candidateSurfaceIds = action.candidateSurfaceIds.filter((id) => id !== removed.id);
    }
  } else if (mutation === 'duplicate') altered.surfaces.push(structuredClone(altered.surfaces[0]));
  else if (mutation === 'synthetic-live') {
    const surfaceIds = [altered.surfaces[0].id];
    fixtureManifest.push({ id: 'synthetic-negative-control', provenance: 'synthetic', surfaceIds });
    resultManifest.push({ id: 'forged-live-negative-control', fixtureId: 'synthetic-negative-control', provenance: 'synthetic', state: 'verified-live', status: 'PASS', consumerDecoded: true, evidenceSha256: 'a'.repeat(64), client: { version: 'fixture', binary_sha256: 'b'.repeat(64) }, surfaceIds });
  } else throw new Error('Unknown coverage mutation');
  return { ledger: altered, fixtureManifest, resultManifest };
}

async function main(argv) {
  if (argv.includes('--help')) { process.stdout.write(`${JSON.stringify(HELP)}\n`); return; }
  const options = {};
  for (let index = 0; index < argv.length; index += 1) {
    const flag = argv[index];
    if (flag === '--validate') {
      options.validate = true;
      if (argv[index + 1] && !argv[index + 1].startsWith('--')) options.ledger = argv[++index];
    } else if (['--qa', '--codex', '--agy', '--issue-map', '--slug-crosswalk', '--fixtures', '--results', '--mutation'].includes(flag)) {
      if (!argv[index + 1] || argv[index + 1].startsWith('--') || Object.hasOwn(options, flag)) throw new Error('Invalid arguments');
      options[flag] = argv[++index];
    } else throw new Error('Unknown arguments');
  }
  const readJson = async (path) => JSON.parse(await readFile(path, 'utf8'));
  if (options.validate) {
    let ledger = await readJson(options.ledger ?? new URL('./coverage-ledger.json', import.meta.url));
    let fixtureManifest = options['--fixtures'] ? await readJson(options['--fixtures']) : DEFAULT_FIXTURE_MANIFEST;
    let resultManifest = options['--results'] ? await readJson(options['--results']) : [];
    if (options['--mutation']) ({ ledger, fixtureManifest, resultManifest } = mutateCoverage(ledger, options['--mutation']));
    process.stdout.write(`${JSON.stringify(validateCoverage({ ledger, fixtureManifest, resultManifest }))}\n`);
    return;
  }
  if (options['--mutation'] || options['--results']) throw new Error('Validation option requires --validate');
  for (const flag of ['--qa', '--codex', '--agy', '--issue-map']) if (!options[flag]) throw new Error('Missing required input');
  const [qaBytes, codexBytes, agyBytes, issueMap, slugCrosswalk] = await Promise.all([
    readFile(options['--qa']), readFile(options['--codex']), readFile(options['--agy']),
    readJson(options['--issue-map']), options['--slug-crosswalk'] ? readJson(options['--slug-crosswalk']) : undefined,
  ]);
  const fixtureManifest = options['--fixtures'] ? await readJson(options['--fixtures']) : DEFAULT_FIXTURE_MANIFEST;
  const ledger = importCoverage({ qaBytes, codexBytes, agyBytes, issueMap, slugCrosswalk, fixtureManifest });
  process.stdout.write(serializePortableCoverage(ledger, { fixtureManifest }));
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main(process.argv.slice(2)).catch(() => {
    // Filesystem/parser errors may contain private paths or input values. Do not echo them.
    process.stdout.write(`${JSON.stringify({ valid: false, error: 'coverage-validation-failed' })}\n`);
    process.exitCode = 1;
  });
}
