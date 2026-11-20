import assert from 'node:assert/strict';
import test from 'node:test';
import { extractGatedPackages, validatePartitions } from './check-cov-partitions.mjs';

const META = {
  workspace_members: ['pkg-a 1.0.0', 'pkg-b 1.0.0', 'pkg-ex 1.0.0'],
  packages: [
    {
      id: 'pkg-a 1.0.0',
      name: 'pkg-a',
      targets: [
        { name: 'pkg_a', kind: ['lib'] },
        { name: 'pkg-a', kind: ['bin'] },
        { name: 'integration', kind: ['test'] },
      ],
    },
    {
      id: 'pkg-b 1.0.0',
      name: 'pkg-b',
      targets: [{ name: 'pkg_b', kind: ['lib'] }],
    },
    {
      id: 'pkg-ex 1.0.0',
      name: 'pkg-ex',
      targets: [{ name: 'pkg_ex', kind: ['bin'] }],
    },
  ],
};

test('validatePartitions accepts exact split targets across partitions', () => {
  const cfg = {
    excluded: ['pkg-ex'],
    partitions: [
      { name: 'p1', runs: [{ packages: ['pkg-a'], cargo_args: ['--test', 'integration'] }] },
      {
        name: 'p2',
        runs: [
          { packages: ['pkg-a'], cargo_args: ['--lib', '--bins'] },
          { packages: ['pkg-b'], cargo_args: [] },
        ],
      },
    ],
  };
  assert.deepEqual(validatePartitions(cfg, META, new Set(['pkg-a', 'pkg-b'])), []);
});

test('validatePartitions rejects missing target or duplicate target coverage', () => {
  const missingTarget = {
    excluded: ['pkg-ex'],
    partitions: [
      { name: 'p1', runs: [{ packages: ['pkg-a'], cargo_args: ['--test', 'integration'] }] },
      { name: 'p2', runs: [{ packages: ['pkg-a'], cargo_args: ['--lib'] }, { packages: ['pkg-b'], cargo_args: [] }] },
    ],
  };
  const err1 = validatePartitions(missingTarget, META, new Set(['pkg-a']));
  assert.ok(err1.some((e) => e.includes('pkg-a:bin') && e.includes('not covered')));

  const duplicate = {
    excluded: ['pkg-ex'],
    partitions: [
      { name: 'p1', runs: [{ packages: ['pkg-a'], cargo_args: ['--lib'] }] },
      { name: 'p2', runs: [{ packages: ['pkg-a', 'pkg-b'], cargo_args: [] }] },
    ],
  };
  const err2 = validatePartitions(duplicate, META, new Set(['pkg-a']));
  assert.ok(err2.some((e) => e.includes('pkg-a:lib') && e.includes('multiple runs')));
});

test('extractGatedPackages parses scripts/coverage-gate.sh format', () => {
  const gated = extractGatedPackages('set("cc-lb-engine", 60); set("cc-lb-server", 60)');
  assert.deepEqual([...gated].sort(), ['cc-lb-engine', 'cc-lb-server']);
});
