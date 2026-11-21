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

test('validatePartitions accepts exact split targets and nextest shards across partitions', () => {
  const cfg = {
    excluded: ['pkg-ex'],
    partitions: [
      {
        name: 'p1-s1',
        upload_objects: true,
        runs: [{ packages: ['pkg-a'], cargo_args: ['--test', 'integration'], nextest_args: ['--partition', 'hash:1/2'] }],
      },
      {
        name: 'p1-s2',
        upload_objects: false,
        runs: [{ packages: ['pkg-a'], cargo_args: ['--test', 'integration'], nextest_args: ['--partition', 'hash:2/2'] }],
      },
      {
        name: 'p2',
        upload_objects: true,
        runs: [
          { packages: ['pkg-a'], cargo_args: ['--lib', '--bins'] },
          { packages: ['pkg-b'], cargo_args: [] },
        ],
      },
    ],
  };
  assert.deepEqual(validatePartitions(cfg, META, new Set(['pkg-a', 'pkg-b'])), []);
});

test('validatePartitions rejects missing shards or shard groups without object upload', () => {
  const missingShard = {
    excluded: ['pkg-ex'],
    partitions: [
      {
        name: 'p1-s1',
        upload_objects: true,
        runs: [{ packages: ['pkg-a'], cargo_args: ['--test', 'integration'], nextest_args: ['--partition', 'hash:1/2'] }],
      },
      {
        name: 'p2',
        runs: [
          { packages: ['pkg-a'], cargo_args: ['--lib', '--bins'] },
          { packages: ['pkg-b'], cargo_args: [] },
        ],
      },
    ],
  };
  const errMissing = validatePartitions(missingShard, META, new Set(['pkg-a']));
  assert.ok(errMissing.some((e) => e.includes('missing shard hash:2/2')));

  const noUpload = {
    excluded: ['pkg-ex'],
    partitions: [
      {
        name: 'p1-s1',
        upload_objects: false,
        runs: [{ packages: ['pkg-a'], cargo_args: ['--test', 'integration'], nextest_args: ['--partition', 'hash:1/2'] }],
      },
      {
        name: 'p1-s2',
        upload_objects: false,
        runs: [{ packages: ['pkg-a'], cargo_args: ['--test', 'integration'], nextest_args: ['--partition', 'hash:2/2'] }],
      },
      {
        name: 'p2',
        runs: [
          { packages: ['pkg-a'], cargo_args: ['--lib', '--bins'] },
          { packages: ['pkg-b'], cargo_args: [] },
        ],
      },
    ],
  };
  const errUpload = validatePartitions(noUpload, META, new Set(['pkg-a']));
  assert.ok(errUpload.some((e) => e.includes('no partition with upload_objects=true')));
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
