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

test('validatePartitions validates extra_features and test_threads', () => {
  const valid = {
    excluded: ['pkg-ex'],
    partitions: [
      {
        name: 'p1',
        test_threads: 8,
        runs: [{ packages: ['pkg-a', 'pkg-b'], cargo_args: [], extra_features: ['pkg-b/feat'] }],
      },
    ],
  };
  assert.deepEqual(validatePartitions(valid, META, new Set(['pkg-a', 'pkg-b'])), []);

  const mismatchedShards = {
    excluded: ['pkg-ex'],
    partitions: [
      {
        name: 'p1-s1',
        upload_objects: true,
        runs: [
          {
            packages: ['pkg-a'],
            cargo_args: [],
            extra_features: ['pkg-a/dto-roundtrip'],
            nextest_args: ['--partition', 'hash:1/2'],
          },
        ],
      },
      {
        name: 'p1-s2',
        upload_objects: false,
        runs: [{ packages: ['pkg-a'], cargo_args: [], nextest_args: ['--partition', 'hash:2/2'] }],
      },
      { name: 'p2', runs: [{ packages: ['pkg-b'], cargo_args: [] }] },
    ],
  };
  const errMismatch = validatePartitions(mismatchedShards, META, new Set(['pkg-a']));
  assert.ok(errMismatch.some((e) => e.includes('missing shard hash:2/2')));

  const badThreads = {
    excluded: ['pkg-ex'],
    partitions: [{ name: 'p1', test_threads: 0, runs: [{ packages: ['pkg-a', 'pkg-b'], cargo_args: [] }] }],
  };
  assert.ok(validatePartitions(badThreads, META, new Set(['pkg-a'])).some((e) => e.includes('invalid test_threads')));
});

test('extractGatedPackages parses scripts/coverage-gate.sh format', () => {
  const gated = extractGatedPackages('set("cc-lb-engine", 60); set("cc-lb-server", 60)');
  assert.deepEqual([...gated].sort(), ['cc-lb-engine', 'cc-lb-server']);
});

test('validatePartitions skips target.test === false and accepts zero-testable-target crates', () => {
  const metaWithNoTestTargets = {
    workspace_members: ['pkg-lib 1.0.0', 'pkg-bin-skip 1.0.0', 'pkg-fixture 1.0.0'],
    packages: [
      {
        id: 'pkg-lib 1.0.0',
        name: 'pkg-lib',
        targets: [
          { name: 'pkg_lib', kind: ['lib'], test: true },
          { name: 'pkg-bin', kind: ['bin'], test: false },
        ],
      },
      {
        id: 'pkg-bin-skip 1.0.0',
        name: 'pkg-bin-skip',
        targets: [{ name: 'bin_only', kind: ['bin'], test: false }],
      },
      {
        id: 'pkg-fixture 1.0.0',
        name: 'pkg-fixture',
        targets: [{ name: 'fixture_cdylib', kind: ['cdylib'], test: false }],
      },
    ],
  };

  const valid = {
    excluded: [],
    partitions: [
      {
        name: 'p1',
        upload_objects: true,
        runs: [
          { packages: ['pkg-lib'], cargo_args: ['--lib'] },
          { packages: ['pkg-bin-skip', 'pkg-fixture'], cargo_args: [] },
        ],
      },
    ],
  };
  assert.deepEqual(validatePartitions(valid, metaWithNoTestTargets, new Set(['pkg-lib'])), []);

  const missingFixture = {
    excluded: [],
    partitions: [
      {
        name: 'p1',
        upload_objects: true,
        runs: [{ packages: ['pkg-lib'], cargo_args: ['--lib'] }],
      },
    ],
  };
  const errMissing = validatePartitions(missingFixture, metaWithNoTestTargets, new Set(['pkg-lib']));
  assert.ok(errMissing.some((e) => e.includes('pkg-fixture') && e.includes('missing from all partitions')));
});

test('validatePartitions validates seed_excluded subset rules', () => {
  const valid = {
    excluded: ['pkg-ex'],
    seed_excluded: ['pkg-ex'],
    partitions: [
      {
        name: 'p1',
        runs: [{ packages: ['pkg-a', 'pkg-b'], cargo_args: [] }],
      },
    ],
  };
  assert.deepEqual(validatePartitions(valid, META, new Set(['pkg-a', 'pkg-b'])), []);

  const badType = {
    ...valid,
    seed_excluded: 'pkg-ex',
  };
  assert.ok(
    validatePartitions(badType, META, new Set(['pkg-a', 'pkg-b'])).some((e) =>
      e.includes('seed_excluded must be an array'),
    ),
  );

  const duplicate = {
    ...valid,
    seed_excluded: ['pkg-ex', 'pkg-ex'],
  };
  assert.ok(
    validatePartitions(duplicate, META, new Set(['pkg-a', 'pkg-b'])).some((e) =>
      e.includes("duplicate seed_excluded package 'pkg-ex'"),
    ),
  );

  const unknownPkg = {
    ...valid,
    seed_excluded: ['missing-pkg'],
  };
  const unknownErrors = validatePartitions(unknownPkg, META, new Set(['pkg-a', 'pkg-b']));
  assert.ok(unknownErrors.some((e) => e.includes("seed_excluded package 'missing-pkg' is not a workspace member")));

  const notInExcluded = {
    ...valid,
    seed_excluded: ['pkg-b'],
  };
  assert.ok(
    validatePartitions(notInExcluded, META, new Set(['pkg-a', 'pkg-b'])).some((e) =>
      e.includes("seed_excluded package 'pkg-b' must also be listed in excluded"),
    ),
  );
});
