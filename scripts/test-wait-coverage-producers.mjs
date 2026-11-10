import assert from "node:assert/strict";
import test from "node:test";

import {
  pendingCoverageProducers,
  waitForCoverageProducers,
} from "./wait-coverage-producers.mjs";

const PRODUCERS = [
  "nextest-cov-contract",
  "nextest-cov-shard (1)",
  "nextest-cov-shard (2)",
  "nextest-cov-shard (3)",
  "nextest-cov-shard (4)",
];
const ENV = {
  GITHUB_REPOSITORY: "isac322/cc-lb",
  GITHUB_RUN_ID: "42866047398",
  GITHUB_RUN_ATTEMPT: "1",
  GH_TOKEN: "test-token-never-log",
};

function successfulJobs() {
  return PRODUCERS.map((name) => ({
    name,
    status: "completed",
    conclusion: "success",
  }));
}

function clock() {
  let elapsed = 0;
  const delays = [];
  return {
    now: () => elapsed,
    pause: async (delay) => {
      delays.push(delay);
      elapsed += delay;
    },
    advance: (duration) => { elapsed += duration; },
    delays,
  };
}

test("waits for missing and pending jobs, then accepts success across API pages", async () => {
  const timer = clock();
  const responses = [
    [{ jobs: [] }],
    [{ jobs: successfulJobs().map((job) => ({ ...job, status: "queued", conclusion: null })) }],
    [{ jobs: successfulJobs().slice(0, 2) }, { jobs: successfulJobs().slice(2) }],
  ];
  const calls = [];
  await waitForCoverageProducers({
    env: ENV,
    ...timer,
    execute: async (...args) => {
      calls.push(args);
      return { stdout: JSON.stringify(responses.shift()) };
    },
  });
  assert.deepEqual(timer.delays, [15_000, 15_000]);
  assert.equal(calls.length, 3);
  assert.deepEqual(calls[0].slice(0, 2), [
    "gh",
    [
      "api",
      "repos/isac322/cc-lb/actions/runs/42866047398/attempts/1/jobs?per_page=100",
      "--paginate",
      "--slurp",
    ],
  ]);
  assert.equal(calls[0][2].env, ENV);
  assert.equal(calls[0][2].timeout, 900_000);
  assert.equal(calls[1][2].timeout, 885_000);
  assert.equal(calls[2][2].timeout, 870_000);
});

test("each exact producer is required; unrelated and near-matching jobs do not count", () => {
  for (const name of PRODUCERS) {
    const jobs = successfulJobs().filter((job) => job.name !== name);
    jobs.push({ name: `${name} extra`, status: "completed", conclusion: "success" });
    jobs.push({ name: "nextest-cov", status: "in_progress", conclusion: null });
    assert.deepEqual(pendingCoverageProducers([{ jobs }]), [name]);
  }
});

test("all supported nonterminal statuses remain pending", () => {
  for (const status of ["queued", "in_progress", "waiting", "pending", "requested"]) {
    const jobs = successfulJobs();
    jobs[0] = { ...jobs[0], status, conclusion: null };
    assert.deepEqual(pendingCoverageProducers([{ jobs }]), [PRODUCERS[0]]);
  }
});

test("every unsuccessful completed producer fails, including cancelled and skipped", () => {
  for (const name of PRODUCERS) {
    for (const conclusion of ["failure", "cancelled", "skipped", "timed_out", "neutral", "action_required", "stale", null]) {
      const jobs = successfulJobs().map((job) => job.name === name ? { ...job, conclusion } : job);
      assert.throws(
        () => pendingCoverageProducers([{ jobs }]),
        { message: `Coverage producer did not succeed: ${name}.` },
      );
    }
  }
});

test("duplicates fail even when successful or split between API pages", () => {
  for (const name of PRODUCERS) {
    for (const status of ["completed", "queued"]) {
      assert.throws(
        () => pendingCoverageProducers([
          { jobs: successfulJobs() },
          { jobs: [{ name, status, conclusion: status === "completed" ? "success" : null }] },
        ]),
        { message: `Duplicate coverage producer: ${name}.` },
      );
    }
  }
});

test("invalid API data and unknown producer statuses fail closed", () => {
  for (const pages of [null, {}, [], [{}], [{ jobs: null }]]) {
    assert.throws(() => pendingCoverageProducers(pages), /GitHub jobs API returned/);
  }
  const jobs = successfulJobs();
  jobs[0].status = "unexpected";
  assert.throws(() => pendingCoverageProducers([{ jobs }]), /invalid status/);
});

test("missing producers time out at 15 minutes without a final unbounded API request", async () => {
  const timer = clock();
  let calls = 0;
  await assert.rejects(waitForCoverageProducers({
    env: ENV,
    ...timer,
    execute: async () => {
      calls += 1;
      return { stdout: JSON.stringify([{ jobs: [] }]) };
    },
  }), /Timed out waiting for coverage producers after 15 minutes/);
  assert.equal(timer.now(), 900_000);
  assert.equal(calls, 60);
  assert.equal(timer.delays.length, 60);
  assert.ok(timer.delays.every((delay) => delay === 15_000));
});

test("success arriving after the deadline is not accepted", async () => {
  const timer = clock();
  await assert.rejects(waitForCoverageProducers({
    env: ENV,
    ...timer,
    execute: async () => {
      timer.advance(900_000);
      return { stdout: JSON.stringify([{ jobs: successfulJobs() }]) };
    },
  }), /Timed out waiting for coverage producers/);
  assert.deepEqual(timer.delays, []);
});

test("API failures fail immediately without logging subprocess output or the token", async () => {
  const timer = clock();
  let calls = 0;
  await assert.rejects(waitForCoverageProducers({
    env: ENV,
    ...timer,
    execute: async () => {
      calls += 1;
      throw new Error(`authentication error: ${ENV.GH_TOKEN}`);
    },
  }), { message: "GitHub jobs API request failed; coverage producer barrier remains closed." });
  assert.equal(calls, 1);
  assert.deepEqual(timer.delays, []);
});

test("invalid API JSON fails without waiting or reflecting response contents", async () => {
  const timer = clock();
  await assert.rejects(waitForCoverageProducers({
    env: ENV,
    ...timer,
    execute: async () => ({ stdout: ENV.GH_TOKEN }),
  }), { message: "GitHub jobs API returned invalid JSON." });
  assert.deepEqual(timer.delays, []);
});

test("attempt 2 is explicitly rejected before any API call", async () => {
  let calls = 0;
  await assert.rejects(waitForCoverageProducers({
    env: { ...ENV, GITHUB_RUN_ATTEMPT: "2" },
    execute: async () => { calls += 1; },
  }), /Only GITHUB_RUN_ATTEMPT=1 is supported; reruns may contain artifacts from an earlier attempt/);
  assert.equal(calls, 0);
});

test("missing or malformed workflow configuration fails before querying GitHub", async () => {
  for (const overrides of [
    { GITHUB_RUN_ATTEMPT: undefined },
    { GITHUB_REPOSITORY: undefined },
    { GITHUB_REPOSITORY: "not-a-repository" },
    { GITHUB_RUN_ID: "0" },
    { GITHUB_RUN_ID: undefined },
    { GH_TOKEN: "" },
  ]) {
    let calls = 0;
    await assert.rejects(waitForCoverageProducers({
      env: { ...ENV, ...overrides },
      execute: async () => { calls += 1; },
    }));
    assert.equal(calls, 0);
  }
});
