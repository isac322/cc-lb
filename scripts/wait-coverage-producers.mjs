import { execFile } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";
import { pathToFileURL } from "node:url";
import { promisify } from "node:util";

const runGh = promisify(execFile);
const POLL_INTERVAL_MS = 15_000;
const DEADLINE_MS = 15 * 60_000;
const REPOSITORY = /^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/;
const RUN_ID = /^[1-9]\d*$/;
const PRODUCERS = [
  "nextest-cov-contract",
  "nextest-cov-shard (1)",
  "nextest-cov-shard (2)",
  "nextest-cov-shard (3)",
  "nextest-cov-shard (4)",
];
const PENDING_STATUSES = new Set([
  "queued",
  "in_progress",
  "waiting",
  "pending",
  "requested",
]);

// Missing jobs can still be waiting for a runner or dependency. Only the exact,
// unique, successfully completed producer set permits the profraw download.
export function pendingCoverageProducers(pages) {
  if (!Array.isArray(pages) || pages.length === 0) {
    throw new Error("GitHub jobs API returned invalid paginated data.");
  }
  const jobs = [];
  for (const page of pages) {
    if (!page || !Array.isArray(page.jobs)) {
      throw new Error("GitHub jobs API returned a page without jobs.");
    }
    jobs.push(...page.jobs);
  }

  const pending = [];
  for (const name of PRODUCERS) {
    const matches = jobs.filter((job) => job?.name === name);
    if (matches.length > 1) {
      throw new Error(`Duplicate coverage producer: ${name}.`);
    }
    if (matches.length === 0) {
      pending.push(name);
      continue;
    }
    const [job] = matches;
    if (job.status === "completed") {
      if (job.conclusion !== "success") {
        throw new Error(`Coverage producer did not succeed: ${name}.`);
      }
    } else if (PENDING_STATUSES.has(job.status)) {
      pending.push(name);
    } else {
      throw new Error(`Coverage producer has an invalid status: ${name}.`);
    }
  }
  return pending;
}

export async function waitForCoverageProducers({
  env = process.env,
  execute = runGh,
  now = () => performance.now(),
  pause = sleep,
} = {}) {
  // Artifacts are run-scoped, not attempt-scoped. Do not silently consume an
  // earlier attempt's artifacts when only some jobs were rerun.
  if (env.GITHUB_RUN_ATTEMPT !== "1") {
    throw new Error(
      "Only GITHUB_RUN_ATTEMPT=1 is supported; reruns may contain artifacts from an earlier attempt.",
    );
  }
  if (!REPOSITORY.test(env.GITHUB_REPOSITORY ?? "")) {
    throw new Error("GITHUB_REPOSITORY must identify owner/repository.");
  }
  if (!RUN_ID.test(env.GITHUB_RUN_ID ?? "")) {
    throw new Error("GITHUB_RUN_ID must be a positive integer.");
  }
  if (!env.GH_TOKEN) {
    throw new Error("GH_TOKEN is required to read coverage producer results.");
  }

  const endpoint = `repos/${env.GITHUB_REPOSITORY}/actions/runs/${env.GITHUB_RUN_ID}/attempts/${env.GITHUB_RUN_ATTEMPT}/jobs?per_page=100`;
  const deadline = now() + DEADLINE_MS;
  const timeout = () => new Error("Timed out waiting for coverage producers after 15 minutes.");
  for (;;) {
    const remaining = Math.floor(deadline - now());
    if (remaining <= 0) throw timeout();

    let stdout;
    try {
      ({ stdout } = await execute(
        "gh",
        ["api", endpoint, "--paginate", "--slurp"],
        { env, timeout: remaining, maxBuffer: 16 * 1024 * 1024 },
      ));
    } catch {
      // Never include subprocess output: authentication failures may contain
      // sensitive data. API errors are fatal rather than an implicit success.
      throw new Error("GitHub jobs API request failed; coverage producer barrier remains closed.");
    }
    if (now() >= deadline) throw timeout();

    let pages;
    try {
      pages = JSON.parse(stdout);
    } catch {
      throw new Error("GitHub jobs API returned invalid JSON.");
    }
    if (pendingCoverageProducers(pages).length === 0) return;

    const delay = Math.min(POLL_INTERVAL_MS, deadline - now());
    if (delay <= 0) throw timeout();
    await pause(delay);
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    await waitForCoverageProducers();
    console.log("All five coverage producers completed successfully.");
  } catch (error) {
    console.error(`::error::${error.message}`);
    process.exitCode = 1;
  }
}
