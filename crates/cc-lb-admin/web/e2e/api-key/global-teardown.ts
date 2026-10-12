// Stops every process global-setup started (each leads its own process group)
// and removes the scratch directory once those groups have exited.

import { readFileSync, rmSync } from 'node:fs';
import { setTimeout as delay } from 'node:timers/promises';
import type { E2eState } from './harness';

/** How long a group may drain after SIGTERM before it is SIGKILLed. */
const TERM_GRACE_MS = 10_000;
/** How long a group may take to disappear after SIGKILL. */
const KILL_GRACE_MS = 5_000;
const EXIT_POLL_MS = 50;

function signalGroups(pgids: readonly number[], signal: NodeJS.Signals): void {
  for (const pgid of pgids) {
    try {
      process.kill(-pgid, signal);
    } catch {
      // Already gone.
    }
  }
}

function groupAlive(pgid: number): boolean {
  try {
    process.kill(-pgid, 0);
    return true;
  } catch (error) {
    // EPERM: a member still exists but cannot be signalled.
    return (error as NodeJS.ErrnoException).code === 'EPERM';
  }
}

/** Polls until every group has exited or `timeoutMs` passes; returns survivors. */
async function waitForGroupsExit(
  pgids: readonly number[],
  timeoutMs: number,
): Promise<number[]> {
  const deadline = Date.now() + timeoutMs;
  let alive = pgids.filter(groupAlive);
  while (alive.length > 0 && Date.now() < deadline) {
    await delay(EXIT_POLL_MS);
    alive = alive.filter(groupAlive);
  }
  return alive;
}

/**
 * SIGTERMs each process group and waits for it to exit, escalating to
 * SIGKILL after a bounded grace period. Returns the groups that still
 * exist after SIGKILL. Setup reuses it when it fails midway.
 */
export async function stopProcesses(
  pids: readonly number[],
): Promise<number[]> {
  const pgids = pids.filter((pid) => pid > 0);
  signalGroups(pgids, 'SIGTERM');
  const lingering = await waitForGroupsExit(pgids, TERM_GRACE_MS);
  if (lingering.length === 0) return [];
  signalGroups(lingering, 'SIGKILL');
  return waitForGroupsExit(lingering, KILL_GRACE_MS);
}

export default async function globalTeardown(): Promise<void> {
  const statePath = process.env.CCLB_E2E_STATE;
  if (!statePath) return;
  const state: E2eState = JSON.parse(readFileSync(statePath, 'utf8'));
  const survivors = await stopProcesses(state.pids);
  if (survivors.length > 0) {
    throw new Error(
      `process groups ${survivors.join(', ')} survived SIGKILL; leaving ${state.dir} in place`,
    );
  }
  if (process.env.CCLB_E2E_KEEP_DIR !== '1') {
    rmSync(state.dir, { recursive: true, force: true });
  }
}
