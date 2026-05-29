export const STEP = 1000;

export function nextAfter(existing: number[]): number {
  if (existing.length === 0) {
    return STEP;
  }
  return Math.max(...existing) + STEP;
}

export function between(lower: number, upper: number): number {
  if (upper - lower < 2) {
    throw new Error('needs_rebalance');
  }
  return Math.floor((lower + upper) / 2);
}

export function rebalance(existing: number[]): number[] {
  return existing.map((_, idx) => STEP * (idx + 1));
}

export function needsRebalance(existing: number[]): boolean {
  if (existing.length < 2) {
    return false;
  }
  const sorted = [...existing].sort((a, b) => a - b);
  for (let i = 0; i < sorted.length - 1; i++) {
    if (sorted[i + 1] - sorted[i] < 2) {
      return true;
    }
  }
  return false;
}
