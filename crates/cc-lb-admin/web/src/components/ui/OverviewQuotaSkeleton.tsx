// Skeleton that mirrors the Overview "Subscription Quota" loaded grid
// exactly (grid-cols-1/2/3, h-[180px] cards) so loading→loaded transition
// produces no layout shift. Cell count: explicit prop > LS cache > fallback.

import { Card, Skeleton } from './primitives';

const FALLBACK_CELL_COUNT = 3;
const STORAGE_KEY = 'cc-lb-admin:overview:upstreamCount';

export function readCachedUpstreamCount(): number | undefined {
  if (typeof window === 'undefined') return undefined;
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY);
    if (!raw) return undefined;
    const n = Number(raw);
    return Number.isFinite(n) && n > 0 ? Math.floor(n) : undefined;
  } catch {
    return undefined;
  }
}

export function writeCachedUpstreamCount(n: number): void {
  if (typeof window === 'undefined') return;
  if (!Number.isFinite(n) || n <= 0) return;
  try {
    window.localStorage.setItem(STORAGE_KEY, String(Math.floor(n)));
  } catch {
    /* localStorage may be unavailable (private mode / quota); ignore */
  }
}

export function OverviewQuotaSkeleton({ cellCount }: { cellCount?: number }) {
  const n = Math.max(
    1,
    cellCount ?? readCachedUpstreamCount() ?? FALLBACK_CELL_COUNT,
  );
  return (
    <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
      {Array.from({ length: n }).map((_, i) => (
        <Card
          key={`skel-${i}`}
          data-testid="overview-quota-skeleton-cell"
          className="flex flex-col bg-overlay-1 border-subtle h-[180px]"
        >
          <div className="px-3 py-2 border-b border-subtle">
            <Skeleton className="h-3 w-32" />
          </div>
          <div className="h-[140px] p-2">
            <Skeleton className="h-full w-full" />
          </div>
        </Card>
      ))}
    </div>
  );
}
