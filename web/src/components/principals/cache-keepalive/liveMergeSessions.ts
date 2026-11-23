import type { CacheKeepaliveRow } from '../../../lib/cacheKeepaliveApi';

// Pages are ordered newest first. A session that moved to an earlier page
// after a poll carries its freshest copy there, so the first copy wins.
export function mergeLiveSessions(
  rows: CacheKeepaliveRow[],
): CacheKeepaliveRow[] {
  const byId = new Map<string, CacheKeepaliveRow>();
  for (const row of rows) {
    if (!byId.has(row.id)) {
      byId.set(row.id, row);
    }
  }
  return [...byId.values()];
}
