import type { CacheKeepaliveRow } from '../../../lib/cacheKeepaliveApi';

export function mergeLiveSessions(
  rows: CacheKeepaliveRow[],
  maxRows: number,
): CacheKeepaliveRow[] {
  if (!rows || rows.length === 0) return [];

  const seen = new Set<string>();
  const merged: CacheKeepaliveRow[] = [];

  for (const row of rows) {
    if (!seen.has(row.id)) {
      seen.add(row.id);
      merged.push(row);
      if (merged.length >= maxRows) {
        break;
      }
    }
  }

  return merged;
}
