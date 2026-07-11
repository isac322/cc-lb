export const LOGS_PAGE_SIZE = 50;

export function getLogsPageCount(
  rowCount: number,
  pageSize: number = LOGS_PAGE_SIZE,
): number {
  const size =
    Number.isNaN(pageSize) || pageSize <= 0 ? LOGS_PAGE_SIZE : pageSize;
  const count = Number.isNaN(rowCount) || rowCount <= 0 ? 0 : rowCount;
  return Math.max(1, Math.ceil(count / size));
}

export function clampLogsPage(page: number, pageCount: number): number {
  const count = Number.isNaN(pageCount) || pageCount <= 0 ? 0 : pageCount;
  if (count === 0) {
    return 0;
  }
  const p = Number.isNaN(page) || page < 0 ? 0 : page;
  return Math.min(p, count - 1);
}

export function selectLogsPageRows<T>(
  rows: readonly T[],
  page: number,
  pageSize: number = LOGS_PAGE_SIZE,
): readonly T[] {
  if (rows.length === 0) {
    return [];
  }
  const size =
    Number.isNaN(pageSize) || pageSize <= 0 ? LOGS_PAGE_SIZE : pageSize;
  const pageCount = getLogsPageCount(rows.length, size);
  const clampedPage = clampLogsPage(page, pageCount);
  const start = clampedPage * size;
  return rows.slice(start, start + size);
}

export function isLastLogsPage(page: number, pageCount: number): boolean {
  const count = Number.isNaN(pageCount) || pageCount <= 0 ? 0 : pageCount;
  if (count <= 1) {
    return true;
  }
  const p = Number.isNaN(page) ? 0 : page;
  return p >= count - 1;
}
