import { describe, expect, it } from 'vitest';
import {
  clampLogsPage,
  getLogsPageCount,
  isLastLogsPage,
  selectLogsPageRows,
} from './logsPagination';

describe('getLogsPageCount', () => {
  it.each([
    [0, undefined, 1, 'returns 1 when rowCount is 0 (empty)'],
    [1, undefined, 1, 'returns 1 when rowCount is 1'],
    [50, undefined, 1, 'returns 1 when rowCount is 50 (exactly one page)'],
    [51, undefined, 2, 'returns 2 when rowCount is 51 (spills to second page)'],
    [100, undefined, 2, 'returns 2 when rowCount is 100 (exactly two pages)'],
    [500, undefined, 10, 'returns 10 when rowCount is 500'],
    [15, 10, 2, 'supports custom page size'],
    [-10, undefined, 1, 'handles negative rowCount gracefully by returning 1'],
    [
      Number.NaN,
      undefined,
      1,
      'handles NaN rowCount gracefully by returning 1',
    ],
  ])('%s', (rowCount, pageSize, expected, _desc) => {
    // Given: rowCount and optional pageSize
    // When: calculating page count
    const result = getLogsPageCount(rowCount, pageSize);
    // Then: expected page count is returned
    expect(result).toBe(expected);
  });
});

describe('clampLogsPage', () => {
  it.each([
    [-5, 5, 0, 'clamps negative page to 0'],
    [Number.NaN, 5, 0, 'clamps NaN page to 0'],
    [10, 5, 4, 'clamps overflow page to pageCount - 1'],
    [2, 5, 2, 'returns the page unchanged when it is within range'],
    [2, 0, 0, 'clamps to 0 when pageCount is 0 or negative'],
  ])('%s', (page, pageCount, expected, _desc) => {
    // Given: page and pageCount
    // When: clamping the page
    const result = clampLogsPage(page, pageCount);
    // Then: expected clamped page is returned
    expect(result).toBe(expected);
  });
});

describe('selectLogsPageRows', () => {
  const createRows = (count: number): readonly string[] =>
    Array.from({ length: count }, (_, i) => `row-${i}`);

  it.each([
    [createRows(0), 0, undefined, 0, undefined, undefined, 'empty rows'],
    [createRows(100), 0, undefined, 50, 'row-0', 'row-49', 'first page'],
    [createRows(150), 1, undefined, 50, 'row-50', 'row-99', 'middle page'],
    [
      createRows(120),
      2,
      undefined,
      20,
      'row-100',
      'row-119',
      'last page partial',
    ],
    [createRows(120), 10, undefined, 20, 'row-100', 'row-119', 'overflow page'],
    [createRows(100), -3, undefined, 50, 'row-0', 'row-49', 'negative page'],
    [createRows(25), 1, 10, 10, 'row-10', 'row-19', 'custom page size'],
  ])(
    '%s',
    (rows, page, pageSize, expectedLength, expectedFirst, expectedLast, _desc) => {
      // Given: rows, page, and optional pageSize
      // When: selecting rows for the page
      const result = selectLogsPageRows(rows, page, pageSize);
      // Then: expected slice is returned
      expect(result).toHaveLength(expectedLength);
      if (expectedLength > 0) {
        expect(result[0]).toBe(expectedFirst);
        expect(result[expectedLength - 1]).toBe(expectedLast);
      }
    },
  );
});

describe('isLastLogsPage', () => {
  it.each([
    [4, 5, true, 'returns true when page is the last page'],
    [2, 5, false, 'returns false when page is not the last page'],
    [0, 1, true, 'returns true when pageCount is 1 and page is 0'],
    [
      10,
      5,
      true,
      'returns true when page is greater than or equal to pageCount - 1',
    ],
    [0, 0, true, 'returns true when pageCount is 0 or negative'],
  ])('%s', (page, pageCount, expected, _desc) => {
    // Given: page and pageCount
    // When: checking if it is the last page
    const result = isLastLogsPage(page, pageCount);
    // Then: expected boolean is returned
    expect(result).toBe(expected);
  });
});
