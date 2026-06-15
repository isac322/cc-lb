// Regression: the dashboard used to flicker (chart + adjacent panels) every
// time a query key changed (range toggle, upstream selection, etc.) because
// TanStack Query treated the new key as a brand-new query and flipped
// `isLoading` to true, which unmounted the chart and rendered the loading
// skeleton in its place. Setting `placeholderData: keepPreviousData` at the
// QueryClient default keeps the previously fetched data on screen while the
// new query is in flight, eliminating the unmount cycle for ALL hooks at
// once (not just the chart ones).
//
// This test guards that systemic default and must stay green.

import { keepPreviousData } from '@tanstack/react-query';
import { describe, expect, test } from 'vitest';
import { queryClient } from './queryClient';

describe('queryClient default options', () => {
  test('defaultOptions.queries.placeholderData uses keepPreviousData', () => {
    const opts = queryClient.getDefaultOptions();
    expect(opts.queries?.placeholderData).toBe(keepPreviousData);
  });
});
