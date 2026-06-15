// Regression: the Overview page used to render a single `h-[300px]` "Loading…"
// box while quotaSeries was loading, then swap to a grid of mini-chart cards
// whose height varies wildly (180px on desktop, 540px on mobile). The result
// was a violent layout shift on every cold load and range toggle. This test
// guards the replacement: a stable skeleton grid whose cells match the loaded
// card geometry, so the surrounding page never jumps.

import { cleanup, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, test } from 'vitest';
import { OverviewQuotaSkeleton } from '../ui/OverviewQuotaSkeleton';

afterEach(cleanup);

describe('OverviewQuotaSkeleton', () => {
  test('renders exactly cellCount skeleton cells when given a count', () => {
    render(<OverviewQuotaSkeleton cellCount={4} />);
    const cells = screen.getAllByTestId('overview-quota-skeleton-cell');
    expect(cells).toHaveLength(4);
  });

  test('each cell matches the loaded card height (h-[180px])', () => {
    render(<OverviewQuotaSkeleton cellCount={3} />);
    const cells = screen.getAllByTestId('overview-quota-skeleton-cell');
    for (const cell of cells) {
      expect(cell.className).toContain('h-[180px]');
    }
  });

  test('falls back to a non-zero cell count when cellCount is undefined', () => {
    render(<OverviewQuotaSkeleton />);
    const cells = screen.getAllByTestId('overview-quota-skeleton-cell');
    expect(cells.length).toBeGreaterThan(0);
  });

  test('container uses the same grid layout as the loaded state', () => {
    const { container } = render(<OverviewQuotaSkeleton cellCount={2} />);
    const grid = container.firstElementChild as HTMLElement | null;
    expect(grid).not.toBeNull();
    // Must match the loaded grid in routes/index.tsx:
    // grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3
    expect(grid?.className).toContain('grid');
    expect(grid?.className).toContain('grid-cols-1');
    expect(grid?.className).toContain('sm:grid-cols-2');
    expect(grid?.className).toContain('lg:grid-cols-3');
  });
});
