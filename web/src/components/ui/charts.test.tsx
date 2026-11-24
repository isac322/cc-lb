import { cleanup, render } from '@testing-library/react';
import type { ReactNode } from 'react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { Sparkline } from './charts';

const captured = vi.hoisted(() => ({
  data: [] as { value: number | null }[],
  areas: [] as Record<string, unknown>[],
}));
vi.mock('recharts', () => ({
  ResponsiveContainer: ({ children }: { children: ReactNode }) => children,
  AreaChart: ({
    data,
    children,
  }: {
    data: typeof captured.data;
    children: ReactNode;
  }) => {
    captured.data = data;
    return <div>{children}</div>;
  },
  YAxis: () => null,
  Area: (props: Record<string, unknown>) => {
    captured.areas.push(props);
    return null;
  },
}));
afterEach(() => {
  cleanup();
  captured.areas = [];
  captured.data = [];
});

describe('Sparkline primary cost gaps', () => {
  it.each([
    { data: [] },
    { data: [0, 0] },
    { data: [null, null] },
    { data: [null, 0, null] },
  ])('omits an empty plot: $data', ({ data }) => {
    const { container } = render(<Sparkline data={data} />);
    expect(container.childElementCount).toBe(0);
  });
  it('retains unknown intervals and disables connecting them', () => {
    render(<Sparkline data={[1, null, 0, 2]} ariaLabel="Cost history" />);
    expect(captured.data.map((point) => point.value)).toEqual([1, null, 0, 2]);
    expect(captured.areas[0].connectNulls).toBe(false);
    expect(captured.areas[0].isAnimationActive).toBe(false);
  });
});
