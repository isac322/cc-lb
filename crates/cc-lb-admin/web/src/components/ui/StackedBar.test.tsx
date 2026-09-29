import { cleanup, render } from '@testing-library/react';
import { afterEach, describe, expect, it } from 'vitest';
import { StackedBar } from './StackedBar';

afterEach(cleanup);

describe('StackedBar', () => {
  it('applies Tailwind bg-* utility colors through className', () => {
    const { container } = render(
      <StackedBar segments={[{ value: 10, color: 'bg-sky-400' }]} />,
    );

    const segments = Array.from(container.querySelectorAll('span'));

    expect(segments).toHaveLength(1);
    expect(segments[0]?.className).toContain('bg-sky-400');
    expect(segments[0]?.style.backgroundColor).toBe('');
  });

  it('applies raw CSS colors through inline style', () => {
    const { container } = render(
      <StackedBar segments={[{ value: 20, color: '#ff0000' }]} />,
    );

    const segments = Array.from(container.querySelectorAll('span'));

    expect(segments).toHaveLength(1);
    expect(segments[0]?.className).not.toContain('#ff0000');
    expect(segments[0]?.style.backgroundColor).toBe('rgb(255, 0, 0)');
  });

  it('keeps compound Tailwind bg-* utility colors in className', () => {
    const compoundColor =
      'bg-slate-700/30 bg-[repeating-linear-gradient(45deg,_transparent_0_4px,_rgba(255,255,255,0.05)_4px_8px)]';
    const { container } = render(
      <StackedBar segments={[{ value: 30, color: compoundColor }]} />,
    );

    const segments = Array.from(container.querySelectorAll('span'));

    expect(segments).toHaveLength(1);
    expect(segments[0]?.className).toContain('bg-slate-700/30');
    expect(segments[0]?.className).toContain(
      'bg-[repeating-linear-gradient(45deg,_transparent_0_4px,_rgba(255,255,255,0.05)_4px_8px)]',
    );
    expect(segments[0]?.style.backgroundColor).toBe('');
  });

  it('leaves explicit unobserved total as empty track space', () => {
    const { container } = render(
      <StackedBar
        segments={[
          { value: 10, color: 'bg-sky-400' },
          { value: 20, color: 'bg-violet-400' },
        ]}
        total={100}
      />,
    );

    expect(
      Array.from(
        container.querySelectorAll<HTMLElement>('span'),
        (segment) => segment.style.width,
      ),
    ).toEqual(['10%', '20%']);
  });

  it('renders nothing when no segment is positive', () => {
    const { container } = render(
      <StackedBar
        segments={[
          { value: 0, color: 'bg-sky-400' },
          { value: -10, color: '#ff0000' },
        ]}
      />,
    );

    expect(container.firstChild).toBeNull();
  });
});
