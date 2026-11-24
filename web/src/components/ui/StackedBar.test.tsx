import { cleanup, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it } from 'vitest';
import { StackedBar } from './StackedBar';

afterEach(cleanup);

describe('StackedBar', () => {
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

  it('scales the outer width without renormalizing its category composition', () => {
    render(
      <>
        <StackedBar
          ariaLabel="Highest cost"
          segments={[{ value: 100, color: '#ff0000' }]}
          total={100}
          maxTotal={100}
        />
        <StackedBar
          ariaLabel="Half cost"
          segments={[
            { value: 20, color: '#ff0000' },
            { value: 30, color: '#0000ff' },
          ]}
          total={50}
          maxTotal={100}
        />
        <StackedBar
          ariaLabel="Zero cost"
          segments={[]}
          total={0}
          maxTotal={100}
        />
      </>,
    );

    expect(screen.getByRole('img', { name: 'Highest cost' }).style.width).toBe(
      '100%',
    );
    const half = screen.getByRole('img', { name: 'Half cost' });
    expect(half.style.width).toBe('50%');
    expect(
      Array.from(
        half.querySelectorAll('span'),
        (segment) => segment.style.width,
      ),
    ).toEqual(['40%', '60%']);
    expect(screen.queryByRole('img', { name: 'Zero cost' })).toBeNull();
  });

  it('scales by the authoritative total when recorded categories outrun it', () => {
    render(
      <StackedBar
        ariaLabel="Authoritative cost"
        segments={[{ value: 75, color: '#ff0000' }]}
        total={50}
        maxTotal={100}
      />,
    );

    expect(
      screen.getByRole('img', { name: 'Authoritative cost' }).style.width,
    ).toBe('50%');
  });

  it('keeps unknown positive totals as a proportionate bare track', () => {
    render(
      <StackedBar
        ariaLabel="Unrecorded cost"
        segments={[]}
        total={50}
        maxTotal={100}
      />,
    );

    const track = screen.getByRole('img', { name: 'Unrecorded cost' });
    expect(track.style.width).toBe('50%');
    expect(track.querySelector('span')).toBeNull();
  });

  it('leaves default bars at their own full width for any total', () => {
    render(
      <>
        <StackedBar
          ariaLabel="First request"
          segments={[{ value: 100, color: '#ff0000' }]}
          total={100}
        />
        <StackedBar
          ariaLabel="Second request"
          segments={[{ value: 50, color: '#ff0000' }]}
          total={50}
        />
      </>,
    );

    for (const name of ['First request', 'Second request']) {
      const bar = screen.getByRole('img', { name });
      expect(bar.style.width).toBe('');
      expect(bar.querySelector('span')?.style.width).toBe('100%');
    }
  });

  it('renders no relative bar without a positive reference total', () => {
    const { container } = render(
      <StackedBar
        segments={[{ value: 50, color: '#ff0000' }]}
        total={50}
        maxTotal={0}
      />,
    );

    expect(container.firstChild).toBeNull();
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
