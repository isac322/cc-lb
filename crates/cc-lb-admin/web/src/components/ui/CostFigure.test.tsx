import { cleanup, render } from '@testing-library/react';
import { afterEach, describe, expect, it } from 'vitest';
import { CostFigure } from './CostFigure';

afterEach(cleanup);

describe('CostFigure', () => {
  it('mutes only right-hand fractional padding zeroes', () => {
    const { container } = render(
      <div>
        <CostFigure text="$12.30" />
        <CostFigure text="$12.00" />
        <CostFigure text="$0.0030" />
        <CostFigure text="Est. $0.0030" />
        <CostFigure text="$1200" />
      </div>,
    );
    const figures = Array.from(container.firstElementChild!.children);

    expect(figures.map((figure) => figure.textContent)).toEqual([
      '$12.30',
      '$12.00',
      '$0.0030',
      'Est. $0.0030',
      '$1200',
    ]);
    expect(figures[0]!.querySelector('.text-text-muted')?.textContent).toBe(
      '0',
    );
    expect(
      figures[0]!.querySelector('.text-text-muted')?.previousSibling
        ?.textContent,
    ).toBe('3');
    expect(figures[1]!.querySelector('.text-text-muted')?.textContent).toBe(
      '00',
    );
    expect(
      figures[2]!.querySelector('.text-text-muted')?.previousSibling
        ?.textContent,
    ).toBe('003');
    expect(
      figures[3]!.querySelector('.text-text-muted')?.previousSibling
        ?.textContent,
    ).toBe('003');
    expect(figures[4]!.querySelector('.text-text-muted')).toBeNull();
  });
});
