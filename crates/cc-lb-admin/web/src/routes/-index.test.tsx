import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import { PoolQuotaLegend } from './index';

describe('PoolQuotaLegend', () => {
  it('renders Fable only when the selected range has Fable data', () => {
    const { rerender } = render(
      <PoolQuotaLegend
        latest={{ '5h': 20, '7d': 40, '7d_fable': null }}
        showFable={false}
      />,
    );
    expect(screen.queryByText(/Fable/)).toBeNull();

    rerender(
      <PoolQuotaLegend
        latest={{ '5h': 20, '7d': 40, '7d_fable': 28 }}
        showFable
      />,
    );
    expect(screen.getByText(/Fable/).textContent).toBe('Fable · 28%');
  });
});
