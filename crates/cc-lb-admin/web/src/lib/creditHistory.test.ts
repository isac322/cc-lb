import { describe, expect, it } from 'vitest';
import { formatCreditInteger, mergeCreditChartRows } from './creditHistory';

describe('credit history display', () => {
  it('truncates raw decimals toward zero and keeps negative zero neutral', () => {
    expect(formatCreditInteger('12.99')).toBe('12');
    expect(formatCreditInteger('0.42')).toBe('0');
    expect(formatCreditInteger('-0.42')).toBe('0');
    expect(formatCreditInteger('-12.99')).toBe('-12');
    expect(formatCreditInteger('999999999999999999999.99')).toBe(
      '999,999,999,999,999,999,999',
    );
    expect(formatCreditInteger('NaN')).toBe('—');
  });

  it('preserves independent quota and credit timestamps and explicit gaps', () => {
    const rows = mergeCreditChartRows(
      [
        { unix: 100, '5h': 25 },
        { unix: 200, '5h': 30 },
      ],
      [
        {
          at_unix_millis: 100_000,
          balance: '0.6',
          observed_at_unix_millis: 100_000,
          contributing_upstreams: 1,
          eligible_upstreams: 2,
        },
        {
          at_unix_millis: 150_000,
          balance: null,
          observed_at_unix_millis: null,
          contributing_upstreams: 0,
          eligible_upstreams: 2,
        },
        {
          at_unix_millis: 200_000,
          balance: '1.2',
          observed_at_unix_millis: 200_000,
          contributing_upstreams: 2,
          eligible_upstreams: 2,
        },
      ],
    );
    expect(rows.map((row) => [row.unix, row.credit_balance])).toEqual([
      [100, '0.6'],
      [150, null],
      [200, '1.2'],
    ]);
    expect(rows[0]['5h']).toBe(25);
    expect(rows.at(-1)?.credits).toBe(1.2);
  });
});
