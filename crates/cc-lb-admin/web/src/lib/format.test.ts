import { describe, expect, it } from 'vitest';
import { fmtMsCompact } from './format';

describe('fmtMsCompact', () => {
  it('returns dash for null', () => {
    expect(fmtMsCompact(null)).toEqual({ value: '—', unit: '' });
  });
  it('returns dash for undefined', () => {
    expect(fmtMsCompact(undefined)).toEqual({ value: '—', unit: '' });
  });
  it('formats 0 as 0ms', () => {
    expect(fmtMsCompact(0)).toEqual({ value: '0', unit: 'ms' });
  });
  it('formats sub-second as ms', () => {
    expect(fmtMsCompact(850)).toEqual({ value: '850', unit: 'ms' });
  });
  it('rounds sub-second to integer ms', () => {
    expect(fmtMsCompact(123.7)).toEqual({ value: '124', unit: 'ms' });
  });
  it('formats 1000 as 1.0s', () => {
    expect(fmtMsCompact(1000)).toEqual({ value: '1.0', unit: 's' });
  });
  it('formats 1500 as 1.5s', () => {
    expect(fmtMsCompact(1500)).toEqual({ value: '1.5', unit: 's' });
  });
  it('formats 12345 as 12.3s', () => {
    expect(fmtMsCompact(12_345)).toEqual({ value: '12.3', unit: 's' });
  });
  it('formats 60000 as 1.0m', () => {
    expect(fmtMsCompact(60_000)).toEqual({ value: '1.0', unit: 'm' });
  });
  it('formats 72000 as 1.2m', () => {
    expect(fmtMsCompact(72_000)).toEqual({ value: '1.2', unit: 'm' });
  });
});
