import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it } from 'vitest';
import { LogSelect } from './LogSelect';

describe('LogSelect', () => {
  afterEach(cleanup);

  it('sizes the popup from the measured trigger width', () => {
    render(
      <LogSelect
        value=""
        options={[{ value: '1', label: 'One' }]}
        onChange={() => {}}
        allLabel="All"
        widthClass="w-44"
      />,
    );

    const trigger = screen.getByRole('combobox');
    expect(trigger.className).toContain('w-44');

    fireEvent.click(trigger);

    const popup = screen.getByTestId('log-select-popup');
    expect(popup.style.width).toBe('var(--anchor-width)');
    expect(popup.className).not.toContain('min-w-[220px]');
  });

  it('applies non-shrinking and single-line classes to the trigger and selected value', () => {
    render(
      <LogSelect
        value="1"
        options={[{ value: '1', label: 'One Long Label That Should Truncate' }]}
        onChange={() => {}}
        allLabel="All"
        widthClass="w-64"
      />,
    );

    const trigger = screen.getByRole('combobox');
    expect(trigger.className).toContain('shrink-0');

    const selectedValue = screen.getByText(
      'One Long Label That Should Truncate',
    );
    expect(selectedValue.className).toContain('truncate');

    expect(selectedValue.parentElement?.className).toContain('min-w-0');
    expect(selectedValue.parentElement?.className).toContain('flex-1');
    expect(selectedValue.parentElement?.className).toContain('overflow-hidden');
  });
});
