import { cleanup, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { Select } from './Select';

describe('Select', () => {
  afterEach(cleanup);

  it('keeps a long selected label on one truncated line', () => {
    render(
      <Select
        value="1"
        options={[{ value: '1', label: 'One Long Label That Should Truncate' }]}
        onChange={() => {}}
        allLabel="All"
        className="w-64"
      />,
    );

    const trigger = screen.getByRole('combobox');
    expect(trigger.className).toContain('shrink-0');

    const selectedValue = screen.getByText(
      'One Long Label That Should Truncate',
    );
    expect(selectedValue.className).toContain('truncate');
    expect(selectedValue.parentElement?.className).toContain('min-w-0');
    expect(selectedValue.parentElement?.className).toContain('overflow-hidden');
  });

  it('shows the placeholder when no option matches and reports picks', async () => {
    const onChange = vi.fn();
    render(
      <Select
        value=""
        options={[
          { value: 'en-US', label: 'English' },
          { value: 'ko-KR', label: 'Korean' },
        ]}
        onChange={onChange}
        placeholder="Choose a locale"
        aria-label="Locale"
      />,
    );

    const user = userEvent.setup();
    const trigger = screen.getByRole('combobox', { name: 'Locale' });
    expect(trigger.textContent).toContain('Choose a locale');

    await user.click(trigger);
    // Base UI opens a mouse-pressed Select on the next animation frame, so the
    // listbox options appear asynchronously after the click resolves.
    // Without `allLabel` there is no synthetic "all" item.
    expect(await screen.findAllByRole('option')).toHaveLength(2);
    await user.click(await screen.findByRole('option', { name: 'Korean' }));
    expect(onChange).toHaveBeenCalledWith('ko-KR');
  });
});
