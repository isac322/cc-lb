import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { MAX_FORMATTABLE_UNIX_SECONDS } from '../../lib/timezone';
import { TimeRangeSelect } from './TimeRangeSelect';
import { parseBound } from './TimeRangeSelect.helpers';

vi.mock('../../lib/locale', () => ({
  useTimezone: () => ({ effective: 'America/New_York' }),
}));

describe('TimeRangeSelect', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2024-01-01T12:00:00Z'));
  });

  afterEach(() => {
    cleanup();
    vi.useRealTimers();
  });

  it('shows custom inputs when mode is custom', () => {
    const onChange = vi.fn();
    render(<TimeRangeSelect value={{ mode: 'custom' }} onChange={onChange} />);

    expect(screen.queryByText('Since (America/New_York)')).not.toBeNull();
    expect(screen.queryByText('Until (America/New_York)')).not.toBeNull();
  });

  it('uses text inputs with YYYY-MM-DD HH:mm format instead of datetime-local', () => {
    const onChange = vi.fn();
    render(<TimeRangeSelect value={{ mode: 'custom' }} onChange={onChange} />);

    const datetimeInputs = document.querySelectorAll(
      'input[type="datetime-local"]',
    );
    expect(datetimeInputs.length).toBe(0);

    const sinceInput = screen.getByLabelText<HTMLInputElement>('Since time');
    const untilInput = screen.getByLabelText<HTMLInputElement>('Until time');

    expect(sinceInput.type).toBe('text');
    expect(sinceInput.placeholder).toBe('YYYY-MM-DD HH:mm');
    expect(untilInput.type).toBe('text');
    expect(untilInput.placeholder).toBe('YYYY-MM-DD HH:mm');

    fireEvent.change(sinceInput, { target: { value: '2024-11-03 03:30' } });
    fireEvent.change(untilInput, { target: { value: '2024-11-03 04:30' } });

    fireEvent.click(screen.getByRole('button', { name: 'Apply' }));

    expect(onChange).toHaveBeenCalledWith({
      mode: 'custom',
      since_unix_secs: 1_730_622_600,
      until_unix_secs: 1_730_626_259,
    });
  });

  it('validates ambiguous DST times', () => {
    const onChange = vi.fn();
    render(<TimeRangeSelect value={{ mode: 'custom' }} onChange={onChange} />);

    fireEvent.change(screen.getByLabelText('Since time'), {
      target: { value: '2024-11-03 01:30' },
    });

    const error = screen.getByText('Ambiguous time (DST)');
    const sinceInput = screen.getByLabelText<HTMLInputElement>('Since time');
    expect(sinceInput.getAttribute('aria-invalid')).toBe('true');
    expect(sinceInput.getAttribute('aria-describedby')).toBe(error.id);
    expect(
      screen.getByRole('button', { name: 'Apply' }).hasAttribute('disabled'),
    ).toBe(true);
    expect(onChange).not.toHaveBeenCalled();
  });

  it('validates nonexistent DST times', () => {
    const onChange = vi.fn();
    render(<TimeRangeSelect value={{ mode: 'custom' }} onChange={onChange} />);

    fireEvent.change(screen.getByLabelText('Since time'), {
      target: { value: '2024-03-10 02:30' },
    });

    expect(screen.queryByText('Time does not exist (DST)')).not.toBeNull();
    expect(
      screen.getByRole('button', { name: 'Apply' }).hasAttribute('disabled'),
    ).toBe(true);
    expect(onChange).not.toHaveBeenCalled();
  });

  it('reports calendar-invalid wall times separately from DST gaps', () => {
    render(<TimeRangeSelect value={{ mode: 'custom' }} onChange={vi.fn()} />);

    fireEvent.change(screen.getByLabelText('Since time'), {
      target: { value: '2024-02-30 12:00' },
    });
    fireEvent.change(screen.getByLabelText('Until time'), {
      target: { value: '2024-03-01 12:00' },
    });

    expect(screen.queryByText('Invalid time')).not.toBeNull();
    expect(screen.queryByText('Time does not exist (DST)')).toBeNull();
  });

  it('requires both inputs before emitting bounds', () => {
    const onChange = vi.fn();
    render(<TimeRangeSelect value={{ mode: 'custom' }} onChange={onChange} />);

    fireEvent.change(screen.getByLabelText('Since time'), {
      target: { value: '2024-11-03 03:30' },
    });
    expect(onChange).not.toHaveBeenCalled();
    expect(screen.getByText('Required')).toBeDefined();
    expect(
      screen.getByRole('button', { name: 'Apply' }).hasAttribute('disabled'),
    ).toBe(true);

    fireEvent.change(screen.getByLabelText('Until time'), {
      target: { value: '2024-11-03 04:30' },
    });
    expect(
      screen.getByRole('button', { name: 'Apply' }).hasAttribute('disabled'),
    ).toBe(false);

    fireEvent.click(screen.getByRole('button', { name: 'Apply' }));
    expect(onChange).toHaveBeenCalledWith({
      mode: 'custom',
      since_unix_secs: 1_730_622_600,
      until_unix_secs: 1_730_626_259,
    });
  });

  it('never emits inverted range', () => {
    const onChange = vi.fn();
    render(
      <TimeRangeSelect
        value={{ mode: 'custom', since_unix_secs: 1700000000 }}
        onChange={onChange}
      />,
    );

    fireEvent.change(screen.getByLabelText('Until time'), {
      target: { value: '2023-11-03 03:30' },
    });
    expect(screen.queryByText('Must be >= Since')).not.toBeNull();
    expect(
      screen.getByRole('button', { name: 'Apply' }).hasAttribute('disabled'),
    ).toBe(true);
    expect(onChange).not.toHaveBeenCalled();
  });

  it('Cancel restores applied values', () => {
    const onChange = vi.fn();
    render(
      <TimeRangeSelect
        value={{
          mode: 'custom',
          since_unix_secs: 1700000000,
          until_unix_secs: 1700003600,
        }}
        onChange={onChange}
      />,
    );

    const sinceInput = screen.getByLabelText<HTMLInputElement>('Since time');
    const initialValue = sinceInput.value;

    fireEvent.change(sinceInput, { target: { value: '2024-11-03 03:30' } });
    expect(sinceInput.value).toBe('2024-11-03 03:30');

    fireEvent.click(screen.getByRole('button', { name: 'Cancel' }));
    expect(sinceInput.value).toBe(initialValue);
    expect(onChange).not.toHaveBeenCalled();
  });

  it('Escape restores applied values', () => {
    const onChange = vi.fn();
    render(
      <TimeRangeSelect
        value={{
          mode: 'custom',
          since_unix_secs: 1700000000,
          until_unix_secs: 1700003600,
        }}
        onChange={onChange}
      />,
    );

    const sinceInput = screen.getByLabelText<HTMLInputElement>('Since time');
    const initialValue = sinceInput.value;

    fireEvent.change(sinceInput, { target: { value: '2024-11-03 03:30' } });

    fireEvent.keyDown(sinceInput, { key: 'Escape' });
    expect(sinceInput.value).toBe(initialValue);
    expect(onChange).not.toHaveBeenCalled();
  });

  it('preset Refresh reanchors canonical since', () => {
    const onChange = vi.fn();
    render(
      <TimeRangeSelect
        value={{ mode: '1h', since_unix_secs: 1000 }}
        onChange={onChange}
      />,
    );

    const refreshBtn = screen.getByTitle('Refresh time window');
    fireEvent.click(refreshBtn);

    expect(onChange).toHaveBeenCalledWith({
      mode: '1h',
      since_unix_secs: Math.floor(Date.now() / 1000) - 3600,
    });
  });

  it('has exactly one All time option', () => {
    const onChange = vi.fn();
    render(<TimeRangeSelect value={{ mode: 'all' }} onChange={onChange} />);

    const trigger = screen.getByRole('combobox');
    fireEvent.click(trigger);

    const options = screen.getAllByRole('option', { name: 'All time' });
    expect(options.length).toBe(1);
  });

  it('uses compact labels for every preset and custom range', () => {
    const onChange = vi.fn();
    render(<TimeRangeSelect value={{ mode: 'all' }} onChange={onChange} />);

    fireEvent.click(screen.getByRole('combobox'));

    for (const label of ['1h', '6h', '24h', '7d', 'Custom']) {
      expect(screen.getByRole('option', { name: label })).toBeDefined();
    }
  });

  it('writes a calendar-picked date into the Since field defaulting to 00:00', () => {
    const onChange = vi.fn();
    render(<TimeRangeSelect value={{ mode: 'custom' }} onChange={onChange} />);

    fireEvent.click(screen.getByLabelText('Since (America/New_York) calendar'));
    const dayButtons = document.querySelectorAll<HTMLButtonElement>(
      'button.rdp-day_button',
    );
    const enabledDay = Array.from(dayButtons).find((b) => !b.disabled);
    if (enabledDay === undefined) throw new Error('no enabled day button');
    fireEvent.click(enabledDay);

    const sinceInput = screen.getByLabelText<HTMLInputElement>('Since time');
    expect(sinceInput.value).toMatch(/^\d{4}-\d{2}-\d{2} 00:00$/);
  });

  it('uses empty-bound defaults after clearing an applied range', () => {
    render(
      <TimeRangeSelect
        value={{
          mode: 'custom',
          since_unix_secs: 1_783_247_400,
          until_unix_secs: 1_783_352_700,
        }}
        onChange={vi.fn()}
      />,
    );

    const sinceInput = screen.getByLabelText<HTMLInputElement>('Since time');
    const untilInput = screen.getByLabelText<HTMLInputElement>('Until time');
    fireEvent.change(sinceInput, { target: { value: '' } });
    fireEvent.change(untilInput, { target: { value: '' } });
    fireEvent.click(screen.getByLabelText('Since (America/New_York) calendar'));

    const dayButtons = document.querySelectorAll<HTMLButtonElement>(
      'button.rdp-day_button',
    );
    const enabledDay = Array.from(dayButtons).find(
      (button) => !button.disabled,
    );
    if (enabledDay === undefined) throw new Error('no enabled day button');
    fireEvent.click(enabledDay);

    expect(sinceInput.value).toMatch(/^\d{4}-\d{2}-\d{2} 00:00$/);
    expect(untilInput.value).toBe('');
  });

  it('round-trips the maximum supported Until minute', () => {
    expect(
      parseBound(
        '9999-12-31 09:59',
        '9999-12-31 09:59',
        'Pacific/Kiritimati',
        'until',
      ).ts,
    ).toBe(MAX_FORMATTABLE_UNIX_SECONDS);
  });
});
