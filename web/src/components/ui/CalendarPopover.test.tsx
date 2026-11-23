import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { CalendarPopover } from './CalendarPopover';

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

describe('CalendarPopover', () => {
  it('renders a labelled trigger with no calendar until opened', () => {
    render(
      <CalendarPopover
        value=""
        onPickDate={vi.fn()}
        triggerLabel="Since calendar"
      />,
    );
    const trigger = screen.getByLabelText('Since calendar');
    expect(trigger).toBeDefined();
    expect(trigger.className).toContain('w-6');
    expect(trigger.className).toContain('h-6');
    expect(trigger.className).toContain('lg:w-5');
    expect(trigger.className).toContain('lg:h-5');
    expect(screen.queryByRole('grid')).toBeNull();
  });

  it('opens a date-only calendar (no time input) on trigger click', () => {
    render(
      <CalendarPopover
        value="2026-07-15 09:30"
        onPickDate={vi.fn()}
        triggerLabel="Since calendar"
      />,
    );
    fireEvent.click(screen.getByLabelText('Since calendar'));
    expect(screen.getByRole('grid')).toBeDefined();
    expect(document.querySelector('.rdp-root input')).toBeNull();
  });

  it('opens above the field so it does not cover following Custom actions', () => {
    render(
      <CalendarPopover
        value="2026-07-15 09:30"
        onPickDate={vi.fn()}
        triggerLabel="Since calendar"
      />,
    );
    fireEvent.click(screen.getByLabelText('Since calendar'));

    expect(document.querySelector('[data-side="top"]')).not.toBeNull();
  });

  it('uses the existing accessible Modal surface below the desktop breakpoint', () => {
    const onEscape = vi.fn();
    vi.stubGlobal(
      'matchMedia',
      vi.fn(() => ({
        matches: false,
        media: '(min-width: 1024px)',
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
        dispatchEvent: vi.fn(),
      })),
    );
    render(
      <CalendarPopover
        value="2026-07-15 09:30"
        onPickDate={vi.fn()}
        onEscape={onEscape}
        triggerLabel="Since calendar"
      />,
    );
    fireEvent.click(screen.getByLabelText('Since calendar'));

    expect(screen.getByRole('dialog')).toBeDefined();
    expect(screen.getByRole('heading', { name: 'Choose date' })).toBeDefined();
    expect(screen.getByRole('grid')).toBeDefined();

    fireEvent.keyDown(screen.getByRole('button', { name: 'Close dialog' }), {
      key: 'Escape',
    });
    expect(onEscape).toHaveBeenCalledTimes(1);
  });

  it('closes the calendar on Escape', () => {
    const onEscape = vi.fn();
    render(
      <CalendarPopover
        value="2026-07-15 09:30"
        onPickDate={vi.fn()}
        onEscape={onEscape}
        triggerLabel="Since calendar"
      />,
    );
    fireEvent.click(screen.getByLabelText('Since calendar'));
    expect(screen.getByRole('grid')).toBeDefined();

    fireEvent.keyDown(screen.getByRole('grid'), { key: 'Escape' });
    expect(screen.queryByRole('grid')).toBeNull();
    expect(onEscape).toHaveBeenCalledTimes(1);
  });

  it('calls onPickDate with a YYYY-MM-DD string when a day is selected', () => {
    const onPickDate = vi.fn<(iso: string) => void>();
    render(
      <CalendarPopover
        value="2026-07-15 09:30"
        onPickDate={onPickDate}
        triggerLabel="Since calendar"
      />,
    );
    fireEvent.click(screen.getByLabelText('Since calendar'));

    const dayButtons = document.querySelectorAll<HTMLButtonElement>(
      'button.rdp-day_button',
    );
    expect(dayButtons.length).toBeGreaterThan(0);
    const enabledDay = Array.from(dayButtons).find((b) => !b.disabled);
    if (enabledDay === undefined) throw new Error('no enabled day button');
    fireEvent.click(enabledDay);

    expect(onPickDate).toHaveBeenCalledTimes(1);
    expect(onPickDate).toHaveBeenCalledWith(
      expect.stringMatching(/^\d{4}-\d{2}-\d{2}$/),
    );
  });
});
