import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { Hint } from './primitives';

describe('Hint', () => {
  beforeEach(() => vi.useFakeTimers());

  afterEach(() => {
    cleanup();
    vi.useRealTimers();
  });

  it('mounts no popover until the full hover delay elapses', () => {
    const onPointerEnter = vi.fn();
    render(
      <Hint label={<span data-testid="hint-content">Tooltip Content</span>}>
        <button
          data-testid="hint-trigger"
          type="button"
          onPointerEnter={onPointerEnter}
        >
          Hover me
        </button>
      </Hint>,
    );

    const trigger = screen.getByTestId('hint-trigger');
    expect(screen.queryByTestId('hint-content')).toBeNull();
    fireEvent.pointerEnter(trigger);
    expect(onPointerEnter).toHaveBeenCalledOnce();

    act(() => vi.advanceTimersByTime(199));
    expect(screen.queryByTestId('hint-content')).toBeNull();
    act(() => vi.advanceTimersByTime(1));
    expect(screen.getByTestId('hint-content')).toBeDefined();
    expect(document.activeElement).not.toBe(screen.getByTestId('hint-trigger'));
  });

  it('keeps the first keyboard activation focused and Escape leaves focus on the trigger', () => {
    render(
      <Hint label={<span data-testid="hint-content">Tooltip Content</span>}>
        <button data-testid="hint-trigger" type="button">
          Focus me
        </button>
      </Hint>,
    );

    act(() => screen.getByTestId('hint-trigger').focus());
    expect(screen.getByTestId('hint-content')).toBeDefined();
    expect(document.activeElement).toBe(screen.getByTestId('hint-trigger'));

    fireEvent.keyDown(document, { key: 'Escape' });
    act(() => vi.advanceTimersByTime(1000));
    expect(screen.queryByTestId('hint-content')).toBeNull();
    expect(document.activeElement).toBe(screen.getByTestId('hint-trigger'));
  });

  it.each(['{Enter}', ' '])(
    'preserves first keyboard activation with openOnFocus=false using %s',
    async (key) => {
      vi.useRealTimers();
      const user = userEvent.setup();
      render(
        <Hint
          openOnFocus={false}
          label={<span data-testid="hint-content">Tooltip Content</span>}
        >
          <button type="button" data-testid="hint-trigger">
            Show breakdown
          </button>
        </Hint>,
      );

      await user.tab();
      expect(document.activeElement).toBe(screen.getByTestId('hint-trigger'));
      expect(screen.queryByTestId('hint-content')).toBeNull();
      await user.keyboard(key);
      expect(screen.getByTestId('hint-content')).toBeDefined();
      expect(document.activeElement).toBe(screen.getByTestId('hint-trigger'));

      await user.keyboard('{Escape}');
      await waitFor(() =>
        expect(screen.queryByTestId('hint-content')).toBeNull(),
      );
      expect(document.activeElement).toBe(screen.getByTestId('hint-trigger'));
    },
  );

  it.each([true, false])(
    'keeps the focused trigger focused through hover and Escape with openOnFocus=%s',
    (openOnFocus) => {
      render(
        <Hint
          openOnFocus={openOnFocus}
          label={<span data-testid="hint-content">Tooltip Content</span>}
        >
          <button type="button" data-testid="hint-trigger">
            Show breakdown
          </button>
        </Hint>,
      );

      act(() => screen.getByTestId('hint-trigger').focus());
      const trigger = screen.getByTestId('hint-trigger');
      if (!openOnFocus) expect(screen.queryByTestId('hint-content')).toBeNull();
      fireEvent.pointerEnter(trigger, { pointerType: 'mouse' });
      act(() => vi.advanceTimersByTime(200));
      expect(screen.getByTestId('hint-content')).toBeDefined();
      expect(document.activeElement).toBe(screen.getByTestId('hint-trigger'));

      fireEvent.keyDown(document, { key: 'Escape' });
      act(() => vi.advanceTimersByTime(1000));
      expect(screen.queryByTestId('hint-content')).toBeNull();
      expect(document.activeElement).toBe(screen.getByTestId('hint-trigger'));
    },
  );

  it.each([true, false])(
    'hovering an unfocused hint preserves the other focused control with openOnFocus=%s',
    (openOnFocus) => {
      render(
        <>
          <button type="button" data-testid="other-control">
            Other control
          </button>
          <Hint
            openOnFocus={openOnFocus}
            label={<span data-testid="hint-content">Tooltip Content</span>}
          >
            <button type="button" data-testid="hint-trigger">
              Show breakdown
            </button>
          </Hint>
        </>,
      );

      const other = screen.getByTestId('other-control');
      act(() => other.focus());
      fireEvent.pointerEnter(screen.getByTestId('hint-trigger'));
      act(() => vi.advanceTimersByTime(200));
      expect(screen.getByTestId('hint-content')).toBeDefined();
      expect(document.activeElement).toBe(other);
      fireEvent.keyDown(document, { key: 'Escape' });
      act(() => vi.advanceTimersByTime(1000));
      expect(screen.queryByTestId('hint-content')).toBeNull();
      expect(document.activeElement).toBe(other);
    },
  );

  it('samples focus when the hover delay completes rather than at pointer entry', () => {
    render(
      <>
        <button type="button" data-testid="other-control">
          Other control
        </button>
        <Hint
          openOnFocus={false}
          label={<span data-testid="hint-content">Tooltip Content</span>}
        >
          <button type="button" data-testid="hint-trigger">
            Show breakdown
          </button>
        </Hint>
      </>,
    );

    act(() => screen.getByTestId('hint-trigger').focus());
    fireEvent.pointerEnter(screen.getByTestId('hint-trigger'));
    act(() => vi.advanceTimersByTime(100));
    const other = screen.getByTestId('other-control');
    act(() => other.focus());
    act(() => vi.advanceTimersByTime(100));
    expect(screen.getByTestId('hint-content')).toBeDefined();
    expect(document.activeElement).toBe(other);
    fireEvent.keyDown(document, { key: 'Escape' });
    act(() => vi.advanceTimersByTime(1000));
    expect(screen.queryByTestId('hint-content')).toBeNull();
    expect(document.activeElement).toBe(other);
  });

  it.each([true, false])(
    'preserves openOnFocus=%s after the hint has already been engaged',
    (openOnFocus) => {
      render(
        <Hint
          openOnFocus={openOnFocus}
          label={<span data-testid="hint-content">Tooltip Content</span>}
        >
          <button type="button" data-testid="hint-trigger">
            Hover me
          </button>
        </Hint>,
      );

      fireEvent.pointerEnter(screen.getByTestId('hint-trigger'));
      act(() => vi.advanceTimersByTime(200));
      expect(screen.getByTestId('hint-content')).toBeDefined();
      fireEvent.keyDown(document, { key: 'Escape' });
      act(() => vi.advanceTimersByTime(1000));
      expect(screen.queryByTestId('hint-content')).toBeNull();

      fireEvent.focus(screen.getByTestId('hint-trigger'));
      expect(screen.queryByTestId('hint-content') !== null).toBe(openOnFocus);
    },
  );

  it('leaves children untouched when no label is provided', () => {
    const onChildClick = vi.fn();
    const onParentClick = vi.fn();
    render(
      <div onClick={onParentClick}>
        <Hint label="">
          <button type="button" onClick={onChildClick}>
            Click me
          </button>
        </Hint>
      </div>,
    );

    fireEvent.click(screen.getByRole('button', { name: 'Click me' }));
    expect(onChildClick).toHaveBeenCalledOnce();
    expect(onParentClick).toHaveBeenCalledOnce();
  });
  it('can preserve click bubbling when requested', () => {
    const onChildClick = vi.fn();
    const onParentClick = vi.fn();
    render(
      <div onClick={onParentClick}>
        <Hint label="Tooltip Content" stopClickPropagation={false}>
          <button type="button" onClick={onChildClick}>
            Click me
          </button>
        </Hint>
      </div>,
    );

    fireEvent.click(screen.getByRole('button', { name: 'Click me' }));
    expect(onChildClick).toHaveBeenCalledOnce();
    expect(onParentClick).toHaveBeenCalledOnce();
  });

  it('preserves child click handling without bubbling', () => {
    const onChildClick = vi.fn();
    const onParentClick = vi.fn();
    render(
      <div onClick={onParentClick}>
        <Hint label="Tooltip Content">
          <button type="button" onClick={onChildClick}>
            Click me
          </button>
        </Hint>
      </div>,
    );

    fireEvent.click(screen.getByRole('button', { name: 'Click me' }));
    expect(onChildClick).toHaveBeenCalledOnce();
    expect(onParentClick).not.toHaveBeenCalled();
  });
});
