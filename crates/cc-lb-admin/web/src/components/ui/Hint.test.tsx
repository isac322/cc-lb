import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
} from '@testing-library/react';
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
