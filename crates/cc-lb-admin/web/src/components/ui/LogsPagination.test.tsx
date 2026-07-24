// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { LogsPagination } from './LogsPagination';

describe('LogsPagination', () => {
  afterEach(() => {
    cleanup();
  });

  it('renders correctly on the first page', () => {
    const onPrev = vi.fn();
    const onNext = vi.fn();

    render(
      <LogsPagination
        page={0}
        pageCount={10}
        totalRows={450}
        pageSize={50}
        hasMore={false}
        onPrev={onPrev}
        onNext={onNext}
      />,
    );

    const nav = screen.getByRole('navigation', { name: 'Log pagination' });
    expect(nav).toBeDefined();

    const statusText = screen.getByText('Showing 1–50 of 450');
    expect(statusText).toBeDefined();
    expect(statusText.parentElement?.getAttribute('aria-live')).toBe('polite');

    expect(screen.getByText('Page 1 of 10')).toBeDefined();

    const prevButton = screen.getByRole('button', { name: 'Previous page' });
    expect(prevButton.hasAttribute('disabled')).toBe(true);
    expect(prevButton.textContent).toContain('Prev');

    const nextButton = screen.getByRole('button', { name: 'Next page' });
    expect(nextButton.hasAttribute('disabled')).toBe(false);
    expect(nextButton.textContent).toContain('Next');
  });

  it('renders correctly on the last page', () => {
    const onPrev = vi.fn();
    const onNext = vi.fn();

    render(
      <LogsPagination
        page={8}
        pageCount={9}
        totalRows={450}
        pageSize={50}
        hasMore={false}
        onPrev={onPrev}
        onNext={onNext}
      />,
    );

    expect(screen.getByText('Showing 401–450 of 450')).toBeDefined();
    expect(screen.getByText('Page 9 of 9')).toBeDefined();

    const prevButton = screen.getByRole('button', { name: 'Previous page' });
    expect(prevButton.hasAttribute('disabled')).toBe(false);
    expect(prevButton.textContent).toContain('Prev');

    const nextButton = screen.getByRole('button', { name: 'Next page' });
    expect(nextButton.hasAttribute('disabled')).toBe(true);
    expect(nextButton.textContent).toContain('Next');
  });

  it('keeps Next enabled and avoids an exact count while older rows exist', () => {
    render(
      <LogsPagination
        page={8}
        pageCount={9}
        totalRows={450}
        pageSize={50}
        hasMore
        onPrev={vi.fn()}
        onNext={vi.fn()}
      />,
    );

    expect(screen.getByText('Showing 401–450 of 450+')).toBeDefined();
    expect(screen.getByText('Page 9 of 9+')).toBeDefined();
    expect(
      screen
        .getByRole('button', { name: 'Next page' })
        .hasAttribute('disabled'),
    ).toBe(false);
  });

  it('caps the open-ended row label at 999+', () => {
    render(
      <LogsPagination
        page={19}
        pageCount={24}
        totalRows={1_200}
        pageSize={50}
        hasMore
        onPrev={vi.fn()}
        onNext={vi.fn()}
      />,
    );

    expect(screen.getByText('Showing 951–1000 of 999+')).toBeDefined();
  });

  it('renders correctly on a single page', () => {
    const onPrev = vi.fn();
    const onNext = vi.fn();

    render(
      <LogsPagination
        page={0}
        pageCount={1}
        totalRows={30}
        pageSize={50}
        hasMore={false}
        onPrev={onPrev}
        onNext={onNext}
      />,
    );

    expect(screen.getByText('Showing 1–30 of 30')).toBeDefined();
    expect(screen.getByText('Page 1 of 1')).toBeDefined();

    const prevButton = screen.getByRole('button', { name: 'Previous page' });
    expect(prevButton.hasAttribute('disabled')).toBe(true);
    expect(prevButton.textContent).toContain('Prev');

    const nextButton = screen.getByRole('button', { name: 'Next page' });
    expect(nextButton.hasAttribute('disabled')).toBe(true);
    expect(nextButton.textContent).toContain('Next');
  });

  it('renders correctly with 0 rows', () => {
    const onPrev = vi.fn();
    const onNext = vi.fn();

    render(
      <LogsPagination
        page={0}
        pageCount={1}
        totalRows={0}
        pageSize={50}
        hasMore={false}
        onPrev={onPrev}
        onNext={onNext}
      />,
    );

    expect(screen.getByText('Showing 0–0 of 0')).toBeDefined();
    expect(screen.getByText('Page 1 of 1')).toBeDefined();

    const prevButton = screen.getByRole('button', { name: 'Previous page' });
    expect(prevButton.hasAttribute('disabled')).toBe(true);
    expect(prevButton.textContent).toContain('Prev');

    const nextButton = screen.getByRole('button', { name: 'Next page' });
    expect(nextButton.hasAttribute('disabled')).toBe(true);
    expect(nextButton.textContent).toContain('Next');
  });

  it('calls onPrev and onNext when buttons are clicked', () => {
    const onPrev = vi.fn();
    const onNext = vi.fn();

    render(
      <LogsPagination
        page={1}
        pageCount={10}
        totalRows={450}
        pageSize={50}
        hasMore={false}
        onPrev={onPrev}
        onNext={onNext}
      />,
    );

    const prevButton = screen.getByRole('button', { name: 'Previous page' });
    const nextButton = screen.getByRole('button', { name: 'Next page' });

    fireEvent.click(prevButton);
    expect(onPrev).toHaveBeenCalledTimes(1);

    fireEvent.click(nextButton);
    expect(onNext).toHaveBeenCalledTimes(1);
  });
});
