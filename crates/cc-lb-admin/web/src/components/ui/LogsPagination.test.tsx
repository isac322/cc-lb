// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { LogsPagination } from './LogsPagination';

describe('LogsPagination', () => {
  afterEach(() => {
    cleanup();
  });
  it('keeps a fixed disabled shell while initial rows load', () => {
    render(
      <LogsPagination
        page={0}
        pageCount={1}
        totalRows={0}
        pageSize={50}
        hasMore={false}
        loading
        onPrev={vi.fn()}
        onNext={vi.fn()}
      />,
    );

    const nav = screen.getByRole('navigation', { name: 'Log pagination' });
    expect(nav.className).toContain('h-14');
    expect(nav.className).toContain('shrink-0');
    expect(nav.getAttribute('aria-busy')).toBe('true');
    expect(nav.querySelectorAll('.skeleton')).toHaveLength(1);
    expect(screen.queryByText('Showing 0–0 of 0')).toBeNull();
    expect(
      screen
        .getByRole('button', { name: 'Previous page' })
        .hasAttribute('disabled'),
    ).toBe(true);
    expect(
      screen
        .getByRole('button', { name: 'Next page' })
        .hasAttribute('disabled'),
    ).toBe(true);
  });

  it('shows next-page pending feedback inside the existing button slot', () => {
    const onNext = vi.fn();
    const props = {
      page: 0,
      pageCount: 1,
      totalRows: 50,
      pageSize: 50,
      hasMore: true,
      onPrev: vi.fn(),
      onNext,
    };
    const { rerender } = render(<LogsPagination {...props} />);
    const readyButton = screen.getByRole('button', { name: 'Next page' });
    const readyClassName = readyButton.className;

    rerender(<LogsPagination {...props} loadingNext />);

    const pendingButton = screen.getByRole('button', {
      name: 'Loading next page',
    });
    expect(pendingButton.className).toBe(readyClassName);
    expect(pendingButton.textContent).toContain('Next');
    expect(pendingButton.hasAttribute('disabled')).toBe(true);
    expect(pendingButton.getAttribute('aria-busy')).toBe('true');
    expect(pendingButton.querySelector('svg.animate-spin')).not.toBeNull();

    fireEvent.click(pendingButton);
    expect(onNext).not.toHaveBeenCalled();
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
    expect(nav.className).toContain('h-14');
    expect(nav.className).toContain('shrink-0');

    const statusText = screen.getByText('Showing 1–50 of 450');
    expect(statusText).toBeDefined();
    expect(statusText.parentElement?.getAttribute('aria-live')).toBe('polite');

    expect(screen.queryByText(/^Page /)).toBeNull();

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
    expect(
      screen
        .getByRole('button', { name: 'Next page' })
        .hasAttribute('disabled'),
    ).toBe(false);
  });

  it('shows the loaded maximum in the open-ended row label', () => {
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

    expect(screen.getByText('Showing 951–1000 of 1200+')).toBeDefined();
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
    const nav = screen.getByRole('navigation', { name: 'Log pagination' });
    expect(nav.className).toContain('h-14');
    expect(nav.className).toContain('shrink-0');

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
  it('disables Next while the next page is loading', () => {
    render(
      <LogsPagination
        page={0}
        pageCount={1}
        totalRows={50}
        pageSize={50}
        hasMore={true}
        loadingNext={true}
        onPrev={vi.fn()}
        onNext={vi.fn()}
      />,
    );

    const nextButton = screen.getByRole('button', {
      name: 'Loading next page',
    });
    expect(nextButton.hasAttribute('disabled')).toBe(true);
    expect(nextButton.textContent).toContain('Next');
  });
});
