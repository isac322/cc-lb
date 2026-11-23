import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, test, vi } from 'vitest';
import { DetailHeader, DetailPane, DetailSection } from './DetailPane';

afterEach(cleanup);

function renderPane(onEnabledChange = vi.fn()) {
  return render(
    <DetailPane
      header={
        <DetailHeader
          backLabel="All things"
          onBack={() => {}}
          title="thing-1"
          titleId="thing-title"
          id="id-1"
          idLabel="Thing ID"
          enabled
          onEnabledChange={onEnabledChange}
          onDelete={() => {}}
        />
      }
    >
      <DetailSection title="Primary">body</DetailSection>
      <DetailSection title="Secondary" collapsible>
        hidden body
      </DetailSection>
      <DetailSection title="Opened" collapsible defaultOpen>
        open body
      </DetailSection>
    </DetailPane>,
  );
}

describe('DetailPane', () => {
  test('the sticky header gains its bottom line only once the pane scrolls', () => {
    const { container } = renderPane();
    const pane = container.querySelector('[data-detail-pane]') as HTMLElement;
    const header = container.querySelector(
      '[data-detail-header]',
    ) as HTMLElement;
    expect(header.className).toContain('border-transparent');

    pane.scrollTop = 120;
    fireEvent.scroll(pane);
    expect(header.dataset.scrolled).toBe('true');
    expect(header.className).toContain('border-subtle');

    pane.scrollTop = 0;
    fireEvent.scroll(pane);
    expect(header.dataset.scrolled).toBeUndefined();
  });

  test('secondary sections start collapsed unless opened by default', () => {
    renderPane();
    const secondary = screen
      .getByRole('heading', { name: 'Secondary' })
      .closest('details');
    const opened = screen
      .getByRole('heading', { name: 'Opened' })
      .closest('details');
    expect(secondary?.open).toBe(false);
    expect(opened?.open).toBe(true);
  });

  test('the enabled switch reports the requested state', () => {
    const onEnabledChange = vi.fn();
    renderPane(onEnabledChange);
    fireEvent.click(screen.getByRole('switch', { name: 'Enabled' }));
    expect(onEnabledChange).toHaveBeenCalledWith(false);
  });
});
