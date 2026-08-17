import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  Button,
  CardHeader,
  ConfirmDialog,
  Modal,
  Section,
  Skeleton,
} from './primitives';

afterEach(cleanup);

function renderSkeleton(className?: string, as?: 'div' | 'span') {
  const { container } = render(<Skeleton as={as} className={className} />);
  return container.firstElementChild as HTMLElement;
}

describe('Skeleton element semantics', () => {
  it('renders an aria-hidden div by default', () => {
    const skeleton = renderSkeleton();

    expect(skeleton.tagName).toBe('DIV');
    expect(skeleton.getAttribute('aria-hidden')).toBe('true');
  });

  it('renders an aria-hidden span when requested', () => {
    const skeleton = renderSkeleton(undefined, 'span');

    expect(skeleton.tagName).toBe('SPAN');
    expect(skeleton.getAttribute('aria-hidden')).toBe('true');
  });
});

describe('Subtitle slot semantics', () => {
  it('keeps block CardHeader subtitles in a block-safe slot', () => {
    const { container } = render(
      <CardHeader
        title="Card title"
        subtitle={<div data-testid="card-subtitle-content">Details</div>}
      />,
    );

    const subtitle = container.querySelector('[data-slot="card-subtitle"]');
    expect(subtitle?.tagName).toBe('DIV');
    expect(subtitle?.className).toContain('min-h-4');
    expect(
      subtitle?.querySelector('[data-testid="card-subtitle-content"]'),
    ).not.toBeNull();
    expect(container.querySelector('p')).toBeNull();
  });

  it('keeps block Section subtitles in a block-safe slot', () => {
    const { container } = render(
      <Section
        title="Section title"
        subtitle={<div data-testid="section-subtitle-content">Details</div>}
      >
        <div>Body</div>
      </Section>,
    );

    const subtitle = container.querySelector('[data-slot="section-subtitle"]');
    expect(subtitle?.tagName).toBe('DIV');
    expect(subtitle?.className).toContain('min-h-4');
    expect(
      subtitle?.querySelector('[data-testid="section-subtitle-content"]'),
    ).not.toBeNull();
    expect(container.querySelector('p')).toBeNull();
  });

  it('reserves a minimum line for plain text subtitle slots', () => {
    const { container } = render(
      <>
        <CardHeader title="Card title" subtitle="Card details" />
        <Section title="Section title" subtitle="Section details">
          <div>Body</div>
        </Section>
      </>,
    );

    expect(
      container.querySelector('[data-slot="card-subtitle"]')?.className,
    ).toContain('min-h-4');
    expect(
      container.querySelector('[data-slot="section-subtitle"]')?.className,
    ).toContain('min-h-4');
  });
});

describe('Skeleton dimensions', () => {
  it('uses the default height and width without custom dimensions', () => {
    expect(renderSkeleton().className).toBe('skeleton h-4 w-full');
  });

  it('keeps the default height with a custom width', () => {
    expect(renderSkeleton('w-32').className).toBe('skeleton h-4 w-32');
  });

  it('keeps the default width with a custom height', () => {
    expect(renderSkeleton('h-2').className).toBe('skeleton w-full h-2');
  });

  it('omits both defaults with custom height and width', () => {
    expect(renderSkeleton('h-2 w-2').className).toBe('skeleton h-2 w-2');
  });

  it('recognizes arbitrary height and width classes', () => {
    expect(renderSkeleton('h-[2.5rem] w-[calc(100%-1rem)]').className).toBe(
      'skeleton h-[2.5rem] w-[calc(100%-1rem)]',
    );
  });

  it('uses a size class instead of both defaults', () => {
    expect(renderSkeleton('size-8').className).toBe('skeleton size-8');
  });

  it('keeps base defaults for responsive-only dimensions', () => {
    expect(renderSkeleton('sm:h-8 md:w-32 lg:size-12').className).toBe(
      'skeleton h-4 w-full sm:h-8 md:w-32 lg:size-12',
    );
  });
});

describe('Button loading state', () => {
  it('disables the button, exposes busy state, and replaces its leading icon with a spinner', () => {
    render(
      <Button
        iconLeft={<span data-testid="button-leading-icon">Icon</span>}
        loading
      >
        Save changes
      </Button>,
    );

    const button = screen.getByRole('button', { name: 'Save changes' });
    expect(button.hasAttribute('disabled')).toBe(true);
    expect(button.getAttribute('aria-busy')).toBe('true');
    expect(button.querySelector('svg.animate-spin')).not.toBeNull();
    expect(screen.getByText('Save changes')).toBeDefined();
    expect(screen.queryByTestId('button-leading-icon')).toBeNull();
  });
});

describe('Modal dismissal state', () => {
  it('prevents close requests and disables the close control', () => {
    const onOpenChange = vi.fn();
    render(
      <Modal
        open
        onOpenChange={onOpenChange}
        preventDismiss
        title="Pending modal"
      >
        Modal body
      </Modal>,
    );

    const closeButton = screen.getByRole('button', { name: 'Close dialog' });
    expect(closeButton.hasAttribute('disabled')).toBe(true);

    fireEvent.click(closeButton);
    fireEvent.keyDown(screen.getByRole('dialog'), {
      key: 'Escape',
      code: 'Escape',
    });

    expect(onOpenChange).not.toHaveBeenCalled();
  });
});

describe('ConfirmDialog pending state', () => {
  it('disables actions, marks confirmation busy, and ignores dismissal requests', () => {
    const onConfirm = vi.fn();
    const onOpenChange = vi.fn();
    render(
      <ConfirmDialog
        open
        onOpenChange={onOpenChange}
        onConfirm={onConfirm}
        pending
        title="Delete item?"
        confirmLabel="Delete"
      />,
    );

    const cancelButton = screen.getByRole('button', { name: 'Cancel' });
    const confirmButton = screen.getByRole('button', { name: 'Delete' });
    expect(cancelButton.hasAttribute('disabled')).toBe(true);
    expect(confirmButton.hasAttribute('disabled')).toBe(true);
    expect(confirmButton.getAttribute('aria-busy')).toBe('true');
    expect(confirmButton.querySelector('svg.animate-spin')).not.toBeNull();

    fireEvent.click(cancelButton);
    fireEvent.click(confirmButton);
    fireEvent.keyDown(screen.getByRole('alertdialog'), {
      key: 'Escape',
      code: 'Escape',
    });

    expect(onConfirm).not.toHaveBeenCalled();
    expect(onOpenChange).not.toHaveBeenCalled();
  });

  it('can confirm without synchronously closing', () => {
    const onConfirm = vi.fn();
    const onOpenChange = vi.fn();
    render(
      <ConfirmDialog
        open
        closeOnConfirm={false}
        onOpenChange={onOpenChange}
        onConfirm={onConfirm}
        title="Apply change?"
      />,
    );

    fireEvent.click(screen.getByRole('button', { name: 'Confirm' }));

    expect(onConfirm).toHaveBeenCalledTimes(1);
    expect(onOpenChange).not.toHaveBeenCalled();
  });
});
