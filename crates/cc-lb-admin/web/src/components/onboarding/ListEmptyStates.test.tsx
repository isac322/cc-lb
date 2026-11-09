// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { UpstreamsListEmpty } from './ListEmptyStates';

afterEach(() => {
  cleanup();
});

describe('UpstreamsListEmpty', () => {
  it('describes provider credentials and keeps the new-upstream action', () => {
    const onCreate = vi.fn();

    render(<UpstreamsListEmpty onCreate={onCreate} />);

    const description = screen.getByText(
      /An upstream is a provider credential the pool routes requests through\./,
    );
    expect(description).toBeDefined();
    expect(description.textContent).not.toMatch(/Claude|Anthropic/);
    const newUpstreamButton = screen.getByRole('button', {
      name: 'New upstream',
    });
    expect(newUpstreamButton).toBeDefined();
    fireEvent.click(newUpstreamButton);
    expect(onCreate).toHaveBeenCalledOnce();
  });
});
