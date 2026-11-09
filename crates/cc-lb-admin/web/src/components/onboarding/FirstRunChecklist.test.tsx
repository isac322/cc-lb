// @vitest-environment jsdom
import { cleanup, render, screen } from '@testing-library/react';
import type { ReactNode } from 'react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { usePrincipals, useUpstreams } from '../../lib/queries';
import { FirstRunChecklist } from './FirstRunChecklist';

vi.mock('@tanstack/react-router', () => ({
  Link: ({
    to,
    search,
    children,
    ...rest
  }: {
    to: string;
    search?: Record<string, string>;
    children?: ReactNode;
  }) => (
    <a
      href={search ? `${to}?${new URLSearchParams(search).toString()}` : to}
      {...rest}
    >
      {children}
    </a>
  ),
}));

vi.mock('../../lib/queries', () => ({
  usePrincipals: vi.fn(),
  useUpstreams: vi.fn(),
}));

afterEach(() => {
  cleanup();
  vi.clearAllMocks();
});

describe('FirstRunChecklist', () => {
  it('describes an upstream as a provider credential and keeps the new-upstream action', () => {
    vi.mocked(useUpstreams).mockReturnValue({
      data: { upstreams: [] },
    } as never);
    vi.mocked(usePrincipals).mockReturnValue({
      data: { principals: [] },
    } as never);

    render(<FirstRunChecklist requestSeen={false} />);

    expect(
      screen.getByText(
        'A provider credential the pool routes requests through.',
      ),
    ).toBeDefined();
    expect(
      screen.getByRole('link', { name: 'New upstream' }).getAttribute('href'),
    ).toBe('/upstreams?action=new');
  });
});
