// @vitest-environment jsdom
import { cleanup, render, renderHook, screen } from '@testing-library/react';
import type { ReactNode } from 'react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { usePrincipals, useUpstreams } from '../../lib/queries';
import { FirstRunChecklist, useFirstRunIncomplete } from './FirstRunChecklist';

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

  it.each([
    'anthropic_api_key',
    'anthropic_oauth',
    'openai_api_key',
    'openai_oauth',
    'gemini_api_key',
    'gemini_oauth',
  ])(
    'does not show setup when a %s upstream exists in the overview',
    (kind) => {
      vi.mocked(useUpstreams).mockReturnValue({
        data: { upstreams: [{ kind }] },
      } as never);
      vi.mocked(usePrincipals).mockReturnValue({
        data: { principals: [] },
      } as never);

      const { container } = render(<FirstRunChecklist requestSeen={false} />);

      expect(container.textContent).toBe('');
    },
  );

  it('shows all three steps when only other providers have upstreams', () => {
    vi.mocked(useUpstreams).mockReturnValue({
      data: { upstreams: [{ kind: 'anthropic_oauth' }] },
    } as never);
    vi.mocked(usePrincipals).mockReturnValue({
      data: { principals: [{ id: 'principal-1' }] },
    } as never);

    render(<FirstRunChecklist requestSeen={false} scopedProvider="openai" />);

    expect(
      screen.getByRole('heading', { name: 'Set up the pool' }),
    ).toBeDefined();
    expect(screen.getByText('Add an upstream')).toBeDefined();
    expect(screen.getByText('Create a principal')).toBeDefined();
    expect(
      screen.getByText('Issue a proxy key and send a request'),
    ).toBeDefined();
  });

  it.each(['upstreams', 'principals', 'requests'])(
    'renders nothing while %s are loading',
    (source) => {
      vi.mocked(useUpstreams).mockReturnValue({
        data: source === 'upstreams' ? undefined : { upstreams: [] },
      } as never);
      vi.mocked(usePrincipals).mockReturnValue({
        data: source === 'principals' ? undefined : { principals: [] },
      } as never);

      const { container } = render(
        <FirstRunChecklist
          requestSeen={source === 'requests' ? undefined : false}
        />,
      );

      expect(container.textContent).toBe('');
    },
  );
});

describe('useFirstRunIncomplete', () => {
  it('waits for upstream inventory without depending on principal or request data', () => {
    vi.mocked(useUpstreams).mockReturnValue({ data: undefined } as never);
    const { result, rerender } = renderHook(() => useFirstRunIncomplete());

    expect(result.current).toBeUndefined();
    vi.mocked(useUpstreams).mockReturnValue({
      data: { upstreams: [{ kind: 'openai_api_key' }] },
    } as never);
    rerender();

    expect(result.current).toBe(false);
    expect(usePrincipals).not.toHaveBeenCalled();
  });

  it.each([
    { scope: undefined, kind: 'gemini_oauth', incomplete: false },
    { scope: 'anthropic', kind: 'anthropic_api_key', incomplete: false },
    { scope: 'openai', kind: 'openai_oauth', incomplete: false },
    { scope: 'gemini', kind: 'gemini_api_key', incomplete: false },
    { scope: 'openai', kind: 'anthropic_oauth', incomplete: true },
    { scope: 'gemini', kind: 'openai_api_key', incomplete: true },
  ] as const)(
    'gates $scope with $kind: $incomplete',
    ({ scope, kind, incomplete }) => {
      vi.mocked(useUpstreams).mockReturnValue({
        data: { upstreams: [{ kind }] },
      } as never);

      const { result } = renderHook(() => useFirstRunIncomplete(scope));

      expect(result.current).toBe(incomplete);
    },
  );

  it('gates an empty inventory', () => {
    vi.mocked(useUpstreams).mockReturnValue({
      data: { upstreams: [] },
    } as never);

    const { result } = renderHook(() => useFirstRunIncomplete());

    expect(result.current).toBe(true);
  });
});
