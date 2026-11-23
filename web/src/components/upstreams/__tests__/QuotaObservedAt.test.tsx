import { cleanup, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, test, vi } from 'vitest';
import { RelativeTime } from '../../ui/RelativeTime';
import { QuotaObservedAt } from '../QuotaObservedAt';

vi.mock('../../ui/RelativeTime', () => ({
  RelativeTime: vi.fn(({ ts }: { readonly ts: number | null }) => (
    <span data-testid="relative-time">{String(ts)}</span>
  )),
}));

afterEach(() => {
  cleanup();
  vi.clearAllMocks();
});

describe('QuotaObservedAt', () => {
  test('renders the snapshot observed timestamp through live RelativeTime', () => {
    render(
      <QuotaObservedAt
        snapshot={{ observed_at_unix_millis: 1_797_523_200_000 }}
      />,
    );

    expect(RelativeTime).toHaveBeenCalledWith(
      { className: 'normal-case', ts: 1_797_523_200_000 },
      undefined,
    );
    expect(screen.getByTestId('relative-time').textContent).toBe(
      '1797523200000',
    );
  });

  test('does not invent a relative label without an observation time', () => {
    render(<QuotaObservedAt snapshot={{ observed_at_unix_millis: null }} />);

    expect(screen.getByText('—').textContent).toBe('—');
    expect(RelativeTime).not.toHaveBeenCalled();
  });
});
