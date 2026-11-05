import { cleanup, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it } from 'vitest';
import { ProviderLogo } from '../ProviderLogo';

afterEach(cleanup);

describe('ProviderLogo', () => {
  it.each([14, 16, 20, 24])(
    'preserves the requested %spx mark size',
    (size) => {
      render(
        <ProviderLogo provider="anthropic" size={size} title="Anthropic" />,
      );

      const logo = screen.getByRole('img', { name: 'Anthropic' });
      expect(logo.getAttribute('style')).toContain(`width: ${size}px`);
      expect(logo.getAttribute('style')).toContain(`height: ${size}px`);
      expect(logo.querySelector('path')?.getAttribute('d')).not.toBeNull();
    },
  );

  it('uses provider-only accessible naming', () => {
    render(<ProviderLogo provider="openai" />);

    expect(screen.getByRole('img', { name: 'OpenAI provider' })).toBeDefined();
    expect(screen.queryByText('API')).toBeNull();
  });
});
