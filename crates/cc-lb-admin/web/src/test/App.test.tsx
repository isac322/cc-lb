import { describe, it, expect } from 'bun:test';
import { renderToString } from 'react-dom/server';
import { MemoryRouter } from 'react-router';
import App from '../App';

describe('App', () => {
  it('renders the dashboard shell and auth gate', () => {
    const html = renderToString(
      <MemoryRouter>
        <App />
      </MemoryRouter>
    );
    
    // Since localStorage is empty in test, it should render the AuthRequiredGate
    expect(html).toContain('Authentication Required');
    expect(html).toContain('Please enter your admin token');
  });
});
