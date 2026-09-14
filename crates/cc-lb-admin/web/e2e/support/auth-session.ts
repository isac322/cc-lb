import type { Route } from '@playwright/test';
import type { AuthSession } from '../../src/lib/api';

const AUTHENTICATED_SESSION = {
  authority: 'https://access.example.com',
  subject: 'alice',
  kind: 'human',
  provider_id: 'cloudflare',
  email: 'alice@example.com',
  display_name: 'Alice',
  expires_at_unix_secs: 2_000_000_000,
  auth_mode: 'external',
} satisfies AuthSession;

export async function fulfillAuthenticatedSession(
  route: Route,
): Promise<boolean> {
  if (new URL(route.request().url()).pathname !== '/admin/v1/auth/session') {
    return false;
  }

  await route.fulfill({
    status: 200,
    contentType: 'application/json',
    body: JSON.stringify(AUTHENTICATED_SESSION),
  });
  return true;
}
