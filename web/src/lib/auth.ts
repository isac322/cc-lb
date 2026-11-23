export const AUTH_TOKEN_KEY = 'cc-lb-admin-token';

export function getAdminToken(): string | null {
  return localStorage.getItem(AUTH_TOKEN_KEY);
}

export function setAdminToken(token: string): void {
  localStorage.setItem(AUTH_TOKEN_KEY, token);
}

export function clearAdminToken(): void {
  localStorage.removeItem(AUTH_TOKEN_KEY);
}
