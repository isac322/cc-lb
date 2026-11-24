import { createContext, type ReactNode, useContext } from 'react';
import type { AuthSession } from './api';

export const AuthSessionContext = createContext<AuthSession | null>(null);

export function AuthSessionProvider({
  children,
  session,
}: {
  children: ReactNode;
  session: AuthSession;
}) {
  return (
    <AuthSessionContext.Provider value={session}>
      {children}
    </AuthSessionContext.Provider>
  );
}

export function useAuthSessionContext(): AuthSession | null {
  return useContext(AuthSessionContext);
}
