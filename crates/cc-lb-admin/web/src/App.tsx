import { Suspense } from 'react';
import { AppShell } from './components/layout/AppShell';
import { AuthRequiredGate } from './components/feedback/AuthRequiredGate';
import { ErrorBoundary } from './components/feedback/ErrorBoundary';
import { LoadingState } from './components/primitives/LoadingState';
import { AppRoutes } from './routes';

export default function App() {
  return (
    <ErrorBoundary>
      <AuthRequiredGate>
        <AppShell>
          <Suspense fallback={<div className="flex items-center justify-center h-full"><LoadingState /></div>}>
            <AppRoutes />
          </Suspense>
        </AppShell>
      </AuthRequiredGate>
    </ErrorBoundary>
  );
}
