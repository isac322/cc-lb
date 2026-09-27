import '@fontsource-variable/hanken-grotesk';
import '@fontsource-variable/geist-mono';
import './index.css';

import { QueryClientProvider } from '@tanstack/react-query';
import { createRouter, RouterProvider } from '@tanstack/react-router';
import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { Toaster } from 'sonner';

import { initLocale, initTimezone } from './lib/locale';
import { queryClient } from './lib/queryClient';
import { initTheme, useTheme } from './lib/theme';
import { routeTree } from './routeTree.gen';

initTheme();
initLocale();
initTimezone();

const router = createRouter({
  routeTree,
  context: { queryClient },
  defaultPreload: 'intent',
  defaultPreloadStaleTime: 0,
  scrollRestoration: true,
});

declare module '@tanstack/react-router' {
  interface Register {
    router: typeof router;
  }
}

function App() {
  const { effective } = useTheme();
  return (
    <QueryClientProvider client={queryClient}>
      <RouterProvider router={router} />
      <Toaster
        theme={effective}
        position="bottom-right"
        closeButton
        richColors={false}
        duration={4000}
        // Clear the phone/tablet tab bar (`--shell-bottom` is 0 from `lg`).
        offset={{ bottom: 'calc(var(--shell-bottom) + 24px)', right: 24 }}
        mobileOffset={{ bottom: 'calc(var(--shell-bottom) + 16px)' }}
      />
    </QueryClientProvider>
  );
}

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
