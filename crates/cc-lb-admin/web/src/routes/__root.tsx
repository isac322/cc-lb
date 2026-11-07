import type { QueryClient } from '@tanstack/react-query';
import {
  createRootRouteWithContext,
  Outlet,
  retainSearchParams,
} from '@tanstack/react-router';
import { useState } from 'react';
import { AuthRequiredGate } from '../components/AuthRequiredGate';
import { CommandPalette } from '../components/CommandPalette';
import { AppShell } from '../components/layout/AppShell';
import {
  PreviewControls,
  PreviewProvider,
  type PreviewSearch,
  validatePreviewSearch,
} from '../lib/previewControls';

export const Route = createRootRouteWithContext<{ queryClient: QueryClient }>()(
  {
    validateSearch: validatePreviewSearch,
    search: {
      middlewares: [
        ({ search, next }) =>
          retainSearchParams<PreviewSearch>([
            'previewProviders',
            'provider',
            'range',
          ])({ search, next }),
      ],
    },
    component: RootLayout,
  },
);

function RootLayout() {
  const [paletteOpen, setPaletteOpen] = useState(false);
  return (
    <AuthRequiredGate>
      <PreviewProvider>
        <AppShell onCommandPalette={() => setPaletteOpen(true)}>
          <Outlet />
        </AppShell>
        <CommandPalette open={paletteOpen} onOpenChange={setPaletteOpen} />
        <PreviewControls />
      </PreviewProvider>
    </AuthRequiredGate>
  );
}
