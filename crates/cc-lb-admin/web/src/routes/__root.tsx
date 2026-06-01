import { Outlet, createRootRouteWithContext } from '@tanstack/react-router';
import { useState } from 'react';
import type { QueryClient } from '@tanstack/react-query';
import { AppShell } from '../components/layout/AppShell';
import { CommandPalette } from '../components/CommandPalette';
import { AuthRequiredGate } from '../components/AuthRequiredGate';

export const Route = createRootRouteWithContext<{ queryClient: QueryClient }>()({
  component: RootLayout,
});

function RootLayout() {
  const [paletteOpen, setPaletteOpen] = useState(false);
  return (
    <AuthRequiredGate>
      <AppShell onCommandPalette={() => setPaletteOpen(true)}>
        <Outlet />
      </AppShell>
      <CommandPalette open={paletteOpen} onOpenChange={setPaletteOpen} />
    </AuthRequiredGate>
  );
}
