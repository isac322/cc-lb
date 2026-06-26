import type { QueryClient } from '@tanstack/react-query';
import { createRootRouteWithContext, Outlet } from '@tanstack/react-router';
import { useState } from 'react';
import { AuthRequiredGate } from '../components/AuthRequiredGate';
import { CommandPalette } from '../components/CommandPalette';
import { AppShell } from '../components/layout/AppShell';

export const Route = createRootRouteWithContext<{ queryClient: QueryClient }>()(
  {
    component: RootLayout,
  },
);

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
