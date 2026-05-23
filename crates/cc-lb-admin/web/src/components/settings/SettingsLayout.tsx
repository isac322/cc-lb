import type { ReactNode } from 'react';

interface SettingsLayoutProps {
  nav: ReactNode;
  main: ReactNode;
  sidebar?: ReactNode;
}

export function SettingsLayout({ nav, main, sidebar }: SettingsLayoutProps) {
  return (
    <div className="flex h-[calc(100vh-4rem)] overflow-hidden">
      <div className="w-64 flex-shrink-0 border-r border-graphite-800 overflow-y-auto p-4">
        {nav}
      </div>
      <div className="flex-1 overflow-y-auto p-6 relative">
        <div className="max-w-4xl mx-auto pb-32">{main}</div>
      </div>
      {sidebar && (
        <div className="w-80 flex-shrink-0 border-l border-graphite-800 overflow-y-auto bg-graphite-900">
          {sidebar}
        </div>
      )}
    </div>
  );
}
