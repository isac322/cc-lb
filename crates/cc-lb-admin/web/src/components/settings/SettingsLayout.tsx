import type { ReactNode } from 'react';

interface SettingsLayoutProps {
  nav: ReactNode;
  main: ReactNode;
  sidebar?: ReactNode;
  footer?: ReactNode;
}

export function SettingsLayout({
  nav,
  main,
  sidebar,
  footer,
}: SettingsLayoutProps) {
  return (
    <div className="flex h-[calc(100vh-4rem)] overflow-hidden">
      <div className="w-64 flex-shrink-0 border-r border-graphite-800 overflow-y-auto p-4">
        {nav}
      </div>
      <div className="flex-1 flex flex-col overflow-hidden relative">
        <div className="flex-1 overflow-y-auto p-6">
          <div className="max-w-4xl mx-auto pb-32">{main}</div>
        </div>
        {footer && <div className="shrink-0">{footer}</div>}
      </div>
      {sidebar && (
        <div className="w-80 flex-shrink-0 border-l border-graphite-800 overflow-y-auto bg-graphite-900">
          {sidebar}
        </div>
      )}
    </div>
  );
}
