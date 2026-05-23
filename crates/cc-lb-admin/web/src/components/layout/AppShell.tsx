import { Sidebar } from './Sidebar';
import { Topbar } from './Topbar';
import { useLocation } from 'react-router';

const ROUTE_TITLES: Record<string, string> = {
  '/': 'Overview',
  '/principals': 'Principal Limits',
  '/log': 'Realtime Log',
  '/management': 'Principal Management',
  '/settings': 'Settings',
  '/upstreams': 'Upstreams',
  '/activity': 'Admin Activity',
  '/credentials': 'Credential Status',
  '/plugins': 'Plugin Status',
};

export function AppShell({ children }: { children: React.ReactNode }) {
  const location = useLocation();
  const title = ROUTE_TITLES[location.pathname] || 'Not Found';

  return (
    <div className="flex min-h-screen bg-graphite-900 text-graphite-100 font-sans">
      <Sidebar />
      <div className="flex-1 flex flex-col min-w-0">
        <Topbar title={title} />
        <main className="flex-1 p-6 overflow-x-hidden">
          {children}
        </main>
      </div>
    </div>
  );
}
