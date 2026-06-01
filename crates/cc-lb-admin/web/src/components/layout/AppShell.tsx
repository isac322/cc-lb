import * as Dialog from '@radix-ui/react-dialog';
import { Link } from '@tanstack/react-router';
import {
  Activity,
  Blocks,
  Command,
  FileClock,
  KeyRound,
  LayoutDashboard,
  Menu,
  ScrollText,
  Server,
  Settings as SettingsIcon,
  Users,
  X,
} from 'lucide-react';
import { type ReactNode, useEffect, useState } from 'react';
import { useHealth } from '../../lib/queries';
import { ThemeToggle } from '../ThemeToggle';
import { cx } from '../ui/primitives';

const NAV = [
  { path: '/', label: 'Overview', Icon: LayoutDashboard },
  { path: '/upstreams', label: 'Upstreams', Icon: Server },
  { path: '/principals', label: 'Principals', Icon: Users },
  { path: '/plugins', label: 'Plugins', Icon: Blocks },
  { path: '/logs', label: 'Logs', Icon: ScrollText },
  { path: '/audit', label: 'Audit', Icon: FileClock },
  { path: '/credentials', label: 'Credentials', Icon: KeyRound },
  { path: '/status', label: 'Status', Icon: Activity },
  { path: '/settings', label: 'Settings', Icon: SettingsIcon },
] as const;

const SIDEBAR_KEY = 'cclb.sidebar.collapsed';

export interface AppShellProps {
  children: ReactNode;
  onCommandPalette: () => void;
}

export function AppShell({ children, onCommandPalette }: AppShellProps) {
  const [collapsed, setCollapsed] = useState<boolean>(() => {
    if (typeof window === 'undefined') return false;
    return localStorage.getItem(SIDEBAR_KEY) === '1';
  });
  const [mobileOpen, setMobileOpen] = useState(false);
  const health = useHealth();

  useEffect(() => {
    localStorage.setItem(SIDEBAR_KEY, collapsed ? '1' : '0');
  }, [collapsed]);

  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'b') {
        e.preventDefault();
        setCollapsed((c) => !c);
      }
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, []);

  const connection = health.isLoading
    ? 'connecting'
    : health.isError
      ? 'down'
      : 'live';
  const version = health.data?.version ?? null;

  return (
    <div className="min-h-screen flex bg-bg text-text">
      {/* Desktop sidebar — sticky to viewport so body can scroll */}
      <aside
        className={cx(
          'hidden md:flex flex-col bg-bg-sub border-r border-subtle shrink-0 transition-[width] duration-150',
          'sticky top-0 self-start h-screen z-20',
          collapsed ? 'w-14' : 'w-56',
        )}
      >
        <SidebarBrand collapsed={collapsed} />
        <SidebarNav collapsed={collapsed} onNavigate={() => {}} />
        <SidebarFooter collapsed={collapsed} />
      </aside>

      {/* Mobile drawer — Radix Dialog with left-side slide-in */}
      <Dialog.Root open={mobileOpen} onOpenChange={setMobileOpen}>
        <Dialog.Portal>
          <Dialog.Overlay className="fixed inset-0 z-40 bg-modal-backdrop backdrop-blur-sm md:hidden" />
          <Dialog.Content
            aria-describedby={undefined}
            style={{ height: '100vh' }}
            className="fixed left-0 top-0 z-50 w-64 bg-bg-sub border-r border-subtle grid grid-rows-[3rem_1fr_auto] outline-none md:hidden"
          >
            <Dialog.Title className="sr-only">Navigation</Dialog.Title>
            <div className="flex items-center justify-between px-4 border-b border-subtle">
              <SidebarBrand collapsed={false} />
              <Dialog.Close asChild>
                <button
                  type="button"
                  aria-label="Close menu"
                  className="text-text-muted hover:text-text"
                >
                  <X className="w-4 h-4" />
                </button>
              </Dialog.Close>
            </div>
            <div className="overflow-y-auto min-h-0">
              <SidebarNav
                collapsed={false}
                onNavigate={() => setMobileOpen(false)}
              />
            </div>
            <SidebarFooter collapsed={false} />
          </Dialog.Content>
        </Dialog.Portal>
      </Dialog.Root>

      <main className="flex-1 flex flex-col min-w-0 bg-bg">
        <Topbar
          onMobileMenu={() => setMobileOpen(true)}
          onToggleSidebar={() => setCollapsed((c) => !c)}
          onCommandPalette={onCommandPalette}
          connection={connection}
          version={version}
        />
        {/* Content renders into normal flow — body scrolls naturally on overflow */}
        <div>{children}</div>
      </main>
    </div>
  );
}

function SidebarBrand({ collapsed }: { collapsed: boolean }) {
  return (
    <div
      className={cx(
        'flex items-center gap-2.5 px-3 h-12 border-b border-subtle shrink-0',
        collapsed ? 'justify-center' : '',
      )}
    >
      <div className="w-6 h-6 bg-accent text-[color:var(--color-bg)] flex items-center justify-center font-bold text-[11px] rounded-sm">
        CC
      </div>
      {!collapsed ? (
        <span className="font-medium text-sm tracking-wide">cc-lb</span>
      ) : null}
    </div>
  );
}

function SidebarNav({
  collapsed,
  onNavigate,
}: {
  collapsed: boolean;
  onNavigate: () => void;
}) {
  return (
    <nav className="flex-1 py-3 px-2 space-y-0.5 overflow-y-auto">
      {NAV.map(({ path, label, Icon }) => (
        <Link
          key={path}
          to={path}
          onClick={onNavigate}
          activeOptions={{ exact: path === '/' }}
          className={cx(
            'flex items-center gap-2.5 px-2.5 h-9 rounded-sm text-sm transition-colors hover:bg-overlay-5 text-text-muted hover:text-text',
            collapsed ? 'justify-center' : '',
          )}
          activeProps={{ className: 'bg-overlay-6 text-text' }}
        >
          <Icon className="w-4 h-4 shrink-0" aria-hidden="true" />
          {!collapsed ? (
            <span className="truncate">{label}</span>
          ) : (
            <span className="sr-only">{label}</span>
          )}
        </Link>
      ))}
    </nav>
  );
}

function SidebarFooter({ collapsed }: { collapsed: boolean }) {
  return (
    <div
      className={cx(
        'border-t border-subtle px-3 py-3',
        collapsed ? 'text-center' : '',
      )}
    >
      <div className="text-[10px] uppercase tracking-wider text-text-faint">
        {collapsed ? 'v' : 'cc-lb admin · v1.4.0'}
      </div>
    </div>
  );
}

function Topbar({
  onMobileMenu,
  onToggleSidebar,
  onCommandPalette,
  connection,
  version,
}: {
  onMobileMenu: () => void;
  onToggleSidebar: () => void;
  onCommandPalette: () => void;
  connection: 'live' | 'connecting' | 'down';
  version: string | null;
}) {
  const liveLabel = version ? `cc-lb · v${version}` : 'cc-lb · live';
  return (
    <header className="h-12 shrink-0 flex items-center justify-between px-3 md:px-5 border-b border-subtle bg-bg sticky top-0 z-30">
      <div className="flex items-center gap-2 min-w-0">
        <button
          type="button"
          aria-label="Open menu"
          className="md:hidden h-9 w-9 inline-flex items-center justify-center text-text-muted hover:text-text"
          onClick={onMobileMenu}
        >
          <Menu className="w-5 h-5" />
        </button>
        <button
          type="button"
          aria-label="Toggle sidebar"
          className="hidden md:inline-flex h-8 w-8 items-center justify-center text-text-muted hover:text-text rounded-sm hover:bg-overlay-5"
          onClick={onToggleSidebar}
        >
          <Menu className="w-4 h-4" />
        </button>
        <div className="hidden sm:flex items-center gap-2 text-xs text-text-faint pl-1">
          <span>cc-lb</span>
          <span className="text-text-faint/60">/</span>
          <span className="text-text">admin</span>
        </div>
      </div>
      <div className="flex items-center gap-2 md:gap-3">
        <button
          type="button"
          onClick={onCommandPalette}
          className="hidden sm:flex items-center gap-2 h-8 px-2.5 text-xs text-text-muted bg-overlay-2 border border-subtle rounded-sm hover:text-text hover:border-[color:var(--color-border-strong)]"
        >
          <span>Search...</span>
          <kbd className="font-mono text-[10px] px-1 bg-overlay-3 border border-subtle rounded-sm">
            ⌘K
          </kbd>
        </button>
        <button
          type="button"
          onClick={onCommandPalette}
          aria-label="Open command palette"
          className="sm:hidden h-9 w-9 inline-flex items-center justify-center text-text-muted hover:text-text"
        >
          <Command className="w-4 h-4" />
        </button>
        <ThemeToggle />
        <div className="flex items-center gap-2 text-[11px] text-text-faint">
          <span
            className={cx(
              'status-dot',
              connection === 'live'
                ? 'live'
                : connection === 'down'
                  ? 'danger'
                  : 'neutral',
            )}
          />
          <span className="hidden sm:inline">
            {connection === 'live'
              ? liveLabel
              : connection === 'down'
                ? 'disconnected'
                : 'connecting…'}
          </span>
        </div>
      </div>
    </header>
  );
}
