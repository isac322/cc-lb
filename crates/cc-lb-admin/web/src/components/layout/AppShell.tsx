import { Dialog as BaseDialog } from '@base-ui/react/dialog';
import { Command, Menu, X } from 'lucide-react';
import { type ReactNode, useEffect, useState } from 'react';
import { useAuthSessionContext } from '../../lib/authSession';
import { useHealth } from '../../lib/queries';
import { ThemeToggle } from '../ThemeToggle';
import { cx } from '../ui/primitives';
import { SidebarBrand, SidebarFooter, SidebarNav } from './Sidebar';

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
  const version =
    import.meta.env.VITE_CC_LB_VERSION || health.data?.version || null;

  return (
    <div className="min-h-screen flex bg-bg text-text">
      {/* Desktop sidebar — sticky to viewport so body can scroll */}
      <aside
        className={cx(
          'hidden lg:flex flex-col bg-bg-sub border-r border-subtle shrink-0 transition-[width] duration-150',
          'sticky top-0 self-start h-screen z-20',
          collapsed ? 'w-14' : 'w-56',
        )}
      >
        <SidebarBrand collapsed={collapsed} />
        <SidebarNav collapsed={collapsed} onNavigate={() => {}} />
        <SidebarFooter collapsed={collapsed} version={version} />
      </aside>

      <BaseDialog.Root onOpenChange={setMobileOpen} open={mobileOpen}>
        <BaseDialog.Portal>
          <BaseDialog.Backdrop className="fixed inset-0 z-40 bg-modal-backdrop backdrop-blur-sm transition-opacity duration-150 ease-out data-[ending-style]:opacity-0 data-[starting-style]:opacity-0 lg:hidden" />
          <BaseDialog.Popup
            className="fixed left-0 top-0 z-50 w-64 bg-bg-sub border-r border-subtle grid grid-rows-[3rem_1fr_auto] outline-none transition-transform duration-150 ease-out data-[ending-style]:-translate-x-full data-[starting-style]:-translate-x-full lg:hidden"
            style={{ height: '100vh' }}
          >
            <BaseDialog.Title className="sr-only">Navigation</BaseDialog.Title>
            <div className="flex items-center justify-between px-4 border-b border-subtle">
              <SidebarBrand collapsed={false} />
              <BaseDialog.Close
                aria-label="Close menu"
                className="text-text-muted hover:text-text"
              >
                <X className="w-4 h-4" />
              </BaseDialog.Close>
            </div>
            <div className="overflow-y-auto min-h-0 pb-8">
              <SidebarNav
                collapsed={false}
                onNavigate={() => setMobileOpen(false)}
              />
            </div>
            <SidebarFooter collapsed={false} version={version} />
          </BaseDialog.Popup>
        </BaseDialog.Portal>
      </BaseDialog.Root>

      <main className="flex-1 flex flex-col min-w-0 bg-bg min-h-screen">
        <Topbar
          onMobileMenu={() => setMobileOpen(true)}
          onToggleSidebar={() => setCollapsed((c) => !c)}
          onCommandPalette={onCommandPalette}
          connection={connection}
        />
        {/* Content renders into normal flow — body scrolls naturally on overflow */}
        <div className="flex-1 flex flex-col">{children}</div>
      </main>
    </div>
  );
}

function Topbar({
  onMobileMenu,
  onToggleSidebar,
  onCommandPalette,
  connection,
}: {
  onMobileMenu: () => void;
  onToggleSidebar: () => void;
  onCommandPalette: () => void;
  connection: 'live' | 'connecting' | 'down';
}) {
  const authSession = useAuthSessionContext();
  const identityLabel =
    authSession?.email ??
    authSession?.display_name ??
    authSession?.subject ??
    'Unknown administrator';
  const identityKind = authSession?.kind.replaceAll('_', ' ') ?? 'unknown';

  return (
    <header className="h-12 shrink-0 flex items-center justify-between px-3 md:px-5 border-b border-subtle bg-bg sticky top-0 z-30">
      <div className="flex items-center gap-2 min-w-0">
        <button
          type="button"
          aria-label="Open menu"
          className="lg:hidden h-9 w-9 inline-flex items-center justify-center text-text-muted hover:text-text"
          onClick={onMobileMenu}
        >
          <Menu className="w-5 h-5" />
        </button>
        <button
          type="button"
          aria-label="Toggle sidebar"
          className="hidden lg:inline-flex h-8 w-8 items-center justify-center text-text-muted hover:text-text rounded-sm hover:bg-overlay-5"
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
        <div
          className="flex max-w-24 sm:max-w-48 flex-col items-end leading-tight"
          data-testid="admin-identity"
          title={`${identityLabel} (${identityKind})`}
        >
          <span className="max-w-full truncate text-xs text-text">
            {identityLabel}
          </span>
          <span className="text-[10px] uppercase tracking-wider text-text-faint">
            {identityKind}
          </span>
        </div>
        <ThemeToggle />
        <div
          aria-label={`Connection: ${connection}`}
          className="flex items-center gap-2 text-[11px] text-text-faint"
          title={connection}
        >
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
        </div>
      </div>
    </header>
  );
}
