import { Dialog as BaseDialog } from '@base-ui/react/dialog';
import { Popover as BasePopover } from '@base-ui/react/popover';
import { useRouterState } from '@tanstack/react-router';
import { HelpCircle, Search, UserRound, X } from 'lucide-react';
import { type ReactNode, useEffect, useState } from 'react';
import { useAuthSessionContext } from '../../lib/authSession';
import { useHealth } from '../../lib/queries';
import { ThemeToggle } from '../ThemeToggle';
import { cx, Hint, ShellPageTitleProvider } from '../ui/primitives';
import { navItemForPath } from './navItems';
import {
  BrandMark,
  SidebarBrand,
  SidebarFooter,
  SidebarNav,
  SidebarTabBar,
  useUpstreamOAuthAttention,
} from './Sidebar';

const SIDEBAR_KEY = 'cclb.sidebar.collapsed';
const MAIN_ID = 'main-content';

type Connection = 'live' | 'connecting' | 'down';

const BREAK_GLASS_HELP =
  'Signed in with the shared admin token, not a personal identity-provider login. Audit entries record the token, not a person.';

/**
 * `Page · cc-lb`, prefixed with `(n)` while n upstreams need reconnecting so
 * a background tab still signals the incident.
 */
function useDocumentTitle() {
  const pathname = useRouterState({ select: (s) => s.location.pathname });
  const pageLabel = navItemForPath(pathname)?.label ?? null;
  const attention = useUpstreamOAuthAttention().nudges.size;

  useEffect(() => {
    const base = pageLabel ? `${pageLabel} · cc-lb` : 'cc-lb';
    document.title = attention > 0 ? `(${attention}) ${base}` : base;
  }, [pageLabel, attention]);
}

/** Display and size are set per button so responsive variants never collide. */
const TOPBAR_ICON_BUTTON =
  'shrink-0 items-center justify-center rounded-sm text-text-muted transition-colors hover:bg-overlay-5 hover:text-text focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-1';

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
  const pathname = useRouterState({ select: (s) => s.location.pathname });
  const pageLabel = navItemForPath(pathname)?.label ?? null;
  const health = useHealth();
  useDocumentTitle();

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

  const connection: Connection = health.isLoading
    ? 'connecting'
    : health.isError
      ? 'down'
      : 'live';
  const version =
    import.meta.env.VITE_CC_LB_VERSION || health.data?.version || null;

  return (
    <div className="min-h-screen flex bg-bg text-text">
      <a
        href={`#${MAIN_ID}`}
        onClick={(e) => {
          // Move focus without touching the router-owned URL.
          e.preventDefault();
          document.getElementById(MAIN_ID)?.focus();
        }}
        className="fixed left-3 top-2 z-[60] inline-flex items-center h-9 px-3 rounded-sm bg-bg-sub border border-subtle-strong text-body text-text shadow-overlay -translate-y-16 focus:translate-y-0 focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-1"
      >
        Skip to content
      </a>
      {/* Desktop rail — on the ground, sticky to the viewport so the body scrolls */}
      <aside
        className={cx(
          'hidden lg:flex flex-col bg-bg border-r border-subtle shrink-0',
          'sticky top-0 self-start h-screen z-20',
          collapsed ? 'w-14' : 'w-52',
        )}
      >
        <SidebarBrand
          collapsed={collapsed}
          onToggleCollapsed={() => setCollapsed((c) => !c)}
        />
        <SidebarNav collapsed={collapsed} onNavigate={() => {}} />
        <SidebarFooter collapsed={collapsed} version={version} />
      </aside>

      {/* "More" sheet below `lg`: the full navigation, opened from the tab bar. */}
      <BaseDialog.Root onOpenChange={setMobileOpen} open={mobileOpen}>
        <BaseDialog.Portal>
          <BaseDialog.Backdrop className="fixed inset-0 z-40 bg-modal-backdrop transition-opacity duration-150 ease-out data-[ending-style]:opacity-0 data-[starting-style]:opacity-0 lg:hidden" />
          <BaseDialog.Popup
            className="fixed left-0 top-0 z-50 w-72 max-w-[85vw] bg-bg border-r border-subtle-strong shadow-overlay grid grid-rows-[3rem_1fr_auto] outline-none transition-transform duration-150 ease-out data-[ending-style]:-translate-x-full data-[starting-style]:-translate-x-full motion-reduce:transition-none lg:hidden"
            style={{ height: '100dvh' }}
          >
            <BaseDialog.Title className="sr-only">Navigation</BaseDialog.Title>
            <div className="flex items-center justify-between pr-1">
              <SidebarBrand collapsed={false} />
              <BaseDialog.Close
                aria-label="Close menu"
                className={cx(TOPBAR_ICON_BUTTON, 'inline-flex h-11 w-11')}
              >
                <X className="w-4 h-4" aria-hidden="true" />
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

      <div className="flex-1 flex flex-col min-w-0 bg-bg min-h-screen">
        <Topbar
          pageLabel={pageLabel}
          onCommandPalette={onCommandPalette}
          connection={connection}
        />
        {/* Content renders into normal flow — body scrolls naturally on overflow.
            Below `lg` it clears the fixed tab bar (`--shell-bottom`). */}
        <main
          id={MAIN_ID}
          tabIndex={-1}
          className="flex-1 flex flex-col outline-none pb-[var(--shell-bottom)]"
        >
          <ShellPageTitleProvider value={pageLabel}>
            {children}
          </ShellPageTitleProvider>
        </main>
      </div>
      <SidebarTabBar
        pathname={pathname}
        moreOpen={mobileOpen}
        onMore={() => setMobileOpen(true)}
      />
    </div>
  );
}

/**
 * Page name on the left (the brand mark joins it below `lg`); search,
 * identity, API status and the Night/Day pill on the right.
 */
function Topbar({
  pageLabel,
  onCommandPalette,
  connection,
}: {
  pageLabel: string | null;
  onCommandPalette: () => void;
  connection: Connection;
}) {
  const authSession = useAuthSessionContext();
  const isBreakGlass = authSession?.kind === 'break_glass';
  const identityLabel =
    authSession?.email ??
    authSession?.display_name ??
    authSession?.subject ??
    'Unknown administrator';
  const rawKind = authSession?.kind.replaceAll('_', ' ') ?? 'unknown';
  // Sentence case: "Shared admin token", "Break glass".
  const identityKind = rawKind.charAt(0).toUpperCase() + rawKind.slice(1);

  return (
    <header className="h-12 shrink-0 flex items-center justify-between gap-2 pl-4 pr-2 md:pl-6 md:pr-5 lg:pr-6 border-b border-subtle bg-bg sticky top-0 z-30">
      <div className="flex items-center gap-2.5 min-w-0">
        <span className="lg:hidden">
          <BrandMark />
        </span>
        <span className="truncate text-title-section text-text">
          {pageLabel ?? 'cc-lb'}
        </span>
      </div>
      <div className="flex items-center gap-1 md:gap-3 min-w-0">
        <button
          type="button"
          onClick={onCommandPalette}
          className="hidden md:flex items-center gap-2 h-8 pl-2.5 pr-1.5 text-body-sm text-text-muted border border-subtle rounded-sm transition-colors hover:text-text hover:border-subtle-strong focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-1"
        >
          <span>Search…</span>
          <kbd className="font-sans text-caption text-text-faint px-1 border border-subtle rounded-sm">
            ⌘K
          </kbd>
        </button>
        <button
          type="button"
          onClick={onCommandPalette}
          aria-label="Open command palette"
          className={cx(TOPBAR_ICON_BUTTON, 'inline-flex md:hidden h-11 w-11')}
        >
          <Search className="w-4 h-4" strokeWidth={1.75} aria-hidden="true" />
        </button>
        <div
          className="hidden min-w-0 max-w-56 flex-col items-end xl:flex"
          data-testid="admin-identity"
          title={`${identityLabel} (${identityKind})`}
        >
          <span className="max-w-full truncate text-label text-text">
            {identityLabel}
          </span>
          <span className="flex max-w-full min-w-0 items-center gap-1">
            <span className="truncate text-caption text-text-faint">
              {identityKind}
            </span>
            {isBreakGlass ? (
              <Hint
                label={
                  <span className="block max-w-72 whitespace-normal leading-5">
                    {BREAK_GLASS_HELP}
                  </span>
                }
                side="bottom"
              >
                <button
                  type="button"
                  aria-label="What is break glass?"
                  className="inline-flex h-4 w-4 shrink-0 cursor-help items-center justify-center rounded-sm text-text-faint transition-colors hover:bg-overlay-5 hover:text-text focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-1"
                >
                  <HelpCircle size={12} strokeWidth={1.75} aria-hidden="true" />
                </button>
              </Hint>
            ) : null}
          </span>
        </div>
        <BasePopover.Root>
          <BasePopover.Trigger
            aria-label={`Signed in as ${identityLabel}`}
            className={cx(
              TOPBAR_ICON_BUTTON,
              'inline-flex xl:hidden h-11 w-11 md:h-8 md:w-8',
            )}
          >
            <UserRound
              className="w-4 h-4"
              strokeWidth={1.75}
              aria-hidden="true"
            />
          </BasePopover.Trigger>
          <BasePopover.Portal>
            <BasePopover.Positioner
              className="z-50"
              side="bottom"
              align="end"
              sideOffset={4}
            >
              <BasePopover.Popup className="glass-strong max-w-[calc(100vw-2rem)] rounded-md px-3 py-2 outline-none">
                <div className="truncate text-body text-text">
                  {identityLabel}
                </div>
                <div className="text-caption text-text-faint">
                  {identityKind}
                </div>
                {isBreakGlass ? (
                  <p className="mt-1 max-w-64 text-caption text-text-muted">
                    {BREAK_GLASS_HELP}
                  </p>
                ) : null}
              </BasePopover.Popup>
            </BasePopover.Positioner>
          </BasePopover.Portal>
        </BasePopover.Root>
        <ConnectionStatus connection={connection} />
        <ThemeToggle />
      </div>
    </header>
  );
}

const CONNECTION_COPY: Record<Connection, { label: string; a11y: string }> = {
  live: { label: 'Live', a11y: 'Admin API connected' },
  connecting: { label: 'Connecting', a11y: 'Connecting to the admin API' },
  down: { label: 'Offline', a11y: 'Admin API unreachable' },
};

/**
 * Health of the admin API: dot + word. On phones a healthy connection shows
 * the dot alone; any other state always shows its word, so it never relies
 * on color.
 */
function ConnectionStatus({ connection }: { connection: Connection }) {
  const copy = CONNECTION_COPY[connection];
  return (
    <div
      role="status"
      className="flex shrink-0 items-center gap-1.5 pl-1 text-caption"
      title={copy.a11y}
    >
      <span
        aria-hidden="true"
        className={cx(
          'status-dot',
          connection === 'live'
            ? 'ok'
            : connection === 'down'
              ? 'danger'
              : 'neutral',
        )}
      />
      {connection === 'live' ? (
        <>
          <span
            aria-hidden="true"
            className="hidden md:inline text-traffic-success-text"
          >
            {copy.label}
          </span>
          <span className="sr-only">{copy.a11y}</span>
        </>
      ) : (
        <>
          <span
            aria-hidden="true"
            className={
              connection === 'down' ? 'text-danger-text' : 'text-text-muted'
            }
          >
            {copy.label}
          </span>
          <span className="sr-only">{copy.a11y}</span>
        </>
      )}
    </div>
  );
}
