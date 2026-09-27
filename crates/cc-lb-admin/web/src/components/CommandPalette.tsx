import { Dialog as BaseDialog } from '@base-ui/react/dialog';
import { useNavigate } from '@tanstack/react-router';
import { Command } from 'cmdk';
import {
  ArrowUpDown,
  CornerDownLeft,
  Plus,
  Server,
  Users,
  X,
} from 'lucide-react';
import { useEffect } from 'react';
import { usePrincipals, useUpstreams } from '../lib/queries';
import { NAV_ITEMS } from './layout/navItems';

export interface CommandPaletteProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export function CommandPalette({ open, onOpenChange }: CommandPaletteProps) {
  const navigate = useNavigate();
  const upstreams = useUpstreams();
  const principals = usePrincipals();

  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'k') {
        e.preventDefault();
        onOpenChange(!open);
      }
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [open, onOpenChange]);

  const go = (path: string) => {
    onOpenChange(false);
    navigate({ to: path });
  };

  return (
    <BaseDialog.Root onOpenChange={onOpenChange} open={open}>
      <BaseDialog.Portal>
        <BaseDialog.Backdrop className="fixed inset-0 z-50 bg-modal-backdrop" />
        <BaseDialog.Popup
          className="fixed top-[5vh] left-1/2 -translate-x-1/2 z-50 w-[calc(100%-2rem)] max-w-xl outline-none"
          style={{ maxHeight: 'min(700px, 80vh)' }}
        >
          <BaseDialog.Title className="sr-only">
            Command palette
          </BaseDialog.Title>
          <BaseDialog.Description className="sr-only">
            Search resources or run commands
          </BaseDialog.Description>
          <Command
            label="Command palette"
            className="glass-strong rounded-md grid grid-rows-[auto_minmax(0,1fr)_auto] max-h-full overflow-hidden"
          >
            <div className="flex items-center gap-2 px-3 h-11 border-b border-subtle">
              <Command.Input
                placeholder="Search resources or run commands…"
                autoFocus
                className="flex-1 bg-transparent text-body text-text placeholder:text-text-faint outline-none"
              />
              <kbd className="hidden md:inline font-sans text-caption text-text-faint bg-overlay-3 px-1.5 rounded-sm">
                Esc
              </kbd>
              <BaseDialog.Close
                aria-label="Close"
                className="-mr-2 md:mr-0 inline-flex shrink-0 items-center justify-center rounded-sm text-text-muted transition-colors h-11 w-11 md:h-8 md:w-8 hover:bg-overlay-5 hover:text-text focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-1"
              >
                <X className="w-4 h-4" aria-hidden="true" />
              </BaseDialog.Close>
            </div>
            <Command.List
              className="overflow-y-auto"
              style={{ paddingBottom: '0.5rem' }}
            >
              <Command.Empty>No matches found.</Command.Empty>
              <Command.Group heading="Pages">
                {NAV_ITEMS.map(({ path, label, Icon }) => (
                  <Command.Item key={path} onSelect={() => go(path)}>
                    <Icon
                      className="w-4 h-4 text-text-faint"
                      aria-hidden="true"
                    />
                    Go to {label}
                  </Command.Item>
                ))}
              </Command.Group>
              <Command.Group heading="Actions">
                <Command.Item onSelect={() => go('/upstreams?action=new')}>
                  <Plus className="w-4 h-4 text-text-faint" />
                  Create upstream
                </Command.Item>
                <Command.Item onSelect={() => go('/principals?action=new')}>
                  <Plus className="w-4 h-4 text-text-faint" />
                  Create principal
                </Command.Item>
                <Command.Item onSelect={() => go('/plugins?action=upload')}>
                  <Plus className="w-4 h-4 text-text-faint" />
                  Open plugin upload
                </Command.Item>
              </Command.Group>
              <Command.Group heading="Upstreams">
                {upstreams.isLoading ? (
                  <Command.Item disabled>Loading upstreams…</Command.Item>
                ) : (
                  (upstreams.data?.upstreams ?? []).map((u) => (
                    <Command.Item
                      key={u.id}
                      value={`upstream-${u.name}`}
                      onSelect={() => go(`/upstreams?selectedId=${u.id}`)}
                    >
                      <Server className="w-4 h-4 text-text-faint" />
                      <span>{u.name}</span>
                      <span className="ml-auto font-mono text-data text-text-faint">
                        {u.kind}
                      </span>
                    </Command.Item>
                  ))
                )}
              </Command.Group>
              <Command.Group heading="Principals">
                {principals.isLoading ? (
                  <Command.Item disabled>Loading principals…</Command.Item>
                ) : (
                  (principals.data?.principals ?? []).map((p) => (
                    <Command.Item
                      key={p.id}
                      value={`principal-${p.name}`}
                      onSelect={() => go(`/principals?selectedId=${p.id}`)}
                    >
                      <Users className="w-4 h-4 text-text-faint" />
                      <span>{p.name}</span>
                      <span className="ml-auto font-mono text-data text-text-faint">
                        {p.kind}
                      </span>
                    </Command.Item>
                  ))
                )}
              </Command.Group>
            </Command.List>
            <div className="hidden md:flex items-center justify-between px-3 h-9 border-t border-subtle text-caption text-text-faint">
              <div className="flex items-center gap-3">
                <span className="inline-flex items-center gap-1">
                  <ArrowUpDown className="size-3" aria-hidden="true" /> Navigate
                </span>
                <span className="inline-flex items-center gap-1">
                  <CornerDownLeft className="size-3" aria-hidden="true" />{' '}
                  Select
                </span>
              </div>
              <span>Esc to close</span>
            </div>
          </Command>
        </BaseDialog.Popup>
      </BaseDialog.Portal>
    </BaseDialog.Root>
  );
}
