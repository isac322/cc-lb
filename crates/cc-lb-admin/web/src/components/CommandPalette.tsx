import { Command } from 'cmdk';
import { useEffect } from 'react';
import * as Dialog from '@radix-ui/react-dialog';
import { useNavigate } from '@tanstack/react-router';
import {
  ArrowUpDown,
  Box,
  CornerDownLeft,
  KeyRound,
  LayoutDashboard,
  Plus,
  Power,
  Server,
  Settings,
  Users,
  X,
} from 'lucide-react';
import { usePrincipals, useUpstreams } from '../lib/queries';

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
    <Dialog.Root open={open} onOpenChange={onOpenChange}>
      <Dialog.Portal>
        <Dialog.Overlay className="fixed inset-0 z-50 bg-modal-backdrop backdrop-blur-sm" />
        <Dialog.Content
          aria-describedby={undefined}
          style={{ maxHeight: 'min(700px, 80vh)' }}
          className="fixed top-[5vh] left-1/2 -translate-x-1/2 z-50 w-[calc(100%-2rem)] max-w-xl outline-none"
        >
          <Dialog.Title className="sr-only">Command palette</Dialog.Title>
          <Dialog.Description className="sr-only">Search resources or run commands</Dialog.Description>
          <Command label="Command palette" className="bg-bg-sub border border-subtle-strong rounded-sm shadow-2xl grid grid-rows-[auto_minmax(0,1fr)_auto] max-h-full overflow-hidden">
            <div className="flex items-center gap-2 px-3 h-11 border-b border-subtle">
              <Command.Input placeholder="Search resources or run commands…" autoFocus className="flex-1 bg-transparent text-sm text-text outline-none" />
              <kbd className="font-mono text-[10px] text-text-faint bg-overlay-3 px-1.5 py-0.5 rounded-sm border border-subtle">ESC</kbd>
              <Dialog.Close asChild>
                <button type="button" aria-label="Close" className="text-text-muted hover:text-text">
                  <X className="w-4 h-4" />
                </button>
              </Dialog.Close>
            </div>
            <Command.List className="overflow-y-auto" style={{ paddingBottom: '0.5rem' }}>
              <Command.Empty>No matches found.</Command.Empty>
              <Command.Group heading="Pages">
                <Command.Item onSelect={() => go('/')}><LayoutDashboard className="w-4 h-4 text-text-faint" />Go to Overview</Command.Item>
                <Command.Item onSelect={() => go('/upstreams')}><Server className="w-4 h-4 text-text-faint" />Go to Upstreams</Command.Item>
                <Command.Item onSelect={() => go('/principals')}><Users className="w-4 h-4 text-text-faint" />Go to Principals</Command.Item>
                <Command.Item onSelect={() => go('/plugins')}><Box className="w-4 h-4 text-text-faint" />Go to Plugins</Command.Item>
                <Command.Item onSelect={() => go('/logs')}><Box className="w-4 h-4 text-text-faint" />Go to Logs</Command.Item>
                <Command.Item onSelect={() => go('/status')}><Power className="w-4 h-4 text-text-faint" />Go to Status</Command.Item>
                <Command.Item onSelect={() => go('/settings')}><Settings className="w-4 h-4 text-text-faint" />Go to Settings</Command.Item>
              </Command.Group>
              <Command.Group heading="Actions">
                <Command.Item onSelect={() => { go('/upstreams'); setTimeout(() => document.getElementById('btn-new-upstream')?.click(), 100); }}><Plus className="w-4 h-4 text-text-faint" />Create upstream</Command.Item>
                <Command.Item onSelect={() => { go('/principals'); setTimeout(() => document.getElementById('btn-new-principal')?.click(), 100); }}><Plus className="w-4 h-4 text-text-faint" />Create principal</Command.Item>
                <Command.Item onSelect={() => { go('/plugins'); setTimeout(() => document.getElementById('btn-upload-wasm')?.click(), 100); }}><Plus className="w-4 h-4 text-text-faint" />Upload Wasm plugin</Command.Item>
                <Command.Item onSelect={() => { go('/settings'); setTimeout(() => document.getElementById('btn-rotate-token')?.click(), 100); }}><KeyRound className="w-4 h-4 text-text-faint" />Rotate admin token</Command.Item>
              </Command.Group>
              <Command.Group heading="Upstreams">
                {upstreams.isLoading ? <Command.Item disabled>Loading upstreams…</Command.Item> :
                  (upstreams.data?.upstreams ?? []).map((u) => (
                    <Command.Item key={u.id} value={`upstream-${u.name}`} onSelect={() => go(`/upstreams?selectedId=${u.id}`)}>
                      <Server className="w-4 h-4 text-text-faint" />
                      <span>{u.name}</span>
                      <span className="ml-auto text-[10px] text-text-faint font-mono">{u.kind}</span>
                    </Command.Item>
                  ))}
              </Command.Group>
              <Command.Group heading="Principals">
                {principals.isLoading ? <Command.Item disabled>Loading principals…</Command.Item> :
                  (principals.data?.principals ?? []).map((p) => (
                    <Command.Item key={p.id} value={`principal-${p.name}`} onSelect={() => go(`/principals?selectedId=${p.id}`)}>
                      <Users className="w-4 h-4 text-text-faint" />
                      <span>{p.name}</span>
                      <span className="ml-auto text-[10px] text-text-faint font-mono">{p.kind}</span>
                    </Command.Item>
                  ))}
              </Command.Group>
            </Command.List>
            <div className="flex items-center justify-between px-3 h-9 border-t border-subtle bg-overlay-1 text-[10px] text-text-faint">
              <div className="flex items-center gap-3">
                <span className="inline-flex items-center gap-1"><ArrowUpDown className="w-3 h-3" /> Navigate</span>
                <span className="inline-flex items-center gap-1"><CornerDownLeft className="w-3 h-3" /> Select</span>
              </div>
              <span className="inline-flex items-center gap-1">ESC Close</span>
            </div>
          </Command>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}
