import * as DropdownMenu from '@radix-ui/react-dropdown-menu';
import { Monitor, Moon, Sun } from 'lucide-react';
import { type Theme, useTheme } from '../lib/theme';
import { cx } from './ui/primitives';

const OPTIONS: { id: Theme; label: string; Icon: typeof Sun }[] = [
  { id: 'light', label: 'Light', Icon: Sun },
  { id: 'dark', label: 'Dark', Icon: Moon },
  { id: 'system', label: 'System', Icon: Monitor },
];

export function ThemeToggle() {
  const { theme, effective, setTheme } = useTheme();
  const ActiveIcon =
    theme === 'system' ? Monitor : theme === 'light' ? Sun : Moon;

  return (
    <DropdownMenu.Root>
      <DropdownMenu.Trigger asChild>
        <button
          type="button"
          aria-label={`Theme: ${theme} (${effective})`}
          title={`Theme: ${theme} (${effective})`}
          className="inline-flex items-center justify-center h-8 w-8 rounded-sm text-text-muted hover:text-text hover:bg-overlay-5 focus-visible:outline focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-1"
        >
          <ActiveIcon className="w-4 h-4" />
        </button>
      </DropdownMenu.Trigger>
      <DropdownMenu.Portal>
        <DropdownMenu.Content
          align="end"
          sideOffset={6}
          className="z-50 min-w-[160px] glass-strong rounded-sm border border-subtle p-1 shadow-2xl"
        >
          {OPTIONS.map(({ id, label, Icon }) => (
            <DropdownMenu.Item
              key={id}
              onSelect={() => setTheme(id)}
              className={cx(
                'flex items-center gap-2 px-2 py-1.5 text-xs rounded-sm cursor-pointer outline-none',
                'hover:bg-overlay-5 focus:bg-overlay-5',
                theme === id ? 'text-accent' : 'text-text',
              )}
            >
              <Icon className="w-3.5 h-3.5" />
              <span className="flex-1">{label}</span>
              {theme === id ? (
                <span className="text-[10px] text-text-faint">·</span>
              ) : null}
            </DropdownMenu.Item>
          ))}
        </DropdownMenu.Content>
      </DropdownMenu.Portal>
    </DropdownMenu.Root>
  );
}
