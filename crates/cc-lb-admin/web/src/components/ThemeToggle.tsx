import { Menu as BaseMenu } from '@base-ui/react/menu';
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
    <BaseMenu.Root>
      <BaseMenu.Trigger
        aria-label={`Theme: ${theme} (${effective})`}
        className="inline-flex items-center justify-center h-8 w-8 rounded-sm text-text-muted hover:text-text hover:bg-overlay-5 focus-visible:outline focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-1"
        title={`Theme: ${theme} (${effective})`}
      >
        <ActiveIcon className="w-4 h-4" />
      </BaseMenu.Trigger>
      <BaseMenu.Portal>
        <BaseMenu.Positioner align="end" sideOffset={6}>
          <BaseMenu.Popup className="z-50 min-w-[160px] glass-strong rounded-sm border border-subtle p-1 shadow-2xl">
            <BaseMenu.RadioGroup
              onValueChange={(value) => setTheme(value as Theme)}
              value={theme}
            >
              {OPTIONS.map(({ id, label, Icon }) => (
                <BaseMenu.RadioItem
                  className={cx(
                    'flex items-center gap-2 px-2 py-1.5 text-xs rounded-sm cursor-pointer outline-none',
                    'data-[highlighted]:bg-[color:var(--color-overlay-5)]',
                    theme === id ? 'text-accent' : 'text-text',
                  )}
                  key={id}
                  value={id}
                >
                  <Icon className="w-3.5 h-3.5" />
                  <span className="flex-1">{label}</span>
                  <BaseMenu.RadioItemIndicator className="text-[10px] text-text-faint">
                    ·
                  </BaseMenu.RadioItemIndicator>
                </BaseMenu.RadioItem>
              ))}
            </BaseMenu.RadioGroup>
          </BaseMenu.Popup>
        </BaseMenu.Positioner>
      </BaseMenu.Portal>
    </BaseMenu.Root>
  );
}
