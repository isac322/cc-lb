import { Menu as BaseMenu } from '@base-ui/react/menu';
import { Check, Monitor, Moon, Sun } from 'lucide-react';
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
        className="inline-flex shrink-0 items-center justify-center h-11 w-11 md:h-8 md:w-8 rounded-sm text-text-muted transition-colors hover:text-text hover:bg-overlay-5 focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-1"
        title={`Theme: ${theme} (${effective})`}
      >
        <ActiveIcon className="w-4 h-4" aria-hidden="true" />
      </BaseMenu.Trigger>
      <BaseMenu.Portal>
        <BaseMenu.Positioner align="end" sideOffset={6}>
          <BaseMenu.Popup className="z-50 min-w-[160px] glass-strong rounded-md p-1 outline-none">
            <BaseMenu.RadioGroup
              onValueChange={(value) => setTheme(value as Theme)}
              value={theme}
            >
              {OPTIONS.map(({ id, label, Icon }) => (
                <BaseMenu.RadioItem
                  className={cx(
                    'flex items-center gap-2 px-2 min-h-9 md:min-h-8 text-body-sm rounded-sm cursor-pointer outline-none text-text',
                    'data-[highlighted]:bg-overlay-5',
                  )}
                  key={id}
                  value={id}
                >
                  <Icon
                    className="size-3.5 text-text-muted"
                    strokeWidth={1.75}
                    aria-hidden="true"
                  />
                  <span className="flex-1">{label}</span>
                  <BaseMenu.RadioItemIndicator className="text-accent-text">
                    <Check
                      className="size-3.5"
                      strokeWidth={1.75}
                      aria-hidden="true"
                    />
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
