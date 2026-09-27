import { Menu as BaseMenu } from '@base-ui/react/menu';
import { Check, Monitor, Moon, Sun } from 'lucide-react';
import { type Theme, useTheme } from '../lib/theme';
import { cx } from './ui/primitives';

/** Night = graphite (dark), Day = neutral daylight (light). */
const OPTIONS: { id: Theme; label: string; Icon: typeof Sun }[] = [
  { id: 'dark', label: 'Night', Icon: Moon },
  { id: 'light', label: 'Day', Icon: Sun },
  { id: 'system', label: 'Match system', Icon: Monitor },
];

/**
 * Pill naming the theme in effect ("Night" / "Day") with a half-filled
 * dial glyph; opens a menu to pick Night, Day or follow the system.
 */
export function ThemeToggle() {
  const { theme, effective, setTheme } = useTheme();
  const effectiveLabel = effective === 'light' ? 'Day' : 'Night';
  const description = `Theme: ${effectiveLabel}${theme === 'system' ? ' (matching system)' : ''}`;

  return (
    <BaseMenu.Root>
      <BaseMenu.Trigger
        aria-label={description}
        className="inline-flex shrink-0 items-center gap-2 h-9 md:h-8 px-3 rounded-full border border-subtle-strong text-label text-text transition-colors hover:bg-panel-strong focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-2"
        title={description}
      >
        <span
          aria-hidden="true"
          className="size-2.5 rounded-full border border-text bg-[linear-gradient(90deg,var(--color-text)_50%,transparent_50%)]"
        />
        <span aria-hidden="true">{effectiveLabel}</span>
      </BaseMenu.Trigger>
      <BaseMenu.Portal>
        <BaseMenu.Positioner align="end" sideOffset={6}>
          <BaseMenu.Popup className="z-50 min-w-[176px] glass-strong rounded-md p-1 outline-none">
            <BaseMenu.RadioGroup
              onValueChange={(value) => setTheme(value as Theme)}
              value={theme}
            >
              {OPTIONS.map(({ id, label, Icon }) => (
                <BaseMenu.RadioItem
                  className={cx(
                    'flex items-center gap-2 px-2 min-h-9 md:min-h-8 text-body rounded-sm cursor-pointer outline-none text-text',
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
