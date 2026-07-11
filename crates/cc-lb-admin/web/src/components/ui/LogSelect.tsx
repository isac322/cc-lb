import { Select as BaseSelect } from '@base-ui/react/select';
import { Check, ChevronDown } from 'lucide-react';
import type { ReactNode } from 'react';
import { cx, INPUT_CLASS } from './primitives';

export type FilterOption = {
  value: string;
  label: ReactNode;
  hint?: ReactNode;
};

const LOG_SELECT_ITEM_CLASS =
  'flex items-center gap-2 px-2 py-1.5 text-xs rounded-sm cursor-pointer outline-none data-[highlighted]:bg-[color:var(--color-overlay-5)]';

export function LogSelect({
  value,
  options,
  onChange,
  allLabel,
  widthClass,
}: {
  value: string;
  options: FilterOption[];
  onChange: (value: string) => void;
  allLabel: string;
  widthClass: string;
}) {
  return (
    <BaseSelect.Root value={value} onValueChange={(v) => onChange(v ?? '')}>
      <BaseSelect.Trigger
        className={cx(
          INPUT_CLASS,
          widthClass,
          'flex items-center justify-between gap-2 cursor-pointer shrink-0',
        )}
      >
        <BaseSelect.Value className="min-w-0 flex-1 overflow-hidden text-left">
          {(selected: unknown) => {
            const key = typeof selected === 'string' ? selected : '';
            if (!key)
              return (
                <span className="text-text-faint text-sm truncate whitespace-nowrap block">
                  {allLabel}
                </span>
              );
            const opt = options.find((o) => o.value === key);
            return (
              <span className="truncate whitespace-nowrap block text-sm">
                {opt?.label ?? key}
              </span>
            );
          }}
        </BaseSelect.Value>
        <BaseSelect.Icon className="shrink-0 text-text-faint">
          <ChevronDown className="w-3.5 h-3.5" />
        </BaseSelect.Icon>
      </BaseSelect.Trigger>
      <BaseSelect.Portal>
        <BaseSelect.Positioner sideOffset={4} alignItemWithTrigger={false}>
          <BaseSelect.Popup
            className="z-50 max-h-[320px] overflow-auto glass-strong rounded-sm border border-subtle p-1 shadow-2xl"
            data-testid="log-select-popup"
            style={{ width: 'var(--anchor-width)' }}
          >
            <BaseSelect.List>
              <BaseSelect.Item value="" className={LOG_SELECT_ITEM_CLASS}>
                <BaseSelect.ItemIndicator className="w-3.5 shrink-0 text-accent">
                  <Check className="w-3 h-3" />
                </BaseSelect.ItemIndicator>
                <span className="text-text">{allLabel}</span>
              </BaseSelect.Item>
              {options.map((opt) => (
                <BaseSelect.Item
                  key={opt.value}
                  value={opt.value}
                  className={LOG_SELECT_ITEM_CLASS}
                >
                  <BaseSelect.ItemIndicator className="w-3.5 shrink-0 text-accent">
                    <Check className="w-3 h-3" />
                  </BaseSelect.ItemIndicator>
                  <BaseSelect.ItemText className="flex-1 min-w-0">
                    {opt.label}
                  </BaseSelect.ItemText>
                  {opt.hint && (
                    <span className="text-[10px] text-text-faint shrink-0">
                      {opt.hint}
                    </span>
                  )}
                </BaseSelect.Item>
              ))}
            </BaseSelect.List>
          </BaseSelect.Popup>
        </BaseSelect.Positioner>
      </BaseSelect.Portal>
    </BaseSelect.Root>
  );
}
