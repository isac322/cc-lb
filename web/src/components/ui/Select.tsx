import { Select as BaseSelect } from '@base-ui/react/select';
import { Check, ChevronDown } from 'lucide-react';
import type { ReactNode } from 'react';
import { cx, INPUT_CLASS, INPUT_SM_CLASS } from './primitives';

export type SelectOption = {
  value: string;
  label: ReactNode;
  /** Right-aligned secondary text in the list (counts, units). */
  hint?: ReactNode;
  disabled?: boolean;
};

const ITEM_CLASS =
  'flex items-center gap-2 px-2 min-h-8 max-md:min-h-10 py-1.5 text-body text-text rounded-sm cursor-pointer outline-none select-none data-[highlighted]:bg-overlay-5 data-[disabled]:cursor-not-allowed data-[disabled]:text-text-disabled data-[disabled]:data-[highlighted]:bg-transparent';

/** Reserves the check column so labels never shift when selection changes. */
function ItemIndicator() {
  return (
    <span className="flex w-3.5 shrink-0">
      <BaseSelect.ItemIndicator className="flex text-accent-text">
        <Check className="size-3.5" strokeWidth={1.75} aria-hidden="true" />
      </BaseSelect.ItemIndicator>
    </span>
  );
}
/**
 * The one select control (DESIGN.md §5 Forms). Base UI listbox with the
 * shared input chrome; never use a native `<select>`.
 *
 * - `allLabel` adds a leading "all" item whose value is `''` — the unfiltered
 *   default for filter bars. Without it, `placeholder` shows when `value`
 *   matches no option.
 * - Width comes from `className` (`w-44`, `w-full`); the popup is at least
 *   the trigger's width and grows to fit its longest option.
 * - Pass `aria-label` when no surrounding `Field`/`<label>` names it.
 */
export function Select({
  value,
  options,
  onChange,
  allLabel,
  placeholder = 'Select…',
  size = 'md',
  className,
  disabled,
  id,
  name,
  'aria-label': ariaLabel,
  'aria-labelledby': ariaLabelledBy,
  'aria-describedby': ariaDescribedBy,
  'aria-invalid': ariaInvalid,
  ...dataAttributes
}: {
  value: string;
  options: readonly SelectOption[];
  onChange: (value: string) => void;
  allLabel?: string;
  placeholder?: string;
  size?: 'sm' | 'md';
  className?: string;
  disabled?: boolean;
  id?: string;
  name?: string;
  'aria-label'?: string;
  'aria-labelledby'?: string;
  'aria-describedby'?: string;
  'aria-invalid'?: boolean;
} & { [dataAttribute: `data-${string}`]: string | boolean | undefined }) {
  const emptyLabel = allLabel ?? placeholder;
  return (
    <BaseSelect.Root
      value={value}
      onValueChange={(next) => onChange(typeof next === 'string' ? next : '')}
      disabled={disabled}
      name={name}
    >
      <BaseSelect.Trigger
        id={id}
        aria-label={ariaLabel}
        aria-labelledby={ariaLabelledBy}
        aria-describedby={ariaDescribedBy}
        aria-invalid={ariaInvalid || undefined}
        {...dataAttributes}
        className={cx(
          size === 'sm' ? INPUT_SM_CLASS : INPUT_CLASS,
          'group/select flex items-center justify-between gap-2 cursor-pointer shrink-0 text-left',
          'data-[popup-open]:border-accent data-[disabled]:control-disabled',
          className,
        )}
      >
        <BaseSelect.Value className="min-w-0 flex-1 overflow-hidden text-left">
          {(selected: unknown) => {
            const key = typeof selected === 'string' ? selected : '';
            const option = key
              ? options.find((candidate) => candidate.value === key)
              : undefined;
            if (!option && !key)
              return (
                <span className="block truncate whitespace-nowrap text-text-faint group-data-[disabled]/select:text-text-disabled">
                  {emptyLabel}
                </span>
              );
            return (
              <span className="block truncate whitespace-nowrap">
                {option?.label ?? key}
              </span>
            );
          }}
        </BaseSelect.Value>
        <BaseSelect.Icon className="flex shrink-0 text-text-faint group-data-[disabled]/select:text-text-disabled">
          <ChevronDown
            className="size-3.5"
            strokeWidth={1.75}
            aria-hidden="true"
          />
        </BaseSelect.Icon>
      </BaseSelect.Trigger>
      <BaseSelect.Portal>
        <BaseSelect.Positioner
          className="z-50"
          sideOffset={4}
          alignItemWithTrigger={false}
        >
          <BaseSelect.Popup
            className="max-h-[320px] max-w-[calc(100vw-2rem)] overflow-auto glass-strong rounded-md p-1 outline-none"
            data-testid="select-popup"
            style={{ minWidth: 'var(--anchor-width)' }}
          >
            <BaseSelect.List>
              {allLabel !== undefined ? (
                <BaseSelect.Item value="" className={ITEM_CLASS}>
                  <ItemIndicator />
                  <BaseSelect.ItemText className="min-w-0 flex-1 truncate">
                    {allLabel}
                  </BaseSelect.ItemText>
                </BaseSelect.Item>
              ) : null}
              {options.map((option) => (
                <BaseSelect.Item
                  key={option.value}
                  value={option.value}
                  disabled={option.disabled}
                  className={ITEM_CLASS}
                >
                  <ItemIndicator />
                  <BaseSelect.ItemText className="min-w-0 flex-1 truncate">
                    {option.label}
                  </BaseSelect.ItemText>
                  {option.hint ? (
                    <span className="shrink-0 text-caption text-text-faint tabular-nums">
                      {option.hint}
                    </span>
                  ) : null}
                </BaseSelect.Item>
              ))}
            </BaseSelect.List>
          </BaseSelect.Popup>
        </BaseSelect.Positioner>
      </BaseSelect.Portal>
    </BaseSelect.Root>
  );
}
