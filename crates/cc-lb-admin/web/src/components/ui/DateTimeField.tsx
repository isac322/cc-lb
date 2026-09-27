import { Input as BaseInput } from '@base-ui/react/input';
import { useId } from 'react';
import { parseDatePart } from '../../lib/calendarDate';
import { parseInTimezone } from '../../lib/timezone';
import { CalendarPopover } from './CalendarPopover';
import { cx, Field, INPUT_SM_CLASS } from './primitives';

export function parseBound(
  str: string,
  otherStr: string,
  tz: string,
  field: 'since' | 'until',
): { ts?: number; err?: string } {
  if (!str) return { err: otherStr ? 'Required' : undefined };
  const match = /^(\d{4}-\d{2}-\d{2})[ T](\d{2}):(\d{2})$/.exec(str);
  const datePart = match?.[1];
  const hourPart = match?.[2];
  const minutePart = match?.[3];
  if (
    datePart === undefined ||
    hourPart === undefined ||
    minutePart === undefined ||
    parseDatePart(datePart) === undefined ||
    Number(hourPart) > 23 ||
    Number(minutePart) > 59
  ) {
    return { err: 'Invalid time' };
  }
  const parsed = parseInTimezone(str.replace(' ', 'T'), tz);
  if (parsed.error || parsed.ts === null) {
    const err =
      parsed.error === 'nonexistent'
        ? 'Time does not exist (DST)'
        : parsed.error === 'ambiguous'
          ? 'Ambiguous time (DST)'
          : 'Invalid time';
    return { err };
  }
  const minuteStart = Math.floor(parsed.ts / 1000);
  return { ts: field === 'until' ? minuteStart + 59 : minuteStart };
}

export function DateTimeField({
  label,
  error,
  value,
  onChange,
  onPickDate,
  onEscape,
  placeholder = 'YYYY-MM-DD HH:mm',
  'aria-label': ariaLabel,
}: {
  readonly label: string;
  readonly error?: string;
  readonly value: string;
  readonly onChange: (value: string) => void;
  readonly onPickDate: (isoDate: string) => void;
  readonly onEscape?: () => void;
  /** Shown while empty; defaults to the accepted format. */
  readonly placeholder?: string;
  readonly 'aria-label'?: string;
}) {
  const errorId = useId();

  return (
    <Field label={label} error={error} errorId={errorId}>
      <div className="relative">
        <BaseInput
          aria-label={ariaLabel}
          type="text"
          inputMode="numeric"
          placeholder={placeholder}
          autoComplete="off"
          className={cx(INPUT_SM_CLASS, '!w-48 tabular-nums pl-8')}
          value={value}
          aria-invalid={error != null ? true : undefined}
          aria-describedby={error != null ? errorId : undefined}
          onChange={(e) => onChange(e.target.value)}
          onKeyDown={(event) => {
            if (event.key === 'Escape') onEscape?.();
          }}
        />
        <CalendarPopover
          value={value}
          onPickDate={onPickDate}
          onEscape={onEscape}
          triggerLabel={`${label} calendar`}
        />
      </div>
    </Field>
  );
}
