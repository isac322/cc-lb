import { Popover as BasePopover } from '@base-ui/react/popover';
import { Calendar as CalendarIcon } from 'lucide-react';
import { useEffect, useState } from 'react';
import { DayPicker } from 'react-day-picker';
import 'react-day-picker/style.css';
import { formatDatePart, parseDatePart } from '../../lib/calendarDate';
import { cx, Modal } from './primitives';

// Themed via the `.rdp-root.cclb-calendar` rule in index.css — that selector must
// out-specify react-day-picker's own `.rdp-root` variable defaults. Date-only.
interface CalendarPopoverProps {
  readonly value: string;
  readonly onPickDate: (isoDate: string) => void;
  readonly onEscape?: () => void;
  readonly triggerLabel: string;
}

const DESKTOP_QUERY = '(min-width: 1024px)';
const TRIGGER_CLASS = cx(
  'absolute left-1.5 top-1/2 -translate-y-1/2 inline-flex items-center justify-center',
  'w-6 h-6 lg:w-5 lg:h-5 rounded-sm text-text-faint hover:text-text hover:bg-overlay-5 transition-colors',
  'focus-visible:outline focus-visible:outline-2 focus-visible:outline-accent',
);

function useDesktopCalendar(): boolean {
  const [isDesktop, setIsDesktop] = useState(() =>
    typeof window.matchMedia === 'function'
      ? window.matchMedia(DESKTOP_QUERY).matches
      : true,
  );

  useEffect(() => {
    if (typeof window.matchMedia !== 'function') return;
    const media = window.matchMedia(DESKTOP_QUERY);
    const update = () => setIsDesktop(media.matches);
    media.addEventListener('change', update);
    update();
    return () => media.removeEventListener('change', update);
  }, []);

  return isDesktop;
}

function CalendarContent({
  selected,
  onSelect,
}: {
  readonly selected?: Date;
  readonly onSelect: (date: Date) => void;
}) {
  return (
    <DayPicker
      mode="single"
      showOutsideDays
      selected={selected}
      defaultMonth={selected}
      className="cclb-calendar"
      onSelect={(date) => {
        if (date !== undefined) onSelect(date);
      }}
    />
  );
}

export function CalendarPopover({
  value,
  onPickDate,
  onEscape,
  triggerLabel,
}: CalendarPopoverProps) {
  const [open, setOpen] = useState(false);
  const selected = parseDatePart(value);
  const isDesktop = useDesktopCalendar();
  const selectDate = (date: Date) => {
    onPickDate(formatDatePart(date));
    setOpen(false);
  };

  if (!isDesktop) {
    return (
      <>
        <button
          type="button"
          aria-label={triggerLabel}
          className={TRIGGER_CLASS}
          onClick={() => setOpen(true)}
        >
          <CalendarIcon className="w-4 h-4" />
        </button>
        <Modal
          open={open}
          onOpenChange={(nextOpen, eventDetails) => {
            setOpen(nextOpen);
            if (!nextOpen && eventDetails.reason === 'escape-key') onEscape?.();
          }}
          title="Choose date"
          description={triggerLabel}
          size="sm"
        >
          <div className="flex justify-center">
            <CalendarContent selected={selected} onSelect={selectDate} />
          </div>
        </Modal>
      </>
    );
  }

  return (
    <BasePopover.Root
      open={open}
      onOpenChange={(nextOpen, eventDetails) => {
        setOpen(nextOpen);
        if (!nextOpen && eventDetails.reason === 'escape-key') onEscape?.();
      }}
    >
      <BasePopover.Trigger aria-label={triggerLabel} className={TRIGGER_CLASS}>
        <CalendarIcon className="w-4 h-4" />
      </BasePopover.Trigger>
      <BasePopover.Portal>
        <BasePopover.Positioner align="start" side="top" sideOffset={6}>
          <BasePopover.Popup className="z-50 bg-bg-sub border border-subtle-strong rounded-md shadow-xl p-2 text-text">
            <CalendarContent selected={selected} onSelect={selectDate} />
          </BasePopover.Popup>
        </BasePopover.Positioner>
      </BasePopover.Portal>
    </BasePopover.Root>
  );
}
