import { toast } from 'sonner';

/**
 * Success toast for a reversible mutation. `onUndo` should apply the same
 * mutation with the previous value; the toast closes when Undo is pressed.
 * Returns the sonner toast id.
 */
export function undoToast({
  message,
  onUndo,
}: {
  message: string;
  onUndo: () => void;
}): string | number {
  return toast.success(message, {
    duration: 6000,
    action: { label: 'Undo', onClick: () => onUndo() },
  });
}
