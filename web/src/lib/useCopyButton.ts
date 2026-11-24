import { useCallback, useEffect, useRef, useState } from 'react';
import { toast } from 'sonner';

/**
 * Shared clipboard-copy hook for dashboard "Copy ..." buttons. Centralises:
 * - The `navigator.clipboard` availability guard (undefined in non-secure
 *   contexts such as `http://<lan-ip>` or sandboxed iframes — the cause of
 *   the previous silent failures).
 * - The promise/try-catch so a rejection surfaces as a sonner error toast
 *   instead of an unhandled rejection or a silently-aborted click handler.
 * - The success toast and a `copied` flag that auto-resets after `resetMs`
 *   for buttons whose label flips to "Copied".
 *
 * The `noun` arg names the thing being copied:
 *   success → `<Noun> copied`         (or just `Copied` when omitted)
 *   failure → `Failed to copy <noun>` (or `Copy failed` when omitted)
 */
export function useCopyButton(resetMs = 1500) {
  const [copied, setCopied] = useState(false);
  const timeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(
    () => () => {
      if (timeoutRef.current) clearTimeout(timeoutRef.current);
    },
    [],
  );

  const copy = useCallback(
    async (value: string, noun?: string): Promise<boolean> => {
      const ok = await writeToClipboard(value);
      if (ok) {
        toast.success(noun ? `${noun} copied` : 'Copied');
        setCopied(true);
        if (timeoutRef.current) clearTimeout(timeoutRef.current);
        timeoutRef.current = setTimeout(() => setCopied(false), resetMs);
      } else {
        toast.error(
          noun ? `Failed to copy ${noun.toLowerCase()}` : 'Copy failed',
        );
      }
      return ok;
    },
    [resetMs],
  );

  return { copied, copy };
}

/**
 * Low-level helper for the rare non-React caller. Returns `true` on success,
 * `false` on any failure (missing API, rejected promise). Never throws.
 */
export async function writeToClipboard(value: string): Promise<boolean> {
  if (typeof navigator === 'undefined' || !navigator.clipboard) {
    return false;
  }
  try {
    await navigator.clipboard.writeText(value);
    return true;
  } catch {
    return false;
  }
}
