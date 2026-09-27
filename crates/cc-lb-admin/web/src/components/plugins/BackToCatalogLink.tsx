import { ArrowLeft } from 'lucide-react';

/** A quiet 12px link that sits above the plugin title, never on its baseline. */
export function BackToCatalogLink({ onBack }: { onBack: () => void }) {
  return (
    <button
      type="button"
      onClick={onBack}
      className="-ml-1 inline-flex h-11 md:h-6 items-center gap-1 rounded-sm px-1 text-caption text-text-muted transition-colors hover:text-text focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-1"
    >
      <ArrowLeft className="size-3.5" strokeWidth={1.75} aria-hidden="true" />
      Back to catalog
    </button>
  );
}
