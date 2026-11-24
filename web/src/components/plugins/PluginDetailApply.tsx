import type { PluginEntry } from '../../lib/queries';
import { Section } from '../ui/primitives';

/**
 * Same look as a `secondary` md `Button`, for real navigation anchors. Phones
 * stack them full width at a 40px touch height.
 */
const SECONDARY_LINK_CLASS =
  'inline-flex h-8 items-center justify-center rounded-sm border border-subtle-strong px-3 text-label font-medium whitespace-nowrap text-text transition-colors hover:bg-panel-strong focus-visible:outline focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-2 max-sm:h-10 max-sm:w-full';

export function PluginDetailApply({ plugin }: { plugin: PluginEntry }) {
  return (
    <Section title="Use this plugin">
      <p className="text-body text-text-muted">
        Choose where this plugin should run. You can attach it from the page
        that owns that traffic or upstream.
      </p>
      <div className="flex flex-col gap-2 sm:flex-row sm:flex-wrap">
        {plugin.supported_slots?.includes('router') && (
          <a href="/principals" className={SECONDARY_LINK_CLASS}>
            Apply to router (Principals)
          </a>
        )}
        {plugin.supported_slots?.includes('shape') && (
          <>
            <a href="/principals" className={SECONDARY_LINK_CLASS}>
              Apply to shape (Principals)
            </a>
            <a href="/upstreams" className={SECONDARY_LINK_CLASS}>
              Apply to warmup dialect (Upstreams)
            </a>
          </>
        )}
      </div>
    </Section>
  );
}
