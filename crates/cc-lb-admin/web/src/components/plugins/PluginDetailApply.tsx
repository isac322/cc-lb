import type { PluginEntry } from '../../lib/queries';
import { Card, CardBody, Section } from '../ui/primitives';

/** Same look as a `secondary` md `Button`, for real navigation anchors. */
const SECONDARY_LINK_CLASS =
  'inline-flex h-8 items-center justify-center rounded-sm border border-subtle bg-panel-strong px-3 text-[0.8125rem] font-medium whitespace-nowrap text-text transition-colors hover:border-subtle-strong hover:bg-hover-bg focus-visible:outline focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-2';

export function PluginDetailApply({ plugin }: { plugin: PluginEntry }) {
  return (
    <Section title="Use this plugin">
      <Card>
        <CardBody>
          <p className="text-body-sm text-text-muted">
            Choose where this plugin should run. You can attach it from the page
            that owns that traffic or upstream.
          </p>
          <div className="mt-3 flex flex-wrap gap-2">
            {plugin.supported_slots?.includes('router') && (
              <a href="/principals" className={SECONDARY_LINK_CLASS}>
                Apply to router (Principals)
              </a>
            )}
            {plugin.supported_slots?.includes('observability_hook') && (
              <a href="/principals" className={SECONDARY_LINK_CLASS}>
                Apply to observability (Principals)
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
        </CardBody>
      </Card>
    </Section>
  );
}
