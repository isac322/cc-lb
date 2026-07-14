import type { PluginEntry } from '../../lib/queries';
import { Card, CardBody, Section } from '../ui/primitives';

const SHARED_LINK_CLASS =
  'inline-flex items-center justify-center rounded-sm font-medium transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent disabled:pointer-events-none disabled:opacity-50 bg-bg-sub text-text border border-subtle hover:bg-overlay-1 h-9 px-4 py-2 text-sm';

export function PluginDetailApply({ plugin }: { plugin: PluginEntry }) {
  return (
    <Section title="Use this plugin">
      <Card>
        <CardBody className="space-y-4">
          <div className="text-sm space-y-1">
            <p className="text-text-faint">
              Choose where this plugin should run. You can attach it from the
              page that owns that traffic or upstream.
            </p>
          </div>
          <div className="flex flex-wrap gap-3">
            {plugin.supported_slots?.includes('router') && (
              <a href="/principals" className={SHARED_LINK_CLASS}>
                Apply to Router (Principals)
              </a>
            )}
            {plugin.supported_slots?.includes('observability_hook') && (
              <a href="/principals" className={SHARED_LINK_CLASS}>
                Apply to Observability (Principals)
              </a>
            )}
            {plugin.supported_slots?.includes('shape') && (
              <>
                <a href="/principals" className={SHARED_LINK_CLASS}>
                  Apply to Shape (Principals)
                </a>
                <a href="/upstreams" className={SHARED_LINK_CLASS}>
                  Apply to Warmup Dialect (Upstreams)
                </a>
              </>
            )}
          </div>
        </CardBody>
      </Card>
    </Section>
  );
}
