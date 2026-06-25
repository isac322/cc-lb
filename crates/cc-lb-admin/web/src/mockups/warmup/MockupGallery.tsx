import { PageContainer } from '../../components/ui/primitives';
import { UpstreamDetailMock } from './UpstreamDetailMock';

export function MockupGallery() {
  return (
    <PageContainer>
      <div className="space-y-8 py-6">
        <header className="space-y-4 border-b border-subtle pb-5">
          <div>
            <p className="text-[11px] uppercase tracking-wider text-text-faint">
              Warm-up card redesign mockup
            </p>
            <h1 className="mt-2 text-xl font-semibold text-text">
              Variant C feedback revision
            </h1>
            <p className="mt-2 max-w-2xl text-sm leading-6 text-text-muted">
              Rejected A/B directions are removed. This page now compares only
              Variant C-based Warm-up cards with aligned OAuth Status cards in
              an upstream-detail-like layout.
            </p>
          </div>
        </header>
        <UpstreamDetailMock />
      </div>
    </PageContainer>
  );
}
