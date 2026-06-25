import { createFileRoute } from '@tanstack/react-router';

import { MockupGallery } from '../mockups/warmup/MockupGallery';

export const Route = createFileRoute('/mockups/warmup')({
  component: MockupGallery,
});
