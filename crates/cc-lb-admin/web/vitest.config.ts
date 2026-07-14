import { defineConfig } from 'vitest/config';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  test: {
    projects: [
      {
        test: {
          name: 'lib',
          environment: 'node',
          include: ['src/lib/**/*.test.ts', 'src/components/ui/latency/**/*.test.ts', 'src/routes/**/*.test.ts', 'src/components/upstreams/**/*.test.ts'],
          exclude: ['src/lib/hooks/__tests__/**'],
        },
      },
      {
        plugins: [react()],
        test: {
          name: 'components',
          environment: 'jsdom',
          setupFiles: ['./vitest.setup.ts'],
          include: [
            'src/lib/hooks/__tests__/**/*.test.ts',
            'src/components/__tests__/**/*.test.tsx',
            'src/components/**/*.test.tsx',
            // Route-adjacent tests use the TanStack `-` prefix (e.g. `-plugins.test.tsx`).
            'src/routes/**/-*.test.tsx',
          ],
        },
      },
    ],
  },
});
