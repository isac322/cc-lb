/// <reference types="vitest" />
import tailwindcss from '@tailwindcss/vite';
import { TanStackRouterVite } from '@tanstack/router-plugin/vite';
import react from '@vitejs/plugin-react';
import { defineConfig } from 'vitest/config';

const ADMIN_TARGET = process.env.CC_LB_ADMIN_URL ?? 'http://127.0.0.1:8001';

export default defineConfig({
  // TanStackRouterVite MUST run before @vitejs/plugin-react.
  plugins: [
    TanStackRouterVite({ target: 'react', autoCodeSplitting: true }),
    react(),
    tailwindcss(),
  ],
  server: {
    host: '0.0.0.0',
    port: 5173,
    strictPort: true,
    proxy: {
      '/admin/v1': { target: ADMIN_TARGET, changeOrigin: true },
      '/admin/events': { target: ADMIN_TARGET, changeOrigin: true },
      '/admin/health': { target: ADMIN_TARGET, changeOrigin: true },
      '/admin/dashboard': { target: ADMIN_TARGET, changeOrigin: true },
      '/admin/usage': { target: ADMIN_TARGET, changeOrigin: true },
      '/admin/audit': { target: ADMIN_TARGET, changeOrigin: true },
      '/admin/credentials': { target: ADMIN_TARGET, changeOrigin: true },
      '/admin/oauth': { target: ADMIN_TARGET, changeOrigin: true },
      '/admin/config': { target: ADMIN_TARGET, changeOrigin: true },
      '/admin/status': { target: ADMIN_TARGET, changeOrigin: true },
      '/admin/killswitch': { target: ADMIN_TARGET, changeOrigin: true },
    },
  },
  test: {
    environment: 'jsdom',
    setupFiles: ['./vitest.setup.ts'],
  },
});
