/// <reference types="vitest" />
import tailwindcss from '@tailwindcss/vite';
import { TanStackRouterVite } from '@tanstack/router-plugin/vite';
import react from '@vitejs/plugin-react';
import { defineConfig } from 'vitest/config';

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
      '/admin/v1': { target: 'http://127.0.0.1:8001', changeOrigin: true },
      '/admin/events': { target: 'http://127.0.0.1:8001', changeOrigin: true },
      '/admin/health': { target: 'http://127.0.0.1:8001', changeOrigin: true },
      '/admin/dashboard': {
        target: 'http://127.0.0.1:8001',
        changeOrigin: true,
      },
      '/admin/usage': { target: 'http://127.0.0.1:8001', changeOrigin: true },
      '/admin/audit': { target: 'http://127.0.0.1:8001', changeOrigin: true },
      '/admin/credentials': {
        target: 'http://127.0.0.1:8001',
        changeOrigin: true,
      },
      '/admin/oauth': { target: 'http://127.0.0.1:8001', changeOrigin: true },
      '/admin/config': { target: 'http://127.0.0.1:8001', changeOrigin: true },
      '/admin/status': { target: 'http://127.0.0.1:8001', changeOrigin: true },
      '/admin/killswitch': {
        target: 'http://127.0.0.1:8001',
        changeOrigin: true,
      },
    },
  },
  test: {
    environment: 'jsdom',
    setupFiles: ['./vitest.setup.ts'],
  },
});
