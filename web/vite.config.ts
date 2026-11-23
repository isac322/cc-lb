/// <reference types="vitest" />
import tailwindcss from '@tailwindcss/vite';
import { TanStackRouterVite } from '@tanstack/router-plugin/vite';
import react from '@vitejs/plugin-react';
import { defineConfig } from 'vitest/config';

const ADMIN_TARGET = process.env.CC_LB_ADMIN_URL ?? 'http://127.0.0.1:8001';
const ADMIN_TOKEN = process.env.CC_LB_ADMIN_TOKEN;
const ADMIN_PROXY_HEADERS = ADMIN_TOKEN
  ? { Authorization: `Bearer ${ADMIN_TOKEN}` }
  : undefined;

export default defineConfig({
  // TanStackRouterVite MUST run before @vitejs/plugin-react.
  plugins: [
    TanStackRouterVite({ target: 'react', autoCodeSplitting: true }),
    react(),
    tailwindcss(),
  ],
  build: {
    assetsInlineLimit: 0,
  },
  server: {
    host: '0.0.0.0',
    port: 5173,
    strictPort: true,
    proxy: {
      '/admin/v1': {
        target: ADMIN_TARGET,
        changeOrigin: true,
        headers: ADMIN_PROXY_HEADERS,
      },
      '/admin/health': {
        target: ADMIN_TARGET,
        changeOrigin: true,
        headers: ADMIN_PROXY_HEADERS,
      },
    },
  },
  test: {
    environment: 'jsdom',
    setupFiles: ['./vitest.setup.ts'],
  },
});
