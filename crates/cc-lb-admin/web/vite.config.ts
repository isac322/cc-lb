import tailwindcss from '@tailwindcss/vite';
import react from '@vitejs/plugin-react';
import { defineConfig } from 'vite';

export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    proxy: {
      '/admin/v1': {
        target: 'http://127.0.0.1:8082',
        changeOrigin: true,
      },
      '/admin/events': {
        target: 'http://127.0.0.1:8082',
        changeOrigin: true,
      },
      '/admin/health': {
        target: 'http://127.0.0.1:8082',
        changeOrigin: true,
      },
    },
  },
});
