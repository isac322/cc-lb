import tailwindcss from '@tailwindcss/vite';
import react from '@vitejs/plugin-react';
import { defineConfig } from 'vite';

export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    proxy: {
      '/admin': {
        target: 'http://localhost:8082',
        changeOrigin: true,
      },
    },
  },
});
