import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  build: {
    outDir: '../static',    // wynikowy build idzie do app/static
    emptyOutDir: true,
  },
  base: './',               // ścieżki względne, żeby działało poprawnie
})