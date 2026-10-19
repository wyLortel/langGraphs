import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// 개발 서버(npm run dev)로 띄울 때만 쓰는 프록시. Docker에서는 nginx가 /api를 api:8000으로 넘긴다.
export default defineConfig({
  plugins: [react()],
  server: { proxy: { '/api': 'http://localhost:8000' } },
})
