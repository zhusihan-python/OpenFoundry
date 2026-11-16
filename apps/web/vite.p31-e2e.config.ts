import { fileURLToPath, URL } from 'node:url';
import type { Connect, ViteDevServer } from 'vite';
import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react-swc';

// Test-only dev server for the P3.1 real-backend workflow (and Root's
// ego-browser review). The isolated p31 stack has no auth service, so the two
// identity endpoints the app boot needs are stubbed locally; every other /api
// call is proxied to the p31 edge gateway (127.0.0.1:3931), which forwards
// /api/v1/datasets/* to DVS. The normal dev config (vite.config.ts) is
// untouched. Profile values mirror the public admin fixture claims in
// /private/tmp/openfoundry-p31-review/runtime-public.json.
const ADMIN_PROFILE = {
  id: '1a5554ba-d8be-485c-923b-234d6f2629a6',
  email: 'p31-admin@example.com',
  name: 'P31 Admin',
  is_active: true,
  roles: ['admin'],
  groups: [],
  permissions: ['dataset.write', 'dataset:write', 'dataset.read'],
  organization_id: 'dd3f286c-9c17-4a0e-932f-286968d8a8a7',
  attributes: {},
  mfa_enabled: false,
  mfa_enforced: false,
  auth_source: 'local',
  created_at: '2026-11-16T00:00:00Z',
};

function sendJson(res: Connect.ServerResponse, body: unknown) {
  res.statusCode = 200;
  res.setHeader('content-type', 'application/json');
  res.end(JSON.stringify(body));
}

// P31_* overrides keep loopback defaults; see e2e/fixtures/README.md.
const WEB_PORT = Number(process.env.P31_WEB_PORT ?? 5195);
const GW_URL = process.env.P31_GW_URL ?? 'http://127.0.0.1:3931';

export default defineConfig({
  plugins: [
    react(),
    {
      name: 'p31-identity-stub',
      configureServer(server: ViteDevServer) {
        // Registered before Vite's proxy middleware so these two paths never
        // reach the gateway.
        server.middlewares.use((req, res, next) => {
          const url = req.url ?? '';
          if (url.startsWith('/api/v1/auth/bootstrap-status')) {
            sendJson(res, { requires_initial_admin: false });
            return;
          }
          if (url.startsWith('/api/v1/users/me')) {
            sendJson(res, ADMIN_PROFILE);
            return;
          }
          next();
        });
      },
    },
  ],
  resolve: {
    alias: {
      '@': fileURLToPath(new URL('./src', import.meta.url)),
      '@api': fileURLToPath(new URL('./src/lib/api', import.meta.url)),
      '@components': fileURLToPath(new URL('./src/lib/components', import.meta.url)),
      '@stores': fileURLToPath(new URL('./src/lib/stores', import.meta.url)),
      '@utils': fileURLToPath(new URL('./src/lib/utils', import.meta.url)),
    },
  },
  server: {
    host: '127.0.0.1',
    port: WEB_PORT,
    strictPort: true,
    proxy: {
      '/api': {
        target: GW_URL,
        changeOrigin: true,
      },
    },
  },
});
