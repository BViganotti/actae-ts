import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    environment: 'node',
    include: ['test/**/*.test.ts'],
    testTimeout: 60000,
    env: {
      // Live tests require an explicit opt-in AND a running Actae:
      //   cd actae && cargo run -- --dev
      //   cd sdks/typescript && ACTAE_LIVE=1 npm run test:live
      ACTAE_LIVE: process.env.ACTAE_LIVE ?? '0',
      ACTAE_URL: process.env.ACTAE_URL ?? 'http://localhost:8002',
      ACTAE_API_KEY:
        process.env.ACTAE_API_KEY ?? 'sk-dev-0000000000000000000000',
    },
  },
});
