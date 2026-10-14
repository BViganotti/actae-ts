import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    environment: 'node',
    include: ['test/**/*.test.ts'],
    testTimeout: 15000,
    coverage: {
      provider: 'v8',
      include: ['src/**/*.ts'],
      exclude: ['src/index.ts', 'src/uuid.ts', 'src/version.ts', '**/*.d.ts'],
      reporter: ['text', 'json-summary'],
      thresholds: {
        statements: 90,
        lines: 90,
        functions: 75,
        branches: 75,
      },
    },
  },
});
