import swc from 'unplugin-swc';
import { defineConfig } from 'vitest/config';

export default defineConfig({
  plugins: [
    swc.vite({
      jsc: { transform: { legacyDecorator: true, decoratorMetadata: true } },
    }),
  ],
  test: {
    // `npm run test:cov` measures the unit tests only (they need no NATS server); the
    // thresholds sit about one point under the coverage they reach, so CI fails on a drop.
    coverage: {
      provider: 'v8',
      include: ['src/**/*.ts'],
      thresholds: { lines: 94, statements: 92, functions: 93, branches: 89 },
    },
    projects: [
      {
        test: {
          name: 'unit',
          include: ['test/unit/**/*.spec.ts'],
          setupFiles: ['test/setup/reflect.ts'],
        },
      },
      {
        test: {
          name: 'integration',
          include: ['test/integration/**/*.int.spec.ts'],
          setupFiles: ['test/setup/reflect.ts'],
          testTimeout: 60_000,
          hookTimeout: 120_000,
          fileParallelism: false,
        },
      },
    ],
  },
});
