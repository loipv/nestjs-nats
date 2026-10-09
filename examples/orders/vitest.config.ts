import swc from 'unplugin-swc';
import { defineConfig } from 'vitest/config';

// SWC emits the decorator metadata that Nest's dependency injection needs; esbuild does not.
export default defineConfig({
  plugins: [
    swc.vite({
      jsc: { transform: { legacyDecorator: true, decoratorMetadata: true } },
    }),
  ],
  test: {
    include: ['test/**/*.spec.ts'],
    setupFiles: ['test/setup.ts'],
  },
});
