import { fileURLToPath } from 'node:url';
import { defineConfig } from 'vitest/config';

const packagesDir = fileURLToPath(new URL('./packages', import.meta.url));

export default defineConfig({
  resolve: {
    // Run tests against package sources, not build output
    alias: [
      { find: /^@zusammen\/([^/]+)$/, replacement: `${packagesDir}/$1/src/index.ts` },
      { find: /^@zusammen\/([^/]+)\/(.+)$/, replacement: `${packagesDir}/$1/src/$2/index.ts` },
    ],
  },
  test: {
    include: ['packages/*/test/**/*.test.ts'],
    passWithNoTests: true,
    hookTimeout: 120_000,
    testTimeout: 30_000,
  },
});
