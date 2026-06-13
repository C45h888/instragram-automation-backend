// Phase 8 test configuration — ESM.
// Mirrors tests/phase-7/vitest.config.js shape but scoped to
// tests/phase-8/**.test.{js,ts}.
//
// All phase-8 test files use `import { ... } from 'vitest'` to
// match the vitest 4.x ESM-first surface.

import { defineConfig } from 'vitest/config';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

export default defineConfig({
  test: {
    pool: 'forks',
    forks: { singleFork: true },

    include: ['tests/phase-8/**/*.test.{js,mjs,ts}'],
    exclude: ['node_modules/**', 'dist/**', 'coverage/**', '**/.worktrees/**'],

    testTimeout: 30000,
    hookTimeout: 15000,

    reporters: [
      'default',
      ['json', { outputFile: path.resolve(__dirname, 'reports', 'vitest-results.json') }],
    ],
  },
});
