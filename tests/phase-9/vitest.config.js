// Phase 9 test configuration.
// Mirrors phase-8 layout, scoped to tests/phase-9/**.test.{mjs,js}.

import { defineConfig } from 'vitest/config';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

export default defineConfig({
  test: {
    pool: 'forks',
    forks: { singleFork: true },
    include: ['tests/phase-9/**/*.test.{js,mjs,ts}'],
    exclude: ['node_modules/**', 'dist/**', 'coverage/**', '**/.worktrees/**'],
    testTimeout: 60000,
    hookTimeout: 30000,
    reporters: [
      'default',
      ['json', { outputFile: path.resolve(__dirname, 'reports', 'vitest-results.json') }],
    ],
  },
});
