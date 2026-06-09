// Minimal vitest config — bypass global setup (Redis dependency) for the
// graph-capability-flow test, which mocks its own I/O.
import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    globals: true,
    environment: 'node',
    include: ['tests/**/*.test.{js,ts}'],
    testTimeout: 30_000,
    pool: 'forks',
    singleFork: true,
    setupFiles: [],
    globalSetup: undefined,
  },
});
