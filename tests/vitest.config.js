// ============================================
// Vitest Configuration
// Constitutional Runtime Test Suite
// ============================================
//
// Unified execution: ALL tests run inside the test-runner container.
// Container env:  REDIS_URL=redis://test-redis:6379
//                 POSTGRES_HOST=test-postgres
//
// Key design decisions:
//   pool=forks + singleFork=true  → one forked process per test file.
//     Prevents cross-test Redis contamination from parallel forks.
//     Sequential within a single `vitest run` invocation.
//   globalSetup                    → flushes Redis test:* keys once at suite start
//   setupFiles                     → per-test-file Redis flush before each file
//   testTimeout=3_800_000           → 5D soak needs up to 1hr + buffer
//
// ============================================

import { defineConfig } from 'vitest/config';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

export default defineConfig({
  test: {
    // ----------------------------------------
    // Execution Mode
    // ----------------------------------------
    globals: true,
    environment: 'node',
    include: ['tests/**/*.test.{js,ts}', 'tests/**/*.spec.{js,ts}'],
    exclude: ['node_modules/**', 'dist/**', 'coverage/**'],

    // ----------------------------------------
    // Test Timeout
    // ----------------------------------------
    // 5B: 5min concurrent + 30s buffer = 330s
    // 5D: 1hr soak + 2min buffer = 3_720s → round to 3_800s
    testTimeout: 3_800_000,

    // ----------------------------------------
    // Stack trace — off for cleaner output
    // ----------------------------------------
    unsafeStackTrace: false,

    // ----------------------------------------
    // Pool isolation
    // ----------------------------------------
    // singleFork=true: one process per test file, sequential.
    // No parallel forks → no cross-test Redis contamination.
    // Vitest 4: poolOptions removed — singleFork is now a top-level option.
    pool: 'forks',
    singleFork: true,

    // ----------------------------------------
    // Setup hooks — Redis cleanup per test file
    // ----------------------------------------
    // Vitest 4: <rootDir> token is not resolved for globalSetup/setupFiles.
    // Use path.resolve with __dirname for explicit absolute paths.
    globalSetup: [path.resolve(__dirname, 'setup/global-setup.js')],
    setupFiles: [path.resolve(__dirname, 'setup/test-setup.js')],

    // ----------------------------------------
    // Environment — container DNS, not localhost
    // ----------------------------------------
    // REDIS_URL and POSTGRES_HOST are injected via docker-compose.yml
    // env block here is for reference only; vitest does not re-read
    // process.env at config-evaluation time.
    //
    // Inside test-runner container:
    //   REDIS_URL    = redis://test-redis:6379
    //   POSTGRES_HOST= test-postgres
    //   NODE_ENV     = test

    // ----------------------------------------
    // Reporter — verbose for CI logs
    // ----------------------------------------
    reporter: ['verbose'],
  },
});