// ============================================
// Vitest Configuration
// Constitutional Runtime Test Suite
// ============================================
// Purpose: Deterministic governance runtime
// testing with isolated substrate mocking,
// fault injection support, and replay-safe
// execution validation.
// ============================================

import { defineConfig } from 'vitest/config';

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
    // Deterministic Execution
    // ----------------------------------------
    // Deterministic order for replay-safe tests
    unsafeStackTrace: false,

    // Pool strategy for isolation
    pool: 'forks',
    poolOptions: {
      forks: {
        singleFork: false,
      },
    },

    // ----------------------------------------
    // Test Isolation & Cleanup
    // ----------------------------------------
    // Clean up after each test file
    globalSetup: ['<rootDir>/tests/setup/global-setup.js'],
    setupFiles: ['<rootDir>/tests/setup/test-setup.js'],

    // Environment variables for test context
    env: {
      NODE_ENV: 'test',
      REDIS_URL: process.env.REDIS_URL || 'redis://localhost:6379',
      SKIP_DB_TUNNEL: 'true',
    },

    // ----------------------------------------
    // Coverage (when run with --coverage)
    // ----------------------------------------
    coverage: {
      provider: 'v8',
      reporter: ['text', 'json', 'html'],
      exclude: [
        'node_modules/**',
        'tests/**',
        '**/*.test.js',
        '**/*.spec.js',
        'dist/**',
        'coverage/**',
        '**/coverage/**',
      ],
    },

    // ----------------------------------------
    // Test Timeout & Retry
    // ----------------------------------------
    // Phase 5D soak requires 61-minute timeout (1hr soak + 2min teardown)
    testTimeout: 3660000,
    hookTimeout: 120000,

    // Retry failed tests once for flakiness detection
    retry: process.env.CI ? 2 : 0,

    // ----------------------------------------
    // Reporting
    // ----------------------------------------
    reporters: ['default', 'verbose'],
    outputFile: {
      json: './tests/output/test-results.json',
    },

    // ----------------------------------------
    // API: Expose test container utilities
    // ----------------------------------------
    // Custom test APIs exposed via globalThis
    // for governance runtime simulation
  },

  // ----------------------------------------
  // Resolve Configuration
  // ----------------------------------------
  resolve: {
    alias: {
      '@config': '/config',
      '@substrates': '/substrates',
      '@control-plane': '/control-plane',
      '@services': '/services',
      '@lib': '/lib',
    },
  },
});
