// Phase 7 test configuration — extends the base tests/vitest.config.js
// with the phase-7-specific setup file that hooks Module._load to
// handle a pre-existing production typo (engagement-substrate requires
// '../../../postgres-telemetry-kernel/readers' but the directory is
// 'reading' — singular). The hook is a test-harness adapter for a
// non-existent module path. It does NOT stub the CK, any FSM, or any
// substrate.
//
// We add the setup via the test environment's NODE_OPTIONS so it loads
// before any test file. The actual hook is in _phase-7-setup.js. The
// runner script (phase-7-runner.sh) sets NODE_OPTIONS=--import
// /app/tests/phase-7/_phase-7-setup-loader.mjs which pre-imports the
// hook file.

import { defineConfig } from 'vitest/config';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

export default defineConfig({
  test: {
    // Forward all base config knobs (timeout, pool, etc.) by reading
    // the base file. We cannot import it dynamically at top-level
    // (CJS build restriction) — so the base config is replicated
    // minimally here. The phase-7 tests only need the setupFile
    // addition; everything else uses vitest defaults.
    setupFiles: [
      path.resolve(__dirname, '_phase-7-setup.js'),
    ],
    include: ['tests/**/*.test.{js,ts}', 'tests/**/*.spec.{js,ts}'],
    exclude: ['node_modules/**', 'dist/**', 'coverage/**', '**/.worktrees/**'],
  },
});
