// ============================================
// Per-Test Setup
// Constitutional Runtime Simulation
// ============================================
// Purpose: Per-test isolation, mock cleanup,
// and substrate state reset between tests.
// ============================================

import { getRedisClient } from '../config/redis.js';

/**
 * Flush Redis test keys to ensure isolation.
 * Uses SCAN to avoid blocking on large keyspaces.
 */
async function flushTestRedisKeys() {
  const client = getRedisClient();
  const testPrefix = 'test:';
  let cursor = '0';

  do {
    const [nextCursor, keys] = await client.scan(cursor, 'MATCH', `${testPrefix}*`, 'COUNT', 100);
    cursor = nextCursor;

    if (keys.length > 0) {
      await client.del(...keys);
    }
  } while (cursor !== '0');
}

/**
 * Per-test setup hook.
 * Runs before each test file.
 */
export function setupTestEnvironment() {
  // Seed deterministic randomness for replay-safe tests
  const seed = process.env.TEST_SEED || Date.now();
  Math.random.seedRandom?.(seed); // if available

  // Reset test keyspace
  flushTestRedisKeys().catch((err) => {
    console.warn('[TestSetup] Redis flush warning:', err.message);
  });
}

/**
 * Per-test teardown hook.
 * Runs after each test file.
 */
export function teardownTestEnvironment() {
  // Clear any lingering test state
  // No-op for Redis since global-setup flushes
}

/**
 * Mock helper: isolate substrate calls.
 * Returns a function that restores original behavior.
 */
export function isolateSubstrate(mock) {
  const original = { ...mock };
  return () => {
    Object.assign(mock, original);
  };
}

export default { setupTestEnvironment, teardownTestEnvironment, isolateSubstrate };
