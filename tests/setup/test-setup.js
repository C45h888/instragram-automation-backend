// ============================================
// Per-Test-File Setup
// Constitutional Runtime Test Suite
// ============================================
// Purpose: Per-test-file Redis flush — runs in the vitest fork
// BEFORE each test file starts. Ensures the test:* keyspace is
// clean so that global-setup's initial flush is the only one
// needed at suite start.
//
// NOTE: This runs inside the vitest fork process. Redis client
// must already be connected (globalSetup runs first and waits
// for getRedisClient() to become ready before any fork is
// spawned). This file just flushes — it does NOT init Redis.
// ============================================

import { getRedisClient } from '../config/redis.js';

/**
 * Flush all test:* keys from Redis.
 * Uses SCAN to avoid blocking on large keyspaces.
 * No-op if Redis is not yet connected (globalSetup guarantees it is).
 */
async function flushTestRedisKeys() {
  const client = getRedisClient();

  // Defensive: if client is not connected, skip flush.
  // This can happen in singleFork=true mode where setupFiles
  // runs after globalSetup's initial connection but before
  // a subsequent test file's setup if there were connectivity issues.
  if (client.status !== 'ready' && client.status !== 'connect') {
    console.warn('[test-setup] Redis not connected (status=%s) — skipping flush', client.status);
    return;
  }

  const prefix = 'test:';
  let cursor = '0';

  do {
    const [nextCursor, keys] = await client.scan(cursor, 'MATCH', `${prefix}*`, 'COUNT', 100);
    cursor = nextCursor;
    if (keys.length > 0) {
      await client.del(...keys);
    }
  } while (cursor !== '0');
}

/**
 * Per-test-file setup hook.
 * Runs once per test file, after globalSetup, before the file's tests.
 */
export async function setupTestEnvironment() {
  await flushTestRedisKeys();
}

/**
 * Per-test-file teardown hook.
 * Currently a no-op — Redis is ephemeral in test containers and
 * setupFiles handles pre-test flush. Add here if you need
 * post-test verification or cleanup.
 */
export function teardownTestEnvironment() {
  // no-op
}

export default { setupTestEnvironment, teardownTestEnvironment };