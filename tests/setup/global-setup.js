// ============================================
// Global Test Setup
// Constitutional Runtime Simulation
// ============================================
// Purpose: One-time setup before all test files.
// Flushes Redis test keyspace to ensure clean
// state across the entire test suite run.
// ============================================

import { getRedisClient } from '../../config/redis.js';

/**
 * Global setup — runs once before all test files.
 * Awaits Redis connection and flushes test keys.
 */
export async function setup() {
  const client = getRedisClient();

  // Ensure Redis is ready before flushing
  if (client.status !== 'ready') {
    await new Promise((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('Redis connection timeout in global-setup')), 10000);
      client.once('ready', () => { clearTimeout(timeout); resolve(); });
      client.once('error', (err) => { clearTimeout(timeout); reject(err); });
    });
  }

  // Flush all test:* keys to ensure clean slate
  const testPrefix = 'test:';
  let cursor = '0';

  do {
    const [nextCursor, keys] = await client.scan(cursor, 'MATCH', `${testPrefix}*`, 'COUNT', 500);
    cursor = nextCursor;
    if (keys.length > 0) {
      await client.del(...keys);
    }
  } while (cursor !== '0');

  console.log('[global-setup] Redis test keyspace flushed — ready for test run');
}

/**
 * Global teardown — runs once after all test files complete.
 * Currently a no-op since test-setup.js handles per-test cleanup
 * and Redis is ephemeral in docker-compose.test.yml.
 */
export async function teardown() {
  // No-op: ephemeral containers are torn down by docker-compose down
}
