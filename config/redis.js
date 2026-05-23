// backend/config/redis.js
// Redis client singleton — shared by all acquisition workers.
//
// Connection from REDIS_URL env var (default: redis://localhost:6379).
// Lazy-initialises on first getRedisClient() call.
// Graceful shutdown via closeRedis().

const Redis = require('ioredis');

let _client = null;
let _closing = false;

/**
 * Returns the shared Redis client. Lazy-initialises on first call.
 * Retries connection up to 3 times with backoff.
 *
 * @returns {import('ioredis').Redis|null}
 */
function getRedisClient() {
  if (_closing) return null;
  if (_client && _client.status === 'ready') return _client;

  const url = process.env.REDIS_URL || 'redis://localhost:6379';

  if (_client) {
    // Existing client in non-ready state — disconnect and recreate
    _client.disconnect();
    _client = null;
  }

  console.log(`[Redis] Connecting to ${url.replace(/\/\/.*@/, '//***@')}...`);

  _client = new Redis(url, {
    maxRetriesPerRequest: 3,
    retryStrategy(times) {
      if (times > 3) {
        console.error('[Redis] Max retries exceeded — giving up');
        return null; // stop retrying
      }
      const delay = Math.min(times * 1000, 3000);
      console.warn(`[Redis] Retry ${times}/3 in ${delay}ms...`);
      return delay;
    },
    lazyConnect: false,
    enableOfflineQueue: false, // fail fast — don't queue commands while disconnected
  });

  _client.on('error', (err) => {
    if (_closing) return;
    console.error('[Redis] Connection error:', err.message);
  });

  _client.on('connect', () => {
    console.log('[Redis] Connected');
  });

  _client.on('close', () => {
    console.log('[Redis] Connection closed');
  });

  return _client;
}

/**
 * Gracefully closes the Redis connection.
 * Idempotent — safe to call multiple times.
 */
async function closeRedis() {
  _closing = true;
  if (_client) {
    console.log('[Redis] Closing connection...');
    await _client.quit().catch(() => {});
    _client = null;
    console.log('[Redis] Closed');
  }
  _closing = false;
}

/**
 * Returns a health check result for the Redis connection.
 *
 * @returns {Promise<{healthy: boolean, error?: string}>}
 */
async function checkRedisHealth() {
  try {
    const client = getRedisClient();
    if (!client || client.status !== 'ready') {
      return { healthy: false, error: 'client not ready' };
    }
    const pong = await client.ping();
    return { healthy: pong === 'PONG' };
  } catch (err) {
    return { healthy: false, error: err.message };
  }
}

module.exports = { getRedisClient, closeRedis, checkRedisHealth };
