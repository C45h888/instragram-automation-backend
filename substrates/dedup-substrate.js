// substrates/dedup-substrate.js
// Governance substrate: Redis-backed deduplication and idempotency.
//
// Owns: in-flight intent dedup, idempotency key tracking with TTL.
// Does NOT own: evaluation logic, orchestration, intent emission.
//
// Architecture:
//   - Redis is authoritative store for dedup keys (SET NX EX 120s)
//   - Local Set is a read-through cache for fast in-batch checks
//   - TTL expiry is the crash-safety mechanism — no manual cleanup
//   - clearTick() clears local cache only — Redis keys self-expire
//
// Key: governance:dedup:{accountId}:{actionType}:{resourceId}
// TTL: 120s — covers debounce window + evaluation time + margin

const { getRedisClient } = require('../config/redis');

const DEDUP_KEY_PREFIX = 'governance:dedup:';
const TTL_SECONDS = 120;
const MAX_LOCAL_ENTRIES = 2000;

const _inFlight = new Set(); // local read-through cache

function _makeKey(accountId, actionType, resourceId) {
  return `${DEDUP_KEY_PREFIX}${accountId}:${actionType}:${resourceId}`;
}

function _evictOldest() {
  if (_inFlight.size < MAX_LOCAL_ENTRIES) return;
  const entries = [..._inFlight];
  const evictCount = Math.floor(MAX_LOCAL_ENTRIES * 0.2);
  for (let i = 0; i < evictCount; i++) _inFlight.delete(entries[i]);
}

/**
 * Marks a resource as in-flight — writes to Redis (authoritative) and local cache.
 * Uses SET NX EX — only first writer wins, 120s TTL.
 *
 * @param {string} accountId
 * @param {string} actionType
 * @param {string} resourceId
 */
async function markInFlight(accountId, actionType, resourceId) {
  const key = _makeKey(accountId, actionType, resourceId);
  _inFlight.add(key);
  _evictOldest();

  const redis = getRedisClient();
  if (redis && redis.status === 'ready') {
    // NX = only set if not exists (atomic dedup — first writer wins)
    // EX = TTL 120s (crash-safety: Redis auto-expires key if process dies)
    await redis.set(key, '1', 'EX', TTL_SECONDS, 'NX').catch(() => {});
  }
}

/**
 * Checks if a resource is in-flight — reads local cache first, Redis on miss.
 * Async — caller must await.
 *
 * @returns {Promise<boolean>}
 */
async function isInFlight(accountId, actionType, resourceId) {
  const key = _makeKey(accountId, actionType, resourceId);
  if (_inFlight.has(key)) return true;

  const redis = getRedisClient();
  if (redis && redis.status === 'ready') {
    const exists = await redis.exists(key).catch(() => 0);
    if (exists) {
      _inFlight.add(key);
      _evictOldest();
      return true;
    }
  }
  return false;
}

/**
 * Clears the local cache after each evaluation batch.
 * Redis keys self-expire via TTL — no manual DEL needed.
 */
function clearTick() {
  _inFlight.clear();
}

module.exports = { markInFlight, isInFlight, clearTick };
