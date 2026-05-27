// substrates/dedup-substrate.js
// Governance substrate: Redis-backed deduplication and idempotency.
//
// Owns: in-flight intent dedup, idempotency key tracking with TTL,
//        replay detection (lineage-aware identity).
// Does NOT own: evaluation logic, orchestration, intent emission,
//               lineage ledger reads, governance interpretation.
//
// Architecture (Phase 4a — Lineage-Aware Identity):
//   Dual-key design:
//     Identity key:  governance:dedup:{accountId}:{actionType}:{resourceId}:{intentId}
//                    → blocks exact duplicates (same intent re-emitted)
//     Resource key:  governance:dedup:resource:{accountId}:{actionType}:{resourceId}
//                    → value = intentId of last intent to touch this resource
//                    → enables replay detection (different intent, same resource)
//
//   Both keys use 120s TTL. Resource key is overwritten on every markInFlight() —
//   always tracks the most recent intent for that resource.
//
//   Local caches: Map-based (was Set) to store structured { intentId, epochId, ts }.
//   Resource tracker persists across ticks (TTL-scoped, not batch-scoped).
//
// Observability emissions:
//   dedup_entry:      PENDING → IN_FLIGHT       (identity key — existing, enriched)
//   dedup_entry:      IN_FLIGHT → CLEARED       (identity key — batch complete)
//   resource_tracker: null → TRACKED             (new — first time resource seen)
//   resource_tracker: TRACKED → REPLAY_DETECTED  (new — different intent on same resource)

const { getRedisClient } = require('../config/redis');

const DEDUP_KEY_PREFIX = 'governance:dedup:';
const RESOURCE_KEY_PREFIX = 'governance:dedup:resource:';
const TTL_SECONDS = 120;
const MAX_LOCAL_ENTRIES = 2000;

// Local read-through caches
const _inFlight = new Map();       // identityKey → { intentId, epochId, ts }
const _resourceTracker = new Map(); // resourceKey → { lastIntentId, ts }

function _makeIdentityKey(accountId, actionType, resourceId, intentId) {
  return `${DEDUP_KEY_PREFIX}${accountId}:${actionType}:${resourceId}:${intentId}`;
}

function _makeResourceKey(accountId, actionType, resourceId) {
  return `${RESOURCE_KEY_PREFIX}${accountId}:${actionType}:${resourceId}`;
}

function _evictOldest(cache) {
  if (cache.size < MAX_LOCAL_ENTRIES) return;
  const entries = [...cache.keys()];
  const evictCount = Math.floor(MAX_LOCAL_ENTRIES * 0.2);
  for (let i = 0; i < evictCount; i++) cache.delete(entries[i]);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Observability helpers
// ═══════════════════════════════════════════════════════════════════════════════

function _emitTransition(params) {
  try {
    const observability = require('../control-plane/observability/emitters/transition-emitter');
    observability.transition(params);
  } catch (err) {
    console.warn('[dedup] Observability transition error:', err.message);
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Public API
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Marks a resource+intent as in-flight — writes to Redis (authoritative) and local cache.
 *
 * Dual-key write:
 *   Identity key SET NX EX — only first writer wins (exact duplicate blocked)
 *   Resource key SET EX — overwrites previous (always tracks latest intent)
 *
 * @param {string} accountId
 * @param {string} actionType
 * @param {string} resourceId
 * @param {object} [opts]
 * @param {string} [opts.intentId] — intent identifier for lineage-aware identity
 * @param {string} [opts.epochId]  — optional reconciliation epoch marker
 */
async function markInFlight(accountId, actionType, resourceId, opts = {}) {
  const intentId = opts.intentId || 'legacy';
  const epochId = opts.epochId || null;
  const now = Date.now();

  const identityKey = _makeIdentityKey(accountId, actionType, resourceId, intentId);
  const resourceKey = _makeResourceKey(accountId, actionType, resourceId);

  // Local caches
  _inFlight.set(identityKey, { intentId, epochId, ts: now });
  _evictOldest(_inFlight);

  const previousIntentId = _resourceTracker.has(resourceKey)
    ? _resourceTracker.get(resourceKey).lastIntentId
    : null;
  _resourceTracker.set(resourceKey, { lastIntentId: intentId, ts: now });
  _evictOldest(_resourceTracker);

  // Observability: identity entry transition (enriched raw payload)
  _emitTransition({
    domain: 'dedup',
    entity: 'dedup_entry',
    entityId: identityKey,
    previousState: 'PENDING',
    nextState: 'IN_FLIGHT',
    authority: 'dedup-substrate',
    raw: { accountId, actionType, resourceId, intentId, epochId },
  });

  // Observability: resource tracker transition (first touch or replay)
  if (previousIntentId === null) {
    _emitTransition({
      domain: 'dedup',
      entity: 'resource_tracker',
      entityId: resourceKey,
      previousState: null,
      nextState: 'TRACKED',
      authority: 'dedup-substrate',
      raw: { accountId, actionType, resourceId, intentId, epochId },
    });
  } else if (previousIntentId !== intentId) {
    _emitTransition({
      domain: 'dedup',
      entity: 'resource_tracker',
      entityId: resourceKey,
      previousState: 'TRACKED',
      nextState: 'REPLAY_DETECTED',
      authority: 'dedup-substrate',
      raw: { accountId, actionType, resourceId, previousIntentId, intentId, epochId },
    });
  }

  // Redis writes (fire-and-forget — failures do not block evaluation)
  const redis = getRedisClient();
  if (redis && redis.status === 'ready') {
    // Identity key: SET NX — only first writer wins
    redis.set(identityKey, String(now), 'EX', TTL_SECONDS, 'NX').catch(() => {});
    // Resource key: SET — always overwrites with latest intent
    redis.set(resourceKey, intentId, 'EX', TTL_SECONDS).catch(() => {});
  }
}

/**
 * Checks if a resource+intent is in-flight — reads local cache first, Redis on miss.
 *
 * Returns a structured result instead of a boolean:
 *   { blocked, reason, existingIntentId }
 *
 * - blocked=true, reason='duplicate'  → same intentId already in-flight (exact duplicate)
 * - blocked=false, reason='replay'    → different intentId previously touched this resource
 * - blocked=false, reason=null        → resource never seen, free to proceed
 *
 * When intentId is null (backward-compat mode), only resource-level check is performed:
 *   - blocked=true if ANY intent touched this resource
 *   - reason is always 'duplicate' in this mode (no replay distinction)
 *
 * @param {string} accountId
 * @param {string} actionType
 * @param {string} resourceId
 * @param {string|null} [intentId=null] — intent identifier for lineage-aware identity
 * @returns {Promise<{ blocked: boolean, reason: string|null, existingIntentId: string|null }>}
 */
async function isInFlight(accountId, actionType, resourceId, intentId = null) {
  const resourceKey = _makeResourceKey(accountId, actionType, resourceId);

  // ── Check identity key (exact duplicate) ──────────────────────────
  if (intentId) {
    const identityKey = _makeIdentityKey(accountId, actionType, resourceId, intentId);
    if (_inFlight.has(identityKey)) {
      return { blocked: true, reason: 'duplicate', existingIntentId: intentId };
    }

    const redis = getRedisClient();
    if (redis && redis.status === 'ready') {
      const exists = await redis.exists(identityKey).catch(() => 0);
      if (exists) {
        _inFlight.set(identityKey, { intentId, epochId: null, ts: Date.now() });
        _evictOldest(_inFlight);
        return { blocked: true, reason: 'duplicate', existingIntentId: intentId };
      }
    }
  }

  // ── Check resource key (replay detection) ─────────────────────────
  // Local cache first
  if (_resourceTracker.has(resourceKey)) {
    const tracker = _resourceTracker.get(resourceKey);
    if (tracker.lastIntentId !== intentId || !intentId) {
      return { blocked: false, reason: tracker.lastIntentId !== intentId ? 'replay' : 'duplicate', existingIntentId: tracker.lastIntentId };
    }
    // same intentId — already caught by identity check above
  }

  // Redis fallback
  const redis = getRedisClient();
  if (redis && redis.status === 'ready') {
    const storedIntentId = await redis.get(resourceKey).catch(() => null);
    if (storedIntentId) {
      _resourceTracker.set(resourceKey, { lastIntentId: storedIntentId, ts: Date.now() });
      _evictOldest(_resourceTracker);

      if (!intentId) {
        return { blocked: true, reason: 'duplicate', existingIntentId: storedIntentId };
      }
      if (storedIntentId !== intentId) {
        return { blocked: false, reason: 'replay', existingIntentId: storedIntentId };
      }
      return { blocked: true, reason: 'duplicate', existingIntentId: storedIntentId };
    }
  }

  return { blocked: false, reason: null, existingIntentId: null };
}

/**
 * Convenience: check if a resource was previously touched by a different intentId.
 * Used by callers that want to classify execution mode (original vs replay).
 *
 * @param {string} accountId
 * @param {string} actionType
 * @param {string} resourceId
 * @param {string} intentId
 * @returns {Promise<boolean>}
 */
async function isReplay(accountId, actionType, resourceId, intentId) {
  const result = await isInFlight(accountId, actionType, resourceId, intentId);
  return result.reason === 'replay';
}

/**
 * Clears the identity cache after each evaluation batch.
 * Resource tracker persists across ticks (TTL-scoped, not batch-scoped).
 * Redis keys self-expire via TTL — no manual DEL needed.
 */
function clearTick() {
  for (const [key, entry] of _inFlight) {
    _emitTransition({
      domain: 'dedup',
      entity: 'dedup_entry',
      entityId: key,
      previousState: 'IN_FLIGHT',
      nextState: 'CLEARED',
      authority: 'dedup-substrate',
      raw: { clearedAt: Date.now(), intentId: entry.intentId, epochId: entry.epochId },
    });
  }
  _inFlight.clear();
}

/**
 * Return a snapshot of current substrate state for reconciliation consumers.
 * Read-only — never mutates. Used by reconciliation engine (Phase 4b).
 *
 * @returns {{ identityCount: number, resourceCount: number, sample: Array<{ key: string, intentId: string }> }}
 */
function getInflightSnapshot() {
  const identitySample = [];
  let i = 0;
  for (const [key, entry] of _inFlight) {
    if (i++ >= 10) break;
    identitySample.push({ key, intentId: entry.intentId });
  }
  return {
    identityCount: _inFlight.size,
    resourceCount: _resourceTracker.size,
    sample: identitySample,
  };
}

module.exports = { markInFlight, isInFlight, isReplay, clearTick, getInflightSnapshot };
